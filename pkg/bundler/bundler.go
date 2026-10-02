package bundler

import (
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
	"time"
)

type account struct {
	pub     ed25519.PublicKey
	balance uint64
	nonce   uint64
}

// Paymaster 是代付方：拥有独立押金，并按账户设置累计代付额度。
type Paymaster struct {
	Address Address
	Deposit uint64
	Quota   map[Address]uint64
	used    map[Address]uint64
}

// Bundler 是本地内存版打包者，同时维护全部链上状态账本。
type Bundler struct {
	mu        sync.Mutex
	cond      *sync.Cond
	now       func() time.Time
	feePrice  uint64
	nonceWait time.Duration

	accounts    map[Address]*account
	sponsors    map[Address]*Paymaster
	beneficiary Address
	bundlerBal  uint64

	mempool  map[opKey]UserOperation
	rejected map[opKey]bool

	settledBatches map[string]*BatchReceipt
}

// New 创建本地打包者/账本。feePrice 是每笔操作收取的手续费。
func New(feePrice uint64) *Bundler {
	b := &Bundler{
		now:            time.Now,
		feePrice:       feePrice,
		nonceWait:      3 * time.Second,
		accounts:       map[Address]*account{},
		sponsors:       map[Address]*Paymaster{},
		mempool:        map[opKey]UserOperation{},
		rejected:       map[opKey]bool{},
		settledBatches: map[string]*BatchReceipt{},
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// WithClock 注入时间源（测试过期逻辑用）。
func (b *Bundler) WithClock(clock func() time.Time) *Bundler {
	b.now = clock
	return b
}

// OpenAccount 注册智能账户并赋予初始余额。
func (b *Bundler) OpenAccount(pub ed25519.PublicKey, balance uint64) Address {
	addr := AddressFromKey(pub)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.accounts[addr] = &account{pub: pub, balance: balance}
	return addr
}

// RegisterPaymaster 注册代付方，存入押金，并为各账户设置代付额度。
func (b *Bundler) RegisterPaymaster(addr Address, deposit uint64, quota map[Address]uint64) *Paymaster {
	b.mu.Lock()
	defer b.mu.Unlock()
	p := &Paymaster{Address: addr, Deposit: deposit, Quota: quota, used: map[Address]uint64{}}
	b.sponsors[addr] = p
	return p
}

// SetBeneficiary 设置收取手续费的打包者收益地址。
func (b *Bundler) SetBeneficiary(addr Address) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.beneficiary = addr
}

// Balance 返回已登记地址（账户/代付方/打包者）的余额。
func (b *Bundler) Balance(addr Address) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if a, ok := b.accounts[addr]; ok {
		return a.balance
	}
	if p, ok := b.sponsors[addr]; ok {
		return p.Deposit
	}
	if addr == b.beneficiary {
		return b.bundlerBal
	}
	return 0
}

// SponsorUsed 返回代付方对某账户已垫付的累计手续费。
func (b *Bundler) SponsorUsed(sponsor, user Address) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if p, ok := b.sponsors[sponsor]; ok {
		return p.used[user]
	}
	return 0
}

// AccountNonce 返回账户已生效的序号计数。
func (b *Bundler) AccountNonce(addr Address) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if a, ok := b.accounts[addr]; ok {
		return a.nonce
	}
	return 0
}

func (b *Bundler) pendingNonceLocked(sender Address) uint64 {
	next := b.accounts[sender].nonce
	for {
		if _, ok := b.mempool[opKey{sender, next}]; ok {
			next++
			continue
		}
		return next
	}
}

// Submit 校验并接收一笔操作。单个操作被拒绝不影响同批其它操作。
// 同一账户并发提交时按序号严格排序进入 mempool；超前序号等待前序提交。
func (b *Bundler) Submit(op UserOperation) (*OpResult, error) {
	hash := sha256.Sum256(SigningDigest(op))

	b.mu.Lock()
	defer b.mu.Unlock()

	acc, ok := b.accounts[op.Sender]
	if !ok {
		return rejectResult(hash, op, ReasonUnknownAccount, "account is not registered")
	}
	if b.now().Unix() > op.Deadline {
		return rejectResult(hash, op, ReasonExpired,
			fmt.Sprintf("deadline=%d before now=%d", op.Deadline, b.now().Unix()))
	}
	if len(op.Signature) == 0 || !ed25519.Verify(acc.pub, SigningDigest(op), op.Signature) {
		return rejectResult(hash, op, ReasonBadSignature, "signature does not match account public key")
	}

	k := op.key()
	if op.Nonce < acc.nonce || b.rejected[k] {
		return rejectResult(hash, op, ReasonNonceUsed,
			fmt.Sprintf("nonce %d already consumed (current=%d)", op.Nonce, acc.nonce))
	}

	waitDeadline := b.now().Add(b.nonceWait)
	for op.Nonce > b.pendingNonceLocked(op.Sender) {
		if b.waitUntil(waitDeadline) {
			return rejectResult(hash, op, ReasonNonceGap,
				fmt.Sprintf("nonce %d leaves a gap (next=%d)", op.Nonce, b.pendingNonceLocked(op.Sender)))
		}
	}

	if _, dup := b.mempool[k]; dup {
		return rejectResult(hash, op, ReasonDuplicateMempool, "identical nonce is already pending in mempool")
	}
	if rerr := b.checkFeePrefundLocked(op); rerr != nil {
		return &OpResult{OpHash: hash, Accepted: false, Reason: rerr.Reason}, rerr
	}

	b.mempool[k] = op
	b.cond.Broadcast()
	return &OpResult{OpHash: hash, Accepted: true}, nil
}

func rejectResult(hash [32]byte, op UserOperation, reason RejectReason, detail string) (*OpResult, error) {
	return &OpResult{OpHash: hash, Accepted: false, Reason: reason}, newRejectError(op, reason, detail)
}

func (b *Bundler) waitUntil(t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return true
	}
	timer := time.AfterFunc(d, b.cond.Broadcast)
	defer timer.Stop()
	b.cond.Wait()
	return b.now().After(t)
}

// checkFeePrefundLocked 只做只读的手续费可垫付性校验，不发生任何扣减。
// mempool 中已等待结算的操作会预留对应的手续费/额度，避免超额预占。
func (b *Bundler) checkFeePrefundLocked(op UserOperation) *RejectedOpError {
	if op.MaxFee < b.feePrice {
		return newRejectError(op, ReasonFeeTooLow,
			fmt.Sprintf("maxFee=%d < required=%d", op.MaxFee, b.feePrice))
	}
	if op.Sponsor.IsZero() {
		reserved := b.reservedFeeLocked(op.Sender, Address{})
		avail := b.accounts[op.Sender].balance - reserved
		if avail < b.feePrice {
			return newRejectError(op, ReasonAccountInsufficient,
				fmt.Sprintf("account available=%d (balance=%d reserved=%d) < fee=%d",
					avail, b.accounts[op.Sender].balance, reserved, b.feePrice))
		}
		return nil
	}
	p, ok := b.sponsors[op.Sponsor]
	if !ok {
		return newRejectError(op, ReasonUnknownSponsor, "sponsor is not registered")
	}
	depositReserved := b.reservedFeeLocked(Address{}, op.Sponsor)
	if p.Deposit-depositReserved < b.feePrice {
		return newRejectError(op, ReasonSponsorDepositLow,
			fmt.Sprintf("sponsor available=%d (deposit=%d reserved=%d) < fee=%d",
				p.Deposit-depositReserved, p.Deposit, depositReserved, b.feePrice))
	}
	used := p.used[op.Sender]
	quotaReserved := b.reservedQuotaLocked(op.Sponsor, op.Sender)
	quota := p.Quota[op.Sender]
	if used+quotaReserved+b.feePrice > quota {
		return newRejectError(op, ReasonSponsorQuotaExceed,
			fmt.Sprintf("account quota: settled=%d pending=%d plus fee=%d exceeds quota=%d",
				used, quotaReserved, b.feePrice, quota))
	}
	return nil
}

// reservedFeeLocked 统计 mempool 中已预留的手续费：
// sponsor 为零地址时统计某账户自付操作占用的余额；
// 否则统计某代付方所有待结算操作占用的押金。
func (b *Bundler) reservedFeeLocked(sender, sponsor Address) uint64 {
	var sum uint64
	for _, pending := range b.mempool {
		if sponsor.IsZero() {
			if pending.Sender == sender && pending.Sponsor.IsZero() {
				sum += b.feePrice
			}
			continue
		}
		if pending.Sponsor == sponsor {
			sum += b.feePrice
		}
	}
	return sum
}

// reservedQuotaLocked 统计 mempool 中代付方为某账户待结算操作预留的额度。
func (b *Bundler) reservedQuotaLocked(sponsor, sender Address) uint64 {
	var sum uint64
	for _, pending := range b.mempool {
		if pending.Sponsor == sponsor && pending.Sender == sender {
			sum += b.feePrice
		}
	}
	return sum
}

// PendingOps 返回 mempool 中待打包操作数量。
func (b *Bundler) PendingOps() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.mempool)
}

// SettleBatch 按确定顺序（Sender 升序，同账户 Nonce 升序）打包结算。
// 同一 batchID 只结算一次；已结算操作在任何批次重放都会被跳过且不重复扣费。
func (b *Bundler) SettleBatch(batchID string, ops []UserOperation) (*BatchReceipt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if cached, ok := b.settledBatches[batchID]; ok {
		return cached, nil
	}

	totalBefore := b.totalSupplyLocked()

	ordered := make([]UserOperation, len(ops))
	copy(ordered, ops)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Sender != ordered[j].Sender {
			return ordered[i].Sender.String() < ordered[j].Sender.String()
		}
		return ordered[i].Nonce < ordered[j].Nonce
	})

	receipts := make([]OpReceipt, 0, len(ordered))
	seen := map[opKey]bool{}
	var totalFee uint64

	for _, op := range ordered {
		k := op.key()
		hash := sha256.Sum256(SigningDigest(op))

		if seen[k] {
			receipts = append(receipts, skipReceipt(hash, op, "duplicate within batch"))
			continue
		}
		acc := b.accounts[op.Sender]
		if acc == nil {
			receipts = append(receipts, skipReceipt(hash, op, "unknown account"))
			continue
		}
		if _, pending := b.mempool[k]; !pending {
			if b.rejected[k] {
				receipts = append(receipts, skipReceipt(hash, op, "rejected at a previous settlement"))
			} else {
				receipts = append(receipts, skipReceipt(hash, op, "already settled"))
			}
			continue
		}

		r := OpReceipt{OpHash: hash, Sender: op.Sender, Nonce: op.Nonce, FeePaid: b.feePrice}

		// 第一步：整笔原子垫付手续费。条件不足则整笔拒绝，任何余额不变，
		// 操作移出 mempool、序号消耗，避免阻塞后续操作。
		if op.Sponsor.IsZero() {
			if acc.balance < b.feePrice {
				receipts = append(receipts, b.settleReject(hash, op,
					"account cannot prefund fee"))
				continue
			}
			acc.balance -= b.feePrice
			r.Payer = op.Sender
			r.FeeBurden = fmt.Sprintf("fee %d paid by sender %s", b.feePrice, op.Sender)
		} else {
			p := b.sponsors[op.Sponsor]
			if p == nil || p.Deposit < b.feePrice ||
				p.used[op.Sender]+b.feePrice > p.Quota[op.Sender] {
				receipts = append(receipts, b.settleReject(hash, op,
					"sponsor deposit or quota insufficient"))
				continue
			}
			p.Deposit -= b.feePrice
			p.used[op.Sender] += b.feePrice
			r.Payer = p.Address
			r.FeeBurden = fmt.Sprintf("fee %d paid by paymaster %s for sender %s",
				b.feePrice, p.Address, op.Sender)
		}

		// 手续费扣取成功即不可回滚：序号生效，手续费归属打包者。
		b.bundlerBal += b.feePrice
		totalFee += b.feePrice
		acc.nonce++
		delete(b.mempool, k)
		seen[k] = true

		// 第二步：执行操作主体。主体失败不影响手续费归属。
		switch {
		case op.Value == 0:
			r.Executed = true
			r.Status = "executed: no value transfer"
		case acc.balance < op.Value:
			r.Reverted = true
			r.Status = fmt.Sprintf("reverted: balance %d < value %d; fee kept by bundler %s",
				acc.balance, op.Value, b.beneficiary)
		default:
			target, exists := b.accounts[op.To]
			if !exists {
				r.Reverted = true
				r.Status = fmt.Sprintf("reverted: target %s not registered; fee kept by bundler %s",
					op.To, b.beneficiary)
			} else {
				acc.balance -= op.Value
				target.balance += op.Value
				r.Executed = true
				r.Status = fmt.Sprintf("executed: transferred %d to %s", op.Value, op.To)
			}
		}
		receipts = append(receipts, r)
	}

	totalAfter := b.totalSupplyLocked()
	receipt := &BatchReceipt{
		BatchID:     batchID,
		Receipts:    receipts,
		TotalFee:    totalFee,
		TotalBefore: totalBefore,
		TotalAfter:  totalAfter,
		Conserved:   totalBefore == totalAfter,
	}
	b.settledBatches[batchID] = receipt
	return receipt, nil
}

// settleReject 处理结算阶段的整笔拒绝：不扣任何余额，消耗序号。
func (b *Bundler) settleReject(hash [32]byte, op UserOperation, detail string) OpReceipt {
	b.accounts[op.Sender].nonce++
	delete(b.mempool, op.key())
	b.rejected[op.key()] = true
	return OpReceipt{
		OpHash: hash, Sender: op.Sender, Nonce: op.Nonce, Skipped: true,
		FeeBurden: "none",
		Status:    "rejected at settlement: " + detail + "; no balance changed; nonce consumed",
	}
}

func skipReceipt(hash [32]byte, op UserOperation, reason string) OpReceipt {
	return OpReceipt{
		OpHash: hash, Sender: op.Sender, Nonce: op.Nonce, Skipped: true,
		FeeBurden: "none",
		Status:    "skipped: " + reason + ", no charge",
	}
}

// totalSupplyLocked 汇总所有账户、代付方押金与打包者收益，用于守恒校验。
func (b *Bundler) totalSupplyLocked() uint64 {
	var sum uint64
	for _, a := range b.accounts {
		sum += a.balance
	}
	for _, p := range b.sponsors {
		sum += p.Deposit
	}
	sum += b.bundlerBal
	return sum
}

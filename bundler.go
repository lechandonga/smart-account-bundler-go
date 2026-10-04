package bundler

import (
	"fmt"
	"sort"
	"sync"
)

// Bundler collects executable operations from the mempool and settles them in
// a deterministic order. Settlement is idempotent: re-settling the same
// operation is a recorded no-op and never charges twice.
type Bundler struct {
	ID      string // ledger account receiving fees
	ledger  *Ledger
	mempool *Mempool

	mu         sync.Mutex
	settleMu   sync.Mutex // serializes settlement so replays interleave safely
	paymasters map[string]*Paymaster
	receipts   map[string]Receipt
}

func NewBundler(id string, ledger *Ledger, mempool *Mempool) *Bundler {
	return &Bundler{
		ID:         id,
		ledger:     ledger,
		mempool:    mempool,
		paymasters: make(map[string]*Paymaster),
		receipts:   make(map[string]Receipt),
	}
}

func (b *Bundler) RegisterPaymaster(p *Paymaster) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.paymasters[p.ID] = p
}

// Receipt returns the recorded terminal state of an operation.
func (b *Bundler) Receipt(opID string) (Receipt, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.receipts[opID]
	return r, ok
}

// CollectAndSettle drains all currently executable operations and settles
// them in deterministic (sender, nonce) order. One operation's failure never
// changes the outcome of the others.
func (b *Bundler) CollectAndSettle() []Receipt {
	b.settleMu.Lock()
	defer b.settleMu.Unlock()
	executable, timedOut := b.mempool.Process()
	receipts := b.settleMany(executable)
	receipts = append(receipts, b.finalizeTimeouts(timedOut)...)
	return receipts
}

// finalizeTimeouts records the terminal state of operations whose gap wait
// window elapsed. No fee is charged and no balance moves: they never
// executed. Their nonces have already been consumed by the mempool, so a
// later resubmission reports the nonce as used. A terminal receipt that
// already exists (e.g. the operation settled through another entry path
// first) is never overwritten. The caller must hold b.settleMu.
func (b *Bundler) finalizeTimeouts(timedOut []*Operation) []Receipt {
	receipts := make([]Receipt, 0, len(timedOut))
	for _, op := range timedOut {
		r := Receipt{
			OpID:   op.ID(),
			Status: StatusTimedOut,
			Reason: ReasonNonceGapTimeout,
			Detail: fmt.Sprintf(
				"sender %s nonce %d timed out waiting for the gap to fill; no fee charged, no balance changes",
				op.Sender[:8], op.Nonce),
		}
		b.recordReceiptIfAbsent(r)
		b.mempool.markSettled(r.OpID)
		receipts = append(receipts, r)
	}
	return receipts
}

// SettleDirect settles a batch delivered straight from an external scheduler
// scheduler. Admission is literally the queued-submission path: every
// operation — including ones that never entered the queue — goes through
// Mempool.Submit, so signature, expiry, nonce history, settled-content
// replay and capacity conclusions are identical for both entry paths. A
// nonce gap is not rejected: the operation waits in the queue for the
// configured window, exactly like a queued submission, and either takes
// effect in nonce order once the gap fills or ends in the distinguishable
// TIMED_OUT_NONCE_GAP state. A rejected operation never moves balances and
// never affects the rest of the batch.
//
// The returned receipts follow the deterministic (sender, nonce) order of
// the batch, one receipt per distinct operation. Operations still waiting
// for a missing nonce report PENDING; their terminal receipt becomes
// available through Receipt once a later collection settles or times out.
func (b *Bundler) SettleDirect(ops []*Operation) []Receipt {
	b.settleMu.Lock()
	defer b.settleMu.Unlock()

	ordered := append([]*Operation(nil), ops...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Sender != ordered[j].Sender {
			return ordered[i].Sender < ordered[j].Sender
		}
		return ordered[i].Nonce < ordered[j].Nonce
	})

	byID := make(map[string]Receipt, len(ordered))
	ids := make([]string, 0, len(ordered))
	seen := make(map[string]struct{}, len(ordered))
	for _, op := range ordered {
		id := op.ID()
		if _, dup := seen[id]; dup {
			continue // identical operation repeated within the batch
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
		if reason, err := b.mempool.Submit(op); err != nil {
			r := Receipt{OpID: id, Status: StatusRejected, Reason: reason, Detail: err.Error()}
			b.recordReceiptIfAbsent(r)
			byID[id] = r
		}
	}

	executable, timedOut := b.mempool.Process()
	for _, r := range b.settleMany(executable) {
		byID[r.OpID] = r
	}
	for _, r := range b.finalizeTimeouts(timedOut) {
		byID[r.OpID] = r
	}
	// Operations admitted but still blocked by a missing nonce keep waiting
	// for the remainder of the gap window; PENDING is not a terminal state
	// and is deliberately not recorded over any existing receipt.
	for _, op := range ordered {
		id := op.ID()
		if _, done := byID[id]; done {
			continue
		}
		byID[id] = Receipt{OpID: id, Status: StatusPending, Detail: fmt.Sprintf(
			"sender %s nonce %d queued by direct batch, waiting for the missing nonce within the gap window",
			op.Sender[:8], op.Nonce)}
	}

	receipts := make([]Receipt, 0, len(ids))
	for _, id := range ids {
		receipts = append(receipts, byID[id])
	}
	return receipts
}

// Settle settles the given operations in order. It is safe to call with an
// already-settled batch: every operation then resolves to REPLAYED_NOOP and
// no balance moves.
func (b *Bundler) Settle(ops []*Operation) []Receipt {
	b.settleMu.Lock()
	defer b.settleMu.Unlock()
	return b.settleMany(ops)
}

// settleMany performs settlement for ops already admitted/removed from the
// mempool. The caller must hold b.settleMu.
func (b *Bundler) settleMany(ops []*Operation) []Receipt {
	receipts := make([]Receipt, 0, len(ops))
	for _, op := range ops {
		receipts = append(receipts, b.settleOne(op))
	}
	ids := make([]string, 0, len(ops))
	for _, op := range ops {
		ids = append(ids, op.ID())
	}
	b.mempool.markSettled(ids...)
	return receipts
}

func (b *Bundler) recordReceipt(r Receipt) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.receipts[r.OpID] = r
}

// recordReceiptIfAbsent keeps the original terminal receipt when an operation
// ID was already settled and rejected again.
func (b *Bundler) recordReceiptIfAbsent(r Receipt) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.receipts[r.OpID]; !ok {
		b.receipts[r.OpID] = r
	}
}

func (b *Bundler) settleOne(op *Operation) Receipt {
	id := op.ID()

	b.mu.Lock()
	if _, ok := b.receipts[id]; ok {
		b.mu.Unlock()
		// Replay: the original receipt stays recorded; the caller gets a
		// REPLAYED_NOOP and no balance moves.
		return Receipt{OpID: id, Status: StatusReplayed, Detail: "already settled; no balance changes"}
	}
	b.mu.Unlock()

	var r Receipt
	switch {
	case op.Paymaster != "":
		r = b.settleSponsored(op)
	default:
		r = b.settleSelfPaid(op)
	}
	r.OpID = id

	b.mu.Lock()
	b.receipts[id] = r
	b.mu.Unlock()
	return r
}

// settleSponsored charges the fee to the paymaster. The charge is
// all-or-nothing: if the deposit or the per-account allowance cannot cover
// the full fee, the operation is rejected and no balance moves anywhere.
func (b *Bundler) settleSponsored(op *Operation) Receipt {
	b.mu.Lock()
	pm, ok := b.paymasters[op.Paymaster]
	b.mu.Unlock()
	if !ok {
		return Receipt{Status: StatusRejected, Reason: ReasonPaymasterDepositTooLow,
			Detail: fmt.Sprintf("unknown paymaster %q", op.Paymaster)}
	}
	if reason, ok := pm.canSponsor(b.ledger, op.Sender, op.Fee); !ok {
		return Receipt{Status: StatusRejected, Reason: reason,
			Detail: fmt.Sprintf("paymaster %s cannot cover fee %d for %s; rejected atomically", pm.ID, op.Fee, op.Sender[:8])}
	}
	pm.charge(b.ledger, b.ID, op.Sender, op.Fee)
	return b.execute(op)
}

// settleSelfPaid charges the fee to the sender, atomically.
func (b *Bundler) settleSelfPaid(op *Operation) Receipt {
	if !b.ledger.canCover(op.Sender, op.Fee) {
		return Receipt{Status: StatusRejected, Reason: ReasonInsufficientFunds,
			Detail: fmt.Sprintf("sender %s cannot cover fee %d; rejected atomically", op.Sender[:8], op.Fee)}
	}
	b.ledger.transfer(op.Sender, b.ID, op.Fee)
	return b.execute(op)
}

// execute applies the value transfer. Execution failure (op.Fail) does not
// refund the fee: the bundler keeps it as compensation for the attempted
// execution, which makes the failed operation's fee ownership explicit.
func (b *Bundler) execute(op *Operation) Receipt {
	if op.Fail {
		return Receipt{Status: StatusFailed,
			Detail: fmt.Sprintf("execution failed; fee %d kept by bundler %s, value transfer skipped", op.Fee, b.ID)}
	}
	if op.Value > 0 {
		if !b.ledger.canCover(op.Sender, op.Value) {
			return Receipt{Status: StatusFailed,
				Detail: fmt.Sprintf("insufficient funds for value %d; fee %d kept by bundler %s", op.Value, op.Fee, b.ID)}
		}
		b.ledger.transfer(op.Sender, op.Target, op.Value)
	}
	return Receipt{Status: StatusSuccess, Detail: fmt.Sprintf("settled; fee %d to bundler %s", op.Fee, b.ID)}
}

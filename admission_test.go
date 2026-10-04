package bundler

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gapFixture returns a fixture whose mempool clock is driven by the returned
// atomic counter, so gap-wait timeouts are deterministic.
func gapFixture(t *testing.T, window time.Duration) (*Ledger, *Mempool, *Bundler, *int64) {
	t.Helper()
	ledger := NewLedger()
	pool := NewMempool()
	pool.GapWaitWindow = window
	var now int64 = 1_000_000
	pool.SetClock(func() time.Time { return time.Unix(atomic.LoadInt64(&now), 0) })
	b := NewBundler("bundler-1", ledger, pool)
	return ledger, pool, b, &now
}

// TestDirectBatchAdmissionBoundaries covers: an externally delivered direct
// batch faces exactly the queued-submission rules. A forged signature on an
// operation that never entered the queue and a reused nonce with different
// content are rejected with the same distinguishable reasons Submit would
// give; rejections move no balances and do not affect valid batch siblings.
func TestDirectBatchAdmissionBoundaries(t *testing.T) {
	t.Log("业务覆盖: 外部直送批次与排队入口同一套准入规则(签名/有效期/序号历史), 拒绝不影响同批其它操作")
	ledger, pool, b := newFixture(t)
	acct := mustAccount(t)
	ledger.Mint(acct.Address, 10_000)
	totalBefore := ledger.Total()

	// An operation that never went through the queue, with a forged signature.
	forged := acct.NewOperation(3, "", 0, 1, "", 0, false)
	forged.Signature = append([]byte(nil), forged.Signature...)
	forged.Signature[0] ^= 0xff
	// Same nonce 0 as the valid op below, but different content (fee 2).
	conflict := acct.NewOperation(0, "", 0, 2, "", 0, false)
	valid := acct.NewOperation(0, "", 0, 1, "", 0, false)
	next := acct.NewOperation(1, "", 0, 1, "", 0, false)

	receipts := b.SettleDirect([]*Operation{valid, conflict, next, forged})
	if len(receipts) != 4 {
		t.Fatalf("expected 4 receipts, got %d", len(receipts))
	}
	if receipts[0].Status != StatusSuccess {
		t.Fatalf("valid op: %s", receipts[0])
	}
	if receipts[1].Status != StatusRejected || receipts[1].Reason != ReasonNonceUsed {
		t.Fatalf("conflicting nonce: %s", receipts[1])
	}
	t.Logf("判定依据: 同序号不同内容 -> %s, 与排队入口结论一致", receipts[1].Reason)
	if receipts[2].Status != StatusSuccess {
		t.Fatalf("next nonce op: %s", receipts[2])
	}
	if receipts[3].Status != StatusRejected || receipts[3].Reason != ReasonBadSignature {
		t.Fatalf("forged op: %s", receipts[3])
	}
	t.Logf("判定依据: 从未进过队列的伪造签名 -> %s, 与排队入口结论一致", receipts[3].Reason)

	// The queued entry must agree with the direct batch on the same inputs.
	if reason, err := pool.Submit(forged); reason != ReasonBadSignature || err == nil {
		t.Fatalf("queued forged op: reason=%s err=%v", reason, err)
	}
	if reason, err := pool.Submit(conflict); reason != ReasonNonceUsed || err == nil {
		t.Fatalf("queued conflicting op: reason=%s err=%v", reason, err)
	}
	t.Log("判定依据: 同一输入改走排队入口, 拒绝原因完全相同")

	// Only the two valid operations charged fees; rejected ones moved nothing.
	if got := ledger.Balance(b.ID); got != 2 {
		t.Fatalf("bundler balance=%d, want 2 (only valid ops charged)", got)
	}
	if got := ledger.Balance(acct.Address); got != 10_000-2 {
		t.Fatalf("account balance=%d, want %d", got, 10_000-2)
	}
	t.Log("判定依据: 被拒操作未引起账户/打包者任何余额变动, 同批合法操作正常结算")
	assertConserved(t, ledger, totalBefore)
}

// TestDirectBatchGapWaitsLikeQueue covers: a nonce gap in a direct batch is
// not rejected on the spot; the operation waits within the configured window
// and takes effect in nonce order once the missing nonce arrives — the same
// behavior as a queued submission.
func TestDirectBatchGapWaitsLikeQueue(t *testing.T) {
	t.Log("业务覆盖: 外部直送批次遇到序号缺口, 按可配置窗口等待, 补齐后按序号依次生效")
	ledger, pool, b, _ := gapFixture(t, 5*time.Second)
	acct := mustAccount(t)
	sink := mustAccount(t)
	ledger.Mint(acct.Address, 10_000)
	totalBefore := ledger.Total()

	// Direct batch contains only nonce 1 while nonce 0 is missing.
	op1 := acct.NewOperation(1, sink.Address, 100, 1, "", 0, false)
	receipts := b.SettleDirect([]*Operation{op1})
	if len(receipts) != 1 || receipts[0].Status != StatusPending {
		t.Fatalf("gap op should wait, got %v", receipts)
	}
	t.Logf("判定依据: 缺号操作未被拒绝, 状态=%s 进入等待窗口", receipts[0].Status)
	if ledger.Balance(acct.Address) != 10_000 || ledger.Balance(b.ID) != 0 {
		t.Fatal("waiting op moved balances")
	}

	// The missing nonce arrives through the queued entry; both take effect
	// in nonce order regardless of entry path.
	op0 := acct.NewOperation(0, sink.Address, 50, 1, "", 0, false)
	if _, err := pool.Submit(op0); err != nil {
		t.Fatal(err)
	}
	receipts = b.CollectAndSettle()
	if len(receipts) != 2 || receipts[0].Status != StatusSuccess || receipts[1].Status != StatusSuccess {
		t.Fatalf("gap fill should settle both in order: %v", receipts)
	}
	if receipts[0].OpID != op0.ID() || receipts[1].OpID != op1.ID() {
		t.Fatalf("settlement order not nonce-ordered: %v", receipts)
	}
	if got := ledger.Balance(sink.Address); got != 150 {
		t.Fatalf("sink balance=%d, want 150", got)
	}
	t.Log("判定依据: 缺口补齐后两笔按序号 0,1 顺序生效, 转账与手续费完整")
	assertConserved(t, ledger, totalBefore)
}

// TestNonceGapTimeout covers: when the missing nonce never arrives within
// the configured window, blocked operations end in the distinguishable
// TIMED_OUT_NONCE_GAP state, no balance moves, and late arrivals report the
// nonce as already used. Applies identically to both entry paths.
func TestNonceGapTimeout(t *testing.T) {
	t.Log("业务覆盖: 序号缺口等待窗口耗尽, 给出可区分超时终态且无任何余额变动")
	ledger, pool, b, now := gapFixture(t, 5*time.Second)
	acct := mustAccount(t)
	ledger.Mint(acct.Address, 10_000)
	totalBefore := ledger.Total()

	queued := acct.NewOperation(1, "", 0, 1, "", 0, false)   // queued entry
	direct := acct.NewOperation(2, "", 0, 1, "", 0, false)   // direct entry
	if _, err := pool.Submit(queued); err != nil {
		t.Fatal(err)
	}
	if r := b.SettleDirect([]*Operation{direct}); r[0].Status != StatusPending {
		t.Fatalf("direct gap op should wait: %v", r)
	}

	atomic.AddInt64(now, 6) // window (5s) elapses
	receipts := b.CollectAndSettle()
	if len(receipts) != 2 {
		t.Fatalf("expected 2 timeout receipts, got %d", len(receipts))
	}
	for _, r := range receipts {
		if r.Status != StatusTimedOut || r.Reason != ReasonNonceGapTimeout {
			t.Fatalf("expected distinguishable timeout, got %s", r)
		}
	}
	t.Logf("判定依据: 两个入口的等待操作均得到终态 %s / %s", StatusTimedOut, ReasonNonceGapTimeout)
	if ledger.Balance(acct.Address) != 10_000 || ledger.Balance(b.ID) != 0 {
		t.Fatal("timed-out ops moved balances")
	}
	t.Log("判定依据: 超时未收取任何手续费, 账户与打包者余额不变")

	// The window consumed the nonces: the missing nonce and the blocked
	// ones report NONCE_ALREADY_USED on both entry paths.
	late := acct.NewOperation(0, "", 0, 1, "", 0, false)
	if reason, err := pool.Submit(late); reason != ReasonNonceUsed || err == nil {
		t.Fatalf("late gap filler: reason=%s err=%v", reason, err)
	}
	if r := b.SettleDirect([]*Operation{acct.NewOperation(1, "", 0, 2, "", 0, false)}); r[0].Reason != ReasonNonceUsed {
		t.Fatalf("direct late op: %s", r[0])
	}
	t.Log("判定依据: 窗口耗尽后序号已被消费, 迟到操作在两个入口均判 NONCE_ALREADY_USED")
	assertConserved(t, ledger, totalBefore)
}

// TestCrossEntrySingleTerminalState covers: an operation suspended in the
// queue can take effect only once across both entry paths; its terminal
// receipt is unique and never overwritten by a later timeout, and the
// suspended record converges instead of hanging.
func TestCrossEntrySingleTerminalState(t *testing.T) {
	t.Log("业务覆盖: 队列挂起与外部直送交叉时, 同序号最多生效一次, 终态唯一不被覆盖")
	ledger, pool, b, now := gapFixture(t, 5*time.Second)
	acct := mustAccount(t)
	ledger.Mint(acct.Address, 10_000)
	totalBefore := ledger.Total()

	// The operation is executable (nonce 0) but still suspended in the queue
	// when the same operation arrives in a direct batch.
	op := acct.NewOperation(0, "", 0, 1, "", 0, false)
	if _, err := pool.Submit(op); err != nil {
		t.Fatal(err)
	}
	receipts := b.SettleDirect([]*Operation{op})
	if receipts[0].Status != StatusSuccess {
		t.Fatalf("direct batch should settle the queued op once: %s", receipts[0])
	}
	if got := ledger.Balance(b.ID); got != 1 {
		t.Fatalf("bundler balance=%d, want exactly one fee", got)
	}

	// The suspended record must have converged: no duplicate settlement and
	// no timeout receipt may appear or overwrite the SUCCESS terminal state.
	atomic.AddInt64(now, 60)
	if leftover := b.CollectAndSettle(); len(leftover) != 0 {
		t.Fatalf("expected no further receipts, got %v", leftover)
	}
	r, ok := b.Receipt(op.ID())
	if !ok || r.Status != StatusSuccess {
		t.Fatalf("terminal receipt overwritten or missing: %v ok=%v", r, ok)
	}
	t.Log("判定依据: 成功回执唯一且未被后续超时覆盖, 挂起记录已收敛")

	// Same nonce with different content can never take effect afterwards.
	other := acct.NewOperation(0, "", 0, 9, "", 0, false)
	if reason, err := pool.Submit(other); reason != ReasonNonceUsed || err == nil {
		t.Fatalf("same nonce different content: reason=%s err=%v", reason, err)
	}
	if r2 := b.SettleDirect([]*Operation{other}); r2[0].Reason != ReasonNonceUsed {
		t.Fatalf("direct same-nonce op: %s", r2[0])
	}
	if got := ledger.Balance(b.ID); got != 1 {
		t.Fatalf("bundler balance=%d, want still 1 (no double effect)", got)
	}
	t.Log("判定依据: 同序号不同内容在两个入口均被拒, 序号只生效一次")
	assertConserved(t, ledger, totalBefore)
}

// TestConcurrentPendingCapacity covers: many goroutines racing to suspend
// operations observe the per-account and global pending caps exactly; the
// cap is never exceeded and every overflow is rejected with the
// distinguishable capacity reason. Run with -race.
func TestConcurrentPendingCapacity(t *testing.T) {
	t.Log("业务覆盖: 并发抢占挂起容量上限, 超限拒绝原因可区分且上限不被突破")
	ledger := NewLedger()
	pool := NewMempool()
	pool.MaxPendingPerAccount = 10
	pool.MaxPendingTotal = 15
	b := NewBundler("bundler-1", ledger, pool)
	acct := mustAccount(t)
	other := mustAccount(t)
	ledger.Mint(acct.Address, 1_000_000)
	ledger.Mint(other.Address, 1_000_000)

	const attempts = 40
	var admitted, cappedPerAccount, cappedGlobal int64
	var wg sync.WaitGroup
	// All nonces start at 1 so nothing is executable and everything
	// competes for suspended capacity.
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			a := acct
			if i%2 == 1 {
				a = other
			}
			op := a.NewOperation(uint64(i/2+1), "", 0, 1, "", 0, false)
			reason, err := pool.Submit(op)
			if err == nil {
				atomic.AddInt64(&admitted, 1)
				return
			}
			switch reason {
			case ReasonPendingCapPerAccount:
				atomic.AddInt64(&cappedPerAccount, 1)
			case ReasonPendingCapGlobal:
				atomic.AddInt64(&cappedGlobal, 1)
			default:
				t.Errorf("unexpected rejection: %s (%v)", reason, err)
			}
		}(i)
	}
	wg.Wait()

	total, perAcct := pool.PendingCount(acct.Address)
	_, perOther := pool.PendingCount(other.Address)
	if perAcct > 10 || perOther > 10 {
		t.Fatalf("per-account cap exceeded: %d / %d", perAcct, perOther)
	}
	if total > 15 {
		t.Fatalf("global cap exceeded: %d", total)
	}
	if admitted != int64(total) {
		t.Fatalf("admitted=%d but pending total=%d", admitted, total)
	}
	if admitted+cappedPerAccount+cappedGlobal != attempts {
		t.Fatalf("outcomes do not add up: %d+%d+%d != %d",
			admitted, cappedPerAccount, cappedGlobal, attempts)
	}
	t.Logf("判定依据: 挂起数 账户A=%d 账户B=%d 总计=%d, 未超上限(10/15); 超限拒绝 %d+%d 笔原因可区分",
		perAcct, perOther, total, cappedPerAccount, cappedGlobal)

	// Capacity frees up once gaps fill and operations settle. The admitted
	// nonces are not contiguous, so fill the gaps over a few rounds: each
	// round drains the contiguous run, freeing capacity for the next gap.
	settled := 0
	for round := 0; round < 25; round++ {
		for n := 0; n <= attempts/2; n++ {
			_, _ = pool.Submit(acct.NewOperation(uint64(n), "", 0, 1, "", 0, false))
			_, _ = pool.Submit(other.NewOperation(uint64(n), "", 0, 1, "", 0, false))
		}
		for _, r := range b.CollectAndSettle() {
			if r.Status != StatusSuccess {
				t.Fatalf("settled op failed: %s", r)
			}
			settled++
		}
		if total, _ := pool.PendingCount(acct.Address); total == 0 {
			break
		}
	}
	if total, _ := pool.PendingCount(acct.Address); total != 0 {
		t.Fatalf("pending not drained: %d", total)
	}
	t.Logf("判定依据: 缺口逐轮补齐后 %d 笔操作全部按序结算, 挂起容量完全释放", settled)
}

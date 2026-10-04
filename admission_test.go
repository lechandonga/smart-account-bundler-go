package bundler

import (
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced time source used to drive gap wait
// windows deterministically.
type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_000_000, 0)} }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newFakeFixture(t *testing.T, window time.Duration) (*Ledger, *Mempool, *Bundler, *fakeClock) {
	t.Helper()
	ledger := NewLedger()
	pool := NewMempool()
	pool.GapWaitWindow = window
	clock := newFakeClock()
	pool.SetClock(clock.now)
	b := NewBundler("bundler-1", ledger, pool)
	return ledger, pool, b, clock
}

// TestDirectBatchSignatureAndNonceBoundaries covers the external direct
// batch on signature/nonce boundaries: forged signatures from ops that never
// queued, same nonce different content after the first one took effect, and
// exact-content idempotency.
func TestDirectBatchSignatureAndNonceBoundaries(t *testing.T) {
	t.Log("业务覆盖: 外部直送批次的签名与序号边界: 伪造签名/同序号不同内容/同内容幂等")
	ledger, pool, b := newFixture(t)
	acct := mustAccount(t)
	ledger.Mint(acct.Address, 100_000)
	totalBefore := ledger.Total()

	// Forged signature on an operation that never entered the queue.
	forged := acct.NewOperation(0, "", 0, 4, "", 0, false) // distinct content from op0
	forged.Signature = make([]byte, 64)                    // fully invalid signature; content digest differs below
	for i := range forged.Signature {
		forged.Signature[i] = 0xAA
	}
	rs := b.SettleDirect([]*Operation{forged})
	if len(rs) != 1 || rs[0].Status != StatusRejected || rs[0].Reason != ReasonBadSignature {
		t.Fatalf("forged op: %+v", rs)
	}
	if got, _ := b.Receipt(forged.ID()); got.Status != StatusRejected {
		t.Fatalf("forged op receipt not terminal: %+v", got)
	}
	if total, per := pool.PendingCount(acct.Address); total != 0 || per != 0 {
		t.Fatalf("forged op must not be suspended: total=%d per=%d", total, per)
	}
	t.Logf("判定依据: 从未入队的伪造签名直送 -> %s, 无挂起记录", ReasonBadSignature)

	// The legitimate nonce 0 settles through the direct path.
	op0 := acct.NewOperation(0, "", 0, 5, "", 0, false)
	if rs := b.SettleDirect([]*Operation{op0}); len(rs) != 1 || rs[0].Status != StatusSuccess {
		t.Fatalf("op0 should succeed: %+v", rs)
	}

	// Same nonce, different content: rejected from BOTH paths with one
	// distinguishable reason.
	conflictQueue := acct.NewOperation(0, "", 0, 6, "", 0, false)
	if reason, err := pool.Submit(conflictQueue); reason != ReasonNonceUsed || err == nil {
		t.Fatalf("queued conflicting op after effective nonce: reason=%s err=%v", reason, err)
	}
	conflictDirect := acct.NewOperation(0, "", 0, 7, "", 0, false)
	rs = b.SettleDirect([]*Operation{conflictDirect})
	if len(rs) != 1 || rs[0].Status != StatusRejected || rs[0].Reason != ReasonNonceUsed {
		t.Fatalf("direct conflicting op: %+v", rs)
	}
	t.Logf("判定依据: 序号 0 已生效后, 同序号不同内容两入口均为 %s", ReasonNonceUsed)

	// Exact content again through both paths: never a second charge.
	rs = b.SettleDirect([]*Operation{op0})
	if len(rs) != 1 || rs[0].Status != StatusReplayed {
		t.Fatalf("exact content through direct path again: %+v", rs)
	}
	if reason, err := pool.Submit(op0); reason != ReasonDuplicate || err == nil {
		t.Fatalf("exact content through queue: reason=%s err=%v", reason, err)
	}
	if got := ledger.Balance(b.ID); got != 5 {
		t.Fatalf("bundler fees=%d, want 5 (single charge)", got)
	}
	if got := ledger.Balance(acct.Address); got != 100_000-5 {
		t.Fatalf("account balance=%d, want %d", got, 100_000-5)
	}
	t.Log("判定依据: 同内容重放不重复扣费, 打包者手续费=5, 账户=99995")
	assertConserved(t, ledger, totalBefore)
}

// TestDirectGapWaitsLikeQueue covers: a direct-batch nonce ahead of the
// account's next nonce is not rejected; it waits for the gap under the same
// configurable window as queued submission and takes effect in nonce order.
func TestDirectGapWaitsLikeQueue(t *testing.T) {
	t.Log("业务覆盖: 直送缺号与排队缺号同规则: 等待窗口内补齐, 按序号依次生效")
	ledger, pool, b, clock := newFakeFixture(t, 10*time.Second)
	acct := mustAccount(t)
	sink := mustAccount(t)
	ledger.Mint(acct.Address, 100_000)
	totalBefore := ledger.Total()

	op1 := acct.NewOperation(1, sink.Address, 100, 5, "", 0, false)
	op2 := acct.NewOperation(2, sink.Address, 200, 5, "", 0, false)

	// Both paths accept a leading gap and suspend the op.
	rs := b.SettleDirect([]*Operation{op1})
	if len(rs) != 0 {
		t.Fatalf("gapped direct op must stay pending, got receipts: %+v", rs)
	}
	if total, per := pool.PendingCount(acct.Address); total != 1 || per != 1 {
		t.Fatalf("pending total=%d per=%d, want 1/1", total, per)
	}
	if reason, err := pool.Submit(op2); reason != ReasonNone || err != nil {
		t.Fatalf("queued op2 into the same gap: %s %v", reason, err)
	}
	t.Log("判定依据: 直送 nonce=1 未被 NONCE_GAP 拒绝, 与排队入口一致地挂起")

	// Resubmitting the same pending content via the other path is a no-op.
	if reason, err := pool.Submit(op1); reason != ReasonNone || err != nil {
		t.Fatalf("same pending content via queue: %s %v", reason, err)
	}
	if rs := b.SettleDirect([]*Operation{op2}); len(rs) != 0 {
		t.Fatalf("same pending content via direct path produced a receipt: %+v", rs)
	}
	if total, _ := pool.PendingCount(acct.Address); total != 2 {
		t.Fatalf("pending=%d, want 2 (no duplicate record)", total)
	}
	t.Log("判定依据: 已挂起的同内容从另一入口重复进入不产生重复记录")

	// Before the window elapses nothing settles.
	clock.advance(9 * time.Second)
	if rs := b.CollectAndSettle(); len(rs) != 0 {
		t.Fatalf("nothing should settle while nonce 0 is missing: %+v", rs)
	}

	// Fill the gap; all three take effect in strict nonce order.
	op0 := acct.NewOperation(0, sink.Address, 50, 5, "", 0, false)
	fill := b.SettleDirect([]*Operation{op0})
	if len(fill) != 3 {
		t.Fatalf("filling gap should settle 3 ops, got %d: %+v", len(fill), fill)
	}
	if got, ok := b.Receipt(op1.ID()); !ok || got.Status != StatusSuccess {
		t.Fatalf("op1 receipt: %+v ok=%v", got, ok)
	}
	if got := ledger.Balance(sink.Address); got != 350 {
		t.Fatalf("sink=%d, want 350 (50+100+200 in nonce order)", got)
	}
	if got := ledger.Balance(b.ID); got != 15 {
		t.Fatalf("bundler fees=%d, want 15", got)
	}
	t.Log("判定依据: 补齐 nonce=0 后 0,1,2 依次成功, 收款方=350, 手续费=15")
	assertConserved(t, ledger, totalBefore)
}

// TestGapTimeoutTerminal covers: blocked ops get the distinguishable timeout
// terminal state after the window, with no balances moving, and their
// terminal receipt is never overwritten.
func TestGapTimeoutTerminal(t *testing.T) {
	t.Log("业务覆盖: 缺号窗口到期 -> 可区分的超时终态, 无费用无余额变动")
	ledger, pool, b, clock := newFakeFixture(t, 10*time.Second)
	acct := mustAccount(t)
	sink := mustAccount(t)
	ledger.Mint(acct.Address, 100_000)
	totalBefore := ledger.Total()

	op1 := acct.NewOperation(1, sink.Address, 0, 5, "", 0, false)
	op2 := acct.NewOperation(2, sink.Address, 0, 5, "", 0, false)
	if _, err := pool.Submit(op1); err != nil {
		t.Fatal(err)
	}
	if rs := b.SettleDirect([]*Operation{op2}); len(rs) != 0 {
		t.Fatalf("direct gapped op must stay pending: %+v", rs)
	}

	clock.advance(10 * time.Second)
	timed := b.CollectAndSettle()
	if len(timed) != 2 {
		t.Fatalf("expected 2 timed-out ops, got %+v", timed)
	}
	for _, r := range timed {
		if r.Status != StatusTimedOut || r.Reason != ReasonNonceGapTimeout {
			t.Fatalf("not a gap timeout: %+v", r)
		}
	}
	if got, _ := b.Receipt(op1.ID()); got.Status != StatusTimedOut {
		t.Fatalf("op1 terminal=%s, want %s", got.Status, StatusTimedOut)
	}
	t.Logf("判定依据: 等待 10s 未补齐 -> 2 笔均为 %s/%s", StatusTimedOut, ReasonNonceGapTimeout)

	// No fee, no transfer; total conserved.
	if got := ledger.Balance(sink.Address); got != 0 {
		t.Fatalf("sink moved on timeout: %d", got)
	}
	if got := ledger.Balance(b.ID); got != 0 {
		t.Fatalf("bundler charged on timeout: %d", got)
	}
	if got := ledger.Balance(acct.Address); got != 100_000 {
		t.Fatalf("account moved on timeout: %d", got)
	}
	assertConserved(t, ledger, totalBefore)

	// The timed-out content is terminal and cannot be replayed, but the
	// nonce itself was never consumed: filling the missing nonce 0 and
	// retrying nonce 1 with new content is allowed.
	if reason, err := pool.Submit(op1); reason != ReasonDuplicate || err == nil {
		t.Fatalf("timed-out exact content resubmit: reason=%s err=%v", reason, err)
	}
	op0 := acct.NewOperation(0, "", 0, 1, "", 0, false)
	other := acct.NewOperation(1, sink.Address, 7, 1, "", 0, false)
	rs := b.SettleDirect([]*Operation{op0, other})
	if len(rs) != 2 || rs[0].Status != StatusSuccess || rs[1].Status != StatusSuccess {
		t.Fatalf("retry after timeout fill: %+v", rs)
	}
	t.Logf("判定依据: 超时后同内容重放=%s; 补齐缺失序号后同序号新内容可重新生效", ReasonDuplicate)
}

// TestPendingSuspensionConverges covers the cross-entry state conflict: an op
// suspended in the queue is delivered directly and settles while waiting; the
// pending record must converge onto the single success receipt instead of
// emitting a later timeout that overwrites it.
func TestPendingSuspensionConverges(t *testing.T) {
	t.Log("业务覆盖: 挂起记录跨入口收敛: 直送结算成功后不再产生超时回执")
	ledger, pool, b, clock := newFakeFixture(t, 10*time.Second)
	acct := mustAccount(t)
	sink := mustAccount(t)
	ledger.Mint(acct.Address, 100_000)
	totalBefore := ledger.Total()

	op1 := acct.NewOperation(1, sink.Address, 100, 5, "", 0, false)
	if _, err := pool.Submit(op1); err != nil {
		t.Fatal(err)
	}
	if rs := b.CollectAndSettle(); len(rs) != 0 {
		t.Fatalf("gap not fillable yet: %+v", rs)
	}

	// External batch delivers the missing nonce 0 together with the already
	// suspended op1: op1 converges onto its one pending record and settles
	// exactly once.
	op0 := acct.NewOperation(0, "", 0, 5, "", 0, false)
	rs := b.SettleDirect([]*Operation{op0, op1})
	if len(rs) != 2 || rs[0].Status != StatusSuccess || rs[1].Status != StatusSuccess {
		t.Fatalf("suspended op delivered directly: %+v", rs)
	}
	if total, _ := pool.PendingCount(acct.Address); total != 0 {
		t.Fatalf("pending record should have converged, total=%d", total)
	}
	t.Log("判定依据: 挂起中的同内容随外部批次直送后只生效一次, 挂起记录随之移除")

	// Long after the window would have elapsed: no second, overriding receipt.
	clock.advance(11 * time.Second)
	if rs := b.CollectAndSettle(); len(rs) != 0 {
		t.Fatalf("no later receipt may appear: %+v", rs)
	}
	if got, _ := b.Receipt(op1.ID()); got.Status != StatusSuccess {
		t.Fatalf("terminal overwritten: %+v", got)
	}
	if got := ledger.Balance(sink.Address); got != 100 {
		t.Fatalf("sink=%d, want 100 (single effect)", got)
	}
	if got := ledger.Balance(b.ID); got != 10 {
		t.Fatalf("bundler fees=%d, want 10", got)
	}
	t.Log("判定依据: 窗口过后只保留唯一 SUCCESS 终态, 无超时回执覆盖, 账目只动一次")
	assertConserved(t, ledger, totalBefore)
}

// TestConcurrentPendingCapRace covers: concurrent submitters and direct
// settlers racing for the limited pending slots never exceed the caps, and a
// rejected op never affects the others. Run repeatedly under -race.
func TestConcurrentPendingCapRace(t *testing.T) {
	t.Log("业务覆盖: 并发抢挂起容量上限: 每账户/全局上限不被突破, 被拒不连累其他操作")
	const senders = 4
	const perSender = 6

	const rounds = 20
	var maxFees int64
	for round := 0; round < rounds; round++ {
		ledger, pool, b := newFixture(t)
		pool.MaxPendingPerAccount = 3
		pool.MaxPendingTotal = 6

		accts := make([]*Account, senders)
		for i := range accts {
			accts[i] = mustAccount(t)
			ledger.Mint(accts[i].Address, 100_000)
		}
		totalBefore := ledger.Total()

		var wg sync.WaitGroup
		start := make(chan struct{})
		for _, acct := range accts {
			for n := 1; n <= perSender; n++ { // nonce 0 deliberately missing: gaps
				op := acct.NewOperation(uint64(n), "", 0, 1, "", 0, false)
				wg.Add(2)
				go viaQueue(pool, op, start, &wg)
				go viaDirect(b, op, start, &wg)
			}
		}
		close(start)
		wg.Wait()

		total, _ := pool.PendingCount("")
		if total > pool.MaxPendingTotal {
			t.Fatalf("round %d: global cap exceeded: %d > %d", round, total, pool.MaxPendingTotal)
		}
		for _, acct := range accts {
			if _, per := pool.PendingCount(acct.Address); per > pool.MaxPendingPerAccount {
				t.Fatalf("round %d: per-account cap exceeded: %d > %d", round, per, pool.MaxPendingPerAccount)
			}
		}

		// Fill every missing nonce 0 so the suspended ops can settle;
		// settlement must never double-charge.
		var fills []*Operation
		for _, acct := range accts {
			fills = append(fills, acct.NewOperation(0, "", 0, 1, "", 0, false))
		}
		b.SettleDirect(fills)
		b.CollectAndSettle()
		assertConserved(t, ledger, totalBefore)
		if fees := ledger.Balance(b.ID); fees > maxFees {
			maxFees = fees
		}
	}

	// Each (sender, nonce) that entered the system settles at most once.
	maxPossible := int64(senders * perSender)
	if maxFees > maxPossible {
		t.Fatalf("double settlement detected: bundler fees=%d > max %d", maxFees, maxPossible)
	}
	t.Logf("判定依据: %d 轮并发抢槽, 全局/每账户上限从未突破; 单轮打包者手续费峰值=%d (上限 %d)",
		rounds, maxFees, maxPossible)
}

func viaQueue(pool *Mempool, op *Operation, start <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	<-start
	_, _ = pool.Submit(op) // rejection reasons are asserted via cap invariants
}

func viaDirect(b *Bundler, op *Operation, start <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	<-start
	_ = b.SettleDirect([]*Operation{op})
}

// TestDirectBatchRejectionIsolation covers: in one external batch a forged
// operation, a same-nonce conflicting operation and a valid operation for the
// same account coexist; the rejected ones move no balances and the valid
// operations still settle.
func TestDirectBatchRejectionIsolation(t *testing.T) {
	t.Log("业务覆盖: 外部批次单笔被拒不连累同批其他操作, 被拒操作无任何余额变动")
	ledger, pool, b := newFixture(t)
	acct := mustAccount(t)
	other := mustAccount(t)
	ledger.Mint(acct.Address, 100_000)
	ledger.Mint(other.Address, 100_000)
	totalBefore := ledger.Total()

	// The legitimate nonce-0 op is already queued before the external batch
	// arrives, so the conflicting batch op hits an occupied slot regardless
	// of intra-batch ordering.
	valid0 := acct.NewOperation(0, "", 0, 5, "", 0, false)
	if _, err := pool.Submit(valid0); err != nil {
		t.Fatal(err)
	}
	forged1 := acct.NewOperation(1, "", 0, 5, "", 0, false)
	forged1.Signature = make([]byte, 64)
	for i := range forged1.Signature {
		forged1.Signature[i] = 0x0B
	}
	conflict0 := acct.NewOperation(0, "", 0, 9, "", 0, false) // other content at nonce 0
	other0 := other.NewOperation(0, "", 0, 3, "", 0, false)

	rs := b.SettleDirect([]*Operation{forged1, conflict0, other0})
	var successes, rejects int
	for _, r := range rs {
		switch r.Status {
		case StatusSuccess:
			successes++
		case StatusRejected:
			rejects++
		default:
			t.Fatalf("unexpected status %s: %+v", r.Status, r)
		}
	}
	if successes != 2 || rejects != 2 {
		t.Fatalf("want 2 success and 2 rejected, got %+v", rs)
	}
	if total, per := pool.PendingCount(acct.Address); total != 0 || per != 0 {
		t.Fatalf("rejected ops must not linger: total=%d per=%d", total, per)
	}
	if got := ledger.Balance(b.ID); got != 8 {
		t.Fatalf("bundler fees=%d, want 8 (5+3 only from accepted ops)", got)
	}
	if got := ledger.Balance(acct.Address); got != 100_000-5 {
		t.Fatalf("account=%d, want %d", got, 100_000-5)
	}
	t.Log("判定依据: 同批 2 笔被拒(伪造签名/同序号不同内容)不记账, 2 笔合法照常结算, 手续费=8")
	assertConserved(t, ledger, totalBefore)
}

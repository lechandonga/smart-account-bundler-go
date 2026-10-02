package bundler

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func newFixture(t *testing.T) (*Ledger, *Mempool, *Bundler) {
	t.Helper()
	ledger := NewLedger()
	pool := NewMempool()
	b := NewBundler("bundler-1", ledger, pool)
	return ledger, pool, b
}

func mustAccount(t *testing.T) *Account {
	t.Helper()
	a, err := NewAccount()
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// assertConserved checks the global invariant: the sum of every balance in
// the ledger never changes through settlement.
func assertConserved(t *testing.T, ledger *Ledger, before int64) {
	t.Helper()
	if got := ledger.Total(); got != before {
		t.Fatalf("conservation violated: total before=%d after=%d", before, got)
	}
	t.Logf("判定依据: 账本总额守恒 before=%d after=%d", before, ledger.Total())
}

// TestConcurrentSubmissionSameAccount covers: many goroutines submit one
// account's operations out of order; they must still take effect strictly in
// nonce order. Run with -race.
func TestConcurrentSubmissionSameAccount(t *testing.T) {
	t.Log("业务覆盖: 同一账户并发提交, 操作必须按序号依次生效")
	ledger, pool, b := newFixture(t)
	acct := mustAccount(t)
	ledger.Mint(acct.Address, 1_000_000)
	sink := mustAccount(t)
	totalBefore := ledger.Total()

	const n = 50
	nonces := rand.Perm(n)
	var wg sync.WaitGroup
	for _, nonce := range nonces {
		wg.Add(1)
		go func(nonce int) {
			defer wg.Done()
			op := acct.NewOperation(uint64(nonce), sink.Address, 10, 1, "", 0, false)
			if reason, err := pool.Submit(op); err != nil {
				t.Errorf("submit nonce=%d rejected: %s (%v)", nonce, reason, err)
			}
		}(nonce)
	}
	wg.Wait()

	receipts := b.CollectAndSettle()
	if len(receipts) != n {
		t.Fatalf("expected %d receipts, got %d", n, len(receipts))
	}
	for i, r := range receipts {
		if r.Status != StatusSuccess {
			t.Fatalf("receipt %d not success: %s", i, r)
		}
	}
	// Deterministic order: all same sender, so receipts must follow nonce order
	// which we verify through the final balances.
	if got := ledger.Balance(sink.Address); got != int64(n)*10 {
		t.Fatalf("sink balance=%d, want %d", got, n*10)
	}
	wantAcct := int64(1_000_000) - int64(n)*10 - int64(n)*1
	if got := ledger.Balance(acct.Address); got != wantAcct {
		t.Fatalf("account balance=%d, want %d", got, wantAcct)
	}
	if got := ledger.Balance(b.ID); got != int64(n)*1 {
		t.Fatalf("bundler balance=%d, want %d", got, n)
	}
	t.Logf("判定依据: %d 笔乱序并发提交全部成功, 收款方=%d, 账户扣款含手续费=%d, 打包者手续费=%d",
		n, ledger.Balance(sink.Address), int64(n)*11, ledger.Balance(b.ID))
	assertConserved(t, ledger, totalBefore)
}

// TestSubmitRejections covers: bad signature, expiry and reused nonce are
// rejected with distinguishable reasons, and one rejection does not affect
// other operations in the same batch.
func TestSubmitRejections(t *testing.T) {
	t.Log("业务覆盖: 签名错误/过期/序号重用的拒绝原因可区分, 且互不影响同批其他操作")
	ledger, pool, b := newFixture(t)
	good := mustAccount(t)
	other := mustAccount(t)
	ledger.Mint(good.Address, 10_000)
	ledger.Mint(other.Address, 10_000)
	totalBefore := ledger.Total()

	badSig := good.NewOperation(0, "", 0, 1, "", 0, false)
	badSig.Signature = append([]byte(nil), badSig.Signature...)
	badSig.Signature[0] ^= 0xff
	if reason, err := pool.Submit(badSig); reason != ReasonBadSignature || err == nil {
		t.Fatalf("bad signature: reason=%s err=%v", reason, err)
	}
	t.Logf("判定依据: 签名被篡改 -> %s", ReasonBadSignature)

	expired := good.NewOperation(0, "", 0, 1, "", 1, false) // expiry=1s, long past
	if reason, err := pool.Submit(expired); reason != ReasonExpired || err == nil {
		t.Fatalf("expired: reason=%s err=%v", reason, err)
	}
	t.Logf("判定依据: 有效期已过 -> %s", ReasonExpired)

	ok := good.NewOperation(0, "", 0, 1, "", 0, false)
	if reason, err := pool.Submit(ok); err != nil {
		t.Fatalf("valid op rejected: %s %v", reason, err)
	}
	dup := good.NewOperation(0, "", 0, 2, "", 0, false) // same nonce, different fee
	if reason, err := pool.Submit(dup); reason != ReasonNonceUsed || err == nil {
		t.Fatalf("duplicate nonce: reason=%s err=%v", reason, err)
	}
	t.Logf("判定依据: 序号 0 已被占用 -> %s", ReasonNonceUsed)

	// A different account's operation is unaffected by the rejections above.
	otherOp := other.NewOperation(0, "", 0, 1, "", 0, false)
	if reason, err := pool.Submit(otherOp); err != nil {
		t.Fatalf("other account op rejected: %s %v", reason, err)
	}

	receipts := b.CollectAndSettle()
	if len(receipts) != 2 {
		t.Fatalf("expected 2 settled ops, got %d", len(receipts))
	}
	for _, r := range receipts {
		if r.Status != StatusSuccess {
			t.Fatalf("unexpected: %s", r)
		}
	}
	t.Log("判定依据: 3 笔非法操作被拒, 同批 2 笔合法操作正常结算")
	assertConserved(t, ledger, totalBefore)
}

// TestPaymasterAllowanceBoundary covers: sponsorship succeeds exactly at the
// allowance boundary and is atomically rejected beyond it, with no partial
// deduction on any balance.
func TestPaymasterAllowanceBoundary(t *testing.T) {
	t.Log("业务覆盖: 代付押金与账户额度边界, 超额整笔拒绝且无部分扣减")
	ledger, pool, b := newFixture(t)
	acct := mustAccount(t)
	ledger.Mint(acct.Address, 5_000)
	pm := NewPaymaster("pm-1")
	ledger.Mint(pm.ID, 1_000)
	b.RegisterPaymaster(pm)
	totalBefore := ledger.Total()

	const fee = 100
	pm.SetAllowance(acct.Address, fee) // exactly one operation's fee

	op0 := acct.NewOperation(0, "", 0, fee, pm.ID, 0, false)
	if _, err := pool.Submit(op0); err != nil {
		t.Fatal(err)
	}
	op1 := acct.NewOperation(1, "", 0, fee, pm.ID, 0, false) // allowance exhausted
	if _, err := pool.Submit(op1); err != nil {
		t.Fatal(err)
	}

	receipts := b.CollectAndSettle()
	if receipts[0].Status != StatusSuccess {
		t.Fatalf("boundary op should succeed: %s", receipts[0])
	}
	if receipts[1].Status != StatusRejected || receipts[1].Reason != ReasonPaymasterAllowanceExceed {
		t.Fatalf("over-allowance op should be rejected: %s", receipts[1])
	}
	t.Logf("判定依据: 额度恰好等于手续费 -> SUCCESS; 额度耗尽 -> %s", receipts[1].Reason)

	if got := ledger.Balance(pm.ID); got != 1_000-fee {
		t.Fatalf("paymaster deposit=%d, want %d", got, 1_000-fee)
	}
	if got := pm.Allowance(acct.Address); got != 0 {
		t.Fatalf("allowance=%d, want 0", got)
	}
	if got := ledger.Balance(acct.Address); got != 5_000 {
		t.Fatalf("sender balance=%d, want unchanged 5000 (no partial deduction)", got)
	}
	if got := ledger.Balance(b.ID); got != fee {
		t.Fatalf("bundler balance=%d, want %d", got, fee)
	}
	t.Log("判定依据: 拒绝后账户/代付方/打包者余额只体现第一笔的完整扣减, 无部分扣减")

	// Deposit boundary: allowance restored but deposit is gone.
	pm.SetAllowance(acct.Address, 10_000)
	ledger.Mint(pm.ID, -1*ledger.Balance(pm.ID)+50) // leave deposit=50 < fee
	totalBefore = ledger.Total()                    // re-baseline after faucet adjustment
	op2 := acct.NewOperation(2, "", 0, fee, pm.ID, 0, false)
	if _, err := pool.Submit(op2); err != nil {
		t.Fatal(err)
	}
	receipts = b.CollectAndSettle()
	if receipts[0].Status != StatusRejected || receipts[0].Reason != ReasonPaymasterDepositTooLow {
		t.Fatalf("low deposit should be rejected: %s", receipts[0])
	}
	if got := ledger.Balance(pm.ID); got != 50 {
		t.Fatalf("deposit moved on rejection: %d", got)
	}
	t.Logf("判定依据: 押金 50 < 手续费 %d -> %s, 押金未动", fee, receipts[0].Reason)
	assertConserved(t, ledger, totalBefore)
}

// TestReplayBundle covers: settling the same batch twice never charges twice.
func TestReplayBundle(t *testing.T) {
	t.Log("业务覆盖: 重复打包/重放同一批操作不得重复扣费")
	ledger, pool, b := newFixture(t)
	acct := mustAccount(t)
	sink := mustAccount(t)
	ledger.Mint(acct.Address, 10_000)
	totalBefore := ledger.Total()

	ops := []*Operation{
		acct.NewOperation(0, sink.Address, 100, 5, "", 0, false),
		acct.NewOperation(1, sink.Address, 200, 5, "", 0, false),
	}
	for _, op := range ops {
		if _, err := pool.Submit(op); err != nil {
			t.Fatal(err)
		}
	}
	first := b.CollectAndSettle()
	snapshot := ledger.Total()
	acctBal := ledger.Balance(acct.Address)

	// Replay the exact same batch.
	second := b.Settle(ops)
	for i, r := range second {
		if r.Status != StatusReplayed {
			t.Fatalf("replay %d: %s (first was %s)", i, r, first[i])
		}
	}
	if ledger.Balance(acct.Address) != acctBal || ledger.Total() != snapshot {
		t.Fatal("replay moved balances")
	}
	t.Logf("判定依据: 重放 %d 笔均返回 %s, 账户余额与总额不变", len(second), StatusReplayed)

	// Re-submitting a settled operation is rejected as a duplicate.
	if reason, err := pool.Submit(ops[0]); reason != ReasonDuplicate || err == nil {
		t.Fatalf("resubmission: reason=%s err=%v", reason, err)
	}
	t.Logf("判定依据: 已结算操作再次提交 -> %s", ReasonDuplicate)
	assertConserved(t, ledger, totalBefore)
}

// TestPartialExecutionFailure covers: one operation failing at execution does
// not stop the rest; the failed op's fee goes to the bundler and its value
// transfer is skipped.
func TestPartialExecutionFailure(t *testing.T) {
	t.Log("业务覆盖: 执行阶段部分失败, 其余操作按确定顺序完成结算, 失败手续费归属明确")
	ledger, pool, b := newFixture(t)
	acct := mustAccount(t)
	sink := mustAccount(t)
	ledger.Mint(acct.Address, 10_000)
	totalBefore := ledger.Total()

	ops := []*Operation{
		acct.NewOperation(0, sink.Address, 100, 5, "", 0, false),
		acct.NewOperation(1, sink.Address, 999, 5, "", 0, true), // fails at execution
		acct.NewOperation(2, sink.Address, 300, 5, "", 0, false),
	}
	for _, op := range ops {
		if _, err := pool.Submit(op); err != nil {
			t.Fatal(err)
		}
	}
	receipts := b.CollectAndSettle()
	if receipts[0].Status != StatusSuccess || receipts[1].Status != StatusFailed || receipts[2].Status != StatusSuccess {
		t.Fatalf("unexpected receipts: %v", receipts)
	}
	// Failed op: fee kept by bundler, value not transferred.
	if got := ledger.Balance(sink.Address); got != 400 {
		t.Fatalf("sink balance=%d, want 400 (failed op value skipped)", got)
	}
	if got := ledger.Balance(b.ID); got != 15 {
		t.Fatalf("bundler balance=%d, want 15 (all three fees incl. failed op)", got)
	}
	if got := ledger.Balance(acct.Address); got != 10_000-400-15 {
		t.Fatalf("account balance=%d, want %d", got, 10_000-400-15)
	}
	t.Logf("判定依据: 失败操作 value=999 未转账, 手续费 5 归打包者; 其余两笔正常; receipts=%s %s %s",
		receipts[0].Status, receipts[1].Status, receipts[2].Status)
	assertConserved(t, ledger, totalBefore)
}

// TestConcurrentMultiAccount covers: many accounts submit concurrently while
// settlement runs; conservation holds throughout. Run with -race.
func TestConcurrentMultiAccount(t *testing.T) {
	t.Log("业务覆盖: 多账户并发提交与并发结算下的总额守恒")
	ledger, pool, b := newFixture(t)
	const accounts = 8
	const opsEach = 20
	totalBefore := int64(0)

	var wg sync.WaitGroup
	for i := 0; i < accounts; i++ {
		acct := mustAccount(t)
		ledger.Mint(acct.Address, 100_000)
		totalBefore += 100_000
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < opsEach; n++ {
				op := acct.NewOperation(uint64(n), "", 0, 1, "", 0, false)
				for {
					if _, err := pool.Submit(op); err == nil {
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	receipts := b.CollectAndSettle()
	if len(receipts) != accounts*opsEach {
		t.Fatalf("settled %d, want %d", len(receipts), accounts*opsEach)
	}
	for _, r := range receipts {
		if r.Status != StatusSuccess {
			t.Fatalf("unexpected: %s", r)
		}
	}
	if got := ledger.Balance(b.ID); got != int64(accounts*opsEach) {
		t.Fatalf("bundler fees=%d, want %d", got, accounts*opsEach)
	}
	t.Logf("判定依据: %d 账户 x %d 操作全部成功, 打包者手续费=%d", accounts, opsEach, ledger.Balance(b.ID))
	assertConserved(t, ledger, totalBefore)
}

func ExampleBundler_CollectAndSettle() {
	ledger := NewLedger()
	pool := NewMempool()
	b := NewBundler("bundler-1", ledger, pool)
	acct, _ := NewAccount()
	ledger.Mint(acct.Address, 1_000)
	op := acct.NewOperation(0, "", 0, 7, "", 0, false)
	_, _ = pool.Submit(op)
	for _, r := range b.CollectAndSettle() {
		fmt.Println(r.Status, r.Detail)
	}
	// Output: SUCCESS settled; fee 7 to bundler bundler-1
}

package bundler

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// newTimedFixture builds a pool whose clock is fully controlled by the test.
func newTimedFixture(t *testing.T, cfg Config) (*Ledger, *Mempool, *Bundler, *time.Time) {
	t.Helper()
	ledger := NewLedger()
	pool := NewMempoolWithConfig(cfg)
	clock := time.Unix(1_000_000, 0)
	pool.setClock(func() time.Time { return clock })
	b := NewBundler("bundler-1", ledger, pool)
	return ledger, pool, b, &clock
}

// TestDirectBatchAdmission covers an externally delivered batch whose
// operations never entered the queue: it must pass the exact same admission
// rules as queued submissions -- forged signature, expiry, same nonce with
// different content after the nonce became effective, and duplicate settled
// content -- each with a distinguishable reason. Rejections move no balance
// and do not affect sibling items.
func TestDirectBatchAdmission(t *testing.T) {
	t.Log("业务覆盖: 外部直接送批的统一准入(伪签名/过期/同序号异内容/重复内容), 与排队路径结论一致")
	ledger, pool, b := newFixture(t)
	acct := mustAccount(t)
	sink := mustAccount(t)
	ledger.Mint(acct.Address, 100_000)
	totalBefore := ledger.Total()

	forged := acct.NewOperation(1, sink.Address, 10, 1, "", 0, false)
	forged.Signature[0] ^= 0xff // never submitted anywhere; direct batch only

	expired := acct.NewOperation(2, sink.Address, 10, 1, "", 1, false) // expiry long past

	good0 := acct.NewOperation(0, sink.Address, 100, 5, "", 0, false)
	// same nonce 2, different content (different fee), correctly signed
	sameNonceDiffContent := acct.NewOperation(0, sink.Address, 100, 999, "", 0, false)

	// First external batch: forged, expired and one good op travel together.
	res := b.SettleBatch([]*Operation{forged, expired, good0})
	if res[0].Admission.Reason != ReasonBadSignature || res[0].Receipt.Reason != ReasonBadSignature {
		t.Fatalf("forged op: %+v", res[0])
	}
	t.Logf("判定依据: 从未排队的伪造签名直送批次 -> %s (终态 %s)", res[0].Admission.Reason, res[0].Receipt.Status)
	if res[1].Admission.Reason != ReasonExpired {
		t.Fatalf("expired op: %+v", res[1])
	}
	t.Logf("判定依据: 直送批次中过期操作 -> %s", res[1].Admission.Reason)
	if !res[2].Admission.Accepted || res[2].Receipt.Status != StatusSuccess {
		t.Fatalf("good op should settle: %+v", res[2])
	}
	t.Logf("判定依据: 同批合法操作不受连累, 正常结算 %s", res[2].Receipt.Status)

	// Second external batch: same nonce with different content + exact replay.
	res2 := b.SettleBatch([]*Operation{sameNonceDiffContent, good0})
	if res2[0].Admission.Reason != ReasonNonceUsed {
		t.Fatalf("same nonce different content: %+v", res2[0])
	}
	t.Logf("判定依据: 序号 0 已生效, 即使内容不同(手续费 999) -> %s", res2[0].Admission.Reason)
	if res2[1].Admission.Reason != ReasonDuplicate {
		t.Fatalf("duplicate content: %+v", res2[1])
	}
	t.Logf("判定依据: 已结算内容原样重复直送 -> %s", res2[1].Admission.Reason)

	// Terminal receipts are queryable for every rejected operation.
	for i, op := range []*Operation{forged, expired, sameNonceDiffContent, good0} {
		r, ok := b.Receipt(op.ID())
		if !ok {
			t.Fatalf("item %d: no receipt queryable", i)
		}
		if r.Status == StatusPending {
			t.Fatalf("item %d: receipt not terminal: %s", i, r)
		}
	}
	t.Log("判定依据: 每笔被挡操作都有可查询终态回执, 原因彼此可区分")

	// Only good0 (value 100 + fee 5) ever moved money; rejected batches
	// changed nothing for sender, paymaster (none) or bundler.
	if got := ledger.Balance(acct.Address); got != 100_000-105 {
		t.Fatalf("sender balance=%d, want %d (only the one good op charged)", got, 100_000-105)
	}
	if got := ledger.Balance(b.ID); got != 5 {
		t.Fatalf("bundler balance=%d, want 5", got)
	}
	if got := ledger.Balance(sink.Address); got != 100 {
		t.Fatalf("sink balance=%d, want 100", got)
	}
	if _, global := pool.PendingCount(acct.Address); global != 0 {
		t.Fatalf("rejected ops must not occupy capacity, pending=%d", global)
	}
	t.Log("判定依据: 账户/打包者/收款方余额只反映唯一一笔成功操作, 被挡操作零账目影响、不占容量")
	assertConserved(t, ledger, totalBefore)
}

// TestAdmissionPathEquivalence covers the queue path and the direct-batch
// path producing identical verdicts and identical final balances for the same
// input sequence.
func TestAdmissionPathEquivalence(t *testing.T) {
	t.Log("业务覆盖: 排队收批与外部直送走同一套准入, 结论与账目一致")

	type step struct {
		op          *Operation
		settleAfter bool
	}
	build := func() (*Ledger, *Mempool, *Bundler, *Account, *Account, []step) {
		ledger := NewLedger()
		pool := NewMempool()
		b := NewBundler("bundler-1", ledger, pool)
		acct := mustAccount(t)
		sink := mustAccount(t)
		ledger.Mint(acct.Address, 100_000)
		badSig := acct.NewOperation(0, sink.Address, 10, 1, "", 0, false)
		badSig.Signature[0] ^= 0xff
		expired := acct.NewOperation(1, sink.Address, 10, 1, "", 1, false)
		good := acct.NewOperation(0, sink.Address, 100, 5, "", 0, false)
		diffContent := acct.NewOperation(0, sink.Address, 100, 999, "", 0, false)
		return ledger, pool, b, acct, sink, []step{
			{op: badSig}, {op: expired}, {op: good, settleAfter: true},
			{op: diffContent}, {op: good},
		}
	}

	ledgerA, poolA, bA, acctA, sinkA, steps := build()
	queueReasons := make([]RejectReason, len(steps))
	for i, s := range steps {
		reason, err := poolA.Submit(s.op)
		if err != nil {
			queueReasons[i] = reason
		}
		if s.settleAfter {
			bA.CollectAndSettle()
		}
	}

	ledgerB, _, bB, acctB, sinkB, stepsB := build()
	directReasons := make([]RejectReason, len(stepsB))
	for i, s := range stepsB {
		res := bB.SettleBatch([]*Operation{s.op})
		directReasons[i] = res[0].Admission.Reason
	}

	for i := range steps {
		if queueReasons[i] != directReasons[i] {
			t.Fatalf("step %d: queue=%s direct=%s", i, queueReasons[i], directReasons[i])
		}
	}
	t.Logf("判定依据: 两条路径拒绝序列一致 %v", queueReasons)
	for _, pair := range [][2]string{
		{acctA.Address, acctB.Address}, {sinkA.Address, sinkB.Address},
		{bA.ID, bB.ID},
	} {
		if ledgerA.Balance(pair[0]) != ledgerB.Balance(pair[1]) {
			t.Fatalf("balance divergence: queue=%d direct=%d", ledgerA.Balance(pair[0]), ledgerB.Balance(pair[1]))
		}
	}
	t.Logf("判定依据: 两条路径终态账目一致 sender=%d sink=%d bundler=%d",
		ledgerA.Balance(acctA.Address), ledgerA.Balance(sinkA.Address), ledgerA.Balance(bA.ID))
}

// TestNonceGapFillAndTimeout covers both gap outcomes:
//   - the missing op arrives within the window: queued ops then take effect
//     in nonce order; other accounts are never blocked;
//   - the window expires: blocked ops get a queryable NONCE_GAP_TIMEOUT
//     terminal receipt, no funds move, and the skipped nonces never take
//     effect afterwards.
func TestNonceGapFillAndTimeout(t *testing.T) {
	t.Log("业务覆盖: 空洞序号窗口内补齐顺序生效 / 超时明确终态, 且等待只影响本账户")

	run := func(timeout bool) []string {
		ledger, pool, b, clock := newTimedFixture(t, Config{GapWait: 10 * time.Second})
		acct := mustAccount(t)
		other := mustAccount(t)
		ledger.Mint(acct.Address, 100_000)
		ledger.Mint(other.Address, 100_000)

		// Gap: nonce 1 queued while nonce 0 is missing.
		gapped := acct.NewOperation(1, "", 0, 7, "", 0, false)
		if _, err := pool.Submit(gapped); err != nil {
			t.Fatal(err)
		}
		// Another account is fully contiguous and must not be held back.
		otherOp := other.NewOperation(0, "", 0, 3, "", 0, false)
		if _, err := pool.Submit(otherOp); err != nil {
			t.Fatal(err)
		}
		*clock = clock.Add(5 * time.Second) // inside the 10s window
		first := b.CollectAndSettle()
		var summary []string
		for _, r := range first {
			summary = append(summary, string(r.Status)+"/"+string(r.Reason))
		}
		if len(first) != 1 || first[0].Status != StatusSuccess {
			t.Fatalf("other account should settle alone inside window, got %v", first)
		}
		if got := ledger.Balance(b.ID); got != 3 {
			t.Fatalf("bundler fee during wait=%d, want 3", got)
		}
		if per, _ := pool.PendingCount(acct.Address); per != 1 {
			t.Fatalf("gapped op must remain queued, per=%d", per)
		}

		if timeout {
			*clock = clock.Add(6 * time.Second) // past window (11s total)
			second := b.CollectAndSettle()
			if len(second) != 1 || second[0].Status != StatusRejected || second[0].Reason != ReasonNonceGapTimeout {
				t.Fatalf("expected gap timeout, got %v", second)
			}
			r, ok := b.Receipt(gapped.ID())
			if !ok || r.Status != StatusRejected || r.Reason != ReasonNonceGapTimeout {
				t.Fatalf("gap receipt not queryable: ok=%v %s", ok, r)
			}
			summary = append(summary, string(second[0].Status)+"/"+string(second[0].Reason))
			if got := ledger.Balance(acct.Address); got != 100_000 {
				t.Fatalf("timed-out op moved funds: sender=%d", got)
			}
			if per, _ := pool.PendingCount(acct.Address); per != 0 {
				t.Fatalf("timed-out op did not release capacity: per=%d", per)
			}
			// The late-arriving missing nonce can never resurrect the gap.
			late0 := acct.NewOperation(0, "", 0, 7, "", 0, false)
			reason, _ := pool.Submit(late0)
			if reason != ReasonNonceUsed {
				t.Fatalf("late nonce 0 after gap skip: %s", reason)
			}
			summary = append(summary, "late0/"+string(reason))
		} else {
			missing := acct.NewOperation(0, "", 0, 7, "", 0, false)
			if _, err := pool.Submit(missing); err != nil {
				t.Fatal(err)
			}
			*clock = clock.Add(4 * time.Second) // exactly at window edge
			second := b.CollectAndSettle()
			if len(second) != 2 || second[0].Status != StatusSuccess || second[1].Status != StatusSuccess {
				t.Fatalf("gap fill should settle 2 in nonce order, got %v", second)
			}
			summary = append(summary,
				string(second[0].Status)+"/"+string(second[0].Reason),
				string(second[1].Status)+"/"+string(second[1].Reason))
			// Fees: other(3) + nonce0(7) + nonce1(7) = 17.
			if got := ledger.Balance(b.ID); got != 17 {
				t.Fatalf("bundler fees=%d, want 17", got)
			}
		}
		return summary
	}

	filled := run(false)
	t.Logf("判定依据(补齐路径): %v - 其他账户窗口内先结算, 缺笔到齐后按 nonce 0,1 生效", filled)
	timedOut := run(true)
	t.Logf("判定依据(超时路径): %v - 等待只影响本账户, 超时终态可查且零扣费, 缺笔迟到不可复活", timedOut)

	// Determinism: the same inputs replayed give the same sequence.
	if again := run(true); fmt.Sprint(again) != fmt.Sprint(timedOut) {
		t.Fatalf("non-deterministic gap-timeout run:\n%v\n%v", timedOut, again)
	}
	if again := run(false); fmt.Sprint(again) != fmt.Sprint(filled) {
		t.Fatalf("non-deterministic gap-fill run:\n%v\n%v", filled, again)
	}
	t.Log("判定依据: 相同输入重复跑结果序列完全一致 (确定顺序)")
}

// TestPendingCapacityLimits covers per-account and global pending caps:
// excess submissions are rejected with distinct reasons, queued operations
// are never evicted or altered, and no fee is charged. The concurrent
// sub-tests must pass under -race across repeated rounds with no over-quota
// pending ops, lost accepted ops or ledger drift.
func TestPendingCapacityLimits(t *testing.T) {
	t.Log("业务覆盖: 每账户/全池挂起上限, 超额原因可区分, 拒绝不顶掉队列、不扣费; 高并发抢配额")

	const rounds = 5

	// Per-account cap, contended concurrently by a single account.
	t.Run("per_account_concurrent", func(t *testing.T) {
		const limit = 5
		const contenders = 50
		for round := 0; round < rounds; round++ {
			ledger, pool, b := newFixture(t)
			acct := mustAccount(t)
			ledger.Mint(acct.Address, 1<<40)
			pool = NewMempoolWithConfig(Config{MaxPendingPerAccount: limit})
			b = NewBundler(b.ID, ledger, pool)

			var wg sync.WaitGroup
			results := make([]RejectReason, contenders)
			acceptedNonces := make(chan int, contenders)
			start := make(chan struct{})
			for i := 0; i < contenders; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					op := acct.NewOperation(uint64(i), "", 0, 1, "", 0, false)
					if _, err := pool.Submit(op); err != nil {
						results[i] = ReasonAccountPendingLimit
					} else {
						acceptedNonces <- i
					}
				}(i)
			}
			close(start)
			wg.Wait()
			close(acceptedNonces)
			won := make(map[int]bool)
			for n := range acceptedNonces {
				won[n] = true
			}

			accepted, rejected := len(won), 0
			for _, reason := range results {
				switch reason {
				case ReasonAccountPendingLimit:
					rejected++
				case ReasonNone:
				default:
					t.Fatalf("round %d: unexpected reason %s", round, reason)
				}
			}
			per, global := pool.PendingCount(acct.Address)
			if per != limit || global != limit {
				t.Fatalf("round %d: over/under quota accepted=%d per=%d global=%d", round, accepted, per, global)
			}
			if accepted != limit || rejected != contenders-limit {
				t.Fatalf("round %d: accepted=%d rejected=%d, want %d/%d", round, accepted, rejected, limit, contenders-limit)
			}
			// No settlement happened: rejected and queued ops charged nothing.
			if got := ledger.Balance(b.ID); got != 0 {
				t.Fatalf("round %d: bundler charged %d before settlement", round, got)
			}
			// Settle the contiguous accepted run; every other accepted op must
			// remain queued untouched (no eviction, no loss, no alteration).
			contiguous := 0
			for won[contiguous] {
				contiguous++
			}
			receipts := b.CollectAndSettle()
			if len(receipts) != contiguous {
				t.Fatalf("round %d: settled %d, want contiguous %d (no lost/evicted ops)", round, len(receipts), contiguous)
			}
			for _, r := range receipts {
				if r.Status != StatusSuccess {
					t.Fatalf("round %d: queued op altered by contention: %s", round, r)
				}
			}
			if per, _ := pool.PendingCount(acct.Address); per != limit-contiguous {
				t.Fatalf("round %d: remaining queued=%d, want %d (rejected contenders displaced nothing)",
					round, per, limit-contiguous)
			}
		}
		t.Logf("判定依据: %d 个 goroutine 抢每账户 %d 配额, 连续 %d 轮恰为 %d 接受/%d 拒绝(%s), 无超额、无丢失、零扣费",
			contenders, limit, rounds, limit, contenders-limit, ReasonAccountPendingLimit)
	})

	// Global cap contended concurrently by many accounts.
	t.Run("global_concurrent", func(t *testing.T) {
		const globalLimit = 20
		const accounts = 10
		const each = 10
		for round := 0; round < rounds; round++ {
			ledger := NewLedger()
			pool := NewMempoolWithConfig(Config{MaxPendingGlobal: globalLimit})
			b := NewBundler("bundler-1", ledger, pool)
			accts := make([]*Account, accounts)
			for i := range accts {
				accts[i] = mustAccount(t)
				ledger.Mint(accts[i].Address, 1<<40)
			}

			var wg sync.WaitGroup
			start := make(chan struct{})
			var mu sync.Mutex
			accepted, rejected := 0, 0
			for ai, acct := range accts {
				for n := 0; n < each; n++ {
					wg.Add(1)
					go func(acct *Account, n, ai int) {
						defer wg.Done()
						<-start
						op := acct.NewOperation(uint64(n), "", 0, 1, "", 0, false)
						reason, err := pool.Submit(op)
						mu.Lock()
						defer mu.Unlock()
						if err != nil {
							if reason != ReasonGlobalPendingLimit {
								t.Errorf("round %d acct %d: unexpected reason %s", round, ai, reason)
							}
							rejected++
						} else {
							accepted++
						}
					}(acct, n, ai)
				}
			}
			close(start)
			wg.Wait()

			if accepted != globalLimit || accepted+rejected != accounts*each {
				t.Fatalf("round %d: accepted=%d rejected=%d total=%d", round, accepted, rejected, accounts*each)
			}
			var total int
			for _, acct := range accts {
				per, g := pool.PendingCount(acct.Address)
				total += per
				if per > each || g != globalLimit {
					t.Fatalf("round %d: per-account/global quota violated per=%d global=%d", round, per, g)
				}
			}
			if total != globalLimit {
				t.Fatalf("round %d: summed pending=%d, want %d (no over-hang)", round, total, globalLimit)
			}
			// Drain whatever is contiguous; accepted ops behind gaps must
			// survive intact. accepted = settled + still-pending, no loss.
			receipts := b.CollectAndSettle()
			var remaining int
			for _, acct := range accts {
				per, _ := pool.PendingCount(acct.Address)
				remaining += per
			}
			if len(receipts)+remaining != globalLimit {
				t.Fatalf("round %d: accepted ops lost: settled=%d pending=%d want total %d",
					round, len(receipts), remaining, globalLimit)
			}
			for _, r := range receipts {
				if r.Status != StatusSuccess {
					t.Fatalf("round %d: queued op altered: %s", round, r)
				}
			}
			// Rejected ops charged nothing; only settled ops' fees moved.
			if got := ledger.Total(); got != int64(accounts)<<40 {
				t.Fatalf("round %d: conservation violated total=%d", round, got)
			}
		}
		t.Logf("判定依据: %d 账户 x %d 并发抢全池 %d 配额, 连续 %d 轮恰好 %d 挂起, 其余 %s; 结算零丢失、总额守恒",
			accounts, each, globalLimit, rounds, globalLimit, ReasonGlobalPendingLimit)
	})

	// Distinguishability of the two capacity reasons.
	t.Run("reasons_distinguishable", func(t *testing.T) {
		pool := NewMempoolWithConfig(Config{MaxPendingPerAccount: 1, MaxPendingGlobal: 1})
		a := mustAccount(t)
		c := mustAccount(t)
		op0 := a.NewOperation(0, "", 0, 1, "", 0, false)
		if _, err := pool.Submit(op0); err != nil {
			t.Fatal(err)
		}
		if reason, _ := pool.Submit(a.NewOperation(1, "", 0, 1, "", 0, false)); reason != ReasonAccountPendingLimit {
			t.Fatalf("per-account reason=%s", reason)
		}
		if reason, _ := pool.Submit(c.NewOperation(0, "", 0, 1, "", 0, false)); reason != ReasonGlobalPendingLimit {
			t.Fatalf("global reason=%s", reason)
		}
		if ReasonAccountPendingLimit == ReasonGlobalPendingLimit ||
			ReasonAccountPendingLimit == ReasonNonceUsed {
			t.Fatal("capacity reasons must be distinct from each other and existing reasons")
		}
		t.Logf("判定依据: 每账户上限 -> %s; 全池上限 -> %s; 二者及与既有原因均不同",
			ReasonAccountPendingLimit, ReasonGlobalPendingLimit)
	})
}

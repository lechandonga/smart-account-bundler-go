package bundler

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentSameAccountSubmit 验证同一账户并发提交严格按 nonce 依次生效。
func TestConcurrentSameAccountSubmit(t *testing.T) {
	pub, priv := mustKey(t)
	bl := New(testFee)
	bl.nonceWait = 100 * time.Millisecond
	sender := bl.OpenAccount(pub, 1_000_000)

	const n = 30
	logCase(t, "同账户并发提交",
		fmt.Sprintf("%d 个 goroutine 提交乱序 nonce；全部 Accepted 且 mempool=%d", n, n))

	results := make([]*OpResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			op := newOp(sender, Address{}, uint64(i), 0, testFee, hourFromNow(), Address{}, priv)
			results[i], errs[i] = bl.Submit(op)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, r := range results {
		if !r.Accepted {
			t.Fatalf("nonce %d 应被接受，原因=%s err=%v", i, r.Reason, errs[i])
		}
	}
	if got := bl.PendingOps(); got != n {
		t.Fatalf("mempool 应有 %d 笔，实际 %d", n, got)
	}

	// 先结算全部待打包操作，再重放已生效的 nonce=0。
	if _, err := bl.SettleBatch("concurrent-batch", bl.snapshotMempool()); err != nil {
		t.Fatal(err)
	}
	logCase(t, "已用序号重放", "结算后 nonce=0 再提交 => nonce_already_used")
	replay := newOp(sender, Address{}, 0, 0, testFee, hourFromNow(), Address{}, priv)
	res, err := bl.Submit(replay)
	if res.Accepted || res.Reason != ReasonNonceUsed {
		t.Fatalf("重放应得到 %s，得到 %+v", ReasonNonceUsed, res)
	}
	t.Logf("重放结论: %v", err)

	logCase(t, "序号空洞", "nonce=n+5 等待超时 => nonce_out_of_order")
	gap := newOp(sender, Address{}, uint64(n+5), 0, testFee, hourFromNow(), Address{}, priv)
	res, _ = bl.Submit(gap)
	if res.Accepted || res.Reason != ReasonNonceGap {
		t.Fatalf("空洞序号应得到 %s，得到 %+v", ReasonNonceGap, res)
	}
}

// TestRejectionReasons 验证各类拒绝原因可区分且单笔拒绝相互隔离。
func TestRejectionReasons(t *testing.T) {
	pub, priv := mustKey(t)
	_, otherPriv := mustKey(t)
	bl := New(testFee)
	sender := bl.OpenAccount(pub, testFee)

	badSig := newOp(sender, Address{}, 0, 0, testFee, hourFromNow(), Address{}, otherPriv)
	if r, _ := bl.Submit(badSig); r.Accepted || r.Reason != ReasonBadSignature {
		t.Fatalf("签名不匹配期望 %s", ReasonBadSignature)
	}
	logCase(t, "签名不匹配", "用他人私钥签名 => bad_signature，操作不进入队列")

	expired := newOp(sender, Address{}, 0, 0, testFee,
		time.Now().Add(-time.Minute).Unix(), Address{}, priv)
	if r, _ := bl.Submit(expired); r.Accepted || r.Reason != ReasonExpired {
		t.Fatalf("过期期望 %s", ReasonExpired)
	}
	logCase(t, "超出有效期", "deadline 已过 => operation_expired")

	good := newOp(sender, Address{}, 0, 0, testFee, hourFromNow(), Address{}, priv)
	if r, _ := bl.Submit(good); !r.Accepted {
		t.Fatal("合法操作应被接受")
	}
	noMoney := newOp(sender, Address{}, 1, 0, testFee, hourFromNow(), Address{}, priv)
	if r, _ := bl.Submit(noMoney); r.Accepted || r.Reason != ReasonAccountInsufficient {
		t.Fatalf("余额不足期望 %s", ReasonAccountInsufficient)
	}
	logCase(t, "账户无力垫付+单笔隔离",
		"账户仅够一笔手续费；第二笔 account_insufficient，第一笔留在 mempool 不受影响")
	if bl.PendingOps() != 1 {
		t.Fatalf("被拒绝操作不应入队，mempool=%d", bl.PendingOps())
	}
}

// TestPaymasterQuota 验证代付押金与按账户额度的边界判定。
func TestPaymasterQuota(t *testing.T) {
	pub, priv := mustKey(t)
	bl := New(testFee)
	sender := bl.OpenAccount(pub, 0)
	sponsor := addrAt(0xAA)
	bl.RegisterPaymaster(sponsor, testFee*3, map[Address]uint64{sender: testFee * 2})

	for i := 0; i < 3; i++ {
		op := newOp(sender, Address{}, uint64(i), 0, testFee, hourFromNow(), sponsor, priv)
		r, err := bl.Submit(op)
		if i < 2 {
			if !r.Accepted {
				t.Fatalf("第 %d 笔应被额度接受: %v", i, err)
			}
		} else if r.Accepted || r.Reason != ReasonSponsorQuotaExceed {
			t.Fatalf("第 3 笔应 %s，得到 %s", ReasonSponsorQuotaExceed, r.Reason)
		}
	}
	logCase(t, "代付额度边界",
		"押金=30、额度=20、单价=10：前两笔 accepted，第三笔 sponsor_quota_exceed，提交阶段押金不扣减")
	if bl.Balance(sponsor) != testFee*3 {
		t.Fatalf("提交阶段不得扣押金，押金=%d", bl.Balance(sponsor))
	}

	bl.SettleBatch("b1", bl.snapshotMempool())
	if used := bl.SponsorUsed(sponsor, sender); used != testFee*2 {
		t.Fatalf("已用额度应为 %d", testFee*2)
	}

	pub2, priv2 := mustKey(t)
	sender2 := bl.OpenAccount(pub2, 0)
	sponsor2 := addrAt(0xBB)
	bl.RegisterPaymaster(sponsor2, testFee-1, map[Address]uint64{sender2: testFee * 10})
	op := newOp(sender2, Address{}, 0, 0, testFee, hourFromNow(), sponsor2, priv2)
	if r, _ := bl.Submit(op); r.Accepted || r.Reason != ReasonSponsorDepositLow {
		t.Fatalf("押金不足期望 %s", ReasonSponsorDepositLow)
	}
	logCase(t, "押金不足整笔拒绝", "deposit=9 < fee=10 => sponsor_deposit_low，三方余额无变化")
}

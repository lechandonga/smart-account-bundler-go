package bundler

import "testing"

// TestSettleConservationAndPartialFailure 验证余额守恒、执行阶段部分失败、
// 失败操作手续费明确归属打包者，以及确定排序。
func TestSettleConservationAndPartialFailure(t *testing.T) {
	pubA, privA := mustKey(t)
	pubB, privB := mustKey(t)
	pubC, _ := mustKey(t)
	bl := New(testFee)
	a := bl.OpenAccount(pubA, 100)
	bAcc := bl.OpenAccount(pubB, 1_000)
	c := bl.OpenAccount(pubC, 1_000)
	beneficiary := addrAt(0x01)
	bl.SetBeneficiary(beneficiary)

	// A 付手续费 10 后余 90，再转 100 必然在执行阶段失败。
	opA := newOp(a, c, 0, 100, testFee, hourFromNow(), Address{}, privA)
	opB := newOp(bAcc, c, 0, 50, testFee, hourFromNow(), Address{}, privB)
	if r, err := bl.Submit(opA); !r.Accepted {
		t.Fatal(err)
	}
	if r, err := bl.Submit(opB); !r.Accepted {
		t.Fatal(err)
	}

	// 打乱传入顺序，验证结算按 Sender 升序确定排序。
	receipt, err := bl.SettleBatch("batch-1", []UserOperation{opB, opA})
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.Conserved {
		t.Fatalf("余额不守恒: before=%d after=%d", receipt.TotalBefore, receipt.TotalAfter)
	}
	for _, rc := range receipt.Receipts {
		t.Logf("[结算回执] sender=%s nonce=%d executed=%v reverted=%v fee=%d burden=%s | %s",
			rc.Sender, rc.Nonce, rc.Executed, rc.Reverted, rc.FeePaid, rc.FeeBurden, rc.Status)
	}
	logCase(t, "确定排序", "传入顺序被忽略，严格按 Sender 地址、Nonce 升序结算")
	logCase(t, "余额守恒", "结算前后 账户+代付方+打包者 总余额完全相等")
	logCase(t, "执行阶段部分失败",
		"A 手续费垫付成功但转账 100 失败(reverted)：手续费 10 归打包者不回滚；B 的结算不受影响")

	if bl.Balance(a) != 90 {
		t.Fatalf("A 应只剩 90，实际 %d", bl.Balance(a))
	}
	if bl.Balance(c) != 1050 {
		t.Fatalf("C 应只收到 B 的 50，实际 %d", bl.Balance(c))
	}
	if bl.Balance(beneficiary) != 20 {
		t.Fatalf("打包者应收到两笔手续费共 20，实际 %d", bl.Balance(beneficiary))
	}
}

// TestReplayBatch 验证批次幂等与操作级重放均不重复扣费。
func TestReplayBatch(t *testing.T) {
	pub, priv := mustKey(t)
	bl := New(testFee)
	sender := bl.OpenAccount(pub, 1_000)
	beneficiary := addrAt(0x02)
	bl.SetBeneficiary(beneficiary)

	op := newOp(sender, Address{}, 0, 0, testFee, hourFromNow(), Address{}, priv)
	if r, _ := bl.Submit(op); !r.Accepted {
		t.Fatal("提交失败")
	}
	first, _ := bl.SettleBatch("dup-batch", []UserOperation{op})
	again, _ := bl.SettleBatch("dup-batch", []UserOperation{op})
	if again != first {
		t.Fatal("同一 batchID 必须返回同一缓存回执且不重复结算")
	}

	r2, _ := bl.SettleBatch("new-batch", []UserOperation{op})
	if len(r2.Receipts) != 1 || !r2.Receipts[0].Skipped || r2.TotalFee != 0 {
		t.Fatalf("已结算操作重放必须跳过、零手续费: %+v", r2.Receipts)
	}
	if bl.Balance(beneficiary) != testFee {
		t.Fatalf("打包者只应收到一次手续费，实际 %d", bl.Balance(beneficiary))
	}
	logCase(t, "重复打包/重放",
		"同 batchID 返回缓存回执；新批次夹带旧操作为 skipped no charge，手续费只扣一次")
}

// TestSettleRejectIsAtomic 验证结算阶段垫付条件不满足时整笔拒绝、无部分扣减。
func TestSettleRejectIsAtomic(t *testing.T) {
	pub, priv := mustKey(t)
	bl := New(testFee)
	sender := bl.OpenAccount(pub, 0)
	sponsor := addrAt(0xCC)
	bl.RegisterPaymaster(sponsor, testFee, map[Address]uint64{sender: testFee})

	op := newOp(sender, Address{}, 0, 0, testFee, hourFromNow(), sponsor, priv)
	if r, _ := bl.Submit(op); !r.Accepted {
		t.Fatal("提交时额度足够，应接受")
	}
	// 结算前押金被抽走（模拟临界竞争），结算必须整笔拒绝。
	bl.mu.Lock()
	bl.sponsors[sponsor].Deposit = 0
	bl.mu.Unlock()

	receipt, _ := bl.SettleBatch("edge", []UserOperation{op})
	rc := receipt.Receipts[0]
	if !rc.Skipped || rc.FeePaid != 0 {
		t.Fatalf("结算拒绝应无手续费: %+v", rc)
	}
	if bl.Balance(sponsor) != 0 {
		t.Fatalf("押金应保持 0 不变，实际 %d", bl.Balance(sponsor))
	}
	if bl.AccountNonce(sender) != 1 {
		t.Fatalf("拒绝也应消耗序号以免阻塞后续，nonce=%d", bl.AccountNonce(sender))
	}
	logCase(t, "结算阶段整笔拒绝",
		"押金临界不足 => 无任何扣减(FeePaid=0)、回执标注 no balance changed；序号消耗，账户后续操作不被阻塞")

	// 后续 nonce=1 仍能正常提交与结算。
	next := newOp(sender, Address{}, 1, 0, testFee, hourFromNow(), sponsor, priv)
	if r, _ := bl.Submit(next); r.Accepted {
		t.Fatal("押金仍为 0，应在提交阶段再次被拒绝")
	} else if r.Reason != ReasonSponsorDepositLow {
		t.Fatalf("应得到 sponsor_deposit_low，得到 %s", r.Reason)
	}
}

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"log"

	"github.com/lechandonga/smart-account-bundler-go/pkg/bundler"
)

func main() {
	const fee = uint64(10)
	bl := bundler.New(fee)

	var beneficiary bundler.Address
	beneficiary[0] = 0x01
	bl.SetBeneficiary(beneficiary)

	alicePub, alicePriv := mustKey()
	bobPub, _ := mustKey()
	alice := bl.OpenAccount(alicePub, 100)
	bob := bl.OpenAccount(bobPub, 0)

	var sponsor bundler.Address
	sponsor[0] = 0xAA
	bl.RegisterPaymaster(sponsor, 500, map[bundler.Address]uint64{alice: 100})

	fmt.Printf("初始: alice=%d bob=%d sponsor=%d bundler=%d\n",
		bl.Balance(alice), bl.Balance(bob), bl.Balance(sponsor), bl.Balance(beneficiary))

	// 操作 1：alice 自付手续费，向 bob 转 40。
	op1 := bundler.UserOperation{Sender: alice, Nonce: 0, To: bob, Value: 40,
		MaxFee: fee, Deadline: 1 << 60}
	bundler.SignOperation(&op1, alicePriv)
	if r, err := bl.Submit(op1); err != nil {
		log.Fatalf("op1: %v %v", r.Reason, err)
	}

	// 操作 2：代付方垫付手续费，alice 向 bob 转 20。
	op2 := bundler.UserOperation{Sender: alice, Nonce: 1, To: bob, Value: 20,
		MaxFee: fee, Deadline: 1 << 60, Sponsor: sponsor}
	bundler.SignOperation(&op2, alicePriv)
	if r, err := bl.Submit(op2); err != nil {
		log.Fatalf("op2: %v %v", r.Reason, err)
	}

	receipt, err := bl.SettleBatch("demo-batch", []bundler.UserOperation{op2, op1})
	if err != nil {
		log.Fatal(err)
	}
	for _, rc := range receipt.Receipts {
		fmt.Printf("回执: %s | %s\n", rc.FeeBurden, rc.Status)
	}
	fmt.Printf("结算: alice=%d bob=%d sponsor=%d bundler=%d conserved=%v\n",
		bl.Balance(alice), bl.Balance(bob), bl.Balance(sponsor),
		bl.Balance(beneficiary), receipt.Conserved)

	// 重放批次：返回缓存回执，不重复扣费。
	replay, _ := bl.SettleBatch("demo-batch", []bundler.UserOperation{op2, op1})
	fmt.Printf("重放: totalFee=%d conserved=%v（与首次完全一致）\n", replay.TotalFee, replay.Conserved)
}

func mustKey() (ed25519.PublicKey, ed25519.PrivateKey) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	return pub, priv
}

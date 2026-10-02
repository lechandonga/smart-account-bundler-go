package bundler

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"
)

const testFee = uint64(10)

func mustKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return pub, priv
}

func newOp(sender, to Address, nonce, value, maxFee uint64, deadline int64, sponsor Address, priv ed25519.PrivateKey) UserOperation {
	op := UserOperation{
		Sender: sender, Nonce: nonce, To: to, Value: value,
		MaxFee: maxFee, Deadline: deadline, Sponsor: sponsor,
	}
	SignOperation(&op, priv)
	return op
}

func logCase(t *testing.T, name, basis string) {
	t.Helper()
	t.Logf("[业务覆盖] 用例=%s | 判定依据=%s", name, basis)
}

func addrAt(b byte) Address {
	var a Address
	a[0] = b
	return a
}

func hourFromNow() int64 { return time.Now().Add(time.Hour).Unix() }

func (b *Bundler) snapshotMempool() []UserOperation {
	b.mu.Lock()
	defer b.mu.Unlock()
	ops := make([]UserOperation, 0, len(b.mempool))
	for _, op := range b.mempool {
		ops = append(ops, op)
	}
	return ops
}

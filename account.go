package bundler

import (
	"crypto/ed25519"
	"encoding/hex"
)

// Account is a locally generated smart account key pair.
type Account struct {
	Address string
	Pub     ed25519.PublicKey
	Priv    ed25519.PrivateKey
}

// NewAccount generates a fresh ed25519 key pair; the address is the hex
// encoding of the public key.
func NewAccount() (*Account, error) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, err
	}
	return &Account{
		Address: hex.EncodeToString(pub),
		Pub:     pub,
		Priv:    priv,
	}, nil
}

// NewOperation builds and signs an operation from this account.
func (a *Account) NewOperation(nonce uint64, target string, value, fee int64, paymaster string, expiry uint64, fail bool) *Operation {
	op := &Operation{
		Sender:    a.Address,
		Nonce:     nonce,
		Target:    target,
		Value:     value,
		Fee:       fee,
		Paymaster: paymaster,
		Expiry:    expiry,
		Fail:      fail,
	}
	op.Sign(a.Priv)
	return op
}

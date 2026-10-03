// Package bundler implements a local, in-memory simulation of a smart-account
// bundling and paymaster-sponsored fee flow.
package bundler

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// RejectReason identifies, in a machine-distinguishable way, why an operation
// was rejected either at submission time or during settlement.
type RejectReason string

const (
	ReasonNone                     RejectReason = ""
	ReasonBadSignature             RejectReason = "BAD_SIGNATURE"
	ReasonExpired                  RejectReason = "EXPIRED"
	ReasonNonceUsed                RejectReason = "NONCE_ALREADY_USED"
	ReasonNonceGap                 RejectReason = "NONCE_GAP"
	ReasonDuplicate                RejectReason = "DUPLICATE_OPERATION"
	ReasonInsufficientFunds        RejectReason = "INSUFFICIENT_FUNDS"
	ReasonPaymasterDepositTooLow   RejectReason = "PAYMASTER_DEPOSIT_TOO_LOW"
	ReasonPaymasterAllowanceExceed RejectReason = "PAYMASTER_ALLOWANCE_EXCEEDED"
	ReasonNonceGapTimeout          RejectReason = "NONCE_GAP_TIMEOUT"
	ReasonAccountPendingLimit      RejectReason = "ACCOUNT_PENDING_LIMIT_EXCEEDED"
	ReasonGlobalPendingLimit       RejectReason = "GLOBAL_PENDING_LIMIT_EXCEEDED"
)

// OpStatus is the terminal state of an operation.
type OpStatus string

const (
	StatusPending  OpStatus = "PENDING"
	StatusSuccess  OpStatus = "SUCCESS"
	StatusFailed   OpStatus = "FAILED_EXECUTION"
	StatusRejected OpStatus = "REJECTED"
	StatusReplayed OpStatus = "REPLAYED_NOOP"
)

// Operation is a user operation submitted by a smart account.
type Operation struct {
	Sender    string // account address (hex of ed25519 public key)
	Nonce     uint64
	Target    string // ledger account receiving Value; empty means no transfer
	Value     int64
	Fee       int64  // fee paid to the bundler
	Paymaster string // optional paymaster ID sponsoring the fee
	Expiry    uint64 // unix seconds; 0 means never expires
	Fail      bool   // simulates an execution-stage failure
	Signature []byte
}

// ID returns the deterministic identifier of the operation.
func (op *Operation) ID() string {
	sum := sha256.Sum256(op.digest())
	return hex.EncodeToString(sum[:])
}

// digest is the canonical byte encoding covered by the signature.
func (op *Operation) digest() []byte {
	buf := make([]byte, 0, 128)
	buf = append(buf, []byte(op.Sender)...)
	buf = binary.BigEndian.AppendUint64(buf, op.Nonce)
	buf = append(buf, []byte(op.Target)...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(op.Value))
	buf = binary.BigEndian.AppendUint64(buf, uint64(op.Fee))
	buf = append(buf, []byte(op.Paymaster)...)
	buf = binary.BigEndian.AppendUint64(buf, op.Expiry)
	if op.Fail {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

// Sign signs the operation with the account private key.
func (op *Operation) Sign(priv ed25519.PrivateKey) {
	op.Signature = ed25519.Sign(priv, op.digest())
}

// VerifySignature checks the operation signature against the sender public key.
func (op *Operation) VerifySignature() bool {
	pub, err := hex.DecodeString(op.Sender)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(pub), op.digest(), op.Signature)
}

// Receipt is the explainable, terminal outcome of an operation.
type Receipt struct {
	OpID   string
	Status OpStatus
	Reason RejectReason
	Detail string
}

func (r Receipt) String() string {
	return fmt.Sprintf("op=%s status=%s reason=%s %s", r.OpID[:8], r.Status, r.Reason, r.Detail)
}

package bundler

import (
	"fmt"
	"sync"
)

// Bundler collects executable operations from the mempool and settles them in
// a deterministic order. Settlement is idempotent: re-settling the same
// operation is a recorded no-op and never charges twice.
type Bundler struct {
	ID      string // ledger account receiving fees
	ledger  *Ledger
	mempool *Mempool

	mu         sync.Mutex
	settleMu   sync.Mutex // serializes settlement so replays interleave safely
	paymasters map[string]*Paymaster
	receipts   map[string]Receipt
}

func NewBundler(id string, ledger *Ledger, mempool *Mempool) *Bundler {
	return &Bundler{
		ID:         id,
		ledger:     ledger,
		mempool:    mempool,
		paymasters: make(map[string]*Paymaster),
		receipts:   make(map[string]Receipt),
	}
}

func (b *Bundler) RegisterPaymaster(p *Paymaster) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.paymasters[p.ID] = p
}

// Receipt returns the recorded terminal state of an operation.
func (b *Bundler) Receipt(opID string) (Receipt, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.receipts[opID]
	return r, ok
}

// CollectAndSettle drains all currently executable operations and settles
// them in deterministic (sender, nonce) order. One operation's failure never
// changes the outcome of the others.
func (b *Bundler) CollectAndSettle() []Receipt {
	return b.Settle(b.mempool.drainExecutable())
}

// Settle settles the given operations in order. It is safe to call with an
// already-settled batch: every operation then resolves to REPLAYED_NOOP and
// no balance moves.
func (b *Bundler) Settle(ops []*Operation) []Receipt {
	b.settleMu.Lock()
	defer b.settleMu.Unlock()
	receipts := make([]Receipt, 0, len(ops))
	for _, op := range ops {
		receipts = append(receipts, b.settleOne(op))
	}
	ids := make([]string, 0, len(ops))
	for _, op := range ops {
		ids = append(ids, op.ID())
	}
	b.mempool.markSettled(ids...)
	return receipts
}

func (b *Bundler) settleOne(op *Operation) Receipt {
	id := op.ID()

	b.mu.Lock()
	if _, ok := b.receipts[id]; ok {
		b.mu.Unlock()
		// Replay: the original receipt stays recorded; the caller gets a
		// REPLAYED_NOOP and no balance moves.
		return Receipt{OpID: id, Status: StatusReplayed, Detail: "already settled; no balance changes"}
	}
	b.mu.Unlock()

	var r Receipt
	switch {
	case op.Paymaster != "":
		r = b.settleSponsored(op)
	default:
		r = b.settleSelfPaid(op)
	}
	r.OpID = id

	b.mu.Lock()
	b.receipts[id] = r
	b.mu.Unlock()
	return r
}

// settleSponsored charges the fee to the paymaster. The charge is
// all-or-nothing: if the deposit or the per-account allowance cannot cover
// the full fee, the operation is rejected and no balance moves anywhere.
func (b *Bundler) settleSponsored(op *Operation) Receipt {
	b.mu.Lock()
	pm, ok := b.paymasters[op.Paymaster]
	b.mu.Unlock()
	if !ok {
		return Receipt{Status: StatusRejected, Reason: ReasonPaymasterDepositTooLow,
			Detail: fmt.Sprintf("unknown paymaster %q", op.Paymaster)}
	}
	if reason, ok := pm.canSponsor(b.ledger, op.Sender, op.Fee); !ok {
		return Receipt{Status: StatusRejected, Reason: reason,
			Detail: fmt.Sprintf("paymaster %s cannot cover fee %d for %s; rejected atomically", pm.ID, op.Fee, op.Sender[:8])}
	}
	pm.charge(b.ledger, b.ID, op.Sender, op.Fee)
	return b.execute(op)
}

// settleSelfPaid charges the fee to the sender, atomically.
func (b *Bundler) settleSelfPaid(op *Operation) Receipt {
	if !b.ledger.canCover(op.Sender, op.Fee) {
		return Receipt{Status: StatusRejected, Reason: ReasonInsufficientFunds,
			Detail: fmt.Sprintf("sender %s cannot cover fee %d; rejected atomically", op.Sender[:8], op.Fee)}
	}
	b.ledger.transfer(op.Sender, b.ID, op.Fee)
	return b.execute(op)
}

// execute applies the value transfer. Execution failure (op.Fail) does not
// refund the fee: the bundler keeps it as compensation for the attempted
// execution, which makes the failed operation's fee ownership explicit.
func (b *Bundler) execute(op *Operation) Receipt {
	if op.Fail {
		return Receipt{Status: StatusFailed,
			Detail: fmt.Sprintf("execution failed; fee %d kept by bundler %s, value transfer skipped", op.Fee, b.ID)}
	}
	if op.Value > 0 {
		if !b.ledger.canCover(op.Sender, op.Value) {
			return Receipt{Status: StatusFailed,
				Detail: fmt.Sprintf("insufficient funds for value %d; fee %d kept by bundler %s", op.Value, op.Fee, b.ID)}
		}
		b.ledger.transfer(op.Sender, op.Target, op.Value)
	}
	return Receipt{Status: StatusSuccess, Detail: fmt.Sprintf("settled; fee %d to bundler %s", op.Fee, b.ID)}
}

package bundler

import (
	"fmt"
	"sort"
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
	executable, timedOut := b.mempool.Process()
	receipts := b.Settle(executable)
	// Blocked operations whose gap wait window elapsed get an explicit,
	// queryable terminal state. No fee is charged and no balance moves: they
	// never executed. Their nonces have already been consumed by the mempool,
	// so a later resubmission reports NONCE_ALREADY_USED.
	for _, op := range timedOut {
		r := Receipt{
			OpID:   op.ID(),
			Status: StatusTimedOut,
			Reason: ReasonNonceGapTimeout,
			Detail: fmt.Sprintf(
				"sender %s nonce %d timed out waiting for the gap to fill; no fee charged, no balance changes",
				op.Sender[:8], op.Nonce),
		}
		b.recordReceipt(r)
		b.mempool.markSettled(r.OpID)
		receipts = append(receipts, r)
	}
	return receipts
}

// SettleDirect settles a batch delivered straight from an external scheduler
// without going through the queue. Every operation passes through the very
// same admission rules as a queued submission (signature, expiry, nonce
// history and settled-content replay); operations that never queued are
// checked no less strictly. Admission and settlement happen per operation, so
// a rejected operation neither moves balances nor affects the others. The
// batch is processed in deterministic (sender, nonce) order; a nonce that
// would open a gap is rejected with NONCE_GAP.
func (b *Bundler) SettleDirect(ops []*Operation) []Receipt {
	b.settleMu.Lock()
	defer b.settleMu.Unlock()

	ordered := append([]*Operation(nil), ops...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Sender != ordered[j].Sender {
			return ordered[i].Sender < ordered[j].Sender
		}
		return ordered[i].Nonce < ordered[j].Nonce
	})

	receipts := make([]Receipt, 0, len(ordered))
	reserved := make([]*Operation, 0, len(ordered))
	b.mempool.mu.Lock()
	now := b.mempool.nowUnix()
	b.mempool.mu.Unlock()

	seen := make(map[string]struct{}, len(ordered))
	for _, op := range ordered {
		id := op.ID()
		if _, dupWithin := seen[id]; dupWithin {
			r := Receipt{OpID: id, Status: StatusRejected, Reason: ReasonDuplicate,
				Detail: rejectDetail(ReasonDuplicate, op) + " (duplicate within batch)"}
			b.recordReceiptIfAbsent(r)
			receipts = append(receipts, r)
			continue
		}
		seen[id] = struct{}{}

		if reason, err := b.mempool.reserveDirect(op, now); reason != ReasonNone {
			r := Receipt{OpID: id, Status: StatusRejected, Reason: reason, Detail: err.Error()}
			b.recordReceiptIfAbsent(r)
			receipts = append(receipts, r)
			continue
		}
		reserved = append(reserved, op)
	}

	settled := b.settleMany(reserved)
	ids := make([]string, 0, len(reserved))
	for _, op := range reserved {
		ids = append(ids, op.ID())
	}
	b.mempool.markSettled(ids...)
	return append(receipts, settled...)
}

// Settle settles the given operations in order. It is safe to call with an
// already-settled batch: every operation then resolves to REPLAYED_NOOP and
// no balance moves.
func (b *Bundler) Settle(ops []*Operation) []Receipt {
	b.settleMu.Lock()
	defer b.settleMu.Unlock()
	return b.settleMany(ops)
}

// settleMany performs settlement for ops already admitted/removed from the
// mempool. The caller must hold b.settleMu.
func (b *Bundler) settleMany(ops []*Operation) []Receipt {
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

func (b *Bundler) recordReceipt(r Receipt) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.receipts[r.OpID] = r
}

// recordReceiptIfAbsent keeps the original terminal receipt when an operation
// ID was already settled and rejected again.
func (b *Bundler) recordReceiptIfAbsent(r Receipt) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.receipts[r.OpID]; !ok {
		b.receipts[r.OpID] = r
	}
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

package bundler

import (
	"fmt"
	"sort"
	"sync"
)

// Bundler settles operations from both supported entry paths and hands out
// explainable receipts. Settlement is serialized by settleMu, and every
// operation content ID receives exactly one terminal receipt that is never
// overwritten by any later result.
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

// recordReceiptOnce stores r only if the operation ID has no terminal receipt
// yet. Terminal states are final and unique: a later success, failure,
// rejection or timeout can never overwrite the first one.
func (b *Bundler) recordReceiptOnce(r Receipt) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.receipts[r.OpID]; !ok {
		b.receipts[r.OpID] = r
	}
}

// CollectAndSettle drains the unified queue and settles every currently
// executable operation in deterministic (sender, nonce) order. Operations
// whose gap wait window elapsed get an explicit TIMED_OUT_NONCE_GAP terminal
// state; no fee is charged and no balance moves for them. One operation's
// failure never changes the outcome of the others.
func (b *Bundler) CollectAndSettle() []Receipt {
	b.settleMu.Lock()
	defer b.settleMu.Unlock()

	executable, timedOut := b.mempool.Drain()
	receipts := b.settleMany(executable)

	for _, op := range timedOut {
		r := Receipt{
			OpID:   op.ID(),
			Status: StatusTimedOut,
			Reason: ReasonNonceGapTimeout,
			Detail: fmt.Sprintf(
				"sender %s nonce %d timed out waiting for the gap to fill; no fee charged, no balance changes",
				op.Sender[:8], op.Nonce),
		}
		b.recordReceiptOnce(r)
		receipts = append(receipts, r)
	}
	b.mempool.Commit(idsOf(executable, timedOut)...)
	return receipts
}

// SettleDirect admits and settles a batch delivered straight from an external
// scheduler, without callers having to queue first. Every operation passes
// through the exact same admission rules as a queued submission (signature,
// expiry, nonce history and settled-content replay): an operation that never
// queued is checked no less strictly, and a nonce ahead of the account's next
// one is suspended to wait for the gap under the configured window rather
// than rejected on the spot.
//
// Admission happens per operation, so a rejected operation neither moves
// balances nor affects the others in the batch. Operations that are already
// executable right after admission settle immediately in deterministic
// (sender, nonce) order; the rest remain suspended and surface later through
// CollectAndSettle/SettleDirect, filling the gap in nonce order or reaching
// the distinguishable timeout terminal state. Each input produces exactly one
// receipt in the returned slice.
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

	b.mempool.mu.Lock()
	now := b.mempool.nowUnix()
	b.mempool.mu.Unlock()

	receipts := make([]Receipt, 0, len(ordered))
	seen := make(map[string]struct{}, len(ordered))
	for _, op := range ordered {
		id := op.ID()
		if _, dupWithin := seen[id]; dupWithin {
			r := Receipt{OpID: id, Status: StatusRejected, Reason: ReasonDuplicate,
				Detail: rejectDetail(ReasonDuplicate, op) + " (duplicate within batch)"}
			b.recordReceiptOnce(r)
			receipts = append(receipts, r)
			continue
		}
		seen[id] = struct{}{}

		reason, _, err := b.mempool.Admit(op, now)
		if reason != ReasonNone {
			if reason == ReasonDuplicate {
				// Re-delivering already terminal content through a
				// settlement entry is a recorded no-op: the original
				// terminal receipt is preserved and no balance moves.
				existing, _ := b.Receipt(id)
				r := Receipt{OpID: id, Status: StatusReplayed,
					Reason: existing.Reason, Detail: err.Error() + "; no balance changes"}
				b.recordReceiptOnce(r)
				receipts = append(receipts, r)
				continue
			}
			r := Receipt{OpID: id, Status: StatusRejected, Reason: reason, Detail: err.Error()}
			b.recordReceiptOnce(r)
			receipts = append(receipts, r)
		}
	}

	// Anything that became executable through this batch (which may also
	// include operations queued earlier by the other entry path) settles
	// now, following the same drain/settle path as CollectAndSettle.
	executable, timedOut := b.mempool.Drain()
	receipts = append(receipts, b.settleMany(executable)...)
	for _, op := range timedOut {
		r := Receipt{
			OpID:   op.ID(),
			Status: StatusTimedOut,
			Reason: ReasonNonceGapTimeout,
			Detail: fmt.Sprintf(
				"sender %s nonce %d timed out waiting for the gap to fill; no fee charged, no balance changes",
				op.Sender[:8], op.Nonce),
		}
		b.recordReceiptOnce(r)
		receipts = append(receipts, r)
	}
	b.mempool.Commit(idsOf(executable, timedOut)...)
	return receipts
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
// mempool. The caller must hold b.settleMu. Settlement-stage rejection still
// consumes the nonce (the mempool has already drained it), and the rejection
// receipt is the operation's unique terminal state.
func (b *Bundler) settleMany(ops []*Operation) []Receipt {
	receipts := make([]Receipt, 0, len(ops))
	for _, op := range ops {
		r := b.settleOne(op)
		b.recordReceiptOnce(r)
		receipts = append(receipts, r)
	}
	return receipts
}

func (b *Bundler) settleOne(op *Operation) Receipt {
	id := op.ID()

	if existing, ok := b.Receipt(id); ok {
		// Replay: the original terminal receipt stays recorded; the caller
		// gets a REPLAYED_NOOP and no balance moves.
		return Receipt{OpID: id, Status: StatusReplayed,
			Reason: existing.Reason, Detail: "already settled; no balance changes"}
	}

	var r Receipt
	switch {
	case op.Paymaster != "":
		r = b.settleSponsored(op)
	default:
		r = b.settleSelfPaid(op)
	}
	r.OpID = id
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

func idsOf(executable, timedOut []*Operation) []string {
	ids := make([]string, 0, len(executable)+len(timedOut))
	for _, op := range executable {
		ids = append(ids, op.ID())
	}
	for _, op := range timedOut {
		ids = append(ids, op.ID())
	}
	return ids
}

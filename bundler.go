package bundler

import (
	"fmt"
	"sync"
)

// Bundler collects executable operations from the mempool and settles them in
// a deterministic order. Settlement is idempotent: re-settling the same
// operation is a recorded no-op and never charges twice.
type Bundler struct {
	ID     string // ledger account receiving fees
	ledger *Ledger
	// mempool is the single source of admission rules: the queue path
	// (Mempool.Submit) and the external direct-batch path (SettleBatch) both
	// delegate to Mempool.Admit.
	mempool *Mempool

	mu         sync.Mutex
	settleMu   sync.Mutex // serializes settlement so replays interleave safely
	paymasters map[string]*Paymaster
	receipts   map[string]Receipt
	// settledIDs marks operations that have actually gone through a
	// settlement pass. It is deliberately separate from admission/gap-timeout
	// receipts so REPLAYED_NOOP stays tied to settlement only.
	settledIDs map[string]struct{}
}

func NewBundler(id string, ledger *Ledger, mempool *Mempool) *Bundler {
	return &Bundler{
		ID:         id,
		ledger:     ledger,
		mempool:    mempool,
		paymasters: make(map[string]*Paymaster),
		receipts:   make(map[string]Receipt),
		settledIDs: make(map[string]struct{}),
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
// them in deterministic (sender, nonce) order. Operations whose nonce-gap
// wait window has expired leave the pool with a terminal
// NONCE_GAP_TIMEOUT receipt instead, moving no funds. One operation's
// failure never changes the outcome of the others.
func (b *Bundler) CollectAndSettle() []Receipt {
	executable, gapTimedOut := b.mempool.drainExecutable()
	receipts := b.Settle(executable)
	for _, op := range gapTimedOut {
		r := Receipt{
			OpID:   op.ID(),
			Status: StatusRejected,
			Reason: ReasonNonceGapTimeout,
			Detail: fmt.Sprintf(
				"sender %s nonce %d waited past the gap window; rejected, no balance changes",
				op.Sender[:8], op.Nonce),
		}
		b.recordTerminal(r)
		receipts = append(receipts, r)
	}
	return receipts
}

// BatchItemResult is the per-operation outcome of an externally delivered
// direct batch. Every input operation gets exactly one result, in input
// order, even when rejected.
type BatchItemResult struct {
	Op *Operation
	// Admission carries the shared admission verdict — the identical rules
	// used by the mempool queue path.
	Admission AdmissionDecision
	// Receipt is the terminal receipt once the operation has resolved; while
	// Pending is true it is a PENDING placeholder, not a terminal state.
	Receipt Receipt
	// Pending is true when the operation was admitted but is still blocked
	// behind a nonce gap inside its wait window.
	Pending bool
}

// SettleBatch is the external scheduler entry point: a whole batch delivered
// at once without prior queueing. Every operation passes through the exact
// same admission rules as Mempool.Submit — signature, expiry, nonce history
// (a different payload on an already-effective nonce is still blocked),
// duplicate settled content and pending-capacity limits.
//
// Rejected operations get a terminal receipt, move no funds for any party,
// never affect other items and never displace already queued operations.
// Admitted operations join the pool; everything then currently executable —
// from this batch or previously queued — is settled in deterministic
// (sender, nonce) order, and gap-window expiries resolve as
// NONCE_GAP_TIMEOUT.
func (b *Bundler) SettleBatch(ops []*Operation) []BatchItemResult {
	b.settleMu.Lock()
	defer b.settleMu.Unlock()

	results := make([]BatchItemResult, len(ops))
	byID := make(map[string]int, len(ops))
	for i, op := range ops {
		d := b.mempool.Admit(op)
		results[i] = BatchItemResult{Op: op, Admission: d}
		byID[op.ID()] = i
		if !d.Accepted {
			r := Receipt{OpID: op.ID(), Status: StatusRejected, Reason: d.Reason, Detail: d.Err.Error()}
			b.recordTerminal(r)
			results[i].Receipt = r
		}
	}

	for _, r := range b.collectAndSettleLocked() {
		if i, ok := byID[r.OpID]; ok && results[i].Receipt.Status == "" {
			results[i].Receipt = r
		}
	}
	for i := range results {
		if results[i].Admission.Accepted && results[i].Receipt.Status == "" {
			results[i].Pending = true
			results[i].Receipt = Receipt{
				OpID:   results[i].Op.ID(),
				Status: StatusPending,
				Detail: "queued behind a nonce gap; waiting within the gap window",
			}
		}
	}
	return results
}

// collectAndSettleLocked mirrors CollectAndSettle while the caller already
// holds settleMu, so a direct batch cannot interleave with another drain.
func (b *Bundler) collectAndSettleLocked() []Receipt {
	executable, gapTimedOut := b.mempool.drainExecutable()
	receipts := b.settleAll(executable)
	for _, op := range gapTimedOut {
		r := Receipt{
			OpID:   op.ID(),
			Status: StatusRejected,
			Reason: ReasonNonceGapTimeout,
			Detail: fmt.Sprintf(
				"sender %s nonce %d waited past the gap window; rejected, no balance changes",
				op.Sender[:8], op.Nonce),
		}
		b.recordTerminal(r)
		receipts = append(receipts, r)
	}
	return receipts
}

// Settle settles the given operations in order. It is safe to call with an
// already-settled batch: every operation then resolves to REPLAYED_NOOP and
// no balance moves.
func (b *Bundler) Settle(ops []*Operation) []Receipt {
	b.settleMu.Lock()
	defer b.settleMu.Unlock()
	return b.settleAll(ops)
}

func (b *Bundler) settleAll(ops []*Operation) []Receipt {
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

// recordTerminal stores an externally visible terminal receipt (admission
// rejection or gap timeout) without overwriting an existing receipt.
func (b *Bundler) recordTerminal(r Receipt) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.receipts[r.OpID]; !ok {
		b.receipts[r.OpID] = r
	}
}

func (b *Bundler) settleOne(op *Operation) Receipt {
	id := op.ID()

	b.mu.Lock()
	_, alreadySettled := b.settledIDs[id]
	b.mu.Unlock()
	if alreadySettled {
		// Replay: the original receipt stays recorded; the caller gets a
		// REPLAYED_NOOP and no balance moves.
		return Receipt{OpID: id, Status: StatusReplayed, Detail: "already settled; no balance changes"}
	}

	var r Receipt
	switch {
	case op.Paymaster != "":
		r = b.settleSponsored(op)
	default:
		r = b.settleSelfPaid(op)
	}
	r.OpID = id

	b.mu.Lock()
	// A settlement receipt always wins over a prior admission/gap placeholder.
	b.receipts[id] = r
	b.settledIDs[id] = struct{}{}
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

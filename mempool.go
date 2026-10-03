package bundler

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Config tunes mempool-wide behaviour. Zero values disable the corresponding
// limit (unlimited pending operations / wait indefinitely for a nonce gap).
type Config struct {
	// GapWait is how long operations may stay queued behind a missing nonce
	// before they receive a terminal NONCE_GAP_TIMEOUT rejection.
	GapWait time.Duration
	// MaxPendingPerAccount caps queued (not yet settled) operations for a
	// single sender. Zero means unlimited.
	MaxPendingPerAccount int
	// MaxPendingGlobal caps the total queued operations across all senders.
	// Zero means unlimited.
	MaxPendingGlobal int
}

// Mempool accepts concurrently submitted operations and guarantees that, per
// account, operations become executable strictly in nonce order regardless of
// submission order.
//
// Admission rules are identical no matter how an operation arrives: the
// queue path (Submit) and the external direct-batch path (Bundler.SettleBatch
// -> Admit) share the exact same checks and reject reasons.
type Mempool struct {
	cfg Config

	mu sync.Mutex
	// pending[sender][nonce] holds admitted but not yet settled ops.
	pending map[string]map[uint64]*queuedOp
	// nextNonce[sender] is the next nonce eligible for execution.
	nextNonce map[string]uint64
	// settled op IDs, for replay protection at admission time.
	settled map[string]struct{}

	pendingGlobal int

	now func() time.Time
}

// queuedOp pairs an admitted operation with the instant it entered the pool.
type queuedOp struct {
	op       *Operation
	queuedAt time.Time
}

// NewMempool builds a mempool with the default configuration: operations wait
// indefinitely behind a nonce gap and pending capacity is unlimited.
func NewMempool() *Mempool {
	return NewMempoolWithConfig(Config{})
}

// NewMempoolWithConfig builds a mempool with explicit gap-timeout and capacity
// settings.
func NewMempoolWithConfig(cfg Config) *Mempool {
	return &Mempool{
		cfg:       cfg,
		pending:   make(map[string]map[uint64]*queuedOp),
		nextNonce: make(map[string]uint64),
		settled:   make(map[string]struct{}),
		now:       time.Now,
	}
}

// setClock overrides the time source. Test-only helper.
func (m *Mempool) setClock(now func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = now
}

// AdmissionDecision is the result of running the shared admission rules over
// one operation, whether it came from the queue or from an external batch.
type AdmissionDecision struct {
	Op     *Operation
	Reason RejectReason
	Err    error
	// Accepted is true when the operation now occupies a nonce slot in the
	// pool. Rejected operations never consume capacity.
	Accepted bool
}

// Admitted reports whether the operation passed the admission rules.
func (d AdmissionDecision) Admitted() bool { return d.Accepted }

// Submit validates an operation and queues it. A rejection here never affects
// any other operation. This is the queue entry point; it runs exactly the
// same admission rules as an externally delivered direct batch.
func (m *Mempool) Submit(op *Operation) (RejectReason, error) {
	d := m.Admit(op)
	return d.Reason, d.Err
}

// Admit runs the shared admission rules for one operation and, on success,
// enqueues it under its (sender, nonce) slot. The checks are:
//
//  1. signature must verify against the sender key;
//  2. the operation must not be expired (Expiry is unix seconds, 0 = never);
//  3. the exact operation content must not have settled before (duplicate);
//  4. the nonce must not already be effective (settled or pending) for this
//     sender — a different payload on the same nonce is still blocked;
//  5. per-account and global pending-capacity limits must not be exceeded.
//
// Every rejection has a distinct RejectReason and never mutates balances;
// rejecting an operation never removes or alters any queued operation.
func (m *Mempool) Admit(op *Operation) AdmissionDecision {
	// Steps 1-2 are stateless and run outside the pool lock so a flood of
	// invalid operations cannot block legitimate concurrent submitters.
	if !op.VerifySignature() {
		return AdmissionDecision{Op: op, Reason: ReasonBadSignature,
			Err: fmt.Errorf("operation %s: signature does not match sender %s", op.ID()[:8], senderHint(op.Sender))}
	}
	if op.Expiry != 0 && uint64(m.now().Unix()) > op.Expiry {
		return AdmissionDecision{Op: op, Reason: ReasonExpired,
			Err: fmt.Errorf("operation %s: expired at %d", op.ID()[:8], op.Expiry)}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	id := op.ID()
	if _, ok := m.settled[id]; ok {
		return AdmissionDecision{Op: op, Reason: ReasonDuplicate,
			Err: fmt.Errorf("operation %s: already settled", id[:8])}
	}
	if op.Nonce < m.nextNonce[op.Sender] {
		return AdmissionDecision{Op: op, Reason: ReasonNonceUsed,
			Err: fmt.Errorf("operation %s: nonce %d already effective (next=%d)", id[:8], op.Nonce, m.nextNonce[op.Sender])}
	}
	if _, ok := m.pending[op.Sender][op.Nonce]; ok {
		return AdmissionDecision{Op: op, Reason: ReasonNonceUsed,
			Err: fmt.Errorf("operation %s: nonce %d already pending for sender", id[:8], op.Nonce)}
	}
	if m.cfg.MaxPendingPerAccount > 0 && len(m.pending[op.Sender]) >= m.cfg.MaxPendingPerAccount {
		return AdmissionDecision{Op: op, Reason: ReasonAccountPendingLimit,
			Err: fmt.Errorf("operation %s: sender %s already has %d pending ops (limit %d)",
				id[:8], senderHint(op.Sender), len(m.pending[op.Sender]), m.cfg.MaxPendingPerAccount)}
	}
	if m.cfg.MaxPendingGlobal > 0 && m.pendingGlobal >= m.cfg.MaxPendingGlobal {
		return AdmissionDecision{Op: op, Reason: ReasonGlobalPendingLimit,
			Err: fmt.Errorf("operation %s: mempool already holds %d pending ops (global limit %d)",
				id[:8], m.pendingGlobal, m.cfg.MaxPendingGlobal)}
	}
	if m.pending[op.Sender] == nil {
		m.pending[op.Sender] = make(map[uint64]*queuedOp)
	}
	m.pending[op.Sender][op.Nonce] = &queuedOp{op: op, queuedAt: m.now()}
	m.pendingGlobal++
	return AdmissionDecision{Op: op, Accepted: true}
}

// PendingCount reports the number of queued operations, globally and for one
// sender. Capacity rejections leave these counters unchanged.
func (m *Mempool) PendingCount(sender string) (perSender, global int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pending[sender]), m.pendingGlobal
}

// DrainResult partitions everything that leaves the pool during a drain.
type DrainResult struct {
	// Executable is the contiguous, gap-free run per sender, in deterministic
	// (sender, nonce) order.
	Executable []*Operation
	// GapTimedOut holds operations that waited longer than GapWait behind a
	// missing nonce. They leave the pool with a terminal
	// NONCE_GAP_TIMEOUT rejection and free their capacity.
	GapTimedOut []*Operation
}

// drain removes, for every sender, the contiguous executable run starting at
// its next nonce. When GapWait > 0, operations blocked behind a missing nonce
// that have waited past the window are evicted as gap timeouts; the sender's
// next nonce then skips past them so the same nonce can never take effect
// twice later. Waiting is evaluated per sender and never blocks other senders.
func (m *Mempool) drain() DrainResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var res DrainResult

	for sender, byNonce := range m.pending {
		next := m.nextNonce[sender]
		// Contiguous executable run.
		for {
			q, ok := byNonce[next]
			if !ok {
				break
			}
			res.Executable = append(res.Executable, q.op)
			delete(byNonce, next)
			m.pendingGlobal--
			next++
		}

		// Anything still queued for this sender is blocked by a gap. If the
		// oldest waiter has exceeded the window, skip the whole gap: every
		// queued op for this sender receives the same terminal reason.
		if len(byNonce) > 0 && m.cfg.GapWait > 0 {
			oldest := now
			for _, q := range byNonce {
				if q.queuedAt.Before(oldest) {
					oldest = q.queuedAt
				}
			}
			if now.Sub(oldest) >= m.cfg.GapWait {
				nonces := make([]uint64, 0, len(byNonce))
				for n := range byNonce {
					nonces = append(nonces, n)
				}
				sort.Slice(nonces, func(i, j int) bool { return nonces[i] < nonces[j] })
				for _, n := range nonces {
					res.GapTimedOut = append(res.GapTimedOut, byNonce[n].op)
					delete(byNonce, n)
					m.pendingGlobal--
					if n >= next {
						next = n + 1
					}
				}
			}
		}

		m.nextNonce[sender] = next
		if len(byNonce) == 0 {
			delete(m.pending, sender)
		}
	}

	sort.Slice(res.Executable, func(i, j int) bool {
		if res.Executable[i].Sender != res.Executable[j].Sender {
			return res.Executable[i].Sender < res.Executable[j].Sender
		}
		return res.Executable[i].Nonce < res.Executable[j].Nonce
	})
	sort.Slice(res.GapTimedOut, func(i, j int) bool {
		if res.GapTimedOut[i].Sender != res.GapTimedOut[j].Sender {
			return res.GapTimedOut[i].Sender < res.GapTimedOut[j].Sender
		}
		return res.GapTimedOut[i].Nonce < res.GapTimedOut[j].Nonce
	})
	return res
}

// drainExecutable keeps the original bundler entry point working: executable
// ops plus any ops whose gap window has expired. Callers must record terminal
// receipts for the second group; gap timeouts settle no funds.
func (m *Mempool) drainExecutable() ([]*Operation, []*Operation) {
	res := m.drain()
	return res.Executable, res.GapTimedOut
}

// markSettled records op IDs so replays are detectable.
func (m *Mempool) markSettled(ids ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		m.settled[id] = struct{}{}
	}
}

func senderHint(sender string) string {
	if len(sender) >= 8 {
		return sender[:8]
	}
	return sender
}

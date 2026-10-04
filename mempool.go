package bundler

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// DefaultGapWaitWindow is used when Mempool.GapWaitWindow is left zero.
const DefaultGapWaitWindow = 30 * time.Second

// Mempool accepts operations from every entry path (queued submission and
// externally delivered direct-settlement batches) and applies one identical
// set of admission rules to all of them. Per account, operations become
// effective strictly in nonce order regardless of arrival order or entry.
type Mempool struct {
	mu sync.Mutex
	// pending[sender][nonce] holds admitted but not yet effective ops. There
	// is at most one record per (sender, nonce), shared by every entry path.
	pending map[string]map[uint64]*Operation
	// nextNonce[sender] is the next nonce eligible to take effect.
	nextNonce map[string]uint64
	// settled holds content IDs that already reached a terminal state, so
	// exact-content replays are detectable on every entry path.
	settled map[string]struct{}
	// inflight holds content IDs currently being settled between Drain and
	// Commit. It closes the race in which a concurrent submission could
	// re-enter the very same operation while it has left pending but is not
	// yet recorded as settled.
	inflight map[string]struct{}
	// gapSince[sender] is when the current nonce gap was first observed;
	// absent while the account has no gap.
	gapSince map[string]uint64

	// GapWaitWindow is how long a missing nonce blocks later operations of
	// the same account before the blocked operations time out. Zero means
	// DefaultGapWaitWindow.
	GapWaitWindow time.Duration
	// MaxPendingPerAccount caps how many operations one account may have
	// suspended at the same time. Zero means unlimited.
	MaxPendingPerAccount int
	// MaxPendingTotal caps the suspended operations across all accounts.
	// Zero means unlimited.
	MaxPendingTotal int

	clock func() time.Time
}

func NewMempool() *Mempool {
	return &Mempool{
		pending:   make(map[string]map[uint64]*Operation),
		nextNonce: make(map[string]uint64),
		settled:   make(map[string]struct{}),
		inflight:  make(map[string]struct{}),
		gapSince:  make(map[string]uint64),
		clock:     time.Now,
	}
}

// SetClock overrides the time source (used to drive gap timeouts
// deterministically in tests).
func (m *Mempool) SetClock(f func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clock = f
}

func (m *Mempool) nowUnix() uint64 { return uint64(m.clock().Unix()) }

func (m *Mempool) gapWindow() time.Duration {
	if m.GapWaitWindow > 0 {
		return m.GapWaitWindow
	}
	return DefaultGapWaitWindow
}

// PendingCount reports suspended operation counts: total first, then the
// count for the given sender.
func (m *Mempool) PendingCount(sender string) (total, perSender int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, byNonce := range m.pending {
		total += len(byNonce)
	}
	return total, len(m.pending[sender])
}

// precheck runs the source-independent checks needing no mempool state.
// Queued submission and externally delivered direct batches go through the
// exact same precheck, so conclusions never depend on the entry path.
func precheck(op *Operation, now uint64) (RejectReason, bool) {
	if !op.VerifySignature() {
		return ReasonBadSignature, false
	}
	if op.Expiry != 0 && now > op.Expiry {
		return ReasonExpired, false
	}
	return ReasonNone, true
}

func rejectDetail(reason RejectReason, op *Operation) string {
	switch reason {
	case ReasonBadSignature:
		return fmt.Sprintf("operation %s: signature does not match sender %s", op.ID()[:8], op.Sender[:8])
	case ReasonExpired:
		return fmt.Sprintf("operation %s: expired at %d", op.ID()[:8], op.Expiry)
	case ReasonNonceUsed:
		return fmt.Sprintf("operation %s: nonce %d already effective or occupied for sender", op.ID()[:8], op.Nonce)
	case ReasonNonceGap:
		return fmt.Sprintf("operation %s: nonce %d skips the expected nonce for sender", op.ID()[:8], op.Nonce)
	case ReasonDuplicate:
		return fmt.Sprintf("operation %s: already settled", op.ID()[:8])
	case ReasonPendingCapPerAccount:
		return fmt.Sprintf("operation %s: per-account pending cap reached", op.ID()[:8])
	case ReasonPendingCapGlobal:
		return fmt.Sprintf("operation %s: global pending cap reached", op.ID()[:8])
	default:
		return fmt.Sprintf("operation %s: rejected (%s)", op.ID()[:8], reason)
	}
}

// Submit runs the unified admission and queues the operation when it is new.
// Submitting the exact same content again while it is pending or settling is
// an idempotent no-op; a same-nonce operation with different content is
// rejected with NONCE_ALREADY_USED. A rejection never affects any other
// operation and never moves balances.
func (m *Mempool) Submit(op *Operation) (RejectReason, error) {
	m.mu.Lock()
	now := m.nowUnix()
	m.mu.Unlock()

	reason, _, err := m.Admit(op, now)
	return reason, err
}

// Admit applies the single admission rule set shared by queued submission and
// direct external batches: signature and expiry first, then exact-content
// replay/settling protection, nonce history, same-nonce content conflict and
// pending capacity. A nonce ahead of the account's next nonce opens a gap and
// is suspended to wait for the missing nonce; it is never rejected merely for
// being ahead, regardless of the entry path.
//
// When reason is empty and inserted is true, the op is now stored as a
// pending record. When reason is empty but inserted is false, the exact same
// content was already pending or settling: the call converged onto that one
// record and changed nothing. Any non-empty reason is a rejection and leaves
// all state untouched. Admit acquires its own lock and is safe for concurrent
// use from every entry path.
func (m *Mempool) Admit(op *Operation, now uint64) (reason RejectReason, inserted bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := op.ID()
	if _, ok := m.settled[id]; ok {
		return ReasonDuplicate, false, fmt.Errorf("%s", rejectDetail(ReasonDuplicate, op))
	}
	if _, ok := m.inflight[id]; ok {
		// The identical content is currently being settled by another
		// goroutine. It already owns its single terminal outcome; treat this
		// arrival as the same idempotent operation.
		return ReasonNone, false, nil
	}
	if op.Nonce < m.nextNonce[op.Sender] {
		return ReasonNonceUsed, false, fmt.Errorf(
			"operation %s: nonce %d already effective for sender (next=%d)",
			id[:8], op.Nonce, m.nextNonce[op.Sender])
	}
	if existing, ok := m.pending[op.Sender][op.Nonce]; ok {
		if existing.ID() == id {
			// Same content through any path: converge onto the one record.
			return ReasonNone, false, nil
		}
		return ReasonNonceUsed, false, fmt.Errorf(
			"operation %s: nonce %d already occupied by a different operation for sender",
			id[:8], op.Nonce)
	}

	// Content-level checks run only once the nonce history and the
	// (sender, nonce) slot are valid for admission. This keeps a gapped but
	// otherwise invalid operation (bad signature, expiry) from being parked
	// and settling implicitly when the gap fills: the conclusion is taken at
	// admission time and is identical for every entry path.
	if reason, ok := precheck(op, now); !ok {
		return reason, false, fmt.Errorf("%s", rejectDetail(reason, op))
	}
	if m.MaxPendingPerAccount > 0 && len(m.pending[op.Sender]) >= m.MaxPendingPerAccount {
		return ReasonPendingCapPerAccount, false, fmt.Errorf(
			"operation %s: per-account pending cap %d reached for sender", id[:8], m.MaxPendingPerAccount)
	}
	if m.MaxPendingTotal > 0 {
		total := 0
		for _, byNonce := range m.pending {
			total += len(byNonce)
		}
		if total >= m.MaxPendingTotal {
			return ReasonPendingCapGlobal, false, fmt.Errorf(
				"operation %s: global pending cap %d reached", id[:8], m.MaxPendingTotal)
		}
	}

	if m.pending[op.Sender] == nil {
		m.pending[op.Sender] = make(map[uint64]*Operation)
	}
	m.pending[op.Sender][op.Nonce] = op
	m.refreshGapLocked(op.Sender)
	return ReasonNone, true, nil
}

// refreshGapLocked (re)computes the gap marker for a sender. The first
// observation time is sticky: a persistent gap keeps its original window
// start even after later operations arrive.
func (m *Mempool) refreshGapLocked(sender string) {
	byNonce := m.pending[sender]
	next := m.nextNonce[sender]
	if _, ok := byNonce[next]; ok || len(byNonce) == 0 {
		delete(m.gapSince, sender)
		return
	}
	if _, ok := m.gapSince[sender]; !ok {
		m.gapSince[sender] = m.nowUnix()
	}
}

// Drain returns the currently executable operations and the operations that
// have exhausted their gap wait window. One account's gap never blocks other
// accounts. Both slices are in deterministic (sender, nonce) order. The
// returned content IDs are registered as in-flight until Commit is called, so
// concurrent admission can never lead to a second settlement.
func (m *Mempool) Drain() (executable, timedOut []*Operation) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.nowUnix()
	window := uint64(m.gapWindow().Seconds())
	for sender, byNonce := range m.pending {
		next := m.nextNonce[sender]
		for {
			op, ok := byNonce[next]
			if !ok {
				break
			}
			executable = append(executable, op)
			m.inflight[op.ID()] = struct{}{}
			delete(byNonce, next)
			next++
		}
		m.nextNonce[sender] = next

		if len(byNonce) > 0 {
			since, waited := m.gapSince[sender]
			if !waited {
				since = now
				m.gapSince[sender] = now
			}
			if window > 0 && now-since >= window {
				for nonce, op := range byNonce {
					timedOut = append(timedOut, op)
					m.inflight[op.ID()] = struct{}{}
					delete(byNonce, nonce)
				}
				// The blocked operations leave the queue with a terminal
				// timeout, but their nonces are not consumed: the account
				// goes back to waiting for the missing nonce. Filling the
				// missing nonce afterwards reopens the account, and the same
				// nonce may be retried with new content. An exact resubmit of
				// the timed-out content remains DUPLICATE_OPERATION.
				m.nextNonce[sender] = next
				delete(m.gapSince, sender)
			}
		}
		if len(byNonce) == 0 {
			delete(m.pending, sender)
			delete(m.gapSince, sender)
		}
	}
	sortOps(executable)
	sortOps(timedOut)
	return executable, timedOut
}

// Commit records drained content IDs as having reached a terminal state and
// clears their in-flight marker.
func (m *Mempool) Commit(ids ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		m.settled[id] = struct{}{}
		delete(m.inflight, id)
	}
}

func sortOps(ops []*Operation) {
	sort.Slice(ops, func(i, j int) bool {
		if ops[i].Sender != ops[j].Sender {
			return ops[i].Sender < ops[j].Sender
		}
		return ops[i].Nonce < ops[j].Nonce
	})
}

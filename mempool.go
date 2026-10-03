package bundler

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// DefaultGapWaitWindow is used when Mempool.GapWaitWindow is left zero.
const DefaultGapWaitWindow = 30 * time.Second

// Mempool accepts concurrently submitted operations and guarantees that, per
// account, operations become executable strictly in nonce order regardless of
// submission order. The same admission rules serve queued submissions and
// externally delivered direct-settlement batches.
type Mempool struct {
	mu sync.Mutex
	// pending[sender][nonce] holds validated but not yet executable ops.
	pending map[string]map[uint64]*Operation
	// nextNonce[sender] is the next nonce eligible for execution.
	nextNonce map[string]uint64
	// settled op IDs, for replay protection at any entry path.
	settled map[string]struct{}
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
		return fmt.Sprintf("operation %s: nonce %d already effective or pending for sender", op.ID()[:8], op.Nonce)
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

// Submit validates an operation and queues it. A rejection never affects any
// other operation and never moves balances. Distinct RejectReasons separate
// bad signatures, expiry, reused nonces and capacity limits.
func (m *Mempool) Submit(op *Operation) (RejectReason, error) {
	m.mu.Lock()
	now := m.nowUnix()
	m.mu.Unlock()
	if reason, ok := precheck(op, now); !ok {
		return reason, fmt.Errorf("%s", rejectDetail(reason, op))
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if reason, err := m.admitLocked(op); reason != ReasonNone {
		return reason, err
	}
	if m.pending[op.Sender] == nil {
		m.pending[op.Sender] = make(map[uint64]*Operation)
	}
	m.pending[op.Sender][op.Nonce] = op
	m.refreshGapLocked(op.Sender)
	return ReasonNone, nil
}

// admitLocked applies the stateful admission rules shared by every entry
// path: settled-content replay, nonce history and pending capacity. The
// caller must hold m.mu and the op must already have passed precheck.
func (m *Mempool) admitLocked(op *Operation) (RejectReason, error) {
	if _, ok := m.settled[op.ID()]; ok {
		return ReasonDuplicate, fmt.Errorf("%s", rejectDetail(ReasonDuplicate, op))
	}
	if op.Nonce < m.nextNonce[op.Sender] {
		return ReasonNonceUsed, fmt.Errorf("operation %s: nonce %d already effective for sender (next=%d)",
			op.ID()[:8], op.Nonce, m.nextNonce[op.Sender])
	}
	if _, ok := m.pending[op.Sender][op.Nonce]; ok {
		return ReasonNonceUsed, fmt.Errorf("operation %s: nonce %d already pending for sender", op.ID()[:8], op.Nonce)
	}
	if m.MaxPendingPerAccount > 0 && len(m.pending[op.Sender]) >= m.MaxPendingPerAccount {
		return ReasonPendingCapPerAccount, fmt.Errorf("operation %s: per-account pending cap %d reached for sender",
			op.ID()[:8], m.MaxPendingPerAccount)
	}
	if m.MaxPendingTotal > 0 {
		total := 0
		for _, byNonce := range m.pending {
			total += len(byNonce)
		}
		if total >= m.MaxPendingTotal {
			return ReasonPendingCapGlobal, fmt.Errorf("operation %s: global pending cap %d reached",
				op.ID()[:8], m.MaxPendingTotal)
		}
	}
	return ReasonNone, nil
}

// reserveDirect admits an operation delivered straight to settlement without
// ever queuing. Admission is identical to Submit's: same signature, expiry,
// settled-content and nonce-history checks. In addition the nonce must be
// exactly the account's next one (a direct batch cannot introduce a waiting
// gap). On success the nonce is reserved atomically, so concurrent settlers
// can never consume the same nonce twice. The caller must hold no lock.
func (m *Mempool) reserveDirect(op *Operation, now uint64) (RejectReason, error) {
	if reason, ok := precheck(op, now); !ok {
		return reason, fmt.Errorf("%s", rejectDetail(reason, op))
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.settled[op.ID()]; ok {
		return ReasonDuplicate, fmt.Errorf("%s", rejectDetail(ReasonDuplicate, op))
	}
	next := m.nextNonce[op.Sender]
	switch {
	case op.Nonce < next:
		return ReasonNonceUsed, fmt.Errorf("operation %s: nonce %d already effective for sender (next=%d)",
			op.ID()[:8], op.Nonce, next)
	case op.Nonce > next:
		return ReasonNonceGap, fmt.Errorf("operation %s: nonce %d skips expected nonce %d for sender",
			op.ID()[:8], op.Nonce, next)
	}
	m.nextNonce[op.Sender] = next + 1
	return ReasonNone, nil
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

// Process returns the currently executable operations and the operations that
// have exhausted their gap wait window. One account's gap never blocks other
// accounts. Both slices are in deterministic (sender, nonce) order.
func (m *Mempool) Process() (executable, timedOut []*Operation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.processLocked(m.nowUnix())
}

// processLocked requires the caller to hold m.mu.
func (m *Mempool) processLocked(now uint64) (executable, timedOut []*Operation) {
	window := uint64(m.gapWindow().Seconds())
	for sender, byNonce := range m.pending {
		next := m.nextNonce[sender]
		for {
			op, ok := byNonce[next]
			if !ok {
				break
			}
			executable = append(executable, op)
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
				high := next - 1
				for nonce, op := range byNonce {
					timedOut = append(timedOut, op)
					if nonce > high {
						high = nonce
					}
					delete(byNonce, nonce)
				}
				// The missing nonce and every blocked nonce are consumed once
				// the window elapses: the account starts clean afterwards and
				// a resubmitted blocked op reports an already-used nonce.
				m.nextNonce[sender] = high + 1
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

func sortOps(ops []*Operation) {
	sort.Slice(ops, func(i, j int) bool {
		if ops[i].Sender != ops[j].Sender {
			return ops[i].Sender < ops[j].Sender
		}
		return ops[i].Nonce < ops[j].Nonce
	})
}

// drainExecutable removes and returns, for every sender, the contiguous run of
// operations starting at the sender's next nonce. Gaps stay queued (unless
// their wait window has elapsed, in which case blocked ops time out).
func (m *Mempool) drainExecutable() []*Operation {
	executable, _ := m.Process()
	return executable
}

// markSettled records op IDs so replays are detectable on every entry path.
func (m *Mempool) markSettled(ids ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		m.settled[id] = struct{}{}
	}
}

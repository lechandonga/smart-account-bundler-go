package bundler

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Mempool accepts concurrently submitted operations and guarantees that, per
// account, operations become executable strictly in nonce order regardless of
// submission order.
type Mempool struct {
	mu sync.Mutex
	// pending[sender][nonce] holds validated but not yet executable ops.
	pending map[string]map[uint64]*Operation
	// nextNonce[sender] is the next nonce eligible for execution.
	nextNonce map[string]uint64
	// settled op IDs, for replay protection at submission time.
	settled map[string]struct{}

	now func() uint64
}

func NewMempool() *Mempool {
	return &Mempool{
		pending:   make(map[string]map[uint64]*Operation),
		nextNonce: make(map[string]uint64),
		settled:   make(map[string]struct{}),
		now:       func() uint64 { return uint64(time.Now().Unix()) },
	}
}

// Submit validates an operation and queues it. A rejection here never affects
// any other operation. Distinct RejectReasons distinguish bad signatures,
// expiry and reused nonces.
func (m *Mempool) Submit(op *Operation) (RejectReason, error) {
	if !op.VerifySignature() {
		return ReasonBadSignature, fmt.Errorf("operation %s: signature does not match sender %s", op.ID()[:8], op.Sender[:8])
	}
	if op.Expiry != 0 && m.now() > op.Expiry {
		return ReasonExpired, fmt.Errorf("operation %s: expired at %d", op.ID()[:8], op.Expiry)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.settled[op.ID()]; ok {
		return ReasonDuplicate, fmt.Errorf("operation %s: already settled", op.ID()[:8])
	}
	if op.Nonce < m.nextNonce[op.Sender] {
		return ReasonNonceUsed, fmt.Errorf("operation %s: nonce %d already used (next=%d)", op.ID()[:8], op.Nonce, m.nextNonce[op.Sender])
	}
	if _, ok := m.pending[op.Sender][op.Nonce]; ok {
		return ReasonNonceUsed, fmt.Errorf("operation %s: nonce %d already pending for sender", op.ID()[:8], op.Nonce)
	}
	if m.pending[op.Sender] == nil {
		m.pending[op.Sender] = make(map[uint64]*Operation)
	}
	m.pending[op.Sender][op.Nonce] = op
	return ReasonNone, nil
}

// drainExecutable removes and returns, for every sender, the contiguous run of
// operations starting at the sender's next nonce. Gaps stay queued.
func (m *Mempool) drainExecutable() []*Operation {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Operation
	for sender, byNonce := range m.pending {
		next := m.nextNonce[sender]
		for {
			op, ok := byNonce[next]
			if !ok {
				break
			}
			out = append(out, op)
			delete(byNonce, next)
			next++
		}
		m.nextNonce[sender] = next
	}
	// Deterministic order: by sender, then nonce.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Sender != out[j].Sender {
			return out[i].Sender < out[j].Sender
		}
		return out[i].Nonce < out[j].Nonce
	})
	return out
}

// markSettled records op IDs so replays are detectable.
func (m *Mempool) markSettled(ids ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		m.settled[id] = struct{}{}
	}
}

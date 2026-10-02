package bundler

import "sync"

// Ledger is an in-memory balance sheet simulating on-chain state.
// All mutations happen under a single mutex so concurrent submissions and
// settlement observe a consistent view. The sum of all balances is conserved
// by construction: funds only ever move between ledger accounts.
type Ledger struct {
	mu       sync.Mutex
	balances map[string]int64
}

func NewLedger() *Ledger {
	return &Ledger{balances: make(map[string]int64)}
}

// Mint credits an account; used only to set up initial state (faucet).
func (l *Ledger) Mint(id string, amount int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.balances[id] += amount
}

func (l *Ledger) Balance(id string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.balances[id]
}

// Total returns the sum of every balance; it must remain invariant across
// settlement (minting excluded).
func (l *Ledger) Total() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var sum int64
	for _, b := range l.balances {
		sum += b
	}
	return sum
}

// transfer moves amount between two ledger accounts atomically.
// Callers must hold sufficient funds; checked by the bundler before calling.
func (l *Ledger) transfer(from, to string, amount int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.balances[from] -= amount
	l.balances[to] += amount
}

// canCover reports whether id's balance covers amount.
func (l *Ledger) canCover(id string, amount int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.balances[id] >= amount
}

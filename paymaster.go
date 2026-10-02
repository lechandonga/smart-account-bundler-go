package bundler

import "sync"

// Paymaster holds an independent deposit in the ledger and per-account fee
// allowances. Allowance is bookkeeping, not balance: the deposit is the only
// real funds, so total-value conservation is unaffected by allowance changes.
type Paymaster struct {
	ID string

	mu         sync.Mutex
	allowances map[string]int64 // account address -> remaining allowance
}

func NewPaymaster(id string) *Paymaster {
	return &Paymaster{ID: id, allowances: make(map[string]int64)}
}

// SetAllowance sets the fee allowance granted to an account.
func (p *Paymaster) SetAllowance(account string, amount int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allowances[account] = amount
}

func (p *Paymaster) Allowance(account string) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.allowances[account]
}

// canSponsor reports whether the paymaster can cover fee for the account,
// given its current ledger deposit.
func (p *Paymaster) canSponsor(l *Ledger, account string, fee int64) (RejectReason, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !l.canCover(p.ID, fee) {
		return ReasonPaymasterDepositTooLow, false
	}
	if p.allowances[account] < fee {
		return ReasonPaymasterAllowanceExceed, false
	}
	return ReasonNone, true
}

// charge deducts fee from the deposit and the account allowance. It must only
// be called after canSponsor succeeded; together they form an all-or-nothing
// charge with no partial deduction.
func (p *Paymaster) charge(l *Ledger, bundlerID, account string, fee int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allowances[account] -= fee
	l.transfer(p.ID, bundlerID, fee)
}

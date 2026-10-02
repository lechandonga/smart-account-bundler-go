package bundler

import "fmt"

// RejectReason 以可区分的错误码说明操作被拒绝的原因。
type RejectReason string

const (
	ReasonNone                RejectReason = ""
	ReasonUnknownAccount      RejectReason = "unknown_account"      // 账户未在账本注册
	ReasonBadSignature        RejectReason = "bad_signature"        // 签名与账户公钥不匹配
	ReasonExpired             RejectReason = "operation_expired"    // 超过操作有效期
	ReasonNonceUsed           RejectReason = "nonce_already_used"   // 序号已被已生效操作占用
	ReasonNonceGap            RejectReason = "nonce_out_of_order"   // 序号超前，存在空洞
	ReasonDuplicateMempool    RejectReason = "duplicate_in_mempool" // 同一 (账户,序号) 已在待打包队列
	ReasonFeeTooLow           RejectReason = "fee_too_low"          // 手续费低于打包者定价
	ReasonAccountInsufficient RejectReason = "account_insufficient" // 账户无力预付手续费
	ReasonUnknownSponsor      RejectReason = "unknown_sponsor"      // 指定了未注册的代付方
	ReasonSponsorDepositLow   RejectReason = "sponsor_deposit_low"  // 代付方押金不足以垫付该笔
	ReasonSponsorQuotaExceed  RejectReason = "sponsor_quota_exceed" // 超出代付方对该账户的额度
	ReasonAlreadySettled      RejectReason = "already_settled"      // 操作已完成结算，拒绝重放
)

// RejectedOpError 携带结构化拒绝原因，便于调用方精确判断。
type RejectedOpError struct {
	Sender Address
	Nonce  uint64
	Reason RejectReason
	Detail string
}

func (e *RejectedOpError) Error() string {
	return fmt.Sprintf("operation sender=%s nonce=%d rejected: %s (%s)",
		e.Sender, e.Nonce, e.Reason, e.Detail)
}

func newRejectError(op UserOperation, reason RejectReason, detail string) *RejectedOpError {
	return &RejectedOpError{Sender: op.Sender, Nonce: op.Nonce, Reason: reason, Detail: detail}
}

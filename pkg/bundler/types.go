package bundler

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// Address 是智能账户、代付方或打包者在内存账本中的标识。
// 对智能账户而言，地址由其公钥的 SHA-256 摘要前 20 字节派生。
type Address [20]byte

func (a Address) String() string {
	return fmt.Sprintf("0x%x", [20]byte(a))
}

func (a Address) IsZero() bool {
	return a == Address{}
}

// AddressFromKey 由 ed25519 公钥派生账户地址。
func AddressFromKey(pub ed25519.PublicKey) Address {
	sum := sha256.Sum256(pub)
	var addr Address
	copy(addr[:], sum[:20])
	return addr
}

// UserOperation 表示智能账户提交的一笔操作。
type UserOperation struct {
	Sender   Address // 发起操作的智能账户
	Nonce    uint64  // 账户内严格递增的序号
	To       Address // 操作执行时转账的目标账户（可为零地址，表示纯手续费操作）
	Value    uint64  // 执行时从 Sender 余额转出的金额
	MaxFee   uint64  // 该操作愿意支付的最大手续费
	Deadline int64   // 有效期截止时间（UNIX 秒），包含该时刻
	Sponsor  Address // 代付方地址；为零地址时由账户自付手续费

	// Signature 由 SignOperation 生成，对操作中除签名外的所有字段进行签名。
	Signature []byte
}

// SigningDigest 返回操作的签名摘要。
func SigningDigest(op UserOperation) []byte {
	buf := make([]byte, 0, 20*3+8*4)
	buf = append(buf, op.Sender[:]...)
	buf = binary.BigEndian.AppendUint64(buf, op.Nonce)
	buf = append(buf, op.To[:]...)
	buf = binary.BigEndian.AppendUint64(buf, op.Value)
	buf = binary.BigEndian.AppendUint64(buf, op.MaxFee)
	buf = binary.BigEndian.AppendUint64(buf, uint64(op.Deadline))
	buf = append(buf, op.Sponsor[:]...)
	sum := sha256.Sum256(buf)
	return sum[:]
}

// SignOperation 使用账户私钥对操作进行签名，并把签名写回 op。
func SignOperation(op *UserOperation, priv ed25519.PrivateKey) {
	digest := SigningDigest(*op)
	op.Signature = ed25519.Sign(priv, digest)
}

// OpResult 表示一笔操作在提交校验阶段的结论。
type OpResult struct {
	OpHash   [32]byte
	Accepted bool
	Reason   RejectReason // 仅 Accepted == false 时有意义
	Index    int          // 在提交批次中的下标，便于调用方对应
}

// OpReceipt 表示一笔操作在打包结算阶段的执行回执。
type OpReceipt struct {
	OpHash    [32]byte
	Sender    Address
	Payer     Address // 实际承担手续费的一方（账户自己或代付方）
	Nonce     uint64
	Executed  bool   // 操作主体是否成功执行（Value 转账是否发生）
	Reverted  bool   // 执行阶段是否失败（余额不足等）
	FeePaid   uint64 // 实际支付给打包者的手续费
	FeeBurden string // 手续费承担方的可解释说明
	Skipped   bool   // 是否因为重复打包而跳过
	Status    string // 面向日志的最终状态描述
}

// BatchReceipt 是一次打包结算的汇总回执，含余额守恒快照。
type BatchReceipt struct {
	BatchID     string
	Receipts    []OpReceipt
	TotalFee    uint64
	TotalBefore uint64
	TotalAfter  uint64
	Conserved   bool
}

// opKey 用于按 (Sender, Nonce) 唯一标识操作。
type opKey struct {
	sender Address
	nonce  uint64
}

// key 按 (Sender, Nonce) 唯一标识操作。
func (op UserOperation) key() opKey {
	return opKey{sender: op.Sender, nonce: op.Nonce}
}

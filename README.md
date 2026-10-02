# Smart Account Bundler（Go 本地内存版）

用内存账本模拟链上状态，实现智能账户操作的**并发提交校验 → 代付垫付 → 打包结算**全流程。
无需任何外部节点或数据库，所有状态保存在单个 `Bundler` 实例中，受互斥锁保护，
可在 `-race` 竞态检测下稳定运行。

## 目录结构

- `pkg/bundler/types.go`：`Address`、`UserOperation`、回执等核心类型与 ed25519 签名。
- `pkg/bundler/errors.go`：可区分的结构化拒绝原因 `RejectReason`。
- `pkg/bundler/bundler.go`：账本、代付方、mempool、提交校验与确定性结算。
- `cmd/demo/main.go`：端到端最小示例。

## 账户与操作

- 账户地址 = 账户 ed25519 公钥的 SHA-256 前 20 字节；签名覆盖
  `Sender/Nonce/To/Value/MaxFee/Deadline/Sponsor` 全部字段。
- `Nonce` 是账户内严格递增的序号，由 `(Sender, Nonce)` 唯一标识一笔操作。
- 手续费为打包者统一定价 `feePrice`，操作 `MaxFee` 不得低于该价格。

## 操作状态流转

```
                +-- bad_signature / operation_expired / unknown_account
                |
  Submit -----> 校验（账户、有效期、签名）
                |
                +-- nonce_already_used（序号已在链上消耗）
                |   duplicate_in_mempool（同序号已在队列）
                |   nonce_out_of_order（序号超前形成空洞，等待超时）
                |   fee_too_low / account_insufficient
                |   unknown_sponsor / sponsor_deposit_low / sponsor_quota_exceed
                v
           MEMPOOL（待打包，已预留手续费/额度）
                |
            SettleBatch
                v
     +----------+-----------+
     |                      |
  手续费垫付成功          结算时垫付条件不满足
  （原子扣取，不可回滚）     => 整笔拒绝：零扣减、序号消耗
     |
  执行操作主体（Value 转账）
     |
  +-- 成功 executed
  +-- 失败 reverted（余额不足/目标不存在），手续费仍归打包者
```

**并发语义**：同一账户的操作即使并发提交也必须按序号依次进入 mempool。
超前序号会在条件变量上等待前序操作；超过 `nonceWait`（默认 3s，测试中调短）
仍有空洞则返回 `nonce_out_of_order`。单笔操作被拒绝只影响自身，同批其它操作照常受理。

## 手续费与代付规则

- **自付**：`Sponsor` 为零地址时从账户余额扣手续费；提交时要求
  `余额 - 队列中已预留手续费 >= feePrice`。
- **代付**：代付方拥有独立 `Deposit` 押金，并按账户配置累计 `Quota`。
  受理条件（均为只读校验，**提交阶段不扣任何钱**）：
  - 押金扣除已预留部分后 >= `feePrice`，否则 `sponsor_deposit_low`；
  - `该账户已结算垫付 + 队列中预留 + feePrice <= Quota`，否则
    `sponsor_quota_exceed`；
  - 代付方未注册则 `unknown_sponsor`。
- 任一条件不满足都**整笔拒绝**，账户、代付方、打包者三方余额均无部分扣减。

## 打包结算与失败语义

- `SettleBatch(batchID, ops)` 对入参做**确定性排序**：先按 `Sender` 地址升序，
  同账户按 `Nonce` 升序；调用方传入顺序不影响结果。
- 每笔操作两步处理：
  1. **原子垫付手续费**：成功后手续费立即计入打包者收益，不可回滚，序号生效，
     操作离开 mempool；若结算瞬间条件不满足（如押金临界被占用），整笔拒绝：
     `FeePaid=0`、任何余额不变、序号消耗（避免后续操作永久卡死）。
  2. **执行主体**：`Value` 转账失败只回滚主体，不回滚手续费。回执
     `reverted=true` 并明确写明 `fee kept by bundler <地址>`。
- **余额守恒**：每个 `BatchReceipt` 都带结算前后
  （全部账户 + 全部代付方押金 + 打包者收益）总量及 `Conserved` 标志。
- **幂等/防重放**：
  - 相同 `batchID` 直接返回首次回执缓存，不重复结算；
  - 批内重复的同一 `(Sender,Nonce)` 标记 `skipped: duplicate within batch`；
  - 已结算或已拒绝操作在新批次中出现，标记 `skipped: ... no charge`，零手续费。

## 拒绝原因速查

| Reason | 触发条件 |
| --- | --- |
| `unknown_account` | 账户未注册 |
| `bad_signature` | 签名与账户公钥不匹配 |
| `operation_expired` | 当前时间晚于 `Deadline` |
| `nonce_already_used` | 序号已在链上消耗（重放） |
| `nonce_out_of_order` | 序号超前且等待前序超时 |
| `duplicate_in_mempool` | 同序号操作已在待打包队列 |
| `fee_too_low` | `MaxFee < feePrice` |
| `account_insufficient` | 账户余额（含预留）不足垫付 |
| `unknown_sponsor` | 代付方未注册 |
| `sponsor_deposit_low` | 代付方押金（含预留）不足 |
| `sponsor_quota_exceed` | 超出该账户代付累计额度 |

拒绝均返回 `*RejectedOpError`，可通过 `errors.As` 取到结构化 `Reason` 与明细。

## 本地验证

```bash
# 常规测试（日志展示每个用例覆盖的业务与判定依据）
go test -v ./...

# 竞态检测（并发用例在该模式下稳定通过；需要 CGO）
CGO_ENABLED=1 go test -race -v ./...

# 端到端示例：自付/代付、确定排序、守恒、批次重放
go run ./cmd/demo
```

测试覆盖场景：

- 同一账户 30 个 goroutine 并发提交乱序 nonce、已用序号重放、序号空洞；
- 签名错误、过期、账户余额不足及单笔隔离；
- 代付额度边界（押金够但额度耗尽）、押金不足整笔拒绝；
- 结算余额守恒、执行阶段部分失败时手续费归属、确定性排序；
- 批次幂等与操作级重放不重复扣费；
- 结算临界时代付押金被抽空的原子拒绝。

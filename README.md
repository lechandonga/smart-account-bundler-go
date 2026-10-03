# smart-account-bundler-go

本地运行的智能账户打包与代付流程模拟。用内存账本（`Ledger`）模拟链上状态，
覆盖操作提交、签名校验、代付（Paymaster）、打包结算的完整链路，保证并发
提交下结论一致且可解释。

## 核心概念

| 组件 | 文件 | 职责 |
| --- | --- | --- |
| `Account` | `account.go` | 本地生成 ed25519 密钥对，地址即公钥 hex，负责签名操作 |
| `Operation` | `operation.go` | 用户操作：发送方、序号（nonce）、转账、手续费、代付方、有效期、签名 |
| `Ledger` | `ledger.go` | 内存账本，单互斥锁保护，余额只在账户间移动，总额守恒 |
| `Mempool` | `mempool.go` | 接收并发提交，按账户保证操作严格按 nonce 顺序生效 |
| `Paymaster` | `paymaster.go` | 独立押金 + 按账户额度，代付手续费 |
| `Bundler` | `bundler.go` | 收集可执行操作，按确定顺序结算，发放回执 |

## 操作状态流转

```
Submit / SettleBatch(外部直送)      CollectAndSettle / SettleBatch
  │  (两条入口共用同一套准入 Admit)          │
  ├─ 签名校验失败 ──────> 拒绝 (BAD_SIGNATURE)
  ├─ 超过有效期 ────────> 拒绝 (EXPIRED)
  ├─ nonce 已生效/已挂起 > 拒绝 (NONCE_ALREADY_USED，同序号不同内容同因)
  ├─ 已结算内容重发 ────> 拒绝 (DUPLICATE_OPERATION)
  ├─ 每账户挂起超限 ────> 拒绝 (ACCOUNT_PENDING_LIMIT_EXCEEDED)
  ├─ 全池挂起超限 ──────> 拒绝 (GLOBAL_PENDING_LIMIT_EXCEEDED)
  └─ 通过 ──> PENDING（按账户 nonce 排队，允许乱序提交、顺序生效）
                 │
        序号出现空洞?
         ├─ 窗口内补齐 ─> 按序可执行
         └─ 窗口超时 ───> REJECTED (NONCE_GAP_TIMEOUT, 零扣费, 容量释放)
                          │
              结算时（按 sender, nonce 确定顺序）
                          │
              ├─ 手续费无法支付 ──> REJECTED
              │     (INSUFFICIENT_FUNDS /
              │      PAYMASTER_DEPOSIT_TOO_LOW /
              │      PAYMASTER_ALLOWANCE_EXCEEDED)
              ├─ 执行失败 ──────> FAILED_EXECUTION（手续费仍归打包者）
              └─ 成功 ─────────> SUCCESS

  重复打包同一操作 ──> REPLAYED_NOOP（不重复扣费，原始回执保留）
```

所有拒绝原因都是可区分的 `RejectReason` 枚举；单笔操作被拒绝不影响同批
其他操作。

## 手续费与代付规则

- 无代付：手续费从发送方余额扣给打包者；余额不足则整笔拒绝
  （`INSUFFICIENT_FUNDS`），无任何扣减。
- 有代付：代付方需在账本中有押金，并为账户设置额度
  （`SetAllowance`）。结算时同时检查两者：
  - 押金 < 手续费 → `PAYMASTER_DEPOSIT_TOO_LOW`
  - 剩余额度 < 手续费 → `PAYMASTER_ALLOWANCE_EXCEEDED`
  - 任一不满足即**整笔原子拒绝**：账户、代付方、打包者余额均不变。
- 检查通过后一次性扣减：押金 → 打包者，额度同步减少。额度是记账数据，
  不是余额，因此不影响总额守恒。

## 打包结算的失败语义

- **确定顺序**：每轮结算按 `(sender, nonce)` 排序，结果可复现。
- **总额守恒**：结算只在账户间移动资金，所有余额之和不变（测试中
  `assertConserved` 逐用例校验）。
- **幂等**：同一批操作重复打包/重放，返回 `REPLAYED_NOOP`，不重复扣费；
  已结算操作再次提交返回 `DUPLICATE_OPERATION`。
- **部分失败**：某笔操作执行失败（`Fail` 或转账余额不足）时，该笔手续费
  **不退还，归打包者**（作为尝试执行的补偿），转账不生效；其余操作不受
  影响，按既定顺序完成结算。手续费归属在回执 `Detail` 中明确说明。

## 新增能力

### 1. 统一批次准入（排队路径与外部直送同一套规则）

批量调度器可以不经排队、用 `Bundler.SettleBatch(ops)` 直接把整批操作送来
结算。该入口与排队入口 `Mempool.Submit` 共用同一份准入逻辑
`Mempool.Admit`，两条路径对同样输入的结论与账目完全一致（由
`TestAdmissionPathEquivalence` 守护）。准入检查顺序为：

1. 签名必须能用发送方公钥验过（`BAD_SIGNATURE`）；
2. 未超过 `Expiry`（`EXPIRED`，0 表示永不过期）；
3. 操作内容（其确定性 `ID()`）未结算过（`DUPLICATE_OPERATION`）；
4. 该账户该序号尚未生效、也未挂起（`NONCE_ALREADY_USED`）——**同一序号
   即使内容不同也一律挡住**，因为序号历史以账户为准；
5. 未超过挂起容量上限（见第 3 节）。

语义边界：

- 外部直送批次里从未进过排队的操作，签名、有效期、序号历史同样逐项查验，
  来源不同不放宽。
- 每个输入操作都按输入顺序得到一个 `BatchItemResult`；被挡操作立即获得
  **可查询的终态回执**（`Bundler.Receipt(id)`），原因彼此可区分。
- 单笔被挡不连累同批其他操作；被挡操作不产生账户、代付方、打包者任何一方
  的余额变动，也不占用挂起容量、不改动既有队列。
- 准入通过的操作进入池子，随后与既有挂起操作一起按 `(sender, nonce)`
  确定顺序结算；仍被空洞挡住的在等待窗口内返回 `PENDING`（非终态）。

### 2. 空洞序号的等待与超时归宿

通过 `Config.GapWait` 配置每个账户缺序号时的等待窗口（默认 0 = 无限等待，
保持原有行为）。规则：

- 窗口内缺失的序号补上后，后续操作严格按序号依次生效。
- 等待**只影响出现空洞的那个账户**：其他账户的可执行操作照常结算，不被
  拖住（结算仍按 sender 分组推进）。
- 窗口到点仍未补上，卡在空洞后的全部挂起操作在下次结算/收集时离开队列，
  获得终态 `REJECTED / NONCE_GAP_TIMEOUT`（新原因，与既有原因可区分），
  回执可通过 `Bundler.Receipt(id)` 查询，且**不产生任何
  扣费**，占用的挂起容量立即释放。
- 超时跳过后，账户的下一个期望序号前移；缺失序号即使迟到也返回
  `NONCE_ALREADY_USED`，空洞不可被"复活"，同一序号不可能生效两次。
- 相同输入重复运行结果一致（确定性由用例双跑校验）。

时间源可通过测试辅助方法注入（固定时钟），生产环境默认使用墙钟时间。

### 3. 挂起容量上限

`Config.MaxPendingPerAccount`（每账户，0=不限）与
`Config.MaxPendingGlobal`（全池，0=不限）控制挂起操作数：

- 超每账户上限 → `ACCOUNT_PENDING_LIMIT_EXCEEDED`；超全池上限 →
  `GLOBAL_PENDING_LIMIT_EXCEEDED`。两个原因互相区分，也与全部既有原因
  不同。
- 配额在池子的同一把互斥锁内完成"检查 + 占槽"，高并发同时抢配额时
  **不会超额挂起**；被拒不挤掉、顶替或改动任何已在队列中的操作，也不
  扣费。
- 操作结算成功或空洞超时离开队列后容量立即归还。

配置示例：

```go
pool := bundler.NewMempoolWithConfig(bundler.Config{
    GapWait:              30 * time.Second,
    MaxPendingPerAccount: 64,
    MaxPendingGlobal:     10_000,
})
b := bundler.NewBundler("bundler-1", ledger, pool)
results := b.SettleBatch(externalBatch) // 外部调度器直送整批
```

### 保持不变的既有行为

- **确定顺序**：每轮结算仍按 `(sender, nonce)` 排序，结果可复现。
- **总额守恒**：资金只在账户间移动，准入拒绝与空洞超时一分钱不动。
- **单笔被拒不影响同批**：无论排队批还是外部直送批，逐操作独立结论。
- **失败操作手续费归属**：执行失败（`Fail` 或转账余额不足）的手续费仍
  归打包者、转账不生效；准入阶段/空洞超时的被挡操作则根本不收取手续费。
- **既有拒绝原因语义不变**：`BAD_SIGNATURE`、`EXPIRED`、
  `NONCE_ALREADY_USED`、`NONCE_GAP`（仅等待中的非终态标记）、
  `DUPLICATE_OPERATION`、`INSUFFICIENT_FUNDS`、
  `PAYMASTER_DEPOSIT_TOO_LOW`、`PAYMASTER_ALLOWANCE_EXCEEDED`；
  `REPLAYED_NOOP` 仍只代表已结算内容的重放，不重复扣费。
- 原有 API（`NewMempool`、`Submit`、`CollectAndSettle`、`Settle`）签名与
  行为保持兼容；不传配置时默认无限等待、容量不限。

## 本地验证

```bash
# 全部测试（含竞态检测，并发用例必须稳定通过）
CGO_ENABLED=1 go test -race -v ./...

# 静态检查
go vet ./...
```

测试日志会打印每个用例覆盖的业务与判定依据，例如：

```
业务覆盖: 同一账户并发提交, 操作必须按序号依次生效
判定依据: 50 笔乱序并发提交全部成功, 收款方=500, 账户扣款含手续费=550, 打包者手续费=50
判定依据: 账本总额守恒 before=1000000 after=1000000
```

覆盖场景：

- `TestConcurrentSubmissionSameAccount` — 同一账户 50 笔乱序并发提交，严格按 nonce 生效
- `TestSubmitRejections` — 签名错误 / 过期 / nonce 重用的可区分拒绝，且不影响同批合法操作
- `TestPaymasterAllowanceBoundary` — 额度恰好等于手续费成功、超额与押金不足整笔原子拒绝
- `TestReplayBundle` — 重复打包与重放不重复扣费
- `TestPartialExecutionFailure` — 中间一笔执行失败，其余正常结算，失败手续费归打包者
- `TestConcurrentMultiAccount` — 8 账户并发提交，总额守恒
- `TestDirectBatchAdmission` — 外部直送批：从未排队的伪造签名、过期、同序号不同内容、已结算内容重复，逐种可区分拒绝且零账目影响
- `TestAdmissionPathEquivalence` — 排队路径与直送路径对同一输入序列拒绝结论与终态账目完全一致
- `TestNonceGapFillAndTimeout` — 空洞窗口内补齐按序生效；超时返回 `NONCE_GAP_TIMEOUT` 终态且零扣费；等待不拖累其他账户；双跑确定性一致
- `TestPendingCapacityLimits` — 每账户/全池配额在 50、100 个 goroutine 争抢下连续多轮无超额、无丢失、无扣费，两种容量原因可区分

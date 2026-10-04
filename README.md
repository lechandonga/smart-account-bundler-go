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
Submit                  CollectAndSettle
  │                           │
  ├─ 签名校验失败 ──> 拒绝 (BAD_SIGNATURE)
  ├─ 超过有效期 ────> 拒绝 (EXPIRED)
  ├─ nonce 已用/同序号不同内容 ──> 拒绝 (NONCE_ALREADY_USED)
  ├─ 已结算内容重发 ────────────> 提交入口拒绝 (DUPLICATE_OPERATION)，
  │                               结算入口返回 REPLAYED_NOOP（不扣费）
  ├─ 挂起容量超限 ──> 拒绝 (PENDING_CAP_PER_ACCOUNT / PENDING_CAP_GLOBAL)
  └─ 通过 ──> PENDING（按账户 nonce 排队，允许乱序提交、顺序生效）

  nonce 超前（中间缺号）──> 不拒绝，挂起等待缺失序号（等待窗口可配置）
              ├─ 窗口内补齐 ──> 按序号依次生效
              └─ 窗口到期未补 ──> TIMED_OUT_NONCE_GAP（可区分的超时终态，
                                  不收费、不动账；序号未消费，补号后可重试，
                                  但原超时内容重放仍是 DUPLICATE_OPERATION）
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

## 两条入口的统一准入

系统有两条操作入口：排队提交 `Mempool.Submit`（随后由
`CollectAndSettle` 结算）与外部调度器直送批次 `Bundler.SettleDirect`。
两者共用同一套准入规则（`Mempool.Admit`），同一输入无论走哪条路结论一致：

1. 已终态内容判定（`settled`）：已结算内容从提交入口进入为
   `DUPLICATE_OPERATION`；从结算入口重送为 `REPLAYED_NOOP`，均不动账。
2. 序号历史：小于账户下一个应生效序号 → `NONCE_ALREADY_USED`；同一序号槽
   已被不同内容占用（挂起中或已生效）→ `NONCE_ALREADY_USED`；同序号同内容
   则幂等收敛到同一条挂起记录，不新建记录、不重复生效。
3. 序号超前（缺号）：两条入口都**不**当场拒绝，统一挂起进入等待窗口；直送
   批次中从未进过队列的操作也接受完全相同的签名、有效期、序号检查。
4. 内容校验（签名、有效期）在序号槽判定通过后执行：一个缺号但签名伪造或
   已过期的操作会在准入时被拒，不会因后来缺口补齐而被“补结算”。
5. 挂起容量（每账户 `MaxPendingPerAccount`、全局 `MaxPendingTotal`，0 为
   不限）在准入最后检查；容量满返回可区分的容量原因。

**终态唯一**：每个操作内容 ID 最多有一张终态回执，成功 / 失败 / 拒绝 /
超时一旦确定，任何后来的结果都不会覆盖（回执按“首终态生效”记录）。已挂起
的操作若随外部批次补齐并结算成功，队列中的挂起记录同步收敛移除，窗口过后
不会再产生超时回执。结算在全局串行（`settleMu`）且 Drain→结算→Commit
期间把内容 ID 标记为 in-flight，并发提交与结算交错时同一账户同一序号最多
生效一次，账目不错乱。

直送批次按 `(sender, nonce)` 稳定排序后逐笔准入与结算；某笔被拒不引起
账户、代付方、打包者任何一方余额变动，也不连累同批其它操作；批次中因缺号
暂时挂起的操作随后通过任一入口补齐或超时。

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
- `TestDirectBatchSignatureAndNonceBoundaries` — 直送批次：从未入队的伪造
  签名被拒、同序号已生效后不同内容两入口统一拒绝、同内容重放不重复扣费
- `TestDirectBatchRejectionIsolation` — 同一外部批次内伪造签名 / 同序号
  冲突被拒不动账，合法操作照常结算
- `TestDirectGapWaitsLikeQueue` — 直送缺号与排队缺号同规则：窗口内补齐后
  按序号依次生效，跨入口重复进入收敛为同一记录
- `TestGapTimeoutTerminal` — 缺号等待窗口到期给出 `TIMED_OUT_NONCE_GAP`
  可区分终态，无费用无余额变动，序号未消费、内容不可重放
- `TestPendingSuspensionConverges` — 挂起记录跨入口结算后收敛，不再产生
  覆盖成功结果的超时回执
- `TestConcurrentPendingCapRace` — 4 账户 × 每账户 6 笔从两条入口并发抢
  挂起槽（每账户 3 / 全局 6），连续 20 轮竞态下上限不突破、无重复生效

建议在改动后用竞态检测连续多轮运行（并发用例设计为多轮稳定通过）：

```bash
CGO_ENABLED=1 go test -race -count=20 -v ./...
```

## 保持不变的既有行为

- **确定顺序**：每轮结算按 `(sender, nonce)` 顺序，结果可复现。
- **总额守恒**：资金只在账本账户间移动，所有余额之和不变（逐用例
  `assertConserved` 校验）；超时与拒绝均无任何余额变动。
- **单笔被拒不影响同批**：准入与结算逐笔进行，拒绝只针对该笔。
- **失败操作手续费归属**：执行阶段失败（`Fail` 或转账余额不足）的手续费
  不退还、归打包者，转账不生效。
- **既有拒绝原因语义不变**：`BAD_SIGNATURE`、`EXPIRED`、
  `NONCE_ALREADY_USED`、`DUPLICATE_OPERATION`、`INSUFFICIENT_FUNDS`、
  `PAYMASTER_DEPOSIT_TOO_LOW`、`PAYMASTER_ALLOWANCE_EXCEEDED` 语义保持；
  新增的 `NONCE_GAP_TIMEOUT` 只用于缺号等待超时终态。

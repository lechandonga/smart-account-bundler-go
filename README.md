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
  ├─ nonce 已用 ────> 拒绝 (NONCE_ALREADY_USED)
  ├─ 已结算重发 ────> 拒绝 (DUPLICATE_OPERATION)
  ├─ 挂起容量超限 ──> 拒绝 (PENDING_CAP_PER_ACCOUNT / PENDING_CAP_GLOBAL)
  └─ 通过 ──> PENDING（按账户 nonce 排队，允许乱序提交、顺序生效）
                          │
              结算时（按 sender, nonce 确定顺序）
                          │
              ├─ 手续费无法支付 ──> REJECTED
              │     (INSUFFICIENT_FUNDS /
              │      PAYMASTER_DEPOSIT_TOO_LOW /
              │      PAYMASTER_ALLOWANCE_EXCEEDED)
              ├─ 执行失败 ──────> FAILED_EXECUTION（手续费仍归打包者）
              └─ 成功 ─────────> SUCCESS

  缺号等待窗口耗尽 ──> TIMED_OUT_NONCE_GAP (NONCE_GAP_TIMEOUT)
  重复打包同一操作 ──> REPLAYED_NOOP（不重复扣费，原始回执保留）
```

所有拒绝原因都是可区分的 `RejectReason` 枚举；单笔操作被拒绝不影响同批
其他操作。

## 统一批次准入（排队提交与外部直送）

两条入口共用同一套准入规则：外部调度器直送的批次
（`Bundler.SettleDirect`）内部逐笔走与 `Mempool.Submit` 完全相同的
校验，同一个输入不管从哪条路进来，签名、有效期、序号历史、已结算
重放、挂起容量的结论都一致；从没进过队列的操作一样查到位。

- 直送批次按 `(sender, nonce)` 确定顺序处理，逐笔准入、逐笔结算；
  单笔被拒不移动任何余额，也不影响同批其它操作。
- 批次内同一操作重复出现只处理一次；同序号不同内容按
  `NONCE_ALREADY_USED` 拒绝（与排队入口结论相同）。
- 已挂起的同一操作（内容完全相同）再次送达是幂等空操作，不会重复
  生效；同账户同序号最多生效一次。
- 仍在等待缺号的操作在批次回执中报告 `PENDING`（非终态），终态回执
  由后续结算轮次给出；终态一旦确定（成功 / 失败 / 拒绝 / 超时）就不再
  被后续结果覆盖，挂起记录随结算收敛，不会同一序号既生效又超时。

## 序号空档的等待与超时

- 序号超前（中间缺号）的操作不会被当场拒绝，而是挂起等待缺失的序号，
  等待窗口由 `Mempool.GapWaitWindow` 配置（零值用
  `DefaultGapWaitWindow`，30 秒）；两个入口行为一致。
- 窗口内补上缺失序号：该账户的操作按序号依次生效，与提交顺序无关。
- 窗口耗尽仍未补上：被卡住的操作进入可区分终态
  `TIMED_OUT_NONCE_GAP`（原因 `NONCE_GAP_TIMEOUT`），不收取任何
  手续费、不移动任何余额；缺口及被卡序号同时被消费，之后迟到的操作
  在两个入口都报 `NONCE_ALREADY_USED`（同内容重发报
  `DUPLICATE_OPERATION`）。
- 一个账户的缺号只影响本账户，不阻塞其它账户。

## 挂起容量上限

- `Mempool.MaxPendingPerAccount` 限制单账户同时挂起的操作数，
  `Mempool.MaxPendingTotal` 限制全局挂起总数；零值表示不限制。
- 超限分别报 `PENDING_CAP_PER_ACCOUNT` / `PENDING_CAP_GLOBAL`，
  可区分；只有真正会被挂起的（缺号等待的）操作才占用容量，可立即
  执行的操作不受上限约束。
- 容量随结算 / 超时释放；并发抢占时上限不会被突破（竞态检测下的
  多轮并发用例覆盖）。

## 保持不变的既有行为

- 每轮结算按 `(sender, nonce)` 确定顺序，结果可复现。
- 总额守恒：资金只在账户间移动，所有余额之和不变。
- 单笔被拒（无论准入拒绝还是结算拒绝）不影响同批其它操作。
- 执行失败的操作手续费不退还、归打包者；转账不生效。
- 既有拒绝原因语义不变：`BAD_SIGNATURE` / `EXPIRED` /
  `NONCE_ALREADY_USED` / `DUPLICATE_OPERATION` /
  `INSUFFICIENT_FUNDS` / `PAYMASTER_DEPOSIT_TOO_LOW` /
  `PAYMASTER_ALLOWANCE_EXCEEDED`；重放已结算批次仍返回
  `REPLAYED_NOOP`，不重复扣费。

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

# 并发用例在竞态检测下连续多轮
CGO_ENABLED=1 go test -race -count=5 ./...

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
- `TestDirectBatchAdmissionBoundaries` — 外部直送批次与排队入口同一套准入：伪造签名（从未进队列）、同序号不同内容的拒绝结论一致，拒绝不动余额、不连累同批
- `TestDirectBatchGapWaitsLikeQueue` — 直送批次缺号按窗口等待，补齐后按序号依次生效
- `TestNonceGapTimeout` — 等待窗口耗尽给出 `TIMED_OUT_NONCE_GAP`，无余额变动，迟到操作报序号已用
- `TestCrossEntrySingleTerminalState` — 队列挂起与直送交叉时同序号最多生效一次，终态唯一不被超时覆盖
- `TestConcurrentPendingCapacity` — 并发抢占挂起容量上限（单账户 / 全局），超限原因可区分，容量随结算释放

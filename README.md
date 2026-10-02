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

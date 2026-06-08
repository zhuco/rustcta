# Active Runtime Gateway 缺口审计（任务 3）

状态日期：2026-06-08

本审计只覆盖 `docs/full_workspace_migration_execution_plan_zh.md` 的开发任务 3。
结论用于任务 7、9、10、13 继续迁移 active runtime，不在本任务中重写 gateway、
扩张交易所 endpoint，或让 strategy crate 直接依赖 gateway adapter。

## 任务 3 范围

允许范围：

- `crates/rustcta-exchange-api`
- `crates/rustcta-exchange-gateway`
- `apps/gateway`
- `retired exchange tree`
- live strategy、canary、audit 中的 exchange 调用点
- `docs/交易所网关/*`

禁止事项：

- 不重写 gateway。
- 不新增 active runtime 未使用的交易所 endpoint。
- 不让 strategy crate 直接 import gateway adapters。
- 不修改共享文件：root `Cargo.toml`、`Cargo.lock`、`src/lib.rs`、CI、
  `docs/README.md`、`scripts/check_industrial_boundaries.sh`。

## Active runtime 能力矩阵

| 能力 | Gateway typed surface | Adapter dispatcher | 结论 |
| --- | --- | --- | --- |
| 行情订阅 | `SubscribeBooks` / `subscribe_public_stream` | 已按 symbol exchange 分发 | 可承接 public book stream。 |
| Order book snapshot | `GetOrderBook` / `get_order_book` | 已按 `SymbolScope.exchange` 分发 | 可承接 REST/snapshot resync。 |
| Symbol rules | `GetSymbolRules` / `get_symbol_rules` | 已按首个 symbol exchange 分发 | 可承接 tick/lot/min-notional readback。 |
| 余额 | `GetBalances` / `get_balances` | 已按 request exchange 分发 | 可承接 account snapshot。 |
| 持仓 | `GetPositions` / `get_positions` | 已按 request exchange 分发 | 可承接 perp/spot position snapshot。 |
| 手续费 | `GetFees` / `get_fees` | 已按首个 symbol exchange 分发 | 可承接 fee readback。 |
| 下单 | `PlaceOrder` / `place_order` | 已按 symbol exchange 分发 | 可承接 router 下单入口。 |
| 撤单 | `CancelOrder` / `cancel_order` | 已按 symbol exchange 分发 | 可承接 router 撤单入口。 |
| 批量下单 | `BatchPlaceOrders` / `batch_place_orders` | 已按 request exchange 分发 | 可承接 batch 或 composed batch planner。 |
| 批量撤单 | `BatchCancelOrders` / `batch_cancel_orders` | 已按 request exchange 分发 | 可承接 batch cancel。 |
| Cancel-all | `CancelAllOrders` / `cancel_all_orders` | 已按 request exchange 分发 | 可承接 operator/canary cleanup。 |
| 订单查询 | `QueryOrder` / `query_order` | 已按 symbol exchange 分发 | 可承接 reconciliation。 |
| Open orders | `GetOpenOrders` / `get_open_orders` | 已按 request exchange 分发 | 可承接 recovery/audit。 |
| Recent fills | `GetRecentFills` / `get_recent_fills` | 已按 request exchange 分发 | 可承接 fill reconciliation。 |
| 私有流 | `SubscribePrivate` / `subscribe_private_stream` | 已按 request exchange 分发；空订阅现在拒绝 | 可承接 orders/fills/balances/positions stream readiness。 |

最小代码补齐：

- `AdapterBackedGateway` 对 `SubscribePrivate` 的空订阅请求增加拒绝，避免返回
  “成功但无订阅”的假阳性。
- 新增 `task3_active_runtime_surface` focused tests，使用 paper adapter 在不触网的
  typed gateway harness 下覆盖 active runtime 核心能力。

## 仍由后续任务处理的输入

- `retired root bin directory/bitget_order_canary.rs`、`retired root bin directory/exchange_order_canary.rs`、
  `retired root bin directory/hyperliquid_self_test.rs`、`retired root bin directory/cross_arb_order_admin.rs` 等 root bin
  仍直接构造 legacy exchange/trading adapter。按计划应交给任务 13 和任务 7 收口。
- `retired root bin directory/cross_arb_observe.rs`、`retired root bin directory/cross_arb_server/ws.rs` 仍有 legacy market
  adapter 调用点。按计划应交给任务 10/12 收口。
- `retired root bin directory/control_api.rs` 仍读取 legacy exchange registry 和 unified order DTO。
  按计划应交给任务 6/12 收口。
- `retired strategy tree/poisson_market_maker` 仍有 legacy concrete exchange downcast 和
  cancel/order 调用点。按计划应交给任务 11 收口或退役。
- `retired exchange tree/registry.rs` 仍是 legacy credential/runtime 构造入口。任务 3 不迁
  凭证所有权；该风险应由 execution/router、strategy runtime 和 tools 迁移任务消除。

## Focused validation

本任务验收不运行 `cargo build`。建议命令：

```bash
cargo test -p rustcta-exchange-api
cargo test -p rustcta-exchange-gateway
cargo check -p rustcta-gateway
scripts/check_industrial_boundaries.sh
```

# 跨所交易 API 审计与滑点捕获开仓模块设计

日期：2026-06-10

## 目标

本设计把交易所差异收敛到 `rustcta-exchange-api::ExchangeAdapter`，策略层只消费统一后的下单、撤单、成交、仓位和恢复语义。`cross_exchange_arbitrage` 新增 `slippage_capture` 开仓模块，与原 `dual_taker` 并行，运行时通过 `execution_module` 二选一。

## 2026-06-10 交易所 API 收口状态

本轮收口把跨所现货、跨所合约和资金费率套利需要的共享 API 面统一到 `rustcta-exchange-api` 与 `rustcta-exchange-gateway`。策略侧不再依赖交易所私有 funding/readback 模型，而是通过标准请求/响应访问资金费率、订单、仓位、成交、费率和账户控制能力。

| 交易所 | Spot | Perpetual/Futures | Funding rate | 私有交易读写 | 杠杆控制 | 仓位模式控制 | 说明 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| OKX | `okx` 主站支持，`okxus`/`myokx` 为区域 Spot profile | `okx` 主站支持 perpetual/futures/options | `GET /api/v5/public/funding-rate`，共享 `get_funding_rates` | 下单、撤单、批量、查询、open orders、fills、balances、positions、fees | `POST /api/v5/account/set-leverage` | `POST /api/v5/account/set-position-mode`，仅主站 `okx` 声明支持 | 区域 profile 不声明 derivatives/funding/position mode，避免错误放量。 |
| Bybit | V5 Spot | V5 linear perpetual/futures | `GET /v5/market/funding/history`，共享 `get_funding_rates` | 下单、撤单、批量、查询、open orders、fills、balances、positions、fees | `POST /v5/position/set-leverage` | `POST /v5/position/switch-mode` | 仓位模式映射为 `mode=3` hedge、`mode=0` one-way。 |
| MEXC | Spot REST | Contract perpetual REST | `GET /api/v1/contract/funding_rate/{symbol}`，共享 `get_funding_rates` | Spot/Contract 下单、撤单、查询、open orders、fills、balances、positions、fees | `POST /api/v1/private/position/change_leverage`，long/short 两侧都设置 | 未声明支持 | 未可靠核验统一仓位模式端点前不暴露 `supports_position_mode_change`。 |
| KuCoin / KuCoin Futures | `kucoin` Spot adapter | `kucoinfutures` 独立 perpetual adapter | `GET /api/v1/funding-history`，共享 `get_funding_rates` | Futures 下单、撤单、查询、open orders、fills、balances、positions、fees；Spot 仍由 `kucoin` 承接 | 未声明支持 | 未声明支持 | 运行时将 KuCoin Spot 与 KuCoin Futures 视为同一 venue 的不同 adapter/profile 边界。 |

共享模型：

- `FundingRatesRequest` / `FundingRateSnapshot` / `FundingRatesResponse`：四个目标交易所的资金费率查询入口统一为 `ExchangeClient::get_funding_rates`，并透传到 gateway protocol/client/helper/mock/readonly/provider。
- `AccountControlCapabilities`：adapter 现在声明 `supports_leverage` 与 `supports_position_mode_change`，`ExchangeAdapterProfile` 会把缺失的 `leverage_control` 和 `position_mode_control` 纳入审计。
- `SetLeverageRequest` / `SetPositionModeRequest`：OKX、Bybit、MEXC 的已核验账户控制行为通过 `GatewayAdapter` 统一分发；MEXC/KuCoinFutures 未核验仓位模式时保持显式 unsupported。

## 四交易所能力检查清单

| 交易所 | spot + futures 统一 | limit | market | IOC | post-only | order lifecycle | partial fill | cancel replace | position sync | funding rate | leverage / position mode |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| OKX | 主站 Spot + Perp/Futures/Options；区域 profile 仅 Spot | 完整 | 完整 | 完整 | 完整 | 已归一 | REST query/fills 支持 | `amend_order` 原生声明 | private REST 支持 | 共享 API 已实现 | 主站 derivatives 支持 leverage + position mode |
| Bybit | V5 Spot + linear Perp/Futures | 完整 | 完整 | 完整 | 完整 | 已归一 | REST query/fills 支持 | `amend_order` 原生声明 | private REST 支持 | 共享 API 已实现 | 支持 leverage + position mode |
| MEXC | Spot + Contract Perpetual | 完整 | 完整 | 完整 | Spot/Contract 分 endpoint 声明 | 已归一 | REST query/fills 支持 | 未声明 amend，需 composed cancel+place | private REST 支持 | 共享 API 已实现 | 支持 leverage；position mode 不声明 |
| KuCoin / KuCoin Futures | Spot 与 Futures 分 adapter/profile | 完整 | 完整 | 完整 | 完整 | 已归一 | REST query/fills 支持 | Spot amend；Futures batch 为顺序组合 | Futures private REST 支持 | 共享 API 已实现 | Futures leverage/position mode 暂不声明 |

## 剩余缺口

- `position_mode_control`：OKX/Bybit 已实现；MEXC、KuCoin Futures 未可靠核验前不声明支持。
- `leverage_control`：OKX/Bybit/MEXC 已实现；KuCoin Futures 暂不声明支持。
- `cancel_replace`：MEXC 需要明确 composed cancel+place；KuCoin Futures batch/replace 原子性不一致。
- `private WebSocket + REST fallback`：MEXC 私有 WS 当前明确是 REST fallback；KuCoin Futures private runtime 仍标注 token 获取/续期风险。
- `spot + futures 统一`：KuCoin 主 adapter 仍仅 Spot，运行时需要通过 `ExchangeAdapterProfile` 或 venue 配置把 `kucoin` + `kucoinfutures` 组成一个逻辑 venue。

## 不一致行为列表

- 状态机：现有 `OrderStatus` 包含 `Open/PendingCancel/Expired/Unknown`，策略标准只允许 `NEW/PARTIALLY_FILLED/FILLED/CANCELED/REJECTED`。新增 `NormalizedOrderState`：`Open` 和 `PendingCancel` 映射到 `NEW`，`Expired` 映射到 `CANCELED`，`Unknown` 映射到 `REJECTED` 且必须触发 reconciliation。
- Cancel replace：原生 amend、非原子 cancel+place、顺序 batch 三种行为不能泄漏到策略层，统一由 `CancelReplaceMode` 描述。
- Order book：Binance/OKX/MEXC/KuCoin 声明 strict delta，Bybit/Gate.io 当前 snapshot-only，必须在 gap 后强制 snapshot + REST resync。
- 私有成交回报：有些 adapter 能私有 WS，有些只 REST readback；策略层必须按 `rest_fill_fallback_after_ms` 查询 `query_order/open_orders/recent_fills`。

## 统一 API 行为标准

新增合约位置：

- `crates/rustcta-exchange-api/src/adapter.rs`
- 导出：`rustcta_exchange_api::ExchangeAdapter`

关键标准：

- `place_normalized_order`：策略提交统一订单，adapter 负责 price/qty/min-notional/symbol/rate-limit 归一化。
- `cancel_normalized_order`：撤单响应统一返回 `CancelOrderResponse`。
- `cancel_replace_order`：优先原生 amend；无原生时 composed cancel+place；策略只看 `CancelReplaceMode`。
- `NormalizedOrderState`：唯一策略状态机为 `NEW`、`PARTIALLY_FILLED`、`FILLED`、`CANCELED`、`REJECTED`。

## WebSocket 可靠性补全

标准恢复策略：

1. reconnect：初始 250ms，指数退避到 15s，带 jitter，无限重连或由 adapter profile 限制。
2. snapshot + diff recovery：重连或 gap 后先拿 REST snapshot，再应用新 diff。
3. sequence gap detection：strict delta 交易所必须比对 expected/received sequence；非 strict delta 视为 snapshot-only。
4. fallback REST sync：private stream 断开、订单状态未知、成交缺失时查询 `query_order`、`open_orders`、`recent_fills`、`positions`。
5. gap 恢复期间阻断新机会计算，避免用不可信 L1 做开仓。

## 交易所差异适配层

- precision normalization：买入限价向上取 tick，卖出限价向下取 tick；maker 滑点捕获买单向下偏移，卖单向上偏移。
- quantity step：统一按 base quantity 计算，合约数量通过 `SymbolPrecision.quantity_unit/contracts` 转换。
- min notional：下单前拒绝，必要时由上层 sizing 重新规划，不把 venue reject 当正常风控。
- rate limit adapter：按 exchange/account/market bucket 在写单前限速。
- symbol mapping：canonical symbol 到 exchange symbol 必须按 market_type 分域缓存，symbol rules refresh 后重载。

## Hedge 执行一致性

- 开仓模式二选一：
  - `dual_taker`：原模块，两腿 IOC taker 并发开仓，平仓双 taker。
  - `slippage_capture`：maker 限价开仓，成交后立即选择另一交易所最优报价 taker 对冲，平仓双 taker。
- slippage model：
  - taker buy：`ask * (1 + slippage_pct)`。
  - taker sell：`bid * (1 - slippage_pct)`。
  - slippage-capture maker buy：`best_ask * (1 - maker_price_offset_pct)`。
  - slippage-capture maker sell/short open：`best_bid * (1 + maker_price_offset_pct)`。
- failure retry：
  - maker 开仓 1s 默认超时撤单。
  - maker 成交但 hedge 未确认：先 REST query/fills，再重试 taker hedge。
  - hedge 仍失败：进入 single-leg emergency reduce-only close，阻止新开仓。

## 新策略模块

配置示例：

```yaml
execution_module: slippage_capture
slippage_capture:
  startup_skip_spread_secs: 10
  target_notional_usdt: 5.5
  min_open_spread_pct: 0.005
  max_open_spread_pct: 0.05
  maker_price_offset_pct: 0.0005
  hedge_taker_slippage_pct: 0.0005
  close_taker_slippage_pct: 0.0005
  maker_order_timeout_ms: 1000
  cancel_unfilled_maker: true
  max_maker_top_depth_usdt: 25.0
```

新增核心类型：

- `CrossArbExecutionModule`
- `SlippageCaptureArbitrageConfig`
- `SlippageCaptureOpenOpportunity`
- `SlippageCaptureMakerOrderDraft`
- `SlippageCaptureHedgePlan`
- `SlippageCaptureStartupGate`

新增函数：

- `evaluate_slippage_capture_open_opportunities`
- `slippage_capture_maker_order_command`
- `slippage_capture_hedge_order_command`
- `slippage_capture_maker_cancel_command`

## Missing Features Implementation Plan

1. 合约冻结：所有策略只用 `ExchangeAdapter` 标准五态订单机。
2. 能力矩阵补齐：继续把 `funding_rate`、`leverage_control`、`position_mode_control`、cancel-replace 原子性纳入 profile 审计。
3. WS 恢复：按 strict delta/snapshot-only 分级实现 sequence gap detection 和 REST resync。
4. 归一化强制执行：所有 adapter 下单前走 symbol rules、precision、min notional、rate limit。
5. Hedge 一致性：统一 maker-fill 后 taker hedge、双 taker 平仓、失败重试和单腿保护。

## 本轮验证命令

```bash
cargo fmt
cargo check -p rustcta-exchange-gateway
cargo test -p rustcta-exchange-api capabilities -- --nocapture
cargo test -p rustcta-exchange-api exchange_adapter_profile_should_report_missing_features_and_inconsistencies -- --nocapture
cargo test -p rustcta-exchange-gateway bybit_get_funding_rates_should_send_public_funding_history_request -- --nocapture
cargo test -p rustcta-exchange-gateway bybit_set_position_mode_should_send_signed_v5_switch_mode_request -- --nocapture
cargo test -p rustcta-exchange-gateway okx_adapter_should_set_position_mode_with_standard_account_control_request -- --nocapture
cargo test -p rustcta-exchange-gateway mexc_adapter_should_declare_capabilities_v2_for_toolchain_audit -- --nocapture
cargo test -p rustcta-exchange-gateway kucoinfutures_public_stream_spec_should_normalize_symbol_and_ping_pong -- --nocapture
```

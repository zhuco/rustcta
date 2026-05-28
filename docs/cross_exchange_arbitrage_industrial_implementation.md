# 跨所 USDT 永续套利工业级开发实施文档

本文档用于指导后续补齐跨交易所 USDT 本位永续套利系统的实盘级开发。现有策略文档
`docs/cross_exchange_arbitrage_usdt_perp.md` 更偏 Maker+Taker 模拟与机会计算，本文件聚焦工程落地：
交易所功能全量实现、仓位管理、风险控制、对账恢复、测试验收和上线流程。

目标交易所：

- Binance USD-M Futures
- OKX USDT SWAP
- Bitget USDT-FUTURES
- Gate USDT Futures

目标策略形态：

- 首期仍以 Maker+Taker 跨所套利为主。
- 实盘只允许白名单 USDT 永续，不做无保护全市场开仓。
- 任何开仓必须同时通过行情质量、深度、手续费、资金费率、仓位、保证金、路由健康、对账状态和熔断状态校验。
- 系统必须能在部分成交、单腿暴露、网络超时、交易所拒单、进程重启、私有流断线时进入可恢复状态。

## 1. 当前状态与主要缺口

### 1.1 已有基础

代码中已经存在下列基础模块：

- `src/strategies/cross_exchange_arbitrage/`
  - `config.rs`：套利配置结构与基础校验。
  - `opportunity.rs`、`simulation.rs`：机会计算、VWAP、Maker/Taker 模拟。
  - `funding.rs`、`fees.rs`：资金费率与手续费估算。
  - `risk.rs`：当前主要是机会过滤级别的轻量风控。
  - `state.rs`、`storage.rs`、`dashboard.rs`：模拟状态、持久化、展示模型。
- `src/market/`
  - `MarketDataAdapter`、`InstrumentMeta`、`OrderBook5`、`MarketFundingSnapshot` 已经可承接多交易所行情标准化。
- `src/exchanges/adapters/`
  - `binance_market.rs`、`okx_market.rs` 已有较完整的公有行情适配雏形。
  - `bitget_market.rs`、`gate_market.rs` 已有公有行情骨架，但仍需协议级验证、私有交易实现和生产监控。
  - `trading.rs` 已经提供 `ExchangeTradingAdapter`，但 Bitget/Gate 私有交易当前显式禁用。
- `src/execution/`
  - 已有执行层契约：`TradingAdapter`、`ExecutionRouter`、`ExecutionEngine`、`ArbitrageBundle`、状态机、对账模型、孤腿恢复建议。

### 1.2 不能直接实盘的缺口

下面这些缺口必须补齐，否则禁止进入真实下单：

- 交易所私有交易能力不完整：
  - Bitget/Gate 私有下单、撤单、查单、成交、持仓、余额、杠杆、仓位模式、私有流尚未接入统一执行层。
  - Binance/OKX 虽有基础 `Exchange` 实现与 `ExchangeTradingAdapter`，仍需对套利所需字段做端到端合约测试。
- 仓位管理仍不够完整：
  - 缺少跨所 `PositionManager`，无法统一管理 bundle、腿、成交、资金费、手续费、保证金占用和未对冲敞口。
  - 缺少启动接管、重启恢复、手工干预后的对账闭环。
- 风控仍停留在机会过滤：
  - 需要扩展为分层风控：开仓前、执行中、持仓中、组合级、交易所级、全局熔断。
  - 需要把风控决策变成状态机动作：允许、降级、只平不新开、撤单、应急平仓、人工复核。
- 对账与恢复仍未形成实盘流程：
  - 必须做到本地账本、交易所订单、交易所持仓、私有流事件、资金费结算之间持续一致。
  - 任何未知订单状态都不能用本地假设继续开仓，必须进入 `ReconcileRequired` 或 `CloseOnly`。
- 生产可观测性不足：
  - 需要订单全链路日志、指标、告警、审计表、可复盘事件流。
  - 需要为每个交易所独立展示路由健康、限速、拒单率、私有流延迟、对账漂移。

## 2. 总体架构目标

后续实现按下列边界分层。

```text
Market Layer
  InstrumentLoader / PublicWs / Funding / MarkPrice / RouteHealth

Trading Layer
  TradingAdapter / AccountAdapter / PrivateWs / RateLimiter / ErrorClassifier

Strategy Layer
  OpportunityEngine / SizingEngine / SignalFilter

Execution Layer
  BundleStateMachine / ExecutionRouter / HedgePlanner / Idempotency

Position Layer
  PositionManager / Ledger / FundingSettlement / FeeAttribution / Reconciler

Risk Layer
  PreTradeRisk / ExecutionRisk / PositionRisk / PortfolioRisk / KillSwitch

Operations Layer
  Persistence / Metrics / Alerts / Dashboard / Runbook
```

核心原则：

- 所有交易所差异必须被隔离在适配层，策略层只处理标准化的 `CanonicalSymbol`、`InstrumentMeta`、`OrderCommand`、`ExchangePosition`。
- 所有真实订单必须先写入本地意图日志，再发送到交易所，再写入 ack/fill/cancel 结果。
- 交易所返回未知状态时，以交易所为准，但不能自动假设成交或未成交；必须通过查单、查成交、查持仓闭环确认。
- 系统状态必须可重建：仅依赖内存状态的实现不能上实盘。

## 3. 统一交易所适配器契约

### 3.1 目标接口拆分

当前 `MarketDataAdapter` 和 `TradingAdapter` 已经是正确方向，但实盘需要进一步明确责任边界。建议保留现有模块，同时新增或扩展下列契约：

```rust
pub trait PerpMarketAdapter {
    async fn load_instruments(&self) -> Result<Vec<InstrumentMeta>>;
    async fn load_funding(&self, symbols: &[CanonicalSymbol]) -> Result<Vec<MarketFundingSnapshot>>;
    async fn fetch_orderbook_snapshot(&self, symbol: &ExchangeSymbol, depth: u16) -> Result<OrderBook5>;
    fn build_public_ws_subscriptions(&self, symbols: &[ExchangeSymbol]) -> Vec<WsSubscription>;
    fn parse_public_ws_message(&self, raw: &str, recv_ts: DateTime<Utc>) -> Result<Vec<MarketEvent>>;
}

pub trait PerpTradingAdapter {
    async fn place_order(&self, command: OrderCommand) -> Result<OrderAck>;
    async fn cancel_order(&self, command: CancelCommand) -> Result<CancelAck>;
    async fn get_order(&self, query: OrderQuery) -> Result<OrderState>;
    async fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> Result<Vec<OrderState>>;
    async fn get_fills(&self, query: FillQuery) -> Result<Vec<FillEvent>>;
    async fn get_positions(&self, symbol: Option<&ExchangeSymbol>) -> Result<Vec<ExchangePosition>>;
    async fn get_balances(&self) -> Result<Vec<ExchangeBalance>>;
    async fn set_leverage(&self, command: LeverageCommand) -> Result<LeverageAck>;
    async fn set_position_mode(&self, command: PositionModeCommand) -> Result<PositionModeAck>;
}

pub trait PerpPrivateStreamAdapter {
    async fn connect(&self) -> Result<PrivateStreamHandle>;
    fn parse_private_message(&self, raw: &str, recv_ts: DateTime<Utc>) -> Result<Vec<PrivateEvent>>;
}
```

现有 `TradingAdapter` 可继续使用，但需要补齐：

- `get_fills` 或等价成交查询。
- `PrivateEvent` 标准化。
- 限速、错误分类、重试策略。
- 交易所能力表与运行时自检。

### 3.2 标准化订单语义

策略层只能表达标准语义：

- `OpenLongMaker`
- `OpenShortMaker`
- `HedgeLongTaker`
- `HedgeShortTaker`
- `CloseLongMaker`
- `CloseShortMaker`
- `CloseLongTaker`
- `CloseShortTaker`
- `EmergencyCloseLongTaker`
- `EmergencyCloseShortTaker`

适配层负责转换为交易所参数：

- side/order side
- position side
- open/close trade side
- reduce-only
- post-only
- IOC/FOK/GTC/GTX
- 合约数量单位
- client order id 字段名
- 价格和数量精度
- 杠杆和保证金模式

禁止策略层拼交易所特定字段，例如 `positionSide=LONG`、`tdMode=isolated`、`tradeSide=open`、Gate signed size。

### 3.3 订单幂等

所有真实订单必须满足：

- 使用确定性 `client_order_id`，格式包含策略、bundle、腿、attempt。
- 同一个 `client_order_id` 重试必须被视为同一个意图，不能创建新敞口。
- 下单超时后必须先按 `client_order_id` 查单，查不到再查近期成交，再决定是否重试。
- 订单 ID 映射必须持久化：
  - `client_order_id`
  - `exchange_order_id`
  - `bundle_id`
  - `leg`
  - `attempt`
  - `intent`
  - `submitted_at`
  - `acknowledged_at`
  - `terminal_at`

### 3.4 精度与合约单位

所有交易所必须统一到下列内部单位：

- 价格：USDT quote price。
- 数量：base asset quantity，除非 `InstrumentMeta.contract_size` 明确表示合约张数换算。
- 名义：`base_qty * price`，线性 USDT 永续统一为 USDT。

适配层必须提供双向换算：

```text
internal_base_qty -> exchange_order_size
exchange_position_size -> internal_base_qty
```

开仓 sizing 必须按两边共同可交易数量取最小值：

```text
target_notional
-> long_exchange quantity by ask VWAP and rules
-> short_exchange quantity by maker price and rules
-> min(common rounded qty)
-> re-check min_notional/min_qty/max_qty/depth/slippage
```

价格和数量舍入规则：

- 开仓数量一律向下取整到两边都合法，避免超预算。
- 买入限价若用于 Maker，按交易所 tick 调整后不得穿透对手盘，除非策略显式允许降级为 taker。
- 卖出限价若用于 Maker，按 tick 调整后不得穿透对手盘。
- reduce-only 平仓数量不得超过交易所真实持仓数量减去已有平仓挂单占用。

## 4. 交易所全量实现清单

本节列出每家交易所必须完成的能力。实现时必须以官方文档和真实 API 返回为准；文档链接放在附录，代码内应保留响应 fixture 测试，避免接口字段变化时静默失效。

### 4.1 所有交易所共同能力

市场数据：

- 拉取 USDT 永续合约元数据。
- 解析交易状态：可交易、只减仓、暂停、维护、退市。
- 解析 tick size、quantity step、min qty、min notional、contract size、max order qty、max position。
- 拉取 5 档订单簿快照。
- WebSocket 订阅 5 档订单簿或可构建 5 档的增量深度。
- 拉取标记价格、指数价格、资金费率、下一次资金费时间。
- 识别 sequence gap、stale book、空盘口、倒挂盘口、异常跳价。

私有交易：

- 下单：limit、market、post-only、IOC。
- 撤单：单笔撤单、批量撤单、按 symbol 全撤。
- 查单：按 exchange order id 和 client order id。
- 查成交：按 symbol、order id、时间窗口。
- 查询未成交订单。
- 查询订单历史。
- 查询持仓。
- 查询账户余额、可用保证金、已用保证金。
- 设置杠杆。
- 设置或读取仓位模式。
- 读取手续费等级或 symbol 实际费率；没有权限时使用配置费率但必须标记 `fee_source=config`。

私有流：

- 订单 ack/update/fill/cancel/reject/expire。
- 成交明细，包括手续费、手续费币种、maker/taker 标识。
- 持仓变化。
- 余额和保证金变化。
- listen key 或登录 token 自动续期。
- 私有流断线后自动重连，并触发 REST 全量对账。

运行保护：

- 每个交易所独立限速器。
- 每个 endpoint 的错误分类。
- 每个交易所独立 route health。
- 每个交易所独立 close-only 标志。
- 每个交易所独立 kill switch。

### 4.2 Binance USD-M Futures

必须实现：

- `exchangeInfo` 到 `InstrumentMeta` 的完整映射：
  - `contractType=PERPETUAL`
  - `quoteAsset=USDT`
  - `marginAsset=USDT`
  - `status=TRADING`
  - `PRICE_FILTER.tickSize`
  - `LOT_SIZE.stepSize/minQty`
  - `MIN_NOTIONAL.notional`
  - `timeInForce` 是否包含 `GTX`、`IOC`
- 公有行情：
  - partial depth 5/10/20。
  - mark price、funding rate、next funding time。
- 私有交易：
  - `POST /fapi/v1/order`。
  - `DELETE /fapi/v1/order`。
  - query order、open orders、all orders、user trades。
  - leverage、position mode、position risk、balance/account。
- 参数规则：
  - Hedge Mode 下必须发送 `positionSide=LONG/SHORT`。
  - Hedge Mode 下不得发送 `reduceOnly`，平仓通过反向 side + positionSide 表达。
  - One-way Mode 下使用 `positionSide=BOTH` 或省略，reduce-only 可用于平仓保护。
  - Post-only 使用 `timeInForce=GTX`。
  - IOC 使用 `timeInForce=IOC`。
- 风险读取：
  - liquidation price。
  - margin ratio 或 position risk 字段。
  - ADL/position risk 可用时纳入风险面板。

验收：

- Hedge Mode 开多、开空、平多、平空四条路径都有 dry-run 参数快照测试和真实 testnet/small-live 验证。
- 下单超时后可用 `client_order_id` 查回订单。
- reduce-only 在 Hedge Mode 下不会被错误发送。

### 4.3 OKX USDT SWAP

必须实现：

- instruments 到 `InstrumentMeta` 的完整映射：
  - `instType=SWAP`
  - `settleCcy=USDT`
  - `ctType=linear`
  - `state=live`
  - `tickSz`
  - `lotSz`
  - `minSz`
  - `ctVal`
  - `ctValCcy`
- 公有行情：
  - `books5` 或更高等级深度。
  - funding rate、next funding time、mark/index price。
- 私有交易：
  - place order、cancel order、amend order、orders pending、order history、fills。
  - positions、balance、account position risk。
  - set leverage、set position mode。
- 参数规则：
  - `instId` 使用 `BTC-USDT-SWAP` 格式。
  - `tdMode` 必须由配置控制，首期建议 `isolated`，确认保证金模型后才允许 `cross`。
  - net mode 使用净仓；long/short mode 使用 `posSide=long/short`。
  - post-only 使用 OKX 对应 `ordType=post_only`。
  - IOC 使用 `ordType=ioc` 或交易所要求的等价参数。
  - SWAP/FUTURES 的 `sz` 是合约单位，必须通过 `ctVal` 换算为内部 base quantity。
- 风险读取：
  - account positions。
  - account position risk。
  - margin ratio、liquidation price 可用字段。

验收：

- 同一 symbol 的内部 base quantity 与 OKX contract size 往返换算误差小于配置容忍度。
- net mode 与 long/short mode 不混用；启动自检发现模式不匹配必须阻止实盘。

### 4.4 Bitget USDT-FUTURES

必须实现：

- contract config 到 `InstrumentMeta` 的完整映射：
  - `productType=USDT-FUTURES`
  - `symbolType=perpetual`
  - `quoteCoin=USDT`
  - `supportMarginCoins` 包含 `USDT`
  - `symbolStatus=normal` 才允许开仓
  - `pricePlace`、`priceEndStep`、`sizeMultiplier`、`minTradeNum`、`minTradeUSDT`
  - `maxMarketOrderQty`、`maxOrderQty`、`maxPositionNum`
- 公有行情：
  - merge depth 5 档。
  - funding rate、next update。
  - mark/index/market price。
- 私有交易：
  - place order、batch order、modify、cancel、batch cancel、pending orders、order detail、fill detail。
  - positions、account、leverage、margin mode、position mode。
  - private WebSocket order/fill/account/position events。
- 参数规则：
  - `productType=USDT-FUTURES`，`marginCoin=USDT`。
  - one-way mode 忽略 `tradeSide`。
  - hedge mode 下 open/close 必须按 Bitget 的 `side` + `tradeSide` 规则转换。
  - reduce-only 订单可能在 one-way mode 下触发自动取消已有 reduce-only 单，必须通过 `clientOid` 查回 order id。
  - 平仓前必须扣减已有平仓挂单占用，否则容易触发 insufficient position。
- 风险读取：
  - 持仓保证金、可用保证金、强平价、最大持仓。
  - symbol 维护状态和 API restricted 状态。

验收：

- hedge mode 的开多、平多、开空、平空参数 fixture 必须覆盖。
- one-way reduce-only 返回空 order id 的场景必须有恢复测试。
- `symbolStatus != normal` 时只允许平仓，不允许新开。

### 4.5 Gate USDT Futures

必须实现：

- futures contracts 到 `InstrumentMeta` 的完整映射：
  - `settle=usdt`
  - perpetual contract。
  - contract name 如 `BTC_USDT`。
  - quanto multiplier/order size/price tick。
  - minimum order size。
  - in_delisting、trade status、maintenance 字段。
- 公有行情：
  - futures order book。
  - funding rate。
  - ticker、mark price、index price。
  - WebSocket `futures.order_book` 或等价增量深度；必须按官方流程用 REST 快照 + book id 恢复本地深度。
- 私有交易：
  - create futures order。
  - cancel order、cancel all。
  - list orders、get order、list trades。
  - positions、accounts、risk limit、leverage。
  - futures private WebSocket order/fill/position/account events。
- 参数规则：
  - 适配层必须把内部 `OrderSide`/`PositionSide` 转换为 Gate futures 的 size/side 语义。
  - `reduce_only`、`tif`、post-only、IOC 的真实字段必须用 fixture 固化。
  - Gate 合约可能以张数表达，必须通过 multiplier 换算内部 base quantity。
  - Gate 若只支持单向净仓，需要 PositionManager 按 net side 管理，不允许同一账户同一 symbol 同时假设 long/short 双仓。
- 风险读取：
  - liquidation price。
  - leverage。
  - risk limit。
  - contract maintenance/delisting。

验收：

- signed size 或 side 语义必须用真实小额订单验证，不能只靠文档推断。
- 订单簿 sequence gap 能触发快照重载。
- 单向净仓模式下的反手行为必须被禁止自动发生，除非显式进入“先平后开”流程。

## 5. 仓位管理设计

### 5.1 核心职责

新增 `PositionManager`，建议路径：

```text
src/strategies/cross_exchange_arbitrage/position.rs
```

职责：

- 管理每个套利 bundle 的生命周期。
- 记录每条腿的订单、成交、均价、手续费、资金费、未实现盈亏、已实现盈亏。
- 计算跨所是否对冲平衡。
- 计算每个交易所、每个 symbol、整个组合的名义敞口。
- 维护 open order 对持仓和平仓数量的占用。
- 对接 `execution::reconciler`，在漂移时给出动作。
- 启动时从持久化账本和交易所真实状态恢复。

### 5.2 数据模型

建议新增或扩展：

```rust
pub struct PositionKey {
    pub account_id: String,
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub side: PositionSide,
}

pub struct BundleLegState {
    pub leg_id: String,
    pub exchange: ExchangeId,
    pub exchange_symbol: ExchangeSymbol,
    pub side: PositionSide,
    pub intended_qty: f64,
    pub filled_qty: f64,
    pub remaining_qty: f64,
    pub avg_entry_price: Option<f64>,
    pub avg_exit_price: Option<f64>,
    pub open_fee: f64,
    pub close_fee: f64,
    pub funding_pnl: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub reduce_only_reserved_qty: f64,
    pub status: LegStatus,
}

pub struct BundlePosition {
    pub bundle_id: String,
    pub canonical_symbol: CanonicalSymbol,
    pub long_leg: BundleLegState,
    pub short_leg: BundleLegState,
    pub status: BundleStatus,
    pub entry_edge_pct: f64,
    pub target_notional_usdt: f64,
    pub gross_notional_usdt: f64,
    pub net_base_exposure: f64,
    pub net_usdt_exposure: f64,
    pub max_unhedged_qty: f64,
    pub max_unhedged_ms: i64,
    pub opened_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}
```

`ArbitrageBundle` 已有基础字段，可以逐步扩展，不必一次性重写。但新增字段必须服务于真实对账和恢复，而不是只服务 UI。

### 5.3 生命周期

标准开仓流程：

```text
Observing
  -> MakerPending
  -> MakerFilled(partial/full)
  -> Hedging
  -> Open
```

异常流程：

```text
MakerPending -> MakerTimeout -> CancelMaker -> Closed/Expired
MakerFilled -> HedgeRejected -> OrphanLeg -> Recovery
Hedging -> PartialHedge -> OrphanLeg -> Recovery
Open -> ReconcileRequired -> Open/CloseOnly/ManualReview
Closing -> PartialClose -> OrphanLeg -> Recovery
Any -> RiskStopped -> CloseOnly/EmergencyClose/ManualReview
```

关键规则：

- Maker 部分成交后，只能按实际成交数量对冲，不得按原始挂单数量对冲。
- Hedge 部分成交后，剩余未对冲数量必须进入 orphan 计时。
- 单腿暴露超过 `max_orphan_ms` 或损失超过 `max_orphan_loss_usdt`，必须优先反向平掉单腿，而不是继续等待套利成立。
- 平仓时必须先检查已有 reduce-only 平仓挂单占用，避免重复平仓。
- 任何状态迁移都必须写事件日志。

### 5.4 仓位 sizing

Sizing 输入：

- 两边实时盘口 VWAP。
- 两边 `InstrumentMeta`。
- 两边账户权益和可用保证金。
- 配置目标名义。
- 当前 symbol、exchange、portfolio 敞口。
- 当前交易所限速和路由状态。
- 资金费率窗口。

Sizing 公式建议：

```text
base_budget =
  min(
    config.sizing.target_notional_usdt,
    smaller_exchange_equity * capital_fraction_of_smaller_equity,
    symbol_remaining_limit,
    exchange_remaining_limit_long,
    exchange_remaining_limit_short,
    depth_supported_notional_after_slippage
  )

raw_qty = base_budget / conservative_price
qty = floor_to_common_exchange_steps(raw_qty)
notional = qty * conservative_price
```

必须拒绝：

- 任一腿低于最小下单数量或最小名义。
- 任一腿超过单笔最大数量或 symbol 最大持仓。
- 任一腿预计滑点超过上限。
- 新开后组合杠杆或保证金率超过上限。
- 新开后同 symbol bundle 数量超过上限。

### 5.5 启动接管

启动流程必须是：

```text
1. 加载本地持久化账本和未终态 bundle。
2. 连接所有启用交易所，校验时间同步。
3. 拉取 open orders、positions、balances。
4. 按 client_order_id 前缀识别策略订单。
5. 按 bundle_id 重建本地状态。
6. 找不到 bundle 的策略订单进入 UnknownStrategyOrder。
7. 找不到本地记录的交易所持仓进入 OrphanExposure。
8. 完成全量对账前，禁止新开仓。
```

启动策略：

- 对 `MakerPending` 且已超 TTL 的挂单：先查单，再撤单，再确认撤单或成交。
- 对有持仓无对冲腿的 bundle：进入 orphan recovery。
- 对完全平掉但本地未记录 close 的 bundle：导入成交并补写 close event。
- 对非策略订单：默认不接管，只纳入账户风险敞口；若配置允许 `adopt_manual_positions`，必须人工确认。

## 6. 对账与恢复

### 6.1 对账频率

建议：

- 私有 WebSocket：实时更新订单、成交、持仓。
- open orders REST：每 3-5 秒。
- positions REST：每 5-10 秒。
- balances REST：每 10-30 秒。
- full audit：每 60 秒，或私有流重连后立即执行。

### 6.2 对账分类

使用并扩展 `execution::reconciler` 的严重级别：

- `Ok`：本地与交易所一致。
- `MinorDrift`：数量误差在容忍范围内，可自动修正本地账本。
- `OrderDrift`：订单状态不一致，进入 close-only，重新查单和成交。
- `PositionDrift`：持仓不一致，禁止新开仓，执行全量补数。
- `OrphanExposure`：交易所有未被对冲或未被本地记录的敞口，触发孤腿恢复。
- `UnknownCritical`：无法判断真实状态，禁止自动开平，立即告警。

### 6.3 自动恢复动作

允许自动执行的动作：

- 补写本地 ledger。
- 撤销策略所有未成交 Maker 挂单。
- 重试 hedge，但必须有最大次数和更宽滑点上限。
- 反向平掉孤腿。
- 将交易所或 symbol 切到 close-only。
- 全局暂停新开仓。

禁止自动执行的动作：

- 未确认真实持仓时继续开新 bundle。
- 未确认订单终态时重复提交同方向订单。
- 将非策略手工仓位自动纳入套利 bundle。
- 在交易所只减仓或维护状态下开新仓。
- 在私有流断线且 REST 对账失败时执行新开仓。

## 7. 风控体系

### 7.1 风控分层

新增 `RiskEngine`，建议拆成：

```text
PreTradeRisk
ExecutionRisk
PositionRisk
PortfolioRisk
ExchangeRisk
KillSwitch
```

现有 `RiskGate` 可作为 `PreTradeRisk` 的一部分继续使用。

### 7.2 开仓前风控

必须检查：

- symbol 在实盘白名单内。
- 两个交易所都启用且 route healthy。
- 两边行情未过期，book age 小于上限。
- 两边盘口可用，没有空盘口、交叉盘口、异常跳价。
- 5 档深度覆盖目标成交量。
- VWAP 后净 edge 大于开仓阈值。
- raw spread 不超过异常价差上限。
- maker 价不会立即变 taker，除非策略配置允许。
- taker 预计滑点小于上限。
- 资金费率窗口不危险。
- 资金费方向和预期持仓时间不吞噬收益。
- 两边 symbol 状态允许开仓。
- 两边下单精度合法。
- 两边可用保证金充足。
- 新开后不会超过 per-symbol、per-exchange、portfolio 限额。
- 当前没有未解决对账漂移。
- 当前没有该 symbol 或交易所级熔断。

### 7.3 执行中风控

必须检查：

- Maker 挂单 TTL。
- Maker 排队时间。
- Maker 部分成交比例。
- Hedge 提交延迟。
- Hedge 拒单或超时。
- Hedge 成交滑点。
- 订单 ack 超时。
- cancel ack 超时。
- API 错误率。
- 限速剩余额度。
- 私有流延迟。

动作：

- Maker 超时未成交：撤单并关闭 bundle。
- Maker 部分成交：按成交量 hedge，未成交部分撤单。
- Hedge 超时：按 client order id 查单，不明状态进入 reconcile。
- Hedge 拒单：在预算内重试；预算耗尽则反向平 maker 腿。
- 滑点超过上限：停止继续扩大，转 orphan recovery。

### 7.4 持仓中风控

必须检查：

- long/short 数量差。
- long/short 名义差。
- bundle 当前净 PnL。
- 最大不利价差。
- 最大持仓时间。
- funding settlement 倒计时。
- 标记价格与指数价格偏离。
- liquidation buffer。
- 交易所保证金率。
- ADL 或风险限额状态。
- symbol 维护、退市、只减仓状态。

动作：

- 达到止盈：按平仓策略执行。
- 达到止损：优先双边 taker 或风险最小路径平仓。
- 资金费率变危险：提前平仓或禁止续持。
- liquidation buffer 不足：立即降仓或全平。
- symbol 进入只减仓：撤新开单，只允许平仓。

### 7.5 组合级风控

配置建议：

```yaml
risk:
  max_open_bundles: 3
  max_bundles_per_symbol: 1
  max_notional_per_bundle_usdt: 100.0
  max_notional_per_symbol_usdt: 200.0
  max_notional_per_exchange_usdt: 500.0
  max_gross_notional_usdt: 1000.0
  max_net_base_exposure_usdt: 30.0
  max_margin_usage_pct: 0.35
  min_liquidation_buffer_pct: 0.20
  max_daily_realized_loss_usdt: 20.0
  max_daily_drawdown_pct: 0.03
  max_orphan_qty_usdt: 30.0
  max_orphan_ms: 3000
  max_order_error_rate_1m: 0.05
  max_private_ws_lag_ms: 2000
```

组合级限制必须使用真实账户权益，而不是配置中的静态 `exchange_equity_usdt`。配置值只能作为模拟或账户数据不可用时的保守 fallback。

### 7.6 熔断状态机

建议全局状态：

```text
Normal
  -> Degraded
  -> CloseOnly
  -> EmergencyFlatten
  -> Halted
```

状态含义：

- `Normal`：允许新开、平仓、撤单。
- `Degraded`：降低 size，禁止新增弱信号，只允许高置信机会。
- `CloseOnly`：禁止新开，只允许撤单和平仓。
- `EmergencyFlatten`：撤销所有非平仓挂单，按配置强制平掉策略持仓。
- `Halted`：停止自动交易，只保留监控和人工操作提示。

熔断触发：

- 对账连续失败。
- 私有流断线超过阈值且 REST 补偿失败。
- 任一交易所订单错误率超过阈值。
- 全局日亏损超过阈值。
- 单腿暴露超过阈值。
- 组合保证金率危险。
- 交易所公告或 API 状态显示维护。
- 手动 kill switch 文件或 dashboard 操作。

## 8. 配置扩展建议

当前 `config/cross_exchange_arbitrage_usdt.yml` 已包含部分字段，但 `CrossExchangeArbitrageConfig` 还没有完整承接所有 sizing/risk/account 字段。后续应先让配置结构与模板严格一致，并启用 unknown field 检测，避免配置写了但代码没读。

建议增加：

```yaml
accounts:
  binance:
    account_id: binance_main
    env_prefix: BINANCE_0
    margin_mode: isolated
    position_mode: hedge
    leverage: 3
    live_enabled: false
  okx:
    account_id: okx_main
    env_prefix: OKX_0
    margin_mode: isolated
    position_mode: net
    leverage: 3
    live_enabled: false

execution:
  write_intent_before_submit: true
  require_private_ws_for_live: true
  require_rest_reconcile_before_live: true
  order_ack_timeout_ms: 1500
  cancel_ack_timeout_ms: 1500
  maker_order_ttl_ms: 5000
  hedge_order_timeout_ms: 1200
  max_hedge_retries: 1

reconciliation:
  startup_full_audit_required: true
  block_entries_on_minor_drift: false
  open_orders_interval_secs: 5
  positions_interval_secs: 10
  full_audit_interval_secs: 60

kill_switch:
  enabled: true
  file_path: logs/runtime/cross_arb.kill
  flatten_on_global_kill: false
```

配置校验要求：

- live 模式必须显式白名单 symbol。
- live 模式必须显式启用 account。
- live 模式必须有每个启用交易所的 fee source。
- live 模式必须配置每个交易所的 route。
- live 模式必须通过 startup full audit。
- live 模式下 unknown config field 应报错。

## 9. 持久化与审计

必须持久化：

- signal event。
- risk decision。
- sizing decision。
- execution intent。
- order ack。
- order update。
- fill event。
- funding settlement。
- fee attribution。
- position snapshot。
- reconcile report。
- recovery action。
- kill switch transition。

写入顺序：

```text
SignalDetected
RiskApproved
ExecutionIntentCreated
OrderSubmitAttempt
OrderAck / OrderSubmitUnknown
OrderFill / OrderCancel / OrderReject
PositionUpdated
ReconcileReport
BundleStatusChanged
```

实盘建议：

- JSONL 作为本地不可变审计日志。
- ClickHouse 用于查询和 dashboard。
- 周期性 snapshot 用于快速恢复，但恢复时仍需 replay 后再做交易所全量对账。

## 10. 测试策略

### 10.1 单元测试

必须覆盖：

- symbol 解析和交易所 symbol 映射。
- instrument metadata 解析。
- tick/step/min notional 精度规范化。
- 每个交易所开多、开空、平多、平空参数转换。
- reduce-only/post-only/IOC 参数转换。
- VWAP 和滑点计算。
- funding PnL 方向。
- fee maker/taker 归因。
- state machine 合法迁移。
- orphan recovery 推荐。
- risk gate 拒绝原因。

### 10.2 合约测试

每个交易所必须保存官方或真实 API 响应 fixture：

```text
tests/fixtures/exchanges/binance/
tests/fixtures/exchanges/okx/
tests/fixtures/exchanges/bitget/
tests/fixtures/exchanges/gate/
```

fixture 类型：

- instruments/contracts。
- orderbook snapshot。
- funding snapshot。
- place order ack。
- order update。
- fill。
- positions。
- balances。
- error responses。

测试必须验证：

- 字段缺失不会 panic。
- 新增未知字段不会影响解析。
- 关键字段缺失必须报错。
- 维护/退市/只减仓状态能正确阻止开仓。

### 10.3 集成测试

优先顺序：

1. 纯 mock adapter：可控成交、部分成交、拒单、超时。
2. sandbox/testnet：验证签名、时间同步、参数合法性。
3. 小资金实盘 read-only：只查账户、持仓、订单，不下单。
4. 小资金实盘 dry-run：生成真实参数但不发单。
5. 小资金实盘 live-small：单 bundle、最小名义、短 TTL。

必须覆盖的场景：

- Maker 未成交撤单。
- Maker 部分成交，hedge 成功。
- Maker 成交，hedge 拒单，反向平掉 maker 腿。
- Hedge 超时但最终成交，通过查单恢复。
- 私有流断线，REST 对账恢复。
- 进程在 MakerPending 时崩溃，重启撤单或接管。
- 进程在 OrphanLeg 时崩溃，重启进入 recovery。
- 手工关闭某一腿后，系统识别 position drift 并停止新开。
- 交易所 symbol 进入只减仓，系统停止开仓并平掉已有仓位。

### 10.4 压力和故障演练

必须演练：

- WebSocket 断线重连。
- REST 429 限速。
- 交易所 5xx。
- 本地网络延迟上升。
- 订单 ack 丢失。
- 成交事件乱序。
- duplicate fill event。
- JSONL 写入失败。
- ClickHouse 不可用。
- dashboard 不可用。

验收标准：

- 不因重复事件重复记账。
- 不因超时重复开仓。
- 不因行情过期继续开仓。
- 不因持久化失败继续实盘交易。
- 不因 dashboard 故障影响风控。

## 11. 监控与告警

必须输出指标：

- `cross_arb_signal_count`
- `cross_arb_open_approved_count`
- `cross_arb_open_rejected_count{reason}`
- `cross_arb_order_submit_latency_ms{exchange}`
- `cross_arb_order_ack_latency_ms{exchange}`
- `cross_arb_hedge_latency_ms{exchange}`
- `cross_arb_private_ws_lag_ms{exchange}`
- `cross_arb_book_age_ms{exchange,symbol}`
- `cross_arb_reconcile_severity{exchange,symbol}`
- `cross_arb_orphan_exposure_usdt`
- `cross_arb_bundle_net_pnl_usdt`
- `cross_arb_portfolio_margin_usage_pct`
- `cross_arb_kill_switch_state`

告警等级：

- P0：孤腿暴露超时、保证金危险、全局熔断、未知真实持仓、应急平仓失败。
- P1：私有流断线、对账 position drift、订单错误率超限、交易所 close-only。
- P2：行情 stale、资金费率危险、maker non-fill 异常、手续费 API 失败。

告警内容必须包含：

- exchange。
- symbol。
- bundle_id。
- client_order_id / exchange_order_id。
- 当前状态。
- 推荐动作。
- 是否已经自动执行。

## 12. 分阶段开发计划

### Phase 0：冻结实盘边界

目标：

- 明确 live 模式默认禁用。
- 配置 unknown field 严格校验。
- live 模式必须显式账户、symbol、交易所、风控阈值。

完成标准：

- `cargo test --all-features` 通过。
- 配置模板与 Rust config struct 字段一致。
- live 模式缺少白名单、账户或 full audit 时启动失败。

### Phase 1：统一交易所适配器合约

目标：

- 扩展 `TradingAdapter` 所需字段。
- 增加 `FillEvent`、`PrivateEvent`、`ExchangeErrorClass`、`RateLimitState`。
- 所有交易所实现 `capabilities()`，未实现能力必须显式 false。

完成标准：

- mock adapter 覆盖所有订单语义。
- 交易所参数转换单元测试覆盖四家交易所。
- Bitget/Gate 不再以“缺实现”静默失败，而是 capability 层明确阻止实盘。

### Phase 2：市场数据生产化

目标：

- 四家交易所公有行情、元数据、资金费率完整实现。
- WebSocket sequence/stale/gap 处理完善。
- 长尾 universe 自动过滤。

完成标准：

- `cross_arb_preflight` 对四家交易所输出 symbol 交集、规则、资金费、盘口健康。
- 公有流断线可重连并重新订阅。
- 每个交易所有 fixture tests。

### Phase 3：Binance/OKX 私有交易闭环

目标：

- Binance 和 OKX 先完成私有交易全流程。
- 完成小资金前的 read-only、dry-run、live-small 闭环。

完成标准：

- 能查询余额、持仓、open orders、fills。
- 能设置或验证仓位模式和杠杆。
- 能完成最小名义 Maker 挂单、撤单、查单。
- 能完成平仓 reduce-only 或等价保护。
- 私有流断线可触发 REST 对账。

### Phase 4：Bitget/Gate 私有交易闭环

目标：

- Bitget/Gate 从 public-only 升级为可实盘适配器。
- 补齐特殊仓位模式、reduce-only、合约单位换算。

完成标准：

- 每家交易所真实小额订单验证 open/close 四路径。
- 每家交易所订单异常和限速分类可用。
- 每家交易所可进入 close-only 并批量撤策略挂单。

### Phase 5：PositionManager 与对账恢复

目标：

- 新增跨所仓位管理。
- 实现启动接管、周期对账、孤腿恢复。

完成标准：

- 重启后能重建未终态 bundle。
- 手工改动持仓后系统能识别 drift 并阻止新开。
- orphan recovery 场景全部通过 mock 和小资金实盘验证。

### Phase 6：工业级 RiskEngine

目标：

- 完成分层风控和熔断状态机。
- Dashboard 展示风险状态和可执行动作。

完成标准：

- 所有开仓都附带 risk decision。
- 所有拒绝都有可解释 reason。
- kill switch 文件和 dashboard 操作都能使系统进入 close-only 或 halted。
- 日亏损、保证金、孤腿、对账失败都能触发正确降级。

### Phase 7：受控实盘灰度

目标：

- 只开一个 symbol、一个 bundle、最小名义。
- 每次运行生成完整审计日志和复盘报告。

完成标准：

- 连续 7 天 shadow 无 P0/P1。
- 连续 3 天 live-small 无未解释 drift。
- 所有交易记录能从 signal 追踪到 fill、fee、funding、PnL。
- 手动 kill switch 演练通过。

## 13. 上线门禁

进入 live-small 前必须全部满足：

- `cargo fmt`。
- `cargo clippy --all-targets --all-features` 无新增警告。
- `cargo test --all-features` 通过。
- 四家交易所 read-only preflight 通过。
- 启用交易所 private stream 可连接并接收心跳或账户事件。
- 启用交易所 REST 对账通过。
- 所有 symbol 都通过 metadata 校验。
- 所有配置字段被 Rust config 严格识别。
- `execution.dry_run=false` 只能在 live-small 模式下由显式配置开启。
- dashboard 显示当前模式、账户、仓位、open orders、risk state。
- kill switch 演练通过。
- 单腿恢复演练通过。

禁止上线条件：

- 任一启用交易所缺少私有流或 REST 对账。
- 任一启用交易所无法查成交。
- 任一启用交易所无法可靠撤单。
- 任一启用交易所仓位模式未知。
- 配置中存在代码未识别字段。
- 本地账本不可写。
- 私有流断线后不会触发全量对账。
- 未完成小额真实撤单测试。

## 14. 官方 API 参考

实现时必须以官方文档和当前真实响应为准，尤其是订单参数、仓位模式、reduce-only 行为和限速。建议每次交易所适配器改动都同步更新 fixture。

- Binance USD-M Futures：
  - Exchange information: <https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information>
  - New order: <https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Order>
  - Partial book depth streams: <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams>
  - User data streams: <https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams>
- OKX API v5：
  - API guide: <https://www.okx.com/docs-v5/en/>
  - Public instruments, funding, order book trading, account positions, private WebSocket must be verified against the current guide before implementation.
- Bitget Futures API：
  - Contract config: <https://www.bitget.com/api-doc/contract/market/Get-All-Symbols-Contracts>
  - Merge depth: <https://www.bitget.com/api-doc/contract/market/Get-Merge-Depth>
  - Current funding rate: <https://www.bitget.com/api-doc/contract/market/Get-Current-Funding-Rate>
  - Place order: <https://www.bitget.com/api-doc/contract/trade/Place-Order>
  - Demo Trading uses separate demo API keys and requires the `paptrading: 1` header on private REST requests. Treat demo matching, balance, and depth as validation-only rather than production-equivalent liquidity.
- Gate API v4 Futures：
  - API v4 futures: <https://www.gate.com/docs/developers/apiv4/en/futures/>
  - API v4 main reference: <https://www.gate.com/docs/apiv4/index.html>
  - Futures TestNet uses separate testnet keys and testnet REST/WebSocket endpoints. Treat testnet matching and account state as validation-only rather than production-equivalent liquidity.

## 15. 第一批开发任务建议

建议从下面任务开始，避免直接跳到实盘下单：

1. 让 `CrossExchangeArbitrageConfig` 与 YAML 模板严格一致，开启 unknown field 拒绝。
2. 为 `TradingAdapter` 增加成交查询和私有事件模型。
3. 为四家交易所建立 fixture 目录和 contract tests。
4. 把 Bitget/Gate 私有交易 capability 明确接入任务列表，先实现 read-only account/position/open-orders。
5. 新增 `PositionManager`，先接 mock adapter，覆盖部分成交、孤腿、重启恢复。
6. 扩展 `RiskGate` 为 `RiskEngine`，实现 close-only 和 kill switch 状态。
7. 改造 dashboard，显示真实风控状态、对账状态和交易所 capability。
8. 增加 `cross_arb_preflight --private-readonly`，上线前必须执行。

这些任务完成前，系统只能继续以 simulation/shadow/dry-run 方式运行。

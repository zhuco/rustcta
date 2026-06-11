# 期现套利独立策略设计

状态日期：2026-06-11

## 目标

本文档定义一个新的独立策略入口：`spot_futures_arbitrage`。

该策略不是跨所现货套利的一个下单分支，也不是跨所合约套利的一个路由分支。它应作为独立策略运行，但复用现有跨所现货套利、跨所合约套利和交易所网关的基础能力：

- 复用现货套利的 Spot 行情、Spot 余额、现货下单、库存检查和 symbol universe 管理。
- 复用合约套利的 Perpetual 行情、资金费率、仓位、reduce-only 平仓、杠杆和仓位模式检查。
- 复用统一交易所 API、私有流确认、订单归一化、精度规则、费用模型、账本和风控事件。

第一阶段只做 USDT 本位线性永续和 USDT 现货：

```text
现货多头 + 永续空头
```

收益来源：

```text
1. 开仓时现货和永续之间的基差。
2. 持仓期间永续空头收到的资金费率。
3. maker 开仓节省的手续费或获得的 maker rebate。
```

第一阶段默认不做：

- BTC、ETH、BNB、SOL。
- 反向结构 `现货空 + 永续多`。
- 币本位合约、交割合约、期权。
- 借币做空现货。
- 裸多或裸空。
- 跨链转账作为热路径的一部分。
- 未通过交易所能力核验的 post-only、IOC/FOK、reduce-only、private WS 下单确认。

## 策略边界

### 为什么单独入口

现有三类策略的核心风险不同：

| 策略 | 主产品 | 核心风险 | 是否适合作为本策略入口 |
| --- | --- | --- | --- |
| 跨所现货套利 | Spot + Spot | 库存不均衡、转账、单腿现货库存 | 不适合直接并入 |
| 跨所合约套利 | Perp + Perp | 双合约仓位、单腿合约、保证金 | 不适合直接并入 |
| 期现套利 | Spot + Perp | 现货库存、合约保证金、资金费率、基差收敛 | 独立入口 |

本策略应单独拥有：

- 独立配置文件。
- 独立 runner 或 supervisor process spec。
- 独立策略 ID 和风控状态。
- 独立账本和 PnL 归因。
- 独立 dashboard/read model。

建议命名：

```text
strategy_kind: spot_futures_arbitrage
display_name: Spot Futures Arbitrage
config: config/spot_futures_arbitrage_usdt.yml
logs: logs/spot_futures_arbitrage/
runner: spot-futures-arbitrage-live-runner
```

## 交易结构

### 开仓方向

第一阶段只允许：

```text
买入现货
卖出 USDT 永续
```

开仓后目标净敞口：

```text
spot_base_qty - perp_short_base_qty ~= 0
```

价格暴露被对冲，剩余收益来自：

- 现货买入价格和永续卖出价格之间的基差。
- 持仓期间合约空头的资金费率。
- 手续费和滑点控制。

### 资金费率方向

对 `现货多 + 永续空`：

```text
funding_rate > 0: 空头通常收资金费，收益为正
funding_rate < 0: 空头通常付资金费，收益为负
```

所以资金费率为负不是绝对禁止条件，而是机会收益模型中的成本项。开仓判断必须看净收益：

```text
expected_net_edge_bps =
  basis_edge_bps
+ expected_funding_bps
+ maker_rebate_bps
- maker_fee_bps
- taker_hedge_fee_bps
- expected_close_fee_bps
- hedge_slippage_bps
- close_slippage_buffer_bps
- latency_buffer_bps
- funding_uncertainty_buffer_bps
- safety_buffer_bps
```

只有 `expected_net_edge_bps >= min_open_net_edge_bps` 才允许开仓。

## Universe 选择

### 默认排除币种

策略默认排除：

```yaml
universe:
  excluded_bases:
    - BTC
    - ETH
    - BNB
    - SOL
```

原因：

- 这些币过于热门，竞争拥挤，基差和资金费率通常被快速压平。
- 容量大但收益低，容易拉低资金效率。
- 本策略目标是中等流动性、资金费率和基差更有弹性的币种。

### 过滤条件

每个候选 symbol 必须同时满足：

- Spot 和 USDT 永续都存在。
- Spot 和 Perp 的 canonical base/quote 一致。
- 不在 disabled-symbol registry 中。
- 不在 `excluded_bases` 和 `excluded_symbols` 中。
- 现货盘口和合约盘口都有足够深度。
- 资金费率数据新鲜。
- 下一次资金费率结算时间可用。
- 合约支持 reduce-only。
- 合约支持读取仓位。
- 交易所账号具备 Spot 交易、Perp 交易、余额读取、仓位读取、订单读取和成交读取权限。

建议初始筛选：

```yaml
selection:
  min_quote_volume_usdt_24h: 1000000.0
  max_quote_volume_usdt_24h: 200000000.0
  min_spot_top_depth_usdt: 50.0
  min_perp_top_depth_usdt: 50.0
  min_open_net_edge_bps: 20.0
  min_expected_funding_bps: -5.0
  max_abs_basis_bps: 300.0
  max_spread_bps:
    spot: 30.0
    perp: 20.0
```

`max_quote_volume_usdt_24h` 用于避开过热主流资产；实际部署可以按交易所和账户容量调整。

## 数据模型

### InstrumentKey

期现套利必须按产品维度建 key，不能只用 `exchange + symbol`：

```text
InstrumentKey {
  exchange,
  market_type,       // spot 或 perpetual
  canonical_symbol,
}
```

需要按 `InstrumentKey` 分离：

- symbol rules。
- price tick。
- quantity step。
- min notional。
- contract size。
- fee rate。
- order book。
- balances。
- positions。
- funding snapshots。

### Route

一条可交易路线：

```text
SpotFuturesRoute {
  route_id,
  canonical_symbol,
  spot_exchange,
  spot_symbol,
  perp_exchange,
  perp_symbol,
  quote_asset,
  hedge_direction,       // long_spot_short_perp
  maker_leg,             // spot_buy_maker
  taker_hedge_leg,       // perp_sell_taker
  close_style,           // dual_taker_reduce_only
}
```

第一阶段建议只允许 spot leg 做 maker：

```text
maker leg: spot buy post-only
hedge leg: perp sell IOC/FOK
```

不建议第一阶段做 perp maker short 后 spot taker buy，因为合约 maker 成交后如果 Spot hedge 失败，会产生裸空合约风险，对小币更危险。

## 行情与资金费率输入

每次机会评估需要：

```text
spot_order_book
perp_order_book
perp_mark_price
perp_index_price
current_funding_rate
predicted_funding_rate_optional
next_funding_time
funding_interval_hours
spot_fee_rate
perp_fee_rate
spot_balance
perp_margin_available
current_spot_inventory
current_perp_position
exchange_health
```

行情要求：

- Spot 和 Perp book 都 fresh。
- book 没有 sequence gap。
- gap recovery 期间禁止开新仓。
- 只用 L1 触发可以，但必须用 depth 校验可成交数量。
- funding snapshot 必须小于 `max_funding_snapshot_age_ms`。

## 机会计算

### 基差

对开仓 `买现货 + 卖永续`：

```text
spot_buy_price = spot maker limit price
perp_sell_price = perp taker executable bid

basis_edge_bps =
  (perp_sell_price - spot_buy_price) / spot_buy_price * 10000
```

`basis_edge_bps` 为正，表示永续高于现货，适合做现金持仓套利。

### 资金费率期望

第一阶段使用保守估算：

```text
funding_rate_for_short = current_funding_rate
expected_settlements = expected_holding_hours / funding_interval_hours
expected_funding_bps = funding_rate_for_short * expected_settlements * 10000
```

如果存在预测资金费率：

```text
effective_funding_rate = min(current_funding_rate, predicted_funding_rate)
```

这样对空头更保守。若其中一个为负，按负值计。

临近资金费率结算时：

```text
if funding_rate < 0 and minutes_to_funding <= no_open_before_adverse_funding_mins:
    reject NearAdverseFundingSettlement
```

### 成本

开仓成本：

```text
spot_maker_fee_bps
perp_taker_fee_bps
perp_taker_slippage_bps
maker_adverse_selection_bps
```

平仓成本：

```text
spot_taker_fee_bps
perp_taker_fee_bps
spot_close_slippage_bps
perp_close_slippage_bps
```

总净边际：

```text
expected_net_edge_bps =
  basis_edge_bps
+ expected_funding_bps
- open_fee_bps
- close_fee_bps
- slippage_bps
- latency_buffer_bps
- safety_buffer_bps
```

只有满足以下条件才接受：

```text
expected_net_edge_bps >= min_open_net_edge_bps
basis_edge_bps <= max_open_basis_bps
spot_depth_enough == true
perp_depth_enough == true
funding_allowed == true
route_healthy == true
account_ready == true
```

## 下单流程

### 状态机

```text
Idle
SignalAccepted
SpotMakerPlaced
SpotMakerPartiallyFilled
SpotMakerFilled
PerpHedgeSubmitting
HedgedOpen
Closing
Closed
SpotMakerCanceledNoFill
HedgeFailedOneSidedSpotLong
UnknownOrderState
CloseOnly
RiskStopped
```

### 开仓步骤

1. 机会评估通过。
2. 预留 Spot 买入 USDT。
3. 预留 Perp 初始保证金。
4. 下 Spot post-only maker buy。
5. 如果 post-only 被拒绝，直接放弃，不追单。
6. 如果 maker 未成交且超时，撤单，状态为 `SpotMakerCanceledNoFill`。
7. 如果 maker 部分成交，按实际成交 base quantity 触发 Perp hedge。
8. 用 Perp IOC/FOK limit sell 开空，数量等于已成交现货数量。
9. Perp hedge 成交后进入 `HedgedOpen`。
10. 任一成交确认缺失，进入 `UnknownOrderState`，禁止新开仓，启动 REST readback。

### maker 价格

Spot maker buy 价格：

```text
spot_maker_limit =
  min(spot_best_bid + maker_join_ticks * tick_size,
      spot_best_ask - maker_price_offset_ticks * tick_size)
```

如果交易所不支持 maker post-only，则该 route 不允许开仓。

### hedge 价格

Perp taker sell 价格：

```text
perp_hedge_limit = perp_best_bid * (1 - hedge_taker_slippage_pct)
```

必须是 IOC 或 FOK。第一阶段优先 IOC，若交易所可靠支持 FOK 且 min qty/step 风险可控，可配置 FOK。

### 部分成交

部分成交规则：

```text
if filled_spot_qty >= min_hedge_base_qty:
    hedge filled_spot_qty immediately
else:
    keep maker order until timeout or cancel
```

每次 partial fill 都要累计：

```text
unhedged_spot_qty = spot_filled_qty - perp_hedged_qty
```

只要 `unhedged_spot_qty` 超过阈值，就禁止新开仓。

## 平仓流程

默认平仓：

```text
卖出现货
reduce-only 买回永续空头
```

建议第一阶段使用双腿 taker 平仓：

```text
spot sell taker
perp buy reduce-only taker
```

平仓触发条件：

- 基差收敛达到目标收益。
- 资金费率翻负且预期持仓收益不足。
- 接近不利资金费率结算。
- 达到最大持仓时间。
- 合约保证金风险升高。
- 交易所 route 降级。
- symbol 被禁用。
- 人工切换 close-only。
- 单腿修复需要退出。

平仓必须满足：

- Perp 平仓单必须 reduce-only。
- 平仓前检查现货可用余额和合约仓位。
- 平仓失败时进入 close-only，不允许新开仓。
- 平仓两腿真实成交数量不一致时记录 residual exposure。

## 单腿风险和修复

### Spot maker 成交，Perp hedge 失败

状态：

```text
HedgeFailedOneSidedSpotLong
```

含义：

```text
持有现货多头，但没有合约空头保护
```

处理优先级：

1. 立即暂停该 symbol 新开仓。
2. 查询 Perp 订单和成交，排除实际成交但本地未知。
3. 如果没有成交，重新尝试 Perp taker hedge。
4. 如果 hedge 深度不足或交易所不可用，评估 Spot emergency sell。
5. emergency sell 必须走独立账本，不计入正常套利利润。
6. 如果亏损超过单笔阈值，触发策略级 close-only 或 risk stopped。

### Unknown order state

任一订单 ack 或 fill 状态未知：

```text
禁止新开仓
优先 private WS
超时后 REST query_order
再查 recent_fills
再查 positions/balances
```

在状态恢复前，不允许用本地假设继续交易。

## 风控门禁

### 启动门禁

启动后必须全部通过：

- singleton process lock。
- live preflight。
- 交易所能力检查。
- public WS ready。
- private WS ready 或显式 REST readback canary。
- Spot balances ready。
- Perp balances ready。
- Perp positions ready。
- fee rates ready。
- symbol rules ready。
- funding snapshots ready。
- startup position takeover complete。

### 账户门禁

每次开仓前检查：

- Spot 有足够 USDT。
- Perp 有足够可用保证金。
- Perp 杠杆设置符合配置。
- Perp 仓位模式符合配置。
- 当前 symbol 没有 unmanaged spot inventory。
- 当前 symbol 没有 unmanaged perp position。
- 当前 bundle 数量低于限制。
- 当前 exchange 使用率低于限制。

### 仓位限制

建议默认：

```yaml
risk:
  max_open_bundles: 10
  max_active_bundles_per_symbol: 1
  max_notional_per_symbol_usdt: 100.0
  max_notional_per_exchange_usdt: 500.0
  max_total_notional_usdt: 1000.0
  max_unhedged_spot_notional_usdt: 10.0
  max_position_residual_base_qty: 0.00000001
  max_daily_loss_usdt: 50.0
  max_drawdown_usdt: 100.0
```

小币种必须限制单币和单交易所敞口。不要因为资金费率高而放大到无法快速退出的规模。

### 资金费率门禁

```yaml
funding:
  enabled: true
  max_funding_snapshot_age_ms: 5000
  expected_holding_hours: 8.0
  no_open_before_adverse_funding_mins: 20
  min_expected_funding_bps: -5.0
  max_adverse_funding_bps: 10.0
  close_if_next_funding_bps_below: -3.0
  require_next_funding_time: true
  use_predicted_funding_when_available: true
```

规则：

- 正 funding 是顺风，但仍要覆盖全部成本。
- 负 funding 是成本，不是绝对禁止。
- 负 funding 且即将结算，默认禁止新开仓。
- funding 数据过期，禁止新开仓。
- funding 突然反向，进入重新评估或 close-only。

## PnL 归因

每个 bundle 独立记录：

```text
realized_basis_pnl_usdt
unrealized_basis_pnl_usdt
funding_pnl_usdt
spot_fees_usdt
perp_fees_usdt
slippage_usdt
maker_rebate_usdt
emergency_repair_pnl_usdt
net_pnl_usdt
```

正常套利收益和修复损益必须分开。不能把 hedge 失败后的紧急卖出现货亏损隐藏到套利收益里。

### 年化收益口径

期现套利不能只看资金费率年化，也不能只看开仓基差。策略看的是单位保证金和现货本金占用后的净收益。

单个 bundle 的预期收益：

```text
gross_edge_bps =
  open_basis_edge_bps
+ expected_funding_bps_during_hold
+ maker_rebate_bps

total_cost_bps =
  spot_open_fee_bps
+ perp_open_fee_bps
+ spot_close_fee_bps
+ perp_close_fee_bps
+ hedge_slippage_bps
+ close_slippage_bps
+ borrow_or_transfer_cost_bps
+ latency_and_safety_buffer_bps

net_edge_bps = gross_edge_bps - total_cost_bps
```

资金占用口径：

```text
capital_used_usdt =
  spot_notional_usdt
+ perp_initial_margin_usdt
+ reserve_buffer_usdt
```

对 1x 永续空头，保守估算：

```text
capital_used_usdt ~= spot_notional_usdt * 1.20
```

其中 1.00 是现货本金，0.10 到 0.20 是合约保证金和风险缓冲。若使用更高杠杆，表面资金占用下降，但强平和追加保证金风险上升，第一阶段不建议用高杠杆追年化。

单笔持仓年化：

```text
annualized_return =
  net_pnl_usdt / capital_used_usdt
  * 365 * 24 / holding_hours
```

组合年化应按真实资金闲置率计算：

```text
portfolio_annualized_return =
  realized_net_pnl_usdt / average_total_capital_usdt
  * 365 / active_days
```

不要用单笔机会的瞬时年化代表组合年化。小币机会常见问题是可开仓时间短、容量小、平仓滑点大，真实组合年化通常显著低于机会截图年化。

### 收益区间预期

在不做 BTC、ETH、BNB、SOL，主要做中等流动性 USDT 现货和 USDT 永续的前提下，可接受的工程目标应按以下区间管理：

| 阶段 | 资金规模 | 目标 | 说明 |
| --- | --- | --- | --- |
| Observe/Paper | 0 实盘资金 | 不看收益 | 只验证机会数量、拒绝原因、资金费率方向和盘口容量。 |
| Small live | 100 到 2,000 USDT | 月化 0 到 2% 为合格 | 主要验证成交、对冲、平仓、账实一致。 |
| 稳定单 route | 2,000 到 20,000 USDT | 年化 10% 到 30% 是现实目标 | 取决于资金费率持续性、交易费率、盘口容量和资金闲置率。 |
| 多币多 route | 20,000 USDT 以上 | 年化 15% 到 50% 需要严格风控 | 需要自动择币、限仓、止损、异常修复和低错误率。 |
| 极端行情 | 不固定 | 单周期收益可很高 | 不应作为常态年化假设，且对应单腿和流动性风险更高。 |

更激进的年化通常来自以下来源：

- 放大小币仓位。
- 接受更差的平仓滑点。
- 在负 funding 或临近结算时继续开仓。
- 提高合约杠杆。
- 忽略未对冲库存。

这些做法会把套利策略变成带方向和流动性尾部风险的策略，不应作为第一阶段默认模式。

## 持久化设计

第一阶段可用 JSONL 作为热路径账本，ClickHouse 作为异步下游。热路径只允许 bounded channel `try_send`，不等待磁盘和数据库。

建议目录：

```text
logs/spot_futures_arbitrage/
  trade_events.jsonl
  opportunities.jsonl
  funding_snapshots.jsonl
  dashboard.json
  profit_history.jsonl
```

建议 ClickHouse 表前缀：

```text
spot_futures_arbitrage_*
```

核心事件：

| 事件 | 说明 |
| --- | --- |
| `spot_futures_book_sampled` | Spot/Perp book 采样 |
| `spot_futures_funding_snapshot` | 资金费率快照 |
| `spot_futures_opportunity_evaluated` | 机会评估和拒绝原因 |
| `spot_futures_signal_accepted` | 通过风控的信号 |
| `spot_futures_order_command` | 下单命令 |
| `spot_futures_order_event` | ack、partial、fill、cancel、reject |
| `spot_futures_bundle_event` | bundle 状态流转 |
| `spot_futures_funding_settlement` | 资金费率结算 |
| `spot_futures_pnl` | PnL 归因 |
| `spot_futures_risk_event` | 风控事件 |
| `spot_futures_reconcile_report` | 账实核对 |
| `spot_futures_readback_started` | 订单状态未知时开始回读 |
| `spot_futures_readback_resolved` | 订单、成交、余额、仓位回读完成 |
| `spot_futures_operator_command` | 人工 pause、resume、close-only、force-close 命令 |

### opportunity 字段

```text
event_ts
opportunity_id
route_id
canonical_symbol
spot_exchange
perp_exchange
spot_best_bid
spot_best_ask
perp_best_bid
perp_best_ask
spot_maker_price
perp_hedge_price
basis_edge_bps
current_funding_rate
predicted_funding_rate
expected_funding_bps
open_fee_bps
close_fee_bps
slippage_buffer_bps
safety_buffer_bps
expected_net_edge_bps
target_base_qty
target_notional_usdt
spot_depth_usdt
perp_depth_usdt
accepted
reject_reason
risk_flags
raw_record
```

### order_command 字段

```text
event_ts
bundle_id
client_order_id
exchange
market_type
canonical_symbol
exchange_symbol
role
side
order_type
time_in_force
post_only
reduce_only
price
quantity
notional_usdt
request_id
risk_profile_id
accepted_by_local_risk
submitted_to_router
router_response_status
raw_request
raw_response
```

### order_event 字段

```text
event_ts
bundle_id
client_order_id
exchange_order_id
exchange
market_type
canonical_symbol
role
event_type              // ack, reject, partial_fill, fill, cancel_ack, expired, unknown
last_fill_qty
cumulative_fill_qty
last_fill_price
average_fill_price
fee_asset
fee_amount
liquidity               // maker, taker, unknown
source                  // private_ws, rest_query_order, rest_recent_fills, manual
raw_record
```

### bundle 字段

```text
event_ts
bundle_id
route_id
canonical_symbol
previous_status
next_status
spot_exchange
perp_exchange
spot_filled_qty
perp_hedged_qty
residual_qty
spot_entry_price
perp_entry_price
spot_close_price
perp_close_price
funding_pnl_usdt
fees_usdt
realized_pnl_usdt
unrealized_pnl_usdt
reason
detail
```

### funding_snapshot 字段

```text
event_ts
exchange
market_type
canonical_symbol
funding_rate
predicted_funding_rate
effective_rate_for_short
funding_interval_hours
next_funding_time
minutes_to_next_funding
mark_price
index_price
source
raw_record
```

### pnl 字段

```text
event_ts
bundle_id
route_id
canonical_symbol
spot_exchange
perp_exchange
spot_open_notional_usdt
perp_open_notional_usdt
spot_close_notional_usdt
perp_close_notional_usdt
basis_pnl_usdt
funding_pnl_usdt
spot_fee_usdt
perp_fee_usdt
slippage_usdt
maker_rebate_usdt
emergency_repair_pnl_usdt
net_pnl_usdt
capital_used_usdt
holding_seconds
annualized_return
raw_record
```

### reconcile_report 字段

```text
event_ts
exchange
canonical_symbol
spot_available_qty
spot_locked_qty
perp_position_qty
expected_spot_qty
expected_perp_qty
residual_spot_qty
residual_perp_qty
residual_notional_usdt
blocks_new_entries
requires_operator
detail
```

### ClickHouse 建议表

JSONL 是第一阶段热路径账本，ClickHouse 是查询和报表层。建议按事件类型拆表，避免一个超宽表承载所有 schema：

```text
spot_futures_opportunities
spot_futures_order_commands
spot_futures_order_events
spot_futures_bundles
spot_futures_funding_snapshots
spot_futures_pnl
spot_futures_reconcile_reports
spot_futures_risk_events
spot_futures_operator_commands
```

所有表必须包含：

```text
event_ts
ingested_at
strategy_id
run_id
schema_version
raw_record
```

`raw_record` 用于保留原始 JSON，避免 schema 演进时丢失交易所返回字段。

## 配置样例

```yaml
mode: live
trading_mode: paper
enable_live_trading: false

strategy:
  name: spot_futures_arbitrage
  mode: observe

market:
  quote_asset: USDT
  spot_market_type: spot
  hedge_market_type: perpetual
  depth_levels: 5
  stale_quote_ms: 1000

universe:
  enabled_spot_exchanges:
    - gate
    - bitget
    - okx
  enabled_perp_exchanges:
    - gate
    - bitget
    - okx
  excluded_bases:
    - BTC
    - ETH
    - BNB
    - SOL
  excluded_symbols: []
  mode: dynamic

selection:
  min_quote_volume_usdt_24h: 1000000.0
  max_quote_volume_usdt_24h: 200000000.0
  min_open_basis_bps: 10.0
  max_open_basis_bps: 300.0
  min_open_net_edge_bps: 20.0
  min_spot_top_depth_usdt: 50.0
  min_perp_top_depth_usdt: 50.0

funding:
  enabled: true
  require_next_funding_time: true
  max_funding_snapshot_age_ms: 5000
  expected_holding_hours: 8.0
  no_open_before_adverse_funding_mins: 20
  min_expected_funding_bps: -5.0
  close_if_next_funding_bps_below: -3.0
  use_predicted_funding_when_available: true

execution:
  open_execution_style: spot_maker_then_perp_taker
  close_execution_style: dual_taker_reduce_only
  spot_maker_order_ttl_ms: 3000
  spot_maker_price_offset_ticks: 1
  spot_post_only_required: true
  perp_hedge_tif: ioc
  perp_hedge_slippage_pct: 0.003
  private_ws_confirmation_required: true
  allow_rest_readback_confirmation: true
  max_hedge_retries: 1

sizing:
  min_notional_usdt: 10.0
  target_notional_usdt: 50.0
  max_notional_usdt: 100.0
  top_of_book_capacity_ratio: 0.5
  perp_leverage: 1.0

risk:
  start_paused_new_entries: true
  start_close_only: false
  max_open_bundles: 10
  max_active_bundles_per_symbol: 1
  max_notional_per_symbol_usdt: 100.0
  max_notional_per_exchange_usdt: 500.0
  max_total_notional_usdt: 1000.0
  max_unhedged_spot_notional_usdt: 10.0
  max_hold_seconds: 86400
  max_daily_loss_usdt: 50.0
  max_drawdown_usdt: 100.0
  symbol_whitelist_required_live: true
  orphan_exposure_blocks_new_entries: true

persistence:
  enabled: true
  jsonl_dir: logs/spot_futures_arbitrage
  trade_ledger_path: logs/spot_futures_arbitrage/trade_events.jsonl
  clickhouse_enabled: false
  stop_live_on_unavailable: true

dashboard:
  enabled: true
  bind_host: 0.0.0.0
  port: 8092
```

## WebUI 设计

期现套利必须在 WebUI 中作为独立策略出现，不挂在跨所现货套利或跨所合约套利页面下面。

### 导航入口

```text
Strategies
  Spot Arbitrage
  Perpetual Arbitrage
  Spot Futures Arbitrage
```

`Spot Futures Arbitrage` 页面展示：

- 运行状态：running、paused、close-only、risk-stopped、unknown-order-state。
- 当前模式：observe、paper、router-dry-run、router-live。
- 实盘开关：配置是否允许 live，下单通道是否 live，当前是否暂停新开仓。
- Universe：启用交易所、候选 symbol、排除 symbol、默认排除 BTC/ETH/BNB/SOL。
- 机会表：route、basis、funding、fee、slippage、net edge、reject reason。
- Bundle 表：状态、现货成交、合约对冲、残余敞口、持仓时间、PnL。
- 资金费率表：当前 funding、预测 funding、下一次结算、是否临近不利结算。
- 风控表：总名义本金、单币名义本金、单交易所名义本金、未对冲现货、日内亏损。
- 账实核对：现货余额、合约仓位、本地 bundle、差异和阻断原因。
- 操作日志：人工命令和系统风控事件。

### 操作按钮

第一阶段允许以下操作：

| 操作 | 说明 | 要求 |
| --- | --- | --- |
| Pause New Entries | 暂停新开仓 | 不影响已有仓位平仓。 |
| Resume New Entries | 恢复新开仓 | 必须通过 preflight 和账实核对。 |
| Enter Close Only | 只允许平仓 | 单腿、未知订单、亏损阈值触发时自动进入。 |
| Exit Close Only | 退出 close-only | 需要账实一致且无未知订单。 |
| Force Reconcile | 手动触发账实核对 | 不提交订单。 |
| Force Close Bundle | 人工指定 bundle 平仓 | 需要二次确认和 reduce-only 检查。 |

禁止在 WebUI 中直接提供“忽略风控继续开仓”按钮。

### Dashboard snapshot

runtime 每 15 到 30 秒输出 snapshot：

```text
logs/spot_futures_arbitrage/dashboard_snapshot.json
```

control-api 将该 snapshot 合并到策略列表，WebUI 只读展示，并通过 control-api 发出操作命令。WebUI 不直接连接交易所，不直接读写 `.env`。

snapshot 至少包含：

```text
strategy_id
strategy_kind
run_id
generated_at
execution_mode
live_orders_enabled
active_symbols
enabled_spot_exchanges
enabled_perp_exchanges
open_bundles
pending_orders
latest_market_scan
latest_reconcile_report
risk_state
operator_state
runtime_tasks
```

## 生产运行入口

独立入口命名：

```text
spot-futures-arbitrage-runtime
```

推荐命令：

```bash
cargo run -p rustcta-strategy-spot-futures-arbitrage \
  --bin spot-futures-arbitrage-runtime \
  -- \
  --config config/spot_futures_arbitrage_usdt.yml \
  --strategy-id spot_futures_arb_live \
  --run-id local-observe \
  --tenant-id local \
  --account-id spot_futures_arb \
  --execution-mode router-dry-run \
  --dashboard-snapshot-path logs/spot_futures_arbitrage/dashboard_snapshot.json \
  --quiet-stdout
```

生产 systemd user service：

```text
config/systemd/user/spot-futures-arb-live.service
```

第一阶段服务名虽然带 `live`，但必须默认使用：

```text
--execution-mode router-dry-run
enable_live_trading: false
start_paused_new_entries: true
```

只有完成实盘验收后才允许切到：

```text
--execution-mode router-live
enable_live_trading: true
trading_mode: live
start_paused_new_entries: false
```

切换实盘必须同时满足：

- 配置文件明确 `enable_live_trading: true`。
- CLI 明确 `--execution-mode router-live`。
- WebUI/operator 状态未 paused。
- preflight 全部通过。
- 账实核对无残余敞口。
- 交易所能力检查通过。
- 订单回读和 private stream canary 通过。

任何一个条件不满足，runtime 必须拒绝提交真实订单。

## 模块落地建议

建议新增：

```text
strategies/spot-futures-arbitrage/
  src/core.rs
  src/lib.rs
  src/runtime_contract.rs
  src/app_runtime.rs
  src/bin/spot_futures_arbitrage_runtime.rs

crates/rustcta-spot-futures-arb-live-runner/
  src/lib.rs

config/spot_futures_arbitrage_usdt.yml
```

核心类型：

```text
SpotFuturesArbitrageConfig
SpotFuturesRoute
SpotFuturesOpportunity
SpotFuturesFundingEstimate
SpotFuturesMakerOrderDraft
SpotFuturesHedgePlan
SpotFuturesBundleState
SpotFuturesRiskState
SpotFuturesPnlBreakdown
```

核心函数：

```text
select_spot_futures_universe
evaluate_spot_futures_opportunities
estimate_spot_futures_funding
plan_spot_maker_open
plan_perp_taker_hedge
evaluate_spot_futures_close
inspect_spot_futures_residuals
plan_startup_spot_futures_takeover
```

## 运行时闭环

工业级运行不能停在机会扫描，必须形成完整闭环：

```text
market scan
  -> opportunity evaluation
  -> pre-trade risk check
  -> reserve capital
  -> submit spot maker
  -> observe spot fills
  -> submit perp taker hedge
  -> reconcile balances/positions
  -> manage funding and basis
  -> close decision
  -> submit spot/perp close
  -> realized PnL
  -> release capital
```

### Observe 模式

Observe 模式只做：

- REST/WS 行情采样。
- 资金费率采样。
- symbol rules、fee rates、depth 检查。
- 机会评估。
- reject reason 统计。
- dashboard snapshot。
- JSONL/ClickHouse 落库。

Observe 模式不允许生成真实订单命令。

### Router dry-run 模式

Router dry-run 模式允许：

- 构造真实订单命令。
- 通过执行路由做本地校验。
- 记录 order command。
- 不提交到交易所。

该模式用于检查 symbol mapping、数量精度、价格精度、post-only、reduce-only、IOC/FOK 参数是否正确。

### Router live 模式

Router live 模式允许真实下单，但必须使用双保险：

```text
config enable_live_trading == true
CLI execution_mode == router-live
```

同时还要通过 runtime gate：

```text
operator_state.paused_new_entries == false
operator_state.close_only == false
risk_state.blocks_new_entries == false
latest_reconcile_report.blocks_new_entries == false
unknown_order_count == 0
```

## Startup takeover

策略重启时不能假设本地内存为空。启动后必须读取：

- Spot 余额。
- Perp 仓位。
- Open orders。
- Recent fills。
- 本地 trade ledger。

恢复逻辑：

```text
if ledger bundle exists and exchange state matches:
    resume bundle
elif spot inventory and perp short are matched:
    create recovered hedged bundle
elif spot inventory exists without perp hedge:
    enter close-only and repair
elif perp short exists without spot inventory:
    enter close-only and reduce-only close
elif open orders exist and cannot be matched:
    cancel or readback before new entries
else:
    start clean
```

所有 takeover 结果必须写入 `spot_futures_reconcile_report`。

## 测试要求

最小测试集：

- 默认排除 BTC、ETH、BNB、SOL。
- funding 为正时，`现货多 + 永续空` 的 funding edge 为正。
- funding 为负时，`现货多 + 永续空` 的 funding edge 为负但不直接禁止。
- 负 funding 且临近结算时拒绝开仓。
- Spot maker 成交后按实际 fill qty 生成 Perp taker hedge。
- Spot maker 未成交超时只撤单，不提交 Perp hedge。
- Perp hedge 失败后进入 `HedgeFailedOneSidedSpotLong`。
- `InstrumentKey` 按 spot/perpetual 分离精度和盘口。
- Perp 平仓单必须 reduce-only。
- startup takeover 能识别已有现货和永续空头是否匹配。
- 账本能分离 funding PnL、basis PnL、fee、slippage、emergency repair PnL。

建议验证命令：

```bash
cargo fmt
cargo check
cargo test -p rustcta-strategy-spot-futures-arbitrage
cargo test --all-features spot_futures
```

新增实盘前测试：

- router dry-run 能生成 Spot post-only buy 和 Perp IOC sell。
- Spot post-only 被拒绝时不提交 Perp hedge。
- Spot partial fill 后只按实际成交数量 hedge。
- Perp hedge reject 后进入 close-only。
- 双腿平仓中 Perp buy close 必须带 reduce-only。
- live 配置未开启时，`router-live` 直接启动失败。
- CLI 为 dry-run 时，即使配置开启 live，也不能真实提交订单。
- 交易所返回 unknown order 状态时阻断新开仓。
- REST readback 能恢复 ack、fill、cancel、reject。
- JSONL 写入失败且 `stop_live_on_unavailable=true` 时禁止 live 下单。

## 实盘推进顺序

1. Observe：只采集 spot/perp book、funding 和机会评估，记录 reject reason。
2. Paper：模拟 spot maker fill 和 perp hedge，验证 PnL 归因。
3. Live dry run：生成真实下单计划，但不提交。
4. Small live：单币、单 route、低 notional、close-only 预案就绪。
5. Multi-symbol live：放开动态 universe，但保留严格限仓和排除列表。

上线前必须确认：

- 所有 enabled exchange 的 Spot 和 Perp 能力都通过网关审计。
- private WS 或 REST readback 路径可用。
- reduce-only 行为已实测。
- post-only 行为已实测。
- funding snapshot 解析和下一次结算时间可靠。
- dashboard 和账本不进入交易热路径。

### 上线验收清单

进入 `router-live` 前必须逐项通过：

| 项目 | 验收标准 |
| --- | --- |
| 配置 | `enable_live_trading=false` 的情况下 live 启动失败。 |
| 权限 | 每个交易所 API key 只具备必要权限，不开提现权限。 |
| Symbol mapping | Spot 和 Perp exchange symbol 都能 round-trip。 |
| 精度 | price tick、qty step、min notional、contract size 全部校验通过。 |
| 资金费率 | funding rate、predicted funding、next funding time 均可靠。 |
| 下单 | post-only、IOC/FOK、reduce-only 都在小额或测试环境验证。 |
| 私有流 | ack、partial fill、fill、cancel、reject 都能进入账本。 |
| REST 回读 | private stream 丢失时能 query order、recent fills、balance、position。 |
| 风控 | pause、close-only、risk stopped 能阻止新开仓。 |
| 单腿修复 | Spot long without hedge 能自动阻断并提示修复动作。 |
| WebUI | 状态、机会、bundle、PnL、风险、操作命令都可见。 |
| 账本 | order command、order event、bundle event、PnL、reconcile report 都落库。 |
| 回滚 | 一条命令可切回 dry-run 或 stop service。 |

### 实盘初始参数

第一轮 small live 建议：

```yaml
sizing:
  min_notional_usdt: 5.0
  target_notional_usdt: 10.0
  max_notional_usdt: 20.0

risk:
  max_open_bundles: 1
  max_active_bundles_per_symbol: 1
  max_notional_per_symbol_usdt: 20.0
  max_total_notional_usdt: 50.0
  max_unhedged_spot_notional_usdt: 3.0
  max_daily_loss_usdt: 5.0
```

只放开一个 symbol 和一个 route，连续 20 到 50 个完整 bundle 账实一致后，再逐步放大。

## 当前落地状态

截至 2026-06-11，本仓库中的目标状态应分三层理解：

| 层级 | 状态 | 说明 |
| --- | --- | --- |
| 文档设计 | 完整设计 | 本文档定义独立期现套利入口和工业级闭环。 |
| Observe/Dry-run | 已接入 | `strategies/spot-futures-arbitrage` 可扫描 Spot/Perp route、评估机会、输出 dashboard snapshot 和 JSONL。 |
| WebUI | 已接入 | Control API 合并 `logs/spot_futures_arbitrage/dashboard_snapshot.json`，WebUI 独立展示 `spot_futures_arbitrage`、执行闸门和 reconcile 状态。 |
| Server dry-run | 已部署 | `spot-futures-arb-live.service` 以 `--execution-mode router-dry-run` 运行，读取真实行情、费用、资金费率、能力审计、Spot 余额和 Perp 仓位。 |
| Router live | 未开启 | 真实下单必须等订单确认、回读、账实核对和风控闭环全部通过。 |

当前默认配置必须保持保守：

```yaml
mode: observe
trading_mode: paper
enable_live_trading: false
execution:
  allow_live_order_submission: false
  enable_spot_fill_readback_hedge: false
risk:
  start_paused_new_entries: true
```

这表示服务可以部署到服务器采集真实行情和资金费率，但不会自动提交实盘订单。

当前实盘闸门有三层：

- `--execution-mode router-live` 只有在 `enable_live_trading=true` 时才允许启动。
- 即使 CLI 是 `router-live`，也必须同时设置 `execution.allow_live_order_submission=true` 和 `execution.enable_spot_fill_readback_hedge=true`，否则执行循环会返回阻断原因，不会提交真实订单。
- 若 `risk.orphan_exposure_blocks_new_entries=true`，则必须有最新 `reconcile` 报告，且 `reconcile.blocks_new_entries=false`；余额、仓位读取失败或残余敞口超过阈值都会阻断新开仓。

maker 后 taker 的当前执行闭环：

1. 先提交 Spot post-only maker buy。
2. Spot ack accepted 后等待 `spot_maker_order_ttl_ms`。
3. TTL 到期后撤 Spot maker 剩余挂单。
4. 用 `query_order` 和 `recent_fills` 回读 Spot 实际成交数量。
5. 只有确认 `filled_qty >= min_hedge_base_qty` 时，才按实际成交数量提交 Perp IOC short hedge。
6. readback、撤单和 hedge 事件写入 `readback_events.jsonl`，并进入 dashboard snapshot 的 `execution_cycle.readback_events`。

当前服务器 dry-run 验证结果：

- `preflight.ready_for_live=true` 且 `missing_requirements=0`。
- `market_scan.reference_prices` 有 12 条 route 参考价，用于残余敞口估值。
- `reconcile.blocks_new_entries=false`。
- 检测到 Bitget WLD 现货微小残余约 `0.012 USDT`，低于 `max_unhedged_spot_notional_usdt` 阈值，因此不阻断。
- 当前仍为 `mode=observe`、`execution_mode=router-dry-run`、`live_orders_enabled=false`，不会提交真实订单。

## 运营原则

- 先活下来，再追年化。
- 小币种只做容量内的收益，不追盘口外的虚假 funding。
- 资金费率收益必须和基差、手续费、滑点一起算净值。
- 负 funding 不是绝对禁用，但临近结算的负 funding 应默认禁开。
- 单腿修复损益必须独立记账。
- 所有人工干预都必须进入 ledger。

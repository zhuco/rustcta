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

## 运营原则

- 先活下来，再追年化。
- 小币种只做容量内的收益，不追盘口外的虚假 funding。
- 资金费率收益必须和基差、手续费、滑点一起算净值。
- 负 funding 不是绝对禁用，但临近结算的负 funding 应默认禁开。
- 单腿修复损益必须独立记账。
- 所有人工干预都必须进入 ledger。

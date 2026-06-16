# 统一套利策略一次性落地方案

状态日期：2026-06-15

## 结论

建议把当前未运行的三类套利能力一次性合并为一个最终策略入口：

```text
strategy_kind: unified_arbitrage
display_name: Unified Arbitrage
default_config: config/unified_arbitrage_usdt.yml
runner: unified-arbitrage-live-runner
logs: logs/unified_arbitrage/
```

统一后不再保留独立的现货合约、跨所合约、资金费率或资金费率扩展策略入口。

跨所现货套利不进入新策略。它的风险核心是现货库存和转账再平衡，和本次要合并的“现货合约、合约跨所、结算前后”不是同一类交易结构。旧的跨所现货策略可以删除运行入口或保留为历史代码，但不作为新策略配置的一部分。

资金费率套利不需要再单独做一个策略。跨所合约价差套利只要在同一条 route 上显示并计算两腿资金费率差，就已经覆盖“资金费率差套利”的主要决策需求。单所结算前后套利作为 `single_exchange_settlement` route 类型放进同一个引擎。

## 统一后的策略能力

新策略只围绕同一个核心抽象运行：`route -> bundle -> leg -> owned_position`。

支持三种 route：

```text
spot_perp_basis
  现货多头 + 永续空头
  收益来源：现货/合约基差、合约空头资金费率、maker 费用优势

perp_perp_spread
  低价交易所永续做多 + 高价交易所永续做空
  收益来源：跨所合约价差收敛、两腿资金费率差、maker/taker 执行优势

single_exchange_settlement
  单交易所结算窗口前开仓，结算后平仓
  收益来源：单个合约结算资金费率
  风险性质：非完全中性，必须小仓位、短持仓、强止损、强爆仓距离约束
```

统一策略保留当前核心开平仓方式：

- maker-taker 开仓。
- dual-taker 开仓。
- dual-taker reduce-only 平仓。
- maker-taker / maker-maker reduce-only 平仓作为可选执行样式。
- 所有 taker 单使用带价格保护的 IOC/FOK 类限价单，不使用裸 market order。

新增拆单开仓和平仓：

```text
parent order -> child slices
```

每个 taker 或 maker-taker hedge 任务可拆成多笔延时提交，降低一次吃盘口造成的滑点。

## 统一数据模型

### InstrumentKey

所有行情、精度、费用、仓位、资金费率都必须按产品维度隔离：

```yaml
instrument_key:
  exchange: bitget
  market_type: perpetual   # spot | perpetual | futures
  symbol: ORDI/USDT
```

不能只用 `exchange + symbol`，否则同一个交易所的现货和永续会混用精度、手续费、仓位。

### Route

```yaml
routes:
  - route_id: ordi_spot_perp_gate_bitget
    enabled: true
    kind: spot_perp_basis
    symbol: ORDI/USDT
    legs:
      spot:
        exchange: gate
        market_type: spot
        side_on_open: buy
        position_side: long
      perp:
        exchange: bitget
        market_type: perpetual
        side_on_open: sell
        position_side: short
        leverage: 3
```

`perp_perp_spread`：

```yaml
  - route_id: drift_binance_bitget_perp
    enabled: true
    kind: perp_perp_spread
    symbol: DRIFT/USDT
    legs:
      long:
        exchange: binance
        market_type: perpetual
        side_on_open: buy
        position_side: long
        leverage: 5
      short:
        exchange: bitget
        market_type: perpetual
        side_on_open: sell
        position_side: short
        leverage: 5
```

`single_exchange_settlement`：

```yaml
  - route_id: soph_bitget_settlement
    enabled: true
    kind: single_exchange_settlement
    symbol: SOPH/USDT
    legs:
      settlement:
        exchange: bitget
        market_type: perpetual
        position_side_policy: receive_funding
        leverage: 2
```

`position_side_policy: receive_funding` 的语义：

```text
funding_rate > 0 -> short before settlement, close after settlement
funding_rate < 0 -> long before settlement, close after settlement
```

这类 route 必须默认 `max_hold_secs` 很短，并强制配置 `min_liquidation_distance_pct`、`stop_loss_bps` 和 `max_settlement_position_notional_usdt`。

## 机会评分模型

### spot_perp_basis

默认方向：

```text
buy spot
sell perp
```

开仓基差：

```text
basis_bps = (perp_bid - spot_ask) / spot_ask * 10000
```

预期净收益：

```text
expected_net_edge_bps =
  basis_capture_bps
+ expected_funding_bps
- open_fee_bps
- close_fee_bps
- taker_slippage_bps
- maker_non_fill_penalty_bps
- latency_buffer_bps
- funding_uncertainty_buffer_bps
- safety_buffer_bps
```

只有 `expected_net_edge_bps >= min_open_net_edge_bps` 才允许开仓。

### perp_perp_spread

默认方向：

```text
低价交易所：buy perp, long
高价交易所：sell perp, short
```

开仓价差：

```text
spread_bps = (short_bid - long_ask) / long_ask * 10000
```

资金费率差只作为 route 的字段和评分因子，不再单独实现“跨所资金费率套利策略”：

```text
funding_diff_bps_per_window =
  funding_income_on_short_leg
+ funding_income_on_long_leg
```

按常见永续规则：

```text
long leg funding pnl  = -notional * funding_rate
short leg funding pnl =  notional * funding_rate
```

所以：

```text
net_funding_bps = (short_funding_rate - long_funding_rate) * 10000
```

Dashboard 必须显示：

- long exchange funding rate
- short exchange funding rate
- funding diff bps
- next funding time
- expected funding pnl

这样合约价差套利天然可以按“价差 + 资金费率差”排序，不需要再保留独立跨所资金费率策略。

### single_exchange_settlement

单所结算前后不是严格中性套利，必须明确风控语义。

开仓窗口：

```text
now >= next_funding_time - open_before_settlement_secs
now <= next_funding_time - min_submit_before_settlement_secs
```

平仓窗口：

```text
now >= next_funding_time + close_after_settlement_secs
```

预期收益：

```text
expected_net_edge_bps =
  abs(expected_funding_rate_bps)
- open_fee_bps
- close_fee_bps
- expected_spread_cost_bps
- slippage_buffer_bps
- adverse_price_move_buffer_bps
```

必须满足：

- `expected_net_edge_bps >= min_settlement_net_edge_bps`
- `abs(funding_rate) >= min_abs_funding_rate_bps`
- `liquidation_distance_pct >= min_liquidation_distance_pct`
- `max_hold_secs` 未超过
- 未触发价格止损和价差止损

## 拆单执行机制

新增统一 `split_execution` 配置：

```yaml
split_execution:
  enabled: true
  min_child_notional_usdt: 5.0
  max_child_notional_usdt: 20.0
  max_child_orders: 10
  child_delay_ms: 250
  child_delay_jitter_ms: 100
  reprice_before_each_child: true
  stop_slicing_if_edge_below_bps: 3.0
  allow_partial_parent_completion: true
  min_parent_fill_ratio: 0.80
```

规则：

- parent 机会只做一次准入检查，child 提交前必须重新检查盘口、深度、余额、仓位上限和价格保护。
- dual-taker 的一个 child 仍然是两腿并发提交。
- maker-taker 的 maker child 成交后，只对实际成交数量提交 hedge child。
- 如果某个 child 之后净收益低于阈值，停止剩余 child，不停止策略。
- parent 未完全成交但超过 `min_parent_fill_ratio` 可以保留为已开仓 bundle。

## 数量不完全一致的处理

不能因为两腿对冲数量不完全一致就停止整个策略。

统一处理为：

```text
matched_qty     -> 正常策略仓位
residual_qty    -> residual exposure
residual_value  -> residual notional
```

配置：

```yaml
hedge_tolerance:
  max_leg_imbalance_base: 0.000001
  max_leg_imbalance_usdt: 1.0
  repair_above_imbalance_usdt: 2.0
  block_new_entries_above_imbalance_usdt: 5.0
  emergency_close_above_imbalance_usdt: 10.0
```

行为：

- 小于 `max_leg_imbalance_usdt`：记录残差，不阻断。
- 大于 `repair_above_imbalance_usdt`：生成 repair task，补 hedge 或 reduce-only 减仓。
- 大于 `block_new_entries_above_imbalance_usdt`：该 route 或 symbol close-only。
- 大于 `emergency_close_above_imbalance_usdt`：强制 reduce-only 风险平仓。

## 仓位归属和多策略同交易对管理

即使统一为一个策略，也要允许同一个交易对被多个策略实例或多条 route 同时管理。核心是归属隔离。

每个订单必须写入：

```text
strategy_kind
strategy_id
run_id
route_id
bundle_id
leg_id
child_index
position_owner_key
```

`position_owner_key`：

```text
tenant_id/account_id/strategy_id/run_id/route_id/bundle_id/leg_id
```

仓位统计分两层：

```text
account exposure
  交易所账户真实总仓位，用于风险和爆仓距离

owned exposure
  本策略、本 route、本 bundle 负责管理的数量
```

同一个 symbol 上的外部仓位不能直接归入本策略。启动时要做：

- 已带有本策略 `position_owner_key` 的仓位：恢复为 owned bundle。
- 没有 owner 的同 symbol 仓位：标记 external exposure。
- external exposure 默认阻断该 symbol 新开仓，除非配置显式允许并把外部仓位纳入风险上限。

## 实时输出字段

策略必须在行情事件、私有成交事件、账户/仓位同步事件、定时器事件后更新 snapshot。控制台读取同一份 read model，不进入交易热路径。

每个 route 输出：

```yaml
route_snapshot:
  route_id: drift_binance_bitget_perp
  kind: perp_perp_spread
  symbol: DRIFT/USDT
  status: open
  open_execution_style: dual_taker
  close_execution_style: dual_taker_reduce_only
  spread_bps: 42.5
  funding_diff_bps: 8.2
  next_funding_time: "2026-06-15T16:00:00Z"
  expected_funding_pnl_usdt: 0.18
  position_value_usdt: 52.40
  quantity_base: 18.0
  quantity_contracts:
    binance: 18.0
    bitget: 18.0
  avg_entry_cost:
    long_leg: 2.904
    short_leg: 2.928
  mark_value_usdt:
    long_leg: 52.12
    short_leg: 52.58
  unrealized_pnl_usdt: 0.22
  realized_pnl_usdt: 0.00
  funding_pnl_usdt: 0.04
  fee_pnl_usdt: -0.06
  net_pnl_usdt: 0.16
  pnl_bps: 30.5
  liquidation:
    nearest_exchange: binance
    nearest_liquidation_price: 2.120
    liquidation_distance_pct: 27.0
    maintenance_margin_rate: 0.006
    adl_level: null
  hedge:
    matched_base_qty: 18.0
    residual_base_qty: 0.0
    residual_notional_usdt: 0.0
  age_secs: 1420
  max_hold_secs: 86400
```

每个 leg 输出：

```yaml
leg_snapshot:
  exchange: bitget
  market_type: perpetual
  position_side: short
  side_on_open: sell
  leverage: 5
  quantity_base: 18.0
  quantity_contracts: 18.0
  position_value_usdt: 52.58
  avg_entry_price: 2.928
  mark_price: 2.921
  index_price: 2.919
  liquidation_price: 3.740
  liquidation_distance_pct: 28.0
  unrealized_pnl_usdt: 0.13
  realized_pnl_usdt: 0.0
  funding_rate: 0.00042
  predicted_funding_rate: 0.00038
  next_funding_time: "2026-06-15T16:00:00Z"
  margin_mode: isolated
  margin_used_usdt: 10.52
  maintenance_margin_usdt: 0.31
```

Dashboard 顶层输出：

```yaml
summary:
  open_routes: 4
  open_bundles: 8
  position_value_usdt: 240.0
  gross_exposure_usdt: 480.0
  net_exposure_usdt: 3.2
  unrealized_pnl_usdt: 1.8
  realized_pnl_usdt_today: 0.6
  funding_pnl_usdt_today: 0.4
  fee_usdt_today: 0.2
  nearest_liquidation_distance_pct: 22.5
  routes_in_close_only: 1
  routes_requiring_repair: 0
```

## 一键开仓和平仓接口预留

控制台后续可以直接调用 operator command，不需要再改策略协议。

### 一键开仓

```yaml
command_kind: manual_open_route
payload:
  route_id: drift_binance_bitget_perp
  notional_usdt: 20.0
  execution_style: dual_taker
  use_split_execution: true
  max_slippage_bps: 10.0
  require_current_signal: true
  dry_run_preview: false
  operator_confirmation_id: "confirm-..."
```

行为：

- 必须经过和自动开仓相同的准入检查。
- `require_current_signal=true` 时，当前机会低于阈值则拒绝。
- `dry_run_preview=true` 只返回预计订单、预计成交价格、手续费、风险和爆仓距离，不提交订单。

### 一键平仓

```yaml
command_kind: manual_close
payload:
  scope: route        # bundle | route | symbol | all
  route_id: drift_binance_bitget_perp
  bundle_id: null
  reason: operator_request
  execution_style: dual_taker_reduce_only
  reduce_only: true
  close_residual_only: false
  max_slippage_bps: 15.0
  dry_run_preview: false
  operator_confirmation_id: "confirm-..."
```

行为：

- 必须 reduce-only。
- 可以按 bundle、route、symbol、all 平仓。
- 平仓前输出预计成交、预计费用、预计 PnL、爆仓距离变化。
- 如果一腿关闭成功、另一腿失败，自动进入 repair / emergency close 流程。

### 其他控制命令

```text
pause_new_entries
resume_new_entries
set_route_close_only
clear_route_close_only
cancel_route_orders
repair_route_hedge
set_route_limits
reload_config
```

所有命令必须写入审计日志，包含 operator、命令原文、dry-run 预览、执行结果和失败原因。

## 新配置接口

统一配置文件建议如下：

```yaml
strategy_kind: unified_arbitrage
mode: live_small
trading_mode: paper
enable_live_trading: false

account:
  tenant_id: local
  account_id: unified_arbitrage
  require_private_ws: true
  require_position_readback: true

market:
  quote_asset: USDT
  stale_book_ms: 1000
  depth_levels: 5
  public_book_speed: fastest
  top_of_book_capacity_ratio: 0.8
  funding_snapshot_max_age_ms: 60000

defaults:
  sizing:
    min_order_notional_usdt: 5.0
    max_order_notional_usdt: 20.0
    max_position_notional_usdt: 100.0
    max_route_notional_usdt: 100.0
    max_symbol_notional_usdt: 200.0
    max_exchange_notional_usdt: 300.0
    max_total_notional_usdt: 500.0
  leverage:
    default_perp_leverage: 3
    max_perp_leverage: 10
    margin_mode: isolated
  thresholds:
    min_open_spread_bps: 20.0
    max_open_spread_bps: 1000.0
    min_open_net_edge_bps: 5.0
    close_spread_bps: 5.0
    take_profit_bps: 10.0
    stop_loss_bps: 30.0
    alert_spread_above_bps: 100.0
    alert_spread_below_bps: -20.0
  funding:
    expected_holding_hours: 8.0
    use_predicted_funding: true
    funding_uncertainty_buffer_bps: 5.0
    close_if_funding_edge_below_bps: -5.0
  execution:
    preferred_open_style: dual_taker
    allowed_open_styles: [dual_taker, maker_taker]
    preferred_close_style: dual_taker_reduce_only
    allowed_close_styles: [dual_taker_reduce_only, maker_taker_reduce_only]
    taker_slippage_bps: 10.0
    maker_price_offset_bps: 5.0
    maker_order_timeout_ms: 3000
    allow_market_order: false
  split_execution:
    enabled: true
    min_child_notional_usdt: 5.0
    max_child_notional_usdt: 20.0
    max_child_orders: 10
    child_delay_ms: 250
    child_delay_jitter_ms: 100
    reprice_before_each_child: true
    stop_slicing_if_edge_below_bps: 3.0
    allow_partial_parent_completion: true
    min_parent_fill_ratio: 0.80
  hedge_tolerance:
    max_leg_imbalance_usdt: 1.0
    repair_above_imbalance_usdt: 2.0
    block_new_entries_above_imbalance_usdt: 5.0
    emergency_close_above_imbalance_usdt: 10.0
  risk:
    start_close_only: false
    max_open_bundles: 20
    max_active_bundles_per_symbol: 3
    min_liquidation_distance_pct: 25.0
    max_hold_secs: 86400
    max_daily_loss_usdt: 100.0
    block_on_external_account_exposure: true
    orphan_exposure_blocks_new_entries: true

routes:
  - route_id: ordi_gate_bitget_spot_perp
    enabled: true
    kind: spot_perp_basis
    symbol: ORDI/USDT
    sizing:
      max_position_notional_usdt: 50.0
    legs:
      spot:
        exchange: gate
        market_type: spot
        side_on_open: buy
        position_side: long
      perp:
        exchange: bitget
        market_type: perpetual
        side_on_open: sell
        position_side: short
        leverage: 3

  - route_id: drift_binance_bitget_perp
    enabled: true
    kind: perp_perp_spread
    symbol: DRIFT/USDT
    thresholds:
      min_open_spread_bps: 30.0
      close_spread_bps: 5.0
    legs:
      long:
        exchange: binance
        market_type: perpetual
        side_on_open: buy
        position_side: long
        leverage: 5
      short:
        exchange: bitget
        market_type: perpetual
        side_on_open: sell
        position_side: short
        leverage: 5

  - route_id: soph_bitget_settlement
    enabled: false
    kind: single_exchange_settlement
    symbol: SOPH/USDT
    settlement:
      open_before_settlement_secs: 20
      min_submit_before_settlement_secs: 3
      close_after_settlement_secs: 5
      min_abs_funding_rate_bps: 20.0
      min_settlement_net_edge_bps: 5.0
      max_hold_secs: 120
      max_settlement_position_notional_usdt: 10.0
    legs:
      settlement:
        exchange: bitget
        market_type: perpetual
        position_side_policy: receive_funding
        leverage: 2

alerts:
  enabled: true
  spread_above_bps: 100.0
  spread_below_bps: -20.0
  liquidation_distance_below_pct: 25.0
  pnl_loss_below_usdt: -20.0

persistence:
  jsonl_dir: logs/unified_arbitrage
  trade_ledger_path: logs/unified_arbitrage/trade_events.jsonl
  dashboard_snapshot_path: logs/unified_arbitrage/dashboard.json
  clickhouse_enabled: false

control:
  enable_manual_open: true
  enable_manual_close: true
  manual_commands_require_confirmation: true
  manual_open_requires_current_signal: true
```

配置解析规则：

- `defaults` 是全局默认值。
- `routes[*]` 可以覆盖 `sizing`、`thresholds`、`funding`、`execution`、`split_execution`、`risk`。
- 所有 route 继承统一风控，再叠加 route 级风控。
- 删除旧配置里按策略分散的 `dual_taker`、`slippage_capture`、`selection`、`adding` 顶层结构，统一收敛到 route override。

## 运行和代码组织

一次性落地后的目录建议：

```text
strategies/unified-arbitrage/
  src/core.rs
  src/config.rs
  src/runtime_contract.rs
  src/app_runtime.rs
  src/execution_plan.rs
  src/position_read_model.rs
  src/pnl.rs
  src/control_commands.rs

crates/rustcta-unified-arb-live-runner/
  src/lib.rs

config/unified_arbitrage_usdt.yml
config/supervisor/unified_arb_live.spec.json
config/systemd/user/unified-arb-live.service
```

旧 runner 和 supervisor spec 不再作为生产入口；控制台只展示
`unified_arbitrage`。

## 验收标准

一次性交付完成时必须满足：

- 一个策略入口能解析 `spot_perp_basis`、`perp_perp_spread`、`single_exchange_settlement` 三类 route。
- 一个配置文件能表达所有 route，不再需要三套策略配置。
- 同一 symbol 可被多条 route 管理，但每条 route 只管理自己的 `position_owner_key` 数量。
- 每个 route 实时输出仓位价值、PnL、持仓成本、持仓数量、资金费率、资金费率差、爆仓价和爆仓距离。
- 支持 maker-taker 和 dual-taker 开仓，支持 reduce-only 平仓。
- 支持 taker 拆单延时递交。
- 数量不一致时进入 residual/repair 状态，不停止整个策略。
- 预留并测试 `manual_open_route` 和 `manual_close` operator command schema。
- 单所结算前后 route 有独立强风控，不和中性套利混淆。
- 旧 funding-rate 套利不再单独运行，资金费率差显示在 `perp_perp_spread` route 上。
- 控制台只需要消费统一 dashboard snapshot 和统一 operator command 接口。

## 关键风险

1. 单所结算前后套利不是市场中性策略，必须默认关闭，只有在小仓位、短窗口、爆仓距离充足时启用。
2. 合并后配置能力更强，必须加强 schema 校验，避免 route 写错 leg 方向。
3. 现货和永续必须按 `InstrumentKey` 分离，否则会出现精度、手续费、仓位归属错误。
4. 一键开仓不能绕过自动开仓风控，否则控制台会成为最大风险入口。
5. 多 route 同 symbol 时，仓位归属必须依赖订单/成交 ledger 的 owner key，不能用账户净仓位反推。

## 最终取舍

最终形态应是一个统一策略，而不是三个策略加一层 UI 聚合：

```text
统一机会模型
统一执行模型
统一仓位归属
统一 PnL/read model
统一控制台命令
统一配置 schema
```

这样后续扩展新套利类型时，只新增 route kind，不再新增独立策略、独立 runner、独立 dashboard 和独立风控口径。

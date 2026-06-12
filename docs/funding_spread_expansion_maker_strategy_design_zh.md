# 资金费率 + 价差扩张 Maker 加仓策略开发文档

状态日期：2026-06-12

## 一、结论

本策略建议作为新的独立策略实现：

```text
strategy_kind: funding_spread_expansion_maker
display_name: Funding Spread Expansion Maker
crate: strategies/funding-spread-expansion-maker
```

不要直接改造成现有 `cross_exchange_arbitrage`。

原因：

- 现有 `cross_exchange_arbitrage` 的核心目标是跨所价差即时套利，已有
  `dual_taker` 和 `slippage_capture` maker-taker 开仓模块；它强调快速开仓、
  快速平仓、价差收敛或锁利润。
- 本策略的核心目标是长期持有两个交易所同一 USDT 永续合约的反向仓位，
  先用资金费率差提供持仓理由，再等待价差继续向目标方向扩张后自动平仓。
- 本策略允许负的即时开仓毛利，且默认不因为浮亏平仓；这与现有快速价差套利
  的 admission、PnL、风控和退出语义不同。
- 本策略需要 route 级慢速加仓、资金费率持久性过滤、价差扩张目标平仓、
  funding PnL 归因、长期持仓风险看板，适合独立配置、独立 runner、独立
  snapshot 和独立账本。

可以复用现有能力：

- 复用 `cross_exchange_arbitrage` 已有的 maker-taker 下单模型、双 taker
  reduce-only 平仓模型、base quantity 对齐、单腿 repair、precision 规则。
- 复用 `funding-arbitrage` 已有的 funding snapshot、settlement time、funding
  selection 思路。
- 复用交易所网关、执行路由、私有流确认、REST readback、fee model、risk gate、
  idempotency identity。

不能复用的方式：

- 新策略 crate 不应直接依赖另一个 strategy crate。
- 若有共享类型，应先抽到 `rustcta-strategy-sdk` 或策略通用库，再由两个策略消费。
- 策略层不得写交易所 REST/WebSocket endpoint、签名逻辑或读取 `.env`。

## 二、策略一句话

在两个交易所的同一 USDT 永续合约之间建立等 base 数量、反方向的对冲仓位。
资金费率净收益作为入场和持仓基础；当跨所价差达到配置的开仓方向阈值后，
用可配置的 maker-taker、dual-maker 或受限 taker 方式慢速建仓；当价差继续扩张
到目标清仓阈值时，优先使用低成本 reduce-only maker 或 maker-taker 平仓，必要时
fallback 到双 taker reduce-only，兑现资金费率收益和价差扩张收益。

收益模型：

```text
total_pnl =
  cumulative_funding_pnl
+ realized_spread_expansion_pnl
+ maker_rebate_or_negative_fee
- open_maker_fee_or_rebate
- open_taker_or_hedge_fee
- close_maker_fee_or_rebate
- close_taker_fee
- hedge_slippage
- close_slippage
- single_leg_repair_cost
```

允许负毛利建仓的含义：

```text
immediate_open_edge_after_fee 可以小于 0
但 expected_total_edge_after_funding_and_target_spread 必须 >= min_expected_total_edge_pct
```

默认不做浮亏止损：

```text
mark_to_market_unrealized_loss 不是普通平仓触发条件
```

但以下风险仍可触发 close-only 或 emergency close：

- 保证金风险。
- 爆仓距离过近。
- ADL/MMR 超限。
- 资金费率净收益显著反转。
- 单腿暴露。
- 订单状态 unknown。
- kill switch。

## 三、产品形态参考

外部产品 `https://pulse.astro-btc.xyz/` 可作为产品形态参考：多交易所、多交易对
监控资金费率差和价差，再选择指定交易对开仓。当前公开页面在未登录或无完整
浏览器上下文下只返回加载页，因此本文档不把该页面内容作为可验证交易规则；
策略规则以本仓库边界和用户提供的截图解析为准。

本策略应支持两种入口：

- `configured_route`：配置中直接指定交易所、symbol、方向和仓位上限。
- `scanner_selected_route`：只读 scanner 发现符合长期 funding + spread 条件的
  route 后，运行时按 allowlist 自动交易。

所谓“一键开仓”在 RustCTA 中应表达为：

```text
operator enables a route
strategy automatically evaluates, opens, adds, repairs and closes
```

不能表达为绕过策略风控直接向交易所下单。

## 四、第一阶段范围

第一阶段只做：

- USDT 本位线性永续合约。
- 两个交易所之间的同一 canonical symbol。
- 一边 long，一边 short，目标净 base 数量接近 0。
- 开仓支持 `maker_taker`、`dual_maker`、`dual_taker` 和 `adaptive_open`。
- `maker_taker` 中 maker 成交后立即 taker hedge。
- `dual_maker` 可用于更低成本建仓，但必须有更严格的单腿窗口、撤单和 repair。
- 目标平仓支持 `maker_maker_reduce_only`、`maker_taker_reduce_only`、
  `dual_taker_reduce_only` 和 `adaptive_close`。
- 风险平仓必须支持 `dual_taker_reduce_only` fallback，使用带价格保护的 IOC/FOK
  limit。
- 资金费率是 admission 和 hold filter。
- 价差扩张是主要自动平仓目标。
- 支持慢速分片加仓。
- 支持负即时开仓毛利。
- 支持浮亏不平仓。

第一阶段不做：

- 现货腿。
- 币本位合约。
- 交割合约和期权。
- 三角套利。
- 跨链转账。
- 借币做空。
- 裸仓。
- 未对冲单腿持有。
- 未通过 SDK/execution provider 的下单。
- 策略 crate 中的交易所 endpoint、签名或 credential 处理。

## 五、关键概念

### Leg

```text
leg_a = direction.leg_a_exchange
leg_b = direction.leg_b_exchange
symbol = canonical_symbol
```

两腿必须是同一个 canonical symbol，例如：

```text
HUSDT/USDT on mexc
HUSDT/USDT on bybit
```

### Spread

统一价差公式：

```text
spread_pct = (leg_b_price - leg_a_price) / leg_a_price
```

必须保存方向和符号，不能只用绝对值。

示例：

```text
leg_a_exchange = bitget
leg_b_exchange = binance
leg_a_price = 0.1558
leg_b_price = 0.1486

spread_pct = (0.1486 - 0.1558) / 0.1558 = -4.62%
```

这与截图中的 `实开 -4.69%` 接近，说明截图里的负价差可理解为：

```text
binance 相对 bitget 更便宜
或 bitget 相对 binance 更贵
```

### Target Direction

```text
target_direction = decrease
  目标是 spread_pct 继续下降，例如 -4.69% -> -8.00%

target_direction = increase
  目标是 spread_pct 继续上升，例如 +4.69% -> +8.00%
```

### Position Direction

当目标是 spread 继续下降：

```text
long leg_a
short leg_b
```

当目标是 spread 继续上升：

```text
long leg_b
short leg_a
```

这样价差向目标方向扩张时，价格 PnL 为正。

### Funding

永续资金费率方向：

```text
funding_rate > 0:
  long 通常付费
  short 通常收钱

funding_rate < 0:
  long 通常收钱
  short 通常付费
```

交易所返回的资金费率周期可能不同，例如同一交易对在一个交易所是 1 小时结算，另一个交易所是 4 小时或 8 小时结算。策略不得直接把原始费率相加，必须先归一化到每小时费率：

```text
long_rate_per_hour = long_exchange_funding_rate / long_funding_interval_hours
short_rate_per_hour = short_exchange_funding_rate / short_funding_interval_hours

net_funding_rate_per_hour =
  -long_rate_per_hour
+  short_rate_per_hour

net_funding_rate =
  net_funding_rate_per_hour * funding_rate_basis_hours
```

其中 `funding_rate_basis_hours` 是配置里的比较基准，默认 8 小时。`min_net_funding_rate` 和 `max_adverse_funding_rate` 都按这个基准后的净费率判断。

名义金额维度的结算收益：

```text
long_leg_funding = -notional * long_exchange_funding_rate
short_leg_funding =  notional * short_exchange_funding_rate
net_funding = long_leg_funding + short_leg_funding
```

入场前必须计算：

```text
expected_net_funding_rate >= min_net_funding_rate
```

若配置允许负即时毛利，则还必须计算：

```text
expected_total_edge_pct =
  expected_target_spread_edge_pct
+ expected_funding_edge_pct
+ maker_rebate_edge_pct
- open_fee_pct
- hedge_fee_pct
- close_fee_pct
- slippage_buffer_pct
- funding_uncertainty_buffer_pct
- repair_buffer_pct

expected_total_edge_pct >= min_expected_total_edge_pct
```

`expected_funding_edge_pct` 使用每小时净费率和预期持仓小时数计算：

```text
expected_funding_edge_pct =
  net_funding_rate_per_hour
* expected_funding_horizon_hours
* (1 - funding_decay_haircut_pct)
```

如果 `expected_funding_horizon_hours <= 0`，则回退为：

```text
funding_rate_basis_hours * expected_funding_windows
```

## 六、方向策略

每条 route 必须固定一个方向策略。

```yaml
direction:
  spread_formula: leg_b_minus_leg_a_over_leg_a
  leg_a_exchange: mexc
  leg_b_exchange: bybit
  target_direction: decrease
  direction_policy: balanced
```

### funding_first

只允许资金费率净收益为正且超过阈值的方向。

适用场景：

- 主要赚 funding。
- 价差扩张只是额外收益。
- 允许较长持仓时间。

约束：

```text
net_funding_rate >= min_net_funding_rate
```

若价差方向与 funding 最优方向冲突，拒绝开仓。

### spread_first

主要押价差继续扩张，允许 funding 小幅为负，但必须由价差目标收益覆盖。

约束：

```text
expected_target_spread_edge_pct + expected_funding_edge_pct - cost_buffer
  >= min_expected_total_edge_pct
```

### balanced

推荐默认模式。资金费率提供持仓理由，价差扩张提供目标平仓收益。

约束：

```text
net_funding_rate >= min_net_funding_rate
expected_total_edge_pct >= min_expected_total_edge_pct
spread_direction_matched
```

截图中的策略更接近 `balanced`：

- 有资金费率差。
- 有明确开仓价差。
- 有明确目标清仓价差。
- 可以慢速加仓。
- 不是立即吃掉价差收敛。

## 七、开仓逻辑

### 开仓 Admission

开仓必须同时满足：

```text
market_ready
&& private_stream_ready
&& precision_rules_ready
&& fee_model_ready
&& funding_snapshot_ready
&& account_position_ready
&& no_existing_unmanaged_position
&& no_pending_repair
&& no_unknown_order_state
&& symbol_not_cooling_down
&& current_position_notional < max_position_notional_usdt
&& spread_direction_matched
&& spread_open_threshold_met
&& net_funding_rate >= min_net_funding_rate
&& expected_total_edge_pct >= min_expected_total_edge_pct
&& mmr_pct <= max_mmr_pct
&& adl_pct <= max_adl_pct
&& liquidation_buffer_pct >= min_liquidation_buffer_pct
```

### Spread 阈值

当 `target_direction = decrease`：

```text
open allowed when spread_pct <= open_spread_pct
target close when spread_pct <= target_close_spread_pct
```

示例：

```text
open_spread_pct = -0.0300
target_close_spread_pct = -0.0800
actual_open_spread_pct = -0.0469

-4.69% <= -3.00%  -> 允许开仓
-8.01% <= -8.00%  -> 触发目标平仓
-6.00% <= -8.00%  -> 不平仓
```

当 `target_direction = increase`：

```text
open allowed when spread_pct >= open_spread_pct
target close when spread_pct >= target_close_spread_pct
```

### 负毛利开仓

本策略不得使用现有快速套利的规则：

```text
expected_open_net_profit_pct >= 0
```

替代规则：

```text
immediate_open_edge_pct >= min_immediate_open_edge_pct
expected_total_edge_pct >= min_expected_total_edge_pct
```

其中：

```yaml
thresholds:
  allow_negative_immediate_edge: true
  min_immediate_open_edge_pct: -0.0050
  min_expected_total_edge_pct: 0.0010
```

`immediate_open_edge_pct` 可以为负；但如果资金费率预期和目标价差收益都覆盖不了
手续费、滑点和 repair buffer，仍然拒绝开仓。

### Maker Leg 选择

对同一个目标持仓方向，maker leg 可以有两种：

当目标持仓为 `long leg_a / short leg_b`：

```text
方案 1:
  maker buy leg_a
  taker sell leg_b after maker fill

方案 2:
  maker sell leg_b
  taker buy leg_a after maker fill
```

策略应评估两种方案，选择：

- 预估总 edge 更高。
- maker fee/rebate 更好。
- 队列风险更低。
- top-of-book 或近端盘口容量更足。
- hedge leg 流动性更可靠。

开仓执行方式必须是可配置的，不应写死为 maker-taker：

```text
open_style = maker_taker
  maker_order = post_only limit
  hedge_order = IOC/FOK protected limit after maker fill

open_style = dual_maker
  both legs = post_only limit
  both orders require strict timeout, cancel and repair

open_style = dual_taker
  both legs = protected IOC/FOK limit
  only for explicit config, urgent entry or fallback testing

open_style = adaptive_open
  choose maker_taker or dual_maker by cost, depth, queue risk and hedge reliability
  dual_taker only if allowed by config
```

推荐第一阶段 live 默认：

```text
preferred_open_style = maker_taker
allowed_open_styles = [maker_taker, dual_maker]
emergency_open_style = none
```

原因：

- maker-taker 成本低于双 taker，且比 dual-maker 更容易控制单腿。
- dual-maker 成本最低，但两边成交不同步的风险最大。
- dual-taker 不适合本策略的长期低成本建仓目标，只适合明确配置或测试。

禁止：

```text
裸 market order
maker-taker 模式下 maker 未成交时预先提交 hedge
private stream 未 ready 时开仓
订单 unknown 时继续开新仓
```

## 八、成交后是否立即加库存

答案：不要在 maker 成交后无条件立即加库存。

正确流程：

```text
maker filled
-> hedge taker submitted
-> hedge filled or repaired
-> route_position becomes hedged/open
-> run add evaluator again
-> only add if signal still qualifies
```

加仓是开仓过滤条件的复用，不是成交回调里的机械补单。

支持三种加仓模式：

### ladder_on_spread

推荐默认模式。只有价差向目标方向继续移动至少一个 ladder step，才允许下一次加仓。

```text
target_direction = decrease:
  spread_pct <= last_add_spread_pct - add_step_pct

target_direction = increase:
  spread_pct >= last_add_spread_pct + add_step_pct
```

优点：

- 不会在 maker 成交后立刻连续补库存。
- 越接近目标方向，仓位越大。
- 与“博价差继续扩张”一致。

### steady_funding

只要 funding 仍持续有利、spread 仍满足开仓阈值、仓位未满，就按时间间隔慢慢加。

```text
now - last_add_at >= min_add_interval_secs
spread_open_threshold_met
net_funding_rate >= min_net_funding_rate
```

适用：

- 主要赚资金费率。
- 价差短期可能缩小，但 funding 差有持续性。

### hybrid

同时允许 spread ladder 和 funding interval 两种触发，但必须满足：

```text
spread_open_threshold_met
not target_close_reached
risk_ok
```

第一阶段建议默认：

```yaml
adding:
  add_mode: ladder_on_spread
```

如果后续实盘证明 funding 差稳定，可再启用 `steady_funding`。

## 九、慢速 Maker 加仓逻辑

### 加仓触发

```text
route_state in [open, partially_open]
&& no_pending_maker_order
&& no_pending_hedge_order
&& no_pending_repair
&& no_unknown_order_state
&& current_position_notional < max_position_notional_usdt
&& spread_open_threshold_met
&& target_close_not_reached
&& net_funding_rate >= min_net_funding_rate
&& expected_total_edge_pct >= min_expected_total_edge_pct
&& leg_imbalance_within_limit
&& risk_ok
```

### 每次加仓金额

```text
remaining_notional =
  max_position_notional_usdt - current_position_notional_usdt

next_order_notional =
  min(max_order_notional_usdt, remaining_notional)

if next_order_notional < min_order_notional_usdt:
  reject add
```

截图语义：

```text
current_position_notional_usdt ~= 8
max_position_notional_usdt = 20
order_notional_range = 8..10

remaining = 12
next_order_notional = min(10, 12) = 10
```

如果：

```text
current_position_notional_usdt = 18
max_position_notional_usdt = 20
min_order_notional_usdt = 8
```

则：

```text
remaining = 2
next_order_notional = 2 < 8
不加仓
```

### 双腿偏差处理

```text
base_qty_delta = abs(long_base_qty - short_base_qty)
notional_delta = abs(long_notional_usdt - short_notional_usdt)
```

如果：

```text
notional_delta > max_leg_imbalance_usdt
或 base_qty_delta > max_leg_imbalance_base_qty
```

则：

```text
暂停新增方向性加仓
进入 rebalance_or_repair
优先补齐较小一腿或 reduce-only 缩小较大一腿
```

### Maker 挂单管理

```text
post_only = true
reduce_only = false
auto_cancel_after_ms = maker_order_timeout_ms
max_concurrent_maker_orders_per_route = 1
maker_price_refresh_on_book_move = true
cancel_if_spread_no_longer_qualifies = true
cancel_if_funding_reverses = true
cancel_if_risk_degrades = true
```

若 maker partial fill：

```text
hedge only filled_base_quantity
cancel residual maker quantity if timeout or signal invalid
```

## 十、平仓逻辑

### 正常目标平仓

当 `target_direction = decrease`：

```text
if spread_pct <= target_close_spread_pct:
  close
```

当 `target_direction = increase`：

```text
if spread_pct >= target_close_spread_pct:
  close
```

默认使用绝对价差阈值，因为截图表达的是：

```text
开仓条件: -3.00%
清仓条件: -8.00%
实开: -4.69%
```

也可以支持相对开仓均值：

```yaml
close:
  close_trigger_mode: absolute_spread
  # optional: spread_move_from_weighted_open
```

### 平仓执行模式

目标平仓不应只支持双 taker。正常达到目标价差时，策略可以优先选择更低成本的
maker 平仓方式；但所有 maker 平仓都必须有撤单、超时、单腿 repair 和 taker
fallback。

```text
close_style = maker_maker_reduce_only
  close long leg = sell post-only reduce-only
  close short leg = buy post-only reduce-only
  lowest fee / possible rebate
  highest asynchronous fill risk

close_style = maker_taker_reduce_only
  first close one leg with post-only reduce-only maker
  after maker fill, close the other leg with protected taker reduce-only
  lower cost than dual taker
  lower asynchronous risk than dual maker

close_style = dual_taker_reduce_only
  close long leg = sell reduce-only protected IOC/FOK limit
  close short leg = buy reduce-only protected IOC/FOK limit
  highest cost
  fastest and safest exit

close_style = adaptive_close
  target close: try maker_maker or maker_taker if risk allows
  timeout / signal invalid / risk degraded: fallback dual_taker_reduce_only
  risk close: use dual_taker_reduce_only directly
```

平仓前检查：

```text
both_books_fresh
&& both_private_streams_ready
&& close_quantity > 0
&& reduce_only_supported_or_platform_can_enforce
&& estimated_close_slippage_pct <= max_close_slippage_pct
```

`maker_maker_reduce_only` 额外要求：

```text
both reduce_only post-only orders supported
target spread still valid after price offset
max_close_maker_wait_ms configured
max_close_single_leg_exposure_ms configured
cancel_if_close_signal_invalid = true
fallback_close_style = dual_taker_reduce_only
```

`maker_taker_reduce_only` 额外要求：

```text
maker close leg selected by fee/rebate, depth and queue risk
after maker close fill, taker close opposite leg immediately
partial maker close only hedges filled_base_quantity
residual maker quantity is canceled on timeout or signal invalid
```

如果目标平仓一腿成交、一腿失败：

```text
enter repairing_single_leg
stop new opens
retry reduce-only close or restore hedge according to repair policy
```

### 浮亏不平仓

以下情况默认不触发普通平仓：

```text
spread moves against target
mark-to-market unrealized PnL < 0
open spread becomes less favorable
price gap temporarily narrows after maker-taker open
```

继续持有的条件：

```text
net_funding_rate still acceptable
risk_ok
no single-leg exposure
no unknown order state
margin buffer sufficient
```

### 风险平仓

任一条件成立时进入 close-only 或 emergency close：

```text
net_funding_rate < -max_adverse_funding_rate
mmr_pct > max_mmr_pct
adl_pct > max_adl_pct
liquidation_buffer_pct < min_liquidation_buffer_pct
single_leg_exposure_ms > single_leg_timeout_ms
unknown_order_state_ms > unknown_order_timeout_ms
funding_snapshot_age_ms > max_funding_snapshot_age_ms
private_stream_stale
manual_kill_switch
```

`max_hold_secs` 第一阶段应设计成可选：

```yaml
risk:
  max_hold_secs: null
```

如果配置为 `null`，不因持仓时间自动平仓；如果配置为数字，则超过后进入
`close_only_pending_target_or_risk`，是否强平由配置控制。

## 十一、状态机

推荐 route 级状态：

```text
observing
open_signal
maker_open_pending
maker_partially_filled
hedge_pending
open
add_signal
add_pending
target_close_ready
closing
closed
repairing_single_leg
rebalancing_imbalance
close_only
risk_stopped
```

状态转移：

```text
observing
  -> open_signal
  -> maker_open_pending
  -> maker_partially_filled
  -> hedge_pending
  -> open

open
  -> add_signal
  -> add_pending
  -> open

open
  -> target_close_ready
  -> closing
  -> closed

任意交易状态
  -> repairing_single_leg
  -> close_only
  -> open 或 closed

任意状态
  -> risk_stopped
```

硬约束：

- `maker_open_pending` 时，不允许同 route 新增 maker order。
- `hedge_pending` 时，不允许继续加仓。
- `repairing_single_leg` 时，该 symbol 进入 close-only。
- `unknown_order_state` 未解决时，禁止新开仓和加仓。
- 平仓完成后进入 symbol cooldown。

## 十二、核心数据模型

### RouteKey

```rust
pub struct FundingSpreadRouteKey {
    pub leg_a_exchange: ExchangeId,
    pub leg_b_exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
}
```

### RouteConfig

```rust
pub struct FundingSpreadRouteConfig {
    pub key: FundingSpreadRouteKey,
    pub target_direction: SpreadTargetDirection,
    pub direction_policy: DirectionPolicy,
    pub open_spread_pct: f64,
    pub target_close_spread_pct: f64,
    pub min_net_funding_rate: f64,
    pub max_position_notional_usdt: f64,
}
```

### RoutePosition

加仓后应聚合成一个 route position，同时保留每次 slice。

```rust
pub struct FundingSpreadRoutePosition {
    pub route_id: String,
    pub state: RouteState,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub long_base_quantity: f64,
    pub short_base_quantity: f64,
    pub long_avg_entry_price: f64,
    pub short_avg_entry_price: f64,
    pub weighted_open_spread_pct: f64,
    pub current_spread_pct: f64,
    pub current_notional_usdt: f64,
    pub cumulative_funding_pnl_usdt: f64,
    pub cumulative_fee_usdt: f64,
    pub opened_at: DateTime<Utc>,
    pub last_add_at: Option<DateTime<Utc>>,
    pub last_add_spread_pct: Option<f64>,
}
```

### Slice

每次开仓或加仓记录为 slice。`slice` 必须能表达 maker-taker、dual-maker 和
受限 dual-taker，不要把字段命名写死成单一 maker-taker 模式。

```rust
pub struct FundingSpreadSlice {
    pub slice_id: String,
    pub route_id: String,
    pub state: SliceState,
    pub target_notional_usdt: f64,
    pub filled_base_quantity: f64,
    pub maker_exchange: ExchangeId,
    pub hedge_exchange: ExchangeId,
    pub maker_role: MakerLegKind,
    pub maker_order_id: Option<String>,
    pub hedge_order_id: Option<String>,
    pub actual_open_spread_pct: Option<f64>,
    pub opened_at: Option<DateTime<Utc>>,
}
```

### Opportunity

```rust
pub struct FundingSpreadOpenOpportunity {
    pub route_id: String,
    pub canonical_symbol: CanonicalSymbol,
    pub leg_a_exchange: ExchangeId,
    pub leg_b_exchange: ExchangeId,
    pub spread_pct: f64,
    pub target_direction: SpreadTargetDirection,
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub maker_exchange: ExchangeId,
    pub hedge_exchange: ExchangeId,
    pub target_notional_usdt: f64,
    pub expected_net_funding_rate: f64,
    pub expected_total_edge_pct: f64,
    pub immediate_open_edge_pct: f64,
    pub selected_open_style: OpenExecutionStyle,
    pub maker_orders: Vec<MakerOrderDraft>,
    pub hedge_after_fill: Option<HedgeOrderDraft>,
    pub taker_orders: Vec<TakerOrderDraft>,
}
```

## 十三、配置草案

```yaml
strategy_kind: funding_spread_expansion_maker
display_name: Funding Spread Expansion Maker
mode: observe
dry_run: true

universe:
  quote_asset: USDT
  contract_type: perpetual
  enabled_exchanges:
    - mexc
    - bybit
  symbols:
    - HUSDT/USDT
  excluded_bases:
    - BTC
    - ETH
    - BNB
    - SOL

routes:
  - route_id: husdt_mexc_bybit_decrease
    leg_a_exchange: mexc
    leg_b_exchange: bybit
    symbol: HUSDT/USDT
    target_direction: decrease
    direction_policy: balanced
    enabled: true

thresholds:
  open_spread_pct: -0.0300
  target_close_spread_pct: -0.0800
  close_trigger_mode: absolute_spread
  allow_negative_immediate_edge: true
  min_immediate_open_edge_pct: -0.0050
  min_expected_total_edge_pct: 0.0010
  min_net_funding_rate: -0.0005
  max_adverse_funding_rate: 0.0010
  funding_uncertainty_buffer_pct: 0.0005
  repair_buffer_pct: 0.0005

funding:
  require_funding_snapshot: true
  require_mark_price: true
  require_next_funding_time: true
  max_funding_snapshot_age_ms: 5000
  funding_rate_basis_hours: 8.0
  min_funding_persistence_windows: 2
  expected_funding_windows: 2
  expected_funding_horizon_hours: 16.0
  funding_decay_haircut_pct: 0.50
  no_open_if_net_funding_negative_before_settlement_mins: 5

sizing:
  min_order_notional_usdt: 8.0
  max_order_notional_usdt: 10.0
  max_position_notional_usdt: 20.0
  max_leg_imbalance_usdt: 0.5
  max_leg_imbalance_base_qty: null
  capital_fraction_of_smaller_equity: 1.0

adding:
  add_mode: ladder_on_spread
  add_step_pct: 0.0050
  min_add_interval_secs: 30
  max_open_slices_per_route: 5
  pause_add_before_target_pct: 0.0020

execution:
  preferred_open_style: maker_taker
  allowed_open_styles:
    - maker_taker
    - dual_maker
  preferred_close_style: maker_maker_reduce_only
  allowed_close_styles:
    - maker_maker_reduce_only
    - maker_taker_reduce_only
    - dual_taker_reduce_only
  risk_close_style: dual_taker_reduce_only
  fallback_close_style: dual_taker_reduce_only
  allow_dual_maker_open: true
  allow_dual_maker_close: true
  post_only: true
  maker_price_offset_pct: 0.0005
  maker_order_timeout_ms: 3000
  close_maker_price_offset_pct: 0.0005
  close_maker_order_timeout_ms: 3000
  max_close_single_leg_exposure_ms: 1000
  hedge_taker_slippage_pct: 0.0030
  close_taker_slippage_pct: 0.0030
  cancel_if_signal_invalid: true
  cancel_if_close_signal_invalid: true
  cancel_if_funding_reverses: true
  cancel_if_risk_degrades: true
  use_ioc_limit_for_taker: true
  allow_market_order: false

risk:
  max_mmr_pct: 20.0
  max_adl_pct: 300.0
  min_liquidation_buffer_pct: 30.0
  max_book_age_ms: 500
  min_orderbook_levels: 1
  single_leg_timeout_ms: 1000
  unknown_order_timeout_ms: 3000
  max_consecutive_single_leg_fills: 1
  max_open_routes: 3
  max_active_routes_per_symbol: 1
  max_positions_per_exchange: 10
  symbol_cooldown_secs: 300
  max_hold_secs: null
  close_on_unrealized_loss: false

scanner:
  enabled: false
  min_quote_volume_usdt_24h: 500000.0
  min_top_depth_usdt: 20.0
  min_spread_persistence_secs: 300
  min_funding_persistence_windows: 2
  route_refresh_secs: 30
```

## 十四、执行语义

### Execution Style Selector

策略每次开仓、加仓、平仓前都先运行执行模式选择器：

```text
select_execution_style(context):
  if risk_close:
    return risk_close_style

  if target_close:
    choose preferred_close_style if all close maker gates pass
    else fallback_close_style

  if open_or_add:
    choose preferred_open_style if gates pass
    else next allowed_open_style
```

选择器输入：

```text
fee/rebate
orderbook depth
maker queue risk
funding direction
target spread distance
private stream health
unknown order state
single-leg repair state
MMR/ADL/liquidation buffer
time since signal
```

成本优先级通常是：

```text
dual_maker < maker_taker < dual_taker
```

风险可控性通常是：

```text
dual_taker < maker_taker < dual_maker
```

因此目标平仓和普通加仓可以偏成本，风险退出必须偏确定性。

### Maker-taker Open

```text
1. 选择 maker leg。
2. 提交 post-only limit maker order。
3. 等待 private stream fill 或 REST readback。
4. 对实际 maker filled base quantity 立即提交 hedge taker IOC/FOK limit。
5. hedge filled 后更新 route_position。
6. 如果 hedge 失败，进入 repairing_single_leg。
```

### Dual-maker Open/Add

`dual_maker` 可用于开仓或加仓，成本最低，但必须按实验能力门启用。

如启用，必须满足：

```text
allow_dual_maker_open = true
max_dual_maker_unhedged_ms <= single_leg_timeout_ms
both private streams ready
both maker orders post_only
both orders cancelable
partial fill repair implemented
```

风险：

- 两边 maker fill 不同步。
- 一边排队成交、一边不成交。
- 一边成交后价差快速变化。
- 修复路径更复杂。

推荐策略：

```text
small notional slices only
one active dual-maker pair per route
cancel both orders if one side fills and opposite side does not fill within max_dual_maker_unhedged_ms
filled side enters repair if opposite cannot be hedged
```

### Maker-maker Close

```text
1. target close 触发，且 risk_ok。
2. 冻结该 route 新开仓和加仓。
3. 计算当前可平 base quantity。
4. 同时提交 close long sell post-only reduce-only 与 close short buy post-only reduce-only。
5. 等待双腿成交。
6. 任一腿超时、信号失效或风险恶化，撤销未成交 maker。
7. 对已成交残差使用 fallback_close_style 修复。
8. 双腿完成后记录 realized spread pnl、funding pnl、fee、slippage。
```

### Maker-taker Close

```text
1. target close 触发，且 risk_ok。
2. 选择 close maker leg。
3. 提交 post-only reduce-only maker close。
4. maker close 成交后，对另一腿立即提交 protected taker reduce-only close。
5. maker partial fill 只关闭另一腿对应 filled_base_quantity。
6. maker 未成交或信号失效则撤单，不做 taker leg。
```

### Dual-taker Close

```text
1. target close fallback 或 risk close 触发。
2. 冻结该 route 新开仓和加仓。
3. 计算当前可平 base quantity。
4. close long leg: sell reduce-only protected IOC/FOK limit。
5. close short leg: buy reduce-only protected IOC/FOK limit。
6. 双腿成交后记录 realized spread pnl、funding pnl、fee、slippage。
7. route -> closed。
8. symbol cooldown。
```

## 十五、PnL 拆分

### 价格 PnL

平仓时用真实成交均价计算：

```text
long_price_pnl =
  base_qty * (long_close_price - long_avg_entry_price)

short_price_pnl =
  base_qty * (short_avg_entry_price - short_close_price)

realized_spread_expansion_pnl =
  long_price_pnl + short_price_pnl
```

### 价差方向收益估算

用于机会评估：

```text
if target_direction = decrease:
  directional_spread_move_pct = weighted_open_spread_pct - current_spread_pct

if target_direction = increase:
  directional_spread_move_pct = current_spread_pct - weighted_open_spread_pct
```

估算收益：

```text
estimated_spread_pnl_usdt =
  route_base_notional_usdt * directional_spread_move_pct
```

真实 PnL 以双腿成交价为准。

### Funding PnL

每次 settlement 或 funding ledger 更新：

```text
funding_pnl = long_leg_funding + short_leg_funding
```

并记录到：

```text
route_position.cumulative_funding_pnl_usdt
```

### 总收益

```text
net_realized_pnl =
  realized_spread_expansion_pnl
+ cumulative_funding_pnl_usdt
- cumulative_fee_usdt
- realized_slippage_usdt
- repair_cost_usdt
```

## 十六、Scanner 设计

长期策略需要一个只读 scanner，类似产品页面中的“多交易所多交易对监控”。

Scanner 输入：

- 各交易所 USDT 永续 symbol universe。
- funding rate current/predicted。
- next funding time。
- spread snapshot。
- spread history。
- mark/index price。
- order book top/depth。
- fee rates。
- 24h volume。
- open interest。
- account route allowlist。

Scanner 输出：

```rust
pub struct FundingSpreadRouteCandidate {
    pub route_id: String,
    pub leg_a_exchange: ExchangeId,
    pub leg_b_exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub target_direction: SpreadTargetDirection,
    pub current_spread_pct: f64,
    pub net_funding_rate: f64,
    pub spread_persistence_secs: i64,
    pub funding_persistence_windows: usize,
    pub score: f64,
    pub executable: bool,
    pub reject_reason: Option<String>,
}
```

候选过滤：

```text
same canonical symbol
both perpetual instruments active
both funding snapshots fresh
net_funding_rate passes threshold
spread direction passes threshold
spread persisted for min_spread_persistence_secs
funding persisted for min_funding_persistence_windows
top depth >= min_top_depth_usdt
fees known
precision known
```

Score 示例：

```text
score =
  net_funding_rate_weight * net_funding_rate
+ spread_distance_weight * distance_from_open_threshold
+ persistence_weight * funding_and_spread_persistence
+ liquidity_weight * normalized_depth
- risk_weight * risk_penalty
- fee_weight * fee_cost
```

第一阶段可先不自动全市场开仓，只用 scanner 生成候选和 dashboard。实盘交易仍通过
`routes` allowlist 启用。

## 十七、Snapshot 与 Dashboard

Snapshot 必须不含密钥。

必备字段：

```text
schema_version
strategy_kind
mode
dry_run
route_id
state
configured exchanges
symbol
leg_a_exchange
leg_b_exchange
target_direction
direction_policy
current_spread_pct
open_spread_pct
target_close_spread_pct
weighted_open_spread_pct
actual_close_spread_pct
current_funding_rate_leg_a
current_funding_rate_leg_b
net_funding_rate
next_funding_time_leg_a
next_funding_time_leg_b
position_notional_long
position_notional_short
base_quantity_long
base_quantity_short
leg_imbalance_usdt
leg_imbalance_base_qty
cumulative_funding_pnl_usdt
unrealized_price_pnl_usdt
realized_spread_pnl_usdt
cumulative_fee_usdt
pending_maker_orders
pending_hedge_orders
open_slices
last_add_at
last_add_spread_pct
close_reason
mmr_pct
adl_pct
liquidation_buffer_pct
last_audit_reject_reason
last_updated_at
```

禁止字段：

```text
api_key
secret
passphrase
token
signature
raw_private_payload
private_endpoint
```

Dashboard 行建议接近截图语义：

```text
交易所: leg_a / leg_b
仓位价值(U): long_notional / short_notional + imbalance
开清条件: open_spread_pct / target_close_spread_pct
实开实清: weighted_open_spread_pct / actual_close_spread_pct
开仓均价: long_avg_entry / short_avg_entry
仓位数量: long_base_qty / short_base_qty
仓位限额(U): max_position_notional_usdt
下单范围(U): min_order_notional_usdt / max_order_notional_usdt
实时价差: current_spread_pct
实时费率: leg_a funding / leg_b funding / net funding
费率结算: next funding times
实时MMR: leg risk values
爆率/ADL: liquidation buffer / ADL
```

## 十八、落库与事件

策略应通过现有 storage/event provider 落库，不直接写数据库连接。

建议事件类型：

```text
funding_spread_route_enabled
funding_spread_route_disabled
funding_spread_signal_observed
funding_spread_open_rejected
funding_spread_open_accepted
funding_spread_maker_order_submitted
funding_spread_maker_order_filled
funding_spread_maker_order_canceled
funding_spread_hedge_order_submitted
funding_spread_hedge_order_filled
funding_spread_slice_opened
funding_spread_add_rejected
funding_spread_add_accepted
funding_spread_funding_settled
funding_spread_target_close_triggered
funding_spread_risk_close_triggered
funding_spread_close_submitted
funding_spread_closed
funding_spread_repair_started
funding_spread_repair_completed
funding_spread_unknown_order_state
```

每个事件至少包含：

```text
event_id
strategy_kind
strategy_id
run_id
route_id
slice_id optional
exchange ids
canonical_symbol
state_before
state_after
observed_spread_pct
funding rates
net_funding_rate
position quantities
notional
reason
created_at
```

## 十九、Runtime Contract

新策略必须暴露：

- `StrategySpec`
- `config_schema`
- `snapshot_schema`
- `runtime_contract`
- `command_schema`
- secret-free snapshot

### 标准化输入事件 Payload 合约

新策略 runtime 只消费上游 market/account/risk provider 归一化后的事件 payload。
策略 crate 不读取交易所原始 WebSocket/REST payload，不读取 `.env`，不处理签名，
也不接收任何 credential 字段。所有 payload 都必须是 secret-free，并且只包含
策略决策需要的标准字段。

通用 envelope：

```json
{
  "schema_version": "funding_spread_expansion_maker.runtime.v1",
  "event_type": "order_book_top",
  "event_id": "01JZEVENT000000000000000001",
  "source": "market_data_provider",
  "exchange": "mexc",
  "canonical_symbol": "HUSDT/USDT",
  "exchange_symbol": "HUSDT_USDT",
  "observed_at": "2026-06-12T09:30:00.120Z",
  "received_at": "2026-06-12T09:30:00.145Z",
  "payload": {}
}
```

envelope 必备字段：

```text
schema_version
event_type
event_id
source
exchange
canonical_symbol
observed_at
received_at
payload
```

`event_type` 第一阶段必须支持：

```text
order_book_top
funding_rate
symbol_precision
fee_rates
risk_snapshot
route_position
execution_order_update
```

#### order_book_top

用途：计算 spread、盘口新鲜度、maker/taker 价格保护和 top depth。

payload 字段：

```text
bid_price
bid_quantity_base
bid_notional_usdt
ask_price
ask_quantity_base
ask_notional_usdt
mark_price optional
index_price optional
book_sequence optional
is_snapshot
```

示例：

```json
{
  "schema_version": "funding_spread_expansion_maker.runtime.v1",
  "event_type": "order_book_top",
  "event_id": "01JZOBTOP00000000000000001",
  "source": "market_data_provider",
  "exchange": "mexc",
  "canonical_symbol": "HUSDT/USDT",
  "exchange_symbol": "HUSDT_USDT",
  "observed_at": "2026-06-12T09:30:00.120Z",
  "received_at": "2026-06-12T09:30:00.145Z",
  "payload": {
    "bid_price": 0.1485,
    "bid_quantity_base": 1400.0,
    "bid_notional_usdt": 207.9,
    "ask_price": 0.1487,
    "ask_quantity_base": 1200.0,
    "ask_notional_usdt": 178.44,
    "mark_price": 0.1486,
    "index_price": 0.1484,
    "book_sequence": 981234567,
    "is_snapshot": false
  }
}
```

#### funding_rate

用途：admission、hold filter、risk close 和 funding PnL 归因。

payload 字段：

```text
funding_rate
predicted_funding_rate optional
funding_interval_hours
next_funding_time
previous_funding_time optional
rate_source
settlement_currency
```

示例：

```json
{
  "schema_version": "funding_spread_expansion_maker.runtime.v1",
  "event_type": "funding_rate",
  "event_id": "01JZFUND00000000000000001",
  "source": "funding_provider",
  "exchange": "bybit",
  "canonical_symbol": "HUSDT/USDT",
  "exchange_symbol": "HUSDTUSDT",
  "observed_at": "2026-06-12T09:30:01.000Z",
  "received_at": "2026-06-12T09:30:01.030Z",
  "payload": {
    "funding_rate": 0.00042,
    "predicted_funding_rate": 0.00038,
    "funding_interval_hours": 8,
    "next_funding_time": "2026-06-12T16:00:00Z",
    "previous_funding_time": "2026-06-12T08:00:00Z",
    "rate_source": "exchange",
    "settlement_currency": "USDT"
  }
}
```

#### symbol_precision

用途：base quantity、price tick、min notional 和 contract quantity 归一化。

payload 字段：

```text
price_tick
quantity_step_base
min_quantity_base
min_notional_usdt
contract_size_base
quantity_mode
price_precision optional
quantity_precision optional
```

`quantity_mode` 可取：

```text
base
contract
```

示例：

```json
{
  "schema_version": "funding_spread_expansion_maker.runtime.v1",
  "event_type": "symbol_precision",
  "event_id": "01JZPREC00000000000000001",
  "source": "instrument_provider",
  "exchange": "mexc",
  "canonical_symbol": "HUSDT/USDT",
  "exchange_symbol": "HUSDT_USDT",
  "observed_at": "2026-06-12T09:29:55.000Z",
  "received_at": "2026-06-12T09:29:55.010Z",
  "payload": {
    "price_tick": 0.0001,
    "quantity_step_base": 1.0,
    "min_quantity_base": 1.0,
    "min_notional_usdt": 5.0,
    "contract_size_base": 1.0,
    "quantity_mode": "base",
    "price_precision": 4,
    "quantity_precision": 0
  }
}
```

#### fee_rates

用途：计算 maker rebate/taker fee、expected total edge 和执行模式选择。

payload 字段：

```text
maker_fee_rate
taker_fee_rate
maker_rebate_rate optional
fee_currency
tier optional
effective_from optional
effective_until optional
```

费率统一用正数表示成本；若 maker 是返佣，可用 `maker_fee_rate` 为负数，或使用
`maker_rebate_rate` 表示返佣。runtime 内部必须归一成单一 fee model 后再计算。

示例：

```json
{
  "schema_version": "funding_spread_expansion_maker.runtime.v1",
  "event_type": "fee_rates",
  "event_id": "01JZFEE000000000000000001",
  "source": "fee_provider",
  "exchange": "bybit",
  "canonical_symbol": "HUSDT/USDT",
  "exchange_symbol": "HUSDTUSDT",
  "observed_at": "2026-06-12T09:29:50.000Z",
  "received_at": "2026-06-12T09:29:50.015Z",
  "payload": {
    "maker_fee_rate": 0.0001,
    "taker_fee_rate": 0.00055,
    "maker_rebate_rate": null,
    "fee_currency": "USDT",
    "tier": "default"
  }
}
```

#### risk_snapshot

用途：admission、加仓阻断、close-only、risk close 和 dashboard。

payload 字段：

```text
account_id
margin_mode
equity_usdt
available_balance_usdt
maintenance_margin_usdt
mmr_pct
adl_pct
liquidation_buffer_pct
private_stream_ready
private_stream_stale_ms
kill_switch
unmanaged_same_symbol_position
positions_observed_at
```

`account_id` 必须是内部非敏感别名，不得是 API key、UID 密钥或任何 credential。

示例：

```json
{
  "schema_version": "funding_spread_expansion_maker.runtime.v1",
  "event_type": "risk_snapshot",
  "event_id": "01JZRISK00000000000000001",
  "source": "risk_provider",
  "exchange": "mexc",
  "canonical_symbol": "HUSDT/USDT",
  "exchange_symbol": "HUSDT_USDT",
  "observed_at": "2026-06-12T09:30:02.000Z",
  "received_at": "2026-06-12T09:30:02.025Z",
  "payload": {
    "account_id": "acct_mexc_trading_01",
    "margin_mode": "cross",
    "equity_usdt": 1000.0,
    "available_balance_usdt": 850.0,
    "maintenance_margin_usdt": 12.5,
    "mmr_pct": 1.25,
    "adl_pct": 0.0,
    "liquidation_buffer_pct": 72.0,
    "private_stream_ready": true,
    "private_stream_stale_ms": 120,
    "kill_switch": false,
    "unmanaged_same_symbol_position": false,
    "positions_observed_at": "2026-06-12T09:30:01.900Z"
  }
}
```

#### route_position

用途：恢复 route 状态、判断是否允许加仓/平仓、计算 imbalance、展示持仓。

payload 字段：

```text
route_id
state
long_exchange
short_exchange
long_base_quantity
short_base_quantity
long_avg_entry_price
short_avg_entry_price
weighted_open_spread_pct
current_spread_pct
current_notional_usdt
leg_imbalance_usdt
leg_imbalance_base_qty
cumulative_funding_pnl_usdt
cumulative_fee_usdt
unrealized_price_pnl_usdt
open_slices
pending_order_ids
opened_at optional
last_add_at optional
last_add_spread_pct optional
```

示例：

```json
{
  "schema_version": "funding_spread_expansion_maker.runtime.v1",
  "event_type": "route_position",
  "event_id": "01JZPOS000000000000000001",
  "source": "position_provider",
  "exchange": "route",
  "canonical_symbol": "HUSDT/USDT",
  "observed_at": "2026-06-12T09:30:03.000Z",
  "received_at": "2026-06-12T09:30:03.020Z",
  "payload": {
    "route_id": "husdt_mexc_bybit_decrease",
    "state": "open",
    "long_exchange": "mexc",
    "short_exchange": "bybit",
    "long_base_quantity": 67.0,
    "short_base_quantity": 67.0,
    "long_avg_entry_price": 0.1490,
    "short_avg_entry_price": 0.1560,
    "weighted_open_spread_pct": -0.0469,
    "current_spread_pct": -0.0600,
    "current_notional_usdt": 10.0,
    "leg_imbalance_usdt": 0.0,
    "leg_imbalance_base_qty": 0.0,
    "cumulative_funding_pnl_usdt": 0.018,
    "cumulative_fee_usdt": 0.006,
    "unrealized_price_pnl_usdt": 0.13,
    "open_slices": ["slice_001"],
    "pending_order_ids": [],
    "opened_at": "2026-06-12T09:10:00Z",
    "last_add_at": null,
    "last_add_spread_pct": null
  }
}
```

#### execution_order_update

用途：驱动两阶段 `maker_taker` 执行、清理 pending order、处理 unknown order。
当 maker leg 成交后，runtime 才提交 staged hedge taker IOC/FOK；如果 maker 被撤或超时，
必须丢弃对应 staged hedge，避免无 maker 成交时主动打出 taker 单腿。

payload 字段：

```text
route_id
client_order_id
status
filled
filled_quantity_base optional
terminal
unknown_order_state optional
canceled optional
```

`status` 可取：

```text
new
partially_filled
filled
canceled
rejected
unknown
```

示例：

```json
{
  "schema_version": "funding_spread_expansion_maker.runtime.v1",
  "event_type": "execution_order_update",
  "event_id": "01JZEXEC00000000000000001",
  "source": "execution_provider",
  "exchange": "mexc",
  "canonical_symbol": "HUSDT/USDT",
  "observed_at": "2026-06-12T09:30:04.000Z",
  "received_at": "2026-06-12T09:30:04.012Z",
  "payload": {
    "route_id": "husdt_mexc_bybit_decrease",
    "client_order_id": "funding_spread_expansion_maker-husdt_mexc_bybit_decrease-open-1-0",
    "status": "filled",
    "filled": true,
    "filled_quantity_base": 67.0,
    "terminal": true,
    "unknown_order_state": false,
    "canceled": false
  }
}
```

### Live mode / dry_run 自动提交订单语义

`mode` 表示策略是否允许从观察进入自动交易状态；`dry_run` 表示是否真正提交订单。
二者必须同时参与 execution gate。

```yaml
mode: observe
dry_run: true
```

语义矩阵：

| mode | dry_run | 行为 |
| --- | --- | --- |
| `observe` | `true` | 只消费事件、计算信号、输出 snapshot/audit，不生成真实 order intent。 |
| `observe` | `false` | 仍不得提交订单；该组合只用于验证配置解析，runtime 应给出警告。 |
| `paper` | `true` | 生成模拟 order intent 和模拟成交，用于 live-dry-run。不得调用真实 execution provider。 |
| `live` | `true` | 生成完整 order draft、idempotency key 和 audit，但拦截在提交前；不得向交易所发单。 |
| `live` | `false` | 允许自动提交订单，但必须同时满足 route enabled、admission、risk gate、private stream ready、precision/fee/funding/book fresh 和 kill switch=false。 |

`live + dry_run=false` 是唯一允许真实提交订单的组合。真实提交仍必须遵守：

```text
route.enabled = true
execution provider ready
idempotency key present
post_only/reduce_only flags set as required
allow_market_order = false unless explicitly overridden for a tested adapter path
private_stream_ready = true
risk_snapshot.kill_switch = false
no secret fields in config, snapshot, event or audit log
```

禁止把 API key、secret、passphrase、token、signature、cookie、session、private key、
raw private exchange payload 写入配置、事件、snapshot、日志或 dashboard。运行时只能
通过外部 credential provider/execution provider 使用密钥，策略配置仅引用非敏感
account/exchange 别名。

建议命令：

```text
pause
resume
stop
refresh_routes
enable_route
disable_route
close_route
close_all
sync_positions
repair_route
```

命令约束：

- `enable_route` 只启用自动策略，不直接绕过 admission 下单。
- `close_route` 触发自动 reduce-only 平仓。
- `repair_route` 只能在 single-leg 或 unknown 状态下执行预定义 repair plan。
- 所有写单仍走 execution provider 和 idempotency key。

## 二十、风控门槛

必须实现：

```text
max_position_notional_usdt
min_order_notional_usdt
max_order_notional_usdt
max_leg_imbalance_usdt
max_leg_imbalance_base_qty
max_active_routes_per_symbol
max_open_routes
max_positions_per_exchange
symbol_cooldown_secs
max_mmr_pct
max_adl_pct
min_liquidation_buffer_pct
max_book_age_ms
min_orderbook_levels
maker_order_timeout_ms
single_leg_timeout_ms
unknown_order_timeout_ms
max_consecutive_single_leg_fills
max_adverse_funding_rate
max_funding_snapshot_age_ms
private_stream_stale_ms
kill_switch
```

单腿处理：

```text
if maker_leg_filled && hedge_leg_not_filled:
  immediately submit hedge taker IOC/FOK

if hedge fails:
  enter repairing_single_leg
  stop new opens and adds
  retry hedge or reduce-only close filled maker leg

if order status unknown:
  private stream confirmation first
  REST readback fallback
  no new opens/adds before resolved
```

外部未托管仓位：

```text
if unmanaged same-symbol position exists:
  block open/add
  show unmanaged_position risk
```

## 二十一、与现有策略的关系

| 策略 | 目标 | 入场 | 持仓 | 出场 | 是否适合承载本策略 |
| --- | --- | --- | --- | --- | --- |
| `cross_exchange_arbitrage` | 快速跨所价差套利 | dual taker 或 maker-taker | 短 | 锁利润/价差收敛/最大持仓 | 不适合直接改 |
| `funding-arbitrage` | 资金费率窗口 | funding selection | 按结算窗口 | 结算后关闭 | 不适合直接改 |
| `funding_spread_expansion_maker` | funding + 价差扩张 | maker-taker / dual-maker 慢速建仓 | 长 | maker-maker / maker-taker / dual-taker 目标平仓，风险双 taker | 新策略 |

可以抽取共享能力：

- `FundingModel::leg_funding`
- fee model
- order book freshness
- precision normalization
- maker-taker draft
- dual-maker open/close draft
- maker-taker reduce-only close draft
- dual taker reduce-only fallback close draft
- single-leg repair state

但共享应在 SDK/通用库层完成，不要让策略 crate 互相依赖。

## 二十二、开发任务切分

### 任务 1：策略 crate scaffold 与 config schema

范围：

```text
strategies/funding-spread-expansion-maker/
```

交付：

- `Cargo.toml`
- `src/lib.rs`
- `src/core.rs`
- `src/runtime_contract.rs`
- `src/app_runtime.rs`
- `strategy_kind = funding_spread_expansion_maker`
- config schema
- snapshot schema
- default config

验证：

```bash
cargo check -p rustcta-strategy-funding-spread-expansion-maker
cargo test -p rustcta-strategy-funding-spread-expansion-maker
```

### 任务 2：核心方向、价差、funding 和 PnL 模型

交付：

- `SpreadTargetDirection`
- `DirectionPolicy`
- spread 计算。
- long/short funding 计算。
- expected total edge 计算。
- negative immediate edge admission。
- weighted open spread。
- target close 判断。

必测：

- ESPORTS 示例 spread 约 `-4.6%`。
- `-4.69% -> -8.00%` 目标平仓。
- long 正 funding 付费，short 正 funding 收钱。

### 任务 3：开仓与加仓 evaluator

交付：

- open opportunity evaluator。
- add opportunity evaluator。
- maker leg 选择。
- sizing range。
- max position cap。
- imbalance block。
- audit reject reason。

必测：

- `8U/20U`，单次 `8..10U`，下一单不超过 `10U`。
- `18U/20U`，剩余 `2U` 小于 min order，不加仓。
- spread `-6%` 尚未到 `-8%`，仓位未满且风控通过时可加仓。
- unknown order state 阻断加仓。

### 任务 4：执行命令和状态机

交付：

- maker post-only order command。
- hedge taker IOC/FOK order command。
- dual-maker open/add order pair command。
- maker cancel command。
- maker-maker reduce-only close command。
- maker-taker reduce-only close command。
- dual taker reduce-only fallback close command。
- execution style selector。
- state transitions。
- idempotency key。

必测：

- maker 成交后 hedge command 使用实际 filled base quantity。
- dual-maker 一腿成交、另一腿超时时进入 repair 或 fallback。
- maker-maker 平仓一腿成交、另一腿失败时进入 close repair。
- hedge 失败进入 `repairing_single_leg`。
- `repairing_single_leg` 阻断新开仓。

### 任务 5：平仓与风险退出

交付：

- target close evaluator。
- risk close evaluator。
- close reason。
- close PnL breakdown。
- close-only state。

必测：

- `spread=-8.01%` 时 should_close。
- `spread=-6%` 时不平仓。
- 浮亏不触发普通 close。
- funding 反转超过阈值触发 risk close。

### 任务 6：runtime contract、snapshot、dashboard read model

交付：

- runtime contract。
- dashboard snapshot rows。
- route health rows。
- open/add/close audit events。
- secret-free snapshot tests。

必测：

- snapshot 不包含 `api_key`、`secret`、`passphrase`、`token`。
- route snapshot 展示开清条件、实开实清、仓位价值、费率、MMR/ADL。

### 任务 7：scanner 与 route allowlist

交付：

- read-only scanner model。
- candidate score。
- route allowlist。
- `enable_route` / `disable_route` command。

第一阶段 scanner 可只输出候选，不自动全市场开仓。

### 任务 8：config、supervisor、CLI 合约

交付：

- `config/funding_spread_expansion_maker_usdt.yml`
- supervisor spec。
- CLI observe/contract smoke。
- live-dry-run 示例。

### 任务 9：边界检查与验收

必须通过：

```bash
cargo fmt
cargo check -p rustcta-strategy-funding-spread-expansion-maker
cargo test -p rustcta-strategy-funding-spread-expansion-maker
scripts/check_industrial_boundaries.sh
```

如果改到共享 SDK 或 gateway contract，再追加对应 crate 的 focused tests。

## 二十三、必测用例清单

1. ESPORTS 示例：`bitget=0.1558`，`binance=0.1486`，spread 约 `-4.6%`，
   满足 `-3%` 开仓条件。
2. `actual_open=-4.69%`，`target_close=-8%`，当 `spread=-8.01%` 时
   `should_close=true`。
3. `spread=-6%` 时不平仓，但允许在仓位未满且风控通过时继续 maker 加仓。
4. 仓位 `8U`、上限 `20U`、单次 `8..10U` 时，下次加仓 notional 不超过 `10U`。
5. 仓位 `18U`、上限 `20U` 时，下次加仓不得超过 `2U`；如果小于
   `min_order_notional_usdt`，则不加仓。
6. funding 计算方向正确：long 负担正 funding，short 收取正 funding。
7. 允许负即时毛利，但 `expected_total_edge_pct` 不达标时拒绝。
8. maker 成交但 hedge 失败时进入 `repairing_single_leg`，并阻断新开仓。
9. unknown order state 未解决时阻断开仓和加仓。
10. dual-maker 开仓一腿成交、另一腿未成交时进入 repair，不允许继续加仓。
11. maker-maker 目标平仓双腿都成交时完成低成本平仓。
12. maker-maker 目标平仓一腿成交、另一腿超时时 fallback 到
    `dual_taker_reduce_only` 修复残差。
13. maker-taker 平仓 maker leg partial fill 时，只对 filled base quantity 做 taker close。
14. 风险平仓不等待 maker，直接使用 `dual_taker_reduce_only`。
15. 浮亏不触发普通平仓。
16. MMR/ADL/爆仓距离触发风险退出。
17. snapshot 序列化不包含密钥字段。
18. 策略 crate 不依赖 exchange gateway 或具体 adapter。
19. 精度测试覆盖 base quantity 与 contract quantity 转换。
20. reduce-only close 使用平仓方向，不能误开反向新仓。

## 二十四、验收标准

策略行为验收：

- 能复现截图中的价差符号和开仓判断。
- 能表达 `-3%` 开仓、`-8%` 目标平仓。
- 能在仓位未满时慢速 maker 加仓。
- 不会在 maker 成交后无条件立即加仓。
- 能在目标价差触发时按配置选择 maker-maker、maker-taker 或 dual-taker
  reduce-only 平仓。
- 能在 maker 平仓超时、单腿或风险恶化时 fallback 到 dual taker reduce-only。
- 风险平仓不依赖 maker 排队。
- 能正确计算双腿资金费率净收益。
- 允许负即时开仓毛利，但必须有 funding 和目标价差收益覆盖。
- 浮亏不触发普通平仓。
- 能处理单腿、unknown、超时和风险退出。
- 所有订单均通过 SDK/execution provider 表达为 intent 或 order draft。
- 文档、配置、snapshot 和日志均不泄露密钥。

工程验收：

```bash
cargo fmt
cargo check -p rustcta-strategy-funding-spread-expansion-maker
cargo test -p rustcta-strategy-funding-spread-expansion-maker
scripts/check_industrial_boundaries.sh
```

实盘前额外验收：

- 至少一个 route 完成 live-dry-run。
- private stream ready gate 能阻断开仓。
- REST readback 能解析 maker fill、hedge fill、unknown。
- reduce-only close 在目标交易所能力矩阵中已确认。
- MMR、ADL、liquidation buffer 缺失时 live admission 默认阻断，除非配置明确使用
  保守替代值。
- kill switch 能停止新开仓并触发 close-only。

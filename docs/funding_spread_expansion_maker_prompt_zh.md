# 资金费率 + 价差扩张 Maker 加仓策略提示词

状态日期：2026-06-12

本文把截图中的 `ESPORTS` 跨所永续策略整理成一组可交给 AI 或开发者实现的提示词。目标不是复盘某一笔订单，而是把策略意图、开仓逻辑、慢速 maker 加仓、价差目标平仓、资金费率收益和风控边界描述清楚，便于在 RustCTA 现有策略系统中落地。

## 1. 策略一句话

在两个交易所的同一 USDT 永续合约之间建立等数量、反方向的对冲仓位，以资金费率收益作为持仓基础；当跨所价差达到开仓阈值后，用 maker 或 maker-taker 方式慢慢开仓和加仓；如果价差继续向目标方向扩大到清仓阈值，则自动平仓，同时兑现价差扩张收益。

该策略的收益来源：

```text
总收益 =
  双腿资金费率净收益
+ 价差从开仓阈值继续扩大的收益
+ maker 低手续费或返佣
- taker 对冲手续费
- 平仓手续费
- 滑点
- 单腿暴露修复成本
```

## 2. 策略边界

第一阶段只做：

- USDT 本位永续合约。
- 两个交易所之间的同一 canonical symbol。
- 一边做多，一边做空，目标净 base 数量接近 0。
- 开仓优先 maker；maker 成交后立即对冲另一腿。
- 允许在明确配置下使用双 maker 或 maker-taker。
- 平仓默认使用双 taker reduce-only，保证目标触发后能退出。
- 资金费率是入场和持仓过滤条件，价差扩张是主要出场条件之一。

第一阶段不做：

- 现货腿。
- 三角套利。
- 借币做空。
- 跨链转账。
- 未对冲裸仓。
- 未经过 SDK 和执行路由的交易所私有 API 调用。
- 在策略 crate 中编码交易所 REST/WebSocket endpoint、签名或适配器细节。

## 3. 关键字段语义

参考截图字段：

```text
交易所: bitget / binance
仓位价值(U): 8 / 8 +0.2
开清条件: -3.00% / -8.00%
实开实清: -4.69% / --
开仓均价: 0.1558+17.2% / 0.1486-15.4%
仓位数量: 54.0000 / 54.0000
仓位限额(U): 20
下单范围(U): 8 / 10
实时指数: -2.7955 | 0.0748
实时费率: +1.534 -1.589/4h/2 | -0.055/4h/2
费率结算: 01:59:19 / 01:59:19
实时MMR: 7.47% / 5.88%
爆率/ADL: 66.0% | 267.9%
```

字段解释：

- `开仓条件`: 当价差达到该阈值时允许开仓或继续加仓。截图中为 `-3.00%`。
- `清仓条件`: 当价差继续扩大到该阈值时触发目标平仓。截图中为 `-8.00%`。
- `实开实清`: 当前实际成交的开仓价差和清仓价差。`--` 表示还未清仓。
- `仓位价值`: 两腿当前名义价值，后面的 `+0.2` 可视为两腿价值偏差。
- `仓位数量`: 两腿 base 数量，必须尽量一致。
- `仓位限额`: 本策略对该 symbol 的最大总名义仓位。
- `下单范围`: 每次 maker 加仓的目标名义金额范围。
- `实时费率`: 两腿当前或预测资金费率，以及折算后的净资金费率。
- `实时MMR`、`爆率/ADL`: 风险过滤条件，不参与盈利公式但可以阻断开仓、加仓或触发降风险。

## 4. 价差定义

为避免正负号混乱，策略实现必须显式记录 `spread_direction`。

推荐统一公式：

```text
spread_pct = (leg_b_price - leg_a_price) / leg_a_price
```

当截图为：

```text
leg_a = bitget
leg_b = binance
bitget_avg = 0.1558
binance_avg = 0.1486
```

则：

```text
spread_pct = (0.1486 - 0.1558) / 0.1558 = -4.62%
```

这与截图 `实开 -4.69%` 接近，说明截图中的负价差表示：

```text
binance 相对 bitget 更便宜
或 bitget 相对 binance 更贵
```

策略不得只用绝对值判断，必须保留方向：

```text
开仓方向: spread_pct <= open_spread_pct
目标平仓: spread_pct <= target_close_spread_pct
```

示例：

```text
open_spread_pct = -0.03
target_close_spread_pct = -0.08
actual_open_spread_pct = -0.0469
```

该订单的目标是价差从 `-4.69%` 继续扩大到 `-8.00%` 后自动平仓。

## 5. 持仓方向

策略需要同时支持两类方向，但每个 bundle 必须固定方向。

价差扩张型方向：

```text
如果目标是 spread_pct 继续下降：
  做多 leg_a
  做空 leg_b

如果目标是 spread_pct 继续上升：
  做多 leg_b
  做空 leg_a
```

资金费率优先型方向：

```text
funding_rate > 0: 永续多头通常付费，空头通常收钱
funding_rate < 0: 永续多头通常收钱，空头通常付费
```

因此资金费率收益为：

```text
long_leg_funding = -notional * long_exchange_funding_rate
short_leg_funding =  notional * short_exchange_funding_rate
net_funding = long_leg_funding + short_leg_funding
```

开仓前必须计算：

```text
expected_net_funding_rate >= min_net_funding_rate
```

如果价差方向和资金费率最优方向冲突，策略必须按配置选择：

- `funding_first`: 只允许资金费率为正收益的方向开仓。
- `spread_first`: 允许资金费率小幅负收益，但必须由价差扩张期望覆盖。
- `balanced`: 资金费率净值不得低于安全阈值，价差信号必须满足开仓条件。

截图这一类策略更接近：

```text
balanced 或 spread_first
```

即资金费率提供持仓理由，价差扩大提供自动平仓目标。

## 6. 开仓逻辑

开仓触发条件：

```text
market_ready
&& private_stream_ready
&& precision_rules_ready
&& no_existing_unmanaged_position
&& symbol_not_cooling_down
&& current_position_notional < max_position_notional_usdt
&& spread_direction_matched
&& spread_pct <= open_spread_pct
&& net_funding_rate >= min_net_funding_rate
&& expected_total_edge_after_fee >= min_open_net_edge
&& mmr <= max_mmr
&& adl <= max_adl
&& liquidation_buffer >= min_liquidation_buffer
```

开仓收益评估：

```text
expected_total_edge =
  expected_spread_expansion_edge
+ expected_funding_edge_until_next_settlement
+ maker_rebate_or_negative_fee
- maker_open_fee
- hedge_taker_fee
- expected_close_fee
- hedge_slippage_buffer
- close_slippage_buffer
- funding_uncertainty_buffer
- single_leg_repair_buffer
```

推荐订单方式：

```text
优先级 1: maker + taker hedge
  先在 maker leg 挂 post-only limit
  maker 成交后，立即在 hedge leg 用 IOC/FOK taker 补齐

优先级 2: dual maker
  两边同时挂 post-only limit
  只允许在极严格的单腿暴露窗口和自动撤单机制下使用

优先级 3: dual taker
  只用于紧急平仓、快速退出，或配置明确允许的入场
```

## 7. Maker 慢速加仓逻辑

加仓不是独立策略，而是复用开仓过滤条件，并额外检查仓位上限和偏差。

加仓触发：

```text
position_state in [open, partially_open]
&& current_position_notional < max_position_notional_usdt
&& spread_pct <= open_spread_pct
&& spread_pct has_not_reached_target_close_pct
&& net_funding_rate >= min_net_funding_rate
&& no_pending_repair
&& no_unknown_order_state
&& risk_ok
```

每次加仓金额：

```text
next_order_notional =
  min(max_order_notional_usdt,
      max_position_notional_usdt - current_position_notional)

next_order_notional >= min_order_notional_usdt
```

对应截图：

```text
max_position_notional_usdt = 20
current_position_notional_usdt ~= 8
order_notional_range = 8..10
```

所以还能继续尝试挂入：

```text
8U 到 10U 的 maker 加仓
```

如果接近仓位上限，只允许补到上限附近，不允许超限。

双腿数量修正：

```text
base_qty_delta = abs(long_base_qty - short_base_qty)
notional_delta = abs(long_notional - short_notional)

if notional_delta > max_leg_imbalance_usdt:
  暂停新增方向性加仓
  优先补齐较小的一腿或 reduce 较大的一腿
```

maker 挂单管理：

```text
post_only = true
reduce_only = false
auto_cancel_after_ms = maker_order_timeout_ms
max_concurrent_maker_orders_per_symbol = 1
maker_price_refresh_on_book_move = true
cancel_if_spread_no_longer_qualifies = true
cancel_if_funding_reverses = true
```

## 8. 平仓逻辑

正常目标平仓：

```text
if spread_pct <= target_close_spread_pct:
  submit dual_taker_reduce_only_close
```

截图示例：

```text
actual_open_spread_pct = -4.69%
target_close_spread_pct = -8.00%
```

当实时价差达到或低于 `-8.00%`，策略自动平仓，吃价差继续扩大的收益。

平仓必须使用 reduce-only：

```text
close long leg: sell reduce-only
close short leg: buy reduce-only
```

平仓前检查：

```text
both_books_fresh
&& both_private_streams_ready
&& close_quantity > 0
&& reduce_only_supported_or_platform_can_enforce
&& estimated_close_slippage <= max_close_slippage
```

除了目标价差平仓，还需要支持风险平仓：

```text
资金费率净收益反转并超过 max_adverse_funding_rate
MMR 超过 max_mmr
ADL 超过 max_adl
爆仓距离低于 min_liquidation_buffer
单腿暴露超过 single_leg_max_ms
订单状态 unknown 超过 unknown_order_timeout_ms
持仓超过 max_hold_secs
人工 kill switch
```

风险平仓优先级高于目标平仓。

## 9. 状态机

推荐状态：

```text
observing
open_signal
maker_open_pending
maker_partially_filled
hedge_pending
open
add_pending
target_close_ready
closing
closed
repairing_single_leg
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

- 有 `maker_open_pending` 时，不允许继续开新的同 symbol bundle。
- 有 `hedge_pending` 时，不允许继续加仓。
- 有 `repairing_single_leg` 时，全策略或该 symbol 进入 close-only。
- 有 unknown order state 时，不允许新增订单。
- 平仓完成后进入 symbol cooldown。

## 10. 风控门槛

必须实现的风控：

```text
max_position_notional_usdt
min_order_notional_usdt
max_order_notional_usdt
max_leg_imbalance_usdt
max_active_bundles_per_symbol
max_open_bundles
max_positions_per_exchange
symbol_cooldown_secs
max_mmr
max_adl
min_liquidation_buffer_pct
max_book_age_ms
min_orderbook_levels
max_maker_order_timeout_ms
single_leg_timeout_ms
unknown_order_timeout_ms
max_consecutive_single_leg_fills
max_adverse_funding_rate
max_hold_secs
```

单腿处理：

```text
if maker_leg_filled && hedge_leg_not_filled:
  immediately submit hedge taker IOC/FOK

if hedge fails:
  enter repairing_single_leg
  stop new opens
  either retry hedge or reduce-only close filled maker leg

if order status unknown:
  query private stream first
  then REST readback if allowed
  no new opens before resolved
```

## 11. 配置草案

```yaml
strategy_kind: funding_spread_expansion_maker
display_name: Funding Spread Expansion Maker
mode: observe

universe:
  quote_asset: USDT
  contract_type: perpetual
  enabled_exchanges:
    - bitget
    - binance
  symbols:
    - ESPORTS/USDT

direction:
  spread_formula: leg_b_minus_leg_a_over_leg_a
  leg_a_exchange: bitget
  leg_b_exchange: binance
  target_direction: decrease
  direction_policy: balanced

thresholds:
  open_spread_pct: -0.0300
  target_close_spread_pct: -0.0800
  min_net_funding_rate: -0.0005
  max_adverse_funding_rate: 0.0010
  min_open_net_edge_pct: 0.0000
  funding_uncertainty_buffer_pct: 0.0005

sizing:
  min_order_notional_usdt: 8.0
  max_order_notional_usdt: 10.0
  max_position_notional_usdt: 20.0
  max_leg_imbalance_usdt: 0.5

execution:
  open_style: maker_taker
  allow_dual_maker: true
  close_style: dual_taker_reduce_only
  post_only: true
  maker_price_offset_pct: 0.0005
  maker_order_timeout_ms: 3000
  hedge_taker_slippage_pct: 0.0030
  close_taker_slippage_pct: 0.0030
  cancel_if_signal_invalid: true
  cancel_if_funding_reverses: true

risk:
  max_mmr_pct: 20.0
  max_adl_pct: 300.0
  min_liquidation_buffer_pct: 30.0
  max_book_age_ms: 500
  min_orderbook_levels: 1
  single_leg_timeout_ms: 1000
  unknown_order_timeout_ms: 3000
  max_consecutive_single_leg_fills: 1
  max_open_bundles: 3
  max_active_bundles_per_symbol: 1
  max_positions_per_exchange: 10
  symbol_cooldown_secs: 300
  max_hold_secs: 86400
```

## 12. AI 实现提示词

### Prompt A：策略核心建模

```text
请在 RustCTA 的策略边界内设计一个新策略核心，名称为 funding_spread_expansion_maker。

策略目标：
在两个交易所的同一 USDT 永续合约上建立等 base 数量的多空对冲仓位。资金费率净收益作为持仓过滤条件，跨所价差方向作为开仓、加仓和平仓触发条件。策略要支持 maker-taker 开仓、可选 dual maker 慢速加仓、dual taker reduce-only 平仓。

必须实现：
1. 配置结构，包含 universe、direction、thresholds、sizing、execution、risk。
2. spread_pct 计算，公式为 (leg_b_price - leg_a_price) / leg_a_price。
3. 方向语义，支持 target_direction = decrease 或 increase。
4. 资金费率收益计算，long leg = -notional * funding_rate，short leg = notional * funding_rate。
5. 开仓评估函数，只输出机会和订单意图，不直接调用交易所 adapter。
6. 加仓评估函数，复用开仓过滤条件，同时检查仓位上限和双腿偏差。
7. 平仓评估函数，当价差达到 target_close_spread_pct 或风险条件触发时输出 reduce-only 双腿平仓意图。
8. 状态机和 snapshot schema，snapshot 不得包含密钥。

禁止：
1. 不要在策略 crate 中写交易所 REST/WebSocket endpoint。
2. 不要读取 .env 或 API key。
3. 不要绕过 rustcta-strategy-sdk、执行路由、风控和 idempotency identity。
4. 不要使用 market order；taker 也必须是带价格保护的 IOC/FOK limit。
```

### Prompt B：开仓和 maker 加仓

```text
请实现 funding_spread_expansion_maker 的开仓与加仓评估逻辑。

输入：
- 两个交易所的 OrderBookTop。
- 两边 symbol precision。
- 两边 maker/taker fee。
- 两边 funding snapshot。
- 当前 open position state。
- 当前 risk state。
- 策略配置。

开仓条件：
- 两边 orderbook fresh 且有效。
- private stream、precision、position takeover 已 ready。
- spread_pct 满足方向阈值。
- net_funding_rate >= min_net_funding_rate。
- expected_total_edge_after_fee >= min_open_net_edge_pct。
- 当前 symbol 没有 pending repair 或 unknown order。
- 当前仓位小于 max_position_notional_usdt。
- MMR、ADL、爆仓距离满足风控。

加仓条件：
- 已有对冲仓位处于 open 状态。
- spread 仍满足 open_spread_pct，但尚未达到 target_close_spread_pct。
- 未超过 max_position_notional_usdt。
- 每次加仓 notional 在 min_order_notional_usdt 和 max_order_notional_usdt 之间。
- 两腿数量偏差超过 max_leg_imbalance_usdt 时，先修正偏差，不允许继续方向性加仓。

输出：
- maker post-only order draft。
- hedge taker IOC/FOK order draft。
- opportunity audit，包含接受或拒绝原因。
- 不提交真实订单。
```

### Prompt C：平仓和风险退出

```text
请实现 funding_spread_expansion_maker 的平仓评估逻辑。

正常平仓：
- 当 target_direction = decrease 且 spread_pct <= target_close_spread_pct 时，触发目标平仓。
- 当 target_direction = increase 且 spread_pct >= target_close_spread_pct 时，触发目标平仓。
- 平仓订单必须是双腿 reduce-only。
- close long leg = sell reduce-only。
- close short leg = buy reduce-only。
- 使用带价格保护的 IOC/FOK limit，不使用裸 market order。

风险平仓：
任一条件成立时触发 close-only 或 emergency close：
- net_funding_rate 反转并低于 -max_adverse_funding_rate。
- MMR 超过 max_mmr_pct。
- ADL 超过 max_adl_pct。
- 爆仓距离低于 min_liquidation_buffer_pct。
- 单腿暴露超过 single_leg_timeout_ms。
- unknown order state 超过 unknown_order_timeout_ms。
- 持仓超过 max_hold_secs。
- kill switch 启用。

输出：
- close evaluation。
- reduce-only close order drafts。
- close reason。
- fee、slippage、estimated net pnl、funding pnl、spread pnl 拆分。
```

### Prompt D：运行时 contract 和 dashboard

```text
请为 funding_spread_expansion_maker 增加 runtime contract 和 dashboard snapshot。

snapshot 必须包含：
- schema_version。
- strategy_kind。
- mode。
- configured exchanges。
- symbol。
- leg_a / leg_b。
- current spread_pct。
- open_spread_pct。
- target_close_spread_pct。
- actual_open_spread_pct。
- actual_close_spread_pct。
- current funding rates。
- net_funding_rate。
- position notional per leg。
- base quantity per leg。
- leg imbalance。
- pending maker orders。
- pending hedge orders。
- state。
- close reason。
- MMR、ADL、liquidation buffer。
- last audit reject reason。

snapshot 不得包含：
- API key。
- secret。
- passphrase。
- token。
- 原始签名请求。
- 交易所私有 endpoint payload。
```

### Prompt E：测试要求

```text
请为 funding_spread_expansion_maker 添加 focused unit tests。

必须覆盖：
1. ESPORTS 示例：bitget=0.1558，binance=0.1486，spread 约 -4.6%，满足 -3% 开仓条件。
2. actual_open=-4.69%，target_close=-8%，当 spread=-8.01% 时 should_close=true。
3. spread=-6% 时不平仓，但允许在仓位未满且风控通过时继续 maker 加仓。
4. 仓位 8U、上限 20U、单次 8-10U 时，下次加仓 notional 不超过 10U。
5. 仓位 18U、上限 20U 时，下次加仓不得超过 2U；如果小于 min_order_notional，则不加仓。
6. funding 计算方向正确：long 负担正 funding，short 收取正 funding。
7. maker 成交但 hedge 失败时进入 repairing_single_leg，并阻断新开仓。
8. unknown order state 未解决时阻断加仓。
9. snapshot 序列化不包含密钥字段。
10. 策略 crate 不依赖 exchange gateway 或具体 adapter。
```

## 13. 验收标准

完成实现后至少通过：

```bash
cargo check -p <new-strategy-crate>
cargo test -p <new-strategy-crate>
scripts/check_industrial_boundaries.sh
```

策略行为验收：

- 能复现截图中的价差符号和开仓判断。
- 能表达 `-3%` 开仓、`-8%` 目标平仓。
- 能在仓位未满时慢速 maker 加仓。
- 能在目标价差触发时 dual taker reduce-only 平仓。
- 能正确计算双腿资金费率净收益。
- 能处理单腿、unknown、超时和风险退出。
- 所有订单均通过 SDK/execution provider 表达为 intent 或 order draft。
- 文档、配置、snapshot 和日志均不泄露密钥。


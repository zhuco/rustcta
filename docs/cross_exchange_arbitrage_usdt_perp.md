# 跨所 USDT 永续 Maker+Taker 套利模拟工具设计文档

后续实盘级开发实施规范见 `docs/cross_exchange_arbitrage_industrial_implementation.md`。本文主要描述策略模拟、机会计算和页面设计；交易所私有交易补全、仓位管理、风控、对账恢复和上线门禁以实施文档为准。

## 1. 背景与目标

本工具用于在多个中心化交易所之间监控 USDT 本位永续合约价差，先进行一段时间模拟交易，再决定是否进入小资金实盘。当前目标交易所为 Binance、OKX、Bitget、Gate，后续保留新增交易所能力。

策略方向从“双边同时 Taker 吃单”调整为“Maker + Taker”或统计套利模型：优先在价差较高的一端挂 Maker 单，等待成交后立即在另一端使用 Taker 订单对冲。这样可以显著降低手续费和滑点压力，也更适合小资金先跑模拟。

核心目标：

- 只订阅 USDT 交易对，不处理 USDC、币本位、交割合约、反向合约。
- 行情使用 WebSocket 批量订阅订单簿，深度只取 5 档。
- 不做全市场实盘扫描，首期维护 100-300 个精选长尾 USDT 永续交易对。
- 交易池重点包括刚上线山寨币、流动性较差但有盘口空隙的币种、极端行情中的暴涨暴跌币种。
- 对候选池内大于 `0.1%` 的价差进行展示和记录。
- 模拟开仓阈值暂定为扣除 Maker/Taker 手续费、预计对冲滑点、资金费率影响后的净边际不低于 `0.5%`。
- 开仓模拟以“高价端 Maker 挂单成交后，低价端 Taker 对冲”为主，不再假设两边同时市价吃单。
- 保留统计套利模式：对价差均值、标准差、z-score、回归速度进行记录，用于后续筛选更稳定的长尾交易对。
- 手续费先按官网公开普通费率计算，后续接入账户实际费率。
- 资金费率必须纳入模拟收益，因为不同平台同一交易对资金费率可能明显不同。
- Web 页面展示实时套利机会、模拟持仓、已平仓记录、资金费和手续费拆分。

外部参考页面：

- `http://sj2.jy260412.site:9999`
- 当前页面标题为“Bitget & Gate 合约套利（全自动模拟版）”。
- 该页面可作为 UI 和模拟流程参考，但它目前主要按固定手续费和价差模拟，没有充分处理不同交易所资金费率差异，因此不能直接作为收益依据。

## 2. 策略定义

跨所套利开仓仍由两条腿组成：

```text
低价交易所：做多
高价交易所：做空
```

但执行顺序不再是两边同时吃单，而是：

```text
1. 在价差较高的一端挂 Maker 单。
2. Maker 单成交后，立即在另一端用 Taker 单完成对冲。
3. 如果 Taker 对冲失败或部分成交，进入孤腿风控。
```

常见开仓方向：

```text
价差方向：B 价格显著高于 A

首选执行：
    在 B 挂 Maker 卖单，等待成交后形成空头腿。
    B 成交后，立刻在 A 吃 asks 买入开多，对冲空头腿。

备选执行：
    在 A 挂 Maker 买单，等待成交后形成多头腿。
    A 成交后，立刻在 B 吃 bids 卖出开空，对冲多头腿。
```

当价差收敛后平仓：

```text
多头腿：卖出平多
空头腿：买入平空
```

平仓也优先使用 Maker + Taker：

```text
1. 在更有利的一端挂 Maker 平仓单。
2. Maker 平仓成交后，立刻在另一端 Taker 平掉剩余对冲腿。
3. 如果价差快速恶化或持仓超时，允许两边 Taker 强制平仓，但必须单独记录为 emergency close。
```

本系统模拟的是“跨所价差收敛收益”和“长尾资产盘口错位收益”，不是无风险套利。真实风险包括盘口滑点、部分成交、单腿成交、资金费率变化、交易所指数差异、API 延迟、风控限制、维护暂停、极端行情价差继续扩大、Maker 排队无法成交、Maker 成交后另一端盘口瞬间消失等。

## 3. Maker+Taker 开平仓方向与订单簿规则

这是模拟准确性的核心规则，必须写进代码和测试。

### 3.1 Taker 对冲成交方向

当需要用 Taker 对冲时，吃单方向必须按下面规则计算。

做多开仓 Taker：

- 方向：买入开多。
- 吃的是卖出订单簿，即 order book asks。
- 成交价从最优 ask 开始向上吃 5 档。

做空开仓 Taker：

- 方向：卖出开空。
- 吃的是买入订单簿，即 order book bids。
- 成交价从最优 bid 开始向下吃 5 档。

多头平仓：

- 方向：卖出平多。
- 吃的是买入订单簿，即 bids。
- 成交价从最优 bid 开始向下吃 5 档。

空头平仓：

- 方向：买入平空。
- 吃的是卖出订单簿，即 asks。
- 成交价从最优 ask 开始向上吃 5 档。

### 3.2 Maker 挂单价格

价差方向为 `B 高于 A` 时，首选在 B 做空端挂 Maker 卖单：

```text
maker_exchange = B
maker_side = sell/open_short
maker_price = B best_ask 或略低于 B best_ask 的可配置价格
taker_hedge_exchange = A
taker_hedge_side = buy/open_long
taker_hedge_price = A asks VWAP
```

如果希望提高 Maker 成交概率，可以把挂单放在更靠近盘口的位置：

```text
maker_price = max(B best_bid + tick_size, B best_ask - maker_price_improve_ticks * tick_size)
```

但这会降低价差收益。模拟必须记录：

- 挂单是否在买一/卖一。
- 挂单距离对手盘多少 tick。
- 假设成交所需的队列等待时间。
- 成交后另一端 Taker 对冲的实际 VWAP。

### 3.3 开仓机会计算

价差方向为 `B 高于 A`，在 B 挂 Maker 卖单并在 A Taker 买入对冲：

```text
short_entry_price = B maker_sell_price
long_entry_price  = A asks 按目标金额吃单后的 VWAP
raw_open_spread   = short_entry_price / long_entry_price - 1
```

备选方向为在 A 挂 Maker 买单并在 B Taker 卖出对冲：

```text
long_entry_price  = A maker_buy_price
short_entry_price = B bids 按目标金额吃单后的 VWAP
raw_open_spread   = short_entry_price / long_entry_price - 1
```

### 3.4 平仓机会计算

平仓默认优先 Maker + Taker。若持仓为 A 多、B 空，价差收敛后可以选择：

```text
方案 1:
    在 A 挂 Maker 卖单平多。
    A 成交后，在 B 吃 asks 买入平空。

方案 2:
    在 B 挂 Maker 买单平空。
    B 成交后，在 A 吃 bids 卖出平多。
```

收益计算仍以实际成交价为准：

```text
long_exit_price
short_exit_price
raw_close_spread = short_exit_price / long_exit_price - 1
```

开仓后希望价差收敛，因此平仓时的 `raw_close_spread` 应低于开仓价差。实际盈亏必须按 Maker 成交价、Taker VWAP、手续费、资金费率一起计算。

### 3.5 双 Taker 锁利平仓

如果当前两边同时 Taker 平仓后的净利润率已经足够高，可以直接双边 Taker 平仓锁定利润，不必继续等 Maker 平仓。

触发条件必须用“可成交净利润率”，不是用表面价差：

```text
dual_taker_close_profit_pct =
    (gross_spread_pnl
     + realized_funding_pnl
     - open_fee_paid
     - dual_taker_close_fee_est
     - close_slippage_buffer
     - safety_buffer)
    / initial_bundle_notional
```

建议模拟阶段先记录多个阈值的表现：

```text
dual_taker_close_profit_pct >= 0.20%: 观察，不一定平
dual_taker_close_profit_pct >= 0.30%: 可配置锁利
dual_taker_close_profit_pct >= 0.50%: 强烈建议锁利
```

实际阈值要由模拟数据决定。长尾币波动大，若已出现足够净利润，继续等 Maker 可能会把利润还给市场。

双 Taker 平仓订单簿方向：

```text
多头腿平仓:
    卖出平多，吃 bids

空头腿平仓:
    买入平空，吃 asks
```

双 Taker 锁利必须满足：

- 两边 5 档深度都能覆盖待平数量。
- 两边盘口更新时间都小于 `stale_quote_ms`。
- 两边预计成交后的净利润仍大于配置阈值。
- 两个交易所 API 和订单通道健康。

如果只满足一边深度，不允许主动制造新的孤腿风险；除非当前已经处于紧急风控状态。

## 4. 交易所与长尾交易池范围

首期交易所：

| 交易所 | 状态 | 说明 |
| --- | --- | --- |
| Binance | 必须支持 | RustCTA 已有基础适配，但需补批量行情订阅和统一套利接口 |
| OKX | 必须支持 | RustCTA 已有基础适配，但需补订单簿/资金费率/持仓模式细节 |
| Bitget | 必须新增 | 需要新增交易所适配 |
| Gate | 必须新增 | 需要新增交易所适配 |

首期不以 BTC/ETH 等红海品种为主，也不把“全市场监控”作为实盘目标。系统可以在后台维护全市场元数据，但策略执行池只选 100-300 个长尾 USDT 永续交易对。

候选池来源：

- 新上线合约：上线时间短、不同交易所价格发现速度不一致。
- 流动性较差但仍可小资金成交的币种：盘口空隙大，Maker 更容易排队成交。
- 极端行情币种：短时间暴涨暴跌，交易所之间报价和深度恢复速度不同。
- 资金费率显著分化的币种：价差收益和 funding 收益可能同时存在。
- 朋友模拟页面中持续出现价差的 Bitget/Gate 标的，可作为候选观察源。

默认排除：

- BTC/USDT、ETH/USDT 这类高度有效市场，除非进入极端行情观察名单。
- 盘口 5 档深度低于策略最小成交金额的交易对。
- 价差大但长期无法成交 Maker 的交易对。
- 频繁维护、只减仓、API 状态异常的交易对。
- 资金费率极端不利且即将结算的交易对。

交易对过滤规则：

- 只保留 USDT 永续。
- 必须是线性合约，即 USDT 保证金/结算。
- 必须处于可交易状态。
- 必须存在最小下单金额、步长、价格精度、合约面值等元数据。
- 必须至少出现在两个交易所中，否则无法跨所套利。
- 必须通过长尾评分进入候选池，不直接全市场开仓。

长尾评分建议：

```text
score =
    spread_frequency_score
  + maker_fill_probability_score
  + funding_divergence_score
  + volatility_event_score
  - depth_risk_penalty
  - maintenance_risk_penalty
```

后期只有当模拟收益稳定、资金量变大、服务器资源足够时，再扩大交易对数量并调整服务器配置。

标准符号统一为：

```text
PEPE/USDT
WIF/USDT
新上线币/USDT
```

每个交易所内部再转换为对应格式：

```text
Binance: BTCUSDT
OKX:     BTC-USDT-SWAP
Bitget:  BTCUSDT 或接口要求格式
Gate:    BTC_USDT 或接口要求格式
```

## 5. WebSocket 行情设计

### 5.1 订阅原则

使用批量订阅 WebSocket，不使用 REST 轮询盘口作为主数据源。

订单簿深度只订阅 5 档：

```text
depth_levels = 5
```

原因：

- 小资金对冲腿主要看前 5 档即可。
- 5 档数据更轻，延迟和内存压力低。
- 首期目标是模拟 Maker 挂单触发后的真实 Taker 对冲能力，不是做全深度撮合系统。

### 5.2 每个平台推荐订阅内容

Binance USD-M Futures：

- 订阅 partial book depth，5 档。
- 使用合并流或批量连接。
- 示例概念：`btcusdt@depth5@100ms`。

OKX SWAP：

- 订阅 `books5` 或等价 5 档订单簿频道。
- 参数使用 `instType=SWAP`、`instId=BTC-USDT-SWAP`。

Bitget Futures：

- 订阅合约 5 档订单簿频道。
- 需要确认产品类型为 USDT-FUTURES / USDT perpetual。

Gate Futures：

- 订阅 futures order book 5 档或等价深度频道。
- 只使用 USDT settle 合约。

实现要求：

- 每个交易所一个或多个 WebSocket 连接，按交易所限制切分订阅数量。
- 批量订阅失败时自动拆分重试。
- 每条订单簿消息必须记录交易所时间、本地接收时间、延迟和序列号。
- 如果订单簿超过 `stale_quote_ms` 未更新，机会引擎不得使用该盘口开仓。
- 如果 bids/asks 交叉、为空、价格为 0、数量为 0，标记为坏盘口并丢弃。

### 5.3 多线路与自动切换

跨交易所套利在大波动时最怕交易所单点故障。每个交易所必须配置多条备用线路，并对 WebSocket、REST 行情、REST 下单分别做健康检查。

这里的“线路”不只指交易所 URL，也包括不同网络出口：

- 官方主域名。
- 官方备用域名或备用 WebSocket 频道。
- 不同服务器出口 IP。
- 代理出口或专线出口。
- 自建行情 relay，但只能用于行情，不能伪造交易所订单状态。

交易线路必须优先使用官方接口域名。任何非官方 relay 只能作为行情缓存或延迟对照，不能作为下单确认来源。

线路类型：

```text
market_ws_primary
market_ws_backup
private_ws_primary
private_ws_backup
rest_public_primary
rest_public_backup
rest_private_primary
rest_private_backup
```

每条线路维护健康状态：

```text
endpoint
status
last_ok_at
last_error_at
latency_ms_p50
latency_ms_p95
consecutive_errors
rate_limit_hits
last_sequence_gap_at
```

自动切换规则：

```text
if ws_no_message_ms > ws_stale_limit_ms:
    切换到备用 WebSocket

if sequence_gap_detected:
    标记订单簿不可信，重订阅或切换线路

if rest_private_order_latency_p95 > threshold:
    暂停新开仓，只允许平仓

if order_api_error_rate > threshold:
    交易所进入 degraded 状态
```

故障分级：

```text
healthy:
    可开仓、可平仓

degraded:
    禁止新开仓，允许降低仓位和平仓

close_only:
    只允许平仓和撤单

offline:
    不再依赖该交易所报价，所有含该交易所的新机会失效
```

在大波动中，如果某交易所行情正常但下单接口异常，不能继续开新 Maker 单。否则 Maker 成交后可能无法在另一端对冲。

## 6. Maker+Taker 手续费模型

模拟阶段先使用官网公开普通费率。后续实盘前必须改为账户真实费率，因为 VIP、返佣、点卡、BNB 抵扣、交易量等级都会影响结果。

建议首期默认配置：

| 交易所 | maker | taker | 备注 |
| --- | ---: | ---: | --- |
| Binance USD-M Futures | 0.0200% | 0.0500% | 按普通 USD-M Futures 公开费率作为模拟默认值 |
| OKX Futures/SWAP | 0.0200% | 0.0500% | 按普通等级公开费率作为模拟默认值 |
| Bitget Futures | 0.0200% | 0.0600% | 按普通合约公开费率作为模拟默认值 |
| Gate Futures | 0.0150% | 0.0500% | 按普通合约公开费率作为模拟默认值，需上线前再次核验 |

由于本工具采用 Maker + Taker，手续费必须按每条腿实际角色计算：

```text
open_fee =
    maker_entry_notional * maker_exchange_maker_fee
  + taker_hedge_notional * taker_exchange_taker_fee

close_fee =
    maker_close_notional * maker_close_exchange_maker_fee
  + taker_close_notional * taker_close_exchange_taker_fee

total_fee = open_fee + close_fee
```

如果触发紧急平仓，才按双 Taker 计算：

```text
emergency_close_fee =
    long_exit_notional * long_exchange_taker_fee
  + short_exit_notional * short_exchange_taker_fee
```

注意：

- 手续费以 USDT 计价近似计算。
- Maker 费率可能为正、为 0 或返佣，配置必须允许负数。
- 模拟页面必须展示 Maker 成交手续费、Taker 对冲手续费、紧急 Taker 平仓手续费。
- 页面上必须展示手续费扣减前和扣减后的收益。
- 每个交易所费率必须配置化，不能写死在代码里。
- 若后续账户实际费率低于官网公开费率，应保留两套结果：`public_fee_pnl` 和 `account_fee_pnl`。

## 7. 资金费率模型

资金费率是跨所永续套利的关键变量，必须从模拟第一版开始纳入。

同一交易对在不同交易所可能出现：

- 资金费率方向不同。
- 结算周期不同。
- 结算时间不同。
- 上限/下限不同。
- 预测资金费率和实际结算资金费率不同。

### 7.1 资金费方向

永续合约常见规则：

```text
funding_rate > 0:
    多头支付资金费
    空头收取资金费

funding_rate < 0:
    多头收取资金费
    空头支付资金费
```

对套利组合：

```text
long_leg_funding  = - long_notional  * long_exchange_funding_rate
short_leg_funding = + short_notional * short_exchange_funding_rate
net_funding       = long_leg_funding + short_leg_funding
```

如果资金费率为正，做空那边通常收钱；如果资金费率为负，做空那边通常付钱。不同平台资金费差异会显著改变持仓收益。

### 7.2 资金费结算

模拟阶段至少支持两种模式：

```text
estimated 模式：
    每次刷新使用当前资金费率，按距离下次结算的持仓时间估算。

settlement 模式：
    到达实际资金费结算时间时，将当时持仓名义价值乘以实际资金费率入账。
```

建议首期采用 settlement 模式作为最终收益，estimated 模式只用于风险提示。

资金费入账公式：

```text
funding_payment = signed_position_notional * funding_rate

对多头：
    signed_position_notional = -abs(notional)

对空头：
    signed_position_notional = +abs(notional)
```

模拟持仓必须保存：

- 每条腿的交易所。
- 每条腿方向。
- 每次资金费结算时间。
- 结算时标记价格或盘口中间价。
- 使用的资金费率。
- 单腿资金费。
- 组合净资金费。

### 7.3 开仓过滤

开仓时必须估算下一次资金费风险：

```text
expected_net_profit =
    spread_profit
  - maker_entry_fee
  - taker_hedge_fee
  - expected_maker_close_fee
  - expected_taker_close_fee
  + expected_funding
  - taker_slippage_buffer
  - maker_non_fill_penalty
```

如果距离资金费结算小于配置阈值，例如 5 分钟，应特别谨慎：

```text
if minutes_to_funding <= no_open_before_funding_mins
    and expected_funding < 0:
        不开仓
```

## 8. Maker 触发与 Taker 对冲模拟

模拟必须拆成两步：先判断 Maker 是否可能成交，再判断成交后 Taker 对冲是否有足够深度。

### 8.1 Maker 成交模拟

Maker 挂单不会立刻假设成交，需要记录一个排队与触发模型。

输入：

```text
maker_side: buy 或 sell
maker_price
best_bid
best_ask
last_trade_price
book_updates
trade_updates
max_wait_ms
```

首期可用保守简化模型：

```text
卖出 Maker 成交条件：
    后续 best_ask <= maker_price
    或成交价 >= maker_price

买入 Maker 成交条件：
    后续 best_bid >= maker_price
    或成交价 <= maker_price
```

如果没有逐笔成交数据，使用订单簿变化近似判断；文档和页面必须标记为 `book_inferred_fill`，不能当成真实成交。

Maker 模拟输出：

```text
maker_filled
maker_fill_price
maker_wait_ms
maker_queue_risk
fill_inference_type
```

如果 Maker 在 `max_wait_ms` 内没有成交：

- 撤销模拟挂单。
- 不开对冲腿。
- 记录一次 `maker_timeout`。

### 8.2 Taker 对冲模拟

由于只订阅 5 档订单簿，模拟成交必须明确深度不足时的处理方式。

输入：

```text
side: buy 或 sell
target_notional_usdt
orderbook_bids[5]
orderbook_asks[5]
```

买入吃单：

- 使用 asks。
- 从最低 ask 开始累计。

卖出吃单：

- 使用 bids。
- 从最高 bid 开始累计。

输出：

```text
filled_qty
filled_notional
vwap_price
levels_used
depth_enough
slippage_pct
```

如果 5 档深度不足：

- `depth_enough=false`
- Maker 未成交时不允许开仓。
- Maker 已成交但 Taker 对冲深度不足时，标记为 `orphan_leg_risk`，并按风控规则模拟紧急处理。
- 已有持仓平仓时，允许标记为“平仓深度不足”，但不能假装全部成交。

VWAP 计算：

```text
vwap_price = filled_notional / filled_qty
```

滑点计算：

```text
buy_slippage_pct  = vwap_price / best_ask - 1
sell_slippage_pct = 1 - vwap_price / best_bid
```

### 8.3 孤腿风险模拟

Maker 成交后，如果 Taker 对冲失败或部分成交，必须进入孤腿风险流程：

```text
1. 用更宽的 slippage 上限尝试第二次 Taker 对冲。
2. 仍失败则模拟回滚 Maker 腿，即反向 Taker 平掉已成交 Maker 腿。
3. 记录 orphan_loss、extra_fee、max_unhedged_ms。
```

模拟报告必须单独统计：

- Maker 成交次数。
- Taker 对冲成功率。
- 孤腿次数。
- 孤腿平均损失。
- 最大未对冲时间。

## 9. 机会计算

对任意两个交易所 A、B，同一个 USDT 永续交易对有两个方向：

```text
方向 1:
    A 做多，B 做空

方向 2:
    B 做多，A 做空
```

只要订单簿有效，就计算两种方向。

监控展示条件：

```text
raw_open_spread >= 0.001
```

模拟开仓条件：

```text
maker_taker_net_edge >= 0.005
```

建议 `maker_taker_net_edge` 计算：

```text
maker_taker_net_edge =
    raw_open_spread
  - maker_entry_fee_rate
  - taker_hedge_fee_rate
  - expected_maker_close_fee_rate
  - expected_taker_close_fee_rate
  + expected_funding_until_close
  - taker_slippage_buffer
  - maker_non_fill_penalty
  - safety_buffer
```

说明：

- `raw_open_spread` 只代表当前开仓价差。
- `maker_taker_net_edge` 才是接近真实收益空间的指标。
- 因为未来平仓手续费必然发生，所以开仓判断应提前扣除预估平仓手续费。
- Maker 不是必然成交，因此需要额外记录成交概率，而不是只看价差。

统计套利指标：

```text
spread_mean
spread_std
z_score = (current_spread - spread_mean) / spread_std
half_life_secs
mean_reversion_hit_rate
```

长尾池排序不只按价差大小，还要按：

- Maker 成交概率。
- 成交后 Taker 对冲成功率。
- 价差回归速度。
- 资金费率是否顺向。
- 交易所维护和风控风险。

## 10. 仓位大小

用户是小资金，因此仓位必须保守。

每次模拟开仓名义金额：

```text
base_notional = min(equity_exchange_a, equity_exchange_b) * 0.10
```

最终下单金额：

```text
target_notional = min(
    base_notional,
    max_bundle_notional,
    max_symbol_notional_remaining,
    max_exchange_notional_remaining,
    long_leg_5_level_depth_notional,
    short_leg_5_level_depth_notional
)
```

最小下单约束：

```text
min_required_notional = max(
    long_exchange_min_notional,
    short_exchange_min_notional
)

if target_notional < min_required_notional:
    跳过，不开仓
```

不要为了满足交易所最小下单金额而强行提高仓位。小资金环境下，这会放大冷门币风险。

## 11. 模拟持仓模型

每一次套利开仓生成一个 `ArbitrageBundle`。

建议字段：

```text
bundle_id
symbol
status
open_time
close_time
long_exchange
short_exchange
long_entry_vwap
short_entry_vwap
long_qty
short_qty
entry_spread
current_spread
target_notional
open_fee
close_fee
funding_pnl
gross_spread_pnl
net_pnl
max_adverse_spread
max_favorable_spread
close_reason
```

状态：

```text
observing
open_simulated
closing_simulated
closed
expired
risk_stopped
depth_insufficient
```

## 12. 平仓逻辑

平仓分三档处理。

### 12.1 常规 Maker+Taker 平仓

默认平仓条件：

```text
current_spread <= close_spread_pct
```

例如：

```text
entry_spread = 0.70%
close_spread = 0.10%
```

平仓收益：

```text
long_pnl  = (long_exit_vwap - long_entry_vwap) * long_qty
short_pnl = (short_entry_vwap - short_exit_vwap) * short_qty

gross_spread_pnl = long_pnl + short_pnl
net_pnl = gross_spread_pnl - open_fee - close_fee + funding_pnl
```

### 12.2 双 Taker 锁利平仓

当可成交净利润率达到阈值时，允许两边都用 Taker 平仓，直接锁定利润：

```text
if dual_taker_close_profit_pct >= lock_profit_dual_taker_pct
    and both_books_depth_enough
    and both_order_routes_healthy:
        双 Taker 平仓
```

建议默认参数：

```text
lock_profit_dual_taker_pct: 0.003
strong_lock_profit_dual_taker_pct: 0.005
```

含义：

- `0.003` 表示净利润率达到 0.30% 后，可以双 Taker 锁定。
- `0.005` 表示净利润率达到 0.50% 后，优先锁定利润，不继续等 Maker。

双 Taker 锁利要在日志中单独标记：

```text
close_reason = dual_taker_lock_profit
```

并记录：

- 平仓前理论净利润。
- 平仓后实际净利润。
- 额外 Taker 成本。
- 因锁利避免的后续回撤，模拟中可回放统计。

### 12.3 紧急平仓

以下场景触发紧急平仓或只平不新开：

- 任一交易所进入 `degraded` 且当前 bundle 暴露在该交易所。
- Maker 平仓长时间未成交且价差回撤。
- 资金费率即将结算且方向明显不利。
- 价差扩大超过止损阈值。
- 私有订单通道延迟或错误率异常。
- WebSocket 行情断流，无法确认盘口。

紧急平仓允许双 Taker，但必须先检查是否会造成更大亏损；如果某一端交易所完全不可下单，进入单腿风险处理。

其他平仓条件：

- 持仓时间超过 `max_hold_secs`。
- 价差继续扩大超过 `stop_spread_widen_pct`。
- 任一交易所订单簿长时间不可用。
- 资金费率转为明显不利。
- 交易对进入维护或只减仓状态。

## 13. Web 页面设计

页面应包含五个主要区域。

### 13.1 实时机会

字段：

```text
symbol
long_exchange
short_exchange
long_entry_vwap
short_entry_vwap
raw_open_spread
maker_taker_net_edge
open_fee_est
close_fee_est
expected_funding
depth_notional
slippage_pct
book_age_ms
can_open
reject_reason
```

### 13.2 模拟持仓

字段：

```text
bundle_id
symbol
long_exchange
short_exchange
entry_spread
current_spread
target_notional
gross_spread_pnl
fee_paid_est
funding_pnl
net_pnl
hold_time
next_funding_time
close_signal
dual_taker_close_profit_pct
suggested_close_mode
```

### 13.3 已平仓记录

字段：

```text
bundle_id
symbol
open_time
close_time
entry_spread
exit_spread
gross_pnl
fee_total
funding_pnl
net_pnl
close_reason
```

### 13.4 交易所状态

字段：

```text
exchange
ws_status
subscribed_symbols
last_message_age_ms
bad_book_count
reconnect_count
funding_source_status
fee_config
route_status
active_endpoint
latency_p95_ms
degraded_reason
```

### 13.5 线路与风控事件

字段：

```text
time
exchange
route_type
old_endpoint
new_endpoint
event
latency_ms
error
action
```

必须在页面显示：

- 当前是否允许新开仓。
- 哪些交易所只允许平仓。
- 哪些交易所线路正在备用通道。
- 最近 1 小时孤腿风险次数。
- 最近 1 小时双 Taker 锁利次数。

## 14. 配置示例

```yaml
strategy:
  name: cross_exchange_arbitrage_usdt_perp
  mode: simulation
  log_level: INFO

market:
  quote_asset: USDT
  market_type: futures
  depth_levels: 5
  stale_quote_ms: 1000
  min_common_exchanges: 2

thresholds:
  monitor_raw_spread_pct: 0.001
  open_maker_taker_net_edge_pct: 0.005
  close_spread_pct: 0.001
  lock_profit_dual_taker_pct: 0.003
  strong_lock_profit_dual_taker_pct: 0.005
  stop_spread_widen_pct: 0.004
  slippage_buffer_pct: 0.0005
  safety_buffer_pct: 0.0005

sizing:
  capital_fraction_of_smaller_equity: 0.10
  max_bundle_notional: 100.0
  max_symbol_notional: 200.0
  max_exchange_usage_pct: 0.35

fees:
  use_account_fee_api: false
  defaults:
    binance:
      maker: 0.0002
      taker: 0.0005
    okx:
      maker: 0.0002
      taker: 0.0005
    bitget:
      maker: 0.0002
      taker: 0.0006
    gate:
      maker: 0.00015
      taker: 0.0005

funding:
  enabled: true
  mode: settlement
  refresh_secs: 30
  no_open_before_funding_mins: 5
  block_if_expected_funding_negative_pct: 0.001

exchanges:
  binance:
    enabled: true
    position_mode: hedge
    ws_batch_size: 80
    routes:
      market_ws:
        - wss://fstream.binance.com/ws
        - wss://fstream.binance.com/stream
      rest_public:
        - https://fapi.binance.com
      rest_private:
        - https://fapi.binance.com
  okx:
    enabled: true
    position_mode: net
    ws_batch_size: 80
    routes:
      market_ws:
        - wss://ws.okx.com:8443/ws/v5/public
      private_ws:
        - wss://ws.okx.com:8443/ws/v5/private
      rest_public:
        - https://www.okx.com
      rest_private:
        - https://www.okx.com
  bitget:
    enabled: true
    position_mode: hedge
    ws_batch_size: 80
    routes:
      # 实装时填写 Bitget 官方合约 WebSocket/REST 主线路、备用线路和备用网络出口。
      market_ws: []
      private_ws: []
      rest_public: []
      rest_private: []
  gate:
    enabled: true
    position_mode: one_way
    ws_batch_size: 80
    routes:
      # 实装时填写 Gate 官方 futures WebSocket/REST 主线路、备用线路和备用网络出口。
      market_ws: []
      private_ws: []
      rest_public: []
      rest_private: []

universe:
  mode: curated_long_tail
  target_symbol_count: 100
  max_symbol_count: 300
  include_symbols: []
  exclude_symbols:
    - BTC/USDT
    - ETH/USDT
  min_24h_quote_volume: 1000000

routing:
  ws_stale_limit_ms: 3000
  order_latency_p95_degrade_ms: 1200
  order_error_rate_degrade: 0.05
  auto_failover: true
  degraded_mode_blocks_new_entries: true
```

## 15. 与 RustCTA 现有系统的集成建议

建议新增模块：

```text
src/strategies/cross_exchange_arbitrage/
├── config.rs
├── mod.rs
├── symbols.rs
├── orderbook.rs
├── opportunity.rs
├── simulation.rs
├── funding.rs
├── fees.rs
├── dashboard.rs
├── routing.rs
├── risk.rs
└── exchange_adapter.rs
```

建议新增配置：

```text
config/cross_exchange_arbitrage_usdt.yml
```

建议监控接口：

```text
GET /api/cross-arb
GET /api/cross-arb/opportunities
GET /api/cross-arb/positions
GET /api/cross-arb/history
```

当前 `monitor_server` 已有 Axum Web 服务，可以扩展现有服务，也可以先新增独立 `cross_arb_server`，避免影响现有网格监控。

## 16. 工业级策略评估与补全

从跨交易所对冲量化策略开发角度看，当前思路方向是合理的：小资金优先做 Maker+Taker，精选长尾资产，先模拟再实盘，比“双 Taker 全市场扫价差”更接近可盈利路径。

但要达到工业级，还必须补齐以下能力。

### 16.1 已经合理的部分

- Maker+Taker 降低手续费和滑点，适合小资金。
- 长尾资产池比 BTC/ETH 更可能出现可交易价差。
- 5 档订单簿对小资金足够，数据成本低。
- 资金费率差异从第一版模拟纳入，避免高估收益。
- 双 Taker 锁利平仓作为补充，可以在利润足够时避免等待 Maker 造成回吐。
- 多线路和自动降级是跨所策略必需项。

### 16.2 仍需补齐的关键项

1. 成交概率模型
   Maker 挂单不等于成交。模拟必须统计 Maker 排队时间、成交概率、撤单率、挂单后价差消失概率。

2. 孤腿风险预算
   Maker 成交后另一端 Taker 对冲失败，是这个策略最大的实盘风险。必须设置单笔最大孤腿亏损、最大未对冲时间、最大连续孤腿次数。

3. 交易所状态机
   每个交易所必须有 `healthy/degraded/close_only/offline` 状态。状态不是日志提示，而是直接影响是否允许开仓。

4. 可成交净利润模型
   开仓、平仓、锁利都必须用 5 档 VWAP、手续费、资金费、滑点 buffer、线路状态一起计算。

5. 长尾资产动态池
   100-300 个币不是固定列表，应按每天/每小时得分更新，但实盘交易池变更要有冷却时间，避免频繁追热点。

6. 交易所规则一致性
   Binance/OKX/Bitget/Gate 的持仓模式、reduce-only、position side、最小数量、合约面值、价格步长都不同，必须在适配层标准化。

7. 风控熔断
   包括单币种熔断、交易所熔断、全局熔断、资金费熔断、线路熔断、订单错误熔断。

8. 真实成本复盘
   模拟收益必须拆分为价差收益、Maker 手续费、Taker 手续费、资金费、孤腿损失、锁利机会成本。

### 16.3 建议的工业级运行模式

```text
observe:
    只订阅和记录，不模拟开仓。

simulation:
    盘口驱动模拟 Maker 成交和 Taker 对冲。

shadow:
    生成真实订单计划，但不发送订单，和市场回放对比。

live_small:
    小资金、白名单、低并发实盘。

live_scaled:
    模拟和小资金阶段稳定后再扩大交易对和服务器配置。
```

### 16.4 是否完善的判断

当前方案在策略方向上已经比较完整，但在工程上还不能直接实盘。它适合作为“工业级模拟系统”的开发文档，下一步应先实现：

- 长尾币池构建。
- 批量 WebSocket 5 档盘口。
- Maker 成交概率模拟。
- Taker 对冲 VWAP 模拟。
- 资金费率结算。
- 双 Taker 锁利回测。
- 多线路状态机。

只有这些指标连续跑出稳定结果后，再进入实盘小资金。

## 17. 分阶段计划

### Phase 1：模拟基础设施

- 新增文档和配置。
- 新增交易所元数据加载。
- 建立长尾 USDT 永续候选池。
- 实现 5 档订单簿内存缓存。
- 实现 Maker 成交概率模型。
- 实现 Taker 对冲 VWAP 模型。
- 实现机会扫描和 Web 展示。
- 不下真实订单。

### Phase 2：资金费率与手续费

- 接入四个平台资金费率接口。
- 记录下一次资金费时间。
- 模拟资金费结算。
- 页面展示资金费贡献。
- 手续费从配置默认值读取。

### Phase 3：模拟交易账本

- 满足净价差条件时生成模拟开仓。
- 满足平仓条件时生成模拟平仓。
- 模拟双 Taker 锁利平仓。
- 保存持仓和历史记录。
- 统计胜率、平均持仓时间、最大不利价差、手续费占比、资金费占比。

### Phase 3.5：线路与故障演练

- 配置每个交易所的多条行情和交易线路。
- 模拟 WebSocket 断流、REST 下单超时、交易所只减仓、盘口停滞。
- 验证 degraded/close_only/offline 状态切换。
- 验证故障期间禁止新开仓，只允许撤单和平仓。

### Phase 4：小资金实盘准备

只有当模拟数据稳定后再进入本阶段。

- 接入账户真实手续费。
- 增加 IOC 限价吃单保护。
- 增加单腿成交和部分成交处理。
- 增加真实订单 ID 与套利 bundle 绑定。
- 增加急停、只平不新开、交易所级熔断。
- 先白名单少量长尾币，限制同时持仓数量。

## 18. 必须避免的错误

- 不能用 last price 计算套利机会，必须用盘口可成交 VWAP。
- 不能把做空开仓误算成吃 asks；做空开仓是卖出，吃 bids。
- 不能把做多开仓误算成吃 bids；做多开仓是买入，吃 asks。
- 不能忽略平仓手续费。
- 不能忽略资金费率差异。
- 不能把 0.1% 展示价差当成开仓信号。
- 不能在 5 档深度不足时假设成交。
- 不能强行把小资金仓位提高到超过风险预算。
- 不能在模拟收益里只展示总收益，必须拆分价差收益、手续费、资金费。
- 不能在交易所线路 degraded 时继续开新 Maker 单。
- 不能忽略 Maker 成交后 Taker 对冲失败的孤腿损失。
- 不能因为双 Taker 平仓利润看起来为正，就忽略两边 5 档深度和订单通道状态。

## 19. 上线前验收标准

模拟阶段验收：

- WebSocket 连续运行 7 天无严重数据断层。
- 每个交易所订单簿延迟、重连次数、坏盘口次数可观测。
- 至少记录 7 天机会数据和模拟交易数据。
- 所有模拟成交都能追溯到当时 5 档订单簿。
- 手续费和资金费能按 bundle 拆分。
- 低深度交易对不会被错误开仓。
- 页面能清楚显示当前净收益是否主要来自价差还是资金费。
- Maker 成交概率、Taker 对冲成功率、孤腿损失可统计。
- 双 Taker 锁利阈值经过历史模拟验证。
- 多线路自动切换经过故障演练。

实盘前验收：

- 所有交易所的最小下单金额、数量步长、价格精度通过实测。
- 所有交易所持仓模式配置通过实测。
- IOC 限价单语义通过小额测试。
- 单腿失败处理通过模拟测试。
- 急停功能通过测试。
- 默认只能白名单交易，不能直接全市场实盘。
- 交易所进入 degraded/close_only/offline 后，策略行为符合预期。

# 现货跨所套利库存调整流程

## 目标

库存调整的目标不是“把仓位立刻清掉”，而是让套利策略在库存不均衡时继续可控运行。

核心原则：

- 正常套利后的库存偏移，优先用反向套利或转账修复。
- 单腿成交后的风险仓位，只有在不亏、利润覆盖或紧急风控时才平。
- 手动停止、停止清仓、未激活的交易对，不允许自动接管建仓。
- 每一次库存调整都必须单独记账，不能把仓位平衡亏损藏在套利收益里。

## 从日志看出的问题

POND 日志：

```text
15:40:43 GateIO 买入 9832.8 POND @ 0.002022，MEXC 卖出没有成交
15:40:45 GateIO 仓位平衡卖出 5630.5 POND @ 0.001992，收益 -0.168915
15:40:55 GateIO 仓位平衡卖出 4202.2 POND @ 0.001993，收益 -0.121863
```

这不是正常套利完成后的库存偏移，而是单腿失败：

```text
买入腿成交，卖出腿没有成交
```

系统变成多持有 POND。正确处理方式不是马上卖出，而是判断：

```text
卖出有效价 >= 买入成本价 + 手续费 + 滑点保护
```

当买入成本是 `0.002022`，后续只能在有效卖价高于成本时卖出。日志中
`0.001992` 和 `0.001993` 都低于 `0.002022`，所以这是亏损平衡。除非触发
紧急风控，否则不应该作为普通仓位平衡执行。

OBOL 类似，但方向相反：

```text
卖出腿成交，买入腿没有成交
```

系统变成少币、多 USDT。正确处理方式不是任何价格都买回，而是判断：

```text
买入有效价 <= 原卖出价 - 手续费 - 滑点保护
```

如果买回价格高于原卖出价，就是亏损买回，只能算紧急风控或利润覆盖，不
能算正常套利。

## 两类库存问题必须分开

### 1. 单腿失败风险

单腿失败是订单执行风险：

- 买成了，卖没成：多出 base，需要找机会卖出。
- 卖成了，买没成：少了 base，需要找机会买回。

这类问题进入 `OneSidedExposure` 状态。

### 2. 正常套利库存偏移

完整套利成交后，库存自然会偏：

```text
低价交易所：USDT 减少，币增加
高价交易所：币减少，USDT 增加
```

这不是错误，也不应该马上市价平掉。它进入 `InventoryDrift` 状态。

## 介入时机

### 1. 下单前

每次准备套利前，先检查两边是否都能完成：

```text
高价交易所是否有足够币可以卖
低价交易所是否有足够 USDT 可以买
两边最小下单金额是否满足
订单簿是否新鲜
预计净利润是否大于手续费和滑点
```

如果高价交易所缺币，或者低价交易所缺 USDT，不能下单，直接进入：

```text
PausedNeedInventory
```

或者：

```text
InventoryDrift
```

这里不能为了补库存自动建仓，尤其是手动停止或停止清仓状态。

### 2. 下单中

推荐执行顺序：

```text
1. 先在高价交易所卖出
2. 卖出失败：停止，不买入
3. 卖出成功：再在低价交易所买入
4. 买入失败：记录少币风险，进入 OneSidedExposure
```

这样可以避免 POND 那种“先买入成功，但卖不出去”的多币风险。

如果某些模式必须买入优先，那么买入成功、卖出失败时，要进入多币风险恢
复流程，不能当成普通库存偏移。

### 3. 两腿都成交后

两边都成交，才算一次正常套利。

这时要做三件事：

```text
记录真实套利收益
更新每个交易所的币和 USDT
检查剩余库存还能不能支持下一轮套利
```

如果库存不足以继续同方向套利，停止同方向下单，进入库存调整流程。

### 4. 定时扫描

定时任务只负责寻找合适修复机会：

- 单腿失败是否可以不亏恢复
- 是否出现反向套利
- 是否适合转账
- 是否允许用已实现利润做小额市场调整
- 是否触发紧急风控

定时任务不能看到缺库存就直接市价平衡。

### 5. 手动停止或停止清仓

如果交易对处于手动停止、停止清仓、未激活状态：

```text
禁止自动建仓
禁止自动接管底仓
禁止因为缺库存而自动买入
```

只允许明确配置的平仓或风险恢复任务，并且仍然必须满足：

```text
不亏恢复
利润覆盖
紧急风控
```

## 单腿失败恢复流程

### 买入成功、卖出失败

状态：

```text
LongOneSidedExposure
```

含义：

```text
多持有币，少了 USDT
```

恢复动作是卖出多出来的币，但必须满足：

```text
有效卖价 >= 成本价 + 安全边际
```

有效卖价：

```text
effective_sell_price = bid_price * (1 - sell_fee_rate - slippage_buffer_rate)
```

如果不满足，不卖，继续等待。

POND 就属于这个场景。成本价 `0.002022`，后续卖价 `0.001992` 和
`0.001993` 低于成本，所以普通流程应该等待，而不是卖出。

### 卖出成功、买入失败

状态：

```text
ShortOneSidedExposure
```

含义：

```text
少了币，多了 USDT
```

恢复动作是买回币，但必须满足：

```text
有效买价 <= 原卖出价 - 安全边际
```

有效买价：

```text
effective_buy_price = ask_price * (1 + buy_fee_rate + slippage_buffer_rate)
```

如果不满足，不买，继续等待。

## 正常库存调整流程

正常套利后，如果一个交易所 USDT 用完，另一个交易所币卖少了，处理优先级
如下。

### 1. 等反向套利

如果价格方向反过来了，而且反向套利扣除费用后仍然赚钱，就直接做反向套利。

这是最好的库存修复方式，因为它既恢复库存，又继续赚钱。

### 2. 转账调整

如果交易所支持转账，并且链路、手续费、到账时间都可控：

```text
把低价交易所多出来的币转到高价交易所
把高价交易所多出来的 USDT 转到低价交易所
```

这比亏损市价调整更合理。

### 3. 利润覆盖的市场调整

如果没有反向套利，也不能转账，可以用已实现利润覆盖小额调整：

```text
调整亏损 <= 当前币种已实现利润 - 保底利润
```

更保守的做法：

```text
调整亏损 <= (当前币种已实现利润 - 保底利润) * 最大可用比例
```

例如设置 `max_profit_use_ratio = 0.5`，最多只允许用一半利润修复库存。

### 4. 暂停等待

如果以上都不满足，暂停该交易对：

```text
PausedNeedInventory
```

暂停不是错误，而是防止“赚一点套利，亏更多仓位平衡”。

## 为什么这样能不亏

它能不亏，不是因为库存调整天然赚钱，而是因为策略不在不利价格强平。

多币时：

```text
只有卖出收入 >= 原买入成本 + 手续费 + 滑点保护，才卖
```

少币时：

```text
只有买回成本 <= 原卖出收入 - 手续费 - 滑点保护，才买
```

正常库存偏移时：

```text
优先反向套利，其次转账，再其次用利润覆盖，最后暂停
```

所以它不会像 POND 日志那样，买入 `0.002022` 后又在 `0.001992` 和
`0.001993` 主动卖亏。

## LAB 和 VSN 的当前思路

### VSN

如果当前方向是：

```text
GateIO 低价买入 VSN
Bitget 高价卖出 VSN
```

连续做几次后会变成：

```text
GateIO: USDT 少，VSN 多
Bitget: USDT 多，VSN 少
```

这时不能继续强行同方向套利。正确选择：

- 等 VSN 反向价差出现。
- 把 Bitget 的 USDT 转到 GateIO。
- 把 GateIO 的 VSN 转到 Bitget。
- 只在 VSN 已实现利润覆盖成本时做小额市场调整。
- 否则暂停 VSN。

### LAB

如果 LAB 当前是：

```text
Bitget LAB 不足
GateIO USDT 不足
```

即使看到价差，也不能开套利单。因为高价侧没有币可卖，低价侧没有 USDT 可
买，容易产生单腿风险。

正确处理：

- 手动补 LAB 到高价交易所。
- 手动补 USDT 到低价交易所。
- 等反向套利自然修复。
- 或暂停 LAB。

## 建议状态字段

每个交易对维护以下字段：

```text
symbol
state
active_direction
base_inventory_by_exchange
quote_inventory_by_exchange
one_sided_exposure_qty
one_sided_cost_price
one_sided_started_at
symbol_realized_pnl
rebalance_realized_pnl
emergency_rebalance_pnl
pause_reason
```

建议状态：

```text
Ready
Arbitraging
InventoryDrift
PausedNeedInventory
OneSidedExposure
WaitingRecovery
ProfitPreservingRecovery
EmergencyRecovery
ManualStopped
StopCleaning
```

## 建议配置

```yaml
inventory_rebalance:
  enabled: true

  # 每个交易对总建仓目标，例如 20U，不是每个交易所各 20U。
  target_total_notional_usdt: 20

  # 每次套利单取两个交易所最小订单金额的最大值。
  min_order_notional_mode: max_of_two_venues

  # 保留几轮库存，低于这个值就不继续同方向套利。
  min_cycles_buffer: 3
  target_cycles_buffer: 5

  # 不亏恢复保护。
  no_loss_safety_bps: 8
  slippage_buffer_bps: 5

  # 利润覆盖。
  profit_floor_usdt: 0.02
  max_profit_use_ratio: 0.5
  max_rebalance_notional_usdt: 5

  # 紧急风控。
  emergency_after_seconds: 20
  emergency_max_exposure_usdt: 15
  emergency_adverse_bps: 30

  allow_market_rebalance: true
  allow_transfer_rebalance: false
  allow_auto_initial_entry: false
  allow_auto_exit: false
```

## 决策伪代码

```text
发现价差:
    如果交易对不是 Active:
        记录机会，不建仓，不接管
        return

    如果高价交易所币不足:
        标记 PausedNeedInventory
        return

    如果低价交易所 USDT 不足:
        标记 PausedNeedInventory
        return

    如果订单金额低于两边最小金额最大值:
        return

    如果预计净收益 <= 0:
        return

    先卖高价交易所
    如果卖失败:
        return

    再买低价交易所
    如果买失败:
        记录 ShortOneSidedExposure
        return

    记录套利收益
    检查库存是否还能继续下一轮

处理单腿风险:
    如果是买成卖没成:
        如果有效卖价 >= 成本价 + 安全边际:
            卖出恢复
        否则如果利润池覆盖亏损:
            卖出恢复，记为 rebalance_pnl
        否则如果触发紧急风控:
            卖出恢复，记为 emergency_rebalance_pnl
        否则等待

    如果是卖成买没成:
        如果有效买价 <= 原卖出价 - 安全边际:
            买回恢复
        否则如果利润池覆盖亏损:
            买回恢复，记为 rebalance_pnl
        否则如果触发紧急风控:
            买回恢复，记为 emergency_rebalance_pnl
        否则等待

处理正常库存偏移:
    如果有反向套利且赚钱:
        做反向套利
    否则如果可以转账:
        创建转账任务
    否则如果市场调整不亏或利润覆盖:
        小额市场调整
    否则:
        PausedNeedInventory
```

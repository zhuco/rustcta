# 日内趋势策略重构与新增功能方案

## 1. 目标
- 用低延迟、全链路可追踪的执行栈替换当前“REST 轮询 + 单点入场”流程，确保在 Binance USDⓈ-M 永续市场上以 Maker-only 方式稳定成交，即便账户余额较小（目前约 185 USDT）。
- 将行情、信号、下单、风控、仓位管理串成闭环：三组入场策略同时挂单，动态分配资金，成交后立即布设两档止盈与 ATR 止损，并能在策略重启时自动接管已有仓位。
- 强化可观测性：任何下单、重挂、止盈止损调整、仓位接管等动作都有明确日志，方便回溯。

## 2. 行情与指标管线
### 2.1 WebSocket 主数据源
- 建立 Binance 永续 K 线 WebSocket 订阅，维护 1m / 5m / 15m 三个时间框架的 ring buffer。
- 仅在启动或重连时用 REST 补齐历史；合并逻辑负责时间对齐、缺口填补与去重。
- 提供 `CandleCache` 服务对外暴露 `get_latest(interval)` 和订阅接口，供分析器、入场策略、ATR 止损模块复用。

### 2.2 指标服务
- 依托 WebSocket 缓存计算 EMA 斜率、布林带、ATR(5m/15m)、斐波那契锚点、OFI/盘口倾斜等；生成 `IndicatorSnapshot` 并写入 `SharedSymbolData`。
- 若延迟 > 200 ms 或缺数过多，降低 `data_quality_score`，让信号在置信度层面体现数据质量。

## 3. 入场策略与资金分配
### 3.1 三组入场模型
1. **均线回调**：快慢均线靠拢时，在均线中值附近挂 maker 单。
2. **斐波那契回撤**：根据最近一段波段高/低点，找出 0.382/0.5/0.618 等关键位，以最强共振点为挂单价格。
3. **布林带中轨**：围绕布林中轨挂单，可根据 regime 调整偏差。

三种入场模式在同一个信号周期内同时存在，互相独立，满足“3 组订单都要成交”的需求。

### 3.2 资金与杠杆
- 配置新增 `entry_leverage`（默认 1×）与 `entry_allocation`（如 `{ma:0.4, fib:0.3, bb:0.3}`）。
- 计划名义 = `account_equity * entry_leverage`，再按 allocation 分配给三条挂单；每条挂单仍需满足交易所 `minNotional`，不足时重新归一分配。
- 取消“可用余额 ≥ 20 USDT”硬限制，改为以 exchange metadata 中的 `minNotional` + 最大杠杆约束作为下限。

### 3.3 自适应 Allocation
- 新增 `allocation_regimes` 配置，允许针对不同行情状态（Trending / Ranging / Volatile）设置 `{ma, fib, bb}` 权重，例如：
  ```yaml
  allocation_regimes:
    trending: { ma: 0.6, fib: 0.25, bb: 0.15 }
    ranging:  { ma: 0.25, fib: 0.35, bb: 0.40 }
    extreme:  { ma: 0.4, fib: 0.4,  bb: 0.2  }
  ```
- `RegimeClassifier` 根据多时间框架斜率、ATR/BBW 等指标实时判别状态，`CapitalAllocator` 按 regime 动态切换 allocation 并写日志。
- 若实际 allocation 与 symbol 库存或最小名义冲突，重新分配剩余比重并记录调整原因。

### 3.4 批量 Maker 下单
- 构建三条 `OrderRequest` 后，通过 `create_batch_orders` 一次性提交，全部设置 `postOnly` 和专属 `clientOrderId`。
- Maker 单默认 15 s 超时；若未成，撤单并按最新行情重定价，可配置最大重挂次数。每次动作写日志，包含 entry 模式、价格、原因。

## 4. 成交后动作
### 4.1 成交跟踪
- `OrderTracker` 记录每条挂单所在的入场模式及资金份额。只有收到用户流 `ORDER_TRADE_UPDATE` 的实际成交，才会在 `PositionManager` 中创建/扩展仓位。
- 若三条挂单陆续成交，会按时间顺序追加到同一仓位（须满足库存上限与风险阈值）。

### 4.2 ATR 保护
- 每次成交后立即提交：
  - **两档止盈**：参考 ATR 计算 1R / 2R 目标价，使用 `GTX + reduceOnly + positionSide`。
  - **一档 ATR 止损**：`STOP_MARKET + reduceOnly + positionSide`，触发价 = entry ± ATR × multiplier。
- `StopManager` 周期性根据 5m / 15m ATR 更新止损、止盈位置（撤旧单+下新单），确保浮盈阶段能逐步锁利。所有调整记录到日志。

### 4.3 PnL 驱动止损上移
- 新增 `pnl_trailing` 配置，例如：
  ```yaml
  pnl_trailing:
    enable: true
    atr_multiple: 1.0
    lock_levels:
      - { profit_atr: 1.5, stop_offset_atr: 0.5 }
      - { profit_atr: 2.5, stop_offset_atr: 0.2 }
  ```
- 当仓位浮盈 ≥ `profit_atr * ATR` 时，将止损上移到 `current_price - stop_offset_atr * ATR`（做多）或对称位置（做空），并可选择“关键结构位”作为最终锚点（例如最近 swing high/low 或布林中轨）。
- StopManager 记录每次“PnL 触发止损上移”的事件，包含原止损、目标止损、触发 ATR/浮盈值，方便复盘。

## 5. 仓位管理与风险
### 5.1 启动接管
- 启动时读取 `get_positions()` 和当前挂单：
  - 按 symbol 重建 `TrendPosition`（方向、均价、剩余仓位）。
  - 如果缺少保护单，按照最新 ATR 重新挂止损/止盈，并注册监听。

### 5.2 库存与冷却
- 配置中已有 `symbol_inventory_limits`，继续沿用；新增每 symbol 300 s 冷却（可配置），避免频繁补仓导致库存迅速耗尽。
- `RiskController::approve_trade` 改为使用“拟建仓名义 / 账户权益”来判断 `max_total_exposure`、`max_risk_per_trade`，并结合库存、杠杆做最终裁决。

### 5.3 资金快照
- `refresh_account_metrics` 输出“total / available / used”并缓存为 `AccountCapital`，供仓位 sizing 使用，也方便日志追踪当前杠杆情况。

## 6. 订单生命周期与日志
- 每条挂单会记录“创建 → 超时撤单 → 重挂 → 成交/失败”等事件，字段包括 symbol、entry_mode、价格、数量、原因。
- 批量下单返回错误时，日志列出失败订单及 error code，必要时启用重试。
- 止盈、止损调整均打印“旧价 → 新价”以及触发原因；仓位平掉时，自动清理缓存与剩余挂单。

## 7. 测试与验证
1. **单元测试**：WebSocket K 线合并、指标计算、资金分配器、批量下单 builder、ATR 止损更新。
2. **集成测试**：利用 mock/exchange sandbox 驱动三组入场、部分成交、重新挂止盈止损、重启接管等关键路径。
3. **多轮实盘自测**：
   - 第一次启动 60s 后停机，分析日志，逐步修正报错（精度、reduceOnly、超时等）。
   - 重复启动/停止，直至确认“WebSocket 收到订单 → 成交 → 提交两档止盈 + 一档止损”能稳定运行，再切入长期运行。

## 8. 后续优化方向
- 自适应 allocation（例如趋势强时提高 MA 占比，震荡时提升布林/斐波那契权重）。
- 引入 Funding-aware 过滤器；高 funding 成本时减仓或暂停新单。
- PnL 驱动的止损上移：浮盈到达 N×ATR 时，自动把止损推到关键结构位。
- 暴露 Prometheus 指标（行情延迟、订单执行耗时、保护单覆盖率），方便监控。

---
该文档将作为后续开发的蓝图，指导 `TrendIntradayStrategy` 的数据、信号、执行、风控与测试迭代。*** End Patch

# 工业级多交易对对冲网格整改开发文档

## 1. 背景与目标

当前 `solusdc_hedged_grid` 已修复 Binance USDⓈ-M Futures 用户数据流地址问题，成交事件可以通过 WebSocket 到达，并进入即时处理路径。但在多交易对同时波动时，仍观察到成交处理延迟存在波动，例如最快接近 0ms，较慢样本可到 200ms 以上。

本次整改目标不是简单追求单次极限速度，而是建设长期稳定、可观测、可回退、可扩展的工业级对冲网格执行系统。

核心目标：

- 成交后补单必须由 WebSocket 用户流优先驱动，不依赖普通轮询周期。
- 多交易对同时成交时，延迟应稳定、可解释、可定位。
- 单个交易对异常不得拖垮全部策略；账户级异常必须统一降级和熔断。
- 下单、撤单、重建网格、同步订单必须具备幂等能力。
- 任何重大改动都必须支持 Git 回退和运行时灰度。

## 2. 当前问题判断

### 2.1 已确认修复的问题

- Binance Futures 私有 WebSocket 已使用 `wss://fstream.binance.com/private/ws/<listenKey>`。
- ETH 对冲网格最新日志中成交来源已变为 `source=ws`，不再主要依赖 `reconcile_trade`。
- 对冲网格主循环已存在 WebSocket 直接分支，收到 `ws_rx.recv()` 后会立即执行成交处理。

### 2.2 仍存在的工业级风险

当前 6 个对冲网格交易对以多个独立策略进程运行。若使用同一个 Binance 账户，每个进程会创建独立 listenKey 和 WebSocket 用户流，并各自进行订单解析、日志输出、REST 同步和批量下单。

在市场大幅波动时，多个交易对通常同时成交，因此会出现：

- 多进程重复接收同一账户维度的订单事件。
- 多进程同时竞争 REST 行情、仓位、挂单、成交查询接口。
- 多进程同时提交 `create_batch_orders`，共享同一账户/API Key/IP 的限速。
- 日志 IO 在高频成交时放大延迟抖动。
- 单进程内部如正在执行 REST reconcile，WebSocket 事件虽然优先，但仍可能排队等待当前 await 完成。

因此，当前架构能做到“可用”，但还不是长期工业级的最优形态。

## 3. 与趋势网格的差异结论

趋势网格看起来没有同类问题，不能简单归因于“一个配置文件多个交易对”。代码上看，趋势网格在 `thread_per_config = true` 时仍会为每个交易配置启动独立任务。它的问题暴露较少，主要是策略模型不同：

- 趋势网格是单向持仓，订单状态耦合较少。
- 对冲网格是双向持仓，需要同时维护 LONG/SHORT 两套 positionSide。
- 对冲网格一次成交可能触发对称补单、平仓补单、库存校正，动作数量更多。
- 趋势网格部分 taker 成交逻辑允许延后重置，不是每笔成交都要求毫秒级滚动。
- 对冲网格要求成交价附近快速补齐网格，因此更容易暴露用户流、执行器、限速和日志延迟。

结论：趋势网格代码可作为“多交易对配置管理”和“状态隔离”的参考，但不能直接照搬为对冲网格最终方案。

## 4. 目标架构

建议演进为账户级共享基础设施 + 交易对级独立引擎的混合架构。

```text
Binance Account
    |
    | one listenKey / one private WebSocket
    v
UserStreamRouter
    |
    | route by symbol + client_order_id
    v
+-------------------+  +-------------------+  +-------------------+
| SymbolEngine ETH  |  | SymbolEngine BTC  |  | SymbolEngine SOL  |
| GridEngine        |  | GridEngine        |  | GridEngine        |
| OrderMap          |  | OrderMap          |  | OrderMap          |
| Local State       |  | Local State       |  | Local State       |
+-------------------+  +-------------------+  +-------------------+
    \                    |                    /
     \                   |                   /
      v                  v                  v
          SharedOrderExecutor / RateLimiter
                  |
                  v
          Binance REST Batch Orders
```

### 4.1 核心组件

#### UserStreamRouter

- 每个账户只维护一个 Futures listenKey。
- 只建立一个私有 WebSocket 用户流。
- 解析 `ORDER_TRADE_UPDATE` 后按 `symbol`、`client_order_id`、`order_id` 路由。
- 对未知订单进入隔离队列，不直接影响正常交易对。
- 记录用户流延迟：交易所事件时间、收到时间、入队时间、出队时间。

#### SymbolEngine

- 每个交易对一个独立状态机。
- 内部包含 `GridEngine`、`OrderMap`、持仓快照、订单快照、成交去重表。
- 只处理属于自己的 symbol 和 client id。
- 单交易对异常只暂停该 symbol，不影响其他 symbol。

#### SharedOrderExecutor

- 所有交易对下单、撤单、批量下单统一进入账户级执行器。
- 按交易所限速、请求权重、优先级调度。
- 成交补单优先级高于普通 reconcile。
- 支持小窗口微批处理，但不能牺牲成交补单时效。
- 记录下单耗时：排队时间、签名时间、网络往返时间、交易所响应时间。

#### ReconcileScheduler

- REST reconcile 不再由每个 symbol 随机抢接口。
- 按账户级预算统一调度。
- WebSocket 成交处理期间，reconcile 必须降优先级。
- 支持 symbol 级别退避，不因单个交易对失败拖慢全部。

## 5. 延迟 SLO 与观测指标

工业级系统必须先定义指标，再谈优化。

### 5.1 推荐 SLO

- WebSocket 收到成交到开始处理：P50 < 10ms，P95 < 30ms。
- 开始处理到生成补单 actions：P95 < 10ms。
- 补单 actions 进入执行器到开始 REST 提交：P95 < 20ms。
- REST batch order 往返：P95 < 250ms，P99 单独观测。
- 成交到补单请求发出：P95 < 80ms，P99 < 200ms。

说明：REST 到 Binance 的网络往返不可完全控制，因此最终成交到交易所确认不能只看本地耗时，必须拆分统计。

### 5.2 必须增加的日志字段

每笔成交处理日志必须包含：

- `symbol`
- `source=ws/reconcile_open_order/reconcile_trade`
- `exchange_event_time`
- `ws_receive_time`
- `route_enqueue_time`
- `engine_start_time`
- `engine_done_time`
- `executor_enqueue_time`
- `rest_submit_start_time`
- `rest_submit_done_time`
- `order_id`
- `client_order_id`
- `position_side`
- `actions_count`
- `batch_size`
- `latency_breakdown_ms`

### 5.3 告警规则

- 连续 3 笔成交补单请求发出耗时 > 300ms，输出 WARN。
- 单笔成交补单请求发出耗时 > 1000ms，输出 ERROR 并触发 symbol 级自检。
- 用户流断开超过 5 秒，进入降级模式。
- listenKey 续期失败连续 2 次，触发账户级重连。
- REST 429/418 出现，执行器进入账户级限速保护。

## 6. 分阶段开发计划

### Phase 0：代码快照与回退基线

当前已完成：

- 已提交当前工作区快照。
- 已推送到 `origin/master`。
- 回退基线提交：`08f6277 chore: backup live strategy state before hedged grid refactor`。

后续所有改动必须基于新分支开发，避免直接污染当前可运行版本。

建议分支：

```bash
git checkout -b feature/industrial-hedged-grid
```

### Phase 1：观测与指标补齐

目标：不改变交易行为，只补齐延迟拆分数据。

开发内容：

- 为 WebSocket listener 增加收到时间戳。
- 为 `handle_user_stream_event` 增加处理开始/结束时间戳。
- 为 `execute_actions` 增加执行器提交前后时间戳。
- 将成交日志统一成结构化字段，便于后续脚本分析。
- 增加 `logs/runtime/*hedged_grid*` 延迟分析脚本。

验收标准：

- 能从日志中区分 WS 延迟、引擎处理延迟、REST 下单延迟。
- 能对每个交易对输出 P50/P95/P99。
- 不改变实际下单价格、数量、positionSide。

### Phase 1.5：近端网格完整性专项修复

目标：解决固定价差对冲网格中“成交后补了订单，但远端订单没有被正确取消或近端缺档”的策略正确性问题。该阶段优先级高于多进程改单进程，因为它直接影响网格形态和收益逻辑。

当前 ETH 已观察到的典型异常：

- 固定价差为 `2.5`。
- 最高买入单为 `2368.88`。
- 最低卖出单为 `2376.38`。
- 实际近端买卖间隔为 `7.5`。
- 按策略预期，近端买卖间隔应为 `2 * spacing = 5.0`。
- 该现象说明中间缺少一档，或者成交滚动后未能正确执行“补近端 + 撤远端 + 保持档位连续”的原子动作。

需要重点排查的原因：

- 成交后按单笔 `ExecutionReport` 逐条滚动，而对冲网格一次真实成交经常由两笔订单组成，导致两个滚动动作之间状态短暂不一致。
- 当前滚动逻辑更偏向“数量守恒”，即超过 `levels_per_side` 时才裁剪，但没有把近端价差完整性作为硬约束。
- 某些情况下只补了新网格订单，但应撤的远端订单没有被取消，导致同侧远端残留、近端缺档或档位整体漂移。
- 补单成功、撤单失败、撤单延迟、REST 部分失败时，当前日志不足以明确判断最终交易所挂单形态。
- 周期 reconcile 虽能修复部分状态，但成交后的近端网格缺档不应等待周期修复。

开发内容：

- 新增 `grid_shape_check`，每次成交处理和每次批量下单/撤单后都校验本地目标网格形态。
- 固定价差模式下强制校验 `lowest_sell - highest_buy ~= 2 * spacing`，允许 tick size 量化误差。
- 校验每一侧 open/close 订单是否满足目标档数、连续价差和远端裁剪规则。
- 新增 `repair_near_gap` 动作：发现近端差距为 `3 * spacing` 或更大时，立即补缺失档位，不等待普通 reconcile。
- 新增 `trim_far_orders_after_fill` 动作：成交批次结束后统一裁剪远端多余订单，不能只依赖单笔成交内的即时 `count > levels` 判断。
- 对同一 symbol、同一时间窗口内的成交做小窗口批处理，例如 `50ms` 到 `100ms`，先应用全部成交，再统一生成补单和撤远端动作。
- 批量执行时必须把补近端和撤远端作为同一批次的可观测动作记录，若部分失败必须触发立即 reconcile。
- 日志中增加 `grid_shape_before`、`grid_shape_after`、`highest_buy`、`lowest_sell`、`expected_gap`、`actual_gap`、`missing_levels`、`far_orders_to_cancel`。

验收标准：

- ETH 固定 `2.5` 价差运行时，正常状态下 `lowest_sell - highest_buy` 应稳定等于 `5.0`，只允许 tick size 范围误差。
- 每次成交后，如果补了近端订单，必须能在同一处理链路中看到是否需要取消远端订单，以及取消的是哪一笔、原因是什么。
- 若远端订单未取消成功，日志必须输出 WARN，并在下一轮高优先级修复中继续取消，不能静默残留。
- 若近端出现 `7.5`、`10.0` 等超过预期的空档，必须立即输出 ERROR 并触发 `repair_near_gap`。
- 回放最近 ETH 日志时，分析脚本必须能标记出哪一次成交后产生了缺档，以及当时是否漏撤远端订单。

### Phase 2：降低当前多进程抖动

目标：在不大改架构的前提下提升稳定性。

开发内容：

- WebSocket 成交处理优先级高于普通 tick reconcile。
- 普通 reconcile 拆成低优先级任务，避免占用成交关键路径。
- `cancel_unknown_orders` 降低频率，避免高波动时抢 REST。
- 高频成交日志降噪，保留结构化关键日志，减少同步 IO 压力。
- 对 `get_my_trades` 设置更长周期或仅在 WS 异常时启用。

验收标准：

- 多交易对同时成交时，本地处理耗时 P95 明显下降。
- REST batch 往返仍可能波动，但能明确区分为外部网络/交易所耗时。
- 不影响异常恢复和订单一致性。

### Phase 3：账户级 UserStreamRouter

目标：一个账户只建立一个私有用户流。

开发内容：

- 新增 `AccountUserStreamRouter`。
- 支持多个 symbol 注册和注销。
- 按 symbol/client id 分发 `ExecutionReport`。
- 对未知订单、非策略订单、历史残留订单进入隔离日志。
- listenKey keepalive、重连、过期恢复统一由 router 管理。

验收标准：

- 6 个交易对共享一个 listenKey。
- 所有 symbol 的成交都能正确路由。
- 单个 symbol 停止不影响用户流。
- 用户流断开后能恢复，并触发 reconcile 补偿。

### Phase 4：共享下单执行器与账户级限速

目标：解决多交易对同时成交时的 REST 争抢。

开发内容：

- 新增 `SharedOrderExecutor`。
- 成交补单优先级高于普通重建、同步、撤未知订单。
- 支持账户级请求速率控制。
- 支持 symbol 级公平调度，避免一个交易对刷屏拖慢其他交易对。
- 对 429、418、网络超时做统一退避。

验收标准：

- 高波动时不会出现多个进程无序抢 API。
- 下单排队耗时可观测。
- 交易所限速错误下降。
- 单个 symbol 下单失败不会污染其他 symbol 的 `OrderMap`。

### Phase 5：Multi Hedged Grid 单进程多交易对

目标：形成最终工业级部署形态。

开发内容：

- 新增 `multi_hedged_grid` controller。
- 配置文件支持多个 trading pair。
- 每个 symbol 独立 `GridEngine`、`OrderMap`、risk state。
- 共享用户流、共享执行器、共享 reconcile scheduler。
- 支持 symbol 级启动、暂停、重启、重建网格。
- 支持账户级紧急停止。

验收标准：

- 一个进程可稳定运行 6 个双向持仓交易对。
- 任意单个 symbol 异常不会导致全进程退出。
- 账户级异常能统一熔断，避免失控下单。
- 灰度运行至少 24 小时后，再替换原多进程部署。

## 7. 配置演进建议

短期保留现有单 symbol 配置，减少迁移风险。

中长期新增多 symbol 配置，例如：

```yaml
strategy: multi_hedged_grid
account:
  account_id: binance_main
  market_type: futures

symbols:
  - symbol: ETHUSDC
    enabled: true
    grid:
      spacing: 2.5
      order_qty: 0.011
  - symbol: BTCUSDC
    enabled: true
    grid:
      spacing: 25.0
      order_qty: 0.001

execution:
  ws_priority: true
  shared_order_executor: true
  max_order_queue_ms: 200
  reconcile_priority: low
```

## 8. 风险控制要求

必须具备以下保护后，才能认为是工业级策略：

- 启动前校验账户是否为 Hedge Mode。
- 每个订单必须显式设置 `positionSide`。
- client order id 必须可反推出 symbol、side、intent、grid id。
- 每笔成交必须去重，避免重复补单。
- 部分成交必须按 delta 处理，不能把累计成交误判为新成交。
- 本地 `OrderMap` 与交易所 open orders 不一致时，优先 reconcile，不盲目重建。
- 账户级最大挂单数、最大名义敞口、单 symbol 最大亏损必须强制限制。
- API 限速、网络异常、用户流断开时必须进入降级模式，而不是继续盲目下单。

## 9. 测试方案

### 9.1 单元测试

- 成交去重。
- 部分成交 delta 计算。
- 双向持仓 positionSide 映射。
- client id 路由。
- fill 后 actions 生成。
- reject/cancel/expired 状态处理。

### 9.2 集成测试

- 模拟 6 个 symbol 同时成交。
- 模拟 WebSocket 乱序事件。
- 模拟 REST batch 部分失败。
- 模拟 listenKey 过期。
- 模拟 429 限速。
- 模拟进程重启后从 open orders 恢复状态。

### 9.3 实盘灰度

- 第一阶段只开 ETH，一个 symbol 运行 2 小时。
- 第二阶段 ETH + SOL，运行 6 小时。
- 第三阶段 6 个交易对全量，观察 24 小时。
- 每阶段都必须产出延迟 P50/P95/P99 报告。

## 10. 部署与回退方案

### 10.1 Git 回退

当前安全基线：

```bash
git reset --hard 08f6277
git push --force-with-lease origin master
```

如果后续使用功能分支开发，回退方式为：

```bash
git checkout master
git branch -D feature/industrial-hedged-grid
```

### 10.2 运行时回退

- 新架构先使用新策略名和新配置，不覆盖旧 `solusdc_hedged_grid_*` 配置。
- 保留旧 6 进程启动脚本。
- 灰度失败时，停止新进程，重新启动旧 6 进程。
- 禁止新旧架构同时管理同一 symbol 的同一策略订单前缀。

### 10.3 数据回退

- 所有新 client order id 使用新前缀，例如 `mhg-`。
- 新架构只识别自身前缀订单。
- 若回退旧架构，旧架构启动前必须先确认或清理新前缀挂单。

## 11. 当前实现状态（feature/industrial-hedged-grid-phase1）

截至当前功能分支，开发任务状态如下：

- Phase 1：已完成成交路径延迟拆分日志，输出 `latency_breakdown`，包含 WS 排队、引擎处理、执行器提交和总耗时。
- Phase 1.5：已完成近端网格完整性修复，成交后执行 `repair_near_gap` 和 `trim_far_orders_after_fill`，并增加固定价差回归测试。
- Phase 2：已完成当前多进程模式下的抖动治理，`get_my_trades` 和 `cancel_unknown_orders` 降为低频维护动作，成交 WS 路径保持高优先级。
- Phase 3：已完成共享用户流路由代码路径，`multi_hedged_grid` 使用账户级共享私有 WebSocket，并按 symbol 分发成交事件。
- Phase 4：已完成共享订单执行器基础模块 `SharedOrderExecutor`，包含账户级节流接口和统一批量下单构造；实盘旧单 symbol 路径暂未强制切换到该执行器。
- Phase 5：已完成 `multi_hedged_grid` 多交易对运行入口和配置模型，可在单进程内启动多个 symbol engine。

当前长测策略：

- 已使用功能分支 release binary 重启 `ETH/USDC` 单 symbol 对冲网格。
- 暂不合并 `master`，等待长测日志确认近端 gap、撤远端、成交延迟均稳定后再考虑合并。
- `multi_hedged_grid` 虽已编译通过，但建议先在小仓位或模拟配置灰度，不直接替换当前 6 个实盘进程。

## 11. 推荐执行顺序

推荐先做 Phase 1 和 Phase 2，不立即进入单进程多交易对重构。

原因：

- 当前系统已经可通过 WS 实时处理成交，主要问题是高波动下抖动。
- 先补齐延迟拆分，才能确认慢在本地、队列、REST、还是交易所。
- 直接重构成单进程多交易对风险较高，容易引入状态隔离和回退复杂度问题。

最终方向可以是单进程多交易对双向持仓，但必须经过共享用户流、共享执行器、灰度验证三个阶段后再切换。


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

## 11. 推荐执行顺序

推荐先做 Phase 1 和 Phase 2，不立即进入单进程多交易对重构。

原因：

- 当前系统已经可通过 WS 实时处理成交，主要问题是高波动下抖动。
- 先补齐延迟拆分，才能确认慢在本地、队列、REST、还是交易所。
- 直接重构成单进程多交易对风险较高，容易引入状态隔离和回退复杂度问题。

最终方向可以是单进程多交易对双向持仓，但必须经过共享用户流、共享执行器、灰度验证三个阶段后再切换。


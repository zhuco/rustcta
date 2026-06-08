# 高频现货套利并行 AI 开发任务

状态日期：2026-06-07

本文档把现货跨所套利向准高频方向升级的 8 个优化点拆成 8 个可并行
交付的 AI 开发任务。每个任务应由一个独立 AI 在独立分支或会话中完成。

目标不是一步到位做微秒级传统 HFT，而是在现有 RustCTA 架构上完成：

- 行情事件触发决策
- 严格订单簿状态
- 双腿并发执行
- 本地快路径风控
- 私有流优先的订单状态源
- 端到端延迟观测
- 带延迟模型的回放
- 费用、库存和资金占用模型

当前部署假设：

- 生产策略会部署在低延迟云服务器，不依赖本地家用网络。
- 东京服务器会新增实测；新加坡已有参考延迟：Binance 约 40ms、Bybit
  约 15ms、Bitget 约 45ms、Gate 约 150ms。
- Gate 在 P0 阶段不作为小价差高速主交易腿，除非东京实测显著改善。
- 热路径不能跨区域 RPC。每个区域应本地运行 strategy + gateway + execution
  hot path，中央控制面只做配置、审计、监控和人工操作入口。

## 全局规则

所有 AI 在编辑代码前必须：

1. 运行 `git status --short`，识别无关脏文件。
2. 阅读本文档、`docs/architecture_module_layout.md`、
   `docs/multi_exchange_spot_arbitrage.md`、
   `docs/交易所网关/通用机制/websocket_market_data.md`、`docs/交易所网关/通用机制/order_reconciliation.md`。
3. 只修改自己任务的允许范围。
4. 不回滚、不覆盖其他 AI 或用户已有改动。
5. 不绕过 dry-run、live-dry-run、preflight、kill switch、disabled-symbol、
   reconciliation 等现有实盘安全门。
6. 不把 raw exchange credential 写入新的策略、控制面或测试 fixture。
7. 不让热路径依赖数据库、控制面 HTTP、文件 IO、慢锁、跨区域 RPC。
8. 不引入裸 `Market` 市价单作为现货套利默认执行方式；taker 实盘优先使用
   IOC limit，并有最大滑点保护。
9. 不把 `scan_interval_ms`、dashboard snapshot、报表刷新等控制面周期任务
   当作套利决策触发源。
10. 保持公共结构可序列化，关键事件必须包含 schema version 或兼容迁移说明。

## 精度约束

短期不要求全仓库替换 `f64`，但新代码必须遵守：

- 下单价格、下单数量、成交数量、成交均价、fee、pnl、notional、balance、
  inventory、reservation 不能在核心边界继续扩散裸 `f64`。
- 行情筛选、统计指标、spread bps、latency histogram、评分模型可以继续用
  `f64`。
- 新增热路径类型优先使用定点整数语义，例如 price ticks、quantity lots、
  notional micros。
- 账本、报表、审计可使用 `rust_decimal::Decimal`，但不要在每条行情事件上做
  大量 Decimal 运算。
- 所有实盘订单在提交前必须经过 tick size、step size、min notional、
  max notional 的统一 quantize 和 validate。

## 协作矩阵

| 任务 | 主题 | 主要范围 | 必须协调 |
| --- | --- | --- | --- |
| 1 | 事件驱动行情与价差引擎 | `src/data`, `retired strategy tree/spot_spot_taker_arbitrage`, `strategies/spot-spot-arbitrage` | 2, 6, 7 |
| 2 | 严格 L2 订单簿与 resync | `src/data`, `retired exchange tree`, `crates/rustcta-exchange-*` | 1, 6 |
| 3 | 双腿并发执行与单腿恢复 | `src/execution`, `crates/rustcta-execution-*` | 4, 5, 8 |
| 4 | 风控快路径 | `src/risk`, `src/execution`, `crates/rustcta-execution-*` | 3, 8 |
| 5 | 私有流优先订单状态 | `src/execution/user_stream.rs`, `retired exchange tree`, `crates/rustcta-exchange-gateway` | 3, 6 |
| 6 | 端到端延迟指标 | `src/data`, `src/execution`, `src/market`, `crates/*` | 1, 2, 3, 5, 7 |
| 7 | 带延迟模型的回放 | `retired backtest tree`, `tests`, `data/replay` 相关 | 1, 6, 8 |
| 8 | 费用、库存、资金占用模型 | `src/execution/fee_model.rs`, `retired strategy tree/spot_spot_taker_arbitrage`, `src/control/spot_control` | 3, 4, 7 |

## 共享接口约定

各任务尽量先落接口，再接实现，避免互相阻塞。

### 行情事件约定

新增或扩展事件时应能表达：

```text
exchange
market_type
canonical_symbol
exchange_symbol
book_side / bids / asks
event_kind: snapshot | delta | stale | resync | gap | error
exchange_timestamp
gateway_received_monotonic_ns
strategy_received_monotonic_ns
sequence / first_update_id / final_update_id
is_tradeable
stale_reason
```

### 执行事件约定

新增或扩展执行事件时应能表达：

```text
strategy_id
run_id
bundle_id
leg_id
exchange
client_order_id
exchange_order_id
submit_monotonic_ns
ack_monotonic_ns
private_update_monotonic_ns
fill_monotonic_ns
requested_qty
filled_qty
average_price
fee
status
requires_reconcile
one_sided_exposure
```

### 延迟字段约定

- 审计时间继续使用 `DateTime<Utc>`。
- 热路径耗时使用 monotonic timestamp 或 `Instant` 派生出的 `*_monotonic_ns`。
- 不要用不同机器上的 monotonic timestamp 直接相减；跨进程/跨机器延迟需要
  使用 exchange timestamp、gateway receive wall clock、NTP/chrony 状态一起解释。

## Task 1：事件驱动行情与价差引擎

状态：已收口，2026-06-07，版本 `0.3.10`。

目标：让 spot-spot 套利的热路径由行情事件直接触发增量价差计算，避免依赖
`scan_interval_ms` 或 `BookCache` 周期扫描做交易决策。

允许范围：

- `src/data/book_event.rs`
- `src/data/book_cache.rs`
- `src/data/websocket_books.rs`
- `retired strategy tree/spot_spot_taker_arbitrage/**`
- `strategies/spot-spot-arbitrage/**`
- 相关 focused tests
- 相关配置样例和本文档引用的小补充

目标工作：

1. 引入事件驱动 spread engine 输入接口，例如 `on_book_event` 或
   `BookEventReceiver`。
2. 策略热路径维护每个 `exchange + symbol` 的最新 top-of-book 或 L2 引用状态。
3. 每次任一交易所某 symbol book 更新时，只重算受影响的 directed venue pair。
4. `BookCache` 保留给 read model、监控、debug、fallback，不再作为热路径唯一
   决策状态。
5. 将 `scan_interval_ms` 明确降级为监控刷新、WebSocket idle 健康检查或 replay
   节流参数。
6. 避免每个行情事件 clone 完整 Vec book；能引用或小对象复制时不要深拷贝。
7. 对重复事件、stale 事件、gap 事件做幂等处理。

不得做：

- 不得修改具体交易所签名和下单逻辑。
- 不得让 spread engine 直接调用 REST 下单。
- 不得绕开风控和 execution router。

验收标准：

- 有单测证明：一个 book event 只触发相关 symbol / venue pair 重算。
- 有单测证明：stale book 不会产生可执行机会。
- 有单测证明：禁用 symbol / exchange 仍然阻止机会输出。
- 配置或文档明确 `scan_interval_ms` 不属于套利热路径触发源。

收口摘要：

- `WebSocketBookManager` 已发布非阻塞 `BookEvent` broadcast stream；正常快照、
  stale、reconnect、stream ended、sequence gap 都会进入同一事件通道。
- `BookCache` 新增引用式事件更新接口，继续作为监控、debug、preflight 和 fallback
  read model，不再作为 `websocket_cache` 模式的交易热路径触发源。
- `strategies/spot-spot-arbitrage` 新增 `EventDrivenSpreadEngine`，维护每个
  `exchange + symbol` 的最新可交易 book 状态，并按单事件只返回受影响 symbol
  的 directed venue pair。
- `spot_spot_taker_arbitrage` 在 `market_data_mode: websocket_cache` 下订阅
  `BookEvent`，事件到达才触发受影响 symbol 的机会扫描；`scan_interval_ms`
  仅作为 WebSocket idle 健康检查、监控和报表维护节奏。
- stale / gap / checksum / reconnect / error 事件会把策略内部 book 标为不可交易，
  不输出可执行机会。
- `docs/交易所网关/通用机制/websocket_market_data.md` 已同步说明事件驱动策略路径。

收口验证：

```bash
cargo fmt
cargo test -p rustcta-strategy-spot-spot-arbitrage event_driven --all-features
RUST_TEST_THREADS=1 cargo test spot_spot --all-features
```

交接给后续任务：

- Task 2 继续接入严格 L2 delta merge、sequence gap resync 和 checksum 校验。
- Task 6 在同一 `BookEvent` 链路上补齐 monotonic timestamp 与端到端 latency
  histogram。
- Task 7 回放侧需要复用事件驱动接口，避免 replay 与 live 热路径长期分叉。

建议验证：

```bash
cargo test spot_spot --all-features
cargo test live_websocket_books --all-features
cargo fmt --check
```

分派 Prompt：

```text
你是 AI-1。完成 docs/high_frequency_spot_arbitrage_parallel_tasks_zh.md
中的 Task 1：事件驱动行情与价差引擎。先读取全局规则和共享接口约定。
只修改 Task 1 允许范围。目标是让 spot-spot 套利热路径由 BookEvent 触发
增量 spread 计算，BookCache 只作为 read model/fallback。不要改交易所签名、
下单语义、风控门或私有流实现。添加 focused tests 并运行 Task 1 验收命令。
```

## Task 2：严格 L2 订单簿、sequence gap 与自动 resync

目标：把当前 best-effort sequence 处理升级为严格的 L2 book 状态机，确保脏
订单簿不会进入高频套利决策。

允许范围：

- `src/data/book_event.rs`
- `src/data/books.rs`
- `src/data/book_health.rs`
- `src/data/websocket_books.rs`
- `retired exchange tree/**` 中的 public market-data parser/subscription
- `crates/rustcta-exchange-api/**`
- `crates/rustcta-exchange-gateway/**` 中 public book 相关协议
- 相关 parser fixture 和 tests

目标工作：

1. 定义统一 L2 delta 事件模型，区分 full snapshot、partial snapshot、delta。
2. 每个交易所 adapter 明确声明其 book 能力：snapshot-only、delta-with-sequence、
   checksum、resync endpoint、最大深度。
3. 对支持 sequence 的交易所实现 per-symbol book state machine。
4. 检测 sequence gap、回退、重复、乱序、checksum mismatch。
5. gap 或 checksum mismatch 后立即 mark stale，并触发 resync。
6. resync 成功前，该 symbol/exchange 不得输出 `is_tradeable=true` 的 book。
7. 对不支持严格 delta 的交易所，能力声明为 non-strict，并在策略层施加更高
   latency/staleness penalty 或禁止进入 P0 高频主腿。

不得做：

- 不得伪造严格 sequence 能力。
- 不得在 gap 后继续沿用旧 best bid/ask 作为可交易价格。
- 不得把 REST snapshot 失败静默吞掉并继续交易。

验收标准：

- fixture 覆盖正常 delta merge。
- fixture 覆盖 sequence gap 后 stale。
- fixture 覆盖 resync 成功后恢复 tradeable。
- 文档或 capability model 能看出哪些交易所是 strict，哪些是 best-effort。

建议验证：

```bash
cargo test book --all-features
cargo test exchange_gateway --all-features
cargo fmt --check
```

分派 Prompt：

```text
你是 AI-2。完成 docs/high_frequency_spot_arbitrage_parallel_tasks_zh.md
中的 Task 2：严格 L2 订单簿、sequence gap 与自动 resync。先读取全局规则、
docs/交易所网关/通用机制/websocket_market_data.md 和相关交易所 public WS parser。只修改 Task 2
允许范围。目标是让支持 sequence 的交易所具备严格 L2 状态机，gap/checksum
错误必须 stale 并 resync。不要改策略执行和下单逻辑。添加 fixture tests 并
运行 Task 2 验收命令。
```

## Task 3：双腿并发执行、batch order 与单腿恢复

目标：现货 taker-taker 套利双腿必须并发提交，不能第一腿 ack 后才提交第二腿。
同时要显式处理 partial fill、one-leg fill 和恢复动作。

允许范围：

- `src/execution/engine.rs`
- `src/execution/router.rs`
- `src/execution/bundle.rs`
- `src/execution/hedge.rs`
- `src/execution/order_reconciliation.rs`
- `src/execution/state_machine.rs`
- `crates/rustcta-execution-api/**`
- `crates/rustcta-execution-router/**`
- 相关 tests

目标工作：

1. 为双腿执行新增并发提交路径，例如 `submit_bundle_concurrent`。
2. 当两腿属于同一交易所且 adapter 支持 batch 时，优先 batch；跨交易所时
   使用并发 future。
3. 双腿必须共享 bundle id、leg id、idempotency key、机会 id。
4. 任一腿 ack/reject/timeout/partial fill 都必须产出结构化 bundle event。
5. one-leg fill 进入 `OneSidedExposure`，不得当作普通库存漂移。
6. 恢复动作必须遵循 `docs/spot_spot_inventory_rebalance_flow_zh.md` 的不亏恢复、
   利润覆盖、紧急风控原则。
7. live taker 订单必须有 IOC limit price 和 max slippage。

不得做：

- 不得默认用 market order。
- 不得在双腿执行中吞掉一边失败。
- 不得把 partial fill 当 full fill。
- 不得自动亏损平衡，除非显式紧急风控状态允许。

验收标准：

- 单测证明跨交易所双腿并发提交，不串行等待第一腿 ack。
- 单测证明一腿 reject 后 bundle 标记 requires_reconcile 或 one-sided risk。
- 单测证明 partial fill 会进入明确状态。
- 单测证明 live taker 缺少 IOC limit 或 max slippage 会被 block。

建议验证：

```bash
cargo test execution --all-features
cargo test order_reconciliation --all-features
cargo fmt --check
```

分派 Prompt：

```text
你是 AI-3。完成 docs/high_frequency_spot_arbitrage_parallel_tasks_zh.md
中的 Task 3：双腿并发执行、batch order 与单腿恢复。先读取全局规则、
docs/交易所网关/通用机制/order_reconciliation.md 和 docs/spot_spot_inventory_rebalance_flow_zh.md。
只修改 Task 3 允许范围。目标是双腿 taker-taker 并发提交，支持 batch 能力，
并显式处理 reject/timeout/partial fill/one-leg fill。不要绕过风控和 live gates。
添加 focused tests 并运行 Task 3 验收命令。
```

## Task 4：风控快路径、额度预占与速率限制

目标：在下单热路径中提供本地、低延迟、原子化的风险决策，避免下单前依赖慢
查询或控制面状态拉取。

允许范围：

- `src/risk/**`
- `src/execution/router.rs`
- `src/execution/engine.rs`
- `src/execution/fee_model.rs`
- `retired exchange tree/spot_reservation.rs`
- `src/control/spot_control/runtime_cache.rs`
- `crates/rustcta-execution-api/**`
- `crates/rustcta-execution-router/**`
- 相关 tests

目标工作：

1. 新增或扩展 fast risk context，包含本地 kill switch、symbol disable、
   exchange disable、额度、库存、订单速率、撤单速率、未对冲敞口。
2. 下单前进行原子 reservation：quote/base/notional/order-rate。
3. order ack/reject/fill/cancel 后释放或确认 reservation。
4. 支持 per-exchange、per-symbol、per-strategy、per-run 限额。
5. 支持 API 429、连续 reject、private stream lag、book stale 触发 exchange/symbol
   cooldown。
6. 风控决策必须输出结构化 `RiskDecision`，可审计、可回放。
7. 热路径不得同步读数据库、文件或控制面 HTTP。

不得做：

- 不得只在控制面做风控。
- 不得让 reservation 失败后继续发单。
- 不得因 dry-run 模式删除实盘风控代码路径。

验收标准：

- 单测证明额度预占是原子的。
- 单测证明 order reject/timeout 后 reservation 可释放或进入 reconcile pending。
- 单测证明超订单速率、超撤单速率、超未对冲敞口会 block。
- 单测证明 exchange cooldown 后不再产生可提交订单。

建议验证：

```bash
cargo test risk --all-features
cargo test execution_router --all-features
cargo fmt --check
```

分派 Prompt：

```text
你是 AI-4。完成 docs/high_frequency_spot_arbitrage_parallel_tasks_zh.md
中的 Task 4：风控快路径、额度预占与速率限制。先读取全局规则和现有
kill switch、disabled symbol、reservation、execution router 代码。只修改
Task 4 允许范围。目标是在下单热路径中完成本地原子风险决策和 reservation。
不要引入数据库/HTTP/文件 IO 到热路径。添加 focused tests 并运行 Task 4
验收命令。
```

## Task 5：私有流优先订单状态与 REST reconcile fallback

目标：实盘订单状态以 private WebSocket/user stream 为主，REST polling 只作为
reconciliation fallback。

允许范围：

- `src/execution/user_stream.rs`
- `src/execution/reconciler.rs`
- `src/execution/order_reconciliation.rs`
- `retired exchange tree/**` 中 private stream parser/subscription
- `crates/rustcta-exchange-api/**`
- `crates/rustcta-exchange-gateway/**` 中 private stream 相关协议
- 相关 private stream fixture 和 tests

目标工作：

1. 统一私有流订单事件模型：ack、new、partial_fill、fill、cancel、reject、
   expired、balance_update。
2. 每个 adapter 明确声明 private stream 能力和缺失项。
3. execution state machine 优先消费 private stream 事件。
4. REST get_order/get_open_orders 只在 private stream lag、断线、状态缺口、
   启动恢复时触发。
5. private stream lag 超阈值时触发 risk cooldown 或 block live orders。
6. 所有 REST fallback reconcile 必须产出审计事件。
7. 支持 client_order_id 与 exchange_order_id 双索引匹配。

不得做：

- 不得用 REST polling 替代私有流主路径。
- 不得吞掉无法匹配的私有流事件。
- 不得在 private stream 断开时继续无限制开新仓。

验收标准：

- fixture 覆盖 private fill 更新订单状态。
- fixture 覆盖 partial fill。
- fixture 覆盖 unknown order event 进入 unmatched/reconcile queue。
- 单测证明 private stream lag 会影响 live order gate。

建议验证：

```bash
cargo test user_stream --all-features
cargo test order_reconciliation --all-features
cargo test exchange_gateway --all-features
cargo fmt --check
```

分派 Prompt：

```text
你是 AI-5。完成 docs/high_frequency_spot_arbitrage_parallel_tasks_zh.md
中的 Task 5：私有流优先订单状态与 REST reconcile fallback。先读取全局规则、
docs/交易所网关/通用机制/order_reconciliation.md 和各交易所 private stream adapter。只修改 Task 5
允许范围。目标是让 execution state 优先由 private WS/user stream 驱动，
REST 只做 fallback reconcile。添加 fixture tests 并运行 Task 5 验收命令。
```

## Task 6：端到端延迟指标与区域/交易所延迟矩阵

目标：建立从行情收到、决策、发单、ack、private update、fill、reconcile 的端到端
延迟指标，支持东京/新加坡和交易所组合的真实数据决策。

允许范围：

- `src/data/**`
- `src/market/health.rs`
- `src/execution/**`
- `src/control/spot_control/snapshot_builder.rs`
- `retired root bin directory/ws_proxy_probe.rs`
- `crates/*` 中 telemetry/DTO 相关小范围变更
- `tools/ops/**` 中延迟探测命令
- 相关 tests 和 docs

目标工作：

1. 定义统一 latency span/mark：market receive、strategy receive、decision、
   risk pass、submit、ack、private update、fill、reconcile。
2. 热路径使用 monotonic timestamp 或 `Instant` 派生值记录耗时。
3. dashboard/read model 可以展示 p50/p95/p99/p999、max、sample count。
4. 增加 region/exchange/endpoint 维度：public_ws、private_ws、rest_order_ack、
   cancel_ack、resync。
5. 增加 latency probe 命令或扩展现有 probe，支持东京/新加坡对比。
6. 机会记录中保留延迟字段，便于按真实成交后净利润归因。
7. 不同机器上的 monotonic 值不得直接相减；跨机器报告必须说明时间源。

不得做：

- 不得只记录平均值。
- 不得用 `DateTime<Utc>` 代替热路径耗时指标。
- 不得把指标写入阻塞热路径的同步文件 IO。

验收标准：

- 单测或集成测试证明 latency marks 可以串成一次 opportunity lifecycle。
- read model 有 p95/p99 字段。
- probe 输出包含 region/exchange/endpoint 维度。
- 文档说明如何跑东京 vs 新加坡 24-72 小时对比。

建议验证：

```bash
cargo test latency --all-features
cargo test market_health --all-features
cargo fmt --check
```

区域对比运行说明：

- 在东京和新加坡服务器分别运行 probe，并用 `--region tokyo` /
  `--region singapore` 标记输出。
- 每个区域至少连续采样 24 小时；用于主腿选择前建议采样 72 小时，覆盖亚洲、
  欧洲和美国主要交易时段。
- public WebSocket 样例：

```bash
cargo run -p rustcta-tools-ops -- probe ws-proxy \
  --region tokyo \
  --exchange binance-spot \
  --frames 100 \
  --timeout-ms 12000
```

- 输出行必须包含 `region`、`exchange`、`endpoint`，例如
  `endpoint=public_ws`。REST order ack、cancel ack、resync 和 private WS 的
  probe 应使用相同维度写入后续矩阵。
- 同一进程内的 `*_monotonic_ns` 可相减得到热路径耗时；不同机器或不同进程的
  monotonic 值不得直接相减。跨机器报告必须同时记录 region、exchange
  timestamp、gateway receive wall clock 以及 NTP/chrony 状态。

分派 Prompt：

```text
你是 AI-6。完成 docs/high_frequency_spot_arbitrage_parallel_tasks_zh.md
中的 Task 6：端到端延迟指标与区域/交易所延迟矩阵。先读取全局规则和共享
延迟字段约定。只修改 Task 6 允许范围。目标是记录 market->decision->submit
->ack->private_update->fill 的 p50/p95/p99/p999，并支持东京/新加坡 exchange
endpoint probe。不要在热路径加入阻塞 IO。添加 focused tests 并运行 Task 6
验收命令。
```

## Task 7：带延迟模型、成交概率和队列位置的回放系统

目标：让回放不只是复现 book 事件，还能模拟行情延迟、决策延迟、下单延迟、
成交概率、队列位置和 partial fill。

允许范围：

- `retired backtest tree/**`
- `tests/backtest_*`
- `src/data/book_recorder.rs`
- `src/data/book_event.rs`
- `retired strategy tree/spot_spot_taker_arbitrage/replay.rs`
- `retired strategy tree/spot_spot_taker_arbitrage/report.rs`
- `strategies/spot-spot-arbitrage/**` 中 replay/report 相关代码
- replay fixture 和 docs

目标工作：

1. 扩展 replay event schema，包含 latency marks、sequence/gap/stale、top-of-book
   或 L2 levels。
2. 增加 latency model：固定延迟、分布采样、按 exchange/region/p99 profile。
3. 增加 execution simulator：IOC limit fill、partial fill、reject、timeout。
4. maker 模式预留 queue position 模型，不要求 P0 完整实盘 maker。
5. 回放报告输出：理论机会、延迟后机会、实际可成交机会、净利润、单腿风险次数。
6. 支持用 Task 6 的真实 latency profile 驱动 replay。
7. 回放结果必须可 deterministic repeat，随机模型要支持 seed。

不得做：

- 不得把理想盘口成交当成实盘净利润。
- 不得忽略 partial fill。
- 不得在 replay 中默认所有机会都成交。

验收标准：

- 单测证明延迟后机会可能消失。
- 单测证明 IOC partial fill。
- 单测证明固定 seed 回放结果稳定。
- 报告区分 gross theoretical pnl 与 latency-adjusted realized pnl。

建议验证：

```bash
cargo test backtest --all-features
cargo test replay --all-features
cargo fmt --check
```

分派 Prompt：

```text
你是 AI-7。完成 docs/high_frequency_spot_arbitrage_parallel_tasks_zh.md
中的 Task 7：带延迟模型、成交概率和队列位置的回放系统。先读取全局规则、
现有 backtest/replay/book_recorder 代码和 Task 6 的延迟字段约定。只修改
Task 7 允许范围。目标是让 replay 能模拟延迟、IOC fill、partial fill、
timeout 和成交后净利润。添加 focused tests 并运行 Task 7 验收命令。
```

## Task 8：费用、库存、资金占用与 venue 选择模型

目标：让现货套利决策使用真实费率、VIP 等级、maker rebate、库存可用性、
资金占用、转账成本和慢 venue penalty，而不是只看静态 spread。

允许范围：

- `src/execution/fee_model.rs`
- `src/execution/balance_reconciliation.rs`
- `retired exchange tree/spot_reservation.rs`
- `src/control/spot_control/**`
- `retired strategy tree/spot_spot_taker_arbitrage/**`
- `strategies/spot-spot-arbitrage/**`
- `config/fees.yml`
- `config/spot_spot_taker_arbitrage*.yml`
- 相关 tests 和 docs

目标工作：

1. 统一费率模型：configured fee、exchange-reported fee、VIP tier、maker rebate、
   BNB/平台币折扣、最高手续费兜底。
2. 机会计算必须使用 buy venue 和 sell venue 的实际 taker fee。
3. 增加 inventory-aware venue selection：只有两边库存足以完成交易时才可执行。
4. 增加资金占用成本、转账成本、转账到账延迟、库存再平衡成本。
5. 增加 latency penalty：Gate 这类慢 venue 默认需要更高 min net spread，或只
   进入低频/观察模式。
6. 将 `OneSidedExposure` 和 `InventoryDrift` 分开记账。
7. 报表必须区分：套利交易收益、库存恢复收益/亏损、手续费、滑点、资金占用。

不得做：

- 不得把库存恢复亏损并入套利收益。
- 不得在库存不足时自动亏损补仓。
- 不得用默认费率覆盖交易所已知真实费率而不记录来源。

验收标准：

- 单测证明不同 fee source 会影响机会净 spread。
- 单测证明库存不足会 block 机会。
- 单测证明慢 venue latency penalty 会提高门槛或禁止 P0 主腿。
- 报告区分 arbitrage pnl 与 inventory recovery pnl。

建议验证：

```bash
cargo test fee_model --all-features
cargo test spot_spot --all-features
cargo test live_preflight --all-features
cargo fmt --check
```

分派 Prompt：

```text
你是 AI-8。完成 docs/high_frequency_spot_arbitrage_parallel_tasks_zh.md
中的 Task 8：费用、库存、资金占用与 venue 选择模型。先读取全局规则、
docs/交易所网关/通用机制/fee_model.md、docs/spot_spot_inventory_rebalance_flow_zh.md 和现有
spot_spot_taker_arbitrage 代码。只修改 Task 8 允许范围。目标是让机会计算
使用真实费率、库存、资金占用、转账成本和慢 venue penalty，并把套利收益与
库存恢复收益分开记账。添加 focused tests 并运行 Task 8 验收命令。
```

## 集成顺序

这些任务可以并行开发，但推荐按以下顺序合并：

1. Task 6：延迟字段和观测 DTO，尽量早合并，给其他任务共用。
2. Task 2：严格订单簿能力和 stale/resync 语义。
3. Task 1：事件驱动 spread engine 接入严格 book 状态。
4. Task 4：风控快路径和 reservation。
5. Task 5：私有流优先订单状态。
6. Task 3：双腿并发执行和 one-sided recovery。
7. Task 8：真实费用、库存和 venue selection。
8. Task 7：用最终事件和执行模型完善回放。

如果必须全部同时推进，每个 AI 应先落本任务的 DTO/trait/fixture tests，避免
大规模改同一段 runtime 逻辑。

## P0 完成定义

P0 完成后，系统应能做到：

- 行情事件触发 spot-spot 价差计算。
- 可交易 book 必须来自 fresh 且 sequence/resync 状态可信的数据源。
- 双腿 taker-taker 并发提交，实盘使用 IOC limit 和滑点保护。
- 本地风控在发单前完成 reservation、限速、敞口检查。
- 订单状态以 private stream 为主，REST 只用于 fallback reconcile。
- 每次机会有完整 latency lifecycle。
- replay 能用真实 latency profile 评估机会是否仍可成交。
- 报表能区分套利收益、费用、滑点、库存恢复收益和单腿风险。

## 实盘门槛

进入小额 live taker-taker 前必须满足：

1. 东京和新加坡至少各运行 24 小时 public/private/order latency probe。
2. 选定 venue pair 的 public WS、order ack、private update p99 均有记录。
3. Gate 或其他慢 venue 未通过延迟门槛时不得作为 P0 高频主腿。
4. 每个 symbol 有真实 fee source、tick size、step size、min notional。
5. 每个 exchange/account/symbol 有本地库存和 reservation 状态。
6. private stream lag 超阈值时能自动 block 新开仓。
7. kill switch 默认可一键阻止全部 live orders。
8. live-small 单笔、单 symbol、总敞口均有硬上限。

## 通用最终验证

每个任务完成后至少运行自己任务的 focused tests。集成分支合并前运行：

```bash
cargo fmt --check
cargo clippy --all-targets --all-features
cargo test --all-features
```

如果因为外部服务、已有脏改动或长时间运行导致无法完整验证，必须在交付说明中
明确列出未运行项和原因。

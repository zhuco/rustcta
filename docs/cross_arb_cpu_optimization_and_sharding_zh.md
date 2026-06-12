# 跨所合约套利 CPU 优化与分片方案

Status date: 2026-06-11

本文档针对当前 `cross-exchange-arbitrage-live-runner` 的 USDT 永续合约跨所套利路径。目标是在不降低机会发现完整性的前提下，把盘口热路径 CPU 从“全量 books 扫描 + dashboard 构造 + 文件读取”改成“按 symbol 增量评估 + 后台展示/审计”，并提供适配 2 核服务器和多核本机的分片配置方案。

## 结论

- 可以把“全 books 双循环”改成“按 symbol 增量评估”，不会减少机会发现，前提是每个盘口事件都更新该 symbol 的跨所状态，并且 stale、配置、禁用路由、风控状态变化能触发缓存失效或重算。
- 不能通过丢弃盘口事件、降低 websocket 订阅频率、或者把同一个 symbol 的不同交易所拆到不同分片来降 CPU；这些做法会影响瞬时机会发现。
- Dashboard、open-decision audit、profit/control JSONL 读取不应在盘口事件热路径里执行。它们应改为后台定时任务或内存缓存消费者。
- 分片应该按 `canonical_symbol` 做，而不是按交易所做。一个 symbol 的所有交易所盘口必须在同一分片内，否则跨所价差无法本地快速计算。
- 当前 2 核服务器可以继续跑小规模实盘，但扩到 5-10 个交易所、每所几百个交易对时，应先做本方案的热路径优化，再按机器配置启用 symbol shard。

## 当前实现状态

已在代码中落地的部分：

- 策略核心全量 evaluate 已按 symbol 分组，只在同一 symbol 的交易所盘口之间做组合比较。
- Direct websocket 的 `BookEvent` 已携带 dirty symbol，机会计算改为 per-symbol cache；dashboard tick 才构造展示 JSON rows。
- 盘口 top-of-book 价格或数量变差也会触发该 symbol 重算，避免机会缓存保留过期价差。
- `profit_history` loss guard 和 control command JSONL 已加短 TTL 内存缓存，避免每个盘口事件都读文件。
- `sharding.enabled/mode/shard_id/shard_count/explicit_symbols` 已接入 live runner；分片后同步过滤 strategy config 和 runtime config。
- live 多分片默认要求全局风控协调；未接入协调器时会阻止多 shard live 下单，除非配置显式关闭该安全要求并由运维保证账户/限额隔离。
- Binance、Bitget、Gate direct websocket 盘口解析已改为 typed fast path，只反序列化机会计算需要的字段。
- `performance.stale_sweep_ms` 已接入 direct WS trigger，stale sweep 只清理过期 opportunity cache，不做全量重算或 dashboard JSON 构造。
- dashboard snapshot 已支持 compact JSON 默认写盘，并支持 `dashboard.max_opportunity_rows/max_market_snapshot_rows/max_route_health_rows` 裁剪大数组。
- profit/control JSONL 已接入后台 poller 共享快照；执行热路径优先读内存，close 后强制刷新仍会同步反写共享快照。
- 已增加 synthetic replay 验证，比较 full grouped evaluate 与逐 symbol delta cache 聚合后的 opportunity id 集合一致。
- live runner 支持 `--shard-id/--shard-count` CLI 覆盖；`rustcta-industrial supervisor print-cross-arb-shard-specs` 可生成多 shard supervisor specs。
- control-api extra snapshot paths 已支持聚合多个 cross-arb shard dashboard snapshot，且同一 strategy kind 的不同 shard 不再互相覆盖。
- `performance.evaluator_workers` 已接入同进程 per-symbol evaluator worker pool；默认 1，配置大于 1 时并发计算 dashboard/full rebuild 或积压 dirty symbols 的 per-symbol cache。

仍属于后续增强的部分：

- 当前没有剩余必需优化项；后续性能增强可继续增加更细粒度的 benchmark 指标和运行期 worker 自适应调度。

## 优化前瓶颈

当前 runner 热路径大致是：

- 公共 WS reader 收到 book text。
- `parse_direct_ws_order_book_top()` 用 `serde_json::Value` 解析盘口。
- `spawn_direct_ws_event_collector()` 更新 `DirectWebsocketMarketDataState`，如果 top-of-book 改善则发送 `BookEvent`。
- 主循环每次 `BookEvent` 都调用 `DirectWebsocketMarketData::dashboard_data()`。
- `dashboard_data()` 遍历所有 `state.tops`，构造 `market_rows`、`fresh_tops`，然后调用：
  - `evaluate_dual_taker_open_opportunities_with_audit()`
  - `evaluate_live_slippage_capture_opportunities()`
  - `display_opportunity_rows()`
- `run_live_execution_cycle()` 再做风控、close/open 判断、dashboard hydration、open-decision audit、profit/control JSONL 读取。
- 只有 `emit_report()` 被 `DashboardTick` 限制住了，但机会扫描和 dashboard row 构造已经在每个 book event 上发生。

对应代码位置：

- `crates/rustcta-cross-arb-live-runner/src/lib.rs`
  - 主循环：`run_live_runner()` 的 `loop`。
  - 热路径聚合：`DirectWebsocketMarketData::dashboard_data()`。
  - WS 事件收集：`spawn_direct_ws_event_collector()`。
  - 盘口解析：`parse_direct_ws_order_book_top()`。
  - 执行循环：`run_live_execution_cycle()`。
- `strategies/cross-exchange-arbitrage/src/core.rs`
  - 双腿 taker 开仓评估：`evaluate_dual_taker_open_opportunities_with_audit()`。
  - slippage-capture 评估：`evaluate_slippage_capture_open_opportunities()`。

复杂度问题：

```text
S = symbol 数
E = 交易所数
books = S * E

当前每次 book event:
  dual_taker:      O((S * E)^2)
  slippage_capture: O((S * E)^2)

按 symbol 全量分组:
  O(S * E^2)

按更新 symbol 增量:
  O(E^2)
```

例子：

| 规模 | 当前单个 evaluate 每次事件约 pair 检查 | 按 symbol 增量约 pair 检查 |
| --- | ---: | ---: |
| 400 symbols * 3 venues | 1,440,000 | 9 |
| 1,000 symbols * 5 venues | 25,000,000 | 25 |
| 1,000 symbols * 10 venues | 100,000,000 | 100 |

实际代码还会做 JSON row 构造、字符串 clone、审计行生成和文件读取，所以 CPU 降幅不会等于数学复杂度降幅，但方向是确定的。
当前 runner 在部分模式下还会同时跑 dual-taker 和 slippage-capture 两套评估，因此热路径实际计算量可能接近表中当前列的两倍。

## 为什么不会减少机会发现

跨所开仓机会对某个 symbol 的依赖是局部的：

```text
opportunity(symbol X) =
  f(
    X 在所有交易所的最新 fresh top-of-book,
    fee_model,
    precision_registry,
    disabled routes,
    risk/control state,
    current time/stale rule
  )
```

当 `BINANCE:EDGE/USDT` 的盘口更新时，只有 `EDGE/USDT` 的跨所价差会改变。其他 symbol 的盘口输入没有变化，重新扫描它们不会发现新的机会，只会重复计算。

因此增量评估的等价条件是：

- 每个 book event 必须更新内存里的最新 top-of-book。
- 事件所属 symbol 必须立即进入该 symbol 的评估队列。
- 同一 symbol 的所有交易所盘口必须在同一个分片内。
- 机会缓存按 symbol 保存，更新 symbol 后替换该 symbol 的 best opportunity/audit。
- 全局开仓选择从 per-symbol 缓存中取当前最优，而不是重新扫描所有 books。
- `orderbook_stale_ms` 到期必须让该 symbol 的缓存失效，不能等下一次 book event。
- fee、precision、disabled symbol、disabled exchange、control state、risk state 变更时，必须重算受影响 symbol 或全量缓存。
- 启动、重连、订阅恢复后要做一次全量 grouped rebuild，确认缓存和当前 books 一致。

满足这些条件后，增量评估不会少看机会。它只是避免在一个 symbol 更新时重复计算其他没有变化的 symbol。

## 目标架构

### 1. Market State Store

建立按 symbol 分组的内存状态：

```rust
struct SymbolBookState {
    symbol: CanonicalSymbol,
    tops_by_exchange: SmallVec<[(ExchangeId, OrderBookTop); 16]>,
    last_updated_at: DateTime<Utc>,
    dirty: bool,
}

struct OpportunityCacheEntry {
    symbol: CanonicalSymbol,
    dual_taker: Option<DualTakerOpenOpportunity>,
    slippage_capture: Option<SlippageCaptureOpenOpportunity>,
    audits: Vec<OpenOpportunityAudit>,
    evaluated_at: DateTime<Utc>,
    valid_until: DateTime<Utc>,
}
```

`DirectWebsocketMarketDataState.tops` 可以保留作为兼容读模型，但热路径评估应使用 `books_by_symbol`，避免每次从全局 map 重新筛选。

### 2. SymbolDeltaEvaluator

新增 evaluator 任务：

```text
WS reader -> TopEvent(exchange, symbol, top)
          -> market state update
          -> enqueue SymbolDirty(symbol)
          -> evaluator worker recompute symbol only
          -> opportunity_cache[symbol] = new result
          -> execution selector reads cache
```

要求：

- 队列按 symbol 去重，防止一个高频 symbol 堵塞所有 symbol。
- 同一 symbol 的更新必须串行处理，避免旧结果覆盖新结果。
- worker 数量可配置。2 核服务器默认 1 个 evaluator worker；多核本机可以 2-4 个。
- evaluator 不做 dashboard JSON 构造，不读写文件。

### 3. Global Opportunity Selector

开仓决策从 `opportunity_cache` 里选择当前 best：

- dual-taker：按 `spread_pct` 或 `expected_net_profit_pct` 排序。
- slippage-capture：按 `spread_pct`，再按 maker depth 或当前策略已有规则排序。
- 风控、route cooldown、symbol cooldown、max bundles 等仍在执行前校验。

这一步可以每次有 symbol 更新时执行，也可以通过一个低延迟触发器执行。它不应该重算全量 opportunities。

### 4. Stale Sweep

新增定时 stale sweep：

```yaml
performance:
  stale_sweep_ms: 100
```

每 100-250ms 检查将要过期或已过期的 cache entry：

- 如果某 symbol 任一必需交易所 book stale，删除该 symbol 的 executable opportunity。
- 记录 route health 交给 dashboard publisher。
- 不做大 JSON 输出。

这是保证“不减少机会发现且不产生过期假机会”的关键。

### 5. Dashboard Publisher

Dashboard 从热路径移出：

```text
opportunity_cache + market state + execution state
  -> every dashboard.refresh_ms
  -> build rows
  -> write snapshot
```

改造点：

- `dashboard_data()` 不应由每个 `BookEvent` 触发完整构造。
- `market_snapshot_row()`、`display_opportunity_rows()`、`open_decision_audit_row()` 只在 dashboard tick 或审计 tick 调用。
- `serde_json::to_writer_pretty()` 改成可配置；实盘默认 compact JSON，排障时才 pretty。
- dashboard row 数量可配置，例如只输出 top 200 opportunities 和 top N route health。

### 6. JSONL 和控制命令后台化

当前 `run_live_execution_cycle()` 在热路径里会读：

- `profit_history_loss_guard_triggered()` -> `read_jsonl_rows(path, 500)`。
- `pending_manual_close_commands()` -> `read_jsonl_rows(path, 1000)`。

优化方案：

- Profit summary 在内存中维护，由本进程 append profit event 时同步更新。
- 启动时读一次历史尾部；之后后台 tailer 低频检查外部追加。
- Control command queue 后台 poll，例如 250-1000ms，读到命令后送 mpsc channel。
- 热路径只读内存中的 `LossGuardState` 和 `ControlCommandState`。

Trade ledger 已经是 bounded channel writer，方向正确；profit/control 也应使用同样思路。

## 推荐配置项

新增配置建议：

```yaml
performance:
  profile: server_2c
  evaluation_mode: symbol_delta
  evaluator_workers: 1
  max_dirty_symbols: 8192
  stale_sweep_ms: 100
  full_rebuild_interval_ms: 5000
  full_rebuild_on_reconnect: true
  opportunity_cache_max_age_ms: 3000
  parse_mode: typed_fast_path

dashboard:
  refresh_ms: 5000
  decouple_from_book_events: true
  pretty_json: false
  max_opportunity_rows: 200
  max_market_snapshot_rows: 2000
  max_route_health_rows: 500

persistence:
  trade_ledger_queue_capacity: 8192
  profit_summary_cache: true
  profit_history_tail_ms: 1000
  control_command_poll_ms: 500

sharding:
  enabled: false
  mode: hash_symbol
  shard_id: 0
  shard_count: 1
  explicit_symbols: []
  require_global_risk_coordinator_when_live: true
```

现有 `dashboard.refresh_ms`、`persistence.trade_ledger_queue_capacity` 可复用；其余是新增项。

## 分片策略

### 正确分片方式

必须按 symbol 分片：

```text
shard = hash(normalized_symbol) % shard_count
```

每个 shard 订阅：

```text
该 shard 的 symbols * 所有 enabled_exchanges
```

不要按交易所分片。跨所套利需要同一个 symbol 的所有交易所盘口在同一个计算上下文里，否则需要跨进程同步盘口，延迟和复杂度都会上升。

### 实盘多分片的风控问题

如果多个 shard 使用同一组交易所账户实盘下单，当前每个进程本地的 `max_open_bundles`、`max_total_notional_usdt`、`max_positions_per_exchange` 不能代表全局限制。

上线多 shard live 前必须满足其一：

- 使用全局 risk coordinator/allocator，开仓前原子申请额度。
- 每个 shard 使用独立账户或子账户。
- 配置层手动拆分额度，并且总和不超过账户级风控上限。

read-only、analysis、dry-run 分片不受这个限制。

### 设备配置建议

| 设备 | 建议 profile | shard_count | evaluator_workers/shard | dashboard |
| --- | --- | ---: | ---: | --- |
| 2 vCPU 服务器 | `server_2c` | 1-2 | 1 | 5-10s |
| 4 dedicated vCPU | `server_4c` | 2-4 | 1 | 5s |
| 8 dedicated vCPU | `server_8c` | 4-8 | 1-2 | 3-5s |
| 本机 12-16 核 | `workstation` | 6-12 | 1-2 | 1-3s |

2 核服务器优先使用单进程 + symbol_delta。只有当单进程优化后 CPU 仍超过 70% 且全局风控已处理，才开 2 shard。

## 服务器选型

低延迟交易不要只看 CPU 单价。选型顺序应是：

1. 到目标交易所 websocket endpoint 的 RTT、抖动、丢包。
2. CPU 是否为 dedicated/high-frequency，不要使用低价 burstable/shared vCPU 承载实盘。
3. 单核性能和持续满载能力。
4. 网络稳定性和出站带宽。
5. 月成本。

建议先租 2-3 家按小时计费机器做同一套 replay/WS RTT 测试，再决定长期机器。

当前方向：

- 性价比优先：Hetzner dedicated vCPU 或 dedicated root server。若新加坡到交易所延迟可接受，通常比 hyperscaler 便宜。
- 亚太快速试错：Vultr High Frequency / Dedicated CPU、Hetzner Singapore、AWS ap-southeast/ap-northeast 对照测试。
- 独享硬件：OVHcloud bare metal 或 Hetzner dedicated root server，适合 8 核以上长期跑多 shard。
- 低延迟/合规/运维优先：AWS C7i/C7i-flex，价格高，但区域多、网络和监控生态成熟。

不要在没有 RTT 实测的情况下只因为便宜选择欧洲机房。如果交易所 WS endpoint 对亚太更近，欧洲机器的网络延迟可能抵消 CPU 优势。

## 实施阶段

### Phase 0: 基准测试

新增 synthetic replay benchmark：

- 输入：N symbols * M exchanges 的 book top 流。
- 输出：decision latency p50/p95/p99、CPU、每秒处理 book events。
- 同时跑 old full-scan 和 new symbol-delta。

验收：

- 同一 replay 输入下，新旧 opportunity set 完全一致。
- 开仓 best opportunity 一致。
- stale 后 opportunity 删除一致。

### Phase 1: 按 symbol 分组全量扫描

先把 `evaluate_*` 从全 books 双循环改成按 symbol group：

```text
books -> BTreeMap<symbol, Vec<OrderBookTop>>
for each symbol group:
  evaluate E * E
```

这是低风险中间态，语义最接近当前全量扫描，能立刻把复杂度从 `O((S*E)^2)` 降到 `O(S*E^2)`。

### Phase 2: 增量 symbol cache

新增 `OpportunityCache` 和 `SymbolDirtyQueue`：

- book event 只重算该 symbol。
- dashboard tick 从 cache 读结果。
- execution selector 从 cache 读 best opportunity。

### Phase 3: Dashboard 和 JSONL 出热路径

- dashboard publisher 独立 tick。
- profit summary cache 替代每 event 读文件。
- control queue 后台 poll。
- open-decision audit 只记录接近阈值或状态变化的 symbol，且走异步 ledger。

### Phase 4: Sharding

- 配置支持 `sharding.enabled/mode/shard_id/shard_count`。
- runner 启动时只订阅本 shard symbols。
- supervisor 支持生成多个 shard process spec。
- dashboard/control-api 支持聚合多个 shard snapshot。
- live 模式启用全局风控协调。

### Phase 5: Parser 优化

把 `serde_json::Value` 通用解析替换为交易所 typed fast path：

- Binance depth5 payload typed struct。
- Bitget books5 typed struct。
- Gate futures order_book typed struct。

注意：parser 优化不能改变字段语义。它只减少分配和动态查找，不允许跳过 book event。

## 验收标准

功能正确性：

- Replay 测试中，新旧评估在同一时间点输出相同 symbol 的 opportunity。
- 单 symbol 单交易所 top 改变时，只该 symbol cache 变化。
- book stale 时，对应 symbol opportunity 在 `orderbook_stale_ms + stale_sweep_ms` 内失效。
- reconnect 后 full rebuild 恢复正确 opportunity。
- disabled symbol/exchange 变更后相关 cache 立即失效。
- 多 shard 下同一 symbol 只出现在一个 shard。

性能：

- 2 vCPU 服务器，3 exchanges * 400 symbols，CPU 长时间低于 60-70%。
- book event 到 decision selector p95 小于 5ms，p99 小于 20ms。
- dashboard 写文件不影响 decision p99。
- profit/control JSONL 文件增长到 100MB 时，book event p99 不退化。

实盘安全：

- `--enable-live-trading` 和 `execution.trading_enabled` 双开关保持不变。
- 多 shard live 必须通过全局风控协调或额度拆分检查。
- 任何后台 writer 队列满时只降级审计，不阻塞下单热路径；关键交易事件仍需可观测告警。

## 不建议的优化

- 不建议把 `LIVE_WS_MIN_BOOK_UPDATE_MS` 设大来减少计算。它会跳过盘口变化，可能错过短暂价差。
- 不建议按交易所分片。跨所机会需要同 symbol 全交易所视图。
- 不建议让 dashboard snapshot 成为交易输入。它只能是输出。
- 不建议用更大服务器掩盖 `O((S*E)^2)` 热路径。扩到 5-10 个交易所后，算法复杂度会比硬件更快变差。

## 推荐落地顺序

1. 先做 Phase 0 和 Phase 1，确认 grouped full-scan 与当前输出一致。
2. 再做 Phase 2，把交易热路径改成 symbol_delta。
3. 同时做 Phase 3，把 dashboard/JSONL 移出 book event。
4. 服务器暂时维持 2 核，只把 dashboard refresh 调到 5-10s，并保持 release + `RUST_LOG=warn`。
5. 准备扩到 5 个以上交易所前做 Phase 4 分片和全局风控。
6. 最后做 parser typed fast path，进一步压低 CPU 和分配。

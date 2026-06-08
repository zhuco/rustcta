# 高频永续合约跨所双 Taker 套利优化计划

状态日期：2026-06-07

开发收口状态：第一批开发任务 1-5 已实现并完成聚焦验证；版本收口到
`0.3.11`。已验证项包括双 taker only live admission、最快 L1/Depth
public book profile、WebSocket book update symbol 级评估、双向 dual taker
opportunity 与 L1/Depth 数量校验、开仓 reservation、one-sided/unknown risk
lock、REST readback 和 repair 释放路径。收口验证命令见“九、验收标准”。

本文档面向当前 `cross_exchange_arbitrage` 永续合约跨交易所套利策略，目标是把前期实盘验证收敛为 **双 taker only**：两腿都使用带价格保护的 IOC/FOK 类限价单并发提交，不使用 maker 挂单，不等待 maker 成交后再补 hedge。

核心目标：

- 利用 WebSocket 推送行情快速触发策略评估。
- 公共盘口 WebSocket 订阅使用交易所支持的最快可用推送频率。
- 前期只验证双 taker 成交链路，减少 maker 未成交、排队、post-only reject 带来的不确定性。
- 尽可能降低单腿风险；无法完全消除时，把单腿暴露窗口、暴露数量和恢复动作做成硬约束。
- 先做策略级优化，不做全项目大改。

## 一、优化范围判断

### 结论

第一阶段应以 **策略级优化为主**，只对共享执行/风控接口做必要的小范围增强；不建议一开始做全项目优化。

原因：

1. 当前项目已经有公共 WebSocket 行情、私有永续 REST/WS、执行路由、账本、风控、仓位管理等基础能力。双 taker 前期验证不需要重构全项目。
2. 当前瓶颈主要在 `cross_exchange_arbitrage` 策略热路径：行情虽然来自 WebSocket 推送，但 live 主循环仍存在 ticker 扫描/批处理形态；应先让该策略按 symbol 的行情更新进行增量评估。
3. 全项目优化涉及现货策略、控制面、回放、通用行情抽象、执行 crate 拆分等，会扩大风险和协作成本，不利于快速验证永续双 taker。
4. 单腿风险控制主要发生在策略执行前检查、双腿并发提交、私有流确认、失败恢复和仓位审计这几个局部链路，优先改这些点收益最大。

### 第一阶段允许范围

- `retired strategy tree/cross_exchange_arbitrage/**`
- `retired root bin directory/cross_arb_live.rs`
- `src/execution/**` 中与 bundle 并发提交、one-sided exposure、订单确认相关的小范围改动
- `src/market/cache.rs` 或 WebSocket 更新接入点的轻量扩展
- `retired exchange tree/market_adapters/**` 中公共永续盘口 WebSocket 订阅 channel / speed profile 的小范围改动
- `config/cross_exchange_arbitrage_*.yml` 双 taker 配置样例
- focused tests

### 第一阶段不做

- 不重构全部市场数据系统。
- 不迁移整个项目到新的 crate 布局。
- 不改现货套利策略。
- 不把控制面、数据库、ClickHouse、dashboard 引入交易热路径。
- 不绕过 live preflight、kill switch、私有流 ready、仓位审计、订单 reconciliation。

## 二、当前代码判断

当前代码里行情来源已经是 WebSocket 推送，`ws_market_execution_loop` 会接收 `PublicWsUpdate::OrderBook` 并更新 `MarketStateCache`。

需要区分两件事：

- **行情输入**：已经是 WebSocket 自动推送。
- **策略决策触发**：当前 live 路径仍按 ticker 周期从 cache 拉取快照再扫描机会。

因此优化重点不是“把 REST 扫描改成 WebSocket”，而是把 `cross_exchange_arbitrage` 的决策入口从周期轮询推进到 **WebSocket book event 驱动的 symbol 级增量评估**。

另外，公共盘口推送频次需要单独审计。当前代码中部分适配器已经选择了较快盘口 channel，例如 Binance USD-M 使用 `depth5@100ms`，Bitget 使用 `books5`，OKX 使用 `books5`，Bybit 使用 `orderbook.1`。但“最快”不能只看现有 channel 名称，必须按交易所官方能力确认：

- 该 channel 是否适用于 USDT 永续，而不是现货。
- 是否有更快的 top-of-book / L1 / L5 / L2 channel。
- 更快 channel 是否包含足够深度，能覆盖双 taker 目标 notional。
- 是否有 sequence / checksum / snapshot resync 能力。
- 单连接 symbol 数量、订阅速率、消息速率是否会触发限流。

## 三、双 Taker Only 的阶段目标

前期策略只允许：

```text
低价交易所：taker buy 开 long
高价交易所：taker sell 开 short
```

两腿必须：

- 同一 bundle。
- 同一 canonical symbol。
- 同一 normalized base quantity。
- 都使用显式限价保护。
- 都使用 IOC；若两个交易所均支持 FOK 且适配器能力可确认，优先使用 FOK。
- 并发提交。
- 私有流和 REST readback 必须能确认成交结果。

前期策略禁止：

- maker-taker 开仓。
- maker 挂单等待成交。
- 单腿先下、确认后再下另一腿。
- 裸 market order。
- 私有流未 ready 时开仓。
- 订单状态未知时继续开新仓。

## 四、单腿风险控制原则

双 taker 不能数学上完全消除单腿风险，因为跨交易所是两个独立请求，仍可能出现：

- 一腿 filled，另一腿 rejected。
- 一腿 filled，另一腿 partial filled。
- 一腿 ack 超时但实际成交。
- 一腿私有流延迟，REST readback 暂时 unknown。
- 两边成交数量因精度、合约面值或最小下单量不一致产生残差。

所以目标不是宣称“不会有单腿”，而是：

1. **开仓前尽量避免**：只在盘口、深度、私有流、余额、仓位、交易所健康全部满足时提交。
2. **提交时尽量同步**：两腿并发提交，并使用严格价格保护和统一数量。
3. **成交后立即判定**：优先私有流，短超时后 REST readback。
4. **异常时立即降级**：发现 one-sided exposure 或 unknown 状态后，策略进入 close-only，禁止新开仓。
5. **恢复动作自动化**：按预设 repair plan 补齐 hedge 或 reduce-only 平掉单腿。

## 五、必须增加或确认的安全门

### 开仓前

每次双 taker 开仓必须通过：

- 两个交易所 public book 均 fresh，且没有 sequence gap / resync 状态。
- 两个交易所 private WS 均 ready，且最近私有事件未超过 `max_private_stream_delay_ms`。
- 两个交易所 private REST 可用，最近失败率未超过阈值。
- 两腿 instrument metadata 完整，tick size、step size、contract size、min notional 都可校验。
- 两腿 normalized base quantity 完全一致，或残差低于硬阈值。
- 两边 taker VWAP 深度足够覆盖目标数量。
- 预估净收益覆盖：双边 taker fee、滑点、funding、延迟衰减、安全 buffer。
- 当前 symbol 没有 active bundle、pending repair、orphan exposure。
- 当前账户没有同 symbol 外部未托管仓位，除非显式允许并纳入风控。

### 提交时

- 必须走 bundle 并发提交，不允许顺序下单。
- 必须带 `max_slippage_pct`。
- 必须带 IOC/FOK limit price。
- client order id 必须包含 bundle id、leg id、run id，便于私有流归因。
- 若任一腿提交失败，立即生成 one-sided/unknown 风险事件并进入恢复流程。

### 成交后

- 优先等待私有 WS 的 order/fill 更新。
- 超过短阈值未确认时立即 REST readback。
- 任一腿 partial、reject、unknown、timeout 都不能视为正常开仓。
- one-sided exposure 存在时禁止新开仓。
- repair 结束前保持 close-only 或 symbol-level close-only。

## 六、策略热路径优化设计

### 1. WebSocket 事件驱动评估

目标：每次某交易所某 symbol 的 book 更新后，只重算该 symbol 相关交易所组合。

建议接口：

```text
on_public_book_update(exchange, canonical_symbol, book, received_at)
  -> update MarketStateCache
  -> validate book health
  -> build snapshots for this symbol only
  -> evaluate directed venue pairs
  -> emit best executable dual-taker candidate
```

要求：

- 不在每个事件上 clone 全 universe。
- 不扫描无关 symbol。
- 不在热路径写文件、查数据库、刷新 dashboard。
- ticker 保留给 funding refresh、健康检查、过期任务、close 检查和监控，不作为开仓机会的唯一触发源。

### 2. 最快盘口订阅策略

目标：公共 WebSocket 盘口订阅默认使用每个交易所支持的最快可用推送频率，同时保证数据足够可靠。

前期策略默认采用两层盘口：

```text
trigger_profile: fastest_l1
validation_profile: fastest_depth
```

含义：

- `fastest_l1` 用最快的一档 best bid/ask 推送触发机会，允许使用 `books1`、`bookTicker`、`bbo-tbt`、`orderbook.1` 这类 channel。
- `fastest_depth` 用最快的多档盘口校验实际可成交数量、VWAP 和滑点。
- 如果为了极致速度直接用 L1 下单，则必须用两边最优档名义金额取较小值，再乘 `top_of_book_capacity_ratio`；默认 `0.8`，该值必须覆盖本次 `target_notional_usdt`，否则不允许直接开仓。

建议新增配置：

```yaml
market:
  public_book_speed: fastest
  public_book_trigger_profile: fastest_l1
  public_book_validation_profile: fastest_depth
  public_book_depth: 5
  allow_top_of_book_only: true
  top_of_book_capacity_ratio: 0.8
  max_symbols_per_ws_connection: 50
```

语义：

- `fastest`：选择适配器声明的最快可交易盘口 channel。
- `balanced`：选择消息量和稳定性更平衡的 channel。
- `conservative`：选择当前稳定 channel，适合排障或交易所限流时降级。

适配器应暴露每个交易所的 public book profile：

```text
exchange
channel
depth
expected_push_interval_ms
supports_sequence
supports_checksum
requires_rest_snapshot
max_symbols_per_connection
message_rate_note
```

策略选择最快 channel 时必须满足：

- 深度覆盖 `target_notional_usdt` 的 taker VWAP 计算。
- 双 taker 直接开仓只看一档容量：`min(long best ask price * qty, short best bid price * qty) * top_of_book_capacity_ratio >= target_notional_usdt`。
- 若只使用 top-of-book，需要强制更小 notional、更高 safety buffer，并保留 `top_of_book_capacity_ratio` 配置，前期默认 0.8。
- 订阅失败、消息过载、sequence gap、checksum fail 时自动降级到 close-only 或 conservative profile。
- 每个交易所的订阅发送间隔和每连接 symbol 数量不能超过官方限制。
- 记录实际 `book_recv_interval_ms`、gap 次数、重连次数，用真实数据判断“最快”是否稳定。

### 3. 官方默认 Public Book Profile

以下默认值按 2026-06-07 查询到的官方文档整理。后续实现策略时优先采用本表，不需要重新查 channel 名称；但上线前仍要用 canary 记录真实 `book_recv_interval_ms`、丢包、重连、sequence gap 和本机处理延迟。

| 交易所 | 默认 profile | Channel / stream | 深度 | 官方推送频率 | 备注 |
| --- | --- | --- | --- | --- | --- |
| Binance USD-M | `fastest_l1` | `<symbol>@bookTicker` | 1 档 | Real-time | 官方 individual book ticker 会推送 best bid/ask price/qty 的任意更新；默认触发源。 |
| Binance USD-M | `fastest_depth` | `<symbol>@depth5@100ms` | 5 档 | 100ms | 官方 partial book depth 支持 5/10/20 档，速度为 250/500/100ms；作为默认深度校验。 |
| Binance USD-M | `conservative_depth` | `<symbol>@depth10@100ms` 或 `<symbol>@depth20@100ms` | 10/20 档 | 100ms | 小仓 5 档通常够用；如果 5 档无法覆盖 VWAP，再升到 10/20。 |
| OKX SWAP | `fastest_l1` | `bbo-tbt` | 1 档 | 10ms | 默认触发源。 |
| OKX SWAP | `fastest_public_depth` | `books5` | 5 档 | 100ms | 普通 public 可用，作为默认深度校验。 |
| OKX SWAP | `fastest_vip_depth` | `books50-l2-tbt` | 50 档 | 10ms | 需要登录且官方限制 VIP4+；可作为高级 profile，不作为普通默认。 |
| OKX SWAP | `deep_vip_depth` | `books-l2-tbt` | 400 档 | 10ms | 需要登录且官方限制 VIP4+；消息量更大。 |
| Bitget USDT-FUTURES | `fastest_l1` | `books1` | 1 档 | 10ms | 官方 classic contract 文档说明 `books1` 默认 10ms；默认触发源。 |
| Bitget USDT-FUTURES | `fastest_depth` | `books5` | 5 档 | 150ms | 当前代码使用 classic contract / v2 mix 风格订阅；官方 classic contract 文档说明 `books/books5/books15` 默认 150ms，作为默认深度校验。 |
| Bitget USDT-FUTURES | `deep_depth` | `books15` | 15 档 | 150ms | 5 档深度不足时使用；classic contract 文档未提供 50 档。 |
| Bybit Linear | `fastest_l1` | `orderbook.1.<symbol>` | 1 档 | 10ms | 默认触发源。 |
| Bybit Linear | `fastest_depth` | `orderbook.50.<symbol>` | 50 档 | 20ms | 官方 linear/inverse 没有 5 档 channel；作为默认深度校验。 |
| Bybit Linear | `deep_depth` | `orderbook.200.<symbol>` | 200 档 | 100ms | 大 notional 或 50 档不足时使用。 |
| Gate USDT Futures | `fastest_depth` | `futures.order_book_update` payload `[contract, "20ms", "20"]` | 20 档更新 | 20ms | 官方 futures update 支持 20ms/100ms；20ms 只允许 20 档。需要 REST snapshot + incremental merge。 |
| Gate USDT Futures | `conservative_depth` | `futures.order_book_update` payload `[contract, "100ms", "50"]` | 50 档更新 | 100ms | 消息压力更低，适合降级。 |
| MEXC USDT Contract | `conservative_full_depth` | `sub.depth.full` param `{symbol, limit: 5}` | 5 档 | 官方页面未标明固定频率 | 官方 contract 文档确认 full depth limit 可为 5/10/20，且 incremental `sub.depth` 默认 compress=true；未找到可靠官方固定推送间隔，暂不作为高速主交易腿。 |
| MEXC USDT Contract | `incremental_depth_candidate` | `sub.depth` param `{symbol, compress: true}` | 增量 | 官方页面未标明固定频率 | 需要本地 orderbook merge 和 canary 实测后再启用。 |
| HTX USDT Swap | `fastest_depth` | `market.<contract>.depth.size_20.high_freq` | 20 档增量 | orderbook event 每 30ms 检查一次，有变化才推送 | 官方建议使用 high_freq 增量并维护本地 orderbook。 |
| HTX USDT Swap | `conservative_depth` | `market.<contract>.depth.step6` | 20 档快照 | 官方提示大深度 100ms 数据量可能导致断连 | 当前代码使用 step0；高速 profile 应改为 high_freq 或 step6。 |

默认实现建议：

```yaml
market:
  public_book_speed: fastest
  public_book_depth: 5
  allow_top_of_book_only: true
  exchange_book_profiles:
    binance:
      trigger_profile: fastest_l1
      trigger_channel: bookTicker
      trigger_depth: 1
      trigger_expected_push_interval_ms: null
      validation_profile: fastest_depth
      validation_channel: depth5@100ms
      validation_depth: 5
      validation_expected_push_interval_ms: 100
      requires_local_merge: false
      supports_sequence: true
    okx:
      trigger_profile: fastest_l1
      trigger_channel: bbo-tbt
      trigger_depth: 1
      trigger_expected_push_interval_ms: 10
      validation_profile: fastest_public_depth
      validation_channel: books5
      validation_depth: 5
      validation_expected_push_interval_ms: 100
      requires_local_merge: false
      supports_sequence: true
      vip_upgrade_channel: books50-l2-tbt
    bitget:
      trigger_profile: fastest_l1
      trigger_channel: books1
      trigger_depth: 1
      trigger_expected_push_interval_ms: 10
      validation_profile: fastest_depth
      validation_channel: books5
      validation_depth: 5
      validation_expected_push_interval_ms: 150
      requires_local_merge: false
      supports_sequence: true
    bybit:
      trigger_profile: fastest_l1
      trigger_channel: orderbook.1
      trigger_depth: 1
      trigger_expected_push_interval_ms: 10
      validation_profile: fastest_depth
      validation_channel: orderbook.50
      validation_depth: 50
      validation_expected_push_interval_ms: 20
      requires_local_merge: true
      supports_sequence: true
    gate:
      profile: fastest_depth
      channel: futures.order_book_update
      depth: 20
      expected_push_interval_ms: 20
      requires_local_merge: true
      supports_sequence: true
    mexc:
      profile: conservative_full_depth
      channel: sub.depth.full
      depth: 5
      expected_push_interval_ms: null
      requires_local_merge: false
      supports_sequence: true
      use_as_primary_fast_leg: false
    htx:
      profile: fastest_depth
      channel: depth.size_20.high_freq
      depth: 20
      expected_push_interval_ms: 30
      requires_local_merge: true
      supports_sequence: true
```

官方文档来源：

- Binance USD-M partial book depth：`https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams`
- Binance USD-M diff book depth：`https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams`
- Binance USD-M book ticker：`https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams`
- OKX order book channel：`https://tr.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-channel`
- Bitget classic contract depth channel：`https://www.bitget.com/api-doc/classic/contract/websocket/public/Order-Book-Channel`
- Bitget classic best practices：`https://www.bitget.com/api-doc/classic/best-practices`
- Bybit V5 orderbook：`https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook`
- Gate futures WebSocket：`https://www.gate.com/en/docs/developers/futures/`
- MEXC contract API：`https://mexcdevelop.github.io/apidocs/contract_v1_en/`
- HTX USDT Swap API：`https://huobiapi.github.io/docs/usdt_swap/v1/en/`

注意：更快的 channel 不一定更适合双 taker。若最快 channel 只有 1 档盘口，而目标 notional 需要吃多档，必须使用足够深度的 channel 或把下单 notional 降到 1 档可覆盖范围内。

### 4. 双向价差评估

每个 symbol 对每组交易所都评估：

```text
A long / B short
B long / A short
```

双 taker 价格：

```text
long leg:  buy from ask side
short leg: sell into bid side
```

排序指标建议：

```text
net_edge_after_fee_funding_slippage
- latency_decay_penalty
- one_sided_risk_penalty
- exchange_health_penalty
```

### 5. 双 Taker 执行请求

配置上应强制：

```yaml
execution:
  open_execution_style: dual_taker
  close_execution_style: dual_taker
  taker_ioc_slippage_limit_pct: 0.001
  allow_maker_taker_live: false
```

如果当前配置结构没有 `allow_maker_taker_live`，可以先通过 validate 逻辑或 live runner admission 限制：前期 live run 只接受 `open_execution_style=dual_taker`。

### 6. 成交状态机

建议把双 taker bundle 的结果状态收敛为：

```text
Submitted
BothAcked
BothFilled
PartialBalanced
OneSidedExposure
UnknownRequiresReconcile
Repairing
Repaired
Closed
FailedCloseOnly
```

关键约束：

- `BothFilled` 才能进入正常持仓。
- `PartialBalanced` 只有两腿成交数量在容忍阈值内才允许保留。
- `OneSidedExposure` 和 `UnknownRequiresReconcile` 必须触发 close-only。

## 七、配置建议

前期建议只跑高流动性、低延迟、私有接口稳定的交易所组合，例如 Binance + Bitget / OKX / Bybit 中实测最稳的一组。

建议初始配置：

```yaml
market:
  stale_quote_ms: 500
  min_common_exchanges: 2
  public_book_speed: fastest
  public_book_trigger_profile: fastest_l1
  public_book_validation_profile: fastest_depth
  public_book_depth: 5
  allow_top_of_book_only: true
  top_of_book_capacity_ratio: 0.8

thresholds:
  min_display_raw_spread: 0.0002
  min_open_maker_taker_net_edge: 0.0005
  max_open_raw_spread: 0.03

execution:
  dry_run: true
  open_execution_style: dual_taker
  close_execution_style: dual_taker
  taker_ioc_slippage_limit_pct: 0.001
  max_hedge_retries: 1

risk:
  max_book_age_ms: 500
  max_taker_slippage_pct: 0.001
  taker_slippage_buffer: 0.0002
  safety_buffer: 0.0002
  max_open_bundles: 1
  max_private_stream_delay_ms: 1000

sizing:
  min_notional_usdt: 5.5
  target_notional_usdt: 5.5
  max_notional_usdt: 5.5
  max_symbol_notional_usdt: 5.5
```

说明：

- 第一阶段 `max_open_bundles` 建议为 1，先验证链路，不追求并发收益。
- `dry_run=true` 跑稳定后，再进入 live-small。
- 双 taker 机会成本先只扣两边交易所的开仓 taker 手续费和平仓 taker 手续费；平台币抵扣、返佣和后返不参与开仓准入，等真实成交流水回填后作为额外收益或成本修正。
- 前期小额验证按 5.5U 每次对冲金额执行；一档容量按 80% 折算后必须大于等于 5.5U，才允许发双腿 taker。
- 真实阈值不要只拍脑袋调低，要用记录数据计算：成交前 edge、成交后 edge、费用、滑点、延迟衰减。

## 八、并行 AI 开发任务

第一批任务设计为 5 个 AI 可同时开工。你可以直接分派：

```text
请完成 docs/high_frequency_perpetual_dual_taker_arbitrage_plan_zh.md 的开发任务 1
请完成 docs/high_frequency_perpetual_dual_taker_arbitrage_plan_zh.md 的开发任务 2
...
请完成 docs/high_frequency_perpetual_dual_taker_arbitrage_plan_zh.md 的开发任务 5
```

### 全局并行规则

每个 AI 开始前必须：

1. 运行 `git status --short`，识别无关脏文件。
2. 阅读本文档，以及 `docs/交易所网关/总览/exchange_abstraction.md`、`docs/交易所网关/总览/exchange_api_completion_matrix.md`、`docs/交易所网关/通用机制/order_reconciliation.md`、`docs/live_preflight.md`。
3. 只修改自己任务允许范围。
4. 不回滚、不覆盖其他 AI 或用户已有改动。
5. 不绕过 dry-run、live-small admission、preflight、kill switch、private WS ready、reconciliation。
6. 不把裸 market order 作为默认实盘订单；双 taker 必须使用 IOC/FOK limit 和 max slippage。
7. 不让热路径依赖数据库、dashboard HTTP、ClickHouse、文件 IO 或跨区域 RPC。
8. 不提交真实 API key、secret、passphrase 或账户敏感信息。

### 任务协作矩阵

| 任务 | 主题 | 主要范围 | 必须协调 |
| --- | --- | --- | --- |
| 1 | 双 Taker Only Admission 与配置门 | `retired strategy tree/cross_exchange_arbitrage/config.rs`, `retired root bin directory/cross_arb_live.rs`, `config/` | 2, 3, 5 |
| 2 | 最快 L1/Depth Public WS Profile | `retired exchange tree/market_adapters/**`, `src/market/**`, `config/` | 3, 4 |
| 3 | WebSocket 事件驱动 symbol 级评估 | `retired root bin directory/cross_arb_live.rs`, `retired strategy tree/cross_exchange_arbitrage/tasks.rs`, `src/market/cache.rs` | 1, 2, 4, 5 |
| 4 | 双向双 Taker Opportunity 与 L1/Depth 校验 | `retired strategy tree/cross_exchange_arbitrage/opportunity.rs`, `simulation.rs`, `sizing.rs` | 2, 3, 5 |
| 5 | 原子风控 Reservation 与单腿恢复 | `retired strategy tree/cross_exchange_arbitrage/risk.rs`, `execution.rs`, `position.rs`, `src/execution/**` | 1, 3, 4 |

### 共享接口约定

任务 2 和任务 3 共享 public book event 语义：

```text
exchange
canonical_symbol
exchange_symbol
profile_kind: fastest_l1 | fastest_depth | conservative_depth
depth
best_bid_price
best_bid_qty
best_ask_price
best_ask_qty
recv_ts
exchange_ts
sequence
source_route
quality
```

任务 4 和任务 5 共享 dual taker candidate 语义：

```text
bundle_id
canonical_symbol
long_exchange
short_exchange
long_exchange_symbol
short_exchange_symbol
long_limit_price
short_limit_price
normalized_base_qty
target_notional_usdt
expected_net_edge
max_slippage_pct
trigger_book_profile
validation_book_profile
top_of_book_capacity_usdt
generated_at
```

任务 5 的风险锁语义：

```text
reservation_id
bundle_id
canonical_symbol
exchanges
notional_usdt
status: reserved | released | risk_locked | repaired
reason
created_at
updated_at
```

### 开发任务 1：双 Taker Only Admission 与配置门

目标：前期 live-small 只允许双 taker 开仓，禁止 maker-taker 混入。

允许范围：

- `retired strategy tree/cross_exchange_arbitrage/config.rs`
- `retired root bin directory/cross_arb_live.rs`
- `config/cross_exchange_arbitrage_*.yml`
- focused tests

工作：

- live admission 校验 `execution.open_execution_style=dual_taker`。
- live admission 校验 `execution.close_execution_style=dual_taker`。
- dry-run 可继续兼容旧配置，但 `--execute` 或 live-small 非 dry-run 必须拒绝 maker-taker。
- 新增双 taker only 配置样例，默认 `public_book_trigger_profile=fastest_l1`、`public_book_validation_profile=fastest_depth`。
- 配置 validation 明确：双 taker live 必须有正数 `taker_ioc_slippage_limit_pct`。
- 单测覆盖：非 dual_taker live execute 被拒绝，dual_taker live admission 通过。

不得做：

- 不改交易所签名和下单语义。
- 不绕过现有 live confirmation flag。
- 不把 maker-taker 删除；只是在前期 live admission 中禁用。

验收建议：

```bash
cargo test cross_exchange_arbitrage_config --all-features
cargo test cross_arb_live --all-features
cargo fmt --check
```

分派 Prompt：

```text
你是 AI-1。完成 docs/high_frequency_perpetual_dual_taker_arbitrage_plan_zh.md 的开发任务 1：双 Taker Only Admission 与配置门。先读取全局并行规则和共享接口约定。只修改任务 1 允许范围。目标是 live-small 实盘前期只允许 open/close 都是 dual_taker，非 dual_taker 在 --execute 或非 dry-run live admission 中必须拒绝。新增双 taker only 配置样例和 focused tests。不要改交易所签名、真实下单语义或其他任务范围。
```

### 开发任务 2：最快 L1/Depth Public WS Profile

目标：每个永续交易所适配器都能按配置选择最快 L1 触发盘口和多档校验盘口。

允许范围：

- `src/market/adapter.rs`
- `src/market/event.rs`
- `src/market/cache.rs`
- `retired exchange tree/market_adapters/**`
- `config/cross_exchange_arbitrage_*.yml`
- focused tests

工作：

- 增加 public book profile 类型，支持 `fastest_l1`、`fastest_depth`、`conservative_depth`。
- Binance 默认 trigger=`bookTicker`，validation=`depth5@100ms`。
- OKX 默认 trigger=`bbo-tbt`，validation=`books5`。
- Bitget 默认 trigger=`books1`，validation=`books5`。
- Bybit 默认 trigger=`orderbook.1`，validation=`orderbook.50`。
- Gate / MEXC / HTX 按本文档默认表实现 profile 声明；可先不作为 primary fast leg。
- WebSocket subscription 构建不能再只写死单一 channel。
- 记录或暴露实际 source profile、route、sequence、depth。
- 单测覆盖各交易所 fastest profile 会选择预期 channel。

不得做：

- 不为了最快选择现货 channel。
- 不降低数据质量门；无法 parse 或无法校验的 book 不可作为 executable。
- 不把 VIP-only channel 作为普通默认。

验收建议：

```bash
cargo test market_adapters --all-features
cargo test public_book_profile --all-features
cargo fmt --check
```

分派 Prompt：

```text
你是 AI-2。完成 docs/high_frequency_perpetual_dual_taker_arbitrage_plan_zh.md 的开发任务 2：最快 L1/Depth Public WS Profile。先读取官方默认 Public Book Profile 表、全局并行规则和共享接口约定。只修改任务 2 允许范围。目标是按 fastest_l1/fastest_depth/conservative_depth profile 构建永续 public WS 订阅，默认 Binance bookTicker+depth5@100ms、OKX bbo-tbt+books5、Bitget books1+books5、Bybit orderbook.1+orderbook.50。添加 focused tests。不要改策略执行、风控 reservation 或 live admission。
```

### 开发任务 3：WebSocket 事件驱动 Symbol 级评估

目标：public WebSocket book update 直接触发相关 symbol 的增量评估，ticker 不再是开仓唯一触发源。

允许范围：

- `retired root bin directory/cross_arb_live.rs`
- `retired strategy tree/cross_exchange_arbitrage/tasks.rs`
- `retired strategy tree/cross_exchange_arbitrage/runtime.rs`
- `src/market/cache.rs`
- focused tests

工作：

- 在 `PublicWsUpdate::OrderBook` 更新 cache 后触发该 symbol 的 evaluation。
- 每次只构建当前 symbol 的 snapshots，不扫描全 universe。
- L1 trigger 到达时可触发候选计算，但开仓前必须拿到 validation depth 或确认 L1 数量足够。
- ticker 保留用于 funding refresh、close candidates、repair tasks、periodic audit 和监控。
- 加入轻量 debounce / in-flight guard，避免同 symbol 高频事件重入提交。
- 单测覆盖：一个 book event 只触发对应 symbol；stale/gap book 不触发 executable open。

不得做：

- 不在 public WS reader 任务里直接下单。
- 不跳过任务 1 的 admission 和任务 5 的 reservation。
- 不把 dashboard 或持久化写入放进每条行情热路径。

验收建议：

```bash
cargo test cross_exchange_arbitrage_event --all-features
cargo test cross_arb_live --all-features
cargo fmt --check
```

分派 Prompt：

```text
你是 AI-3。完成 docs/high_frequency_perpetual_dual_taker_arbitrage_plan_zh.md 的开发任务 3：WebSocket 事件驱动 Symbol 级评估。先读取全局并行规则和共享接口约定。只修改任务 3 允许范围。目标是 public WS book update 后只评估该 symbol 的 directed venue pairs，ticker 不再作为开仓唯一触发源。L1 trigger 可触发评估，但开仓前必须满足 validation depth 或 L1 数量覆盖。不要直接在 WS reader 中下单，不绕过 admission、reservation、kill switch 或 private stream ready。
```

### 开发任务 4：双向双 Taker Opportunity 与 L1/Depth 校验

目标：完整评估 A long/B short 和 B long/A short，并支持 L1 触发、多档深度校验的双 taker 机会模型。

允许范围：

- `retired strategy tree/cross_exchange_arbitrage/opportunity.rs`
- `retired strategy tree/cross_exchange_arbitrage/simulation.rs`
- `retired strategy tree/cross_exchange_arbitrage/sizing.rs`
- `retired strategy tree/cross_exchange_arbitrage/types.rs`
- focused tests

工作：

- dual_taker 模式下完整评估两个方向：A long/B short、B long/A short。
- 对 long leg 使用 ask side，对 short leg 使用 bid side。
- 支持 trigger book 与 validation book 的来源标记。
- 如果只用 L1 下单，必须检查 best level quantity 覆盖目标 base qty。
- 如果使用 validation depth，必须用多档 VWAP 计算可成交 notional 和 slippage。
- 输出 normalized base qty、双腿 limit price、expected edge、reject reasons。
- 单测覆盖：A 低 B 高、B 低 A 高、L1 数量不足、depth 足够、depth 不足。

不得做：

- 不改真实下单。
- 不绕过 fee/funding/slippage/safety buffer。
- 不让 maker leg kind 限制 dual_taker 的方向。

验收建议：

```bash
cargo test cross_exchange_arbitrage_opportunity --all-features
cargo test cross_exchange_arbitrage_vwap --all-features
cargo fmt --check
```

分派 Prompt：

```text
你是 AI-4。完成 docs/high_frequency_perpetual_dual_taker_arbitrage_plan_zh.md 的开发任务 4：双向双 Taker Opportunity 与 L1/Depth 校验。先读取全局并行规则和共享接口约定。只修改任务 4 允许范围。目标是 dual_taker 下完整评估 A long/B short 和 B long/A short，支持最快 L1 触发和多档 depth 校验，L1 直接开仓必须检查一档名义容量：`min(long best ask price * qty, short best bid price * qty) * top_of_book_capacity_ratio >= target_notional_usdt`，默认比例 0.8。添加 focused tests。不要改真实执行、live admission 或 reservation。
```

### 开发任务 5：原子风控 Reservation 与单腿恢复

目标：开仓前做本地原子 reservation；双 taker 结果异常时立即 close-only / symbol close-only，并进入 repair/reconcile。

允许范围：

- `retired strategy tree/cross_exchange_arbitrage/risk.rs`
- `retired strategy tree/cross_exchange_arbitrage/execution.rs`
- `retired strategy tree/cross_exchange_arbitrage/position.rs`
- `retired strategy tree/cross_exchange_arbitrage/tasks.rs`
- `src/execution/**`
- focused tests

工作：

- 开仓候选提交前 reserve `symbol + long_exchange + short_exchange + notional`。
- 并发候选对同一 symbol 或同一 bundle key 只能有一个通过 reservation。
- 提交失败、拒单、取消、正常 both-filled 后按状态释放或转移 reservation。
- 一腿 reject、partial、unknown、readback timeout 时标记 one-sided/unknown risk lock。
- risk lock 期间禁止新开仓，允许 reduce-only close、repair、reconcile。
- 私有流优先确认 fills，短超时后 REST readback。
- repair 期间进入 symbol close-only；严重 unknown 可进入全局 close-only。
- 单测覆盖：并发候选冲突、一腿 reject、一腿 partial、一腿 unknown、repair 后释放。

不得做：

- 不把 unknown 当作正常成交。
- 不在 one-sided exposure 存在时允许新开仓。
- 不绕过 reduce-only 和 close-only 语义。
- 不删除现有 hedge repair 逻辑；应复用或收敛。

验收建议：

```bash
cargo test cross_exchange_arbitrage_risk --all-features
cargo test cross_exchange_arbitrage_execution --all-features
cargo test one_sided --all-features
cargo fmt --check
```

分派 Prompt：

```text
你是 AI-5。完成 docs/high_frequency_perpetual_dual_taker_arbitrage_plan_zh.md 的开发任务 5：原子风控 Reservation 与单腿恢复。先读取全局并行规则和共享接口约定。只修改任务 5 允许范围。目标是双 taker 提交前本地原子 reservation，异常成交结果进入 one-sided/unknown risk lock 和 close-only/symbol close-only，并通过私有流优先、REST readback 辅助完成确认和 repair。添加 focused tests。不要把 unknown 当作正常成交，不允许 one-sided 期间新开仓。
```

### 第二批后续任务

第一批任务 1-5 完成后，再启动以下任务：

- 延迟与成交质量归因：记录 signal/eval/submit/ack/fill/readback 全链路耗时和实际费用滑点。
- 严格 L2 sequence/checksum/resync：对需要本地 merge 的 Gate、Bybit、HTX 增加断档恢复。
- 回放与 canary 报告：用历史 book event 和模拟延迟验证双 taker edge 衰减。

## 九、验收标准

进入 live-small 前必须满足：

- dry-run 下 WebSocket 推送能触发 symbol 级机会评估。
- 公共盘口订阅 profile 能选择最快可用 channel，并记录实际推送间隔。
- 双 taker only admission 生效。
- 两腿并发提交路径有测试覆盖。
- 一腿 reject / partial / unknown 都会进入 close-only 或 symbol close-only。
- one-sided exposure 不会允许新开仓。
- REST readback 失败不会被当作正常成交。
- 所有实盘订单都有 IOC/FOK limit price 和 max slippage。
- 10U 小仓 canary 能完整记录从 signal 到 fill/reconcile 的全链路事件。

建议验证命令：

```bash
cargo test cross_exchange_arbitrage --all-features
cargo test router_should_submit_cross_exchange_bundle_concurrently --all-features
cargo test one_sided --all-features
cargo fmt --check
```

完整发布前再跑：

```bash
cargo clippy --all-targets --all-features
cargo test --all-features
```

## 十、推荐实施顺序

第一批任务 1-5 可以并行开发，但合并和实盘启用建议按以下顺序：

1. 先合并开发任务 1，强制双 taker only，避免策略同时混入 maker-taker 风险。
2. 合并开发任务 2，确认最快 L1 trigger 和 depth validation profile。
3. 合并开发任务 4，保证双向 dual taker opportunity 与 L1/depth 校验完整。
4. 合并开发任务 5，补齐 reservation、one-sided risk lock 和恢复闭环。
5. 最后合并开发任务 3，把开仓评估从 ticker 推进到 WebSocket event driven。

这个顺序的原则是：先缩小交易行为，再确认行情速度上限，同时补牢异常恢复。最快盘口订阅可以先完成 dry-run 接入，但 live-small 前必须保证单腿风险恢复、reservation 和 close-only 逻辑已经生效。

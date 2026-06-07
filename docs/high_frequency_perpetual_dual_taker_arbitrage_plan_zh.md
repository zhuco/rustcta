# 高频永续合约跨所双 Taker 套利优化计划

状态日期：2026-06-07

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

- `src/strategies/cross_exchange_arbitrage/**`
- `src/bin/cross_arb_live.rs`
- `src/execution/**` 中与 bundle 并发提交、one-sided exposure、订单确认相关的小范围改动
- `src/market/cache.rs` 或 WebSocket 更新接入点的轻量扩展
- `src/exchanges/market_adapters/**` 中公共永续盘口 WebSocket 订阅 channel / speed profile 的小范围改动
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

建议新增配置：

```yaml
market:
  public_book_speed: fastest
  public_book_depth: 5
  allow_top_of_book_only: false
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
- 若只使用 top-of-book，需要强制更小 notional 和更高 safety buffer。
- 订阅失败、消息过载、sequence gap、checksum fail 时自动降级到 close-only 或 conservative profile。
- 每个交易所的订阅发送间隔和每连接 symbol 数量不能超过官方限制。
- 记录实际 `book_recv_interval_ms`、gap 次数、重连次数，用真实数据判断“最快”是否稳定。

当前优先审计对象：

```text
Binance USD-M: depth5@100ms 或交易所支持的更快 top-of-book channel
OKX SWAP: books5 / books-l2-tbt 等可用快照或增量 channel
Bitget USDT-FUTURES: books5 / books1 / books 等可用 channel
Bybit Linear: orderbook.1 / orderbook.50 等可用 channel
Gate / MEXC / HTX: 只在延迟和稳定性达标后纳入高速主交易腿
```

注意：更快的 channel 不一定更适合双 taker。若最快 channel 只有 1 档盘口，而目标 notional 需要吃多档，必须使用足够深度的 channel 或把下单 notional 降到 1 档可覆盖范围内。

### 3. 双向价差评估

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

### 4. 双 Taker 执行请求

配置上应强制：

```yaml
execution:
  open_execution_style: dual_taker
  close_execution_style: dual_taker
  taker_ioc_slippage_limit_pct: 0.001
  allow_maker_taker_live: false
```

如果当前配置结构没有 `allow_maker_taker_live`，可以先通过 validate 逻辑或 live runner admission 限制：前期 live run 只接受 `open_execution_style=dual_taker`。

### 5. 成交状态机

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
  public_book_depth: 5
  allow_top_of_book_only: false

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
  target_notional_usdt: 10.0
  max_notional_usdt: 10.0
  max_symbol_notional_usdt: 10.0
```

说明：

- 第一阶段 `max_open_bundles` 建议为 1，先验证链路，不追求并发收益。
- `dry_run=true` 跑稳定后，再进入 live-small。
- 真实阈值不要只拍脑袋调低，要用记录数据计算：成交前 edge、成交后 edge、费用、滑点、延迟衰减。

## 八、开发任务拆分

### Task 1：双 Taker Only Admission

目标：live runner 前期只允许双 taker 开仓。

工作：

- live admission 校验 `open_execution_style=dual_taker`。
- 禁止 maker-taker live 开仓。
- 配置样例新增双 taker only 文件。
- 单测覆盖：配置不是 dual_taker 时 live-small execute 被拒绝。

### Task 2：最快公共盘口 WebSocket Profile

目标：每个永续交易所适配器都能声明并选择最快可用盘口订阅 profile。

工作：

- 为 `MarketDataAdapter` 或永续 market adapter 增加 public book profile 描述。
- 配置支持 `public_book_speed=fastest|balanced|conservative`。
- Binance / OKX / Bitget / Bybit 优先确认最快可用 USDT 永续盘口 channel。
- 根据 profile 构建 WebSocket subscription，而不是在适配器里写死单一 channel。
- 记录实际推送间隔、重连、gap、checksum/resync 指标。
- 单测覆盖：fastest profile 会选择预期 channel，conservative profile 可降级。

不得做：

- 不为了追求速度选择无法校验或明显不适合永续的现货 channel。
- 不在数据不完整时继续输出可执行机会。
- 不忽略交易所订阅速率和每连接 symbol 数量限制。

### Task 3：行情事件驱动的 symbol 级评估

目标：WebSocket book 更新后直接触发相关 symbol 评估。

工作：

- 在 public WS update 处理后触发 symbol-level candidate evaluation。
- ticker 不再作为开仓唯一触发源。
- 每次只重算当前 symbol 的 directed venue pairs。
- 单测覆盖：一个 book update 只影响对应 symbol。

### Task 4：双向双 Taker Opportunity

目标：完整评估 A long/B short 和 B long/A short。

工作：

- 确认 dual taker 模式下不受 maker leg kind 单向限制。
- 输出 long_exchange、short_exchange、buy/sell price、normalized qty。
- 单测覆盖：A 低 B 高、B 低 A 高都能产生双 taker 机会。

### Task 5：Pre-Trade Atomic Risk Reservation

目标：开仓前对 symbol/exchange/notional 做本地原子 reservation。

工作：

- 开仓候选提交前 reserve symbol + two exchanges + notional。
- 提交失败、拒单、超时、取消时释放 reservation。
- one-sided/unknown 时 reservation 转为 risk lock，不允许新开仓。
- 单测覆盖并发两个候选只能通过一个。

### Task 6：双腿结果确认与单腿恢复

目标：双 taker 成交后快速确认 BothFilled / OneSided / Unknown。

工作：

- 私有流优先归因 fills。
- 短超时后 REST readback。
- partial/reject/unknown 统一进入 repair 状态。
- repair 期间 symbol close-only。
- 单测覆盖一腿 reject、一腿 partial、一腿 unknown。

### Task 7：延迟与成交质量归因

目标：知道真实收益被哪里吃掉。

每个 bundle 记录：

```text
book_recv_ts
strategy_eval_ts
submit_ts
ack_ts
private_fill_ts
readback_ts
edge_at_signal
edge_at_submit
expected_fee
actual_fee
expected_slippage
actual_slippage
one_sided_duration_ms
repair_pnl
```

验收：每笔双 taker 都能输出完整归因记录。

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

1. 先做 Task 1，强制双 taker only，避免策略同时混入 maker-taker 风险。
2. 做 Task 2，确认最快公共盘口订阅 profile，避免策略继续吃慢行情。
3. 做 Task 4，保证机会方向完整。
4. 做 Task 6，先把单腿恢复闭环补牢。
5. 做 Task 5，增加开仓 reservation，避免并发重复开仓。
6. 做 Task 3，把策略开仓评估改成 WebSocket event driven。
7. 做 Task 7，用真实数据调阈值。

这个顺序的原则是：先缩小交易行为，再确认行情速度上限，同时补牢异常恢复。最快盘口订阅可以先完成 dry-run 接入，但 live-small 前必须保证单腿风险恢复、reservation 和 close-only 逻辑已经生效。

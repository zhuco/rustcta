# 跨所合约套利 Live Runner 低延迟运行约定

Status date: 2026-06-09

本文档约束当前 `cross_exchange_arbitrage` 永续合约跨所套利的运行方式。目标不是把观察、dry-run、实盘拆成多套进程，而是统一到一个 runner：默认只分析，满足双开关后才允许真实下单。

## 当前结论

- 唯一运行程序：`cross-exchange-arbitrage-live-runner`。
- 默认模式：分析模式，不提交订单。
- 实盘模式必须同时满足：
  - 配置 `execution.trading_enabled: true`。
  - 启动参数显式传入 `--enable-live-trading`。
- 实盘部署只使用 release binary：`cargo build --release --bin cross-exchange-arbitrage-live-runner`。
- 行情热路径使用同进程 direct websocket 事件驱动，不再使用 `snapshot_interval_ms` 或外部 snapshot 文件轮询。
- Dashboard 刷新只做 UI/观测输出，默认 5000ms，不参与交易决策等待。
- 私有 WebSocket 订单/成交事件是实盘确认主路径；REST readback 只允许用 `--allow-rest-readback-confirmation` 做受控 canary/排障。
- ClickHouse、Dashboard、文件归档和交易日志栈不能进入交易热路径。

## 运行入口

分析模式：

```bash
/home/cta/rustcta/target/release/cross-exchange-arbitrage-live-runner \
  --config /home/cta/rustcta/config/cross_exchange_arbitrage_usdt.yml \
  --strategy-id cross_arb_live \
  --run-id server-live \
  --tenant-id server \
  --account-id cross_arb_3venues \
  --lock-file /home/cta/rustcta/logs/cross_exchange_arbitrage/cross_arb_live.lock \
  --dashboard-snapshot-path /home/cta/rustcta/logs/cross_exchange_arbitrage/cross_arb_live_dashboard.json \
  --market-data-source direct_websocket \
  --profit-history-path /home/cta/rustcta/logs/cross_exchange_arbitrage/profit_history.jsonl \
  --trade-ledger-path /home/cta/rustcta/logs/cross_exchange_arbitrage/trade_events.jsonl \
  --dashboard-refresh-ms 5000
```

实盘模式只在 release binary 下额外加：

```bash
--enable-live-trading
```

配置侧还必须把 `execution.trading_enabled` 改为 `true`。两者缺一时 runner 仍保持分析模式。

## 已废弃入口

以下入口不再用于跨所合约套利实盘：

- `--snapshot-interval-ms`
- `--market-data-snapshot-path`
- `--market-data-snapshot-stale-ms`
- `--market-data-snapshot-readiness-wait-ms`
- `--market-data-source snapshot_file`
- 独立 cross-arb observe 程序/服务
- 独立 cross-arb dry-run 程序/配置

Dashboard snapshot 现在只是输出文件；交易决策不能读取它作为行情输入。

## 延迟指标

runner 会向 `trade_events.jsonl` 写入 `cross_arb_latency_span` ledger 事件，覆盖两类链路：

- 行情到决策：`exchange_ts -> received_at -> decision_started_at/decision_finished_at`。
- 订单到确认：`submit_started_at -> exchange_ack_at -> private_fill_at`。

字段重点：

- `market_data_latency_ms`
- `decision_latency_ms`
- `submit_to_ack_ms`
- `fill_confirm_latency_ms`
- `exchange`
- `symbol`
- `role`
- `client_order_id`

交易日志技术栈可以直接消费这些事件做 P50/P95/P99、按交易所拆分、按 symbol 拆分和异常样本回放。延迟事件不写 dashboard/profit history 文件，避免下单前等待文件 IO。

## 热路径边界

允许在热路径中发生：

- 公共 WS book event 更新本地内存状态。
- 对受影响机会进行本地评估。
- 双腿 taker IOC/FOK 类订单并发提交。
- 通过私有 WS 等待订单/成交确认。

不允许在热路径中等待：

- Dashboard 刷新。
- ClickHouse 写入。
- 控制面 HTTP。
- 外部 snapshot 文件。
- 非必要 REST readback。

## 日志约定

实盘 systemd 默认 `RUST_LOG=warn`。常规行情、dashboard 和分析输出不应刷爆实盘日志；关键事件和异常应保留：

- 下单接受/拒绝。
- 私有 WS 确认超时。
- 单腿成交和 emergency close。
- 风控阻断。
- 私有 WS 断线、重连、登录失败。
- capability gate 失败。

运行日志统一走 `tracing`/`tracing-subscriber`，实盘默认 `RUST_LOG=warn`。交易级详细归因走 `rustcta-event-ledger`：

- `trade_events.jsonl` 是结构化交易 ledger，记录 order ack、fill、关键审计事件和延迟链路。
- live runner 热路径只 `try_send` 到 bounded channel，不等待磁盘 fsync。
- `profit_history.jsonl` 保留给 dashboard 和历史盈亏兼容，不作为交易日志主链路。
- dashboard/ClickHouse 只能作为下游消费，不允许成为交易路径依赖。

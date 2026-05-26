# OBI 高频剥头皮策略说明

## 定位

`obi_scalper` 是多品种并行的订单流不平衡（OBI）剥头皮策略。当前默认配置为 `DOGE/USDC`，架构支持在同一配置文件中添加多个交易对并行运行。

## 架构

- 每个交易对一个独立 actor，行情、成交、超时处理在本 symbol 内串行执行，避免多品种互相阻塞。
- 全局任务只负责 WebSocket 分发、用户成交流分发和 30 分钟汇总通知。
- 全局风控聚合所有 actor 的持仓名义金额和已开仓 symbol 数，统一限制总敞口。

## 行情输入

- 直接使用 Binance WebSocket 深度流：`@depth20@100ms`。
- 多交易对使用 combined stream，一条连接接收多个 symbol 的 20 档深度。
- 热路径不再使用 REST 轮询获取订单簿；REST 仅应作为未来故障恢复或校验补充。
- 当前采用 20 档全量深度快照，不依赖 diff-depth 的 `U/u/pu` 连续性；如果后续切换 diff-depth，必须在 `pu != previous_u` 时触发重建。

## 信号

- 计算前 10 档加权 OBI：越靠近盘口权重越高。
- 使用 EMA Fast=5 和 EMA Slow=20 过滤噪音。
- 做多：`EMA_Fast_OBI > long_entry_threshold` 且 Fast > Slow，价格稳定或微升。
- 做空：`EMA_Fast_OBI < short_entry_threshold` 且 Fast < Slow，价格稳定或微降。
- 平仓：信号回落、固定止损或持仓超时。

## 执行

- 默认 Maker/PostOnly 优先，期货使用 `timeInForce=GTX`。
- 开仓默认不吃单；只有显式开启 `allow_taker_entry` 才允许 IOC 入场。
- 止损、超时、信号退出在 `allow_taker_exit=true` 时使用 IOC 退出，降低风险暴露时间。
- 每单金额默认 `5U`，手续费按 maker `0`、taker `0.0004` 计入统计。

## 未成交与失败处理

- 开仓挂单超过 `entry_order_ttl_ms` 未成交：撤单并回到空闲状态。
- 平仓挂单超过 `exit_order_ttl_ms` 未成交：撤单并保留持仓状态，等待下一次行情触发继续退出。
- 实盘通过 Binance 用户数据 WebSocket 处理 `ORDER_TRADE_UPDATE`，以成交回报驱动订单状态机。
- 订单状态覆盖 `NEW / PARTIALLY_FILLED / FILLED / CANCELED / EXPIRED / REJECTED`。
- 订单提交失败会记录错误计数，不修改成本地已成交状态。
- 连续亏损达到 `max_consecutive_losses` 后进入 `consecutive_loss_cooldown_ms` 冷静期；未达到阈值则使用 `cooldown_after_loss_ms`。

## 风控

- `dry_run` 默认开启，防止新策略误实盘。
- `max_daily_loss` 达到后停止新开仓。
- `max_symbol_notional` 和 `max_total_notional` 控制持仓名义金额上限。
- WebSocket 超过 `emergency_close_on_disconnect_ms` 无数据会自动重连。
- 实盘启动前会检查交易所连接、已有挂单、已有仓位；发现冲突会拒绝启动。

## 日志与通知

- 关键开仓、平仓、超时、WebSocket 重连、行情处理延迟都会写日志。
- `logging.max_lines` 限制关键日志最大行数，避免异常行情下刷爆日志。
- 每 `summary_interval_minutes` 发送一次企业微信 Markdown 总结，包括仓位、挂单、交易次数、手续费、30 分钟 PNL 和日内 PNL。

## 启动命令

```bash
cargo run --bin rustcta -- --strategy obi_scalper --config config/obi_scalper_doge_usdc.yml
```

实盘前必须把配置中的 `dry_run` 改为 `false`，并先小资金观察日志、成交、撤单和通知链路。

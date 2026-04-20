# 订单流策略开发说明（RustCTA）

## 目标
- 交易所：Binance Futures（示例：SOL/USDC，如该合约不可用请改为 SOL/USDT）
- 策略形态：事件驱动、轻量补单/换价，优先 Maker，止损/紧急平仓可用 Taker
- 参数示例：每单 5.5U，批量上限按交易所（Binance 单批 5 单）

## 分阶段实施
1) 基础校验/风控前置：符号有效性、精度/最小名义检查，下单前保证金/滑点/冲击限额
2) 盘口/队列与事件补单：本地 orderbook + 队列位次，成交/订单事件驱动补缺口，不做全量 reset，异常时才撤单重建
3) 执行与批量：批量并行（5 笔一批），幂等 clientOrderId，撤换价策略可配
4) 监控与回放：延迟、失败率、滑点、库存、PnL、挂单完整度；离线回放深度+成交验证
5) 灰度上线：单标的、小额度，开启熔断，验证后放量

## 配置示例
`config/orderflow_sol.yml`
```
name: "SOL Orderflow"
account_id: "binance_hcr"
symbol: "SOL/USDC"
order_size: 5.5
# 价差（基点），0.08% 为 8 bps
spread_bps: 8.0
# 双边挂单阶梯数
orders_per_side: 5
# 刷新/轮换挂单间隔（秒）
refresh_secs: 5
# 盘口不平衡对中价偏移（bps），使用前3档计算
imbalance_bias_bps: 5.0
imbalance_levels: 3
# OFI（订单流失衡）档数与灵敏度
ofi_levels: 5
ofi_sensitivity_bps: 6.0
ofi_rebuild_bps: 12.0
# 事件轮询间隔（ms），检测成交后快速重挂
event_poll_ms: 1000
# 挂单 TTL（秒），超时强制重挂
order_ttl_secs: 90
# REST 获取挂单最小轮询间隔（秒），其余用本地状态
open_orders_poll_secs: 12
# 队列前方允许的最大名义（超出触发撤换价）
queue_ahead_notional: 150
# 顶档滑点/跳价阈值（bps），超出触发重挂
slippage_requote_bps: 3.0
# 仓位名义上限（USDC），达到上限仅保留减仓侧挂单
max_position_notional: 250
# 库存偏斜对价差的偏移（bps）
inventory_spread_bps: 5.0
# 风控阈值示例
max_unrealized_loss: 120
max_daily_loss: 200
max_consecutive_losses: 8
stop_loss_pct: 0.02
```

## 代码入口
- 模块：`src/strategies/orderflow/mod.rs`（当前为占位策略，后续可扩展 orderbook/队列/事件补单）
- main 启动参数：`--strategy orderflow --config config/orderflow_sol.yml`

## 后续扩展建议
- 增加本地 orderbook 与队列位次估算，挂单完整度检查
- 事件驱动补单：成交/订单状态变化直接补缺口，避免全量 reset
- 前置风控：滑点/冲击/保证金/库存/日亏熔断
- 监控：延迟、撤单失败率、滑点、队列位次、挂单总数、PnL
- 回放：录制/回放深度+成交，验证胜率与延迟

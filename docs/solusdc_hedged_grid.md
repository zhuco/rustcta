# SOLUSDC 永续对冲滚动网格策略

## 1) 策略概览
该策略在 Hedge Mode 下维护双向网格，围绕参考价滚动挂单，收益来源为震荡期间的做市价差。网格采用固定名义下单与滚动更新，避免因仓位偏移导致的单边停滞。长期可跑的关键在于：严格的仓位覆盖约束、风险阀对净敞口/总名义/保证金的限制、资金费率窗口管理，以及订单/持仓对账与重建。

核心收益来源：在可控波动区间内反复成交的 Maker 价差收益；非趋势策略，不承诺收益。

核心风险与控制点：
- 单边行情导致库存偏移：通过激进跟随与滚动补单保持梯子贴近现价。
- 资金费率与保证金压力：按资金费率阈值与保证金安全阀限制开仓。
- 订单一致性与断线：通过对账与重建保持账本一致，避免幽灵订单。

## 2) 关键参数表（示例默认值）

| 参数 | 保守 | 中等 | 激进 | 说明 |
| --- | --- | --- | --- | --- |
| `levels_per_side (K)` | 2 | 3 | 5 | 单侧网格层数 |
| `grid_spacing_pct` | 0.0015 | 0.0010 | 0.0006 | 几何间距 |
| `grid_spacing_abs` | 0.20 | 0.14 | 0.08 | 固定价差（与 `grid_spacing_pct` 二选一） |
| `order_notional` | 8 | 10 | 15 | 每档名义 |
| `order_qty` | 0.02 | 0.04 | 0.06 | 固定下单数量（与 `order_notional` 二选一） |
| `max_gap_steps` | 2 | 1 | 1 | 激进跟随触发格数 |
| `follow_cooldown_ms` | 1200 | 800 | 400 | 跟随冷却 |
| `max_follow_actions_per_minute` | 15 | 30 | 45 | 每分钟跟随上限 |
| `max_net_notional` | 300 | 500 | 800 | 净敞口上限 |
| `max_total_notional` | 600 | 1000 | 1500 | 总名义上限 |
| `margin_ratio_limit` | 0.5 | 0.65 | 0.75 | 保证金安全阈值 |
| `funding_rate_limit` | 0.001 | 0.002 | 0.003 | 资金费率阈值 |

## 3) 槽位分配算法说明
开仓/平仓双网格并行维护：每个方向各维护 K 档开仓单与 K 档平仓单。初始平仓网格以参考价生成、与开仓网格镜像分布（平多在上方、平空在下方）；成交后补单则按成交价±网格间距滚动，库存不足时跳过最远档位的平仓单。

- Sell Book：`OPEN_SHORT_SELL`（K 档） + `CLOSE_LONG_SELL`（最多 K 档）
- Buy Book：`OPEN_LONG_BUY`（K 档） + `CLOSE_SHORT_BUY`（最多 K 档）

`fill_remaining_slots_with_opens = false` 时，仅维护平仓网格，不再补开仓网格。

## 4) 成交滚动与激进跟随（伪代码）

**OPEN_LONG_BUY 成交：**
```
extend_price = fill_price * (1 - grid * K)
place OPEN_LONG_BUY(extend_price, order_notional)

close_price = fill_price * (1 + grid)
place CLOSE_LONG_SELL(close_price, fill_qty)

if close_orders > K:
  cancel farthest CLOSE_LONG_SELL
```

**OPEN_SHORT_SELL 成交：**
```
extend_price = fill_price * (1 + grid * K)
place OPEN_SHORT_SELL(extend_price, order_notional)

close_price = fill_price * (1 - grid)
place CLOSE_SHORT_BUY(close_price, fill_qty)

if close_orders > K:
  cancel farthest CLOSE_SHORT_BUY
```

**平仓成交（CLOSE_LONG_SELL）：**
```
extend_close = fill_price * (1 + grid * K)
place CLOSE_LONG_SELL(extend_close, order_notional)

extend_open = fill_price * (1 - grid)
place OPEN_LONG_BUY(extend_open, order_notional)

cancel farthest OPEN_LONG_BUY
```

**平仓成交（CLOSE_SHORT_BUY）：**
```
extend_close = fill_price * (1 - grid * K)
place CLOSE_SHORT_BUY(extend_close, order_notional)

extend_open = fill_price * (1 + grid)
place OPEN_SHORT_SELL(extend_open, order_notional)

cancel farthest OPEN_SHORT_SELL
```

**激进跟随（补1撤1）：**
```
gap_up = ln(mid / best_buy) / ln(1 + grid)
if gap_up > max_gap_steps:
  new_buy = best_buy * (1 + grid)
  place OPEN_LONG_BUY
  cancel farthest OPEN_LONG_BUY

gap_dn = ln(best_sell / mid) / ln(1 + grid)
if gap_dn > max_gap_steps:
  new_sell = best_sell * (1 - grid)
  place OPEN_SHORT_SELL
  cancel farthest OPEN_SHORT_SELL
```

## 5) 风控规则清单（触发 -> 动作）
- `|net_notional| > max_net_notional` -> 禁止进一步扩大净敞口的开仓单。
- `total_notional > max_total_notional` -> 禁止所有开仓单，仅保留平仓单。
- `margin_ratio > margin_ratio_limit` -> 立即取消所有 `OPEN_*` 挂单，只保留平仓单。
- `funding_rate` 或 `expected_funding_cost` 超阈值 -> 暂停开仓补单。
- 触发 kill switch -> 撤掉所有挂单，停止开仓。

## 6) Rust 模块划分与关键接口
- `config.rs`: `StrategyConfig`, `GridConfig`, `PrecisionConfig`, `RiskLimits`。
- `ledger.rs`: `OrderIntent`, `OrderSlot`, `OrderLedger`, `GridSideBook`。
- `risk.rs`: `RiskState` 与风控阀计算。
- `engine.rs`: `GridEngine`、`MarketSnapshot`、`FillEvent`、`EngineAction`。
- `sim.rs`: `SimulationEngine` 与简易回测输出。

与现有系统复用点：
- 精度处理复用 `market_making::precision::apply_precision`。
- 订单可映射至 `core::types::OrderRequest`，并使用 `params.positionSide` 指定 `LONG/SHORT`。
- 交易对与 API 封装可直接接入现有 `AccountManager`、`Exchange` 调用。

## 实时接入配置与启动
策略运行入口：`solusdc_hedged_grid`，配置文件需包含账户信息与引擎参数。
默认启用 Binance 用户数据流 WebSocket，用于实时成交/订单推送；不支持的交易所会自动回退到轮询对账。

示例配置（YAML）：
```
strategy:
  name: solusdc_hedged_grid
  log_level: INFO
account:
  account_id: binance_main
  market_type: Futures
engine:
  symbol: "SOL/USDC"
  require_hedge_mode: true
  price_reference: mid
  risk_reference: mark
  grid:
    levels_per_side: 3
    grid_spacing_pct: 0.001
    grid_spacing_abs: 0.14
    order_notional: 10.0
    order_qty: 0.04
    fill_remaining_slots_with_opens: true
  follow:
    max_gap_steps: 1.0
    follow_cooldown_ms: 800
    max_follow_actions_per_minute: 30
  execution:
    cooldown_ms: 500
    post_only: true
    post_only_retries: 3
  precision:
    tick_size: 0.01
    step_size: 0.001
    min_qty: 0.001
    min_notional: 5.0
  fees:
    maker_fee: 0.0
    taker_fee: 0.0004
  risk:
    max_net_notional: 500.0
    max_total_notional: 1000.0
    margin_ratio_limit: 0.7
    funding_rate_limit: 0.002
    funding_cost_limit: 3.0
polling:
  tick_interval_ms: 1000
  reconcile_interval_ms: 3000
  trade_fetch_limit: 50
websocket:
  enabled: true
  keepalive_interval_secs: 1200
  reconnect_delay_ms: 1000
  max_reconnect_delay_ms: 30000
execution:
  startup_cancel_all: true
  shutdown_cancel_all: true
  max_consecutive_failures: 5
```

启动命令：
```
cargo run --release -- --strategy solusdc_hedged_grid --config path/to/solusdc_hedged_grid.yml
```

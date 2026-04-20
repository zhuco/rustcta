# 对冲网格策略开发文档

## 1. 策略概述
- **目标**：通过在多个高相关交易对上配置多空底仓与网格，持续捕捉价差收益并降低底仓成本。
- **特点**：
  - 每个交易对可独立设定底仓方向（Long/Short）与网格参数。
  - 多币种组合中同时存在做多网格与做空网格，实现组合级对冲。
  - 库存不足时，自动将网格中心贴近最新成交价，维持做 T 频率。
  - 成交后的挂单补单流程复用现有趋势网格 / 自动网格的实现。

## 2. 适用场景与收益来源
- **行情条件**：主流币、平台币、L2 核心标的等具有高流动性、强相关性，波动中枢呈缓慢趋势或震荡；对极端单边行情需配套杀阈。
- **收益结构**：
  1. 底仓增值（多头底仓受益于上涨、空头底仓在下跌中获利）。
  2. 网格差价收益（低吸高抛 / 高抛低吸），在挂单免手续费场景中纯收益化。
  3. 风险对冲（空头组合在整体下跌时缓冲组合损失）。
  4. 资金费率收益（可选）：在多空对冲配置中择优持有正向 Funding 一侧。

## 3. 模块复用与依赖
| 功能 | 复用模块 | 说明 |
| --- | --- | --- |
| 账户管理 & 下单 | `cta::account_manager` + `exchanges::*` | 使用统一的 `OrderRequest` 与 `create_batch_orders`；保留 Binance 批量下单修复。 |
| 网格补单引擎 | `strategies::trend_grid_v2::operations` | 成交 → 撤边缘 → 补单流程复用，减少重复开发。 |
| 行情缓存 | `mean_reversion::model::SymbolSnapshot` + `common::shared_data` | 可选，用于波动率/趋势评估与网格自适应。 |
| 风控／通知 | `common::application` + `utils::webhook` | 共享统一风控接口与企业微信通知渠道。 |

## 4. 策略结构设计
```
src/strategies/hedged_grid/
├── config.rs      # 策略配置、底仓与网格定义
├── mod.rs         # 对外导出，注册 StrategyInstance
├── controller.rs  # 配置解析、依赖注入、任务启动
├── manager.rs     # 每个交易对的运行状态（底仓、网格中心、敞口）
├── grid_engine.rs # 调用 trend_grid 补单逻辑，生成价位与数量
├── hedging.rs     # 相关性计算、权重再平衡、底仓调节
├── risk.rs        # 组合保证金、回撤、资金费率风控
├── tasks.rs       # 行情订阅、定时调度、心跳检查
└── docs/hedged_dual_direction_grid.md
```

## 5. 核心流程
1. **配置解析**：读取 YAML，包含：
   - `base_account`：默认账户、保证金模式、最大杠杆、资金费率阈值。
   - `symbols[]`：方向（long/short）、目标底仓、网格参数、最大净敞口、库存回归参数、波动阈值、`min_inventory_ratio`（库存不足触发补仓的底仓比例阈值）。
   - `hedge_matrix`：相关性窗口、最小相关系数、目标权重、再平衡频率。
   - `kill_switch`：极端行情触发条件（如价格跌破关键支撑、净敞口超标、实时 VaR 超阈）。
2. **初始化**：
   - 查询账户当前持仓、资金费率、最新行情与历史波动，计算底仓差值与可用保证金。
   - 通过 `trend_grid_v2::operations::reset_grid` 生成首批挂单；若波动率 > 上限则放宽 spacing 或暂缓部署。
3. **实时执行**：
   - 成交 → 撤销最远端订单 → 根据 spacing/vol-adjusted spacing 重新补单（复用现有补单逻辑）。
   - 底仓不足时，根据库存偏差回归系数将网格中心贴近最新价；底仓超限时削减近端网格或主动冲击平仓。
   - 若波动率、斜率或成交量骤变，自动调整网格层数与间距（扩大 spacing、降低 levels）。
4. **对冲再平衡**：
   - 定期计算组合净敞口、相关性矩阵、资金费率净暴露；根据目标权重重置底仓与网格中心。
   - 当资金费率为负时自动减仓或切换对冲边；允许设置跨市场对冲（如使用期货或指数仓位）。
5. **风险控制**：
   - **保证金 + 杠杆**：实时监控账户保证金率、总杠杆，超过阈值则进入降风控模式（减少网格、关闭补单、强制对冲）。
   - **回撤限制**：跟踪组合净值与最高净值，超出最大回撤或当日亏损阈值即关闭所有网格并执行对冲平仓。
   - **库存偏差**：设置目标库存区间与回归速度，偏差超阈值时自动迁移网格中心或触发紧急对冲。
   - **波动跳变 Kill Switch**：如 5 分钟波动率 > 平均值 3 倍、或价格跌破关键支撑/突破阻力，则暂停相关交易对。
   - **急停机制**：遇到交易所异常、网络异常、订单大量拒绝时，立即取消所有挂单并锁定追加操作。

- 复用趋势网格的成交监听：订单成交后触发补单生成对称价位。
- 单边策略（如只做多）仅补买单和获利卖单；若多空都有，则按配置补齐双向。
- 延续批量下单接口，`OrderType::Market` 不附带 `timeInForce`，确保 Binance 兼容。
- `client_order_id`、`post_only`、`reduce_only` 等字段与现有实现保持一致，便于排查。
- 根据实时波动率动态调整 `spacing` 与 `levels_per_side`：波动升高时扩大间距、减少层数；波动回落后逐步恢复。
- 若净敞口超限或 kill switch 触发，立即撤销未成交订单并暂停补单，待库存归位后恢复。

## 7. 配置示例
```yaml
strategy:
  name: "Hedged Grid"
  enabled: true
  market_type: "Futures"

symbols:
  - config_id: "btc_usdc_long"
    symbol: "BTC/USDC"
    direction: "long"
    base_position: 0.3
    grid:
      spacing: 150.0
      order_amount: 0.01
      levels_per_side: 3
    position_limit: 0.6
    rebalance_on_shortage: true

  - config_id: "eth_usdc_short"
    symbol: "ETH/USDC"
    direction: "short"
    base_position: 1.2
    grid:
      spacing: 10.0
      order_amount: 0.05
      levels_per_side: 2
    position_limit: 1.5
    rebalance_on_shortage: true

hedge_matrix:
  correlation_window: 7200
  min_correlation: 0.7
  rebalance_interval: 900

risk_control:
  max_leverage: 2.0
  max_drawdown_pct: 0.12
  max_daily_loss: 100
  emergency_stop_trigger: 0.18

execution:
  startup_cancel_all: true
  shutdown_cancel_all: true
  thread_per_config: true

logging:
  level: "info"
  file: "logs/hedged_grid.log"
```

## 8. 开发步骤
1. **配置与模型**：在 `config.rs` 定义新结构，解析 YAML 时复用 `serde`。
2. **状态管理**：扩展运行时状态记录底仓、网格中心、敞口、补单队列、波动指标、资金费率、回撤曲线。
3. **网格引擎**：通过适配器调用 `trend_grid_v2::operations::submit_orders`，并增加波动自适应与库存偏差约束。
4. **对冲模块**：实现 `hedging.rs`，计算相关矩阵、敞口差值，触发底仓调整或网格移位；支持资金费率与跨市场对冲策略。
5. **风控串联**：使用 `common::build_unified_risk_evaluator` 叠加组合级风险指标、Kill Switch、VaR/回撤监控。
6. **任务调度**：参考 `range_grid::application::tasks`，实现行情订阅、定期再平衡、风险扫描、资金费率检查。
7. **测试**：
   - 单元测试：价位生成、库存回归函数、波动自适应逻辑、资金费率调节。
   - 集成测试：模拟成交回调，验证补单、风控、对冲联动；构造极端行情脚本，确保 Kill Switch 与紧急平仓可执行。

## 9. 注意事项
- 默认保持低杠杆（1x-2x），限制最大持仓与回撤。
- 当相关性显著下降或个别标的突发事件时，支持快速降权或暂停。
- 结合资金费率调整底仓方向，避免负 Funding 消耗。
- 定期记录做 T 收益、底仓成本、对冲效果，便于复盘调参。

## 10. 后续扩展
- **自适应网格**：根据实时波动率/流动性调整 spacing 与层数。
- **智能对冲**：引入资金流向、链上指标等量化信号动态分配多空权重。
- **可视化监控**：复用监控模块呈现多空敞口、底仓平均成本、资金费率等。
- **策略协同**：允许与趋势网格、均值回归共享行情订阅和下单通道，降低资源占用。

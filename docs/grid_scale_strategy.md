RustCTA Grid Scale 策略说明
============================

本文档将 Hummingbot PR #7585（`grid_scale` 控制器）的逻辑映射到 RustCTA，说明要点、配置字段、运行流与风险控制，便于后续在 RustCTA 中实现等价策略。

1. 策略概览
-----------
- 目标：在预设价格区间内铺设多档买/卖限价单（Post Only），成交后按目标价/收益触发止盈退出，网格动态跟随行情和激活区间更新。
- 关键特征：
  - 价格区间切片：多个 GridRange，每个区间单独定义起止价、方向、权重（占用总资金比例）、开仓/止盈订单类型。
  - 资金分配：按照区间 `total_amount_pct` 占用总报价资金 `total_amount_quote`，在区间内按价格均匀分布，同时对每档设置递增/递减的下单量以在区间两端更激进。
  - 步长/精度：基于交易规则 `min_price_increment` 和可选 `min_spread_between_orders` 决定档位间距；下单量取 `max(min_notional_size, min_order_amount)`。
  - 激活边界：只有当买单价高于 `mid*(1-activation_bounds)` 或卖单价低于 `mid*(1+activation_bounds)` 时才允许挂单；超出区间的执行器会被停止。
  - 执行器模型：每个档位对应一个 PositionExecutor，入场价格为档位价或当前 mid（取决于方向与位置），止盈通过 TripleBarrier（take_profit 百分比 + time_limit，可选 trailing_stop）。
  - 周期更新：每 `grid_range_update_interval` 秒重算档位（使用当前 mid 和规则），并剔除已被标记 inactive 的区间。

2. 配置字段映射建议（RustCTA）
----------------------------
在 RustCTA 可新增 `config/grid_scale_<symbol>.yml`，核心字段参考：
- `account`：账户 ID、`market_type`。
- `symbol`：交易对。
- `total_amount_quote`：可用名义资金（USDT）。
- `grid_ranges`（数组）：
  - `id`：区间标识。
  - `start_price` / `end_price`：区间上下界（绝对价格）。
  - `total_amount_pct`：占用总资金比例（0-1）。
  - `side`：BUY 或 SELL（BUY 区间在下侧补仓，SELL 区间在上侧做空/减仓）。
  - `open_order_type`：默认 LIMIT_MAKER（Post Only）。
  - `take_profit_order_type`：LIMIT 或市价类。
  - `active`：是否参与计算。
- `min_spread_between_orders`：可选，若为空则使用交易所最小 tick；否则按 `min_spread * mid` 与 `tick` 取较大者。
- `min_order_amount`：可选，若未设则使用交易规则的最小量/名义。
- `max_open_orders`：同时活跃的档位数上限（按距离 mid 的近远截断）。
- `activation_bounds`：激活边界百分比（如 0.01 表示 ±1% 之外才挂单）。
- `grid_range_update_interval`：重算网格的周期（秒）。
- `position_mode` / `leverage`：期货模式与杠杆。
- `time_limit`：单档执行器的最长存活时间（秒）。
- `extra_balance_base`：现货模式下的额外基准币缓冲。

3. 核心计算流程
---------------
1) 读取交易规则：tickSize、min_notional、min_order_size 等，用于量化价格/数量。
2) 为每个激活的 GridRange：
   - 计算步长 `step = max(tick, min_spread_between_orders * mid)`。
   - 计算在区间内能放置的订单数：`orders = min((end-start)/step, total_amount_quote / min_amount)`。
   - 生成线性价格序列 `prices`（start->end 等距）。
   - 资金分配：基础量 `base_amount` + 递增差值 `different_per_level`，使区间两端订单更大。
   - 量化：价格按 tick 量化，买/卖量按 step_size 或最小名义量量化。
   - 目标价：对称目标价（区间另一侧的对应价）作为止盈参考。
   - 构建 GridLevel：`price`, `target_price`, `buy_amount`, `sell_amount`, `step`, `side`, `open_order_type`, `take_profit_order_type`。
3) 每 `grid_range_update_interval` 秒刷新网格；刷新后依据激活边界和已存在的执行器过滤可创建的档位。

4. 执行与风控逻辑
-----------------
- 创建执行器：对符合激活边界且未占用的最近档位（按与 mid 距离排序）取前 `max_open_orders` 个，创建 PositionExecutor：
  - 入场价：若 BUY 价高于 mid 则用 mid（避免吃单），止盈按 `target_price` 或对称收益率；SELL 同理。
  - 下单类型：开仓 LIMIT_MAKER；止盈 `take_profit_order_type`；可选 trailing_stop（原策略未启用）。
- 停止执行器：
  - 区间 inactive；
  - BUY 入场价已跌破 `mid*(1-activation_bounds)` 或 SELL 入场价高于 `mid*(1+activation_bounds)`（脱离激活范围）。
- 风控扩展（RustCTA 可复用统一风险管理）：
  - 名义/仓位上限、U 型减仓、API 错误熔断。
  - Post Only 被拒时，可外层回退：加大 spread 或暂时放宽 Post Only。

5. 数据与事件
-------------
- 行情：mid 价、行情 tick/tickSize、深度数据用于量化。
- 时间：`grid_range_update_interval` 定期刷新；执行器内部跟踪 time_limit。
- 执行器状态：`is_trading` / `is_active`，用于过滤和停止。
- 指标：输出当前网格档位表（价格、买卖量、目标价、方向、订单类型）。

6. RustCTA 实现要点
-------------------
- 新增模块 `src/strategies/grid_scale`（建议以 trait/struct 组合类似现有做市模块）：
  - `GridScaleConfig` 对应字段，支持从 YAML 反序列化。
  - `GridRange`, `GridLevel` 结构体及生成函数。
  - 核心调度：定时刷新网格 → 根据激活边界/现有订单生成下单计划 → 使用统一下单器（支持 Post Only/GTX）创建/撤销订单。
  - 止盈/超时退出：可复用已有 PositionExecutor/TripleBarrier 概念，或简单以挂反向限价实现。
- 价格/数量量化：复用 `PrecisionService` 或交易规则缓存；保证满足 tick/step/min_notional。
- 风险控制：接入 `UnifiedRiskEvaluator` 或在策略内做名义/订单数硬阈值。
- 日志/监控：输出当前网格档位、活跃执行器数、激活边界，记录被拒原因（Post Only 5022 等）便于调参。

7. 参数建议（初始）
-------------------
- `activation_bounds`: 0.01（±1% 激活），`max_open_orders`: 5。
- `base_spread_between_orders`: 留空则用 tick；若 Post Only 常被拒，可设置 0.0005~0.001 * mid。
- 区间示例：`[40000,60000]` BUY 权重 10%，必要时对称设置 SELL 区间。
- `time_limit`: 1~2 天；`min_order_amount`: 交易所最小名义的 1.2x 作为保守值。

8. 运行与调试
-------------
- 配置文件：`config/grid_scale_<symbol>.yml`。
- 运行示例：`cargo run --release -- --strategy grid_scale --config config/grid_scale_btc.yml`（实现后）。
- 调试要点：
  - 确认网格生成档数 >0；若为 0 调大资金或放宽步长/最小额。
  - 观察被拒订单：Post Only 5022 时拉大步长或暂时关 Post Only。
  - 确认激活边界：若过窄导致全部停摆，适当放宽。

本说明以 PR #7585 的 `grid_scale` 逻辑为基准，覆盖区间化铺单、资金分配、激活边界、执行器创建/停止与风险要点，可直接指导在 RustCTA 中实现等价策略。***

# 趋势自适应做市策略（T-AMM）开发文档

## 1. 策略概述
- **目标**：在提供连续买卖报价的同时，借助趋势与订单流信号动态偏移报价与库存，提升单位风险收益。
- **定位**：保留做市深度供给职责，仅以挂单（maker-only）形式进场；当信号显著时倾向顺势偏仓，信号衰减时回归中性。
- **特性**：
  - 多尺度趋势感知（秒级至分钟级）与成交强度结合，降低噪声引发的误判。
  - 报价引擎支持价差、挂单数量、库存目标三维联动。
  - 风险模型引入库存成本、波动熔断、PnL 回撤及流动性压力测试，确保可控回撤。
- **零手续费适配**：在币安免手续费账户上强化信号守门与排队管理，避免过量无效挂单导致库存偏离。

## 2. 场景与收益来源
- **适用市场**：主流交易所高流动性币对（BTC/ETH、核心稳定币对、部分 L2 资产），具备连续撮合与 post-only 支持。
- **收益结构**：
  1. **点差收益**：通过提报在两侧的 maker 价差赚取挂单 Rebates/无手续费点差。
  2. **趋势额外收益**：在明确方向时，利用偏仓与价差调整获取顺势价差；对冲成本降低。
  3. **库存效率**：库存目标与趋势信号协同，减少频繁对冲造成的手续费及滑点。
- **限制因素**：信号误判、流动性骤降、行情跳空会导致库存压力及未成交订单积压，需依赖风险模型约束。

## 3. 系统结构总览

历史版本的 `trend_adaptive_mm` 模块已归档。后续的专业级趋势做市流程将围绕 `strategies/trend/` 工具箱与 `trend_grid_v2/` 执行引擎重新划分信号、仓位、风控与执行层。

建议的分层：
- `trend/signal_generator`：统一信号调度与指标融合。
- `trend/position_manager`：仓位目标、加减仓节奏。
- `trend/risk_control`：全局与局部风险限额校验。
- `trend_grid_v2` 执行组件：订单规划、撮合与补单逻辑。

### 3.1 现有模块复用与依赖
| 模块 | 现有实现 | 复用方式 |
| --- | --- | --- |
| 账户与资金 | `cta::account_manager` | 调用统一账户/保证金接口，获取币安净头寸与保证金占用 |
| 交易接口 | `exchanges::binance` | 使用 `create_order` 的 `TimeInForce::GTX`/`LIMIT_MAKER` 支持及节流处理 |
| 行情共享 | `strategies::common::shared_data` | 缓存深度、成交、指标结果供信号模块引用 |
| 指标库 | `strategies::mean_reversion::indicators` | 复用 EMA、VWAP、波动率等基础指标实现 |
| 风控工具 | `strategies::common::risk` & `trend::risk_control` | 结合现有风控抽象实现库存/回撤限制 |
| 日志与监控 | `common::application`、`utils::webhook` | 继承统一日志、Webhook 告警与 Prometheus 暴露规范 |

> 后续重构将以 `trend` 工具箱为核心，复用上述依赖，减少重复造轮子，并与既有策略共享任务调度和监控链路。

## 4. 数据与信号体系
### 4.1 数据来源
- **行情数据**：L2 深度增量、逐笔成交（tick）、逐秒 mid-price；保留时间戳、撮合方向。
- **衍生数据**：交易所 Funding、持仓量、跨市场价差（可选）；用于强趋势判定或风险降级。
- **缓存设计**：
  - `SignalContext` 结构体缓存最近 `N` 个 tick、聚合桶（250ms、1s、5s）。
  - 支持滑动窗口聚合 + 异步指标计算，避免阻塞主线程。

### 4.2 单项指标定义
| 指标 | 符号 | 计算方式 | 作用 |
| --- | --- | --- | --- |
| 快速均线斜率 | `EMA^{fast}` | `EMA^{fast}_t = (1-β_f)·EMA^{fast}_{t-1} + β_f·p_t`，斜率 `ΔEMA^{fast} = EMA^{fast}_t - EMA^{fast}_{t-δ}` | 识别瞬时趋势方向与力度 |
| 慢速均线 | `EMA^{slow}` | 同上但 `β_s < β_f` | 作为基准趋势，结合快速线形成 `EMA` 交叉信号 |
| VWAP 偏离 | `ΔVWAP` | `VWAP_t = Σ(p_i·v_i)/Σv_i`（窗口 1-5 分钟）；偏离 `Δ = (p_t - VWAP_t)/VWAP_t` | 检测价格是否显著高于/低于成交平均 |
| 订单流失衡 | `OFI` | `OFI_t = Σ (ΔBidVol_i - ΔAskVol_i)`，按 L2 更新累加 | 表征挂单侧流向 |
| 成交量差 (`CVD`) | `CVD` | `CVD_t = CVD_{t-1} + sign(trade)·vol` | 成交方向累计 |
| 微价格偏移 | `MicroPx` | `Micro_t = (ask·bid_{vol} + bid·ask_{vol})/(bid_{vol}+ask_{vol})`；斜率反映短期压力 | 作为短周期反应加权 |
| 实现波动率 | `σ_real` | `σ_t = sqrt(Σ (r_i - r)^2 / (n-1))`，r_i 为对数收益 | 风险过滤，调节信号置信度 |

### 4.3 信号归一化
- **尺度对齐**：对每个指标使用滑动均值、标准差或分位数做 Z-Score/Min-Max 变换，得到 `s_k ∈ [-1, 1]`。
- **噪声抑制**：设置最小显著阈值 `θ_k`，若 `|s_k| < θ_k` 则置零；对 `OFI`、`CVD` 使用指数衰减平滑 `s'_k = λ·s_k + (1-λ)·s'_{k-1}`。
- **波动调节**：当 `σ_real` 超过上限 `σ_hi` 时整体置信度折扣 `w_vol = min(1, σ_ref/σ_real)`；当低于 `σ_lo` 时提高权重，避免报价过宽。

### 4.4 信号融合
- **加权线性模型**：`score_raw = Σ w_k · s'_k`；权重通过历史回测（Sharpe 最大化或 Information Ratio）求解，可按日/周再训练。
- **自适应权重**：引入协方差矩阵 `Σ_s`，将优化写成 `max w^T μ_s - λ_r w^T Σ_s w`；提供风控参数 `λ_r` 控制权重波动。
- **置信度压缩**：使用双曲正切压缩 `α = tanh(score_raw)`，保障输出落于 `(-1,1)`，便于在报价模块中直接映射。
- **多周期合成**：
  - `α_short`（30s-1min）、`α_mid`（1-5min）、`α_long`（5-15min）使用独立指标集合。
  - 最终得分 `α* = η_s α_short + η_m α_mid + η_l α_long`，其中权重依赖实时波动与成交频率动态调整。
- **状态机**：定义 `Regime ∈ {Neutral, TrendUp, TrendDown, VolShock}`，依据 `α*` 阈值与 `σ_real` 判定；不同状态对应各自的点差和库存策略。

### 4.5 伪代码示例
```
fn compute_alpha(ctx: &SignalContext) -> TrendScore {
    let indicators = Indicators::from(ctx);
    let normalized = indicators.normalize();
    let score_raw = weights.dot(normalized.vector());
    let alpha = (score_raw).tanh();
    let regime = classify(alpha, normalized.volatility);
    TrendScore { alpha, regime, components: normalized }
}
```

### 4.6 信号守门与一致性校验
- **置信度阈值**：设定 `|α_short|`, `|α_mid|` 均需超过 `signal_floor` 才允许触发趋势偏移；若任一低于阈值，策略退回对称做市。
- **多周期一致性**：当 `sign(α_short) == sign(α_mid)` 且 `α_long` 未出现反向信号时，才允许扩大库存偏移；否则收敛至中性。
- **成交密度过滤**：统计最近 `K` 个成交的`vol_avg`，若低于 `liquidity_floor` 则将 `OFI`、`CVD` 权重降至 0，避免低流动性噪声。
- **信号冷却**：对频繁翻转的信号施加冷却时间 `cooldown_ms`，期间保持中性报价，降低过度调价对排队顺位的影响。

## 5. 报价与库存策略
- **基准价**：`mid = (best_bid + best_ask)/2`。
- **报价偏移**：
  - 买侧价差 `δ_bid = base_spread · f_vol(σ_real) · (1 - κ · α*)`。
  - 卖侧价差 `δ_ask = base_spread · f_vol(σ_real) · (1 + κ · α*)`。
  - 当 `α* > 0`（上涨趋势）时，买价靠近 mid，卖价外移；`α* < 0` 反之。
- **挂单量**：`q_side = q_base · g_inventory(I, α*, regime)`，对顺势侧提升挂单量，逆势侧收缩。
- **库存目标**：
  - 目标库存 `I_target = clamp(ρ · α*, -I_bias_max, I_bias_max)`。
  - 现货库存 `I` 偏离目标时，调节补单优先级；若超过硬限 `I_hard`，触发风险降级。
- **maker-only 执行**：下单接口强制 `post_only=true`，若交易所提示将吃单则自动调价 `±ε` 重新提交。
- **排队管理**：实时估计挂单排队位置 `queue_rank`，若超过 `queue_threshold` 或 `time_in_queue > queue_ttl`，自动撤单重挂以抢占更优顺位。
- **零手续费策略**：在免手续费环境下，适度提高挂单密度，但限制库存偏仓增长速度 `bias_step_max`，防止大量无效成交导致仓位累积。

## 6. 风险模型
### 6.1 风险框架
- **核心变量**：实时库存 `I_t`、标的价格 `S_t`、未实现 PnL `UPnL_t`、已实现 PnL `RPnL_t`、回撤 `DD_t`、波动率 `σ_real`、成交延迟 `τ_fill`。
- **风险源分类**：库存风险、价格跳变、流动性冻结、信号失效、技术故障。

### 6.2 库存风险模型
- **成本函数**：`C(I_t, α*) = λ · I_t^2 - γ · α* · I_t`，其中 `λ > 0` 控制库存惩罚，`γ` 体现趋势带来的容忍度。
- **动态回归速度**：根据 `α*` 调节库存回归系数 `k_revert`：
  - `k_revert = k_base · (1 - |α*|)`，信号弱时加速回到 0；信号强时放缓。
  - 引入渐进式回归：当 `|I_t|` 接近 `I_soft` 时启用线性回归，当触及 `I_hard` 前加速为指数回归，并允许触发 `taker hedge` 清仓。
- **硬限额**：`|I_t| ≥ I_hard` 时：
  1. 立即撤销与库存方向相同的挂单。
  2. 启动对冲任务（允许 taker）或进入保护模式（只挂与库存相反方向）。
- **库存 CVaR**：定期计算 `CVaR_{95%}` 基于历史或模拟路径，调整 `λ`、`I_hard`。

### 6.3 价格与波动风险
- **波动熔断**：当 `σ_real > σ_cut` 或 `|return_{1m}| > r_cut`：
  - 执行报价降级：点差扩大 `× spread_widen_factor`，挂单量减半。
  - 若持续 `N` 个窗口超标，暂停策略并发出报警。
- **跳空检测**：利用盘口缺口 `gap = best_ask_{t-1} - best_bid_t`；若 `gap` 超阈值，撤单并重新定位价差。
- **滑点监控**：记录所有成交的 `fill_price - quote_price`，当平均滑点超阈值 `slip_cut` 时收紧库存偏移。

### 6.4 PnL 与回撤控制
- **实时监控**：
  - `PNL_t = RPnL_t + UPnL_t`
  - `DD_t = max(DD_{t-1}, peak_PNL - PNL_t)`
- **回撤阈值**：
  - `DD_soft`：进入保护模式（只挂较宽点差、禁止增量偏仓）。
  - `DD_hard`：停止策略，保留风控任务直至人工复核。
- **情景压力**：每 `Δt` 执行基于最新库存的价格冲击测试：`stress_pnl = Σ I_level · ΔS_scenario`，并与 `PnL_buffer` 比较决定是否降级。

### 6.7 PnL 归因与信号回退
- **收益拆分**：将 PnL 拆解为 `spread_pnl`、`trend_bias_pnl`、`inventory_cost` 与 `hedge_cost`，每日输出报表。
- **自适应权重**：若 `trend_bias_pnl` 连续 `N` 日为负，而 `spread_pnl` 正常，则降低趋势权重 `κ` 与库存偏置上限。
- **自动回退**：满足亏损条件或信号显著弱化时，策略自动切换至基准 AS 做市配置，待人工确认后再启用 T-AMM 模块。

### 6.5 流动性与订单风险
- **挂单寿命监控**：`T_alive` 超过阈值触发撤单重挂，防止过时报价被动成交。
- **市场深度依赖**：测量自身挂单占盘口比例 `ratio = q_ours / depth@level`; 若 `ratio > ratio_max`，自动稀释挂单量。
- **外部信号失效**：使用心跳检测外部数据源（行情、成交、Funding）；若丢失超过 `heartbeat_max_gap`，策略暂挂。

### 6.6 风险决策伪代码
```
fn risk_guard(state: &StrategyState, score: &TrendScore) -> RiskAction {
    if state.drawdown > limits.dd_hard { return RiskAction::Stop; }
    if state.volatility > limits.sigma_cut { return RiskAction::Downgrade; }
    if state.inventory.abs() > limits.inventory_hard { return RiskAction::Hedge; }
    if score.alpha.abs() < limits.signal_floor { return RiskAction::Neutralize; }
    RiskAction::Proceed
}
```

## 7. maker-only 执行策略
- **委托接口**：封装 `OrderRequest::new_maker(symbol, side, price, qty)`，自动附带 `post_only`；若交易所返回 `PostOnlyRejected`，价格调整 `±tick` 后重试。
- **币安实现细节**：
  - 现货与 U 本位合约采用 `type=LIMIT_MAKER`（或 `timeInForce=GTX`）确保只作为挂单。
  - 校验 `LOT_SIZE`、`PRICE_FILTER`、`MIN_NOTIONAL` 规则，在调价时对齐 tick 与最小成交额。
  - 对于合约端可选 `reduceOnly=false` 保持双向持仓，必要时在风险模块触发 `reduceOnly` 对冲。
- **撤单策略**：
  - 行情侧：当 mid 与委托差小于 `cancel_threshold` 时撤单防止被动成交。
  - 风控侧：风险模块一旦发出 `RiskAction::Stop/Downgrade`，立即批量撤单。
- **订单追踪**：维护 `OrderBookState`，定时校正外部成交回报与内部状态；任何不一致进入补偿逻辑。

## 8. 配置与参数管理
- 采用 `config/market_maker_trend.yaml` 管理：
  - `signals`: 指标窗口、阈值、权重、训练周期。
  - `quoting`: 基础点差、增益系数 `κ`、挂单量基准 `q_base`。
  - `risk`: 库存限额、波动熔断阈值、PnL 回撤阈值、滑点限制。
  - `maker_only`: 价格偏移 `ε`、最大重试次数、订单寿命。
- 支持 Redis/ETCD 热更新，策略实例监听配置版本号并应用差分。

## 9. 回测与仿真验证
- **数据回放**：使用逐笔或 Level 2 重构撮合，复现真实挂单排队；评估 fill ratio、撤单率、滑点分布。
- **对照试验**：
  1. 基准 AS 对称策略。
  2. 仅信号偏移（不改库存）。
  3. 完整趋势做市策略。
  - 指标：日度 PnL、Sharpe、最大回撤、库存标准差、订单成功率。
- **敏感度分析**：测试不同信号窗口、权重、熔断参数对绩效影响；记录最优组合。
- **压力场景**：插入极端行情（闪崩、交易所熔断、信号缺失），确认风险模块反应。

## 9.1 零手续费场景专项测试
- **高密度挂单仿真**：模拟零费环境下的深度拥挤，验证排队管理逻辑是否有效提高成交效率。
- **库存漂移测试**：构建长时间横盘数据，观察无成交时库存是否保持在目标带内；必要时调高回归速度。
- **返佣核算**：对比有无返佣情境下的净收益，将返佣收益单独记录以评估策略依赖度。

## 10. 上线与运维
- **监控**：Prometheus/Grafana 指标包括 `alpha`, `inventory`, `spread_bias`, `risk_state`, `cancel_rate`, `fill_slippage`。
- **告警**：钉钉/企业微信推送回撤、库存、信号缺失、订单异常；区分告警级别（Info/Warning/Critical）。
- **降级策略**：当风险模块进入保护模式时，策略自动切换到对称做市或暂停；支持人工一键恢复。
- **日志**：结构化 JSON 日志记录信号组件贡献、风险决策、订单动作；敏感字段（API Key、账户）脱敏。
- **队列与信号监控**：新增 `queue_rank`, `time_in_queue`, `signal_flip_rate` 指标，确保排队与信号守门在免手续费环境下稳定工作。

## 11. 双向 vs 单向持仓评估
- **推荐方案**：默认双向做市，允许依据 `α*` 在 `[-I_bias_max, I_bias_max]` 范围偏仓；确保库存回归机制到位。
- **单向模式适用**：仅在有外部对冲仓位或需要承接大额客户方向时作为特例；需额外设置资金占用与风险限额。
- **风险考量**：单向持仓使策略更接近趋势交易，流动性指标下降；缺乏对冲能力时实盘风险显著增大。

## 12. 后续迭代方向
- **信号优化**：引入深度学习短序列模型（TCN/LSTM）预测 `α*`，或使用贝叶斯更新权重。
- **多市场协同**：对接跨市场价差/订单流，利用外盘提示强化信号。
- **风险模型深化**：接入实时保证金占用、跨资产相关性，构建组合级 VaR、ES 指标。
- **自动参数调优**：上线 Bayesian Optimization/遗传算法对信号窗口、权重、库存阈值做自适应调参。

## 13. 币安双向持仓部署要点
- **双向仓位策略**：
  - 默认启用双向持仓模式，保持 `hedgeMode=false`（现货）或合约逐仓/全仓双向持仓；库存目标 `I_target` 允许正负偏离，风险模块负责回归。
  - 使用 `positionSide=BOTH`（币安合约）避免多空拆分账户，简化库存管理。
- **账户与限额**：
  - 监控币安账户的 `notional`、保证金率与 `ADL` 阈值，必要时联动风险模块执行仓位压缩。
  - 记录交易手续费等级与返佣，评估 maker 返佣对策略点差的贡献。
- **接口特性**：
  - 使用 WebSocket 深度与逐笔流（`@depth@100ms`, `@trade`）为信号模块提供高频输入。
  - REST 下单需处理 `429` 与 `418` 节流，策略内实现重试与退避；长时间限速触发告警降级。
- **风控协同**：
  - 当库存触及硬限额时，优先通过币安反向挂单回归；若市场流动性不足，调用对冲模块执行 `reduceOnly` taker 平仓。
  - 将币安的 `MARK_PRICE` 与 `FUNDING_RATE` 纳入信号/风险输入，避免在资金费率极端时保持同向偏仓。

## 14. 实施前必备内容
- **配置模板**：在 `config/market_maker_trend.yaml` 中预置币安参数、零手续费开关、信号阈值；添加 default/production profile。
- **数据接入**：确认行情流（深度、逐笔）、账户、订单回报在现有 `exchanges::binance` 中均已启用，并编写集成测试验证。
- **回测资源**：准备至少 30 天的逐笔与 Level2 数据，用于信号权重训练与压力测试；补充脚本至 `scripts/` 目录。
- **监控面板**：在 `docs/` 或 `config/` 中新增 Grafana Dashboard 模板，覆盖新增指标（alpha、queue、PnL attribution）。
- **测试计划**：撰写单元与集成测试待办，包含信号守门、库存回归、风险熔断、maker-only 失败重试等用例。
- **部署清单**：列出上线前检查项（API 权限、风控参数、限速监控、告警 webhook），确保策略启用流程完整。

# 日内多时间框架趋势策略开发计划（基于 `strategies/trend/` 模块）

## 1. 背景与目标
- **目标**：在现有 `strategies/trend/` 工具箱基础上，实现一个覆盖 30+ 币安永续合约交易对的日内趋势策略，完成“信号 → 仓位 → 风控 → 执行 → 监控”的全链路闭环，并支持多空双向交易。
- **范围**：
  - 仅使用币安永续市场官方 REST/WS 数据源。
  - 策略参数在启动前通过配置文件加载，运行期间不做热更新。
  - 上线阶段直接使用极小名义本金进行实盘验证。
  - 不实现主备冗余，仅关注单进程稳定性。
- **高层特性**：
  - 多时间框架指标评分，结合行情状态（趋势/震荡/极端波动）。
  - 置信度与风险调节因子驱动目标仓位。
  - 日内风险曲线、组合风险管理与执行质量反馈。
  - 自研执行引擎（限价优先、队列优先级估算、失败重试、滑点统计），默认优先使用免手续费的 maker 挂单通道，并在必要时自动切换至 taker。

## 2. 可复用模块与调整方向

| 模块 | 位置 | 现有能力 | 调整/增强点 |
| ---- | ---- | -------- | ----------- |
| `config.rs` | `src/strategies/trend/config.rs` | 定义风险、指标、信号、仓位、止损等配置结构 | 扩展评分权重、行情状态、日内风险曲线、交易对标签等字段；保持兼容原字段 |
| `trend_analyzer.rs` | 同上 | 多时间框架趋势分析，输出 `TrendSignal` | 增强为多时间框架评分引擎，输出 `TrendScore`（含 raw/adjusted score、置信度、状态标签、风险修正因子），并确保做多/做空方向判断准确 |
| `signal_generator.rs` | 同上 | 基于 `TrendSignal` 生成 `TradeSignal`，包含形态检测、成交量确认等 | 接入新的评分/置信度分段逻辑、趋势状态过滤；复用 Pattern/Level 检测但调整为分档开仓条件，兼容多空信号 |
| `position_manager.rs` | 同上 | 管理 `TrendPosition`，现阶段 `calculate_position_size` 返回固定测试仓位 | 替换为评分+波动+日内曲线+风险预算的目标仓位公式；实现相关性约束、执行质量调节，并针对多空分别统计净/绝对风险 |
| `risk_control.rs` | 同上 | 四层风控（系统/账户/策略/订单），具备事件记录、指标收集 | 接入组合 VaR、回撤分层、资金费率异常、panic_exit 等逻辑；复用现有结构体 |
| `stop_manager.rs` | 同上 | ATR/保本/追踪止损、时间止损 | 结合评分降级与新风险指标优化止盈/止损触发 |
| `monitoring.rs` | 同上 | 维护 `TrendMonitor`、性能指标、交易日志 | 扩展健康度评分、状态分层收益、信号投产率、执行质量统计 |
| `strategies::common::shared_data` | `src/strategies/common/shared_data.rs` | 提供 `SharedSymbolData`，包含 `SymbolSnapshot`、指标输出 | 扩展共享指标字段（如资金费率、订单流指标），或引入新的结构存储日内特有数据 |
| `utils::indicators` 与 `mean_reversion::indicators` | `src/utils/indicators.rs` 等 | 已实现 EMA、MACD、ATR、RSI 等常用指标 | 复用基础指标函数，并在 `signal_generator`/`trend_analyzer` 中组合 |
| `cta::AccountManager` | `src/cta/account_manager.rs` | 已支持账户/子账户管理、下单接口 | 直接复用执行与账户信息接口，执行层基于此下单、撤单 |

> 说明：旧版 `TrendFollowingStrategy` 已删除，需要在本计划中补齐新的策略编排（见第 9 节）。

## 3. 数据与指标流水线

### 3.1 数据源接入
- 使用币安永续 WebSocket 获取逐笔成交、L2 深度（合并 20 档）、实时资金费率/未平仓量推送（若有）。
- 使用 REST 周期性补全：1s/15s/1m/5m/15m/1h K 线、资金费率历史与预测、未平仓量、成交额、指数价格。
- 数据流落地到 `signal_generator` 内的统一数据管线，缓存最近 N 秒 Tick 及多周期聚合结果。
- 延迟目标：行情到达 → 指标更新 < 80ms；延迟超过 200ms 时标记数据质量异常。

### 3.2 指标服务改造（`signal_generator.rs`）
- 复用现有 `SignalGenerator` 结构，拆分出 `IndicatorService`（可放置在同文件或新增子模块），职责：
  - 结合 `SharedSymbolData`、实时行情计算以下指标：EMA 斜率、MACD、SuperTrend、RSI、Stoch RSI、布林带带宽、CVD、OFI、盘口倾斜、撤单率、ATR、realized variance、跳空检测、资金费率偏离、未平仓量变化。
  - 输出统一的 `IndicatorSnapshot` 给评分引擎与风险层。
- 扩展 `SharedSymbolData` 或新增扩展结构，使 `TrendAnalyzer` 与 `StopManager` 能读取最新价格、波动、成交量等信息。
- 引入数据质量评分 `data_quality_score`，根据延迟、缺失指标数量、异常值判断，可在评分中用作置信度的一部分。

### 3.3 行情状态分类
- 在 `trend` 模块新增 `regime_classifier.rs`（或在 `trend_analyzer.rs` 内实现），使用轻量 HMM/聚类对每个交易对实时判别：
  - 状态枚举：`Trending`, `RangeBound`, `HighVolatility`。
  - 输入特征：价格斜率、波动率、成交量变化、订单流指标。
  - 状态结果写入 `TrendScore.regime`，并提供权重映射（默认趋势 ×1.0，震荡 ×0.6，极端波动 ×0.4）。

## 4. 多时间框架评分体系（`trend_analyzer.rs`）
- 扩展/重构 `TrendAnalyzer::analyze`：
  - 读取 `IndicatorSnapshot`、状态标签、数据质量指标。
  - 计算短期 (0–15 分钟)、中期 (15–60 分钟)、长期 (1–2 小时) 三档评分，默认权重 0.55 / 0.30 / 0.15。
  - 合成 `TrendScore`（可在文件顶部定义新的结构体）：
    ```rust
    pub struct TrendScore {
        pub raw_score: f64,
        pub adjusted_score: f64,
        pub confidence: f64,
        pub direction: TrendDirection,
        pub regime: MarketRegime,
        pub modifiers: ScoreModifiers,
        pub timestamp: DateTime<Utc>,
    }
    ```
  - `confidence` 由指标一致性、数据质量、执行反馈组成。
  - `modifiers` 包含资金费率、波动率、组合回撤、执行质量等风险调节因子。
- 向下兼容：保留 `TrendSignal` 供旧逻辑使用，或直接将 `TrendScore` 嵌入 `TrendSignal`（新增字段）。
- 在 `TrendConfig` 中增加 `ScoringWeights`、`RiskModifiersConfig` 等子配置，以便在 YAML 中控制权重与阈值。

## 5. 信号生成与评分联动（`signal_generator.rs`）
- 利用增强后的 `TrendScore`：
  - 评分 ≥ 85：强趋势；允许突破/加仓信号，设置最激进的止盈/止损参数。
  - 75–85：顺势；触发常规模块（突破、回调、动量）。
  - 60–75：轻仓观望；仅在指标一致性高时保留或轻仓。
  - 40–60：观望，信号直接过滤。
  - 25–40：偏空反手；考虑对冲或轻仓空。
  - < 25：强空；允许做空信号。
- 信号结构扩展：
  - 在 `TradeSignal.metadata` 中加入 `adjusted_score`, `confidence`, `regime`, `risk_adjustments`。
  - 置信度低于阈值（默认 0.5）时返回 `None`。
- 保留现有形态检测 (`PatternDetector`) 与关键位分析 (`LevelAnalyzer`)，将其作为评分通过时的二次确认。

## 6. 仓位与资金管理（`position_manager.rs`）
- 现有 `calculate_position_size` 使用固定仓位，需要根据评分与风险重新实现：
  - 公式示例：
    ```
    base = config.base_risk_ratio * account_equity
    score_factor = map_score(adjusted_score, confidence)
    vol_factor = atr_scaler(current_atr, atr_baseline)
    time_factor = daypart_curve(current_time)  // 高峰=1.0，常规=0.7，低流动=0.4
    exec_factor = execution_quality_factor(symbol)
    target_position = base * score_factor * vol_factor * time_factor * exec_factor
    ```
  - 将结果限制在 `max_single_exposure` 与组合暴露上限内。
  - 根据评分区间实现分批加减仓（每升一档加仓 25%，降一档减仓 30%，降两档平仓）。
- 维护相关性矩阵（可储存在 `PositionManager` 的 `correlation_matrix` 字段），按照配置定期更新，限制高相关资产的合计仓位。
- 为 `TrendPosition` 增加执行质量字段（如 `avg_slippage`、`fill_ratio`）以便风控；在部分止盈或浮盈达到阈值时，触发自动上调止损/止盈价至合理保护位以锁定利润。

## 7. 风控与止损层（`risk_control.rs` & `stop_manager.rs`）
- **风险控制器**：
  - 接入组合 VaR/CVaR 计算（可在 `StrategyMetrics` 中新增字段），结合 `RiskConfig` 阈值做动作决策。
  - 实现日内回撤分层：回撤 ≥4% → 轻仓；≥6% → 仅保留高评分；≥8% → 调用 panic_exit。
  - 监控资金费率、盘口深度、API 状态，打通至 `RiskEvent` 记录。
  - 添加执行失败统计，每触发一次记录到 `OrderMetrics.failed_orders`，超过阈值触发降级。
- **止损管理**：
  - 使用评分降级、执行质量、波动收缩作为追踪止损/保本止损触发条件；在部分止盈或浮盈显著时，将止损/止盈价格上移（下移）至关键结构或成本价以上，实现利润锁定。
  - 补充在 `StopUpdate` 中返回风险原因。

## 8. 执行引擎（新增 `src/strategies/trend/execution_engine.rs`）
- 设计一个面向 `AccountManager` 的执行器：
  - 接收目标仓位、期望成交价格区间、风险动作（如主动撤单）。
  - 下单策略：限价 → 冰山 → 隐形限价 → 市价兜底（可配置）。
  - 队列优先级估算：根据盘口涨跌、成交量推断当前订单排队可能性，决定是否调整价格。
  - 失败重试：指数退避 100ms → 200ms → 400ms → 1s（可配置），超过上限后返回错误并触发风控。
  - 滑点统计、成交率、均价与延迟写入执行指标，供评分与风险模块使用。
- 输出接口：
  ```rust
  pub struct ExecutionFeedback {
      pub avg_price: f64,
      pub filled_qty: f64,
      pub slippage: f64,
      pub fill_ratio: f64,
      pub latency_ms: u64,
  }
  ```
- 将该执行器封装在策略 orchestrator 中，使 `PositionManager` 调用时能拿到反馈。

## 9. 策略编排与集成
- 新建 `src/strategies/trend/strategy.rs`（或 `controller.rs`），负责：
  - 初始化各子模块（Analyzer、SignalGenerator、PositionManager、RiskController、StopManager、ExecutionEngine、TrendMonitor）。
  - 轮询交易对：读取共享指标 → 分析评分 → 风控校验 → 生成信号 → 计算目标仓位 → 执行订单 → 更新仓位与监控。
  - 维护策略生命周期方法：`start`、`stop`、`status`。
- 在 `src/strategies/mod.rs` 中导出新策略类型，如 `TrendIntradayStrategy`。
- 在 `src/main.rs` CLI 中增加 `trend_intraday` 分支，读取配置文件并实例化策略。
- 若需要加入 common 注册表（`strategies/common/application/registry.rs`），可提供工厂函数以兼容统一构建流程。

## 10. 配置文件调整（`config.rs` 与 YAML）
- 在 `TrendConfig` 中新增/调整：
  - `scoring`：短/中/长期权重、阈值、置信度阈值、执行质量调节参数。
  - `regime`：状态分类阈值、权重映射。
  - `time_curve`：日内时间段定义与系数。
  - `risk_budget`：组合/单品种风险、VaR/CVaR 阈值、回撤阈值、panic_exit 设置。
  - `execution`：限价距离、冰山参数、重试次数/间隔、滑点告警阈值。
  - `monitoring`：健康度指标权重、告警阈值、按天滚动日志文件（文件名包含日期或使用轮转策略）。
- 提供示例配置文件 `config/trend_intraday.yml`，包含多个交易对的样例参数与日内曲线。

## 11. 监控与报表（`monitoring.rs`）
- 扩展 `PerformanceMetrics` 和 `TrendMonitor`：
  - 增加健康度评分 `health_score`，基于数据质量、信号一致性、风险利用率、执行质量计算。
  - 记录状态分层收益、信号投产率、执行成功率，支持在日报输出。
  - 告警等级：`Info`（提示）、`Warning`（需人工确认）、`Critical`（自动动作）。
  - `Critical` 告警触发 panic_exit 并记录至审计日志。
- 输出渠道：日志、Webhook（如需要）、本地报表文件（可复用现有异步写入逻辑）；日志按日切分保存，确保每天生成独立文件或目录。

## 12. 测试与上线流程
- **单元测试**：指标计算、评分函数、仓位公式、风控动作、执行重试。
- **集成测试**：使用录制的行情或实时沙盒，验证指标更新、评分输出、风险触发、执行反馈；任何新增指标须先在样本数据上完成统计分布与回测验证，并记录测试结论后方可入库。
- **实盘小额测试**：
  - 选择 5–10 个交易对，以最小下单规模运行 1–2 周。
  - 监控指标：延迟、执行滑点、告警触发、PnL 曲线、仓位控制。
  - 根据日报/周报调整阈值与参数后扩展交易对数量。
- **扩容**：实盘稳定后逐步增加至 30+ 交易对，并扩大单笔仓位。

## 13. 开发里程碑（建议）
1. **M1：结构梳理** – 复查现有模块、完成设计细化、补充配置定义。
2. **M2：指标 & 评分层** – 完成数据采集、指标服务、状态分类、评分输出。
3. **M3：仓位 & 风控层** – 实现目标仓位公式、风险控制逻辑、止损策略。
4. **M4：执行 & 监控层** – 构建执行引擎、滑点反馈、监控与告警。
5. **M5：策略编排 & 集成测试** – 搭建策略 orchestrator，打通全链路，完成小额实盘上线。

## 14. 交付物
- 更新后的 `strategies/trend/` 源码（含新增 `execution_engine.rs`、`strategy.rs`、`regime_classifier.rs` 等文件）。
- 新的配置文件范例与参数说明。
- 更新 `README` 或单独文档，说明策略运行、配置、告警处理方法。
- 实盘小额测试日志与复盘报告（可选，但建议保留）。

---

> 阅读本开发计划后，可按照章节实施开发。每个模块的任务均指向现有代码位置，优先复用并增强已有能力，减少重复造轮子。开发过程中的任何新增文件均应保留在 `strategies/trend/` 命名空间内，确保策略结构清晰、可维护。

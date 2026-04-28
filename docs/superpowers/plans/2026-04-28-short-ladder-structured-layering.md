# 短梯结构化补仓实现计划

> 本文档记录短梯做空策略从固定 ATR 补仓升级到结构化补仓的实现计划。当前实盘推荐版本已经进一步收敛为 L1 单层策略，本文作为历史设计记录保留。

## 目标

在短梯做空回测模块中增加可复用的结构化补仓模式，使 L2/L3/L4 只有在同时满足 ATR 间距、近期压力位和最小浮亏阈值时才允许加仓。

## 架构思路

- 在 `ShortLadderConfig` 中增加 `LayerPriceMode` 枚举。
- 默认保留旧的 ATR 固定间距补仓，保证历史行为兼容。
- 新增结构化补仓参数：压力位回看窗口、ATR 缓冲、每层最小浮亏 bps。
- 在网格回测中同时输出补仓模式和结构参数，便于横向比较。

## 技术栈

- Rust 2021。
- `serde` 配置解析。
- 既有短梯做空回测执行器。
- 回测结果输出到 `logs/` 目录下的 CSV 和 Markdown 报告。

## 任务 1：增加回归测试

涉及文件：`src/backtest/strategy/short_ladder.rs`。

- 增加测试：当价格触及 ATR 层级但浮亏未达到当前层阈值时，不允许补仓。
- 增加测试：当近期压力位加缓冲高于 ATR 间距价格时，使用压力位触发价。
- 运行 `cargo test backtest::strategy::short_ladder --all-features`，确认新测试覆盖结构化补仓行为。

## 任务 2：实现结构化补仓定价

涉及文件：`src/backtest/strategy/short_ladder.rs`。

- 增加 `LayerPriceMode::AtrSpacing` 和 `LayerPriceMode::AtrResistanceLoss`。
- 增加配置字段：`layer_price_mode`、`layer_resistance_lookback_bars`、`layer_resistance_buffer_atr`、`layer_min_loss_bps`。
- 增加浮亏 bps 计算函数。
- 增加下一层空单触发价计算函数。
- 更新旧 5m 执行路径和 MTF 1m 执行路径，统一使用新触发价函数。
- 默认模式保持旧 ATR 间距行为不变。

## 任务 3：扩展网格回测工具

涉及文件：`src/bin/short_ladder_mtf_grid.rs`。

- 增加补仓模式参数。
- 增加结构化补仓参数组合。
- 在 CSV 中输出补仓模式、压力位回看、ATR 缓冲和最小浮亏阈值。
- 在报告文件名和标签中加入结构化补仓参数，便于追踪结果。
- 支持只输出汇总，避免大规模 JSON 报告占用磁盘。

## 任务 4：验证与回测

输出目录：`logs/short_ladder_structured_layering_*`。

- 运行 `cargo fmt`。
- 运行 `cargo check --bin short_ladder_mtf_grid --all-features`。
- 运行 `cargo test backtest::strategy::short_ladder --all-features`。
- 对比旧 ATR 补仓和结构化补仓。
- 对比固定止盈与 ATR 移动止盈。
- 保存推荐参数和风险说明。

## 后续结论

结构化补仓可以明显降低快速打满 L4 的风险，但仍然保留补仓带来的尾部风险。结合实盘小账户约束，当前推荐版本已改为 `layer_weights = [1.0]` 的 L1 单层做空策略，使用 ATR 移动止盈和明确 ATR 止损控制风险。

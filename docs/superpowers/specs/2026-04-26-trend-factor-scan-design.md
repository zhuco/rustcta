# 趋势因子组合扫描框架设计

## 目标
构建长期可复用的 Rust 指标组合研究框架，用于在现有 backtest 数据集上扫描趋势因子组合、网格化参数，并输出可解释且更稳健的候选参数。

## 范围
第一版实现固定规则枚举和 YAML 参数网格，不实现自由表达式语言、机器学习优化或完整订单簿撮合。执行模型沿用当前 scan-trend 的简化 K 线内止盈/止损模型。

## 架构
- `src/backtest/indicators/`：指标序列计算，输入 `&[Kline]`，输出 `Vec<Option<f64>>` 或布尔序列。
- `src/backtest/factors/`：趋势因子规则和配置，包括入场规则、过滤规则、出场规则、参数展开。
- `src/backtest/scoring/`：综合评分与风险标签，不只按 ROI 排序。
- `src/backtest/strategy/trend_factor.rs`：基于规则组合生成交易与汇总。
- `scan-trend-factor` CLI：读取回测数据集和 YAML 配置，执行多组合扫描并输出 JSON 报告。

## 第一版指标
EMA、RSI、ATR、ADX、MACD histogram、Donchian channel、Bollinger Band Width、SuperTrend、Volume Ratio、ROC。

## 第一版规则
- 入场：EMA 趋势、Donchian 突破、SuperTrend 方向、MACD histogram 方向、ROC 方向。
- 过滤：ADX 下限、RSI 区间、BB Width 下限、Volume Ratio 下限、ATR 百分比区间。
- 出场：ATR 止损、R 倍数止盈、反向信号退出、时间止损。

## 有用性判定
默认按综合分排序：收益、回撤、盈亏因子、交易数、收益回撤比、低交易数惩罚和参数稳定性风险标签。ROI 第一不直接等于最佳实盘参数。

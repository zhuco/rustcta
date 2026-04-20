pub mod application;
pub mod domain;
pub mod grid;
pub mod infrastructure;
pub mod risk;
pub mod shared_data;
pub mod snapshot;

pub use application::{
    RiskLevel, Strategy, StrategyContext, StrategyDeps, StrategyDepsBuilder, StrategyInstance,
    StrategyPosition, StrategyState, StrategyStatus,
};
pub use infrastructure::{OrderExecutionEvent, OrderExecutor};
pub use risk::{
    build_unified_risk_evaluator, RiskAction, RiskDecision, RiskNotifyLevel, UnifiedRiskEvaluator,
};
pub use shared_data::{
    get_trend_inputs, list_shared_symbols, publish_trend_inputs,
    publish_trend_inputs_with_timestamp, remove_trend_inputs, MarketRegime, SharedSymbolData,
    TrendFibAnchors, TrendIndicatorSnapshot,
};
pub use snapshot::{
    StrategyExposureSnapshot, StrategyPerformanceSnapshot, StrategyRiskLimits, StrategySnapshot,
};

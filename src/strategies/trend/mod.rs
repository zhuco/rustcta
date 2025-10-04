//! 趋势跟踪策略模块

pub mod config;
pub mod monitoring;
pub mod position_manager;
pub mod risk_control;
pub mod signal_generator;
pub mod stop_manager;
pub mod trend_analyzer;

pub use config::TrendConfig;
pub use monitoring::{PerformanceMetrics, TrendMonitor};
pub use position_manager::{PositionManager, TrendPosition};
pub use risk_control::{RiskController, RiskLevel};
pub use signal_generator::{SignalGenerator, TradeSignal};
pub use stop_manager::{StopManager, StopUpdate};
pub use trend_analyzer::{TrendAnalyzer, TrendSignal};

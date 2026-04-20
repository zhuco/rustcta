//! 趋势跟踪策略模块

pub mod config;
pub mod entry;
pub mod execution_engine;
pub mod indicator_service;
pub mod market_feed;
pub mod monitoring;
pub mod order_tracker;
pub mod position_manager;
pub mod risk_control;
pub mod signal_generator;
pub mod stop_manager;
pub mod strategy;
pub mod trend_analyzer;
pub mod user_stream;

pub use config::TrendConfig;
pub use entry::{CapitalAllocator, EntryMode, EntryOrderPlan, EntryPlanner};
pub use execution_engine::{ExecutionReport, SymbolPrecision, TrendExecutionEngine};
pub use indicator_service::TrendIndicatorService;
pub use market_feed::{CandleCache, CandleEvent, CandleInterval, CandleSnapshot, TrendMarketFeed};
pub use monitoring::{PerformanceMetrics, TrendMonitor};
pub use order_tracker::{FillEvent, OrderTracker};
pub use position_manager::{PositionManager, TrendPosition};
pub use risk_control::{RiskController, RiskLevel};
pub use signal_generator::{SignalGenerator, TradeSignal};
pub use stop_manager::{StopManager, StopUpdate};
pub use strategy::TrendIntradayStrategy;
pub use trend_analyzer::{TrendAnalyzer, TrendSignal};

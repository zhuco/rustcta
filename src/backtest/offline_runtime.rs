mod deps {
    pub use crate::backtest::data::depth_dataset::DepthDatasetReader;
    #[cfg(test)]
    pub use crate::backtest::data::depth_dataset::DepthDatasetWriter;
    #[cfg(test)]
    pub use crate::backtest::data::kline_dataset::KlineDatasetWriter;
    pub use crate::backtest::data::trade_dataset::TradeDatasetReader;
    #[cfg(test)]
    pub use crate::backtest::data::trade_dataset::TradeDatasetWriter;
    pub use crate::backtest::replay::{DepthReplay, KlineReplay, ReplayPartition, TradeReplay};
    pub use crate::backtest::schema::BacktestEvent;
    #[cfg(test)]
    pub use crate::backtest::schema::DepthDeltaEvent;
    pub use crate::core::types::Kline as BacktestKline;
    #[cfg(test)]
    pub use crate::core::types::{OrderSide as BacktestOrderSide, Trade as BacktestTrade};
}

include!("../../crates/rustcta-backtest/src/offline_runtime.rs");

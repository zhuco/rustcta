//! Backtest workspace boundary.
//!
//! The implementation still lives in the legacy root crate while the large
//! `src/backtest/*` tree is extracted incrementally. Keep this facade narrow so
//! app crates can depend on `rustcta-backtest` instead of the legacy root.

pub mod data;
pub mod factors;
pub mod indicators;
pub mod matching;
pub mod offline_runtime {
    mod deps {
        pub use crate::data::depth_dataset::DepthDatasetReader;
        #[cfg(test)]
        pub use crate::data::depth_dataset::DepthDatasetWriter;
        #[cfg(test)]
        pub use crate::data::kline_dataset::KlineDatasetWriter;
        pub use crate::data::trade_dataset::TradeDatasetReader;
        #[cfg(test)]
        pub use crate::data::trade_dataset::TradeDatasetWriter;
        pub use crate::replay::{DepthReplay, KlineReplay, ReplayPartition, TradeReplay};
        pub use crate::schema::BacktestEvent;
        #[cfg(test)]
        pub use crate::schema::DepthDeltaEvent;
        pub use crate::types::BacktestKline;
        #[cfg(test)]
        pub use crate::types::{BacktestOrderSide, BacktestTrade};
    }

    include!("offline_runtime.rs");
}
pub mod replay;
pub mod runtime_support {
    mod dto {
        pub use crate::types::{BacktestMarketType as MarketType, BacktestOrderSide as OrderSide};
    }

    include!("runtime_support.rs");
}
pub mod schema;
pub mod scoring;
pub mod strategy;
pub mod symbol;

pub mod types {
    pub use rustcta_types::{
        BacktestFee, BacktestKline, BacktestMarketType, BacktestOrderSide, BacktestOrderType,
        BacktestTrade, BacktestTradingPair,
    };
}

#[cfg(feature = "legacy-runtime")]
pub mod runtime {
    pub use rustcta::backtest::runtime::*;
}

mod dto {
    pub use crate::core::types::{Kline, MarketType, Trade};
}

include!("../../../crates/rustcta-backtest/src/schema_impl.rs");

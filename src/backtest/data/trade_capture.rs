mod dto {
    pub use crate::core::types::{OrderSide, Trade};
}

include!("../../../crates/rustcta-backtest/src/data/trade_capture_impl.rs");

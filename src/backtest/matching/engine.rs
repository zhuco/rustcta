mod dto {
    pub use crate::backtest::schema::BacktestEvent;
    pub use crate::backtest::strategy::StrategySignal;
    pub use crate::core::types::{Kline, MarketType, OrderSide, OrderType};
}

include!("../../../crates/rustcta-backtest/src/matching/engine_impl.rs");

//! Cross-exchange arbitrage strategy skeleton.
//!
//! The strategy consumes normalized market contracts and will emit simulation or
//! execution signals in later phases. It does not place live orders directly.

pub mod config;
pub mod dashboard;
pub mod execution;
pub mod fees;
pub mod funding;
pub mod market_data;
pub mod opportunity;
pub mod position;
pub mod private_sync;
pub mod recorder;
pub mod replay;
pub mod risk;
pub mod runtime;
pub mod signal;
pub mod simulation;
pub mod sizing;
pub mod spread_engine;
pub mod state;
pub mod storage;
pub mod symbols;
pub mod tasks;
pub mod types;

pub use crate::market::{
    BookLevel, BookQuality, CanonicalSymbol, ExchangeId, ExchangeSymbol, OrderBook5, RouteStatus,
    RuntimeMode,
};
pub use config::*;
pub use dashboard::*;
pub use execution::*;
pub use fees::*;
pub use funding::*;
pub use market_data::*;
pub use opportunity::*;
pub use position::*;
pub use private_sync::*;
pub use recorder::*;
pub use replay::*;
pub use risk::*;
pub use runtime::*;
pub use signal::*;
pub use simulation::*;
pub use sizing::*;
pub use spread_engine::*;
pub use state::*;
pub use storage::*;
pub use symbols::*;
pub use tasks::*;
pub use types::*;

pub const CROSS_EXCHANGE_ARBITRAGE_STRATEGY_NAME: &str = "cross_exchange_arbitrage";

#[cfg(test)]
mod phase0_tests {
    use super::*;

    #[test]
    fn cross_exchange_arbitrage_should_export_phase0_contracts() {
        let symbol = CanonicalSymbol::new("wif", "usdt");
        let exchange_symbol = ExchangeSymbol::new(ExchangeId::Gate, "WIF_USDT");

        assert_eq!(
            CROSS_EXCHANGE_ARBITRAGE_STRATEGY_NAME,
            "cross_exchange_arbitrage"
        );
        assert_eq!(symbol.to_string(), "WIF/USDT");
        assert_eq!(exchange_symbol.exchange, ExchangeId::Gate);
    }
}

#[cfg(test)]
mod tests;

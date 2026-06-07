mod dto {
    pub use crate::core::types::Trade;
}

include!("../../../crates/rustcta-backtest/src/data/trade_dataset_impl.rs");

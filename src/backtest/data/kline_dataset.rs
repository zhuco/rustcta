mod dto {
    pub use crate::core::types::Kline;
}

include!("../../../crates/rustcta-backtest/src/data/kline_dataset_impl.rs");

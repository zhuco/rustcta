mod dto {
    pub use crate::backtest::schema::DepthDeltaEvent;
}

include!("../../../crates/rustcta-backtest/src/data/depth_dataset_impl.rs");

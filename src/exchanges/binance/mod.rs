//! Binance exchange adapters.

pub mod core;
pub mod spot;

pub use core::*;
pub use spot::{BinanceSpotClient, BinanceSpotConfig};

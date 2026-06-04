//! Smart-money adaptive alpha platform primitives.
//!
//! This module keeps domain logic side-effect free so ingestion, persistence,
//! execution, and backtesting adapters can be added without coupling research
//! logic to infrastructure.

pub mod alpha;
pub mod binance_market;
pub mod binance_replay;
pub mod clustering;
pub mod config;
pub mod domain;
pub mod execution_sim;
pub mod filtering;
pub mod hyperliquid_wallet;
pub mod monitor;
pub mod pipeline;
pub mod portfolio;
pub mod position;
pub mod profiling;
pub mod risk;
pub mod scoring;
pub mod simulation;
pub mod storage;

pub use alpha::*;
pub use binance_market::*;
pub use binance_replay::*;
pub use clustering::*;
pub use config::*;
pub use domain::*;
pub use execution_sim::*;
pub use filtering::*;
pub use hyperliquid_wallet::*;
pub use monitor::*;
pub use pipeline::*;
pub use portfolio::*;
pub use position::*;
pub use profiling::*;
pub use risk::*;
pub use scoring::*;
pub use simulation::*;
pub use storage::*;

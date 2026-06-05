#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

pub mod analysis;
pub mod backtest;
pub mod control;
pub mod core;
pub mod cta;
pub mod data;
pub mod exchanges;
pub mod execution;
pub mod live_preflight;
pub mod market;
pub mod risk;
pub mod scanner;
pub mod smart_money;
pub mod strategies;
pub mod utils;
pub mod web;

// 选择性导出，避免命名冲突
pub use core::{
    config::*, error::*, exchange::*, memory_pool::*, monitoring::*, request_manager::*, types::*,
};
// WebSocket 单独导出避免 Result 冲突
pub use core::websocket::{MessageHandler, WebSocketClient};
pub use cta::*;
pub use exchanges::*;
pub use strategies::*;
pub use utils::*;

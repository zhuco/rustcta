#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

pub mod analysis;
pub mod core;
pub mod cta;
pub mod exchanges;
pub mod strategies;
pub mod utils;

// 选择性导出，避免命名冲突
pub use core::{config::*, error::*, exchange::*, memory_pool::*, monitoring::*, request_manager::*, types::*};
// WebSocket 单独导出避免 Result 冲突
pub use core::websocket::{MessageHandler, WebSocketClient};
pub use cta::*;
pub use exchanges::*;
pub use strategies::*;
pub use utils::*;

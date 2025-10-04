// 核心模块 - 只包含核心业务逻辑
pub mod config;
pub mod error;
pub mod exchange;
pub mod memory_pool;
pub mod monitoring;
pub mod order_cache;
pub mod request_manager;
pub mod retry_policy;
pub mod risk_manager;
pub mod traits;
pub mod types;
pub mod websocket;

// 性能优化模块 - 暂时禁用直到修复编译错误
// pub mod enhanced_memory_pool;
// pub mod enhanced_websocket;

pub use config::*;
pub use error::*;
pub use exchange::*;
pub use memory_pool::*;
pub use monitoring::*;
pub use request_manager::*;
pub use types::{
    AccountSnapshot, Balance, BatchOrderRequest, BatchOrderResponse, Interval, Kline, MarketType,
    Order, OrderBook, OrderRequest, OrderSide, OrderStatus, OrderType, Position, Ticker, Trade,
    TradeFee, TransferResult,
};
pub use websocket::{MessageHandler, WebSocketClient};

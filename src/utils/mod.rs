// 工具模块 - 通用工具函数
pub mod default_config;
pub mod indicators;
pub mod order_id;
pub mod position_reporter;
pub mod safe_macros;
pub mod signature;
pub mod symbol;
pub mod time_sync;
pub mod trading_pair_info;
pub mod unified_logger;
pub mod webhook;

pub use default_config::create_default_config;
pub use signature::*;
pub use symbol::*;
// 使用统一的日志工具
pub use order_id::{generate_order_id, generate_order_id_with_tag, parse_order_id, OrderIdInfo};
pub use time_sync::{get_synced_timestamp, get_time_sync, init_global_time_sync};
pub use unified_logger::{create_strategy_logger, get_strategy_log_path, init_strategy_logger};

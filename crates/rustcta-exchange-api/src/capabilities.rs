use serde::{Deserialize, Serialize};

use crate::{ExchangeId, MarketType, OrderType, TimeInForce, EXCHANGE_API_SCHEMA_VERSION};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeClientCapabilities {
    pub schema_version: u16,
    pub exchange: ExchangeId,
    pub market_types: Vec<MarketType>,
    pub supports_public_rest: bool,
    pub supports_private_rest: bool,
    pub supports_public_streams: bool,
    pub supports_private_streams: bool,
    pub supports_symbol_rules: bool,
    pub supports_order_book_snapshot: bool,
    pub supports_balances: bool,
    pub supports_positions: bool,
    pub supports_fees: bool,
    pub supports_place_order: bool,
    pub supports_cancel_order: bool,
    pub supports_query_order: bool,
    pub supports_open_orders: bool,
    pub supports_recent_fills: bool,
    pub supports_batch_place_order: bool,
    pub supports_batch_cancel_order: bool,
    pub supports_cancel_all_orders: bool,
    pub supports_client_order_id: bool,
    pub supports_reduce_only: bool,
    pub supports_post_only: bool,
    pub supports_time_in_force: Vec<TimeInForce>,
    pub supports_order_types: Vec<OrderType>,
    pub max_order_book_depth: Option<u32>,
    pub max_recent_fill_limit: Option<u32>,
}

impl ExchangeClientCapabilities {
    pub fn new(exchange: ExchangeId) -> Self {
        Self {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange,
            market_types: Vec::new(),
            supports_public_rest: false,
            supports_private_rest: false,
            supports_public_streams: false,
            supports_private_streams: false,
            supports_symbol_rules: false,
            supports_order_book_snapshot: false,
            supports_balances: false,
            supports_positions: false,
            supports_fees: false,
            supports_place_order: false,
            supports_cancel_order: false,
            supports_query_order: false,
            supports_open_orders: false,
            supports_recent_fills: false,
            supports_batch_place_order: false,
            supports_batch_cancel_order: false,
            supports_cancel_all_orders: false,
            supports_client_order_id: false,
            supports_reduce_only: false,
            supports_post_only: false,
            supports_time_in_force: Vec::new(),
            supports_order_types: Vec::new(),
            max_order_book_depth: None,
            max_recent_fill_limit: None,
        }
    }
}

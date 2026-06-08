#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{novadax_symbol, parse_order_book_snapshot, parse_symbol_rules};
use super::NovadaxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const SYMBOLS_PATH: &str = "/v1/common/symbols";
pub const ORDERBOOK_PATH: &str = "/v1/market/depth";
pub const TICKER_PATH: &str = "/v1/market/ticker";
pub const TRADES_PATH: &str = "/v1/market/trades";

pub fn order_book_request_spec(symbol: &str, limit: u32) -> Value {
    json!({
        "method": "GET",
        "path": ORDERBOOK_PATH,
        "auth": "none",
        "query": {
            "symbol": novadax_symbol(symbol),
            "limit": limit.to_string()
        },
        "headers": { "Accept": "application/json" }
    })
}

pub async fn get_symbol_rules_public_rest(
    adapter: &NovadaxGatewayAdapter,
    request: SymbolRulesRequest,
) -> ExchangeApiResult<SymbolRulesResponse> {
    ensure_exchange_api_schema(request.schema_version)?;
    for symbol in &request.symbols {
        adapter.ensure_exchange(&symbol.exchange)?;
        adapter.ensure_supported_market_type(symbol.market_type)?;
    }
    let value = adapter
        .rest
        .send_public_get(SYMBOLS_PATH, &HashMap::new())
        .await?;
    Ok(SymbolRulesResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(adapter.exchange_id.clone(), request.context.request_id),
        rules: parse_symbol_rules(&adapter.exchange_id, &request.symbols, &value)?,
    })
}

pub async fn get_order_book_public_rest(
    adapter: &NovadaxGatewayAdapter,
    request: OrderBookRequest,
) -> ExchangeApiResult<OrderBookResponse> {
    ensure_exchange_api_schema(request.schema_version)?;
    adapter.ensure_exchange(&request.symbol.exchange)?;
    adapter.ensure_supported_market_type(request.symbol.market_type)?;
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        novadax_symbol(&request.symbol.exchange_symbol.symbol),
    );
    if let Some(depth) = request.depth {
        params.insert("limit".to_string(), depth.min(50).to_string());
    }
    let value = adapter
        .rest
        .send_public_get(ORDERBOOK_PATH, &params)
        .await?;
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(request.symbol.exchange.clone(), request.context.request_id),
        order_book: parse_order_book_snapshot(&adapter.exchange_id, request.symbol, &value)?,
    })
}

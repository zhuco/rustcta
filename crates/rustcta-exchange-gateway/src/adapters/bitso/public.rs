#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{bitso_book, parse_order_book_snapshot, parse_symbol_rules};
use super::BitsoGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const AVAILABLE_BOOKS_PATH: &str = "/available_books";
pub const ORDER_BOOK_PATH: &str = "/order_book";
pub const TRADES_PATH: &str = "/trades";

pub fn order_book_request_spec(symbol: &str) -> Value {
    json!({
        "method": "GET",
        "path": ORDER_BOOK_PATH,
        "auth": "none",
        "query": {
            "book": bitso_book(symbol),
            "aggregate": "true"
        }
    })
}

pub async fn get_symbol_rules_public_rest(
    adapter: &BitsoGatewayAdapter,
    request: SymbolRulesRequest,
) -> ExchangeApiResult<SymbolRulesResponse> {
    ensure_exchange_api_schema(request.schema_version)?;
    for symbol in &request.symbols {
        adapter.ensure_exchange(&symbol.exchange)?;
        adapter.ensure_supported_market_type(symbol.market_type)?;
    }
    let value = adapter
        .rest
        .send_public_get(AVAILABLE_BOOKS_PATH, &HashMap::new())
        .await?;
    Ok(SymbolRulesResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(adapter.exchange_id.clone(), request.context.request_id),
        rules: parse_symbol_rules(&adapter.exchange_id, &request.symbols, &value)?,
    })
}

pub async fn get_order_book_public_rest(
    adapter: &BitsoGatewayAdapter,
    request: OrderBookRequest,
) -> ExchangeApiResult<OrderBookResponse> {
    ensure_exchange_api_schema(request.schema_version)?;
    adapter.ensure_exchange(&request.symbol.exchange)?;
    adapter.ensure_supported_market_type(request.symbol.market_type)?;
    let mut params = HashMap::new();
    params.insert(
        "book".to_string(),
        bitso_book(&request.symbol.exchange_symbol.symbol),
    );
    params.insert("aggregate".to_string(), "true".to_string());
    let value = adapter
        .rest
        .send_public_get(ORDER_BOOK_PATH, &params)
        .await?;
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(request.symbol.exchange.clone(), request.context.request_id),
        order_book: parse_order_book_snapshot(&adapter.exchange_id, request.symbol, &value)?,
    })
}

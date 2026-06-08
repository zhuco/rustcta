#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{
    foxbit_symbol, normalize_depth, parse_order_book_snapshot, parse_symbol_rules,
};
use super::FoxbitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const MARKETS_PATH: &str = "/markets";
pub const ORDERBOOK_PATH_SUFFIX: &str = "/orderbook";

pub fn markets_request_spec() -> Value {
    super::transport::public_get_request_spec(MARKETS_PATH, None)
}

pub fn order_book_request_spec(symbol: &str, depth: usize) -> Value {
    super::transport::public_get_request_spec(
        &format!(
            "{MARKETS_PATH}/{}{ORDERBOOK_PATH_SUFFIX}",
            foxbit_symbol(symbol)
        ),
        Some(json!({ "depth": depth })),
    )
}

pub async fn get_symbol_rules_public_rest(
    adapter: &FoxbitGatewayAdapter,
    request: SymbolRulesRequest,
) -> ExchangeApiResult<SymbolRulesResponse> {
    ensure_exchange_api_schema(request.schema_version)?;
    for symbol in &request.symbols {
        adapter.ensure_exchange(&symbol.exchange)?;
        adapter.ensure_supported_market_type(symbol.market_type)?;
    }
    let value = adapter
        .rest
        .send_public_get(MARKETS_PATH, &HashMap::new())
        .await?;
    Ok(SymbolRulesResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(adapter.exchange_id.clone(), request.context.request_id),
        rules: parse_symbol_rules(&adapter.exchange_id, &request.symbols, &value)?,
    })
}

pub async fn get_order_book_public_rest(
    adapter: &FoxbitGatewayAdapter,
    request: OrderBookRequest,
) -> ExchangeApiResult<OrderBookResponse> {
    ensure_exchange_api_schema(request.schema_version)?;
    adapter.ensure_exchange(&request.symbol.exchange)?;
    adapter.ensure_supported_market_type(request.symbol.market_type)?;
    let depth = normalize_depth(request.depth)?;
    let mut query = HashMap::new();
    query.insert("depth".to_string(), depth.to_string());
    let path = format!(
        "{MARKETS_PATH}/{}{ORDERBOOK_PATH_SUFFIX}",
        foxbit_symbol(&request.symbol.exchange_symbol.symbol)
    );
    let value = adapter.rest.send_public_get(&path, &query).await?;
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(request.symbol.exchange.clone(), request.context.request_id),
        order_book: parse_order_book_snapshot(&adapter.exchange_id, request.symbol, depth, &value)?,
    })
}

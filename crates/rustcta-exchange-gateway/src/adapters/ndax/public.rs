#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{ndax_symbol, parse_order_book_snapshot, parse_symbol_rules};
use super::transport::public_gateway_request_spec;
use super::NdaxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const GET_INSTRUMENTS_CALL: &str = "GetInstruments";
pub const GET_L2_SNAPSHOT_CALL: &str = "GetL2Snapshot";
pub const GET_PRODUCTS_CALL: &str = "GetProducts";

pub fn get_instruments_payload(oms_id: i64) -> Value {
    json!({ "OMSId": oms_id })
}

pub fn get_l2_snapshot_payload(oms_id: i64, symbol: &str, depth: u32) -> Value {
    json!({
        "OMSId": oms_id,
        "Symbol": ndax_symbol(symbol),
        "Depth": depth
    })
}

pub fn instruments_request_spec(oms_id: i64) -> Value {
    public_gateway_request_spec(GET_INSTRUMENTS_CALL, get_instruments_payload(oms_id))
}

pub fn l2_snapshot_request_spec(oms_id: i64, symbol: &str, depth: u32) -> Value {
    public_gateway_request_spec(
        GET_L2_SNAPSHOT_CALL,
        get_l2_snapshot_payload(oms_id, symbol, depth),
    )
}

pub async fn get_symbol_rules_public_rest(
    adapter: &NdaxGatewayAdapter,
    request: SymbolRulesRequest,
) -> ExchangeApiResult<SymbolRulesResponse> {
    ensure_exchange_api_schema(request.schema_version)?;
    adapter.ensure_public_rest()?;
    for symbol in &request.symbols {
        adapter.ensure_exchange(&symbol.exchange)?;
        adapter.ensure_supported_market_type(symbol.market_type)?;
    }
    let value = adapter
        .rest
        .send_public_call(
            1,
            GET_INSTRUMENTS_CALL,
            get_instruments_payload(adapter.config.oms_id),
        )
        .await?;
    Ok(SymbolRulesResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(adapter.exchange_id.clone(), request.context.request_id),
        rules: parse_symbol_rules(&adapter.exchange_id, &request.symbols, &value)?,
    })
}

pub async fn get_order_book_public_rest(
    adapter: &NdaxGatewayAdapter,
    request: OrderBookRequest,
) -> ExchangeApiResult<OrderBookResponse> {
    ensure_exchange_api_schema(request.schema_version)?;
    adapter.ensure_public_rest()?;
    adapter.ensure_exchange(&request.symbol.exchange)?;
    adapter.ensure_supported_market_type(request.symbol.market_type)?;
    let depth = request.depth.unwrap_or(50).clamp(1, 200);
    let value = adapter
        .rest
        .send_public_call(
            1,
            GET_L2_SNAPSHOT_CALL,
            get_l2_snapshot_payload(
                adapter.config.oms_id,
                &request.symbol.exchange_symbol.symbol,
                depth,
            ),
        )
        .await?;
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(request.symbol.exchange.clone(), request.context.request_id),
        order_book: parse_order_book_snapshot(
            &adapter.exchange_id,
            request.symbol,
            Some(depth),
            &value,
        )?,
    })
}

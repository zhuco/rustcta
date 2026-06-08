use rustcta_exchange_api::{ExchangeApiResult, SymbolRules, SymbolScope};
use rustcta_types::{ExchangeId, MarketType, OrderBookSnapshot};
use serde_json::Value;

pub const PARSER_BOUNDARY: &str =
    "myokx reuses OKX Spot public REST response shapes on the EEA host profile";

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    crate::adapters::okx::parser::parse_symbol_rules(exchange_id, MarketType::Spot, value)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    crate::adapters::okx::parser::parse_orderbook_snapshot(exchange_id, symbol, value)
}

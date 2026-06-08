use rustcta_exchange_api::{ExchangeApiResult, SymbolRules, SymbolScope};
use rustcta_types::{ExchangeId, OrderBookSnapshot};
use serde_json::Value;

pub const PARSER_BOUNDARY: &str = "okxus reuses OKX v5 spot REST response envelopes";

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    crate::adapters::okx::parser::parse_symbol_rules(exchange_id, value)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    crate::adapters::okx::parser::parse_orderbook_snapshot(exchange_id, symbol, value)
}

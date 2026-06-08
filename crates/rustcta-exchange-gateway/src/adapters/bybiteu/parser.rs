use rustcta_exchange_api::{ExchangeApiResult, SymbolRules, SymbolScope};
use rustcta_types::{ExchangeId, MarketType, OrderBookSnapshot};
use serde_json::Value;

pub const PARSER_BOUNDARY: &str =
    "bybiteu reuses Bybit V5 public REST and WebSocket response shapes";

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    crate::adapters::bybit::parser::parse_symbol_rules(exchange_id, market_type, value)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    crate::adapters::bybit::parser::parse_orderbook_snapshot(exchange_id, symbol, value)
}

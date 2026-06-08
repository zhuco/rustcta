use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    normalize_depth, normalize_zebpay_symbol, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::ZebpayGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl ZebpayGatewayAdapter {
    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        let exchange = request
            .symbols
            .first()
            .map(|symbol| symbol.exchange.clone())
            .unwrap_or_else(|| self.exchange_id.clone());
        self.ensure_exchange(&exchange)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            normalize_zebpay_symbol(&symbol.exchange_symbol.symbol)?;
        }
        let query = market_query(&self.config.group);
        let response = self.rest.send_public_get("/market", &query).await?;
        let requested = request
            .symbols
            .iter()
            .map(|symbol| normalize_zebpay_symbol(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut rules = parse_symbol_rules(&self.exchange_id, &response)?;
        if !requested.is_empty() {
            rules.retain(|rule| requested.contains(&rule.symbol.exchange_symbol.symbol));
        }
        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            rules,
        })
    }

    pub(super) async fn get_order_book_impl(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let depth = normalize_depth(request.depth);
        let symbol = normalize_zebpay_symbol(&request.symbol.exchange_symbol.symbol)?;
        let endpoint = order_book_endpoint(&symbol, depth);
        let query = order_book_query(&self.config.group);
        let value = self.rest.send_public_get(&endpoint, &query).await?;
        let order_book =
            parse_orderbook_snapshot(&self.exchange_id, request.symbol, depth, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

pub(super) fn market_query(group: &str) -> HashMap<String, String> {
    let mut query = HashMap::new();
    query.insert("group".to_string(), group.to_string());
    query
}

pub(super) fn order_book_endpoint(symbol: &str, depth: u32) -> String {
    if depth > 15 {
        format!("/market/{symbol}/book_long")
    } else {
        format!("/market/{symbol}/book")
    }
}

pub(super) fn order_book_query(group: &str) -> HashMap<String, String> {
    let mut query = HashMap::new();
    query.insert("group".to_string(), group.to_string());
    query.insert("converted".to_string(), "0".to_string());
    query
}

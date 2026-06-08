use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::Value;

use super::parser::{normalize_market_symbol, parse_orderbook_snapshot, parse_symbol_rules};
use super::BitstampGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BitstampGatewayAdapter {
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
        }
        let requested = request
            .symbols
            .iter()
            .map(|symbol| normalize_market_symbol(&symbol.exchange_symbol.symbol))
            .collect::<Vec<_>>();
        let mut response = self
            .rest
            .send_public_get("/api/v2/markets/", &HashMap::new())
            .await?;
        retain_requested_markets(&mut response, &requested);
        let rules = parse_symbol_rules(&self.exchange_id, &response)?;
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
        let market_symbol = normalize_market_symbol(&request.symbol.exchange_symbol.symbol);
        let endpoint = format!("/api/v2/order_book/{market_symbol}/");
        let mut params = HashMap::new();
        params.insert("group".to_string(), "1".to_string());
        let value = self.rest.send_public_get(&endpoint, &params).await?;
        let order_book =
            parse_orderbook_snapshot(&self.exchange_id, request.symbol, request.depth, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

fn retain_requested_markets(response: &mut Value, requested: &[String]) {
    if requested.is_empty() {
        return;
    }
    let Some(markets) = response.as_array_mut() else {
        return;
    };
    markets.retain(|market| {
        market
            .get("market_symbol")
            .and_then(Value::as_str)
            .map(normalize_market_symbol)
            .is_some_and(|symbol| requested.contains(&symbol))
    });
}

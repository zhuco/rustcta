use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::parser::{
    normalize_depth, normalize_p2b_symbol, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::P2bGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl P2bGatewayAdapter {
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
            normalize_p2b_symbol(&symbol.exchange_symbol.symbol)?;
        }
        let response = self
            .rest
            .send_public_get("/api/v2/public/markets", &HashMap::new())
            .await?;
        let requested = request
            .symbols
            .iter()
            .map(|symbol| normalize_p2b_symbol(&symbol.exchange_symbol.symbol))
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
        let symbol = normalize_p2b_symbol(&request.symbol.exchange_symbol.symbol)?;
        let mut bid_query = HashMap::new();
        bid_query.insert("market".to_string(), symbol.clone());
        bid_query.insert("side".to_string(), "buy".to_string());
        bid_query.insert("offset".to_string(), "0".to_string());
        bid_query.insert("limit".to_string(), depth.to_string());
        let mut ask_query = bid_query.clone();
        ask_query.insert("side".to_string(), "sell".to_string());
        let bids = self
            .rest
            .send_public_get("/api/v2/public/book", &bid_query)
            .await?;
        let asks = self
            .rest
            .send_public_get("/api/v2/public/book", &ask_query)
            .await?;
        let value = json!({
            "bids": bids.get("result").and_then(|result| result.get("orders")).cloned().unwrap_or_default(),
            "asks": asks.get("result").and_then(|result| result.get("orders")).cloned().unwrap_or_default(),
            "current_time": bids.get("current_time").or_else(|| asks.get("current_time")).cloned()
        });
        let order_book =
            parse_orderbook_snapshot(&self.exchange_id, request.symbol, depth, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

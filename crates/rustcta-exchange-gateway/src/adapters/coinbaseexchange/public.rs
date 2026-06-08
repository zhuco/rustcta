use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    normalize_coinbaseexchange_depth, normalize_coinbaseexchange_symbol, parse_orderbook_snapshot,
    parse_symbol_rules,
};
use super::CoinbaseExchangeGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinbaseExchangeGatewayAdapter {
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

        let response = self
            .rest
            .send_public_get("/products", &HashMap::new())
            .await?;
        let requested = request
            .symbols
            .iter()
            .map(|symbol| normalize_coinbaseexchange_symbol(&symbol.exchange_symbol.symbol))
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
        let depth = normalize_coinbaseexchange_depth(request.depth.unwrap_or(50));
        let mut params = HashMap::new();
        params.insert("level".to_string(), "2".to_string());
        let endpoint = format!(
            "/products/{}/book",
            normalize_coinbaseexchange_symbol(&request.symbol.exchange_symbol.symbol)?
        );
        let value = self.rest.send_public_get(&endpoint, &params).await?;
        let mut order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        order_book.bids.truncate(depth as usize);
        order_book.asks.truncate(depth as usize);
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

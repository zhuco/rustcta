use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    bitmex_symbol_key, normalize_bitmex_symbol, normalize_depth, parse_orderbook_snapshot,
    parse_symbol_rules,
};
use super::BitmexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BitmexGatewayAdapter {
    pub(super) async fn get_symbol_rules_public_rest(
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
            self.ensure_supported_market_type(symbol.market_type)?;
        }

        let response = self
            .rest
            .send_public_request("/api/v1/instrument/active", &HashMap::new())
            .await?;
        let requested = request
            .symbols
            .iter()
            .map(|symbol| bitmex_symbol_key(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut rules = parse_symbol_rules(&self.exchange_id, &response)?;
        if !requested.is_empty() {
            rules.retain(|rule| {
                bitmex_symbol_key(&rule.symbol.exchange_symbol.symbol)
                    .map(|symbol| requested.contains(&symbol))
                    .unwrap_or(false)
            });
        }

        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            rules,
        })
    }

    pub(super) async fn get_order_book_public_rest(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let depth = normalize_depth(request.depth.unwrap_or(25));
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bitmex_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert("depth".to_string(), depth.to_string());
        let value = self
            .rest
            .send_public_request("/api/v1/orderBook/L2", &params)
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{normalize_fmfwio_symbol, parse_orderbook_snapshot, parse_symbol_rules};
use super::FmfwioGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl FmfwioGatewayAdapter {
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
        let requested_symbols = request
            .symbols
            .iter()
            .map(|symbol| {
                self.ensure_exchange(&symbol.exchange)?;
                self.ensure_spot(symbol.market_type)?;
                normalize_fmfwio_symbol(symbol)
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;

        let response = self
            .rest
            .send_public_get("/public/symbol", &HashMap::new())
            .await?;
        let mut rules = parse_symbol_rules(&self.exchange_id, &response)?;
        if !requested_symbols.is_empty() {
            rules.retain(|rule| {
                requested_symbols
                    .iter()
                    .any(|symbol| symbol == &rule.symbol.exchange_symbol.symbol)
            });
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
        let symbol = normalize_fmfwio_symbol(&request.symbol)?;
        let depth = normalize_depth(request.depth);
        let mut params = HashMap::new();
        params.insert("depth".to_string(), depth.to_string());
        let response = self
            .rest
            .send_public_get(&format!("/public/orderbook/{symbol}"), &params)
            .await?;
        let order_book =
            parse_orderbook_snapshot(&self.exchange_id, request.symbol, depth, &response)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

fn normalize_depth(depth: Option<u32>) -> u32 {
    depth.unwrap_or(20).clamp(1, 100)
}

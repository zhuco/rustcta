use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{normalize_indodax_symbol, parse_orderbook_snapshot, parse_symbol_rules};
use super::IndodaxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl IndodaxGatewayAdapter {
    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let value = self
            .rest
            .send_public_request("/api/pairs", &HashMap::new())
            .await?;
        let mut rules = parse_symbol_rules(&self.exchange_id, &value)?;
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| symbol.exchange_symbol.symbol.to_ascii_lowercase())
                .collect::<Vec<_>>();
            rules.retain(|rule| {
                requested.contains(&rule.symbol.exchange_symbol.symbol.to_ascii_lowercase())
            });
        }
        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
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
        let symbol = normalize_indodax_symbol(&request.symbol.exchange_symbol.symbol)?;
        let value = self
            .rest
            .send_public_request(&format!("/api/{symbol}/depth"), &HashMap::new())
            .await?;
        let order_book = parse_orderbook_snapshot(
            &self.exchange_id,
            request.symbol.clone(),
            &value,
            self.config.stale_book_ms,
        )?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order_book,
        })
    }
}

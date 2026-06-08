use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{normalize_bithumb_symbol, parse_orderbook_snapshot, parse_symbol_rules};
use super::BithumbGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BithumbGatewayAdapter {
    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let mut params = HashMap::new();
        params.insert("isDetails".to_string(), "true".to_string());
        let value = self.rest.send_public_get("/v1/market/all", &params).await?;
        let requested = request
            .symbols
            .iter()
            .map(|symbol| normalize_bithumb_symbol(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut rules = parse_symbol_rules(&self.exchange_id, &value)?;
        if !requested.is_empty() {
            rules.retain(|rule| requested.contains(&rule.symbol.exchange_symbol.symbol));
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
        let mut params = HashMap::new();
        params.insert(
            "markets".to_string(),
            normalize_bithumb_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        let value = self.rest.send_public_get("/v1/orderbook", &params).await?;
        let mut order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        if let Some(depth) = request.depth {
            let depth = depth.min(30) as usize;
            order_book.bids.truncate(depth);
            order_book.asks.truncate(depth);
        }
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

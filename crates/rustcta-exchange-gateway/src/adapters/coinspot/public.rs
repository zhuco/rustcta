use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    normalize_coinspot_symbol, normalize_depth, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::CoinspotGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinspotGatewayAdapter {
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
            normalize_coinspot_symbol(&symbol.exchange_symbol.symbol)?;
        }

        let response = self
            .rest
            .send_public_get("/pubapi/v2/markets", &HashMap::new())
            .await?;
        let requested = request
            .symbols
            .iter()
            .map(|symbol| normalize_coinspot_symbol(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut rules = parse_symbol_rules(&self.exchange_id, &response)?;
        if !requested.is_empty() {
            let requested_bases = requested
                .iter()
                .map(|(base, _)| base.to_ascii_uppercase())
                .collect::<Vec<_>>();
            rules.retain(|rule| requested_bases.contains(&rule.base_asset));
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
        let (coin, market) = normalize_coinspot_symbol(&request.symbol.exchange_symbol.symbol)?;
        let mut params = HashMap::new();
        params.insert("cointype".to_string(), coin);
        params.insert("markettype".to_string(), market);
        let value = self
            .rest
            .send_public_get("/pubapi/v2/orders/open", &params)
            .await?;
        let order_book =
            parse_orderbook_snapshot(&self.exchange_id, request.symbol, depth, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

use std::collections::BTreeMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_depth, normalize_futures_symbol, parse_futures_orderbook_snapshot,
    parse_futures_symbol_rules,
};
use super::KrakenFuturesGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl KrakenFuturesGatewayAdapter {
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
            self.ensure_market_type(symbol.market_type)?;
        }

        let include_futures = request.symbols.is_empty()
            || request
                .symbols
                .iter()
                .any(|symbol| symbol.market_type == MarketType::Perpetual);
        let mut rules = Vec::new();
        if include_futures {
            let value = self
                .rest
                .futures_public_get("instruments", &BTreeMap::new())
                .await?;
            rules.extend(parse_futures_symbol_rules(&self.exchange_id, &value)?);
        }
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| {
                    (
                        symbol.market_type,
                        symbol.exchange_symbol.symbol.to_ascii_uppercase(),
                    )
                })
                .collect::<std::collections::HashSet<_>>();
            rules.retain(|rule| {
                requested.contains(&(
                    rule.symbol.market_type,
                    rule.symbol.exchange_symbol.symbol.to_ascii_uppercase(),
                ))
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
        self.ensure_market_type(request.symbol.market_type)?;
        let mut params = BTreeMap::new();
        let depth = normalize_depth(request.depth.unwrap_or(10)).to_string();
        params.insert(
            "symbol".to_string(),
            normalize_futures_symbol(&request.symbol)?,
        );
        params.insert("depth".to_string(), depth);
        let value = self.rest.futures_public_get("orderbook", &params).await?;
        let order_book =
            parse_futures_orderbook_snapshot(&self.exchange_id, request.symbol.clone(), &value)?;

        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order_book,
        })
    }
}

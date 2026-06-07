use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{normalize_bigone_symbol, parse_order_book, parse_symbol_rules};
use super::BigOneGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl BigOneGatewayAdapter {
    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        let market_type = request
            .symbols
            .first()
            .map(|symbol| symbol.market_type)
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let endpoint = if market_type == MarketType::Perpetual {
            "/api/contract/v2/instruments"
        } else {
            "/api/v3/asset_pairs"
        };
        let value = self
            .rest
            .get_public(
                market_type == MarketType::Perpetual,
                endpoint,
                &HashMap::new(),
            )
            .await?;
        let mut response = parse_symbol_rules(&self.exchange_id, market_type, &value)?;
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| {
                    normalize_bigone_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)
                })
                .collect::<Vec<_>>();
            response.rules.retain(|rule| {
                requested.contains(&normalize_bigone_symbol(
                    &rule.symbol.exchange_symbol.symbol,
                    rule.symbol.market_type,
                ))
            });
        }
        response.metadata.request_id = request.context.request_id;
        Ok(response)
    }

    pub(super) async fn get_order_book_impl(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let normalized_symbol = normalize_bigone_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        );
        let mut params = HashMap::new();
        if request.symbol.market_type == MarketType::Perpetual {
            params.insert("instrument_id".to_string(), normalized_symbol.clone());
        }
        if let Some(depth) = request.depth {
            params.insert("limit".to_string(), depth.min(200).to_string());
        }
        let endpoint = if request.symbol.market_type == MarketType::Perpetual {
            "/api/contract/v2/depth"
        } else {
            return self
                .rest
                .get_public(
                    false,
                    &format!("/api/v3/asset_pairs/{normalized_symbol}/depth"),
                    &params,
                )
                .await
                .and_then(|value| parse_order_book(&self.exchange_id, request.symbol, &value))
                .map(|mut response| {
                    response.schema_version = EXCHANGE_API_SCHEMA_VERSION;
                    response.metadata.request_id = request.context.request_id;
                    response
                });
        };
        let value = self
            .rest
            .get_public(
                request.symbol.market_type == MarketType::Perpetual,
                endpoint,
                &params,
            )
            .await?;
        let mut response = parse_order_book(&self.exchange_id, request.symbol, &value)?;
        response.schema_version = EXCHANGE_API_SCHEMA_VERSION;
        response.metadata.request_id = request.context.request_id;
        Ok(response)
    }
}

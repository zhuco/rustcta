use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{normalize_symbol, parse_orderbook_response, parse_symbol_rules};
use super::CointrGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CointrGatewayAdapter {
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
            self.ensure_supported_market(symbol.market_type)?;
        }
        let requested = request
            .symbols
            .iter()
            .map(normalize_symbol)
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let has_spot = request.symbols.is_empty()
            || request
                .symbols
                .iter()
                .any(|symbol| symbol.market_type == MarketType::Spot);
        let has_perp = request.symbols.is_empty()
            || request
                .symbols
                .iter()
                .any(|symbol| symbol.market_type == MarketType::Perpetual);
        let mut rules = Vec::new();
        if has_spot {
            let value = self
                .rest
                .send_public_get("/api/v2/spot/public/symbols", &HashMap::new())
                .await?;
            rules.extend(parse_symbol_rules(
                &self.exchange_id,
                MarketType::Spot,
                &value,
            )?);
        }
        if has_perp {
            let mut params = HashMap::new();
            params.insert("productType".to_string(), "USDT-FUTURES".to_string());
            let value = self
                .rest
                .send_public_get("/api/v2/mix/market/contracts", &params)
                .await?;
            rules.extend(parse_symbol_rules(
                &self.exchange_id,
                MarketType::Perpetual,
                &value,
            )?);
        }
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
        self.ensure_supported_market(request.symbol.market_type)?;
        let depth = request.depth.unwrap_or(100).clamp(1, 100);
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), normalize_symbol(&request.symbol)?);
        params.insert("limit".to_string(), depth.to_string());
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/api/v2/spot/market/orderbook",
            MarketType::Perpetual => {
                params.insert("productType".to_string(), "USDT-FUTURES".to_string());
                "/api/v2/mix/market/orderbook"
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self.rest.send_public_get(endpoint, &params).await?;
        parse_orderbook_response(
            &self.exchange_id,
            request.symbol,
            request.context.request_id,
            &value,
        )
    }
}

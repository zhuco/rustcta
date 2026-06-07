use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_depth, normalize_whitebit_symbol, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::WhiteBitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl WhiteBitGatewayAdapter {
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
        let wants_spot = request.symbols.is_empty()
            || request
                .symbols
                .iter()
                .any(|symbol| symbol.market_type == MarketType::Spot);
        let wants_perp = request.symbols.is_empty()
            || request
                .symbols
                .iter()
                .any(|symbol| symbol.market_type == MarketType::Perpetual);
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
        }

        let requested = request
            .symbols
            .iter()
            .map(|symbol| normalize_whitebit_symbol(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut rules = Vec::new();
        if wants_spot {
            let response = self
                .rest
                .send_public_request("/api/v4/public/markets", &HashMap::new())
                .await?;
            rules.extend(parse_symbol_rules(&self.exchange_id, &response)?);
        }
        if wants_perp {
            let response = self
                .rest
                .send_public_request("/api/v4/public/futures", &HashMap::new())
                .await?;
            rules.extend(parse_symbol_rules(&self.exchange_id, &response)?);
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
        let depth = normalize_depth(request.depth.unwrap_or(5));
        let mut params = HashMap::new();
        params.insert("limit".to_string(), depth.to_string());
        params.insert("level".to_string(), "0".to_string());
        let market = normalize_whitebit_symbol(&request.symbol.exchange_symbol.symbol)?;
        let value = self
            .rest
            .send_public_request(&format!("/api/v4/public/orderbook/{market}"), &params)
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

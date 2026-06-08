use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_bitget_symbol, normalize_depth, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::{BitgetGatewayAdapter, BITGET_PERP_PRODUCT_TYPE};
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BitgetGatewayAdapter {
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
            self.ensure_supported_market_type(symbol.market_type)?;
        }

        let requested = request
            .symbols
            .iter()
            .map(|symbol| normalize_bitget_symbol(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let wants_spot = request.symbols.is_empty()
            || request
                .symbols
                .iter()
                .any(|symbol| symbol.market_type == MarketType::Spot);
        let wants_perpetual = request.symbols.is_empty()
            || request
                .symbols
                .iter()
                .any(|symbol| symbol.market_type == MarketType::Perpetual);
        let mut rules = Vec::new();
        if wants_spot {
            let response = self
                .rest
                .send_public_request("/api/v2/spot/public/symbols", &HashMap::new())
                .await?;
            rules.extend(parse_symbol_rules(
                &self.exchange_id,
                MarketType::Spot,
                &response,
            )?);
        }
        if wants_perpetual {
            let mut params = HashMap::new();
            params.insert(
                "productType".to_string(),
                BITGET_PERP_PRODUCT_TYPE.to_string(),
            );
            let response = self
                .rest
                .send_public_request("/api/v2/mix/market/contracts", &params)
                .await?;
            rules.extend(parse_symbol_rules(
                &self.exchange_id,
                MarketType::Perpetual,
                &response,
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
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let depth = normalize_depth(request.depth.unwrap_or(5));
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bitget_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert("limit".to_string(), depth.to_string());
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => {
                params.insert("type".to_string(), "step0".to_string());
                "/api/v2/spot/market/orderbook"
            }
            MarketType::Perpetual => {
                params.insert(
                    "productType".to_string(),
                    BITGET_PERP_PRODUCT_TYPE.to_string(),
                );
                "/api/v2/mix/market/orderbook"
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        let value = self.rest.send_public_request(endpoint, &params).await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

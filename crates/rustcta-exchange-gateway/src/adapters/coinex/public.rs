use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    normalize_coinex_symbol, normalize_depth, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::CoinExGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinExGatewayAdapter {
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
        let market_type = request
            .symbols
            .first()
            .map(|symbol| symbol.market_type)
            .unwrap_or(rustcta_types::MarketType::Spot);
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_market_type(symbol.market_type)?;
            if symbol.market_type != market_type {
                return Err(rustcta_exchange_api::ExchangeApiError::InvalidRequest {
                    message: "coinex.get_symbol_rules does not support mixed market types"
                        .to_string(),
                });
            }
        }

        let endpoint = coinex_product_path(market_type, "market")?;
        let response = self
            .rest
            .send_public_request(endpoint, &HashMap::new())
            .await?;
        let requested = request
            .symbols
            .iter()
            .map(|symbol| normalize_coinex_symbol(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut rules = parse_symbol_rules(&self.exchange_id, market_type, &response)?;
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
        self.ensure_market_type(request.symbol.market_type)?;
        let depth = normalize_depth(request.depth.unwrap_or(5));
        let mut params = HashMap::new();
        params.insert(
            "market".to_string(),
            normalize_coinex_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert("limit".to_string(), depth.to_string());
        params.insert("interval".to_string(), "0".to_string());
        let value = self
            .rest
            .send_public_request(
                coinex_product_path(request.symbol.market_type, "depth")?,
                &params,
            )
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

pub(super) fn coinex_product_path(
    market_type: rustcta_types::MarketType,
    suffix: &str,
) -> ExchangeApiResult<&'static str> {
    match (market_type, suffix) {
        (rustcta_types::MarketType::Spot, "market") => Ok("/spot/market"),
        (rustcta_types::MarketType::Spot, "depth") => Ok("/spot/depth"),
        (rustcta_types::MarketType::Perpetual, "market") => Ok("/futures/market"),
        (rustcta_types::MarketType::Perpetual, "depth") => Ok("/futures/depth"),
        _ => Err(rustcta_exchange_api::ExchangeApiError::Unsupported {
            operation: "coinex.unsupported_market_type",
        }),
    }
}

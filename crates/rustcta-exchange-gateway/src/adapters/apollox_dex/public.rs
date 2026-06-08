use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    normalize_apollox_symbol, normalize_depth, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::ApolloxDexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl ApolloxDexGatewayAdapter {
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
            self.ensure_perpetual(symbol.market_type)?;
        }
        if !self.config.enabled_public_rest {
            return Err(ExchangeApiError::Unsupported {
                operation: "apollox_dex.public_rest_disabled",
            });
        }
        let response = self
            .rest
            .send_public_get(
                super::transport::ApolloxDexRest::exchange_info_path(),
                &HashMap::new(),
            )
            .await?;
        let requested = request
            .symbols
            .iter()
            .map(|symbol| symbol.exchange_symbol.symbol.clone())
            .collect::<Vec<_>>();
        let mut rules = parse_symbol_rules(&self.exchange_id, &response)?;
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
        self.ensure_perpetual(request.symbol.market_type)?;
        if !self.config.enabled_public_rest {
            return Err(ExchangeApiError::Unsupported {
                operation: "apollox_dex.public_rest_disabled",
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_apollox_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert(
            "limit".to_string(),
            normalize_depth(request.depth.unwrap_or(100)).to_string(),
        );
        let response = self
            .rest
            .send_public_get(super::transport::ApolloxDexRest::depth_path(), &params)
            .await?;
        let order_book =
            parse_orderbook_snapshot(&self.exchange_id, request.symbol.clone(), &response)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order_book,
        })
    }
}

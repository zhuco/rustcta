use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{normalize_market_id, parse_orderbook_snapshot, parse_symbol_rules};
use super::BtcMarketsGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BtcMarketsGatewayAdapter {
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
        }
        let value = self.rest.public_get("/v3/markets", &HashMap::new()).await?;
        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            rules: parse_symbol_rules(&self.exchange_id, &request.symbols, &value)?,
        })
    }

    pub(super) async fn get_order_book_impl(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let market_id = normalize_market_id(&request.symbol.exchange_symbol.symbol)?;
        let level = match request.depth.unwrap_or(50) {
            0..=50 => "1",
            _ => "2",
        };
        let mut params = HashMap::new();
        params.insert("level".to_string(), level.to_string());
        let value = self
            .rest
            .public_get(&format!("/v3/markets/{market_id}/orderbook"), &params)
            .await?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book: parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?,
        })
    }
}

pub(super) fn ensure_aud_spot_market(
    symbol: &rustcta_exchange_api::SymbolScope,
) -> ExchangeApiResult<()> {
    if symbol.market_type != MarketType::Spot {
        return Err(rustcta_exchange_api::ExchangeApiError::Unsupported {
            operation: "btcmarkets.unsupported_market_type",
        });
    }
    if let Some(canonical) = &symbol.canonical_symbol {
        if canonical.quote_asset() != "AUD" {
            return Err(rustcta_exchange_api::ExchangeApiError::Unsupported {
                operation: "btcmarkets.non_aud_spot_market",
            });
        }
    }
    Ok(())
}

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse, OrderBookRequest,
    OrderBookResponse, SymbolRulesRequest, SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    parse_hibachi_fee_snapshots, parse_hibachi_orderbook_snapshot, parse_hibachi_symbol_rules,
};
use super::HibachiGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl HibachiGatewayAdapter {
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
        if !self.config.enabled_public_rest {
            return Err(ExchangeApiError::Unsupported {
                operation: "hibachi.public_rest_disabled",
            });
        }
        let response = self
            .rest
            .send_data_get("/market/exchange-info", &[])
            .await?;
        let requested = request
            .symbols
            .iter()
            .map(|symbol| symbol.exchange_symbol.symbol.clone())
            .collect::<Vec<_>>();
        let mut rules = parse_hibachi_symbol_rules(&self.exchange_id, &response)?;
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
        if !self.config.enabled_public_rest {
            return Err(ExchangeApiError::Unsupported {
                operation: "hibachi.public_rest_disabled",
            });
        }
        let depth = request.depth.unwrap_or(100).to_string();
        let response = self
            .rest
            .send_data_get(
                "/market/data/orderbook",
                &[
                    ("symbol", request.symbol.exchange_symbol.symbol.clone()),
                    ("depth", depth),
                    ("granularity", "0.01".to_string()),
                ],
            )
            .await?;
        let order_book =
            parse_hibachi_orderbook_snapshot(&self.exchange_id, request.symbol, &response)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        if !self.config.enabled_public_rest {
            return Err(ExchangeApiError::Unsupported {
                operation: "hibachi.public_rest_disabled",
            });
        }
        let response = self
            .rest
            .send_data_get("/market/exchange-info", &[])
            .await?;
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_hibachi_fee_snapshots(&request.symbols, &response),
        })
    }
}

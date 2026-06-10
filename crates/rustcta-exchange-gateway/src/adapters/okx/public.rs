use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FundingRatesRequest, FundingRatesResponse,
    OrderBookRequest, OrderBookResponse, SymbolRulesRequest, SymbolRulesResponse,
    EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    is_okx_derivative_market, normalize_depth, normalize_okx_symbol_for_market, okx_inst_type,
    parse_orderbook_snapshot, parse_symbol_rules,
};
use super::types::parse_funding_rate;
use super::OkxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl OkxGatewayAdapter {
    pub(super) async fn get_symbol_rules_public_rest(
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
                    message: "okx.get_symbol_rules does not support mixed market types".to_string(),
                });
            }
        }

        let mut params = HashMap::new();
        params.insert(
            "instType".to_string(),
            okx_inst_type(market_type)?.to_string(),
        );
        let response = self
            .rest
            .send_public_request("/api/v5/public/instruments", &params)
            .await?;
        let requested = request
            .symbols
            .iter()
            .map(|symbol| {
                normalize_okx_symbol_for_market(&symbol.exchange_symbol.symbol, symbol.market_type)
            })
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

    pub(super) async fn get_order_book_public_rest(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        let depth = normalize_depth(request.depth.unwrap_or(5));
        let mut params = HashMap::new();
        params.insert(
            "instId".to_string(),
            normalize_okx_symbol_for_market(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?,
        );
        params.insert("sz".to_string(), depth.to_string());
        let value = self
            .rest
            .send_public_request("/api/v5/market/books", &params)
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }

    pub(super) async fn get_funding_rates_public_rest(
        &self,
        request: FundingRatesRequest,
    ) -> ExchangeApiResult<FundingRatesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "okx.get_funding_rates requires at least one symbol".to_string(),
            });
        }
        let mut rates = Vec::with_capacity(request.symbols.len());
        for symbol in request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_market_type(symbol.market_type)?;
            if !is_okx_derivative_market(symbol.market_type) {
                return Err(ExchangeApiError::Unsupported {
                    operation: self.profile_operation(
                        "okx.get_funding_rates_spot_unsupported",
                        "okxus.get_funding_rates_unsupported",
                        "myokx.get_funding_rates_unsupported",
                    ),
                });
            }
            let mut params = HashMap::new();
            params.insert(
                "instId".to_string(),
                normalize_okx_symbol_for_market(
                    &symbol.exchange_symbol.symbol,
                    symbol.market_type,
                )?,
            );
            let value = self
                .rest
                .send_public_request("/api/v5/public/funding-rate", &params)
                .await?;
            rates.push(parse_funding_rate(&self.exchange_id, symbol, &value)?);
        }
        Ok(FundingRatesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            rates,
        })
    }
}

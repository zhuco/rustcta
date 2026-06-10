use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FundingRatesRequest, FundingRatesResponse,
    OrderBookRequest, OrderBookResponse, SymbolRulesRequest, SymbolRulesResponse,
    EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{
    normalize_depth, normalize_mexc_symbol_for_market, parse_funding_rate_snapshot,
    parse_orderbook_snapshot, parse_symbol_rules,
};
use super::MexcGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl MexcGatewayAdapter {
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
                    message: "mexc.get_symbol_rules does not support mixed market types"
                        .to_string(),
                });
            }
        }

        let response = if market_type == rustcta_types::MarketType::Perpetual {
            self.rest
                .send_contract_public_request("/api/v1/contract/detail", &HashMap::new())
                .await?
        } else {
            self.rest
                .send_public_request("/api/v3/exchangeInfo", &HashMap::new())
                .await?
        };
        let requested = request
            .symbols
            .iter()
            .map(|symbol| {
                normalize_mexc_symbol_for_market(&symbol.exchange_symbol.symbol, symbol.market_type)
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

    pub(super) async fn get_order_book_impl(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        let depth = normalize_depth(request.depth.unwrap_or(5));
        let mut params = HashMap::new();
        let normalized_symbol = normalize_mexc_symbol_for_market(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?;
        let value = if request.symbol.market_type == rustcta_types::MarketType::Perpetual {
            params.insert("limit".to_string(), depth.to_string());
            self.rest
                .send_contract_public_request(
                    &format!("/api/v1/contract/depth/{normalized_symbol}"),
                    &params,
                )
                .await?
        } else {
            params.insert("symbol".to_string(), normalized_symbol);
            params.insert("limit".to_string(), depth.to_string());
            self.rest
                .send_public_request("/api/v3/depth", &params)
                .await?
        };
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }

    pub(super) async fn get_funding_rates_impl(
        &self,
        request: FundingRatesRequest,
    ) -> ExchangeApiResult<FundingRatesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "mexc.get_funding_rates requires at least one symbol".to_string(),
            });
        }
        let mut rates = Vec::with_capacity(request.symbols.len());
        for symbol in request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            if symbol.market_type != rustcta_types::MarketType::Perpetual {
                return Err(ExchangeApiError::Unsupported {
                    operation: "mexc.get_funding_rates_non_perpetual",
                });
            }
            let normalized_symbol = normalize_mexc_symbol_for_market(
                &symbol.exchange_symbol.symbol,
                symbol.market_type,
            )?;
            let value = self
                .rest
                .send_contract_public_request(
                    &format!("/api/v1/contract/funding_rate/{normalized_symbol}"),
                    &HashMap::new(),
                )
                .await?;
            rates.push(parse_funding_rate_snapshot(
                &self.exchange_id,
                symbol,
                &value,
            )?);
        }
        Ok(FundingRatesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            rates,
        })
    }
}

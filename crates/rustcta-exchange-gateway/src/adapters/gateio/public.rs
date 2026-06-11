use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FundingRatesRequest, FundingRatesResponse,
    OrderBookRequest, OrderBookResponse, SymbolRulesRequest, SymbolRulesResponse,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_depth, normalize_gateio_symbol, parse_funding_rate_snapshot,
    parse_orderbook_snapshot, parse_perpetual_symbol_rules, parse_symbol_rules,
};
use super::GateIoGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl GateIoGatewayAdapter {
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

        let wants_spot = request.symbols.is_empty()
            || request
                .symbols
                .iter()
                .any(|symbol| symbol.market_type == rustcta_types::MarketType::Spot);
        let wants_perpetual = request
            .symbols
            .iter()
            .any(|symbol| symbol.market_type == rustcta_types::MarketType::Perpetual);
        let requested_spot = request
            .symbols
            .iter()
            .filter(|symbol| symbol.market_type == rustcta_types::MarketType::Spot)
            .map(|symbol| normalize_gateio_symbol(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let requested_perpetual = request
            .symbols
            .iter()
            .filter(|symbol| symbol.market_type == rustcta_types::MarketType::Perpetual)
            .map(|symbol| normalize_gateio_symbol(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;

        let mut rules = Vec::new();
        if wants_spot {
            let response = self
                .rest
                .send_public_request("/spot/currency_pairs", &HashMap::new())
                .await?;
            let mut spot_rules = parse_symbol_rules(&self.exchange_id, &response)?;
            if !requested_spot.is_empty() {
                spot_rules
                    .retain(|rule| requested_spot.contains(&rule.symbol.exchange_symbol.symbol));
            }
            rules.extend(spot_rules);
        }
        if wants_perpetual {
            let response = self
                .rest
                .send_public_request("/futures/usdt/contracts", &HashMap::new())
                .await?;
            let mut perpetual_rules = parse_perpetual_symbol_rules(&self.exchange_id, &response)?;
            if !requested_perpetual.is_empty() {
                perpetual_rules.retain(|rule| {
                    requested_perpetual.contains(&rule.symbol.exchange_symbol.symbol)
                });
            }
            rules.extend(perpetual_rules);
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
        let endpoint = match request.symbol.market_type {
            rustcta_types::MarketType::Spot => {
                params.insert(
                    "currency_pair".to_string(),
                    normalize_gateio_symbol(&request.symbol.exchange_symbol.symbol)?,
                );
                "/spot/order_book"
            }
            rustcta_types::MarketType::Perpetual => {
                params.insert(
                    "contract".to_string(),
                    normalize_gateio_symbol(&request.symbol.exchange_symbol.symbol)?,
                );
                params.insert("with_id".to_string(), "true".to_string());
                "/futures/usdt/order_book"
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self.rest.send_public_request(endpoint, &params).await?;
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
                message: "gateio.get_funding_rates requires at least one symbol".to_string(),
            });
        }
        let mut rates = Vec::with_capacity(request.symbols.len());
        for symbol in request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            if symbol.market_type != MarketType::Perpetual {
                return Err(ExchangeApiError::Unsupported {
                    operation: "gateio.get_funding_rates_spot_unsupported",
                });
            }
            let contract = normalize_gateio_symbol(&symbol.exchange_symbol.symbol)?;
            let value = self
                .rest
                .send_public_request(
                    &format!("/futures/usdt/contracts/{contract}"),
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

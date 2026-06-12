use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FundingRatesRequest, FundingRatesResponse,
    OrderBookRequest, OrderBookResponse, SymbolRulesRequest, SymbolRulesResponse,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_bybit_symbol, parse_funding_rate_snapshot, parse_orderbook_snapshot,
    parse_symbol_rules,
};
use super::BybitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BybitGatewayAdapter {
    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        let market_type = request
            .symbols
            .first()
            .map(|symbol| symbol.market_type)
            .unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market_type(market_type)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        let mut params = HashMap::new();
        params.insert(
            "category".to_string(),
            bybit_category(market_type).to_string(),
        );
        if request.symbols.len() == 1 {
            params.insert(
                "symbol".to_string(),
                normalize_bybit_symbol(&request.symbols[0].exchange_symbol.symbol)?,
            );
        }
        let value = self
            .rest
            .send_public_get("/v5/market/instruments-info", &params)
            .await?;
        let mut rules = parse_symbol_rules(&self.exchange_id, market_type, &value)?;
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| normalize_bybit_symbol(&symbol.exchange_symbol.symbol))
                .collect::<ExchangeApiResult<Vec<_>>>()?;
            rules.retain(|rule| requested.contains(&rule.symbol.exchange_symbol.symbol));
        }
        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
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
        let mut params = HashMap::new();
        params.insert(
            "category".to_string(),
            bybit_category(request.symbol.market_type).to_string(),
        );
        params.insert(
            "symbol".to_string(),
            normalize_bybit_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert(
            "limit".to_string(),
            request.depth.unwrap_or(50).min(200).to_string(),
        );
        let value = self
            .rest
            .send_public_get("/v5/market/orderbook", &params)
            .await?;
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
                message: "bybit.get_funding_rates requires at least one symbol".to_string(),
            });
        }
        let mut rates = Vec::with_capacity(request.symbols.len());
        for symbol in request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            if symbol.market_type == MarketType::Spot {
                return Err(ExchangeApiError::Unsupported {
                    operation: "bybit.get_funding_rates_spot_unsupported",
                });
            }
            let mut params = HashMap::new();
            params.insert(
                "category".to_string(),
                bybit_category(symbol.market_type).to_string(),
            );
            params.insert(
                "symbol".to_string(),
                normalize_bybit_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let ticker_value = self
                .rest
                .send_public_get("/v5/market/tickers", &params)
                .await?;
            let mut history_params = params.clone();
            history_params.insert("limit".to_string(), "1".to_string());
            let history_value = self
                .rest
                .send_public_get("/v5/market/funding/history", &history_params)
                .await
                .ok();
            rates.push(parse_funding_rate_snapshot(
                &self.exchange_id,
                symbol,
                &ticker_value,
                history_value.as_ref(),
            )?);
        }
        Ok(FundingRatesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            rates,
        })
    }
}

pub(super) fn bybit_category(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "spot",
        MarketType::Perpetual | MarketType::Futures => "linear",
        _ => "linear",
    }
}

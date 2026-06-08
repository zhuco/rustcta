use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse, OrderBookRequest,
    OrderBookResponse, SymbolRulesRequest, SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::Value;

use super::parser::{
    normalize_bydfi_symbol, normalize_depth, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::private_parser::parse_fee_snapshots;
use super::BydfiGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BydfiGatewayAdapter {
    pub async fn get_bydfi_mark_price(&self, symbol: &str) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), normalize_bydfi_symbol(symbol)?);
        self.rest
            .send_public_get("/v1/fapi/market/mark_price", &params)
            .await
    }

    pub async fn get_bydfi_funding_rate(&self, symbol: Option<&str>) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            params.insert("symbol".to_string(), normalize_bydfi_symbol(symbol)?);
        }
        self.rest
            .send_public_get("/v1/fapi/market/funding_rate", &params)
            .await
    }

    pub async fn get_bydfi_funding_rate_history(
        &self,
        symbol: Option<&str>,
        start_time_ms: Option<i64>,
        end_time_ms: Option<i64>,
        limit: Option<u32>,
    ) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            params.insert("symbol".to_string(), normalize_bydfi_symbol(symbol)?);
        }
        if let Some(start_time_ms) = start_time_ms {
            params.insert("startTime".to_string(), start_time_ms.to_string());
        }
        if let Some(end_time_ms) = end_time_ms {
            params.insert("endTime".to_string(), end_time_ms.to_string());
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.min(1000).to_string());
        }
        self.rest
            .send_public_get("/v1/fapi/market/funding_rate_history", &params)
            .await
    }

    pub async fn get_bydfi_risk_limit(&self, symbol: &str) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), normalize_bydfi_symbol(symbol)?);
        self.rest
            .send_public_get("/v1/fapi/market/risk_limit", &params)
            .await
    }

    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_perpetual(symbol.market_type, "bydfi.spot_symbol_rules_unsupported")?;
        }
        let response = self
            .rest
            .send_public_get("/v1/fapi/market/exchange_info", &HashMap::new())
            .await?;
        let mut rules = parse_symbol_rules(&self.exchange_id, &response)?;
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| normalize_bydfi_symbol(&symbol.exchange_symbol.symbol))
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
        self.ensure_perpetual(
            request.symbol.market_type,
            "bydfi.spot_order_book_unsupported",
        )?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bydfi_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert(
            "limit".to_string(),
            normalize_depth(request.depth.unwrap_or(50)),
        );
        let value = self
            .rest
            .send_public_get("/v1/fapi/market/depth", &params)
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
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
            self.ensure_perpetual(symbol.market_type, "bydfi.spot_fees_unsupported")?;
        }
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "BYDFi get_fees requires at least one symbol".to_string(),
            });
        }
        let response = self
            .rest
            .send_public_get("/v1/fapi/market/exchange_info", &HashMap::new())
            .await?;
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_snapshots(&request.symbols, &response)?,
        })
    }
}

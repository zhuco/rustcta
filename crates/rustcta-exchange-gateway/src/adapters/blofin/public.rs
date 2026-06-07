#![allow(dead_code)]

use std::collections::HashMap;

use super::parser::{
    normalize_blofin_symbol, normalize_depth, parse_orderbook_snapshot, parse_symbol_rules,
};
use super::BlofinGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use rustcta_exchange_api::{
    ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::Value;

impl BlofinGatewayAdapter {
    pub async fn get_blofin_tickers(&self, inst_id: Option<&str>) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        optional_symbol_param(&mut params, "instId", inst_id)?;
        self.rest
            .send_public_get("/api/v1/market/tickers", &params)
            .await
    }

    pub async fn get_blofin_trades(
        &self,
        inst_id: &str,
        limit: Option<u32>,
    ) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        params.insert("instId".to_string(), normalize_blofin_symbol(inst_id)?);
        optional_limit_param(&mut params, limit, 100);
        self.rest
            .send_public_get("/api/v1/market/trades", &params)
            .await
    }

    pub async fn get_blofin_mark_price(&self, inst_id: Option<&str>) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        optional_symbol_param(&mut params, "instId", inst_id)?;
        self.rest
            .send_public_get("/api/v1/market/mark-price", &params)
            .await
    }

    pub async fn get_blofin_funding_rate(&self, inst_id: Option<&str>) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        optional_symbol_param(&mut params, "instId", inst_id)?;
        self.rest
            .send_public_get("/api/v1/market/funding-rate", &params)
            .await
    }

    pub async fn get_blofin_funding_rate_history(
        &self,
        inst_id: &str,
        before: Option<&str>,
        after: Option<&str>,
        limit: Option<u32>,
    ) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        params.insert("instId".to_string(), normalize_blofin_symbol(inst_id)?);
        optional_str_param(&mut params, "before", before);
        optional_str_param(&mut params, "after", after);
        optional_limit_param(&mut params, limit, 100);
        self.rest
            .send_public_get("/api/v1/market/funding-rate-history", &params)
            .await
    }

    pub async fn get_blofin_candles(
        &self,
        inst_id: &str,
        bar: Option<&str>,
        after: Option<&str>,
        before: Option<&str>,
        limit: Option<u32>,
    ) -> ExchangeApiResult<Value> {
        self.get_blofin_candle_endpoint(
            "/api/v1/market/candles",
            inst_id,
            bar,
            after,
            before,
            limit,
        )
        .await
    }

    pub async fn get_blofin_index_candles(
        &self,
        inst_id: &str,
        bar: Option<&str>,
        after: Option<&str>,
        before: Option<&str>,
        limit: Option<u32>,
    ) -> ExchangeApiResult<Value> {
        self.get_blofin_candle_endpoint(
            "/api/v1/market/index-candles",
            inst_id,
            bar,
            after,
            before,
            limit,
        )
        .await
    }

    pub async fn get_blofin_mark_price_candles(
        &self,
        inst_id: &str,
        bar: Option<&str>,
        after: Option<&str>,
        before: Option<&str>,
        limit: Option<u32>,
    ) -> ExchangeApiResult<Value> {
        self.get_blofin_candle_endpoint(
            "/api/v1/market/mark-price-candles",
            inst_id,
            bar,
            after,
            before,
            limit,
        )
        .await
    }

    pub async fn get_blofin_position_tiers(
        &self,
        inst_id: &str,
        margin_mode: BlofinMarginMode,
    ) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        params.insert("instId".to_string(), normalize_blofin_symbol(inst_id)?);
        params.insert("marginMode".to_string(), margin_mode.as_str().to_string());
        self.rest
            .send_public_get("/api/v1/market/position-tiers", &params)
            .await
    }

    async fn get_blofin_candle_endpoint(
        &self,
        endpoint: &str,
        inst_id: &str,
        bar: Option<&str>,
        after: Option<&str>,
        before: Option<&str>,
        limit: Option<u32>,
    ) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        params.insert("instId".to_string(), normalize_blofin_symbol(inst_id)?);
        optional_str_param(&mut params, "bar", bar);
        optional_str_param(&mut params, "after", after);
        optional_str_param(&mut params, "before", before);
        optional_limit_param(&mut params, limit, 1440);
        self.rest.send_public_get(endpoint, &params).await
    }

    pub(super) async fn get_symbol_rules_impl(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_perpetual(symbol.market_type, "blofin.spot_symbol_rules_unsupported")?;
        }
        let mut params = HashMap::new();
        if request.symbols.len() == 1 {
            params.insert(
                "instId".to_string(),
                normalize_blofin_symbol(&request.symbols[0].exchange_symbol.symbol)?,
            );
        }
        let response = self
            .rest
            .send_public_get("/api/v1/market/instruments", &params)
            .await?;
        let mut rules = parse_symbol_rules(&self.exchange_id, &response)?;
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| normalize_blofin_symbol(&symbol.exchange_symbol.symbol))
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
            "blofin.spot_order_book_unsupported",
        )?;
        let mut params = HashMap::new();
        params.insert(
            "instId".to_string(),
            normalize_blofin_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert(
            "size".to_string(),
            normalize_depth(request.depth.unwrap_or(50)),
        );
        let value = self
            .rest
            .send_public_get("/api/v1/market/books", &params)
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlofinMarginMode {
    Cross,
    Isolated,
}

impl BlofinMarginMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cross => "cross",
            Self::Isolated => "isolated",
        }
    }
}

fn optional_symbol_param(
    params: &mut HashMap<String, String>,
    key: &str,
    value: Option<&str>,
) -> ExchangeApiResult<()> {
    if let Some(value) = value {
        params.insert(key.to_string(), normalize_blofin_symbol(value)?);
    }
    Ok(())
}

fn optional_str_param(params: &mut HashMap<String, String>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        params.insert(key.to_string(), value.to_string());
    }
}

fn optional_limit_param(params: &mut HashMap<String, String>, limit: Option<u32>, max: u32) {
    if let Some(limit) = limit {
        params.insert("limit".to_string(), limit.clamp(1, max).to_string());
    }
}

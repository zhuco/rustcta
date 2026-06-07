#![allow(dead_code)]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_cryptocom_symbol, normalize_depth, parse_candles, parse_orderbook_snapshot,
    parse_public_trades, parse_symbol_rules, parse_ticker_24h, parse_valuations, CryptoComCandle,
    CryptoComPublicTrade, CryptoComTicker24h, CryptoComValuationPoint,
};
use super::CryptoComGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CryptoComGatewayAdapter {
    pub async fn get_public_raw(
        &self,
        method: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        validate_public_method(method)?;
        self.rest.send_public_get(method, params).await
    }

    pub async fn get_ticker_24h(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
    ) -> ExchangeApiResult<CryptoComTicker24h> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_public_market(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "instrument_name".to_string(),
            normalize_cryptocom_symbol(&symbol)?,
        );
        let value = self
            .rest
            .send_public_get("public/get-ticker", &params)
            .await?;
        parse_ticker_24h(&self.exchange_id, symbol, &value)
    }

    pub async fn get_recent_public_trades(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
    ) -> ExchangeApiResult<Vec<CryptoComPublicTrade>> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_public_market(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "instrument_name".to_string(),
            normalize_cryptocom_symbol(&symbol)?,
        );
        let value = self
            .rest
            .send_public_get("public/get-trades", &params)
            .await?;
        parse_public_trades(&self.exchange_id, symbol, &value)
    }

    pub async fn get_candles(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        interval: &str,
        count: Option<u32>,
    ) -> ExchangeApiResult<Vec<CryptoComCandle>> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_public_market(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "instrument_name".to_string(),
            normalize_cryptocom_symbol(&symbol)?,
        );
        params.insert(
            "timeframe".to_string(),
            normalize_candle_interval(interval)?.to_string(),
        );
        if let Some(count) = count {
            params.insert("count".to_string(), count.min(300).to_string());
        }
        let value = self
            .rest
            .send_public_get("public/get-candlestick", &params)
            .await?;
        parse_candles(&self.exchange_id, symbol, interval, &value)
    }

    pub async fn get_valuations(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        valuation_type: &str,
        count: Option<u32>,
    ) -> ExchangeApiResult<Vec<CryptoComValuationPoint>> {
        self.ensure_exchange(&symbol.exchange)?;
        if symbol.market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "cryptocom.spot_valuations",
            });
        }
        let instrument_name = normalize_cryptocom_symbol(&symbol)?;
        let valuation_type = normalize_valuation_type(valuation_type)?;
        let mut params = HashMap::new();
        params.insert("instrument_name".to_string(), instrument_name.clone());
        params.insert("valuation_type".to_string(), valuation_type.to_string());
        if let Some(count) = count {
            params.insert("count".to_string(), count.min(300).to_string());
        }
        let value = self
            .rest
            .send_public_get("public/get-valuations", &params)
            .await?;
        parse_valuations(&self.exchange_id, &instrument_name, valuation_type, &value)
    }

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
            self.ensure_public_market(symbol.market_type)?;
        }

        let response = self
            .rest
            .send_public_get("public/get-instruments", &HashMap::new())
            .await?;
        let requested = request
            .symbols
            .iter()
            .map(normalize_cryptocom_symbol)
            .collect::<ExchangeApiResult<Vec<_>>>()?;
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
        self.ensure_public_market(request.symbol.market_type)?;
        let depth = normalize_depth(request.depth.unwrap_or(10));
        let mut params = HashMap::new();
        params.insert(
            "instrument_name".to_string(),
            normalize_cryptocom_symbol(&request.symbol)?,
        );
        params.insert("depth".to_string(), depth.to_string());
        let value = self
            .rest
            .send_public_get("public/get-book", &params)
            .await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

fn validate_public_method(method: &str) -> ExchangeApiResult<()> {
    if method.starts_with("public/") && !method.contains('?') && !method.contains("..") {
        return Ok(());
    }
    Err(ExchangeApiError::InvalidRequest {
        message: "cryptocom public method must be a public/* method without query text".to_string(),
    })
}

fn normalize_candle_interval(interval: &str) -> ExchangeApiResult<&'static str> {
    match interval.trim().to_ascii_lowercase().as_str() {
        "1m" | "m1" => Ok("1m"),
        "5m" | "m5" => Ok("5m"),
        "15m" | "m15" => Ok("15m"),
        "30m" | "m30" => Ok("30m"),
        "1h" | "h1" => Ok("1h"),
        "4h" | "h4" => Ok("4h"),
        "6h" | "h6" => Ok("6h"),
        "12h" | "h12" => Ok("12h"),
        "1d" | "d1" => Ok("1D"),
        "7d" | "1w" | "w1" => Ok("7D"),
        "14d" | "2w" | "w2" => Ok("14D"),
        "1mth" | "1mo" | "mth1" => Ok("1M"),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported Crypto.com candlestick interval {interval}"),
        }),
    }
}

fn normalize_valuation_type(valuation_type: &str) -> ExchangeApiResult<&'static str> {
    match valuation_type.trim().to_ascii_lowercase().as_str() {
        "index_price" | "index" => Ok("index_price"),
        "mark_price" | "mark" => Ok("mark_price"),
        "funding_hist" | "funding" | "funding_rate" => Ok("funding_hist"),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported Crypto.com valuation_type {valuation_type}"),
        }),
    }
}

#![allow(dead_code)]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_phemex_symbol, parse_funding_history, parse_orderbook_snapshot, parse_perp_candles,
    parse_public_trades, parse_server_time, parse_symbol_rules, parse_ticker_24h,
    parse_tickers_24h, PhemexCandle, PhemexFundingRate, PhemexPublicTrade, PhemexServerTime,
    PhemexTicker24h,
};
use super::PhemexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl PhemexGatewayAdapter {
    pub async fn get_server_time(&self) -> ExchangeApiResult<PhemexServerTime> {
        let value = self
            .rest
            .send_public_request("/public/time", &HashMap::new())
            .await?;
        parse_server_time(&value)
    }

    pub async fn get_products_plus(&self) -> ExchangeApiResult<serde_json::Value> {
        self.rest
            .send_public_request("/public/products-plus", &HashMap::new())
            .await
    }

    pub async fn get_public_raw(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        validate_raw_endpoint(endpoint)?;
        self.rest.send_public_request(endpoint, params).await
    }

    pub async fn get_index_sources(&self) -> ExchangeApiResult<serde_json::Value> {
        self.rest
            .send_public_request("/public/index-sources", &HashMap::new())
            .await
    }

    pub async fn get_full_order_book(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        self.rest.send_public_request("/md/fullbook", &params).await
    }

    pub async fn get_ticker_24h(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
    ) -> ExchangeApiResult<PhemexTicker24h> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        let endpoint = if symbol.market_type == MarketType::Spot {
            "/md/spot/ticker/24hr"
        } else {
            "/md/v2/ticker/24hr"
        };
        let value = self.rest.send_public_request(endpoint, &params).await?;
        parse_ticker_24h(&self.exchange_id, symbol, &value)
    }

    pub async fn get_spot_tickers_24h_all(&self) -> ExchangeApiResult<Vec<PhemexTicker24h>> {
        let value = self
            .rest
            .send_public_request("/md/spot/ticker/24hr/all", &HashMap::new())
            .await?;
        parse_tickers_24h(&self.exchange_id, MarketType::Spot, &value)
    }

    pub async fn get_legacy_ticker_24h_v1(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        self.rest
            .send_public_request("/md/v1/ticker/24hr", &params)
            .await
    }

    pub async fn get_legacy_tickers_24h_v1_all(&self) -> ExchangeApiResult<serde_json::Value> {
        self.rest
            .send_public_request("/md/v1/ticker/24hr/all", &HashMap::new())
            .await
    }

    pub async fn get_perp_tickers_24h_v2_all(&self) -> ExchangeApiResult<serde_json::Value> {
        self.rest
            .send_public_request("/md/v2/ticker/24hr/all", &HashMap::new())
            .await
    }

    pub async fn get_perp_ticker_24h_v3(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
        );
        self.rest
            .send_public_request("/md/v3/ticker/24hr", &params)
            .await
    }

    pub async fn get_perp_tickers_24h_v3_all(&self) -> ExchangeApiResult<serde_json::Value> {
        self.rest
            .send_public_request("/md/v3/ticker/24hr/all", &HashMap::new())
            .await
    }

    pub async fn get_chain_settings(&self, currency: &str) -> ExchangeApiResult<serde_json::Value> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        self.rest
            .send_public_request("/exchange/public/cfg/chain-settings", &params)
            .await
    }

    pub async fn get_real_funding_rates(
        &self,
        symbol: Option<rustcta_exchange_api::SymbolScope>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_perpetual(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_phemex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
            );
        }
        self.rest
            .send_public_request("/contract-biz/public/real-funding-rates", &params)
            .await
    }

    pub async fn get_trader_performance_info(
        &self,
        strategy_ids: &[String],
        page_num: Option<u32>,
        page_size: Option<u32>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let mut params = HashMap::new();
        if !strategy_ids.is_empty() {
            params.insert("strategyIds".to_string(), strategy_ids.join(","));
        }
        if let Some(page_num) = page_num {
            params.insert("pageNum".to_string(), page_num.to_string());
        }
        if let Some(page_size) = page_size {
            params.insert("pageSize".to_string(), page_size.min(200).to_string());
        }
        self.rest
            .send_public_request("/phemex-lb/public/api/trader/performance-info", &params)
            .await
    }

    pub async fn get_recent_public_trades(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
    ) -> ExchangeApiResult<Vec<PhemexPublicTrade>> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        let endpoint = if symbol.market_type == MarketType::Spot {
            "/md/trade"
        } else {
            "/md/v2/trade"
        };
        let value = self.rest.send_public_request(endpoint, &params).await?;
        parse_public_trades(&self.exchange_id, symbol, &value)
    }

    pub async fn get_funding_history(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        limit: Option<u32>,
    ) -> ExchangeApiResult<Vec<PhemexFundingRate>> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
        );
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.min(200).to_string());
        }
        let value = self
            .rest
            .send_public_request("/api-data/public/data/funding-rate-history", &params)
            .await?;
        parse_funding_history(&self.exchange_id, symbol, &value)
    }

    pub async fn get_perp_candles(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        interval: &str,
        limit: Option<u32>,
    ) -> ExchangeApiResult<Vec<PhemexCandle>> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
        );
        params.insert(
            "resolution".to_string(),
            normalize_kline_resolution(interval)?.to_string(),
        );
        params.insert(
            "limit".to_string(),
            limit.unwrap_or(100).min(1000).to_string(),
        );
        let value = self
            .rest
            .send_public_request("/exchange/public/md/v2/kline/last", &params)
            .await?;
        parse_perp_candles(&self.exchange_id, symbol, interval, &value)
    }

    pub async fn get_legacy_contract_candles(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        interval: &str,
        from: i64,
        to: i64,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        params.insert(
            "resolution".to_string(),
            normalize_kline_resolution(interval)?.to_string(),
        );
        params.insert("from".to_string(), from.to_string());
        params.insert("to".to_string(), to.to_string());
        self.rest
            .send_public_request("/exchange/public/md/kline", &params)
            .await
    }

    pub async fn get_perp_candles_v2(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        interval: &str,
        limit: Option<u32>,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
        );
        params.insert(
            "resolution".to_string(),
            normalize_kline_resolution(interval)?.to_string(),
        );
        params.insert(
            "limit".to_string(),
            limit.unwrap_or(100).min(1000).to_string(),
        );
        self.rest
            .send_public_request("/exchange/public/md/v2/kline", &params)
            .await
    }

    pub async fn get_perp_candles_v2_list(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        interval: &str,
        from: i64,
        to: i64,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
        );
        params.insert(
            "resolution".to_string(),
            normalize_kline_resolution(interval)?.to_string(),
        );
        params.insert("from".to_string(), from.to_string());
        params.insert("to".to_string(), to.to_string());
        self.rest
            .send_public_request("/exchange/public/md/v2/kline/list", &params)
            .await
    }

    pub async fn get_nomics_trades(
        &self,
        market: &str,
        since: Option<i64>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let mut params = HashMap::new();
        params.insert("market".to_string(), non_empty("market", market)?);
        if let Some(since) = since {
            params.insert("since".to_string(), since.to_string());
        }
        self.rest
            .send_public_request("/exchange/public/nomics/trades", &params)
            .await
    }

    pub async fn get_spot_candles(
        &self,
        _symbol: rustcta_exchange_api::SymbolScope,
        _interval: &str,
        _limit: Option<u32>,
    ) -> ExchangeApiResult<Vec<PhemexCandle>> {
        Err(ExchangeApiError::Unsupported {
            operation: "phemex.spot_rest_candles",
        })
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
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        let response = self
            .rest
            .send_public_request("/public/products", &HashMap::new())
            .await?;
        let requested = request
            .symbols
            .iter()
            .map(|symbol| {
                normalize_phemex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)
                    .map(|normalized| (symbol.market_type, normalized))
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut rules = parse_symbol_rules(&self.exchange_id, &response)?;
        if !requested.is_empty() {
            rules.retain(|rule| {
                requested.contains(&(
                    rule.symbol.market_type,
                    rule.symbol.exchange_symbol.symbol.clone(),
                ))
            });
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
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?,
        );
        let endpoint = if request.symbol.market_type == MarketType::Spot {
            "/md/orderbook"
        } else {
            "/md/v2/orderbook"
        };
        let value = self.rest.send_public_request(endpoint, &params).await?;
        let order_book = parse_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?;
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("{field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn validate_raw_endpoint(endpoint: &str) -> ExchangeApiResult<()> {
    if endpoint.starts_with('/') && !endpoint.contains('?') && !endpoint.contains("..") {
        return Ok(());
    }
    Err(ExchangeApiError::InvalidRequest {
        message: "phemex raw endpoint must be an absolute path without query text".to_string(),
    })
}

fn normalize_kline_resolution(interval: &str) -> ExchangeApiResult<&'static str> {
    match interval.trim().to_ascii_lowercase().as_str() {
        "1m" | "60" => Ok("60"),
        "5m" | "300" => Ok("300"),
        "15m" | "900" => Ok("900"),
        "30m" | "1800" => Ok("1800"),
        "1h" | "3600" => Ok("3600"),
        "4h" | "14400" => Ok("14400"),
        "1d" | "86400" => Ok("86400"),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported Phemex kline interval {interval}"),
        }),
    }
}

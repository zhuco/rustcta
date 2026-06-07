#![allow(dead_code)]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_contract_depth, normalize_contract_symbol, normalize_spot_depth,
    normalize_spot_symbol, parse_contract_orderbook_snapshot, parse_contract_symbol_rules,
    parse_spot_book_ticker, parse_spot_currency_pairs, parse_spot_klines,
    parse_spot_orderbook_snapshot, parse_spot_public_trades, parse_spot_server_time,
    parse_spot_symbol_rules, parse_spot_ticker, LBankBookTicker, LBankKline, LBankPublicTrade,
    LBankTicker,
};
use super::LBankGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl LBankGatewayAdapter {
    pub async fn get_spot_server_time(&self) -> ExchangeApiResult<i64> {
        let response = self
            .rest
            .send_spot_public_get("/v2/timestamp.do", &HashMap::new())
            .await?;
        parse_spot_server_time(&self.exchange_id, &response)
    }

    pub async fn get_spot_currency_pairs(&self) -> ExchangeApiResult<Vec<String>> {
        let response = self
            .rest
            .send_spot_public_get("/v2/currencyPairs.do", &HashMap::new())
            .await?;
        parse_spot_currency_pairs(&self.exchange_id, &response)
    }

    pub async fn get_spot_ticker(&self, symbol: &str) -> ExchangeApiResult<LBankTicker> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), normalize_spot_symbol(symbol)?);
        let response = self
            .rest
            .send_spot_public_get("/v2/ticker.do", &params)
            .await?;
        parse_spot_ticker(&self.exchange_id, &response)
    }

    pub async fn get_spot_book_ticker(&self, symbol: &str) -> ExchangeApiResult<LBankBookTicker> {
        let normalized_symbol = normalize_spot_symbol(symbol)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), normalized_symbol.clone());
        let response = self
            .rest
            .send_spot_public_get("/v2/supplement/ticker/bookTicker.do", &params)
            .await?;
        parse_spot_book_ticker(&self.exchange_id, &normalized_symbol, &response)
    }

    pub async fn get_spot_public_trades(
        &self,
        symbol: &str,
        limit: Option<u32>,
        since_ms: Option<i64>,
    ) -> ExchangeApiResult<Vec<LBankPublicTrade>> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), normalize_spot_symbol(symbol)?);
        params.insert(
            "size".to_string(),
            limit.unwrap_or(60).clamp(1, 600).to_string(),
        );
        if let Some(since_ms) = since_ms {
            params.insert("time".to_string(), since_ms.to_string());
        }
        let response = self
            .rest
            .send_spot_public_get("/v2/trades.do", &params)
            .await?;
        parse_spot_public_trades(&self.exchange_id, &response)
    }

    pub async fn get_spot_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: u32,
        start_time_s: i64,
    ) -> ExchangeApiResult<Vec<LBankKline>> {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), normalize_spot_symbol(symbol)?);
        params.insert(
            "type".to_string(),
            normalize_rest_kline_interval(interval)?.to_string(),
        );
        params.insert("size".to_string(), limit.clamp(1, 2880).to_string());
        params.insert("time".to_string(), start_time_s.to_string());
        let response = self
            .rest
            .send_spot_public_get("/v2/kline.do", &params)
            .await?;
        parse_spot_klines(&self.exchange_id, &response)
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
        let market_type = request
            .symbols
            .first()
            .map(|symbol| symbol.market_type)
            .unwrap_or(MarketType::Spot);
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_market_type(symbol.market_type)?;
            if symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "lbank get_symbol_rules requires one market_type per request"
                        .to_string(),
                });
            }
        }

        let mut rules = match market_type {
            MarketType::Spot => {
                let mut params = HashMap::new();
                if let Some(symbol) = request.symbols.first() {
                    params.insert(
                        "symbol".to_string(),
                        normalize_spot_symbol(&symbol.exchange_symbol.symbol)?,
                    );
                }
                let response = self
                    .rest
                    .send_spot_public_get("/v2/accuracy.do", &params)
                    .await?;
                parse_spot_symbol_rules(&self.exchange_id, &response)?
            }
            MarketType::Perpetual => {
                let mut params = HashMap::new();
                params.insert(
                    "productGroup".to_string(),
                    self.config.contract_product_group.clone(),
                );
                let response = self
                    .rest
                    .send_contract_public_get("/cfd/openApi/v1/pub/instrument", &params)
                    .await?;
                parse_contract_symbol_rules(&self.exchange_id, &response)?
            }
            _ => return self.unsupported("lbank.get_symbol_rules.market_type"),
        };
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| match market_type {
                    MarketType::Spot => normalize_spot_symbol(&symbol.exchange_symbol.symbol),
                    MarketType::Perpetual => {
                        normalize_contract_symbol(&symbol.exchange_symbol.symbol)
                    }
                    _ => Ok(symbol.exchange_symbol.symbol.clone()),
                })
                .collect::<ExchangeApiResult<Vec<_>>>()?;
            rules.retain(|rule| {
                requested.iter().any(|requested| {
                    rule.symbol
                        .exchange_symbol
                        .symbol
                        .eq_ignore_ascii_case(requested)
                })
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
        self.ensure_market_type(request.symbol.market_type)?;
        match request.symbol.market_type {
            MarketType::Spot => {
                let mut params = HashMap::new();
                params.insert(
                    "symbol".to_string(),
                    normalize_spot_symbol(&request.symbol.exchange_symbol.symbol)?,
                );
                params.insert(
                    "size".to_string(),
                    normalize_spot_depth(request.depth.unwrap_or(20)).to_string(),
                );
                let response = self
                    .rest
                    .send_spot_public_get("/v2/depth.do", &params)
                    .await?;
                let order_book =
                    parse_spot_orderbook_snapshot(&self.exchange_id, request.symbol, &response)?;
                Ok(OrderBookResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(
                        self.exchange_id.clone(),
                        request.context.request_id,
                    ),
                    order_book,
                })
            }
            MarketType::Perpetual => {
                let mut params = HashMap::new();
                params.insert(
                    "symbol".to_string(),
                    normalize_contract_symbol(&request.symbol.exchange_symbol.symbol)?,
                );
                params.insert(
                    "depth".to_string(),
                    normalize_contract_depth(request.depth.unwrap_or(20)).to_string(),
                );
                let response = self
                    .rest
                    .send_contract_public_get("/cfd/openApi/v1/pub/marketOrder", &params)
                    .await?;
                let order_book = parse_contract_orderbook_snapshot(
                    &self.exchange_id,
                    request.symbol,
                    &response,
                )?;
                Ok(OrderBookResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(
                        self.exchange_id.clone(),
                        request.context.request_id,
                    ),
                    order_book,
                })
            }
            _ => self.unsupported("lbank.get_order_book.market_type"),
        }
    }
}

fn normalize_rest_kline_interval(interval: &str) -> ExchangeApiResult<&'static str> {
    match interval.trim() {
        "1m" | "1min" | "minute1" => Ok("minute1"),
        "5m" | "5min" | "minute5" => Ok("minute5"),
        "15m" | "15min" | "minute15" => Ok("minute15"),
        "30m" | "30min" | "minute30" => Ok("minute30"),
        "1h" | "1hr" | "hour1" => Ok("hour1"),
        "4h" | "4hr" | "hour4" => Ok("hour4"),
        "8h" | "8hr" | "hour8" => Ok("hour8"),
        "12h" | "12hr" | "hour12" => Ok("hour12"),
        "1d" | "day" | "day1" => Ok("day1"),
        "1w" | "week" | "week1" => Ok("week1"),
        "1M" | "month" | "month1" => Ok("month1"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "lbank.rest_kline_interval",
        }),
    }
}

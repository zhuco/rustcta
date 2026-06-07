#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderBookRequest, OrderBookResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;

use super::parser::{
    normalize_depth, normalize_spot_symbol, parse_funding_rate, parse_klines, parse_server_time,
    parse_spot_orderbook_snapshot, parse_spot_public_trades, parse_spot_symbol_rules,
    parse_swap_orderbook_snapshot, parse_swap_public_trades, parse_swap_symbol_rules, parse_ticker,
    swap_depth_instrument, TapbitFundingRate, TapbitKline, TapbitPublicTrade, TapbitTicker,
};
use super::TapbitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl TapbitGatewayAdapter {
    pub async fn get_spot_server_time(&self) -> ExchangeApiResult<i64> {
        let value = self
            .rest
            .send_spot_public_get("/api/spot/instruments/current/timestamp", &HashMap::new())
            .await?;
        parse_server_time(&self.exchange_id, &value)
    }

    pub async fn get_perpetual_server_time(&self) -> ExchangeApiResult<i64> {
        let value = self
            .rest
            .send_swap_public_get("/api/v1/usdt/time", &HashMap::new())
            .await?;
        parse_server_time(&self.exchange_id, &value)
    }

    pub async fn get_spot_ticker(&self, symbol: &str) -> ExchangeApiResult<TapbitTicker> {
        let mut params = HashMap::new();
        params.insert("instrument_id".to_string(), normalize_spot_symbol(symbol)?);
        let value = self
            .rest
            .send_spot_public_get("/api/spot/instruments/ticker_one", &params)
            .await?;
        parse_ticker(&self.exchange_id, &value)
    }

    pub async fn get_perpetual_ticker(&self, symbol: &str) -> ExchangeApiResult<TapbitTicker> {
        let mut params = HashMap::new();
        params.insert("instrument_id".to_string(), swap_depth_instrument(symbol)?);
        let value = self
            .rest
            .send_swap_public_get("/api/usdt/instruments/ticker_one", &params)
            .await?;
        parse_ticker(&self.exchange_id, &value)
    }

    pub async fn get_spot_public_trades(
        &self,
        symbol: &str,
    ) -> ExchangeApiResult<Vec<TapbitPublicTrade>> {
        let mut params = HashMap::new();
        params.insert("instrument_id".to_string(), normalize_spot_symbol(symbol)?);
        let value = self
            .rest
            .send_spot_public_get("/api/spot/instruments/trade_list", &params)
            .await?;
        parse_spot_public_trades(&self.exchange_id, &value)
    }

    pub async fn get_perpetual_public_trades(
        &self,
        symbol: &str,
    ) -> ExchangeApiResult<Vec<TapbitPublicTrade>> {
        let instrument = swap_depth_instrument(symbol)?;
        let mut params = HashMap::new();
        params.insert("instrument_id".to_string(), instrument.clone());
        let value = self
            .rest
            .send_swap_public_get("/api/usdt/instruments/trade_list", &params)
            .await?;
        parse_swap_public_trades(&self.exchange_id, &format!("{instrument}-SWAP"), &value)
    }

    pub async fn get_spot_klines(
        &self,
        symbol: &str,
        interval: &str,
    ) -> ExchangeApiResult<Vec<TapbitKline>> {
        let mut params = HashMap::new();
        params.insert("instrument_id".to_string(), normalize_spot_symbol(symbol)?);
        params.insert(
            "period".to_string(),
            normalize_kline_interval(interval)?.to_string(),
        );
        let value = self
            .rest
            .send_spot_public_get("/api/spot/instruments/candles", &params)
            .await?;
        parse_klines(&self.exchange_id, &value)
    }

    pub async fn get_perpetual_klines(
        &self,
        symbol: &str,
        interval: &str,
    ) -> ExchangeApiResult<Vec<TapbitKline>> {
        let mut params = HashMap::new();
        params.insert("instrument_id".to_string(), swap_depth_instrument(symbol)?);
        params.insert(
            "period".to_string(),
            normalize_kline_interval(interval)?.to_string(),
        );
        let value = self
            .rest
            .send_swap_public_get("/api/usdt/instruments/candles", &params)
            .await?;
        parse_klines(&self.exchange_id, &value)
    }

    pub async fn get_perpetual_funding_rate(
        &self,
        symbol: &str,
    ) -> ExchangeApiResult<TapbitFundingRate> {
        let instrument = swap_depth_instrument(symbol)?;
        let mut params = HashMap::new();
        params.insert("instrument_id".to_string(), instrument.clone());
        let value = self
            .rest
            .send_swap_public_get("/api/usdt/instruments/funding_rate", &params)
            .await?;
        parse_funding_rate(&self.exchange_id, &format!("{instrument}-SWAP"), &value)
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
                    message: "tapbit get_symbol_rules requires one market_type per request"
                        .to_string(),
                });
            }
        }
        let mut rules = match market_type {
            MarketType::Spot => {
                let value = self
                    .rest
                    .send_spot_public_get("/api/spot/instruments/trade_pair_list", &HashMap::new())
                    .await?;
                parse_spot_symbol_rules(&self.exchange_id, &value)?
            }
            MarketType::Perpetual => {
                let value = self
                    .rest
                    .send_swap_public_get("/api/usdt/instruments/list", &HashMap::new())
                    .await?;
                parse_swap_symbol_rules(&self.exchange_id, &value)?
            }
            _ => return self.unsupported("tapbit.get_symbol_rules.market_type"),
        };
        if !request.symbols.is_empty() {
            let requested = request
                .symbols
                .iter()
                .map(|symbol| match market_type {
                    MarketType::Spot => normalize_spot_symbol(&symbol.exchange_symbol.symbol),
                    MarketType::Perpetual => {
                        super::parser::normalize_swap_symbol(&symbol.exchange_symbol.symbol)
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
        let depth = normalize_depth(request.depth.unwrap_or(10));
        let order_book = match request.symbol.market_type {
            MarketType::Spot => {
                let mut params = HashMap::new();
                params.insert(
                    "instrument_id".to_string(),
                    normalize_spot_symbol(&request.symbol.exchange_symbol.symbol)?,
                );
                params.insert("depth".to_string(), depth.to_string());
                let value = self
                    .rest
                    .send_spot_public_get("/api/spot/instruments/depth", &params)
                    .await?;
                parse_spot_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?
            }
            MarketType::Perpetual => {
                let mut params = HashMap::new();
                params.insert(
                    "instrument_id".to_string(),
                    swap_depth_instrument(&request.symbol.exchange_symbol.symbol)?,
                );
                params.insert("depth".to_string(), depth.to_string());
                let value = self
                    .rest
                    .send_swap_public_get("/api/usdt/instruments/depth", &params)
                    .await?;
                parse_swap_orderbook_snapshot(&self.exchange_id, request.symbol, &value)?
            }
            _ => return self.unsupported("tapbit.get_order_book.market_type"),
        };
        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order_book,
        })
    }
}

fn normalize_kline_interval(interval: &str) -> ExchangeApiResult<&'static str> {
    match interval.trim().to_ascii_lowercase().as_str() {
        "1" | "1m" => Ok("1"),
        "3" | "3m" => Ok("3"),
        "5" | "5m" => Ok("5"),
        "15" | "15m" => Ok("15"),
        "30" | "30m" => Ok("30"),
        "60" | "1h" => Ok("60"),
        "120" | "2h" => Ok("120"),
        "240" | "4h" => Ok("240"),
        "360" | "6h" => Ok("360"),
        "720" | "12h" => Ok("720"),
        "d" | "1d" => Ok("D"),
        "w" | "1w" => Ok("W"),
        "m" | "1mo" | "1month" => Ok("M"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "tapbit.rest_kline_interval",
        }),
    }
}

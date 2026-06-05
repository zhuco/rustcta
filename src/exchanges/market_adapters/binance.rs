use super::{
    compact_symbol_to_canonical, compact_usdt_symbol, datetime_from_millis, parse_json_f64,
    parse_json_u64, parse_level_pair, ExchangeMarketAdapterInfo, MarketAdapterInfo,
    MarketCapabilities,
};
use crate::market::{
    CanonicalSymbol, ContractType, ExchangeId, ExchangeSymbol, InstrumentMeta, InstrumentStatus,
    MarketDataAdapter, MarketEvent, MarketFundingSnapshot, OrderBook5, OrderBookSnapshot,
    WsSubscription,
};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, Default)]
pub struct BinanceMarketAdapter;

const BINANCE_FAPI_BASE: &str = "https://fapi.binance.com";

impl MarketAdapterInfo for BinanceMarketAdapter {
    fn info(&self) -> ExchangeMarketAdapterInfo {
        ExchangeMarketAdapterInfo {
            exchange: ExchangeId::Binance,
            name: "binance-usdm-market",
            venue_symbol_example: "BTCUSDT",
            capabilities: MarketCapabilities::new(true, true, true, true, true),
            protocol_notes: &[
                "USD-M futures public market data adapter skeleton.",
                "Order book sequence support is declared because Binance depth streams carry update ids.",
            ],
        }
    }

    fn to_exchange_symbol(&self, canonical: &CanonicalSymbol) -> ExchangeSymbol {
        ExchangeSymbol::new(ExchangeId::Binance, compact_usdt_symbol(canonical))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinanceDepthSummary {
    pub symbol: String,
    pub first_update_id: Option<u64>,
    pub final_update_id: Option<u64>,
    pub bid_levels: usize,
    pub ask_levels: usize,
}

#[derive(Debug, Deserialize)]
struct BinanceDepthMessage {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "U")]
    first_update_id: Option<u64>,
    #[serde(rename = "u")]
    final_update_id: Option<u64>,
    #[serde(rename = "b", default)]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a", default)]
    asks: Vec<[String; 2]>,
}

pub fn parse_binance_depth_summary(raw: &str) -> serde_json::Result<BinanceDepthSummary> {
    let msg: BinanceDepthMessage = serde_json::from_str(raw)?;
    Ok(BinanceDepthSummary {
        symbol: msg.symbol,
        first_update_id: msg.first_update_id,
        final_update_id: msg.final_update_id,
        bid_levels: msg.bids.len(),
        ask_levels: msg.asks.len(),
    })
}

#[async_trait]
impl MarketDataAdapter for BinanceMarketAdapter {
    fn exchange(&self) -> ExchangeId {
        ExchangeId::Binance
    }

    fn capabilities(&self) -> crate::market::MarketCapabilities {
        crate::market::MarketCapabilities::new(true, true, true, true, true)
    }

    async fn load_instruments(&self) -> anyhow::Result<Vec<InstrumentMeta>> {
        let value: Value = reqwest::get(format!("{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo"))
            .await?
            .error_for_status()?
            .json()
            .await?;

        let symbols = value
            .get("symbols")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("binance exchangeInfo missing symbols array"))?;

        Ok(symbols
            .iter()
            .filter_map(parse_binance_instrument)
            .collect())
    }

    async fn load_funding(
        &self,
        symbols: &[CanonicalSymbol],
    ) -> anyhow::Result<Vec<MarketFundingSnapshot>> {
        let client = reqwest::Client::new();
        let mut snapshots = Vec::with_capacity(symbols.len());

        if symbols.is_empty() || symbols.len() > 20 {
            let values: Vec<Value> = client
                .get(format!("{BINANCE_FAPI_BASE}/fapi/v1/premiumIndex"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await
                .context("decode binance premiumIndex")?;
            let wanted = symbols.iter().cloned().collect::<HashSet<_>>();
            for value in values {
                let Some(symbol) = value
                    .get("symbol")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                else {
                    continue;
                };
                let Some(canonical) = compact_symbol_to_canonical(&symbol) else {
                    continue;
                };
                if !wanted.is_empty() && !wanted.contains(&canonical) {
                    continue;
                }
                snapshots.push(binance_funding_snapshot(
                    value,
                    canonical,
                    &symbol,
                    Utc::now(),
                ));
            }
            return Ok(snapshots);
        }

        for canonical in symbols {
            let exchange_symbol = self.to_exchange_symbol(canonical);
            let value: Value = client
                .get(format!("{BINANCE_FAPI_BASE}/fapi/v1/premiumIndex"))
                .query(&[("symbol", exchange_symbol.symbol.as_str())])
                .send()
                .await?
                .error_for_status()?
                .json()
                .await
                .with_context(|| {
                    format!("decode binance premiumIndex for {}", exchange_symbol.symbol)
                })?;

            snapshots.push(binance_funding_snapshot(
                value,
                canonical.clone(),
                &exchange_symbol.symbol,
                Utc::now(),
            ));
        }

        Ok(snapshots)
    }

    fn build_public_ws_subscriptions(&self, symbols: &[ExchangeSymbol]) -> Vec<WsSubscription> {
        symbols
            .iter()
            .map(|symbol| {
                WsSubscription::new(ExchangeId::Binance, "depth5@100ms", vec![symbol.clone()])
                    .with_route(format!(
                        "{}@depth5@100ms",
                        symbol.symbol.to_ascii_lowercase()
                    ))
            })
            .collect()
    }

    fn parse_public_ws_message(
        &self,
        raw: &str,
        recv_ts: DateTime<Utc>,
    ) -> anyhow::Result<Vec<MarketEvent>> {
        let value: Value = serde_json::from_str(raw)?;
        let data = value
            .get("data")
            .filter(|data| data.is_object())
            .unwrap_or(&value);
        let Some(book) = parse_binance_book(data, recv_ts, "binance.depth5", None) else {
            return Ok(Vec::new());
        };
        Ok(vec![MarketEvent::OrderBook(book)])
    }

    async fn fetch_orderbook_snapshot(
        &self,
        symbol: &ExchangeSymbol,
        depth: u16,
    ) -> anyhow::Result<OrderBookSnapshot> {
        let limit = depth.clamp(5, 20).to_string();
        let value: Value = reqwest::Client::new()
            .get(format!("{BINANCE_FAPI_BASE}/fapi/v1/depth"))
            .query(&[
                ("symbol", symbol.symbol.as_str()),
                ("limit", limit.as_str()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        parse_binance_book(
            &value,
            Utc::now(),
            "binance.rest.depth",
            Some(symbol.symbol.as_str()),
        )
        .ok_or_else(|| anyhow!("binance depth response could not be converted to OrderBook5"))
    }
}

fn parse_binance_book(
    value: &Value,
    recv_ts: DateTime<Utc>,
    route: &str,
    fallback_symbol: Option<&str>,
) -> Option<OrderBook5> {
    let symbol = value
        .get("s")
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .or(fallback_symbol)?;
    let canonical = compact_symbol_to_canonical(symbol)?;
    let exchange_symbol = ExchangeSymbol::new(ExchangeId::Binance, symbol);
    let bids = parse_levels(value.get("b").or_else(|| value.get("bids"))?);
    let asks = parse_levels(value.get("a").or_else(|| value.get("asks"))?);
    let exchange_ts = datetime_from_millis(
        value
            .get("E")
            .or_else(|| value.get("T"))
            .and_then(parse_json_u64),
        recv_ts,
    );
    let sequence = value
        .get("u")
        .or_else(|| value.get("lastUpdateId"))
        .and_then(parse_json_u64);

    Some(OrderBook5::new(
        ExchangeId::Binance,
        canonical,
        exchange_symbol,
        bids,
        asks,
        exchange_ts,
        recv_ts,
        sequence,
        Some(route.to_string()),
    ))
}

fn parse_levels(value: &Value) -> Vec<crate::market::BookLevel> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_array)
        .filter_map(|level| parse_level_pair(level))
        .collect()
}

fn parse_binance_instrument(value: &Value) -> Option<InstrumentMeta> {
    if value.get("quoteAsset")?.as_str()? != "USDT"
        || value.get("contractType")?.as_str()? != "PERPETUAL"
    {
        return None;
    }

    let symbol = value.get("symbol")?.as_str()?;
    let canonical = compact_symbol_to_canonical(symbol)?;
    let base = value
        .get("baseAsset")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| canonical.base().to_string());
    let status = match value.get("status").and_then(Value::as_str) {
        Some("TRADING") => InstrumentStatus::Trading,
        Some("BREAK") | Some("PENDING_TRADING") => InstrumentStatus::Paused,
        Some("CLOSE") => InstrumentStatus::CloseOnly,
        _ => InstrumentStatus::Unknown,
    };
    let price_tick = filter_number(value, "PRICE_FILTER", "tickSize").unwrap_or(0.0);
    let quantity_step = filter_number(value, "LOT_SIZE", "stepSize").unwrap_or(0.0);
    let min_qty = filter_number(value, "LOT_SIZE", "minQty").unwrap_or(0.0);
    let min_notional = filter_number(value, "MIN_NOTIONAL", "notional").unwrap_or(0.0);
    let price_precision = value
        .get("pricePrecision")
        .and_then(parse_json_u64)
        .unwrap_or_default() as u32;
    let quantity_precision = value
        .get("quantityPrecision")
        .and_then(parse_json_u64)
        .unwrap_or_default() as u32;

    Some(InstrumentMeta::new(
        ExchangeId::Binance,
        canonical,
        ExchangeSymbol::new(ExchangeId::Binance, symbol),
        base,
        "USDT",
        "USDT",
        ContractType::LinearPerpetual,
        1.0,
        price_tick,
        quantity_step,
        min_qty,
        min_notional,
        price_precision,
        quantity_precision,
        status,
    ))
}

fn binance_funding_snapshot(
    value: Value,
    canonical: CanonicalSymbol,
    exchange_symbol: &str,
    recv_ts: DateTime<Utc>,
) -> MarketFundingSnapshot {
    let funding_rate = value
        .get("lastFundingRate")
        .and_then(parse_json_f64)
        .unwrap_or_default();
    let next_funding_time = datetime_from_millis(
        value.get("nextFundingTime").and_then(parse_json_u64),
        recv_ts,
    );
    let mut snapshot = MarketFundingSnapshot::new(
        ExchangeId::Binance,
        canonical,
        Some(ExchangeSymbol::new(ExchangeId::Binance, exchange_symbol)),
        funding_rate,
        Some(next_funding_time),
        recv_ts,
    )
    .with_prices(
        value.get("markPrice").and_then(parse_json_f64),
        value.get("indexPrice").and_then(parse_json_f64),
    );
    snapshot.predicted_funding_rate = value.get("interestRate").and_then(parse_json_f64);
    snapshot
}

fn filter_number(value: &Value, filter_type: &str, field: &str) -> Option<f64> {
    value
        .get("filters")?
        .as_array()?
        .iter()
        .find(|filter| filter.get("filterType").and_then(Value::as_str) == Some(filter_type))?
        .get(field)
        .and_then(parse_json_f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapters_should_build_binance_depth_routes_for_stream_batches() {
        let symbols = vec![
            ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            ExchangeSymbol::new(ExchangeId::Binance, "ETHUSDT"),
        ];

        let subscriptions = BinanceMarketAdapter.build_public_ws_subscriptions(&symbols);

        assert_eq!(subscriptions.len(), 2);
        assert_eq!(subscriptions[0].channel, "depth5@100ms");
        assert_eq!(subscriptions[0].symbols, vec![symbols[0].clone()]);
        assert_eq!(
            subscriptions[0].route.as_deref(),
            Some("btcusdt@depth5@100ms")
        );
        assert_eq!(
            subscriptions[1].route.as_deref(),
            Some("ethusdt@depth5@100ms")
        );

        let combined_streams = subscriptions
            .iter()
            .filter_map(|subscription| subscription.route.as_deref())
            .collect::<Vec<_>>()
            .join("/");
        assert_eq!(
            combined_streams,
            "btcusdt@depth5@100ms/ethusdt@depth5@100ms"
        );
    }

    #[test]
    fn adapters_should_parse_binance_depth_summary() {
        let raw = r#"{
            "e":"depthUpdate",
            "E":1710000000000,
            "T":1710000000000,
            "s":"BTCUSDT",
            "U":100,
            "u":105,
            "pu":99,
            "b":[["65000.1","0.2"],["65000.0","0.1"]],
            "a":[["65000.2","0.3"]]
        }"#;

        let summary = parse_binance_depth_summary(raw).expect("valid binance depth json");

        assert_eq!(summary.symbol, "BTCUSDT");
        assert_eq!(summary.first_update_id, Some(100));
        assert_eq!(summary.final_update_id, Some(105));
        assert_eq!(summary.bid_levels, 2);
        assert_eq!(summary.ask_levels, 1);
    }

    #[test]
    fn adapters_should_parse_binance_combined_depth_event_to_orderbook() {
        let raw = r#"{
            "stream":"btcusdt@depth5@100ms",
            "data":{
                "e":"depthUpdate",
                "E":1710000000000,
                "T":1710000000000,
                "s":"BTCUSDT",
                "U":100,
                "u":105,
                "pu":99,
                "b":[["65000.1","0.2"]],
                "a":[["65000.2","0.3"]]
            }
        }"#;

        let events = BinanceMarketAdapter
            .parse_public_ws_message(raw, Utc::now())
            .expect("parse ws");

        assert_eq!(events.len(), 1);
        let MarketEvent::OrderBook(book) = &events[0] else {
            panic!("expected orderbook event");
        };
        assert_eq!(book.canonical_symbol, CanonicalSymbol::new("BTC", "USDT"));
        assert_eq!(book.sequence, Some(105));
        assert!(book.is_usable());
    }

    #[test]
    fn adapters_should_parse_binance_rest_depth_with_request_symbol() {
        let raw = r#"{
            "lastUpdateId":10636757896518,
            "E":1779821773594,
            "T":1779821773586,
            "bids":[["0.108000","255723.7"]],
            "asks":[["0.108100","303653.7"]]
        }"#;
        let value: Value = serde_json::from_str(raw).expect("valid rest depth json");

        let book = parse_binance_book(&value, Utc::now(), "binance.rest.depth", Some("ARBUSDT"))
            .expect("rest book should parse with fallback symbol");

        assert_eq!(book.exchange_symbol.symbol, "ARBUSDT");
        assert_eq!(book.canonical_symbol, CanonicalSymbol::new("ARB", "USDT"));
        assert_eq!(book.sequence, Some(10636757896518));
        assert!(book.is_usable());
    }
}

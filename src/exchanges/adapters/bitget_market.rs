use super::{
    compact_symbol_to_canonical, compact_usdt_symbol, parse_json_f64, parse_json_u64,
    ExchangeMarketAdapterInfo, MarketAdapterInfo, MarketCapabilities,
};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashSet;

use crate::market::{
    BookLevel, CanonicalSymbol, ContractType, ExchangeId, ExchangeSymbol, InstrumentMeta,
    InstrumentStatus, MarketCapabilities as DataMarketCapabilities, MarketDataAdapter, MarketEvent,
    MarketFundingSnapshot, OrderBook5, OrderBookSnapshot, WsSubscription,
};

const BITGET_REST_BASE: &str = "https://api.bitget.com";
const BITGET_PUBLIC_WS: &str = "wss://ws.bitget.com/v2/ws/public";
const BITGET_PRODUCT_TYPE: &str = "usdt-futures";

#[derive(Debug, Clone, Copy, Default)]
pub struct BitgetMarketAdapter;

impl MarketAdapterInfo for BitgetMarketAdapter {
    fn info(&self) -> ExchangeMarketAdapterInfo {
        ExchangeMarketAdapterInfo {
            exchange: ExchangeId::Bitget,
            name: "bitget-usdt-futures-market",
            venue_symbol_example: "BTCUSDT",
            capabilities: MarketCapabilities::new(true, true, true, true, false),
            protocol_notes: &[
                "Skeleton only. Verify product type, instrument id suffixes, and sequence semantics against official Bitget futures docs before enabling live parsing.",
                "Do not assume sequence gap recovery until message fields are validated.",
            ],
        }
    }

    fn to_exchange_symbol(&self, canonical: &CanonicalSymbol) -> ExchangeSymbol {
        ExchangeSymbol::new(ExchangeId::Bitget, compact_usdt_symbol(canonical))
    }
}

#[async_trait]
impl MarketDataAdapter for BitgetMarketAdapter {
    fn exchange(&self) -> ExchangeId {
        ExchangeId::Bitget
    }

    fn capabilities(&self) -> DataMarketCapabilities {
        DataMarketCapabilities::new(true, true, true, true, false)
    }

    async fn load_instruments(&self) -> anyhow::Result<Vec<InstrumentMeta>> {
        let url = format!(
            "{BITGET_REST_BASE}/api/v2/mix/market/contracts?productType={BITGET_PRODUCT_TYPE}"
        );
        let value = get_json(&url, "bitget contracts").await?;
        ensure_bitget_success(&value, "bitget contracts")?;

        let instruments = value
            .get("data")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(parse_instrument)
            .collect();

        Ok(instruments)
    }

    async fn load_funding(
        &self,
        symbols: &[CanonicalSymbol],
    ) -> anyhow::Result<Vec<MarketFundingSnapshot>> {
        let recv_ts = Utc::now();

        if symbols.is_empty() || symbols.len() > 20 {
            let url = format!(
                "{BITGET_REST_BASE}/api/v2/mix/market/current-fund-rate?productType={BITGET_PRODUCT_TYPE}"
            );
            let value = get_json(&url, "bitget funding").await?;
            ensure_bitget_success(&value, "bitget funding")?;
            let wanted = symbols.iter().cloned().collect::<HashSet<_>>();
            let snapshots = parse_funding_response(&value, recv_ts)
                .into_iter()
                .filter(|snapshot| wanted.is_empty() || wanted.contains(&snapshot.canonical_symbol))
                .collect();
            return Ok(snapshots);
        }

        let mut snapshots = Vec::new();
        for canonical in symbols {
            let exchange_symbol = self.to_exchange_symbol(canonical);
            let url = format!(
                "{BITGET_REST_BASE}/api/v2/mix/market/current-fund-rate?productType={BITGET_PRODUCT_TYPE}&symbol={}",
                exchange_symbol.symbol
            );
            let value = get_json(&url, "bitget funding").await?;
            ensure_bitget_success(&value, "bitget funding")?;
            snapshots.extend(parse_funding_response(&value, recv_ts));
        }

        Ok(snapshots)
    }

    fn build_public_ws_subscriptions(&self, symbols: &[ExchangeSymbol]) -> Vec<WsSubscription> {
        if symbols.is_empty() {
            return Vec::new();
        }

        vec![
            WsSubscription::new(ExchangeId::Bitget, "books5", symbols.to_vec())
                .with_route(BITGET_PUBLIC_WS),
        ]
    }

    fn parse_public_ws_message(
        &self,
        raw: &str,
        recv_ts: DateTime<Utc>,
    ) -> anyhow::Result<Vec<MarketEvent>> {
        let value: Value = serde_json::from_str(raw).context("parse bitget public ws json")?;
        let mut events = Vec::new();

        if let Some(data) = value.get("data").and_then(Value::as_array) {
            for item in data {
                if let Some(book) = parse_orderbook_payload(&value, item, recv_ts, None)? {
                    events.push(MarketEvent::OrderBook(book));
                }
            }
            return Ok(events);
        }

        if let Some(book) = parse_orderbook_payload(&value, &value, recv_ts, None)? {
            events.push(MarketEvent::OrderBook(book));
        }

        Ok(events)
    }

    async fn fetch_orderbook_snapshot(
        &self,
        symbol: &ExchangeSymbol,
        depth: u16,
    ) -> anyhow::Result<OrderBookSnapshot> {
        if symbol.exchange != ExchangeId::Bitget {
            return Err(anyhow!(
                "bitget orderbook snapshot received non-bitget symbol: {}",
                symbol.symbol
            ));
        }

        let limit = depth.clamp(1, 5);
        let url = format!(
            "{BITGET_REST_BASE}/api/v2/mix/market/merge-depth?productType={BITGET_PRODUCT_TYPE}&symbol={}&precision=scale0&limit={limit}",
            symbol.symbol
        );
        let value = get_json(&url, "bitget orderbook").await?;
        ensure_bitget_success(&value, "bitget orderbook")?;
        let data = value
            .get("data")
            .ok_or_else(|| anyhow!("bitget orderbook response missing data"))?;

        parse_orderbook_payload(&value, data, Utc::now(), Some(symbol.symbol.as_str()))?.ok_or_else(
            || {
                anyhow!(
                    "bitget orderbook response missing bids/asks for {}",
                    symbol.symbol
                )
            },
        )
    }
}

async fn get_json(url: &str, label: &str) -> anyhow::Result<Value> {
    let response = reqwest::get(url)
        .await
        .with_context(|| format!("{label} request failed: {url}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("{label} response body read failed"))?;

    if !status.is_success() {
        return Err(anyhow!("{label} HTTP {status}: {body}"));
    }

    serde_json::from_str(&body).with_context(|| format!("{label} invalid json: {body}"))
}

fn ensure_bitget_success(value: &Value, label: &str) -> anyhow::Result<()> {
    let code = value.get("code").and_then(Value::as_str).unwrap_or("00000");
    if code == "00000" {
        return Ok(());
    }

    let message = value
        .get("msg")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    Err(anyhow!("{label} returned code {code}: {message}"))
}

fn parse_orderbook_payload(
    root: &Value,
    payload: &Value,
    recv_ts: DateTime<Utc>,
    fallback_symbol: Option<&str>,
) -> anyhow::Result<Option<OrderBook5>> {
    let bids_value = payload
        .get("bids")
        .or_else(|| payload.get("b"))
        .or_else(|| root.get("bids"))
        .or_else(|| root.get("b"));
    let asks_value = payload
        .get("asks")
        .or_else(|| payload.get("a"))
        .or_else(|| root.get("asks"))
        .or_else(|| root.get("a"));

    let (Some(bids_value), Some(asks_value)) = (bids_value, asks_value) else {
        return Ok(None);
    };

    let symbol = extract_symbol(root)
        .or_else(|| extract_symbol(payload))
        .or(fallback_symbol)
        .ok_or_else(|| anyhow!("bitget orderbook message missing symbol/instId/contract"))?;
    let canonical_symbol = bitget_symbol_to_canonical(symbol)
        .ok_or_else(|| anyhow!("bitget orderbook message has unsupported symbol: {symbol}"))?;
    let exchange_symbol = ExchangeSymbol::new(ExchangeId::Bitget, symbol.to_ascii_uppercase());
    let bids = parse_levels(bids_value);
    let asks = parse_levels(asks_value);
    let exchange_ts = timestamp_from_value(
        payload
            .get("ts")
            .or_else(|| payload.get("t"))
            .or_else(|| root.get("ts"))
            .or_else(|| root.get("time_ms"))
            .or_else(|| root.get("timestamp")),
        recv_ts,
    );
    let sequence = payload
        .get("sequence")
        .or_else(|| payload.get("seq"))
        .or_else(|| payload.get("id"))
        .or_else(|| payload.get("u"))
        .and_then(parse_json_u64);

    Ok(Some(OrderBook5::new(
        ExchangeId::Bitget,
        canonical_symbol,
        exchange_symbol,
        bids,
        asks,
        exchange_ts,
        recv_ts,
        sequence,
        Some("bitget-public".to_string()),
    )))
}

fn extract_symbol(value: &Value) -> Option<&str> {
    value
        .get("symbol")
        .or_else(|| value.get("instId"))
        .or_else(|| value.get("contract"))
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("arg")
                .and_then(|arg| {
                    arg.get("instId")
                        .or_else(|| arg.get("symbol"))
                        .or_else(|| arg.get("contract"))
                })
                .and_then(Value::as_str)
        })
}

fn bitget_symbol_to_canonical(symbol: &str) -> Option<CanonicalSymbol> {
    compact_symbol_to_canonical(symbol).or_else(|| {
        let upper = symbol.trim().to_ascii_uppercase();
        let usdt_end = upper.find("USDT")? + 4;
        compact_symbol_to_canonical(&upper[..usdt_end])
    })
}

fn parse_levels(value: &Value) -> Vec<BookLevel> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(parse_level)
        .collect()
}

fn parse_level(value: &Value) -> Option<BookLevel> {
    if let Some(level) = value.as_array() {
        let price = level.first().and_then(parse_json_f64)?;
        let quantity = level.get(1).and_then(parse_json_f64)?;
        return Some(BookLevel::new(price, quantity));
    }

    let price = value
        .get("p")
        .or_else(|| value.get("price"))
        .and_then(parse_json_f64)?;
    let quantity = value
        .get("s")
        .or_else(|| value.get("size"))
        .or_else(|| value.get("quantity"))
        .or_else(|| value.get("amount"))
        .and_then(parse_json_f64)?;
    Some(BookLevel::new(price, quantity))
}

fn timestamp_from_value(value: Option<&Value>, fallback: DateTime<Utc>) -> DateTime<Utc> {
    let Some(value) = value else {
        return fallback;
    };
    let Some(timestamp) = parse_json_f64(value) else {
        return fallback;
    };

    let millis = if timestamp > 1_000_000_000_000.0 {
        timestamp as i64
    } else {
        (timestamp * 1000.0) as i64
    };

    DateTime::<Utc>::from_timestamp_millis(millis).unwrap_or(fallback)
}

fn parse_instrument(value: &Value) -> Option<InstrumentMeta> {
    let symbol = value.get("symbol").and_then(Value::as_str)?;
    let base = value
        .get("baseCoin")
        .and_then(Value::as_str)
        .or_else(|| symbol.strip_suffix("USDT"))?;
    let quote = value
        .get("quoteCoin")
        .and_then(Value::as_str)
        .unwrap_or("USDT");
    if !quote.eq_ignore_ascii_case("USDT") {
        return None;
    }

    let canonical_symbol = CanonicalSymbol::new(base, quote);
    let price_precision = parse_u32(value.get("pricePlace")).unwrap_or(0);
    let quantity_precision = parse_u32(value.get("volumePlace")).unwrap_or(0);
    let price_end_step = value
        .get("priceEndStep")
        .and_then(parse_json_f64)
        .unwrap_or(1.0);
    let price_tick = precision_step(price_precision) * price_end_step.max(1.0);
    let quantity_step = value
        .get("sizeMultiplier")
        .and_then(parse_json_f64)
        .unwrap_or_else(|| precision_step(quantity_precision));
    let min_qty = value
        .get("minTradeNum")
        .and_then(parse_json_f64)
        .unwrap_or(0.0);
    let min_notional = value
        .get("minTradeUSDT")
        .and_then(parse_json_f64)
        .unwrap_or(0.0);
    let status = match value
        .get("symbolStatus")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_ascii_lowercase()
        .as_str()
    {
        "normal" | "online" | "trading" => InstrumentStatus::Trading,
        "limit_open" | "restrictedapi" => InstrumentStatus::CloseOnly,
        "off" | "delisted" => InstrumentStatus::Delisted,
        _ => InstrumentStatus::Unknown,
    };

    Some(
        InstrumentMeta::new(
            ExchangeId::Bitget,
            canonical_symbol,
            ExchangeSymbol::new(ExchangeId::Bitget, symbol.to_ascii_uppercase()),
            base,
            quote,
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
        )
        .with_order_capabilities(true, true, true, true),
    )
}

fn parse_funding_response(value: &Value, recv_ts: DateTime<Utc>) -> Vec<MarketFundingSnapshot> {
    value
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let symbol = item.get("symbol").and_then(Value::as_str)?;
            let canonical_symbol = bitget_symbol_to_canonical(symbol)?;
            let funding_rate = item.get("fundingRate").and_then(parse_json_f64)?;
            let next_funding_time = item
                .get("nextUpdate")
                .map(|value| timestamp_from_value(Some(value), recv_ts));

            Some(MarketFundingSnapshot::new(
                ExchangeId::Bitget,
                canonical_symbol,
                Some(ExchangeSymbol::new(
                    ExchangeId::Bitget,
                    symbol.to_ascii_uppercase(),
                )),
                funding_rate,
                next_funding_time,
                recv_ts,
            ))
        })
        .collect()
}

fn parse_u32(value: Option<&Value>) -> Option<u32> {
    value
        .and_then(parse_json_u64)
        .and_then(|value| value.try_into().ok())
}

fn precision_step(precision: u32) -> f64 {
    10_f64.powi(-(precision as i32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitget_should_build_single_books5_subscription_for_multiple_symbols() {
        let symbols = vec![
            ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
            ExchangeSymbol::new(ExchangeId::Bitget, "ETHUSDT"),
        ];

        let subscriptions = BitgetMarketAdapter.build_public_ws_subscriptions(&symbols);

        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].exchange, ExchangeId::Bitget);
        assert_eq!(subscriptions[0].channel, "books5");
        assert_eq!(subscriptions[0].symbols, symbols);
        assert_eq!(subscriptions[0].route.as_deref(), Some(BITGET_PUBLIC_WS));
    }

    #[test]
    fn bitget_should_parse_books5_snapshot() {
        let raw = r#"{
            "action":"snapshot",
            "arg":{"instType":"USDT-FUTURES","channel":"books5","instId":"BTCUSDT"},
            "data":[{
                "bids":[["65000.1","0.2"],["65000.0","0.1"]],
                "asks":[["65000.2","0.3"],["65000.3","0.4"]],
                "ts":"1710000000000"
            }]
        }"#;
        let recv_ts = DateTime::<Utc>::from_timestamp_millis(1710000000100).unwrap();

        let events = BitgetMarketAdapter
            .parse_public_ws_message(raw, recv_ts)
            .expect("valid bitget orderbook");

        assert_eq!(events.len(), 1);
        let MarketEvent::OrderBook(book) = &events[0] else {
            panic!("expected orderbook event");
        };
        assert_eq!(book.exchange, ExchangeId::Bitget);
        assert_eq!(book.canonical_symbol.as_pair(), "BTC/USDT");
        assert_eq!(book.exchange_symbol.symbol, "BTCUSDT");
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.asks.len(), 2);
        assert_eq!(book.bids[0], BookLevel::new(65000.1, 0.2));
        assert_eq!(
            book.exchange_ts,
            DateTime::<Utc>::from_timestamp_millis(1710000000000).unwrap()
        );
        assert_eq!(book.sequence, None);
    }

    #[test]
    fn bitget_should_parse_books_update_with_topic_symbol() {
        let raw = r#"{
            "data":[{
                "a":[["3350.2","1.5"],["3350.3","2.0"]],
                "b":[["3350.1","0.7"],["3350.0","0.8"]],
                "pseq":1304314508780744704,
                "seq":1304314508780744705,
                "maxDepth":"50",
                "ts":"1746698732562"
            }],
            "arg":{"instType":"usdt-futures","symbol":"ETHUSDT","topic":"books"},
            "action":"update",
            "ts":1746698732563
        }"#;
        let recv_ts = DateTime::<Utc>::from_timestamp_millis(1746698732600).unwrap();

        let events = BitgetMarketAdapter
            .parse_public_ws_message(raw, recv_ts)
            .expect("valid bitget update");

        assert_eq!(events.len(), 1);
        let MarketEvent::OrderBook(book) = &events[0] else {
            panic!("expected orderbook event");
        };
        assert_eq!(book.exchange, ExchangeId::Bitget);
        assert_eq!(book.canonical_symbol.as_pair(), "ETH/USDT");
        assert_eq!(book.exchange_symbol.symbol, "ETHUSDT");
        assert_eq!(book.bids[0], BookLevel::new(3350.1, 0.7));
        assert_eq!(book.asks[0], BookLevel::new(3350.2, 1.5));
        assert_eq!(
            book.exchange_ts,
            DateTime::<Utc>::from_timestamp_millis(1746698732562).unwrap()
        );
        assert_eq!(book.sequence, Some(1304314508780744705));
    }

    #[test]
    fn bitget_should_parse_rest_merge_depth_with_request_symbol() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":{
                "asks":[[0.1081,49092.27]],
                "bids":[[0.1080,283614.47]],
                "ts":"1779821773645",
                "scale":"0.0001"
            }
        }"#;
        let value: Value = serde_json::from_str(raw).expect("valid bitget rest depth json");
        let data = value.get("data").expect("data");

        let book = parse_orderbook_payload(&value, data, Utc::now(), Some("ARBUSDT"))
            .expect("parse should succeed")
            .expect("book should exist");

        assert_eq!(book.exchange_symbol.symbol, "ARBUSDT");
        assert_eq!(book.canonical_symbol, CanonicalSymbol::new("ARB", "USDT"));
        assert!(book.is_usable());
    }
}

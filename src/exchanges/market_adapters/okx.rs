use super::{
    dashed_swap_symbol, datetime_from_millis, okx_symbol_to_canonical, parse_json_f64,
    parse_json_u64, parse_level_pair, ExchangeMarketAdapterInfo, MarketAdapterInfo,
    MarketCapabilities,
};
use crate::market::{
    CanonicalSymbol, ContractType, ExchangeId, ExchangeSymbol, InstrumentMeta, InstrumentStatus,
    MarketDataAdapter, MarketEvent, MarketFundingSnapshot, OrderBook5, OrderBookSnapshot,
    PublicBookProfile, PublicBookProfileKind, WsSubscription,
};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, Default)]
pub struct OkxMarketAdapter;

const OKX_API_BASE: &str = "https://www.okx.com";

impl MarketAdapterInfo for OkxMarketAdapter {
    fn info(&self) -> ExchangeMarketAdapterInfo {
        ExchangeMarketAdapterInfo {
            exchange: ExchangeId::Okx,
            name: "okx-swap-market",
            venue_symbol_example: "BTC-USDT-SWAP",
            capabilities: MarketCapabilities::new(true, true, true, true, false),
            protocol_notes: &[
                "USDT swap public market data adapter skeleton.",
                "Sequence behavior must be verified against the selected OKX books5/books channel before enabling gap recovery.",
            ],
        }
    }

    fn to_exchange_symbol(&self, canonical: &CanonicalSymbol) -> ExchangeSymbol {
        ExchangeSymbol::new(ExchangeId::Okx, dashed_swap_symbol(canonical))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OkxBooks5Summary {
    pub instrument_id: String,
    pub action: Option<String>,
    pub bid_levels: usize,
    pub ask_levels: usize,
}

#[derive(Debug, Deserialize)]
struct OkxBooks5Message {
    arg: OkxBooks5Arg,
    action: Option<String>,
    data: Vec<OkxBooks5Data>,
}

#[derive(Debug, Deserialize)]
struct OkxBooks5Arg {
    #[serde(rename = "instId")]
    instrument_id: String,
}

#[derive(Debug, Deserialize)]
struct OkxBooks5Data {
    #[serde(default)]
    bids: Vec<Vec<String>>,
    #[serde(default)]
    asks: Vec<Vec<String>>,
}

pub fn parse_okx_books5_summary(raw: &str) -> serde_json::Result<OkxBooks5Summary> {
    let msg: OkxBooks5Message = serde_json::from_str(raw)?;
    let first = msg.data.first();
    Ok(OkxBooks5Summary {
        instrument_id: msg.arg.instrument_id,
        action: msg.action,
        bid_levels: first.map(|data| data.bids.len()).unwrap_or_default(),
        ask_levels: first.map(|data| data.asks.len()).unwrap_or_default(),
    })
}

#[async_trait]
impl MarketDataAdapter for OkxMarketAdapter {
    fn exchange(&self) -> ExchangeId {
        ExchangeId::Okx
    }

    fn capabilities(&self) -> crate::market::MarketCapabilities {
        crate::market::MarketCapabilities::new(true, true, true, true, false)
    }

    async fn load_instruments(&self) -> anyhow::Result<Vec<InstrumentMeta>> {
        let value: Value = crate::core::http2_fix::shared_http_client()
            .get(format!("{OKX_API_BASE}/api/v5/public/instruments"))
            .query(&[("instType", "SWAP")])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let data = value
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("okx instruments response missing data array"))?;

        Ok(data.iter().filter_map(parse_okx_instrument).collect())
    }

    async fn load_funding(
        &self,
        symbols: &[CanonicalSymbol],
    ) -> anyhow::Result<Vec<MarketFundingSnapshot>> {
        let client = crate::core::http2_fix::shared_http_client();
        let mut snapshots = Vec::with_capacity(symbols.len());

        for canonical in symbols {
            let exchange_symbol = self.to_exchange_symbol(canonical);
            let value: Value = client
                .get(format!("{OKX_API_BASE}/api/v5/public/funding-rate"))
                .query(&[("instId", exchange_symbol.symbol.as_str())])
                .send()
                .await?
                .error_for_status()?
                .json()
                .await
                .with_context(|| {
                    format!("decode okx funding-rate for {}", exchange_symbol.symbol)
                })?;
            let Some(item) = value
                .get("data")
                .and_then(Value::as_array)
                .and_then(|data| data.first())
            else {
                continue;
            };

            let recv_ts = Utc::now();
            let next_funding_time = item
                .get("nextFundingTime")
                .and_then(parse_json_u64)
                .and_then(|ms| chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms as i64));
            snapshots.push(
                MarketFundingSnapshot::new(
                    ExchangeId::Okx,
                    canonical.clone(),
                    Some(exchange_symbol),
                    item.get("fundingRate")
                        .and_then(parse_json_f64)
                        .unwrap_or_default(),
                    next_funding_time,
                    recv_ts,
                )
                .with_prices(
                    item.get("markPx").and_then(parse_json_f64),
                    item.get("idxPx").and_then(parse_json_f64),
                ),
            );
        }

        Ok(snapshots)
    }

    fn public_book_profiles(&self) -> Vec<PublicBookProfile> {
        vec![
            PublicBookProfile::new(PublicBookProfileKind::FastestL1, "bbo-tbt", 1, Some(10))
                .with_sequence(true)
                .with_max_symbols_per_connection(100),
            PublicBookProfile::new(PublicBookProfileKind::FastestDepth, "books5", 5, Some(100))
                .with_sequence(true)
                .with_checksum(true)
                .with_max_symbols_per_connection(100),
            PublicBookProfile::new(
                PublicBookProfileKind::ConservativeDepth,
                "books",
                400,
                Some(100),
            )
            .with_sequence(true)
            .with_checksum(true)
            .with_max_symbols_per_connection(100),
        ]
    }

    fn build_public_ws_subscriptions_for_profile(
        &self,
        symbols: &[ExchangeSymbol],
        profile: PublicBookProfileKind,
    ) -> Vec<WsSubscription> {
        let selected = self
            .public_book_profiles()
            .into_iter()
            .find(|candidate| candidate.kind == profile)
            .unwrap_or_else(|| {
                PublicBookProfile::new(PublicBookProfileKind::FastestDepth, "books5", 5, Some(100))
                    .with_sequence(true)
                    .with_checksum(true)
            });
        symbols
            .iter()
            .map(|symbol| {
                WsSubscription::new(ExchangeId::Okx, selected.channel, vec![symbol.clone()])
                    .with_route(format!("{}:{}", selected.channel, symbol.symbol))
                    .with_profile(selected)
            })
            .collect()
    }

    fn build_public_ws_subscriptions(&self, symbols: &[ExchangeSymbol]) -> Vec<WsSubscription> {
        self.build_public_ws_subscriptions_for_profile(symbols, PublicBookProfileKind::FastestDepth)
    }

    fn parse_public_ws_message(
        &self,
        raw: &str,
        recv_ts: DateTime<Utc>,
    ) -> anyhow::Result<Vec<MarketEvent>> {
        let value: Value = serde_json::from_str(raw)?;
        let route = value
            .get("arg")
            .and_then(|arg| arg.get("channel"))
            .and_then(Value::as_str)
            .map(|channel| format!("okx.{channel}"))
            .unwrap_or_else(|| "okx.books5".to_string());
        let Some(events) = parse_okx_books(&value, recv_ts, &route, None) else {
            return Ok(Vec::new());
        };
        Ok(events.into_iter().map(MarketEvent::OrderBook).collect())
    }

    async fn fetch_orderbook_snapshot(
        &self,
        symbol: &ExchangeSymbol,
        depth: u16,
    ) -> anyhow::Result<OrderBookSnapshot> {
        let sz = depth.clamp(5, 20).to_string();
        let value: Value = crate::core::http2_fix::shared_http_client()
            .get(format!("{OKX_API_BASE}/api/v5/market/books"))
            .query(&[("instId", symbol.symbol.as_str()), ("sz", sz.as_str())])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        parse_okx_books(
            &value,
            Utc::now(),
            "okx.rest.books",
            Some(symbol.symbol.as_str()),
        )
        .and_then(|mut books| books.pop())
        .ok_or_else(|| anyhow!("okx books response could not be converted to OrderBook5"))
    }
}

fn parse_okx_books(
    value: &Value,
    recv_ts: DateTime<Utc>,
    route: &str,
    fallback_symbol: Option<&str>,
) -> Option<Vec<OrderBook5>> {
    let inst_id = value
        .get("arg")
        .and_then(|arg| arg.get("instId"))
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("data")
                .and_then(Value::as_array)
                .and_then(|data| data.first())
                .and_then(|first| first.get("instId"))
                .and_then(Value::as_str)
        })
        .or(fallback_symbol)?;
    let canonical = okx_symbol_to_canonical(inst_id)?;
    let exchange_symbol = ExchangeSymbol::new(ExchangeId::Okx, inst_id);
    let data = value.get("data")?.as_array()?;

    Some(
        data.iter()
            .filter_map(|item| {
                let bids = parse_levels(item.get("bids")?);
                let asks = parse_levels(item.get("asks")?);
                let exchange_ts =
                    datetime_from_millis(item.get("ts").and_then(parse_json_u64), recv_ts);
                let sequence = item
                    .get("seqId")
                    .or_else(|| item.get("seq_id"))
                    .and_then(parse_json_u64);
                Some(OrderBook5::new(
                    ExchangeId::Okx,
                    canonical.clone(),
                    exchange_symbol.clone(),
                    bids,
                    asks,
                    exchange_ts,
                    recv_ts,
                    sequence,
                    Some(route.to_string()),
                ))
            })
            .collect(),
    )
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

fn parse_okx_instrument(value: &Value) -> Option<InstrumentMeta> {
    if value.get("instType")?.as_str()? != "SWAP" || value.get("settleCcy")?.as_str()? != "USDT" {
        return None;
    }

    let inst_id = value.get("instId")?.as_str()?;
    let canonical = okx_symbol_to_canonical(inst_id)?;
    if !inst_id.ends_with("-USDT-SWAP") || canonical.quote() != "USDT" {
        return None;
    }
    let status = match value.get("state").and_then(Value::as_str) {
        Some("live") => InstrumentStatus::Trading,
        Some("suspend") | Some("preopen") | Some("test") => InstrumentStatus::Paused,
        _ => InstrumentStatus::Unknown,
    };
    let price_tick = value
        .get("tickSz")
        .and_then(parse_json_f64)
        .unwrap_or_default();
    let quantity_step = value
        .get("lotSz")
        .and_then(parse_json_f64)
        .unwrap_or_default();
    let min_qty = value
        .get("minSz")
        .and_then(parse_json_f64)
        .unwrap_or_default();
    let contract_size = value.get("ctVal").and_then(parse_json_f64).unwrap_or(1.0);

    Some(InstrumentMeta::new(
        ExchangeId::Okx,
        canonical.clone(),
        ExchangeSymbol::new(ExchangeId::Okx, inst_id),
        value
            .get("baseCcy")
            .and_then(Value::as_str)
            .unwrap_or_else(|| canonical.base()),
        "USDT",
        "USDT",
        ContractType::LinearPerpetual,
        contract_size,
        price_tick,
        quantity_step,
        min_qty,
        0.0,
        decimal_places(price_tick),
        decimal_places(quantity_step),
        status,
    ))
}

fn decimal_places(value: f64) -> u32 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    let text = format!("{value:.12}");
    text.trim_end_matches('0')
        .split('.')
        .nth(1)
        .map(|fraction| fraction.len() as u32)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapters_should_build_okx_books5_args_for_batch_request() {
        let symbols = vec![
            ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
            ExchangeSymbol::new(ExchangeId::Okx, "ETH-USDT-SWAP"),
        ];

        let subscriptions = OkxMarketAdapter.build_public_ws_subscriptions(&symbols);

        assert_eq!(subscriptions.len(), 2);
        assert_eq!(subscriptions[0].channel, "books5");
        assert_eq!(subscriptions[0].symbols, vec![symbols[0].clone()]);
        assert_eq!(
            subscriptions[0].route.as_deref(),
            Some("books5:BTC-USDT-SWAP")
        );
        assert_eq!(
            subscriptions[1].route.as_deref(),
            Some("books5:ETH-USDT-SWAP")
        );
    }

    #[test]
    fn adapters_should_parse_okx_books5_summary() {
        let raw = r#"{
            "arg":{"channel":"books5","instId":"BTC-USDT-SWAP"},
            "action":"snapshot",
            "data":[{
                "bids":[["65000.1","1","0","1"]],
                "asks":[["65000.2","2","0","1"],["65000.3","3","0","1"]],
                "ts":"1710000000000"
            }]
        }"#;

        let summary = parse_okx_books5_summary(raw).expect("valid okx books5 json");

        assert_eq!(summary.instrument_id, "BTC-USDT-SWAP");
        assert_eq!(summary.action.as_deref(), Some("snapshot"));
        assert_eq!(summary.bid_levels, 1);
        assert_eq!(summary.ask_levels, 2);
    }

    #[test]
    fn adapters_should_parse_okx_books5_to_orderbook() {
        let raw = r#"{
            "arg":{"channel":"books5","instId":"BTC-USDT-SWAP"},
            "action":"snapshot",
            "data":[{
                "bids":[["65000.1","1","0","1"]],
                "asks":[["65000.2","2","0","1"]],
                "ts":"1710000000000"
            }]
        }"#;

        let events = OkxMarketAdapter
            .parse_public_ws_message(raw, Utc::now())
            .expect("parse ws");

        let MarketEvent::OrderBook(book) = &events[0] else {
            panic!("expected orderbook event");
        };
        assert_eq!(book.exchange_symbol.symbol, "BTC-USDT-SWAP");
        assert_eq!(book.canonical_symbol, CanonicalSymbol::new("BTC", "USDT"));
        assert!(book.is_usable());
    }

    #[test]
    fn adapters_should_parse_okx_books_update_to_orderbook() {
        let raw = r#"{
            "arg":{"channel":"books","instId":"ETH-USDT-SWAP"},
            "action":"update",
            "data":[{
                "asks":[["3350.2","4","0","2"],["3350.3","1","0","1"]],
                "bids":[["3350.1","3","0","4"],["3350.0","2","0","2"]],
                "ts":"1710000001000",
                "checksum":-855196043,
                "prevSeqId":123456,
                "seqId":123457
            }]
        }"#;
        let recv_ts = DateTime::<Utc>::from_timestamp_millis(1710000001100).unwrap();

        let events = OkxMarketAdapter
            .parse_public_ws_message(raw, recv_ts)
            .expect("parse ws update");

        assert_eq!(events.len(), 1);
        let MarketEvent::OrderBook(book) = &events[0] else {
            panic!("expected orderbook event");
        };
        assert_eq!(book.exchange_symbol.symbol, "ETH-USDT-SWAP");
        assert_eq!(book.canonical_symbol, CanonicalSymbol::new("ETH", "USDT"));
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.asks.len(), 2);
        assert_eq!(
            book.exchange_ts,
            DateTime::<Utc>::from_timestamp_millis(1710000001000).unwrap()
        );
        assert!(book.is_usable());
    }

    #[test]
    fn adapters_should_parse_okx_usdt_swap_instrument_with_empty_quote_ccy() {
        let raw = r#"{
            "instType":"SWAP",
            "instId":"ARB-USDT-SWAP",
            "baseCcy":"",
            "quoteCcy":"",
            "settleCcy":"USDT",
            "ctType":"linear",
            "state":"live",
            "tickSz":"0.0001",
            "lotSz":"0.1",
            "minSz":"0.1",
            "ctVal":"1"
        }"#;
        let value: Value = serde_json::from_str(raw).expect("valid okx instrument json");

        let instrument = parse_okx_instrument(&value).expect("instrument should parse");

        assert_eq!(instrument.exchange_symbol.symbol, "ARB-USDT-SWAP");
        assert_eq!(
            instrument.canonical_symbol,
            CanonicalSymbol::new("ARB", "USDT")
        );
        assert_eq!(instrument.quote, "USDT");
        assert_eq!(instrument.settle_asset, "USDT");
        assert_eq!(instrument.price_tick, 0.0001);
        assert_eq!(instrument.quantity_step, 0.1);
    }

    #[test]
    fn adapters_should_parse_okx_rest_books_with_request_symbol() {
        let raw = r#"{
            "code":"0",
            "msg":"",
            "data":[{
                "asks":[["0.10804","291.6","0","15"]],
                "bids":[["0.10803","805.3","0","5"]],
                "ts":"1779821773651",
                "seqId":26663809551
            }]
        }"#;
        let value: Value = serde_json::from_str(raw).expect("valid okx rest books json");

        let books = parse_okx_books(&value, Utc::now(), "okx.rest.books", Some("ARB-USDT-SWAP"))
            .expect("rest books should parse with fallback symbol");
        let book = books.first().expect("one orderbook");

        assert_eq!(book.exchange_symbol.symbol, "ARB-USDT-SWAP");
        assert_eq!(book.canonical_symbol, CanonicalSymbol::new("ARB", "USDT"));
        assert!(book.is_usable());
    }
}

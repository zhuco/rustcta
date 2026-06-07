use super::{
    compact_symbol_to_canonical, parse_json_f64, parse_json_u64, parse_level_pair,
    underscored_symbol, ExchangeMarketAdapterInfo, MarketAdapterInfo, MarketCapabilities,
};
use crate::market::{
    BookLevel, CanonicalSymbol, ContractType, ExchangeId, ExchangeSymbol, InstrumentMeta,
    InstrumentStatus, MarketCapabilities as DataMarketCapabilities, MarketDataAdapter, MarketEvent,
    MarketFundingSnapshot, OrderBook5, OrderBookSnapshot, PublicBookProfile, PublicBookProfileKind,
    WsSubscription,
};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashSet;

const MEXC_REST_BASE: &str = "https://contract.mexc.com";
const MEXC_PUBLIC_WS: &str = "wss://contract.mexc.com/edge";

#[derive(Debug, Clone, Copy, Default)]
pub struct MexcMarketAdapter;

impl MarketAdapterInfo for MexcMarketAdapter {
    fn info(&self) -> ExchangeMarketAdapterInfo {
        ExchangeMarketAdapterInfo {
            exchange: ExchangeId::Mexc,
            name: "mexc-contract-usdt-market",
            venue_symbol_example: "BTC_USDT",
            capabilities: MarketCapabilities::new(true, true, true, true, false),
            protocol_notes: &[
                "MEXC contract API uses underscored symbols such as BTC_USDT.",
                "Depth subscriptions provide full-depth snapshots/updates; this adapter consumes levels as snapshots for scanner best bid/ask.",
            ],
        }
    }

    fn to_exchange_symbol(&self, canonical: &CanonicalSymbol) -> ExchangeSymbol {
        ExchangeSymbol::new(ExchangeId::Mexc, underscored_symbol(canonical))
    }
}

#[async_trait]
impl MarketDataAdapter for MexcMarketAdapter {
    fn exchange(&self) -> ExchangeId {
        ExchangeId::Mexc
    }

    fn capabilities(&self) -> DataMarketCapabilities {
        DataMarketCapabilities::new(true, true, true, true, false)
    }

    async fn load_instruments(&self) -> anyhow::Result<Vec<InstrumentMeta>> {
        let value = get_json(
            &format!("{MEXC_REST_BASE}/api/v1/contract/detail"),
            "mexc contract detail",
        )
        .await?;
        ensure_mexc_success(&value, "mexc contract detail")?;

        Ok(value
            .get("data")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(parse_instrument)
            .collect())
    }

    async fn load_funding(
        &self,
        symbols: &[CanonicalSymbol],
    ) -> anyhow::Result<Vec<MarketFundingSnapshot>> {
        let client = crate::core::http2_fix::shared_http_client();
        let recv_ts = Utc::now();
        let wanted = symbols.iter().cloned().collect::<HashSet<_>>();
        let request_symbols = if symbols.is_empty() {
            Vec::new()
        } else {
            symbols
                .iter()
                .map(|symbol| self.to_exchange_symbol(symbol))
                .collect()
        };

        let mut snapshots = Vec::new();
        for exchange_symbol in request_symbols {
            let value: Value = client
                .get(format!(
                    "{MEXC_REST_BASE}/api/v1/contract/funding_rate/{}",
                    exchange_symbol.symbol
                ))
                .send()
                .await
                .with_context(|| {
                    format!("mexc funding request failed for {}", exchange_symbol.symbol)
                })?
                .error_for_status()?
                .json()
                .await
                .context("decode mexc funding")?;
            ensure_mexc_success(&value, "mexc funding")?;
            if let Some(snapshot) = value
                .get("data")
                .and_then(|data| parse_funding(data, recv_ts))
                .filter(|snapshot| wanted.is_empty() || wanted.contains(&snapshot.canonical_symbol))
            {
                snapshots.push(snapshot);
            }
        }
        Ok(snapshots)
    }

    fn public_book_profiles(&self) -> Vec<PublicBookProfile> {
        vec![
            PublicBookProfile::new(PublicBookProfileKind::FastestL1, "sub.depth.full", 5, None)
                .with_sequence(true)
                .with_primary_fast_leg(false)
                .with_max_symbols_per_connection(50),
            PublicBookProfile::new(
                PublicBookProfileKind::FastestDepth,
                "sub.depth.full",
                5,
                None,
            )
            .with_sequence(true)
            .with_primary_fast_leg(false)
            .with_max_symbols_per_connection(50),
            PublicBookProfile::new(
                PublicBookProfileKind::ConservativeDepth,
                "sub.depth.full",
                20,
                None,
            )
            .with_sequence(true)
            .with_primary_fast_leg(false)
            .with_max_symbols_per_connection(50),
        ]
    }

    fn build_public_ws_subscriptions_for_profile(
        &self,
        symbols: &[ExchangeSymbol],
        profile: PublicBookProfileKind,
    ) -> Vec<WsSubscription> {
        if symbols.is_empty() {
            return Vec::new();
        }
        let selected = self
            .public_book_profiles()
            .into_iter()
            .find(|candidate| candidate.kind == profile)
            .unwrap_or_else(|| {
                PublicBookProfile::new(
                    PublicBookProfileKind::FastestDepth,
                    "sub.depth.full",
                    5,
                    None,
                )
                .with_sequence(true)
                .with_primary_fast_leg(false)
            });
        vec![
            WsSubscription::new(ExchangeId::Mexc, selected.channel, symbols.to_vec())
                .with_route(format!(
                    "{MEXC_PUBLIC_WS}:{}:{}",
                    selected.channel, selected.depth
                ))
                .with_profile(selected),
        ]
    }

    fn build_public_ws_subscriptions(&self, symbols: &[ExchangeSymbol]) -> Vec<WsSubscription> {
        self.build_public_ws_subscriptions_for_profile(symbols, PublicBookProfileKind::FastestDepth)
    }

    fn parse_public_ws_message(
        &self,
        raw: &str,
        recv_ts: DateTime<Utc>,
    ) -> anyhow::Result<Vec<MarketEvent>> {
        let value: Value = serde_json::from_str(raw).context("parse mexc public ws json")?;
        let Some(book) = parse_orderbook(&value, recv_ts, "mexc.depth", None)? else {
            return Ok(Vec::new());
        };
        Ok(vec![MarketEvent::OrderBook(book)])
    }

    async fn fetch_orderbook_snapshot(
        &self,
        symbol: &ExchangeSymbol,
        depth: u16,
    ) -> anyhow::Result<OrderBookSnapshot> {
        if symbol.exchange != ExchangeId::Mexc {
            return Err(anyhow!(
                "mexc orderbook snapshot received non-mexc symbol: {}",
                symbol.symbol
            ));
        }

        let limit = depth.clamp(5, 20).to_string();
        let value = get_json(
            &format!(
                "{MEXC_REST_BASE}/api/v1/contract/depth/{}?limit={limit}",
                symbol.symbol
            ),
            "mexc depth",
        )
        .await?;
        ensure_mexc_success(&value, "mexc depth")?;
        let data = value
            .get("data")
            .ok_or_else(|| anyhow!("mexc depth response missing data"))?;
        parse_orderbook(
            data,
            Utc::now(),
            "mexc.rest.depth",
            Some(symbol.symbol.as_str()),
        )?
        .ok_or_else(|| anyhow!("mexc depth response missing bids/asks"))
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

fn ensure_mexc_success(value: &Value, label: &str) -> anyhow::Result<()> {
    let success = value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let code = value.get("code").and_then(parse_json_u64).unwrap_or(0);
    if success && code == 0 {
        return Ok(());
    }
    let message = value
        .get("message")
        .or_else(|| value.get("msg"))
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    Err(anyhow!("{label} returned code {code}: {message}"))
}

fn parse_orderbook(
    value: &Value,
    recv_ts: DateTime<Utc>,
    route: &str,
    fallback_symbol: Option<&str>,
) -> anyhow::Result<Option<OrderBook5>> {
    let payload = value.get("data").unwrap_or(value);
    let bids_value = payload
        .get("bids")
        .or_else(|| payload.get("b"))
        .or_else(|| payload.get("bids20"));
    let asks_value = payload
        .get("asks")
        .or_else(|| payload.get("a"))
        .or_else(|| payload.get("asks20"));
    let (Some(bids_value), Some(asks_value)) = (bids_value, asks_value) else {
        return Ok(None);
    };
    let symbol = value
        .get("symbol")
        .or_else(|| payload.get("symbol"))
        .or_else(|| value.get("s"))
        .or_else(|| payload.get("s"))
        .and_then(Value::as_str)
        .or(fallback_symbol)
        .ok_or_else(|| anyhow!("mexc orderbook missing symbol"))?;
    let canonical = compact_symbol_to_canonical(symbol)
        .ok_or_else(|| anyhow!("mexc unsupported symbol: {symbol}"))?;
    let exchange_ts = timestamp_from_value(
        payload
            .get("timestamp")
            .or_else(|| payload.get("ts"))
            .or_else(|| payload.get("t"))
            .or_else(|| value.get("ts")),
        recv_ts,
    );
    let sequence = payload
        .get("version")
        .or_else(|| payload.get("r"))
        .or_else(|| payload.get("id"))
        .and_then(parse_json_u64);
    let source_route = value
        .get("channel")
        .and_then(Value::as_str)
        .map(|channel| format!("mexc.{channel}"))
        .unwrap_or_else(|| route.to_string());

    Ok(Some(OrderBook5::new(
        ExchangeId::Mexc,
        canonical,
        ExchangeSymbol::new(ExchangeId::Mexc, symbol.to_ascii_uppercase()),
        parse_levels(bids_value),
        parse_levels(asks_value),
        exchange_ts,
        recv_ts,
        sequence,
        Some(source_route),
    )))
}

fn parse_levels(value: &Value) -> Vec<BookLevel> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_array)
        .filter_map(|level| parse_level_pair(level))
        .collect()
}

fn parse_instrument(value: &Value) -> Option<InstrumentMeta> {
    let symbol = value.get("symbol").and_then(Value::as_str)?;
    let canonical = compact_symbol_to_canonical(symbol)?;
    if canonical.quote() != "USDT" {
        return None;
    }
    let price_tick = value
        .get("priceUnit")
        .or_else(|| value.get("price_unit"))
        .and_then(parse_json_f64)
        .unwrap_or_else(|| {
            precision_step(
                value
                    .get("priceScale")
                    .and_then(parse_json_u64)
                    .unwrap_or(0) as u32,
            )
        });
    let quantity_step = value
        .get("volUnit")
        .or_else(|| value.get("vol_unit"))
        .and_then(parse_json_f64)
        .unwrap_or(1.0);
    let min_qty = value
        .get("minVol")
        .or_else(|| value.get("min_vol"))
        .and_then(parse_json_f64)
        .unwrap_or(quantity_step);
    let contract_size = value
        .get("contractSize")
        .or_else(|| value.get("contract_size"))
        .and_then(parse_json_f64)
        .unwrap_or(1.0);
    let min_notional = value
        .get("minNotional")
        .or_else(|| value.get("min_notional"))
        .and_then(parse_json_f64)
        .unwrap_or(0.0);
    let status = match value.get("state").and_then(parse_json_u64) {
        Some(0) => InstrumentStatus::Trading,
        Some(1) | Some(2) => InstrumentStatus::Paused,
        Some(3) => InstrumentStatus::Delisted,
        _ => InstrumentStatus::Unknown,
    };

    Some(
        InstrumentMeta::new(
            ExchangeId::Mexc,
            canonical.clone(),
            ExchangeSymbol::new(ExchangeId::Mexc, symbol.to_ascii_uppercase()),
            value
                .get("baseCoin")
                .or_else(|| value.get("base_coin"))
                .and_then(Value::as_str)
                .unwrap_or_else(|| canonical.base()),
            "USDT",
            "USDT",
            ContractType::LinearPerpetual,
            contract_size,
            price_tick,
            quantity_step,
            min_qty,
            min_notional,
            decimal_places(price_tick),
            decimal_places(quantity_step),
            status,
        )
        .with_order_capabilities(true, true, true, true),
    )
}

fn parse_funding(value: &Value, recv_ts: DateTime<Utc>) -> Option<MarketFundingSnapshot> {
    let symbol = value.get("symbol").and_then(Value::as_str)?;
    let canonical = compact_symbol_to_canonical(symbol)?;
    let funding_rate = value
        .get("fundingRate")
        .or_else(|| value.get("funding_rate"))
        .and_then(parse_json_f64)?;
    let next_funding_time = value
        .get("nextSettleTime")
        .or_else(|| value.get("next_settle_time"))
        .or_else(|| value.get("settleTime"))
        .map(|value| timestamp_from_value(Some(value), recv_ts));
    Some(MarketFundingSnapshot::new(
        ExchangeId::Mexc,
        canonical,
        Some(ExchangeSymbol::new(
            ExchangeId::Mexc,
            symbol.to_ascii_uppercase(),
        )),
        funding_rate,
        next_funding_time,
        recv_ts,
    ))
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

fn decimal_places(value: f64) -> u32 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    format!("{value:.12}")
        .trim_end_matches('0')
        .split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
        .unwrap_or_default()
}

fn precision_step(precision: u32) -> f64 {
    10_f64.powi(-(precision as i32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mexc_should_build_depth_subscription() {
        let symbols = vec![
            ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT"),
            ExchangeSymbol::new(ExchangeId::Mexc, "ETH_USDT"),
        ];

        let subscriptions = MexcMarketAdapter.build_public_ws_subscriptions(&symbols);

        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].channel, "sub.depth.full");
        assert_eq!(
            subscriptions[0].route.as_deref(),
            Some("wss://contract.mexc.com/edge:sub.depth.full:5")
        );
        assert_eq!(subscriptions[0].symbols, symbols);
        assert_eq!(
            subscriptions[0].profile,
            PublicBookProfileKind::FastestDepth
        );
        assert_eq!(subscriptions[0].depth, 5);
    }

    #[test]
    fn mexc_should_parse_depth_message() {
        let raw = r#"{
            "channel":"push.depth.full",
            "symbol":"BTC_USDT",
            "data":{
                "asks":[["65000.2","0.3"]],
                "bids":[["65000.1","0.2"]],
                "version":123,
                "timestamp":1710000000000
            },
            "ts":1710000000001
        }"#;
        let recv_ts = DateTime::<Utc>::from_timestamp_millis(1710000000100).unwrap();

        let events = MexcMarketAdapter
            .parse_public_ws_message(raw, recv_ts)
            .expect("valid mexc depth");

        assert_eq!(events.len(), 1);
        let MarketEvent::OrderBook(book) = &events[0] else {
            panic!("expected orderbook");
        };
        assert_eq!(book.exchange, ExchangeId::Mexc);
        assert_eq!(book.exchange_symbol.symbol, "BTC_USDT");
        assert_eq!(book.canonical_symbol, CanonicalSymbol::new("BTC", "USDT"));
        assert_eq!(book.sequence, Some(123));
        assert!(book.is_usable());
    }

    #[test]
    fn mexc_should_parse_instrument_rules() {
        let raw = r#"{
            "symbol":"ARB_USDT",
            "baseCoin":"ARB",
            "quoteCoin":"USDT",
            "state":0,
            "priceUnit":"0.0001",
            "volUnit":"1",
            "minVol":"1",
            "contractSize":"1",
            "minNotional":"5"
        }"#;
        let value: Value = serde_json::from_str(raw).unwrap();

        let instrument = parse_instrument(&value).expect("instrument");

        assert_eq!(instrument.exchange, ExchangeId::Mexc);
        assert_eq!(instrument.price_tick, 0.0001);
        assert_eq!(instrument.quantity_step, 1.0);
        assert_eq!(instrument.min_notional, 5.0);
    }
}

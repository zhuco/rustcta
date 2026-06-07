use super::{
    compact_symbol_to_canonical, compact_usdt_symbol, datetime_from_millis, parse_json_f64,
    parse_json_u64, parse_level_pair, ExchangeMarketAdapterInfo, MarketAdapterInfo,
    MarketCapabilities,
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

const BYBIT_REST_BASE: &str = "https://api.bybit.com";
const BYBIT_PUBLIC_WS: &str = "wss://stream.bybit.com/v5/public/linear";
const BYBIT_CATEGORY: &str = "linear";

#[derive(Debug, Clone, Copy, Default)]
pub struct BybitMarketAdapter;

impl MarketAdapterInfo for BybitMarketAdapter {
    fn info(&self) -> ExchangeMarketAdapterInfo {
        ExchangeMarketAdapterInfo {
            exchange: ExchangeId::Bybit,
            name: "bybit-v5-linear-market",
            venue_symbol_example: "BTCUSDT",
            capabilities: MarketCapabilities::new(true, true, true, true, true),
            protocol_notes: &[
                "Bybit V5 linear market data uses category=linear and compact symbols.",
                "Public orderbook topics carry update ids; production gap recovery must resync from REST on sequence gaps.",
            ],
        }
    }

    fn to_exchange_symbol(&self, canonical: &CanonicalSymbol) -> ExchangeSymbol {
        ExchangeSymbol::new(ExchangeId::Bybit, compact_usdt_symbol(canonical))
    }
}

#[async_trait]
impl MarketDataAdapter for BybitMarketAdapter {
    fn exchange(&self) -> ExchangeId {
        ExchangeId::Bybit
    }

    fn capabilities(&self) -> DataMarketCapabilities {
        DataMarketCapabilities::new(true, true, true, true, true)
    }

    async fn load_instruments(&self) -> anyhow::Result<Vec<InstrumentMeta>> {
        let client = crate::core::http2_fix::shared_http_client();
        let mut cursor: Option<String> = None;
        let mut instruments = Vec::new();

        loop {
            let mut request = client
                .get(format!("{BYBIT_REST_BASE}/v5/market/instruments-info"))
                .query(&[("category", BYBIT_CATEGORY), ("limit", "1000")]);
            if let Some(cursor) = cursor.as_deref() {
                request = request.query(&[("cursor", cursor)]);
            }

            let value: Value = request
                .send()
                .await
                .context("bybit instruments request failed")?
                .error_for_status()?
                .json()
                .await
                .context("decode bybit instruments")?;
            ensure_bybit_success(&value, "bybit instruments")?;

            let result = value
                .get("result")
                .ok_or_else(|| anyhow!("bybit instruments response missing result"))?;
            instruments.extend(
                result
                    .get("list")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(parse_instrument),
            );

            cursor = result
                .get("nextPageCursor")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            if cursor.is_none() {
                break;
            }
        }

        Ok(instruments)
    }

    async fn load_funding(
        &self,
        symbols: &[CanonicalSymbol],
    ) -> anyhow::Result<Vec<MarketFundingSnapshot>> {
        let client = crate::core::http2_fix::shared_http_client();
        let recv_ts = Utc::now();
        let mut request = client
            .get(format!("{BYBIT_REST_BASE}/v5/market/tickers"))
            .query(&[("category", BYBIT_CATEGORY)]);
        if symbols.len() == 1 {
            let symbol = self.to_exchange_symbol(&symbols[0]);
            request = request.query(&[("symbol", symbol.symbol.as_str())]);
        }

        let value: Value = request
            .send()
            .await
            .context("bybit tickers request failed")?
            .error_for_status()?
            .json()
            .await
            .context("decode bybit tickers")?;
        ensure_bybit_success(&value, "bybit tickers")?;

        let wanted = symbols.iter().cloned().collect::<HashSet<_>>();
        let snapshots = value
            .get("result")
            .and_then(|result| result.get("list"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|item| parse_funding(item, recv_ts))
            .filter(|snapshot| wanted.is_empty() || wanted.contains(&snapshot.canonical_symbol))
            .collect();
        Ok(snapshots)
    }

    fn public_book_profiles(&self) -> Vec<PublicBookProfile> {
        vec![
            PublicBookProfile::new(PublicBookProfileKind::FastestL1, "orderbook.1", 1, Some(10))
                .with_sequence(true)
                .with_local_merge(true)
                .with_max_symbols_per_connection(50),
            PublicBookProfile::new(
                PublicBookProfileKind::FastestDepth,
                "orderbook.50",
                50,
                Some(20),
            )
            .with_sequence(true)
            .with_local_merge(true)
            .with_max_symbols_per_connection(50),
            PublicBookProfile::new(
                PublicBookProfileKind::ConservativeDepth,
                "orderbook.200",
                200,
                Some(100),
            )
            .with_sequence(true)
            .with_local_merge(true)
            .with_max_symbols_per_connection(50),
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
                PublicBookProfile::new(
                    PublicBookProfileKind::FastestDepth,
                    "orderbook.50",
                    50,
                    Some(20),
                )
                .with_sequence(true)
                .with_local_merge(true)
            });
        symbols
            .iter()
            .map(|symbol| {
                WsSubscription::new(ExchangeId::Bybit, selected.channel, vec![symbol.clone()])
                    .with_route(format!(
                        "{BYBIT_PUBLIC_WS}:{}.{}",
                        selected.channel, symbol.symbol
                    ))
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
        let value: Value = serde_json::from_str(raw).context("parse bybit public ws json")?;
        let route = value
            .get("topic")
            .and_then(Value::as_str)
            .map(|topic| format!("bybit.{topic}"))
            .unwrap_or_else(|| "bybit.orderbook".to_string());
        let Some(book) = parse_orderbook(&value, recv_ts, &route, None)? else {
            return Ok(Vec::new());
        };
        Ok(vec![MarketEvent::OrderBook(book)])
    }

    async fn fetch_orderbook_snapshot(
        &self,
        symbol: &ExchangeSymbol,
        depth: u16,
    ) -> anyhow::Result<OrderBookSnapshot> {
        if symbol.exchange != ExchangeId::Bybit {
            return Err(anyhow!(
                "bybit orderbook snapshot received non-bybit symbol: {}",
                symbol.symbol
            ));
        }

        let limit = depth.clamp(1, 50).to_string();
        let value: Value = crate::core::http2_fix::shared_http_client()
            .get(format!("{BYBIT_REST_BASE}/v5/market/orderbook"))
            .query(&[
                ("category", BYBIT_CATEGORY),
                ("symbol", symbol.symbol.as_str()),
                ("limit", limit.as_str()),
            ])
            .send()
            .await
            .context("bybit orderbook request failed")?
            .error_for_status()?
            .json()
            .await
            .context("decode bybit orderbook")?;
        ensure_bybit_success(&value, "bybit orderbook")?;

        let result = value
            .get("result")
            .ok_or_else(|| anyhow!("bybit orderbook response missing result"))?;
        parse_orderbook(
            result,
            Utc::now(),
            "bybit.rest.orderbook",
            Some(symbol.symbol.as_str()),
        )?
        .ok_or_else(|| anyhow!("bybit orderbook response missing bids/asks"))
    }
}

fn ensure_bybit_success(value: &Value, label: &str) -> anyhow::Result<()> {
    let code = value
        .get("retCode")
        .and_then(parse_json_u64)
        .unwrap_or_default();
    if code == 0 {
        return Ok(());
    }
    let message = value
        .get("retMsg")
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    Err(anyhow!("{label} returned retCode {code}: {message}"))
}

fn parse_orderbook(
    value: &Value,
    recv_ts: DateTime<Utc>,
    route: &str,
    fallback_symbol: Option<&str>,
) -> anyhow::Result<Option<OrderBook5>> {
    let payload = value.get("data").unwrap_or(value);
    let Some(bids_value) = payload.get("b").or_else(|| payload.get("bids")) else {
        return Ok(None);
    };
    let Some(asks_value) = payload.get("a").or_else(|| payload.get("asks")) else {
        return Ok(None);
    };
    let symbol = payload
        .get("s")
        .or_else(|| payload.get("symbol"))
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("topic")
                .and_then(Value::as_str)
                .and_then(symbol_from_topic)
        })
        .or(fallback_symbol)
        .ok_or_else(|| anyhow!("bybit orderbook missing symbol"))?;
    let canonical = compact_symbol_to_canonical(symbol)
        .ok_or_else(|| anyhow!("bybit unsupported symbol: {symbol}"))?;
    let bids = parse_levels(bids_value);
    let asks = parse_levels(asks_value);
    let exchange_ts = datetime_from_millis(
        payload
            .get("ts")
            .or_else(|| value.get("ts"))
            .or_else(|| payload.get("T"))
            .and_then(parse_json_u64),
        recv_ts,
    );
    let sequence = payload
        .get("u")
        .or_else(|| payload.get("seq"))
        .or_else(|| value.get("seq"))
        .and_then(parse_json_u64);

    Ok(Some(OrderBook5::new(
        ExchangeId::Bybit,
        canonical,
        ExchangeSymbol::new(ExchangeId::Bybit, symbol.to_ascii_uppercase()),
        bids,
        asks,
        exchange_ts,
        recv_ts,
        sequence,
        Some(route.to_string()),
    )))
}

fn symbol_from_topic(topic: &str) -> Option<&str> {
    topic.rsplit('.').next().filter(|symbol| !symbol.is_empty())
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
    if value
        .get("quoteCoin")
        .and_then(Value::as_str)?
        .eq_ignore_ascii_case("USDT")
    {
    } else {
        return None;
    }

    let symbol = value.get("symbol").and_then(Value::as_str)?;
    let canonical = compact_symbol_to_canonical(symbol)?;
    let lot = value.get("lotSizeFilter").unwrap_or(&Value::Null);
    let price = value.get("priceFilter").unwrap_or(&Value::Null);
    let price_tick = price
        .get("tickSize")
        .and_then(parse_json_f64)
        .unwrap_or_default();
    let quantity_step = lot
        .get("qtyStep")
        .and_then(parse_json_f64)
        .unwrap_or_default();
    let min_qty = lot
        .get("minOrderQty")
        .and_then(parse_json_f64)
        .unwrap_or_default();
    let min_notional = lot
        .get("minNotionalValue")
        .and_then(parse_json_f64)
        .unwrap_or_default();
    let status = match value.get("status").and_then(Value::as_str) {
        Some("Trading") => InstrumentStatus::Trading,
        Some("PreLaunch") | Some("Settling") => InstrumentStatus::Paused,
        Some("Closed") => InstrumentStatus::Delisted,
        _ => InstrumentStatus::Unknown,
    };

    Some(
        InstrumentMeta::new(
            ExchangeId::Bybit,
            canonical.clone(),
            ExchangeSymbol::new(ExchangeId::Bybit, symbol.to_ascii_uppercase()),
            value
                .get("baseCoin")
                .and_then(Value::as_str)
                .unwrap_or_else(|| canonical.base()),
            "USDT",
            "USDT",
            ContractType::LinearPerpetual,
            1.0,
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
    let funding_rate = value.get("fundingRate").and_then(parse_json_f64)?;
    let next_funding_time = datetime_from_millis(
        value.get("nextFundingTime").and_then(parse_json_u64),
        recv_ts,
    );
    Some(
        MarketFundingSnapshot::new(
            ExchangeId::Bybit,
            canonical,
            Some(ExchangeSymbol::new(
                ExchangeId::Bybit,
                symbol.to_ascii_uppercase(),
            )),
            funding_rate,
            Some(next_funding_time),
            recv_ts,
        )
        .with_prices(
            value.get("markPrice").and_then(parse_json_f64),
            value.get("indexPrice").and_then(parse_json_f64),
        ),
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bybit_should_build_orderbook_routes() {
        let symbols = vec![
            ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT"),
            ExchangeSymbol::new(ExchangeId::Bybit, "ETHUSDT"),
        ];

        let subscriptions = BybitMarketAdapter.build_public_ws_subscriptions(&symbols);

        assert_eq!(subscriptions.len(), 2);
        assert_eq!(subscriptions[0].channel, "orderbook.50");
        assert_eq!(
            subscriptions[0].route.as_deref(),
            Some("wss://stream.bybit.com/v5/public/linear:orderbook.50.BTCUSDT")
        );
        assert_eq!(
            subscriptions[0].profile,
            PublicBookProfileKind::FastestDepth
        );
        assert_eq!(subscriptions[0].depth, 50);
    }

    #[test]
    fn bybit_should_parse_ws_orderbook() {
        let raw = r#"{
            "topic":"orderbook.1.BTCUSDT",
            "type":"snapshot",
            "ts":1710000000000,
            "data":{
                "s":"BTCUSDT",
                "b":[["65000.1","0.2"]],
                "a":[["65000.2","0.3"]],
                "u":100,
                "seq":200
            }
        }"#;
        let recv_ts = DateTime::<Utc>::from_timestamp_millis(1710000000100).unwrap();

        let events = BybitMarketAdapter
            .parse_public_ws_message(raw, recv_ts)
            .expect("valid bybit ws");

        assert_eq!(events.len(), 1);
        let MarketEvent::OrderBook(book) = &events[0] else {
            panic!("expected orderbook");
        };
        assert_eq!(book.exchange, ExchangeId::Bybit);
        assert_eq!(book.canonical_symbol, CanonicalSymbol::new("BTC", "USDT"));
        assert_eq!(book.sequence, Some(100));
        assert!(book.is_usable());
    }

    #[test]
    fn bybit_should_parse_instrument_rules() {
        let raw = r#"{
            "symbol":"ARBUSDT",
            "baseCoin":"ARB",
            "quoteCoin":"USDT",
            "status":"Trading",
            "priceFilter":{"tickSize":"0.0001"},
            "lotSizeFilter":{"qtyStep":"0.1","minOrderQty":"0.1","minNotionalValue":"5"}
        }"#;
        let value: Value = serde_json::from_str(raw).unwrap();

        let instrument = parse_instrument(&value).expect("instrument");

        assert_eq!(instrument.exchange, ExchangeId::Bybit);
        assert_eq!(instrument.exchange_symbol.symbol, "ARBUSDT");
        assert_eq!(instrument.price_tick, 0.0001);
        assert_eq!(instrument.quantity_step, 0.1);
        assert_eq!(instrument.min_notional, 5.0);
        assert!(instrument.supports_hedge_mode);
    }
}

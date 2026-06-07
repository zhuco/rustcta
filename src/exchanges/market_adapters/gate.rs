use super::{
    compact_symbol_to_canonical, parse_json_f64, parse_json_u64, underscored_symbol,
    ExchangeMarketAdapterInfo, MarketAdapterInfo, MarketCapabilities,
};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::market::{
    BookLevel, CanonicalSymbol, ContractType, ExchangeId, ExchangeSymbol, InstrumentMeta,
    InstrumentStatus, MarketCapabilities as DataMarketCapabilities, MarketDataAdapter, MarketEvent,
    MarketFundingSnapshot, OrderBook5, OrderBookSnapshot, PublicBookProfile, PublicBookProfileKind,
    WsSubscription,
};

const GATE_REST_BASE: &str = "https://api.gateio.ws/api/v4";
const GATE_PUBLIC_WS: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";

#[derive(Debug, Clone, Copy, Default)]
pub struct GateMarketAdapter;

impl MarketAdapterInfo for GateMarketAdapter {
    fn info(&self) -> ExchangeMarketAdapterInfo {
        ExchangeMarketAdapterInfo {
            exchange: ExchangeId::Gate,
            name: "gate-usdt-futures-market",
            venue_symbol_example: "BTC_USDT",
            capabilities: MarketCapabilities::new(true, true, true, true, false),
            protocol_notes: &[
                "Skeleton only. Verify futures contract naming, channel names, and sequence semantics against official Gate futures docs before enabling live parsing.",
                "Do not assume sequence gap recovery until message fields are validated.",
            ],
        }
    }

    fn to_exchange_symbol(&self, canonical: &CanonicalSymbol) -> ExchangeSymbol {
        ExchangeSymbol::new(ExchangeId::Gate, underscored_symbol(canonical))
    }
}

#[async_trait]
impl MarketDataAdapter for GateMarketAdapter {
    fn exchange(&self) -> ExchangeId {
        ExchangeId::Gate
    }

    fn capabilities(&self) -> DataMarketCapabilities {
        DataMarketCapabilities::new(true, true, true, true, true)
    }

    async fn load_instruments(&self) -> anyhow::Result<Vec<InstrumentMeta>> {
        let value = get_json(
            &format!("{GATE_REST_BASE}/futures/usdt/contracts"),
            "gate contracts",
        )
        .await?;

        let instruments = value
            .as_array()
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
        let value = get_json(
            &format!("{GATE_REST_BASE}/futures/usdt/contracts"),
            "gate funding",
        )
        .await?;
        let wanted = symbols
            .iter()
            .map(|symbol| underscored_symbol(symbol))
            .collect::<std::collections::HashSet<_>>();

        let snapshots = value
            .as_array()
            .into_iter()
            .flatten()
            .filter(|item| {
                wanted.is_empty()
                    || item
                        .get("name")
                        .or_else(|| item.get("contract"))
                        .and_then(Value::as_str)
                        .map(|symbol| wanted.contains(symbol))
                        .unwrap_or(false)
            })
            .filter_map(|item| parse_funding(item, recv_ts))
            .collect();

        Ok(snapshots)
    }

    fn public_book_profiles(&self) -> Vec<PublicBookProfile> {
        vec![
            PublicBookProfile::new(
                PublicBookProfileKind::FastestL1,
                "futures.book_ticker",
                1,
                None,
            )
            .with_sequence(true)
            .with_max_symbols_per_connection(50),
            PublicBookProfile::new(
                PublicBookProfileKind::FastestDepth,
                "futures.order_book_update",
                20,
                Some(20),
            )
            .with_sequence(true)
            .with_rest_snapshot(true)
            .with_local_merge(true)
            .with_max_symbols_per_connection(50),
            PublicBookProfile::new(
                PublicBookProfileKind::ConservativeDepth,
                "futures.order_book_update",
                50,
                Some(100),
            )
            .with_sequence(true)
            .with_rest_snapshot(true)
            .with_local_merge(true)
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
                    "futures.order_book_update",
                    20,
                    Some(20),
                )
                .with_sequence(true)
                .with_rest_snapshot(true)
                .with_local_merge(true)
            });

        vec![
            WsSubscription::new(ExchangeId::Gate, selected.channel, symbols.to_vec())
                .with_route(format!(
                    "{GATE_PUBLIC_WS}:{}:{}ms:{}",
                    selected.channel,
                    selected.expected_push_interval_ms.unwrap_or(100),
                    selected.depth
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
        let value: Value = serde_json::from_str(raw).context("parse gate public ws json")?;
        let mut events = Vec::new();

        if let Some(result) = value.get("result") {
            if let Some(book) = parse_orderbook_payload(&value, result, recv_ts, None)? {
                events.push(MarketEvent::OrderBook(book));
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
        if symbol.exchange != ExchangeId::Gate {
            return Err(anyhow!(
                "gate orderbook snapshot received non-gate symbol: {}",
                symbol.symbol
            ));
        }

        let limit = depth.clamp(1, 5);
        let url = format!(
            "{GATE_REST_BASE}/futures/usdt/order_book?contract={}&limit={limit}&with_id=true",
            symbol.symbol
        );
        let value = get_json(&url, "gate orderbook").await?;

        parse_orderbook_payload(&value, &value, Utc::now(), Some(symbol.symbol.as_str()))?
            .ok_or_else(|| {
                anyhow!(
                    "gate orderbook response missing bids/asks for {}",
                    symbol.symbol
                )
            })
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
        .ok_or_else(|| anyhow!("gate orderbook message missing symbol/instId/contract"))?;
    let canonical_symbol = compact_symbol_to_canonical(symbol)
        .ok_or_else(|| anyhow!("gate orderbook message has unsupported symbol: {symbol}"))?;
    let exchange_symbol = ExchangeSymbol::new(ExchangeId::Gate, symbol.to_ascii_uppercase());
    let bids = parse_gate_side_levels(payload, bids_value, "b", "B");
    let asks = parse_gate_side_levels(payload, asks_value, "a", "A");
    let exchange_ts = timestamp_from_value(
        payload
            .get("t")
            .or_else(|| payload.get("ts"))
            .or_else(|| root.get("time_ms"))
            .or_else(|| root.get("current"))
            .or_else(|| root.get("update")),
        recv_ts,
    );
    let sequence = payload
        .get("id")
        .or_else(|| payload.get("u"))
        .or_else(|| payload.get("sequence"))
        .or_else(|| root.get("id"))
        .and_then(parse_json_u64);
    let source_route = root
        .get("channel")
        .and_then(Value::as_str)
        .map(|channel| format!("gate.{channel}"))
        .unwrap_or_else(|| "gate-public".to_string());

    Ok(Some(OrderBook5::new(
        ExchangeId::Gate,
        canonical_symbol,
        exchange_symbol,
        bids,
        asks,
        exchange_ts,
        recv_ts,
        sequence,
        Some(source_route),
    )))
}

fn extract_symbol(value: &Value) -> Option<&str> {
    value
        .get("contract")
        .or_else(|| value.get("symbol"))
        .or_else(|| value.get("instId"))
        .or_else(|| value.get("s"))
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("arg")
                .and_then(|arg| {
                    arg.get("contract")
                        .or_else(|| arg.get("symbol"))
                        .or_else(|| arg.get("instId"))
                })
                .and_then(Value::as_str)
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

fn parse_gate_side_levels(
    payload: &Value,
    value: &Value,
    price_key: &str,
    quantity_key: &str,
) -> Vec<BookLevel> {
    if value.as_array().is_some() {
        return parse_levels(value);
    }

    let Some(price) = value
        .as_str()
        .and_then(|text| {
            if text.is_empty() {
                None
            } else {
                text.parse::<f64>().ok()
            }
        })
        .or_else(|| parse_json_f64(value))
    else {
        return Vec::new();
    };
    let Some(quantity) = payload.get(quantity_key).and_then(parse_json_f64) else {
        return Vec::new();
    };
    if payload.get(price_key).is_some() {
        vec![BookLevel::new(price, quantity)]
    } else {
        Vec::new()
    }
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
    let symbol = value
        .get("name")
        .or_else(|| value.get("contract"))
        .and_then(Value::as_str)?;
    let canonical_symbol = compact_symbol_to_canonical(symbol)?;
    if canonical_symbol.quote() != "USDT" {
        return None;
    }

    let price_tick = value
        .get("order_price_round")
        .or_else(|| value.get("mark_price_round"))
        .and_then(parse_json_f64)
        .unwrap_or(0.0);
    let quantity_step = value
        .get("order_size_round")
        .and_then(parse_json_f64)
        .unwrap_or(1.0);
    let min_qty = value
        .get("order_size_min")
        .and_then(parse_json_f64)
        .unwrap_or(0.0);
    let contract_size = value
        .get("quanto_multiplier")
        .and_then(parse_json_f64)
        .unwrap_or(1.0);
    let price_precision = decimal_precision(price_tick);
    let quantity_precision = decimal_precision(quantity_step);
    let status = if value
        .get("in_delisting")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        InstrumentStatus::CloseOnly
    } else if value
        .get("trade_status")
        .and_then(Value::as_str)
        .map(|status| status.eq_ignore_ascii_case("tradable"))
        .unwrap_or(true)
    {
        InstrumentStatus::Trading
    } else {
        InstrumentStatus::Unknown
    };

    Some(InstrumentMeta::new(
        ExchangeId::Gate,
        canonical_symbol.clone(),
        ExchangeSymbol::new(ExchangeId::Gate, symbol.to_ascii_uppercase()),
        canonical_symbol.base(),
        canonical_symbol.quote(),
        "USDT",
        ContractType::LinearPerpetual,
        contract_size,
        price_tick,
        quantity_step,
        min_qty,
        0.0,
        price_precision,
        quantity_precision,
        status,
    ))
}

fn parse_funding(value: &Value, recv_ts: DateTime<Utc>) -> Option<MarketFundingSnapshot> {
    let symbol = value
        .get("name")
        .or_else(|| value.get("contract"))
        .and_then(Value::as_str)?;
    let canonical_symbol = compact_symbol_to_canonical(symbol)?;
    let funding_rate = value.get("funding_rate").and_then(parse_json_f64)?;
    let next_funding_time = value
        .get("funding_next_apply")
        .map(|value| timestamp_from_value(Some(value), recv_ts));

    Some(
        MarketFundingSnapshot::new(
            ExchangeId::Gate,
            canonical_symbol,
            Some(ExchangeSymbol::new(
                ExchangeId::Gate,
                symbol.to_ascii_uppercase(),
            )),
            funding_rate,
            next_funding_time,
            recv_ts,
        )
        .with_prices(
            value.get("mark_price").and_then(parse_json_f64),
            value.get("index_price").and_then(parse_json_f64),
        ),
    )
}

fn decimal_precision(step: f64) -> u32 {
    if !step.is_finite() || step <= 0.0 {
        return 0;
    }

    let text = format!("{step:.12}");
    text.trim_end_matches('0')
        .split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_should_build_single_orderbook_subscription_for_multiple_symbols() {
        let symbols = vec![
            ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
            ExchangeSymbol::new(ExchangeId::Gate, "ETH_USDT"),
        ];

        let subscriptions = GateMarketAdapter.build_public_ws_subscriptions(&symbols);

        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].exchange, ExchangeId::Gate);
        assert_eq!(subscriptions[0].channel, "futures.order_book_update");
        assert_eq!(subscriptions[0].symbols, symbols);
        assert_eq!(
            subscriptions[0].route.as_deref(),
            Some("wss://fx-ws.gateio.ws/v4/ws/usdt:futures.order_book_update:20ms:20")
        );
        assert_eq!(
            subscriptions[0].profile,
            PublicBookProfileKind::FastestDepth
        );
        assert_eq!(subscriptions[0].depth, 20);
    }

    #[test]
    fn gate_should_build_book_ticker_subscription_for_fastest_l1() {
        let symbols = vec![
            ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
            ExchangeSymbol::new(ExchangeId::Gate, "ETH_USDT"),
        ];

        let subscriptions = GateMarketAdapter
            .build_public_ws_subscriptions_for_profile(&symbols, PublicBookProfileKind::FastestL1);

        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].exchange, ExchangeId::Gate);
        assert_eq!(subscriptions[0].channel, "futures.book_ticker");
        assert_eq!(subscriptions[0].symbols, symbols);
        assert_eq!(subscriptions[0].profile, PublicBookProfileKind::FastestL1);
        assert_eq!(subscriptions[0].depth, 1);
        assert!(!subscriptions[0].requires_rest_snapshot);
        assert!(!subscriptions[0].requires_local_merge);
    }

    #[test]
    fn gate_should_parse_orderbook_snapshot() {
        let raw = r#"{
            "time":1541500161,
            "time_ms":1541500161123,
            "channel":"futures.order_book",
            "event":"all",
            "result":{
                "t":1710000000000,
                "contract":"BTC_USDT",
                "id":93973511,
                "asks":[{"p":"65000.2","s":"0.3"},{"p":"65000.3","s":"0.4"}],
                "bids":[{"p":"65000.1","s":"0.2"},{"p":"65000.0","s":"0.1"}]
            }
        }"#;
        let recv_ts = DateTime::<Utc>::from_timestamp_millis(1710000000100).unwrap();

        let events = GateMarketAdapter
            .parse_public_ws_message(raw, recv_ts)
            .expect("valid gate orderbook");

        assert_eq!(events.len(), 1);
        let MarketEvent::OrderBook(book) = &events[0] else {
            panic!("expected orderbook event");
        };
        assert_eq!(book.exchange, ExchangeId::Gate);
        assert_eq!(book.canonical_symbol.as_pair(), "BTC/USDT");
        assert_eq!(book.exchange_symbol.symbol, "BTC_USDT");
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.asks.len(), 2);
        assert_eq!(book.bids[0], BookLevel::new(65000.1, 0.2));
        assert_eq!(
            book.exchange_ts,
            DateTime::<Utc>::from_timestamp_millis(1710000000000).unwrap()
        );
        assert_eq!(book.sequence, Some(93973511));
    }

    #[test]
    fn gate_should_parse_orderbook_update() {
        let raw = r#"{
            "time":1615366381,
            "time_ms":1615366381123,
            "channel":"futures.order_book_update",
            "event":"update",
            "result":{
                "t":1615366381417,
                "s":"ETH_USDT",
                "U":2517661101,
                "u":2517661113,
                "b":[
                    {"p":"3350.1","s":0},
                    {"p":"3350.0","s":58794}
                ],
                "a":[
                    {"p":"3350.2","s":0},
                    {"p":"3350.3","s":95}
                ]
            }
        }"#;
        let recv_ts = DateTime::<Utc>::from_timestamp_millis(1615366381500).unwrap();

        let events = GateMarketAdapter
            .parse_public_ws_message(raw, recv_ts)
            .expect("valid gate update");

        assert_eq!(events.len(), 1);
        let MarketEvent::OrderBook(book) = &events[0] else {
            panic!("expected orderbook event");
        };
        assert_eq!(book.exchange, ExchangeId::Gate);
        assert_eq!(book.canonical_symbol.as_pair(), "ETH/USDT");
        assert_eq!(book.exchange_symbol.symbol, "ETH_USDT");
        assert_eq!(book.bids[0], BookLevel::new(3350.1, 0.0));
        assert_eq!(book.asks[0], BookLevel::new(3350.2, 0.0));
        assert_eq!(
            book.exchange_ts,
            DateTime::<Utc>::from_timestamp_millis(1615366381417).unwrap()
        );
        assert_eq!(book.sequence, Some(2517661113));
    }

    #[test]
    fn gate_should_parse_rest_orderbook_with_request_symbol() {
        let raw = r#"{
            "id":6440317516,
            "current":1779821773.897,
            "update":1779821773.863,
            "asks":[{"s":2710,"p":"0.10802"}],
            "bids":[{"s":76,"p":"0.108"}]
        }"#;
        let value: Value = serde_json::from_str(raw).expect("valid gate rest book json");

        let book = parse_orderbook_payload(&value, &value, Utc::now(), Some("ARB_USDT"))
            .expect("parse should succeed")
            .expect("book should exist");

        assert_eq!(book.exchange_symbol.symbol, "ARB_USDT");
        assert_eq!(book.canonical_symbol, CanonicalSymbol::new("ARB", "USDT"));
        assert_eq!(book.sequence, Some(6440317516));
        assert!(book.is_usable());
    }

    #[test]
    fn gate_should_parse_non_ascii_contract_name() {
        let value = serde_json::json!({
            "name": "草根文化_USDT",
            "funding_rate": "-0.009172",
            "funding_next_apply": 1780142400,
            "quanto_multiplier": "1000",
            "order_price_round": "0.0000001",
            "order_size_round": "1",
            "order_size_min": 1,
            "status": "trading"
        });

        let instrument = parse_instrument(&value).expect("instrument");
        assert_eq!(instrument.canonical_symbol.as_pair(), "草根文化/USDT");
        assert_eq!(instrument.exchange_symbol.symbol, "草根文化_USDT");

        let funding = parse_funding(&value, Utc::now()).expect("funding");
        assert_eq!(funding.canonical_symbol.as_pair(), "草根文化/USDT");
        assert_eq!(funding.exchange_symbol.unwrap().symbol, "草根文化_USDT");
        assert_eq!(funding.funding_rate, -0.009172);
    }

    #[test]
    fn gate_should_parse_book_ticker_update() {
        let raw = r#"{
            "time":1615366379,
            "time_ms":1615366379123,
            "channel":"futures.book_ticker",
            "event":"update",
            "result":{
                "t":1615366379123,
                "u":"2517661076",
                "s":"BTC_USDT",
                "b":"54696.6",
                "B":"37000",
                "a":"54696.7",
                "A":"47061"
            }
        }"#;

        let events = GateMarketAdapter
            .parse_public_ws_message(raw, Utc::now())
            .expect("valid gate book ticker");

        assert_eq!(events.len(), 1);
        let MarketEvent::OrderBook(book) = &events[0] else {
            panic!("expected orderbook event");
        };
        assert_eq!(book.exchange, ExchangeId::Gate);
        assert_eq!(book.canonical_symbol.as_pair(), "BTC/USDT");
        assert_eq!(book.exchange_symbol.symbol, "BTC_USDT");
        assert_eq!(book.best_bid(), Some(BookLevel::new(54696.6, 37000.0)));
        assert_eq!(book.best_ask(), Some(BookLevel::new(54696.7, 47061.0)));
        assert_eq!(book.sequence, Some(2517661076));
        assert!(book.is_usable());
    }
}

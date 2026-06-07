use super::{
    compact_symbol_to_canonical, datetime_from_millis, parse_json_f64, parse_json_u64,
    parse_level_pair, ExchangeMarketAdapterInfo, MarketAdapterInfo, MarketCapabilities,
};
use crate::market::{
    BookLevel, CanonicalSymbol, ContractType, EventMeta, ExchangeId, ExchangeSymbol,
    InstrumentMeta, InstrumentStatus, MarkPriceSnapshot,
    MarketCapabilities as DataMarketCapabilities, MarketDataAdapter, MarketEvent,
    MarketFundingSnapshot, OrderBook5, OrderBookSnapshot, PublicBookProfile, PublicBookProfileKind,
    WsSubscription,
};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

const TOOBIT_REST_BASE: &str = "https://api.toobit.com";
const TOOBIT_PUBLIC_WS: &str = "wss://stream.toobit.com/quote/ws/v1";

#[derive(Debug, Clone, Copy, Default)]
pub struct ToobitMarketAdapter;

impl MarketAdapterInfo for ToobitMarketAdapter {
    fn info(&self) -> ExchangeMarketAdapterInfo {
        ExchangeMarketAdapterInfo {
            exchange: ExchangeId::Toobit,
            name: "toobit-usdt-futures-market",
            venue_symbol_example: "BTC-SWAP-USDT",
            capabilities: MarketCapabilities::new(true, true, true, true, false),
            protocol_notes: &[
                "Toobit USDT-M public market data uses /api/v1/exchangeInfo contracts and /quote/v1 market data.",
                "Depth websocket payloads include a version string but do not provide a strict monotonic sequence guarantee.",
            ],
        }
    }

    fn to_exchange_symbol(&self, canonical: &CanonicalSymbol) -> ExchangeSymbol {
        ExchangeSymbol::new(ExchangeId::Toobit, toobit_swap_symbol(canonical))
    }
}

#[async_trait]
impl MarketDataAdapter for ToobitMarketAdapter {
    fn exchange(&self) -> ExchangeId {
        ExchangeId::Toobit
    }

    fn capabilities(&self) -> DataMarketCapabilities {
        DataMarketCapabilities::new(true, true, true, true, false)
    }

    async fn load_instruments(&self) -> anyhow::Result<Vec<InstrumentMeta>> {
        let value = get_json(
            &format!("{TOOBIT_REST_BASE}/api/v1/exchangeInfo"),
            "toobit exchangeInfo",
        )
        .await?;
        let contracts = value
            .get("contracts")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("toobit exchangeInfo missing contracts array"))?;

        Ok(contracts.iter().filter_map(parse_instrument).collect())
    }

    async fn load_funding(
        &self,
        symbols: &[CanonicalSymbol],
    ) -> anyhow::Result<Vec<MarketFundingSnapshot>> {
        let recv_ts = Utc::now();
        let client = crate::core::http2_fix::shared_http_client();
        let wanted = symbols
            .iter()
            .map(toobit_swap_symbol)
            .collect::<HashSet<_>>();

        let funding_value: Value = client
            .get(format!("{TOOBIT_REST_BASE}/api/v1/futures/fundingRate"))
            .send()
            .await
            .context("toobit fundingRate request failed")?
            .error_for_status()?
            .json()
            .await
            .context("decode toobit fundingRate")?;
        let mark_prices = load_mark_prices(&client).await.unwrap_or_default();

        let snapshots = value_items(&funding_value)
            .filter(|item| {
                wanted.is_empty()
                    || item
                        .get("symbol")
                        .and_then(Value::as_str)
                        .map(|symbol| wanted.contains(symbol))
                        .unwrap_or(false)
            })
            .filter_map(|item| parse_funding(item, recv_ts, &mark_prices))
            .collect();
        Ok(snapshots)
    }

    fn public_book_profiles(&self) -> Vec<PublicBookProfile> {
        vec![
            PublicBookProfile::new(PublicBookProfileKind::FastestL1, "depth", 5, None)
                .with_max_symbols_per_connection(50),
            PublicBookProfile::new(PublicBookProfileKind::FastestDepth, "depth", 20, None)
                .with_max_symbols_per_connection(50),
            PublicBookProfile::new(PublicBookProfileKind::ConservativeDepth, "depth", 50, None)
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
                PublicBookProfile::new(PublicBookProfileKind::FastestDepth, "depth", 20, None)
            });

        vec![
            WsSubscription::new(ExchangeId::Toobit, selected.channel, symbols.to_vec())
                .with_route(format!(
                    "{TOOBIT_PUBLIC_WS}:{}:{}",
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
        let value: Value = serde_json::from_str(raw).context("parse toobit public ws json")?;
        if value.get("pong").is_some() || value.get("event").is_some() {
            return Ok(Vec::new());
        }

        let topic = value
            .get("topic")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut events = Vec::new();
        match topic {
            "depth" => {
                for payload in data_items(&value) {
                    if let Some(book) = parse_orderbook_payload(
                        &value,
                        payload,
                        recv_ts,
                        Some("toobit.ws.depth"),
                        None,
                    )? {
                        events.push(MarketEvent::OrderBook(book));
                    }
                }
            }
            "markPrice" => {
                for payload in data_items(&value) {
                    if let Some(event) = parse_mark_price_event(&value, payload, recv_ts)? {
                        events.push(MarketEvent::MarkPrice(event));
                    }
                }
            }
            _ => {
                if let Some(book) =
                    parse_orderbook_payload(&value, &value, recv_ts, Some("toobit.ws.depth"), None)?
                {
                    events.push(MarketEvent::OrderBook(book));
                }
            }
        }

        Ok(events)
    }

    async fn fetch_orderbook_snapshot(
        &self,
        symbol: &ExchangeSymbol,
        depth: u16,
    ) -> anyhow::Result<OrderBookSnapshot> {
        if symbol.exchange != ExchangeId::Toobit {
            return Err(anyhow!(
                "toobit orderbook snapshot received non-toobit symbol: {}",
                symbol.symbol
            ));
        }

        let limit = normalize_depth(depth).to_string();
        let value: Value = crate::core::http2_fix::shared_http_client()
            .get(format!("{TOOBIT_REST_BASE}/quote/v1/depth"))
            .query(&[
                ("symbol", symbol.symbol.as_str()),
                ("limit", limit.as_str()),
            ])
            .send()
            .await
            .context("toobit orderbook request failed")?
            .error_for_status()?
            .json()
            .await
            .context("decode toobit orderbook")?;

        parse_orderbook_payload(
            &value,
            &value,
            Utc::now(),
            Some("toobit.rest.depth"),
            Some(symbol.symbol.as_str()),
        )?
        .ok_or_else(|| anyhow!("toobit orderbook response missing bids/asks"))
    }
}

async fn get_json(url: &str, label: &str) -> anyhow::Result<Value> {
    crate::core::http2_fix::shared_http_client()
        .get(url)
        .send()
        .await
        .with_context(|| format!("{label} request failed"))?
        .error_for_status()?
        .json()
        .await
        .with_context(|| format!("decode {label}"))
}

async fn load_mark_prices(client: &reqwest::Client) -> anyhow::Result<HashMap<String, f64>> {
    let value: Value = client
        .get(format!("{TOOBIT_REST_BASE}/quote/v1/markPrice"))
        .send()
        .await
        .context("toobit markPrice request failed")?
        .error_for_status()?
        .json()
        .await
        .context("decode toobit markPrice")?;

    Ok(value_items(&value)
        .filter_map(|item| {
            let symbol = item
                .get("symbolId")
                .or_else(|| item.get("symbol"))
                .and_then(Value::as_str)?;
            let price = item
                .get("price")
                .or_else(|| item.get("markPrice"))
                .and_then(parse_json_f64)?;
            Some((symbol.to_ascii_uppercase(), price))
        })
        .collect())
}

fn parse_orderbook_payload(
    root: &Value,
    payload: &Value,
    recv_ts: DateTime<Utc>,
    source_route: Option<&str>,
    fallback_symbol: Option<&str>,
) -> anyhow::Result<Option<OrderBook5>> {
    let bids_value = payload
        .get("b")
        .or_else(|| payload.get("bids"))
        .or_else(|| root.get("b"))
        .or_else(|| root.get("bids"));
    let asks_value = payload
        .get("a")
        .or_else(|| payload.get("asks"))
        .or_else(|| root.get("a"))
        .or_else(|| root.get("asks"));
    let (Some(bids_value), Some(asks_value)) = (bids_value, asks_value) else {
        return Ok(None);
    };

    let symbol = payload
        .get("s")
        .or_else(|| payload.get("symbol"))
        .or_else(|| root.get("symbol"))
        .and_then(Value::as_str)
        .or(fallback_symbol)
        .ok_or_else(|| anyhow!("toobit orderbook missing symbol"))?;
    let canonical = compact_symbol_to_canonical(symbol)
        .ok_or_else(|| anyhow!("toobit unsupported symbol: {symbol}"))?;
    let exchange_ts = datetime_from_millis(
        payload
            .get("t")
            .or_else(|| payload.get("time"))
            .or_else(|| root.get("t"))
            .or_else(|| root.get("sendTime"))
            .and_then(parse_json_u64),
        recv_ts,
    );
    let sequence = payload
        .get("lastUpdateId")
        .or_else(|| payload.get("u"))
        .or_else(|| payload.get("v"))
        .or_else(|| root.get("v"))
        .and_then(sequence_from_value);

    Ok(Some(OrderBook5::new(
        ExchangeId::Toobit,
        canonical,
        ExchangeSymbol::new(ExchangeId::Toobit, symbol.to_ascii_uppercase()),
        parse_levels(bids_value),
        parse_levels(asks_value),
        exchange_ts,
        recv_ts,
        sequence,
        source_route.map(str::to_string),
    )))
}

fn parse_mark_price_event(
    root: &Value,
    payload: &Value,
    recv_ts: DateTime<Utc>,
) -> anyhow::Result<Option<MarkPriceSnapshot>> {
    let symbol = payload
        .get("symbol")
        .or_else(|| payload.get("symbolId"))
        .or_else(|| root.get("symbol"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("toobit markPrice missing symbol"))?;
    let mark_price = payload
        .get("markPrice")
        .or_else(|| payload.get("price"))
        .and_then(parse_json_f64)
        .ok_or_else(|| anyhow!("toobit markPrice missing price"))?;
    let canonical = compact_symbol_to_canonical(symbol)
        .ok_or_else(|| anyhow!("toobit unsupported markPrice symbol: {symbol}"))?;
    let exchange_ts = payload
        .get("time")
        .or_else(|| root.get("sendTime"))
        .and_then(parse_json_u64)
        .and_then(|millis| DateTime::<Utc>::from_timestamp_millis(millis as i64));

    Ok(Some(MarkPriceSnapshot {
        meta: EventMeta {
            exchange: ExchangeId::Toobit,
            canonical_symbol: Some(canonical),
            exchange_symbol: Some(ExchangeSymbol::new(
                ExchangeId::Toobit,
                symbol.to_ascii_uppercase(),
            )),
            exchange_ts,
            recv_ts,
            sequence: None,
            source_route: Some("toobit.ws.markPrice".to_string()),
        },
        mark_price,
        index_price: payload.get("indexPrice").and_then(parse_json_f64),
    }))
}

fn parse_instrument(value: &Value) -> Option<InstrumentMeta> {
    if value
        .get("inverse")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return None;
    }

    let symbol = value.get("symbol").and_then(Value::as_str)?;
    let canonical = compact_symbol_to_canonical(symbol)?;
    if canonical.quote() != "USDT" {
        return None;
    }

    let filters = value
        .get("filters")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let price_filter = find_filter(&filters, "PRICE_FILTER");
    let lot_filter = find_filter(&filters, "LOT_SIZE");
    let notional_filter = find_filter(&filters, "MIN_NOTIONAL");
    let price_tick = price_filter
        .and_then(|filter| filter.get("tickSize"))
        .and_then(parse_json_f64)
        .unwrap_or_default();
    let quantity_step = lot_filter
        .and_then(|filter| filter.get("stepSize"))
        .and_then(parse_json_f64)
        .unwrap_or_default();
    let min_qty = lot_filter
        .and_then(|filter| filter.get("minQty"))
        .and_then(parse_json_f64)
        .unwrap_or_default();
    let min_notional = notional_filter
        .and_then(|filter| filter.get("minNotional"))
        .and_then(parse_json_f64)
        .unwrap_or_default();
    let contract_size = value
        .get("contractMultiplier")
        .or_else(|| value.get("contractSize"))
        .or_else(|| value.get("multiplier"))
        .and_then(parse_json_f64)
        .unwrap_or(1.0);
    let status = match value.get("status").and_then(Value::as_str) {
        Some("TRADING") | Some("ONLINE") => InstrumentStatus::Trading,
        Some("SUSPENDED") | Some("HALT") => InstrumentStatus::Paused,
        Some("DELISTED") => InstrumentStatus::Delisted,
        _ => InstrumentStatus::Unknown,
    };

    Some(
        InstrumentMeta::new(
            ExchangeId::Toobit,
            canonical.clone(),
            ExchangeSymbol::new(ExchangeId::Toobit, symbol.to_ascii_uppercase()),
            value
                .get("underlying")
                .or_else(|| value.get("baseAsset"))
                .and_then(Value::as_str)
                .unwrap_or_else(|| canonical.base())
                .replace("-SWAP-USDT", ""),
            value
                .get("quoteAsset")
                .and_then(Value::as_str)
                .unwrap_or_else(|| canonical.quote()),
            value
                .get("marginToken")
                .and_then(Value::as_str)
                .unwrap_or("USDT"),
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

fn parse_funding(
    value: &Value,
    recv_ts: DateTime<Utc>,
    mark_prices: &HashMap<String, f64>,
) -> Option<MarketFundingSnapshot> {
    let symbol = value.get("symbol").and_then(Value::as_str)?;
    let canonical = compact_symbol_to_canonical(symbol)?;
    let funding_rate = value.get("rate").and_then(parse_json_f64)?;
    let next_funding_time = value
        .get("nextFundingTime")
        .and_then(parse_json_u64)
        .and_then(|millis| DateTime::<Utc>::from_timestamp_millis(millis as i64));
    let mark_price = value
        .get("markPrice")
        .or_else(|| value.get("price"))
        .and_then(parse_json_f64)
        .or_else(|| mark_prices.get(&symbol.to_ascii_uppercase()).copied());

    Some(
        MarketFundingSnapshot::new(
            ExchangeId::Toobit,
            canonical,
            Some(ExchangeSymbol::new(
                ExchangeId::Toobit,
                symbol.to_ascii_uppercase(),
            )),
            funding_rate,
            next_funding_time,
            recv_ts,
        )
        .with_prices(mark_price, value.get("indexPrice").and_then(parse_json_f64)),
    )
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

fn data_items(value: &Value) -> Box<dyn Iterator<Item = &Value> + '_> {
    match value.get("data") {
        Some(Value::Array(items)) => Box::new(items.iter()),
        Some(item) => Box::new(std::iter::once(item)),
        None => Box::new(std::iter::once(value)),
    }
}

fn value_items(value: &Value) -> Box<dyn Iterator<Item = &Value> + '_> {
    match value {
        Value::Array(items) => Box::new(items.iter()),
        Value::Object(_) => Box::new(std::iter::once(value)),
        _ => Box::new(std::iter::empty()),
    }
}

fn sequence_from_value(value: &Value) -> Option<u64> {
    parse_json_u64(value).or_else(|| {
        value
            .as_str()
            .and_then(|text| text.split('_').next())
            .and_then(|text| text.parse().ok())
    })
}

fn find_filter<'a>(filters: &'a [Value], filter_type: &str) -> Option<&'a Value> {
    filters.iter().find(|filter| {
        filter
            .get("filterType")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case(filter_type))
    })
}

fn toobit_swap_symbol(canonical: &CanonicalSymbol) -> String {
    format!("{}-SWAP-{}", canonical.base(), canonical.quote())
}

fn normalize_depth(depth: u16) -> u16 {
    match depth {
        0..=5 => 5,
        6..=20 => 20,
        21..=50 => 50,
        51..=100 => 100,
        _ => 500,
    }
}

fn decimal_places(value: f64) -> u32 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    format!("{value:.16}")
        .trim_end_matches('0')
        .split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toobit_should_build_grouped_depth_subscription() {
        let symbols = vec![
            ExchangeSymbol::new(ExchangeId::Toobit, "BTC-SWAP-USDT"),
            ExchangeSymbol::new(ExchangeId::Toobit, "ETH-SWAP-USDT"),
        ];

        let subscriptions = ToobitMarketAdapter.build_public_ws_subscriptions(&symbols);

        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].exchange, ExchangeId::Toobit);
        assert_eq!(subscriptions[0].channel, "depth");
        assert_eq!(subscriptions[0].depth, 20);
        assert_eq!(
            subscriptions[0].route.as_deref(),
            Some("wss://stream.toobit.com/quote/ws/v1:depth:20")
        );
    }

    #[test]
    fn toobit_should_parse_contract_instruments() {
        let value: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/exchanges/toobit/exchange_info.json"
        ))
        .unwrap();
        let contracts = value.get("contracts").and_then(Value::as_array).unwrap();

        let instruments = contracts
            .iter()
            .filter_map(parse_instrument)
            .collect::<Vec<_>>();

        assert_eq!(instruments.len(), 1);
        assert_eq!(instruments[0].exchange, ExchangeId::Toobit);
        assert_eq!(
            instruments[0].canonical_symbol,
            CanonicalSymbol::new("BTC", "USDT")
        );
        assert_eq!(instruments[0].exchange_symbol.symbol, "BTC-SWAP-USDT");
        assert_eq!(instruments[0].price_tick, 0.01);
        assert_eq!(instruments[0].quantity_step, 0.0001);
        assert_eq!(instruments[0].min_notional, 10.0);
    }

    #[test]
    fn toobit_should_parse_rest_orderbook() {
        let value: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/exchanges/toobit/orderbook.json"
        ))
        .unwrap();

        let book = parse_orderbook_payload(
            &value,
            &value,
            Utc::now(),
            Some("toobit.rest.depth"),
            Some("BTC-SWAP-USDT"),
        )
        .unwrap()
        .unwrap();

        assert_eq!(book.exchange, ExchangeId::Toobit);
        assert_eq!(book.canonical_symbol.as_pair(), "BTC/USDT");
        assert_eq!(book.bids[0], BookLevel::new(3.9, 431.0));
        assert_eq!(
            book.exchange_ts,
            DateTime::<Utc>::from_timestamp_millis(1672035413265).unwrap()
        );
        assert!(book.is_usable());
    }

    #[test]
    fn toobit_should_parse_ws_depth_and_mark_price() {
        let recv_ts = DateTime::<Utc>::from_timestamp_millis(1710000000100).unwrap();
        let raw = r#"{
          "symbol": "BTC-SWAP-USDT",
          "topic": "depth",
          "data": [{
            "s": "BTC-SWAP-USDT",
            "t": 1710000000000,
            "v": "112801745_18",
            "b": [["65000.1", "0.2"]],
            "a": [["65000.2", "0.3"]]
          }],
          "f": true
        }"#;

        let events = ToobitMarketAdapter
            .parse_public_ws_message(raw, recv_ts)
            .expect("valid depth");
        assert_eq!(events.len(), 1);
        let MarketEvent::OrderBook(book) = &events[0] else {
            panic!("expected orderbook");
        };
        assert_eq!(book.exchange_symbol.symbol, "BTC-SWAP-USDT");
        assert_eq!(book.sequence, Some(112801745));

        let mark_raw = r#"{
          "symbol": "BTC-SWAP-USDT",
          "topic": "markPrice",
          "data": [{
            "symbol": "BTC-SWAP-USDT",
            "markPrice": "16792.28",
            "time": 1668754084000
          }],
          "sendTime": 1668754084738
        }"#;
        let events = ToobitMarketAdapter
            .parse_public_ws_message(mark_raw, recv_ts)
            .expect("valid mark price");
        assert_eq!(events.len(), 1);
        let MarketEvent::MarkPrice(mark) = &events[0] else {
            panic!("expected mark price");
        };
        assert_eq!(mark.meta.exchange, ExchangeId::Toobit);
        assert_eq!(mark.mark_price, 16792.28);
    }

    #[test]
    fn toobit_should_parse_funding_with_mark_price() {
        let value = serde_json::json!({
            "symbol": "BTC-SWAP-USDT",
            "rate": "0.0018099173553719",
            "period": "8H",
            "nextFundingTime": "1668427200000"
        });
        let mut mark_prices = HashMap::new();
        mark_prices.insert("BTC-SWAP-USDT".to_string(), 17042.54471);

        let funding = parse_funding(&value, Utc::now(), &mark_prices).expect("funding");

        assert_eq!(funding.exchange, ExchangeId::Toobit);
        assert_eq!(funding.canonical_symbol.as_pair(), "BTC/USDT");
        assert_eq!(funding.mark_price, Some(17042.54471));
        assert_eq!(
            funding.next_funding_time,
            DateTime::<Utc>::from_timestamp_millis(1668427200000)
        );
    }
}

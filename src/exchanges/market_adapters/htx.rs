use super::{
    compact_symbol_to_canonical, dashed_symbol, parse_json_f64, parse_json_u64, parse_level_pair,
    ExchangeMarketAdapterInfo, MarketAdapterInfo, MarketCapabilities,
};
use crate::market::{
    BookLevel, CanonicalSymbol, ContractType, ExchangeId, ExchangeSymbol, InstrumentMeta,
    InstrumentStatus, MarketCapabilities as DataMarketCapabilities, MarketDataAdapter, MarketEvent,
    MarketFundingSnapshot, OrderBook5, OrderBookSnapshot, WsSubscription,
};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashSet;

const HTX_REST_BASE: &str = "https://api.hbdm.com";
const HTX_PUBLIC_WS: &str = "wss://api.hbdm.com/linear-swap-ws";

#[derive(Debug, Clone, Copy, Default)]
pub struct HtxMarketAdapter;

impl MarketAdapterInfo for HtxMarketAdapter {
    fn info(&self) -> ExchangeMarketAdapterInfo {
        ExchangeMarketAdapterInfo {
            exchange: ExchangeId::Htx,
            name: "htx-linear-swap-market",
            venue_symbol_example: "BTC-USDT",
            capabilities: MarketCapabilities::new(true, true, true, true, false),
            protocol_notes: &[
                "HTX USDT-margined swap uses dashed contract codes such as BTC-USDT.",
                "Public WebSocket frames are gzip-compressed at transport level; this adapter parses the JSON payload after decompression.",
            ],
        }
    }

    fn to_exchange_symbol(&self, canonical: &CanonicalSymbol) -> ExchangeSymbol {
        ExchangeSymbol::new(ExchangeId::Htx, dashed_symbol(canonical))
    }
}

#[async_trait]
impl MarketDataAdapter for HtxMarketAdapter {
    fn exchange(&self) -> ExchangeId {
        ExchangeId::Htx
    }

    fn capabilities(&self) -> DataMarketCapabilities {
        DataMarketCapabilities::new(true, true, true, true, false)
    }

    async fn load_instruments(&self) -> anyhow::Result<Vec<InstrumentMeta>> {
        let value = get_json(
            &format!("{HTX_REST_BASE}/linear-swap-api/v1/swap_contract_info"),
            "htx contract info",
        )
        .await?;
        ensure_htx_success(&value, "htx contract info")?;

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
            let value = get_json(
                &format!(
                    "{HTX_REST_BASE}/linear-swap-api/v1/swap_funding_rate?contract_code={}",
                    exchange_symbol.symbol
                ),
                "htx funding",
            )
            .await?;
            ensure_htx_success(&value, "htx funding")?;
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

    fn build_public_ws_subscriptions(&self, symbols: &[ExchangeSymbol]) -> Vec<WsSubscription> {
        symbols
            .iter()
            .map(|symbol| {
                WsSubscription::new(ExchangeId::Htx, "market.depth.step0", vec![symbol.clone()])
                    .with_route(format!(
                        "{HTX_PUBLIC_WS}:market.{}.depth.step0",
                        symbol.symbol
                    ))
            })
            .collect()
    }

    fn parse_public_ws_message(
        &self,
        raw: &str,
        recv_ts: DateTime<Utc>,
    ) -> anyhow::Result<Vec<MarketEvent>> {
        let value: Value = serde_json::from_str(raw).context("parse htx public ws json")?;
        let Some(book) = parse_orderbook(&value, recv_ts, "htx.depth.step0", None)? else {
            return Ok(Vec::new());
        };
        Ok(vec![MarketEvent::OrderBook(book)])
    }

    async fn fetch_orderbook_snapshot(
        &self,
        symbol: &ExchangeSymbol,
        depth: u16,
    ) -> anyhow::Result<OrderBookSnapshot> {
        if symbol.exchange != ExchangeId::Htx {
            return Err(anyhow!(
                "htx orderbook snapshot received non-htx symbol: {}",
                symbol.symbol
            ));
        }

        let depth_type = if depth <= 5 { "step0" } else { "step0" };
        let value = get_json(
            &format!(
                "{HTX_REST_BASE}/linear-swap-ex/market/depth?contract_code={}&type={depth_type}",
                symbol.symbol
            ),
            "htx depth",
        )
        .await?;
        parse_orderbook(
            &value,
            Utc::now(),
            "htx.rest.depth",
            Some(symbol.symbol.as_str()),
        )?
        .ok_or_else(|| anyhow!("htx depth response missing bids/asks"))
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

fn ensure_htx_success(value: &Value, label: &str) -> anyhow::Result<()> {
    let status = value
        .get("status")
        .or_else(|| value.get("err-code"))
        .and_then(Value::as_str)
        .unwrap_or("ok");
    if status.eq_ignore_ascii_case("ok") {
        return Ok(());
    }
    let message = value
        .get("err_msg")
        .or_else(|| value.get("err-msg"))
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    Err(anyhow!("{label} returned status {status}: {message}"))
}

fn parse_orderbook(
    value: &Value,
    recv_ts: DateTime<Utc>,
    route: &str,
    fallback_symbol: Option<&str>,
) -> anyhow::Result<Option<OrderBook5>> {
    let payload = value.get("tick").unwrap_or(value);
    let (Some(bids_value), Some(asks_value)) = (payload.get("bids"), payload.get("asks")) else {
        return Ok(None);
    };
    let symbol = value
        .get("ch")
        .and_then(Value::as_str)
        .and_then(contract_from_channel)
        .or_else(|| value.get("contract_code").and_then(Value::as_str))
        .or_else(|| payload.get("contract_code").and_then(Value::as_str))
        .or(fallback_symbol)
        .ok_or_else(|| anyhow!("htx orderbook missing contract_code"))?;
    let canonical = compact_symbol_to_canonical(symbol)
        .ok_or_else(|| anyhow!("htx unsupported contract_code: {symbol}"))?;
    let exchange_ts = timestamp_from_value(
        payload
            .get("ts")
            .or_else(|| value.get("ts"))
            .or_else(|| payload.get("mrid")),
        recv_ts,
    );
    let sequence = payload
        .get("version")
        .or_else(|| payload.get("mrid"))
        .or_else(|| value.get("rep"))
        .and_then(parse_json_u64);

    Ok(Some(OrderBook5::new(
        ExchangeId::Htx,
        canonical,
        ExchangeSymbol::new(ExchangeId::Htx, symbol.to_ascii_uppercase()),
        parse_levels(bids_value),
        parse_levels(asks_value),
        exchange_ts,
        recv_ts,
        sequence,
        Some(route.to_string()),
    )))
}

fn contract_from_channel(channel: &str) -> Option<&str> {
    let mut parts = channel.split('.');
    (parts.next()? == "market").then_some(())?;
    parts.next().filter(|contract| !contract.is_empty())
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
    let symbol = value.get("contract_code").and_then(Value::as_str)?;
    let canonical = compact_symbol_to_canonical(symbol)?;
    if canonical.quote() != "USDT" {
        return None;
    }
    let price_tick = value
        .get("price_tick")
        .and_then(parse_json_f64)
        .unwrap_or_else(|| {
            precision_step(
                value
                    .get("price_tick")
                    .and_then(parse_json_u64)
                    .unwrap_or(0) as u32,
            )
        });
    let quantity_step = value
        .get("contract_size")
        .and_then(parse_json_f64)
        .unwrap_or(1.0);
    let min_qty = value
        .get("trade_partition")
        .and_then(parse_json_f64)
        .unwrap_or(1.0);
    let contract_size = value
        .get("contract_size")
        .and_then(parse_json_f64)
        .unwrap_or(1.0);
    let status = match value.get("contract_status").and_then(parse_json_u64) {
        Some(1) => InstrumentStatus::Trading,
        Some(2 | 3 | 4) => InstrumentStatus::Paused,
        Some(5 | 6 | 7) => InstrumentStatus::CloseOnly,
        _ => InstrumentStatus::Unknown,
    };

    Some(
        InstrumentMeta::new(
            ExchangeId::Htx,
            canonical.clone(),
            ExchangeSymbol::new(ExchangeId::Htx, symbol.to_ascii_uppercase()),
            value
                .get("symbol")
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
        )
        .with_order_capabilities(true, true, true, true),
    )
}

fn parse_funding(value: &Value, recv_ts: DateTime<Utc>) -> Option<MarketFundingSnapshot> {
    let symbol = value.get("contract_code").and_then(Value::as_str)?;
    let canonical = compact_symbol_to_canonical(symbol)?;
    let funding_rate = value.get("funding_rate").and_then(parse_json_f64)?;
    let next_funding_time = value
        .get("next_funding_time")
        .or_else(|| value.get("funding_time"))
        .map(|value| timestamp_from_value(Some(value), recv_ts));
    Some(MarketFundingSnapshot::new(
        ExchangeId::Htx,
        canonical,
        Some(ExchangeSymbol::new(
            ExchangeId::Htx,
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
    fn htx_should_build_depth_routes() {
        let symbols = vec![ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT")];

        let subscriptions = HtxMarketAdapter.build_public_ws_subscriptions(&symbols);

        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].channel, "market.depth.step0");
        assert_eq!(
            subscriptions[0].route.as_deref(),
            Some("wss://api.hbdm.com/linear-swap-ws:market.BTC-USDT.depth.step0")
        );
    }

    #[test]
    fn htx_should_parse_depth_message() {
        let raw = r#"{
            "ch":"market.BTC-USDT.depth.step0",
            "ts":1710000000001,
            "tick":{
                "mrid":123,
                "ts":1710000000000,
                "bids":[["65000.1","0.2"]],
                "asks":[["65000.2","0.3"]]
            }
        }"#;
        let recv_ts = DateTime::<Utc>::from_timestamp_millis(1710000000100).unwrap();

        let events = HtxMarketAdapter
            .parse_public_ws_message(raw, recv_ts)
            .expect("valid htx depth");

        assert_eq!(events.len(), 1);
        let MarketEvent::OrderBook(book) = &events[0] else {
            panic!("expected orderbook");
        };
        assert_eq!(book.exchange, ExchangeId::Htx);
        assert_eq!(book.exchange_symbol.symbol, "BTC-USDT");
        assert_eq!(book.canonical_symbol, CanonicalSymbol::new("BTC", "USDT"));
        assert_eq!(book.sequence, Some(123));
        assert!(book.is_usable());
    }

    #[test]
    fn htx_should_parse_contract_rules() {
        let raw = r#"{
            "symbol":"ARB",
            "contract_code":"ARB-USDT",
            "contract_status":1,
            "contract_size":"1",
            "price_tick":"0.0001"
        }"#;
        let value: Value = serde_json::from_str(raw).unwrap();

        let instrument = parse_instrument(&value).expect("instrument");

        assert_eq!(instrument.exchange, ExchangeId::Htx);
        assert_eq!(instrument.exchange_symbol.symbol, "ARB-USDT");
        assert_eq!(instrument.price_tick, 0.0001);
        assert_eq!(instrument.quantity_step, 1.0);
    }
}

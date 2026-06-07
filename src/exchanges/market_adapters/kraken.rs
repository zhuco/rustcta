use super::{
    parse_json_f64, parse_json_u64, parse_level_pair, ExchangeMarketAdapterInfo, MarketAdapterInfo,
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

const KRAKEN_FUTURES_REST_BASE: &str = "https://futures.kraken.com/derivatives/api/v3";
const KRAKEN_FUTURES_WS: &str = "wss://futures.kraken.com/ws/v1";

#[derive(Debug, Clone, Copy, Default)]
pub struct KrakenMarketAdapter;

impl MarketAdapterInfo for KrakenMarketAdapter {
    fn info(&self) -> ExchangeMarketAdapterInfo {
        ExchangeMarketAdapterInfo {
            exchange: ExchangeId::Kraken,
            name: "kraken-futures-market",
            venue_symbol_example: "PF_XBTUSDT",
            capabilities: MarketCapabilities::new(true, true, true, true, false),
            protocol_notes: &[
                "Kraken Spot REST is implemented in src/exchanges/kraken; this market adapter targets Futures public market data.",
                "Futures REST v3 uses /derivatives/api/v3/instruments, /orderbook, and /tickers.",
                "Sequence support is best-effort because public futures snapshots do not expose Binance-style update ids.",
            ],
        }
    }

    fn to_exchange_symbol(&self, canonical: &CanonicalSymbol) -> ExchangeSymbol {
        ExchangeSymbol::new(ExchangeId::Kraken, kraken_futures_symbol(canonical))
    }
}

#[async_trait]
impl MarketDataAdapter for KrakenMarketAdapter {
    fn exchange(&self) -> ExchangeId {
        ExchangeId::Kraken
    }

    fn capabilities(&self) -> DataMarketCapabilities {
        DataMarketCapabilities::new(true, true, true, true, false)
    }

    async fn load_instruments(&self) -> anyhow::Result<Vec<InstrumentMeta>> {
        let value = get_json(
            &format!("{KRAKEN_FUTURES_REST_BASE}/instruments"),
            "kraken futures instruments",
        )
        .await?;
        let instruments = value
            .get("instruments")
            .or_else(|| {
                value
                    .get("result")
                    .and_then(|result| result.get("instruments"))
            })
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("kraken futures instruments response missing instruments"))?;
        Ok(instruments
            .iter()
            .filter_map(parse_instrument)
            .collect::<Vec<_>>())
    }

    async fn load_funding(
        &self,
        symbols: &[CanonicalSymbol],
    ) -> anyhow::Result<Vec<MarketFundingSnapshot>> {
        let value = get_json(
            &format!("{KRAKEN_FUTURES_REST_BASE}/tickers"),
            "kraken futures tickers",
        )
        .await?;
        let tickers = value
            .get("tickers")
            .or_else(|| value.get("result").and_then(|result| result.get("tickers")))
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("kraken futures tickers response missing tickers"))?;
        let wanted = symbols.iter().cloned().collect::<HashSet<_>>();
        let recv_ts = Utc::now();
        Ok(tickers
            .iter()
            .filter_map(|ticker| parse_funding_snapshot(ticker, recv_ts))
            .filter(|snapshot| wanted.is_empty() || wanted.contains(&snapshot.canonical_symbol))
            .collect())
    }

    fn public_book_profiles(&self) -> Vec<PublicBookProfile> {
        vec![
            PublicBookProfile::new(PublicBookProfileKind::FastestL1, "ticker", 1, None)
                .with_max_symbols_per_connection(100),
            PublicBookProfile::new(PublicBookProfileKind::FastestDepth, "book", 10, None)
                .with_local_merge(true)
                .with_rest_snapshot(true)
                .with_max_symbols_per_connection(100),
            PublicBookProfile::new(PublicBookProfileKind::ConservativeDepth, "book", 25, None)
                .with_local_merge(true)
                .with_rest_snapshot(true)
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
                PublicBookProfile::new(PublicBookProfileKind::FastestDepth, "book", 10, None)
                    .with_local_merge(true)
                    .with_rest_snapshot(true)
            });
        symbols
            .iter()
            .map(|symbol| {
                WsSubscription::new(ExchangeId::Kraken, selected.channel, vec![symbol.clone()])
                    .with_route(format!(
                        "{KRAKEN_FUTURES_WS}:{}:{}",
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
        let value: Value = serde_json::from_str(raw).context("parse kraken futures ws json")?;
        let Some(book) = parse_orderbook(&value, recv_ts, "kraken.futures.ws.book", None)? else {
            return Ok(Vec::new());
        };
        Ok(vec![MarketEvent::OrderBook(book)])
    }

    async fn fetch_orderbook_snapshot(
        &self,
        symbol: &ExchangeSymbol,
        _depth: u16,
    ) -> anyhow::Result<OrderBookSnapshot> {
        if symbol.exchange != ExchangeId::Kraken {
            return Err(anyhow!(
                "kraken orderbook snapshot received non-kraken symbol: {}",
                symbol.symbol
            ));
        }
        let value = get_json(
            &format!(
                "{KRAKEN_FUTURES_REST_BASE}/orderbook?symbol={}",
                urlencoding::encode(&symbol.symbol)
            ),
            "kraken futures orderbook",
        )
        .await?;
        parse_orderbook(
            &value,
            Utc::now(),
            "kraken.futures.rest.orderbook",
            Some(symbol.symbol.as_str()),
        )?
        .ok_or_else(|| anyhow!("kraken futures orderbook response missing bids/asks"))
    }
}

async fn get_json(url: &str, label: &str) -> anyhow::Result<Value> {
    let response = crate::core::http2_fix::shared_http_client()
        .get(url)
        .send()
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
    let value: Value =
        serde_json::from_str(&body).with_context(|| format!("{label} invalid json: {body}"))?;
    if value
        .get("result")
        .and_then(Value::as_str)
        .is_some_and(|result| !result.eq_ignore_ascii_case("success"))
    {
        return Err(anyhow!("{label} returned non-success result: {body}"));
    }
    Ok(value)
}

fn kraken_futures_symbol(canonical: &CanonicalSymbol) -> String {
    let base = match canonical.base() {
        "BTC" => "XBT",
        "DOGE" => "XDG",
        other => other,
    };
    format!("PF_{}{}", base, canonical.quote())
}

fn parse_instrument(value: &Value) -> Option<InstrumentMeta> {
    let symbol = value
        .get("symbol")
        .or_else(|| value.get("name"))
        .and_then(Value::as_str)?;
    let canonical = futures_symbol_to_canonical(symbol)
        .or_else(|| parse_canonical_from_assets(value))
        .filter(|canonical| canonical.quote() == "USDT" || canonical.quote() == "USD")?;
    let contract_type = match value
        .get("type")
        .or_else(|| value.get("contractType"))
        .and_then(Value::as_str)
        .unwrap_or("perpetual")
        .to_ascii_lowercase()
        .as_str()
    {
        text if text.contains("perpetual") || text == "futures_inverse" || text == "flexible" => {
            if canonical.quote() == "USDT" {
                ContractType::LinearPerpetual
            } else {
                ContractType::Other("kraken_futures_perpetual".to_string())
            }
        }
        text if text.contains("future") => ContractType::DeliveryFuture,
        other => ContractType::Other(other.to_string()),
    };
    let status = if value
        .get("tradeable")
        .or_else(|| value.get("isTradeable"))
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        InstrumentStatus::Trading
    } else {
        InstrumentStatus::Paused
    };
    let price_tick = value
        .get("tickSize")
        .or_else(|| value.get("tick_size"))
        .and_then(parse_json_f64)
        .unwrap_or(0.1);
    let quantity_step = value
        .get("lotSize")
        .or_else(|| value.get("lot_size"))
        .or_else(|| value.get("contractSize"))
        .and_then(parse_json_f64)
        .unwrap_or(1.0);
    let contract_size = value
        .get("contractSize")
        .or_else(|| value.get("contract_size"))
        .and_then(parse_json_f64)
        .unwrap_or(1.0);
    let min_qty = value
        .get("minTradeSize")
        .or_else(|| value.get("minimumOrderSize"))
        .and_then(parse_json_f64)
        .unwrap_or(quantity_step);
    Some(InstrumentMeta::new(
        ExchangeId::Kraken,
        canonical.clone(),
        ExchangeSymbol::new(ExchangeId::Kraken, symbol.to_ascii_uppercase()),
        canonical.base(),
        canonical.quote(),
        value
            .get("marginCurrency")
            .or_else(|| value.get("settlementCurrency"))
            .and_then(Value::as_str)
            .map(normalize_asset)
            .unwrap_or_else(|| canonical.quote().to_string()),
        contract_type,
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

fn parse_canonical_from_assets(value: &Value) -> Option<CanonicalSymbol> {
    let base = value
        .get("underlying")
        .or_else(|| value.get("base"))
        .or_else(|| value.get("baseCurrency"))
        .and_then(Value::as_str)
        .map(normalize_asset)?;
    let quote = value
        .get("quoteCurrency")
        .or_else(|| value.get("quote"))
        .and_then(Value::as_str)
        .map(normalize_asset)?;
    Some(CanonicalSymbol::new(base, quote))
}

fn parse_funding_snapshot(value: &Value, recv_ts: DateTime<Utc>) -> Option<MarketFundingSnapshot> {
    let symbol = value.get("symbol").and_then(Value::as_str)?;
    let canonical = futures_symbol_to_canonical(symbol)?;
    let funding_rate = value
        .get("fundingRate")
        .or_else(|| value.get("funding_rate"))
        .or_else(|| value.get("relativeFundingRate"))
        .and_then(parse_json_f64)
        .unwrap_or(0.0);
    let mark_price = value
        .get("markPrice")
        .or_else(|| value.get("mark_price"))
        .and_then(parse_json_f64);
    let index_price = value
        .get("indexPrice")
        .or_else(|| value.get("index_price"))
        .and_then(parse_json_f64);
    Some(
        MarketFundingSnapshot::new(
            ExchangeId::Kraken,
            canonical,
            Some(ExchangeSymbol::new(
                ExchangeId::Kraken,
                symbol.to_ascii_uppercase(),
            )),
            funding_rate,
            None,
            recv_ts,
        )
        .with_prices(mark_price, index_price),
    )
}

fn parse_orderbook(
    value: &Value,
    recv_ts: DateTime<Utc>,
    route: &str,
    fallback_symbol: Option<&str>,
) -> anyhow::Result<Option<OrderBook5>> {
    let payload = value
        .get("orderBook")
        .or_else(|| value.get("orderbook"))
        .or_else(|| value.get("book"))
        .or_else(|| value.get("result"))
        .unwrap_or(value);
    let Some(bids_value) = payload.get("bids").or_else(|| payload.get("bid")) else {
        return Ok(None);
    };
    let Some(asks_value) = payload.get("asks").or_else(|| payload.get("ask")) else {
        return Ok(None);
    };
    let symbol = payload
        .get("symbol")
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .or(fallback_symbol)
        .ok_or_else(|| anyhow!("kraken futures orderbook missing symbol"))?;
    let canonical = futures_symbol_to_canonical(symbol)
        .ok_or_else(|| anyhow!("unsupported kraken futures symbol: {symbol}"))?;
    let sequence = payload
        .get("sequence")
        .or_else(|| payload.get("seq"))
        .and_then(parse_json_u64);
    Ok(Some(OrderBook5::new(
        ExchangeId::Kraken,
        canonical,
        ExchangeSymbol::new(ExchangeId::Kraken, symbol.to_ascii_uppercase()),
        parse_levels(bids_value),
        parse_levels(asks_value),
        Utc::now(),
        recv_ts,
        sequence,
        Some(route.to_string()),
    )))
}

fn parse_levels(value: &Value) -> Vec<BookLevel> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|level| {
            if let Some(values) = level.as_array() {
                return parse_level_pair(values);
            }
            Some(BookLevel::new(
                level
                    .get("price")
                    .or_else(|| level.get("p"))
                    .and_then(parse_json_f64)?,
                level
                    .get("qty")
                    .or_else(|| level.get("quantity"))
                    .or_else(|| level.get("size"))
                    .and_then(parse_json_f64)?,
            ))
        })
        .collect()
}

fn futures_symbol_to_canonical(symbol: &str) -> Option<CanonicalSymbol> {
    let normalized = symbol
        .trim()
        .to_ascii_uppercase()
        .replace('-', "_")
        .replace('/', "");
    let raw = normalized
        .strip_prefix("PF_")
        .or_else(|| normalized.strip_prefix("PI_"))
        .or_else(|| normalized.strip_prefix("FI_"))
        .unwrap_or(&normalized);
    for quote in ["USDT", "USDC", "USD", "EUR"] {
        if let Some(base) = raw.strip_suffix(quote) {
            if !base.is_empty() {
                return Some(CanonicalSymbol::new(
                    normalize_asset(base),
                    normalize_asset(quote),
                ));
            }
        }
    }
    None
}

fn normalize_asset(asset: &str) -> String {
    match asset.trim().to_ascii_uppercase().as_str() {
        "XBT" => "BTC".to_string(),
        "XDG" => "DOGE".to_string(),
        value => value.to_string(),
    }
}

fn decimal_places(value: f64) -> u32 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    let text = format!("{value:.12}");
    text.trim_end_matches('0')
        .split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn kraken_market_adapter_should_normalize_symbols() {
        let adapter = KrakenMarketAdapter;
        let symbol = adapter.to_exchange_symbol(&CanonicalSymbol::new("btc", "usdt"));

        assert_eq!(symbol.exchange, ExchangeId::Kraken);
        assert_eq!(symbol.symbol, "PF_XBTUSDT");
    }

    #[test]
    fn kraken_futures_instrument_should_parse_linear_perp() {
        let value = json!({
            "symbol": "PF_XBTUSDT",
            "type": "perpetual",
            "tradeable": true,
            "tickSize": "0.1",
            "contractSize": "1",
            "minTradeSize": "0.001",
            "marginCurrency": "USDT"
        });

        let instrument = parse_instrument(&value).unwrap();

        assert_eq!(instrument.exchange, ExchangeId::Kraken);
        assert_eq!(
            instrument.canonical_symbol,
            CanonicalSymbol::new("btc", "usdt")
        );
        assert_eq!(instrument.contract_type, ContractType::LinearPerpetual);
        assert_eq!(instrument.price_tick, 0.1);
        assert_eq!(instrument.status, InstrumentStatus::Trading);
    }

    #[test]
    fn kraken_futures_orderbook_should_parse_snapshot() {
        let value = json!({
            "orderBook": {
                "symbol": "PF_XBTUSDT",
                "bids": [["37500.0", "2"]],
                "asks": [["37501.0", "3"]],
                "sequence": 42
            }
        });

        let book = parse_orderbook(&value, Utc::now(), "test", None)
            .unwrap()
            .unwrap();

        assert_eq!(book.exchange, ExchangeId::Kraken);
        assert_eq!(book.canonical_symbol, CanonicalSymbol::new("btc", "usdt"));
        assert_eq!(book.best_bid().map(|level| level.price), Some(37500.0));
        assert_eq!(book.sequence, Some(42));
    }

    #[test]
    fn kraken_futures_funding_should_parse_ticker() {
        let value = json!({
            "symbol": "PF_XBTUSDT",
            "fundingRate": "0.0001",
            "markPrice": "37500.5",
            "indexPrice": "37499.9"
        });

        let snapshot = parse_funding_snapshot(&value, Utc::now()).unwrap();

        assert_eq!(
            snapshot.canonical_symbol,
            CanonicalSymbol::new("btc", "usdt")
        );
        assert_eq!(snapshot.funding_rate, 0.0001);
        assert_eq!(snapshot.mark_price, Some(37500.5));
    }
}

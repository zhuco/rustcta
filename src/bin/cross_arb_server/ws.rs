use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rustcta::market::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketDataAdapter, MarketEvent, OrderBook5,
    PublicBookProfileKind, WsSubscription,
};
use serde::Serialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration as TokioDuration};
use tokio_tungstenite::tungstenite::Message;

use rustcta::core::ws_connect::connect_async;

const PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS: u64 = 50;

#[derive(Debug, Clone, Copy)]
pub struct PublicWsConfig {
    pub connect_timeout_ms: u64,
    pub reconnect_delay_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub max_symbols_per_connection: usize,
    pub profile: PublicBookProfileKind,
}

impl Default for PublicWsConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            reconnect_delay_ms: 2_000,
            heartbeat_interval_ms: 20_000,
            max_symbols_per_connection: 80,
            profile: PublicBookProfileKind::FastestDepth,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicWsMarketError {
    pub exchange: ExchangeId,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub kind: String,
    pub message: String,
    pub latency_ms: u128,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum PublicWsUpdate {
    OrderBook(OrderBook5),
    Error(PublicWsMarketError),
    Connected {
        exchange: ExchangeId,
        route: String,
        symbol_count: usize,
        connected_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicWsEndpoint {
    pub exchange: ExchangeId,
    pub url: String,
    pub subscribe_messages: Vec<String>,
    pub symbol_count: usize,
    pub send_interval_ms: u64,
    pub channel: String,
    pub profile: PublicBookProfileKind,
    pub depth: u16,
    pub expected_push_interval_ms: Option<u64>,
}

#[cfg(test)]
pub fn build_public_ws_endpoints(
    exchange: ExchangeId,
    symbols: &[ExchangeSymbol],
    max_symbols_per_connection: usize,
) -> anyhow::Result<Vec<PublicWsEndpoint>> {
    let batch_size = match exchange {
        ExchangeId::Gate => max_symbols_per_connection.min(30).max(1),
        ExchangeId::Htx => max_symbols_per_connection.min(50).max(1),
        _ => max_symbols_per_connection.max(1),
    };
    let mut endpoints = Vec::new();

    for chunk in symbols.chunks(batch_size) {
        endpoints.push(build_public_ws_endpoint(exchange.clone(), chunk)?);
    }

    Ok(endpoints)
}

#[cfg(test)]
fn build_public_ws_endpoint(
    exchange: ExchangeId,
    symbols: &[ExchangeSymbol],
) -> anyhow::Result<PublicWsEndpoint> {
    if symbols.is_empty() {
        return Err(anyhow!("public ws endpoint requires at least one symbol"));
    }

    let endpoint = match exchange {
        ExchangeId::Binance => {
            let streams = symbols
                .iter()
                .map(|symbol| format!("{}@depth5@100ms", symbol.symbol.to_ascii_lowercase()))
                .collect::<Vec<_>>()
                .join("/");
            PublicWsEndpoint {
                exchange,
                url: format!("wss://fstream.binance.com/stream?streams={streams}"),
                subscribe_messages: Vec::new(),
                symbol_count: symbols.len(),
                send_interval_ms: 0,
                channel: "depth5@100ms".to_string(),
                profile: PublicBookProfileKind::FastestDepth,
                depth: 5,
                expected_push_interval_ms: Some(100),
            }
        }
        ExchangeId::Okx => {
            let args = symbols
                .iter()
                .map(|symbol| json!({"channel": "books5", "instId": symbol.symbol}))
                .collect::<Vec<_>>();
            PublicWsEndpoint {
                exchange,
                url: "wss://ws.okx.com:8443/ws/v5/public".to_string(),
                subscribe_messages: vec![json!({"op": "subscribe", "args": args}).to_string()],
                symbol_count: symbols.len(),
                send_interval_ms: PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS,
                channel: "books5".to_string(),
                profile: PublicBookProfileKind::FastestDepth,
                depth: 5,
                expected_push_interval_ms: Some(100),
            }
        }
        ExchangeId::Bitget => {
            let args = symbols
                .iter()
                .map(|symbol| {
                    json!({
                        "instType": "USDT-FUTURES",
                        "channel": "books5",
                        "instId": symbol.symbol
                    })
                })
                .collect::<Vec<_>>();
            PublicWsEndpoint {
                exchange,
                url: "wss://ws.bitget.com/v2/ws/public".to_string(),
                subscribe_messages: vec![json!({"op": "subscribe", "args": args}).to_string()],
                symbol_count: symbols.len(),
                send_interval_ms: PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS,
                channel: "books5".to_string(),
                profile: PublicBookProfileKind::FastestDepth,
                depth: 5,
                expected_push_interval_ms: Some(150),
            }
        }
        ExchangeId::Gate => {
            let subscribe_messages = symbols
                .iter()
                .map(|symbol| {
                    json!({
                        "time": Utc::now().timestamp(),
                        "channel": "futures.order_book",
                        "event": "subscribe",
                        "payload": [symbol.symbol, "5", "0"]
                    })
                    .to_string()
                })
                .collect();
            PublicWsEndpoint {
                exchange,
                url: "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string(),
                subscribe_messages,
                symbol_count: symbols.len(),
                send_interval_ms: PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS,
                channel: "futures.order_book".to_string(),
                profile: PublicBookProfileKind::FastestDepth,
                depth: 5,
                expected_push_interval_ms: None,
            }
        }
        ExchangeId::Bybit => {
            let args = symbols
                .iter()
                .map(|symbol| format!("orderbook.1.{}", symbol.symbol))
                .collect::<Vec<_>>();
            PublicWsEndpoint {
                exchange,
                url: "wss://stream.bybit.com/v5/public/linear".to_string(),
                subscribe_messages: vec![json!({"op": "subscribe", "args": args}).to_string()],
                symbol_count: symbols.len(),
                send_interval_ms: PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS,
                channel: "orderbook.1".to_string(),
                profile: PublicBookProfileKind::FastestL1,
                depth: 1,
                expected_push_interval_ms: Some(10),
            }
        }
        ExchangeId::Mexc => {
            let subscribe_messages = symbols
                .iter()
                .map(|symbol| {
                    json!({
                        "method": "sub.depth.full",
                        "param": {
                            "symbol": symbol.symbol,
                            "limit": 5
                        }
                    })
                    .to_string()
                })
                .collect();
            PublicWsEndpoint {
                exchange,
                url: "wss://contract.mexc.com/edge".to_string(),
                subscribe_messages,
                symbol_count: symbols.len(),
                send_interval_ms: PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS,
                channel: "sub.depth.full".to_string(),
                profile: PublicBookProfileKind::FastestDepth,
                depth: 5,
                expected_push_interval_ms: None,
            }
        }
        ExchangeId::Htx => {
            let subscribe_messages = symbols
                .iter()
                .map(|symbol| {
                    json!({
                        "sub": format!("market.{}.depth.step0", symbol.symbol),
                        "id": format!("{}-depth-step0", symbol.symbol)
                    })
                    .to_string()
                })
                .collect();
            PublicWsEndpoint {
                exchange,
                url: "wss://api.hbdm.com/linear-swap-ws".to_string(),
                subscribe_messages,
                symbol_count: symbols.len(),
                send_interval_ms: PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS,
                channel: "depth.step0".to_string(),
                profile: PublicBookProfileKind::FastestDepth,
                depth: 20,
                expected_push_interval_ms: None,
            }
        }
        ExchangeId::CoinEx | ExchangeId::KuCoin | ExchangeId::Kraken | ExchangeId::Toobit => {
            return Err(anyhow!(
                "unsupported public ws exchange in legacy perp ws server: {}",
                exchange.as_str()
            ));
        }
        ExchangeId::Other(other) => {
            return Err(anyhow!("unsupported public ws exchange: {other}"));
        }
    };

    Ok(endpoint)
}

pub async fn run_public_ws_adapter(
    adapter: Arc<dyn MarketDataAdapter + Send + Sync>,
    symbols: Vec<ExchangeSymbol>,
    config: PublicWsConfig,
    tx: mpsc::Sender<PublicWsUpdate>,
) {
    let exchange = adapter.exchange();
    let endpoints = match build_public_ws_profile_endpoints(
        adapter.as_ref(),
        &symbols,
        config.profile,
        config.max_symbols_per_connection,
    ) {
        Ok(endpoints) => endpoints,
        Err(err) => {
            let _ = tx
                .send(PublicWsUpdate::Error(PublicWsMarketError {
                    exchange,
                    canonical_symbol: None,
                    kind: "ws_config".to_string(),
                    message: err.to_string(),
                    latency_ms: 0,
                    occurred_at: Utc::now(),
                }))
                .await;
            return;
        }
    };

    for endpoint in endpoints {
        let tx = tx.clone();
        let adapter = Arc::clone(&adapter);
        let adapter_exchange = adapter.exchange();
        tokio::spawn(async move {
            run_public_ws_endpoint(adapter, adapter_exchange, endpoint, config, tx).await;
        });
    }
}

pub fn build_public_ws_profile_endpoints(
    adapter: &(dyn MarketDataAdapter + Send + Sync),
    symbols: &[ExchangeSymbol],
    profile: PublicBookProfileKind,
    max_symbols_per_connection: usize,
) -> anyhow::Result<Vec<PublicWsEndpoint>> {
    let exchange = adapter.exchange();
    let profile_batch_size = adapter
        .public_book_profiles()
        .into_iter()
        .find(|candidate| candidate.kind == profile)
        .and_then(|candidate| candidate.max_symbols_per_connection)
        .unwrap_or(max_symbols_per_connection);
    let batch_size = profile_batch_size
        .min(max_symbols_per_connection.max(1))
        .max(1);
    let mut endpoints = Vec::new();

    for chunk in symbols.chunks(batch_size) {
        let subscriptions = adapter.build_public_ws_subscriptions_for_profile(chunk, profile);
        if subscriptions.is_empty() {
            continue;
        }
        endpoints.push(build_public_ws_endpoint_from_subscriptions(
            exchange.clone(),
            &subscriptions,
        )?);
    }

    Ok(endpoints)
}

fn build_public_ws_endpoint_from_subscriptions(
    exchange: ExchangeId,
    subscriptions: &[WsSubscription],
) -> anyhow::Result<PublicWsEndpoint> {
    let first = subscriptions
        .first()
        .ok_or_else(|| anyhow!("public ws endpoint requires at least one subscription"))?;
    let profile = first.profile;
    let depth = first.depth;
    let expected_push_interval_ms = first.expected_push_interval_ms;
    let channel = first.channel.clone();
    let symbols = subscriptions
        .iter()
        .flat_map(|subscription| subscription.symbols.iter().cloned())
        .collect::<Vec<_>>();
    if symbols.is_empty() {
        return Err(anyhow!("public ws endpoint requires at least one symbol"));
    }

    let endpoint = match exchange {
        ExchangeId::Binance => {
            let streams = subscriptions
                .iter()
                .filter_map(|subscription| subscription.route.as_deref())
                .map(str::to_string)
                .collect::<Vec<_>>();
            if streams.is_empty() {
                return Err(anyhow!(
                    "binance public ws subscription missing stream routes"
                ));
            }
            PublicWsEndpoint {
                exchange,
                url: format!(
                    "wss://fstream.binance.com/stream?streams={}",
                    streams.join("/")
                ),
                subscribe_messages: Vec::new(),
                symbol_count: symbols.len(),
                send_interval_ms: 0,
                channel,
                profile,
                depth,
                expected_push_interval_ms,
            }
        }
        ExchangeId::Okx => {
            let args = symbols
                .iter()
                .map(|symbol| json!({"channel": channel.as_str(), "instId": symbol.symbol}))
                .collect::<Vec<_>>();
            PublicWsEndpoint {
                exchange,
                url: "wss://ws.okx.com:8443/ws/v5/public".to_string(),
                subscribe_messages: vec![json!({"op": "subscribe", "args": args}).to_string()],
                symbol_count: symbols.len(),
                send_interval_ms: PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS,
                channel,
                profile,
                depth,
                expected_push_interval_ms,
            }
        }
        ExchangeId::Bitget => {
            let args = symbols
                .iter()
                .map(|symbol| {
                    json!({
                        "instType": "USDT-FUTURES",
                        "channel": channel.as_str(),
                        "instId": symbol.symbol
                    })
                })
                .collect::<Vec<_>>();
            PublicWsEndpoint {
                exchange,
                url: "wss://ws.bitget.com/v2/ws/public".to_string(),
                subscribe_messages: vec![json!({"op": "subscribe", "args": args}).to_string()],
                symbol_count: symbols.len(),
                send_interval_ms: PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS,
                channel,
                profile,
                depth,
                expected_push_interval_ms,
            }
        }
        ExchangeId::Gate => {
            let subscribe_messages = symbols
                .iter()
                .map(|symbol| {
                    let payload = if channel == "futures.book_ticker" {
                        json!([symbol.symbol])
                    } else {
                        let interval = format!("{}ms", expected_push_interval_ms.unwrap_or(100));
                        json!([symbol.symbol, interval, depth.to_string()])
                    };
                    json!({
                        "time": Utc::now().timestamp(),
                        "channel": channel.as_str(),
                        "event": "subscribe",
                        "payload": payload
                    })
                    .to_string()
                })
                .collect();
            PublicWsEndpoint {
                exchange,
                url: "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string(),
                subscribe_messages,
                symbol_count: symbols.len(),
                send_interval_ms: PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS,
                channel,
                profile,
                depth,
                expected_push_interval_ms,
            }
        }
        ExchangeId::Bybit => {
            let args = symbols
                .iter()
                .map(|symbol| format!("{channel}.{}", symbol.symbol))
                .collect::<Vec<_>>();
            PublicWsEndpoint {
                exchange,
                url: "wss://stream.bybit.com/v5/public/linear".to_string(),
                subscribe_messages: vec![json!({"op": "subscribe", "args": args}).to_string()],
                symbol_count: symbols.len(),
                send_interval_ms: PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS,
                channel,
                profile,
                depth,
                expected_push_interval_ms,
            }
        }
        ExchangeId::Mexc => {
            let subscribe_messages = symbols
                .iter()
                .map(|symbol| {
                    json!({
                        "method": channel.as_str(),
                        "param": {
                            "symbol": symbol.symbol,
                            "limit": depth
                        }
                    })
                    .to_string()
                })
                .collect();
            PublicWsEndpoint {
                exchange,
                url: "wss://contract.mexc.com/edge".to_string(),
                subscribe_messages,
                symbol_count: symbols.len(),
                send_interval_ms: PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS,
                channel,
                profile,
                depth,
                expected_push_interval_ms,
            }
        }
        ExchangeId::Htx => {
            let subscribe_messages = symbols
                .iter()
                .map(|symbol| {
                    json!({
                        "sub": format!("market.{}.{}", symbol.symbol, channel),
                        "id": format!("{}-{}", symbol.symbol, channel)
                    })
                    .to_string()
                })
                .collect();
            PublicWsEndpoint {
                exchange,
                url: "wss://api.hbdm.com/linear-swap-ws".to_string(),
                subscribe_messages,
                symbol_count: symbols.len(),
                send_interval_ms: PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS,
                channel,
                profile,
                depth,
                expected_push_interval_ms,
            }
        }
        ExchangeId::Kraken | ExchangeId::Toobit => {
            return Err(anyhow!(
                "profile public ws endpoint is not wired for exchange {}",
                exchange.as_str()
            ));
        }
        ExchangeId::CoinEx | ExchangeId::KuCoin => {
            return Err(anyhow!(
                "unsupported public ws exchange in perp ws server: {}",
                exchange.as_str()
            ));
        }
        ExchangeId::Other(other) => return Err(anyhow!("unsupported public ws exchange: {other}")),
    };

    Ok(endpoint)
}

async fn run_public_ws_endpoint(
    adapter: Arc<dyn MarketDataAdapter + Send + Sync>,
    exchange: ExchangeId,
    endpoint: PublicWsEndpoint,
    config: PublicWsConfig,
    tx: mpsc::Sender<PublicWsUpdate>,
) {
    loop {
        let started = Instant::now();
        match timeout(
            TokioDuration::from_millis(config.connect_timeout_ms.max(100)),
            connect_async(&endpoint.url),
        )
        .await
        {
            Ok(Ok((mut ws, _))) => {
                let _ = tx
                    .send(PublicWsUpdate::Connected {
                        exchange: exchange.clone(),
                        route: endpoint.url.clone(),
                        symbol_count: endpoint.symbol_count,
                        connected_at: Utc::now(),
                    })
                    .await;

                for payload in &endpoint.subscribe_messages {
                    if let Err(err) = ws.send(Message::Text(payload.clone())).await {
                        let _ = tx
                            .send(PublicWsUpdate::Error(ws_error(
                                exchange.clone(),
                                "ws_subscribe",
                                err.to_string(),
                                started.elapsed().as_millis(),
                            )))
                            .await;
                    }
                    if endpoint.send_interval_ms > 0 {
                        sleep(TokioDuration::from_millis(endpoint.send_interval_ms)).await;
                    }
                }

                let mut heartbeat =
                    tokio::time::interval(TokioDuration::from_millis(config.heartbeat_interval_ms));
                loop {
                    tokio::select! {
                        _ = heartbeat.tick() => {
                            let heartbeat = if exchange == ExchangeId::Bitget {
                                Message::Text("ping".to_string())
                            } else {
                                Message::Ping(Vec::new())
                            };
                            if let Err(err) = ws.send(heartbeat).await {
                                let _ = tx.send(PublicWsUpdate::Error(ws_error(
                                    exchange.clone(),
                                    "ws_heartbeat",
                                    err.to_string(),
                                    0,
                                ))).await;
                                break;
                            }
                        }
                        message = ws.next() => {
                            match message {
                                Some(Ok(Message::Text(raw))) => {
                                    handle_ws_text(adapter.as_ref(), exchange.clone(), &endpoint, &raw, &tx).await;
                                }
                                Some(Ok(Message::Binary(raw))) => {
                                    match String::from_utf8(raw) {
                                        Ok(text) => handle_ws_text(adapter.as_ref(), exchange.clone(), &endpoint, &text, &tx).await,
                                        Err(err) => {
                                            let _ = tx.send(PublicWsUpdate::Error(ws_error(
                                                exchange.clone(),
                                                "ws_binary_decode",
                                                err.to_string(),
                                                0,
                                            ))).await;
                                        }
                                    }
                                }
                                Some(Ok(Message::Ping(payload))) => {
                                    let _ = ws.send(Message::Pong(payload)).await;
                                }
                                Some(Ok(Message::Pong(_))) => {}
                                Some(Ok(Message::Close(frame))) => {
                                    let _ = tx.send(PublicWsUpdate::Error(ws_error(
                                        exchange.clone(),
                                        "ws_close",
                                        format!("{frame:?}"),
                                        0,
                                    ))).await;
                                    break;
                                }
                                Some(Err(err)) => {
                                    let _ = tx.send(PublicWsUpdate::Error(ws_error(
                                        exchange.clone(),
                                        "ws_read",
                                        err.to_string(),
                                        0,
                                    ))).await;
                                    break;
                                }
                                None => {
                                    let _ = tx.send(PublicWsUpdate::Error(ws_error(
                                        exchange.clone(),
                                        "ws_eof",
                                        "websocket stream ended".to_string(),
                                        0,
                                    ))).await;
                                    break;
                                }
                                Some(Ok(Message::Frame(_))) => {}
                            }
                        }
                    }
                }
            }
            Ok(Err(err)) => {
                let _ = tx
                    .send(PublicWsUpdate::Error(ws_error(
                        exchange.clone(),
                        "ws_connect",
                        err.to_string(),
                        started.elapsed().as_millis(),
                    )))
                    .await;
            }
            Err(_) => {
                let _ = tx
                    .send(PublicWsUpdate::Error(ws_error(
                        exchange.clone(),
                        "ws_connect_timeout",
                        format!("timed out after {} ms", config.connect_timeout_ms),
                        started.elapsed().as_millis(),
                    )))
                    .await;
            }
        }

        sleep(TokioDuration::from_millis(
            config.reconnect_delay_ms.max(250),
        ))
        .await;
    }
}

async fn handle_ws_text(
    adapter: &(dyn MarketDataAdapter + Send + Sync),
    exchange: ExchangeId,
    endpoint: &PublicWsEndpoint,
    raw: &str,
    tx: &mpsc::Sender<PublicWsUpdate>,
) {
    let trimmed = raw.trim();
    if trimmed == "pong" || trimmed == "ping" {
        return;
    }

    match adapter
        .parse_public_ws_message(raw, Utc::now())
        .with_context(|| format!("parse {exchange} public ws message"))
    {
        Ok(events) => {
            for event in events {
                if let MarketEvent::OrderBook(book) = event {
                    let book = annotate_public_ws_book(book, endpoint);
                    let _ = tx.send(PublicWsUpdate::OrderBook(book)).await;
                }
            }
        }
        Err(err) => {
            let _ = tx
                .send(PublicWsUpdate::Error(ws_error(
                    exchange,
                    "ws_parse",
                    err.to_string(),
                    0,
                )))
                .await;
        }
    }
}

fn annotate_public_ws_book(mut book: OrderBook5, endpoint: &PublicWsEndpoint) -> OrderBook5 {
    let marker = format!(
        "profile_kind={};depth={};channel={}",
        endpoint.profile, endpoint.depth, endpoint.channel
    );
    book.source_route = Some(match book.source_route {
        Some(source_route) if !source_route.is_empty() => format!("{source_route};{marker}"),
        _ => marker,
    });
    book
}

fn ws_error(
    exchange: ExchangeId,
    kind: impl Into<String>,
    message: impl Into<String>,
    latency_ms: u128,
) -> PublicWsMarketError {
    PublicWsMarketError {
        exchange,
        canonical_symbol: None,
        kind: kind.into(),
        message: message.into(),
        latency_ms,
        occurred_at: Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_ws_should_build_binance_combined_stream_for_many_symbols() {
        let endpoint = build_public_ws_endpoint(
            ExchangeId::Binance,
            &[
                ExchangeSymbol::new(ExchangeId::Binance, "ARBUSDT"),
                ExchangeSymbol::new(ExchangeId::Binance, "OPUSDT"),
            ],
        )
        .unwrap();

        assert!(endpoint.url.contains("arbusdt@depth5@100ms"));
        assert!(endpoint.url.contains("opusdt@depth5@100ms"));
        assert!(endpoint.subscribe_messages.is_empty());
    }

    #[test]
    fn public_ws_profile_endpoint_should_use_fastest_l1_subscription() {
        let adapter = rustcta::exchanges::market_adapters::BitgetMarketAdapter;
        let endpoints = build_public_ws_profile_endpoints(
            &adapter,
            &[
                ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                ExchangeSymbol::new(ExchangeId::Bitget, "ETHUSDT"),
            ],
            PublicBookProfileKind::FastestL1,
            80,
        )
        .unwrap();

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].profile, PublicBookProfileKind::FastestL1);
        assert_eq!(endpoints[0].channel, "books1");
        assert_eq!(endpoints[0].depth, 1);
        assert_eq!(endpoints[0].expected_push_interval_ms, Some(10));
        assert!(endpoints[0].subscribe_messages[0].contains("\"channel\":\"books1\""));
    }

    #[test]
    fn public_ws_should_build_okx_single_batch_payload() {
        let endpoint = build_public_ws_endpoint(
            ExchangeId::Okx,
            &[
                ExchangeSymbol::new(ExchangeId::Okx, "ARB-USDT-SWAP"),
                ExchangeSymbol::new(ExchangeId::Okx, "OP-USDT-SWAP"),
            ],
        )
        .unwrap();

        let payload = endpoint.subscribe_messages.join("\n");
        assert!(payload.contains("\"op\":\"subscribe\""));
        assert!(payload.contains("ARB-USDT-SWAP"));
        assert!(payload.contains("OP-USDT-SWAP"));
        assert_eq!(
            endpoint.send_interval_ms,
            PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS
        );
    }

    #[test]
    fn public_ws_should_split_large_symbol_batches() {
        let symbols = (0..3)
            .map(|index| ExchangeSymbol::new(ExchangeId::Bitget, format!("TEST{index}USDT")))
            .collect::<Vec<_>>();

        let endpoints = build_public_ws_endpoints(ExchangeId::Bitget, &symbols, 2).unwrap();

        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].symbol_count, 2);
        assert_eq!(endpoints[1].symbol_count, 1);
    }

    #[test]
    fn public_ws_should_build_gate_payload_per_symbol() {
        let endpoint = build_public_ws_endpoint(
            ExchangeId::Gate,
            &[
                ExchangeSymbol::new(ExchangeId::Gate, "ARB_USDT"),
                ExchangeSymbol::new(ExchangeId::Gate, "OP_USDT"),
            ],
        )
        .unwrap();

        assert_eq!(endpoint.subscribe_messages.len(), 2);
        assert!(endpoint.subscribe_messages[0].contains("futures.order_book"));
        assert!(endpoint.subscribe_messages[0].contains("ARB_USDT"));
        assert!(endpoint.subscribe_messages[0].contains("\"0\""));
        assert_eq!(
            endpoint.send_interval_ms,
            PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS
        );
    }

    #[test]
    fn public_ws_should_build_bybit_orderbook_payload() {
        let endpoint = build_public_ws_endpoint(
            ExchangeId::Bybit,
            &[ExchangeSymbol::new(ExchangeId::Bybit, "BTCUSDT")],
        )
        .unwrap();

        assert_eq!(endpoint.url, "wss://stream.bybit.com/v5/public/linear");
        assert_eq!(endpoint.subscribe_messages.len(), 1);
        assert!(endpoint.subscribe_messages[0].contains("\"op\":\"subscribe\""));
        assert!(endpoint.subscribe_messages[0].contains("orderbook.1.BTCUSDT"));
        assert_eq!(
            endpoint.send_interval_ms,
            PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS
        );
    }

    #[test]
    fn public_ws_should_build_mexc_depth_payload_per_symbol() {
        let endpoint = build_public_ws_endpoint(
            ExchangeId::Mexc,
            &[ExchangeSymbol::new(ExchangeId::Mexc, "BTC_USDT")],
        )
        .unwrap();

        assert_eq!(endpoint.url, "wss://contract.mexc.com/edge");
        assert_eq!(endpoint.subscribe_messages.len(), 1);
        assert!(endpoint.subscribe_messages[0].contains("sub.depth.full"));
        assert!(endpoint.subscribe_messages[0].contains("BTC_USDT"));
        assert_eq!(
            endpoint.send_interval_ms,
            PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS
        );
    }

    #[test]
    fn public_ws_should_build_htx_depth_payload_per_symbol() {
        let endpoint = build_public_ws_endpoint(
            ExchangeId::Htx,
            &[ExchangeSymbol::new(ExchangeId::Htx, "BTC-USDT")],
        )
        .unwrap();

        assert_eq!(endpoint.url, "wss://api.hbdm.com/linear-swap-ws");
        assert_eq!(endpoint.subscribe_messages.len(), 1);
        assert!(endpoint.subscribe_messages[0].contains("market.BTC-USDT.depth.step0"));
        assert_eq!(
            endpoint.send_interval_ms,
            PUBLIC_MARKET_SUBSCRIBE_INTERVAL_MS
        );
    }
}

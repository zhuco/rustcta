use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rustcta::market::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketDataAdapter, MarketEvent, OrderBook5,
};
use serde::Serialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration as TokioDuration};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Debug, Clone, Copy)]
pub struct PublicWsConfig {
    pub connect_timeout_ms: u64,
    pub reconnect_delay_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub max_symbols_per_connection: usize,
}

impl Default for PublicWsConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            reconnect_delay_ms: 2_000,
            heartbeat_interval_ms: 20_000,
            max_symbols_per_connection: 80,
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
}

pub fn build_public_ws_endpoints(
    exchange: ExchangeId,
    symbols: &[ExchangeSymbol],
    max_symbols_per_connection: usize,
) -> anyhow::Result<Vec<PublicWsEndpoint>> {
    let batch_size = match exchange {
        ExchangeId::Gate => max_symbols_per_connection.min(30).max(1),
        _ => max_symbols_per_connection.max(1),
    };
    let mut endpoints = Vec::new();

    for chunk in symbols.chunks(batch_size) {
        endpoints.push(build_public_ws_endpoint(exchange.clone(), chunk)?);
    }

    Ok(endpoints)
}

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
                send_interval_ms: 0,
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
                send_interval_ms: 0,
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
                send_interval_ms: 50,
            }
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
    let endpoints = match build_public_ws_endpoints(
        exchange.clone(),
        &symbols,
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
                            if let Err(err) = ws.send(Message::Ping(Vec::new())).await {
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
                                    handle_ws_text(adapter.as_ref(), exchange.clone(), &raw, &tx).await;
                                }
                                Some(Ok(Message::Binary(raw))) => {
                                    match String::from_utf8(raw) {
                                        Ok(text) => handle_ws_text(adapter.as_ref(), exchange.clone(), &text, &tx).await,
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
    raw: &str,
    tx: &mpsc::Sender<PublicWsUpdate>,
) {
    match adapter
        .parse_public_ws_message(raw, Utc::now())
        .with_context(|| format!("parse {exchange} public ws message"))
    {
        Ok(events) => {
            for event in events {
                if let MarketEvent::OrderBook(book) = event {
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
        assert_eq!(endpoint.send_interval_ms, 50);
    }
}

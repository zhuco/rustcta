use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use secp256k1::{ecdsa::RecoverableSignature, Message as SecpMessage, Secp256k1, SecretKey};
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use tokio::sync::mpsc;
use tokio::time::{timeout, Instant};
use tokio_tungstenite::tungstenite::Message;

use crate::private_ws_probe::{
    env_first, gateio_ws_signature, hmac_sha256_base64, hmac_sha256_hex, read_probe_frame,
    sanitize_value,
};
use crate::ws_proxy_probe::{connect_websocket, reqwest_client_builder_with_ws_proxy};

#[derive(Debug, Clone)]
pub struct PrivateWsObserveConfig {
    pub timeout_ms: u64,
    pub reconnect_delay_ms: u64,
    pub gateio_user_id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum PrivateWsObserveEvent {
    Status { exchange: String, row: Value },
    PrivateEvent(Value),
}

#[derive(Debug, Clone)]
struct PrivateWsObserveState {
    exchange: String,
    connected: bool,
    login_ok: Option<bool>,
    subscribed_channels: Vec<String>,
    message_count: usize,
    event_count: usize,
    disconnect_count: usize,
    last_latency_ms: Option<i64>,
}

pub async fn run_private_ws_observe_once(
    exchanges: &[String],
    config: PrivateWsObserveConfig,
    tx: mpsc::Sender<PrivateWsObserveEvent>,
) {
    dotenv::dotenv().ok();
    for exchange in exchanges {
        match normalize_private_exchange(exchange) {
            Some("binance") => {
                observe_private_exchange_once("binance", config.clone(), tx.clone()).await
            }
            Some("bitget") => {
                observe_private_exchange_once("bitget", config.clone(), tx.clone()).await
            }
            Some("gateio") => {
                observe_private_exchange_once("gateio", config.clone(), tx.clone()).await
            }
            Some("aster") => {
                observe_private_exchange_once("aster", config.clone(), tx.clone()).await
            }
            Some("mexc") => observe_private_exchange_once("mexc", config.clone(), tx.clone()).await,
            Some("kucoinfutures") => {
                observe_private_exchange_once("kucoinfutures", config.clone(), tx.clone()).await
            }
            Some("bybit") => {
                observe_private_exchange_once("bybit", config.clone(), tx.clone()).await
            }
            _ => {}
        }
    }
}

async fn observe_private_exchange_once(
    exchange: &'static str,
    config: PrivateWsObserveConfig,
    tx: mpsc::Sender<PrivateWsObserveEvent>,
) {
    let mut state = PrivateWsObserveState::new(exchange);
    let result = match exchange {
        "binance" => run_binance_private_stream(&config, &mut state, &tx).await,
        "bitget" => run_bitget_private_stream(&config, &mut state, &tx).await,
        "gateio" => run_gateio_private_stream(&config, &mut state, &tx).await,
        "aster" => run_aster_private_stream(&config, &mut state, &tx).await,
        "mexc" => run_mexc_private_stream(&config, &mut state, &tx).await,
        "kucoinfutures" => run_kucoinfutures_private_stream(&config, &mut state, &tx).await,
        "bybit" => run_bybit_private_stream(&config, &mut state, &tx).await,
        _ => unreachable!("private exchange is normalized"),
    };
    if let Err(error) = result {
        state.connected = false;
        state.login_ok = match exchange {
            "bitget" => Some(false),
            _ => state.login_ok,
        };
        state.disconnect_count = state.disconnect_count.saturating_add(1);
        send_status(
            &tx,
            &state,
            "error",
            format!("private websocket error: {error:#}"),
        )
        .await;
    }
}

async fn run_binance_private_stream(
    config: &PrivateWsObserveConfig,
    state: &mut PrivateWsObserveState,
    tx: &mpsc::Sender<PrivateWsObserveEvent>,
) -> Result<()> {
    state.connected = false;
    state.login_ok = None;
    state.subscribed_channels = vec![
        "ORDER_TRADE_UPDATE".to_string(),
        "TRADE_LITE".to_string(),
        "ACCOUNT_UPDATE".to_string(),
    ];

    let api_key = env_first(&[
        "RUSTCTA_BINANCE_API_KEY",
        "BINANCE_0_API_KEY",
        "BINANCE_USDM_API_KEY",
        "BINANCE_API_KEY",
    ])
    .context("missing Binance API key for user data stream listenKey")?;
    let rest_base = env_first(&[
        "RUSTCTA_BINANCE_USDM_REST_BASE_URL",
        "BINANCE_USDM_REST_BASE_URL",
    ])
    .unwrap_or_else(|| "https://fapi.binance.com".to_string());
    let ws_base = env_first(&[
        "RUSTCTA_BINANCE_USDM_PRIVATE_WS_URL",
        "BINANCE_USDM_PRIVATE_WS_URL",
    ])
    .unwrap_or_else(|| "wss://fstream.binance.com/private/ws".to_string());
    let timeout_duration = Duration::from_millis(config.timeout_ms.max(1000));
    let client = reqwest_client_builder_with_ws_proxy()
        .timeout(timeout_duration)
        .build()
        .context("build Binance REST client")?;
    let listen_key = start_binance_listen_key(&client, &rest_base, &api_key).await?;
    let url = format!("{}/{}", ws_base.trim_end_matches('/'), listen_key);
    let result = async {
        let (mut ws, _) = timeout(timeout_duration, connect_websocket(url.as_str()))
            .await
            .context("connect Binance private websocket timed out")?
            .context("connect Binance private websocket")?;
        state.connected = true;
        send_status(
            tx,
            state,
            "online",
            "Binance user data stream connected; listenKey bootstrap used",
        )
        .await;

        let mut next_keepalive = Instant::now() + Duration::from_secs(30 * 60);
        loop {
            if Instant::now() >= next_keepalive {
                keepalive_binance_listen_key(&client, &rest_base, &api_key, &listen_key).await?;
                next_keepalive = Instant::now() + Duration::from_secs(30 * 60);
                send_status(tx, state, "online", "Binance listenKey keepalive ok").await;
            }
            match read_probe_frame(&mut ws, timeout_duration).await? {
                Some(frame) => {
                    state.message_count = state.message_count.saturating_add(1);
                    let now = Utc::now();
                    for row in binance_private_event_rows(&frame.sample, frame.value.as_ref(), now)
                    {
                        if row
                            .get("private_kind")
                            .and_then(Value::as_str)
                            .is_some_and(|kind| matches!(kind, "fill" | "order" | "balance"))
                        {
                            state.event_count = state.event_count.saturating_add(1);
                        }
                        state.last_latency_ms = row.get("latency_ms").and_then(Value::as_i64);
                        tx.send(PrivateWsObserveEvent::PrivateEvent(row)).await.ok();
                    }
                    send_status(
                        tx,
                        state,
                        "online",
                        "Binance private websocket frame received",
                    )
                    .await;
                }
                None => {
                    send_status(tx, state, "online", "Binance private websocket idle").await;
                }
            }
        }
    }
    .await;
    let _ = close_binance_listen_key(&client, &rest_base, &api_key, &listen_key).await;
    result
}

async fn run_bitget_private_stream(
    config: &PrivateWsObserveConfig,
    state: &mut PrivateWsObserveState,
    tx: &mpsc::Sender<PrivateWsObserveEvent>,
) -> Result<()> {
    state.connected = false;
    state.login_ok = Some(false);
    state.subscribed_channels.clear();

    let api_key = env_first(&["RUSTCTA_BITGET_API_KEY", "BITGET_API_KEY"])
        .context("missing Bitget API key")?;
    let api_secret = env_first(&["RUSTCTA_BITGET_API_SECRET", "BITGET_API_SECRET"])
        .context("missing Bitget API secret")?;
    let passphrase = env_first(&[
        "RUSTCTA_BITGET_API_PASSPHRASE",
        "BITGET_PASSPHRASE",
        "BITGET_API_PASSPHRASE",
    ])
    .context("missing Bitget API passphrase")?;
    let url = env_first(&["RUSTCTA_BITGET_PRIVATE_WS_URL", "BITGET_PRIVATE_WS_URL"])
        .unwrap_or_else(|| "wss://ws.bitget.com/v2/ws/private".to_string());
    let timeout_duration = Duration::from_millis(config.timeout_ms.max(1000));
    let (mut ws, _) = timeout(timeout_duration, connect_websocket(url.as_str()))
        .await
        .context("connect Bitget private websocket timed out")?
        .context("connect Bitget private websocket")?;
    state.connected = true;
    send_status(tx, state, "starting", "Bitget private websocket connected").await;

    let timestamp = Utc::now().timestamp_millis().to_string();
    let sign = hmac_sha256_base64(&api_secret, &format!("{timestamp}GET/user/verify"))?;
    let login = json!({
        "op": "login",
        "args": [{
            "apiKey": api_key,
            "passphrase": passphrase,
            "timestamp": timestamp,
            "sign": sign,
        }]
    });
    ws.send(Message::Text(login.to_string()))
        .await
        .context("send Bitget private login")?;

    loop {
        let Some(frame) = read_probe_frame(&mut ws, timeout_duration).await? else {
            continue;
        };
        state.message_count = state.message_count.saturating_add(1);
        let now = Utc::now();
        let value = frame.value.as_ref().unwrap_or(&frame.sample);
        if value.get("event").and_then(Value::as_str) == Some("login")
            && value.get("code").and_then(Value::as_str).unwrap_or("0") == "0"
        {
            state.login_ok = Some(true);
            tx.send(PrivateWsObserveEvent::PrivateEvent(private_event_row(
                "bitget",
                "heartbeat",
                Some("login"),
                None,
                None,
                None,
                None,
                "Bitget private websocket login ok",
                &frame.sample,
                now,
            )))
            .await
            .ok();
            break;
        }
        if value.get("event").and_then(Value::as_str) == Some("error") {
            bail!("Bitget login error: {}", frame.sample);
        }
    }

    let subscribe = json!({
        "op": "subscribe",
        "args": [
            {"instType": "USDT-FUTURES", "channel": "orders", "instId": "default"},
            {"instType": "USDT-FUTURES", "channel": "fill", "instId": "default"},
        ]
    });
    ws.send(Message::Text(subscribe.to_string()))
        .await
        .context("send Bitget private subscribe")?;
    send_status(
        tx,
        state,
        "starting",
        "Bitget private websocket subscriptions sent",
    )
    .await;

    loop {
        match read_probe_frame(&mut ws, timeout_duration).await? {
            Some(frame) => {
                state.message_count = state.message_count.saturating_add(1);
                let now = Utc::now();
                for row in bitget_private_event_rows(&frame.sample, frame.value.as_ref(), now) {
                    if let Some(channel) = row.get("channel").and_then(Value::as_str) {
                        if row
                            .get("private_kind")
                            .and_then(Value::as_str)
                            .is_some_and(|kind| kind == "heartbeat")
                            && !state
                                .subscribed_channels
                                .iter()
                                .any(|known| known == channel)
                        {
                            state.subscribed_channels.push(channel.to_string());
                        }
                    }
                    if row
                        .get("private_kind")
                        .and_then(Value::as_str)
                        .is_some_and(|kind| matches!(kind, "fill" | "order"))
                    {
                        state.event_count = state.event_count.saturating_add(1);
                    }
                    state.last_latency_ms = row.get("latency_ms").and_then(Value::as_i64);
                    tx.send(PrivateWsObserveEvent::PrivateEvent(row)).await.ok();
                }
                let status = if state.subscribed_channels.len() >= 2 {
                    "online"
                } else {
                    "starting"
                };
                send_status(tx, state, status, "Bitget private websocket active").await;
            }
            None => {
                send_status(tx, state, "online", "Bitget private websocket idle").await;
            }
        }
    }
}

async fn run_gateio_private_stream(
    config: &PrivateWsObserveConfig,
    state: &mut PrivateWsObserveState,
    tx: &mpsc::Sender<PrivateWsObserveEvent>,
) -> Result<()> {
    state.connected = false;
    state.login_ok = None;
    state.subscribed_channels.clear();

    let api_key = env_first(&[
        "RUSTCTA_GATEIO_API_KEY",
        "GATEIO_API_KEY",
        "GATE_API_KEY",
        "GATE__16076371__API_KEY",
    ])
    .context("missing Gate.io API key")?;
    let api_secret = env_first(&[
        "RUSTCTA_GATEIO_API_SECRET",
        "GATEIO_API_SECRET",
        "GATE_API_SECRET",
        "GATE__16076371__API_SECRET",
    ])
    .context("missing Gate.io API secret")?;
    let user_id = config
        .gateio_user_id
        .clone()
        .or_else(|| {
            env_first(&[
                "RUSTCTA_GATEIO_USER_ID",
                "GATEIO_USER_ID",
                "GATE_USER_ID",
                "GATE__16076371__USER_ID",
            ])
        })
        .context("missing Gate.io futures user id")?;
    let url = env_first(&[
        "RUSTCTA_GATEIO_FUTURES_PRIVATE_WS_URL",
        "GATEIO_FUTURES_PRIVATE_WS_URL",
        "GATEIO_FUTURES_WS_URL",
    ])
    .unwrap_or_else(|| "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string());
    let timeout_duration = Duration::from_millis(config.timeout_ms.max(1000));
    let (mut ws, _) = timeout(timeout_duration, connect_websocket(url.as_str()))
        .await
        .context("connect Gate.io private websocket timed out")?
        .context("connect Gate.io private websocket")?;
    state.connected = true;
    send_status(tx, state, "starting", "Gate.io private websocket connected").await;

    for channel in ["futures.orders", "futures.usertrades"] {
        let event = "subscribe";
        let timestamp = Utc::now().timestamp();
        let sign = gateio_ws_signature(&api_secret, channel, event, timestamp)?;
        let subscribe = json!({
            "time": timestamp,
            "channel": channel,
            "event": event,
            "payload": [user_id.as_str(), "!all"],
            "auth": {
                "method": "api_key",
                "KEY": api_key.as_str(),
                "SIGN": sign,
            }
        });
        ws.send(Message::Text(subscribe.to_string()))
            .await
            .with_context(|| format!("send Gate.io {channel} subscribe"))?;
    }

    loop {
        match read_probe_frame(&mut ws, timeout_duration).await? {
            Some(frame) => {
                state.message_count = state.message_count.saturating_add(1);
                let now = Utc::now();
                for row in gateio_private_event_rows(&frame.sample, frame.value.as_ref(), now) {
                    if let Some(channel) = row.get("channel").and_then(Value::as_str) {
                        if row
                            .get("private_kind")
                            .and_then(Value::as_str)
                            .is_some_and(|kind| kind == "heartbeat")
                            && !state
                                .subscribed_channels
                                .iter()
                                .any(|known| known == channel)
                        {
                            state.subscribed_channels.push(channel.to_string());
                        }
                    }
                    if row
                        .get("private_kind")
                        .and_then(Value::as_str)
                        .is_some_and(|kind| matches!(kind, "fill" | "order"))
                    {
                        state.event_count = state.event_count.saturating_add(1);
                    }
                    state.last_latency_ms = row.get("latency_ms").and_then(Value::as_i64);
                    tx.send(PrivateWsObserveEvent::PrivateEvent(row)).await.ok();
                }
                let status = if state.subscribed_channels.len() >= 2 {
                    "online"
                } else {
                    "starting"
                };
                send_status(tx, state, status, "Gate.io private websocket active").await;
            }
            None => {
                send_status(tx, state, "online", "Gate.io private websocket idle").await;
            }
        }
    }
}

async fn run_aster_private_stream(
    config: &PrivateWsObserveConfig,
    state: &mut PrivateWsObserveState,
    tx: &mpsc::Sender<PrivateWsObserveEvent>,
) -> Result<()> {
    state.connected = false;
    state.login_ok = None;
    state.subscribed_channels = vec![
        "ORDER_TRADE_UPDATE".to_string(),
        "ACCOUNT_UPDATE".to_string(),
    ];

    let user_address = env_first(&["RUSTCTA_ASTER_USER_ADDRESS", "ASTER_USER_ADDRESS"])
        .context("missing Aster user address")?;
    let signer_address = env_first(&["RUSTCTA_ASTER_SIGNER_ADDRESS", "ASTER_SIGNER_ADDRESS"])
        .context("missing Aster signer address")?;
    let signer_private_key = env_first(&[
        "RUSTCTA_ASTER_SIGNER_PRIVATE_KEY",
        "ASTER_SIGNER_PRIVATE_KEY",
    ])
    .context("missing Aster signer private key")?;
    let rest_base = env_first(&["RUSTCTA_ASTER_REST_BASE_URL", "ASTER_REST_BASE_URL"])
        .unwrap_or_else(|| "https://fapi.asterdex.com".to_string());
    let ws_base = env_first(&["RUSTCTA_ASTER_PRIVATE_WS_URL", "ASTER_PRIVATE_WS_URL"])
        .unwrap_or_else(|| "wss://fstream.asterdex.com/ws".to_string());
    let timeout_duration = Duration::from_millis(config.timeout_ms.max(1000));
    let client = reqwest_client_builder_with_ws_proxy()
        .timeout(timeout_duration)
        .build()
        .context("build Aster REST client")?;
    let listen_key = start_aster_listen_key(
        &client,
        &rest_base,
        &user_address,
        &signer_address,
        &signer_private_key,
    )
    .await?;
    let url = format!("{}/{}", ws_base.trim_end_matches('/'), listen_key);
    let result = async {
        let (mut ws, _) = timeout(timeout_duration, connect_websocket(url.as_str()))
            .await
            .context("connect Aster private websocket timed out")?
            .context("connect Aster private websocket")?;
        state.connected = true;
        send_status(
            tx,
            state,
            "online",
            "Aster private websocket connected; listenKey bootstrap used",
        )
        .await;

        let mut next_keepalive = Instant::now() + Duration::from_secs(30 * 60);
        loop {
            if Instant::now() >= next_keepalive {
                keepalive_aster_listen_key(
                    &client,
                    &rest_base,
                    &user_address,
                    &signer_address,
                    &signer_private_key,
                    &listen_key,
                )
                .await?;
                next_keepalive = Instant::now() + Duration::from_secs(30 * 60);
                send_status(tx, state, "online", "Aster listenKey keepalive ok").await;
            }
            match read_probe_frame(&mut ws, timeout_duration).await? {
                Some(frame) => {
                    state.message_count = state.message_count.saturating_add(1);
                    let now = Utc::now();
                    for row in aster_private_event_rows(&frame.sample, frame.value.as_ref(), now) {
                        if row
                            .get("private_kind")
                            .and_then(Value::as_str)
                            .is_some_and(|kind| {
                                matches!(kind, "fill" | "order" | "balance" | "position")
                            })
                        {
                            state.event_count = state.event_count.saturating_add(1);
                        }
                        state.last_latency_ms = row.get("latency_ms").and_then(Value::as_i64);
                        tx.send(PrivateWsObserveEvent::PrivateEvent(row)).await.ok();
                    }
                    send_status(
                        tx,
                        state,
                        "online",
                        "Aster private websocket frame received",
                    )
                    .await;
                }
                None => {
                    send_status(tx, state, "online", "Aster private websocket idle").await;
                }
            }
        }
    }
    .await;
    let _ = close_aster_listen_key(
        &client,
        &rest_base,
        &user_address,
        &signer_address,
        &signer_private_key,
        &listen_key,
    )
    .await;
    result
}

async fn run_mexc_private_stream(
    config: &PrivateWsObserveConfig,
    state: &mut PrivateWsObserveState,
    tx: &mpsc::Sender<PrivateWsObserveEvent>,
) -> Result<()> {
    state.connected = false;
    state.login_ok = Some(false);
    state.subscribed_channels.clear();

    let api_key = env_first(&[
        "RUSTCTA_MEXC_API_KEY",
        "MEXC_CONTRACT_API_KEY",
        "MEXC_API_KEY",
    ])
    .context("missing MEXC API key")?;
    let api_secret = env_first(&[
        "RUSTCTA_MEXC_API_SECRET",
        "MEXC_CONTRACT_API_SECRET",
        "MEXC_API_SECRET",
    ])
    .context("missing MEXC API secret")?;
    let url = env_first(&[
        "RUSTCTA_MEXC_CONTRACT_PRIVATE_WS_URL",
        "MEXC_CONTRACT_WS_URL",
    ])
    .unwrap_or_else(|| "wss://contract.mexc.com/edge".to_string());
    let timeout_duration = Duration::from_millis(config.timeout_ms.max(1000));
    let (mut ws, _) = timeout(timeout_duration, connect_websocket(url.as_str()))
        .await
        .context("connect MEXC contract private websocket timed out")?
        .context("connect MEXC contract private websocket")?;
    state.connected = true;
    send_status(
        tx,
        state,
        "starting",
        "MEXC contract private websocket connected",
    )
    .await;

    let req_time = Utc::now().timestamp_millis().to_string();
    let signature = hmac_sha256_hex(&api_secret, &format!("{api_key}{req_time}"))?;
    ws.send(Message::Text(
        json!({
            "method": "login",
            "param": {
                "apiKey": api_key,
                "reqTime": req_time,
                "signature": signature,
            },
            "subscribe": false,
        })
        .to_string(),
    ))
    .await
    .context("send MEXC private login")?;

    let filters = ["order", "order.deal", "position", "asset"];
    loop {
        let Some(frame) = read_probe_frame(&mut ws, timeout_duration).await? else {
            continue;
        };
        state.message_count = state.message_count.saturating_add(1);
        let value = frame.value.as_ref().unwrap_or(&frame.sample);
        if mexc_login_ok(value) {
            state.login_ok = Some(true);
            break;
        }
        if mexc_error(value) {
            bail!("MEXC private login error: {}", frame.sample);
        }
    }

    ws.send(Message::Text(
        json!({
            "method": "personal.filter",
            "param": {
                "filters": filters.iter().map(|filter| json!({"filter": filter})).collect::<Vec<_>>()
            }
        })
        .to_string(),
    ))
    .await
    .context("send MEXC private filters")?;
    send_status(tx, state, "starting", "MEXC private filters sent").await;

    loop {
        match read_probe_frame(&mut ws, timeout_duration).await? {
            Some(frame) => {
                state.message_count = state.message_count.saturating_add(1);
                let now = Utc::now();
                let value = frame.value.as_ref().unwrap_or(&frame.sample);
                if mexc_filter_ack(value) {
                    for filter in filters {
                        if !state
                            .subscribed_channels
                            .iter()
                            .any(|known| known == filter)
                        {
                            state.subscribed_channels.push(filter.to_string());
                        }
                    }
                }
                for row in mexc_private_event_rows(&frame.sample, frame.value.as_ref(), now) {
                    if row
                        .get("private_kind")
                        .and_then(Value::as_str)
                        .is_some_and(|kind| {
                            matches!(kind, "fill" | "order" | "balance" | "position")
                        })
                    {
                        state.event_count = state.event_count.saturating_add(1);
                    }
                    state.last_latency_ms = row.get("latency_ms").and_then(Value::as_i64);
                    tx.send(PrivateWsObserveEvent::PrivateEvent(row)).await.ok();
                }
                let status = if state.login_ok == Some(true) && state.subscribed_channels.len() >= 4
                {
                    "online"
                } else {
                    "starting"
                };
                send_status(tx, state, status, "MEXC private websocket active").await;
            }
            None => {
                let status = if state.login_ok == Some(true) {
                    "online"
                } else {
                    "starting"
                };
                send_status(tx, state, status, "MEXC private websocket idle").await;
            }
        }
    }
}

async fn run_kucoinfutures_private_stream(
    config: &PrivateWsObserveConfig,
    state: &mut PrivateWsObserveState,
    tx: &mpsc::Sender<PrivateWsObserveEvent>,
) -> Result<()> {
    state.connected = false;
    state.login_ok = None;
    state.subscribed_channels.clear();

    let api_key = env_first(&["RUSTCTA_KUCOIN_FUTURES_API_KEY", "KUCOIN_FUTURES_API_KEY"])
        .context("missing KuCoin Futures API key")?;
    let api_secret = env_first(&[
        "RUSTCTA_KUCOIN_FUTURES_API_SECRET",
        "KUCOIN_FUTURES_API_SECRET",
    ])
    .context("missing KuCoin Futures API secret")?;
    let api_passphrase = env_first(&[
        "RUSTCTA_KUCOIN_FUTURES_API_PASSPHRASE",
        "KUCOIN_FUTURES_API_PASSPHRASE",
    ])
    .context("missing KuCoin Futures API passphrase")?;
    let timeout_duration = Duration::from_millis(config.timeout_ms.max(1000));
    let (endpoint, token) = kucoinfutures_private_bullet_token(
        &api_key,
        &api_secret,
        &api_passphrase,
        timeout_duration,
    )
    .await?;
    let connect_id = format!("rustcta-private-{}", Utc::now().timestamp_millis());
    let separator = if endpoint.contains('?') { '&' } else { '?' };
    let url = format!("{endpoint}{separator}token={token}&connectId={connect_id}");
    let (mut ws, _) = timeout(timeout_duration, connect_websocket(url.as_str()))
        .await
        .context("connect KuCoin Futures private websocket timed out")?
        .context("connect KuCoin Futures private websocket")?;
    state.connected = true;
    send_status(
        tx,
        state,
        "starting",
        "KuCoin Futures private websocket connected",
    )
    .await;

    let topics = [
        "/contractMarket/tradeOrders",
        "/contractAccount/wallet",
        "/contract/position",
    ];
    for (index, topic) in topics.iter().enumerate() {
        ws.send(Message::Text(
            json!({
                "id": format!("private-{index}"),
                "type": "subscribe",
                "topic": topic,
                "privateChannel": true,
                "response": true,
            })
            .to_string(),
        ))
        .await
        .with_context(|| format!("send KuCoin Futures private subscribe {topic}"))?;
    }

    loop {
        match read_probe_frame(&mut ws, timeout_duration).await? {
            Some(frame) => {
                state.message_count = state.message_count.saturating_add(1);
                let now = Utc::now();
                let value = frame.value.as_ref().unwrap_or(&frame.sample);
                if let Some(topic) = kucoin_ack_topic(value) {
                    if !state.subscribed_channels.iter().any(|known| known == topic) {
                        state.subscribed_channels.push(topic.to_string());
                    }
                }
                for row in
                    kucoinfutures_private_event_rows(&frame.sample, frame.value.as_ref(), now)
                {
                    if row
                        .get("private_kind")
                        .and_then(Value::as_str)
                        .is_some_and(|kind| {
                            matches!(kind, "fill" | "order" | "balance" | "position")
                        })
                    {
                        state.event_count = state.event_count.saturating_add(1);
                    }
                    state.last_latency_ms = row.get("latency_ms").and_then(Value::as_i64);
                    tx.send(PrivateWsObserveEvent::PrivateEvent(row)).await.ok();
                }
                let status = if state.subscribed_channels.len() >= topics.len() {
                    "online"
                } else {
                    "starting"
                };
                send_status(tx, state, status, "KuCoin Futures private websocket active").await;
            }
            None => {
                let status = if state.subscribed_channels.len() >= topics.len() {
                    "online"
                } else {
                    "starting"
                };
                send_status(tx, state, status, "KuCoin Futures private websocket idle").await;
            }
        }
    }
}

async fn run_bybit_private_stream(
    config: &PrivateWsObserveConfig,
    state: &mut PrivateWsObserveState,
    tx: &mpsc::Sender<PrivateWsObserveEvent>,
) -> Result<()> {
    state.connected = false;
    state.login_ok = Some(false);
    state.subscribed_channels.clear();

    let api_key =
        env_first(&["RUSTCTA_BYBIT_API_KEY", "BYBIT_API_KEY"]).context("missing Bybit API key")?;
    let api_secret = env_first(&["RUSTCTA_BYBIT_API_SECRET", "BYBIT_API_SECRET"])
        .context("missing Bybit API secret")?;
    let url = env_first(&["RUSTCTA_BYBIT_PRIVATE_WS_URL", "BYBIT_PRIVATE_WS_URL"])
        .unwrap_or_else(|| "wss://stream.bybit.com/v5/private".to_string());
    let timeout_duration = Duration::from_millis(config.timeout_ms.max(1000));
    let (mut ws, _) = timeout(timeout_duration, connect_websocket(url.as_str()))
        .await
        .context("connect Bybit private websocket timed out")?
        .context("connect Bybit private websocket")?;
    state.connected = true;
    send_status(tx, state, "starting", "Bybit private websocket connected").await;

    let expires_ms = Utc::now().timestamp_millis() + 60_000;
    let signature = hmac_sha256_hex(&api_secret, &format!("GET/realtime{expires_ms}"))?;
    ws.send(Message::Text(
        json!({
            "op": "auth",
            "args": [api_key, expires_ms, signature],
        })
        .to_string(),
    ))
    .await
    .context("send Bybit auth")?;

    loop {
        let Some(frame) = read_probe_frame(&mut ws, timeout_duration).await? else {
            continue;
        };
        state.message_count = state.message_count.saturating_add(1);
        let value = frame.value.as_ref().unwrap_or(&frame.sample);
        if bybit_auth_ok(value) {
            state.login_ok = Some(true);
            break;
        }
        if bybit_error(value) {
            bail!("Bybit private auth error: {}", frame.sample);
        }
    }

    let topics = ["order", "execution", "position", "wallet"];
    ws.send(Message::Text(
        json!({
            "op": "subscribe",
            "args": topics,
        })
        .to_string(),
    ))
    .await
    .context("send Bybit private subscribe")?;

    loop {
        match read_probe_frame(&mut ws, timeout_duration).await? {
            Some(frame) => {
                state.message_count = state.message_count.saturating_add(1);
                let now = Utc::now();
                let value = frame.value.as_ref().unwrap_or(&frame.sample);
                if bybit_subscribe_ok(value) {
                    for topic in topics {
                        if !state.subscribed_channels.iter().any(|known| known == topic) {
                            state.subscribed_channels.push(topic.to_string());
                        }
                    }
                }
                for row in bybit_private_event_rows(&frame.sample, frame.value.as_ref(), now) {
                    if row
                        .get("private_kind")
                        .and_then(Value::as_str)
                        .is_some_and(|kind| {
                            matches!(kind, "fill" | "order" | "balance" | "position")
                        })
                    {
                        state.event_count = state.event_count.saturating_add(1);
                    }
                    state.last_latency_ms = row.get("latency_ms").and_then(Value::as_i64);
                    tx.send(PrivateWsObserveEvent::PrivateEvent(row)).await.ok();
                }
                let status = if state.login_ok == Some(true) && state.subscribed_channels.len() >= 4
                {
                    "online"
                } else {
                    "starting"
                };
                send_status(tx, state, status, "Bybit private websocket active").await;
            }
            None => {
                let status = if state.login_ok == Some(true) {
                    "online"
                } else {
                    "starting"
                };
                send_status(tx, state, status, "Bybit private websocket idle").await;
            }
        }
    }
}

async fn send_status(
    tx: &mpsc::Sender<PrivateWsObserveEvent>,
    state: &PrivateWsObserveState,
    status: &str,
    message: impl Into<String>,
) {
    let row = json!({
        "exchange": state.exchange,
        "status": status,
        "private_stream_status": status,
        "component": "private_user_websocket_order_fills",
        "connected": state.connected,
        "login_ok": state.login_ok,
        "subscribed_channels": state.subscribed_channels,
        "streamed_symbol_count": state.subscribed_channels.len(),
        "message_count": state.message_count,
        "event_count": state.event_count,
        "disconnect_count": state.disconnect_count,
        "last_latency_ms": state.last_latency_ms,
        "last_message_at": Utc::now(),
        "message": message.into(),
        "source": "private_user_websocket",
    });
    tx.send(PrivateWsObserveEvent::Status {
        exchange: state.exchange.clone(),
        row,
    })
    .await
    .ok();
}

fn binance_private_event_rows(
    sample: &Value,
    value: Option<&Value>,
    now: DateTime<Utc>,
) -> Vec<Value> {
    let value = value.unwrap_or(sample);
    let data = value.get("data").unwrap_or(value);
    let event = data.get("e").and_then(Value::as_str).unwrap_or("message");
    let order = data.get("o").unwrap_or(data);
    let private_kind = match event {
        "TRADE_LITE" => "fill",
        "ORDER_TRADE_UPDATE" => {
            if order.get("x").and_then(Value::as_str) == Some("TRADE") {
                "fill"
            } else {
                "order"
            }
        }
        "ACCOUNT_UPDATE" => "balance",
        "listenKeyExpired" => "error",
        _ => "heartbeat",
    };
    vec![private_event_row(
        "binance",
        private_kind,
        Some(event),
        order
            .get("s")
            .and_then(Value::as_str)
            .or_else(|| data.get("s").and_then(Value::as_str)),
        order
            .get("S")
            .and_then(Value::as_str)
            .or_else(|| data.get("S").and_then(Value::as_str)),
        number_like(
            order
                .get("l")
                .or_else(|| order.get("q"))
                .or_else(|| data.get("q")),
        ),
        number_like(
            order
                .get("L")
                .or_else(|| order.get("p"))
                .or_else(|| data.get("p")),
        ),
        match private_kind {
            "fill" => "Binance private fill/update event",
            "order" => "Binance private order update event",
            "balance" => "Binance private account update event",
            "error" => "Binance private stream error event",
            _ => "Binance private stream message",
        },
        sample,
        now,
    )
    .with_extra(json!({
        "order_status": order.get("X").cloned().unwrap_or(Value::Null),
        "execution_type": order.get("x").cloned().unwrap_or(Value::Null),
        "exchange_order_id": order.get("i").cloned().unwrap_or(Value::Null),
        "client_order_id": order.get("c").cloned().unwrap_or(Value::Null),
        "last_quantity": order.get("l").cloned().unwrap_or(Value::Null),
        "last_price": order.get("L").cloned().unwrap_or(Value::Null),
        "cumulative_quantity": order.get("z").cloned().unwrap_or(Value::Null),
        "cumulative_quote_quantity": order.get("Z").cloned().unwrap_or(Value::Null),
        "average_fill_price": average_price_from_quantity_and_quote(order.get("z"), order.get("Z")),
        "fee_amount": binance_fee_amount(order, data),
        "fee_asset": binance_fee_asset(order, data),
        "latency_ms": latency_ms(now, millis_timestamp(data.get("E").or_else(|| data.get("T")))),
    }))]
}

fn binance_fee_amount(order: &Value, data: &Value) -> Value {
    number_like(
        order
            .get("n")
            .or_else(|| order.get("commission"))
            .or_else(|| data.get("n"))
            .or_else(|| data.get("commission")),
    )
    .map(|fee| json!(fee.abs()))
    .unwrap_or(Value::Null)
}

fn binance_fee_asset(order: &Value, data: &Value) -> Value {
    order
        .get("N")
        .or_else(|| order.get("commissionAsset"))
        .or_else(|| data.get("N"))
        .or_else(|| data.get("commissionAsset"))
        .cloned()
        .unwrap_or(Value::Null)
}

fn bitget_private_event_rows(
    sample: &Value,
    value: Option<&Value>,
    now: DateTime<Utc>,
) -> Vec<Value> {
    let value = value.unwrap_or(sample);
    let event = value.get("event").and_then(Value::as_str);
    let channel = value
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(Value::as_str);
    if event == Some("subscribe") {
        return vec![private_event_row(
            "bitget",
            "heartbeat",
            channel,
            None,
            None,
            None,
            None,
            "Bitget private websocket subscribed",
            sample,
            now,
        )];
    }
    if event == Some("error") {
        return vec![private_event_row(
            "bitget",
            "error",
            channel,
            None,
            None,
            None,
            None,
            "Bitget private websocket error",
            sample,
            now,
        )];
    }
    let Some(items) = value.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    items
        .iter()
        .map(|item| {
            private_event_row(
                "bitget",
                if channel == Some("fill") { "fill" } else { "order" },
                channel,
                item.get("instId").and_then(Value::as_str),
                item.get("side").and_then(Value::as_str),
                number_like(
                    item.get("fillSize")
                        .or_else(|| item.get("accBaseVolume"))
                        .or_else(|| item.get("baseVolume"))
                        .or_else(|| item.get("filledSize"))
                        .or_else(|| item.get("size")),
                ),
                number_like(item.get("fillPrice").or_else(|| item.get("price"))),
                "Bitget private order/fill event",
                sample,
                now,
            )
            .with_extra(json!({
                "order_status": item.get("status").cloned().unwrap_or(Value::Null),
                "exchange_order_id": item.get("orderId").cloned().unwrap_or(Value::Null),
                "client_order_id": item.get("clientOid").cloned().unwrap_or(Value::Null),
                "fee_amount": bitget_fee_amount(item),
                "fee_asset": bitget_fee_asset(item),
                "latency_ms": latency_ms(now, millis_timestamp(item.get("uTime").or_else(|| item.get("cTime")))),
            }))
        })
        .collect()
}

fn bitget_fee_amount(item: &Value) -> Value {
    number_like(
        item.get("fillFee")
            .or_else(|| item.get("fee"))
            .or_else(|| item.get("feeAmount"))
            .or_else(|| {
                item.get("feeDetail")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                    .and_then(|detail| {
                        detail
                            .get("totalFee")
                            .or_else(|| detail.get("fee"))
                            .or_else(|| detail.get("totalDeductionFee"))
                    })
            }),
    )
    .map(|fee| json!(fee.abs()))
    .unwrap_or(Value::Null)
}

fn bitget_fee_asset(item: &Value) -> Value {
    item.get("fillFeeCoin")
        .or_else(|| item.get("feeCoin"))
        .or_else(|| item.get("feeCcy"))
        .or_else(|| {
            item.get("feeDetail")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(|detail| detail.get("feeCoin").or_else(|| detail.get("feeCcy")))
        })
        .cloned()
        .unwrap_or(Value::Null)
}

fn gateio_private_event_rows(
    sample: &Value,
    value: Option<&Value>,
    now: DateTime<Utc>,
) -> Vec<Value> {
    let value = value.unwrap_or(sample);
    let channel = value.get("channel").and_then(Value::as_str);
    let event = value.get("event").and_then(Value::as_str);
    if event == Some("subscribe") {
        return vec![private_event_row(
            "gateio",
            "heartbeat",
            channel,
            None,
            None,
            None,
            None,
            "Gate.io private websocket subscribed",
            sample,
            now,
        )];
    }
    if value.get("error").is_some_and(|error| !error.is_null()) {
        return vec![private_event_row(
            "gateio",
            "error",
            channel,
            None,
            None,
            None,
            None,
            "Gate.io private websocket error",
            sample,
            now,
        )];
    }
    let result = value.get("result").unwrap_or(value);
    let items = result
        .as_array()
        .map(|items| items.iter().collect::<Vec<_>>())
        .unwrap_or_else(|| vec![result]);
    items
        .into_iter()
        .filter(|item| item.is_object())
        .map(|item| {
            let is_user_trade = gateio_user_trade_channel(channel);
            private_event_row(
                "gateio",
                if is_user_trade { "fill" } else { "order" },
                channel,
                item.get("contract")
                    .or_else(|| item.get("currency_pair"))
                    .and_then(Value::as_str),
                item.get("text")
                    .and_then(Value::as_str)
                    .or_else(|| item.get("side").and_then(Value::as_str)),
                number_like(item.get("size").or_else(|| item.get("amount"))),
                number_like(item.get("price").or_else(|| item.get("fill_price"))),
                "Gate.io private order/fill event",
                sample,
                now,
            )
            .with_extra(json!({
                "order_status": item.get("status").cloned().unwrap_or(Value::Null),
                "exchange_order_id": item.get("order_id").or_else(|| item.get("id")).cloned().unwrap_or(Value::Null),
                "client_order_id": item.get("text").cloned().unwrap_or(Value::Null),
                "fill_id": item.get("id").cloned().unwrap_or(Value::Null),
                "fee_amount": gateio_fee_amount(item),
                "fee_asset": gateio_fee_asset(item),
                "latency_ms": latency_ms(now, millis_timestamp(value.get("time_ms").or_else(|| item.get("create_time_ms")))),
            }))
        })
        .collect()
}

fn gateio_user_trade_channel(channel: Option<&str>) -> bool {
    matches!(channel, Some("spot.usertrades" | "futures.usertrades"))
}

fn gateio_fee_amount(item: &Value) -> Value {
    number_like(item.get("fee"))
        .map(|fee| json!(fee.abs()))
        .unwrap_or(Value::Null)
}

fn gateio_fee_asset(item: &Value) -> Value {
    item.get("fee_currency")
        .cloned()
        .or_else(|| {
            item.get("contract")
                .and_then(Value::as_str)
                .and_then(gateio_contract_settle_asset)
                .map(Value::String)
        })
        .unwrap_or(Value::Null)
}

fn gateio_contract_settle_asset(contract: &str) -> Option<String> {
    contract
        .rsplit_once('_')
        .map(|(_, quote)| quote.to_ascii_uppercase())
        .filter(|quote| !quote.is_empty())
}

fn aster_private_event_rows(
    sample: &Value,
    value: Option<&Value>,
    now: DateTime<Utc>,
) -> Vec<Value> {
    let value = value.unwrap_or(sample);
    let event = value.get("e").and_then(Value::as_str).unwrap_or("message");
    match event {
        "ORDER_TRADE_UPDATE" => {
            let order = value.get("o").unwrap_or(value);
            let private_kind = if order.get("x").and_then(Value::as_str) == Some("TRADE") {
                "fill"
            } else {
                "order"
            };
            vec![private_event_row(
                "aster",
                private_kind,
                Some(event),
                order.get("s").and_then(Value::as_str),
                order.get("S").and_then(Value::as_str),
                number_like(order.get("l").or_else(|| order.get("q"))),
                number_like(order.get("L").or_else(|| order.get("p"))),
                "Aster private order/fill event",
                sample,
                now,
            )
            .with_extra(json!({
                "order_status": order.get("X").cloned().unwrap_or(Value::Null),
                "execution_type": order.get("x").cloned().unwrap_or(Value::Null),
                "exchange_order_id": order.get("i").cloned().unwrap_or(Value::Null),
                "client_order_id": order.get("c").cloned().unwrap_or(Value::Null),
                "last_quantity": order.get("l").cloned().unwrap_or(Value::Null),
                "last_price": order.get("L").cloned().unwrap_or(Value::Null),
                "cumulative_quantity": order.get("z").cloned().unwrap_or(Value::Null),
                "average_fill_price": average_price_from_quantity_and_quote(order.get("z"), order.get("Z")),
                "latency_ms": latency_ms(now, millis_timestamp(value.get("E").or_else(|| value.get("T")))),
            }))]
        }
        "ACCOUNT_UPDATE" => {
            let account = value.get("a").unwrap_or(value);
            let mut rows = Vec::new();
            if let Some(balances) = account.get("B").and_then(Value::as_array) {
                rows.extend(balances.iter().map(|balance| {
                    private_event_row(
                        "aster",
                        "balance",
                        Some(event),
                        None,
                        None,
                        None,
                        None,
                        "Aster private balance update",
                        sample,
                        now,
                    )
                    .with_extra(json!({
                        "asset": balance.get("a").cloned().unwrap_or(Value::Null),
                        "wallet_balance": balance.get("wb").cloned().unwrap_or(Value::Null),
                        "cross_wallet_balance": balance.get("cw").cloned().unwrap_or(Value::Null),
                        "latency_ms": latency_ms(now, millis_timestamp(value.get("E").or_else(|| value.get("T")))),
                    }))
                }));
            }
            if let Some(positions) = account.get("P").and_then(Value::as_array) {
                rows.extend(positions.iter().map(|position| {
                    private_event_row(
                        "aster",
                        "position",
                        Some(event),
                        position.get("s").and_then(Value::as_str),
                        position.get("ps").and_then(Value::as_str),
                        number_like(position.get("pa")),
                        number_like(position.get("ep")),
                        "Aster private position update",
                        sample,
                        now,
                    )
                    .with_extra(json!({
                        "position_side": position.get("ps").cloned().unwrap_or(Value::Null),
                        "unrealized_pnl": position.get("up").cloned().unwrap_or(Value::Null),
                        "latency_ms": latency_ms(now, millis_timestamp(value.get("E").or_else(|| value.get("T")))),
                    }))
                }));
            }
            rows
        }
        "listenKeyExpired" => vec![private_event_row(
            "aster",
            "error",
            Some(event),
            None,
            None,
            None,
            None,
            "Aster listenKey expired",
            sample,
            now,
        )],
        _ => Vec::new(),
    }
}

fn mexc_login_ok(value: &Value) -> bool {
    let method = value
        .get("method")
        .or_else(|| value.get("channel"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let code_ok = value
        .get("code")
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        .is_none_or(|code| code == 0);
    method.contains("login")
        && code_ok
        && !value
            .get("success")
            .and_then(Value::as_bool)
            .is_some_and(|success| !success)
}

fn mexc_filter_ack(value: &Value) -> bool {
    let method = value
        .get("method")
        .or_else(|| value.get("channel"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    method.contains("personal.filter") || method.contains("filter")
}

fn mexc_error(value: &Value) -> bool {
    value
        .get("code")
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        .is_some_and(|code| code != 0)
        || value
            .get("success")
            .and_then(Value::as_bool)
            .is_some_and(|success| !success)
}

fn mexc_private_event_rows(
    sample: &Value,
    value: Option<&Value>,
    now: DateTime<Utc>,
) -> Vec<Value> {
    let value = value.unwrap_or(sample);
    let channel = value
        .get("channel")
        .or_else(|| value.get("method"))
        .and_then(Value::as_str);
    let data = value.get("data").unwrap_or(value);
    let items = data
        .as_array()
        .map(|items| items.iter().collect::<Vec<_>>())
        .unwrap_or_else(|| vec![data]);
    items
        .into_iter()
        .filter(|item| item.is_object())
        .map(|item| {
            let private_kind = match channel.unwrap_or_default() {
                channel if channel.contains("order.deal") => "fill",
                channel if channel.contains("position") => "position",
                channel if channel.contains("asset") || channel.contains("balance") => "balance",
                channel if channel.contains("order") => "order",
                _ => "heartbeat",
            };
            private_event_row(
                "mexc",
                private_kind,
                channel,
                item.get("symbol").and_then(Value::as_str),
                mexc_side_text(item),
                number_like(
                    item.get("dealVol")
                        .or_else(|| item.get("vol"))
                        .or_else(|| item.get("quantity")),
                ),
                number_like(
                    item.get("dealAvgPrice")
                        .or_else(|| item.get("price"))
                        .or_else(|| item.get("dealPrice")),
                ),
                "MEXC private order/fill/account event",
                sample,
                now,
            )
            .with_extra(json!({
                "order_status": item.get("state").or_else(|| item.get("status")).cloned().unwrap_or(Value::Null),
                "exchange_order_id": item.get("orderId").or_else(|| item.get("id")).cloned().unwrap_or(Value::Null),
                "client_order_id": item.get("externalOid").or_else(|| item.get("clientOrderId")).cloned().unwrap_or(Value::Null),
                "last_quantity": item.get("dealVol").or_else(|| item.get("vol")).cloned().unwrap_or(Value::Null),
                "last_price": item.get("dealAvgPrice").or_else(|| item.get("price")).cloned().unwrap_or(Value::Null),
                "latency_ms": latency_ms(now, millis_timestamp(value.get("ts").or_else(|| item.get("ts")).or_else(|| item.get("updateTime")))),
            }))
        })
        .collect()
}

fn mexc_side_text(item: &Value) -> Option<&str> {
    match item.get("side").and_then(Value::as_i64) {
        Some(1) | Some(4) => Some("buy"),
        Some(2) | Some(3) => Some("sell"),
        _ => item.get("side").and_then(Value::as_str),
    }
}

async fn kucoinfutures_private_bullet_token(
    api_key: &str,
    api_secret: &str,
    api_passphrase: &str,
    timeout_duration: Duration,
) -> Result<(String, String)> {
    let rest_base = env_first(&[
        "RUSTCTA_KUCOIN_FUTURES_REST_BASE_URL",
        "KUCOIN_FUTURES_REST_BASE_URL",
    ])
    .unwrap_or_else(|| "https://api-futures.kucoin.com".to_string());
    let endpoint = "/api/v1/bullet-private";
    let timestamp = Utc::now().timestamp_millis().to_string();
    let prehash = format!("{timestamp}POST{endpoint}");
    let signature = hmac_sha256_base64(api_secret, &prehash)?;
    let passphrase = hmac_sha256_base64(api_secret, api_passphrase)?;
    let client = reqwest_client_builder_with_ws_proxy()
        .timeout(timeout_duration)
        .build()
        .context("build KuCoin Futures private token client")?;
    let response = client
        .post(format!("{}{}", rest_base.trim_end_matches('/'), endpoint))
        .header("KC-API-KEY", api_key)
        .header("KC-API-SIGN", signature)
        .header("KC-API-TIMESTAMP", timestamp)
        .header("KC-API-PASSPHRASE", passphrase)
        .header("KC-API-KEY-VERSION", "2")
        .send()
        .await
        .context("POST KuCoin Futures private websocket token")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("read KuCoin token response")?;
    if !status.is_success() {
        bail!("HTTP {status}: {}", sanitize_text_sample(&body));
    }
    let value: Value = serde_json::from_str(&body).context("parse KuCoin token response")?;
    let data = value.get("data").unwrap_or(&value);
    let token = data
        .get("token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .context("KuCoin Futures private token response missing token")?
        .to_string();
    let endpoint = data
        .get("instanceServers")
        .and_then(Value::as_array)
        .and_then(|servers| servers.first())
        .and_then(|server| server.get("endpoint"))
        .and_then(Value::as_str)
        .filter(|endpoint| !endpoint.is_empty())
        .unwrap_or("wss://ws-api-futures.kucoin.com/endpoint")
        .to_string();
    Ok((endpoint, token))
}

fn kucoin_ack_topic(value: &Value) -> Option<&str> {
    if value.get("type").and_then(Value::as_str) == Some("ack") {
        value.get("topic").and_then(Value::as_str)
    } else {
        None
    }
}

fn kucoinfutures_private_event_rows(
    sample: &Value,
    value: Option<&Value>,
    now: DateTime<Utc>,
) -> Vec<Value> {
    let value = value.unwrap_or(sample);
    let topic = value.get("topic").and_then(Value::as_str);
    let data = value.get("data").unwrap_or(value);
    let private_kind = match topic.unwrap_or_default() {
        topic if topic.contains("tradeOrders") => {
            if data.get("type").and_then(Value::as_str) == Some("match") {
                "fill"
            } else {
                "order"
            }
        }
        topic if topic.contains("wallet") => "balance",
        topic if topic.contains("position") => "position",
        _ => "heartbeat",
    };
    if private_kind == "heartbeat" {
        return Vec::new();
    }
    vec![private_event_row(
        "kucoinfutures",
        private_kind,
        topic,
        data.get("symbol").and_then(Value::as_str),
        data.get("side").and_then(Value::as_str),
        number_like(
            data.get("matchSize")
                .or_else(|| data.get("filledSize"))
                .or_else(|| data.get("size")),
        ),
        number_like(
            data.get("matchPrice")
                .or_else(|| data.get("price"))
                .or_else(|| data.get("avgDealPrice")),
        ),
        "KuCoin Futures private order/fill/account event",
        sample,
        now,
    )
    .with_extra(json!({
        "order_status": data.get("status").or_else(|| data.get("type")).cloned().unwrap_or(Value::Null),
        "exchange_order_id": data.get("orderId").or_else(|| data.get("order_id")).cloned().unwrap_or(Value::Null),
        "client_order_id": data.get("clientOid").or_else(|| data.get("client_order_id")).cloned().unwrap_or(Value::Null),
        "last_quantity": data.get("matchSize").or_else(|| data.get("filledSize")).cloned().unwrap_or(Value::Null),
        "last_price": data.get("matchPrice").or_else(|| data.get("price")).cloned().unwrap_or(Value::Null),
        "latency_ms": latency_ms(now, millis_timestamp(data.get("ts").or_else(|| data.get("orderTime")).or_else(|| data.get("timestamp")))),
    }))]
}

fn bybit_auth_ok(value: &Value) -> bool {
    value.get("op").and_then(Value::as_str) == Some("auth")
        && value
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
}

fn bybit_subscribe_ok(value: &Value) -> bool {
    value.get("op").and_then(Value::as_str) == Some("subscribe")
        && value
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
}

fn bybit_error(value: &Value) -> bool {
    value
        .get("success")
        .and_then(Value::as_bool)
        .is_some_and(|success| !success)
        || value
            .get("retCode")
            .and_then(Value::as_i64)
            .is_some_and(|code| code != 0)
}

fn bybit_private_event_rows(
    sample: &Value,
    value: Option<&Value>,
    now: DateTime<Utc>,
) -> Vec<Value> {
    let value = value.unwrap_or(sample);
    let topic = value.get("topic").and_then(Value::as_str);
    let Some(items) = value.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    items
        .iter()
        .map(|item| {
            let private_kind = match topic.unwrap_or_default() {
                topic if topic.starts_with("execution") => "fill",
                topic if topic.starts_with("position") => "position",
                topic if topic.starts_with("wallet") => "balance",
                topic if topic.starts_with("order") => "order",
                _ => "heartbeat",
            };
            private_event_row(
                "bybit",
                private_kind,
                topic,
                item.get("symbol").and_then(Value::as_str),
                item.get("side").and_then(Value::as_str),
                number_like(
                    item.get("execQty")
                        .or_else(|| item.get("cumExecQty"))
                        .or_else(|| item.get("qty"))
                        .or_else(|| item.get("size")),
                ),
                number_like(
                    item.get("execPrice")
                        .or_else(|| item.get("avgPrice"))
                        .or_else(|| item.get("price")),
                ),
                "Bybit private order/execution/account event",
                sample,
                now,
            )
            .with_extra(json!({
                "order_status": item.get("orderStatus").or_else(|| item.get("positionStatus")).cloned().unwrap_or(Value::Null),
                "exchange_order_id": item.get("orderId").cloned().unwrap_or(Value::Null),
                "client_order_id": item.get("orderLinkId").cloned().unwrap_or(Value::Null),
                "fill_id": item.get("execId").cloned().unwrap_or(Value::Null),
                "fee_amount": item.get("execFee").or_else(|| item.get("fee")).cloned().unwrap_or(Value::Null),
                "fee_asset": item.get("feeCurrency").cloned().unwrap_or(Value::Null),
                "last_quantity": item.get("execQty").or_else(|| item.get("cumExecQty")).cloned().unwrap_or(Value::Null),
                "last_price": item.get("execPrice").or_else(|| item.get("avgPrice")).cloned().unwrap_or(Value::Null),
                "latency_ms": latency_ms(now, millis_timestamp(item.get("updatedTime").or_else(|| item.get("creationTime")).or_else(|| value.get("creationTime")))),
            }))
        })
        .collect()
}

fn sanitize_text_sample(text: &str) -> String {
    if let Ok(value) = serde_json::from_str::<Value>(text) {
        serde_json::to_string(&sanitize_value(&value))
            .unwrap_or_else(|_| "<unprintable>".to_string())
    } else {
        text.chars().take(1000).collect()
    }
}

async fn start_aster_listen_key(
    client: &reqwest::Client,
    rest_base: &str,
    user_address: &str,
    signer_address: &str,
    signer_private_key: &str,
) -> Result<String> {
    let form = aster_signed_form(
        BTreeMap::new(),
        user_address,
        signer_address,
        signer_private_key,
    )?;
    let response = client
        .post(format!(
            "{}/fapi/v3/listenKey",
            rest_base.trim_end_matches('/')
        ))
        .form(&form)
        .send()
        .await
        .context("POST Aster listenKey")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("read Aster listenKey response")?;
    if !status.is_success() {
        bail!("HTTP {status}: {}", sanitize_text_sample(&body));
    }
    let value: Value = serde_json::from_str(&body).context("parse Aster listenKey response")?;
    value
        .get("listenKey")
        .or_else(|| value.get("listen_key"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .context("Aster listenKey missing in response")
}

async fn keepalive_aster_listen_key(
    client: &reqwest::Client,
    rest_base: &str,
    user_address: &str,
    signer_address: &str,
    signer_private_key: &str,
    listen_key: &str,
) -> Result<()> {
    let mut params = BTreeMap::new();
    params.insert("listenKey".to_string(), listen_key.to_string());
    let form = aster_signed_form(params, user_address, signer_address, signer_private_key)?;
    let response = client
        .put(format!(
            "{}/fapi/v3/listenKey",
            rest_base.trim_end_matches('/')
        ))
        .form(&form)
        .send()
        .await
        .context("PUT Aster listenKey")?;
    if !response.status().is_success() {
        bail!("HTTP {}", response.status());
    }
    Ok(())
}

async fn close_aster_listen_key(
    client: &reqwest::Client,
    rest_base: &str,
    user_address: &str,
    signer_address: &str,
    signer_private_key: &str,
    listen_key: &str,
) -> Result<()> {
    let mut params = BTreeMap::new();
    params.insert("listenKey".to_string(), listen_key.to_string());
    let form = aster_signed_form(params, user_address, signer_address, signer_private_key)?;
    let response = client
        .delete(format!(
            "{}/fapi/v3/listenKey",
            rest_base.trim_end_matches('/')
        ))
        .form(&form)
        .send()
        .await
        .context("DELETE Aster listenKey")?;
    if !response.status().is_success() {
        bail!("HTTP {}", response.status());
    }
    Ok(())
}

fn aster_signed_form(
    mut params: BTreeMap<String, String>,
    user_address: &str,
    signer_address: &str,
    signer_private_key: &str,
) -> Result<BTreeMap<String, String>> {
    params.insert(
        "nonce".to_string(),
        Utc::now().timestamp_micros().to_string(),
    );
    params.insert("user".to_string(), user_address.to_string());
    params.insert("signer".to_string(), signer_address.to_string());
    let query = params
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let signature = aster_sign_message(signer_private_key, &query)?;
    params.insert("signature".to_string(), signature);
    Ok(params)
}

fn aster_sign_message(private_key_hex: &str, message_text: &str) -> Result<String> {
    let key = private_key_hex.trim().trim_start_matches("0x");
    let bytes = hex::decode(key).context("decode Aster signer private key")?;
    let secret = SecretKey::from_slice(&bytes).context("parse Aster signer private key")?;
    let digest = aster_eip712_digest(message_text);
    let message = SecpMessage::from_slice(&digest).context("build Aster signer message")?;
    let secp = Secp256k1::new();
    let signature: RecoverableSignature = secp.sign_ecdsa_recoverable(&message, &secret);
    let (recovery_id, compact) = signature.serialize_compact();
    let mut bytes = Vec::with_capacity(65);
    bytes.extend_from_slice(&compact);
    bytes.push((recovery_id.to_i32() + 27) as u8);
    Ok(format!("0x{}", hex::encode(bytes)))
}

fn aster_eip712_digest(message_text: &str) -> [u8; 32] {
    let typehash_domain = keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let typehash_message = keccak(b"Message(string msg)");
    let mut message_enc = Vec::with_capacity(64);
    message_enc.extend_from_slice(&typehash_message);
    message_enc.extend_from_slice(&keccak(message_text.as_bytes()));
    let message_hash = keccak(&message_enc);

    let mut chain_id = [0u8; 32];
    chain_id[24..].copy_from_slice(&1666_u64.to_be_bytes());
    let mut domain_enc = Vec::with_capacity(160);
    domain_enc.extend_from_slice(&typehash_domain);
    domain_enc.extend_from_slice(&keccak(b"AsterSignTransaction"));
    domain_enc.extend_from_slice(&keccak(b"1"));
    domain_enc.extend_from_slice(&chain_id);
    domain_enc.extend_from_slice(&[0u8; 32]);
    let domain_hash = keccak(&domain_enc);

    let mut encoded = Vec::with_capacity(66);
    encoded.extend_from_slice(&[0x19, 0x01]);
    encoded.extend_from_slice(&domain_hash);
    encoded.extend_from_slice(&message_hash);
    keccak(&encoded)
}

fn keccak(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

fn private_event_row(
    exchange: &str,
    private_kind: &str,
    channel: Option<&str>,
    symbol: Option<&str>,
    side: Option<&str>,
    quantity: Option<f64>,
    price: Option<f64>,
    message: &str,
    sample: &Value,
    now: DateTime<Utc>,
) -> Value {
    let notional = quantity.zip(price).map(|(qty, px)| qty.abs() * px);
    json!({
        "exchange": exchange,
        "private_kind": private_kind,
        "kind": private_kind,
        "channel": channel,
        "symbol": symbol.map(normalize_symbol),
        "canonical_symbol": symbol.map(canonical_symbol),
        "side": side,
        "quantity": quantity,
        "price": price,
        "quote_quantity": notional,
        "notional": notional,
        "observed_at": now,
        "message": message,
        "source": "private_user_websocket",
        "raw_sample": sanitize_value(sample),
    })
}

fn average_price_from_quantity_and_quote(quantity: Option<&Value>, quote: Option<&Value>) -> Value {
    match (number_like(quantity), number_like(quote)) {
        (Some(quantity), Some(quote)) if quantity.abs() > f64::EPSILON => json!(quote / quantity),
        _ => Value::Null,
    }
}

trait ValueExt {
    fn with_extra(self, extra: Value) -> Value;
}

impl ValueExt for Value {
    fn with_extra(mut self, extra: Value) -> Value {
        if let (Some(base), Some(extra)) = (self.as_object_mut(), extra.as_object()) {
            for (key, value) in extra {
                base.insert(key.clone(), value.clone());
            }
        }
        self
    }
}

async fn start_binance_listen_key(
    client: &reqwest::Client,
    rest_base: &str,
    api_key: &str,
) -> Result<String> {
    let url = format!("{}/fapi/v1/listenKey", rest_base.trim_end_matches('/'));
    let response = client
        .post(url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .context("POST Binance futures listenKey")?;
    let status = response.status();
    let body = response.text().await.context("read listenKey response")?;
    if !status.is_success() {
        bail!("HTTP {status}: {body}");
    }
    let value = serde_json::from_str::<Value>(&body).context("parse listenKey response")?;
    value
        .get("listenKey")
        .and_then(Value::as_str)
        .map(str::to_string)
        .context("listenKey missing in response")
}

async fn keepalive_binance_listen_key(
    client: &reqwest::Client,
    rest_base: &str,
    api_key: &str,
    listen_key: &str,
) -> Result<()> {
    let url = format!("{}/fapi/v1/listenKey", rest_base.trim_end_matches('/'));
    let response = client
        .put(url)
        .header("X-MBX-APIKEY", api_key)
        .query(&[("listenKey", listen_key)])
        .send()
        .await
        .context("PUT Binance futures listenKey")?;
    if !response.status().is_success() {
        bail!("HTTP {}", response.status());
    }
    Ok(())
}

async fn close_binance_listen_key(
    client: &reqwest::Client,
    rest_base: &str,
    api_key: &str,
    listen_key: &str,
) -> Result<()> {
    let url = format!("{}/fapi/v1/listenKey", rest_base.trim_end_matches('/'));
    let response = client
        .delete(url)
        .header("X-MBX-APIKEY", api_key)
        .query(&[("listenKey", listen_key)])
        .send()
        .await
        .context("DELETE Binance futures listenKey")?;
    if !response.status().is_success() {
        bail!("HTTP {}", response.status());
    }
    Ok(())
}

impl PrivateWsObserveState {
    fn new(exchange: impl Into<String>) -> Self {
        Self {
            exchange: exchange.into(),
            connected: false,
            login_ok: None,
            subscribed_channels: Vec::new(),
            message_count: 0,
            event_count: 0,
            disconnect_count: 0,
            last_latency_ms: None,
        }
    }
}

fn normalize_private_exchange(exchange: &str) -> Option<&'static str> {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "binance" | "binance-usdm" | "binance-perp" => Some("binance"),
        "bitget" | "bitget-usdt-futures" | "bitget-perp" => Some("bitget"),
        "gate" | "gateio" | "gate-io" | "gateio-usdt-futures" | "gateio-perp" => Some("gateio"),
        "aster" | "aster-perp" | "aster-futures" => Some("aster"),
        "mexc" | "mexc-contract" | "mexc-perp" => Some("mexc"),
        "kucoinfutures" | "kucoin-futures" | "kucoin_futures" | "kucoinfutures-perp" => {
            Some("kucoinfutures")
        }
        "bybit" | "bybit-linear" | "bybit-perp" => Some("bybit"),
        _ => None,
    }
}

fn number_like(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn millis_timestamp(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let millis = match value? {
        Value::Number(number) => number.as_i64()?,
        Value::String(text) => text.parse::<i64>().ok()?,
        _ => return None,
    };
    DateTime::<Utc>::from_timestamp_millis(millis)
}

fn latency_ms(now: DateTime<Utc>, timestamp: Option<DateTime<Utc>>) -> Option<i64> {
    timestamp.map(|timestamp| now.signed_duration_since(timestamp).num_milliseconds())
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .replace('_', "")
        .replace('-', "")
        .to_ascii_uppercase()
}

fn canonical_symbol(symbol: &str) -> String {
    let normalized = normalize_symbol(symbol);
    if let Some(base) = normalized.strip_suffix("USDTM") {
        let base = if base == "XBT" { "BTC" } else { base };
        format!("{base}/USDT")
    } else if let Some(base) = normalized.strip_suffix("USDT") {
        format!("{base}/USDT")
    } else {
        normalized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_event_rows_should_classify_exchange_fill_messages() {
        let binance = json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 1_780_000_000_000_i64,
            "o": {
                "s": "BTCUSDT",
                "S": "BUY",
                "x": "TRADE",
                "X": "FILLED",
                "l": "9",
                "L": "0.08895",
                "z": "66",
                "Z": "5.8707",
                "n": "0.00293535",
                "N": "USDT"
            }
        });
        let rows =
            binance_private_event_rows(&sanitize_value(&binance), Some(&binance), Utc::now());
        assert_eq!(rows[0]["private_kind"], "fill");
        assert_eq!(rows[0]["canonical_symbol"], "BTC/USDT");
        assert_eq!(rows[0]["quantity"], 9.0);
        assert_eq!(rows[0]["cumulative_quantity"], "66");
        assert_eq!(rows[0]["average_fill_price"], 0.08895);
        assert_eq!(rows[0]["fee_amount"], 0.00293535);
        assert_eq!(rows[0]["fee_asset"], "USDT");

        let bitget = json!({
            "arg": {"channel": "fill"},
            "data": [{
                "instId": "ETHUSDT",
                "fillSize": "0.2",
                "fillPrice": "3000",
                "fillFee": "-0.3",
                "fillFeeCoin": "USDT"
            }]
        });
        let rows = bitget_private_event_rows(&sanitize_value(&bitget), Some(&bitget), Utc::now());
        assert_eq!(rows[0]["private_kind"], "fill");
        assert_eq!(rows[0]["quote_quantity"], 600.0);
        assert_eq!(rows[0]["fee_amount"], 0.3);
        assert_eq!(rows[0]["fee_asset"], "USDT");

        let bitget_cancelled_order = json!({
            "arg": {"channel": "orders"},
            "data": [{
                "instId": "AIOUSDT",
                "clientOid": "ca-cl-bitget",
                "status": "canceled",
                "accBaseVolume": "0",
                "size": "26",
                "price": "0.20496"
            }]
        });
        let rows = bitget_private_event_rows(
            &sanitize_value(&bitget_cancelled_order),
            Some(&bitget_cancelled_order),
            Utc::now(),
        );
        assert_eq!(rows[0]["private_kind"], "order");
        assert_eq!(rows[0]["order_status"], "canceled");
        assert_eq!(rows[0]["quantity"], 0.0);
        assert_eq!(rows[0]["quote_quantity"], 0.0);

        let gate = json!({
            "channel": "futures.usertrades",
            "event": "update",
            "result": [{
                "id": "3335259",
                "contract": "SOL_USDT",
                "order_id": "4872460",
                "size": "2",
                "price": "100",
                "fee": 0.05
            }]
        });
        let rows = gateio_private_event_rows(&sanitize_value(&gate), Some(&gate), Utc::now());
        assert_eq!(rows[0]["private_kind"], "fill");
        assert_eq!(rows[0]["canonical_symbol"], "SOL/USDT");
        assert_eq!(rows[0]["exchange_order_id"], "4872460");
        assert_eq!(rows[0]["fill_id"], "3335259");
        assert_eq!(rows[0]["fee_amount"], 0.05);
        assert_eq!(rows[0]["fee_asset"], "USDT");

        let gate_spot = json!({
            "channel": "spot.usertrades",
            "event": "update",
            "result": [{
                "id": 5736713,
                "order_id": "30784428",
                "currency_pair": "BTC_USDT",
                "side": "sell",
                "amount": "1.0",
                "price": "10000",
                "fee": "0.002",
                "fee_currency": "BTC",
                "text": "t-gate-spot"
            }]
        });
        let rows =
            gateio_private_event_rows(&sanitize_value(&gate_spot), Some(&gate_spot), Utc::now());
        assert_eq!(rows[0]["private_kind"], "fill");
        assert_eq!(rows[0]["canonical_symbol"], "BTC/USDT");
        assert_eq!(rows[0]["client_order_id"], "t-gate-spot");
        assert_eq!(rows[0]["fee_amount"], 0.002);
        assert_eq!(rows[0]["fee_asset"], "BTC");
    }

    #[test]
    fn private_event_rows_should_emit_subscription_heartbeat() {
        let gate = json!({
            "channel": "futures.orders",
            "event": "subscribe",
            "payload": ["53636022", "!all"],
            "result": {"status": "success"}
        });
        let rows = gateio_private_event_rows(&sanitize_value(&gate), Some(&gate), Utc::now());
        assert_eq!(rows[0]["private_kind"], "heartbeat");
        assert_eq!(rows[0]["channel"], "futures.orders");
        assert_eq!(rows[0]["raw_sample"]["payload"][0], "<redacted>");
    }

    #[test]
    fn target_contract_private_event_rows_should_classify_fills() {
        let now = Utc::now();
        let aster = json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 1_700_000_000_000_i64,
            "o": {
                "s": "BTCUSDT",
                "S": "BUY",
                "x": "TRADE",
                "X": "FILLED",
                "l": "0.1",
                "L": "64999",
                "z": "0.1",
                "Z": "6499.9",
                "i": "a-1",
                "c": "client-a"
            }
        });
        let rows = aster_private_event_rows(&sanitize_value(&aster), Some(&aster), now);
        assert_eq!(rows[0]["private_kind"], "fill");
        assert_eq!(rows[0]["canonical_symbol"], "BTC/USDT");
        assert_eq!(rows[0]["client_order_id"], "client-a");

        let mexc = json!({
            "channel": "push.personal.order.deal",
            "symbol": "BTC_USDT",
            "ts": 1_700_000_000_000_i64,
            "data": {
                "symbol": "BTC_USDT",
                "side": 1,
                "dealVol": "0.2",
                "dealAvgPrice": "65000",
                "orderId": "m-1",
                "externalOid": "client-m"
            }
        });
        let rows = mexc_private_event_rows(&sanitize_value(&mexc), Some(&mexc), now);
        assert_eq!(rows[0]["private_kind"], "fill");
        assert_eq!(rows[0]["canonical_symbol"], "BTC/USDT");
        assert_eq!(rows[0]["side"], "buy");
        assert_eq!(rows[0]["client_order_id"], "client-m");

        let kucoin = json!({
            "topic": "/contractMarket/tradeOrders",
            "data": {
                "type": "match",
                "symbol": "XBTUSDTM",
                "side": "buy",
                "matchSize": "1",
                "matchPrice": "65001",
                "orderId": "k-1",
                "clientOid": "client-k",
                "ts": 1_700_000_000_000_i64
            }
        });
        let rows = kucoinfutures_private_event_rows(&sanitize_value(&kucoin), Some(&kucoin), now);
        assert_eq!(rows[0]["private_kind"], "fill");
        assert_eq!(rows[0]["canonical_symbol"], "BTC/USDT");
        assert_eq!(rows[0]["client_order_id"], "client-k");

        let bybit = json!({
            "topic": "execution.linear",
            "creationTime": 1_700_000_000_000_i64,
            "data": [{
                "symbol": "BTCUSDT",
                "side": "Buy",
                "execQty": "0.3",
                "execPrice": "65002",
                "orderId": "b-1",
                "orderLinkId": "client-b",
                "execId": "fill-b"
            }]
        });
        let rows = bybit_private_event_rows(&sanitize_value(&bybit), Some(&bybit), now);
        assert_eq!(rows[0]["private_kind"], "fill");
        assert_eq!(rows[0]["canonical_symbol"], "BTC/USDT");
        assert_eq!(rows[0]["client_order_id"], "client-b");
        assert_eq!(rows[0]["fill_id"], "fill-b");
    }

    #[test]
    fn aster_observer_signing_should_match_gateway_shape() {
        let signature = aster_sign_message(
            "0000000000000000000000000000000000000000000000000000000000000001",
            "nonce=1748310859508867&signer=0x7e5f4552091a69125d5dfcb7b8c2659029395bdf&symbol=ASTERUSDT&type=LIMIT",
        )
        .expect("signature");
        assert_eq!(signature.len(), 132);
        assert!(signature.starts_with("0x"));
    }
}

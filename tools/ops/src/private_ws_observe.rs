use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::{timeout, Instant};
use tokio_tungstenite::tungstenite::Message;

use crate::private_ws_probe::{
    env_first, gateio_ws_signature, hmac_sha256_base64, read_probe_frame, sanitize_value,
};
use crate::ws_proxy_probe::connect_websocket;

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
struct PrivateWsRuntimeState {
    exchange: String,
    connected: bool,
    login_ok: Option<bool>,
    subscribed_channels: Vec<String>,
    message_count: usize,
    event_count: usize,
    disconnect_count: usize,
    last_latency_ms: Option<i64>,
}

pub fn spawn_private_ws_observe_tasks(
    exchanges: &[String],
    config: PrivateWsObserveConfig,
    tx: mpsc::Sender<PrivateWsObserveEvent>,
) {
    dotenv::dotenv().ok();
    for exchange in exchanges {
        match normalize_private_exchange(exchange) {
            Some("binance") => spawn_private_task("binance", config.clone(), tx.clone()),
            Some("bitget") => spawn_private_task("bitget", config.clone(), tx.clone()),
            Some("gateio") => spawn_private_task("gateio", config.clone(), tx.clone()),
            _ => {}
        }
    }
}

fn spawn_private_task(
    exchange: &'static str,
    config: PrivateWsObserveConfig,
    tx: mpsc::Sender<PrivateWsObserveEvent>,
) {
    tokio::spawn(async move {
        let mut state = PrivateWsRuntimeState::new(exchange);
        loop {
            let result = match exchange {
                "binance" => run_binance_private_stream(&config, &mut state, &tx).await,
                "bitget" => run_bitget_private_stream(&config, &mut state, &tx).await,
                "gateio" => run_gateio_private_stream(&config, &mut state, &tx).await,
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
                    format!("private websocket error: {error}"),
                )
                .await;
            }
            tokio::time::sleep(Duration::from_millis(config.reconnect_delay_ms.max(1000))).await;
        }
    });
}

async fn run_binance_private_stream(
    config: &PrivateWsObserveConfig,
    state: &mut PrivateWsRuntimeState,
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
    let client = reqwest::Client::builder()
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
    state: &mut PrivateWsRuntimeState,
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
    state: &mut PrivateWsRuntimeState,
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

async fn send_status(
    tx: &mpsc::Sender<PrivateWsObserveEvent>,
    state: &PrivateWsRuntimeState,
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
        "latency_ms": latency_ms(now, millis_timestamp(data.get("E").or_else(|| data.get("T")))),
    }))]
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
            private_event_row(
                "gateio",
                if channel == Some("futures.usertrades") {
                    "fill"
                } else {
                    "order"
                },
                channel,
                item.get("contract").and_then(Value::as_str),
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
                "exchange_order_id": item.get("id").or_else(|| item.get("order_id")).cloned().unwrap_or(Value::Null),
                "client_order_id": item.get("text").cloned().unwrap_or(Value::Null),
                "latency_ms": latency_ms(now, millis_timestamp(value.get("time_ms").or_else(|| item.get("create_time_ms")))),
            }))
        })
        .collect()
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

impl PrivateWsRuntimeState {
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
    if let Some(base) = normalized.strip_suffix("USDT") {
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
                "Z": "5.8707"
            }
        });
        let rows =
            binance_private_event_rows(&sanitize_value(&binance), Some(&binance), Utc::now());
        assert_eq!(rows[0]["private_kind"], "fill");
        assert_eq!(rows[0]["canonical_symbol"], "BTC/USDT");
        assert_eq!(rows[0]["quantity"], 9.0);
        assert_eq!(rows[0]["cumulative_quantity"], "66");
        assert_eq!(rows[0]["average_fill_price"], 0.08895);

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
            "result": [{"contract": "SOL_USDT", "size": "2", "price": "100"}]
        });
        let rows = gateio_private_event_rows(&sanitize_value(&gate), Some(&gate), Utc::now());
        assert_eq!(rows[0]["private_kind"], "fill");
        assert_eq!(rows[0]["canonical_symbol"], "SOL/USDT");
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
}

use std::time::Duration;

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Utc};
use clap::Args;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256, Sha512};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::ws_proxy_probe::connect_websocket;

type HmacSha256 = Hmac<Sha256>;
type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Clone, Args)]
pub struct PrivateWsProbeArgs {
    #[arg(long, default_value = "all")]
    pub exchange: String,
    #[arg(long, default_value_t = 12000)]
    pub timeout_ms: u64,
    #[arg(long, default_value_t = 8)]
    pub frames: usize,
    #[arg(long, default_value_t = 5)]
    pub sample_frames: usize,
    #[arg(long, default_value_t = false)]
    pub require_events: bool,
    #[arg(long)]
    pub gateio_user_id: Option<String>,
    #[arg(long, default_value = "!all")]
    pub gateio_contract: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PrivateWsProbeReport {
    pub generated_at: DateTime<Utc>,
    pub selected_exchange: String,
    pub live_orders_enabled: bool,
    pub public_market_data_requirement: &'static str,
    pub order_place_path: &'static str,
    pub order_query_path: &'static str,
    pub user_fill_requirement: &'static str,
    pub private_ws_ready: bool,
    pub exchanges: Vec<PrivateWsExchangeReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PrivateWsExchangeReport {
    pub exchange: String,
    pub status: String,
    pub endpoint: String,
    pub connected: bool,
    pub login_ok: Option<bool>,
    pub subscriptions: Vec<PrivateWsSubscriptionReport>,
    pub frames_received: usize,
    pub event_frames: usize,
    pub sample_frames: Vec<Value>,
    pub notes: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PrivateWsSubscriptionReport {
    pub channel: String,
    pub subscribed: bool,
    pub ack_received: bool,
    pub event_count: usize,
}

#[derive(Debug, Clone)]
pub struct ProbeFrame {
    pub value: Option<Value>,
    pub sample: Value,
}

pub async fn run_private_ws_probe(args: PrivateWsProbeArgs) -> Result<PrivateWsProbeReport> {
    dotenv::dotenv().ok();
    let selected = selected_private_exchanges(&args.exchange)?;
    let timeout_duration = Duration::from_millis(args.timeout_ms.max(1000));
    let mut exchanges = Vec::new();

    for exchange in selected {
        let report = match exchange {
            "binance" => probe_binance_private_ws(&args, timeout_duration).await,
            "bitget" => probe_bitget_private_ws(&args, timeout_duration).await,
            "gateio" => probe_gateio_private_ws(&args, timeout_duration).await,
            _ => unreachable!("selected exchange is validated"),
        };
        exchanges.push(report);
    }

    let private_ws_ready = exchanges.iter().all(|exchange| {
        exchange.status == "ok"
            && exchange.connected
            && exchange
                .subscriptions
                .iter()
                .all(|subscription| subscription.subscribed)
    });

    Ok(PrivateWsProbeReport {
        generated_at: Utc::now(),
        selected_exchange: args.exchange,
        live_orders_enabled: false,
        public_market_data_requirement: "websocket_only",
        order_place_path: "rest_allowed",
        order_query_path: "rest_allowed",
        user_fill_requirement: "private_user_websocket_required",
        private_ws_ready,
        exchanges,
    })
}

async fn probe_binance_private_ws(
    args: &PrivateWsProbeArgs,
    timeout_duration: Duration,
) -> PrivateWsExchangeReport {
    let ws_base = env_first(&[
        "RUSTCTA_BINANCE_USDM_PRIVATE_WS_URL",
        "BINANCE_USDM_PRIVATE_WS_URL",
    ])
    .unwrap_or_else(|| "wss://fstream.binance.com/private/ws".to_string());
    let endpoint = format!("{}/<listenKey>", ws_base.trim_end_matches('/'));
    let mut report = PrivateWsExchangeReport::new("binance", endpoint);
    report.login_ok = None;
    report.subscriptions = vec![
        PrivateWsSubscriptionReport::new("ORDER_TRADE_UPDATE"),
        PrivateWsSubscriptionReport::new("TRADE_LITE"),
    ];

    let Some(api_key) = env_first(&[
        "RUSTCTA_BINANCE_API_KEY",
        "BINANCE_0_API_KEY",
        "BINANCE_USDM_API_KEY",
        "BINANCE_API_KEY",
    ]) else {
        report.skip("missing Binance API key for futures listenKey bootstrap");
        return report;
    };

    let rest_base = env_first(&[
        "RUSTCTA_BINANCE_USDM_REST_BASE_URL",
        "BINANCE_USDM_REST_BASE_URL",
    ])
    .unwrap_or_else(|| "https://fapi.binance.com".to_string());
    report.notes.push(
        "uses Binance REST listenKey bootstrap only; public market data remains websocket-only"
            .to_string(),
    );

    let client = match reqwest::Client::builder()
        .timeout(timeout_duration)
        .build()
        .context("build reqwest client")
    {
        Ok(client) => client,
        Err(error) => {
            report.error(error.to_string());
            return report;
        }
    };

    let listen_key = match start_binance_listen_key(&client, &rest_base, &api_key).await {
        Ok(listen_key) => listen_key,
        Err(error) => {
            report.error(format!("start listenKey failed: {error}"));
            return report;
        }
    };

    let url = format!("{}/{}", ws_base.trim_end_matches('/'), listen_key);
    let (mut ws, _) = match timeout(timeout_duration, connect_websocket(url.as_str())).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => {
            report.error(format!("connect private websocket failed: {error}"));
            let _ = close_binance_listen_key(&client, &rest_base, &api_key, &listen_key).await;
            return report;
        }
        Err(_) => {
            report.error("connect private websocket timed out".to_string());
            let _ = close_binance_listen_key(&client, &rest_base, &api_key, &listen_key).await;
            return report;
        }
    };
    report.connected = true;
    for subscription in &mut report.subscriptions {
        subscription.subscribed = true;
    }
    report.notes.push(
        "Binance user stream does not send subscription acks; successful websocket handshake is the readiness signal".to_string(),
    );

    read_binance_frames(&mut ws, args, timeout_duration, &mut report).await;

    match close_binance_listen_key(&client, &rest_base, &api_key, &listen_key).await {
        Ok(()) => report
            .notes
            .push("Binance listenKey closed after probe".to_string()),
        Err(error) => report.notes.push(format!(
            "Binance listenKey close failed after probe: {error}"
        )),
    }
    report.finish(args.require_events);
    report
}

async fn probe_bitget_private_ws(
    args: &PrivateWsProbeArgs,
    timeout_duration: Duration,
) -> PrivateWsExchangeReport {
    let url = env_first(&["RUSTCTA_BITGET_PRIVATE_WS_URL", "BITGET_PRIVATE_WS_URL"])
        .unwrap_or_else(|| "wss://ws.bitget.com/v2/ws/private".to_string());
    let mut report = PrivateWsExchangeReport::new("bitget", url.clone());
    report.login_ok = Some(false);
    report.subscriptions = vec![
        PrivateWsSubscriptionReport::new("orders"),
        PrivateWsSubscriptionReport::new("fill"),
    ];

    let Some(api_key) = env_first(&["RUSTCTA_BITGET_API_KEY", "BITGET_API_KEY"]) else {
        report.skip("missing Bitget API key");
        return report;
    };
    let Some(api_secret) = env_first(&["RUSTCTA_BITGET_API_SECRET", "BITGET_API_SECRET"]) else {
        report.skip("missing Bitget API secret");
        return report;
    };
    let Some(passphrase) = env_first(&[
        "RUSTCTA_BITGET_API_PASSPHRASE",
        "BITGET_PASSPHRASE",
        "BITGET_API_PASSPHRASE",
    ]) else {
        report.skip("missing Bitget API passphrase");
        return report;
    };

    let (mut ws, _) = match timeout(timeout_duration, connect_websocket(url.as_str())).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => {
            report.error(format!("connect private websocket failed: {error}"));
            return report;
        }
        Err(_) => {
            report.error("connect private websocket timed out".to_string());
            return report;
        }
    };
    report.connected = true;

    let timestamp = Utc::now().timestamp_millis().to_string();
    let sign = match hmac_sha256_base64(&api_secret, &format!("{timestamp}GET/user/verify")) {
        Ok(sign) => sign,
        Err(error) => {
            report.error(format!("build login signature failed: {error}"));
            return report;
        }
    };
    let login = json!({
        "op": "login",
        "args": [{
            "apiKey": api_key,
            "passphrase": passphrase,
            "timestamp": timestamp,
            "sign": sign,
        }]
    });
    if let Err(error) = ws.send(Message::Text(login.to_string())).await {
        report.error(format!("send login failed: {error}"));
        return report;
    }

    match wait_for_bitget_login(&mut ws, args, timeout_duration, &mut report).await {
        Ok(true) => report.login_ok = Some(true),
        Ok(false) => {
            report.error("login ack not received".to_string());
            report.finish(args.require_events);
            return report;
        }
        Err(error) => {
            report.error(format!("read login failed: {error}"));
            report.finish(args.require_events);
            return report;
        }
    }

    let subscribe = json!({
        "op": "subscribe",
        "args": [
            {"instType": "USDT-FUTURES", "channel": "orders", "instId": "default"},
            {"instType": "USDT-FUTURES", "channel": "fill", "instId": "default"},
        ]
    });
    if let Err(error) = ws.send(Message::Text(subscribe.to_string())).await {
        report.error(format!("send subscribe failed: {error}"));
        report.finish(args.require_events);
        return report;
    }

    if let Err(error) =
        read_bitget_subscription_frames(&mut ws, args, timeout_duration, &mut report).await
    {
        report.error(format!("read subscribe frames failed: {error}"));
    }
    report.finish(args.require_events);
    report
}

async fn probe_gateio_private_ws(
    args: &PrivateWsProbeArgs,
    timeout_duration: Duration,
) -> PrivateWsExchangeReport {
    let url = env_first(&[
        "RUSTCTA_GATEIO_FUTURES_PRIVATE_WS_URL",
        "GATEIO_FUTURES_PRIVATE_WS_URL",
        "GATEIO_FUTURES_WS_URL",
    ])
    .unwrap_or_else(|| "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string());
    let mut report = PrivateWsExchangeReport::new("gateio", url.clone());
    report.login_ok = None;
    report.subscriptions = vec![
        PrivateWsSubscriptionReport::new("futures.orders"),
        PrivateWsSubscriptionReport::new("futures.usertrades"),
    ];

    let Some(api_key) = env_first(&[
        "RUSTCTA_GATEIO_API_KEY",
        "GATEIO_API_KEY",
        "GATE_API_KEY",
        "GATE__16076371__API_KEY",
    ]) else {
        report.skip("missing Gate.io API key");
        return report;
    };
    let Some(api_secret) = env_first(&[
        "RUSTCTA_GATEIO_API_SECRET",
        "GATEIO_API_SECRET",
        "GATE_API_SECRET",
        "GATE__16076371__API_SECRET",
    ]) else {
        report.skip("missing Gate.io API secret");
        return report;
    };
    let user_id = match args.gateio_user_id.clone().or_else(|| {
        env_first(&[
            "RUSTCTA_GATEIO_USER_ID",
            "GATEIO_USER_ID",
            "GATE_USER_ID",
            "GATE__16076371__USER_ID",
        ])
    }) {
        Some(user_id) => user_id,
        None => match fetch_gateio_futures_user_id(&api_key, &api_secret, timeout_duration).await {
            Ok(Some(user_id)) => {
                report.notes.push(
                    "Gate.io user id loaded from read-only futures account REST query".to_string(),
                );
                user_id
            }
            Ok(None) => {
                report.skip("missing Gate.io futures user id and REST account response did not include user/user_id");
                return report;
            }
            Err(error) => {
                report.skip(format!(
                    "missing Gate.io futures user id and REST account lookup failed: {error}"
                ));
                return report;
            }
        },
    };

    let (mut ws, _) = match timeout(timeout_duration, connect_websocket(url.as_str())).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => {
            report.error(format!("connect private websocket failed: {error}"));
            return report;
        }
        Err(_) => {
            report.error("connect private websocket timed out".to_string());
            return report;
        }
    };
    report.connected = true;

    let contract = args.gateio_contract.trim();
    for channel in ["futures.orders", "futures.usertrades"] {
        let event = "subscribe";
        let timestamp = Utc::now().timestamp();
        let sign = match gateio_ws_signature(&api_secret, channel, event, timestamp) {
            Ok(sign) => sign,
            Err(error) => {
                report.error(format!("build {channel} signature failed: {error}"));
                return report;
            }
        };
        let subscribe = json!({
            "time": timestamp,
            "channel": channel,
            "event": event,
            "payload": [user_id.as_str(), contract],
            "auth": {
                "method": "api_key",
                "KEY": api_key.as_str(),
                "SIGN": sign,
            }
        });
        if let Err(error) = ws.send(Message::Text(subscribe.to_string())).await {
            report.error(format!("send {channel} subscribe failed: {error}"));
            report.finish(args.require_events);
            return report;
        }
    }

    if let Err(error) =
        read_gateio_subscription_frames(&mut ws, args, timeout_duration, &mut report).await
    {
        report.error(format!("read subscribe frames failed: {error}"));
    }
    report.finish(args.require_events);
    report
}

async fn read_binance_frames(
    ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    args: &PrivateWsProbeArgs,
    timeout_duration: Duration,
    report: &mut PrivateWsExchangeReport,
) {
    for _ in 0..args.frames {
        let frame = match read_probe_frame(ws, timeout_duration).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(error) => {
                report.error(format!("read frame failed: {error}"));
                break;
            }
        };
        record_frame(report, &frame, args.sample_frames);
        let Some(value) = frame.value.as_ref() else {
            continue;
        };
        let event = value
            .get("e")
            .or_else(|| value.pointer("/data/e"))
            .and_then(Value::as_str);
        match event {
            Some("ORDER_TRADE_UPDATE") => {
                report.event_frames += 1;
                if let Some(subscription) = report.subscription_mut("ORDER_TRADE_UPDATE") {
                    subscription.event_count += 1;
                }
            }
            Some("TRADE_LITE") => {
                report.event_frames += 1;
                if let Some(subscription) = report.subscription_mut("TRADE_LITE") {
                    subscription.event_count += 1;
                }
            }
            Some("ACCOUNT_UPDATE") => report.event_frames += 1,
            Some("listenKeyExpired") => report.error("Binance listenKey expired event received"),
            _ => {}
        }
    }
}

async fn wait_for_bitget_login(
    ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    args: &PrivateWsProbeArgs,
    timeout_duration: Duration,
    report: &mut PrivateWsExchangeReport,
) -> Result<bool> {
    for _ in 0..args.frames.max(4) {
        let Some(frame) = read_probe_frame(ws, timeout_duration).await? else {
            return Ok(false);
        };
        record_frame(report, &frame, args.sample_frames);
        let Some(value) = frame.value.as_ref() else {
            continue;
        };
        if value.get("event").and_then(Value::as_str) == Some("login")
            && value.get("code").and_then(Value::as_str).unwrap_or("0") == "0"
        {
            return Ok(true);
        }
        if value.get("event").and_then(Value::as_str) == Some("error") {
            report.error(format!(
                "Bitget login error: {}",
                sanitized_value_text(value)
            ));
            return Ok(false);
        }
    }
    Ok(false)
}

async fn read_bitget_subscription_frames(
    ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    args: &PrivateWsProbeArgs,
    timeout_duration: Duration,
    report: &mut PrivateWsExchangeReport,
) -> Result<()> {
    for _ in 0..args.frames.max(4) {
        if report.subscription_ack_count() == report.subscriptions.len() {
            break;
        }
        let Some(frame) = read_probe_frame(ws, timeout_duration).await? else {
            break;
        };
        record_frame(report, &frame, args.sample_frames);
        let Some(value) = frame.value.as_ref() else {
            continue;
        };
        if value.get("event").and_then(Value::as_str) == Some("error") {
            report.error(format!("Bitget error: {}", sanitized_value_text(value)));
            continue;
        }
        if value.get("event").and_then(Value::as_str) == Some("subscribe") {
            if let Some(channel) = value
                .get("arg")
                .and_then(|arg| arg.get("channel"))
                .and_then(Value::as_str)
            {
                if let Some(subscription) = report.subscription_mut(channel) {
                    subscription.subscribed = true;
                    subscription.ack_received = true;
                }
            }
        }
        if let Some(channel) = value
            .get("arg")
            .and_then(|arg| arg.get("channel"))
            .and_then(Value::as_str)
        {
            if value.get("data").is_some() {
                report.event_frames += 1;
                if let Some(subscription) = report.subscription_mut(channel) {
                    subscription.event_count += 1;
                }
            }
        }
    }
    for subscription in &report.subscriptions {
        if !subscription.ack_received {
            report.errors.push(format!(
                "missing Bitget {} subscribe ack",
                subscription.channel
            ));
        }
    }
    Ok(())
}

async fn read_gateio_subscription_frames(
    ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    args: &PrivateWsProbeArgs,
    timeout_duration: Duration,
    report: &mut PrivateWsExchangeReport,
) -> Result<()> {
    for _ in 0..args.frames.max(4) {
        if report.subscription_ack_count() == report.subscriptions.len() {
            break;
        }
        let Some(frame) = read_probe_frame(ws, timeout_duration).await? else {
            break;
        };
        record_frame(report, &frame, args.sample_frames);
        let Some(value) = frame.value.as_ref() else {
            continue;
        };
        if value.get("error").is_some_and(|error| !error.is_null()) {
            report.error(format!("Gate.io error: {}", sanitized_value_text(value)));
            continue;
        }
        let channel = value.get("channel").and_then(Value::as_str);
        let event = value.get("event").and_then(Value::as_str);
        if event == Some("subscribe") {
            if let Some(channel) = channel {
                if let Some(subscription) = report.subscription_mut(channel) {
                    subscription.subscribed = true;
                    subscription.ack_received = true;
                }
            }
        } else if event == Some("update") || event == Some("all") {
            report.event_frames += 1;
            if let Some(channel) = channel {
                if let Some(subscription) = report.subscription_mut(channel) {
                    subscription.event_count += 1;
                }
            }
        }
    }
    for subscription in &report.subscriptions {
        if !subscription.ack_received {
            report.errors.push(format!(
                "missing Gate.io {} subscribe ack",
                subscription.channel
            ));
        }
    }
    Ok(())
}

pub async fn read_probe_frame(
    ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    timeout_duration: Duration,
) -> Result<Option<ProbeFrame>> {
    let message = match timeout(timeout_duration, ws.next()).await {
        Ok(Some(Ok(message))) => message,
        Ok(Some(Err(error))) => return Err(error).context("read websocket message"),
        Ok(None) => return Ok(None),
        Err(_) => return Ok(None),
    };

    let frame = match message {
        Message::Text(text) => {
            if text == "ping" {
                ws.send(Message::Text("pong".to_string()))
                    .await
                    .context("send app pong")?;
            }
            let value = serde_json::from_str::<Value>(&text).ok();
            ProbeFrame {
                sample: value
                    .as_ref()
                    .map(sanitize_value)
                    .unwrap_or_else(|| json!(truncate_text(&text, 1000))),
                value,
            }
        }
        Message::Binary(bytes) => ProbeFrame {
            sample: json!({
                "type": "binary",
                "bytes": bytes.len(),
            }),
            value: None,
        },
        Message::Ping(bytes) => {
            ws.send(Message::Pong(bytes.clone()))
                .await
                .context("send protocol pong")?;
            ProbeFrame {
                sample: json!({
                    "type": "ping",
                    "bytes": bytes.len(),
                }),
                value: None,
            }
        }
        Message::Pong(bytes) => ProbeFrame {
            sample: json!({
                "type": "pong",
                "bytes": bytes.len(),
            }),
            value: None,
        },
        Message::Close(close) => {
            let reason = close
                .map(|close| close.reason.to_string())
                .unwrap_or_default();
            bail!("websocket closed: {reason}");
        }
        Message::Frame(_) => ProbeFrame {
            sample: json!({"type": "raw_frame"}),
            value: None,
        },
    };
    Ok(Some(frame))
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
        .context("POST listenKey")?;
    let status = response.status();
    let body = response.text().await.context("read listenKey response")?;
    if !status.is_success() {
        bail!("HTTP {status}: {}", redact_text_sample(&body));
    }
    let value = serde_json::from_str::<Value>(&body).context("parse listenKey response")?;
    value
        .get("listenKey")
        .and_then(Value::as_str)
        .map(str::to_string)
        .context("listenKey missing in response")
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
        .context("DELETE listenKey")?;
    if !response.status().is_success() {
        bail!("HTTP {}", response.status());
    }
    Ok(())
}

async fn fetch_gateio_futures_user_id(
    api_key: &str,
    api_secret: &str,
    timeout_duration: Duration,
) -> Result<Option<String>> {
    let rest_base = env_first(&["RUSTCTA_GATEIO_REST_BASE_URL", "GATEIO_REST_BASE_URL"])
        .unwrap_or_else(|| "https://api.gateio.ws/api/v4".to_string());
    let endpoint = "/futures/usdt/accounts";
    let timestamp = Utc::now().timestamp().to_string();
    let path = gateio_signed_request_path(&rest_base, endpoint);
    let signature = gateio_rest_signature(api_secret, "GET", &path, "", "", &timestamp)?;
    let client = reqwest::Client::builder()
        .timeout(timeout_duration)
        .build()
        .context("build Gate.io REST client")?;
    let response = client
        .get(format!("{}{}", rest_base.trim_end_matches('/'), endpoint))
        .header("KEY", api_key)
        .header("Timestamp", timestamp)
        .header("SIGN", signature)
        .header("Content-Type", "application/json")
        .header("X-Gate-Size-Decimal", "1")
        .send()
        .await
        .context("GET Gate.io futures account")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("read Gate.io futures account response")?;
    if !status.is_success() {
        bail!("HTTP {status}: {}", redact_text_sample(&body));
    }
    let value = serde_json::from_str::<Value>(&body).context("parse Gate.io account response")?;
    Ok(extract_gateio_user_id(&value))
}

fn extract_gateio_user_id(value: &Value) -> Option<String> {
    let item = value
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(value);
    item.get("user")
        .or_else(|| item.get("user_id"))
        .and_then(value_to_string)
        .filter(|value| !value.is_empty())
}

impl PrivateWsExchangeReport {
    fn new(exchange: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            exchange: exchange.into(),
            status: "starting".to_string(),
            endpoint: endpoint.into(),
            connected: false,
            login_ok: None,
            subscriptions: Vec::new(),
            frames_received: 0,
            event_frames: 0,
            sample_frames: Vec::new(),
            notes: Vec::new(),
            errors: Vec::new(),
        }
    }

    fn skip(&mut self, reason: impl Into<String>) {
        self.status = "skipped".to_string();
        self.notes.push(reason.into());
    }

    fn error(&mut self, error: impl Into<String>) {
        self.errors.push(error.into());
    }

    fn finish(&mut self, require_events: bool) {
        if require_events && self.event_frames == 0 {
            self.errors.push(
                "private websocket connected but no user event frames were observed".to_string(),
            );
        }
        if self.errors.is_empty() {
            if self.status != "skipped" {
                self.status = "ok".to_string();
            }
        } else {
            self.status = "error".to_string();
        }
    }

    fn subscription_mut(&mut self, channel: &str) -> Option<&mut PrivateWsSubscriptionReport> {
        self.subscriptions
            .iter_mut()
            .find(|subscription| subscription.channel == channel)
    }

    fn subscription_ack_count(&self) -> usize {
        self.subscriptions
            .iter()
            .filter(|subscription| subscription.ack_received)
            .count()
    }
}

impl PrivateWsSubscriptionReport {
    fn new(channel: impl Into<String>) -> Self {
        Self {
            channel: channel.into(),
            subscribed: false,
            ack_received: false,
            event_count: 0,
        }
    }
}

fn record_frame(
    report: &mut PrivateWsExchangeReport,
    frame: &ProbeFrame,
    sample_frame_limit: usize,
) {
    report.frames_received += 1;
    if report.sample_frames.len() < sample_frame_limit {
        report.sample_frames.push(frame.sample.clone());
    }
}

fn selected_private_exchanges(filter: &str) -> Result<Vec<&'static str>> {
    let filter = filter.trim().to_ascii_lowercase();
    if filter == "all" {
        return Ok(vec!["binance", "bitget", "gateio"]);
    }
    match filter.as_str() {
        "binance" | "binance-usdm" | "binance-perp" => Ok(vec!["binance"]),
        "bitget" | "bitget-usdt-futures" | "bitget-perp" => Ok(vec!["bitget"]),
        "gate" | "gateio" | "gate-io" | "gateio-usdt-futures" | "gateio-perp" => Ok(vec!["gateio"]),
        _ => bail!("unsupported private websocket exchange filter: {filter}"),
    }
}

pub fn hmac_sha256_base64(secret: &str, payload: &str) -> Result<String> {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).context("create hmac-sha256 signer")?;
    mac.update(payload.as_bytes());
    Ok(general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

pub fn gateio_ws_signature(
    secret: &str,
    channel: &str,
    event: &str,
    timestamp: i64,
) -> Result<String> {
    let payload = format!("channel={channel}&event={event}&time={timestamp}");
    let mut mac =
        HmacSha512::new_from_slice(secret.as_bytes()).context("create hmac-sha512 signer")?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn gateio_rest_signature(
    secret: &str,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
    timestamp: &str,
) -> Result<String> {
    let body_hash = hex::encode(Sha512::digest(body.as_bytes()));
    let payload = format!("{method}\n{path}\n{query}\n{body_hash}\n{timestamp}");
    let mut mac =
        HmacSha512::new_from_slice(secret.as_bytes()).context("create hmac-sha512 signer")?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn gateio_signed_request_path(base_url: &str, endpoint: &str) -> String {
    let base_path = reqwest::Url::parse(base_url)
        .ok()
        .map(|url| url.path().trim_end_matches('/').to_string())
        .unwrap_or_default();
    format!("{base_path}{endpoint}")
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.trim().to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub fn env_first(keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}

pub fn sanitize_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let private_gateio_channel = object
                .get("channel")
                .and_then(Value::as_str)
                .is_some_and(is_gateio_private_channel);
            Value::Object(
                object
                    .iter()
                    .map(|(key, value)| {
                        if is_sensitive_key(key) {
                            (key.clone(), json!("<redacted>"))
                        } else if key == "payload" && private_gateio_channel {
                            (key.clone(), sanitize_gateio_private_payload(value))
                        } else {
                            (key.clone(), sanitize_value(value))
                        }
                    })
                    .collect(),
            )
        }
        Value::Array(items) => Value::Array(items.iter().map(sanitize_value).collect()),
        Value::String(text) => Value::String(truncate_text(text, 1000)),
        _ => value.clone(),
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    matches!(
        key.as_str(),
        "apikey"
            | "api_key"
            | "key"
            | "secret"
            | "api_secret"
            | "passphrase"
            | "sign"
            | "signature"
            | "listenkey"
            | "listen_key"
            | "user"
            | "user_id"
    )
}

fn is_gateio_private_channel(channel: &str) -> bool {
    matches!(
        channel,
        "futures.orders"
            | "futures.usertrades"
            | "futures.positions"
            | "futures.balances"
            | "futures.autoorders"
    )
}

fn sanitize_gateio_private_payload(value: &Value) -> Value {
    let Some(items) = value.as_array() else {
        return sanitize_value(value);
    };
    Value::Array(
        items
            .iter()
            .enumerate()
            .map(|(index, value)| {
                if index == 0 {
                    json!("<redacted>")
                } else {
                    sanitize_value(value)
                }
            })
            .collect(),
    )
}

fn sanitized_value_text(value: &Value) -> String {
    serde_json::to_string(&sanitize_value(value)).unwrap_or_else(|_| "<unprintable>".to_string())
}

fn redact_text_sample(text: &str) -> String {
    if let Ok(value) = serde_json::from_str::<Value>(text) {
        sanitized_value_text(&value)
    } else {
        truncate_text(text, 1000)
    }
}

fn truncate_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut truncated = text.chars().take(max_chars).collect::<String>();
    truncated.push_str("...");
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitget_login_signature_should_match_known_hmac_shape() {
        let signature = hmac_sha256_base64("secret", "123GET/user/verify").expect("signature");
        assert_eq!(signature, "XpAU8vwrJ95jhtr7s74XesWTc8cYO9lNqA6gt+bToVY=");
    }

    #[test]
    fn gateio_signature_should_match_known_hmac_shape() {
        let signature =
            gateio_ws_signature("secret", "futures.orders", "subscribe", 123).expect("signature");
        assert_eq!(
            signature,
            "98469b972344dc77a8192f82834af67728d9bba8f9c16eec3c33db2a7f48c09e9dda1b834edffbf9935a73dcf84bdb72d2a52f62e716d885497253b53d26bb96"
        );
    }

    #[test]
    fn private_ws_probe_sanitizer_should_redact_secret_fields() {
        let value = json!({
            "apiKey": "abc",
            "passphrase": "def",
            "auth": {"KEY": "ghi", "SIGN": "jkl"},
            "listenKey": "mno",
            "user": "53636022",
            "user_id": 53636022,
            "payload": ["53636022", "!all"],
            "channel": "orders",
        });
        let sanitized = sanitize_value(&value);
        assert_eq!(sanitized["apiKey"], "<redacted>");
        assert_eq!(sanitized["passphrase"], "<redacted>");
        assert_eq!(sanitized["auth"]["KEY"], "<redacted>");
        assert_eq!(sanitized["auth"]["SIGN"], "<redacted>");
        assert_eq!(sanitized["listenKey"], "<redacted>");
        assert_eq!(sanitized["user"], "<redacted>");
        assert_eq!(sanitized["user_id"], "<redacted>");
        assert_eq!(sanitized["payload"][0], "53636022");
        assert_eq!(sanitized["channel"], "orders");
    }

    #[test]
    fn private_ws_probe_sanitizer_should_redact_gateio_private_payload_user_id() {
        let value = json!({
            "channel": "futures.orders",
            "payload": ["53636022", "!all"],
        });
        let sanitized = sanitize_value(&value);
        assert_eq!(sanitized["payload"][0], "<redacted>");
        assert_eq!(sanitized["payload"][1], "!all");
    }

    #[test]
    fn extract_gateio_user_id_should_accept_account_response_shapes() {
        assert_eq!(
            extract_gateio_user_id(&json!({"user": 53636022})).as_deref(),
            Some("53636022")
        );
        assert_eq!(
            extract_gateio_user_id(&json!([{"user_id": "53636022"}])).as_deref(),
            Some("53636022")
        );
    }

    #[test]
    fn selected_private_exchanges_should_accept_aliases() {
        assert_eq!(selected_private_exchanges("all").unwrap().len(), 3);
        assert_eq!(selected_private_exchanges("gate").unwrap(), vec!["gateio"]);
        assert_eq!(
            selected_private_exchanges("bitget-usdt-futures").unwrap(),
            vec!["bitget"]
        );
    }
}

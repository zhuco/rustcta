use std::io::Read;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::{engine::general_purpose, Engine as _};
use clap::Args;
use flate2::read::GzDecoder;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::{Error, UrlError};
use tokio_tungstenite::tungstenite::handshake::client::Response;
use tokio_tungstenite::{
    client_async_tls_with_config, tungstenite::Message, MaybeTlsStream, WebSocketStream,
};
use url::Url;

#[derive(Debug, Clone, Args)]
pub struct WsProxyProbeArgs {
    #[arg(long, default_value = "all")]
    pub exchange: String,
    #[arg(long, default_value = "unknown")]
    pub region: String,
    #[arg(long, default_value_t = 12000)]
    pub timeout_ms: u64,
    #[arg(long, default_value_t = 1)]
    pub frames: usize,
}

pub async fn run_ws_proxy_probe(args: WsProxyProbeArgs) -> Result<Vec<String>> {
    let cases = selected_cases(&args.exchange).await?;
    let timeout_duration = Duration::from_millis(args.timeout_ms);
    let mut failures = Vec::new();
    let mut lines = Vec::new();

    for case in cases {
        let region = args.region.trim();
        let exchange = case.exchange_name();
        let endpoint = case.endpoint_kind();
        match probe_case(&case, timeout_duration, args.frames.max(1)).await {
            Ok(summary) => lines.push(format!(
                "OK   {:<18} region={} exchange={} endpoint={} {}",
                case.name, region, exchange, endpoint, summary
            )),
            Err(error) => {
                lines.push(format!(
                    "FAIL {:<18} region={} exchange={} endpoint={} {}",
                    case.name, region, exchange, endpoint, error
                ));
                failures.push(case.name);
            }
        }
    }

    if failures.is_empty() {
        Ok(lines)
    } else {
        anyhow::bail!(
            "{} websocket probe(s) failed: {:?}",
            failures.len(),
            failures
        )
    }
}

async fn probe_case(
    case: &ProbeCase,
    timeout_duration: Duration,
    frame_count: usize,
) -> Result<String> {
    let (mut ws, _) = timeout(timeout_duration, connect_websocket(case.url.as_str()))
        .await
        .context("connect timed out")?
        .with_context(|| format!("connect {}", case.url))?;

    for message in &case.subscribe_messages {
        ws.send(Message::Text(message.clone()))
            .await
            .with_context(|| format!("send subscribe for {}", case.name))?;
    }

    let mut summaries = Vec::new();
    for _ in 0..frame_count {
        let message = timeout(timeout_duration, ws.next())
            .await
            .context("receive timed out")?
            .context("closed before first message")?
            .context("read websocket message")?;
        let summary = summarize_message(message);
        if summary.contains("Blocked") || summary.contains("Not Subscribed") {
            anyhow::bail!("{summary}");
        }
        summaries.push(summary);
    }
    Ok(summaries.join(" | "))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpProxy {
    host: String,
    port: u16,
    authorization: Option<String>,
    source: &'static str,
}

pub async fn connect_websocket<R>(
    request: R,
) -> Result<(WebSocketStream<MaybeTlsStream<TcpStream>>, Response), Error>
where
    R: IntoClientRequest + Unpin,
{
    let request = request.into_client_request()?;
    let Some(proxy) = http_proxy_from_env(request.uri().host()) else {
        return tokio_tungstenite::connect_async(request).await;
    };

    let host = request
        .uri()
        .host()
        .ok_or(Error::Url(UrlError::NoHostName))?
        .to_string();
    let port = request
        .uri()
        .port_u16()
        .or_else(|| match request.uri().scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .ok_or(Error::Url(UrlError::UnsupportedUrlScheme))?;

    let stream = connect_http_tunnel(&proxy, &host, port).await?;
    client_async_tls_with_config(request, stream, None, None).await
}

pub fn reqwest_client_builder_with_ws_proxy() -> reqwest::ClientBuilder {
    let mut builder = reqwest::Client::builder();
    if let Ok(proxy_url) = std::env::var("RUSTCTA_WS_PROXY") {
        let proxy_url = proxy_url.trim();
        if !proxy_url.is_empty() {
            if let Ok(proxy) = reqwest::Proxy::all(proxy_url) {
                builder = builder.proxy(proxy);
            }
        }
    }
    builder
}

async fn connect_http_tunnel(
    proxy: &HttpProxy,
    target_host: &str,
    target_port: u16,
) -> Result<TcpStream, Error> {
    let mut stream = TcpStream::connect((proxy.host.as_str(), proxy.port))
        .await
        .map_err(Error::Io)?;
    let authority = format!("{target_host}:{target_port}");
    let mut request = format!(
        "CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nProxy-Connection: Keep-Alive\r\n"
    );
    if let Some(authorization) = &proxy.authorization {
        request.push_str("Proxy-Authorization: Basic ");
        request.push_str(authorization);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(Error::Io)?;

    let mut reader = BufReader::new(stream);
    let mut status_line = String::new();
    let bytes = reader
        .read_line(&mut status_line)
        .await
        .map_err(Error::Io)?;
    if bytes == 0 || !status_line.contains(" 200 ") {
        return Err(Error::Url(UrlError::UnableToConnect(format!(
            "proxy CONNECT {authority} failed: {}",
            status_line.trim()
        ))));
    }

    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line).await.map_err(Error::Io)?;
        if bytes == 0 || line == "\r\n" || line == "\n" {
            break;
        }
    }

    Ok(reader.into_inner())
}

fn http_proxy_from_env(target_host: Option<&str>) -> Option<HttpProxy> {
    for source in [
        "RUSTCTA_WS_PROXY",
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        let Ok(value) = std::env::var(source) else {
            continue;
        };
        let value = value.trim();
        if value.is_empty() || no_proxy_matches(target_host, source) {
            continue;
        }
        if let Some(proxy) = parse_http_proxy(value, source) {
            return Some(proxy);
        }
    }
    None
}

fn parse_http_proxy(value: &str, source: &'static str) -> Option<HttpProxy> {
    let url_text = if value.contains("://") {
        value.to_string()
    } else {
        format!("http://{value}")
    };
    let url = Url::parse(&url_text).ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    let host = url.host_str()?.to_string();
    let port = url.port().or_else(|| match url.scheme() {
        "http" => Some(80),
        "https" => Some(443),
        _ => None,
    })?;
    let authorization = if url.username().is_empty() {
        None
    } else {
        let password = url.password().unwrap_or("");
        Some(general_purpose::STANDARD.encode(format!("{}:{password}", url.username())))
    };
    Some(HttpProxy {
        host,
        port,
        authorization,
        source,
    })
}

fn no_proxy_matches(target_host: Option<&str>, source: &str) -> bool {
    if source == "RUSTCTA_WS_PROXY" {
        return false;
    }
    let Some(target_host) = target_host else {
        return false;
    };
    let no_proxy = std::env::var("NO_PROXY")
        .or_else(|_| std::env::var("no_proxy"))
        .unwrap_or_default();
    no_proxy.split(',').any(|entry| {
        let entry = entry.trim();
        if entry.is_empty() {
            false
        } else if entry == "*" {
            true
        } else if let Some(domain) = entry.strip_prefix('.') {
            target_host.ends_with(domain)
        } else {
            target_host == entry
        }
    })
}

#[derive(Debug, Clone)]
struct ProbeCase {
    name: &'static str,
    url: String,
    subscribe_messages: Vec<String>,
}

impl ProbeCase {
    fn exchange_name(&self) -> &str {
        self.name
            .split_once('-')
            .map(|(exchange, _)| exchange)
            .unwrap_or(self.name)
    }

    fn endpoint_kind(&self) -> &'static str {
        "public_ws"
    }
}

async fn selected_cases(filter: &str) -> Result<Vec<ProbeCase>> {
    let filter = filter.trim().to_ascii_lowercase();
    let mut cases = static_cases();

    if filter == "all" || filter == "kucoin" || filter == "kucoin-spot" {
        cases.push(kucoin_spot_case().await?);
    }

    if filter == "all" {
        return Ok(cases);
    }

    let selected = cases
        .into_iter()
        .filter(|case| case.name == filter || case.name.starts_with(&format!("{filter}-")))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        anyhow::bail!(
            "unsupported exchange {}; use all or a printed case name",
            filter
        );
    }
    Ok(selected)
}

fn static_cases() -> Vec<ProbeCase> {
    vec![
        ProbeCase {
            name: "binance-spot",
            url: "wss://stream.binance.com:9443/ws/btcusdt@bookTicker".to_string(),
            subscribe_messages: Vec::new(),
        },
        ProbeCase {
            name: "binance-perp",
            url: "wss://fstream.binance.com/stream?streams=btcusdt@depth5@100ms".to_string(),
            subscribe_messages: Vec::new(),
        },
        ProbeCase {
            name: "okx-spot",
            url: "wss://ws.okx.com:8443/ws/v5/public".to_string(),
            subscribe_messages: vec![json!({
                "op": "subscribe",
                "args": [{"channel": "books5", "instId": "BTC-USDT"}]
            })
            .to_string()],
        },
        ProbeCase {
            name: "okx-perp",
            url: "wss://ws.okx.com:8443/ws/v5/public".to_string(),
            subscribe_messages: vec![json!({
                "op": "subscribe",
                "args": [{"channel": "books5", "instId": "BTC-USDT-SWAP"}]
            })
            .to_string()],
        },
        ProbeCase {
            name: "bitget-spot",
            url: "wss://ws.bitget.com/v2/ws/public".to_string(),
            subscribe_messages: vec![json!({
                "op": "subscribe",
                "args": [{"instType": "SPOT", "channel": "books5", "instId": "BTCUSDT"}]
            })
            .to_string()],
        },
        ProbeCase {
            name: "bitget-perp",
            url: "wss://ws.bitget.com/v2/ws/public".to_string(),
            subscribe_messages: vec![json!({
                "op": "subscribe",
                "args": [{"instType": "USDT-FUTURES", "channel": "books5", "instId": "BTCUSDT"}]
            })
            .to_string()],
        },
        ProbeCase {
            name: "gate-spot",
            url: "wss://api.gateio.ws/ws/v4/".to_string(),
            subscribe_messages: vec![json!({
                "time": chrono::Utc::now().timestamp(),
                "channel": "spot.book_ticker",
                "event": "subscribe",
                "payload": ["BTC_USDT"]
            })
            .to_string()],
        },
        ProbeCase {
            name: "gate-perp",
            url: "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string(),
            subscribe_messages: vec![json!({
                "time": chrono::Utc::now().timestamp(),
                "channel": "futures.order_book",
                "event": "subscribe",
                "payload": ["BTC_USDT", "5", "0"]
            })
            .to_string()],
        },
        ProbeCase {
            name: "mexc-spot",
            url: "wss://wbs-api.mexc.com/ws".to_string(),
            subscribe_messages: vec![json!({
                "method": "SUBSCRIPTION",
                "params": ["spot@public.limit.depth.v3.api.pb@BTCUSDT@5"]
            })
            .to_string()],
        },
        ProbeCase {
            name: "mexc-perp",
            url: "wss://contract.mexc.com/edge".to_string(),
            subscribe_messages: vec![json!({
                "method": "sub.depth.full",
                "param": {"symbol": "BTC_USDT", "limit": 5}
            })
            .to_string()],
        },
        ProbeCase {
            name: "coinex-spot",
            url: "wss://socket.coinex.com/v2/spot".to_string(),
            subscribe_messages: vec![json!({
                "method": "depth.subscribe",
                "params": {"market_list": [["BTCUSDT", 5, "0", true]]},
                "id": chrono::Utc::now().timestamp_millis()
            })
            .to_string()],
        },
        ProbeCase {
            name: "bybit-perp",
            url: "wss://stream.bybit.com/v5/public/linear".to_string(),
            subscribe_messages: vec![json!({
                "op": "subscribe",
                "args": ["orderbook.1.BTCUSDT"]
            })
            .to_string()],
        },
        ProbeCase {
            name: "htx-perp",
            url: "wss://api.hbdm.com/linear-swap-ws".to_string(),
            subscribe_messages: vec![json!({
                "sub": "market.BTC-USDT.depth.step0",
                "id": "BTC-USDT-depth-step0"
            })
            .to_string()],
        },
        ProbeCase {
            name: "hyperliquid",
            url: "wss://api.hyperliquid.xyz/ws".to_string(),
            subscribe_messages: vec![json!({
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": "BTC"}
            })
            .to_string()],
        },
        ProbeCase {
            name: "bitmart-spot",
            url: "wss://ws-manager-compress.bitmart.com/api?protocol=1.1".to_string(),
            subscribe_messages: vec![json!({
                "op": "subscribe",
                "args": ["spot/depth5:BTC_USDT"]
            })
            .to_string()],
        },
        ProbeCase {
            name: "bitmart-perp",
            url: "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1".to_string(),
            subscribe_messages: vec![json!({
                "action": "subscribe",
                "args": ["futures/depth5:BTCUSDT"]
            })
            .to_string()],
        },
    ]
}

async fn kucoin_spot_case() -> Result<ProbeCase> {
    let value: Value = reqwest::Client::new()
        .post("https://api.kucoin.com/api/v1/bullet-public")
        .send()
        .await
        .context("request KuCoin bullet-public")?
        .error_for_status()?
        .json()
        .await
        .context("decode KuCoin bullet-public")?;
    let data = value
        .get("data")
        .ok_or_else(|| anyhow::anyhow!("KuCoin bullet-public response missing data"))?;
    let server = data
        .get("instanceServers")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("endpoint"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("KuCoin bullet-public response missing endpoint"))?;
    let token = data
        .get("token")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("KuCoin bullet-public response missing token"))?;
    Ok(ProbeCase {
        name: "kucoin-spot",
        url: format!("{server}?token={token}"),
        subscribe_messages: vec![json!({
            "id": chrono::Utc::now().timestamp_millis().to_string(),
            "type": "subscribe",
            "topic": "/spotMarket/level2Depth5:BTC-USDT",
            "privateChannel": false,
            "response": true
        })
        .to_string()],
    })
}

pub(crate) fn summarize_message(message: Message) -> String {
    match message {
        Message::Text(text) => summarize_text("text", text),
        Message::Binary(bytes) => summarize_binary(bytes),
        Message::Ping(bytes) => format!("ping {} bytes", bytes.len()),
        Message::Pong(bytes) => format!("pong {} bytes", bytes.len()),
        Message::Close(frame) => format!("close {frame:?}"),
        Message::Frame(_) => "frame".to_string(),
    }
}

fn summarize_text(kind: &str, mut text: String) -> String {
    text = text.replace('\n', " ");
    text.truncate(220);
    format!("{kind} {text}")
}

fn summarize_binary(bytes: Vec<u8>) -> String {
    if let Ok(text) = std::str::from_utf8(&bytes) {
        return summarize_text("binary-utf8", text.to_string());
    }
    let mut decoder = GzDecoder::new(bytes.as_slice());
    let mut text = String::new();
    if decoder.read_to_string(&mut text).is_ok() {
        return summarize_text("binary-gzip", text);
    }
    match rmp_serde::from_slice::<Value>(&bytes) {
        Ok(value) => summarize_text("binary-msgpack", value.to_string()),
        Err(_) => {
            let hex = bytes
                .iter()
                .take(24)
                .map(|byte| format!("{byte:02x}"))
                .collect::<Vec<_>>()
                .join(" ");
            format!("binary {} bytes [{}]", bytes.len(), hex)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_case_should_expose_region_matrix_dimensions() {
        let case = ProbeCase {
            name: "binance-spot",
            url: "wss://example.test".to_string(),
            subscribe_messages: Vec::new(),
        };

        assert_eq!(case.exchange_name(), "binance");
        assert_eq!(case.endpoint_kind(), "public_ws");
    }
}

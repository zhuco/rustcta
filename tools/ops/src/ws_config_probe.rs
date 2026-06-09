use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Args;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::time::{timeout, Instant};
use tokio_tungstenite::tungstenite::Message;

use crate::ws_proxy_probe::{connect_websocket, summarize_message};

#[derive(Debug, Clone, Args)]
pub struct WsConfigProbeArgs {
    #[arg(long)]
    pub config: PathBuf,
    #[arg(long, default_value = "all")]
    pub exchange: String,
    #[arg(long, default_value_t = 15_000)]
    pub timeout_ms: u64,
    #[arg(long, default_value_t = 80)]
    pub binance_chunk_size: usize,
    #[arg(long, default_value_t = 50)]
    pub bitget_chunk_size: usize,
    #[arg(long, default_value_t = 30)]
    pub gate_chunk_size: usize,
    #[arg(long, default_value_t = 120)]
    pub max_frames_per_connection: usize,
    #[arg(long, default_value_t = 0)]
    pub max_symbols: usize,
    #[arg(long, default_value_t = 2)]
    pub connection_retries: usize,
    #[arg(long, default_value_t = 250)]
    pub connection_pause_ms: u64,
    #[arg(long, default_value_t = 20)]
    pub subscribe_pause_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WsConfigProbeReport {
    pub schema_version: u16,
    pub config: String,
    pub websocket_only: bool,
    pub requested_exchanges: Vec<String>,
    pub configured_symbols: usize,
    pub total_subscriptions: usize,
    pub exchanges: Vec<ExchangeProbeReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExchangeProbeReport {
    pub exchange: String,
    pub endpoint: &'static str,
    pub configured_symbols: usize,
    pub attempted_subscriptions: usize,
    pub connection_count: usize,
    pub connections_ok: usize,
    pub connections_failed: usize,
    pub frames_read: usize,
    pub unique_symbols_seen: usize,
    pub sample_symbols_seen: Vec<String>,
    pub rejected_symbols: Vec<String>,
    pub errors: Vec<String>,
    pub sample_frames: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ConfigProbeInput {
    pub(crate) exchanges: Vec<String>,
    pub(crate) symbols: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct WsProbeConnection {
    pub(crate) exchange: String,
    pub(crate) url: String,
    pub(crate) subscribe_messages: Vec<String>,
    pub(crate) symbols: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct ProbeConnectionOutcome {
    frames: usize,
    rejected_symbols: Vec<String>,
    errors: Vec<String>,
}

pub async fn run_ws_config_probe(args: WsConfigProbeArgs) -> Result<WsConfigProbeReport> {
    let input = load_config_probe_input(&args.config, args.max_symbols)?;
    let requested_exchanges = selected_exchanges(&input.exchanges, &args.exchange)?;
    let mut reports = Vec::new();
    for exchange in &requested_exchanges {
        let connections = build_connections(exchange, &input.symbols, &args)?;
        reports.push(probe_exchange(exchange, connections, &args).await);
    }
    Ok(WsConfigProbeReport {
        schema_version: 1,
        config: args.config.display().to_string(),
        websocket_only: true,
        requested_exchanges,
        configured_symbols: input.symbols.len(),
        total_subscriptions: reports
            .iter()
            .map(|report| report.attempted_subscriptions)
            .sum(),
        exchanges: reports,
    })
}

pub(crate) fn load_config_probe_input(
    path: &PathBuf,
    max_symbols: usize,
) -> Result<ConfigProbeInput> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;
    let value = serde_json::to_value(yaml).context("convert yaml config")?;

    let exchanges = first_string_array(
        &value,
        &[
            &["enabled_exchanges"][..],
            &["universe", "enabled_exchanges"][..],
            &["detection", "exchanges"][..],
        ],
    )
    .unwrap_or_else(|| {
        vec![
            "binance".to_string(),
            "bitget".to_string(),
            "gate".to_string(),
        ]
    })
    .into_iter()
    .map(|exchange| normalize_exchange(&exchange))
    .fold(Vec::new(), push_unique);

    let mut symbols = first_string_array(
        &value,
        &[
            &["enabled_symbols"][..],
            &["symbols"][..],
            &["universe", "symbols"][..],
            &["universe", "include_symbols"][..],
            &["detection", "symbols"][..],
        ],
    )
    .unwrap_or_default()
    .into_iter()
    .filter_map(|symbol| normalize_canonical_symbol(&symbol))
    .fold(Vec::new(), push_unique);

    let excluded_symbols = first_string_array(
        &value,
        &[
            &["exclude_symbols"][..],
            &["universe", "exclude_symbols"][..],
        ],
    )
    .unwrap_or_default()
    .into_iter()
    .filter_map(|symbol| normalize_canonical_symbol(&symbol))
    .collect::<BTreeSet<_>>();
    let excluded_bases = first_string_array(&value, &[&["excluded_bases"][..]])
        .unwrap_or_default()
        .into_iter()
        .map(|base| base.trim().to_ascii_uppercase())
        .collect::<BTreeSet<_>>();
    symbols.retain(|symbol| {
        !excluded_symbols.contains(symbol)
            && symbol
                .split_once('/')
                .is_none_or(|(base, _)| !excluded_bases.contains(base))
    });
    if max_symbols > 0 && symbols.len() > max_symbols {
        symbols.truncate(max_symbols);
    }
    anyhow::ensure!(!symbols.is_empty(), "config contains no probe symbols");
    Ok(ConfigProbeInput { exchanges, symbols })
}

pub(crate) fn selected_exchanges(configured: &[String], filter: &str) -> Result<Vec<String>> {
    let filter = filter.trim().to_ascii_lowercase();
    if filter == "all" {
        return Ok(configured.to_vec());
    }
    let normalized = normalize_exchange(&filter);
    anyhow::ensure!(
        configured.contains(&normalized),
        "exchange {filter} is not enabled in config; enabled={configured:?}"
    );
    Ok(vec![normalized])
}

pub(crate) fn build_connections(
    exchange: &str,
    symbols: &[String],
    args: &WsConfigProbeArgs,
) -> Result<Vec<WsProbeConnection>> {
    let chunk_size = match exchange {
        "binance" => args.binance_chunk_size,
        "bitget" => args.bitget_chunk_size,
        "gate" | "gateio" => args.gate_chunk_size,
        other => anyhow::bail!("WS config probe supports binance, bitget, gate; got {other}"),
    }
    .max(1);

    let mut connections = Vec::new();
    for chunk in symbols.chunks(chunk_size) {
        connections.push(match exchange {
            "binance" => binance_connection(chunk),
            "bitget" => bitget_connection(chunk),
            "gate" | "gateio" => gate_connection(chunk),
            _ => unreachable!(),
        });
    }
    Ok(connections)
}

fn binance_connection(symbols: &[String]) -> WsProbeConnection {
    let streams = symbols
        .iter()
        .map(|symbol| {
            format!(
                "{}@depth5@100ms",
                compact_symbol(symbol).to_ascii_lowercase()
            )
        })
        .collect::<Vec<_>>()
        .join("/");
    WsProbeConnection {
        exchange: "binance".to_string(),
        url: format!("wss://fstream.binance.com/stream?streams={streams}"),
        subscribe_messages: Vec::new(),
        symbols: symbols.to_vec(),
    }
}

fn bitget_connection(symbols: &[String]) -> WsProbeConnection {
    let args = symbols
        .iter()
        .map(|symbol| {
            json!({
                "instType": "USDT-FUTURES",
                "channel": "books5",
                "instId": compact_symbol(symbol),
            })
        })
        .collect::<Vec<_>>();
    WsProbeConnection {
        exchange: "bitget".to_string(),
        url: "wss://ws.bitget.com/v2/ws/public".to_string(),
        subscribe_messages: vec![json!({ "op": "subscribe", "args": args }).to_string()],
        symbols: symbols.to_vec(),
    }
}

fn gate_connection(symbols: &[String]) -> WsProbeConnection {
    let subscribe_messages = symbols
        .iter()
        .map(|symbol| {
            json!({
                "time": chrono::Utc::now().timestamp(),
                "channel": "futures.order_book",
                "event": "subscribe",
                "payload": [gate_symbol(symbol), "5", "0"],
            })
            .to_string()
        })
        .collect::<Vec<_>>();
    WsProbeConnection {
        exchange: "gate".to_string(),
        url: "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string(),
        subscribe_messages,
        symbols: symbols.to_vec(),
    }
}

async fn probe_exchange(
    exchange: &str,
    connections: Vec<WsProbeConnection>,
    args: &WsConfigProbeArgs,
) -> ExchangeProbeReport {
    let mut report = ExchangeProbeReport {
        exchange: exchange.to_string(),
        endpoint: "public_ws",
        configured_symbols: connections
            .iter()
            .flat_map(|connection| connection.symbols.iter())
            .collect::<BTreeSet<_>>()
            .len(),
        attempted_subscriptions: connections
            .iter()
            .map(|connection| connection.symbols.len())
            .sum(),
        connection_count: connections.len(),
        connections_ok: 0,
        connections_failed: 0,
        frames_read: 0,
        unique_symbols_seen: 0,
        sample_symbols_seen: Vec::new(),
        rejected_symbols: Vec::new(),
        errors: Vec::new(),
        sample_frames: Vec::new(),
    };
    let mut seen_symbols = BTreeSet::new();
    let mut rejected_symbols = BTreeSet::new();
    let timeout_duration = Duration::from_millis(args.timeout_ms);

    for (index, connection) in connections.into_iter().enumerate() {
        match probe_connection_with_retries(
            &connection,
            args,
            timeout_duration,
            &mut seen_symbols,
            &mut report.sample_frames,
        )
        .await
        {
            Ok(outcome) => {
                report.connections_ok += 1;
                report.frames_read += outcome.frames;
                rejected_symbols.extend(outcome.rejected_symbols);
                for error in outcome.errors {
                    report.errors.push(format!(
                        "{} connection {} error: {}",
                        connection.exchange,
                        index + 1,
                        error
                    ));
                }
            }
            Err(error) => {
                report.connections_failed += 1;
                report.errors.push(format!(
                    "{} connection {} failed: {}",
                    connection.exchange,
                    index + 1,
                    format_error_chain(&error)
                ));
            }
        }
    }
    report.unique_symbols_seen = seen_symbols.len();
    report.sample_symbols_seen = seen_symbols.into_iter().take(20).collect();
    report.rejected_symbols = rejected_symbols.into_iter().collect();
    report
}

async fn probe_connection_with_retries(
    connection: &WsProbeConnection,
    args: &WsConfigProbeArgs,
    timeout_duration: Duration,
    seen_symbols: &mut BTreeSet<String>,
    sample_frames: &mut Vec<String>,
) -> Result<ProbeConnectionOutcome> {
    let attempts = args.connection_retries.saturating_add(1);
    let mut last_error = None;
    for attempt in 1..=attempts {
        match probe_connection(
            connection,
            timeout_duration,
            args.max_frames_per_connection.max(1),
            args.subscribe_pause_ms,
            seen_symbols,
            sample_frames,
        )
        .await
        {
            Ok(outcome) => return Ok(outcome),
            Err(error) => {
                last_error = Some(error);
                if attempt < attempts {
                    tokio::time::sleep(Duration::from_millis(args.connection_pause_ms)).await;
                }
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("connection was not attempted")))
}

async fn probe_connection(
    connection: &WsProbeConnection,
    timeout_duration: Duration,
    max_frames: usize,
    subscribe_pause_ms: u64,
    seen_symbols: &mut BTreeSet<String>,
    sample_frames: &mut Vec<String>,
) -> Result<ProbeConnectionOutcome> {
    let (mut ws, _) = timeout(timeout_duration, connect_websocket(connection.url.as_str()))
        .await
        .context("connect timed out")?
        .with_context(|| format!("connect {}", connection.url))?;
    for message in &connection.subscribe_messages {
        ws.send(Message::Text(message.clone()))
            .await
            .with_context(|| format!("send subscribe for {}", connection.exchange))?;
        if subscribe_pause_ms > 0 {
            tokio::time::sleep(Duration::from_millis(subscribe_pause_ms)).await;
        }
    }

    let deadline = Instant::now() + timeout_duration;
    let mut frames = 0usize;
    let mut errors = Vec::new();
    let mut rejected_symbols = BTreeSet::new();
    while frames < max_frames && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Some(message) = timeout(remaining, ws.next())
            .await
            .context("receive timed out")?
        else {
            break;
        };
        let message = message.context("read websocket message")?;
        let summary = summarize_message(message.clone());
        if sample_frames.len() < 12 {
            sample_frames.push(summary.clone());
        }
        if is_error_message(&message) {
            if let Some(symbol) = symbol_from_message(&connection.exchange, &message) {
                rejected_symbols.insert(symbol);
            }
            errors.push(format!("exchange returned error frame: {}", summary));
        }
        if let Some(symbol) = symbol_from_message(&connection.exchange, &message) {
            seen_symbols.insert(symbol);
        }
        frames += 1;
    }
    anyhow::ensure!(frames > 0, "no websocket frames received");
    Ok(ProbeConnectionOutcome {
        frames,
        rejected_symbols: rejected_symbols.into_iter().collect(),
        errors,
    })
}

fn is_error_message(message: &Message) -> bool {
    let text = match message {
        Message::Text(text) => text.as_str(),
        _ => return false,
    };
    serde_json::from_str::<Value>(text)
        .ok()
        .is_some_and(|value| {
            value.get("event").and_then(Value::as_str) == Some("error")
                || value
                    .get("code")
                    .and_then(Value::as_str)
                    .is_some_and(|code| !matches!(code, "0" | "00000"))
                || value.get("error").is_some()
        })
}

fn symbol_from_message(exchange: &str, message: &Message) -> Option<String> {
    let Message::Text(text) = message else {
        return None;
    };
    let value = serde_json::from_str::<Value>(text).ok()?;
    match exchange {
        "binance" => value
            .get("stream")
            .and_then(Value::as_str)
            .and_then(|stream| stream.split('@').next())
            .map(|symbol| symbol.to_ascii_uppercase()),
        "bitget" => value
            .get("arg")
            .and_then(|arg| arg.get("instId"))
            .and_then(Value::as_str)
            .or_else(|| {
                value
                    .get("data")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                    .and_then(|item| item.get("instId"))
                    .and_then(Value::as_str)
            })
            .map(|symbol| symbol.to_ascii_uppercase()),
        "gate" | "gateio" => value
            .get("result")
            .and_then(|result| {
                result
                    .get("contract")
                    .or_else(|| result.get("s"))
                    .or_else(|| result.get("currency_pair"))
            })
            .and_then(Value::as_str)
            .or_else(|| {
                value
                    .get("payload")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                    .and_then(Value::as_str)
            })
            .map(|symbol| symbol.replace('_', "").to_ascii_uppercase()),
        _ => None,
    }
}

fn format_error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

fn first_string_array(value: &Value, paths: &[&[&str]]) -> Option<Vec<String>> {
    for path in paths {
        let mut current = value;
        let mut found = true;
        for segment in *path {
            if let Some(next) = current.get(*segment) {
                current = next;
            } else {
                found = false;
                break;
            }
        }
        if !found {
            continue;
        }
        let Some(items) = current.as_array() else {
            continue;
        };
        let values = items
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if !values.is_empty() {
            return Some(values);
        }
    }
    None
}

fn normalize_exchange(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gateio" | "gate.io" | "gate_io" => "gate".to_string(),
        other => other.to_string(),
    }
}

fn normalize_canonical_symbol(symbol: &str) -> Option<String> {
    let normalized = symbol.trim().replace('-', "/").to_ascii_uppercase();
    if let Some((base, quote)) = normalized.split_once('/') {
        let base = base.trim();
        let quote = quote.trim();
        (!base.is_empty() && quote == "USDT").then(|| format!("{base}/USDT"))
    } else {
        normalized
            .strip_suffix("USDT")
            .filter(|base| !base.is_empty())
            .map(|base| format!("{base}/USDT"))
    }
}

fn compact_symbol(symbol: &str) -> String {
    symbol.replace('/', "").to_ascii_uppercase()
}

fn gate_symbol(symbol: &str) -> String {
    symbol.replace('/', "_").to_ascii_uppercase()
}

fn push_unique(mut values: Vec<String>, value: String) -> Vec<String> {
    if !values.contains(&value) {
        values.push(value);
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_normalization_should_keep_usdt_pairs() {
        assert_eq!(
            normalize_canonical_symbol("btc-usdt").as_deref(),
            Some("BTC/USDT")
        );
        assert_eq!(
            normalize_canonical_symbol("ethusdt").as_deref(),
            Some("ETH/USDT")
        );
    }

    #[test]
    fn message_symbol_parsers_should_extract_common_public_ws_shapes() {
        let binance = Message::Text(json!({"stream":"btcusdt@depth5@100ms"}).to_string());
        assert_eq!(
            symbol_from_message("binance", &binance).as_deref(),
            Some("BTCUSDT")
        );
        let bitget = Message::Text(json!({"arg":{"instId":"BTCUSDT"}}).to_string());
        assert_eq!(
            symbol_from_message("bitget", &bitget).as_deref(),
            Some("BTCUSDT")
        );
        let gate = Message::Text(json!({"result":{"contract":"BTC_USDT"}}).to_string());
        assert_eq!(
            symbol_from_message("gate", &gate).as_deref(),
            Some("BTCUSDT")
        );
        let gate_ack = Message::Text(json!({"payload":["BTC_USDT","5","0"]}).to_string());
        assert_eq!(
            symbol_from_message("gate", &gate_ack).as_deref(),
            Some("BTCUSDT")
        );
    }

    #[test]
    fn config_path_arrays_should_ignore_empty_first_candidate() {
        let value = json!({
            "enabled_symbols": [],
            "universe": {"symbols": ["BTC/USDT"]}
        });
        assert_eq!(
            first_string_array(
                &value,
                &[&["enabled_symbols"][..], &["universe", "symbols"][..]]
            )
            .unwrap(),
            vec!["BTC/USDT"]
        );
    }
}

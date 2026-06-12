use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use rustcta_exchange_gateway::{
    AdapterBackedGateway, BinanceGatewayConfig, BitgetGatewayConfig, InProcessGatewayClient,
};
use rustcta_execution_api::{
    CancelCommand, CancellationIds, MutationIdentity, OrderCommand as RouterOrderCommand,
};
use rustcta_execution_router::{ExecutionRouter, ExecutionRouterConfig};
use rustcta_strategy_funding_spread_expansion_maker::{
    FundingSpreadExpansionMakerConfig, FundingSpreadExpansionMakerRuntime, RouteConfig,
    STRATEGY_KIND,
};
use rustcta_strategy_sdk::{
    AccountEvent, ExecutionCancelAck, ExecutionCancelCommand, ExecutionEvent, ExecutionIntent,
    ExecutionIntentAck, ExecutionOrderAck, ExecutionOrderCommand, MarketDataEvent, SdkResult,
    StrategyContext, StrategyEvent, StrategyExecutionClient, StrategyInstanceId, StrategyRuntime,
    StrategySdkError,
};
use rustcta_tools_ops::private_ws_observe::{
    run_private_ws_observe_once, PrivateWsObserveConfig, PrivateWsObserveEvent,
};
use rustcta_tools_ops::ws_proxy_probe::{connect_websocket, reqwest_client_builder_with_ws_proxy};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId as GatewayExchangeId, ExchangeSymbol,
    MarketType as GatewayMarketType, OrderSide as GatewayOrderSide, OrderType as GatewayOrderType,
    PositionSide as GatewayPositionSide, RunId, StrategyId, TenantId,
    TimeInForce as GatewayTimeInForce,
};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug)]
struct Args {
    config: PathBuf,
    run_seconds: u64,
    snapshot_interval_ms: u64,
    funding_refresh_secs: u64,
    private_ws: bool,
    enable_live_trading: bool,
    submit_intents_dry_run: bool,
    live_config_override: bool,
    require_private_ws_ready: bool,
}

#[derive(Debug, Serialize)]
struct LocalWsReport {
    generated_at: DateTime<Utc>,
    strategy_kind: &'static str,
    mode: &'static str,
    private_ws_enabled: bool,
    live_orders_enabled: bool,
    submitted_orders: Vec<Value>,
    snapshot: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = parse_args()?;
    let raw_config = read_yaml_config(&args.config)?;
    let file_config: FundingSpreadExpansionMakerConfig =
        serde_json::from_value(raw_config.clone())?;
    let mut runtime_config_value = raw_config.clone();
    if args.submit_intents_dry_run || args.live_config_override {
        set_runtime_submit_dry_run(&mut runtime_config_value)?;
    }
    let config: FundingSpreadExpansionMakerConfig =
        serde_json::from_value(runtime_config_value.clone())?;
    let live_config_ready = (file_config.mode.eq_ignore_ascii_case("live") && !file_config.dry_run)
        || args.live_config_override;
    let live_orders_enabled = args.enable_live_trading && live_config_ready;
    if args.enable_live_trading && !live_config_ready {
        bail!("--enable-live-trading requires config mode=live and dry_run=false, or --live-config-override");
    }
    if args.live_config_override && !args.enable_live_trading {
        bail!("--live-config-override requires --enable-live-trading");
    }
    if args.enable_live_trading && !args.private_ws {
        bail!(
            "--enable-live-trading requires private websocket observation; remove --no-private-ws"
        );
    }

    let execution = Arc::new(LocalExecutionClient::new(
        live_orders_enabled,
        args.submit_intents_dry_run,
        config.active_exchanges(),
    )?);
    let mut runtime = FundingSpreadExpansionMakerRuntime::new();
    runtime
        .start(strategy_context(runtime_config_value, execution.clone()))
        .await?;

    let (tx, mut rx) = mpsc::channel::<StrategyEvent>(4096);
    seed_account_state(
        &config,
        tx.clone(),
        !(live_orders_enabled || args.require_private_ws_ready),
    )
    .await?;
    spawn_public_book_tasks(&config, tx.clone());
    spawn_funding_refresh_task(&config, tx.clone(), args.funding_refresh_secs);
    if args.private_ws {
        spawn_private_ws_task(&config, tx.clone());
    }

    let deadline = Instant::now() + Duration::from_secs(args.run_seconds.max(1));
    let mut snapshot_tick =
        tokio::time::interval(Duration::from_millis(args.snapshot_interval_ms.max(1_000)));
    loop {
        tokio::select! {
            maybe_event = rx.recv() => {
                if let Some(event) = maybe_event {
                    runtime.handle_event(event).await?;
                }
            }
            _ = snapshot_tick.tick() => {
                emit_report(&args, &runtime, &execution, live_orders_enabled).await?;
            }
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    emit_report(&args, &runtime, &execution, live_orders_enabled).await?;
    runtime.stop().await?;
    Ok(())
}

fn parse_args() -> Result<Args> {
    let mut values = std::env::args().skip(1);
    let mut args = Args {
        config: PathBuf::from("config/funding_spread_expansion_maker_usdt.yml"),
        run_seconds: 60,
        snapshot_interval_ms: 5_000,
        funding_refresh_secs: 30,
        private_ws: true,
        enable_live_trading: false,
        submit_intents_dry_run: false,
        live_config_override: false,
        require_private_ws_ready: false,
    };
    while let Some(arg) = values.next() {
        match arg.as_str() {
            "--config" => args.config = PathBuf::from(next_value(&mut values, "--config")?),
            "--run-seconds" => {
                args.run_seconds = next_value(&mut values, "--run-seconds")?
                    .parse()
                    .context("--run-seconds must be a positive integer")?
            }
            "--snapshot-interval-ms" => {
                args.snapshot_interval_ms = next_value(&mut values, "--snapshot-interval-ms")?
                    .parse()
                    .context("--snapshot-interval-ms must be a positive integer")?
            }
            "--funding-refresh-secs" => {
                args.funding_refresh_secs = next_value(&mut values, "--funding-refresh-secs")?
                    .parse()
                    .context("--funding-refresh-secs must be a positive integer")?
            }
            "--no-private-ws" => args.private_ws = false,
            "--enable-live-trading" => args.enable_live_trading = true,
            "--submit-intents-dry-run" => args.submit_intents_dry_run = true,
            "--live-config-override" => args.live_config_override = true,
            "--require-private-ws-ready" => args.require_private_ws_ready = true,
            "--help" | "-h" => {
                println!(
                    "funding-spread-expansion-maker-ws-local --config <path> [--run-seconds <n>] [--no-private-ws] [--require-private-ws-ready] [--submit-intents-dry-run] [--enable-live-trading --live-config-override]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(args)
}

fn set_runtime_submit_dry_run(config: &mut Value) -> Result<()> {
    let object = config
        .as_object_mut()
        .context("runtime config root must be an object")?;
    object.insert("mode".to_string(), json!("live"));
    object.insert("dry_run".to_string(), json!(false));
    Ok(())
}

fn next_value(values: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    values
        .next()
        .with_context(|| format!("{flag} requires a value"))
}

fn read_yaml_config(path: &PathBuf) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;
    serde_json::to_value(yaml).context("convert runtime config to json")
}

fn strategy_context(
    config: serde_json::Value,
    execution: Arc<dyn StrategyExecutionClient>,
) -> StrategyContext {
    StrategyContext::new(
        StrategyInstanceId::new("funding_spread_expansion_maker_ws_local:local"),
        "local",
        "default",
        "funding_spread_expansion_maker_ws_local",
        "local",
        config,
        execution,
    )
}

async fn emit_report(
    args: &Args,
    runtime: &FundingSpreadExpansionMakerRuntime,
    execution: &LocalExecutionClient,
    live_orders_enabled: bool,
) -> Result<()> {
    let report = LocalWsReport {
        generated_at: Utc::now(),
        strategy_kind: STRATEGY_KIND,
        mode: "local_ws",
        private_ws_enabled: args.private_ws,
        live_orders_enabled,
        submitted_orders: execution.orders(),
        snapshot: serde_json::to_value(runtime.snapshot().await?)?,
    };
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

async fn seed_account_state(
    config: &FundingSpreadExpansionMakerConfig,
    tx: mpsc::Sender<StrategyEvent>,
    private_stream_ready: bool,
) -> Result<()> {
    for route in config.routes.iter().filter(|route| route.enabled) {
        for exchange in [&route.leg_a_exchange, &route.leg_b_exchange] {
            tx.send(account_event(
                exchange,
                &route.symbol,
                json!({
                    "event_kind": "symbol_precision",
                    "exchange": exchange,
                    "canonical_symbol": route.symbol,
                    "price_tick": 0.000001,
                    "quantity_step_base": 1.0,
                    "min_quantity_base": 1.0,
                    "min_notional_usdt": 5.0
                }),
            ))
            .await
            .ok();
            tx.send(account_event(
                exchange,
                &route.symbol,
                json!({
                    "event_kind": "fee_rates",
                    "exchange": exchange,
                    "canonical_symbol": route.symbol,
                    "maker_fee_rate": 0.0002,
                    "taker_fee_rate": 0.0006
                }),
            ))
            .await
            .ok();
            tx.send(account_event(
                exchange,
                &route.symbol,
                json!({
                    "event_kind": "risk_snapshot",
                    "exchange": exchange,
                    "canonical_symbol": route.symbol,
                    "private_stream_ready": private_stream_ready,
                    "precision_ready": true,
                    "account_position_ready": true,
                    "no_unmanaged_position": true,
                    "symbol_cooling_down": false,
                    "pending_repair": false,
                    "unknown_order_state": false,
                    "mmr_pct": 0.0,
                    "adl_pct": 0.0,
                    "liquidation_buffer_pct": 100.0,
                    "positions_observed_at": Utc::now().to_rfc3339()
                }),
            ))
            .await
            .ok();
        }
    }
    Ok(())
}

fn spawn_public_book_tasks(
    config: &FundingSpreadExpansionMakerConfig,
    tx: mpsc::Sender<StrategyEvent>,
) {
    for route in config.routes.iter().filter(|route| route.enabled) {
        for exchange in [&route.leg_a_exchange, &route.leg_b_exchange] {
            let route = route.clone();
            let exchange = exchange.to_ascii_lowercase();
            let tx = tx.clone();
            tokio::spawn(async move {
                loop {
                    let result = match exchange.as_str() {
                        "binance" => run_binance_book_ws(&route, tx.clone()).await,
                        "bitget" => run_bitget_book_ws(&route, tx.clone()).await,
                        "gate" | "gateio" | "gate.io" => {
                            run_gateio_book_ws(&route, tx.clone()).await
                        }
                        "aster" => run_aster_book_ws(&route, tx.clone()).await,
                        "mexc" => run_mexc_book_ws(&route, tx.clone()).await,
                        "kucoinfutures" | "kucoin_futures" => {
                            run_kucoinfutures_book_ws(&route, tx.clone()).await
                        }
                        "bybit" => run_bybit_book_ws(&route, tx.clone()).await,
                        other => Err(anyhow::anyhow!("unsupported public ws exchange {other}")),
                    };
                    if let Err(error) = result {
                        eprintln!("public ws {exchange} error: {error}");
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            });
        }
    }
}

fn spawn_funding_refresh_task(
    config: &FundingSpreadExpansionMakerConfig,
    tx: mpsc::Sender<StrategyEvent>,
    refresh_secs: u64,
) {
    let routes = config.routes.clone();
    tokio::spawn(async move {
        let client = match reqwest_client_builder_with_ws_proxy().build() {
            Ok(client) => client,
            Err(error) => {
                eprintln!("build funding refresh client error: {error}");
                return;
            }
        };
        loop {
            for route in routes.iter().filter(|route| route.enabled) {
                for exchange in [&route.leg_a_exchange, &route.leg_b_exchange] {
                    match fetch_funding(&client, exchange, &route.symbol).await {
                        Ok(payload) => {
                            tx.send(market_event(exchange, &route.symbol, payload))
                                .await
                                .ok();
                        }
                        Err(error) => {
                            eprintln!("funding refresh {exchange} {} error: {error}", route.symbol)
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(refresh_secs.max(5))).await;
        }
    });
}

fn spawn_private_ws_task(
    config: &FundingSpreadExpansionMakerConfig,
    tx: mpsc::Sender<StrategyEvent>,
) {
    let exchanges = config.active_exchanges();
    let routes = config.routes.clone();
    let (private_tx, mut private_rx) = mpsc::channel::<PrivateWsObserveEvent>(1024);
    for exchange in exchanges {
        let private_tx = private_tx.clone();
        tokio::spawn(async move {
            let exchanges = vec![exchange];
            loop {
                run_private_ws_observe_once(
                    &exchanges,
                    PrivateWsObserveConfig {
                        timeout_ms: 15_000,
                        reconnect_delay_ms: 2_000,
                        gateio_user_id: std::env::var("GATEIO_USER_ID").ok(),
                    },
                    private_tx.clone(),
                )
                .await;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }
    tokio::spawn(async move {
        let mut private_ready_by_exchange: HashMap<String, bool> = HashMap::new();
        while let Some(event) = private_rx.recv().await {
            match event {
                PrivateWsObserveEvent::Status { exchange, row } => {
                    let normalized_exchange = exchange.to_ascii_lowercase();
                    private_ready_by_exchange
                        .insert(normalized_exchange.clone(), private_status_ready(&row));
                    for (symbol, payload) in private_status_to_risk_events(
                        &routes,
                        &normalized_exchange,
                        &private_ready_by_exchange,
                        &row,
                    ) {
                        tx.send(account_event(&exchange, &symbol, payload))
                            .await
                            .ok();
                    }
                }
                PrivateWsObserveEvent::PrivateEvent(row) => {
                    if let Some(exec_event) = private_row_to_execution_event(row) {
                        tx.send(StrategyEvent::Execution(exec_event)).await.ok();
                    }
                }
            }
        }
    });
}

fn private_status_ready(row: &Value) -> bool {
    let status = row
        .get("private_stream_status")
        .or_else(|| row.get("status"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let connected = row
        .get("connected")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let login_ok = row.get("login_ok").and_then(Value::as_bool).unwrap_or(true);
    connected && login_ok && status == "online"
}

fn private_status_to_risk_events(
    routes: &[RouteConfig],
    exchange: &str,
    private_ready_by_exchange: &HashMap<String, bool>,
    row: &Value,
) -> Vec<(String, Value)> {
    let normalized_exchange = exchange.to_ascii_lowercase();
    let status = row
        .get("private_stream_status")
        .or_else(|| row.get("status"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    routes
        .iter()
        .filter(|route| route.enabled)
        .filter(|route| {
            route
                .leg_a_exchange
                .eq_ignore_ascii_case(&normalized_exchange)
                || route
                    .leg_b_exchange
                    .eq_ignore_ascii_case(&normalized_exchange)
        })
        .map(|route| {
            let leg_a_ready = private_ready_by_exchange
                .get(&route.leg_a_exchange.to_ascii_lowercase())
                .copied()
                .unwrap_or(false);
            let leg_b_ready = private_ready_by_exchange
                .get(&route.leg_b_exchange.to_ascii_lowercase())
                .copied()
                .unwrap_or(false);
            let private_stream_ready = leg_a_ready && leg_b_ready;
            (
                route.symbol.clone(),
                json!({
                    "event_kind": "risk_snapshot",
                    "exchange": normalized_exchange,
                    "canonical_symbol": route.symbol,
                    "private_stream_ready": private_stream_ready,
                    "precision_ready": true,
                    "account_position_ready": true,
                    "no_unmanaged_position": true,
                    "symbol_cooling_down": false,
                    "pending_repair": false,
                    "unknown_order_state": !private_stream_ready,
                    "mmr_pct": 0.0,
                    "adl_pct": 0.0,
                    "liquidation_buffer_pct": 100.0,
                    "positions_observed_at": Utc::now().to_rfc3339(),
                    "private_stream_status": status,
                    "private_stream_message": row.get("message").cloned().unwrap_or(Value::Null),
                }),
            )
        })
        .collect()
}

async fn run_binance_book_ws(route: &RouteConfig, tx: mpsc::Sender<StrategyEvent>) -> Result<()> {
    let symbol = exchange_symbol(&route.symbol).to_ascii_lowercase();
    let url = format!(
        "wss://fstream.binance.com/stream?streams={symbol}@depth5@100ms/{symbol}@markPrice@1s"
    );
    let (mut ws, _) =
        tokio::time::timeout(Duration::from_secs(10), connect_websocket(url.as_str()))
            .await
            .context("connect Binance market ws timed out")?
            .context("connect Binance market ws")?;
    while let Some(message) = ws.next().await {
        let Message::Text(text) = message? else {
            continue;
        };
        let value: Value = serde_json::from_str(&text)?;
        let data = value.get("data").unwrap_or(&value);
        if data.get("r").is_some() {
            tx.send(market_event(
                "binance",
                &route.symbol,
                json!({
                    "event_kind": "funding_rate",
                    "exchange": "binance",
                    "canonical_symbol": route.symbol,
                    "funding_rate": data.get("r").cloned().unwrap_or(Value::Null),
                    "funding_interval_hours": 4.0,
                    "next_funding_time": millis_to_rfc3339(data.get("T")),
                    "mark_price": data.get("p").cloned().unwrap_or(Value::Null),
                    "index_price": data.get("i").cloned().unwrap_or(Value::Null),
                    "received_at": Utc::now().to_rfc3339()
                }),
            ))
            .await
            .ok();
            continue;
        }
        let Some(bid_row) = data
            .get("b")
            .or_else(|| data.get("bids"))
            .and_then(Value::as_array)
            .and_then(|bids| bids.first())
        else {
            continue;
        };
        let Some(ask_row) = data
            .get("a")
            .or_else(|| data.get("asks"))
            .and_then(Value::as_array)
            .and_then(|asks| asks.first())
        else {
            continue;
        };
        let bid = array_f64(bid_row, 0)?;
        let bid_qty = array_f64(bid_row, 1)?;
        let ask = array_f64(ask_row, 0)?;
        let ask_qty = array_f64(ask_row, 1)?;
        tx.send(book_event(
            "binance",
            &route.symbol,
            bid,
            bid_qty,
            ask,
            ask_qty,
        ))
        .await
        .ok();
    }
    Ok(())
}

async fn run_bitget_book_ws(route: &RouteConfig, tx: mpsc::Sender<StrategyEvent>) -> Result<()> {
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_websocket("wss://ws.bitget.com/v2/ws/public"),
    )
    .await
    .context("connect Bitget public ws timed out")?
    .context("connect Bitget public ws")?;
    ws.send(Message::Text(
        json!({
            "op": "subscribe",
            "args": [{
                "instType": "USDT-FUTURES",
                "channel": "books5",
                "instId": exchange_symbol(&route.symbol)
            }, {
                "instType": "USDT-FUTURES",
                "channel": "ticker",
                "instId": exchange_symbol(&route.symbol)
            }]
        })
        .to_string(),
    ))
    .await?;
    while let Some(message) = ws.next().await {
        let Message::Text(text) = message? else {
            continue;
        };
        let value: Value = serde_json::from_str(&text)?;
        let Some(item) = value
            .get("data")
            .and_then(Value::as_array)
            .and_then(|data| data.first())
        else {
            continue;
        };
        if item.get("fundingRate").is_some() {
            tx.send(market_event(
                "bitget",
                &route.symbol,
                json!({
                    "event_kind": "funding_rate",
                    "exchange": "bitget",
                    "canonical_symbol": route.symbol,
                    "funding_rate": item.get("fundingRate").cloned().unwrap_or(Value::Null),
                    "funding_interval_hours": item.get("fundingRateInterval").cloned().unwrap_or(json!(4.0)),
                    "next_funding_time": millis_to_rfc3339(
                        item.get("nextFundingTime").or_else(|| item.get("nextUpdate"))
                    ),
                    "mark_price": item.get("markPrice").or_else(|| item.get("markPricePr")).cloned().unwrap_or(Value::Null),
                    "index_price": item.get("indexPrice").or_else(|| item.get("indexPricePr")).cloned().unwrap_or(Value::Null),
                    "received_at": Utc::now().to_rfc3339()
                }),
            ))
            .await
            .ok();
        }
        let Some(bid_row) = item
            .get("bids")
            .and_then(Value::as_array)
            .and_then(|bids| bids.first())
        else {
            continue;
        };
        let Some(ask_row) = item
            .get("asks")
            .and_then(Value::as_array)
            .and_then(|asks| asks.first())
        else {
            continue;
        };
        tx.send(book_event(
            "bitget",
            &route.symbol,
            array_f64(bid_row, 0)?,
            array_f64(bid_row, 1)?,
            array_f64(ask_row, 0)?,
            array_f64(ask_row, 1)?,
        ))
        .await
        .ok();
    }
    Ok(())
}

async fn run_gateio_book_ws(route: &RouteConfig, tx: mpsc::Sender<StrategyEvent>) -> Result<()> {
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_websocket("wss://fx-ws.gateio.ws/v4/ws/usdt"),
    )
    .await
    .context("connect Gate.io futures ws timed out")?
    .context("connect Gate.io futures ws")?;
    ws.send(Message::Text(
        json!({
            "time": Utc::now().timestamp(),
            "channel": "futures.order_book",
            "event": "subscribe",
            "payload": [gateio_symbol(&route.symbol), "5", "0"]
        })
        .to_string(),
    ))
    .await?;
    while let Some(message) = ws.next().await {
        let Message::Text(text) = message? else {
            continue;
        };
        let value: Value = serde_json::from_str(&text)?;
        let Some(result) = value.get("result") else {
            continue;
        };
        let Some(bid_row) = result
            .get("b")
            .or_else(|| result.get("bids"))
            .and_then(Value::as_array)
            .and_then(|bids| bids.first())
        else {
            continue;
        };
        let Some(ask_row) = result
            .get("a")
            .or_else(|| result.get("asks"))
            .and_then(Value::as_array)
            .and_then(|asks| asks.first())
        else {
            continue;
        };
        tx.send(book_event(
            "gate",
            &route.symbol,
            array_f64(bid_row, 0)?,
            array_f64(bid_row, 1)?,
            array_f64(ask_row, 0)?,
            array_f64(ask_row, 1)?,
        ))
        .await
        .ok();
    }
    Ok(())
}

async fn run_aster_book_ws(route: &RouteConfig, tx: mpsc::Sender<StrategyEvent>) -> Result<()> {
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_websocket("wss://fstream.asterdex.com/ws"),
    )
    .await
    .context("connect Aster public ws timed out")?
    .context("connect Aster public ws")?;
    ws.send(Message::Text(
        json!({
            "method": "SUBSCRIBE",
            "params": [format!("{}@depth5@100ms", exchange_symbol(&route.symbol).to_ascii_lowercase())],
            "id": 1
        })
        .to_string(),
    ))
    .await?;
    while let Some(message) = ws.next().await {
        let Message::Text(text) = message? else {
            continue;
        };
        let value: Value = serde_json::from_str(&text)?;
        let Some((bid, bid_qty, ask, ask_qty)) = top_from_bids_asks(
            value.get("b").or_else(|| value.get("bids")),
            value.get("a").or_else(|| value.get("asks")),
        )?
        else {
            continue;
        };
        tx.send(book_event(
            "aster",
            &route.symbol,
            bid,
            bid_qty,
            ask,
            ask_qty,
        ))
        .await
        .ok();
    }
    Ok(())
}

async fn run_mexc_book_ws(route: &RouteConfig, tx: mpsc::Sender<StrategyEvent>) -> Result<()> {
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_websocket("wss://contract.mexc.com/edge"),
    )
    .await
    .context("connect MEXC contract ws timed out")?
    .context("connect MEXC contract ws")?;
    ws.send(Message::Text(
        json!({
            "method": "sub.depth.full",
            "param": {
                "symbol": mexc_contract_symbol(&route.symbol),
                "limit": 20
            }
        })
        .to_string(),
    ))
    .await?;
    while let Some(message) = ws.next().await {
        let Message::Text(text) = message? else {
            continue;
        };
        let value: Value = serde_json::from_str(&text)?;
        let data = value.get("data").unwrap_or(&value);
        let Some((bid, bid_qty, ask, ask_qty)) =
            top_from_bids_asks(data.get("bids"), data.get("asks"))?
        else {
            continue;
        };
        tx.send(book_event(
            "mexc",
            &route.symbol,
            bid,
            bid_qty,
            ask,
            ask_qty,
        ))
        .await
        .ok();
    }
    Ok(())
}

async fn run_kucoinfutures_book_ws(
    route: &RouteConfig,
    tx: mpsc::Sender<StrategyEvent>,
) -> Result<()> {
    let url = kucoinfutures_public_ws_connect_url().await?;
    let (mut ws, _) = tokio::time::timeout(Duration::from_secs(10), connect_websocket(&url))
        .await
        .context("connect KuCoin Futures public ws timed out")?
        .context("connect KuCoin Futures public ws")?;
    ws.send(Message::Text(
        json!({
            "id": "funding-spread-book",
            "type": "subscribe",
            "topic": format!("/contractMarket/level2Depth5:{}", kucoinfutures_symbol(&route.symbol)),
            "privateChannel": false,
            "response": true
        })
        .to_string(),
    ))
    .await?;
    while let Some(message) = ws.next().await {
        let Message::Text(text) = message? else {
            continue;
        };
        let value: Value = serde_json::from_str(&text)?;
        let data = value.get("data").unwrap_or(&value);
        let Some((bid, bid_qty, ask, ask_qty)) =
            top_from_bids_asks(data.get("bids"), data.get("asks"))?
        else {
            continue;
        };
        tx.send(book_event(
            "kucoinfutures",
            &route.symbol,
            bid,
            bid_qty,
            ask,
            ask_qty,
        ))
        .await
        .ok();
    }
    Ok(())
}

async fn run_bybit_book_ws(route: &RouteConfig, tx: mpsc::Sender<StrategyEvent>) -> Result<()> {
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_websocket("wss://stream.bybit.com/v5/public/linear"),
    )
    .await
    .context("connect Bybit public ws timed out")?
    .context("connect Bybit public ws")?;
    ws.send(Message::Text(
        json!({
            "op": "subscribe",
            "args": [format!("orderbook.1.{}", exchange_symbol(&route.symbol))]
        })
        .to_string(),
    ))
    .await?;
    while let Some(message) = ws.next().await {
        let Message::Text(text) = message? else {
            continue;
        };
        let value: Value = serde_json::from_str(&text)?;
        let data = value.get("data").unwrap_or(&value);
        let Some((bid, bid_qty, ask, ask_qty)) = top_from_bids_asks(data.get("b"), data.get("a"))?
        else {
            continue;
        };
        tx.send(book_event(
            "bybit",
            &route.symbol,
            bid,
            bid_qty,
            ask,
            ask_qty,
        ))
        .await
        .ok();
    }
    Ok(())
}

async fn fetch_funding(client: &reqwest::Client, exchange: &str, symbol: &str) -> Result<Value> {
    let exchange = exchange.to_ascii_lowercase();
    match exchange.as_str() {
        "binance" => {
            let symbol_text = exchange_symbol(symbol);
            let premium: Value = client
                .get(format!(
                    "https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol_text}"
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            Ok(json!({
                "event_kind": "funding_rate",
                "exchange": "binance",
                "canonical_symbol": symbol,
                "funding_rate": premium.get("lastFundingRate").cloned().unwrap_or(Value::Null),
                "funding_interval_hours": 4.0,
                "next_funding_time": millis_to_rfc3339(premium.get("nextFundingTime")),
                "mark_price": premium.get("markPrice").cloned().unwrap_or(Value::Null),
                "index_price": premium.get("indexPrice").cloned().unwrap_or(Value::Null),
            }))
        }
        "bitget" => {
            let symbol_text = exchange_symbol(symbol);
            let current: Value = client
                .get(format!("https://api.bitget.com/api/v2/mix/market/current-fund-rate?symbol={symbol_text}&productType=USDT-FUTURES"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let item = current
                .get("data")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .cloned()
                .unwrap_or(Value::Null);
            let ticker: Value = client
                .get(format!(
                    "https://api.bitget.com/api/v2/mix/market/ticker?symbol={symbol_text}&productType=USDT-FUTURES"
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let ticker_item = ticker
                .get("data")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .or_else(|| ticker.get("data"))
                .unwrap_or(&Value::Null)
                .clone();
            Ok(json!({
                "event_kind": "funding_rate",
                "exchange": "bitget",
                "canonical_symbol": symbol,
                "funding_rate": item.get("fundingRate").cloned().unwrap_or(Value::Null),
                "funding_interval_hours": item.get("fundingRateInterval").cloned().unwrap_or(json!(4.0)),
                "next_funding_time": millis_to_rfc3339(item.get("nextUpdate")),
                "mark_price": ticker_item.get("markPrice").or_else(|| ticker_item.get("markPricePr")).cloned().unwrap_or(Value::Null),
                "index_price": ticker_item.get("indexPrice").or_else(|| ticker_item.get("indexPricePr")).cloned().unwrap_or(Value::Null),
            }))
        }
        "aster" => {
            let symbol_text = exchange_symbol(symbol);
            let premium: Value = client
                .get(format!(
                    "https://fapi.asterdex.com/fapi/v3/premiumIndex?symbol={symbol_text}"
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            Ok(json!({
                "event_kind": "funding_rate",
                "exchange": "aster",
                "canonical_symbol": symbol,
                "funding_rate": premium
                    .get("lastFundingRate")
                    .or_else(|| premium.get("fundingRate"))
                    .cloned()
                    .unwrap_or(Value::Null),
                "funding_interval_hours": premium.get("fundingIntervalHours").cloned().unwrap_or(json!(4.0)),
                "next_funding_time": millis_to_rfc3339(premium.get("nextFundingTime")),
                "mark_price": premium.get("markPrice").cloned().unwrap_or(Value::Null),
                "index_price": premium.get("indexPrice").cloned().unwrap_or(Value::Null),
            }))
        }
        "mexc" => {
            let symbol_text = mexc_contract_symbol(symbol);
            let funding: Value = client
                .get(format!(
                    "https://contract.mexc.com/api/v1/contract/funding_rate/{symbol_text}"
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let data = funding.get("data").unwrap_or(&funding);
            Ok(json!({
                "event_kind": "funding_rate",
                "exchange": "mexc",
                "canonical_symbol": symbol,
                "funding_rate": data.get("fundingRate").or_else(|| data.get("rate")).cloned().unwrap_or(Value::Null),
                "funding_interval_hours": data.get("collectCycle").cloned().unwrap_or(json!(8.0)),
                "next_funding_time": millis_to_rfc3339(data.get("nextSettleTime")),
                "mark_price": data.get("fairPrice").or_else(|| data.get("markPrice")).cloned().unwrap_or(Value::Null),
                "index_price": data.get("idxPrice").or_else(|| data.get("indexPrice")).cloned().unwrap_or(Value::Null),
            }))
        }
        "kucoinfutures" | "kucoin_futures" => {
            let symbol_text = kucoinfutures_symbol(symbol);
            let contracts: Value = client
                .get("https://api-futures.kucoin.com/api/v1/contracts/active")
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let history: Value = client
                .get(format!(
                    "https://api-futures.kucoin.com/api/v1/funding-history?symbol={symbol_text}&pageSize=1"
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let contract = contracts
                .get("data")
                .and_then(Value::as_array)
                .and_then(|items| {
                    items.iter().find(|item| {
                        item.get("symbol").and_then(Value::as_str) == Some(symbol_text.as_str())
                    })
                })
                .unwrap_or(&Value::Null);
            let row = history
                .get("data")
                .and_then(|data| data.get("items"))
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .unwrap_or(&Value::Null);
            Ok(json!({
                "event_kind": "funding_rate",
                "exchange": "kucoinfutures",
                "canonical_symbol": symbol,
                "funding_rate": contract
                    .get("fundingFeeRate")
                    .or_else(|| contract.get("fundingRate"))
                    .or_else(|| row.get("fundingRate"))
                    .cloned()
                    .unwrap_or(Value::Null),
                "predicted_funding_rate": contract
                    .get("predictedFundingFeeRate")
                    .or_else(|| contract.get("predictedFundingRate"))
                    .cloned()
                    .unwrap_or(Value::Null),
                "funding_interval_hours": contract.get("fundingRateGranularity").and_then(Value::as_f64).map(|millis| millis / 3_600_000.0).unwrap_or(8.0),
                "next_funding_time": millis_to_rfc3339(
                    contract
                        .get("nextFundingRateDateTime")
                        .or_else(|| contract.get("nextFundingTime"))
                ),
                "mark_price": contract.get("markPrice").cloned().unwrap_or(Value::Null),
                "index_price": contract.get("indexPrice").or_else(|| contract.get("index_price")).cloned().unwrap_or(Value::Null),
                "open_interest": contract.get("openInterest").cloned().unwrap_or(Value::Null),
            }))
        }
        "bybit" => {
            let symbol_text = exchange_symbol(symbol);
            let ticker: Value = client
                .get(format!(
                    "https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol_text}"
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let item = ticker
                .get("result")
                .and_then(|result| result.get("list"))
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .cloned()
                .unwrap_or(Value::Null);
            Ok(json!({
                "event_kind": "funding_rate",
                "exchange": "bybit",
                "canonical_symbol": symbol,
                "funding_rate": item.get("fundingRate").cloned().unwrap_or(Value::Null),
                "funding_interval_hours": 8.0,
                "next_funding_time": millis_to_rfc3339(item.get("nextFundingTime")),
                "mark_price": item.get("markPrice").cloned().unwrap_or(Value::Null),
                "index_price": item.get("indexPrice").cloned().unwrap_or(Value::Null),
                "open_interest": item.get("openInterest").cloned().unwrap_or(Value::Null),
                "turnover_24h": item.get("turnover24h").cloned().unwrap_or(Value::Null),
                "volume_24h": item.get("volume24h").cloned().unwrap_or(Value::Null),
            }))
        }
        "gate" | "gateio" | "gate.io" => {
            let contracts: Vec<Value> = client
                .get("https://api.gateio.ws/api/v4/futures/usdt/contracts")
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let gate_symbol = gateio_symbol(symbol);
            let item = contracts
                .into_iter()
                .find(|item| item.get("name").and_then(Value::as_str) == Some(gate_symbol.as_str()))
                .unwrap_or(Value::Null);
            let interval_secs = item
                .get("funding_interval")
                .and_then(Value::as_f64)
                .unwrap_or(28_800.0);
            Ok(json!({
                "event_kind": "funding_rate",
                "exchange": "gate",
                "canonical_symbol": symbol,
                "funding_rate": item.get("funding_rate").cloned().unwrap_or(Value::Null),
                "funding_interval_hours": interval_secs / 3600.0,
                "next_funding_time": secs_to_rfc3339(item.get("funding_next_apply")),
                "mark_price": item.get("mark_price").cloned().unwrap_or(Value::Null),
                "index_price": item.get("index_price").cloned().unwrap_or(Value::Null),
            }))
        }
        other => bail!("unsupported funding exchange {other}"),
    }
}

fn book_event(
    exchange: &str,
    symbol: &str,
    bid: f64,
    bid_qty: f64,
    ask: f64,
    ask_qty: f64,
) -> StrategyEvent {
    market_event(
        exchange,
        symbol,
        json!({
            "event_kind": "order_book_top",
            "exchange": exchange,
            "canonical_symbol": symbol,
            "bid_price": bid,
            "bid_quantity_base": bid_qty,
            "ask_price": ask,
            "ask_quantity_base": ask_qty,
            "levels": 1,
            "received_at": Utc::now().to_rfc3339()
        }),
    )
}

fn market_event(exchange: &str, symbol: &str, payload: Value) -> StrategyEvent {
    StrategyEvent::MarketData(MarketDataEvent {
        schema_version: 1,
        exchange_id: exchange.to_ascii_lowercase(),
        symbol: symbol.to_string(),
        received_at: Utc::now(),
        payload,
    })
}

fn account_event(exchange: &str, symbol: &str, payload: Value) -> StrategyEvent {
    StrategyEvent::Account(AccountEvent {
        schema_version: 1,
        account_id: exchange.to_ascii_lowercase(),
        received_at: Utc::now(),
        payload: json!({
            "exchange": exchange,
            "canonical_symbol": symbol,
            "payload": payload
        }),
    })
}

fn private_row_to_execution_event(row: Value) -> Option<ExecutionEvent> {
    let client_order_id = row
        .get("client_order_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && *value != "null")
        .map(ToString::to_string);
    let quantity = row
        .get("quantity")
        .or_else(|| row.get("last_quantity"))
        .cloned()
        .unwrap_or(Value::Null);
    let status = row
        .get("order_status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    Some(ExecutionEvent {
        schema_version: 1,
        event_id: format!(
            "private-ws-{}",
            row.get("observed_at")
                .and_then(Value::as_str)
                .unwrap_or("event")
        ),
        client_order_id,
        occurred_at: Utc::now(),
        payload: json!({
            "client_order_id": row.get("client_order_id").cloned().unwrap_or(Value::Null),
            "exchange": row.get("exchange").cloned().unwrap_or(Value::Null),
            "symbol": row.get("symbol").cloned().unwrap_or(Value::Null),
            "status": status,
            "filled": row.get("private_kind").and_then(Value::as_str) == Some("fill"),
            "filled_quantity_base": quantity,
            "fill_price": row.get("price").or_else(|| row.get("last_price")).cloned().unwrap_or(Value::Null),
            "terminal": matches!(status.as_str(), "filled" | "closed" | "cancelled" | "canceled" | "expired" | "rejected"),
            "raw_private_ws": row
        }),
    })
}

fn exchange_symbol(symbol: &str) -> String {
    symbol.replace('/', "").to_ascii_uppercase()
}

fn mexc_contract_symbol(symbol: &str) -> String {
    symbol.replace('/', "_").to_ascii_uppercase()
}

fn gateio_symbol(symbol: &str) -> String {
    symbol.replace('/', "_").to_ascii_uppercase()
}

fn kucoinfutures_symbol(symbol: &str) -> String {
    let compact = exchange_symbol(symbol);
    let base = compact.strip_suffix("USDT").unwrap_or(compact.as_str());
    let kucoin_base = if base == "BTC" { "XBT" } else { base };
    format!("{kucoin_base}USDTM")
}

fn top_from_bids_asks(
    bids: Option<&Value>,
    asks: Option<&Value>,
) -> Result<Option<(f64, f64, f64, f64)>> {
    let Some(bid_row) = bids.and_then(Value::as_array).and_then(|rows| rows.first()) else {
        return Ok(None);
    };
    let Some(ask_row) = asks.and_then(Value::as_array).and_then(|rows| rows.first()) else {
        return Ok(None);
    };
    Ok(Some((
        array_f64(bid_row, 0)?,
        array_f64(bid_row, 1)?,
        array_f64(ask_row, 0)?,
        array_f64(ask_row, 1)?,
    )))
}

fn array_f64(value: &Value, index: usize) -> Result<f64> {
    value
        .as_array()
        .and_then(|items| items.get(index))
        .and_then(|value| {
            value
                .as_f64()
                .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        })
        .with_context(|| format!("missing numeric array index {index} in {value}"))
}

fn millis_to_rfc3339(value: Option<&Value>) -> Option<String> {
    let millis = value.and_then(Value::as_i64).or_else(|| {
        value
            .and_then(Value::as_str)
            .and_then(|text| text.parse().ok())
    })?;
    Utc.timestamp_millis_opt(millis)
        .single()
        .map(|time| time.to_rfc3339())
}

fn secs_to_rfc3339(value: Option<&Value>) -> Option<String> {
    let secs = value.and_then(Value::as_i64).or_else(|| {
        value
            .and_then(Value::as_str)
            .and_then(|text| text.parse().ok())
    })?;
    Utc.timestamp_opt(secs, 0)
        .single()
        .map(|time| time.to_rfc3339())
}

async fn kucoinfutures_public_ws_connect_url() -> Result<String> {
    let client = reqwest_client_builder_with_ws_proxy()
        .build()
        .context("build KuCoin Futures public websocket token client")?;
    let response = client
        .post("https://api-futures.kucoin.com/api/v1/bullet-public")
        .send()
        .await
        .context("request KuCoin Futures public websocket token")?;
    let status = response.status();
    let value = response
        .json::<Value>()
        .await
        .context("decode KuCoin Futures public websocket token response")?;
    if !status.is_success() {
        bail!("KuCoin Futures public websocket token request failed: status={status} body={value}");
    }
    let data = value.get("data").unwrap_or(&value);
    let token = data
        .get("token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .context("KuCoin Futures public websocket token response missing data.token")?;
    let endpoint = data
        .get("instanceServers")
        .and_then(Value::as_array)
        .and_then(|servers| servers.first())
        .and_then(|server| server.get("endpoint"))
        .and_then(Value::as_str)
        .filter(|endpoint| !endpoint.is_empty())
        .unwrap_or("wss://ws-api-futures.kucoin.com/endpoint");
    let separator = if endpoint.contains('?') { '&' } else { '?' };
    Ok(format!(
        "{endpoint}{separator}token={token}&connectId=funding-spread-{}",
        Utc::now().timestamp_millis()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn funding_spread_ws_local_symbols_should_match_contract_venues() {
        assert_eq!(mexc_contract_symbol("BTC/USDT"), "BTC_USDT");
        assert_eq!(kucoinfutures_symbol("BTC/USDT"), "XBTUSDTM");
        assert_eq!(kucoinfutures_symbol("HUSDT/USDT"), "HUSDTUSDTM");
        assert_eq!(exchange_symbol("HUSDT/USDT"), "HUSDTUSDT");
    }

    #[test]
    fn funding_spread_ws_local_book_helper_should_parse_string_and_numeric_levels() {
        let book = json!({
            "bids": [["0.1485", "1400"], [0.1484, 1000]],
            "asks": [[0.1487, 1200, 178.44]]
        });
        let top = top_from_bids_asks(book.get("bids"), book.get("asks"))
            .expect("parse")
            .expect("top");
        assert_eq!(top, (0.1485, 1400.0, 0.1487, 1200.0));
    }
}

type LocalGatewayClient = InProcessGatewayClient<AdapterBackedGateway>;
type LocalExecutionRouter = ExecutionRouter<LocalGatewayClient>;

#[derive(Clone)]
struct LocalExecutionClient {
    live_orders_enabled: bool,
    ack_recorded_orders: bool,
    orders: Arc<Mutex<Vec<Value>>>,
    router: Option<Arc<LocalExecutionRouter>>,
}

impl LocalExecutionClient {
    fn new(
        live_orders_enabled: bool,
        ack_recorded_orders: bool,
        exchanges: Vec<String>,
    ) -> Result<Self> {
        let router = if live_orders_enabled {
            Some(Arc::new(ExecutionRouter::new(
                ExecutionRouterConfig::live(),
                InProcessGatewayClient::new(Arc::new(build_gateway(exchanges)?)),
            )))
        } else {
            None
        };
        Ok(Self {
            live_orders_enabled,
            ack_recorded_orders,
            orders: Arc::new(Mutex::new(Vec::new())),
            router,
        })
    }

    fn orders(&self) -> Vec<Value> {
        self.orders
            .lock()
            .map(|orders| orders.clone())
            .unwrap_or_default()
    }

    fn map_order_command(&self, command: ExecutionOrderCommand) -> SdkResult<RouterOrderCommand> {
        let exchange = gateway_exchange_id(&command.exchange_id);
        let exchange_id = GatewayExchangeId::new(exchange.clone()).map_sdk_err()?;
        let canonical_symbol = CanonicalSymbol::parse(&command.symbol).map_sdk_err()?;
        let exchange_symbol = ExchangeSymbol::new(
            exchange_id.clone(),
            GatewayMarketType::Perpetual,
            exchange_symbol_text(&exchange, &canonical_symbol),
        )
        .map_sdk_err()?;
        let identity = MutationIdentity {
            tenant_id: TenantId::new(command.tenant_id).map_sdk_err()?,
            account_id: AccountId::new(account_for_exchange(&exchange, &command.account_id))
                .map_sdk_err()?,
            strategy_id: StrategyId::new(command.strategy_id).map_sdk_err()?,
            run_id: RunId::new(command.run_id).map_sdk_err()?,
            idempotency_key: command.idempotency_key,
            risk_profile_id: command.risk_profile_id,
            requested_at: command.requested_at,
        };
        let post_only = command
            .metadata
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let order_type = if post_only {
            GatewayOrderType::PostOnly
        } else {
            order_type(command.order_type)?
        };
        let time_in_force = command
            .time_in_force
            .map(time_in_force)
            .transpose()?
            .unwrap_or_else(|| default_time_in_force(order_type));
        let quantity = parse_positive_f64("quantity", &command.quantity)?;
        let price = command
            .price
            .as_deref()
            .map(|value| parse_positive_f64("price", value))
            .transpose()?;
        let exchange_client_order_id =
            exchange_client_order_id(&exchange, &command.client_order_id);
        let mut order = RouterOrderCommand::new(
            identity,
            command.client_order_id.clone(),
            exchange_id,
            GatewayMarketType::Perpetual,
            canonical_symbol,
            exchange_symbol,
            exchange_client_order_id,
            order_side(command.side),
            position_side(&command.metadata),
            order_type,
            time_in_force,
            quantity,
            price,
        );
        order.reduce_only = command.reduce_only;
        order.post_only = post_only;
        order.max_slippage_bps = Some(30);
        Ok(order)
    }

    fn map_cancel_command(&self, command: ExecutionCancelCommand) -> SdkResult<CancelCommand> {
        let exchange = gateway_exchange_id(&command.exchange_id);
        let exchange_id = GatewayExchangeId::new(exchange.clone()).map_sdk_err()?;
        let canonical_symbol = CanonicalSymbol::parse(&command.symbol).map_sdk_err()?;
        let exchange_symbol = ExchangeSymbol::new(
            exchange_id.clone(),
            GatewayMarketType::Perpetual,
            exchange_symbol_text(&exchange, &canonical_symbol),
        )
        .map_sdk_err()?;
        let identity = MutationIdentity {
            tenant_id: TenantId::new(command.tenant_id).map_sdk_err()?,
            account_id: AccountId::new(account_for_exchange(&exchange, &command.account_id))
                .map_sdk_err()?,
            strategy_id: StrategyId::new(command.strategy_id).map_sdk_err()?,
            run_id: RunId::new(command.run_id).map_sdk_err()?,
            idempotency_key: command.idempotency_key,
            risk_profile_id: command.risk_profile_id,
            requested_at: command.requested_at,
        };
        let cancellation_ids = match (
            command.client_order_id.clone(),
            command.execution_order_id.clone(),
        ) {
            (Some(client_order_id), _) => CancellationIds::by_client_order_id(client_order_id),
            (None, Some(exchange_order_id)) => {
                CancellationIds::by_exchange_order_id(exchange_order_id)
            }
            (None, None) => {
                return Err(StrategySdkError::InvalidCommand(
                    "cancel command requires client_order_id or execution_order_id".to_string(),
                ))
            }
        };
        let command_id = command
            .client_order_id
            .clone()
            .or(command.execution_order_id.clone())
            .map(|id| format!("cancel:{id}"))
            .unwrap_or_else(|| "cancel:unknown".to_string());
        Ok(CancelCommand::new(
            identity,
            command_id,
            exchange_id,
            GatewayMarketType::Perpetual,
            canonical_symbol,
            exchange_symbol,
            cancellation_ids,
        ))
    }
}

#[async_trait]
impl StrategyExecutionClient for LocalExecutionClient {
    async fn submit_order(&self, command: ExecutionOrderCommand) -> SdkResult<ExecutionOrderAck> {
        let schema_version = command.schema_version;
        let client_order_id = command.client_order_id.clone();
        let accepted = self.live_orders_enabled || self.ack_recorded_orders;
        if let Ok(mut orders) = self.orders.lock() {
            orders.push(json!({
                "accepted": accepted,
                "dry_run": !self.live_orders_enabled,
                "client_order_id": client_order_id,
                "exchange": command.exchange_id,
                "symbol": command.symbol,
                "side": command.side,
                "quantity": command.quantity,
                "price": command.price,
                "time_in_force": command.time_in_force,
                "reduce_only": command.reduce_only,
                "metadata": command.metadata,
                "recorded_at": Utc::now().to_rfc3339()
            }));
        }
        if let Some(router) = &self.router {
            let ack = router
                .place_order(self.map_order_command(command)?)
                .await
                .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))?;
            return Ok(ExecutionOrderAck {
                schema_version,
                accepted: ack.accepted,
                client_order_id,
                execution_order_id: ack.exchange_order_id,
                reason: ack.message,
                received_at: ack.acknowledged_at,
            });
        }
        Ok(ExecutionOrderAck {
            schema_version,
            accepted,
            client_order_id,
            execution_order_id: None,
            reason: Some(if accepted {
                "local ws runner dry-run: order recorded and acknowledged".to_string()
            } else {
                "local ws runner dry-run: order recorded but not submitted".to_string()
            }),
            received_at: Utc::now(),
        })
    }

    async fn cancel_order(&self, command: ExecutionCancelCommand) -> SdkResult<ExecutionCancelAck> {
        let schema_version = command.schema_version;
        let client_order_id = command.client_order_id.clone();
        let execution_order_id = command.execution_order_id.clone();
        if let Some(router) = &self.router {
            let ack = router
                .cancel_order(self.map_cancel_command(command)?)
                .await
                .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))?;
            return Ok(ExecutionCancelAck {
                schema_version,
                accepted: ack.accepted,
                client_order_id,
                execution_order_id,
                reason: ack.message,
                received_at: ack.acknowledged_at,
            });
        }
        Ok(ExecutionCancelAck {
            schema_version,
            accepted: false,
            client_order_id,
            execution_order_id,
            reason: Some("local ws runner does not route cancels in dry-run".to_string()),
            received_at: Utc::now(),
        })
    }

    async fn submit_raw_intent(&self, intent: ExecutionIntent) -> SdkResult<ExecutionIntentAck> {
        Ok(ExecutionIntentAck {
            schema_version: intent.schema_version,
            accepted: false,
            intent_kind: intent.intent_kind,
            reason: Some("local ws runner does not route raw intents".to_string()),
            received_at: Utc::now(),
            payload: json!({}),
        })
    }
}

fn build_gateway(exchanges: Vec<String>) -> Result<AdapterBackedGateway> {
    let gateway = AdapterBackedGateway::new("funding-spread-ws-local");
    for exchange in exchanges {
        match gateway_exchange_id(&exchange).as_str() {
            "binance" => gateway
                .register_binance_adapter(local_binance_gateway_config())
                .context("register Binance gateway adapter")?,
            "bitget" => gateway
                .register_bitget_adapter(BitgetGatewayConfig::default())
                .context("register Bitget gateway adapter")?,
            other => bail!("live execution currently supports binance and bitget, got {other}"),
        }
    }
    Ok(gateway)
}

fn exchange_client_order_id(exchange: &str, original: &str) -> String {
    let mut hasher = DefaultHasher::new();
    exchange.hash(&mut hasher);
    original.hash(&mut hasher);
    let hash = hasher.finish();
    let exchange_prefix = exchange
        .chars()
        .find(|ch| ch.is_ascii_alphanumeric())
        .unwrap_or('x');
    format!(
        "fs{}{}{:08x}",
        exchange_prefix,
        Utc::now().timestamp_millis().rem_euclid(10_000_000_000),
        hash & 0xffff_ffff
    )
}

fn local_binance_gateway_config() -> BinanceGatewayConfig {
    let mut config = BinanceGatewayConfig::default();
    config.api_key = first_env(&[
        "RUSTCTA_BINANCE_API_KEY",
        "BINANCE_0_API_KEY",
        "BINANCE_USDM_API_KEY",
        "BINANCE_API_KEY",
    ])
    .or(config.api_key);
    config.api_secret = first_env(&[
        "RUSTCTA_BINANCE_API_SECRET",
        "BINANCE_0_API_SECRET",
        "BINANCE_0_SECRET_KEY",
        "BINANCE_USDM_API_SECRET",
        "BINANCE_API_SECRET",
    ])
    .or(config.api_secret);
    if let Some(recv_window_ms) = first_env(&[
        "RUSTCTA_BINANCE_RECV_WINDOW_MS",
        "BINANCE_0_RECV_WINDOW_MS",
        "BINANCE_USDM_RECV_WINDOW_MS",
        "BINANCE_SPOT_RECV_WINDOW_MS",
    ])
    .and_then(|value| value.parse().ok())
    {
        config.recv_window_ms = recv_window_ms;
    }
    config
}

fn first_env(keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn gateway_exchange_id(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gateio" | "gate.io" | "gate_io" => "gateio".to_string(),
        "binance" => "binance".to_string(),
        "bitget" => "bitget".to_string(),
        other => other.to_string(),
    }
}

fn account_for_exchange(exchange: &str, fallback: &str) -> String {
    match gateway_exchange_id(exchange).as_str() {
        "binance" => std::env::var("RUSTCTA_BINANCE_ACCOUNT_ID")
            .or_else(|_| std::env::var("BINANCE_ACCOUNT_ID"))
            .unwrap_or_else(|_| fallback.to_string()),
        "bitget" => std::env::var("RUSTCTA_BITGET_ACCOUNT_ID")
            .or_else(|_| std::env::var("BITGET_ACCOUNT_ID"))
            .unwrap_or_else(|_| fallback.to_string()),
        _ => fallback.to_string(),
    }
}

fn exchange_symbol_text(exchange: &str, canonical_symbol: &CanonicalSymbol) -> String {
    let base = canonical_symbol.base_asset();
    let quote = canonical_symbol.quote_asset();
    match gateway_exchange_id(exchange).as_str() {
        "gateio" => format!("{base}_{quote}"),
        _ => format!("{base}{quote}"),
    }
}

fn order_side(side: rustcta_strategy_sdk::OrderSide) -> GatewayOrderSide {
    match side {
        rustcta_strategy_sdk::OrderSide::Buy => GatewayOrderSide::Buy,
        rustcta_strategy_sdk::OrderSide::Sell => GatewayOrderSide::Sell,
    }
}

fn order_type(order_type: rustcta_strategy_sdk::OrderType) -> SdkResult<GatewayOrderType> {
    Ok(match order_type {
        rustcta_strategy_sdk::OrderType::Market => GatewayOrderType::Market,
        rustcta_strategy_sdk::OrderType::Limit => GatewayOrderType::Limit,
        rustcta_strategy_sdk::OrderType::PostOnly => GatewayOrderType::PostOnly,
        rustcta_strategy_sdk::OrderType::ImmediateOrCancel => GatewayOrderType::IOC,
        rustcta_strategy_sdk::OrderType::Custom(value) if value.eq_ignore_ascii_case("ioc") => {
            GatewayOrderType::IOC
        }
        rustcta_strategy_sdk::OrderType::Custom(value) => {
            return Err(StrategySdkError::InvalidCommand(format!(
                "unsupported custom order type: {value}"
            )))
        }
    })
}

fn time_in_force(
    time_in_force: rustcta_strategy_sdk::TimeInForce,
) -> SdkResult<GatewayTimeInForce> {
    Ok(match time_in_force {
        rustcta_strategy_sdk::TimeInForce::GoodTilCanceled => GatewayTimeInForce::GTC,
        rustcta_strategy_sdk::TimeInForce::ImmediateOrCancel => GatewayTimeInForce::IOC,
        rustcta_strategy_sdk::TimeInForce::FillOrKill => GatewayTimeInForce::FOK,
        rustcta_strategy_sdk::TimeInForce::PostOnly => GatewayTimeInForce::GTX,
        rustcta_strategy_sdk::TimeInForce::Custom(value) if value.eq_ignore_ascii_case("ioc") => {
            GatewayTimeInForce::IOC
        }
        rustcta_strategy_sdk::TimeInForce::Custom(value) => {
            return Err(StrategySdkError::InvalidCommand(format!(
                "unsupported custom time-in-force: {value}"
            )))
        }
    })
}

fn default_time_in_force(order_type: GatewayOrderType) -> GatewayTimeInForce {
    match order_type {
        GatewayOrderType::IOC => GatewayTimeInForce::IOC,
        GatewayOrderType::FOK => GatewayTimeInForce::FOK,
        GatewayOrderType::PostOnly => GatewayTimeInForce::GTX,
        _ => GatewayTimeInForce::GTC,
    }
}

fn position_side(metadata: &std::collections::BTreeMap<String, Value>) -> GatewayPositionSide {
    match metadata
        .get("position_side")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "long" => GatewayPositionSide::Long,
        "short" => GatewayPositionSide::Short,
        _ => GatewayPositionSide::Net,
    }
}

fn parse_positive_f64(field: &str, value: &str) -> SdkResult<f64> {
    let parsed = value.parse::<f64>().map_err(|error| {
        StrategySdkError::InvalidCommand(format!("{field} must be a number: {error}"))
    })?;
    if parsed.is_finite() && parsed > 0.0 {
        Ok(parsed)
    } else {
        Err(StrategySdkError::InvalidCommand(format!(
            "{field} must be positive"
        )))
    }
}

trait MapSdkErr<T> {
    fn map_sdk_err(self) -> SdkResult<T>;
}

impl<T, E> MapSdkErr<T> for std::result::Result<T, E>
where
    E: std::fmt::Display,
{
    fn map_sdk_err(self) -> SdkResult<T> {
        self.map_err(|error| StrategySdkError::InvalidCommand(error.to_string()))
    }
}

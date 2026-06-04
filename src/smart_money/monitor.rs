use crate::smart_money::{
    parse_user_fills, simulate_taker_market_order, BookLevel, Direction, HyperliquidWalletClient,
    OrderBookSnapshot, SmartMoneyMonitorConfig, TakerExecutionConfig, TrackedWalletConfig,
    WalletId, WalletTrade,
};
use anyhow::{Context, Result};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use rand::{seq::SliceRandom, Rng};
use reqwest::Client;
use rusqlite::{params, Connection, OptionalExtension};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio::{net::TcpListener, time::sleep};
use uuid::Uuid;

#[derive(Clone)]
pub struct SmartMoneyMonitorState {
    db: Arc<Mutex<MonitorDb>>,
    client: Client,
    hyperliquid: HyperliquidWalletClient,
    config: SmartMoneyMonitorConfig,
}

impl SmartMoneyMonitorState {
    pub fn new(config: SmartMoneyMonitorConfig) -> Result<Self> {
        let db = MonitorDb::open(&config.database_path)?;
        db.seed_wallets(&configured_or_demo_wallets(&config))?;
        Ok(Self {
            db: Arc::new(Mutex::new(db)),
            client: Client::new(),
            hyperliquid: HyperliquidWalletClient::new(&config.hyperliquid_api_base_url),
            config,
        })
    }

    pub fn bind_addr(&self) -> Result<SocketAddr> {
        self.config
            .bind_addr
            .parse::<SocketAddr>()
            .with_context(|| format!("invalid monitor bind_addr {}", self.config.bind_addr))
    }
}

pub async fn serve_smart_money_monitor(config: SmartMoneyMonitorConfig) -> Result<()> {
    let state = SmartMoneyMonitorState::new(config)?;
    let bind_addr = state.bind_addr()?;
    let simulation_state = state.clone();
    tokio::spawn(async move {
        run_monitor_simulation_loop(simulation_state).await;
    });

    let app = smart_money_monitor_router(state);
    let listener = TcpListener::bind(bind_addr).await?;
    log::info!("smart-money monitor listening on http://{}", bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}

pub fn smart_money_monitor_router(state: SmartMoneyMonitorState) -> Router {
    Router::new()
        .route("/", get(monitor_index))
        .route("/api/summary", get(api_summary))
        .route("/api/wallets", get(api_wallets))
        .route("/api/wallet-trades", get(api_wallet_trades))
        .route("/api/orders", get(api_orders))
        .route("/api/scores", get(api_scores))
        .route("/api/orderbooks", get(api_orderbooks))
        .route("/api/simulate", post(api_simulate_once))
        .with_state(state)
}

pub async fn run_monitor_simulation_loop(state: SmartMoneyMonitorState) {
    let interval = state.config.simulation_interval_secs.max(1);
    loop {
        if let Err(error) = simulate_monitor_tick(&state).await {
            log::warn!("smart-money monitor tick failed: {error:#}");
        }
        sleep(std::time::Duration::from_secs(interval)).await;
    }
}

pub async fn simulate_monitor_tick(state: &SmartMoneyMonitorState) -> Result<usize> {
    if state.config.synthetic_activity_enabled {
        return simulate_synthetic_activity_tick(state).await;
    }
    poll_real_wallet_activity_tick(state).await
}

async fn poll_real_wallet_activity_tick(state: &SmartMoneyMonitorState) -> Result<usize> {
    let wallets = state.db.lock().expect("monitor db lock").wallets()?;
    let mut copied_orders = 0usize;
    for wallet in wallets {
        let start_time_millis = state
            .db
            .lock()
            .expect("monitor db lock")
            .fill_checkpoint_millis(&wallet.address, state.config.wallet_fill_lookback_secs)?;
        let end_time_millis = Utc::now().timestamp_millis();
        let raw = match state
            .hyperliquid
            .fetch_user_fills_by_time(&wallet.address, start_time_millis, Some(end_time_millis))
            .await
        {
            Ok(raw) => raw,
            Err(err) => {
                state
                    .db
                    .lock()
                    .expect("monitor db lock")
                    .update_fill_checkpoint(
                        &wallet.address,
                        (end_time_millis - 10_000).max(start_time_millis),
                        Some(err.to_string().as_str()),
                    )?;
                continue;
            }
        };
        let trades = match parse_user_fills(WalletId(Uuid::new_v4()), &raw) {
            Ok(trades) => trades,
            Err(err) => {
                state
                    .db
                    .lock()
                    .expect("monitor db lock")
                    .update_fill_checkpoint(
                        &wallet.address,
                        (end_time_millis - 10_000).max(start_time_millis),
                        Some(err.to_string().as_str()),
                    )?;
                continue;
            }
        };
        let mut next_checkpoint = (end_time_millis - 10_000).max(start_time_millis);
        for trade in trades
            .into_iter()
            .filter(|trade| trade.executed_at.timestamp_millis() >= start_time_millis)
        {
            next_checkpoint = next_checkpoint.max(trade.executed_at.timestamp_millis() + 1);
            let copy_signal = evaluate_copy_signal(&wallet, &trade, &state.config);
            let observed_id = state
                .db
                .lock()
                .expect("monitor db lock")
                .insert_observed_wallet_trade(&wallet.address, &trade, &copy_signal)?;
            if let Some(observed_id) = observed_id {
                if copy_signal.should_copy {
                    match copy_observed_trade(state, &wallet, observed_id, &trade, &copy_signal)
                        .await
                    {
                        Ok(()) => copied_orders += 1,
                        Err(err) => {
                            state
                                .db
                                .lock()
                                .expect("monitor db lock")
                                .mark_observed_trade(
                                    observed_id,
                                    "execution_error",
                                    err.to_string().as_str(),
                                    None,
                                )?;
                        }
                    }
                }
            }
        }
        state
            .db
            .lock()
            .expect("monitor db lock")
            .update_fill_checkpoint(&wallet.address, next_checkpoint, None)?;
    }
    Ok(copied_orders)
}

async fn copy_observed_trade(
    state: &SmartMoneyMonitorState,
    wallet: &WalletMonitorRow,
    observed_trade_id: i64,
    trade: &WalletTrade,
    copy_signal: &CopySignalDecision,
) -> Result<()> {
    let book = fetch_binance_top5_book(
        &state.client,
        &state.config.binance_rest_base_url,
        &trade.symbol,
    )
    .await
    .with_context(|| format!("fetch Binance REST top-5 depth for {}", trade.symbol))?;
    let book_id = state
        .db
        .lock()
        .expect("monitor db lock")
        .insert_orderbook_snapshot(&book, "binance_rest", None)?;

    let exposure = state
        .db
        .lock()
        .expect("monitor db lock")
        .simulated_position_exposure(
            &trade.symbol,
            state.config.simulation_position_reset_at.as_deref(),
        )?;
    let sizing = capped_simulation_notional(&state.config, &exposure, trade.direction);
    if !sizing.should_simulate {
        state
            .db
            .lock()
            .expect("monitor db lock")
            .mark_observed_trade(
                observed_trade_id,
                "simulation_skipped",
                sizing.reason.as_str(),
                None,
            )?;
        return Ok(());
    }

    let fill = simulate_taker_market_order(
        &book,
        trade.direction,
        sizing.notional_usdt,
        &TakerExecutionConfig {
            taker_fee_rate: Decimal::new(4, 4),
            max_depth_levels: 5,
            max_participation_notional_usdt: None,
        },
    )
    .map_err(|err| anyhow::anyhow!("execution simulation failed: {err:?}"))?;
    let order_id = state
        .db
        .lock()
        .expect("monitor db lock")
        .insert_simulated_order(
            wallet,
            Some(observed_trade_id),
            book_id,
            &fill,
            "binance_rest",
            copy_signal.signal_strength,
        )?;
    state
        .db
        .lock()
        .expect("monitor db lock")
        .mark_observed_trade(
            observed_trade_id,
            "simulated",
            sizing.reason.as_str(),
            Some(order_id),
        )?;
    Ok(())
}

async fn simulate_synthetic_activity_tick(state: &SmartMoneyMonitorState) -> Result<usize> {
    let wallets = state.db.lock().expect("monitor db lock").wallets()?;
    let symbols = if state.config.symbols.is_empty() {
        vec!["BTCUSDT".to_string()]
    } else {
        state.config.symbols.clone()
    };
    let mut candidates = Vec::new();
    for wallet in wallets {
        let should_simulate = {
            let db = state.db.lock().expect("monitor db lock");
            let last_order_at = db.last_order_at(&wallet.address)?;
            let trades_last_hour = db.order_count_since(&wallet.address, Duration::hours(1))?;
            should_simulate_wallet_activity(
                &wallet,
                state.config.simulation_interval_secs.max(1),
                last_order_at,
                trades_last_hour,
            )
        };
        if should_simulate {
            candidates.push(wallet);
        }
    }
    let candidates = {
        let mut rng = rand::thread_rng();
        candidates.shuffle(&mut rng);
        candidates.truncate(MAX_SIMULATED_ORDERS_PER_TICK);
        candidates
    };
    let mut simulated = 0usize;
    for wallet in candidates {
        let symbol = symbols
            .choose(&mut rand::thread_rng())
            .cloned()
            .unwrap_or_else(|| "BTCUSDT".to_string());
        if simulate_wallet_order(state, &wallet, &symbol).await? {
            simulated += 1;
        }
    }
    Ok(simulated)
}

const MAX_SIMULATED_ORDERS_PER_TICK: usize = 3;

fn should_simulate_wallet_activity(
    wallet: &WalletMonitorRow,
    interval_secs: u64,
    last_order_at: Option<DateTime<Utc>>,
    trades_last_hour: i64,
) -> bool {
    let profile = activity_profile(wallet);
    if trades_last_hour >= profile.max_trades_per_hour {
        return false;
    }
    if let Some(last_order_at) = last_order_at {
        let elapsed = Utc::now().signed_duration_since(last_order_at);
        if elapsed < Duration::seconds(profile.cooldown_secs) {
            return false;
        }
    }
    let probability = if wallet.trade_count == 0 {
        profile.cold_start_probability
    } else {
        let interval_hours = interval_secs as f64 / 3600.0;
        let grade_multiplier = match wallet.grade.as_str() {
            "A+" => 1.20,
            "A" | "A-" => 1.10,
            "B+" => 1.00,
            "B" => 0.85,
            "B-" => 0.65,
            _ => 0.80,
        };
        let rank_multiplier = match wallet.source_rank {
            Some(rank) if rank <= 10 => 1.15,
            Some(rank) if rank <= 30 => 1.00,
            Some(_) => 0.80,
            None => 0.90,
        };
        let score_multiplier = (0.65 + wallet.score.clamp(0.0, 100.0) / 180.0).clamp(0.65, 1.20);
        (profile.trades_per_hour
            * interval_hours
            * grade_multiplier
            * rank_multiplier
            * score_multiplier)
            .clamp(0.0, 0.35)
    };
    rand::thread_rng().gen_bool(probability)
}

#[derive(Debug, Clone, Copy)]
struct WalletActivityProfile {
    trades_per_hour: f64,
    cooldown_secs: i64,
    max_trades_per_hour: i64,
    cold_start_probability: f64,
}

fn activity_profile(wallet: &WalletMonitorRow) -> WalletActivityProfile {
    match wallet.style.to_ascii_lowercase().as_str() {
        "momentum" => WalletActivityProfile {
            trades_per_hour: 2.0,
            cooldown_secs: 5 * 60,
            max_trades_per_hour: 4,
            cold_start_probability: 0.25,
        },
        "trend" => WalletActivityProfile {
            trades_per_hour: 0.5,
            cooldown_secs: 20 * 60,
            max_trades_per_hour: 2,
            cold_start_probability: 0.15,
        },
        "swing" => WalletActivityProfile {
            trades_per_hour: 0.8,
            cooldown_secs: 12 * 60,
            max_trades_per_hour: 3,
            cold_start_probability: 0.18,
        },
        "mixed" => WalletActivityProfile {
            trades_per_hour: 1.0,
            cooldown_secs: 10 * 60,
            max_trades_per_hour: 3,
            cold_start_probability: 0.20,
        },
        _ => WalletActivityProfile {
            trades_per_hour: 0.7,
            cooldown_secs: 15 * 60,
            max_trades_per_hour: 2,
            cold_start_probability: 0.16,
        },
    }
}

#[derive(Debug, Clone)]
struct CopySignalDecision {
    signal_strength: f64,
    should_copy: bool,
    decision: &'static str,
    reason: String,
}

fn evaluate_copy_signal(
    wallet: &WalletMonitorRow,
    trade: &WalletTrade,
    config: &SmartMoneyMonitorConfig,
) -> CopySignalDecision {
    let signal_strength = copy_signal_strength(wallet, trade);
    let age_secs = Utc::now()
        .signed_duration_since(trade.executed_at)
        .num_seconds();
    let configured_symbols = config
        .symbols
        .iter()
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let symbol_supported =
        configured_symbols.is_empty() || configured_symbols.contains(&trade.symbol);
    let min_notional = dec_to_f64(config.min_wallet_trade_notional_usdt);
    let threshold = dec_to_f64(config.copy_signal_threshold);

    if trade.direction == Direction::Flat {
        return CopySignalDecision {
            signal_strength,
            should_copy: false,
            decision: "ignored",
            reason: "flat_or_unknown_direction".to_string(),
        };
    }
    if !symbol_supported {
        return CopySignalDecision {
            signal_strength,
            should_copy: false,
            decision: "ignored",
            reason: "unsupported_symbol".to_string(),
        };
    }
    if dec_to_f64(trade.notional_usdt) < min_notional {
        return CopySignalDecision {
            signal_strength,
            should_copy: false,
            decision: "ignored",
            reason: "below_min_wallet_trade_notional".to_string(),
        };
    }
    if age_secs > config.copy_max_fill_age_secs as i64 {
        return CopySignalDecision {
            signal_strength,
            should_copy: false,
            decision: "observed_only",
            reason: "stale_fill_not_copied".to_string(),
        };
    }
    if signal_strength < threshold {
        return CopySignalDecision {
            signal_strength,
            should_copy: false,
            decision: "observed_only",
            reason: "signal_below_copy_threshold".to_string(),
        };
    }
    CopySignalDecision {
        signal_strength,
        should_copy: true,
        decision: "copy_candidate",
        reason: "fresh_fill_passed_signal_gate".to_string(),
    }
}

fn copy_signal_strength(wallet: &WalletMonitorRow, trade: &WalletTrade) -> f64 {
    let score_component = (wallet.score / 100.0).clamp(0.0, 1.0);
    let seed_component = wallet.seed_weight.clamp(0.0, 1.0);
    let notional = dec_to_f64(trade.notional_usdt).max(0.0);
    let notional_component = ((notional / 1_000.0) + 1.0).ln().min(2.0) / 2.0;
    let age_secs = Utc::now()
        .signed_duration_since(trade.executed_at)
        .num_seconds()
        .max(0) as f64;
    let recency_component = (1.0 - age_secs / 300.0).clamp(0.0, 1.0);
    (0.55 * score_component
        + 0.20 * seed_component
        + 0.15 * notional_component
        + 0.10 * recency_component)
        .clamp(0.0, 1.0)
}

fn wallet_trade_dedupe_key(address: &str, trade: &WalletTrade) -> String {
    if let Some(external_id) = &trade.external_id {
        if !external_id.is_empty() {
            return format!("{}:{external_id}", address.to_ascii_lowercase());
        }
    }
    format!(
        "{}:{}:{}:{}:{}:{}",
        address.to_ascii_lowercase(),
        trade.symbol,
        trade.executed_at.timestamp_millis(),
        side_string(trade.direction),
        trade.price,
        trade.quantity
    )
}

fn side_string(direction: Direction) -> &'static str {
    match direction {
        Direction::Long => "BUY",
        Direction::Short => "SELL",
        Direction::Flat => "FLAT",
    }
}

async fn simulate_wallet_order(
    state: &SmartMoneyMonitorState,
    wallet: &WalletMonitorRow,
    symbol: &str,
) -> Result<bool> {
    let direction = if rand::thread_rng().gen_bool(direction_probability(wallet.score)) {
        Direction::Long
    } else {
        Direction::Short
    };
    let book_fetch =
        fetch_binance_top5_book(&state.client, &state.config.binance_rest_base_url, symbol).await;
    let (book, source, error) = match book_fetch {
        Ok(book) => (book, "binance_rest".to_string(), None),
        Err(err) => (
            fallback_book(symbol),
            "fallback".to_string(),
            Some(err.to_string()),
        ),
    };
    let book_id = state
        .db
        .lock()
        .expect("monitor db lock")
        .insert_orderbook_snapshot(&book, &source, error.as_deref())?;

    let exposure = state
        .db
        .lock()
        .expect("monitor db lock")
        .simulated_position_exposure(
            symbol,
            state.config.simulation_position_reset_at.as_deref(),
        )?;
    let sizing = capped_simulation_notional(&state.config, &exposure, direction);
    if !sizing.should_simulate {
        return Ok(false);
    }

    let fill = simulate_taker_market_order(
        &book,
        direction,
        sizing.notional_usdt,
        &TakerExecutionConfig {
            taker_fee_rate: Decimal::new(4, 4),
            max_depth_levels: 5,
            max_participation_notional_usdt: None,
        },
    )
    .map_err(|err| anyhow::anyhow!("execution simulation failed: {err:?}"))?;
    state
        .db
        .lock()
        .expect("monitor db lock")
        .insert_simulated_order(wallet, None, book_id, &fill, &source, 0.0)?;
    Ok(true)
}

#[derive(Debug, Clone, Copy)]
struct SimulatedPositionExposure {
    symbol_net_notional: f64,
    total_abs_notional: f64,
}

#[derive(Debug, Clone)]
struct SimulationSizingDecision {
    notional_usdt: Decimal,
    should_simulate: bool,
    reason: String,
}

fn capped_simulation_notional(
    config: &SmartMoneyMonitorConfig,
    exposure: &SimulatedPositionExposure,
    direction: Direction,
) -> SimulationSizingDecision {
    let desired = dec_to_f64(config.order_notional_usdt).max(0.0);
    let symbol_cap = dec_to_f64(config.max_symbol_position_notional_usdt).max(0.0);
    let total_cap = dec_to_f64(config.max_total_position_notional_usdt).max(0.0);
    let current_symbol = exposure.symbol_net_notional;
    let total_abs = exposure.total_abs_notional.max(current_symbol.abs());
    let side = match direction {
        Direction::Long => 1.0,
        Direction::Short => -1.0,
        Direction::Flat => {
            return SimulationSizingDecision {
                notional_usdt: Decimal::ZERO,
                should_simulate: false,
                reason: "simulation skipped: flat direction".to_string(),
            };
        }
    };

    if desired <= 0.0 || symbol_cap <= 0.0 || total_cap <= 0.0 {
        return SimulationSizingDecision {
            notional_usdt: Decimal::ZERO,
            should_simulate: false,
            reason: "simulation skipped: non-positive simulation notional or cap".to_string(),
        };
    }

    let mut allowed = desired.min(max_delta_for_abs_cap(current_symbol, side, symbol_cap));
    let other_total_abs = (total_abs - current_symbol.abs()).max(0.0);
    let global_symbol_abs_cap = total_cap - other_total_abs;
    if global_symbol_abs_cap >= 0.0 {
        allowed = allowed.min(max_delta_for_abs_cap(
            current_symbol,
            side,
            global_symbol_abs_cap,
        ));
    } else if current_symbol * side < 0.0 {
        allowed = allowed.min(current_symbol.abs());
    } else {
        allowed = 0.0;
    }

    if allowed <= 0.0 {
        return SimulationSizingDecision {
            notional_usdt: Decimal::ZERO,
            should_simulate: false,
            reason: format!(
                "simulation skipped: position cap reached, symbol_net={current_symbol:.2}, total_abs={total_abs:.2}, symbol_cap={symbol_cap:.2}, total_cap={total_cap:.2}"
            ),
        };
    }

    let notional_usdt = Decimal::from_f64(allowed).unwrap_or(Decimal::ZERO);
    let reason = if allowed + 1e-9 < desired {
        format!(
            "simulated by capped taker model; notional reduced from {desired:.2} to {allowed:.2}, symbol_cap={symbol_cap:.2}, total_cap={total_cap:.2}"
        )
    } else {
        format!(
            "simulated by capped taker model; notional={allowed:.2}, symbol_cap={symbol_cap:.2}, total_cap={total_cap:.2}"
        )
    };

    SimulationSizingDecision {
        notional_usdt,
        should_simulate: true,
        reason,
    }
}

fn max_delta_for_abs_cap(current: f64, side: f64, cap: f64) -> f64 {
    if cap < 0.0 {
        return 0.0;
    }
    if side > 0.0 {
        (cap - current).max(0.0)
    } else {
        (current + cap).max(0.0)
    }
}

async fn fetch_binance_top5_book(
    client: &Client,
    base_url: &str,
    symbol: &str,
) -> Result<OrderBookSnapshot> {
    let url = format!(
        "{}/fapi/v1/depth?symbol={}&limit=5",
        base_url.trim_end_matches('/'),
        symbol
    );
    let raw = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    let bids = parse_depth_levels(raw.get("bids").and_then(Value::as_array), "bids")?;
    let asks = parse_depth_levels(raw.get("asks").and_then(Value::as_array), "asks")?;
    Ok(OrderBookSnapshot {
        symbol: symbol.to_string(),
        ts: Utc::now(),
        bids,
        asks,
    })
}

fn parse_depth_levels(levels: Option<&Vec<Value>>, field: &str) -> Result<Vec<BookLevel>> {
    let levels = levels.with_context(|| format!("missing {field}"))?;
    levels
        .iter()
        .take(5)
        .map(|level| {
            let arr = level
                .as_array()
                .with_context(|| format!("invalid {field} level"))?;
            let price = arr
                .first()
                .and_then(Value::as_str)
                .context("missing price")?;
            let quantity = arr
                .get(1)
                .and_then(Value::as_str)
                .context("missing quantity")?;
            Ok(BookLevel {
                price: Decimal::from_str(price)?,
                quantity: Decimal::from_str(quantity)?,
            })
        })
        .collect()
}

fn fallback_book(symbol: &str) -> OrderBookSnapshot {
    let base = match symbol {
        "ETHUSDT" => Decimal::new(3500, 0),
        "SOLUSDT" => Decimal::new(160, 0),
        "HYPEUSDT" => Decimal::new(35, 0),
        _ => Decimal::new(65000, 0),
    };
    let bids = (0..5)
        .map(|i| BookLevel {
            price: base - Decimal::from(i + 1),
            quantity: Decimal::new(10 + i as i64, 2),
        })
        .collect();
    let asks = (0..5)
        .map(|i| BookLevel {
            price: base + Decimal::from(i + 1),
            quantity: Decimal::new(10 + i as i64, 2),
        })
        .collect();
    OrderBookSnapshot {
        symbol: symbol.to_string(),
        ts: Utc::now(),
        bids,
        asks,
    }
}

fn configured_or_demo_wallets(config: &SmartMoneyMonitorConfig) -> Vec<TrackedWalletConfig> {
    if !config.wallets.is_empty() {
        return config.wallets.clone();
    }
    vec![
        demo_wallet(
            "0xsmart000000000000000000000000000000000001",
            "Demo Trend / 趋势",
        ),
        demo_wallet(
            "0xsmart000000000000000000000000000000000002",
            "Demo Swing / 波段",
        ),
        demo_wallet(
            "0xsmart000000000000000000000000000000000003",
            "Demo Whale / 巨鲸",
        ),
    ]
}

fn demo_wallet(address: &str, label: &str) -> TrackedWalletConfig {
    TrackedWalletConfig {
        address: address.to_string(),
        label: Some(label.to_string()),
        tags: Vec::new(),
        enabled: true,
        source_rank: None,
        style: None,
        grade: None,
        initial_score: None,
        initial_weight: None,
        group_name: None,
        cluster: None,
    }
}

fn direction_probability(score: f64) -> f64 {
    (0.35 + score.clamp(0.0, 100.0) / 300.0).clamp(0.2, 0.8)
}

#[derive(Debug)]
struct MonitorDb {
    conn: Connection,
}

impl MonitorDb {
    fn open(path: &str) -> Result<Self> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        let db = Self { conn };
        db.init()?;
        Ok(db)
    }

    fn init(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            create table if not exists wallets (
              address text primary key,
              label text,
              group_name text not null,
              score real not null,
              cluster text not null,
              source_rank integer,
              style text,
              grade text,
              seed_score real not null default 50,
              seed_weight real not null default 0.5,
              trade_count integer not null default 0,
              win_rate real not null default 0,
              total_notional real not null default 0,
              total_pnl real not null default 0,
              last_seen text
            );
            create table if not exists orderbook_snapshots (
              id integer primary key autoincrement,
              ts text not null,
              symbol text not null,
              bids_json text not null,
              asks_json text not null,
              source text not null,
              error text
            );
            create table if not exists simulated_orders (
              id integer primary key autoincrement,
              wallet_address text not null,
              observed_trade_id integer,
              orderbook_id integer not null,
              ts text not null,
              symbol text not null,
              side text not null,
              requested_notional real not null,
              filled_notional real not null,
              filled_quantity real not null,
              average_price real,
              fee_usdt real not null,
              slippage_bps real not null,
              realized_pnl real not null,
              signal_strength real not null default 0,
              status text not null,
              source text not null
            );
            create table if not exists observed_wallet_trades (
              id integer primary key autoincrement,
              dedupe_key text not null unique,
              wallet_address text not null,
              external_id text,
              observed_at text not null,
              executed_at text not null,
              symbol text not null,
              side text not null,
              wallet_price real not null,
              wallet_quantity real not null,
              wallet_notional real not null,
              wallet_fee_usdt real not null,
              wallet_realized_pnl real not null,
              signal_strength real not null,
              copy_decision text not null,
              reason text not null,
              simulated_order_id integer
            );
            create table if not exists wallet_poll_state (
              wallet_address text primary key,
              last_fill_millis integer not null,
              last_poll_at text not null,
              last_error text
            );
            create table if not exists wallet_score_history (
              id integer primary key autoincrement,
              wallet_address text not null,
              ts text not null,
              score real not null,
              group_name text not null,
              cluster text not null,
              seed_score real not null default 50,
              live_weight real not null default 0,
              trade_count integer not null,
              win_rate real not null,
              total_notional real not null,
              total_pnl real not null
            );
            create index if not exists idx_orders_wallet_ts on simulated_orders(wallet_address, ts);
            create index if not exists idx_orders_symbol_ts on simulated_orders(symbol, ts);
            create index if not exists idx_observed_wallet_ts on observed_wallet_trades(wallet_address, executed_at);
            create index if not exists idx_observed_symbol_ts on observed_wallet_trades(symbol, executed_at);
            create index if not exists idx_orderbooks_symbol_ts on orderbook_snapshots(symbol, ts);
            create index if not exists idx_scores_wallet_ts on wallet_score_history(wallet_address, ts);
            "#,
        )?;
        self.ensure_column("wallets", "source_rank", "source_rank integer")?;
        self.ensure_column("wallets", "style", "style text")?;
        self.ensure_column("wallets", "grade", "grade text")?;
        self.ensure_column(
            "wallets",
            "seed_score",
            "seed_score real not null default 50",
        )?;
        self.ensure_column(
            "wallets",
            "seed_weight",
            "seed_weight real not null default 0.5",
        )?;
        self.ensure_column(
            "wallet_score_history",
            "seed_score",
            "seed_score real not null default 50",
        )?;
        self.ensure_column(
            "wallet_score_history",
            "live_weight",
            "live_weight real not null default 0",
        )?;
        self.ensure_column(
            "simulated_orders",
            "observed_trade_id",
            "observed_trade_id integer",
        )?;
        self.ensure_column(
            "simulated_orders",
            "signal_strength",
            "signal_strength real not null default 0",
        )?;
        Ok(())
    }

    fn ensure_column(&self, table: &str, column: &str, ddl: &str) -> Result<()> {
        let mut stmt = self.conn.prepare(&format!("pragma table_info({table})"))?;
        let columns = stmt.query_map([], |row| row.get::<_, String>(1))?;
        for existing in columns {
            if existing? == column {
                return Ok(());
            }
        }
        self.conn
            .execute(&format!("alter table {table} add column {ddl}"), [])?;
        Ok(())
    }

    fn seed_wallets(&self, wallets: &[TrackedWalletConfig]) -> Result<()> {
        for wallet in wallets.iter().filter(|wallet| wallet.enabled) {
            let address = wallet.address.to_ascii_lowercase();
            let label = wallet.label.clone().unwrap_or_default();
            let seed_score = wallet.initial_score.map(dec_to_f64).unwrap_or(50.0);
            let seed_weight = wallet
                .initial_weight
                .map(dec_to_f64)
                .or_else(|| wallet.grade.as_deref().map(weight_from_grade))
                .unwrap_or(0.5);
            let style = wallet.style.clone().unwrap_or_default();
            let grade = wallet.grade.clone().unwrap_or_default();
            let (default_group, default_cluster) =
                group_from_seed(style.as_str(), grade.as_str(), seed_score);
            let group_name = wallet
                .group_name
                .clone()
                .unwrap_or_else(|| default_group.to_string());
            let cluster = wallet
                .cluster
                .clone()
                .unwrap_or_else(|| default_cluster.to_string());
            let now = Utc::now().to_rfc3339();
            self.conn.execute(
                "insert or ignore into wallets(address,label,group_name,score,cluster,source_rank,style,grade,seed_score,seed_weight,last_seen)
                 values (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    address,
                    label,
                    group_name,
                    seed_score,
                    cluster,
                    wallet.source_rank.map(i64::from),
                    style,
                    grade,
                    seed_score,
                    seed_weight,
                    now
                ],
            )?;
            self.conn.execute(
                "update wallets set
                   label=?2,
                   group_name=case when trade_count = 0 then ?3 else group_name end,
                   score=case when trade_count = 0 then ?4 else score end,
                   cluster=case when trade_count = 0 then ?5 else cluster end,
                   source_rank=?6,
                   style=?7,
                   grade=?8,
                   seed_score=?9,
                   seed_weight=?10,
                   last_seen=coalesce(last_seen, ?11)
                 where address=?1",
                params![
                    address,
                    label,
                    group_name,
                    seed_score,
                    cluster,
                    wallet.source_rank.map(i64::from),
                    style,
                    grade,
                    seed_score,
                    seed_weight,
                    now
                ],
            )?;
        }
        Ok(())
    }

    fn wallets(&self) -> Result<Vec<WalletMonitorRow>> {
        let mut stmt = self.conn.prepare(
            "select address,label,group_name,score,cluster,source_rank,style,grade,seed_score,seed_weight,
             trade_count,win_rate,total_notional,total_pnl,last_seen
             from wallets order by score desc, source_rank asc, address asc",
        )?;
        let rows = stmt.query_map([], row_to_wallet)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    fn last_order_at(&self, address: &str) -> Result<Option<DateTime<Utc>>> {
        let raw = self
            .conn
            .query_row(
                "select ts from simulated_orders where wallet_address=?1 order by ts desc limit 1",
                params![address],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        raw.map(|ts| DateTime::parse_from_rfc3339(&ts).map(|parsed| parsed.with_timezone(&Utc)))
            .transpose()
            .map_err(Into::into)
    }

    fn order_count_since(&self, address: &str, duration: Duration) -> Result<i64> {
        let since = (Utc::now() - duration).to_rfc3339();
        self.conn
            .query_row(
                "select count(*) from simulated_orders where wallet_address=?1 and ts >= ?2",
                params![address, since],
                |row| row.get::<_, i64>(0),
            )
            .map_err(Into::into)
    }

    fn simulated_position_exposure(
        &self,
        symbol: &str,
        reset_at: Option<&str>,
    ) -> Result<SimulatedPositionExposure> {
        let target_symbol = symbol.to_ascii_uppercase();
        let mut stmt = self.conn.prepare(
            "select upper(symbol),
                    coalesce(sum(case
                        when side = 'BUY' then filled_notional
                        when side = 'SELL' then -filled_notional
                        else 0
                    end), 0)
             from simulated_orders
             where status in ('filled', 'partial')
               and (?1 = '' or ts >= ?1)
             group by upper(symbol)",
        )?;
        let mut rows = stmt.query(params![reset_at.unwrap_or_default()])?;
        let mut symbol_net_notional = 0.0;
        let mut total_abs_notional = 0.0;
        while let Some(row) = rows.next()? {
            let row_symbol: String = row.get(0)?;
            let net_notional: f64 = row.get(1)?;
            if row_symbol == target_symbol {
                symbol_net_notional = net_notional;
            }
            total_abs_notional += net_notional.abs();
        }
        Ok(SimulatedPositionExposure {
            symbol_net_notional,
            total_abs_notional,
        })
    }

    fn fill_checkpoint_millis(&self, address: &str, lookback_secs: u64) -> Result<i64> {
        let address = address.to_ascii_lowercase();
        let existing = self
            .conn
            .query_row(
                "select last_fill_millis from wallet_poll_state where wallet_address=?1",
                params![address],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            return Ok(existing);
        }
        let lookback = Duration::seconds(lookback_secs.min(24 * 60 * 60) as i64);
        let checkpoint = (Utc::now() - lookback).timestamp_millis();
        self.conn.execute(
            "insert or ignore into wallet_poll_state(wallet_address,last_fill_millis,last_poll_at,last_error)
             values (?1,?2,?3,'')",
            params![address, checkpoint, Utc::now().to_rfc3339()],
        )?;
        Ok(checkpoint)
    }

    fn update_fill_checkpoint(
        &self,
        address: &str,
        last_fill_millis: i64,
        error: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            "insert into wallet_poll_state(wallet_address,last_fill_millis,last_poll_at,last_error)
             values (?1,?2,?3,?4)
             on conflict(wallet_address) do update set
               last_fill_millis=excluded.last_fill_millis,
               last_poll_at=excluded.last_poll_at,
               last_error=excluded.last_error",
            params![
                address.to_ascii_lowercase(),
                last_fill_millis,
                Utc::now().to_rfc3339(),
                error.unwrap_or_default()
            ],
        )?;
        Ok(())
    }

    fn insert_observed_wallet_trade(
        &self,
        address: &str,
        trade: &WalletTrade,
        copy_signal: &CopySignalDecision,
    ) -> Result<Option<i64>> {
        let dedupe_key = wallet_trade_dedupe_key(address, trade);
        let changed = self.conn.execute(
            "insert or ignore into observed_wallet_trades(dedupe_key,wallet_address,external_id,observed_at,executed_at,
             symbol,side,wallet_price,wallet_quantity,wallet_notional,wallet_fee_usdt,wallet_realized_pnl,
             signal_strength,copy_decision,reason)
             values (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
            params![
                dedupe_key,
                address.to_ascii_lowercase(),
                trade.external_id.clone().unwrap_or_default(),
                Utc::now().to_rfc3339(),
                trade.executed_at.to_rfc3339(),
                trade.symbol,
                side_string(trade.direction),
                dec_to_f64(trade.price),
                dec_to_f64(trade.quantity),
                dec_to_f64(trade.notional_usdt),
                dec_to_f64(trade.fee_usdt),
                dec_to_f64(trade.realized_pnl_usdt),
                copy_signal.signal_strength,
                copy_signal.decision,
                copy_signal.reason,
            ],
        )?;
        if changed == 0 {
            return Ok(None);
        }
        self.recompute_wallet_score(address)?;
        Ok(Some(self.conn.last_insert_rowid()))
    }

    fn mark_observed_trade(
        &self,
        observed_trade_id: i64,
        decision: &str,
        reason: &str,
        simulated_order_id: Option<i64>,
    ) -> Result<()> {
        self.conn.execute(
            "update observed_wallet_trades
             set copy_decision=?1, reason=?2, simulated_order_id=coalesce(?3, simulated_order_id)
             where id=?4",
            params![decision, reason, simulated_order_id, observed_trade_id],
        )?;
        Ok(())
    }

    fn insert_orderbook_snapshot(
        &self,
        book: &OrderBookSnapshot,
        source: &str,
        error: Option<&str>,
    ) -> Result<i64> {
        self.conn.execute(
            "insert into orderbook_snapshots(ts,symbol,bids_json,asks_json,source,error)
             values (?1,?2,?3,?4,?5,?6)",
            params![
                book.ts.to_rfc3339(),
                book.symbol,
                serde_json::to_string(&book.bids)?,
                serde_json::to_string(&book.asks)?,
                source,
                error.unwrap_or_default()
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    fn insert_simulated_order(
        &self,
        wallet: &WalletMonitorRow,
        observed_trade_id: Option<i64>,
        orderbook_id: i64,
        fill: &crate::smart_money::SimulatedFill,
        source: &str,
        signal_strength: f64,
    ) -> Result<i64> {
        let realized_pnl = -dec_to_f64(fill.fee_usdt);
        self.conn.execute(
            "insert into simulated_orders(wallet_address,observed_trade_id,orderbook_id,ts,symbol,side,requested_notional,filled_notional,
             filled_quantity,average_price,fee_usdt,slippage_bps,realized_pnl,signal_strength,status,source)
             values (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
            params![
                wallet.address,
                observed_trade_id,
                orderbook_id,
                fill.filled_at.to_rfc3339(),
                fill.symbol,
                match fill.direction {
                    Direction::Long => "BUY",
                    Direction::Short => "SELL",
                    Direction::Flat => "FLAT",
                },
                dec_to_f64(fill.requested_notional_usdt),
                dec_to_f64(fill.filled_notional_usdt),
                dec_to_f64(fill.filled_quantity),
                fill.average_price.map(dec_to_f64),
                dec_to_f64(fill.fee_usdt),
                dec_to_f64(fill.slippage_bps),
                realized_pnl,
                signal_strength,
                if fill.partial_fill { "partial" } else { "filled" },
                source,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    fn recompute_wallet_score(&self, address: &str) -> Result<()> {
        let stats = self.wallet_stats(address)?;
        let score = score_from_stats(&stats);
        let live_weight = live_weight_from_activity(stats.trade_count);
        let (group_name, cluster) = group_from_score(score, stats.total_pnl, stats.win_rate);
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            "update wallets set group_name=?1, score=?2, cluster=?3, trade_count=?4, win_rate=?5,
             total_notional=?6, total_pnl=?7, last_seen=?8 where address=?9",
            params![
                group_name,
                score,
                cluster,
                stats.trade_count,
                stats.win_rate,
                stats.total_notional,
                stats.total_pnl,
                now,
                address
            ],
        )?;
        self.conn.execute(
            "insert into wallet_score_history(wallet_address,ts,score,group_name,cluster,seed_score,live_weight,trade_count,win_rate,total_notional,total_pnl)
             values (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                address,
                now,
                score,
                group_name,
                cluster,
                stats.seed_score,
                live_weight,
                stats.trade_count,
                stats.win_rate,
                stats.total_notional,
                stats.total_pnl
            ],
        )?;
        Ok(())
    }

    fn wallet_stats(&self, address: &str) -> Result<WalletStats> {
        let seed_score = self
            .conn
            .query_row(
                "select seed_score from wallets where address=?1",
                params![address],
                |row| row.get::<_, f64>(0),
            )
            .optional()?
            .unwrap_or(50.0);
        let mut stats = self
            .conn
            .query_row(
                "select count(*), coalesce(sum(case when wallet_realized_pnl > 0 then 1 else 0 end),0),
                 coalesce(sum(wallet_notional),0), coalesce(sum(wallet_realized_pnl),0)
                 from observed_wallet_trades where wallet_address=?1",
                params![address],
                |row| {
                    let trade_count: i64 = row.get(0)?;
                    let wins: i64 = row.get(1)?;
                    let total_notional: f64 = row.get(2)?;
                    let total_pnl: f64 = row.get(3)?;
                    Ok(WalletStats {
                        trade_count,
                        win_rate: if trade_count > 0 {
                            wins as f64 / trade_count as f64
                        } else {
                            0.0
                        },
                        total_notional,
                        total_pnl,
                        seed_score,
                    })
                },
            )
            .map_err(anyhow::Error::from)?;
        stats.seed_score = seed_score;
        Ok(stats)
    }

    fn observed_trades(&self, query: &HistoryQuery) -> Result<Vec<ObservedWalletTradeRow>> {
        let since = since_ts(query.minutes);
        let limit = query.limit.unwrap_or(300).clamp(1, 1000);
        let wallet = query
            .wallet
            .clone()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let mut stmt = self.conn.prepare(
            "select id,wallet_address,external_id,observed_at,executed_at,symbol,side,wallet_price,
             wallet_quantity,wallet_notional,wallet_fee_usdt,wallet_realized_pnl,signal_strength,
             copy_decision,reason,simulated_order_id
             from observed_wallet_trades
             where executed_at >= ?1 and (?2 = '' or wallet_address = ?2)
             order by executed_at desc limit ?3",
        )?;
        let rows = stmt.query_map(params![since, wallet, limit], row_to_observed_trade)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    fn orders(&self, query: &HistoryQuery) -> Result<Vec<SimulatedOrderRow>> {
        let since = since_ts(query.minutes);
        let limit = query.limit.unwrap_or(200).clamp(1, 1000);
        let wallet = query
            .wallet
            .clone()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let mut stmt = self.conn.prepare(
            "select id,wallet_address,observed_trade_id,orderbook_id,ts,symbol,side,requested_notional,filled_notional,
             filled_quantity,average_price,fee_usdt,slippage_bps,realized_pnl,signal_strength,status,source
             from simulated_orders
             where ts >= ?1 and (?2 = '' or wallet_address = ?2)
             order by ts desc limit ?3",
        )?;
        let rows = stmt.query_map(params![since, wallet, limit], row_to_order)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    fn scores(&self, query: &HistoryQuery) -> Result<Vec<WalletScoreRow>> {
        let since = since_ts(query.minutes);
        let limit = query.limit.unwrap_or(500).clamp(1, 2000);
        let wallet = query
            .wallet
            .clone()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let mut stmt = self.conn.prepare(
            "select wallet_address,ts,score,group_name,cluster,seed_score,live_weight,trade_count,win_rate,total_notional,total_pnl
             from wallet_score_history
             where ts >= ?1 and (?2 = '' or wallet_address = ?2)
             order by ts desc limit ?3",
        )?;
        let rows = stmt.query_map(params![since, wallet, limit], row_to_score)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    fn orderbooks(&self, query: &BookQuery) -> Result<Vec<OrderBookRow>> {
        let since = since_ts(query.minutes);
        let limit = query.limit.unwrap_or(100).clamp(1, 500);
        let symbol = query
            .symbol
            .clone()
            .unwrap_or_default()
            .to_ascii_uppercase();
        let mut stmt = self.conn.prepare(
            "select id,ts,symbol,bids_json,asks_json,source,error from orderbook_snapshots
             where ts >= ?1 and (?2 = '' or symbol = ?2)
             order by ts desc limit ?3",
        )?;
        let rows = stmt.query_map(params![since, symbol, limit], row_to_book)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }
}

#[derive(Debug)]
struct WalletStats {
    trade_count: i64,
    win_rate: f64,
    total_notional: f64,
    total_pnl: f64,
    seed_score: f64,
}

fn score_from_stats(stats: &WalletStats) -> f64 {
    let live_weight = live_weight_from_activity(stats.trade_count);
    let pnl_component = (stats.total_pnl / 100.0).clamp(-25.0, 25.0);
    let win_component = (stats.win_rate - 0.5) * 50.0;
    let activity_component = (stats.trade_count as f64).sqrt().min(10.0);
    let live_score = (50.0 + pnl_component + win_component + activity_component).clamp(0.0, 100.0);
    (stats.seed_score.clamp(0.0, 100.0) * (1.0 - live_weight) + live_score * live_weight)
        .clamp(0.0, 100.0)
}

fn live_weight_from_activity(trade_count: i64) -> f64 {
    if trade_count <= 0 {
        0.0
    } else if trade_count < 20 {
        0.30
    } else if trade_count < 100 {
        0.50
    } else {
        0.70
    }
}

fn group_from_score(score: f64, pnl: f64, win_rate: f64) -> (&'static str, &'static str) {
    if score >= 80.0 && pnl > 0.0 {
        ("Elite Alpha / 顶级Alpha", "ConsistentAlpha")
    } else if score >= 65.0 {
        ("Consistent / 稳定表现", "InstitutionalStyle")
    } else if score >= 52.0 && win_rate >= 0.45 {
        ("New Rising / 新锐观察", "NewAlpha")
    } else if score < 35.0 {
        ("High Risk / 高风险", "Gambler")
    } else {
        ("Watchlist / 观察名单", "Swing")
    }
}

fn group_from_seed(style: &str, grade: &str, score: f64) -> (&'static str, &'static str) {
    if score >= 90.0 || grade == "A+" {
        ("Core Composite / 核心综合", "ConsistentAlpha")
    } else if style.eq_ignore_ascii_case("trend") {
        ("Trend Followers / 趋势跟随", "Trend")
    } else if style.eq_ignore_ascii_case("swing") {
        ("Swing Capacity / 波段容量", "Swing")
    } else if style.eq_ignore_ascii_case("momentum") {
        ("Momentum Short-Cycle / 动量短周期", "Momentum")
    } else {
        ("High Consistency / 高一致性", "InstitutionalStyle")
    }
}

fn weight_from_grade(grade: &str) -> f64 {
    match grade {
        "A+" => 1.00,
        "A" => 0.90,
        "A-" => 0.80,
        "B+" => 0.65,
        "B" => 0.50,
        "B-" => 0.35,
        _ => 0.50,
    }
}

fn dec_to_f64(value: Decimal) -> f64 {
    value.to_f64().unwrap_or(0.0)
}

fn since_ts(minutes: Option<i64>) -> String {
    (Utc::now() - Duration::minutes(minutes.unwrap_or(180).clamp(1, 30 * 24 * 60))).to_rfc3339()
}

#[derive(Debug, Clone, Serialize)]
pub struct WalletMonitorRow {
    pub address: String,
    pub label: String,
    pub group_name: String,
    pub score: f64,
    pub cluster: String,
    pub source_rank: Option<i64>,
    pub style: String,
    pub grade: String,
    pub seed_score: f64,
    pub seed_weight: f64,
    pub trade_count: i64,
    pub win_rate: f64,
    pub total_notional: f64,
    pub total_pnl: f64,
    pub last_seen: Option<String>,
}

fn row_to_wallet(row: &rusqlite::Row<'_>) -> rusqlite::Result<WalletMonitorRow> {
    Ok(WalletMonitorRow {
        address: row.get(0)?,
        label: row.get(1)?,
        group_name: row.get(2)?,
        score: row.get(3)?,
        cluster: row.get(4)?,
        source_rank: row.get(5)?,
        style: row.get(6)?,
        grade: row.get(7)?,
        seed_score: row.get(8)?,
        seed_weight: row.get(9)?,
        trade_count: row.get(10)?,
        win_rate: row.get(11)?,
        total_notional: row.get(12)?,
        total_pnl: row.get(13)?,
        last_seen: row.get(14)?,
    })
}

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    pub wallet: Option<String>,
    pub minutes: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct BookQuery {
    pub symbol: Option<String>,
    pub minutes: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ObservedWalletTradeRow {
    pub id: i64,
    pub wallet_address: String,
    pub external_id: String,
    pub observed_at: String,
    pub executed_at: String,
    pub symbol: String,
    pub side: String,
    pub wallet_price: f64,
    pub wallet_quantity: f64,
    pub wallet_notional: f64,
    pub wallet_fee_usdt: f64,
    pub wallet_realized_pnl: f64,
    pub signal_strength: f64,
    pub copy_decision: String,
    pub reason: String,
    pub simulated_order_id: Option<i64>,
}

fn row_to_observed_trade(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObservedWalletTradeRow> {
    Ok(ObservedWalletTradeRow {
        id: row.get(0)?,
        wallet_address: row.get(1)?,
        external_id: row.get(2)?,
        observed_at: row.get(3)?,
        executed_at: row.get(4)?,
        symbol: row.get(5)?,
        side: row.get(6)?,
        wallet_price: row.get(7)?,
        wallet_quantity: row.get(8)?,
        wallet_notional: row.get(9)?,
        wallet_fee_usdt: row.get(10)?,
        wallet_realized_pnl: row.get(11)?,
        signal_strength: row.get(12)?,
        copy_decision: row.get(13)?,
        reason: row.get(14)?,
        simulated_order_id: row.get(15)?,
    })
}

#[derive(Debug, Serialize)]
pub struct SimulatedOrderRow {
    pub id: i64,
    pub wallet_address: String,
    pub observed_trade_id: Option<i64>,
    pub orderbook_id: i64,
    pub ts: String,
    pub symbol: String,
    pub side: String,
    pub requested_notional: f64,
    pub filled_notional: f64,
    pub filled_quantity: f64,
    pub average_price: Option<f64>,
    pub fee_usdt: f64,
    pub slippage_bps: f64,
    pub realized_pnl: f64,
    pub signal_strength: f64,
    pub status: String,
    pub source: String,
}

fn row_to_order(row: &rusqlite::Row<'_>) -> rusqlite::Result<SimulatedOrderRow> {
    Ok(SimulatedOrderRow {
        id: row.get(0)?,
        wallet_address: row.get(1)?,
        observed_trade_id: row.get(2)?,
        orderbook_id: row.get(3)?,
        ts: row.get(4)?,
        symbol: row.get(5)?,
        side: row.get(6)?,
        requested_notional: row.get(7)?,
        filled_notional: row.get(8)?,
        filled_quantity: row.get(9)?,
        average_price: row.get(10)?,
        fee_usdt: row.get(11)?,
        slippage_bps: row.get(12)?,
        realized_pnl: row.get(13)?,
        signal_strength: row.get(14)?,
        status: row.get(15)?,
        source: row.get(16)?,
    })
}

#[derive(Debug, Serialize)]
pub struct WalletScoreRow {
    pub wallet_address: String,
    pub ts: String,
    pub score: f64,
    pub group_name: String,
    pub cluster: String,
    pub seed_score: f64,
    pub live_weight: f64,
    pub trade_count: i64,
    pub win_rate: f64,
    pub total_notional: f64,
    pub total_pnl: f64,
}

fn row_to_score(row: &rusqlite::Row<'_>) -> rusqlite::Result<WalletScoreRow> {
    Ok(WalletScoreRow {
        wallet_address: row.get(0)?,
        ts: row.get(1)?,
        score: row.get(2)?,
        group_name: row.get(3)?,
        cluster: row.get(4)?,
        seed_score: row.get(5)?,
        live_weight: row.get(6)?,
        trade_count: row.get(7)?,
        win_rate: row.get(8)?,
        total_notional: row.get(9)?,
        total_pnl: row.get(10)?,
    })
}

#[derive(Debug, Serialize)]
pub struct OrderBookRow {
    pub id: i64,
    pub ts: String,
    pub symbol: String,
    pub bids: Value,
    pub asks: Value,
    pub source: String,
    pub error: Option<String>,
}

fn row_to_book(row: &rusqlite::Row<'_>) -> rusqlite::Result<OrderBookRow> {
    let bids: String = row.get(3)?;
    let asks: String = row.get(4)?;
    Ok(OrderBookRow {
        id: row.get(0)?,
        ts: row.get(1)?,
        symbol: row.get(2)?,
        bids: serde_json::from_str(&bids).unwrap_or_else(|_| json!([])),
        asks: serde_json::from_str(&asks).unwrap_or_else(|_| json!([])),
        source: row.get(5)?,
        error: row.get::<_, Option<String>>(6)?,
    })
}

async fn monitor_index() -> Html<&'static str> {
    Html(MONITOR_HTML)
}

async fn api_wallets(State(state): State<SmartMoneyMonitorState>) -> impl IntoResponse {
    json_result(state.db.lock().expect("monitor db lock").wallets())
}

async fn api_wallet_trades(
    State(state): State<SmartMoneyMonitorState>,
    Query(query): Query<HistoryQuery>,
) -> impl IntoResponse {
    json_result(
        state
            .db
            .lock()
            .expect("monitor db lock")
            .observed_trades(&query),
    )
}

async fn api_orders(
    State(state): State<SmartMoneyMonitorState>,
    Query(query): Query<HistoryQuery>,
) -> impl IntoResponse {
    json_result(state.db.lock().expect("monitor db lock").orders(&query))
}

async fn api_scores(
    State(state): State<SmartMoneyMonitorState>,
    Query(query): Query<HistoryQuery>,
) -> impl IntoResponse {
    json_result(state.db.lock().expect("monitor db lock").scores(&query))
}

async fn api_orderbooks(
    State(state): State<SmartMoneyMonitorState>,
    Query(query): Query<BookQuery>,
) -> impl IntoResponse {
    json_result(state.db.lock().expect("monitor db lock").orderbooks(&query))
}

async fn api_summary(State(state): State<SmartMoneyMonitorState>) -> impl IntoResponse {
    let db = state.db.lock().expect("monitor db lock");
    let wallets = match db.wallets() {
        Ok(wallets) => wallets,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let orders = match db.orders(&HistoryQuery {
        wallet: None,
        minutes: Some(24 * 60),
        limit: Some(10_000),
    }) {
        Ok(orders) => orders,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let observed_trades = match db.observed_trades(&HistoryQuery {
        wallet: None,
        minutes: Some(24 * 60),
        limit: Some(10_000),
    }) {
        Ok(trades) => trades,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let total_pnl = wallets.iter().map(|wallet| wallet.total_pnl).sum::<f64>();
    Json(json!({
        "wallet_count": wallets.len(),
        "observed_trades_24h": observed_trades.len(),
        "copied_orders_24h": orders.len(),
        "orders_24h": orders.len(),
        "total_pnl": total_pnl,
        "avg_score": if wallets.is_empty() { 0.0 } else { wallets.iter().map(|w| w.score).sum::<f64>() / wallets.len() as f64 },
    }))
    .into_response()
}

async fn api_simulate_once(State(state): State<SmartMoneyMonitorState>) -> impl IntoResponse {
    match simulate_monitor_tick(&state).await {
        Ok(count) => Json(json!({ "copied_orders": count })).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

fn json_result<T: Serialize>(result: Result<T>) -> axum::response::Response {
    match result {
        Ok(value) => Json(value).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

const MONITOR_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Smart Money Monitor / 聪明钱监控</title>
  <style>
    :root { color-scheme: light; --bg:#f6f7f9; --panel:#fff; --ink:#17202a; --muted:#667085; --line:#d8dee8; --accent:#1464f4; --good:#087443; --bad:#b42318; }
    body { margin:0; font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background:var(--bg); color:var(--ink); }
    header { padding:18px 24px; background:#101828; color:white; display:flex; justify-content:space-between; align-items:center; gap:16px; }
    h1 { margin:0; font-size:20px; }
    main { padding:18px 24px 32px; }
    .toolbar, .grid, .panel { max-width:1440px; margin:0 auto 14px; }
    .toolbar { display:flex; flex-wrap:wrap; gap:10px; align-items:end; padding:14px; background:var(--panel); border:1px solid var(--line); border-radius:8px; }
    label { display:grid; gap:4px; font-size:12px; color:var(--muted); }
    input, select, button { height:34px; border:1px solid var(--line); border-radius:6px; padding:0 10px; background:white; color:var(--ink); }
    button { background:var(--accent); color:white; border-color:var(--accent); cursor:pointer; }
    button.secondary { background:white; color:var(--ink); border-color:var(--line); }
    .grid { display:grid; grid-template-columns: repeat(5, minmax(0,1fr)); gap:12px; }
    .card, .panel { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px; }
    .metric { font-size:24px; font-weight:700; margin-top:4px; }
    .muted { color:var(--muted); }
    table { width:100%; border-collapse:collapse; font-size:13px; }
    th, td { border-bottom:1px solid var(--line); padding:8px 6px; text-align:left; vertical-align:top; }
    th { color:var(--muted); font-weight:600; position:sticky; top:0; background:white; }
    .split { display:grid; grid-template-columns: 1.2fr 1fr; gap:14px; max-width:1440px; margin:0 auto; }
    .scroll { max-height:420px; overflow:auto; }
    .pos { color:var(--good); font-weight:600; }
    .neg { color:var(--bad); font-weight:600; }
    @media (max-width: 900px) { .grid, .split { grid-template-columns:1fr; } header { align-items:flex-start; flex-direction:column; } }
  </style>
</head>
<body>
  <header>
    <div>
      <h1 data-en="Smart Money Wallet Monitor" data-zh="聪明钱钱包监控">Smart Money Wallet Monitor</h1>
      <div class="muted" data-en="Real Hyperliquid wallet fills with capped Binance REST top-5 taker simulation" data-zh="真实 Hyperliquid 钱包成交 + Binance REST 前5档限额吃单模拟">Real Hyperliquid wallet fills with capped Binance REST top-5 taker simulation</div>
    </div>
    <button class="secondary" onclick="toggleLang()" id="langBtn">中文</button>
  </header>
  <main>
    <section class="toolbar">
      <label><span data-en="Wallet" data-zh="钱包">Wallet</span><select id="wallet"><option value="">All / 全部</option></select></label>
      <label><span data-en="Time Window" data-zh="时间窗口">Time Window</span><select id="minutes"><option value="60">1h</option><option value="180" selected>3h</option><option value="1440">24h</option><option value="10080">7d</option></select></label>
      <label><span data-en="Symbol" data-zh="交易对">Symbol</span><input id="symbol" placeholder="BTCUSDT"/></label>
      <button onclick="loadAll()" data-en="Refresh" data-zh="刷新">Refresh</button>
      <button onclick="simulate()" data-en="Poll Now" data-zh="立即轮询">Poll Now</button>
    </section>
    <section class="grid">
      <div class="card"><div class="muted" data-en="Wallets" data-zh="钱包数量">Wallets</div><div class="metric" id="wallet_count">-</div></div>
      <div class="card"><div class="muted" data-en="Observed 24h" data-zh="24小时观察成交">Observed 24h</div><div class="metric" id="observed_trades_24h">-</div></div>
      <div class="card"><div class="muted" data-en="Simulated 24h" data-zh="24小时模拟">Simulated 24h</div><div class="metric" id="copied_orders_24h">-</div></div>
      <div class="card"><div class="muted" data-en="Average Score" data-zh="平均评分">Average Score</div><div class="metric" id="avg_score">-</div></div>
      <div class="card"><div class="muted" data-en="Total PnL" data-zh="总盈亏">Total PnL</div><div class="metric" id="total_pnl">-</div></div>
    </section>
    <section class="panel">
      <h2 data-en="Wallet Ranking and Groups" data-zh="钱包排名与分组">Wallet Ranking and Groups</h2>
      <div class="scroll"><table><thead><tr><th data-en="Rank" data-zh="排名">Rank</th><th data-en="Wallet" data-zh="钱包">Wallet</th><th data-en="Label" data-zh="标签">Label</th><th data-en="Style" data-zh="风格">Style</th><th data-en="Group" data-zh="分组">Group</th><th data-en="Score" data-zh="评分">Score</th><th data-en="Seed" data-zh="初始分">Seed</th><th data-en="Weight" data-zh="权重">Weight</th><th data-en="Trades" data-zh="交易数">Trades</th><th data-en="Win Rate" data-zh="胜率">Win Rate</th><th>PnL</th></tr></thead><tbody id="walletRows"></tbody></table></div>
    </section>
    <section class="panel">
      <h2 data-en="Observed Hyperliquid Wallet Fills" data-zh="观察到的 Hyperliquid 钱包成交">Observed Hyperliquid Wallet Fills</h2>
      <div class="scroll"><table><thead><tr><th>Time</th><th>Wallet</th><th>Symbol</th><th>Side</th><th>Wallet Price</th><th>Notional</th><th>Wallet Fee</th><th>Closed PnL</th><th>Signal</th><th>Decision</th><th>Reason</th></tr></thead><tbody id="observedRows"></tbody></table></div>
    </section>
    <section class="split">
      <div class="panel">
        <h2 data-en="Capped Taker Simulations" data-zh="限额吃单模拟">Capped Taker Simulations</h2>
        <div class="scroll"><table><thead><tr><th>Time</th><th>Wallet</th><th>Symbol</th><th>Side</th><th>Filled</th><th>Price</th><th>Fee</th><th>Slippage</th><th>Signal</th><th>Status</th><th>Source</th></tr></thead><tbody id="orderRows"></tbody></table></div>
      </div>
      <div class="panel">
        <h2 data-en="REST Top-5 Order Books" data-zh="REST 前5档订单簿">REST Top-5 Order Books</h2>
        <div class="scroll"><table><thead><tr><th>Time</th><th>Symbol</th><th>Source</th><th>Bids</th><th>Asks</th></tr></thead><tbody id="bookRows"></tbody></table></div>
      </div>
    </section>
  </main>
<script>
let lang='en';
function t(){document.querySelectorAll('[data-en]').forEach(el=>{el.textContent=el.dataset[lang]}); document.getElementById('langBtn').textContent=lang==='en'?'中文':'English'}
function toggleLang(){lang=lang==='en'?'zh':'en';t()}
const fmt=n=>(Number(n)||0).toFixed(2);
const cls=n=>(Number(n)||0)>=0?'pos':'neg';
async function getJson(url){const r=await fetch(url); if(!r.ok) throw new Error(await r.text()); return r.json();}
function params(){const w=document.getElementById('wallet').value; const m=document.getElementById('minutes').value; return `minutes=${m}&limit=300${w?`&wallet=${encodeURIComponent(w)}`:''}`}
async function loadAll(){
  const [summary,wallets,observed,orders,books]=await Promise.all([
    getJson('/api/summary'), getJson('/api/wallets'), getJson('/api/wallet-trades?'+params()), getJson('/api/orders?'+params()), getJson('/api/orderbooks?minutes='+document.getElementById('minutes').value+'&limit=80'+(document.getElementById('symbol').value?'&symbol='+encodeURIComponent(document.getElementById('symbol').value):''))]);
  wallet_count.textContent=summary.wallet_count; observed_trades_24h.textContent=summary.observed_trades_24h; copied_orders_24h.textContent=summary.copied_orders_24h; avg_score.textContent=fmt(summary.avg_score); total_pnl.textContent=fmt(summary.total_pnl);
  const sel=document.getElementById('wallet'); const cur=sel.value; sel.innerHTML='<option value="">All / 全部</option>'+wallets.map(w=>`<option value="${w.address}">${w.label||w.address.slice(0,10)} (${fmt(w.score)})</option>`).join(''); sel.value=cur;
  walletRows.innerHTML=wallets.map(w=>`<tr><td>${w.source_rank||''}</td><td>${w.address.slice(0,12)}...</td><td>${w.label||''}</td><td>${w.style||''} ${w.grade?'/ '+w.grade:''}</td><td>${w.group_name}</td><td>${fmt(w.score)}</td><td>${fmt(w.seed_score)}</td><td>${fmt((w.seed_weight||0)*100)}%</td><td>${w.trade_count}</td><td>${fmt(w.win_rate*100)}%</td><td class="${cls(w.total_pnl)}">${fmt(w.total_pnl)}</td></tr>`).join('');
  observedRows.innerHTML=observed.map(t=>`<tr><td>${t.executed_at.slice(11,19)}</td><td>${t.wallet_address.slice(0,10)}...</td><td>${t.symbol}</td><td>${t.side}</td><td>${fmt(t.wallet_price)}</td><td>${fmt(t.wallet_notional)}</td><td>${fmt(t.wallet_fee_usdt)}</td><td class="${cls(t.wallet_realized_pnl)}">${fmt(t.wallet_realized_pnl)}</td><td>${fmt(t.signal_strength)}</td><td>${t.copy_decision}</td><td>${t.reason}</td></tr>`).join('');
  orderRows.innerHTML=orders.map(o=>`<tr><td>${o.ts.slice(11,19)}</td><td>${o.wallet_address.slice(0,10)}...</td><td>${o.symbol}</td><td>${o.side}</td><td>${fmt(o.filled_notional)}</td><td>${fmt(o.average_price)}</td><td>${fmt(o.fee_usdt)}</td><td>${fmt(o.slippage_bps)}</td><td>${fmt(o.signal_strength)}</td><td>${o.status}</td><td>${o.source}</td></tr>`).join('');
  bookRows.innerHTML=books.map(b=>`<tr><td>${b.ts.slice(11,19)}</td><td>${b.symbol}</td><td>${b.source}</td><td>${levels(b.bids)}</td><td>${levels(b.asks)}</td></tr>`).join('');
}
function levels(v){return (v||[]).slice(0,5).map(x=>`${fmt(x.price)} @ ${fmt(x.quantity)}`).join('<br>')}
async function simulate(){await fetch('/api/simulate',{method:'POST'}); await loadAll();}
setInterval(loadAll, 8000); t(); loadAll();
</script>
</body>
</html>"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> SmartMoneyMonitorConfig {
        SmartMoneyMonitorConfig {
            order_notional_usdt: Decimal::new(1000, 0),
            max_symbol_position_notional_usdt: Decimal::new(1000, 0),
            max_total_position_notional_usdt: Decimal::new(5000, 0),
            ..SmartMoneyMonitorConfig::default()
        }
    }

    #[test]
    fn capped_sizing_allows_fresh_symbol_with_available_total_capacity() {
        let decision = capped_simulation_notional(
            &test_config(),
            &SimulatedPositionExposure {
                symbol_net_notional: 0.0,
                total_abs_notional: 4000.0,
            },
            Direction::Long,
        );

        assert!(decision.should_simulate);
        assert_eq!(decision.notional_usdt, Decimal::new(1000, 0));
    }

    #[test]
    fn capped_sizing_blocks_same_side_when_symbol_cap_is_full() {
        let decision = capped_simulation_notional(
            &test_config(),
            &SimulatedPositionExposure {
                symbol_net_notional: 1000.0,
                total_abs_notional: 1000.0,
            },
            Direction::Long,
        );

        assert!(!decision.should_simulate);
    }

    #[test]
    fn capped_sizing_reduces_order_to_remaining_total_capacity() {
        let decision = capped_simulation_notional(
            &test_config(),
            &SimulatedPositionExposure {
                symbol_net_notional: 0.0,
                total_abs_notional: 4500.0,
            },
            Direction::Short,
        );

        assert!(decision.should_simulate);
        assert_eq!(decision.notional_usdt, Decimal::new(500, 0));
    }

    #[test]
    fn capped_sizing_allows_opposite_side_trade_to_reduce_over_cap_exposure() {
        let decision = capped_simulation_notional(
            &test_config(),
            &SimulatedPositionExposure {
                symbol_net_notional: 750.0,
                total_abs_notional: 5500.0,
            },
            Direction::Short,
        );

        assert!(decision.should_simulate);
        assert_eq!(decision.notional_usdt, Decimal::new(1000, 0));
    }
}

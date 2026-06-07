//! Spot taker arbitrage strategy for the supported cross-exchange Spot pairs.
//!
//! Paper and live-dry-run modes never submit live orders. `trading_mode=live`
//! may submit Spot orders only after explicit live flags, preflight, kill-switch,
//! small-live-gate, symbol-rule, balance, and order-plan validation pass.

pub mod book_cache;
pub mod book_recorder;
pub mod config;
pub mod inventory;
pub mod lifecycle;
pub mod market_data;
pub mod opportunity;
pub mod paper_execution;
pub mod recorder;
pub mod replay;
pub mod replay_report;
pub mod report;
pub mod risk;
pub mod spread_engine;
pub mod types;
pub mod websocket_market_data;

pub use book_cache::*;
pub use book_recorder::*;
pub use config::*;
pub use inventory::*;
pub use lifecycle::*;
pub use market_data::*;
pub use opportunity::*;
pub use paper_execution::*;
pub use recorder::*;
pub use replay::*;
pub use replay_report::*;
pub use report::*;
pub use risk::*;
pub use spread_engine::*;
pub use types::*;
pub use websocket_market_data::*;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::json;
use tokio::sync::broadcast;
use tokio::time::{interval, sleep, timeout, MissedTickBehavior};

use crate::control::spot_control::{
    normalize_symbol, snapshot_store_from_config, DisableMode, DisableSymbolRequest,
    EnabledDirection, MarketLiquidationPlan, RuntimeExchangeClient, RuntimeReconciliationServices,
    SpotControlRuntimePublisher, SpotControlRuntimePublisherDeps, SpotControlService,
    SpotSymbolLifecycleState, VersionedSymbolRequest,
};
use crate::exchanges::bitget::{BitgetSpotClient, BitgetSpotConfig};
use crate::exchanges::client_order_id::generate_client_order_id;
use crate::exchanges::coinex::{CoinExSpotClient, CoinExSpotConfig};
use crate::exchanges::gateio::{GateIoSpotClient, GateIoSpotConfig};
use crate::exchanges::kucoin::KuCoinSpotClient;
use crate::exchanges::mexc::{MexcSpotClient, MexcSpotConfig};
use crate::exchanges::unified::{
    round_price_to_tick, round_quantity_to_step, validate_order_against_symbol_rule, AssetBalance,
    CancelOrderRequest, ExchangeClient, MarketType, OrderBookSnapshot, OrderRequest, OrderResponse,
    OrderSide, OrderStatus, OrderType, PositionSide, SymbolRule, TimeInForce,
};
use crate::execution::{
    append_live_dry_run_plan, build_live_dry_run_order_plan, reconcile_dashboard_inventory,
    FeeLookupKey, FeeModel, FeeRole, LiveDryRunCheck, LiveDryRunOrderInput, LiveDryRunOrderPlan,
    LiveDryRunValidationResult,
};
use crate::live_preflight::{
    api_permissions_from_env, evaluate_small_live_gate, run_live_preflight, LiveReadinessState,
    SmallLiveGateConfig, SmallLiveGateInput,
};
use crate::risk::{DisabledRegistry, DisabledStatus, KillSwitch, KillSwitchState};
use crate::scanner::{scan_five_exchange_spot, FiveExchangeSpotScannerConfig};
use crate::strategies::arbitrage_core::{
    analyze_relationship, record_analysis_jsonl, ArbitrageRelationship, ArbitrageRelationshipType,
    MarketLeg,
};
use crate::web::{
    spawn_monitoring_snapshot_writer, BookView, ConfigSummaryView, DashboardReadModel,
    DisabledExchangeSymbolView, DisabledExchangeView, DisabledSymbolView, DisabledView,
    ExchangeHealthView, FeeView, InventoryView, MonitoringState, OpportunityView,
    RecorderHealthView, RiskEventView, TradeView, UnmanagedPositionView,
};

pub const SPOT_SPOT_TAKER_ARBITRAGE_STRATEGY_NAME: &str = "spot_spot_taker_arbitrage";

static SPOT_SPOT_WARN_ONCE_KEYS: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();
static SPOT_SPOT_LIVE_OPENING_NOTIONAL_USDT: OnceLock<Mutex<f64>> = OnceLock::new();
const LIVE_INVENTORY_EXCHANGE_SYNC_INTERVAL_SECONDS: i64 = 10 * 60;
const LIQUIDATION_BALANCE_SAFETY_FACTOR: f64 = 0.999;

pub struct SpotSpotTakerArbitrageStrategy {
    config: SpotSpotTakerArbitrageConfig,
}

impl SpotSpotTakerArbitrageStrategy {
    pub fn new(config: SpotSpotTakerArbitrageConfig) -> Result<Self> {
        config.validate_safe_mode()?;
        Ok(Self { config })
    }

    pub async fn start(self) -> Result<()> {
        if !self.config.enabled {
            log::warn!("spot_spot_taker_arbitrage is disabled in config");
            return Ok(());
        }
        if self.config.market_data_mode == MarketDataMode::Replay {
            let report = run_replay_mode(self.config).await?;
            log::info!(
                "spot_spot_taker_arbitrage replay complete events={} opportunities={} accepted={} net_pnl={:.6}",
                report.total_book_events,
                report.opportunities_detected,
                report.opportunities_accepted,
                report.simulated_net_pnl
            );
            return Ok(());
        }

        let recorder = StrategyRecorder::start(&self.config.recording_config()).await?;
        let mut report = SummaryReport::default();
        let mut risk = RiskState::new(&self.config);
        let mut inventory = PaperInventory::from_config(&self.config)?;
        let fee_model = fee_model_with_strategy_overrides(
            FeeModel::load_or_default(&self.config.fee_config_path),
            &self.config,
        );
        let disabled_registry =
            DisabledRegistry::load_or_empty(&self.config.disabled_registry_path);
        let kill_switch = KillSwitch::new(self.config.kill_switch.clone());
        inventory.exclude_unmanaged_positions(&disabled_registry);
        let mut monitoring = if self.config.monitoring.enabled {
            let state = monitoring_state_from_config(&self.config);
            state.publish_fees(fee_views(&fee_model));
            state.publish_disabled(
                disabled_view(&disabled_registry),
                unmanaged_position_views(&disabled_registry),
            );
            state.publish_inventory(config_inventory_views(&inventory, &disabled_registry));
            state.publish_config_summary(config_summary_view(&self.config));
            state.publish_order_reconciliation_config(self.config.order_reconciliation.clone());
            state.publish_kill_switch(kill_switch.state());
            Some(state)
        } else {
            None
        };

        let mexc = MexcSpotClient::new(mexc_spot_runtime_config(&self.config));
        let coinex = CoinExSpotClient::new(coinex_spot_runtime_config(&self.config));
        let gateio = GateIoSpotClient::new(gateio_spot_runtime_config(&self.config));
        let bitget = BitgetSpotClient::new(bitget_spot_runtime_config(&self.config));
        let kucoin = KuCoinSpotClient::new(kucoin_spot_runtime_config(&self.config));
        let book_cache = BookCache::default();
        let mut live_inventory_cache = LiveInventoryCache::default();
        if let Some(state) = &monitoring {
            live_inventory_cache
                .sync_if_due(&self.config, &inventory, &mexc, &coinex, &gateio, &bitget)
                .await;
            let live_inventory = live_inventory_cache
                .inventory_views(&inventory, &disabled_registry, &book_cache)
                .await;
            state.publish_inventory(live_inventory);
            state.publish_open_orders(
                live_open_orders(&self.config, &mexc, &coinex, &gateio, &bitget).await,
            );
        }

        let symbol_rules =
            load_common_symbol_rules(&self.config, &mexc, &coinex, &gateio, &bitget, &kucoin)
                .await?;
        if symbol_rules.is_empty() {
            return Err(anyhow!(
                "no symbols are available on the configured spot pair for spot_spot_taker_arbitrage"
            ));
        }
        if let Some(state) = &monitoring {
            state.publish_spot_symbol_rules(flatten_spot_symbol_rules(&symbol_rules));
        }
        log::info!(
            "spot_spot_taker_arbitrage loaded {} common symbols",
            symbol_rules.len()
        );
        let websocket_runtime = if self.config.market_data_mode == MarketDataMode::WebsocketCache {
            Some(
                start_websocket_market_data(
                    &self.config,
                    mexc.clone(),
                    coinex.clone(),
                    gateio.clone(),
                    bitget.clone(),
                    kucoin.clone(),
                    book_cache.clone(),
                )
                .await?,
            )
        } else {
            None
        };
        if let Some(state) = monitoring.take() {
            monitoring = Some(attach_runtime_publisher_if_enabled(
                state,
                &self.config,
                &symbol_rules,
                &fee_model,
                &disabled_registry,
                &kill_switch,
                &book_cache,
                websocket_runtime.as_ref(),
                mexc.clone(),
                coinex.clone(),
                gateio.clone(),
                bitget.clone(),
            ));
        }
        if let Some(state) = &monitoring {
            if self.config.live_preflight.enabled {
                let preflight_state = live_readiness_state(
                    &self.config,
                    &symbol_rules,
                    &inventory,
                    &disabled_registry,
                    &fee_model,
                    &book_cache,
                    websocket_runtime.as_ref(),
                )
                .await;
                let report =
                    run_live_preflight(self.config.live_preflight.clone(), &preflight_state);
                state.publish_live_preflight(report);
            }
        }
        let _monitoring_snapshot_writer = monitoring.clone().and_then(|state| {
            spawn_monitoring_snapshot_writer(self.config.monitoring.clone(), state)
        });

        let mut ticker = interval(Duration::from_millis(self.config.scan_interval_ms));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut last_report_at = chrono::Utc::now();
        let mut enabled_arbitrage_symbols = BTreeSet::<String>::new();
        let mut spread_duration_tracker = SpreadDurationTracker::default();
        let mut arbitrage_pair_states = BTreeMap::<String, ArbitragePairRuntime>::new();
        let mut event_spread_engine = EventDrivenSpreadEngine::new(EventDrivenSpreadEngineConfig {
            exchanges: self.config.exchanges.clone(),
            symbols: self.config.symbols.clone(),
        });
        let mut book_event_rx = websocket_runtime
            .as_ref()
            .map(WebsocketMarketDataRuntime::subscribe_book_events);

        loop {
            let updated_symbol = match self.config.market_data_mode {
                MarketDataMode::WebsocketCache => {
                    next_event_driven_symbol(
                        &mut book_event_rx,
                        &mut event_spread_engine,
                        self.config.scan_interval_ms,
                    )
                    .await
                }
                MarketDataMode::RestPolling => {
                    ticker.tick().await;
                    None
                }
                MarketDataMode::Replay => {
                    unreachable!("replay mode returns before live loop")
                }
            };
            let control_liquidation_blocked_symbols = BTreeSet::new();
            if let Some(state) = &monitoring {
                state.publish_strategy_status("running");
                release_blocked_arbitrage_symbols(&mut enabled_arbitrage_symbols, state).await;
                // Manual mode: strategy execution no longer auto-runs control-plane liquidation.
                // Stop/clear operations should be initiated explicitly from the control surface.
            }
            if kill_switch.state().active {
                if let Some(state) = &monitoring {
                    state.publish_kill_switch(kill_switch.state());
                }
                continue;
            }
            if let Some(reason) = risk_auto_stop_reason(&risk, &self.config) {
                trigger_risk_auto_stop(&kill_switch, &monitoring, reason);
                continue;
            }
            live_inventory_cache
                .sync_if_due(&self.config, &inventory, &mexc, &coinex, &gateio, &bitget)
                .await;
            let mut blocked_control_symbols = control_blocked_arbitrage_symbols(&monitoring).await;
            blocked_control_symbols.extend(control_liquidation_blocked_symbols.iter().cloned());
            release_blocked_arbitrage_pair_states(
                &blocked_control_symbols,
                &mut enabled_arbitrage_symbols,
                &mut arbitrage_pair_states,
            );
            let symbols_to_process = symbols_for_market_data_tick(&self.config, &updated_symbol);
            if should_scan_arbitrage_opportunities(
                &arbitrage_pair_states,
                self.config.max_enabled_arbitrage_symbols,
            ) && !symbols_to_process.is_empty()
            {
                let mut stop_current_scan = false;
                for symbol in &symbols_to_process {
                    if stop_current_scan || kill_switch.state().active {
                        break;
                    }
                    let Some(rules) = symbol_rules.get(symbol) else {
                        continue;
                    };
                    report.symbols_scanned += 1;
                    let (mexc_book, coinex_book, mexc_source, coinex_source) = match self
                        .config
                        .market_data_mode
                    {
                        MarketDataMode::RestPolling => {
                            let (left_venue, right_venue) =
                                configured_spot_pair(&self.config.exchanges);
                            let (mexc_book, coinex_book) = match (left_venue, right_venue) {
                                (SpotVenue::Mexc, SpotVenue::CoinEx) => {
                                    tokio::join!(
                                        mexc.get_orderbook(symbol, self.config.orderbook_depth),
                                        coinex.get_orderbook(symbol, self.config.orderbook_depth)
                                    )
                                }
                                (SpotVenue::GateIo, SpotVenue::Bitget) => {
                                    tokio::join!(
                                        gateio.get_orderbook(symbol, self.config.orderbook_depth),
                                        bitget.get_orderbook(symbol, self.config.orderbook_depth)
                                    )
                                }
                                _ => unreachable!("unsupported configured spot pair"),
                            };
                            match (mexc_book, coinex_book) {
                                (Ok(mexc_book), Ok(coinex_book)) => {
                                    (mexc_book, coinex_book, BookSource::Rest, BookSource::Rest)
                                }
                                (left, right) => {
                                    let reason = RejectionReason::ExchangeHealth;
                                    publish_risk_event(
                                        &monitoring,
                                        symbol,
                                        None,
                                        reason,
                                        Some(format!(
                                            "orderbook fetch failed mexc_ok={} coinex_ok={}",
                                            left.is_ok(),
                                            right.is_ok()
                                        )),
                                    );
                                    log::warn!(
                                        "spot_spot_taker_arbitrage orderbook fetch failed symbol={} mexc_ok={} coinex_ok={}",
                                        symbol,
                                        left.is_ok(),
                                        right.is_ok()
                                    );
                                    risk.record_rejection(symbol, reason);
                                    report.record_rejection(reason);
                                    continue;
                                }
                            }
                        }
                        MarketDataMode::WebsocketCache => {
                            let (left_venue, right_venue) =
                                configured_spot_pair(&self.config.exchanges);
                            let Some((left_book, right_book)) =
                                event_spread_engine.book_pair(left_venue, right_venue, symbol)
                            else {
                                report.record_rejection(RejectionReason::StaleBook);
                                risk.record_rejection(symbol, RejectionReason::StaleBook);
                                publish_risk_event(
                                    &monitoring,
                                    symbol,
                                    None,
                                    RejectionReason::StaleBook,
                                    Some(format!(
                                        "missing event-driven books for {} and {}",
                                        left_venue.as_str(),
                                        right_venue.as_str()
                                    )),
                                );
                                continue;
                            };
                            if left_book.is_stale || right_book.is_stale {
                                report.record_rejection(RejectionReason::StaleBook);
                                risk.record_rejection(symbol, RejectionReason::StaleBook);
                                publish_risk_event(
                                    &monitoring,
                                    symbol,
                                    None,
                                    RejectionReason::StaleBook,
                                    Some(format!(
                                        "stale event-driven book left_stale={} right_stale={}",
                                        left_book.is_stale, right_book.is_stale
                                    )),
                                );
                                continue;
                            }
                            let mexc_source = left_book.source;
                            let coinex_source = right_book.source;
                            (
                                event_engine_snapshot(left_book),
                                event_engine_snapshot(right_book),
                                mexc_source,
                                coinex_source,
                            )
                        }
                        MarketDataMode::Replay => {
                            unreachable!("replay mode returns before live loop")
                        }
                    };

                    let opportunities = detect_opportunities_for_pair_with_source(
                        &self.config,
                        rules,
                        &mexc_book,
                        &coinex_book,
                        &inventory,
                        &risk,
                        &fee_model,
                        &disabled_registry,
                        mexc_source,
                        coinex_source,
                    );
                    if spot_order_plan_mode(&self.config)
                        && self.config.inventory_rebalance.enabled
                        && self.config.inventory_rebalance.allow_market_rebalance
                    {
                        let books = lifecycle_books_for_symbol(
                            &self.config,
                            &book_cache,
                            symbol,
                            &mexc_book,
                            &coinex_book,
                        )
                        .await;
                        let balances_by_exchange = lifecycle_balances_for_books(
                            &books, &inventory, &mexc, &coinex, &gateio, &bitget,
                        )
                        .await;
                        let trim_plans = build_inventory_trim_excess_plans(
                            &self.config,
                            symbol,
                            rules,
                            &books,
                            &balances_by_exchange,
                            self.config.inventory_rebalance.target_total_notional_usdt,
                            &disabled_registry,
                            &fee_model,
                        );
                        if !trim_plans.is_empty() {
                            log::info!(
                                "现货套利库存超目标，先减仓 symbol={} plan_count={} target_total_notional_usdt={:.6}",
                                symbol,
                                trim_plans.len(),
                                self.config.inventory_rebalance.target_total_notional_usdt
                            );
                            for trim_plan in trim_plans {
                                record_or_submit_spot_order_plan(
                                    &self.config,
                                    trim_plan,
                                    &monitoring,
                                    Some(&mut live_inventory_cache),
                                    &mexc,
                                    &coinex,
                                    &gateio,
                                    &bitget,
                                )
                                .await?;
                            }
                            continue;
                        }
                        let top_up_plans = build_inventory_top_up_plans(
                            &self.config,
                            symbol,
                            rules,
                            &books,
                            &balances_by_exchange,
                            self.config.inventory_rebalance.target_total_notional_usdt,
                            &disabled_registry,
                            &fee_model,
                        );
                        if !top_up_plans.is_empty() {
                            log::info!(
                                "现货套利库存低于目标，先补库存 symbol={} plan_count={} target_total_notional_usdt={:.6}",
                                symbol,
                                top_up_plans.len(),
                                self.config.inventory_rebalance.target_total_notional_usdt
                            );
                            for top_up_plan in top_up_plans {
                                record_or_submit_spot_order_plan(
                                    &self.config,
                                    top_up_plan,
                                    &monitoring,
                                    Some(&mut live_inventory_cache),
                                    &mexc,
                                    &coinex,
                                    &gateio,
                                    &bitget,
                                )
                                .await?;
                            }
                            continue;
                        }
                    }

                    for mut opportunity in opportunities {
                        if opportunity.accepted {
                            let symbol_key = normalize_symbol(&opportunity.symbol);
                            if arbitrage_pair_states.contains_key(&symbol_key) {
                                if let Some(state) = &monitoring {
                                    if let Some(control) = state.control() {
                                        if !manual_control_allows_arbitrage(
                                            control,
                                            &opportunity.symbol,
                                            &opportunity.buy_exchange,
                                            &opportunity.sell_exchange,
                                            &self.config.trading_mode,
                                        )
                                        .await
                                        {
                                            let mut blocked_symbols = BTreeSet::new();
                                            blocked_symbols.insert(symbol_key.clone());
                                            release_arbitrage_symbols(
                                                &mut enabled_arbitrage_symbols,
                                                &blocked_symbols,
                                            );
                                            opportunity.accepted = false;
                                            opportunity.rejection_reason =
                                                Some(RejectionReason::ControlPlaneBlocked);
                                            opportunity.rejection_detail = Some(
                                                "manual spot control Active state is required"
                                                    .to_string(),
                                            );
                                            continue;
                                        }
                                        let tradability = control
                                            .effective_tradability(
                                                &opportunity.symbol,
                                                &opportunity.buy_exchange,
                                                &opportunity.sell_exchange,
                                                &self.config.trading_mode,
                                            )
                                            .await;
                                        if !tradability.effective_allowed {
                                            opportunity.accepted = false;
                                            opportunity.rejection_reason =
                                                Some(RejectionReason::ControlPlaneBlocked);
                                            opportunity.rejection_detail =
                                                Some(tradability.rejection_reasons.join(","));
                                        }
                                    }
                                }
                            }
                        }
                        if let Some(state) = &monitoring {
                            state.record_opportunity(opportunity_view(&opportunity));
                        }
                        publish_arbitrage_analysis(
                            &monitoring,
                            &self.config,
                            &opportunity,
                            rules,
                            &mexc_book,
                            &coinex_book,
                            &fee_model,
                        );
                        report.record_opportunity(&opportunity);
                        recorder.record_opportunity(opportunity.clone()).await;
                        if !opportunity.accepted {
                            if spot_order_plan_mode(&self.config) {
                                if opportunity.rejection_reason
                                    != Some(RejectionReason::ControlPlaneBlocked)
                                {
                                    if let Some(pair_state) = arbitrage_pair_states
                                        .get(&normalize_symbol(&opportunity.symbol))
                                    {
                                        if pair_state.status == ArbitragePairStatus::Arbitraging {
                                            let (left_venue, right_venue) =
                                                configured_spot_pair(&self.config.exchanges);
                                            let buy_book = book_for_exchange(
                                                &opportunity.buy_exchange,
                                                &mexc_book,
                                                &coinex_book,
                                            );
                                            let sell_book = book_for_exchange(
                                                &opportunity.sell_exchange,
                                                &mexc_book,
                                                &coinex_book,
                                            );
                                            if let (Some(buy_book), Some(sell_book)) =
                                                (buy_book, sell_book)
                                            {
                                                let buy_exchange = spot_venue_from_exchange(
                                                    &opportunity.buy_exchange,
                                                )
                                                .unwrap_or(left_venue);
                                                let sell_exchange = spot_venue_from_exchange(
                                                    &opportunity.sell_exchange,
                                                )
                                                .unwrap_or(right_venue);
                                                let buy_balances = live_inventory_cache
                                                    .balances_for_exchange(
                                                        buy_exchange,
                                                        &inventory,
                                                    );
                                                let sell_balances = live_inventory_cache
                                                    .balances_for_exchange(
                                                        sell_exchange,
                                                        &inventory,
                                                    );
                                                if let Ok(plans) = build_maker_taker_arbitrage_plans(
                                                    &self.config,
                                                    &opportunity,
                                                    rules,
                                                    buy_book,
                                                    sell_book,
                                                    &buy_balances,
                                                    &sell_balances,
                                                    &disabled_registry,
                                                    &fee_model,
                                                ) {
                                                    log::info!(
                                                    "现货套利执行计划 symbol={} mode=maker_taker plan_count={} buy_exchange={} sell_exchange={} raw_spread_bps={:.3} net_spread_bps={:.3} executable_notional={:.6} estimated_net_pnl={:.6}",
                                                    opportunity.symbol,
                                                    plans.len(),
                                                    opportunity.buy_exchange,
                                                    opportunity.sell_exchange,
                                                    opportunity.raw_spread_bps,
                                                    opportunity.estimated_net_spread_bps,
                                                    opportunity.executable_notional,
                                                    opportunity.estimated_net_pnl
                                                );
                                                    let plans = live_plan_group_ready_for_submit(
                                                        &self.config,
                                                        plans,
                                                    );
                                                    if plans.is_empty() {
                                                        continue;
                                                    }
                                                    let mut plans = plans;
                                                    order_dual_taker_plans_sell_first(&mut plans);
                                                    let mut filled_plans = Vec::new();
                                                    for plan in plans {
                                                        let plan_side = plan.side;
                                                        let plan_snapshot = plan.clone();
                                                        let result =
                                                            record_or_submit_spot_order_plan(
                                                                &self.config,
                                                                plan,
                                                                &monitoring,
                                                                Some(&mut live_inventory_cache),
                                                                &mexc,
                                                                &coinex,
                                                                &gateio,
                                                                &bitget,
                                                            )
                                                            .await?;
                                                        if result.filled {
                                                            filled_plans.push(plan_snapshot);
                                                        }
                                                        if !result.filled {
                                                            break;
                                                        }
                                                        if plan_side == OrderSide::Sell
                                                            && filled_plans.iter().any(|plan| {
                                                                plan.side == OrderSide::Buy
                                                            })
                                                        {
                                                            break;
                                                        }
                                                    }
                                                    let has_buy = filled_plans
                                                        .iter()
                                                        .find(|plan| plan.side == OrderSide::Buy);
                                                    let has_sell = filled_plans
                                                        .iter()
                                                        .find(|plan| plan.side == OrderSide::Sell);
                                                    match (has_buy, has_sell) {
                                                        (Some(buy_plan), None) => {
                                                            if let Some(pair_state) =
                                                                arbitrage_pair_states.get_mut(
                                                                    &normalize_symbol(
                                                                        &opportunity.symbol,
                                                                    ),
                                                                )
                                                            {
                                                                if let Some(exposure) =
                                                                    one_sided_exposure_from_filled_plan(
                                                                        buy_plan,
                                                                        OneSidedExposureKind::LongBase,
                                                                        chrono::Utc::now(),
                                                                    )
                                                                {
                                                                    log_one_sided_exposure(
                                                                        &exposure,
                                                                    );
                                                                    pair_state
                                                                        .mark_one_sided_exposure(
                                                                            exposure,
                                                                        );
                                                                }
                                                            }
                                                        }
                                                        (None, Some(sell_plan)) => {
                                                            if let Some(pair_state) =
                                                                arbitrage_pair_states.get_mut(
                                                                    &normalize_symbol(
                                                                        &opportunity.symbol,
                                                                    ),
                                                                )
                                                            {
                                                                if let Some(exposure) =
                                                                    one_sided_exposure_from_filled_plan(
                                                                        sell_plan,
                                                                        OneSidedExposureKind::ShortBase,
                                                                        chrono::Utc::now(),
                                                                    )
                                                                {
                                                                    log_one_sided_exposure(
                                                                        &exposure,
                                                                    );
                                                                    pair_state
                                                                        .mark_one_sided_exposure(
                                                                            exposure,
                                                                        );
                                                                }
                                                            }
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if let Some(reason) = opportunity.rejection_reason {
                                risk.record_rejection(&opportunity.symbol, reason);
                                publish_risk_event(
                                    &monitoring,
                                    &opportunity.symbol,
                                    None,
                                    reason,
                                    opportunity.rejection_detail.clone(),
                                );
                                if let Some(stop_reason) =
                                    risk_auto_stop_reason(&risk, &self.config)
                                {
                                    trigger_risk_auto_stop(&kill_switch, &monitoring, stop_reason);
                                    stop_current_scan = true;
                                    break;
                                }
                            }
                            continue;
                        }

                        if spot_order_plan_mode(&self.config) {
                            if !spot_live_execution_symbol_allowed(
                                &self.config,
                                &opportunity.symbol,
                            ) {
                                continue;
                            }
                            let now = chrono::Utc::now();
                            let symbol_key = normalize_symbol(&opportunity.symbol);
                            let threshold_ready = spread_duration_tracker.observe(
                                &opportunity,
                                now,
                                self.config.spread_duration_threshold_seconds,
                            );
                            if !arbitrage_pair_states.contains_key(&symbol_key) {
                                if !threshold_ready {
                                    continue;
                                }
                                if !arbitrage_symbol_allowed(
                                    &mut enabled_arbitrage_symbols,
                                    &opportunity.symbol,
                                    self.config.max_enabled_arbitrage_symbols,
                                ) {
                                    publish_risk_event(
                                    &monitoring,
                                    &opportunity.symbol,
                                    None,
                                    RejectionReason::ControlPlaneBlocked,
                                    Some(format!(
                                        "max_enabled_arbitrage_symbols={} reached; cannot enter opening state",
                                        self.config.max_enabled_arbitrage_symbols
                                    )),
                                );
                                    continue;
                                }
                                if let Some(state) = &monitoring {
                                    if let Some(control) = state.control() {
                                        let tradability = control
                                            .effective_tradability(
                                                &opportunity.symbol,
                                                &opportunity.buy_exchange,
                                                &opportunity.sell_exchange,
                                                &self.config.trading_mode,
                                            )
                                            .await;
                                        if !tradability.effective_allowed {
                                            let mut blocked_symbols = BTreeSet::new();
                                            blocked_symbols.insert(symbol_key.clone());
                                            release_arbitrage_symbols(
                                                &mut enabled_arbitrage_symbols,
                                                &blocked_symbols,
                                            );
                                            opportunity.accepted = false;
                                            opportunity.rejection_reason =
                                                Some(RejectionReason::ControlPlaneBlocked);
                                            opportunity.rejection_detail =
                                                Some(tradability.rejection_reasons.join(","));
                                            continue;
                                        }
                                    }
                                } else {
                                    let mut blocked_symbols = BTreeSet::new();
                                    blocked_symbols.insert(symbol_key.clone());
                                    release_arbitrage_symbols(
                                        &mut enabled_arbitrage_symbols,
                                        &blocked_symbols,
                                    );
                                    publish_risk_event(
                                        &monitoring,
                                        &opportunity.symbol,
                                        None,
                                        RejectionReason::ControlPlaneBlocked,
                                        Some(
                                            "manual spot control is required before entering opening state"
                                                .to_string(),
                                        ),
                                    );
                                    continue;
                                }
                                spread_duration_tracker.clear(&opportunity.symbol);
                                let mut runtime =
                                    ArbitragePairRuntime::opening(&opportunity.symbol, now);
                                if restore_arbitraging_from_existing_inventory(
                                    &self.config,
                                    &opportunity,
                                    rules,
                                    &mexc_book,
                                    &coinex_book,
                                    &live_inventory_cache,
                                    &inventory,
                                    &disabled_registry,
                                    &fee_model,
                                    now,
                                    &mut runtime,
                                ) {
                                    log::info!(
                                        "现货套利从现货库存恢复 symbol={} status=arbitraging buy_exchange={} sell_exchange={} executable_notional={:.6}",
                                        opportunity.symbol,
                                        opportunity.buy_exchange,
                                        opportunity.sell_exchange,
                                        opportunity.executable_notional
                                    );
                                }
                                arbitrage_pair_states.insert(symbol_key.clone(), runtime);
                                log::info!(
                                "现货套利新加入交易对 symbol={} status={:?} buy_exchange={} sell_exchange={} raw_spread_bps={:.3} net_spread_bps={:.3} executable_notional={:.6} estimated_net_pnl={:.6}",
                                opportunity.symbol,
                                arbitrage_pair_states.get(&symbol_key).map(|state| state.status).unwrap_or(ArbitragePairStatus::Opening),
                                opportunity.buy_exchange,
                                opportunity.sell_exchange,
                                opportunity.raw_spread_bps,
                                opportunity.estimated_net_spread_bps,
                                opportunity.executable_notional,
                                opportunity.estimated_net_pnl
                            );
                            }

                            let Some(pair_state) = arbitrage_pair_states.get_mut(&symbol_key)
                            else {
                                continue;
                            };
                            if pair_state.status == ArbitragePairStatus::Opening {
                                if !self.config.inventory_rebalance.allow_auto_initial_entry {
                                    pair_state.mark_inventory_drift(
                                        "automatic initial entry disabled; waiting for manual inventory or existing two-sided inventory",
                                    );
                                    log::info!(
                                        "现货套利自动建仓已禁用 symbol={} status=inventory_drift",
                                        opportunity.symbol
                                    );
                                    continue;
                                }
                                if !pair_state.should_attempt_entry(now, 30) {
                                    continue;
                                }
                                let books = lifecycle_books_for_symbol(
                                    &self.config,
                                    &book_cache,
                                    &opportunity.symbol,
                                    &mexc_book,
                                    &coinex_book,
                                )
                                .await;
                                let balances_by_exchange = lifecycle_balances_for_books(
                                    &books, &inventory, &mexc, &coinex, &gateio, &bitget,
                                )
                                .await;
                                let plans = build_entry_live_dry_run_plans(
                                    &self.config,
                                    &opportunity.symbol,
                                    rules,
                                    &books,
                                    &balances_by_exchange,
                                    pair_state.entry_attempts,
                                    &disabled_registry,
                                    &fee_model,
                                );
                                log::info!(
                                "现货套利建仓计划 symbol={} attempt={} plan_count={} status=opening",
                                opportunity.symbol,
                                pair_state.entry_attempts + 1,
                                plans.len()
                            );
                                pair_state.mark_entry_attempt(now);
                                let plans = live_plan_group_ready_for_submit(&self.config, plans);
                                if plans.is_empty() {
                                    continue;
                                }
                                let mut all_entry_filled_or_planned = !plans.is_empty();
                                for plan in plans {
                                    log::info!(
                                    "现货套利建仓订单 intent={} exchange={} symbol={} side={:?} order_type={:?} quantity={} notional={:.8} validation_passed={} rejection_reason={}",
                                    plan.intent,
                                    plan.exchange,
                                    plan.symbol,
                                    plan.side,
                                    plan.order_type,
                                    plan.quantity,
                                    plan.notional,
                                    plan.validation_result.passed,
                                    plan.rejection_reason.clone().unwrap_or_else(|| "-".to_string())
                                );
                                    let result = record_or_submit_spot_order_plan(
                                        &self.config,
                                        plan,
                                        &monitoring,
                                        Some(&mut live_inventory_cache),
                                        &mexc,
                                        &coinex,
                                        &gateio,
                                        &bitget,
                                    )
                                    .await?;
                                    if !result.filled {
                                        all_entry_filled_or_planned = false;
                                    }
                                }
                                if all_entry_filled_or_planned {
                                    pair_state.mark_arbitraging(now);
                                    sync_control_arbitraging_symbol(
                                        &monitoring,
                                        &self.config,
                                        &opportunity.symbol,
                                    )
                                    .await;
                                    log::info!(
                                    "现货套利建仓完成 symbol={} status=arbitraging entry_attempts={}",
                                    opportunity.symbol,
                                    pair_state.entry_attempts
                                );
                                }
                                continue;
                            }
                            if pair_state.status == ArbitragePairStatus::OneSidedExposure {
                                let Some(exposure) = pair_state.one_sided_exposure.clone() else {
                                    continue;
                                };
                                let books = lifecycle_books_for_symbol(
                                    &self.config,
                                    &book_cache,
                                    &opportunity.symbol,
                                    &mexc_book,
                                    &coinex_book,
                                )
                                .await;
                                let balances_by_exchange = lifecycle_balances_for_books(
                                    &books, &inventory, &mexc, &coinex, &gateio, &bitget,
                                )
                                .await;
                                let mut recovered = false;
                                for recovery_plan in build_one_sided_exposure_recovery_plans(
                                    &self.config,
                                    &exposure,
                                    rules,
                                    &books,
                                    &balances_by_exchange,
                                    &disabled_registry,
                                    &fee_model,
                                    inventory.realized_pnl,
                                ) {
                                    let recovery = record_or_submit_spot_order_plan(
                                        &self.config,
                                        recovery_plan,
                                        &monitoring,
                                        Some(&mut live_inventory_cache),
                                        &mexc,
                                        &coinex,
                                        &gateio,
                                        &bitget,
                                    )
                                    .await?;
                                    if recovery.filled {
                                        recovered = true;
                                        break;
                                    }
                                }
                                if recovered {
                                    pair_state.mark_arbitraging(now);
                                    log::info!(
                                        "现货套利单腿风险已恢复 symbol={} status=arbitraging",
                                        opportunity.symbol
                                    );
                                } else {
                                    log::info!(
                                        "现货套利单腿风险等待恢复 symbol={} kind={:?} reference_price={:.10} quantity={:.8}",
                                        exposure.symbol,
                                        exposure.kind,
                                        exposure.reference_price,
                                        exposure.quantity
                                    );
                                }
                                continue;
                            }
                            if pair_state.status == ArbitragePairStatus::InventoryDrift {
                                if self.config.inventory_rebalance.enabled
                                    && self.config.inventory_rebalance.allow_market_rebalance
                                {
                                    let books = lifecycle_books_for_symbol(
                                        &self.config,
                                        &book_cache,
                                        &opportunity.symbol,
                                        &mexc_book,
                                        &coinex_book,
                                    )
                                    .await;
                                    let balances_by_exchange = lifecycle_balances_for_books(
                                        &books, &inventory, &mexc, &coinex, &gateio, &bitget,
                                    )
                                    .await;
                                    let rebalance_plans = match (
                                        spot_venue_from_exchange(&opportunity.buy_exchange),
                                        spot_venue_from_exchange(&opportunity.sell_exchange),
                                    ) {
                                        (Some(buy_exchange), Some(sell_exchange)) => {
                                            let plans = build_blocked_arbitrage_rebalance_plans(
                                                &self.config,
                                                &opportunity,
                                                buy_exchange,
                                                sell_exchange,
                                                rules,
                                                &books,
                                                &balances_by_exchange,
                                                &disabled_registry,
                                                &fee_model,
                                            );
                                            if plans.is_empty() {
                                                build_inventory_rebalance_plans_with_mode(
                                                    &self.config,
                                                    &opportunity.symbol,
                                                    rules,
                                                    &books,
                                                    &balances_by_exchange,
                                                    self.config
                                                        .inventory_rebalance
                                                        .target_total_notional_usdt
                                                        / configured_spot_venues(
                                                            &self.config.exchanges,
                                                        )
                                                        .len()
                                                        .max(1)
                                                            as f64,
                                                    &disabled_registry,
                                                    &fee_model,
                                                    true,
                                                )
                                            } else {
                                                plans
                                            }
                                        }
                                        _ => build_inventory_rebalance_plans_with_mode(
                                            &self.config,
                                            &opportunity.symbol,
                                            rules,
                                            &books,
                                            &balances_by_exchange,
                                            self.config
                                                .inventory_rebalance
                                                .target_total_notional_usdt
                                                / configured_spot_venues(&self.config.exchanges)
                                                    .len()
                                                    .max(1)
                                                    as f64,
                                            &disabled_registry,
                                            &fee_model,
                                            true,
                                        ),
                                    };
                                    if rebalance_plans.is_empty() {
                                        log::info!(
                                            "现货套利库存偏移等待修复 symbol={} reason={}",
                                            opportunity.symbol,
                                            pair_state.pause_reason.clone().unwrap_or_else(|| {
                                                "no profit-preserving rebalance".to_string()
                                            })
                                        );
                                    }
                                    let mut any_filled = false;
                                    for rebalance_plan in rebalance_plans {
                                        let result = record_or_submit_spot_order_plan(
                                            &self.config,
                                            rebalance_plan,
                                            &monitoring,
                                            Some(&mut live_inventory_cache),
                                            &mexc,
                                            &coinex,
                                            &gateio,
                                            &bitget,
                                        )
                                        .await?;
                                        any_filled |= result.filled;
                                    }
                                    if any_filled {
                                        pair_state.mark_arbitraging(now);
                                    }
                                }
                                continue;
                            }
                            if pair_state.status != ArbitragePairStatus::Arbitraging {
                                continue;
                            }
                            pair_state.mark_opportunity(now);
                            let buy_exchange = spot_venue_from_exchange(&opportunity.buy_exchange);
                            let sell_exchange =
                                spot_venue_from_exchange(&opportunity.sell_exchange);
                            let (Some(buy_exchange), Some(sell_exchange)) =
                                (buy_exchange, sell_exchange)
                            else {
                                continue;
                            };
                            let buy_balances = live_inventory_cache
                                .balances_for_exchange(buy_exchange, &inventory);
                            let sell_balances = live_inventory_cache
                                .balances_for_exchange(sell_exchange, &inventory);
                            if !dual_taker_direction_preserves_inventory_targets(
                                &self.config,
                                &opportunity,
                                rules,
                                &buy_balances,
                                &sell_balances,
                            ) {
                                pair_state.mark_inventory_drift(
                                    "dual taker direction would increase inventory on an over-target venue or sell an under-target venue",
                                );
                                log::info!(
                                    "现货套利方向会加重库存偏移，先跳过套利 symbol={} buy_exchange={} sell_exchange={} target_total_notional_usdt={:.6}",
                                    opportunity.symbol,
                                    opportunity.buy_exchange,
                                    opportunity.sell_exchange,
                                    self.config.inventory_rebalance.target_total_notional_usdt
                                );
                                continue;
                            }
                            match build_dual_taker_arbitrage_plans(
                                &self.config,
                                &opportunity,
                                rules,
                                book_for_exchange(
                                    &opportunity.buy_exchange,
                                    &mexc_book,
                                    &coinex_book,
                                )
                                .unwrap_or(&mexc_book),
                                book_for_exchange(
                                    &opportunity.sell_exchange,
                                    &mexc_book,
                                    &coinex_book,
                                )
                                .unwrap_or(&coinex_book),
                                &buy_balances,
                                &sell_balances,
                                &disabled_registry,
                                &fee_model,
                            ) {
                                Ok(plans) => {
                                    log::info!(
                                    "现货套利执行计划 symbol={} mode=dual_taker plan_count={} buy_exchange={} sell_exchange={} raw_spread_bps={:.3} net_spread_bps={:.3} executable_notional={:.6} estimated_net_pnl={:.6}",
                                    opportunity.symbol,
                                    plans.len(),
                                    opportunity.buy_exchange,
                                    opportunity.sell_exchange,
                                    opportunity.raw_spread_bps,
                                    opportunity.estimated_net_spread_bps,
                                    opportunity.executable_notional,
                                    opportunity.estimated_net_pnl
                                );
                                    let mut plans =
                                        live_plan_group_ready_for_submit(&self.config, plans);
                                    if plans.is_empty() {
                                        pair_state.mark_inventory_drift(
                                            "dual taker plan rejected by pre-submit checks; likely missing high-side base or low-side quote",
                                        );
                                        log::warn!(
                                            "现货套利库存不足暂停同方向套利 symbol={} status=inventory_drift",
                                            opportunity.symbol
                                        );
                                        continue;
                                    }
                                    order_dual_taker_plans_sell_first(&mut plans);
                                    let mut filled_arbitrage_plans = Vec::new();
                                    for plan in plans {
                                        let plan_side = plan.side;
                                        let plan_snapshot = plan.clone();
                                        let result = record_or_submit_spot_order_plan(
                                            &self.config,
                                            plan,
                                            &monitoring,
                                            Some(&mut live_inventory_cache),
                                            &mexc,
                                            &coinex,
                                            &gateio,
                                            &bitget,
                                        )
                                        .await?;
                                        if result.filled {
                                            filled_arbitrage_plans.push(plan_snapshot);
                                        }
                                        if !result.filled {
                                            break;
                                        }
                                    }
                                    let has_buy = filled_arbitrage_plans
                                        .iter()
                                        .find(|plan| plan.side == OrderSide::Buy);
                                    let has_sell = filled_arbitrage_plans
                                        .iter()
                                        .find(|plan| plan.side == OrderSide::Sell);
                                    match (has_buy, has_sell) {
                                        (Some(buy_plan), None) => {
                                            if let Some(exposure) =
                                                one_sided_exposure_from_filled_plan(
                                                    buy_plan,
                                                    OneSidedExposureKind::LongBase,
                                                    chrono::Utc::now(),
                                                )
                                            {
                                                log_one_sided_exposure(&exposure);
                                                pair_state.mark_one_sided_exposure(exposure);
                                            }
                                        }
                                        (None, Some(sell_plan)) => {
                                            if let Some(exposure) =
                                                one_sided_exposure_from_filled_plan(
                                                    sell_plan,
                                                    OneSidedExposureKind::ShortBase,
                                                    chrono::Utc::now(),
                                                )
                                            {
                                                log_one_sided_exposure(&exposure);
                                                pair_state.mark_one_sided_exposure(exposure);
                                            }
                                        }
                                        _ => {}
                                    }
                                    if let Some(trade) = live_dual_taker_trade_record(
                                        &opportunity,
                                        &filled_arbitrage_plans,
                                    ) {
                                        risk.record_trade(&trade);
                                        let trade_stop_reason =
                                            if risk.trade_loss_limit_hit(&self.config, &trade) {
                                                Some(RiskStopReason::TradeLoss {
                                                    pnl: trade.net_pnl,
                                                    limit: self.config.max_trade_loss,
                                                    symbol: trade.symbol.clone(),
                                                })
                                            } else {
                                                risk_auto_stop_reason(&risk, &self.config)
                                            };
                                        risk.apply_trade_cooldown(
                                            &trade.symbol,
                                            self.config.cooldown_ms_after_trade,
                                        );
                                        report.record_trade(&trade);
                                        if let Some(state) = &monitoring {
                                            state.record_trade(trade_view(&trade));
                                        }
                                        log::info!(
                                            "现货套利成交记录 symbol={} buy_exchange={} sell_exchange={} quantity={:.8} gross_pnl={:.8} fee={:.8} net_pnl={:.8}",
                                            trade.symbol,
                                            trade.buy_exchange,
                                            trade.sell_exchange,
                                            trade.quantity,
                                            trade.gross_pnl,
                                            trade.buy_fee + trade.sell_fee,
                                            trade.net_pnl
                                        );
                                        recorder.record_trade(trade).await;
                                        if let Some(stop_reason) = trade_stop_reason {
                                            trigger_risk_auto_stop(
                                                &kill_switch,
                                                &monitoring,
                                                stop_reason,
                                            );
                                            stop_current_scan = true;
                                        }
                                    }
                                }
                                Err(error) => {
                                    publish_risk_event(
                                        &monitoring,
                                        &opportunity.symbol,
                                        None,
                                        RejectionReason::PaperExecutionRejected,
                                        Some(format!("live dry-run plan rejected: {error}")),
                                    );
                                }
                            }
                            if self.config.inventory_rebalance.enabled
                                && self.config.inventory_rebalance.allow_market_rebalance
                            {
                                let books = lifecycle_books_for_symbol(
                                    &self.config,
                                    &book_cache,
                                    &opportunity.symbol,
                                    &mexc_book,
                                    &coinex_book,
                                )
                                .await;
                                let balances_by_exchange = lifecycle_balances_for_books(
                                    &books, &inventory, &mexc, &coinex, &gateio, &bitget,
                                )
                                .await;
                                let rebalance_plans = if pair_state.status
                                    == ArbitragePairStatus::InventoryDrift
                                {
                                    build_inventory_rebalance_plans_with_mode(
                                        &self.config,
                                        &opportunity.symbol,
                                        rules,
                                        &books,
                                        &balances_by_exchange,
                                        self.config.inventory_rebalance.target_total_notional_usdt
                                            / configured_spot_venues(&self.config.exchanges)
                                                .len()
                                                .max(1)
                                                as f64,
                                        &disabled_registry,
                                        &fee_model,
                                        true,
                                    )
                                } else {
                                    build_inventory_rebalance_plans(
                                        &self.config,
                                        &opportunity.symbol,
                                        rules,
                                        &books,
                                        &balances_by_exchange,
                                        self.config.inventory_rebalance.target_total_notional_usdt
                                            / configured_spot_venues(&self.config.exchanges)
                                                .len()
                                                .max(1)
                                                as f64,
                                        &disabled_registry,
                                        &fee_model,
                                    )
                                };
                                if rebalance_plans.is_empty()
                                    && pair_state.status == ArbitragePairStatus::InventoryDrift
                                {
                                    log::info!(
                                        "现货套利库存偏移等待修复 symbol={} reason={}",
                                        opportunity.symbol,
                                        pair_state.pause_reason.clone().unwrap_or_else(|| {
                                            "no profit-preserving rebalance".to_string()
                                        })
                                    );
                                }
                                for rebalance_plan in rebalance_plans {
                                    record_or_submit_spot_order_plan(
                                        &self.config,
                                        rebalance_plan,
                                        &monitoring,
                                        Some(&mut live_inventory_cache),
                                        &mexc,
                                        &coinex,
                                        &gateio,
                                        &bitget,
                                    )
                                    .await?;
                                }
                            }
                        } else {
                            let trade = execute_paper_taker_taker(
                                &self.config,
                                &mut inventory,
                                &opportunity,
                                rules,
                                &mexc_book,
                                &coinex_book,
                            );
                            match trade {
                                Ok(trade) => {
                                    risk.record_trade(&trade);
                                    let trade_stop_reason =
                                        if risk.trade_loss_limit_hit(&self.config, &trade) {
                                            Some(RiskStopReason::TradeLoss {
                                                pnl: trade.net_pnl,
                                                limit: self.config.max_trade_loss,
                                                symbol: trade.symbol.clone(),
                                            })
                                        } else {
                                            risk_auto_stop_reason(&risk, &self.config)
                                        };
                                    risk.apply_trade_cooldown(
                                        &trade.symbol,
                                        self.config.cooldown_ms_after_trade,
                                    );
                                    report.record_trade(&trade);
                                    if let Some(state) = &monitoring {
                                        state.record_trade(trade_view(&trade));
                                        let inventory_snapshot = inventory_views(
                                            &inventory,
                                            &disabled_registry,
                                            &book_cache,
                                        )
                                        .await;
                                        state.publish_inventory(inventory_snapshot);
                                    }
                                    recorder.record_trade(trade).await;
                                    if let Some(stop_reason) = trade_stop_reason {
                                        trigger_risk_auto_stop(
                                            &kill_switch,
                                            &monitoring,
                                            stop_reason,
                                        );
                                        stop_current_scan = true;
                                        break;
                                    }
                                }
                                Err(reason) => {
                                    risk.record_rejection(&opportunity.symbol, reason);
                                    report.record_rejection(reason);
                                    publish_risk_event(
                                        &monitoring,
                                        &opportunity.symbol,
                                        None,
                                        reason,
                                        Some("paper execution rejected".to_string()),
                                    );
                                    log::warn!(
                                    "spot_spot_taker_arbitrage paper execution rejected symbol={} reason={:?}",
                                    opportunity.symbol,
                                    reason
                                );
                                }
                            }
                        }
                    }
                }
            } else {
                log::debug!(
                    "现货套利机会扫描跳过 active_or_opening_symbols={} max_enabled_arbitrage_symbols={}",
                    active_or_opening_arbitrage_symbol_count(&arbitrage_pair_states),
                    self.config.max_enabled_arbitrage_symbols
                );
            }

            if spot_order_plan_mode(&self.config) && self.config.inventory_rebalance.allow_auto_exit
            {
                let now = chrono::Utc::now();
                let inactive_symbols = arbitrage_pair_states
                    .iter()
                    .filter(|(_, state)| {
                        state.should_exit_for_inactivity(now, self.config.inactivity_exit_seconds)
                    })
                    .map(|(symbol, _)| symbol.clone())
                    .collect::<Vec<_>>();
                for symbol in inactive_symbols {
                    if let Some(state) = arbitrage_pair_states.get_mut(&symbol) {
                        log::info!(
                            "现货套利交易对退出触发 symbol={} reason=inactive seconds={} exit_attempt={}",
                            symbol,
                            self.config.inactivity_exit_seconds,
                            state.exit_attempts + 1
                        );
                        state.exit_attempts = state.exit_attempts.saturating_add(1);
                        state.last_opportunity_at = now;
                        log::info!(
                            "现货套利自动退出已停用 symbol={} reason=manual_stop_required seconds={} exit_attempt={}",
                            symbol,
                            self.config.inactivity_exit_seconds,
                            state.exit_attempts
                        );
                    }
                }
            }

            let elapsed = chrono::Utc::now()
                .signed_duration_since(last_report_at)
                .num_seconds();
            if elapsed >= self.config.report_interval_seconds as i64 {
                log::info!("{}", report.render());
                last_report_at = chrono::Utc::now();
            }
            if let Some(state) = &monitoring {
                publish_market_views(state, &book_cache, websocket_runtime.as_ref(), &self.config)
                    .await;
                if should_scan_arbitrage_opportunities(
                    &arbitrage_pair_states,
                    self.config.max_enabled_arbitrage_symbols,
                ) {
                    publish_five_exchange_scanner_view(
                        state,
                        &self.config,
                        &symbol_rules,
                        &book_cache,
                        &fee_model,
                    )
                    .await;
                }
                state.publish_recorder(recorder_health_view(
                    &self.config,
                    websocket_runtime.as_ref(),
                ));
                let inventory_snapshot = live_inventory_cache
                    .inventory_views(&inventory, &disabled_registry, &book_cache)
                    .await;
                state.publish_inventory(inventory_snapshot.clone());
                state.publish_open_orders(
                    live_open_orders(&self.config, &mexc, &coinex, &gateio, &bitget).await,
                );
                let balance_report = reconcile_dashboard_inventory(&inventory_snapshot);
                state.publish_balance_reconciliation(balance_report.clone());
                state.publish_kill_switch(kill_switch.state());
                if let Some(publisher) = state.runtime_publisher() {
                    state.publish_runtime_publisher_health(publisher.health().await);
                }
                if self.config.live_preflight.enabled {
                    let preflight_state = live_readiness_state(
                        &self.config,
                        &symbol_rules,
                        &inventory,
                        &disabled_registry,
                        &fee_model,
                        &book_cache,
                        websocket_runtime.as_ref(),
                    )
                    .await;
                    let report =
                        run_live_preflight(self.config.live_preflight.clone(), &preflight_state);
                    state.publish_live_preflight(report);
                }
                let snapshot = state.snapshot().await;
                let small_gate = evaluate_small_live_gate(SmallLiveGateInput {
                    config: &self.config.small_live_gate,
                    preflight: snapshot.live_preflight.as_ref(),
                    live_dry_run_orders: &snapshot.live_dry_run_orders,
                    require_valid_live_dry_run_order: !spot_live_mode(&self.config),
                    kill_switch: snapshot.kill_switch.as_ref().unwrap_or(&KillSwitchState {
                        enabled: true,
                        active: false,
                        reason: None,
                        triggered_by: None,
                        triggered_at: None,
                        allow_paper_trading: true,
                        allow_live_dry_run: true,
                        allow_live_orders: false,
                    }),
                    balance_reconciliation: Some(&balance_report),
                    order_reconciliation: &self.config.order_reconciliation,
                    recorder_enabled: self.config.enable_csv_recording
                        || self.config.enable_database_recording,
                    dashboard_enabled: self.config.monitoring.enabled,
                    fee_model_available: true,
                    books_fresh: snapshot.books.iter().all(|book| !book.is_stale),
                    disabled_symbol_overlap: false,
                    unmanaged_inventory_overlap: has_small_live_unmanaged_inventory_overlap(
                        &self.config.small_live_gate,
                        &snapshot.unmanaged_positions,
                    ),
                });
                state.publish_small_live_gate(small_gate);
            }
        }
    }
}

fn monitoring_state_from_config(config: &SpotSpotTakerArbitrageConfig) -> MonitoringState {
    let mut model = DashboardReadModel::default();
    model.trading_mode = config.trading_mode.clone();
    model.live_trading_enabled = config.live_trading_enabled;
    model.dry_run = config.dry_run;
    model.strategy.strategy_name = SPOT_SPOT_TAKER_ARBITRAGE_STRATEGY_NAME.to_string();
    model.strategy.status = "starting".to_string();
    model.config_summary = config_summary_view(config);
    let state = MonitoringState::from_read_model(config.monitoring.clone(), model);
    if config.spot_symbol_control.enabled {
        match SpotControlService::load_or_new(config.spot_symbol_control.clone()) {
            Ok(control) => state.with_control_service(control),
            Err(error) => {
                log::error!("failed to load spot symbol control service: {error}");
                state
            }
        }
    } else {
        state
    }
}

fn spot_live_mode(config: &SpotSpotTakerArbitrageConfig) -> bool {
    config.trading_mode.eq_ignore_ascii_case("live")
        && config.live_trading_enabled
        && !config.dry_run
}

fn spot_order_plan_mode(config: &SpotSpotTakerArbitrageConfig) -> bool {
    config.trading_mode.eq_ignore_ascii_case("live_dry_run") || spot_live_mode(config)
}

async fn next_event_driven_symbol(
    rx: &mut Option<broadcast::Receiver<crate::data::BookEvent>>,
    engine: &mut EventDrivenSpreadEngine,
    idle_refresh_ms: u64,
) -> Option<String> {
    let Some(rx) = rx.as_mut() else {
        sleep(Duration::from_millis(idle_refresh_ms.max(1))).await;
        return None;
    };
    let wait = Duration::from_millis(idle_refresh_ms.max(1));
    match timeout(wait, rx.recv()).await {
        Ok(Ok(event)) => {
            let Some(spot_event) = spot_book_event_from_shared(&event) else {
                return None;
            };
            let stale_signal = spot_event.event_kind.is_stale_signal() || !spot_event.is_tradeable;
            let result = engine.on_book_event(spot_event);
            if stale_signal || !result.recomputed_pairs.is_empty() {
                Some(result.symbol)
            } else {
                None
            }
        }
        Ok(Err(broadcast::error::RecvError::Lagged(skipped))) => {
            log::warn!(
                "spot_spot_taker_arbitrage websocket book event receiver lagged skipped={skipped}; waiting for next event"
            );
            None
        }
        Ok(Err(broadcast::error::RecvError::Closed)) => {
            log::warn!("spot_spot_taker_arbitrage websocket book event stream closed");
            None
        }
        Err(_) => None,
    }
}

fn symbols_for_market_data_tick(
    config: &SpotSpotTakerArbitrageConfig,
    updated_symbol: &Option<String>,
) -> Vec<String> {
    match config.market_data_mode {
        MarketDataMode::RestPolling => config.symbols.clone(),
        MarketDataMode::WebsocketCache => updated_symbol.iter().cloned().collect(),
        MarketDataMode::Replay => Vec::new(),
    }
}

fn log_warn_once(key: impl Into<String>, message: impl FnOnce() -> String) {
    let key = key.into();
    let keys = SPOT_SPOT_WARN_ONCE_KEYS.get_or_init(|| Mutex::new(BTreeSet::new()));
    match keys.lock() {
        Ok(mut keys) => {
            if keys.insert(key) {
                log::warn!("{}", message());
            }
        }
        Err(_) => log::warn!("{}", message()),
    }
}

#[derive(Debug, Clone)]
struct SpotOrderPlanResult {
    submitted: bool,
    filled: bool,
    terminal_unfilled: bool,
    timed_out: bool,
    cancelled: bool,
    order_id: Option<String>,
    error: Option<String>,
}

impl SpotOrderPlanResult {
    fn dry_run(plan: &LiveDryRunOrderPlan) -> Self {
        Self {
            submitted: false,
            filled: plan.validation_result.passed,
            terminal_unfilled: false,
            timed_out: false,
            cancelled: false,
            order_id: None,
            error: plan.rejection_reason.clone(),
        }
    }

    fn submit_error(error: impl Into<String>) -> Self {
        Self {
            submitted: false,
            filled: false,
            terminal_unfilled: false,
            timed_out: false,
            cancelled: false,
            order_id: None,
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Default)]
struct LiveInventoryCache {
    balances_by_exchange: HashMap<SpotVenue, Vec<AssetBalance>>,
    last_exchange_sync_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl LiveInventoryCache {
    async fn sync_if_due(
        &mut self,
        config: &SpotSpotTakerArbitrageConfig,
        inventory: &PaperInventory,
        mexc: &MexcSpotClient,
        coinex: &CoinExSpotClient,
        gateio: &GateIoSpotClient,
        bitget: &BitgetSpotClient,
    ) {
        let now = chrono::Utc::now();
        let force = self.balances_by_exchange.is_empty()
            || self.last_exchange_sync_at.is_none_or(|last| {
                now.signed_duration_since(last).num_seconds()
                    >= LIVE_INVENTORY_EXCHANGE_SYNC_INTERVAL_SECONDS
            });
        if !force {
            return;
        }
        for exchange in configured_spot_venues(&config.exchanges) {
            let balances =
                live_or_inventory_balances(exchange, inventory, mexc, coinex, gateio, bitget).await;
            self.balances_by_exchange.insert(exchange, balances);
        }
        self.last_exchange_sync_at = Some(now);
    }

    async fn inventory_views(
        &self,
        inventory: &PaperInventory,
        disabled_registry: &DisabledRegistry,
        book_cache: &BookCache,
    ) -> Vec<InventoryView> {
        if self
            .balances_by_exchange
            .values()
            .any(|balances| !balances.is_empty())
        {
            let mut views = Vec::new();
            for (exchange, balances) in &self.balances_by_exchange {
                views.extend(
                    asset_balance_views(*exchange, balances.clone(), disabled_registry, book_cache)
                        .await,
                );
            }
            views.sort_by(|left, right| {
                (left.exchange.as_str(), left.asset.as_str())
                    .cmp(&(right.exchange.as_str(), right.asset.as_str()))
            });
            return views;
        }
        inventory_views(inventory, disabled_registry, book_cache).await
    }

    fn balances_for_exchange(
        &self,
        exchange: SpotVenue,
        inventory: &PaperInventory,
    ) -> Vec<AssetBalance> {
        self.balances_by_exchange
            .get(&exchange)
            .cloned()
            .unwrap_or_else(|| inventory_balances_for_exchange(inventory, exchange))
    }

    fn apply_filled_order(&mut self, plan: &LiveDryRunOrderPlan) {
        let Some(exchange) = spot_venue_from_exchange(&plan.exchange) else {
            return;
        };
        let balances = self.balances_by_exchange.entry(exchange).or_default();
        let base_asset = plan
            .symbol_rule_snapshot
            .base_asset
            .trim()
            .to_ascii_uppercase();
        let quote_asset = plan
            .symbol_rule_snapshot
            .quote_asset
            .trim()
            .to_ascii_uppercase();
        let fee = plan.fee_estimate.fee_amount.max(0.0);
        match plan.side {
            OrderSide::Buy => {
                adjust_cached_balance(balances, &quote_asset, -(plan.notional + fee));
                adjust_cached_balance(balances, &base_asset, plan.quantity);
            }
            OrderSide::Sell => {
                adjust_cached_balance(balances, &base_asset, -plan.quantity);
                adjust_cached_balance(balances, &quote_asset, plan.notional - fee);
            }
        }
    }
}

fn adjust_cached_balance(balances: &mut Vec<AssetBalance>, asset: &str, delta: f64) {
    let asset = asset.trim().to_ascii_uppercase();
    if let Some(balance) = balances
        .iter_mut()
        .find(|balance| balance.asset.eq_ignore_ascii_case(&asset))
    {
        balance.total = (balance.total + delta).max(0.0);
        balance.available = (balance.available + delta).max(0.0);
        balance.effective_available = (balance.available - balance.locally_reserved).max(0.0);
        return;
    }
    if delta > 0.0 {
        balances.push(AssetBalance::new(asset, delta, delta, 0.0));
    }
}

#[allow(clippy::too_many_arguments)]
async fn record_or_submit_spot_order_plan(
    config: &SpotSpotTakerArbitrageConfig,
    mut plan: LiveDryRunOrderPlan,
    monitoring: &Option<MonitoringState>,
    live_inventory_cache: Option<&mut LiveInventoryCache>,
    mexc: &MexcSpotClient,
    coinex: &CoinExSpotClient,
    gateio: &GateIoSpotClient,
    bitget: &BitgetSpotClient,
) -> Result<SpotOrderPlanResult> {
    let live_mode = spot_live_mode(config);
    if live_mode {
        apply_live_order_safety_checks(config, &mut plan);
    }
    append_live_dry_run_plan(&config.live_dry_run.output_path, &plan)?;
    if let Some(state) = monitoring {
        state.record_live_dry_run_order(plan.clone());
    }
    if !live_mode {
        return Ok(SpotOrderPlanResult::dry_run(&plan));
    }
    if !plan.validation_result.passed {
        log::warn!(
            "现货套利订单被安全门拦截 intent={} plan_id={} exchange={} symbol={} side={:?} order_type={:?} quantity={} notional={:.8} reason={}",
            plan.intent,
            plan.plan_id,
            plan.exchange,
            plan.symbol,
            plan.side,
            plan.order_type,
            plan.quantity,
            plan.notional,
            plan.rejection_reason
                .clone()
                .unwrap_or_else(|| "validation failed".to_string())
        );
        return Ok(SpotOrderPlanResult::dry_run(&plan));
    }
    let client = live_spot_exchange_client(&plan.exchange, mexc, coinex, gateio, bitget)?;
    let response = client.place_order(plan.order_request.clone()).await;
    match response {
        Ok(order) => {
            log::info!(
                "spot live order submitted plan_id={} exchange={} symbol={} side={:?} quantity={} price={:?} order_id={} status={:?}",
                plan.plan_id,
                plan.exchange,
                plan.symbol,
                plan.side,
                plan.quantity,
                plan.price,
                order.order_id,
                order.status
            );
            publish_live_open_orders(monitoring, config, mexc, coinex, gateio, bitget).await;
            let result = wait_for_spot_order_fill(config, &plan, order, client).await;
            publish_live_open_orders(monitoring, config, mexc, coinex, gateio, bitget).await;
            if result.filled {
                adjust_live_opening_notional(&plan);
                if let Some(cache) = live_inventory_cache {
                    cache.apply_filled_order(&plan);
                }
            }
            release_terminal_live_spot_reservation(&plan, &result, mexc, coinex, gateio, bitget);
            log::info!(
                "现货套利订单结果 intent={} plan_id={} exchange={} symbol={} side={:?} submitted={} filled={} timed_out={} cancelled={} order_id={:?} error={:?}",
                plan.intent,
                plan.plan_id,
                plan.exchange,
                plan.symbol,
                plan.side,
                result.submitted,
                result.filled,
                result.timed_out,
                result.cancelled,
                result.order_id,
                result.error
            );
            Ok(result)
        }
        Err(error) => {
            let message = format!(
                "spot live order submit failed plan_id={} exchange={} symbol={} side={:?}: {}",
                plan.plan_id, plan.exchange, plan.symbol, plan.side, error
            );
            log::warn!("{message}");
            Ok(SpotOrderPlanResult::submit_error(message))
        }
    }
}

fn release_terminal_live_spot_reservation(
    plan: &LiveDryRunOrderPlan,
    result: &SpotOrderPlanResult,
    mexc: &MexcSpotClient,
    coinex: &CoinExSpotClient,
    gateio: &GateIoSpotClient,
    bitget: &BitgetSpotClient,
) {
    if !result.filled && !result.cancelled && !result.terminal_unfilled {
        return;
    }
    let Some(manager) = live_spot_reservation_manager(&plan.exchange, mexc, coinex, gateio, bitget)
    else {
        return;
    };
    let release_amount = manager.locally_reserved(&plan.exchange, &plan.required_balance_asset);
    if release_amount <= 0.0 {
        return;
    }
    if let Err(error) = manager.release_asset_reservation(
        &plan.exchange,
        &plan.required_balance_asset,
        release_amount,
    ) {
        log::warn!(
            "spot live reservation release failed plan_id={} exchange={} asset={} amount={}: {}",
            plan.plan_id,
            plan.exchange,
            plan.required_balance_asset,
            release_amount,
            error
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn live_spot_reservation_manager(
    exchange: &str,
    mexc: &MexcSpotClient,
    coinex: &CoinExSpotClient,
    gateio: &GateIoSpotClient,
    bitget: &BitgetSpotClient,
) -> Option<crate::exchanges::spot_reservation::BalanceReservationManager> {
    match spot_venue_from_exchange(exchange)? {
        SpotVenue::Mexc => Some(mexc.reservations()),
        SpotVenue::CoinEx => Some(coinex.reservations()),
        SpotVenue::GateIo => Some(gateio.reservations()),
        SpotVenue::Bitget => Some(bitget.reservations()),
    }
}

async fn publish_live_open_orders(
    monitoring: &Option<MonitoringState>,
    config: &SpotSpotTakerArbitrageConfig,
    mexc: &MexcSpotClient,
    coinex: &CoinExSpotClient,
    gateio: &GateIoSpotClient,
    bitget: &BitgetSpotClient,
) {
    let Some(state) = monitoring else {
        return;
    };
    state.publish_open_orders(live_open_orders(config, mexc, coinex, gateio, bitget).await);
}

fn apply_live_order_safety_checks(
    config: &SpotSpotTakerArbitrageConfig,
    plan: &mut LiveDryRunOrderPlan,
) {
    let gate_reasons = small_live_order_block_reasons(config, plan);
    if !gate_reasons.is_empty() {
        plan.validation_result.passed = false;
        let message = gate_reasons.join("; ");
        plan.validation_result.checks.push(LiveDryRunCheck {
            name: "small_live_gate".to_string(),
            passed: false,
            message,
        });
    }
    plan.rejection_reason = live_order_rejection_reason(plan);
    plan.would_submit = plan.validation_result.passed;
}

fn live_plan_group_ready_for_submit(
    config: &SpotSpotTakerArbitrageConfig,
    mut plans: Vec<LiveDryRunOrderPlan>,
) -> Vec<LiveDryRunOrderPlan> {
    if !spot_live_mode(config) {
        return plans;
    }
    for plan in &mut plans {
        apply_live_order_safety_checks(config, plan);
    }
    let blocked = plans
        .iter()
        .find(|plan| !plan.validation_result.passed)
        .map(|plan| {
            format!(
                "intent={} exchange={} symbol={} side={:?} reason={}",
                plan.intent,
                plan.exchange,
                plan.symbol,
                plan.side,
                plan.rejection_reason
                    .clone()
                    .unwrap_or_else(|| "validation failed".to_string())
            )
        });
    if let Some(reason) = blocked {
        for plan in &plans {
            append_live_dry_run_plan(&config.live_dry_run.output_path, plan).ok();
        }
        log::warn!(
            "现货套利订单组预检失败，整组跳过 plan_count={} first_blocked={}",
            plans.len(),
            reason
        );
        return Vec::new();
    }
    plans
}

fn small_live_order_block_reason(
    config: &SpotSpotTakerArbitrageConfig,
    plan: &LiveDryRunOrderPlan,
) -> Option<String> {
    let reasons = small_live_order_block_reasons(config, plan);
    (!reasons.is_empty()).then(|| reasons.join("; "))
}

fn small_live_order_block_reasons(
    config: &SpotSpotTakerArbitrageConfig,
    plan: &LiveDryRunOrderPlan,
) -> Vec<String> {
    let mut reasons = Vec::new();
    let gate = &config.small_live_gate;
    if plan.intent == "control_stop_liquidate" {
        if !config.kill_switch.allow_live_orders {
            reasons.push(
                "kill_switch.allow_live_orders must be true for live spot liquidation orders"
                    .to_string(),
            );
        }
        if config
            .kill_switch
            .initial_state
            .eq_ignore_ascii_case("triggered")
        {
            reasons.push("kill_switch.initial_state must not be triggered".to_string());
        }
        if plan.side != OrderSide::Sell || plan.order_type != OrderType::IOC {
            reasons.push(
                "control_stop_liquidate must be an IOC sell order to reduce spot inventory"
                    .to_string(),
            );
        }
        return reasons;
    }
    if !gate.enabled {
        reasons.push("small_live_gate.enabled must be true for live spot orders".to_string());
    }
    if !gate.explicit_live_confirmation {
        reasons.push(
            "small_live_gate.explicit_live_confirmation must be true for live spot orders"
                .to_string(),
        );
    }
    if !config.kill_switch.allow_live_orders {
        reasons.push("kill_switch.allow_live_orders must be true for live spot orders".to_string());
    }
    if config
        .kill_switch
        .initial_state
        .eq_ignore_ascii_case("triggered")
    {
        reasons.push("kill_switch.initial_state must not be triggered".to_string());
    }
    if plan.notional > gate.max_notional_per_order + 1e-12 {
        reasons.push(format!(
            "plan notional {:.8} exceeds small_live_gate.max_notional_per_order {:.8}",
            plan.notional, gate.max_notional_per_order
        ));
    }
    if !gate.enabled_symbols.is_empty()
        && !gate
            .enabled_symbols
            .iter()
            .any(|symbol| symbol.eq_ignore_ascii_case(&plan.symbol))
    {
        reasons.push(format!(
            "symbol {} is not listed in small_live_gate.enabled_symbols",
            plan.symbol
        ));
    }
    if !gate.enabled_exchanges.is_empty()
        && !gate
            .enabled_exchanges
            .iter()
            .any(|exchange| exchange.eq_ignore_ascii_case(&plan.exchange))
    {
        reasons.push(format!(
            "exchange {} is not listed in small_live_gate.enabled_exchanges",
            plan.exchange
        ));
    }
    if is_exit_order_plan(plan) {
        return reasons;
    }
    let opening = SPOT_SPOT_LIVE_OPENING_NOTIONAL_USDT
        .get_or_init(|| Mutex::new(0.0))
        .lock()
        .map(|total| *total)
        .unwrap_or(f64::INFINITY);
    if opening + plan.notional > gate.max_total_notional + 1e-12 {
        reasons.push(format!(
            "open live entry notional {:.8} + plan {:.8} exceeds small_live_gate.max_total_notional {:.8}",
            opening, plan.notional, gate.max_total_notional
        ));
    }
    reasons
}

fn live_order_rejection_reason(plan: &LiveDryRunOrderPlan) -> Option<String> {
    if plan.validation_result.passed {
        return None;
    }
    let checks = plan
        .validation_result
        .checks
        .iter()
        .filter(|check| !check.passed)
        .map(|check| format!("{}: {}", check.name, check.message))
        .collect::<Vec<_>>();
    if checks.is_empty() {
        plan.rejection_reason.clone()
    } else {
        Some(checks.join("; "))
    }
}

fn is_exit_order_plan(plan: &LiveDryRunOrderPlan) -> bool {
    plan.intent.starts_with("inactive_exit")
        || plan.intent.starts_with("disable_exit")
        || plan.intent.starts_with("liquidation")
        || plan.intent.starts_with("inventory_trim_excess")
        || plan.intent.starts_with("inventory_top_up")
        || plan.intent.starts_with("inventory_rebalance")
        || plan.intent.starts_with("blocked_inventory_rebalance")
}

fn adjust_live_opening_notional(plan: &LiveDryRunOrderPlan) {
    let delta = match plan.side {
        OrderSide::Buy => plan.notional.max(0.0),
        OrderSide::Sell => -plan.notional.max(0.0),
    };
    if let Ok(mut total) = SPOT_SPOT_LIVE_OPENING_NOTIONAL_USDT
        .get_or_init(|| Mutex::new(0.0))
        .lock()
    {
        *total = (*total + delta).max(0.0);
    }
}

fn live_dual_taker_trade_record(
    opportunity: &OpportunityRecord,
    filled_plans: &[LiveDryRunOrderPlan],
) -> Option<SimulatedTradeRecord> {
    let buy_plan = filled_plans
        .iter()
        .find(|plan| plan.intent == "dual_taker_arbitrage" && plan.side == OrderSide::Buy)?;
    let sell_plan = filled_plans
        .iter()
        .find(|plan| plan.intent == "dual_taker_arbitrage" && plan.side == OrderSide::Sell)?;
    if buy_plan.quantity <= 0.0 || sell_plan.quantity <= 0.0 {
        return None;
    }

    let quantity = buy_plan.quantity.min(sell_plan.quantity);
    if quantity <= 0.0 {
        return None;
    }

    let buy_avg_price = buy_plan.notional / buy_plan.quantity;
    let sell_avg_price = sell_plan.notional / sell_plan.quantity;
    if !buy_avg_price.is_finite() || !sell_avg_price.is_finite() {
        return None;
    }

    let buy_notional = buy_avg_price * quantity;
    let sell_notional = sell_avg_price * quantity;
    let buy_fee = buy_plan.fee_estimate.fee_amount * (quantity / buy_plan.quantity);
    let sell_fee = sell_plan.fee_estimate.fee_amount * (quantity / sell_plan.quantity);
    let gross_pnl = sell_notional - buy_notional;
    let fill_ratio = if opportunity.quantity > 0.0 {
        (quantity / opportunity.quantity).clamp(0.0, 1.0)
    } else {
        1.0
    };
    let slippage_cost = opportunity.estimated_slippage_cost * fill_ratio;
    let capital_cost = opportunity.estimated_capital_cost * fill_ratio;
    let transfer_cost = opportunity.estimated_transfer_cost * fill_ratio;
    let inventory_rebalance_cost = opportunity.estimated_inventory_rebalance_cost * fill_ratio;
    let latency_penalty_cost = opportunity.estimated_latency_penalty_cost * fill_ratio;
    let net_pnl = gross_pnl
        - buy_fee
        - sell_fee
        - slippage_cost
        - capital_cost
        - transfer_cost
        - inventory_rebalance_cost
        - latency_penalty_cost;

    Some(SimulatedTradeRecord {
        timestamp: chrono::Utc::now(),
        symbol: opportunity.symbol.clone(),
        buy_exchange: buy_plan.exchange.clone(),
        sell_exchange: sell_plan.exchange.clone(),
        buy_avg_price,
        sell_avg_price,
        quantity,
        notional: buy_notional,
        buy_fee,
        sell_fee,
        gross_pnl,
        net_pnl,
        pnl_category: TradePnlCategory::Arbitrage,
        slippage_cost,
        capital_cost,
        transfer_cost,
        inventory_rebalance_cost,
        latency_penalty_cost,
        latency_ms: opportunity
            .buy_latency_ms
            .unwrap_or_default()
            .max(opportunity.sell_latency_ms.unwrap_or_default()),
        order_book_age_ms: opportunity
            .buy_book_age_ms
            .max(opportunity.sell_book_age_ms),
        execution_mode: "live_dual_taker".to_string(),
    })
}

async fn wait_for_spot_order_fill(
    config: &SpotSpotTakerArbitrageConfig,
    plan: &LiveDryRunOrderPlan,
    initial_order: OrderResponse,
    client: &dyn ExchangeClient,
) -> SpotOrderPlanResult {
    let order_id = initial_order.order_id.clone();
    if order_filled(&initial_order, plan.quantity) {
        return SpotOrderPlanResult {
            submitted: true,
            filled: true,
            terminal_unfilled: false,
            timed_out: false,
            cancelled: false,
            order_id: Some(order_id),
            error: None,
        };
    }
    if terminal_unfilled(initial_order.status) {
        return SpotOrderPlanResult {
            submitted: true,
            filled: false,
            terminal_unfilled: true,
            timed_out: false,
            cancelled: false,
            order_id: Some(order_id),
            error: Some(format!(
                "terminal unfilled status {:?}",
                initial_order.status
            )),
        };
    }

    let timeout_ms = live_order_timeout_ms(config, plan);
    let poll_ms = config
        .order_reconciliation
        .poll_interval_ms
        .clamp(100, 5_000);
    let started = chrono::Utc::now();
    loop {
        if chrono::Utc::now()
            .signed_duration_since(started)
            .num_milliseconds()
            >= timeout_ms as i64
        {
            let cancelled = cancel_live_spot_order(plan, &order_id, client)
                .await
                .is_ok();
            return SpotOrderPlanResult {
                submitted: true,
                filled: false,
                terminal_unfilled: false,
                timed_out: true,
                cancelled,
                order_id: Some(order_id),
                error: Some(format!("order timed out after {timeout_ms}ms")),
            };
        }

        sleep(Duration::from_millis(poll_ms)).await;
        match get_live_spot_order(plan, &order_id, client).await {
            Ok(order) if order_filled(&order, plan.quantity) => {
                return SpotOrderPlanResult {
                    submitted: true,
                    filled: true,
                    terminal_unfilled: false,
                    timed_out: false,
                    cancelled: false,
                    order_id: Some(order_id),
                    error: None,
                };
            }
            Ok(order) if terminal_unfilled(order.status) => {
                return SpotOrderPlanResult {
                    submitted: true,
                    filled: false,
                    terminal_unfilled: true,
                    timed_out: false,
                    cancelled: false,
                    order_id: Some(order_id),
                    error: Some(format!("terminal unfilled status {:?}", order.status)),
                };
            }
            Ok(_) => {}
            Err(error) => {
                log::warn!(
                    "spot live order poll failed plan_id={} exchange={} symbol={} order_id={}: {}",
                    plan.plan_id,
                    plan.exchange,
                    plan.symbol,
                    order_id,
                    error
                );
            }
        }
    }
}

fn live_order_timeout_ms(config: &SpotSpotTakerArbitrageConfig, plan: &LiveDryRunOrderPlan) -> u64 {
    if matches!(
        plan.order_type,
        OrderType::Market | OrderType::IOC | OrderType::FOK
    ) {
        config.request_timeout_ms.max(1_000)
    } else if plan.intent.starts_with("initial_entry") || plan.intent == "maker_taker_arbitrage" {
        config.entry_order_timeout_seconds.saturating_mul(1_000)
    } else if plan.intent.starts_with("inactive_exit") {
        config.exit_order_timeout_seconds.saturating_mul(1_000)
    } else {
        config.entry_order_timeout_seconds.saturating_mul(1_000)
    }
}

fn order_filled(order: &OrderResponse, planned_quantity: f64) -> bool {
    order.status == OrderStatus::Filled
        || (planned_quantity > 0.0 && order.filled_quantity + 1e-12 >= planned_quantity)
}

fn terminal_unfilled(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Cancelled
            | OrderStatus::Rejected
            | OrderStatus::Expired
            | OrderStatus::Unknown
    )
}

async fn get_live_spot_order(
    plan: &LiveDryRunOrderPlan,
    order_id: &str,
    client: &dyn ExchangeClient,
) -> Result<OrderResponse> {
    client
        .get_order(&plan.exchange_symbol, order_id)
        .await
        .map_err(|error| anyhow!(error))
}

async fn cancel_live_spot_order(
    plan: &LiveDryRunOrderPlan,
    order_id: &str,
    client: &dyn ExchangeClient,
) -> Result<()> {
    if !matches!(plan.order_type, OrderType::Limit | OrderType::PostOnly) {
        return Ok(());
    }
    let request = CancelOrderRequest {
        market_type: plan.market_type,
        symbol: plan.exchange_symbol.clone(),
        order_id: Some(order_id.to_string()),
        client_order_id: plan.client_order_id.clone(),
    };
    client
        .cancel_order(request)
        .await
        .map(|_| ())
        .map_err(|error| anyhow!(error))
}

#[allow(clippy::too_many_arguments)]
fn live_spot_exchange_client<'a>(
    exchange: &str,
    mexc: &'a MexcSpotClient,
    coinex: &'a CoinExSpotClient,
    gateio: &'a GateIoSpotClient,
    bitget: &'a BitgetSpotClient,
) -> Result<&'a dyn ExchangeClient> {
    match spot_venue_from_exchange(exchange) {
        Some(SpotVenue::Mexc) => Ok(mexc),
        Some(SpotVenue::CoinEx) => Ok(coinex),
        Some(SpotVenue::GateIo) => Ok(gateio),
        Some(SpotVenue::Bitget) => Ok(bitget),
        None => Err(anyhow!("unsupported live spot order exchange {exchange}")),
    }
}

#[allow(clippy::too_many_arguments)]
fn attach_runtime_publisher_if_enabled(
    state: MonitoringState,
    config: &SpotSpotTakerArbitrageConfig,
    symbol_rules: &HashMap<String, CommonSymbolRules>,
    fee_model: &FeeModel,
    disabled_registry: &DisabledRegistry,
    kill_switch: &KillSwitch,
    book_cache: &BookCache,
    websocket_runtime: Option<&WebsocketMarketDataRuntime>,
    mexc: MexcSpotClient,
    coinex: CoinExSpotClient,
    gateio: GateIoSpotClient,
    bitget: BitgetSpotClient,
) -> MonitoringState {
    if !config.spot_symbol_control.enabled || !config.spot_symbol_control.runtime_publisher.enabled
    {
        return state;
    }
    let Some(control) = state.control().cloned() else {
        log::warn!("runtime publisher requested but spot control service is unavailable");
        return state;
    };

    let mut exchange_clients = BTreeMap::<String, RuntimeExchangeClient>::new();
    match configured_spot_pair(&config.exchanges) {
        (SpotVenue::Mexc, SpotVenue::CoinEx) => {
            exchange_clients.insert("mexc".to_string(), Arc::new(mexc));
            exchange_clients.insert("coinex".to_string(), Arc::new(coinex));
        }
        (SpotVenue::GateIo, SpotVenue::Bitget) => {
            exchange_clients.insert("gateio".to_string(), Arc::new(gateio));
            exchange_clients.insert("bitget".to_string(), Arc::new(bitget));
        }
        _ => unreachable!("unsupported configured spot pair"),
    }

    let registry = crate::exchanges::symbol_registry::UnifiedSymbolRegistry::from_rules(
        flatten_spot_symbol_rules(symbol_rules),
    );
    let publisher = SpotControlRuntimePublisher::new(
        config.spot_symbol_control.runtime_publisher.clone(),
        SpotControlRuntimePublisherDeps {
            exchange_clients,
            symbol_registry: registry,
            book_cache: book_cache.shared(),
            book_health: websocket_runtime
                .map(|runtime| runtime.health.clone())
                .unwrap_or_default(),
            fee_model: fee_model.clone(),
            disabled_registry: disabled_registry.clone(),
            reservation_manager:
                crate::exchanges::spot_reservation::BalanceReservationManager::default(),
            reconciliation_services: RuntimeReconciliationServices {
                unmanaged_positions: disabled_registry.unmanaged_positions().to_vec(),
                order_reconciliation_state: None,
            },
            kill_switch: Some(kill_switch.clone()),
            live_preflight: None,
            small_live_gate: None,
            recorder_health: Some(RecorderHealthView {
                book_recording_enabled: config.websocket.record_books,
                opportunity_recording_enabled: true,
                trade_recording_enabled: true,
                output_paths: vec![config.jsonl_path.clone(), config.csv_path.clone()],
                ..RecorderHealthView::default()
            }),
            snapshot_config: config.spot_symbol_control.runtime_snapshot.clone(),
            snapshot_store: snapshot_store_from_config(
                &config.spot_symbol_control.runtime_publisher,
            ),
        },
    );
    let (_, rx) = tokio::sync::watch::channel(Vec::new());
    tokio::spawn(publisher.clone().run(control, rx));
    state.with_runtime_publisher(publisher)
}

fn flatten_spot_symbol_rules(
    rules: &HashMap<String, CommonSymbolRules>,
) -> Vec<crate::exchanges::unified::SymbolRule> {
    let mut values = Vec::new();
    for item in rules.values() {
        values.push(item.mexc.clone());
        values.push(item.coinex.clone());
        if let Some(rule) = &item.gateio {
            values.push(rule.clone());
        }
        if let Some(rule) = &item.bitget {
            values.push(rule.clone());
        }
        if let Some(rule) = &item.kucoin {
            values.push(rule.clone());
        }
    }
    values
}

fn publish_arbitrage_analysis(
    monitoring: &Option<MonitoringState>,
    config: &SpotSpotTakerArbitrageConfig,
    opportunity: &OpportunityRecord,
    rules: &CommonSymbolRules,
    left_book: &OrderBookSnapshot,
    right_book: &OrderBookSnapshot,
    fee_model: &FeeModel,
) {
    if !config.arbitrage_scanner.enabled {
        return;
    }
    let Some(relationship) =
        spot_spot_arbitrage_relationship(opportunity, rules, left_book, right_book, fee_model)
    else {
        return;
    };
    if let Some(state) = monitoring {
        state.record_arbitrage_relationship(relationship.clone());
    }
    for target_notional in &config.arbitrage_scanner.notionals_usdt {
        if *target_notional <= 0.0 {
            continue;
        }
        let analysis = analyze_relationship(
            &relationship,
            *target_notional,
            &config.arbitrage_scanner,
            None,
        );
        if let Some(path) = &config.arbitrage_scanner.output_path {
            if let Err(error) = record_analysis_jsonl(path, &analysis) {
                log::warn!("failed to record arbitrage analytics JSONL: {error}");
            }
        }
        if let Some(state) = monitoring {
            state.record_arbitrage_opportunity(analysis);
        }
    }
}

fn spot_spot_arbitrage_relationship(
    opportunity: &OpportunityRecord,
    rules: &CommonSymbolRules,
    left_book: &OrderBookSnapshot,
    right_book: &OrderBookSnapshot,
    fee_model: &FeeModel,
) -> Option<ArbitrageRelationship> {
    let buy_book = book_for_exchange(&opportunity.buy_exchange, left_book, right_book)?;
    let sell_book = book_for_exchange(&opportunity.sell_exchange, left_book, right_book)?;
    let buy_venue = spot_venue_from_exchange(&opportunity.buy_exchange)?;
    let sell_venue = spot_venue_from_exchange(&opportunity.sell_exchange)?;
    let buy_rule = rules.for_exchange(buy_venue).clone();
    let sell_rule = rules.for_exchange(sell_venue).clone();
    let buy_fee = fee_model
        .lookup_for_side(
            &FeeLookupKey {
                exchange: opportunity.buy_exchange.clone(),
                market_type: MarketType::Spot,
                symbol: Some(opportunity.symbol.clone()),
                liquidity_role: FeeRole::Taker,
            },
            Some(OrderSide::Buy),
        )
        .effective_rate;
    let sell_fee = fee_model
        .lookup_for_side(
            &FeeLookupKey {
                exchange: opportunity.sell_exchange.clone(),
                market_type: MarketType::Spot,
                symbol: Some(opportunity.symbol.clone()),
                liquidity_role: FeeRole::Taker,
            },
            Some(OrderSide::Sell),
        )
        .effective_rate;
    Some(ArbitrageRelationship {
        relationship_type: ArbitrageRelationshipType::SpotSpot,
        base_asset: buy_rule.base_asset.clone(),
        quote_asset: buy_rule.quote_asset.clone(),
        settlement_asset_optional: None,
        buy_leg: MarketLeg {
            exchange: opportunity.buy_exchange.clone(),
            market_type: MarketType::Spot,
            internal_symbol: opportunity.symbol.clone(),
            exchange_symbol: buy_rule.exchange_symbol.clone(),
            side: OrderSide::Buy,
            order_book: buy_book.clone(),
            fee_rate: buy_fee,
            symbol_rule: buy_rule,
            funding_rate_optional: None,
            margin_info_optional: None,
        },
        sell_leg: MarketLeg {
            exchange: opportunity.sell_exchange.clone(),
            market_type: MarketType::Spot,
            internal_symbol: opportunity.symbol.clone(),
            exchange_symbol: sell_rule.exchange_symbol.clone(),
            side: OrderSide::Sell,
            order_book: sell_book.clone(),
            fee_rate: sell_fee,
            symbol_rule: sell_rule,
            funding_rate_optional: None,
            margin_info_optional: None,
        },
    })
}

fn book_for_exchange<'a>(
    exchange: &str,
    left_book: &'a OrderBookSnapshot,
    right_book: &'a OrderBookSnapshot,
) -> Option<&'a OrderBookSnapshot> {
    if exchange.eq_ignore_ascii_case(&left_book.exchange) {
        Some(left_book)
    } else if exchange.eq_ignore_ascii_case(&right_book.exchange) {
        Some(right_book)
    } else {
        None
    }
}

fn spot_venue_from_exchange(exchange: &str) -> Option<SpotVenue> {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "mexc" => Some(SpotVenue::Mexc),
        "coinex" => Some(SpotVenue::CoinEx),
        "gate" | "gateio" | "gate.io" => Some(SpotVenue::GateIo),
        "bitget" => Some(SpotVenue::Bitget),
        _ => None,
    }
}

fn arbitrage_symbol_allowed(
    enabled_symbols: &mut BTreeSet<String>,
    symbol: &str,
    max_enabled_symbols: usize,
) -> bool {
    if max_enabled_symbols == 0 {
        return false;
    }
    let symbol = symbol.trim().to_ascii_uppercase();
    if enabled_symbols.contains(&symbol) {
        return true;
    }
    if enabled_symbols.len() >= max_enabled_symbols {
        return false;
    }
    enabled_symbols.insert(symbol);
    true
}

fn active_or_opening_arbitrage_symbol_count(
    states: &BTreeMap<String, ArbitragePairRuntime>,
) -> usize {
    states
        .values()
        .filter(|state| {
            matches!(
                state.status,
                ArbitragePairStatus::Opening | ArbitragePairStatus::Arbitraging
            )
        })
        .count()
}

fn should_scan_arbitrage_opportunities(
    states: &BTreeMap<String, ArbitragePairRuntime>,
    max_enabled_symbols: usize,
) -> bool {
    max_enabled_symbols > 0
        && active_or_opening_arbitrage_symbol_count(states) < max_enabled_symbols
}

async fn release_blocked_arbitrage_symbols(
    enabled_symbols: &mut BTreeSet<String>,
    state: &MonitoringState,
) {
    let Some(control) = state.control() else {
        return;
    };
    let read_model = control.read_model().await;
    let blocked = read_model
        .symbols
        .iter()
        .filter(|symbol| symbol.lifecycle_state.blocks_new_arbitrage())
        .map(|symbol| symbol.internal_symbol.trim().to_ascii_uppercase())
        .collect::<BTreeSet<_>>();
    release_arbitrage_symbols(enabled_symbols, &blocked);
}

async fn sync_control_arbitraging_symbol(
    monitoring: &Option<MonitoringState>,
    config: &SpotSpotTakerArbitrageConfig,
    symbol: &str,
) {
    let Some(state) = monitoring else {
        return;
    };
    let Some(control) = state.control() else {
        return;
    };
    let exchanges = configured_spot_venues(&config.exchanges)
        .into_iter()
        .map(|venue| venue.as_str().to_string())
        .collect::<Vec<_>>();
    let directions = spot_control_directions(&exchanges);
    control
        .sync_runtime_active_symbol(
            normalize_symbol(symbol),
            exchanges,
            directions,
            "strategy_runtime",
        )
        .await;
}

async fn manual_control_allows_arbitrage(
    control: &SpotControlService,
    symbol: &str,
    buy_exchange: &str,
    sell_exchange: &str,
    trading_mode: &str,
) -> bool {
    let symbol = normalize_symbol(symbol);
    control
        .read_model()
        .await
        .symbols
        .into_iter()
        .find(|managed| normalize_symbol(&managed.internal_symbol) == symbol)
        .is_some_and(|managed| {
            managed.lifecycle_state == SpotSymbolLifecycleState::Active
                && managed.allows_direction(buy_exchange, sell_exchange)
                && managed.enable_mode.allows_strategy_mode(trading_mode)
        })
}

fn dual_taker_direction_preserves_inventory_targets(
    config: &SpotSpotTakerArbitrageConfig,
    opportunity: &OpportunityRecord,
    rules: &CommonSymbolRules,
    buy_balances: &[AssetBalance],
    sell_balances: &[AssetBalance],
) -> bool {
    if !config.inventory_rebalance.enabled
        || config.inventory_rebalance.target_total_notional_usdt <= 0.0
    {
        return true;
    }
    let venue_count = configured_spot_venues(&config.exchanges).len().max(1) as f64;
    let per_venue_target = config.inventory_rebalance.target_total_notional_usdt / venue_count;
    let tolerance = config.min_notional_per_trade.max(0.0);
    let (Some(buy_venue), Some(sell_venue)) = (
        spot_venue_from_exchange(&opportunity.buy_exchange),
        spot_venue_from_exchange(&opportunity.sell_exchange),
    ) else {
        return true;
    };
    let buy_rule = rules.for_exchange(buy_venue);
    let sell_rule = rules.for_exchange(sell_venue);
    let buy_base_value =
        base_balance_notional(buy_balances, &buy_rule.base_asset, opportunity.buy_price);
    let sell_base_value =
        base_balance_notional(sell_balances, &sell_rule.base_asset, opportunity.sell_price);
    if buy_base_value > per_venue_target + tolerance {
        return false;
    }
    if sell_base_value + tolerance < per_venue_target {
        return false;
    }
    true
}

fn base_balance_notional(balances: &[AssetBalance], base_asset: &str, reference_price: f64) -> f64 {
    if reference_price <= 0.0 {
        return 0.0;
    }
    balances
        .iter()
        .find(|balance| balance.asset.eq_ignore_ascii_case(base_asset))
        .map(|balance| {
            balance
                .effective_available
                .max(balance.available)
                .max(balance.total)
                .max(0.0)
                * reference_price
        })
        .unwrap_or_default()
}

#[allow(clippy::too_many_arguments)]
fn restore_arbitraging_from_existing_inventory(
    config: &SpotSpotTakerArbitrageConfig,
    opportunity: &OpportunityRecord,
    rules: &CommonSymbolRules,
    left_book: &OrderBookSnapshot,
    right_book: &OrderBookSnapshot,
    live_inventory_cache: &LiveInventoryCache,
    inventory: &PaperInventory,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
    now: chrono::DateTime<chrono::Utc>,
    runtime: &mut ArbitragePairRuntime,
) -> bool {
    let (Some(buy_exchange), Some(sell_exchange)) = (
        spot_venue_from_exchange(&opportunity.buy_exchange),
        spot_venue_from_exchange(&opportunity.sell_exchange),
    ) else {
        return false;
    };
    let Some(buy_book) = book_for_exchange(&opportunity.buy_exchange, left_book, right_book) else {
        return false;
    };
    let Some(sell_book) = book_for_exchange(&opportunity.sell_exchange, left_book, right_book)
    else {
        return false;
    };
    let buy_balances = live_inventory_cache.balances_for_exchange(buy_exchange, inventory);
    let sell_balances = live_inventory_cache.balances_for_exchange(sell_exchange, inventory);
    let Ok(plans) = build_dual_taker_arbitrage_plans(
        config,
        opportunity,
        rules,
        buy_book,
        sell_book,
        &buy_balances,
        &sell_balances,
        disabled_registry,
        fee_model,
    ) else {
        return false;
    };
    let plans = live_plan_group_ready_for_submit(config, plans);
    let has_buy = plans.iter().any(|plan| plan.side == OrderSide::Buy);
    let has_sell = plans
        .iter()
        .any(|plan| plan.side == OrderSide::Sell && sell_balance_covers_plan(plan, &sell_balances));
    if has_buy && has_sell {
        runtime.mark_arbitraging(now);
        true
    } else {
        false
    }
}

fn sell_balance_covers_plan(plan: &LiveDryRunOrderPlan, balances: &[AssetBalance]) -> bool {
    let base_asset = plan
        .symbol_rule_snapshot
        .base_asset
        .trim()
        .to_ascii_uppercase();
    balances
        .iter()
        .find(|balance| balance.asset.eq_ignore_ascii_case(&base_asset))
        .is_some_and(|balance| balance.available >= plan.quantity)
}

fn order_dual_taker_plans_sell_first(plans: &mut [LiveDryRunOrderPlan]) {
    plans.sort_by_key(|plan| match (plan.intent.as_str(), plan.side) {
        ("dual_taker_arbitrage", OrderSide::Sell) => 0,
        ("dual_taker_arbitrage", OrderSide::Buy) => 1,
        ("maker_taker_arbitrage", OrderSide::Sell) => 0,
        ("maker_taker_arbitrage", OrderSide::Buy) => 1,
        _ => 2,
    });
}

fn one_sided_exposure_from_filled_plan(
    plan: &LiveDryRunOrderPlan,
    kind: OneSidedExposureKind,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<OneSidedExposure> {
    let exchange = spot_venue_from_exchange(&plan.exchange)?;
    let reference_price = if plan.quantity > 0.0 {
        plan.notional / plan.quantity
    } else {
        0.0
    };
    Some(OneSidedExposure {
        symbol: normalize_symbol(&plan.symbol),
        kind,
        exchange,
        quantity: plan.quantity,
        reference_price,
        notional: plan.notional,
        created_at: now,
    })
}

fn log_one_sided_exposure(exposure: &OneSidedExposure) {
    log::warn!(
        "现货套利单腿风险 symbol={} kind={:?} exchange={} quantity={:.8} reference_price={:.10} notional={:.8}",
        exposure.symbol,
        exposure.kind,
        exposure.exchange.as_str(),
        exposure.quantity,
        exposure.reference_price,
        exposure.notional
    );
}

async fn sync_control_closed_symbol(
    monitoring: &Option<MonitoringState>,
    symbol: &str,
    reason: &str,
) {
    let Some(state) = monitoring else {
        return;
    };
    let Some(control) = state.control() else {
        return;
    };
    control
        .sync_runtime_closed_symbol(normalize_symbol(symbol), "strategy_runtime", reason)
        .await;
}

fn spot_control_directions(exchanges: &[String]) -> Vec<EnabledDirection> {
    let mut directions = Vec::new();
    for buy_exchange in exchanges {
        for sell_exchange in exchanges {
            if buy_exchange != sell_exchange {
                directions.push(EnabledDirection {
                    buy_exchange: buy_exchange.clone(),
                    sell_exchange: sell_exchange.clone(),
                });
            }
        }
    }
    directions
}

#[allow(clippy::too_many_arguments)]
async fn execute_control_market_liquidations(
    config: &SpotSpotTakerArbitrageConfig,
    state: &MonitoringState,
    symbol_rules: &HashMap<String, CommonSymbolRules>,
    fee_model: &FeeModel,
    mexc: &MexcSpotClient,
    coinex: &CoinExSpotClient,
    gateio: &GateIoSpotClient,
    bitget: &BitgetSpotClient,
    enabled_symbols: &mut BTreeSet<String>,
    arbitrage_pair_states: &mut BTreeMap<String, ArbitragePairRuntime>,
    live_inventory_cache: &mut LiveInventoryCache,
    inventory: &PaperInventory,
    book_cache: &BookCache,
) -> Result<BTreeSet<String>> {
    let mut blocked_symbols = BTreeSet::new();
    let Some(control) = state.control().cloned() else {
        return Ok(blocked_symbols);
    };
    let mut read_model = control.read_model().await;
    let residual_candidates = read_model
        .symbols
        .iter()
        .filter(|symbol| control_lifecycle_should_replan_market_liquidation(symbol.lifecycle_state))
        .cloned()
        .collect::<Vec<_>>();
    for managed in residual_candidates {
        let symbol = normalize_symbol(&managed.internal_symbol);
        let Some(rules) = symbol_rules.get(&symbol) else {
            log::warn!("control residual liquidation skipped {symbol}: symbol rules unavailable");
            continue;
        };
        if !control_lifecycle_should_force_market_liquidation_replan(managed.lifecycle_state)
            && !managed_symbol_has_liquidatable_inventory(
                config,
                &managed,
                rules,
                live_inventory_cache,
                inventory,
                book_cache,
            )
            .await
        {
            continue;
        }
        if control_lifecycle_should_force_market_liquidation_replan(managed.lifecycle_state)
            && !managed_symbol_has_any_base_inventory(
                config,
                &managed,
                rules,
                live_inventory_cache,
                inventory,
            )
            .await
        {
            log::info!(
                "control residual liquidation finalizing empty stopped symbol={} state={:?}",
                symbol,
                managed.lifecycle_state
            );
        }
        let mut selected_exchanges =
            crate::control::spot_control::normalize_exchange_list(&managed.selected_exchanges);
        if selected_exchanges.is_empty() {
            selected_exchanges = configured_spot_venues(&config.exchanges)
                .into_iter()
                .map(|venue| venue.as_str().to_string())
                .collect::<Vec<_>>();
        }
        if selected_exchanges.is_empty() {
            log::warn!(
                "control residual liquidation skipped symbol={} reason=no_selected_exchanges state={:?}",
                symbol,
                managed.lifecycle_state
            );
            continue;
        }
        let current_version = control
            .symbol(&symbol)
            .await
            .map(|symbol| symbol.version)
            .unwrap_or(managed.version);
        let control_config = control.config().clone();
        let response = control
            .disable(
                DisableSymbolRequest {
                    symbol: symbol.clone(),
                    selected_exchanges,
                    mode: DisableMode::MarketLiquidate,
                    cancel_active_orders: control_config.disable_defaults.cancel_active_orders,
                    include_managed_inventory_only: control_config
                        .disable_defaults
                        .include_managed_inventory_only,
                    maximum_liquidation_loss_usdt: Some(
                        control_config
                            .disable_defaults
                            .maximum_liquidation_loss_usdt,
                    ),
                    maximum_slippage_bps: Some(
                        control_config.disable_defaults.maximum_slippage_bps,
                    ),
                    requested_by: "strategy_runtime".to_string(),
                    expected_version: current_version,
                },
                format!(
                    "strategy-runtime:residual-market-liquidation:{}:{}",
                    symbol, current_version
                ),
            )
            .await;
        if response
            .validation_errors
            .iter()
            .any(|error| error.critical)
        {
            log::warn!(
                "control residual liquidation request rejected symbol={} state={:?} errors={:?}",
                symbol,
                managed.lifecycle_state,
                response.validation_errors
            );
            continue;
        }
        blocked_symbols.insert(symbol.clone());
        enabled_symbols.remove(&symbol);
        arbitrage_pair_states.remove(&symbol);
        log::info!(
            "control residual liquidation requested symbol={} previous_state={:?} lifecycle_state={:?}",
            symbol,
            managed.lifecycle_state,
            response.lifecycle_state
        );
    }
    read_model = control.read_model().await;
    let planning_symbols = read_model
        .symbols
        .iter()
        .filter(|symbol| symbol.lifecycle_state == SpotSymbolLifecycleState::LiquidationPlanning)
        .cloned()
        .collect::<Vec<_>>();
    for managed in planning_symbols {
        let symbol = normalize_symbol(&managed.internal_symbol);
        let Some(rules) = symbol_rules.get(&symbol) else {
            log::warn!("control liquidation skipped {symbol}: symbol rules unavailable");
            continue;
        };
        let snapshot = control.latest_runtime_snapshot(&symbol).await;
        let (market_plans, snapshot_id, mut dust_remaining) = if let Some(snapshot) = snapshot {
            (
                snapshot.liquidation_preview.market_plans.clone(),
                Some(snapshot.snapshot_id.clone()),
                !snapshot.liquidation_preview.dust.is_empty(),
            )
        } else {
            log::warn!(
                "control liquidation using live fallback {symbol}: runtime snapshot unavailable"
            );
            let plans = fallback_control_market_liquidation_plans(
                config,
                &managed,
                rules,
                fee_model,
                inventory,
                book_cache,
                live_inventory_cache,
            )
            .await;
            let dust = plans
                .iter()
                .any(|plan| plan.rounded_quantity <= 0.0 && plan.managed_sellable_quantity > 0.0);
            (plans, None, dust)
        };
        let has_sellable_inventory = market_plans
            .iter()
            .any(|plan| plan.managed_sellable_quantity > 0.0);
        let has_executable_liquidation_order =
            market_plans.iter().any(|plan| plan.rounded_quantity > 0.0);
        let missing_executable_prices = market_plans
            .iter()
            .filter(|plan| plan.managed_sellable_quantity > 0.0)
            .all(|plan| plan.best_bid <= 0.0 || plan.executable_vwap <= 0.0);
        if has_sellable_inventory && has_executable_liquidation_order && missing_executable_prices {
            log::warn!(
                "control liquidation waiting for executable books symbol={} reason=missing_bid_or_vwap",
                symbol
            );
            continue;
        }
        let mut execution_results = Vec::new();
        let mut failures = Vec::new();
        let mut submitted_count = 0usize;
        let mut filled_count = 0usize;
        for liquidation_plan in market_plans {
            if liquidation_plan.rounded_quantity <= 0.0 {
                if liquidation_plan.managed_sellable_quantity > 0.0 {
                    dust_remaining = true;
                }
                execution_results.push(json!({
                    "exchange": liquidation_plan.exchange,
                    "symbol": liquidation_plan.symbol,
                    "skipped": true,
                    "reason": liquidation_plan.rejection_reason_optional
                        .unwrap_or_else(|| "zero rounded liquidation quantity".to_string()),
                }));
                continue;
            }
            let Some(liquidation_exchange) = spot_venue_from_exchange(&liquidation_plan.exchange)
            else {
                failures.push(json!({
                    "exchange": liquidation_plan.exchange,
                    "symbol": liquidation_plan.symbol,
                    "error": "unsupported liquidation exchange",
                }));
                continue;
            };
            let rule = rules.for_exchange(liquidation_exchange);
            let mut order_plan =
                control_market_liquidation_order_plan(&liquidation_plan, rule, fee_model);
            order_plan.intent = "control_stop_liquidate".to_string();
            let result = record_or_submit_spot_order_plan(
                config,
                order_plan.clone(),
                &Some(state.clone()),
                Some(live_inventory_cache),
                mexc,
                coinex,
                gateio,
                bitget,
            )
            .await?;
            if result.submitted {
                submitted_count += 1;
            }
            if result.filled {
                filled_count += 1;
            } else {
                failures.push(json!({
                    "exchange": order_plan.exchange,
                    "symbol": order_plan.symbol,
                    "quantity": order_plan.quantity,
                    "error": result.error,
                    "timed_out": result.timed_out,
                    "cancelled": result.cancelled,
                }));
            }
            execution_results.push(json!({
                "exchange": order_plan.exchange,
                "symbol": order_plan.symbol,
                "quantity": order_plan.quantity,
                "price": order_plan.price,
                "submitted": result.submitted,
                "filled": result.filled,
                "order_id": result.order_id,
                "error": result.error,
            }));
        }
        let final_state = if !failures.is_empty() {
            SpotSymbolLifecycleState::ManualInterventionRequired
        } else {
            SpotSymbolLifecycleState::DisabledClean
        };
        let current_version = control
            .symbol(&symbol)
            .await
            .map(|symbol| symbol.version)
            .unwrap_or(managed.version);
        let response = control
            .complete_market_liquidation(
                symbol.clone(),
                VersionedSymbolRequest {
                    requested_by: "strategy_runtime".to_string(),
                    expected_version: current_version,
                },
                format!(
                    "strategy-runtime:market-liquidation:{}:{}",
                    symbol,
                    chrono::Utc::now().timestamp_micros()
                ),
                final_state,
                json!({
                    "snapshot_id": snapshot_id,
                    "mode": "MarketLiquidate",
                    "live_mode": spot_live_mode(config),
                    "submitted_count": submitted_count,
                    "filled_count": filled_count,
                    "dust_remaining": dust_remaining,
                    "failures": failures,
                    "results": execution_results,
                }),
            )
            .await;
        if response
            .validation_errors
            .iter()
            .any(|error| error.critical)
        {
            log::warn!(
                "control liquidation lifecycle completion rejected symbol={} errors={:?}",
                symbol,
                response.validation_errors
            );
            continue;
        }
        blocked_symbols.insert(symbol.clone());
        enabled_symbols.remove(&symbol);
        arbitrage_pair_states.remove(&symbol);
        log::info!(
            "control liquidation completed symbol={} final_state={:?}",
            symbol,
            final_state
        );
    }
    Ok(blocked_symbols)
}

fn control_lifecycle_should_replan_market_liquidation(state: SpotSymbolLifecycleState) -> bool {
    matches!(
        state,
        SpotSymbolLifecycleState::DisabledClean
            | SpotSymbolLifecycleState::DisabledWithInventory
            | SpotSymbolLifecycleState::DustRemaining
    )
}

fn control_lifecycle_should_force_market_liquidation_replan(
    state: SpotSymbolLifecycleState,
) -> bool {
    matches!(state, SpotSymbolLifecycleState::DustRemaining)
}

async fn managed_symbol_has_liquidatable_inventory(
    config: &SpotSpotTakerArbitrageConfig,
    managed: &crate::control::spot_control::ManagedSpotSymbol,
    rules: &CommonSymbolRules,
    live_inventory_cache: &LiveInventoryCache,
    inventory: &PaperInventory,
    book_cache: &BookCache,
) -> bool {
    for exchange in control_liquidation_exchanges(config, managed) {
        let Some(venue) = spot_venue_from_exchange(&exchange) else {
            continue;
        };
        let rule = rules.for_exchange(venue);
        let balances = live_inventory_cache.balances_for_exchange(venue, inventory);
        let book = book_cache
            .get_book(&exchange, &managed.internal_symbol)
            .await;
        let reference_price = book
            .as_ref()
            .and_then(|book| book.best_bid.or(book.best_ask))
            .unwrap_or_default();
        if balances_have_liquidatable_base(&balances, rule, reference_price) {
            return true;
        }
    }
    false
}

async fn managed_symbol_has_any_base_inventory(
    config: &SpotSpotTakerArbitrageConfig,
    managed: &crate::control::spot_control::ManagedSpotSymbol,
    rules: &CommonSymbolRules,
    live_inventory_cache: &LiveInventoryCache,
    inventory: &PaperInventory,
) -> bool {
    for exchange in control_liquidation_exchanges(config, managed) {
        let Some(venue) = spot_venue_from_exchange(&exchange) else {
            continue;
        };
        let rule = rules.for_exchange(venue);
        let balances = live_inventory_cache.balances_for_exchange(venue, inventory);
        if balances.iter().any(|balance| {
            balance.asset.eq_ignore_ascii_case(&rule.base_asset)
                && balance
                    .effective_available
                    .max(balance.available)
                    .max(balance.total)
                    > 0.0
        }) {
            return true;
        }
    }
    false
}

fn control_liquidation_exchanges(
    config: &SpotSpotTakerArbitrageConfig,
    managed: &crate::control::spot_control::ManagedSpotSymbol,
) -> Vec<String> {
    let selected =
        crate::control::spot_control::normalize_exchange_list(&managed.selected_exchanges);
    if !selected.is_empty() {
        return selected;
    }
    configured_spot_venues(&config.exchanges)
        .into_iter()
        .map(|venue| venue.as_str().to_string())
        .collect()
}

fn balances_have_liquidatable_base(
    balances: &[AssetBalance],
    rule: &SymbolRule,
    reference_price: f64,
) -> bool {
    let sellable_quantity = balances
        .iter()
        .find(|balance| balance.asset.eq_ignore_ascii_case(&rule.base_asset))
        .map(|balance| balance.effective_available.max(0.0))
        .unwrap_or_default();
    let rounded_quantity = round_quantity_to_step(sellable_quantity, rule.step_size, false);
    if rounded_quantity <= 0.0 || rounded_quantity < rule.min_quantity {
        return false;
    }
    reference_price <= 0.0 || rounded_quantity * reference_price >= rule.min_notional
}

async fn fallback_control_market_liquidation_plans(
    config: &SpotSpotTakerArbitrageConfig,
    managed: &crate::control::spot_control::ManagedSpotSymbol,
    rules: &CommonSymbolRules,
    fee_model: &FeeModel,
    inventory: &PaperInventory,
    book_cache: &BookCache,
    live_inventory_cache: &LiveInventoryCache,
) -> Vec<MarketLiquidationPlan> {
    let symbol = normalize_symbol(&managed.internal_symbol);
    let mut exchanges =
        crate::control::spot_control::normalize_exchange_list(&managed.selected_exchanges);
    if exchanges.is_empty() {
        exchanges = configured_spot_venues(&config.exchanges)
            .into_iter()
            .map(|venue| venue.as_str().to_string())
            .collect::<Vec<_>>();
    }
    let max_slippage_bps = config
        .spot_symbol_control
        .disable_defaults
        .maximum_slippage_bps;
    let mut plans = Vec::new();
    for exchange in exchanges {
        let Some(venue) = spot_venue_from_exchange(&exchange) else {
            continue;
        };
        let rule = rules.for_exchange(venue);
        let balances = live_inventory_cache.balances_for_exchange(venue, inventory);
        let sellable_quantity = balances
            .iter()
            .find(|balance| balance.asset.eq_ignore_ascii_case(&rule.base_asset))
            .map(|balance| balance.effective_available.max(0.0))
            .unwrap_or_default();
        let book = book_cache.get_book(&exchange, &symbol).await;
        let best_bid = book
            .as_ref()
            .and_then(|book| book.best_bid)
            .unwrap_or_default();
        let rounded_quantity = safe_liquidation_quantity(sellable_quantity, rule.step_size);
        let executable_vwap = if rounded_quantity > 0.0 {
            book.as_ref()
                .and_then(|book| book.bids.first().map(|level| level.price))
                .unwrap_or(best_bid)
        } else {
            0.0
        };
        let estimated_proceeds = rounded_quantity * executable_vwap;
        let fee = fee_model.calculate_fee(
            &FeeLookupKey {
                exchange: venue.as_str().to_string(),
                market_type: MarketType::Spot,
                symbol: Some(symbol.clone()),
                liquidity_role: FeeRole::Taker,
            },
            estimated_proceeds,
        );
        let estimated_slippage_bps = if best_bid > 0.0 && executable_vwap > 0.0 {
            ((best_bid - executable_vwap).max(0.0) / best_bid) * 10_000.0
        } else {
            0.0
        };
        let valid = rounded_quantity > 0.0
            && rounded_quantity >= rule.min_quantity
            && estimated_proceeds >= rule.min_notional
            && executable_vwap > 0.0;
        plans.push(MarketLiquidationPlan {
            command_id: "runtime-fallback".to_string(),
            exchange: venue.as_str().to_string(),
            symbol: symbol.clone(),
            managed_sellable_quantity: sellable_quantity,
            rounded_quantity,
            best_bid,
            executable_vwap,
            worst_allowed_price: best_bid * (1.0 - max_slippage_bps / 10_000.0),
            estimated_fee: fee.fee_amount,
            estimated_slippage_bps,
            estimated_proceeds,
            estimated_loss: 0.0,
            validation_status: if valid {
                "valid_dry_run_preview".to_string()
            } else {
                "dust_or_invalid_order_size".to_string()
            },
            would_submit_order: false,
            rejection_reason_optional: (!valid).then(|| {
                "quantity is below exchange minimums, precision constraints, or book is unavailable"
                    .to_string()
            }),
        });
    }
    plans
}

fn control_market_liquidation_order_plan(
    liquidation_plan: &MarketLiquidationPlan,
    rule: &SymbolRule,
    fee_model: &FeeModel,
) -> LiveDryRunOrderPlan {
    let exchange = crate::control::spot_control::normalize_exchange(&liquidation_plan.exchange);
    let symbol = normalize_symbol(&liquidation_plan.symbol);
    let timestamp = chrono::Utc::now();
    let price_source = if liquidation_plan.worst_allowed_price > 0.0 {
        liquidation_plan.worst_allowed_price
    } else {
        liquidation_plan.executable_vwap
    };
    let price = round_price_to_tick(price_source, rule.tick_size, false);
    let quantity = if liquidation_plan.managed_sellable_quantity > 0.0 {
        safe_liquidation_quantity(liquidation_plan.managed_sellable_quantity, rule.step_size)
            .min(liquidation_plan.rounded_quantity)
    } else {
        safe_liquidation_quantity(liquidation_plan.rounded_quantity, rule.step_size)
    };
    let client_order_id =
        generate_client_order_id(&exchange, MarketType::Spot, "sliq").into_string();
    let request = OrderRequest {
        market_type: MarketType::Spot,
        symbol: rule.exchange_symbol.clone(),
        side: OrderSide::Sell,
        position_side: PositionSide::None,
        order_type: OrderType::IOC,
        time_in_force: Some(TimeInForce::IOC),
        quantity,
        price: Some(price),
        client_order_id: Some(client_order_id.clone()),
        reduce_only: false,
    };
    let order_validation = validate_order_against_symbol_rule(&request, rule);
    let mut checks = vec![
        LiveDryRunCheck {
            name: "liquidation_preview".to_string(),
            passed: liquidation_plan.validation_status == "valid_dry_run_preview",
            message: liquidation_plan.validation_status.clone(),
        },
        LiveDryRunCheck {
            name: "quantity".to_string(),
            passed: quantity > 0.0,
            message: format!("quantity={quantity}"),
        },
        LiveDryRunCheck {
            name: "price".to_string(),
            passed: price > 0.0,
            message: format!("price={price}"),
        },
        LiveDryRunCheck {
            name: "symbol_rule_validation".to_string(),
            passed: order_validation.is_ok(),
            message: order_validation
                .as_ref()
                .map(|_| "order satisfies symbol rules".to_string())
                .unwrap_or_else(|error| error.to_string()),
        },
    ];
    if let Some(reason) = &liquidation_plan.rejection_reason_optional {
        checks.push(LiveDryRunCheck {
            name: "liquidation_rejection".to_string(),
            passed: false,
            message: reason.clone(),
        });
    }
    let validation_result = LiveDryRunValidationResult {
        passed: checks.iter().all(|check| check.passed),
        checks,
    };
    let notional = quantity * price;
    let fee_estimate = fee_model.calculate_fee_for_side(
        &FeeLookupKey {
            exchange: exchange.clone(),
            market_type: MarketType::Spot,
            symbol: Some(symbol.clone()),
            liquidity_role: FeeRole::Taker,
        },
        Some(OrderSide::Sell),
        notional,
    );
    LiveDryRunOrderPlan {
        plan_id: format!("sliq-{}-{}", exchange, timestamp.timestamp_micros()),
        timestamp,
        intent: "control_stop_liquidate".to_string(),
        exchange,
        market_type: MarketType::Spot,
        symbol,
        exchange_symbol: rule.exchange_symbol.clone(),
        side: OrderSide::Sell,
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        price: request.price,
        quantity,
        notional,
        client_order_id: Some(client_order_id),
        fee_estimate,
        required_balance_asset: rule.base_asset.clone(),
        required_balance_amount: quantity,
        symbol_rule_snapshot: rule.clone(),
        would_submit: false,
        rejection_reason: (!validation_result.passed).then(|| {
            validation_result
                .checks
                .iter()
                .filter(|check| !check.passed)
                .map(|check| format!("{}: {}", check.name, check.message))
                .collect::<Vec<_>>()
                .join("; ")
        }),
        validation_result,
        order_request: request,
    }
}

fn safe_liquidation_quantity(quantity: f64, step_size: f64) -> f64 {
    if quantity <= 0.0 {
        return 0.0;
    }
    let adjusted = quantity * LIQUIDATION_BALANCE_SAFETY_FACTOR;
    round_quantity_to_step(adjusted, step_size, false)
}

fn release_arbitrage_symbols(enabled_symbols: &mut BTreeSet<String>, blocked: &BTreeSet<String>) {
    enabled_symbols.retain(|symbol| !blocked.contains(symbol));
}

fn release_blocked_arbitrage_pair_states(
    blocked: &BTreeSet<String>,
    enabled_symbols: &mut BTreeSet<String>,
    arbitrage_pair_states: &mut BTreeMap<String, ArbitragePairRuntime>,
) {
    if blocked.is_empty() {
        return;
    }
    enabled_symbols.retain(|symbol| !blocked.contains(symbol));
    arbitrage_pair_states.retain(|symbol, _| !blocked.contains(symbol));
}

#[cfg(test)]
fn release_blocked_initial_entry_symbols(
    blocked: &BTreeSet<String>,
    completed_symbols: &mut BTreeSet<String>,
    planned_legs: &mut BTreeMap<String, BTreeSet<String>>,
    failures_seen: &mut BTreeMap<String, String>,
    enabled_symbols: &mut BTreeSet<String>,
    arbitrage_pair_states: &mut BTreeMap<String, ArbitragePairRuntime>,
) {
    if blocked.is_empty() {
        return;
    }
    completed_symbols.retain(|symbol| !blocked.contains(symbol));
    planned_legs.retain(|symbol, _| !blocked.contains(symbol));
    failures_seen.retain(|key, _| {
        let symbol = key
            .split_once(':')
            .map(|(symbol, _)| symbol)
            .unwrap_or(key.as_str());
        !blocked.contains(symbol)
    });
    enabled_symbols.retain(|symbol| !blocked.contains(symbol));
    arbitrage_pair_states.retain(|symbol, _| !blocked.contains(symbol));
}

async fn publish_market_views(
    state: &MonitoringState,
    book_cache: &BookCache,
    websocket_runtime: Option<&WebsocketMarketDataRuntime>,
    config: &SpotSpotTakerArbitrageConfig,
) {
    let books = book_cache
        .get_all_books()
        .await
        .into_iter()
        .map(book_view)
        .collect::<Vec<_>>();
    state.publish_books(books);
    if let Some(runtime) = websocket_runtime {
        let health = runtime.health.snapshot().await;
        state.publish_exchanges(
            health
                .into_iter()
                .map(|health| ExchangeHealthView {
                    exchange: health.exchange,
                    market_type: health.market_type,
                    connected: health.connected,
                    public_ws_connected: health.connected,
                    private_ws_connected: None,
                    last_message_at: health.last_message_at,
                    last_book_update_at: health.last_book_update_at,
                    stale_symbol_count: health.stale_symbols.len(),
                    fresh_symbol_count: health.fresh_symbols.len(),
                    reconnect_count: health.reconnect_count,
                    parse_error_count: health.parse_error_count,
                    sequence_gap_count: health.sequence_gap_count,
                    heartbeat_timeout_count: health.heartbeat_timeout_count,
                    avg_latency_ms: health.avg_latency_ms,
                    max_latency_ms: health.max_latency_ms,
                })
                .collect(),
        );
    } else {
        state.publish_exchanges(
            config
                .exchanges
                .iter()
                .map(|exchange| ExchangeHealthView {
                    exchange: exchange.trim().to_ascii_lowercase(),
                    market_type: Some(MarketType::Spot),
                    connected: true,
                    public_ws_connected: false,
                    private_ws_connected: None,
                    last_message_at: None,
                    last_book_update_at: None,
                    stale_symbol_count: 0,
                    fresh_symbol_count: 0,
                    reconnect_count: 0,
                    parse_error_count: 0,
                    sequence_gap_count: 0,
                    heartbeat_timeout_count: 0,
                    avg_latency_ms: None,
                    max_latency_ms: None,
                })
                .collect(),
        );
    }
}

async fn publish_five_exchange_scanner_view(
    state: &MonitoringState,
    config: &SpotSpotTakerArbitrageConfig,
    symbol_rules: &HashMap<String, CommonSymbolRules>,
    book_cache: &BookCache,
    fee_model: &FeeModel,
) {
    if config.websocket.exchanges.len() < 5 {
        return;
    }
    let scanner_config = FiveExchangeSpotScannerConfig {
        max_book_age_ms: config
            .arbitrage_scanner
            .stale_book_ms
            .max(config.websocket.stale_book_ms),
        require_fresh_books: true,
        notionals_usdt: config.arbitrage_scanner.notionals_usdt.clone(),
        ..FiveExchangeSpotScannerConfig::default()
    };
    let rules = flatten_spot_symbol_rules(symbol_rules);
    let books = book_cache
        .get_all_books()
        .await
        .into_iter()
        .map(CachedBook::into_snapshot)
        .collect::<Vec<_>>();
    let scanner = scan_five_exchange_spot(&scanner_config, &rules, &books, fee_model);
    state.publish_five_exchange_scanner(scanner);
}

fn book_view(book: CachedBook) -> BookView {
    let book_age_ms = cached_book_age_ms(&book);
    BookView {
        exchange: book.exchange,
        market_type: MarketType::Spot,
        symbol: book.symbol.clone(),
        exchange_symbol: book.symbol,
        best_bid: book.best_bid,
        best_ask: book.best_ask,
        spread: book.best_bid.zip(book.best_ask).map(|(bid, ask)| ask - bid),
        book_age_ms,
        latency_ms: book.latency_ms,
        is_stale: book.is_stale,
        stale_reason: if book.is_stale {
            Some("stale".to_string())
        } else {
            None
        },
        source: book.source.as_str().to_string(),
        sequence: book.sequence,
    }
}

fn opportunity_view(record: &OpportunityRecord) -> OpportunityView {
    OpportunityView {
        timestamp: record.timestamp,
        opportunity_id: format!(
            "{}:{}:{}:{}",
            record.timestamp.timestamp_micros(),
            record.symbol,
            record.buy_exchange,
            record.sell_exchange
        ),
        symbol: record.symbol.clone(),
        relationship_type: "spot_spot_taker".to_string(),
        buy_exchange: record.buy_exchange.clone(),
        sell_exchange: record.sell_exchange.clone(),
        buy_price: record.buy_price,
        sell_price: record.sell_price,
        raw_spread_bps: record.raw_spread_bps,
        fee_bps: record.estimated_fee_bps,
        net_spread_bps: record.estimated_net_spread_bps,
        estimated_net_pnl: record.estimated_net_pnl,
        accepted: record.accepted,
        rejection_reason: record.rejection_reason.map(|reason| format!("{reason:?}")),
        buy_book_age_ms: record.buy_book_age_ms,
        sell_book_age_ms: record.sell_book_age_ms,
    }
}

fn trade_view(record: &SimulatedTradeRecord) -> TradeView {
    TradeView {
        timestamp: record.timestamp,
        trade_id: format!(
            "{}:{}:{}:{}",
            record.timestamp.timestamp_micros(),
            record.symbol,
            record.buy_exchange,
            record.sell_exchange
        ),
        symbol: record.symbol.clone(),
        buy_exchange: record.buy_exchange.clone(),
        sell_exchange: record.sell_exchange.clone(),
        quantity: record.quantity,
        buy_avg_price: record.buy_avg_price,
        sell_avg_price: record.sell_avg_price,
        gross_pnl: record.gross_pnl,
        total_fee: record.buy_fee + record.sell_fee,
        net_pnl: record.net_pnl,
        execution_mode: record.execution_mode.clone(),
        paper_or_live: "paper".to_string(),
    }
}

async fn build_live_dry_run_pair(
    config: &SpotSpotTakerArbitrageConfig,
    opportunity: &OpportunityRecord,
    rules: &CommonSymbolRules,
    mexc_book: &crate::exchanges::unified::OrderBookSnapshot,
    coinex_book: &crate::exchanges::unified::OrderBookSnapshot,
    inventory: &PaperInventory,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
    mexc: &MexcSpotClient,
    coinex: &CoinExSpotClient,
    gateio: &GateIoSpotClient,
    bitget: &BitgetSpotClient,
) -> Result<Vec<LiveDryRunOrderPlan>> {
    let reservations = crate::exchanges::spot_reservation::BalanceReservationManager::default();
    let buy_exchange = parse_spot_venue(&opportunity.buy_exchange)?;
    let sell_exchange = parse_spot_venue(&opportunity.sell_exchange)?;
    let buy_rule = rules.for_exchange(buy_exchange);
    let sell_rule = rules.for_exchange(sell_exchange);
    let (left_exchange, right_exchange) = configured_spot_pair(&config.exchanges);
    let book_for = |exchange| {
        if exchange == left_exchange {
            mexc_book
        } else if exchange == right_exchange {
            coinex_book
        } else {
            mexc_book
        }
    };
    let buy_book = book_for(buy_exchange);
    let sell_book = book_for(sell_exchange);
    let buy_balances =
        live_or_inventory_balances(buy_exchange, inventory, mexc, coinex, gateio, bitget).await;
    let sell_balances =
        live_or_inventory_balances(sell_exchange, inventory, mexc, coinex, gateio, bitget).await;
    let buy_plan = build_live_dry_run_order_plan(
        &config.live_dry_run,
        LiveDryRunOrderInput {
            exchange: buy_exchange.as_str(),
            market_type: MarketType::Spot,
            internal_symbol: &opportunity.symbol,
            side: crate::exchanges::unified::OrderSide::Buy,
            desired_notional: opportunity.executable_notional,
            book: buy_book,
            symbol_rule: buy_rule,
            balances: &buy_balances,
            reservations: &reservations,
            disabled_registry,
            fee_model,
        },
    )?;
    let sell_plan = build_live_dry_run_order_plan(
        &config.live_dry_run,
        LiveDryRunOrderInput {
            exchange: sell_exchange.as_str(),
            market_type: MarketType::Spot,
            internal_symbol: &opportunity.symbol,
            side: crate::exchanges::unified::OrderSide::Sell,
            desired_notional: opportunity.executable_notional,
            book: sell_book,
            symbol_rule: sell_rule,
            balances: &sell_balances,
            reservations: &reservations,
            disabled_registry,
            fee_model,
        },
    )?;
    Ok(vec![buy_plan, sell_plan])
}

async fn control_blocked_arbitrage_symbols(
    monitoring: &Option<MonitoringState>,
) -> BTreeSet<String> {
    let Some(state) = monitoring else {
        return BTreeSet::new();
    };
    let Some(control) = state.control() else {
        return BTreeSet::new();
    };
    control
        .read_model()
        .await
        .symbols
        .into_iter()
        .filter(|symbol| symbol.lifecycle_state.blocks_new_arbitrage())
        .map(|symbol| symbol.internal_symbol.trim().to_ascii_uppercase())
        .collect()
}

fn spot_live_execution_symbols(config: &SpotSpotTakerArbitrageConfig) -> Vec<String> {
    if config.trading_mode.eq_ignore_ascii_case("live")
        && !config.small_live_gate.enabled_symbols.is_empty()
    {
        return config
            .small_live_gate
            .enabled_symbols
            .iter()
            .map(|symbol| normalize_symbol(symbol))
            .collect();
    }
    config
        .symbols
        .iter()
        .map(|symbol| normalize_symbol(symbol))
        .collect()
}

fn spot_live_execution_symbol_allowed(config: &SpotSpotTakerArbitrageConfig, symbol: &str) -> bool {
    if !config.trading_mode.eq_ignore_ascii_case("live")
        || config.small_live_gate.enabled_symbols.is_empty()
    {
        return true;
    }
    let normalized_symbol = normalize_symbol(symbol);
    config
        .small_live_gate
        .enabled_symbols
        .iter()
        .any(|candidate| normalize_symbol(candidate) == normalized_symbol)
}

fn existing_base_notional(
    balances: &[AssetBalance],
    base_asset: &str,
    reference_price: f64,
) -> f64 {
    if reference_price <= 0.0 {
        return 0.0;
    }
    balances
        .iter()
        .find(|balance| balance.asset.eq_ignore_ascii_case(base_asset))
        .map(|balance| balance.total.max(balance.available) * reference_price)
        .unwrap_or_default()
}

#[cfg(test)]
async fn build_initial_entry_live_dry_run_plans(
    config: &SpotSpotTakerArbitrageConfig,
    symbol_rules: &HashMap<String, CommonSymbolRules>,
    balances_by_exchange: &HashMap<SpotVenue, Vec<AssetBalance>>,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
    book_cache: &BookCache,
    already_planned: &BTreeSet<String>,
    blocked_control_symbols: &BTreeSet<String>,
) -> Vec<LiveDryRunOrderPlan> {
    if config.initial_entry_notional_usdt <= 0.0 || config.max_enabled_arbitrage_symbols == 0 {
        return Vec::new();
    }
    let remaining = config
        .max_enabled_arbitrage_symbols
        .saturating_sub(already_planned.len());
    if remaining == 0 {
        return Vec::new();
    }
    let reservations = crate::exchanges::spot_reservation::BalanceReservationManager::default();
    let mut plans = Vec::new();
    let mut symbols_planned = 0usize;
    let initial_entry_symbols = spot_live_execution_symbols(config);
    for symbol in &initial_entry_symbols {
        if symbols_planned >= remaining {
            break;
        }
        let normalized_symbol = normalize_symbol(symbol);
        if already_planned.contains(&normalized_symbol)
            || blocked_control_symbols.contains(&normalized_symbol)
        {
            continue;
        }
        let Some(rules) = symbol_rules.get(&normalized_symbol) else {
            continue;
        };
        let entry_books =
            initial_entry_exchange_books_for_symbol(config, book_cache, &normalized_symbol).await;
        if entry_books.len() < 2 {
            continue;
        }
        let allocation_books = entry_books
            .iter()
            .map(|(venue, cached_book)| (*venue, cached_book.clone().into_snapshot()))
            .collect::<Vec<_>>();
        let allocation_by_exchange = build_entry_allocations(config, &allocation_books)
            .into_iter()
            .map(|allocation| (allocation.exchange, allocation.notional))
            .collect::<HashMap<_, _>>();
        for (entry_exchange, cached_book) in entry_books {
            let desired_notional = allocation_by_exchange
                .get(&entry_exchange)
                .copied()
                .unwrap_or(config.initial_entry_notional_usdt);
            let balances = balances_by_exchange
                .get(&entry_exchange)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let rule = rules.for_exchange(entry_exchange);
            let reference_price = cached_book
                .best_bid
                .or(cached_book.best_ask)
                .unwrap_or_default();
            if existing_base_notional(balances, &rule.base_asset, reference_price)
                >= config.initial_entry_notional_usdt * 0.8
            {
                continue;
            }
            let Ok(mut plan) = build_live_dry_run_order_plan(
                &config.live_dry_run,
                LiveDryRunOrderInput {
                    exchange: entry_exchange.as_str(),
                    market_type: MarketType::Spot,
                    internal_symbol: &normalized_symbol,
                    side: OrderSide::Buy,
                    desired_notional,
                    book: &cached_book.into_snapshot(),
                    symbol_rule: rule,
                    balances,
                    reservations: &reservations,
                    disabled_registry,
                    fee_model,
                },
            ) else {
                continue;
            };
            plan.intent = "initial_entry".to_string();
            plans.push(plan);
        }
        symbols_planned += 1;
    }
    plans
}

#[cfg(test)]
fn should_record_initial_entry_plan(
    plan: &LiveDryRunOrderPlan,
    planned_legs: &mut BTreeMap<String, BTreeSet<String>>,
    completed_symbols: &mut BTreeSet<String>,
    failures_seen: &mut BTreeMap<String, String>,
) -> bool {
    if plan.validation_result.passed {
        let legs = planned_legs.entry(plan.symbol.clone()).or_default();
        legs.insert(plan.exchange.clone());
        if legs.len() >= 2 {
            completed_symbols.insert(plan.symbol.clone());
            failures_seen.remove(&plan.symbol);
        }
        return true;
    }
    let reason = plan
        .rejection_reason
        .clone()
        .unwrap_or_else(|| "validation failed".to_string());
    match failures_seen.get(&plan.symbol) {
        Some(previous) if previous == &reason => false,
        _ => {
            failures_seen.insert(plan.symbol.clone(), reason);
            true
        }
    }
}

async fn initial_entry_exchange_books_for_symbol(
    config: &SpotSpotTakerArbitrageConfig,
    book_cache: &BookCache,
    symbol: &str,
) -> Vec<(SpotVenue, CachedBook)> {
    let (left_exchange, right_exchange) = configured_spot_pair(&config.exchanges);
    let (left_book, right_book) = tokio::join!(
        book_cache.get_book(left_exchange.as_str(), symbol),
        book_cache.get_book(right_exchange.as_str(), symbol),
    );
    [(left_exchange, left_book), (right_exchange, right_book)]
        .into_iter()
        .filter_map(|(exchange, book)| book.map(|book| (exchange, book)))
        .collect()
}

async fn lifecycle_books_for_symbol(
    config: &SpotSpotTakerArbitrageConfig,
    book_cache: &BookCache,
    symbol: &str,
    left_book: &OrderBookSnapshot,
    right_book: &OrderBookSnapshot,
) -> Vec<(SpotVenue, OrderBookSnapshot)> {
    let mut books = Vec::new();
    for venue in configured_spot_venues(&config.exchanges) {
        if venue.as_str().eq_ignore_ascii_case(&left_book.exchange) {
            books.push((venue, left_book.clone()));
            continue;
        }
        if venue.as_str().eq_ignore_ascii_case(&right_book.exchange) {
            books.push((venue, right_book.clone()));
            continue;
        }
        if let Some(book) = book_cache.get_book(venue.as_str(), symbol).await {
            books.push((venue, book.into_snapshot()));
        }
    }
    books
}

async fn lifecycle_books_for_symbol_from_cache(
    config: &SpotSpotTakerArbitrageConfig,
    book_cache: &BookCache,
    symbol: &str,
) -> Vec<(SpotVenue, OrderBookSnapshot)> {
    let mut books = Vec::new();
    for venue in configured_spot_venues(&config.exchanges) {
        if let Some(book) = book_cache.get_book(venue.as_str(), symbol).await {
            books.push((venue, book.into_snapshot()));
        }
    }
    books
}

async fn lifecycle_balances_for_books(
    books: &[(SpotVenue, OrderBookSnapshot)],
    inventory: &PaperInventory,
    mexc: &MexcSpotClient,
    coinex: &CoinExSpotClient,
    gateio: &GateIoSpotClient,
    bitget: &BitgetSpotClient,
) -> HashMap<SpotVenue, Vec<AssetBalance>> {
    let mut balances = HashMap::new();
    for (venue, _) in books {
        balances.insert(
            *venue,
            live_or_inventory_balances(*venue, inventory, mexc, coinex, gateio, bitget).await,
        );
    }
    balances
}

async fn live_or_inventory_balances(
    exchange: SpotVenue,
    inventory: &PaperInventory,
    mexc: &MexcSpotClient,
    coinex: &CoinExSpotClient,
    gateio: &GateIoSpotClient,
    bitget: &BitgetSpotClient,
) -> Vec<crate::exchanges::unified::AssetBalance> {
    let live = match exchange {
        SpotVenue::Mexc => mexc.get_balances().await,
        SpotVenue::CoinEx => coinex.get_balances().await,
        SpotVenue::GateIo => gateio.get_balances().await,
        SpotVenue::Bitget => bitget.get_balances().await,
    };
    match live {
        Ok(snapshot) => snapshot.balances,
        Err(error) => {
            log_warn_once(format!("balance_fallback:{}", exchange.as_str()), || {
                format!(
                        "live dry-run using configured inventory balances for {} because private balance fetch failed: {}",
                        exchange.as_str(),
                        error
                    )
            });
            inventory_balances_for_exchange(inventory, exchange)
        }
    }
}

fn inventory_balances_for_exchange(
    inventory: &PaperInventory,
    exchange: SpotVenue,
) -> Vec<AssetBalance> {
    inventory
        .balances_snapshot()
        .into_iter()
        .filter(|(venue, _, _)| *venue == exchange)
        .map(|(_, asset, state)| state.as_asset_balance(&asset))
        .collect()
}

async fn live_open_orders(
    config: &SpotSpotTakerArbitrageConfig,
    mexc: &MexcSpotClient,
    coinex: &CoinExSpotClient,
    gateio: &GateIoSpotClient,
    bitget: &BitgetSpotClient,
) -> Vec<OrderResponse> {
    let mut orders = Vec::new();
    for exchange in configured_spot_venues(&config.exchanges) {
        let result = match exchange {
            SpotVenue::Mexc => mexc.get_open_orders(None).await,
            SpotVenue::CoinEx => coinex.get_open_orders(None).await,
            SpotVenue::GateIo => gateio.get_open_orders(None).await,
            SpotVenue::Bitget => bitget.get_open_orders(None).await,
        };
        match result {
            Ok(mut exchange_orders) => orders.append(&mut exchange_orders),
            Err(error) => {
                log_warn_once(format!("open_orders:{}", exchange.as_str()), || {
                    format!(
                        "failed to fetch live open orders for {}: {}",
                        exchange.as_str(),
                        error
                    )
                });
            }
        }
    }
    orders.sort_by(|left, right| {
        (
            left.exchange.as_str(),
            left.symbol.as_str(),
            left.created_at,
        )
            .cmp(&(
                right.exchange.as_str(),
                right.symbol.as_str(),
                right.created_at,
            ))
    });
    orders
}

async fn live_or_inventory_views(
    config: &SpotSpotTakerArbitrageConfig,
    inventory: &PaperInventory,
    disabled_registry: &DisabledRegistry,
    book_cache: &BookCache,
    mexc: &MexcSpotClient,
    coinex: &CoinExSpotClient,
    gateio: &GateIoSpotClient,
    bitget: &BitgetSpotClient,
) -> Vec<InventoryView> {
    let exchanges = configured_spot_venues(&config.exchanges);
    let mut views = Vec::new();
    let mut live_ok = false;
    for exchange in exchanges {
        let balances =
            live_or_inventory_balances(exchange, inventory, mexc, coinex, gateio, bitget).await;
        if !balances.is_empty() {
            live_ok = true;
            views.extend(
                asset_balance_views(exchange, balances, disabled_registry, book_cache).await,
            );
        }
    }
    if live_ok {
        views.sort_by(|left, right| {
            (left.exchange.as_str(), left.asset.as_str())
                .cmp(&(right.exchange.as_str(), right.asset.as_str()))
        });
        views
    } else {
        inventory_views(inventory, disabled_registry, book_cache).await
    }
}

async fn asset_balance_views(
    exchange: SpotVenue,
    balances: Vec<AssetBalance>,
    disabled_registry: &DisabledRegistry,
    book_cache: &BookCache,
) -> Vec<InventoryView> {
    let mut views = Vec::new();
    for balance in balances {
        if balance.total.abs() <= 1e-12 && balance.available.abs() <= 1e-12 {
            continue;
        }
        let asset = balance.asset.trim().to_ascii_uppercase();
        let unmanaged_quantity = unmanaged_quantity(disabled_registry, exchange, &asset);
        let valuation_usdt = asset_usdt_value(book_cache, exchange, &asset, balance.total).await;
        views.push(InventoryView {
            exchange: exchange.as_str().to_string(),
            market_type: MarketType::Spot,
            asset,
            total: balance.total,
            available: balance.available,
            locked_by_exchange: balance.locked_by_exchange.max(balance.locked),
            locally_reserved: balance.locally_reserved,
            effective_available: balance.effective_available,
            unmanaged_quantity,
            valuation_usdt,
        });
    }
    views
}

fn parse_spot_venue(value: &str) -> Result<SpotVenue> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mexc" => Ok(SpotVenue::Mexc),
        "coinex" => Ok(SpotVenue::CoinEx),
        "gate" | "gateio" | "gate.io" => Ok(SpotVenue::GateIo),
        "bitget" => Ok(SpotVenue::Bitget),
        other => Err(anyhow!("unsupported spot venue for live dry-run: {other}")),
    }
}

async fn inventory_views(
    inventory: &PaperInventory,
    disabled_registry: &DisabledRegistry,
    book_cache: &BookCache,
) -> Vec<InventoryView> {
    let mut views = Vec::new();
    for (exchange, asset, state) in inventory.balances_snapshot() {
        let unmanaged_quantity = unmanaged_quantity(disabled_registry, exchange, &asset);
        let valuation_usdt = asset_usdt_value(book_cache, exchange, &asset, state.total).await;
        views.push(InventoryView {
            exchange: exchange.as_str().to_string(),
            market_type: MarketType::Spot,
            asset,
            total: state.total,
            available: state.available,
            locked_by_exchange: state.locked_by_exchange,
            locally_reserved: state.locally_reserved,
            effective_available: state.effective_available(),
            unmanaged_quantity,
            valuation_usdt,
        });
    }
    views
}

fn config_inventory_views(
    inventory: &PaperInventory,
    disabled_registry: &DisabledRegistry,
) -> Vec<InventoryView> {
    inventory
        .balances_snapshot()
        .into_iter()
        .map(|(exchange, asset, state)| {
            let unmanaged_quantity = unmanaged_quantity(disabled_registry, exchange, &asset);
            let valuation_usdt =
                if asset.eq_ignore_ascii_case("USDT") || asset.eq_ignore_ascii_case("USDC") {
                    Some(state.total)
                } else {
                    None
                };
            InventoryView {
                exchange: exchange.as_str().to_string(),
                market_type: MarketType::Spot,
                asset,
                total: state.total,
                available: state.available,
                locked_by_exchange: state.locked_by_exchange,
                locally_reserved: state.locally_reserved,
                effective_available: state.effective_available(),
                unmanaged_quantity,
                valuation_usdt,
            }
        })
        .collect()
}

fn unmanaged_quantity(
    disabled_registry: &DisabledRegistry,
    exchange: SpotVenue,
    asset: &str,
) -> f64 {
    disabled_registry
        .unmanaged_positions()
        .iter()
        .filter(|position| {
            position.exchange == exchange.as_str()
                && position.market_type == MarketType::Spot
                && position.asset.eq_ignore_ascii_case(asset)
        })
        .map(|position| position.quantity)
        .sum()
}

fn has_small_live_unmanaged_inventory_overlap(
    config: &SmallLiveGateConfig,
    unmanaged_positions: &[UnmanagedPositionView],
) -> bool {
    let enabled_symbols = config
        .enabled_symbols
        .iter()
        .map(|symbol| normalize_symbol(symbol))
        .collect::<BTreeSet<_>>();
    let enabled_exchanges = config
        .enabled_exchanges
        .iter()
        .map(|exchange| normalize_spot_exchange_key(exchange))
        .collect::<BTreeSet<_>>();

    unmanaged_positions.iter().any(|position| {
        if position.quantity <= 0.0 || position.market_type != MarketType::Spot {
            return false;
        }
        let symbol = normalize_symbol(&position.symbol);
        let exchange = normalize_spot_exchange_key(&position.exchange);
        (enabled_symbols.is_empty() || enabled_symbols.contains(&symbol))
            && (enabled_exchanges.is_empty() || enabled_exchanges.contains(&exchange))
    })
}

fn normalize_spot_exchange_key(exchange: &str) -> String {
    let normalized = exchange
        .trim()
        .to_ascii_lowercase()
        .replace(['.', '-', '_'], "");
    match normalized.as_str() {
        "gate" | "gateio" => "gateio".to_string(),
        "coinex" => "coinex".to_string(),
        "mexc" => "mexc".to_string(),
        "bitget" => "bitget".to_string(),
        other => other.to_string(),
    }
}

async fn asset_usdt_value(
    book_cache: &BookCache,
    exchange: SpotVenue,
    asset: &str,
    total: f64,
) -> Option<f64> {
    if asset.eq_ignore_ascii_case("USDT") || asset.eq_ignore_ascii_case("USDC") {
        return Some(total);
    }
    let symbol = format!("{}USDT", asset.trim().to_ascii_uppercase());
    let (bid, ask) = book_cache
        .get_best_bid_ask(exchange.as_str(), &symbol)
        .await?;
    let mid = match (Some(bid), Some(ask)) {
        (Some(bid), Some(ask)) if bid > 0.0 && ask > 0.0 => (bid + ask) / 2.0,
        (Some(bid), _) if bid > 0.0 => bid,
        (_, Some(ask)) if ask > 0.0 => ask,
        _ => return None,
    };
    Some(total * mid)
}

fn fee_views(model: &FeeModel) -> Vec<FeeView> {
    model
        .summary_rates()
        .into_iter()
        .map(|rate| FeeView {
            exchange: rate.exchange,
            market_type: rate.market_type,
            symbol: rate.symbol,
            maker_fee_bps: rate.maker_fee_bps,
            taker_fee_bps: rate.taker_fee_bps,
            source: rate.source,
            platform_discount_enabled: rate.platform_discount_enabled,
            platform_token: rate.platform_token,
            updated_at: rate.updated_at,
        })
        .collect()
}

fn disabled_view(registry: &DisabledRegistry) -> DisabledView {
    let now = chrono::Utc::now();
    DisabledView {
        symbols: registry
            .disabled_symbols()
            .into_iter()
            .map(|item| DisabledSymbolView {
                symbol: item.symbol,
                status: disabled_status(item.expires_at, now),
                reason: item.reason,
                expires_at: item.expires_at,
            })
            .collect(),
        exchanges: registry
            .disabled_exchanges()
            .into_iter()
            .map(|item| DisabledExchangeView {
                exchange: item.exchange,
                status: disabled_status(item.expires_at, now),
                reason: item.reason,
                expires_at: item.expires_at,
            })
            .collect(),
        exchange_symbols: registry
            .disabled_exchange_symbols()
            .into_iter()
            .map(|item| DisabledExchangeSymbolView {
                exchange: item.exchange,
                market_type: item.market_type,
                symbol: item.symbol,
                status: disabled_status(item.expires_at, now),
                reason: item.reason,
                expires_at: item.expires_at,
            })
            .collect(),
    }
}

fn unmanaged_position_views(registry: &DisabledRegistry) -> Vec<UnmanagedPositionView> {
    registry
        .unmanaged_positions()
        .iter()
        .map(|position| UnmanagedPositionView {
            exchange: position.exchange.clone(),
            market_type: position.market_type,
            symbol: position.symbol.clone(),
            asset: position.asset.clone(),
            quantity: position.quantity,
            reason: position.reason.clone(),
            created_at: position.created_at,
        })
        .collect()
}

fn disabled_status(
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> String {
    if expires_at.is_some_and(|expires_at| expires_at <= now) {
        format!("{:?}", DisabledStatus::Expired).to_ascii_lowercase()
    } else {
        format!("{:?}", DisabledStatus::Active).to_ascii_lowercase()
    }
}

fn recorder_health_view(
    config: &SpotSpotTakerArbitrageConfig,
    websocket_runtime: Option<&WebsocketMarketDataRuntime>,
) -> RecorderHealthView {
    RecorderHealthView {
        book_recording_enabled: config.websocket.record_books,
        opportunity_recording_enabled: true,
        trade_recording_enabled: true,
        dropped_book_events: websocket_runtime
            .and_then(|runtime| runtime.recorder.as_ref())
            .map(|recorder| recorder.dropped_events())
            .unwrap_or(0),
        dropped_opportunity_events: 0,
        dropped_trade_events: 0,
        last_write_at: None,
        output_paths: vec![
            config.jsonl_path.clone(),
            config.csv_path.clone(),
            config.websocket.book_recording_path.clone(),
        ],
    }
}

fn config_summary_view(config: &SpotSpotTakerArbitrageConfig) -> ConfigSummaryView {
    ConfigSummaryView {
        enabled_exchanges: config.exchanges.clone(),
        enabled_symbols: config.symbols.clone(),
        trading_mode: config.trading_mode.clone(),
        live_trading_enabled: config.live_trading_enabled,
        dry_run: config.dry_run,
        min_raw_spread_bps: Some(config.min_raw_spread_bps),
        min_net_spread_bps: Some(config.min_net_spread_bps),
        max_raw_spread_bps: Some(config.max_raw_spread_bps),
        max_notional_per_trade: Some(config.max_notional_per_trade),
        max_notional_per_symbol: Some(config.max_notional_per_symbol),
        max_total_notional: Some(config.max_total_notional),
        max_enabled_arbitrage_symbols: Some(config.max_enabled_arbitrage_symbols),
        initial_entry_notional_usdt: Some(config.initial_entry_notional_usdt),
        fee_config_summary: Some(config.fee_config_path.clone()),
        disabled_config_summary: Some(config.disabled_registry_path.clone()),
        secrets_redacted: true,
    }
}

fn mexc_spot_runtime_config(config: &SpotSpotTakerArbitrageConfig) -> MexcSpotConfig {
    let mut runtime = MexcSpotConfig::default();
    runtime.dry_run = !spot_live_mode(config);
    if !config.mexc.api_key.trim().is_empty() {
        runtime.api_key = config.mexc.api_key.clone();
    }
    if !config.mexc.api_secret.trim().is_empty() {
        runtime.api_secret = config.mexc.api_secret.clone();
    }
    if !config.mexc.base_url.trim().is_empty() {
        runtime.base_url = config.mexc.base_url.clone();
    }
    if !config.mexc.websocket_url.trim().is_empty() {
        runtime.websocket_url = config.mexc.websocket_url.clone();
    }
    runtime.stale_book_ms = config.stale_book_ms;
    runtime.request_timeout_ms = config.request_timeout_ms;
    runtime.orderbook_depth = config.orderbook_depth;
    runtime.fee_override = config.mexc.fee_override.map(Into::into);
    runtime.log_raw_messages = config.websocket.log_raw_messages;
    runtime
}

fn coinex_spot_runtime_config(config: &SpotSpotTakerArbitrageConfig) -> CoinExSpotConfig {
    let mut runtime = CoinExSpotConfig::default();
    runtime.dry_run = !spot_live_mode(config);
    if !config.coinex.api_key.trim().is_empty() {
        runtime.api_key = config.coinex.api_key.clone();
    }
    if !config.coinex.api_secret.trim().is_empty() {
        runtime.api_secret = config.coinex.api_secret.clone();
    }
    if !config.coinex.base_url.trim().is_empty() {
        runtime.base_url = config.coinex.base_url.clone();
    }
    if !config.coinex.websocket_url.trim().is_empty() {
        runtime.websocket_url = config.coinex.websocket_url.clone();
    }
    runtime.stale_book_ms = config.stale_book_ms;
    runtime.request_timeout_ms = config.request_timeout_ms;
    runtime.orderbook_depth = config.orderbook_depth;
    runtime.fee_override = config.coinex.fee_override.map(Into::into);
    runtime.symbol_mappings = config.coinex.symbol_mappings.clone();
    runtime.log_raw_messages = config.websocket.log_raw_messages;
    runtime
}

fn gateio_spot_runtime_config(config: &SpotSpotTakerArbitrageConfig) -> GateIoSpotConfig {
    let mut runtime = GateIoSpotConfig::default();
    runtime.dry_run = !spot_live_mode(config);
    if !config.gateio.api_key.trim().is_empty() {
        runtime.api_key = config.gateio.api_key.clone();
    }
    if !config.gateio.api_secret.trim().is_empty() {
        runtime.api_secret = config.gateio.api_secret.clone();
    }
    if !config.gateio.base_url.trim().is_empty() {
        runtime.base_url = config.gateio.base_url.clone();
    }
    if !config.gateio.websocket_url.trim().is_empty() {
        runtime.websocket_url = config.gateio.websocket_url.clone();
    }
    runtime.stale_book_ms = config.stale_book_ms;
    runtime.request_timeout_ms = config.request_timeout_ms;
    runtime.orderbook_depth = config.orderbook_depth;
    runtime.fee_override = config.gateio.fee_override.map(Into::into);
    runtime.log_raw_messages = config.websocket.log_raw_messages;
    runtime
}

fn bitget_spot_runtime_config(config: &SpotSpotTakerArbitrageConfig) -> BitgetSpotConfig {
    let mut runtime = BitgetSpotConfig::default();
    runtime.dry_run = !spot_live_mode(config);
    if !config.bitget.api_key.trim().is_empty() {
        runtime.api_key = config.bitget.api_key.clone();
    }
    if !config.bitget.api_secret.trim().is_empty() {
        runtime.api_secret = config.bitget.api_secret.clone();
    }
    if !config.bitget.passphrase.trim().is_empty() {
        runtime.passphrase = config.bitget.passphrase.clone();
    }
    if !config.bitget.base_url.trim().is_empty() {
        runtime.base_url = config.bitget.base_url.clone();
    }
    if !config.bitget.websocket_url.trim().is_empty() {
        runtime.websocket_url = config.bitget.websocket_url.clone();
    }
    runtime.stale_book_ms = config.stale_book_ms;
    runtime.request_timeout_ms = config.request_timeout_ms;
    runtime.orderbook_depth = config.orderbook_depth;
    runtime.fee_override = config.bitget.fee_override.map(Into::into);
    runtime.log_raw_messages = config.websocket.log_raw_messages;
    runtime
}

fn kucoin_spot_runtime_config(
    config: &SpotSpotTakerArbitrageConfig,
) -> crate::exchanges::kucoin::KuCoinSpotConfig {
    let mut runtime = crate::exchanges::kucoin::KuCoinSpotConfig::default();
    if !config.kucoin.api_key.trim().is_empty() {
        runtime.api_key = config.kucoin.api_key.clone();
    }
    if !config.kucoin.api_secret.trim().is_empty() {
        runtime.api_secret = config.kucoin.api_secret.clone();
    }
    if !config.kucoin.api_passphrase.trim().is_empty() {
        runtime.api_passphrase = config.kucoin.api_passphrase.clone();
    }
    if !config.kucoin.base_url.trim().is_empty() {
        runtime.base_url = config.kucoin.base_url.clone();
    }
    if !config.kucoin.websocket_url.trim().is_empty() {
        runtime.websocket_url = config.kucoin.websocket_url.clone();
    }
    runtime.stale_book_ms = config.stale_book_ms;
    runtime.request_timeout_ms = config.request_timeout_ms;
    runtime.orderbook_depth = config.orderbook_depth;
    runtime.enabled_symbols = config.symbols.clone();
    runtime.log_raw_messages = config.websocket.log_raw_messages;
    runtime
}

async fn live_readiness_state(
    config: &SpotSpotTakerArbitrageConfig,
    symbol_rules: &HashMap<String, CommonSymbolRules>,
    inventory: &PaperInventory,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
    book_cache: &BookCache,
    websocket_runtime: Option<&WebsocketMarketDataRuntime>,
) -> LiveReadinessState {
    let rules = flatten_spot_symbol_rules(symbol_rules);
    let books = book_cache
        .get_all_books()
        .await
        .into_iter()
        .map(book_view)
        .collect::<Vec<_>>();
    let exchanges = if let Some(runtime) = websocket_runtime {
        runtime
            .health
            .snapshot()
            .await
            .into_iter()
            .map(|health| ExchangeHealthView {
                exchange: health.exchange,
                market_type: health.market_type,
                connected: health.connected,
                public_ws_connected: health.connected,
                private_ws_connected: None,
                last_message_at: health.last_message_at,
                last_book_update_at: health.last_book_update_at,
                stale_symbol_count: health.stale_symbols.len(),
                fresh_symbol_count: health.fresh_symbols.len(),
                reconnect_count: health.reconnect_count,
                parse_error_count: health.parse_error_count,
                sequence_gap_count: health.sequence_gap_count,
                heartbeat_timeout_count: health.heartbeat_timeout_count,
                avg_latency_ms: health.avg_latency_ms,
                max_latency_ms: health.max_latency_ms,
            })
            .collect()
    } else {
        let target_exchanges = if config.live_preflight.exchanges.is_empty() {
            &config.exchanges
        } else {
            &config.live_preflight.exchanges
        };
        target_exchanges
            .iter()
            .map(|exchange| {
                let exchange = exchange.trim().to_ascii_lowercase();
                let exchange_books = books
                    .iter()
                    .filter(|book| book.exchange.eq_ignore_ascii_case(&exchange))
                    .collect::<Vec<_>>();
                ExchangeHealthView {
                    exchange,
                    market_type: Some(MarketType::Spot),
                    connected: !exchange_books.is_empty(),
                    public_ws_connected: false,
                    private_ws_connected: None,
                    last_message_at: None,
                    last_book_update_at: exchange_books
                        .iter()
                        .filter_map(|book| {
                            chrono::Utc::now().checked_sub_signed(chrono::Duration::milliseconds(
                                book.book_age_ms,
                            ))
                        })
                        .max(),
                    stale_symbol_count: exchange_books.iter().filter(|book| book.is_stale).count(),
                    fresh_symbol_count: exchange_books.iter().filter(|book| !book.is_stale).count(),
                    reconnect_count: 0,
                    parse_error_count: 0,
                    sequence_gap_count: 0,
                    heartbeat_timeout_count: 0,
                    avg_latency_ms: None,
                    max_latency_ms: None,
                }
            })
            .collect()
    };
    LiveReadinessState {
        trading_mode: config.trading_mode.clone(),
        live_trading_enabled: config.live_trading_enabled,
        dry_run: config.dry_run,
        monitoring_enabled: config.monitoring.enabled,
        recorder_enabled: config.enable_csv_recording || config.enable_database_recording,
        kill_switch_available: true,
        kill_switch_active: false,
        emergency_stop_configured: config.max_daily_loss > 0.0,
        max_daily_loss: Some(config.max_daily_loss),
        max_order_latency_ms: Some(config.max_book_latency_ms),
        symbol_rules: rules,
        inventory: inventory_views(inventory, disabled_registry, book_cache).await,
        fees: fee_views(fee_model),
        books,
        exchanges,
        disabled_symbols: disabled_registry
            .disabled_symbols()
            .into_iter()
            .map(|item| item.symbol)
            .collect(),
        disabled_exchanges: disabled_registry
            .disabled_exchanges()
            .into_iter()
            .map(|item| item.exchange)
            .collect(),
        disabled_exchange_symbols: disabled_registry
            .disabled_exchange_symbols()
            .into_iter()
            .map(|item| (item.exchange, item.market_type, item.symbol))
            .collect(),
        unmanaged_positions: disabled_registry
            .unmanaged_positions()
            .iter()
            .map(|position| {
                (
                    position.exchange.clone(),
                    position.market_type,
                    position.symbol.clone(),
                    position.asset.clone(),
                    position.quantity,
                )
            })
            .collect(),
        api_permissions: api_permissions_from_env(&config.live_preflight.exchanges),
        recorder: recorder_health_view(config, websocket_runtime),
    }
}

pub async fn build_live_preflight_state_from_config(
    config: &SpotSpotTakerArbitrageConfig,
    inventory: &PaperInventory,
    disabled_registry: &DisabledRegistry,
    fee_model: &FeeModel,
) -> Result<LiveReadinessState> {
    let mexc = MexcSpotClient::new(mexc_spot_runtime_config(config));
    let coinex = CoinExSpotClient::new(coinex_spot_runtime_config(config));
    let gateio = GateIoSpotClient::new(gateio_spot_runtime_config(config));
    let bitget = BitgetSpotClient::new(bitget_spot_runtime_config(config));
    let kucoin = KuCoinSpotClient::new(kucoin_spot_runtime_config(config));
    let symbol_rules =
        load_common_symbol_rules(config, &mexc, &coinex, &gateio, &bitget, &kucoin).await?;
    let book_cache = BookCache::default();
    populate_preflight_rest_books(config, &book_cache, &mexc, &coinex, &gateio, &bitget).await;
    let mut state = live_readiness_state(
        config,
        &symbol_rules,
        inventory,
        disabled_registry,
        fee_model,
        &book_cache,
        None,
    )
    .await;
    state.inventory = live_or_inventory_views(
        config,
        inventory,
        disabled_registry,
        &book_cache,
        &mexc,
        &coinex,
        &gateio,
        &bitget,
    )
    .await;
    Ok(state)
}

async fn populate_preflight_rest_books(
    config: &SpotSpotTakerArbitrageConfig,
    book_cache: &BookCache,
    mexc: &MexcSpotClient,
    coinex: &CoinExSpotClient,
    gateio: &GateIoSpotClient,
    bitget: &BitgetSpotClient,
) {
    let exchanges = if config.live_preflight.exchanges.is_empty() {
        &config.exchanges
    } else {
        &config.live_preflight.exchanges
    };
    let symbols = if config.live_preflight.symbols.is_empty() {
        &config.symbols
    } else {
        &config.live_preflight.symbols
    };
    for exchange in exchanges {
        let Some(venue) = spot_venue_from_exchange(exchange) else {
            log::warn!("preflight skipped unsupported spot exchange {}", exchange);
            continue;
        };
        for symbol in symbols {
            let result = match venue {
                SpotVenue::Mexc => mexc.get_orderbook(symbol, config.orderbook_depth).await,
                SpotVenue::CoinEx => coinex.get_orderbook(symbol, config.orderbook_depth).await,
                SpotVenue::GateIo => gateio.get_orderbook(symbol, config.orderbook_depth).await,
                SpotVenue::Bitget => bitget.get_orderbook(symbol, config.orderbook_depth).await,
            };
            match result {
                Ok(book) => book_cache.update_book(book, BookSource::Rest).await,
                Err(error) => log::warn!(
                    "preflight failed to fetch REST orderbook exchange={} symbol={}: {}",
                    venue.as_str(),
                    symbol,
                    error
                ),
            }
        }
    }
}

fn publish_risk_event(
    monitoring: &Option<MonitoringState>,
    symbol: &str,
    exchange: Option<&str>,
    reason: RejectionReason,
    details: Option<String>,
) {
    if let Some(state) = monitoring {
        state.record_risk_event(RiskEventView {
            timestamp: chrono::Utc::now(),
            event_type: "opportunity_rejection".to_string(),
            symbol: Some(symbol.to_string()),
            exchange: exchange.map(str::to_string),
            severity: "warning".to_string(),
            reason: format!("{reason:?}"),
            details,
        });
    }
}

#[derive(Debug, Clone)]
enum RiskStopReason {
    DailyLoss {
        pnl: f64,
        limit: f64,
    },
    TradeLoss {
        pnl: f64,
        limit: f64,
        symbol: String,
    },
    ConsecutiveRejections {
        count: u32,
        limit: u32,
    },
}

impl RiskStopReason {
    fn message(&self) -> String {
        match self {
            Self::DailyLoss { pnl, limit } => {
                format!("daily pnl {pnl:.6} breached max_daily_loss {limit:.6}")
            }
            Self::TradeLoss { pnl, limit, symbol } => {
                format!("{symbol} trade pnl {pnl:.6} breached max_trade_loss {limit:.6}")
            }
            Self::ConsecutiveRejections { count, limit } => {
                format!("consecutive rejections {count} reached max_consecutive_rejections {limit}")
            }
        }
    }

    fn symbol(&self) -> &str {
        match self {
            Self::TradeLoss { symbol, .. } => symbol,
            _ => "*",
        }
    }
}

fn risk_auto_stop_reason(
    risk: &RiskState,
    config: &SpotSpotTakerArbitrageConfig,
) -> Option<RiskStopReason> {
    if risk.daily_loss_limit_hit(config) {
        Some(RiskStopReason::DailyLoss {
            pnl: risk.daily_pnl(),
            limit: config.max_daily_loss,
        })
    } else if risk.consecutive_rejection_limit_hit(config) {
        Some(RiskStopReason::ConsecutiveRejections {
            count: risk.consecutive_rejections(),
            limit: config.max_consecutive_rejections,
        })
    } else {
        None
    }
}

fn trigger_risk_auto_stop(
    kill_switch: &KillSwitch,
    monitoring: &Option<MonitoringState>,
    reason: RiskStopReason,
) {
    let message = reason.message();
    kill_switch.trigger(message.clone(), "risk_auto_stop");
    if let Some(state) = monitoring {
        state.record_risk_event(RiskEventView {
            timestamp: chrono::Utc::now(),
            event_type: "risk_auto_stop".to_string(),
            symbol: Some(reason.symbol().to_string()),
            exchange: None,
            severity: "critical".to_string(),
            reason: "RiskAutoStop".to_string(),
            details: Some(message),
        });
        state.publish_kill_switch(kill_switch.state());
    }
}

async fn load_common_symbol_rules(
    config: &SpotSpotTakerArbitrageConfig,
    mexc: &MexcSpotClient,
    coinex: &CoinExSpotClient,
    gateio: &GateIoSpotClient,
    bitget: &BitgetSpotClient,
    kucoin: &KuCoinSpotClient,
) -> Result<HashMap<String, CommonSymbolRules>> {
    let (left_venue, right_venue) = configured_spot_pair(&config.exchanges);
    let (left_rules_result, right_rules_result) = match (left_venue, right_venue) {
        (SpotVenue::Mexc, SpotVenue::CoinEx) => {
            let (left, right) = tokio::join!(mexc.load_symbol_rules(), coinex.load_symbol_rules());
            (left, right)
        }
        (SpotVenue::GateIo, SpotVenue::Bitget) => {
            let (left, right) =
                tokio::join!(gateio.load_symbol_rules(), bitget.load_symbol_rules());
            (left, right)
        }
        _ => unreachable!("unsupported configured spot pair"),
    };
    let left_rules = left_rules_result.map_err(anyhow::Error::from)?;
    let right_rules = right_rules_result.map_err(anyhow::Error::from)?;

    let mut mexc_all = Vec::new();
    let mut coinex_all = Vec::new();
    let mut gateio_all = Vec::new();
    let mut bitget_all = Vec::new();
    let kucoin_all: Vec<SymbolRule> = Vec::new();
    match (left_venue, right_venue) {
        (SpotVenue::Mexc, SpotVenue::CoinEx) => {
            mexc_all = left_rules.clone();
            coinex_all = right_rules.clone();
        }
        (SpotVenue::GateIo, SpotVenue::Bitget) => {
            gateio_all = left_rules.clone();
            bitget_all = right_rules.clone();
        }
        _ => unreachable!("unsupported configured spot pair"),
    }
    let _ = kucoin;
    let mexc_by_symbol = mexc_all
        .iter()
        .map(|rule| (rule.internal_symbol.clone(), rule.clone()))
        .collect::<HashMap<_, _>>();
    let coinex_by_symbol = coinex_all
        .iter()
        .map(|rule| (rule.internal_symbol.clone(), rule.clone()))
        .collect::<HashMap<_, _>>();
    let gateio_by_symbol = gateio_all
        .iter()
        .map(|rule| (rule.internal_symbol.clone(), rule.clone()))
        .collect::<HashMap<_, _>>();
    let bitget_by_symbol = bitget_all
        .iter()
        .map(|rule| (rule.internal_symbol.clone(), rule.clone()))
        .collect::<HashMap<_, _>>();
    let _kucoin_by_symbol = kucoin_all
        .iter()
        .map(|rule| (rule.internal_symbol.clone(), rule.clone()))
        .collect::<HashMap<_, _>>();
    let left_by_symbol = left_rules
        .into_iter()
        .map(|rule| (rule.internal_symbol.clone(), rule))
        .collect::<HashMap<_, _>>();
    let right_by_symbol = right_rules
        .into_iter()
        .map(|rule| (rule.internal_symbol.clone(), rule))
        .collect::<HashMap<_, _>>();

    let mut common = HashMap::new();
    for symbol in &config.symbols {
        let normalized = symbol.trim().to_ascii_uppercase();
        if let (Some(left), Some(right)) = (
            left_by_symbol.get(&normalized),
            right_by_symbol.get(&normalized),
        ) {
            let rules = match (left_venue, right_venue) {
                (SpotVenue::Mexc, SpotVenue::CoinEx) => CommonSymbolRules {
                    mexc: left.clone(),
                    coinex: right.clone(),
                    gateio: gateio_by_symbol.get(&normalized).cloned(),
                    bitget: bitget_by_symbol.get(&normalized).cloned(),
                    kucoin: kucoin_all
                        .iter()
                        .find(|rule| rule.internal_symbol == normalized)
                        .cloned(),
                },
                (SpotVenue::GateIo, SpotVenue::Bitget) => CommonSymbolRules {
                    mexc: mexc_by_symbol
                        .get(&normalized)
                        .cloned()
                        .unwrap_or_else(|| left.clone()),
                    coinex: coinex_by_symbol
                        .get(&normalized)
                        .cloned()
                        .unwrap_or_else(|| right.clone()),
                    gateio: Some(left.clone()),
                    bitget: Some(right.clone()),
                    kucoin: kucoin_all
                        .iter()
                        .find(|rule| rule.internal_symbol == normalized)
                        .cloned(),
                },
                _ => unreachable!("unsupported configured spot pair"),
            };
            common.insert(normalized.clone(), rules);
        } else {
            log_warn_once(
                format!(
                    "symbol_missing:{}:{}:{}",
                    left_venue.as_str(),
                    right_venue.as_str(),
                    normalized
                ),
                || {
                    format!(
                        "spot_spot_taker_arbitrage symbol missing on configured venues {}-{}: {}",
                        left_venue.as_str(),
                        right_venue.as_str(),
                        normalized
                    )
                },
            );
        }
    }
    Ok(common)
}

#[cfg(test)]
mod tests;

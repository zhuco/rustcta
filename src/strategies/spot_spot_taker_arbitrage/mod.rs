//! Paper-only MEXC <-> CoinEx Spot taker arbitrage strategy.
//!
//! This module never submits live orders. It reads public Spot order books,
//! evaluates executable taker/taker spreads, simulates fills against paper
//! inventory, and records both accepted and rejected opportunities.

pub mod book_cache;
pub mod book_recorder;
pub mod config;
pub mod inventory;
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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tokio::time::{interval, MissedTickBehavior};

use crate::control::spot_control::{
    snapshot_store_from_config, RuntimeExchangeClient, RuntimeReconciliationServices,
    SpotControlRuntimePublisher, SpotControlRuntimePublisherDeps, SpotControlService,
};
use crate::exchanges::bitget::{BitgetSpotClient, BitgetSpotConfig};
use crate::exchanges::coinex::{CoinExSpotClient, CoinExSpotConfig};
use crate::exchanges::gateio::{GateIoSpotClient, GateIoSpotConfig};
use crate::exchanges::mexc::{MexcSpotClient, MexcSpotConfig};
use crate::exchanges::unified::{ExchangeClient, MarketType, OrderBookSnapshot, OrderSide};
use crate::execution::{
    append_live_dry_run_plan, build_live_dry_run_order_plan, reconcile_dashboard_inventory,
    FeeLookupKey, FeeModel, FeeRole, LiveDryRunOrderInput, LiveDryRunOrderPlan,
};
use crate::live_preflight::{
    api_permissions_from_env, evaluate_small_live_gate, run_live_preflight, LiveReadinessState,
    SmallLiveGateInput,
};
use crate::risk::{DisabledRegistry, DisabledStatus, KillSwitch, KillSwitchState};
use crate::strategies::arbitrage_core::{
    analyze_relationship, record_analysis_jsonl, ArbitrageRelationship, ArbitrageRelationshipType,
    MarketLeg,
};
use crate::web::{
    spawn_monitoring_server, BookView, ConfigSummaryView, DashboardReadModel,
    DisabledExchangeSymbolView, DisabledExchangeView, DisabledSymbolView, DisabledView,
    ExchangeHealthView, FeeView, InventoryView, MonitoringState, OpportunityView,
    RecorderHealthView, RiskEventView, TradeView, UnmanagedPositionView,
};

pub const SPOT_SPOT_TAKER_ARBITRAGE_STRATEGY_NAME: &str = "spot_spot_taker_arbitrage";

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
            state.publish_inventory(inventory_views(&inventory, &disabled_registry));
            state.publish_config_summary(config_summary_view(&self.config));
            state.publish_order_reconciliation_config(self.config.order_reconciliation.clone());
            state.publish_kill_switch(kill_switch.state());
            Some(state)
        } else {
            None
        };

        let mexc = MexcSpotClient::new(MexcSpotConfig {
            dry_run: true,
            api_key: self.config.mexc.api_key.clone(),
            api_secret: self.config.mexc.api_secret.clone(),
            base_url: self.config.mexc.base_url.clone(),
            websocket_url: self.config.mexc.websocket_url.clone(),
            stale_book_ms: self.config.stale_book_ms,
            request_timeout_ms: self.config.request_timeout_ms,
            orderbook_depth: self.config.orderbook_depth,
            fee_override: self.config.mexc.fee_override.map(Into::into),
            log_raw_messages: self.config.websocket.log_raw_messages,
            ..MexcSpotConfig::default()
        });
        let coinex = CoinExSpotClient::new(CoinExSpotConfig {
            dry_run: true,
            api_key: self.config.coinex.api_key.clone(),
            api_secret: self.config.coinex.api_secret.clone(),
            base_url: self.config.coinex.base_url.clone(),
            websocket_url: self.config.coinex.websocket_url.clone(),
            stale_book_ms: self.config.stale_book_ms,
            request_timeout_ms: self.config.request_timeout_ms,
            orderbook_depth: self.config.orderbook_depth,
            fee_override: self.config.coinex.fee_override.map(Into::into),
            symbol_mappings: self.config.coinex.symbol_mappings.clone(),
            log_raw_messages: self.config.websocket.log_raw_messages,
            ..CoinExSpotConfig::default()
        });

        let gateio = GateIoSpotClient::new(GateIoSpotConfig {
            dry_run: true,
            api_key: self.config.gateio.api_key.clone(),
            api_secret: self.config.gateio.api_secret.clone(),
            base_url: if self.config.gateio.base_url.is_empty() {
                GateIoSpotConfig::default().base_url
            } else {
                self.config.gateio.base_url.clone()
            },
            websocket_url: if self.config.gateio.websocket_url.is_empty() {
                GateIoSpotConfig::default().websocket_url
            } else {
                self.config.gateio.websocket_url.clone()
            },
            stale_book_ms: self.config.stale_book_ms,
            request_timeout_ms: self.config.request_timeout_ms,
            orderbook_depth: self.config.orderbook_depth,
            fee_override: self.config.gateio.fee_override.map(Into::into),
            log_raw_messages: self.config.websocket.log_raw_messages,
            ..GateIoSpotConfig::default()
        });
        let bitget = BitgetSpotClient::new(BitgetSpotConfig {
            dry_run: true,
            api_key: self.config.bitget.api_key.clone(),
            api_secret: self.config.bitget.api_secret.clone(),
            passphrase: if self.config.bitget.passphrase.is_empty() {
                std::env::var("BITGET_API_PASSPHRASE")
                    .or_else(|_| std::env::var("BITGET_PASSPHRASE"))
                    .unwrap_or_default()
            } else {
                self.config.bitget.passphrase.clone()
            },
            base_url: if self.config.bitget.base_url.is_empty() {
                BitgetSpotConfig::default().base_url
            } else {
                self.config.bitget.base_url.clone()
            },
            websocket_url: if self.config.bitget.websocket_url.is_empty() {
                BitgetSpotConfig::default().websocket_url
            } else {
                self.config.bitget.websocket_url.clone()
            },
            stale_book_ms: self.config.stale_book_ms,
            request_timeout_ms: self.config.request_timeout_ms,
            orderbook_depth: self.config.orderbook_depth,
            fee_override: self.config.bitget.fee_override.map(Into::into),
            log_raw_messages: self.config.websocket.log_raw_messages,
            ..BitgetSpotConfig::default()
        });

        let symbol_rules =
            load_common_symbol_rules(&self.config, &mexc, &coinex, &gateio, &bitget).await?;
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
        let book_cache = BookCache::default();
        let websocket_runtime = if self.config.market_data_mode == MarketDataMode::WebsocketCache {
            Some(
                start_websocket_market_data(
                    &self.config,
                    mexc.clone(),
                    coinex.clone(),
                    gateio.clone(),
                    bitget.clone(),
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
        let _monitoring_server = if let Some(state) = monitoring.clone() {
            spawn_monitoring_server(self.config.monitoring.clone(), state).await?
        } else {
            None
        };

        let mut ticker = interval(Duration::from_millis(self.config.scan_interval_ms));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut last_report_at = chrono::Utc::now();

        loop {
            ticker.tick().await;
            if let Some(state) = &monitoring {
                state.publish_strategy_status("running");
            }
            for symbol in &self.config.symbols {
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
                        let Some(mexc_book) =
                            book_cache.get_book(left_venue.as_str(), symbol).await
                        else {
                            report.record_rejection(RejectionReason::StaleBook);
                            risk.record_rejection(symbol, RejectionReason::StaleBook);
                            publish_risk_event(
                                &monitoring,
                                symbol,
                                Some(left_venue.as_str()),
                                RejectionReason::StaleBook,
                                Some(format!("missing {} cached book", left_venue.as_str())),
                            );
                            continue;
                        };
                        let Some(coinex_book) =
                            book_cache.get_book(right_venue.as_str(), symbol).await
                        else {
                            report.record_rejection(RejectionReason::StaleBook);
                            risk.record_rejection(symbol, RejectionReason::StaleBook);
                            publish_risk_event(
                                &monitoring,
                                symbol,
                                Some(right_venue.as_str()),
                                RejectionReason::StaleBook,
                                Some(format!("missing {} cached book", right_venue.as_str())),
                            );
                            continue;
                        };
                        let mexc_source = mexc_book.source;
                        let coinex_source = coinex_book.source;
                        (
                            mexc_book.into_snapshot(),
                            coinex_book.into_snapshot(),
                            mexc_source,
                            coinex_source,
                        )
                    }
                    MarketDataMode::Replay => unreachable!("replay mode returns before live loop"),
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

                for mut opportunity in opportunities {
                    if opportunity.accepted {
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
                                    opportunity.accepted = false;
                                    opportunity.rejection_reason =
                                        Some(RejectionReason::ControlPlaneBlocked);
                                    opportunity.rejection_detail =
                                        Some(tradability.rejection_reasons.join(","));
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
                        if let Some(reason) = opportunity.rejection_reason {
                            risk.record_rejection(&opportunity.symbol, reason);
                            publish_risk_event(
                                &monitoring,
                                &opportunity.symbol,
                                None,
                                reason,
                                opportunity.rejection_detail.clone(),
                            );
                        }
                        continue;
                    }

                    if self
                        .config
                        .trading_mode
                        .eq_ignore_ascii_case("live_dry_run")
                    {
                        match build_live_dry_run_pair(
                            &self.config,
                            &opportunity,
                            rules,
                            &mexc_book,
                            &coinex_book,
                            &inventory,
                            &disabled_registry,
                            &fee_model,
                            &mexc,
                            &coinex,
                            &gateio,
                            &bitget,
                        )
                        .await
                        {
                            Ok(plans) => {
                                for plan in plans {
                                    append_live_dry_run_plan(
                                        &self.config.live_dry_run.output_path,
                                        &plan,
                                    )?;
                                    if let Some(state) = &monitoring {
                                        state.record_live_dry_run_order(plan);
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
                                risk.apply_trade_cooldown(
                                    &trade.symbol,
                                    self.config.cooldown_ms_after_trade,
                                );
                                report.record_trade(&trade);
                                if let Some(state) = &monitoring {
                                    state.record_trade(trade_view(&trade));
                                    state.publish_inventory(inventory_views(
                                        &inventory,
                                        &disabled_registry,
                                    ));
                                }
                                recorder.record_trade(trade).await;
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
                state.publish_recorder(recorder_health_view(
                    &self.config,
                    websocket_runtime.as_ref(),
                ));
                let balance_report =
                    reconcile_dashboard_inventory(&inventory_views(&inventory, &disabled_registry));
                state.publish_balance_reconciliation(balance_report.clone());
                state.publish_kill_switch(kill_switch.state());
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
                    unmanaged_inventory_overlap: !snapshot.unmanaged_positions.is_empty(),
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
    exchange_clients.insert("mexc".to_string(), Arc::new(mexc));
    exchange_clients.insert("coinex".to_string(), Arc::new(coinex));
    exchange_clients.insert("gateio".to_string(), Arc::new(gateio));
    exchange_clients.insert("bitget".to_string(), Arc::new(bitget));

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
        .lookup(&FeeLookupKey {
            exchange: opportunity.buy_exchange.clone(),
            market_type: MarketType::Spot,
            symbol: Some(opportunity.symbol.clone()),
            liquidity_role: FeeRole::Taker,
        })
        .effective_rate;
    let sell_fee = fee_model
        .lookup(&FeeLookupKey {
            exchange: opportunity.sell_exchange.clone(),
            market_type: MarketType::Spot,
            symbol: Some(opportunity.symbol.clone()),
            liquidity_role: FeeRole::Taker,
        })
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
            log::warn!(
                "live dry-run using configured inventory balances for {} because private balance fetch failed: {}",
                exchange.as_str(),
                error
            );
            inventory
                .balances_snapshot()
                .into_iter()
                .filter(|(venue, _, _)| *venue == exchange)
                .map(|(_, asset, state)| state.as_asset_balance(&asset))
                .collect()
        }
    }
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

fn inventory_views(
    inventory: &PaperInventory,
    disabled_registry: &DisabledRegistry,
) -> Vec<InventoryView> {
    inventory
        .balances_snapshot()
        .into_iter()
        .map(|(exchange, asset, state)| {
            let unmanaged_quantity = disabled_registry
                .unmanaged_positions()
                .iter()
                .filter(|position| {
                    position.exchange == exchange.as_str()
                        && position.market_type == MarketType::Spot
                        && position.asset == asset
                })
                .map(|position| position.quantity)
                .sum();
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
                valuation_usdt: None,
            }
        })
        .collect()
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
        max_notional_per_trade: Some(config.max_notional_per_trade),
        max_notional_per_symbol: Some(config.max_notional_per_symbol),
        max_total_notional: Some(config.max_total_notional),
        fee_config_summary: Some(config.fee_config_path.clone()),
        disabled_config_summary: Some(config.disabled_registry_path.clone()),
        secrets_redacted: true,
    }
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
    let mut rules = Vec::new();
    for common in symbol_rules.values() {
        rules.push(common.mexc.clone());
        rules.push(common.coinex.clone());
    }
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
        Vec::new()
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
        inventory: inventory_views(inventory, disabled_registry),
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

async fn load_common_symbol_rules(
    config: &SpotSpotTakerArbitrageConfig,
    mexc: &MexcSpotClient,
    coinex: &CoinExSpotClient,
    gateio: &GateIoSpotClient,
    bitget: &BitgetSpotClient,
) -> Result<HashMap<String, CommonSymbolRules>> {
    let (left_venue, right_venue) = configured_spot_pair(&config.exchanges);
    let (left_rules, right_rules) = match (left_venue, right_venue) {
        (SpotVenue::Mexc, SpotVenue::CoinEx) => {
            let (left, right) = tokio::join!(mexc.load_symbol_rules(), coinex.load_symbol_rules());
            (left?, right?)
        }
        (SpotVenue::GateIo, SpotVenue::Bitget) => {
            let (left, right) =
                tokio::join!(gateio.load_symbol_rules(), bitget.load_symbol_rules());
            (left?, right?)
        }
        _ => unreachable!("unsupported configured spot pair"),
    };
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
                    gateio: None,
                    bitget: None,
                },
                (SpotVenue::GateIo, SpotVenue::Bitget) => CommonSymbolRules {
                    mexc: left.clone(),
                    coinex: right.clone(),
                    gateio: Some(left.clone()),
                    bitget: Some(right.clone()),
                },
                _ => unreachable!("unsupported configured spot pair"),
            };
            common.insert(normalized.clone(), rules);
        } else {
            log::warn!(
                "spot_spot_taker_arbitrage symbol missing on one venue: {}",
                normalized
            );
        }
    }
    Ok(common)
}

#[cfg(test)]
mod tests;

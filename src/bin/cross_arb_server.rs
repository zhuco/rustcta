use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[path = "cross_arb_server/ws.rs"]
mod cross_arb_server_ws;

use anyhow::{Context, Result};
use axum::extract::State;
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Duration, Utc};
use clap::{Parser, ValueEnum};
use rustcta::exchanges::adapters::{
    BinanceMarketAdapter, BitgetMarketAdapter, GateMarketAdapter, OkxMarketAdapter,
};
use rustcta::market::{
    exchange_symbol_for, mark_book_freshness, BookLevel, CanonicalSymbol, ContractType, ExchangeId,
    InstrumentMeta, InstrumentStatus, MarketDataAdapter, MarketFundingSnapshot, MarketStateCache,
    OrderBook5, RouteStatus, RuntimeMode,
};
use rustcta::strategies::cross_exchange_arbitrage::{
    calculate_taker_vwap, scan_opportunities, BundleReadModel, CrossArbDashboardStatus,
    CrossExchangeArbitrageConfig, FeeModel, FeeRole, MakerLegKind, MarketSnapshot, Opportunity,
    OpportunityReadModel, OrderBookQualityReadModel, OrderSide, PortfolioExposureSummary,
    ReconcileReadModel, RejectReason, RiskEventReadModel, RouteReadModel, SimulatedBundleState,
    SimulatedBundleStatus,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration as TokioDuration};
use tower_http::cors::{Any, CorsLayer};

const DEFAULT_HISTORY_LIMIT: usize = 1_000;
const DEFAULT_OPPORTUNITY_LIMIT: usize = 250;
const DEFAULT_FILLS_PER_TICK: usize = 20;
const DEFAULT_SHADOW_FILL_COOLDOWN_SECS: i64 = 300;
const SHADOW_FORCE_CLOSE_PROFIT_PCT: f64 = 0.0;

#[derive(Parser, Debug)]
#[command(
    name = "cross_arb_server",
    version,
    about = "RustCTA cross-exchange arbitrage shadow trading analytics dashboard"
)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    host: String,
    #[arg(long, default_value_t = 8090)]
    port: u16,
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    config: PathBuf,
    #[arg(long, default_value_t = true)]
    simulate: bool,
    #[arg(long, value_enum, default_value_t = DataSource::PublicRest)]
    data_source: DataSource,
    #[arg(long, default_value_t = 100)]
    symbol_count: usize,
    #[arg(long, default_value_t = 5)]
    refresh_secs: u64,
    #[arg(long, default_value_t = 5_000)]
    request_timeout_ms: u64,
    #[arg(long, default_value = "data/cross_arb_shadow")]
    shadow_store_dir: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum DataSource {
    PublicRest,
    PublicWs,
    Synthetic,
}

#[derive(Clone)]
struct AppState {
    read_model: Arc<RwLock<CrossArbReadModel>>,
}

impl AppState {
    fn new(
        config_path: impl AsRef<Path>,
        symbol_count: usize,
        data_source: DataSource,
        shadow_store_dir: impl Into<PathBuf>,
    ) -> Result<Self> {
        Ok(Self {
            read_model: Arc::new(RwLock::new(CrossArbReadModel::new(
                config_path.as_ref(),
                symbol_count,
                data_source,
                shadow_store_dir.into(),
            )?)),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
struct CrossArbReadModel {
    status: CrossArbDashboardStatus,
    analytics: AnalyticsSummary,
    data_source: DataSource,
    books_loaded: usize,
    funding_loaded: usize,
    market_errors: Vec<MarketDataError>,
    symbols: Vec<SymbolAnalytics>,
    opportunities: Vec<OpportunityReadModel>,
    #[serde(skip)]
    shadow_positions: HashMap<CanonicalSymbol, ShadowOpenPosition>,
    simulated_fills: Vec<SimulatedMarketFill>,
    open_bundles: Vec<BundleReadModel>,
    bundle_history: Vec<BundleReadModel>,
    exchanges: Vec<ExchangeReadModel>,
    routes: Vec<RouteReadModel>,
    book_quality: Vec<OrderBookQualityReadModel>,
    reconcile_reports: Vec<ReconcileReadModel>,
    risk_events: Vec<RiskEventReadModel>,
    reject_breakdown: Vec<RejectReasonCount>,
    config_summary: ConfigSummaryReadModel,
    control: ControlState,
    tool_commands: Vec<ToolCommandReadModel>,
    capital: Vec<ExchangeCapitalReadModel>,
    #[serde(skip)]
    config: CrossExchangeArbitrageConfig,
    #[serde(skip)]
    monitored_symbols: Vec<CanonicalSymbol>,
    #[serde(skip)]
    market_cache: MarketStateCache,
    #[serde(skip)]
    instrument_coverage: Option<ExchangeInstrumentCoverage>,
    #[serde(skip)]
    last_shadow_fill_by_symbol: HashMap<CanonicalSymbol, DateTime<Utc>>,
    #[serde(skip)]
    tick_index: u64,
    shadow_store: ShadowPersistenceStore,
}

impl CrossArbReadModel {
    fn new(
        config_path: &Path,
        symbol_count: usize,
        data_source: DataSource,
        shadow_store_dir: PathBuf,
    ) -> Result<Self> {
        let now = Utc::now();
        let (config_summary, mut config) = ConfigSummaryReadModel::load_config(config_path)?;
        config.mode = RuntimeMode::Simulation;
        config.execution.dry_run = true;
        let stale_quote_ms = config.risk.stale_quote_ms;
        let monitored_symbols = build_symbol_universe(&config, symbol_count);
        let shadow_store = ShadowPersistenceStore::load(shadow_store_dir, &monitored_symbols);

        let mut model = Self {
            status: CrossArbDashboardStatus {
                mode: RuntimeMode::Simulation,
                updated_at: now,
                enabled_symbols: monitored_symbols.len(),
                enabled_exchanges: config.universe.enabled_exchanges.len().max(2),
                open_bundles: 0,
                position_summary: PortfolioExposureSummary::default(),
                route_health: Vec::new(),
            },
            analytics: AnalyticsSummary::empty(now),
            data_source,
            books_loaded: 0,
            funding_loaded: 0,
            market_errors: Vec::new(),
            symbols: Vec::new(),
            opportunities: Vec::new(),
            shadow_positions: HashMap::new(),
            simulated_fills: Vec::new(),
            open_bundles: Vec::new(),
            bundle_history: Vec::new(),
            exchanges: Vec::new(),
            routes: Vec::new(),
            book_quality: Vec::new(),
            reconcile_reports: Vec::new(),
            risk_events: Vec::new(),
            reject_breakdown: Vec::new(),
            config_summary,
            control: ControlState::default(),
            tool_commands: tool_commands(),
            capital: Vec::new(),
            config,
            monitored_symbols,
            market_cache: MarketStateCache::new(stale_quote_ms),
            instrument_coverage: None,
            last_shadow_fill_by_symbol: HashMap::new(),
            tick_index: 0,
            shadow_store,
        };
        model.restore_shadow_persistence(now);
        if data_source == DataSource::Synthetic {
            model.simulate_synthetic_tick(now);
        }
        Ok(model)
    }

    fn touch(&mut self) {
        self.status.updated_at = Utc::now();
    }

    fn reset_simulation(&mut self, now: DateTime<Utc>) {
        self.simulated_fills.clear();
        self.shadow_positions.clear();
        self.bundle_history.clear();
        self.open_bundles.clear();
        self.last_shadow_fill_by_symbol.clear();
        self.shadow_store
            .record_event(ShadowPersistenceEventKind::Reset, now);
        self.persist_shadow_state(now);
        self.tick_index = 0;
        if self.data_source == DataSource::Synthetic {
            self.simulate_synthetic_tick(now);
        } else {
            self.clear_market_view(now);
        }
    }

    fn restore_shadow_persistence(&mut self, now: DateTime<Utc>) {
        if !self.shadow_store.enabled {
            return;
        }
        let restored_positions = self
            .shadow_store
            .current_positions
            .iter()
            .filter(|position| self.monitored_symbols.contains(&position.canonical_symbol))
            .cloned()
            .map(|position| (position.canonical_symbol.clone(), position))
            .collect::<HashMap<_, _>>();
        let mut restored_fills = self.shadow_store.simulated_fills.clone();
        trim_front(&mut restored_fills, DEFAULT_HISTORY_LIMIT);

        self.shadow_positions = restored_positions;
        self.simulated_fills = restored_fills;
        for fill in &self.simulated_fills {
            self.last_shadow_fill_by_symbol
                .insert(fill.canonical_symbol.clone(), fill.closed_at);
        }
        self.refresh_shadow_bundle_views();
        self.shadow_store.last_saved_at = Some(now);
    }

    fn persist_shadow_state(&mut self, now: DateTime<Utc>) {
        let mut positions = self
            .shadow_positions
            .values()
            .cloned()
            .collect::<Vec<ShadowOpenPosition>>();
        positions.sort_by(|left, right| left.bundle_id.cmp(&right.bundle_id));
        self.shadow_store.current_positions = positions;
        self.shadow_store.simulated_fills = self.simulated_fills.clone();
        self.shadow_store.save(now);
    }

    fn simulate_synthetic_tick(&mut self, now: DateTime<Utc>) {
        self.tick_index += 1;
        let exchanges = simulation_exchanges(&self.config);
        let mut all_opportunities = Vec::new();
        let mut symbols = Vec::with_capacity(self.monitored_symbols.len());
        let mut routes = Vec::new();
        let mut book_quality = Vec::new();
        let mut reject_counts: BTreeMap<String, u64> = BTreeMap::new();
        let mut snapshots_by_symbol = HashMap::new();

        for (index, symbol) in self.monitored_symbols.iter().enumerate() {
            let snapshots =
                synthetic_market_snapshots(symbol, index, self.tick_index, now, &exchanges);
            snapshots_by_symbol.insert(symbol.clone(), snapshots.clone());
            book_quality.extend(book_quality_from_snapshots(
                &snapshots,
                now,
                self.config.risk.max_book_age_ms,
            ));
            all_opportunities.extend(scan_opportunities(symbol, &snapshots, &self.config, now));
        }

        all_opportunities.sort_by(|left, right| {
            right
                .maker_taker_net_edge
                .partial_cmp(&left.maker_taker_net_edge)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        apply_shadow_capacity_to_opportunities(
            &mut all_opportunities,
            &self.config,
            &self.shadow_positions,
        );
        let best_by_symbol = best_opportunity_by_symbol(&all_opportunities);
        for symbol in &self.monitored_symbols {
            let best = best_by_symbol.get(symbol).copied();
            if let Some(best) = best {
                for reason in &best.reject_reasons {
                    *reject_counts.entry(format!("{:?}", reason)).or_insert(0) += 1;
                }
                routes.push(RouteReadModel {
                    exchange: best.maker_exchange.clone(),
                    canonical_symbol: best.canonical_symbol.clone(),
                    status: best.route_status,
                    last_book_age_ms: best.book_age_ms,
                    reject_reasons: best.reject_reasons.clone(),
                });
            }
            symbols.push(SymbolAnalytics::from_best(symbol.clone(), best, now));
        }
        self.update_shadow_positions(&snapshots_by_symbol, &all_opportunities, now);

        self.opportunities = all_opportunities
            .iter()
            .take(DEFAULT_OPPORTUNITY_LIMIT)
            .map(OpportunityReadModel::from)
            .collect();

        self.symbols = symbols;
        self.routes = routes;
        self.book_quality = book_quality;
        self.reject_breakdown = reject_counts
            .into_iter()
            .map(|(reason, count)| RejectReasonCount { reason, count })
            .collect();
        self.exchanges = exchange_read_models(&exchanges, self.monitored_symbols.len(), now);
        self.analytics = AnalyticsSummary::from_state(
            &self.monitored_symbols,
            &self.opportunities,
            &self.simulated_fills,
            &self.reject_breakdown,
            self.books_loaded,
            self.funding_loaded,
            self.market_errors.len(),
            now,
        );
        self.status = CrossArbDashboardStatus {
            mode: RuntimeMode::Simulation,
            updated_at: now,
            enabled_symbols: self.monitored_symbols.len(),
            enabled_exchanges: exchanges.len(),
            open_bundles: self.shadow_positions.len(),
            position_summary: PortfolioExposureSummary::default(),
            route_health: self.routes.iter().take(100).cloned().collect(),
        };
    }

    fn apply_public_market_refresh(&mut self, refresh: PublicMarketRefresh, now: DateTime<Utc>) {
        self.tick_index += 1;
        self.books_loaded = refresh.books_loaded;
        self.funding_loaded = refresh.funding_loaded;
        self.market_errors = refresh.errors;
        self.apply_snapshots(refresh.snapshots_by_symbol, refresh.exchanges, now);
    }

    fn apply_snapshots(
        &mut self,
        snapshots_by_symbol: HashMap<CanonicalSymbol, Vec<MarketSnapshot>>,
        exchanges: Vec<ExchangeId>,
        now: DateTime<Utc>,
    ) {
        let mut all_opportunities = Vec::new();
        let mut symbols = Vec::with_capacity(self.monitored_symbols.len());
        let mut routes = Vec::new();
        let mut book_quality = Vec::new();
        let mut reject_counts: BTreeMap<String, u64> = BTreeMap::new();

        for symbol in &self.monitored_symbols {
            let snapshots = snapshots_by_symbol.get(symbol).cloned().unwrap_or_default();
            book_quality.extend(book_quality_from_snapshots(
                &snapshots,
                now,
                self.config.risk.max_book_age_ms,
            ));
            let opportunities = if snapshots.len() >= 2 {
                scan_opportunities(symbol, &snapshots, &self.config, now)
            } else {
                *reject_counts
                    .entry("NoTwoExchangeBook".to_string())
                    .or_insert(0) += 1;
                Vec::new()
            };
            all_opportunities.extend(opportunities);
        }

        all_opportunities.sort_by(|left, right| {
            right
                .maker_taker_net_edge
                .partial_cmp(&left.maker_taker_net_edge)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        apply_shadow_capacity_to_opportunities(
            &mut all_opportunities,
            &self.config,
            &self.shadow_positions,
        );
        let best_by_symbol = best_opportunity_by_symbol(&all_opportunities);
        for symbol in &self.monitored_symbols {
            let best = best_by_symbol.get(symbol).copied();
            if let Some(best) = best {
                for reason in &best.reject_reasons {
                    *reject_counts.entry(format!("{:?}", reason)).or_insert(0) += 1;
                }
                routes.push(RouteReadModel {
                    exchange: best.maker_exchange.clone(),
                    canonical_symbol: best.canonical_symbol.clone(),
                    status: best.route_status,
                    last_book_age_ms: best.book_age_ms,
                    reject_reasons: best.reject_reasons.clone(),
                });
            }
            symbols.push(SymbolAnalytics::from_best(symbol.clone(), best, now));
        }
        self.update_shadow_positions(&snapshots_by_symbol, &all_opportunities, now);
        self.opportunities = all_opportunities
            .iter()
            .take(DEFAULT_OPPORTUNITY_LIMIT)
            .map(OpportunityReadModel::from)
            .collect();
        self.symbols = symbols;
        self.routes = routes;
        self.book_quality = book_quality;
        self.reject_breakdown = reject_counts
            .into_iter()
            .map(|(reason, count)| RejectReasonCount { reason, count })
            .collect();
        self.exchanges = exchange_read_models_with_errors(
            &exchanges,
            self.monitored_symbols.len(),
            now,
            &self.market_errors,
        );
        self.analytics = AnalyticsSummary::from_state(
            &self.monitored_symbols,
            &self.opportunities,
            &self.simulated_fills,
            &self.reject_breakdown,
            self.books_loaded,
            self.funding_loaded,
            self.market_errors.len(),
            now,
        );
        self.status = CrossArbDashboardStatus {
            mode: RuntimeMode::Simulation,
            updated_at: now,
            enabled_symbols: self.monitored_symbols.len(),
            enabled_exchanges: exchanges.len(),
            open_bundles: self.shadow_positions.len(),
            position_summary: PortfolioExposureSummary::default(),
            route_health: self.routes.iter().take(100).cloned().collect(),
        };
    }

    fn clear_market_view(&mut self, now: DateTime<Utc>) {
        self.books_loaded = 0;
        self.funding_loaded = 0;
        self.opportunities.clear();
        self.book_quality.clear();
        if self.data_source == DataSource::PublicWs {
            self.market_cache = MarketStateCache::new(self.config.risk.stale_quote_ms);
        }
        self.symbols = self
            .monitored_symbols
            .iter()
            .cloned()
            .map(|symbol| SymbolAnalytics::from_best(symbol, None, now))
            .collect();
        self.analytics = AnalyticsSummary::from_state(
            &self.monitored_symbols,
            &self.opportunities,
            &self.simulated_fills,
            &self.reject_breakdown,
            self.books_loaded,
            self.funding_loaded,
            self.market_errors.len(),
            now,
        );
        self.capital = exchange_capital_models(&self.config, &self.shadow_positions);
    }

    fn update_shadow_positions(
        &mut self,
        snapshots_by_symbol: &HashMap<CanonicalSymbol, Vec<MarketSnapshot>>,
        all_opportunities: &[Opportunity],
        now: DateTime<Utc>,
    ) {
        let mut closed = Vec::new();
        let open_symbols = self.shadow_positions.keys().cloned().collect::<Vec<_>>();
        for symbol in open_symbols {
            let Some(position) = self.shadow_positions.get(&symbol).cloned() else {
                continue;
            };
            let Some(snapshots) = snapshots_by_symbol.get(&symbol) else {
                continue;
            };
            let Some(close) = estimate_shadow_close(&position, snapshots, &self.config, now) else {
                continue;
            };
            if let Some(position) = self.shadow_positions.get_mut(&symbol) {
                position.apply_close_estimate(&close, now);
            }
            if close.should_close {
                if let Some(position) = self.shadow_positions.remove(&symbol) {
                    let fill = position.closed_fill(close, self.tick_index, now);
                    self.shadow_store
                        .record_closed_fill(&fill, self.tick_index, now);
                    closed.push(fill);
                    self.last_shadow_fill_by_symbol.insert(symbol, now);
                }
            }
        }

        if !closed.is_empty() {
            self.simulated_fills.extend(closed);
            trim_front(&mut self.simulated_fills, DEFAULT_HISTORY_LIMIT);
            self.persist_shadow_state(now);
        }

        self.open_shadow_positions(all_opportunities, now);
        self.refresh_shadow_bundle_views();
    }

    fn open_shadow_positions(&mut self, all_opportunities: &[Opportunity], now: DateTime<Utc>) {
        if !self.control.new_entries_allowed {
            return;
        }

        let mut opened = 0usize;
        let mut opened_any = false;
        for opportunity in best_open_opportunities_by_symbol(all_opportunities) {
            if opened >= DEFAULT_FILLS_PER_TICK {
                break;
            }
            if self
                .shadow_positions
                .contains_key(&opportunity.canonical_symbol)
            {
                continue;
            }
            if !self.shadow_fill_cooldown_elapsed(&opportunity.canonical_symbol, now) {
                continue;
            }
            if exchange_capacity_reject_reason(&self.config, &self.shadow_positions, opportunity)
                .is_some()
            {
                continue;
            }

            if let Some(position) =
                ShadowOpenPosition::from_opportunity(opportunity, self.tick_index, now)
            {
                self.shadow_store
                    .record_open_position(&position, self.tick_index, now);
                self.shadow_positions
                    .insert(opportunity.canonical_symbol.clone(), position);
                opened += 1;
                opened_any = true;
            }
        }
        if opened_any {
            self.persist_shadow_state(now);
        }
    }

    fn shadow_fill_cooldown_elapsed(&self, symbol: &CanonicalSymbol, now: DateTime<Utc>) -> bool {
        self.last_shadow_fill_by_symbol
            .get(symbol)
            .map(|last_fill| {
                now.signed_duration_since(*last_fill)
                    >= Duration::seconds(DEFAULT_SHADOW_FILL_COOLDOWN_SECS)
            })
            .unwrap_or(true)
    }

    fn refresh_shadow_bundle_views(&mut self) {
        self.open_bundles = self
            .shadow_positions
            .values()
            .map(ShadowOpenPosition::bundle_read_model)
            .collect();
        self.bundle_history = self
            .simulated_fills
            .iter()
            .rev()
            .take(DEFAULT_HISTORY_LIMIT)
            .map(SimulatedMarketFill::bundle_read_model)
            .collect();
        self.capital = exchange_capital_models(&self.config, &self.shadow_positions);
    }

    fn apply_public_ws_orderbook(&mut self, book: OrderBook5, now: DateTime<Utc>) {
        self.market_cache.upsert_orderbook_at(book, now);
    }

    fn apply_instrument_coverage(
        &mut self,
        coverage: ExchangeInstrumentCoverage,
        now: DateTime<Utc>,
    ) {
        for error in coverage.errors.clone() {
            self.market_errors.push(error);
        }
        push_coverage_errors(
            &mut self.market_errors,
            &self.config,
            &self.monitored_symbols,
            &coverage,
            now,
        );
        trim_front(&mut self.market_errors, DEFAULT_HISTORY_LIMIT);
        self.instrument_coverage = Some(coverage);
    }

    fn apply_public_ws_error(&mut self, error: cross_arb_server_ws::PublicWsMarketError) {
        self.market_errors.push(MarketDataError {
            exchange: error.exchange,
            canonical_symbol: error.canonical_symbol,
            kind: error.kind,
            message: error.message,
            latency_ms: error.latency_ms,
            occurred_at: error.occurred_at,
        });
        trim_front(&mut self.market_errors, DEFAULT_HISTORY_LIMIT);
    }

    fn apply_public_ws_cache_refresh(&mut self, refresh: PublicWsCacheRefresh, now: DateTime<Utc>) {
        let mut market = refresh.market;
        if !self.market_errors.is_empty() {
            let mut errors = self.market_errors.clone();
            errors.extend(market.errors);
            trim_front(&mut errors, DEFAULT_HISTORY_LIMIT);
            market.errors = errors;
        }
        for funding in refresh.funding_snapshots {
            self.market_cache.upsert_funding(funding);
        }
        self.apply_public_market_refresh(market, now);
    }
}

#[derive(Debug, Clone, Serialize)]
struct AnalyticsSummary {
    mode: RuntimeMode,
    monitored_symbols: usize,
    opportunities_visible: usize,
    openable_opportunities: usize,
    simulated_trades: usize,
    books_loaded: usize,
    funding_loaded: usize,
    market_error_count: usize,
    total_notional_usdt: f64,
    gross_spread_pnl_usdt: f64,
    fee_pnl_usdt: f64,
    funding_pnl_usdt: f64,
    slippage_cost_usdt: f64,
    net_pnl_usdt: f64,
    win_rate: f64,
    avg_net_edge_pct: f64,
    top_symbol: Option<CanonicalSymbol>,
    top_symbol_net_pnl_usdt: f64,
    reject_reason_kinds: usize,
    updated_at: DateTime<Utc>,
}

impl AnalyticsSummary {
    fn empty(now: DateTime<Utc>) -> Self {
        Self {
            mode: RuntimeMode::Simulation,
            monitored_symbols: 0,
            opportunities_visible: 0,
            openable_opportunities: 0,
            simulated_trades: 0,
            books_loaded: 0,
            funding_loaded: 0,
            market_error_count: 0,
            total_notional_usdt: 0.0,
            gross_spread_pnl_usdt: 0.0,
            fee_pnl_usdt: 0.0,
            funding_pnl_usdt: 0.0,
            slippage_cost_usdt: 0.0,
            net_pnl_usdt: 0.0,
            win_rate: 0.0,
            avg_net_edge_pct: 0.0,
            top_symbol: None,
            top_symbol_net_pnl_usdt: 0.0,
            reject_reason_kinds: 0,
            updated_at: now,
        }
    }

    fn from_state(
        symbols: &[CanonicalSymbol],
        opportunities: &[OpportunityReadModel],
        fills: &[SimulatedMarketFill],
        reject_breakdown: &[RejectReasonCount],
        books_loaded: usize,
        funding_loaded: usize,
        market_error_count: usize,
        now: DateTime<Utc>,
    ) -> Self {
        let total_notional_usdt = fills.iter().map(|fill| fill.executable_notional_usdt).sum();
        let gross_spread_pnl_usdt = fills.iter().map(|fill| fill.gross_spread_pnl_usdt).sum();
        let fee_pnl_usdt = fills.iter().map(|fill| fill.fee_pnl_usdt).sum();
        let funding_pnl_usdt = fills.iter().map(|fill| fill.funding_pnl_usdt).sum();
        let slippage_cost_usdt = fills.iter().map(|fill| fill.slippage_cost_usdt).sum();
        let net_pnl_usdt = fills.iter().map(|fill| fill.net_pnl_usdt).sum();
        let wins = fills.iter().filter(|fill| fill.net_pnl_usdt > 0.0).count();
        let avg_net_edge_pct = if opportunities.is_empty() {
            0.0
        } else {
            opportunities
                .iter()
                .map(|opportunity| opportunity.maker_taker_net_edge)
                .sum::<f64>()
                / opportunities.len() as f64
        };
        let (top_symbol, top_symbol_net_pnl_usdt) = top_symbol_pnl(fills);

        Self {
            mode: RuntimeMode::Simulation,
            monitored_symbols: symbols.len(),
            opportunities_visible: opportunities.len(),
            openable_opportunities: opportunities
                .iter()
                .filter(|opportunity| opportunity.can_open)
                .count(),
            simulated_trades: fills.len(),
            books_loaded,
            funding_loaded,
            market_error_count,
            total_notional_usdt,
            gross_spread_pnl_usdt,
            fee_pnl_usdt,
            funding_pnl_usdt,
            slippage_cost_usdt,
            net_pnl_usdt,
            win_rate: ratio(wins, fills.len()),
            avg_net_edge_pct,
            top_symbol,
            top_symbol_net_pnl_usdt,
            reject_reason_kinds: reject_breakdown.len(),
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct SymbolAnalytics {
    canonical_symbol: CanonicalSymbol,
    best_long_exchange: Option<ExchangeId>,
    best_short_exchange: Option<ExchangeId>,
    raw_spread_pct: f64,
    net_edge_pct: f64,
    target_notional_usdt: f64,
    executable_notional_usdt: f64,
    maker_quantity: Option<f64>,
    taker_quantity: Option<f64>,
    maker_notional_usdt: Option<f64>,
    taker_notional_usdt: Option<f64>,
    can_open: bool,
    reject_reasons: Vec<String>,
    updated_at: DateTime<Utc>,
}

impl SymbolAnalytics {
    fn from_best(
        symbol: CanonicalSymbol,
        opportunity: Option<&Opportunity>,
        now: DateTime<Utc>,
    ) -> Self {
        match opportunity {
            Some(opportunity) => Self {
                canonical_symbol: symbol,
                best_long_exchange: Some(opportunity.long_exchange.clone()),
                best_short_exchange: Some(opportunity.short_exchange.clone()),
                raw_spread_pct: opportunity.raw_open_spread,
                net_edge_pct: opportunity.maker_taker_net_edge,
                target_notional_usdt: opportunity.target_notional_usdt,
                executable_notional_usdt: opportunity.executable_notional_usdt,
                maker_quantity: opportunity.maker_quantity,
                taker_quantity: opportunity.taker_quantity,
                maker_notional_usdt: opportunity.maker_notional_usdt,
                taker_notional_usdt: opportunity.taker_notional_usdt,
                can_open: opportunity.can_open,
                reject_reasons: opportunity
                    .reject_reasons
                    .iter()
                    .map(|reason| format!("{:?}", reason))
                    .collect(),
                updated_at: now,
            },
            None => Self {
                canonical_symbol: symbol,
                best_long_exchange: None,
                best_short_exchange: None,
                raw_spread_pct: 0.0,
                net_edge_pct: 0.0,
                target_notional_usdt: 0.0,
                executable_notional_usdt: 0.0,
                maker_quantity: None,
                taker_quantity: None,
                maker_notional_usdt: None,
                taker_notional_usdt: None,
                can_open: false,
                reject_reasons: vec!["NoRoute".to_string()],
                updated_at: now,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SimulatedMarketFill {
    trade_id: String,
    bundle_id: String,
    opportunity_id: String,
    canonical_symbol: CanonicalSymbol,
    long_exchange: ExchangeId,
    short_exchange: ExchangeId,
    target_notional_usdt: f64,
    executable_notional_usdt: f64,
    maker_quantity: Option<f64>,
    taker_quantity: Option<f64>,
    maker_notional_usdt: Option<f64>,
    taker_notional_usdt: Option<f64>,
    long_fill_price: Option<f64>,
    short_fill_price: Option<f64>,
    long_close_price: Option<f64>,
    short_close_price: Option<f64>,
    entry_spread_pct: f64,
    exit_spread_pct: f64,
    holding_ms: i64,
    gross_spread_pnl_usdt: f64,
    fee_pnl_usdt: f64,
    funding_pnl_usdt: f64,
    #[serde(default)]
    book_slippage_cost_usdt: f64,
    #[serde(default)]
    safety_buffer_cost_usdt: f64,
    slippage_cost_usdt: f64,
    net_pnl_usdt: f64,
    net_edge_pct: f64,
    fill_model: String,
    opened_at: DateTime<Utc>,
    closed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShadowOpenPosition {
    bundle_id: String,
    opportunity_id: String,
    canonical_symbol: CanonicalSymbol,
    long_exchange: ExchangeId,
    short_exchange: ExchangeId,
    maker_exchange: ExchangeId,
    taker_exchange: ExchangeId,
    maker_leg_kind: MakerLegKind,
    target_notional_usdt: f64,
    executable_notional_usdt: f64,
    maker_quantity: Option<f64>,
    taker_quantity: Option<f64>,
    maker_notional_usdt: Option<f64>,
    taker_notional_usdt: Option<f64>,
    long_entry_price: f64,
    short_entry_price: f64,
    entry_spread_pct: f64,
    open_fee_usdt: f64,
    entry_expected_funding_usdt: f64,
    opened_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    last_close_spread_pct: Option<f64>,
    estimated_close_fee_usdt: f64,
    estimated_funding_pnl_usdt: f64,
    #[serde(default)]
    estimated_book_slippage_cost_usdt: f64,
    #[serde(default)]
    estimated_safety_buffer_cost_usdt: f64,
    estimated_slippage_cost_usdt: f64,
    estimated_net_pnl_usdt: f64,
}

#[derive(Debug, Clone)]
struct ShadowCloseEstimate {
    long_close_price: f64,
    short_close_price: f64,
    exit_spread_pct: f64,
    gross_spread_pnl_usdt: f64,
    close_fee_usdt: f64,
    funding_pnl_usdt: f64,
    book_slippage_cost_usdt: f64,
    safety_buffer_cost_usdt: f64,
    slippage_cost_usdt: f64,
    net_pnl_usdt: f64,
    should_close: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ShadowPersistenceReadModel {
    enabled: bool,
    dir: String,
    current_positions_path: String,
    simulated_fills_path: String,
    events_path: String,
    current_positions: usize,
    simulated_fills: usize,
    events_recorded: u64,
    last_saved_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShadowPersistedState<T> {
    saved_at: DateTime<Utc>,
    items: Vec<T>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ShadowPersistenceEventKind {
    OpenPosition,
    CloseFill,
    Reset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShadowPersistenceEvent {
    event_id: u64,
    event_kind: ShadowPersistenceEventKind,
    tick_index: u64,
    recorded_at: DateTime<Utc>,
    canonical_symbol: Option<CanonicalSymbol>,
    bundle_id: Option<String>,
    trade_id: Option<String>,
    payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
struct ShadowPersistenceStore {
    enabled: bool,
    dir: PathBuf,
    current_positions_path: PathBuf,
    simulated_fills_path: PathBuf,
    events_path: PathBuf,
    current_positions: Vec<ShadowOpenPosition>,
    simulated_fills: Vec<SimulatedMarketFill>,
    events_recorded: u64,
    last_saved_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
}

impl ShadowPersistenceStore {
    fn load(dir: PathBuf, monitored_symbols: &[CanonicalSymbol]) -> Self {
        let current_positions_path = dir.join("current_positions.json");
        let simulated_fills_path = dir.join("simulated_fills.json");
        let events_path = dir.join("events.jsonl");
        let mut store = Self {
            enabled: true,
            dir,
            current_positions_path,
            simulated_fills_path,
            events_path,
            current_positions: Vec::new(),
            simulated_fills: Vec::new(),
            events_recorded: 0,
            last_saved_at: None,
            last_error: None,
        };

        if let Err(err) = fs::create_dir_all(&store.dir) {
            store.enabled = false;
            store.last_error = Some(format!("create store dir failed: {err}"));
            return store;
        }

        store.events_recorded = existing_jsonl_lines(&store.events_path).unwrap_or_else(|err| {
            store.last_error = Some(format!("count events failed: {err}"));
            0
        });

        match read_persisted_state::<ShadowOpenPosition>(&store.current_positions_path) {
            Ok(Some(state)) => {
                store.last_saved_at = Some(state.saved_at);
                store.current_positions = state
                    .items
                    .into_iter()
                    .filter(|position| monitored_symbols.contains(&position.canonical_symbol))
                    .collect();
            }
            Ok(None) => {}
            Err(err) => store.last_error = Some(format!("load current positions failed: {err}")),
        }
        match read_persisted_state::<SimulatedMarketFill>(&store.simulated_fills_path) {
            Ok(Some(state)) => {
                store.last_saved_at = store
                    .last_saved_at
                    .map(|existing| existing.max(state.saved_at))
                    .or(Some(state.saved_at));
                store.simulated_fills = state.items;
                trim_front(&mut store.simulated_fills, DEFAULT_HISTORY_LIMIT);
            }
            Ok(None) => {}
            Err(err) => store.last_error = Some(format!("load simulated fills failed: {err}")),
        }

        store
    }

    fn read_model(&self) -> ShadowPersistenceReadModel {
        ShadowPersistenceReadModel {
            enabled: self.enabled,
            dir: self.dir.display().to_string(),
            current_positions_path: self.current_positions_path.display().to_string(),
            simulated_fills_path: self.simulated_fills_path.display().to_string(),
            events_path: self.events_path.display().to_string(),
            current_positions: self.current_positions.len(),
            simulated_fills: self.simulated_fills.len(),
            events_recorded: self.events_recorded,
            last_saved_at: self.last_saved_at,
            last_error: self.last_error.clone(),
        }
    }

    fn save(&mut self, now: DateTime<Utc>) {
        if !self.enabled {
            return;
        }
        if let Err(err) = fs::create_dir_all(&self.dir) {
            self.last_error = Some(format!("create store dir failed: {err}"));
            return;
        }
        if let Err(err) =
            write_persisted_state(&self.current_positions_path, &self.current_positions, now)
        {
            self.last_error = Some(format!("save current positions failed: {err}"));
            return;
        }
        if let Err(err) =
            write_persisted_state(&self.simulated_fills_path, &self.simulated_fills, now)
        {
            self.last_error = Some(format!("save simulated fills failed: {err}"));
            return;
        }
        self.last_saved_at = Some(now);
        self.last_error = None;
    }

    fn record_open_position(
        &mut self,
        position: &ShadowOpenPosition,
        tick_index: u64,
        now: DateTime<Utc>,
    ) {
        self.record_event_with_payload(
            ShadowPersistenceEventKind::OpenPosition,
            tick_index,
            now,
            Some(position.canonical_symbol.clone()),
            Some(position.bundle_id.clone()),
            None,
            serde_json::to_value(position).unwrap_or_else(|_| serde_json::Value::Null),
        );
    }

    fn record_closed_fill(
        &mut self,
        fill: &SimulatedMarketFill,
        tick_index: u64,
        now: DateTime<Utc>,
    ) {
        self.record_event_with_payload(
            ShadowPersistenceEventKind::CloseFill,
            tick_index,
            now,
            Some(fill.canonical_symbol.clone()),
            Some(fill.bundle_id.clone()),
            Some(fill.trade_id.clone()),
            serde_json::to_value(fill).unwrap_or_else(|_| serde_json::Value::Null),
        );
    }

    fn record_event(&mut self, event_kind: ShadowPersistenceEventKind, now: DateTime<Utc>) {
        self.record_event_with_payload(
            event_kind,
            0,
            now,
            None,
            None,
            None,
            serde_json::Value::Null,
        );
    }

    fn record_event_with_payload(
        &mut self,
        event_kind: ShadowPersistenceEventKind,
        tick_index: u64,
        now: DateTime<Utc>,
        canonical_symbol: Option<CanonicalSymbol>,
        bundle_id: Option<String>,
        trade_id: Option<String>,
        payload: serde_json::Value,
    ) {
        if !self.enabled {
            return;
        }
        if let Err(err) = fs::create_dir_all(&self.dir) {
            self.last_error = Some(format!("create store dir failed: {err}"));
            return;
        }

        let event = ShadowPersistenceEvent {
            event_id: self.events_recorded.saturating_add(1),
            event_kind,
            tick_index,
            recorded_at: now,
            canonical_symbol,
            bundle_id,
            trade_id,
            payload,
        };
        match append_jsonl(&self.events_path, &event) {
            Ok(()) => {
                self.events_recorded = event.event_id;
                self.last_error = None;
            }
            Err(err) => self.last_error = Some(format!("append event failed: {err}")),
        }
    }
}

fn read_persisted_state<T>(path: &Path) -> std::io::Result<Option<ShadowPersistedState<T>>>
where
    T: for<'de> Deserialize<'de>,
{
    match File::open(path) {
        Ok(file) => serde_json::from_reader(file)
            .map(Some)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn write_persisted_state<T>(
    path: &Path,
    items: &[T],
    saved_at: DateTime<Utc>,
) -> std::io::Result<()>
where
    T: Clone + Serialize,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("json.tmp");
    let state = ShadowPersistedState {
        saved_at,
        items: items.to_vec(),
    };
    {
        let file = File::create(&tmp_path)?;
        serde_json::to_writer_pretty(file, &state)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    }
    fs::rename(tmp_path, path)
}

fn append_jsonl<T: Serialize>(path: &Path, item: &T) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, item)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    file.write_all(b"\n")
}

fn existing_jsonl_lines(path: &Path) -> std::io::Result<u64> {
    match fs::read_to_string(path) {
        Ok(raw) => Ok(raw.lines().count() as u64),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err),
    }
}

#[derive(Debug, Clone, Serialize)]
struct ShadowPositionReadModel {
    canonical_symbol: CanonicalSymbol,
    long_exchange: ExchangeId,
    short_exchange: ExchangeId,
    notional_usdt: f64,
    entry_edge_pct: f64,
    current_spread_pct: Option<f64>,
    expected_fee_usdt: f64,
    expected_funding_usdt: f64,
    book_slippage_cost_usdt: f64,
    safety_buffer_cost_usdt: f64,
    estimated_net_pnl_usdt: f64,
    holding_secs: i64,
    status: String,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct ExchangeAnalysisReadModel {
    exchange: ExchangeId,
    long_count: usize,
    short_count: usize,
    open_count_7d: usize,
    win_rate_7d: f64,
    simulated_notional_usdt: f64,
    net_pnl_usdt: f64,
    avg_edge_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
struct FeeAnalysisReadModel {
    exchange: ExchangeId,
    estimated_fee_usdt: f64,
    simulated_notional_usdt: f64,
    fee_bps: f64,
}

#[derive(Debug, Clone, Serialize)]
struct FundingAnalysisReadModel {
    exchange: ExchangeId,
    funding_pnl_usdt: f64,
    long_count: usize,
    short_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct ExchangeCapitalReadModel {
    exchange: ExchangeId,
    equity_usdt: f64,
    leverage: f64,
    max_notional_usdt: f64,
    used_notional_usdt: f64,
    remaining_notional_usdt: f64,
    open_positions: usize,
    max_positions: usize,
    remaining_positions: usize,
    usage_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
struct FeeScheduleReadModel {
    exchange: ExchangeId,
    maker_fee_rate: f64,
    taker_fee_rate: f64,
    source: String,
}

#[derive(Debug, Clone, Serialize)]
struct BookHealthSummaryReadModel {
    exchange: ExchangeId,
    total_books: usize,
    usable_books: usize,
    stale_books: usize,
    crossed_books: usize,
    invalid_level_books: usize,
    sequence_gap_books: usize,
    max_age_ms: i64,
}

impl SimulatedMarketFill {
    fn bundle_read_model(&self) -> BundleReadModel {
        BundleReadModel::from(&SimulatedBundleState {
            bundle_id: self.bundle_id.clone(),
            opportunity_id: self.opportunity_id.clone(),
            status: SimulatedBundleStatus::Closed,
            route: rustcta::strategies::cross_exchange_arbitrage::StrategyRoute {
                long_exchange: self.long_exchange.clone(),
                short_exchange: self.short_exchange.clone(),
                maker_exchange: self.short_exchange.clone(),
                taker_exchange: self.long_exchange.clone(),
                maker_side: rustcta::strategies::cross_exchange_arbitrage::OrderSide::Sell,
                taker_side: rustcta::strategies::cross_exchange_arbitrage::OrderSide::Buy,
                maker_leg_kind:
                    rustcta::strategies::cross_exchange_arbitrage::MakerLegKind::ShortMakerSell,
            },
            target_notional_usdt: self.executable_notional_usdt,
            opened_at: Some(self.opened_at),
            updated_at: self.closed_at,
        })
    }
}

impl ShadowOpenPosition {
    fn from_opportunity(
        opportunity: &Opportunity,
        tick_index: u64,
        now: DateTime<Utc>,
    ) -> Option<Self> {
        let long_entry_price = match opportunity.maker_leg_kind {
            MakerLegKind::LongMakerBuy => opportunity.maker_price,
            MakerLegKind::ShortMakerSell => opportunity.taker_vwap?,
        };
        let short_entry_price = match opportunity.maker_leg_kind {
            MakerLegKind::LongMakerBuy => opportunity.taker_vwap?,
            MakerLegKind::ShortMakerSell => opportunity.maker_price,
        };
        if long_entry_price <= 0.0 || short_entry_price <= 0.0 {
            return None;
        }

        Some(Self {
            bundle_id: format!("bundle-open-{}-{}", tick_index, opportunity.opportunity_id),
            opportunity_id: opportunity.opportunity_id.clone(),
            canonical_symbol: opportunity.canonical_symbol.clone(),
            long_exchange: opportunity.long_exchange.clone(),
            short_exchange: opportunity.short_exchange.clone(),
            maker_exchange: opportunity.maker_exchange.clone(),
            taker_exchange: opportunity.taker_exchange.clone(),
            maker_leg_kind: opportunity.maker_leg_kind,
            target_notional_usdt: opportunity.target_notional_usdt,
            executable_notional_usdt: opportunity.executable_notional_usdt,
            maker_quantity: opportunity.maker_quantity,
            taker_quantity: opportunity.taker_quantity,
            maker_notional_usdt: opportunity.maker_notional_usdt,
            taker_notional_usdt: opportunity.taker_notional_usdt,
            long_entry_price,
            short_entry_price,
            entry_spread_pct: opportunity.raw_open_spread,
            open_fee_usdt: opportunity.open_fee_est_usdt,
            entry_expected_funding_usdt: opportunity.expected_funding_usdt,
            opened_at: now,
            updated_at: now,
            last_close_spread_pct: None,
            estimated_close_fee_usdt: opportunity.close_fee_est_usdt,
            estimated_funding_pnl_usdt: 0.0,
            estimated_book_slippage_cost_usdt: 0.0,
            estimated_safety_buffer_cost_usdt: 0.0,
            estimated_slippage_cost_usdt: 0.0,
            estimated_net_pnl_usdt: -opportunity.open_fee_est_usdt,
        })
    }

    fn apply_close_estimate(&mut self, close: &ShadowCloseEstimate, now: DateTime<Utc>) {
        self.last_close_spread_pct = Some(close.exit_spread_pct);
        self.estimated_close_fee_usdt = close.close_fee_usdt;
        self.estimated_funding_pnl_usdt = close.funding_pnl_usdt;
        self.estimated_book_slippage_cost_usdt = close.book_slippage_cost_usdt;
        self.estimated_safety_buffer_cost_usdt = close.safety_buffer_cost_usdt;
        self.estimated_slippage_cost_usdt = close.slippage_cost_usdt;
        self.estimated_net_pnl_usdt = close.net_pnl_usdt;
        self.updated_at = now;
    }

    fn closed_fill(
        self,
        close: ShadowCloseEstimate,
        tick_index: u64,
        now: DateTime<Utc>,
    ) -> SimulatedMarketFill {
        let fee_pnl_usdt = -(self.open_fee_usdt + close.close_fee_usdt);
        let holding_ms = now.signed_duration_since(self.opened_at).num_milliseconds();
        SimulatedMarketFill {
            trade_id: format!("sim-close-{}-{}", tick_index, self.opportunity_id),
            bundle_id: self.bundle_id,
            opportunity_id: self.opportunity_id,
            canonical_symbol: self.canonical_symbol,
            long_exchange: self.long_exchange,
            short_exchange: self.short_exchange,
            target_notional_usdt: self.target_notional_usdt,
            executable_notional_usdt: self.executable_notional_usdt,
            maker_quantity: self.maker_quantity,
            taker_quantity: self.taker_quantity,
            maker_notional_usdt: self.maker_notional_usdt,
            taker_notional_usdt: self.taker_notional_usdt,
            long_fill_price: Some(self.long_entry_price),
            short_fill_price: Some(self.short_entry_price),
            long_close_price: Some(close.long_close_price),
            short_close_price: Some(close.short_close_price),
            entry_spread_pct: self.entry_spread_pct,
            exit_spread_pct: close.exit_spread_pct,
            holding_ms,
            gross_spread_pnl_usdt: close.gross_spread_pnl_usdt,
            fee_pnl_usdt,
            funding_pnl_usdt: close.funding_pnl_usdt,
            book_slippage_cost_usdt: close.book_slippage_cost_usdt,
            safety_buffer_cost_usdt: close.safety_buffer_cost_usdt,
            slippage_cost_usdt: close.slippage_cost_usdt,
            net_pnl_usdt: close.net_pnl_usdt,
            net_edge_pct: close.net_pnl_usdt / self.executable_notional_usdt.max(1.0),
            fill_model: "spread_convergence_shadow".to_string(),
            opened_at: self.opened_at,
            closed_at: now,
        }
    }

    fn bundle_read_model(&self) -> BundleReadModel {
        BundleReadModel::from(&SimulatedBundleState {
            bundle_id: self.bundle_id.clone(),
            opportunity_id: self.opportunity_id.clone(),
            status: SimulatedBundleStatus::OpenSimulated,
            route: rustcta::strategies::cross_exchange_arbitrage::StrategyRoute {
                long_exchange: self.long_exchange.clone(),
                short_exchange: self.short_exchange.clone(),
                maker_exchange: self.maker_exchange.clone(),
                taker_exchange: self.taker_exchange.clone(),
                maker_side: match self.maker_leg_kind {
                    MakerLegKind::LongMakerBuy => OrderSide::Buy,
                    MakerLegKind::ShortMakerSell => OrderSide::Sell,
                },
                taker_side: match self.maker_leg_kind {
                    MakerLegKind::LongMakerBuy => OrderSide::Sell,
                    MakerLegKind::ShortMakerSell => OrderSide::Buy,
                },
                maker_leg_kind: self.maker_leg_kind,
            },
            target_notional_usdt: self.executable_notional_usdt,
            opened_at: Some(self.opened_at),
            updated_at: self.updated_at,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
struct RejectReasonCount {
    reason: String,
    count: u64,
}

#[derive(Debug, Clone, Serialize)]
struct MarketDataError {
    exchange: ExchangeId,
    canonical_symbol: Option<CanonicalSymbol>,
    kind: String,
    message: String,
    latency_ms: u128,
    occurred_at: DateTime<Utc>,
}

#[derive(Debug)]
struct PublicMarketRefresh {
    snapshots_by_symbol: HashMap<CanonicalSymbol, Vec<MarketSnapshot>>,
    books_loaded: usize,
    funding_loaded: usize,
    errors: Vec<MarketDataError>,
    exchanges: Vec<ExchangeId>,
}

#[derive(Debug)]
struct PublicWsCacheRefresh {
    market: PublicMarketRefresh,
    funding_snapshots: Vec<MarketFundingSnapshot>,
}

#[derive(Debug, Clone)]
struct ExchangeInstrumentCoverage {
    supported_by_exchange: HashMap<ExchangeId, HashSet<CanonicalSymbol>>,
    instruments_by_exchange_symbol: HashMap<(ExchangeId, CanonicalSymbol), InstrumentMeta>,
    coverage_by_symbol: HashMap<CanonicalSymbol, HashSet<ExchangeId>>,
    errors: Vec<MarketDataError>,
}

#[derive(Debug, Clone, Serialize)]
struct ToolCommandReadModel {
    name: String,
    command: String,
    scope: String,
}

#[derive(Debug, Clone, Serialize)]
struct DashboardPayload {
    generated_at: DateTime<Utc>,
    status: CrossArbDashboardStatus,
    analytics: AnalyticsSummary,
    data_source: DataSource,
    control: ControlState,
    symbols: Vec<SymbolAnalytics>,
    opportunities: Vec<OpportunityReadModel>,
    current_positions: Vec<ShadowPositionReadModel>,
    simulated_fills: Vec<SimulatedMarketFill>,
    capital: Vec<ExchangeCapitalReadModel>,
    fee_schedule: Vec<FeeScheduleReadModel>,
    exchange_analysis: Vec<ExchangeAnalysisReadModel>,
    fee_analysis: Vec<FeeAnalysisReadModel>,
    funding_analysis: Vec<FundingAnalysisReadModel>,
    exchanges: Vec<ExchangeReadModel>,
    routes: Vec<RouteReadModel>,
    book_quality: Vec<OrderBookQualityReadModel>,
    book_health: Vec<BookHealthSummaryReadModel>,
    reject_breakdown: Vec<RejectReasonCount>,
    market_errors: Vec<MarketDataError>,
    config_summary: ConfigSummaryReadModel,
    tool_commands: Vec<ToolCommandReadModel>,
}

#[derive(Debug, Clone, Serialize)]
struct StatusResponse {
    generated_at: DateTime<Utc>,
    status: CrossArbDashboardStatus,
    analytics: AnalyticsSummary,
    control: ControlState,
}

#[derive(Debug, Clone, Serialize)]
struct ListResponse<T> {
    generated_at: DateTime<Utc>,
    items: Vec<T>,
}

#[derive(Debug, Clone, Serialize)]
struct ExchangeReadModel {
    exchange: String,
    status: String,
    mode: RuntimeMode,
    enabled_symbols: usize,
    route_count: usize,
    last_message_age_ms: Option<i64>,
    reconnect_count: u64,
    sequence_gap_count: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ConfigSummaryReadModel {
    config_path: String,
    effective_mode: RuntimeMode,
    configured_mode: Option<RuntimeMode>,
    loaded_from_file: bool,
    enabled_symbols: usize,
    enabled_exchanges: usize,
    parse_error: Option<String>,
    notes: Vec<String>,
}

impl ConfigSummaryReadModel {
    fn load_config(path: &Path) -> Result<(Self, CrossExchangeArbitrageConfig)> {
        let mut summary = Self {
            config_path: path.display().to_string(),
            effective_mode: RuntimeMode::Simulation,
            configured_mode: None,
            loaded_from_file: false,
            enabled_symbols: 0,
            enabled_exchanges: 0,
            parse_error: None,
            notes: vec![
                "影子交易分析模式：只使用公开行情和本地成交推演，不调用交易所私有下单接口。"
                    .to_string(),
                "成交模型为 market_all_filled：按盘口市价吃单假设全部成交。".to_string(),
            ],
        };

        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let config: CrossExchangeArbitrageConfig = serde_yaml::from_str(&raw)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
        config
            .validate()
            .with_context(|| format!("invalid config {}", path.display()))?;

        summary.loaded_from_file = true;
        summary.configured_mode = Some(config.mode);
        summary.enabled_symbols = config.universe.symbols.len();
        summary.enabled_exchanges = config.universe.enabled_exchanges.len();
        summary
            .notes
            .push("配置模式仅展示；看板服务强制使用 Simulation 防止实盘下单。".to_string());

        Ok((summary, config))
    }
}

#[derive(Debug, Clone, Serialize)]
struct ControlState {
    mode: RuntimeMode,
    new_entries_paused: bool,
    close_only: bool,
    kill_switch: bool,
    new_entries_allowed: bool,
    live_orders_enabled: bool,
    updated_at: DateTime<Utc>,
}

impl Default for ControlState {
    fn default() -> Self {
        let mut state = Self {
            mode: RuntimeMode::Simulation,
            new_entries_paused: false,
            close_only: false,
            kill_switch: false,
            new_entries_allowed: true,
            live_orders_enabled: false,
            updated_at: Utc::now(),
        };
        state.recompute();
        state
    }
}

impl ControlState {
    fn pause_new_entries(&mut self) {
        self.new_entries_paused = true;
        self.recompute();
    }

    fn resume_new_entries(&mut self) {
        self.new_entries_paused = false;
        self.close_only = false;
        self.kill_switch = false;
        self.recompute();
    }

    fn enable_close_only(&mut self) {
        self.close_only = true;
        self.new_entries_paused = true;
        self.recompute();
    }

    fn enable_kill_switch(&mut self) {
        self.kill_switch = true;
        self.close_only = true;
        self.new_entries_paused = true;
        self.recompute();
    }

    fn recompute(&mut self) {
        self.mode = RuntimeMode::Simulation;
        self.live_orders_enabled = false;
        self.new_entries_allowed =
            !self.new_entries_paused && !self.close_only && !self.kill_switch;
        self.updated_at = Utc::now();
    }
}

async fn index() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn healthz() -> &'static str {
    "ok"
}

async fn dashboard(State(state): State<AppState>) -> Json<DashboardPayload> {
    let read_model = state.read_model.read().await;
    Json(DashboardPayload {
        generated_at: Utc::now(),
        status: read_model.status.clone(),
        analytics: read_model.analytics.clone(),
        data_source: read_model.data_source,
        control: read_model.control.clone(),
        symbols: read_model.symbols.clone(),
        opportunities: read_model.opportunities.clone(),
        current_positions: current_positions_from_shadow_positions(&read_model.shadow_positions),
        simulated_fills: read_model
            .simulated_fills
            .iter()
            .rev()
            .take(200)
            .cloned()
            .collect(),
        capital: exchange_capital_models(&read_model.config, &read_model.shadow_positions),
        fee_schedule: fee_schedule_models(&read_model.config),
        exchange_analysis: exchange_analysis_from_fills(&read_model.simulated_fills),
        fee_analysis: fee_analysis_from_fills(&read_model.simulated_fills),
        funding_analysis: funding_analysis_from_fills(&read_model.simulated_fills),
        exchanges: read_model.exchanges.clone(),
        routes: read_model.routes.clone(),
        book_quality: read_model.book_quality.clone(),
        book_health: book_health_summary(&read_model.book_quality),
        reject_breakdown: read_model.reject_breakdown.clone(),
        market_errors: read_model.market_errors.clone(),
        config_summary: read_model.config_summary.clone(),
        tool_commands: read_model.tool_commands.clone(),
    })
}

async fn status(State(state): State<AppState>) -> Json<StatusResponse> {
    let read_model = state.read_model.read().await;
    Json(StatusResponse {
        generated_at: Utc::now(),
        status: read_model.status.clone(),
        analytics: read_model.analytics.clone(),
        control: read_model.control.clone(),
    })
}

async fn analytics(State(state): State<AppState>) -> Json<AnalyticsSummary> {
    let read_model = state.read_model.read().await;
    Json(read_model.analytics.clone())
}

async fn symbols(State(state): State<AppState>) -> Json<ListResponse<SymbolAnalytics>> {
    let read_model = state.read_model.read().await;
    Json(ListResponse {
        generated_at: Utc::now(),
        items: read_model.symbols.clone(),
    })
}

async fn simulated_fills(State(state): State<AppState>) -> Json<ListResponse<SimulatedMarketFill>> {
    let read_model = state.read_model.read().await;
    Json(ListResponse {
        generated_at: Utc::now(),
        items: read_model
            .simulated_fills
            .iter()
            .rev()
            .take(500)
            .cloned()
            .collect(),
    })
}

async fn opportunities(State(state): State<AppState>) -> Json<ListResponse<OpportunityReadModel>> {
    let read_model = state.read_model.read().await;
    Json(ListResponse {
        generated_at: Utc::now(),
        items: read_model.opportunities.clone(),
    })
}

async fn open_bundles(State(state): State<AppState>) -> Json<ListResponse<BundleReadModel>> {
    let read_model = state.read_model.read().await;
    Json(ListResponse {
        generated_at: Utc::now(),
        items: read_model.open_bundles.clone(),
    })
}

async fn bundle_history(State(state): State<AppState>) -> Json<ListResponse<BundleReadModel>> {
    let read_model = state.read_model.read().await;
    Json(ListResponse {
        generated_at: Utc::now(),
        items: read_model.bundle_history.clone(),
    })
}

async fn exchanges(State(state): State<AppState>) -> Json<ListResponse<ExchangeReadModel>> {
    let read_model = state.read_model.read().await;
    Json(ListResponse {
        generated_at: Utc::now(),
        items: read_model.exchanges.clone(),
    })
}

async fn routes(State(state): State<AppState>) -> Json<ListResponse<RouteReadModel>> {
    let read_model = state.read_model.read().await;
    Json(ListResponse {
        generated_at: Utc::now(),
        items: read_model.routes.clone(),
    })
}

async fn book_quality(
    State(state): State<AppState>,
) -> Json<ListResponse<OrderBookQualityReadModel>> {
    let read_model = state.read_model.read().await;
    Json(ListResponse {
        generated_at: Utc::now(),
        items: read_model.book_quality.clone(),
    })
}

async fn reconcile(State(state): State<AppState>) -> Json<ListResponse<ReconcileReadModel>> {
    let read_model = state.read_model.read().await;
    Json(ListResponse {
        generated_at: Utc::now(),
        items: read_model.reconcile_reports.clone(),
    })
}

async fn risk_events(State(state): State<AppState>) -> Json<ListResponse<RiskEventReadModel>> {
    let read_model = state.read_model.read().await;
    Json(ListResponse {
        generated_at: Utc::now(),
        items: read_model.risk_events.clone(),
    })
}

async fn config_summary(State(state): State<AppState>) -> Json<ConfigSummaryReadModel> {
    let read_model = state.read_model.read().await;
    Json(read_model.config_summary.clone())
}

async fn tools(State(state): State<AppState>) -> Json<ListResponse<ToolCommandReadModel>> {
    let read_model = state.read_model.read().await;
    Json(ListResponse {
        generated_at: Utc::now(),
        items: read_model.tool_commands.clone(),
    })
}

async fn shadow_persistence(State(state): State<AppState>) -> Json<ShadowPersistenceReadModel> {
    let read_model = state.read_model.read().await;
    Json(read_model.shadow_store.read_model())
}

async fn pause_new_entries(State(state): State<AppState>) -> Json<ControlState> {
    mutate_control(state, ControlState::pause_new_entries).await
}

async fn resume_new_entries(State(state): State<AppState>) -> Json<ControlState> {
    mutate_control(state, ControlState::resume_new_entries).await
}

async fn close_only(State(state): State<AppState>) -> Json<ControlState> {
    mutate_control(state, ControlState::enable_close_only).await
}

async fn kill_switch(State(state): State<AppState>) -> Json<ControlState> {
    mutate_control(state, ControlState::enable_kill_switch).await
}

async fn reset_simulation(State(state): State<AppState>) -> Json<StatusResponse> {
    let mut read_model = state.read_model.write().await;
    read_model.reset_simulation(Utc::now());
    Json(StatusResponse {
        generated_at: Utc::now(),
        status: read_model.status.clone(),
        analytics: read_model.analytics.clone(),
        control: read_model.control.clone(),
    })
}

async fn mutate_control(
    state: AppState,
    update: impl FnOnce(&mut ControlState),
) -> Json<ControlState> {
    let mut read_model = state.read_model.write().await;
    update(&mut read_model.control);
    read_model.touch();
    Json(read_model.control.clone())
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/healthz", get(healthz))
        .route("/api/cross-arb/dashboard", get(dashboard))
        .route("/api/cross-arb/status", get(status))
        .route("/api/cross-arb/analytics", get(analytics))
        .route("/api/cross-arb/symbols", get(symbols))
        .route("/api/cross-arb/simulated-fills", get(simulated_fills))
        .route("/api/cross-arb/opportunities", get(opportunities))
        .route("/api/cross-arb/bundles/open", get(open_bundles))
        .route("/api/cross-arb/bundles/history", get(bundle_history))
        .route("/api/cross-arb/exchanges", get(exchanges))
        .route("/api/cross-arb/routes", get(routes))
        .route("/api/cross-arb/book-quality", get(book_quality))
        .route("/api/cross-arb/reconcile", get(reconcile))
        .route("/api/cross-arb/risk-events", get(risk_events))
        .route("/api/cross-arb/config-summary", get(config_summary))
        .route("/api/cross-arb/tools", get(tools))
        .route("/api/cross-arb/shadow-persistence", get(shadow_persistence))
        .route(
            "/api/cross-arb/control/pause-new-entries",
            post(pause_new_entries),
        )
        .route(
            "/api/cross-arb/control/resume-new-entries",
            post(resume_new_entries),
        )
        .route("/api/cross-arb/control/close-only", post(close_only))
        .route("/api/cross-arb/control/kill-switch", post(kill_switch))
        .route(
            "/api/cross-arb/control/reset-simulation",
            post(reset_simulation),
        )
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .with_context(|| "failed to parse cross_arb_server host/port")?;

    let state = AppState::new(
        &args.config,
        args.symbol_count,
        args.data_source,
        args.shadow_store_dir.clone(),
    )?;
    if args.simulate {
        spawn_shadow_loop(
            state.clone(),
            args.refresh_secs,
            args.request_timeout_ms,
            args.data_source,
        );
    }
    let app = build_router(state);

    log::info!(
        "cross_arb_server dashboard starting in {:?} shadow mode: http://{}",
        args.data_source,
        addr
    );
    log::info!("config summary path: {}", args.config.display());
    log::info!(
        "shadow persistence dir: {}",
        args.shadow_store_dir.display()
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

fn spawn_shadow_loop(
    state: AppState,
    refresh_secs: u64,
    request_timeout_ms: u64,
    data_source: DataSource,
) {
    if data_source == DataSource::PublicWs {
        spawn_public_ws_ingestion(state.clone(), request_timeout_ms);
    }

    tokio::spawn(async move {
        let refresh_secs = refresh_secs.max(1);
        let funding_refresh_ticks = (60 / refresh_secs).max(1);
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(refresh_secs));
        loop {
            interval.tick().await;
            match data_source {
                DataSource::Synthetic => {
                    let mut read_model = state.read_model.write().await;
                    read_model.simulate_synthetic_tick(Utc::now());
                }
                DataSource::PublicRest => {
                    let (config, symbols) = {
                        let read_model = state.read_model.read().await;
                        (
                            read_model.config.clone(),
                            read_model.monitored_symbols.clone(),
                        )
                    };
                    let refresh = collect_public_market(config, symbols, request_timeout_ms).await;
                    let mut read_model = state.read_model.write().await;
                    read_model.apply_public_market_refresh(refresh, Utc::now());
                }
                DataSource::PublicWs => {
                    let (config, symbols, cache, coverage, should_refresh_funding) = {
                        let read_model = state.read_model.read().await;
                        (
                            read_model.config.clone(),
                            read_model.monitored_symbols.clone(),
                            read_model.market_cache.clone(),
                            read_model.instrument_coverage.clone(),
                            read_model.tick_index % funding_refresh_ticks == 0,
                        )
                    };
                    let refresh = collect_public_ws_cache_refresh(
                        config,
                        symbols,
                        cache,
                        coverage,
                        request_timeout_ms,
                        should_refresh_funding,
                    )
                    .await;
                    let mut read_model = state.read_model.write().await;
                    read_model.apply_public_ws_cache_refresh(refresh, Utc::now());
                }
            }
        }
    });
}

fn spawn_public_ws_ingestion(state: AppState, request_timeout_ms: u64) {
    tokio::spawn(async move {
        let (config, symbols) = {
            let read_model = state.read_model.read().await;
            (
                read_model.config.clone(),
                read_model.monitored_symbols.clone(),
            )
        };
        let (tx, mut rx) = mpsc::channel(10_000);
        let base_ws_config = cross_arb_server_ws::PublicWsConfig {
            connect_timeout_ms: request_timeout_ms.max(1_000),
            reconnect_delay_ms: 2_000,
            heartbeat_interval_ms: 20_000,
            max_symbols_per_connection: 120,
        };

        let coverage =
            load_exchange_instrument_coverage(&config, &symbols, request_timeout_ms).await;
        {
            let mut read_model = state.read_model.write().await;
            read_model.apply_instrument_coverage(coverage.clone(), Utc::now());
        }

        for adapter in configured_market_adapter_arcs(&config) {
            let exchange = adapter.exchange();
            let Some(supported_symbols) = coverage.supported_by_exchange.get(&exchange) else {
                continue;
            };
            let exchange_symbols = symbols
                .iter()
                .filter(|symbol| supported_symbols.contains(*symbol))
                .map(|symbol| exchange_symbol_for(&exchange, symbol))
                .collect::<Vec<_>>();
            if exchange_symbols.is_empty() {
                continue;
            }
            let ws_config = cross_arb_server_ws::PublicWsConfig {
                max_symbols_per_connection: config
                    .ws_batch_size_for(&exchange, base_ws_config.max_symbols_per_connection),
                ..base_ws_config
            };
            tokio::spawn(cross_arb_server_ws::run_public_ws_adapter(
                adapter,
                exchange_symbols,
                ws_config,
                tx.clone(),
            ));
        }
        drop(tx);

        while let Some(update) = rx.recv().await {
            let mut read_model = state.read_model.write().await;
            match update {
                cross_arb_server_ws::PublicWsUpdate::OrderBook(book) => {
                    read_model.apply_public_ws_orderbook(book, Utc::now());
                }
                cross_arb_server_ws::PublicWsUpdate::Error(error) => {
                    read_model.apply_public_ws_error(error);
                }
                cross_arb_server_ws::PublicWsUpdate::Connected {
                    exchange,
                    route,
                    symbol_count,
                    connected_at,
                } => {
                    log::info!(
                        "public ws connected: exchange={} symbols={} route={} at={}",
                        exchange,
                        symbol_count,
                        route,
                        connected_at
                    );
                }
            }
        }
    });
}

async fn collect_public_market(
    config: CrossExchangeArbitrageConfig,
    symbols: Vec<CanonicalSymbol>,
    request_timeout_ms: u64,
) -> PublicMarketRefresh {
    let now = Utc::now();
    let timeout_duration = TokioDuration::from_millis(request_timeout_ms.max(100));
    let mut snapshots_by_symbol: HashMap<CanonicalSymbol, Vec<MarketSnapshot>> = HashMap::new();
    let mut books_loaded = 0usize;
    let mut funding_loaded = 0usize;
    let mut errors = Vec::new();
    let adapters = configured_market_adapters(&config);
    let exchanges = adapters
        .iter()
        .map(|adapter| adapter.exchange())
        .collect::<Vec<_>>();
    let coverage =
        load_exchange_instrument_coverage_from_boxed(&adapters, &symbols, request_timeout_ms, now)
            .await;
    push_coverage_errors(&mut errors, &config, &symbols, &coverage, now);
    errors.extend(coverage.errors.clone());

    for adapter in adapters {
        let exchange = adapter.exchange();
        let symbols_for_exchange = symbols_for_exchange(&symbols, &coverage, &exchange);
        let funding_started = Instant::now();
        let funding_by_symbol = match timeout(
            timeout_duration,
            adapter.load_funding(&symbols_for_exchange),
        )
        .await
        {
            Ok(Ok(funding)) => {
                funding_loaded += funding.len();
                funding
                    .into_iter()
                    .map(|snapshot| (snapshot.canonical_symbol.clone(), snapshot))
                    .collect::<HashMap<_, _>>()
            }
            Ok(Err(err)) => {
                errors.push(MarketDataError {
                    exchange: exchange.clone(),
                    canonical_symbol: None,
                    kind: "funding".to_string(),
                    message: err.to_string(),
                    latency_ms: funding_started.elapsed().as_millis(),
                    occurred_at: now,
                });
                HashMap::new()
            }
            Err(_) => {
                errors.push(MarketDataError {
                    exchange: exchange.clone(),
                    canonical_symbol: None,
                    kind: "funding".to_string(),
                    message: format!("timed out after {} ms", request_timeout_ms),
                    latency_ms: funding_started.elapsed().as_millis(),
                    occurred_at: now,
                });
                HashMap::new()
            }
        };

        for symbol in &symbols_for_exchange {
            let exchange_symbol = exchange_symbol_for(&exchange, symbol);
            let started = Instant::now();
            match timeout(
                timeout_duration,
                adapter.fetch_orderbook_snapshot(&exchange_symbol, 5),
            )
            .await
            {
                Ok(Ok(book)) => {
                    books_loaded += 1;
                    let funding = funding_by_symbol.get(symbol);
                    snapshots_by_symbol
                        .entry(symbol.clone())
                        .or_default()
                        .push(MarketSnapshot {
                            book,
                            route_status: RouteStatus::Healthy,
                            funding_rate: funding
                                .map(|snapshot| snapshot.funding_rate)
                                .unwrap_or(0.0),
                            next_funding_time: funding
                                .and_then(|snapshot| snapshot.next_funding_time),
                            instrument: coverage
                                .instruments_by_exchange_symbol
                                .get(&(exchange.clone(), symbol.clone()))
                                .cloned(),
                        });
                }
                Ok(Err(err)) => errors.push(MarketDataError {
                    exchange: exchange.clone(),
                    canonical_symbol: Some(symbol.clone()),
                    kind: "orderbook".to_string(),
                    message: err.to_string(),
                    latency_ms: started.elapsed().as_millis(),
                    occurred_at: now,
                }),
                Err(_) => errors.push(MarketDataError {
                    exchange: exchange.clone(),
                    canonical_symbol: Some(symbol.clone()),
                    kind: "orderbook".to_string(),
                    message: format!("timed out after {} ms", request_timeout_ms),
                    latency_ms: started.elapsed().as_millis(),
                    occurred_at: now,
                }),
            }
        }
    }

    PublicMarketRefresh {
        snapshots_by_symbol,
        books_loaded,
        funding_loaded,
        errors,
        exchanges,
    }
}

async fn collect_public_ws_cache_refresh(
    config: CrossExchangeArbitrageConfig,
    symbols: Vec<CanonicalSymbol>,
    mut cache: MarketStateCache,
    cached_coverage: Option<ExchangeInstrumentCoverage>,
    request_timeout_ms: u64,
    refresh_funding: bool,
) -> PublicWsCacheRefresh {
    let now = Utc::now();
    let timeout_duration = TokioDuration::from_millis(request_timeout_ms.max(100));
    let mut errors = Vec::new();
    let mut funding_snapshots = Vec::new();
    let adapters = configured_market_adapters(&config);
    let exchanges = adapters
        .iter()
        .map(|adapter| adapter.exchange())
        .collect::<Vec<_>>();
    let coverage = match cached_coverage {
        Some(coverage) => coverage,
        None => {
            let coverage = load_exchange_instrument_coverage_from_boxed(
                &adapters,
                &symbols,
                request_timeout_ms,
                now,
            )
            .await;
            push_coverage_errors(&mut errors, &config, &symbols, &coverage, now);
            errors.extend(coverage.errors.clone());
            coverage
        }
    };

    if refresh_funding {
        for adapter in &adapters {
            let exchange = adapter.exchange();
            let symbols_for_exchange = symbols_for_exchange(&symbols, &coverage, &exchange);
            let started = Instant::now();
            match timeout(
                timeout_duration,
                adapter.load_funding(&symbols_for_exchange),
            )
            .await
            {
                Ok(Ok(funding)) => {
                    for snapshot in funding {
                        cache.upsert_funding(snapshot.clone());
                        funding_snapshots.push(snapshot);
                    }
                }
                Ok(Err(err)) => errors.push(MarketDataError {
                    exchange,
                    canonical_symbol: None,
                    kind: "ws_funding_rest_fallback".to_string(),
                    message: err.to_string(),
                    latency_ms: started.elapsed().as_millis(),
                    occurred_at: now,
                }),
                Err(_) => errors.push(MarketDataError {
                    exchange,
                    canonical_symbol: None,
                    kind: "ws_funding_rest_timeout".to_string(),
                    message: format!("timed out after {} ms", request_timeout_ms),
                    latency_ms: started.elapsed().as_millis(),
                    occurred_at: now,
                }),
            }
        }
    }

    let mut snapshots_by_symbol: HashMap<CanonicalSymbol, Vec<MarketSnapshot>> = HashMap::new();
    let mut books_loaded = 0usize;
    let mut funding_loaded = 0usize;

    for symbol in &symbols {
        for exchange in &exchanges {
            let Some(book) = cache.orderbook(exchange, symbol).cloned() else {
                continue;
            };
            books_loaded += 1;
            let funding = cache.funding(exchange, symbol);
            if funding.is_some() {
                funding_loaded += 1;
            }
            let stale = now.signed_duration_since(book.recv_ts).num_milliseconds()
                > config.risk.max_book_age_ms;
            snapshots_by_symbol
                .entry(symbol.clone())
                .or_default()
                .push(MarketSnapshot {
                    book,
                    instrument: coverage
                        .instruments_by_exchange_symbol
                        .get(&(exchange.clone(), symbol.clone()))
                        .cloned(),
                    route_status: if stale {
                        RouteStatus::Degraded
                    } else {
                        RouteStatus::Healthy
                    },
                    funding_rate: funding
                        .map(|snapshot| snapshot.funding_rate)
                        .unwrap_or_default(),
                    next_funding_time: funding.and_then(|snapshot| snapshot.next_funding_time),
                });
        }
    }

    PublicWsCacheRefresh {
        market: PublicMarketRefresh {
            snapshots_by_symbol,
            books_loaded,
            funding_loaded,
            errors,
            exchanges,
        },
        funding_snapshots,
    }
}

async fn load_exchange_instrument_coverage(
    config: &CrossExchangeArbitrageConfig,
    symbols: &[CanonicalSymbol],
    request_timeout_ms: u64,
) -> ExchangeInstrumentCoverage {
    let adapters = configured_market_adapters(config);
    load_exchange_instrument_coverage_from_boxed(&adapters, symbols, request_timeout_ms, Utc::now())
        .await
}

async fn load_exchange_instrument_coverage_from_boxed(
    adapters: &[Box<dyn MarketDataAdapter + Send + Sync>],
    symbols: &[CanonicalSymbol],
    request_timeout_ms: u64,
    now: DateTime<Utc>,
) -> ExchangeInstrumentCoverage {
    let wanted = symbols.iter().cloned().collect::<HashSet<_>>();
    let timeout_duration = TokioDuration::from_millis(request_timeout_ms.max(100));
    let mut supported_by_exchange = HashMap::new();
    let mut instruments_by_exchange_symbol = HashMap::new();
    let mut coverage_by_symbol: HashMap<CanonicalSymbol, HashSet<ExchangeId>> = HashMap::new();
    let mut errors = Vec::new();

    for adapter in adapters {
        let exchange = adapter.exchange();
        let started = Instant::now();
        match timeout(timeout_duration, adapter.load_instruments()).await {
            Ok(Ok(instruments)) => {
                let (supported, supported_instruments) =
                    supported_configured_symbols(instruments, &wanted);
                for symbol in &supported {
                    coverage_by_symbol
                        .entry(symbol.clone())
                        .or_default()
                        .insert(exchange.clone());
                }
                for instrument in supported_instruments {
                    instruments_by_exchange_symbol.insert(
                        (exchange.clone(), instrument.canonical_symbol.clone()),
                        instrument,
                    );
                }
                supported_by_exchange.insert(exchange, supported);
            }
            Ok(Err(err)) => errors.push(MarketDataError {
                exchange,
                canonical_symbol: None,
                kind: "instruments".to_string(),
                message: err.to_string(),
                latency_ms: started.elapsed().as_millis(),
                occurred_at: now,
            }),
            Err(_) => errors.push(MarketDataError {
                exchange,
                canonical_symbol: None,
                kind: "instruments_timeout".to_string(),
                message: format!("timed out after {} ms", request_timeout_ms),
                latency_ms: started.elapsed().as_millis(),
                occurred_at: now,
            }),
        }
    }

    ExchangeInstrumentCoverage {
        supported_by_exchange,
        instruments_by_exchange_symbol,
        coverage_by_symbol,
        errors,
    }
}

fn supported_configured_symbols(
    instruments: Vec<InstrumentMeta>,
    wanted: &HashSet<CanonicalSymbol>,
) -> (HashSet<CanonicalSymbol>, Vec<InstrumentMeta>) {
    let mut supported = HashSet::new();
    let mut supported_instruments = Vec::new();
    for instrument in instruments
        .into_iter()
        .filter(InstrumentMeta::is_tradeable_usdt_perpetual)
    {
        if wanted.contains(&instrument.canonical_symbol) {
            supported.insert(instrument.canonical_symbol.clone());
            supported_instruments.push(instrument);
        }
    }
    (supported, supported_instruments)
}

fn symbols_for_exchange(
    symbols: &[CanonicalSymbol],
    coverage: &ExchangeInstrumentCoverage,
    exchange: &ExchangeId,
) -> Vec<CanonicalSymbol> {
    let Some(supported) = coverage.supported_by_exchange.get(exchange) else {
        return Vec::new();
    };
    symbols
        .iter()
        .filter(|symbol| supported.contains(*symbol))
        .cloned()
        .collect()
}

fn push_coverage_errors(
    errors: &mut Vec<MarketDataError>,
    config: &CrossExchangeArbitrageConfig,
    symbols: &[CanonicalSymbol],
    coverage: &ExchangeInstrumentCoverage,
    now: DateTime<Utc>,
) {
    for symbol in symbols {
        let venues = coverage
            .coverage_by_symbol
            .get(symbol)
            .map(HashSet::len)
            .unwrap_or_default();
        if venues < config.market.min_common_exchanges {
            errors.push(MarketDataError {
                exchange: ExchangeId::Other("universe".to_string()),
                canonical_symbol: Some(symbol.clone()),
                kind: "coverage".to_string(),
                message: format!(
                    "available on {venues} exchange(s), requires at least {}",
                    config.market.min_common_exchanges
                ),
                latency_ms: 0,
                occurred_at: now,
            });
        }
    }
}

fn configured_market_adapters(
    config: &CrossExchangeArbitrageConfig,
) -> Vec<Box<dyn MarketDataAdapter + Send + Sync>> {
    config
        .universe
        .enabled_exchanges
        .iter()
        .filter_map(|exchange| match exchange {
            ExchangeId::Binance => {
                Some(Box::new(BinanceMarketAdapter) as Box<dyn MarketDataAdapter + Send + Sync>)
            }
            ExchangeId::Okx => {
                Some(Box::new(OkxMarketAdapter) as Box<dyn MarketDataAdapter + Send + Sync>)
            }
            ExchangeId::Bitget => {
                Some(Box::new(BitgetMarketAdapter) as Box<dyn MarketDataAdapter + Send + Sync>)
            }
            ExchangeId::Gate => {
                Some(Box::new(GateMarketAdapter) as Box<dyn MarketDataAdapter + Send + Sync>)
            }
            ExchangeId::Other(_) => None,
        })
        .collect()
}

fn configured_market_adapter_arcs(
    config: &CrossExchangeArbitrageConfig,
) -> Vec<Arc<dyn MarketDataAdapter + Send + Sync>> {
    config
        .universe
        .enabled_exchanges
        .iter()
        .filter_map(|exchange| match exchange {
            ExchangeId::Binance => {
                Some(Arc::new(BinanceMarketAdapter) as Arc<dyn MarketDataAdapter + Send + Sync>)
            }
            ExchangeId::Okx => {
                Some(Arc::new(OkxMarketAdapter) as Arc<dyn MarketDataAdapter + Send + Sync>)
            }
            ExchangeId::Bitget => {
                Some(Arc::new(BitgetMarketAdapter) as Arc<dyn MarketDataAdapter + Send + Sync>)
            }
            ExchangeId::Gate => {
                Some(Arc::new(GateMarketAdapter) as Arc<dyn MarketDataAdapter + Send + Sync>)
            }
            ExchangeId::Other(_) => None,
        })
        .collect()
}

fn simulation_exchanges(config: &CrossExchangeArbitrageConfig) -> Vec<ExchangeId> {
    let mut exchanges = config.universe.enabled_exchanges.clone();
    exchanges.retain(|exchange| {
        matches!(
            exchange,
            ExchangeId::Binance | ExchangeId::Okx | ExchangeId::Bitget | ExchangeId::Gate
        )
    });
    if exchanges.len() < 2 {
        exchanges = vec![ExchangeId::Binance, ExchangeId::Okx];
    }
    exchanges.truncate(2);
    exchanges
}

fn synthetic_market_snapshots(
    symbol: &CanonicalSymbol,
    index: usize,
    tick_index: u64,
    now: DateTime<Utc>,
    exchanges: &[ExchangeId],
) -> Vec<MarketSnapshot> {
    let base = base_price(symbol, index);
    let cycle = ((tick_index + index as u64) % 17) as f64;
    let spread_bias = 0.006 + cycle * 0.00035;
    let long_mid = base * (1.0 - spread_bias * 0.35);
    let short_mid = base * (1.0 + spread_bias * 0.65);
    let funding_phase = ((tick_index + index as u64) % 11) as f64 - 5.0;

    exchanges
        .iter()
        .enumerate()
        .map(|(exchange_index, exchange)| {
            let mid = if exchange_index == 0 {
                long_mid
            } else {
                short_mid
            };
            MarketSnapshot {
                book: synthetic_book(exchange.clone(), symbol, mid, index, now),
                instrument: Some(synthetic_instrument(exchange.clone(), symbol, mid)),
                route_status: RouteStatus::Healthy,
                funding_rate: if exchange_index == 0 {
                    funding_phase * 0.00001
                } else {
                    -funding_phase * 0.000008
                },
                next_funding_time: Some(now + Duration::hours(8)),
            }
        })
        .collect()
}

fn synthetic_instrument(
    exchange: ExchangeId,
    symbol: &CanonicalSymbol,
    reference_price: f64,
) -> InstrumentMeta {
    let price_tick = if reference_price >= 1000.0 {
        0.1
    } else if reference_price >= 1.0 {
        0.0001
    } else {
        0.000001
    };
    let quantity_step = match exchange {
        ExchangeId::Okx => 0.1,
        ExchangeId::Gate => 1.0,
        _ => 0.001,
    };
    let min_qty = quantity_step;
    let min_notional = match exchange {
        ExchangeId::Okx => 5.0,
        ExchangeId::Gate => 1.0,
        _ => 5.0,
    };
    InstrumentMeta::new(
        exchange.clone(),
        symbol.clone(),
        exchange_symbol_for(&exchange, symbol),
        symbol.base(),
        symbol.quote(),
        "USDT",
        ContractType::LinearPerpetual,
        1.0,
        price_tick,
        quantity_step,
        min_qty,
        min_notional,
        rustcta::market::decimal_places(price_tick),
        rustcta::market::decimal_places(quantity_step),
        InstrumentStatus::Trading,
    )
}

fn synthetic_book(
    exchange: ExchangeId,
    symbol: &CanonicalSymbol,
    mid: f64,
    index: usize,
    now: DateTime<Utc>,
) -> OrderBook5 {
    let spread = (0.0004 + (index % 5) as f64 * 0.00005).max(0.0001);
    let bid = mid * (1.0 - spread / 2.0);
    let ask = mid * (1.0 + spread / 2.0);
    let qty = (500.0 / mid.max(0.01)).max(1.0);
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for level in 0..5 {
        let step = 1.0 - level as f64 * 0.0005;
        bids.push(BookLevel::new(bid * step, qty * (1.0 + level as f64 * 0.5)));
        asks.push(BookLevel::new(ask / step, qty * (1.0 + level as f64 * 0.5)));
    }

    OrderBook5::new(
        exchange.clone(),
        symbol.clone(),
        exchange_symbol_for(&exchange, symbol),
        bids,
        asks,
        now,
        now,
        Some(index as u64),
        Some("simulated_market".to_string()),
    )
}

fn base_price(symbol: &CanonicalSymbol, index: usize) -> f64 {
    let seed = symbol.base().bytes().fold(0u64, |acc, byte| {
        acc.wrapping_mul(31).wrapping_add(byte as u64)
    });
    let bucket = (seed % 9) as f64;
    match index % 10 {
        0 => 65_000.0 + bucket * 100.0,
        1 => 3_200.0 + bucket * 25.0,
        2 => 120.0 + bucket * 3.0,
        3 => 0.5 + bucket * 0.03,
        4 => 7.0 + bucket * 0.2,
        5 => 0.08 + bucket * 0.01,
        6 => 25.0 + bucket,
        7 => 1.0 + bucket * 0.05,
        8 => 250.0 + bucket * 5.0,
        _ => 10.0 + bucket * 0.4,
    }
}

fn build_symbol_universe(
    config: &CrossExchangeArbitrageConfig,
    symbol_count: usize,
) -> Vec<CanonicalSymbol> {
    let mut symbols = Vec::new();
    for symbol in &config.universe.symbols {
        if is_shadow_candidate_symbol(symbol) && !symbols.contains(symbol) {
            symbols.push(symbol.clone());
        }
    }
    for base in DEFAULT_BASES {
        let symbol = CanonicalSymbol::new(*base, "USDT");
        if is_shadow_candidate_symbol(&symbol) && !symbols.contains(&symbol) {
            symbols.push(symbol);
        }
        if symbols.len() >= symbol_count {
            break;
        }
    }
    symbols.truncate(symbol_count);
    symbols
}

fn is_shadow_candidate_symbol(symbol: &CanonicalSymbol) -> bool {
    symbol.quote() == "USDT" && !MAINSTREAM_BASES.contains(&symbol.base())
}

fn exchange_read_models(
    exchanges: &[ExchangeId],
    enabled_symbols: usize,
    now: DateTime<Utc>,
) -> Vec<ExchangeReadModel> {
    exchanges
        .iter()
        .map(|exchange| ExchangeReadModel {
            exchange: exchange.as_str().to_string(),
            status: "simulated_healthy".to_string(),
            mode: RuntimeMode::Simulation,
            enabled_symbols,
            route_count: enabled_symbols,
            last_message_age_ms: Some(now.timestamp_millis() % 1_000),
            reconnect_count: 0,
            sequence_gap_count: 0,
        })
        .collect()
}

fn exchange_read_models_with_errors(
    exchanges: &[ExchangeId],
    enabled_symbols: usize,
    now: DateTime<Utc>,
    errors: &[MarketDataError],
) -> Vec<ExchangeReadModel> {
    exchanges
        .iter()
        .map(|exchange| {
            let error_count = errors
                .iter()
                .filter(|error| error.exchange == *exchange)
                .count();
            ExchangeReadModel {
                exchange: exchange.as_str().to_string(),
                status: if error_count == 0 {
                    "public_shadow_healthy".to_string()
                } else {
                    format!("public_shadow_errors_{}", error_count)
                },
                mode: RuntimeMode::Simulation,
                enabled_symbols,
                route_count: enabled_symbols.saturating_sub(error_count),
                last_message_age_ms: Some(now.timestamp_millis() % 1_000),
                reconnect_count: 0,
                sequence_gap_count: 0,
            }
        })
        .collect()
}

fn book_quality_from_snapshots(
    snapshots: &[MarketSnapshot],
    now: DateTime<Utc>,
    max_book_age_ms: i64,
) -> Vec<OrderBookQualityReadModel> {
    snapshots
        .iter()
        .map(|snapshot| {
            let book = mark_book_freshness(snapshot.book.clone(), now, max_book_age_ms);
            let best_bid = book.best_bid().map(|level| level.price);
            let best_ask = book.best_ask().map(|level| level.price);
            let book_age_ms = now.signed_duration_since(book.recv_ts).num_milliseconds();
            let usable = book.is_usable();
            OrderBookQualityReadModel {
                exchange: book.exchange.clone(),
                canonical_symbol: book.canonical_symbol.clone(),
                best_bid,
                best_ask,
                bid_levels: book.bids.len(),
                ask_levels: book.asks.len(),
                book_age_ms,
                source_route: book.source_route.clone(),
                quality: book.quality.clone(),
                usable,
            }
        })
        .collect()
}

fn top_symbol_pnl(fills: &[SimulatedMarketFill]) -> (Option<CanonicalSymbol>, f64) {
    let mut pnl_by_symbol: HashMap<CanonicalSymbol, f64> = HashMap::new();
    for fill in fills {
        *pnl_by_symbol
            .entry(fill.canonical_symbol.clone())
            .or_insert(0.0) += fill.net_pnl_usdt;
    }
    pnl_by_symbol
        .into_iter()
        .max_by(|(_, left), (_, right)| {
            left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(symbol, pnl)| (Some(symbol), pnl))
        .unwrap_or((None, 0.0))
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn trim_front<T>(items: &mut Vec<T>, max_len: usize) {
    if items.len() > max_len {
        let remove = items.len() - max_len;
        items.drain(0..remove);
    }
}

fn tool_commands() -> Vec<ToolCommandReadModel> {
    vec![
        ToolCommandReadModel {
            name: "Local quick gate".to_string(),
            command: "scripts/cross_arb_local_gate.sh quick".to_string(),
            scope: "local".to_string(),
        },
        ToolCommandReadModel {
            name: "Local full gate".to_string(),
            command: "scripts/cross_arb_local_gate.sh full".to_string(),
            scope: "local".to_string(),
        },
        ToolCommandReadModel {
            name: "Public connectivity".to_string(),
            command: "scripts/cross_arb_local_gate.sh network".to_string(),
            scope: "local/server".to_string(),
        },
        ToolCommandReadModel {
            name: "Private preflight".to_string(),
            command: "scripts/cross_arb_local_gate.sh private".to_string(),
            scope: "server".to_string(),
        },
        ToolCommandReadModel {
            name: "Server smoke".to_string(),
            command: "scripts/cross_arb_local_gate.sh server-smoke".to_string(),
            scope: "server".to_string(),
        },
    ]
}

fn current_positions_from_shadow_positions(
    positions: &HashMap<CanonicalSymbol, ShadowOpenPosition>,
) -> Vec<ShadowPositionReadModel> {
    let now = Utc::now();
    let mut rows = positions
        .values()
        .map(|position| {
            let estimated_fee_usdt = position.open_fee_usdt + position.estimated_close_fee_usdt;
            let status = format!(
                "持仓中 {}s，等待价差回归",
                now.signed_duration_since(position.opened_at)
                    .num_seconds()
                    .max(0)
            );
            ShadowPositionReadModel {
                canonical_symbol: position.canonical_symbol.clone(),
                long_exchange: position.long_exchange.clone(),
                short_exchange: position.short_exchange.clone(),
                notional_usdt: position.executable_notional_usdt,
                entry_edge_pct: position.entry_spread_pct,
                current_spread_pct: position.last_close_spread_pct,
                expected_fee_usdt: estimated_fee_usdt,
                expected_funding_usdt: position.estimated_funding_pnl_usdt,
                book_slippage_cost_usdt: position.estimated_book_slippage_cost_usdt,
                safety_buffer_cost_usdt: position.estimated_safety_buffer_cost_usdt,
                estimated_net_pnl_usdt: position.estimated_net_pnl_usdt,
                holding_secs: now
                    .signed_duration_since(position.opened_at)
                    .num_seconds()
                    .max(0),
                status,
                updated_at: position.updated_at,
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .estimated_net_pnl_usdt
            .partial_cmp(&left.estimated_net_pnl_usdt)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows.truncate(50);
    rows
}

fn best_open_opportunities_by_symbol(opportunities: &[Opportunity]) -> Vec<&Opportunity> {
    let mut seen_symbols = HashSet::new();
    opportunities
        .iter()
        .filter(|opportunity| {
            opportunity.can_open && seen_symbols.insert(opportunity.canonical_symbol.clone())
        })
        .collect()
}

fn best_opportunity_by_symbol(
    opportunities: &[Opportunity],
) -> HashMap<CanonicalSymbol, &Opportunity> {
    let mut best = HashMap::new();
    for opportunity in opportunities {
        best.entry(opportunity.canonical_symbol.clone())
            .or_insert(opportunity);
    }
    best
}

fn apply_shadow_capacity_to_opportunities(
    opportunities: &mut [Opportunity],
    config: &CrossExchangeArbitrageConfig,
    positions: &HashMap<CanonicalSymbol, ShadowOpenPosition>,
) {
    for opportunity in opportunities {
        if let Some(reason) = exchange_capacity_reject_reason(config, positions, opportunity) {
            if !opportunity.reject_reasons.contains(&reason) {
                opportunity.reject_reasons.push(reason);
            }
            opportunity.can_open = false;
        }
    }
}

fn exchange_capacity_reject_reason(
    config: &CrossExchangeArbitrageConfig,
    positions: &HashMap<CanonicalSymbol, ShadowOpenPosition>,
    opportunity: &Opportunity,
) -> Option<RejectReason> {
    let max_notional = exchange_max_notional_usdt(config);
    let max_positions = config.sizing.max_positions_per_exchange;
    let usage = exchange_usage_from_shadow_positions(positions);
    for exchange in [&opportunity.long_exchange, &opportunity.short_exchange] {
        let used = usage
            .get(exchange)
            .copied()
            .unwrap_or_else(ExchangeUsage::default);
        if max_positions > 0 && used.position_count >= max_positions {
            return Some(RejectReason::ExchangePositionLimitExceeded);
        }
        if used.notional_usdt + opportunity.executable_notional_usdt > max_notional + f64::EPSILON {
            return Some(RejectReason::ExchangeCapacityExceeded);
        }
    }
    None
}

fn exchange_usage_from_shadow_positions(
    positions: &HashMap<CanonicalSymbol, ShadowOpenPosition>,
) -> HashMap<ExchangeId, ExchangeUsage> {
    let mut usage = HashMap::new();
    for position in positions.values() {
        for exchange in [&position.long_exchange, &position.short_exchange] {
            let entry = usage
                .entry(exchange.clone())
                .or_insert_with(ExchangeUsage::default);
            entry.notional_usdt += position.executable_notional_usdt;
            entry.position_count += 1;
        }
    }
    usage
}

#[derive(Debug, Clone, Copy, Default)]
struct ExchangeUsage {
    notional_usdt: f64,
    position_count: usize,
}

fn exchange_max_notional_usdt(config: &CrossExchangeArbitrageConfig) -> f64 {
    (config.sizing.exchange_equity_usdt * config.sizing.leverage).max(0.0)
}

fn exchange_capital_models(
    config: &CrossExchangeArbitrageConfig,
    positions: &HashMap<CanonicalSymbol, ShadowOpenPosition>,
) -> Vec<ExchangeCapitalReadModel> {
    let usage = exchange_usage_from_shadow_positions(positions);
    let max_notional = exchange_max_notional_usdt(config);
    let max_positions = config.sizing.max_positions_per_exchange;
    let mut rows = config
        .universe
        .enabled_exchanges
        .iter()
        .map(|exchange| {
            let used = usage
                .get(exchange)
                .copied()
                .unwrap_or_else(ExchangeUsage::default);
            ExchangeCapitalReadModel {
                exchange: exchange.clone(),
                equity_usdt: config.sizing.exchange_equity_usdt,
                leverage: config.sizing.leverage,
                max_notional_usdt: max_notional,
                used_notional_usdt: used.notional_usdt,
                remaining_notional_usdt: (max_notional - used.notional_usdt).max(0.0),
                open_positions: used.position_count,
                max_positions,
                remaining_positions: max_positions.saturating_sub(used.position_count),
                usage_pct: if max_notional > 0.0 {
                    used.notional_usdt / max_notional
                } else {
                    0.0
                },
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.exchange.as_str().cmp(right.exchange.as_str()));
    rows
}

fn fee_schedule_models(config: &CrossExchangeArbitrageConfig) -> Vec<FeeScheduleReadModel> {
    let fee_model = FeeModel::from_config(&config.fees);
    let mut rows = config
        .universe
        .enabled_exchanges
        .iter()
        .map(|exchange| {
            let rates = fee_model.rates_for(exchange);
            FeeScheduleReadModel {
                exchange: exchange.clone(),
                maker_fee_rate: rates.maker,
                taker_fee_rate: rates.taker,
                source: "public fee schedule / VIP0 baseline, account API disabled".to_string(),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.exchange.as_str().cmp(right.exchange.as_str()));
    rows
}

fn estimate_shadow_close(
    position: &ShadowOpenPosition,
    snapshots: &[MarketSnapshot],
    config: &CrossExchangeArbitrageConfig,
    now: DateTime<Utc>,
) -> Option<ShadowCloseEstimate> {
    let long_snapshot = snapshots
        .iter()
        .find(|snapshot| snapshot.book.exchange == position.long_exchange)?;
    let short_snapshot = snapshots
        .iter()
        .find(|snapshot| snapshot.book.exchange == position.short_exchange)?;
    if !long_snapshot.book.is_usable() || !short_snapshot.book.is_usable() {
        return None;
    }

    let long_close = calculate_taker_vwap(
        &long_snapshot.book,
        OrderSide::Sell,
        position.executable_notional_usdt,
    );
    let short_close = calculate_taker_vwap(
        &short_snapshot.book,
        OrderSide::Buy,
        position.executable_notional_usdt,
    );
    let long_close_price = long_close.vwap_price?;
    let short_close_price = short_close.vwap_price?;
    if long_close_price <= 0.0 || short_close_price <= 0.0 {
        return None;
    }

    let exit_spread_pct = short_close_price / long_close_price - 1.0;
    let spread_convergence = position.entry_spread_pct - exit_spread_pct;
    let gross_spread_pnl_usdt = spread_convergence * position.executable_notional_usdt;
    let fee_model = FeeModel::from_config(&config.fees);
    let close_fee_usdt = fee_model.fee_amount(
        &position.long_exchange,
        FeeRole::Taker,
        position.executable_notional_usdt,
    ) + fee_model.fee_amount(
        &position.short_exchange,
        FeeRole::Taker,
        position.executable_notional_usdt,
    );
    let held_secs = now
        .signed_duration_since(position.opened_at)
        .num_seconds()
        .max(0) as f64;
    let funding_pnl_usdt = prorated_funding_pnl(position.entry_expected_funding_usdt, held_secs);
    let book_slippage_cost_usdt = (long_close.slippage_pct.abs() + short_close.slippage_pct.abs())
        * position.executable_notional_usdt;
    let safety_buffer_cost_usdt = config.risk.safety_buffer * position.executable_notional_usdt;
    let slippage_cost_usdt = book_slippage_cost_usdt + safety_buffer_cost_usdt;
    let net_pnl_usdt = gross_spread_pnl_usdt + funding_pnl_usdt
        - position.open_fee_usdt
        - close_fee_usdt
        - slippage_cost_usdt;
    let depth_enough = long_close.depth_enough && short_close.depth_enough;
    let profit_pct = net_pnl_usdt / position.executable_notional_usdt.max(1.0);
    let close_threshold = config
        .thresholds
        .lock_profit_dual_taker_pct
        .max(SHADOW_FORCE_CLOSE_PROFIT_PCT);
    let should_close = depth_enough
        && exit_spread_pct < position.entry_spread_pct
        && profit_pct >= close_threshold;

    Some(ShadowCloseEstimate {
        long_close_price,
        short_close_price,
        exit_spread_pct,
        gross_spread_pnl_usdt,
        close_fee_usdt,
        funding_pnl_usdt,
        book_slippage_cost_usdt,
        safety_buffer_cost_usdt,
        slippage_cost_usdt,
        net_pnl_usdt,
        should_close,
    })
}

fn prorated_funding_pnl(expected_funding_usdt: f64, held_secs: f64) -> f64 {
    let funding_period_secs = 8.0 * 60.0 * 60.0;
    expected_funding_usdt * (held_secs / funding_period_secs).clamp(0.0, 1.0)
}

fn exchange_analysis_from_fills(fills: &[SimulatedMarketFill]) -> Vec<ExchangeAnalysisReadModel> {
    #[derive(Default)]
    struct Acc {
        long_count: usize,
        short_count: usize,
        open_count_7d: usize,
        wins_7d: usize,
        notional: f64,
        pnl: f64,
        edge_sum: f64,
        edge_count: usize,
    }
    let mut by_exchange: HashMap<ExchangeId, Acc> = HashMap::new();
    let seven_days_ago = Utc::now() - Duration::days(7);
    for fill in fills {
        let in_7d = fill.opened_at >= seven_days_ago;
        let won = fill.net_pnl_usdt > 0.0;
        let long = by_exchange.entry(fill.long_exchange.clone()).or_default();
        long.long_count += 1;
        if in_7d {
            long.open_count_7d += 1;
            if won {
                long.wins_7d += 1;
            }
        }
        long.notional += fill.executable_notional_usdt;
        long.pnl += fill.net_pnl_usdt / 2.0;
        long.edge_sum += fill.net_edge_pct;
        long.edge_count += 1;

        let short = by_exchange.entry(fill.short_exchange.clone()).or_default();
        short.short_count += 1;
        if in_7d {
            short.open_count_7d += 1;
            if won {
                short.wins_7d += 1;
            }
        }
        short.notional += fill.executable_notional_usdt;
        short.pnl += fill.net_pnl_usdt / 2.0;
        short.edge_sum += fill.net_edge_pct;
        short.edge_count += 1;
    }
    let mut rows = by_exchange
        .into_iter()
        .map(|(exchange, acc)| ExchangeAnalysisReadModel {
            exchange,
            long_count: acc.long_count,
            short_count: acc.short_count,
            open_count_7d: acc.open_count_7d,
            win_rate_7d: ratio(acc.wins_7d, acc.open_count_7d),
            simulated_notional_usdt: acc.notional,
            net_pnl_usdt: acc.pnl,
            avg_edge_pct: if acc.edge_count == 0 {
                0.0
            } else {
                acc.edge_sum / acc.edge_count as f64
            },
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .net_pnl_usdt
            .partial_cmp(&left.net_pnl_usdt)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
}

fn fee_analysis_from_fills(fills: &[SimulatedMarketFill]) -> Vec<FeeAnalysisReadModel> {
    let mut by_exchange: HashMap<ExchangeId, (f64, f64)> = HashMap::new();
    for fill in fills {
        let fee_half = fill.fee_pnl_usdt.abs() / 2.0;
        let long = by_exchange.entry(fill.long_exchange.clone()).or_default();
        long.0 += fee_half;
        long.1 += fill.executable_notional_usdt;
        let short = by_exchange.entry(fill.short_exchange.clone()).or_default();
        short.0 += fee_half;
        short.1 += fill.executable_notional_usdt;
    }
    let mut rows = by_exchange
        .into_iter()
        .map(|(exchange, (fee, notional))| FeeAnalysisReadModel {
            exchange,
            estimated_fee_usdt: fee,
            simulated_notional_usdt: notional,
            fee_bps: if notional > 0.0 {
                fee / notional * 10_000.0
            } else {
                0.0
            },
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .estimated_fee_usdt
            .partial_cmp(&left.estimated_fee_usdt)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
}

fn book_health_summary(books: &[OrderBookQualityReadModel]) -> Vec<BookHealthSummaryReadModel> {
    #[derive(Default)]
    struct Acc {
        total_books: usize,
        usable_books: usize,
        stale_books: usize,
        crossed_books: usize,
        invalid_level_books: usize,
        sequence_gap_books: usize,
        max_age_ms: i64,
    }

    let mut by_exchange: HashMap<ExchangeId, Acc> = HashMap::new();
    for book in books {
        let acc = by_exchange.entry(book.exchange.clone()).or_default();
        acc.total_books += 1;
        if book.usable {
            acc.usable_books += 1;
        }
        if book.quality.stale {
            acc.stale_books += 1;
        }
        if book.quality.crossed {
            acc.crossed_books += 1;
        }
        if book.quality.has_invalid_levels {
            acc.invalid_level_books += 1;
        }
        if book.quality.sequence_gap {
            acc.sequence_gap_books += 1;
        }
        acc.max_age_ms = acc.max_age_ms.max(book.book_age_ms);
    }

    let mut rows = by_exchange
        .into_iter()
        .map(|(exchange, acc)| BookHealthSummaryReadModel {
            exchange,
            total_books: acc.total_books,
            usable_books: acc.usable_books,
            stale_books: acc.stale_books,
            crossed_books: acc.crossed_books,
            invalid_level_books: acc.invalid_level_books,
            sequence_gap_books: acc.sequence_gap_books,
            max_age_ms: acc.max_age_ms,
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.exchange.as_str().cmp(right.exchange.as_str()));
    rows
}

fn funding_analysis_from_fills(fills: &[SimulatedMarketFill]) -> Vec<FundingAnalysisReadModel> {
    #[derive(Default)]
    struct Acc {
        pnl: f64,
        long_count: usize,
        short_count: usize,
    }
    let mut by_exchange: HashMap<ExchangeId, Acc> = HashMap::new();
    for fill in fills {
        let long = by_exchange.entry(fill.long_exchange.clone()).or_default();
        long.long_count += 1;
        long.pnl += fill.funding_pnl_usdt / 2.0;
        let short = by_exchange.entry(fill.short_exchange.clone()).or_default();
        short.short_count += 1;
        short.pnl += fill.funding_pnl_usdt / 2.0;
    }
    let mut rows = by_exchange
        .into_iter()
        .map(|(exchange, acc)| FundingAnalysisReadModel {
            exchange,
            funding_pnl_usdt: acc.pnl,
            long_count: acc.long_count,
            short_count: acc.short_count,
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .funding_pnl_usdt
            .partial_cmp(&left.funding_pnl_usdt)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
}

static MAINSTREAM_BASES: &[&str] = &[
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "TRX", "LINK", "AVAX", "LTC", "BCH", "DOT",
];

static DEFAULT_BASES: &[&str] = &[
    "ARB", "OP", "APT", "INJ", "SEI", "TIA", "WIF", "PEPE", "SHIB", "FLOKI", "BONK", "FET",
    "RENDER", "TAO", "WLD", "CRV", "LDO", "PENDLE", "ENS", "MKR", "COMP", "SNX", "DYDX", "RUNE",
    "STX", "ORDI", "SATS", "JUP", "PYTH", "STRK", "MANTA", "BLUR", "MEME", "NOT", "TON", "POL",
    "MATIC", "XLM", "ICP", "HBAR", "ALGO", "VET", "GRT", "SAND", "MANA", "AXS", "IMX", "GALA",
    "CHZ", "EGLD", "KAS", "ZEC", "DASH", "XMR", "KAVA", "MINA", "ROSE", "ZIL", "IOTA", "ONDO",
    "ENA", "ETHFI", "PEOPLE", "GMT", "MASK", "CFX", "LPT", "AR", "FTM", "CELO", "QTUM", "ANKR",
    "ONE", "HOT", "RVN", "KSM", "WAVES", "YFI", "1INCH", "ZRX", "API3", "MAGIC", "SSV", "ID",
    "HIGH", "CELR", "NKN", "SKL", "COTI", "ALICE", "BAKE", "BEL", "C98", "CTSI", "DUSK", "IOST",
    "KNC", "LINA", "LRC", "STG", "HOOK", "RDNT", "ACH", "JOE", "PERP", "T", "UMA", "WOO", "AGIX",
    "GAL", "GMX", "HFT", "ILV", "KDA", "LEVER", "LQTY", "OXT", "PHB", "RLC", "SPELL",
];

const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>RustCTA 跨交易所套利监控台</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0e1116;
      --band: #131820;
      --panel: #181e27;
      --panel2: #11161d;
      --line: #29313d;
      --text: #e8ecf2;
      --muted: #98a2b3;
      --green: #31c48d;
      --red: #f97066;
      --amber: #fdb022;
      --blue: #60a5fa;
      --cyan: #22d3ee;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", "Microsoft YaHei", sans-serif;
      background: var(--bg);
      color: var(--text);
      letter-spacing: 0;
    }
    header {
      display: grid;
      grid-template-columns: minmax(260px, 1fr) auto;
      gap: 16px;
      align-items: center;
      padding: 14px 18px;
      border-bottom: 1px solid var(--line);
      background: var(--band);
      position: sticky;
      top: 0;
      z-index: 5;
    }
    h1 { font-size: 18px; margin: 0; font-weight: 700; }
    .sub { color: var(--muted); font-size: 12px; }
    main { padding: 14px; display: grid; gap: 14px; }
    .toolbar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; justify-content: flex-end; }
    button, select, input {
      background: #202837;
      border: 1px solid #344055;
      color: var(--text);
      border-radius: 6px;
      min-height: 32px;
      padding: 6px 10px;
      font-size: 12px;
    }
    button { cursor: pointer; }
    button:hover, select:hover, input:hover { border-color: var(--blue); }
    input { min-width: 190px; }
    .kpis { display: grid; grid-template-columns: repeat(8, minmax(120px, 1fr)); gap: 10px; }
    .metric {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 11px 12px;
      min-height: 76px;
    }
    .label { color: var(--muted); font-size: 12px; }
    .value { margin-top: 8px; font-size: 20px; font-weight: 750; }
    .value.small { font-size: 15px; line-height: 1.35; }
    .layout {
      display: grid;
      grid-template-columns: 1.25fr .95fr;
      gap: 14px;
      align-items: start;
    }
    .wide { display: grid; grid-template-columns: 1fr; gap: 14px; }
    .stack { display: grid; grid-template-columns: 1fr; gap: 14px; }
    .tri { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      min-width: 0;
      overflow: hidden;
    }
    .title {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      padding: 11px 13px;
      border-bottom: 1px solid var(--line);
      background: var(--panel2);
      font-size: 14px;
      font-weight: 700;
    }
    .table-wrap { overflow: auto; max-height: 420px; }
    .table-wrap.tall { max-height: 560px; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { padding: 8px 10px; border-bottom: 1px solid var(--line); text-align: left; white-space: nowrap; vertical-align: top; }
    th { color: var(--muted); font-weight: 650; background: #171d26; position: sticky; top: 0; z-index: 1; }
    .ok { color: var(--green); }
    .bad { color: var(--red); }
    .warn { color: var(--amber); }
    .blue { color: var(--blue); }
    .cyan { color: var(--cyan); }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      padding: 2px 8px;
      border-radius: 999px;
      background: #202837;
      border: 1px solid #344055;
      color: var(--muted);
    }
    .note {
      padding: 10px 13px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.6;
      border-bottom: 1px solid var(--line);
      background: #121821;
    }
    code { color: #cad3df; font-size: 12px; }
    @media (max-width: 1280px) {
      .kpis { grid-template-columns: repeat(4, minmax(120px, 1fr)); }
      .layout, .tri { grid-template-columns: 1fr; }
    }
    @media (max-width: 720px) {
      header { grid-template-columns: 1fr; }
      .toolbar { justify-content: flex-start; }
      .kpis { grid-template-columns: repeat(2, minmax(120px, 1fr)); }
      input { width: 100%; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>跨交易所 USDT 永续套利监控台</h1>
      <div class="sub" id="subtitle">正在加载行情状态</div>
    </div>
    <div class="toolbar">
      <input id="symbolFilter" placeholder="筛选交易对，例如 ARB" oninput="render()" />
      <select id="exchangeFilter" onchange="render()"><option value="">全部交易所</option></select>
      <select id="openFilter" onchange="render()">
        <option value="">全部状态</option>
        <option value="open">仅可开仓</option>
        <option value="blocked">仅被风控拦截</option>
      </select>
      <button onclick="post('/api/cross-arb/control/resume-new-entries')">恢复影子开仓</button>
      <button onclick="post('/api/cross-arb/control/pause-new-entries')">暂停新开</button>
      <button onclick="post('/api/cross-arb/control/reset-simulation')">清空历史</button>
      <button onclick="post('/api/cross-arb/control/kill-switch')">熔断</button>
    </div>
  </header>
  <main>
    <section class="kpis" id="metrics"></section>
    <section class="layout">
      <div class="panel">
        <div class="title"><span>资金与仓位容量</span><span class="sub">每所本金 500U，10 倍杠杆，最多 50 个仓位</span></div>
        <div class="note">影子开仓会同时检查做多交易所和做空交易所容量：单交易所名义占用不超过 5000U，仓位数不超过 50。</div>
        <div class="table-wrap"><table id="capital"></table></div>
      </div>
      <div class="panel">
        <div class="title"><span>模拟基准</span><span class="sub">永续合约、盘口方向、手续费与滑点拆分</span></div>
        <div class="note">只订阅 USDT 线性永续合约。开多/平空为买入，吃卖一 ask；开空/平多为卖出，吃买一 bid。盘口滑点按 5 档 VWAP 计算，安全缓冲单独列出，不再混作真实滑点。</div>
        <div class="table-wrap"><table id="feeSchedule"></table></div>
      </div>
    </section>
    <section class="layout">
      <div class="panel">
        <div class="title"><span>当前影子持仓</span><span class="sub">已开仓后等待价差回归，不代表交易所真实持仓</span></div>
        <div class="table-wrap"><table id="positions"></table></div>
      </div>
      <div class="panel">
        <div class="title"><span>计划监控交易对池</span><span class="sub">默认排除 BTC/ETH/SOL/BNB/XRP 等主流品种</span></div>
        <div class="table-wrap"><table id="symbols"></table></div>
      </div>
    </section>
    <section class="layout">
      <div class="panel">
          <div class="title"><span>历史影子成交</span><span class="sub">价差回归后才平仓记录，不代表交易所真实仓位</span></div>
        <div class="table-wrap tall"><table id="fills"></table></div>
      </div>
      <div class="wide">
        <div class="panel">
          <div class="title"><span>PnL 归因</span><span class="sub">价差、手续费、资金费率、滑点</span></div>
          <div class="table-wrap"><table id="pnl"></table></div>
        </div>
        <div class="panel">
          <div class="title"><span>机会明细</span><span class="sub">按净边际排序</span></div>
          <div class="table-wrap"><table id="opps"></table></div>
        </div>
      </div>
    </section>
    <section class="tri">
      <div class="panel">
        <div class="title"><span>交易所分析</span><span class="sub">多空角色、名义金额、PnL</span></div>
        <div class="table-wrap"><table id="exchangeAnalysis"></table></div>
      </div>
      <div class="panel">
        <div class="title"><span>手续费分析</span><span class="sub">按公开 VIP0 基准汇总预估费用</span></div>
        <div class="table-wrap"><table id="feeAnalysis"></table></div>
      </div>
      <div class="panel">
        <div class="title"><span>资金费率分析</span><span class="sub">按多空腿分摊资金费率影响</span></div>
        <div class="table-wrap"><table id="fundingAnalysis"></table></div>
      </div>
    </section>
    <section class="layout">
      <div class="panel">
        <div class="title"><span>订单簿健康度</span><span class="sub">5档批量订阅质量、盘口年龄、异常盘口</span></div>
        <div class="table-wrap"><table id="bookHealth"></table></div>
      </div>
      <div class="panel">
        <div class="title"><span>精度后下单规模</span><span class="sub">每腿目标 100 USDT，按交易所规则归一化</span></div>
        <div class="table-wrap"><table id="sizing"></table></div>
      </div>
    </section>
    <section class="layout">
      <div class="panel">
        <div class="title"><span>行情接口与备用路径</span><span class="sub">public-rest 失败会在这里集中暴露</span></div>
        <div class="table-wrap"><table id="marketErrors"></table></div>
      </div>
      <div class="panel">
        <div class="title"><span>风控拒绝与本地工具</span><span class="sub">测试前检查项</span></div>
        <div class="table-wrap"><table id="rejects"></table></div>
        <div class="table-wrap"><table id="tools"></table></div>
      </div>
    </section>
  </main>
  <script>
    let currentData = null;
    let exchangeOptionsReady = false;
    const fmt = new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 4 });
    const intFmt = new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 0 });
    const money = new Intl.NumberFormat('zh-CN', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 });

    async function load() {
      currentData = await fetch('/api/cross-arb/dashboard').then(r => r.json());
      syncExchangeFilter(currentData);
      render();
    }

    function render() {
      if (!currentData) return;
      const data = currentData;
      const modeText = data.data_source === 'public_ws' ? '真实 WebSocket 行情影子交易' : data.data_source === 'public_rest' ? '真实 REST 行情影子交易' : '界面预览数据';
      document.getElementById('subtitle').textContent =
        `${modeText} | 监控 ${data.analytics.monitored_symbols} 个长尾交易对 | 盘口 ${data.analytics.books_loaded} | 资金费率 ${data.analytics.funding_loaded} | 错误 ${data.analytics.market_error_count} | ${data.control.new_entries_allowed ? '影子开仓开启' : '已暂停/熔断'} | ${data.generated_at}`;
      metrics(data.analytics);
      capital(data.capital || []);
      feeSchedule(data.fee_schedule || []);
      positions(filterBySymbolAndExchange(data.current_positions));
      symbols(filterSymbols(data.symbols));
      fills(filterBySymbolAndExchange(data.simulated_fills));
      pnl(data.analytics);
      exchangeAnalysis(data.exchange_analysis);
      feeAnalysis(data.fee_analysis);
      fundingAnalysis(data.funding_analysis);
      bookHealth(data.book_health || []);
      sizingPreview(filterOpportunities(data.opportunities));
      opportunities(filterOpportunities(data.opportunities));
      marketErrors(data.market_errors || []);
      rejects(data.reject_breakdown || []);
      tools(data.tool_commands || []);
    }

    function syncExchangeFilter(data) {
      if (exchangeOptionsReady) return;
      const values = new Set();
      [...(data.exchanges || []), ...(data.exchange_analysis || [])].forEach(r => {
        const value = r.exchange || r;
        if (value) values.add(value);
      });
      ['binance','okx','bitget','gate'].forEach(v => values.add(v));
      const select = document.getElementById('exchangeFilter');
      [...values].sort().forEach(value => {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = value;
        select.appendChild(option);
      });
      exchangeOptionsReady = true;
    }

    function metric(label, value, cls='') {
      return `<div class="metric"><div class="label">${label}</div><div class="value ${cls}">${value}</div></div>`;
    }

    function metrics(a) {
      document.getElementById('metrics').innerHTML = [
        metric('监控交易对', intFmt.format(a.monitored_symbols)),
        metric('可开机会', intFmt.format(a.openable_opportunities), a.openable_opportunities > 0 ? 'ok' : 'warn'),
        metric('影子成交', intFmt.format(a.simulated_trades)),
        metric('总名义金额', money.format(a.total_notional_usdt), 'small'),
        metric('净 PnL', money.format(a.net_pnl_usdt), a.net_pnl_usdt >= 0 ? 'ok' : 'bad'),
        metric('胜率', pct(a.win_rate)),
        metric('平均净边际', pct(a.avg_net_edge_pct), a.avg_net_edge_pct >= 0 ? 'ok' : 'bad'),
        metric('行情错误', intFmt.format(a.market_error_count), a.market_error_count === 0 ? 'ok' : 'warn')
      ].join('');
    }

    function capital(rows) {
      table('capital', ['交易所','本金','杠杆','最大名义','已用名义','剩余名义','仓位','使用率'],
        rows.map(r => [
          r.exchange, money.format(r.equity_usdt), fmt.format(r.leverage) + 'x',
          money.format(r.max_notional_usdt), money.format(r.used_notional_usdt),
          money.format(r.remaining_notional_usdt),
          `${r.open_positions} / ${r.max_positions}`,
          pct(r.usage_pct)
        ]));
    }

    function feeSchedule(rows) {
      table('feeSchedule', ['交易所','Maker 手续费','Taker 手续费','来源/假设'],
        rows.map(r => [r.exchange, pct(r.maker_fee_rate), pct(r.taker_fee_rate), r.source]));
    }

    function positions(rows) {
      table('positions', ['交易对','做多交易所','做空交易所','名义金额','入场价差','当前平仓价差','持仓时间','开平手续费','资金费率','盘口滑点','安全缓冲','预估净 PnL','状态'],
        rows.slice(0, 100).map(r => [
          r.canonical_symbol, r.long_exchange, r.short_exchange, money.format(r.notional_usdt),
          pct(r.entry_edge_pct), r.current_spread_pct === null || r.current_spread_pct === undefined ? '-' : pct(r.current_spread_pct),
          duration(r.holding_secs || 0), money.format(r.expected_fee_usdt), pnlCell(r.expected_funding_usdt),
          money.format(r.book_slippage_cost_usdt || 0), money.format(r.safety_buffer_cost_usdt || 0),
          pnlCell(r.estimated_net_pnl_usdt), `<span class="pill">${r.status}</span>`
        ]));
    }

    function symbols(rows) {
      table('symbols', ['交易对','最佳做多','最佳做空','原始价差','净边际','目标/可执行','状态','拦截原因'],
        rows.slice().sort((a,b)=>b.net_edge_pct-a.net_edge_pct).map(r => [
          r.canonical_symbol, r.best_long_exchange || '-', r.best_short_exchange || '-',
          pct(r.raw_spread_pct), pct(r.net_edge_pct),
          `${money.format(r.target_notional_usdt || 0)} / ${money.format(r.executable_notional_usdt || 0)}`,
          r.can_open ? '<span class="ok">可开</span>' : '<span class="bad">不可开</span>',
          (r.reject_reasons || []).join(', ')
        ]));
    }

    function fills(rows) {
      table('fills', ['交易对','做多','做空','目标/成交名义','持仓时间','入场/退出价差','净 PnL','价差 PnL','手续费','资金费率','盘口滑点','安全缓冲','总滑点/缓冲','成交模型'],
        rows.slice(0, 160).map(r => [
          r.canonical_symbol, r.long_exchange, r.short_exchange,
          `${money.format(r.target_notional_usdt || 0)} / ${money.format(r.executable_notional_usdt || 0)}`,
          duration(Math.floor((r.holding_ms || 0) / 1000)),
          `${pct(r.entry_spread_pct || 0)} / ${pct(r.exit_spread_pct || 0)}`,
          pnlCell(r.net_pnl_usdt), money.format(r.gross_spread_pnl_usdt), money.format(r.fee_pnl_usdt),
          pnlCell(r.funding_pnl_usdt), money.format(r.book_slippage_cost_usdt || 0),
          money.format(r.safety_buffer_cost_usdt || 0), money.format(r.slippage_cost_usdt), r.fill_model
        ]));
    }

    function pnl(a) {
      table('pnl', ['项目','数值'], [
        ['价差毛收益', money.format(a.gross_spread_pnl_usdt)],
        ['手续费影响', money.format(a.fee_pnl_usdt)],
        ['资金费率影响', pnlCell(a.funding_pnl_usdt)],
        ['滑点/安全缓冲合计', money.format(a.slippage_cost_usdt)],
        ['净 PnL', pnlCell(a.net_pnl_usdt)],
        ['最大贡献交易对', a.top_symbol ? `${a.top_symbol} / ${money.format(a.top_symbol_net_pnl_usdt)}` : '-'],
        ['风控拒绝类型数', intFmt.format(a.reject_reason_kinds)]
      ]);
    }

    function exchangeAnalysis(rows) {
      table('exchangeAnalysis', ['交易所','近7天开仓','近7天胜率','多腿次数','空腿次数','名义金额','净 PnL','平均边际'],
        rows.map(r => [r.exchange, r.open_count_7d || 0, pct(r.win_rate_7d || 0), r.long_count, r.short_count,
          money.format(r.simulated_notional_usdt), pnlCell(r.net_pnl_usdt), pct(r.avg_edge_pct)]));
    }

    function feeAnalysis(rows) {
      table('feeAnalysis', ['交易所','预估手续费','名义金额','费用 bps'],
        rows.map(r => [r.exchange, money.format(r.estimated_fee_usdt), money.format(r.simulated_notional_usdt), fmt.format(r.fee_bps)]));
    }

    function fundingAnalysis(rows) {
      table('fundingAnalysis', ['交易所','资金费率 PnL','多腿次数','空腿次数'],
        rows.map(r => [r.exchange, pnlCell(r.funding_pnl_usdt), r.long_count, r.short_count]));
    }

    function opportunities(rows) {
      table('opps', ['交易对','做多','做空','Maker','Taker','目标/可执行','Maker/Taker 数量','Maker/Taker 名义','原始价差','净边际','手续费','资金费率','盘口深度','盘口年龄'],
        rows.slice(0, 140).map(r => [
          r.canonical_symbol, r.long_exchange, r.short_exchange, r.maker_exchange, r.taker_exchange,
          `${money.format(r.target_notional_usdt || 0)} / ${money.format(r.executable_notional_usdt || 0)}`,
          `${qty(r.maker_quantity)} / ${qty(r.taker_quantity)}`,
          `${money.format(r.maker_notional_usdt || 0)} / ${money.format(r.taker_notional_usdt || 0)}`,
          pct(r.raw_open_spread), pct(r.maker_taker_net_edge),
          money.format((r.open_fee_est_usdt || 0) + (r.close_fee_est_usdt || 0)),
          pnlCell(r.expected_funding_usdt || 0),
          money.format(r.depth_notional_usdt || 0), `${r.book_age_ms || 0} ms`
        ]));
    }

    function bookHealth(rows) {
      table('bookHealth', ['交易所','订单簿','可用','陈旧','交叉','无效档位','序列异常','最大年龄'],
        rows.map(r => [
          r.exchange, r.total_books, r.usable_books, r.stale_books, r.crossed_books,
          r.invalid_level_books, r.sequence_gap_books, `${r.max_age_ms || 0} ms`
        ]));
    }

    function sizingPreview(rows) {
      table('sizing', ['交易对','Maker','Taker','目标名义','Maker名义','Taker名义','Maker数量','Taker数量','状态'],
        rows.slice(0, 80).map(r => [
          r.canonical_symbol, r.maker_exchange, r.taker_exchange, money.format(r.target_notional_usdt || 0),
          money.format(r.maker_notional_usdt || 0), money.format(r.taker_notional_usdt || 0),
          qty(r.maker_quantity), qty(r.taker_quantity),
          r.can_open ? '<span class="ok">可开</span>' : (r.reject_reasons || []).join(', ')
        ]));
    }

    function marketErrors(rows) {
      table('marketErrors', ['交易所','交易对','类型','延迟','时间','错误'],
        rows.slice(0, 160).map(r => [
          r.exchange, r.canonical_symbol || '-', r.kind, `${r.latency_ms} ms`, r.occurred_at,
          `<span class="warn">${escapeHtml(r.message || '')}</span>`
        ]));
    }

    function rejects(rows) {
      table('rejects', ['风控拒绝原因','次数'], rows.map(r => [r.reason, r.count]));
    }

    function tools(rows) {
      table('tools', ['工具','范围','命令'], rows.map(r => [r.name, r.scope, `<code>${r.command}</code>`]));
    }

    function filterSymbols(rows) {
      const q = symbolQuery();
      const ex = exchangeQuery();
      const status = openStatus();
      return rows.filter(r => {
        const matchSymbol = !q || String(r.canonical_symbol).includes(q);
        const matchExchange = !ex || r.best_long_exchange === ex || r.best_short_exchange === ex;
        const matchStatus = status === 'open' ? r.can_open : status === 'blocked' ? !r.can_open : true;
        return matchSymbol && matchExchange && matchStatus;
      });
    }

    function filterOpportunities(rows) {
      const q = symbolQuery();
      const ex = exchangeQuery();
      const status = openStatus();
      return rows.filter(r => {
        const matchSymbol = !q || String(r.canonical_symbol).includes(q);
        const matchExchange = !ex || r.long_exchange === ex || r.short_exchange === ex || r.maker_exchange === ex || r.taker_exchange === ex;
        const matchStatus = status === 'open' ? r.can_open : status === 'blocked' ? !r.can_open : true;
        return matchSymbol && matchExchange && matchStatus;
      }).sort((a,b)=>b.maker_taker_net_edge-a.maker_taker_net_edge);
    }

    function filterBySymbolAndExchange(rows) {
      const q = symbolQuery();
      const ex = exchangeQuery();
      return rows.filter(r => {
        const matchSymbol = !q || String(r.canonical_symbol).includes(q);
        const matchExchange = !ex || r.long_exchange === ex || r.short_exchange === ex;
        return matchSymbol && matchExchange;
      });
    }

    function symbolQuery() {
      return document.getElementById('symbolFilter').value.trim().toUpperCase();
    }
    function exchangeQuery() {
      return document.getElementById('exchangeFilter').value;
    }
    function openStatus() {
      return document.getElementById('openFilter').value;
    }
    function table(id, heads, rows) {
      const empty = rows.length === 0 ? `<tr><td colspan="${heads.length}" class="sub">暂无数据</td></tr>` : '';
      document.getElementById(id).innerHTML =
        `<thead><tr>${heads.map(h=>`<th>${h}</th>`).join('')}</tr></thead>` +
        `<tbody>${rows.map(row=>`<tr>${row.map(v=>`<td>${v}</td>`).join('')}</tr>`).join('')}${empty}</tbody>`;
    }
    function pct(v) { return fmt.format((v || 0) * 100) + '%'; }
    function qty(v) { return v === null || v === undefined ? '-' : fmt.format(v); }
    function duration(seconds) {
      seconds = Math.max(0, Number(seconds || 0));
      const h = Math.floor(seconds / 3600);
      const m = Math.floor((seconds % 3600) / 60);
      const s = Math.floor(seconds % 60);
      if (h > 0) return `${h}h ${m}m`;
      if (m > 0) return `${m}m ${s}s`;
      return `${s}s`;
    }
    function pnlCell(v) { return `<span class="${v >= 0 ? 'ok' : 'bad'}">${money.format(v || 0)}</span>`; }
    function escapeHtml(value) {
      return value.replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    }
    async function post(url) { await fetch(url, { method: 'POST' }); await load(); }
    load();
    setInterval(load, 3000);
  </script>
</body>
</html>
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use clap::CommandFactory;

    fn test_app_state(
        config_path: impl AsRef<Path>,
        symbol_count: usize,
        data_source: DataSource,
    ) -> AppState {
        AppState::new(
            config_path,
            symbol_count,
            data_source,
            std::env::temp_dir().join(format!("rustcta-cross-arb-test-{}", uuid::Uuid::new_v4())),
        )
        .expect("test app state should load config")
    }

    fn test_config_path() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "rustcta-cross-arb-config-{}.yml",
            uuid::Uuid::new_v4()
        ));
        let config = CrossExchangeArbitrageConfig::default();
        std::fs::write(
            &path,
            serde_yaml::to_string(&config).expect("config should serialize"),
        )
        .expect("config should be written");
        path
    }

    #[test]
    fn cross_arb_server_router_should_build() {
        let config_path = test_config_path();
        let state = test_app_state(&config_path, 100, DataSource::Synthetic);
        let _router = build_router(state);
    }

    #[test]
    fn cross_arb_server_should_seed_one_hundred_symbols() {
        let config_path = test_config_path();
        let state = test_app_state(&config_path, 100, DataSource::Synthetic);
        let read_model = state.read_model.blocking_read();

        assert_eq!(read_model.analytics.monitored_symbols, 100);
        assert_eq!(read_model.symbols.len(), 100);
        assert!(!read_model.shadow_positions.is_empty());
        assert!(read_model.simulated_fills.is_empty());
    }

    #[test]
    fn cross_arb_server_symbol_universe_should_exclude_mainstream_pairs() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.universe.symbols = vec![
            CanonicalSymbol::new("BTC", "USDT"),
            CanonicalSymbol::new("ETH", "USDT"),
            CanonicalSymbol::new("SOL", "USDT"),
            CanonicalSymbol::new("ARB", "USDT"),
            CanonicalSymbol::new("ARB", "USDT"),
            CanonicalSymbol::new("OP", "USDT"),
            CanonicalSymbol::new("PEPE", "USDC"),
        ];

        let symbols = build_symbol_universe(&config, 8);

        assert!(symbols.contains(&CanonicalSymbol::new("ARB", "USDT")));
        assert!(symbols.contains(&CanonicalSymbol::new("OP", "USDT")));
        assert!(symbols.iter().all(|symbol| symbol.quote() == "USDT"));
        assert!(symbols
            .iter()
            .all(|symbol| !MAINSTREAM_BASES.contains(&symbol.base())));
        assert_eq!(
            symbols
                .iter()
                .filter(|symbol| symbol.base() == "ARB")
                .count(),
            1
        );
    }

    #[test]
    fn cross_arb_server_data_source_cli_should_build_for_current_variants() {
        let _command = Args::command();

        let public_rest =
            Args::try_parse_from(["cross_arb_server", "--data-source", "public-rest"])
                .expect("public-rest data source should parse");
        assert_eq!(public_rest.data_source, DataSource::PublicRest);

        let public_ws = Args::try_parse_from(["cross_arb_server", "--data-source", "public-ws"])
            .expect("public-ws data source should parse");
        assert_eq!(public_ws.data_source, DataSource::PublicWs);

        let synthetic = Args::try_parse_from(["cross_arb_server", "--data-source", "synthetic"])
            .expect("synthetic data source should parse");
        assert_eq!(synthetic.data_source, DataSource::Synthetic);
    }

    #[test]
    fn cross_arb_server_public_ws_data_source_cli_should_parse_when_available() {
        let args = Args::try_parse_from(["cross_arb_server", "--data-source", "public-ws"])
            .expect("public-ws data source should parse once the variant lands");
        let parsed_value = args
            .data_source
            .to_possible_value()
            .expect("data source should expose a clap possible value");

        assert_eq!(parsed_value.get_name(), "public-ws");
    }

    #[test]
    fn cross_arb_server_capital_model_should_use_500u_10x_and_50_positions() {
        let config = CrossExchangeArbitrageConfig::default();
        let rows = exchange_capital_models(&config, &HashMap::new());

        assert!(rows.iter().all(|row| row.equity_usdt == 500.0));
        assert!(rows.iter().all(|row| row.leverage == 10.0));
        assert!(rows.iter().all(|row| row.max_notional_usdt == 5_000.0));
        assert!(rows.iter().all(|row| row.max_positions == 50));
    }

    #[test]
    fn cross_arb_server_fee_schedule_should_match_public_vip0_baseline() {
        let config = CrossExchangeArbitrageConfig::default();
        let fees = fee_schedule_models(&config)
            .into_iter()
            .map(|row| (row.exchange, row.maker_fee_rate, row.taker_fee_rate))
            .collect::<Vec<_>>();

        assert!(fees.contains(&(ExchangeId::Binance, 0.0002, 0.0005)));
        assert!(fees.contains(&(ExchangeId::Okx, 0.0002, 0.0005)));
        assert!(fees.contains(&(ExchangeId::Bitget, 0.0002, 0.0006)));
        assert!(fees.contains(&(ExchangeId::Gate, 0.0002, 0.0005)));
    }

    #[test]
    fn cross_arb_server_dashboard_should_force_simulation_without_live_orders() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        config.execution.dry_run = false;
        config.universe.symbols = vec![CanonicalSymbol::new("ARB", "USDT")];

        let dir = tempfile::tempdir().expect("tempdir should be created");
        let config_path = dir.path().join("cross-arb-live.yml");
        std::fs::write(
            &config_path,
            serde_yaml::to_string(&config).expect("config should serialize"),
        )
        .expect("config should be written");

        let state = test_app_state(&config_path, 4, DataSource::Synthetic);
        let read_model = state.read_model.blocking_read();

        assert_eq!(
            read_model.config_summary.configured_mode,
            Some(RuntimeMode::LiveSmall)
        );
        assert_eq!(read_model.config.mode, RuntimeMode::Simulation);
        assert!(read_model.config.execution.dry_run);
        assert_eq!(read_model.status.mode, RuntimeMode::Simulation);
        assert_eq!(read_model.control.mode, RuntimeMode::Simulation);
        assert!(!read_model.control.live_orders_enabled);
    }

    #[tokio::test]
    async fn cross_arb_server_control_should_pause_and_resume_new_entries() {
        let config_path = test_config_path();
        let state = test_app_state(&config_path, 100, DataSource::Synthetic);

        let Json(paused) = pause_new_entries(State(state.clone())).await;
        assert!(paused.new_entries_paused);
        assert!(!paused.new_entries_allowed);
        assert!(!paused.live_orders_enabled);

        let Json(resumed) = resume_new_entries(State(state)).await;
        assert!(!resumed.new_entries_paused);
        assert!(resumed.new_entries_allowed);
        assert!(!resumed.live_orders_enabled);
    }

    #[tokio::test]
    async fn cross_arb_server_kill_switch_should_block_entries_without_live_orders() {
        let config_path = test_config_path();
        let state = test_app_state(&config_path, 100, DataSource::Synthetic);

        let Json(control) = kill_switch(State(state)).await;
        assert!(control.kill_switch);
        assert!(control.close_only);
        assert!(control.new_entries_paused);
        assert!(!control.new_entries_allowed);
        assert!(!control.live_orders_enabled);
    }
}

#![recursion_limit = "256"]

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{
    AccountPermission, ExecutionCancelCommand, ExecutionIntent, ExecutionOrderAck,
    ExecutionOrderCommand, MarketDataChannel, MarketDataSubscription, MarketType,
    OrderSide as SdkOrderSide, OrderType, RequiredAccountPermission, RiskCapability,
    RiskCapabilityDeclaration, SdkResult, StrategyCommandSchema, StrategyConfigSchema,
    StrategyContext, StrategyEvent, StrategyInstanceId, StrategyRuntime, StrategySnapshot,
    StrategySnapshotSchema, StrategySpec, StrategyStatus, TimeInForce,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub mod app_runtime;
pub mod core;
pub mod runtime_contract;

pub use app_runtime::{
    CrossArbAppRuntime, CrossArbExecutionIntentSummary, CrossArbNotification, CrossArbRuntimeCycle,
    CrossArbRuntimeInput, CrossArbStorageEvent,
};
pub use core::{
    evaluate_dual_taker_close, evaluate_dual_taker_open_opportunities,
    evaluate_dual_taker_open_opportunities_with_audit,
    evaluate_ready_dual_taker_open_opportunities, evaluate_slippage_capture_open_opportunities,
    filter_open_opportunities_by_risk, inspect_single_leg_net_positions,
    plan_startup_usdt_position_takeover, select_high_volatility_symbols, ArbitrageRiskState,
    CanonicalSymbol, CloseReason, CrossArbExecutionModule, DualTakerArbitrageConfig,
    DualTakerCloseEvaluation, DualTakerFeeEstimate, DualTakerOpenOpportunity, ExchangeFeeRates,
    ExchangeId, ExchangeRuntimeStatus, ExchangeStartupReadiness, ExchangeStatusRegistry,
    FeeBreakdown, FeeModel, FeeRole, FillInferenceType, FundingEstimate, FundingModel,
    FundingSettlementLedger, MakerLegKind, NetPosition, NetPositionWarning, OpenArbitragePosition,
    OpenBlockReason, OpenOpportunityAudit, OpenOpportunityAuditReport, OpenOpportunityDecision,
    OpenOpportunityRejectReason, OrderBookTop, OrderSide, PairedTakerFillState, PositionSide,
    PrecisionRegistry, QuantityUnit, SimulatedBundleState, SimulatedBundleStatus, SingleLegGuard,
    SlippageCaptureArbitrageConfig, SlippageCaptureHedgePlan, SlippageCaptureMakerOrderDraft,
    SlippageCaptureOpenOpportunity, SlippageCaptureOrderRole, SlippageCaptureStartupGate,
    StartupPositionTakeoverPlan, StartupReadiness, StartupSingleLegResolution, StartupUsdtPosition,
    StrategyLogEventKind, StrategyLogRotationConfig, StrategyRoute, SymbolPrecision,
    TakerFillAudit, TakerOrderDraft, TakerOrderRole, VolatilityRankDirection, VolatilityRankTicker,
    VolatilityUniverseConfig,
};
pub use runtime_contract::{
    build_runtime_contract, default_runtime_contract, CrossArbDashboardSnapshot,
    CrossArbDashboardSnapshotProvider, CrossArbExecutionProvider, CrossArbMarketDataProvider,
    CrossArbMarketSnapshotRow, CrossArbNotificationProvider, CrossArbOpportunityRow,
    CrossArbRouteHealthRow, CrossArbRuntimeContract, CrossArbRuntimeMode, CrossArbStorageProvider,
    RuntimeProviderContract, RuntimeTaskContract,
};

pub const STRATEGY_KIND: &str = "cross_exchange_arbitrage";
pub const DISPLAY_NAME: &str = "Cross Exchange Arbitrage";
pub const MIGRATED_FROM: &str = "legacy-strategy:cross_exchange_arbitrage";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossExchangeArbitrageStrategyInfo {
    pub strategy_kind: String,
    pub migrated_from: String,
    pub initialized_at: DateTime<Utc>,
}

impl Default for CrossExchangeArbitrageStrategyInfo {
    fn default() -> Self {
        Self {
            strategy_kind: STRATEGY_KIND.to_string(),
            migrated_from: MIGRATED_FROM.to_string(),
            initialized_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrossExchangeArbitrageConfig {
    #[serde(default = "default_cross_arb_venues")]
    pub venues: Vec<String>,
    #[serde(default = "default_cross_arb_symbols")]
    pub symbols: Vec<String>,
    #[serde(default = "default_excluded_bases")]
    pub excluded_bases: Vec<String>,
    #[serde(default)]
    pub excluded_symbols: Vec<String>,
    #[serde(default = "default_min_profit_bps")]
    pub min_profit_bps: f64,
    #[serde(default = "default_max_position_notional_quote")]
    pub max_position_notional_quote: String,
    #[serde(default = "default_market_type")]
    pub market_type: MarketType,
    #[serde(default)]
    pub dual_taker: DualTakerArbitrageConfig,
    #[serde(default)]
    pub execution_module: CrossArbExecutionModule,
    #[serde(default)]
    pub slippage_capture: SlippageCaptureArbitrageConfig,
    #[serde(default)]
    pub logging: StrategyLogRotationConfig,
    #[serde(default)]
    pub volatility_universe: VolatilityUniverseConfig,
    #[serde(default = "default_max_consecutive_losses")]
    pub max_consecutive_losses: u32,
    #[serde(default)]
    pub dry_run: bool,
}

impl Default for CrossExchangeArbitrageConfig {
    fn default() -> Self {
        Self {
            venues: default_cross_arb_venues(),
            symbols: default_cross_arb_symbols(),
            excluded_bases: default_excluded_bases(),
            excluded_symbols: Vec::new(),
            min_profit_bps: default_min_profit_bps(),
            max_position_notional_quote: default_max_position_notional_quote(),
            market_type: default_market_type(),
            dual_taker: DualTakerArbitrageConfig::default(),
            execution_module: CrossArbExecutionModule::default(),
            slippage_capture: SlippageCaptureArbitrageConfig::default(),
            logging: StrategyLogRotationConfig::default(),
            volatility_universe: VolatilityUniverseConfig::default(),
            max_consecutive_losses: default_max_consecutive_losses(),
            dry_run: true,
        }
    }
}

impl CrossExchangeArbitrageConfig {
    pub fn from_runtime_value(value: &Value) -> Self {
        let mut config = serde_json::from_value(value.clone())
            .unwrap_or_else(|_| CrossExchangeArbitrageConfig::default());

        if let Some(venues) = first_non_empty_strings(
            value,
            &[
                &["venues"],
                &["enabled_exchanges"],
                &["universe", "enabled_exchanges"],
                &["detection", "exchanges"],
            ],
        ) {
            config.venues = venues;
        } else if let Some(venues) = enabled_exchange_keys(value) {
            config.venues = venues;
        }
        if let Some(symbols) = first_non_empty_strings(
            value,
            &[
                &["symbols"],
                &["enabled_symbols"],
                &["universe", "symbols"],
                &["detection", "symbols"],
            ],
        ) {
            config.symbols = symbols;
        }
        if let Some(excluded_bases) = first_non_empty_strings(
            value,
            &[&["excluded_bases"], &["universe", "excluded_bases"]],
        ) {
            config.excluded_bases = excluded_bases;
        }
        if let Some(excluded_symbols) = first_non_empty_strings(
            value,
            &[&["excluded_symbols"], &["universe", "exclude_symbols"]],
        ) {
            config.excluded_symbols = excluded_symbols;
        }
        if let Some(market_type) =
            text_at(value, &["market_type"]).or_else(|| text_at(value, &["market", "market_type"]))
        {
            config.market_type = parse_market_type(&market_type);
        }
        if let Some(dry_run) =
            bool_at(value, &["dry_run"]).or_else(|| bool_at(value, &["execution", "dry_run"]))
        {
            config.dry_run = dry_run;
        } else if bool_at(value, &["enable_live_trading"]).is_some_and(|enabled| enabled) {
            config.dry_run = false;
        }
        if let Some(module) = text_at(value, &["execution_module"])
            .or_else(|| text_at(value, &["execution", "module"]))
            .or_else(|| text_at(value, &["execution", "open_module"]))
        {
            config.execution_module = parse_execution_module(&module);
        }

        let configured_target_notional = f64_at(value, &["max_live_notional_per_trade"])
            .or_else(|| f64_at(value, &["sizing", "target_notional_usdt"]))
            .or_else(|| parse_f64(&config.max_position_notional_quote));

        if let Some(value) = configured_target_notional {
            config.dual_taker.target_notional_usdt = value;
            config.slippage_capture.target_notional_usdt = value;
            config.max_position_notional_quote = trim_float(value);
        }
        if let Some(value) = f64_at(value, &["thresholds", "min_open_raw_spread"]) {
            config.dual_taker.min_open_spread_pct = value;
            config.slippage_capture.min_open_spread_pct = value;
            config.min_profit_bps = value * 10_000.0;
        } else if config.min_profit_bps > 0.0 {
            config.dual_taker.min_open_spread_pct = config.min_profit_bps / 10_000.0;
            config.slippage_capture.min_open_spread_pct = config.min_profit_bps / 10_000.0;
        }
        if let Some(value) = f64_at(value, &["thresholds", "min_open_maker_taker_net_edge"])
            .or_else(|| f64_at(value, &["dual_taker", "min_open_net_profit_pct"]))
            .or_else(|| f64_at(value, &["slippage_capture", "min_open_net_profit_pct"]))
        {
            config.dual_taker.min_open_net_profit_pct = value;
            config.slippage_capture.min_open_net_profit_pct = value;
        }
        if let Some(value) = f64_at(value, &["thresholds", "max_open_raw_spread"]) {
            config.dual_taker.max_open_spread_pct = value;
            config.slippage_capture.max_open_spread_pct = value;
        }
        if let Some(value) = f64_at(value, &["execution", "taker_ioc_slippage_limit_pct"])
            .or_else(|| f64_at(value, &["risk", "max_taker_slippage_pct"]))
        {
            config.dual_taker.taker_slippage_pct = value;
            config.slippage_capture.hedge_taker_slippage_pct = value;
            config.slippage_capture.close_taker_slippage_pct = value;
        }
        if let Some(value) = f64_at(value, &["thresholds", "close_min_net_profit_pct"])
            .or_else(|| f64_at(value, &["thresholds", "lock_profit_dual_taker_pct"]))
        {
            config.dual_taker.close_min_net_profit_pct = value;
            config.slippage_capture.close_min_net_profit_pct = value;
        }
        if let Some(value) = f64_at(value, &["dual_taker", "expected_close_spread_pct"])
            .or_else(|| f64_at(value, &["thresholds", "expected_close_spread_pct"]))
        {
            config.dual_taker.expected_close_spread_pct = value.max(0.0);
        }
        if let Some(value) = f64_at(value, &["dual_taker", "top_of_book_capacity_ratio"])
            .or_else(|| f64_at(value, &["market", "top_of_book_capacity_ratio"]))
        {
            config.dual_taker.top_of_book_capacity_ratio = value.clamp(0.0, 1.0);
            config.slippage_capture.hedge_top_of_book_capacity_ratio = value.clamp(0.0, 1.0);
        }
        if let Some(value) = bool_at(value, &["execution", "enforce_top_depth_on_open"])
            .or_else(|| bool_at(value, &["execution_quality", "enforce_top_depth_on_open"]))
            .or_else(|| bool_at(value, &["dual_taker", "enforce_top_depth_on_open"]))
        {
            config.dual_taker.enforce_top_depth_on_open = value;
            config.slippage_capture.enforce_hedge_top_depth = value;
        }
        if let Some(value) = u64_at(value, &["risk", "max_book_age_ms"])
            .or_else(|| u64_at(value, &["market", "stale_quote_ms"]))
            .or_else(|| u64_at(value, &["detection", "stale_book_ms"]))
        {
            config.dual_taker.orderbook_stale_ms = value;
            config.slippage_capture.orderbook_stale_ms = value;
        }
        if let Some(value) = usize_at(value, &["market", "depth_levels"])
            .or_else(|| usize_at(value, &["market", "public_book_depth"]))
            .or_else(|| usize_at(value, &["detection", "depth_levels"]))
        {
            config.dual_taker.min_orderbook_levels = value.max(1);
            config.slippage_capture.min_orderbook_levels = value.max(1);
        }
        if let Some(value) = u64_at(value, &["execution", "single_leg_timeout_ms"]) {
            config.dual_taker.single_leg_timeout_ms = value;
            config.slippage_capture.maker_order_timeout_ms = value;
        }
        if let Some(value) = u32_at(value, &["risk", "max_consecutive_single_leg_fills"]) {
            config.dual_taker.max_consecutive_single_leg_fills = value.max(1);
        }
        if let Some(value) = u32_at(value, &["risk", "max_consecutive_losses"])
            .or_else(|| u32_at(value, &["risk", "max_consecutive_losing_trades"]))
            .or_else(|| u32_at(value, &["max_consecutive_losses"]))
        {
            config.max_consecutive_losses = value.max(1);
        }
        if let Some(value) = usize_at(value, &["sizing", "max_positions_per_exchange"]) {
            config.dual_taker.max_positions_per_exchange = value.max(1);
            config.slippage_capture.max_positions_per_exchange = value.max(1);
        }
        if let Some(value) = usize_at(value, &["risk", "max_open_bundles"])
            .or_else(|| usize_at(value, &["dual_taker", "max_open_bundles"]))
        {
            config.dual_taker.max_open_bundles = value.max(1);
            config.slippage_capture.max_open_bundles = value.max(1);
        }
        if let Some(value) = i64_at(value, &["risk", "symbol_cooldown_after_close_secs"]) {
            config.dual_taker.symbol_cooldown_secs = value.max(0);
            config.slippage_capture.symbol_cooldown_secs = value.max(0);
        }
        if let Some(value) = i64_at(value, &["risk", "max_hold_seconds"]) {
            config.dual_taker.max_hold_secs = value.max(1);
            config.slippage_capture.max_hold_secs = value.max(1);
        }
        if let Some(value) = bool_at(value, &["dual_taker", "close_on_max_hold_requires_profit"])
            .or_else(|| bool_at(value, &["risk", "close_on_max_hold_requires_profit"]))
        {
            config.dual_taker.close_on_max_hold_requires_profit = value;
            config.slippage_capture.close_on_max_hold_requires_profit = value;
        }
        config.apply_slippage_capture_runtime_value(value);
        if let Some(value) = u64_at(value, &["logging", "max_file_bytes"]) {
            config.logging.max_file_bytes = value;
        }
        if let Some(value) = usize_at(value, &["logging", "retained_files"]) {
            config.logging.retained_files = value;
        }
        if let Some(value) = bool_at(value, &["logging", "persist_only_key_events"]) {
            config.logging.persist_only_key_events = value;
        }
        config.apply_volatility_runtime_value(value);

        config.symbols = config.active_symbols();
        config
    }

    pub fn active_venues(&self) -> Vec<String> {
        self.venues
            .iter()
            .filter_map(|venue| {
                let venue = venue.trim().to_ascii_lowercase();
                (!venue.is_empty()).then_some(venue)
            })
            .fold(Vec::new(), |mut venues, venue| {
                if !venues.contains(&venue) {
                    venues.push(venue);
                }
                venues
            })
    }

    pub fn active_symbols(&self) -> Vec<String> {
        let excluded_bases: Vec<_> = self
            .excluded_bases
            .iter()
            .map(|base| base.trim().to_ascii_uppercase())
            .collect();
        let excluded_symbols: Vec<_> = self
            .excluded_symbols
            .iter()
            .map(|symbol| normalize_symbol(symbol))
            .collect();

        self.symbols
            .iter()
            .filter_map(|symbol| {
                let normalized = normalize_symbol(symbol);
                let base = symbol_base(&normalized);
                (!normalized.is_empty()
                    && !excluded_bases.contains(&base)
                    && !excluded_symbols.contains(&normalized))
                .then_some(normalized)
            })
            .fold(Vec::new(), |mut symbols, symbol| {
                if !symbols.contains(&symbol) {
                    symbols.push(symbol);
                }
                symbols
            })
    }

    pub fn active_symbols_with_high_volatility(
        &self,
        tickers: &[VolatilityRankTicker],
    ) -> Vec<String> {
        let mut symbols = self.active_symbols();
        let dynamic_symbols = select_high_volatility_symbols(
            tickers,
            &self.volatility_universe,
            &self.excluded_bases,
            &self.excluded_symbols,
        );
        for symbol in dynamic_symbols {
            if !symbols.contains(&symbol) {
                symbols.push(symbol);
            }
        }
        symbols
    }

    fn apply_volatility_runtime_value(&mut self, value: &Value) {
        if let Some(enabled) = bool_at(value, &["volatility_universe", "enabled"])
            .or_else(|| bool_at(value, &["universe", "high_volatility", "enabled"]))
        {
            self.volatility_universe.enabled = enabled;
        }
        if let Some(value) = usize_at(value, &["volatility_universe", "top_gainers_per_exchange"])
            .or_else(|| {
                usize_at(
                    value,
                    &["universe", "high_volatility", "top_gainers_per_exchange"],
                )
            })
        {
            self.volatility_universe.top_gainers_per_exchange = value;
        }
        if let Some(value) = usize_at(value, &["volatility_universe", "top_losers_per_exchange"])
            .or_else(|| {
                usize_at(
                    value,
                    &["universe", "high_volatility", "top_losers_per_exchange"],
                )
            })
        {
            self.volatility_universe.top_losers_per_exchange = value;
        }
        if let Some(value) =
            f64_at(value, &["volatility_universe", "min_abs_change_pct"]).or_else(|| {
                f64_at(
                    value,
                    &["universe", "high_volatility", "min_abs_change_pct"],
                )
            })
        {
            self.volatility_universe.min_abs_change_pct = value.max(0.0);
        }
        if let Some(value) = f64_at(value, &["volatility_universe", "min_quote_volume_usdt"])
            .or_else(|| {
                f64_at(
                    value,
                    &["universe", "high_volatility", "min_quote_volume_usdt"],
                )
            })
        {
            self.volatility_universe.min_quote_volume_usdt = value.max(0.0);
        }
        if let Some(value) = u64_at(value, &["volatility_universe", "refresh_secs"])
            .or_else(|| u64_at(value, &["universe", "high_volatility", "refresh_secs"]))
        {
            self.volatility_universe.refresh_secs = value.max(1);
        }
        if let Some(value) = usize_at(value, &["volatility_universe", "max_dynamic_symbols"])
            .or_else(|| {
                usize_at(
                    value,
                    &["universe", "high_volatility", "max_dynamic_symbols"],
                )
            })
        {
            self.volatility_universe.max_dynamic_symbols = value;
        }
        if let Some(value) = bool_at(value, &["volatility_universe", "monitor_orderbook"])
            .or_else(|| bool_at(value, &["universe", "high_volatility", "monitor_orderbook"]))
        {
            self.volatility_universe.monitor_orderbook = value;
        }
    }

    fn apply_slippage_capture_runtime_value(&mut self, value: &Value) {
        if let Some(value) = f64_at(value, &["slippage_capture", "target_notional_usdt"]) {
            self.slippage_capture.target_notional_usdt = value.max(0.0);
        }
        if let Some(value) = f64_at(value, &["slippage_capture", "min_open_spread_pct"]) {
            self.slippage_capture.min_open_spread_pct = value.max(0.0);
        }
        if let Some(value) = f64_at(value, &["slippage_capture", "min_open_net_profit_pct"]) {
            self.slippage_capture.min_open_net_profit_pct = value.max(0.0);
        }
        if let Some(value) = f64_at(value, &["slippage_capture", "max_open_spread_pct"]) {
            self.slippage_capture.max_open_spread_pct = value.max(0.0);
        }
        if let Some(value) = i64_at(value, &["slippage_capture", "startup_skip_spread_secs"])
            .or_else(|| i64_at(value, &["execution", "startup_skip_spread_secs"]))
        {
            self.slippage_capture.startup_skip_spread_secs = value.max(0);
        }
        if let Some(value) = u64_at(value, &["slippage_capture", "max_signal_age_ms"])
            .or_else(|| u64_at(value, &["execution", "max_signal_age_ms"]))
        {
            self.slippage_capture.max_signal_age_ms = value.max(1);
        }
        if let Some(value) = f64_at(value, &["slippage_capture", "maker_price_offset_pct"])
            .or_else(|| f64_at(value, &["execution", "maker_price_offset_pct"]))
            .or_else(|| f64_at(value, &["execution", "slippage_capture_offset_pct"]))
        {
            self.slippage_capture.maker_price_offset_pct = value.max(0.0);
        }
        if let Some(value) = f64_at(value, &["slippage_capture", "hedge_taker_slippage_pct"]) {
            self.slippage_capture.hedge_taker_slippage_pct = value.max(0.0);
        }
        if let Some(value) = f64_at(value, &["slippage_capture", "close_taker_slippage_pct"]) {
            self.slippage_capture.close_taker_slippage_pct = value.max(0.0);
        }
        if let Some(value) = f64_at(value, &["slippage_capture", "close_min_net_profit_pct"]) {
            self.slippage_capture.close_min_net_profit_pct = value.max(0.0);
        }
        if let Some(value) = i64_at(value, &["slippage_capture", "max_hold_secs"]) {
            self.slippage_capture.max_hold_secs = value.max(1);
        }
        if let Some(value) = bool_at(
            value,
            &["slippage_capture", "close_on_max_hold_requires_profit"],
        ) {
            self.slippage_capture.close_on_max_hold_requires_profit = value;
        }
        if let Some(value) = u64_at(value, &["slippage_capture", "maker_order_timeout_ms"])
            .or_else(|| u64_at(value, &["execution", "maker_order_timeout_ms"]))
            .or_else(|| u64_at(value, &["execution", "maker_auto_cancel_ms"]))
        {
            self.slippage_capture.maker_order_timeout_ms = value.max(1);
        }
        if let Some(value) = usize_at(value, &["slippage_capture", "max_concurrent_maker_orders"])
            .or_else(|| usize_at(value, &["execution", "max_concurrent_maker_orders"]))
        {
            self.slippage_capture.max_concurrent_maker_orders = value.max(1);
        }
        if let Some(value) = bool_at(value, &["slippage_capture", "cancel_unfilled_maker"]) {
            self.slippage_capture.cancel_unfilled_maker = value;
        }
        if let Some(value) = f64_at(value, &["slippage_capture", "max_maker_top_depth_usdt"])
            .or_else(|| f64_at(value, &["execution", "max_maker_top_depth_usdt"]))
        {
            self.slippage_capture.max_maker_top_depth_usdt = value.max(0.0);
        }
        if let Some(value) = f64_at(
            value,
            &["slippage_capture", "hedge_top_of_book_capacity_ratio"],
        ) {
            self.slippage_capture.hedge_top_of_book_capacity_ratio = value.clamp(0.0, 1.0);
        }
        if let Some(value) = bool_at(value, &["slippage_capture", "enforce_hedge_top_depth"]) {
            self.slippage_capture.enforce_hedge_top_depth = value;
        }
        if let Some(value) = u64_at(value, &["slippage_capture", "orderbook_stale_ms"]) {
            self.slippage_capture.orderbook_stale_ms = value.max(1);
        }
        if let Some(value) = usize_at(value, &["slippage_capture", "min_orderbook_levels"]) {
            self.slippage_capture.min_orderbook_levels = value.max(1);
        }
        if let Some(value) = usize_at(value, &["slippage_capture", "max_open_bundles"]) {
            self.slippage_capture.max_open_bundles = value.max(1);
        }
        if let Some(value) = usize_at(value, &["slippage_capture", "max_positions_per_exchange"]) {
            self.slippage_capture.max_positions_per_exchange = value.max(1);
        }
        if let Some(value) = usize_at(
            value,
            &["slippage_capture", "max_active_bundles_per_symbol"],
        ) {
            self.slippage_capture.max_active_bundles_per_symbol = value.max(1);
        }
        if let Some(value) = i64_at(value, &["slippage_capture", "symbol_cooldown_secs"]) {
            self.slippage_capture.symbol_cooldown_secs = value.max(0);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossExchangeArbitrageSnapshotPayload {
    pub migrated_from: String,
    pub handled_events: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
    pub configured_venues: Vec<String>,
    pub configured_symbols: usize,
    pub execution_module: String,
    pub excluded_bases: Vec<String>,
    pub target_notional_usdt: String,
    pub min_open_spread_pct: String,
    pub min_open_net_profit_pct: String,
    pub max_open_spread_pct: String,
    pub close_min_net_profit_pct: String,
    pub slippage_capture_maker_price_offset_pct: String,
    pub slippage_capture_maker_order_timeout_ms: u64,
    pub slippage_capture_startup_skip_spread_secs: i64,
    pub orderbook_stale_ms: u64,
    pub high_volatility_universe_enabled: bool,
    pub high_volatility_refresh_secs: u64,
    pub max_consecutive_losses: u32,
}

#[derive(Debug, Clone)]
pub struct CrossExchangeArbitrageRuntime {
    instance_id: StrategyInstanceId,
    strategy_id: String,
    run_id: String,
    status: StrategyStatus,
    started_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,
    handled_events: u64,
    config: CrossExchangeArbitrageConfig,
}

impl CrossExchangeArbitrageRuntime {
    pub fn new() -> Self {
        Self {
            instance_id: StrategyInstanceId::new("unstarted"),
            strategy_id: STRATEGY_KIND.to_string(),
            run_id: "unstarted".to_string(),
            status: StrategyStatus::Stopped,
            started_at: None,
            last_event_at: None,
            handled_events: 0,
            config: CrossExchangeArbitrageConfig::default(),
        }
    }

    pub fn reload_config_value(&mut self, value: &Value) {
        self.config = CrossExchangeArbitrageConfig::from_runtime_value(value);
        self.handled_events += 1;
        self.last_event_at = Some(Utc::now());
    }

    fn snapshot_payload(&self) -> CrossExchangeArbitrageSnapshotPayload {
        CrossExchangeArbitrageSnapshotPayload {
            migrated_from: MIGRATED_FROM.to_string(),
            handled_events: self.handled_events,
            started_at: self.started_at,
            last_event_at: self.last_event_at,
            configured_venues: self.config.active_venues(),
            configured_symbols: self.config.active_symbols().len(),
            execution_module: self.config.execution_module.as_str().to_string(),
            excluded_bases: self.config.excluded_bases.clone(),
            target_notional_usdt: trim_float(self.config.dual_taker.target_notional_usdt),
            min_open_spread_pct: trim_float(self.config.dual_taker.min_open_spread_pct),
            min_open_net_profit_pct: trim_float(self.config.dual_taker.min_open_net_profit_pct),
            max_open_spread_pct: trim_float(self.config.dual_taker.max_open_spread_pct),
            close_min_net_profit_pct: trim_float(self.config.dual_taker.close_min_net_profit_pct),
            slippage_capture_maker_price_offset_pct: trim_float(
                self.config.slippage_capture.maker_price_offset_pct,
            ),
            slippage_capture_maker_order_timeout_ms: self
                .config
                .slippage_capture
                .maker_order_timeout_ms,
            slippage_capture_startup_skip_spread_secs: self
                .config
                .slippage_capture
                .startup_skip_spread_secs,
            orderbook_stale_ms: self.config.dual_taker.orderbook_stale_ms,
            high_volatility_universe_enabled: self.config.volatility_universe.enabled,
            high_volatility_refresh_secs: self.config.volatility_universe.refresh_secs,
            max_consecutive_losses: self.config.max_consecutive_losses,
        }
    }
}

impl Default for CrossExchangeArbitrageRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyRuntime for CrossExchangeArbitrageRuntime {
    fn spec(&self) -> StrategySpec {
        strategy_spec()
    }

    async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()> {
        self.instance_id = ctx.instance_id().clone();
        self.strategy_id = ctx.strategy_id().to_string();
        self.run_id = ctx.run_id().to_string();
        self.started_at = Some(ctx.started_at());
        self.config = CrossExchangeArbitrageConfig::from_runtime_value(ctx.config());
        self.status = StrategyStatus::Running;
        Ok(())
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        self.status = StrategyStatus::Stopped;
        Ok(())
    }

    async fn handle_event(&mut self, event: StrategyEvent) -> anyhow::Result<()> {
        self.handled_events += 1;
        self.last_event_at = Some(event_timestamp(&event));
        if matches!(event, StrategyEvent::Stopping(_)) {
            self.status = StrategyStatus::Stopping;
        }
        Ok(())
    }

    async fn snapshot(&self) -> anyhow::Result<StrategySnapshot> {
        Ok(StrategySnapshot {
            schema_version: 1,
            instance_id: self.instance_id.clone(),
            strategy_kind: STRATEGY_KIND.to_string(),
            strategy_id: self.strategy_id.clone(),
            run_id: self.run_id.clone(),
            captured_at: Utc::now(),
            status: self.status.clone(),
            payload: serde_json::to_value(self.snapshot_payload())?,
            health: Vec::new(),
        })
    }
}

pub fn strategy_spec() -> StrategySpec {
    StrategySpec {
        schema_version: 1,
        strategy_kind: STRATEGY_KIND.to_string(),
        display_name: DISPLAY_NAME.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        description: Some(
            "Cross-exchange arbitrage strategy runtime contract with adapter-free domain core."
                .to_string(),
        ),
        config_schema: config_schema(),
        snapshot_schema: snapshot_schema(),
        supported_commands: runtime_command_schemas(),
        risk_capabilities: vec![
            risk_capability(
                RiskCapability::PlaceOrders,
                "Places cross-venue arbitrage orders",
            ),
            risk_capability(
                RiskCapability::CancelOrders,
                "Cancels stale arbitrage orders",
            ),
            risk_capability(
                RiskCapability::CrossAccountRead,
                "Reads inventory and order state across venues",
            ),
            risk_capability(
                RiskCapability::InventoryReservation,
                "Reserves inventory for paired execution",
            ),
        ],
        market_data_subscriptions: cross_exchange_market_data_subscriptions(
            &CrossExchangeArbitrageConfig::default(),
        ),
        required_account_permissions: account_permissions(&[
            AccountPermission::ReadBalances,
            AccountPermission::ReadPositions,
            AccountPermission::ReadOrders,
            AccountPermission::ReadFills,
            AccountPermission::TradePerpetual,
            AccountPermission::CancelOrders,
        ]),
        metadata: BTreeMap::from([
            ("legacy_module".to_string(), json!(MIGRATED_FROM)),
            ("partial_core_migration".to_string(), json!(false)),
            ("runtime_contract_migration".to_string(), json!(true)),
            (
                "migrated_core_modules".to_string(),
                json!([
                    "state",
                    "fee_model",
                    "funding_model",
                    "settlement_ledger",
                    "dual_taker_open_close_model",
                    "startup_readiness_gate",
                    "single_leg_guard",
                    "position_cooldown_and_hold_limits",
                    "exchange_status_read_model",
                    "log_rotation_policy",
                    "app_runtime_cycle",
                    "runtime_contract",
                    "task_orchestration_contract",
                    "market_data_provider_contract",
                    "execution_provider_contract",
                    "storage_provider_contract",
                    "dashboard_snapshot_contract",
                    "notification_provider_contract",
                    "high_volatility_universe_contract"
                ]),
            ),
            ("remaining_legacy_modules".to_string(), json!([])),
        ]),
    }
}

pub fn cross_exchange_market_data_subscriptions(
    config: &CrossExchangeArbitrageConfig,
) -> Vec<MarketDataSubscription> {
    cross_exchange_market_data_subscriptions_for_symbols(config, config.active_symbols())
}

pub fn cross_exchange_market_data_subscriptions_for_high_volatility(
    config: &CrossExchangeArbitrageConfig,
    tickers: &[VolatilityRankTicker],
) -> Vec<MarketDataSubscription> {
    if !config.volatility_universe.monitor_orderbook {
        return Vec::new();
    }
    cross_exchange_market_data_subscriptions_for_symbols(
        config,
        config.active_symbols_with_high_volatility(tickers),
    )
}

fn cross_exchange_market_data_subscriptions_for_symbols(
    config: &CrossExchangeArbitrageConfig,
    symbols: Vec<String>,
) -> Vec<MarketDataSubscription> {
    let venues = config.active_venues();
    venues
        .into_iter()
        .flat_map(|venue| {
            symbols.iter().map(move |symbol| MarketDataSubscription {
                exchange_id: venue.clone(),
                symbol: symbol.clone(),
                market_type: config.market_type.clone(),
                channels: vec![
                    MarketDataChannel::OrderBookDepth,
                    MarketDataChannel::Custom("fastest_l1_10ms".to_string()),
                ],
            })
        })
        .collect()
}

pub fn cross_exchange_execution_intent(
    ctx: &StrategyContext,
    intent_id: &str,
    requested_at: DateTime<Utc>,
    payload: Value,
) -> ExecutionIntent {
    ExecutionIntent {
        schema_version: 1,
        intent_kind: "cross_exchange_arbitrage_execution_plan".to_string(),
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        idempotency_key: format!("{}:{}:{}", ctx.strategy_id(), ctx.run_id(), intent_id),
        requested_at,
        payload,
    }
}

pub struct ConcurrentTakerOrderSubmission {
    pub first: SdkResult<ExecutionOrderAck>,
    pub second: SdkResult<ExecutionOrderAck>,
}

impl ConcurrentTakerOrderSubmission {
    pub fn both_accepted(&self) -> bool {
        self.first.as_ref().is_ok_and(|ack| ack.accepted)
            && self.second.as_ref().is_ok_and(|ack| ack.accepted)
    }
}

pub async fn submit_taker_order_pair_concurrently(
    ctx: &StrategyContext,
    bundle_id: &str,
    first: &TakerOrderDraft,
    second: &TakerOrderDraft,
    risk_profile_id: &str,
    requested_at: DateTime<Utc>,
) -> ConcurrentTakerOrderSubmission {
    let first_command =
        taker_order_command(ctx, bundle_id, first, risk_profile_id, requested_at, "0");
    let second_command =
        taker_order_command(ctx, bundle_id, second, risk_profile_id, requested_at, "1");
    let first_client = ctx.execution();
    let second_client = ctx.execution();
    let (first, second) = tokio::join!(
        first_client.submit_order(first_command),
        second_client.submit_order(second_command)
    );
    ConcurrentTakerOrderSubmission { first, second }
}

pub async fn submit_slippage_capture_maker_order(
    ctx: &StrategyContext,
    bundle_id: &str,
    order: &SlippageCaptureMakerOrderDraft,
    risk_profile_id: &str,
    requested_at: DateTime<Utc>,
) -> SdkResult<ExecutionOrderAck> {
    ctx.execution()
        .submit_order(slippage_capture_maker_order_command(
            ctx,
            bundle_id,
            order,
            risk_profile_id,
            requested_at,
        ))
        .await
}

pub async fn submit_slippage_capture_hedge_order(
    ctx: &StrategyContext,
    bundle_id: &str,
    hedge: &SlippageCaptureHedgePlan,
    risk_profile_id: &str,
    requested_at: DateTime<Utc>,
) -> SdkResult<ExecutionOrderAck> {
    ctx.execution()
        .submit_order(slippage_capture_hedge_order_command(
            ctx,
            bundle_id,
            hedge,
            risk_profile_id,
            requested_at,
        ))
        .await
}

pub fn slippage_capture_maker_order_command(
    ctx: &StrategyContext,
    bundle_id: &str,
    order: &SlippageCaptureMakerOrderDraft,
    risk_profile_id: &str,
    requested_at: DateTime<Utc>,
) -> ExecutionOrderCommand {
    let role = debug_name_to_snake(order.role);
    let side = sdk_order_side(order.side);
    let client_order_id = format!(
        "{}:{}:{}:{}",
        ctx.strategy_id(),
        bundle_id,
        order.exchange,
        role
    );
    ExecutionOrderCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        idempotency_key: format!("{}:{}", ctx.run_id(), client_order_id),
        client_order_id,
        risk_profile_id: risk_profile_id.to_string(),
        requested_at,
        exchange_id: order.exchange.to_string(),
        symbol: order.canonical_symbol.as_pair(),
        side,
        order_type: OrderType::Limit,
        quantity: trim_float(order.quantity),
        price: Some(trim_float(order.limit_price)),
        time_in_force: Some(TimeInForce::GoodTilCanceled),
        reduce_only: order.reduce_only,
        metadata: BTreeMap::from([
            ("bundle_id".to_string(), json!(bundle_id)),
            ("role".to_string(), json!(role)),
            (
                "execution_style".to_string(),
                json!("slippage_capture_maker_open"),
            ),
            ("open_module".to_string(), json!("slippage_capture")),
            ("post_only_requested".to_string(), json!(order.post_only)),
            (
                "auto_cancel_after_ms".to_string(),
                json!(order.auto_cancel_after_ms),
            ),
            ("cancel_if_unfilled".to_string(), json!(true)),
            (
                "top_of_book_price".to_string(),
                json!(trim_float(order.top_of_book_price)),
            ),
            (
                "planned_execution_price".to_string(),
                json!(trim_float(order.limit_price)),
            ),
            (
                "planned_base_quantity".to_string(),
                json!(trim_float(order.base_quantity)),
            ),
            (
                "exchange_order_quantity".to_string(),
                json!(trim_float(order.quantity)),
            ),
            (
                "quantity_unit".to_string(),
                json!(debug_name_to_snake(order.quantity_unit)),
            ),
            (
                "contract_size_base".to_string(),
                json!(trim_float(order.contract_size)),
            ),
            (
                "planned_notional_usdt".to_string(),
                json!(trim_float(order.planned_notional_usdt())),
            ),
            ("hedge_after_fill_required".to_string(), json!(true)),
            (
                "hedge_trigger".to_string(),
                json!("private_fill_or_rest_sync"),
            ),
        ]),
    }
}

pub fn slippage_capture_hedge_order_command(
    ctx: &StrategyContext,
    bundle_id: &str,
    hedge: &SlippageCaptureHedgePlan,
    risk_profile_id: &str,
    requested_at: DateTime<Utc>,
) -> ExecutionOrderCommand {
    let mut command = taker_order_command(
        ctx,
        bundle_id,
        &hedge.order,
        risk_profile_id,
        requested_at,
        "hedge",
    );
    command.metadata.insert(
        "execution_style".to_string(),
        json!("slippage_capture_taker_hedge"),
    );
    command
        .metadata
        .insert("open_module".to_string(), json!("slippage_capture"));
    command
        .metadata
        .insert("hedge_trigger".to_string(), json!(hedge.trigger));
    command.metadata.insert(
        "filled_maker_base_quantity".to_string(),
        json!(trim_float(hedge.filled_maker_base_quantity)),
    );
    command
}

pub fn slippage_capture_maker_cancel_command(
    ctx: &StrategyContext,
    bundle_id: &str,
    maker_order: &SlippageCaptureMakerOrderDraft,
    client_order_id: String,
    risk_profile_id: &str,
    requested_at: DateTime<Utc>,
) -> ExecutionCancelCommand {
    ExecutionCancelCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        client_order_id: Some(client_order_id.clone()),
        execution_order_id: None,
        idempotency_key: format!("{}:{}:cancel", ctx.run_id(), client_order_id),
        risk_profile_id: risk_profile_id.to_string(),
        requested_at,
        exchange_id: maker_order.exchange.to_string(),
        symbol: maker_order.canonical_symbol.as_pair(),
        metadata: BTreeMap::from([
            ("bundle_id".to_string(), json!(bundle_id)),
            (
                "execution_style".to_string(),
                json!("slippage_capture_maker_cancel"),
            ),
            (
                "cancel_reason".to_string(),
                json!("maker_order_timeout_or_unfilled"),
            ),
            (
                "auto_cancel_after_ms".to_string(),
                json!(maker_order.auto_cancel_after_ms),
            ),
        ]),
    }
}

fn taker_order_command(
    ctx: &StrategyContext,
    bundle_id: &str,
    order: &TakerOrderDraft,
    risk_profile_id: &str,
    requested_at: DateTime<Utc>,
    leg_index: &str,
) -> ExecutionOrderCommand {
    let role = debug_name_to_snake(order.role);
    let side = sdk_order_side(order.side);
    let client_order_id = format!(
        "{}:{}:{}:{}",
        ctx.strategy_id(),
        bundle_id,
        order.exchange,
        role
    );
    let idempotency_key = format!("{}:{}:{}", ctx.run_id(), client_order_id, leg_index);

    ExecutionOrderCommand {
        schema_version: 1,
        tenant_id: ctx.tenant_id().to_string(),
        account_id: ctx.account_id().to_string(),
        strategy_id: ctx.strategy_id().to_string(),
        run_id: ctx.run_id().to_string(),
        client_order_id,
        idempotency_key,
        risk_profile_id: risk_profile_id.to_string(),
        requested_at,
        exchange_id: order.exchange.to_string(),
        symbol: order.canonical_symbol.as_pair(),
        side,
        order_type: OrderType::ImmediateOrCancel,
        quantity: trim_float(order.quantity),
        price: Some(trim_float(order.worst_acceptable_price)),
        time_in_force: Some(TimeInForce::ImmediateOrCancel),
        reduce_only: order.reduce_only,
        metadata: BTreeMap::from([
            ("bundle_id".to_string(), json!(bundle_id)),
            ("role".to_string(), json!(role)),
            ("execution_style".to_string(), json!("dual_taker_ioc_limit")),
            (
                "submit_group".to_string(),
                json!("concurrent_two_leg_arbitrage"),
            ),
            (
                "planned_execution_price".to_string(),
                json!(trim_float(order.reference_price)),
            ),
            (
                "worst_acceptable_price".to_string(),
                json!(trim_float(order.worst_acceptable_price)),
            ),
            (
                "planned_base_quantity".to_string(),
                json!(trim_float(order.base_quantity)),
            ),
            (
                "exchange_order_quantity".to_string(),
                json!(trim_float(order.quantity)),
            ),
            (
                "quantity_unit".to_string(),
                json!(debug_name_to_snake(order.quantity_unit)),
            ),
            (
                "contract_size_base".to_string(),
                json!(trim_float(order.contract_size)),
            ),
            (
                "planned_notional_usdt".to_string(),
                json!(trim_float(order.planned_notional_usdt())),
            ),
            ("hedge_mode_position_side_required".to_string(), json!(true)),
            (
                "provider_must_normalize_exchange_close_flags".to_string(),
                json!(true),
            ),
        ]),
    }
}

fn sdk_order_side(side: OrderSide) -> SdkOrderSide {
    match side {
        OrderSide::Buy => SdkOrderSide::Buy,
        OrderSide::Sell => SdkOrderSide::Sell,
    }
}

fn debug_name_to_snake(value: impl std::fmt::Debug) -> String {
    let name = format!("{value:?}");
    let mut output = String::new();
    for (index, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                output.push('_');
            }
            output.push(ch.to_ascii_lowercase());
        } else {
            output.push(ch);
        }
    }
    output
}

fn default_cross_arb_venues() -> Vec<String> {
    vec![
        "binance".to_string(),
        "gate".to_string(),
        "bitget".to_string(),
    ]
}

fn default_cross_arb_symbols() -> Vec<String> {
    vec!["EDGE/USDT".to_string(), "DRIFT/USDT".to_string()]
}

fn default_excluded_bases() -> Vec<String> {
    [
        "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "TRX", "TON", "LTC",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn default_min_profit_bps() -> f64 {
    50.0
}

fn default_max_consecutive_losses() -> u32 {
    5
}

fn default_max_position_notional_quote() -> String {
    "5.5".to_string()
}

fn default_market_type() -> MarketType {
    MarketType::Perpetual
}

fn normalize_symbol(symbol: &str) -> String {
    let symbol = symbol.trim().to_ascii_uppercase().replace(['-', '_'], "/");
    if symbol.is_empty() || symbol.contains('/') {
        return symbol;
    }
    for quote in ["USDT", "USDC", "USD"] {
        if symbol.ends_with(quote) && symbol.len() > quote.len() {
            let base = &symbol[..symbol.len() - quote.len()];
            return format!("{base}/{quote}");
        }
    }
    symbol
}

fn symbol_base(symbol: &str) -> String {
    normalize_symbol(symbol)
        .split('/')
        .next()
        .unwrap_or_default()
        .to_string()
}

fn parse_market_type(value: &str) -> MarketType {
    match value.trim().to_ascii_lowercase().as_str() {
        "spot" => MarketType::Spot,
        "margin" => MarketType::Margin,
        "perpetual" | "perp" | "swap" => MarketType::Perpetual,
        "future" | "futures" => MarketType::Futures,
        "option" | "options" => MarketType::Option,
        other => MarketType::Custom(other.to_string()),
    }
}

fn parse_execution_module(value: &str) -> CrossArbExecutionModule {
    match value.trim().to_ascii_lowercase().as_str() {
        "slippage_capture" | "maker_taker" | "maker_taker_slippage" | "slippage-maker" => {
            CrossArbExecutionModule::SlippageCapture
        }
        _ => CrossArbExecutionModule::DualTaker,
    }
}

fn value_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    path.iter().try_fold(value, |current, key| current.get(key))
}

fn strings_at(value: &Value, path: &[&str]) -> Option<Vec<String>> {
    let values = value_at(value, path)?.as_array()?;
    Some(
        values
            .iter()
            .filter_map(|value| value.as_str())
            .map(str::to_string)
            .collect(),
    )
}

fn first_non_empty_strings(value: &Value, paths: &[&[&str]]) -> Option<Vec<String>> {
    paths.iter().find_map(|path| {
        let values = strings_at(value, path)?;
        (!values.is_empty()).then_some(values)
    })
}

fn enabled_exchange_keys(value: &Value) -> Option<Vec<String>> {
    let exchanges = value_at(value, &["exchanges"])?.as_object()?;
    let mut venues: Vec<_> = exchanges
        .iter()
        .filter_map(|(exchange, config)| {
            let enabled = bool_at(config, &["enabled"]).unwrap_or(false);
            let operating_mode = text_at(config, &["operating_mode"])
                .unwrap_or_default()
                .to_ascii_lowercase();
            (enabled || operating_mode == "enabled").then_some(exchange.to_string())
        })
        .collect();
    venues.sort();
    (!venues.is_empty()).then_some(venues)
}

fn text_at(value: &Value, path: &[&str]) -> Option<String> {
    value_at(value, path)?.as_str().map(str::to_string)
}

fn bool_at(value: &Value, path: &[&str]) -> Option<bool> {
    value_at(value, path)?.as_bool()
}

fn f64_at(value: &Value, path: &[&str]) -> Option<f64> {
    match value_at(value, path)? {
        Value::Number(number) => number.as_f64(),
        Value::String(value) => parse_f64(value),
        _ => None,
    }
}

fn u64_at(value: &Value, path: &[&str]) -> Option<u64> {
    match value_at(value, path)? {
        Value::Number(number) => number
            .as_u64()
            .or_else(|| number.as_f64().map(|value| value.max(0.0) as u64)),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn usize_at(value: &Value, path: &[&str]) -> Option<usize> {
    u64_at(value, path).map(|value| value as usize)
}

fn u32_at(value: &Value, path: &[&str]) -> Option<u32> {
    u64_at(value, path).map(|value| value as u32)
}

fn i64_at(value: &Value, path: &[&str]) -> Option<i64> {
    match value_at(value, path)? {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_f64().map(|value| value as i64)),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn parse_f64(value: &str) -> Option<f64> {
    value.parse::<f64>().ok().filter(|value| value.is_finite())
}

fn trim_float(value: f64) -> String {
    let mut text = format!("{value:.10}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

pub fn config_schema() -> StrategyConfigSchema {
    StrategyConfigSchema {
        schema_version: 1,
        json_schema: json!({
            "type": "object",
            "additionalProperties": true,
            "required": ["venues", "symbols"],
            "properties": {
                "venues": {
                    "type": "array",
                    "minItems": 2,
                    "items": { "type": "string", "minLength": 1 }
                },
                "symbols": {
                    "type": "array",
                    "minItems": 1,
                    "items": { "type": "string", "minLength": 1 }
                },
                "min_profit_bps": { "type": "number", "minimum": 0.0 },
                "max_position_notional_quote": {
                    "type": "string",
                    "pattern": "^[0-9]+(\\.[0-9]+)?$"
                },
                "market_type": {
                    "type": "string",
                    "enum": ["perpetual", "perp", "swap", "futures", "future", "spot", "margin"]
                },
                "execution_module": {
                    "type": "string",
                    "enum": ["dual_taker", "slippage_capture", "maker_taker", "maker_taker_slippage"],
                    "default": "dual_taker"
                },
                "excluded_bases": {
                    "type": "array",
                    "items": { "type": "string", "minLength": 1 }
                },
                "excluded_symbols": {
                    "type": "array",
                    "items": { "type": "string", "minLength": 1 }
                },
                "max_consecutive_losses": { "type": "integer", "minimum": 1, "default": 5 },
                "dual_taker": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "target_notional_usdt": { "type": "number", "minimum": 0.0 },
                        "min_open_spread_pct": { "type": "number", "minimum": 0.0 },
                        "min_open_net_profit_pct": { "type": "number", "minimum": 0.0 },
                        "max_open_spread_pct": { "type": "number", "minimum": 0.0 },
                        "taker_slippage_pct": { "type": "number", "minimum": 0.0 },
                        "close_min_net_profit_pct": { "type": "number", "minimum": 0.0 },
                        "expected_close_spread_pct": { "type": "number", "minimum": 0.0 },
                        "top_of_book_capacity_ratio": { "type": "number", "exclusiveMinimum": 0.0, "maximum": 1.0 },
                        "orderbook_stale_ms": { "type": "integer", "minimum": 1 },
                        "min_orderbook_levels": { "type": "integer", "minimum": 1 },
                        "single_leg_timeout_ms": { "type": "integer", "minimum": 1 },
                        "max_consecutive_single_leg_fills": { "type": "integer", "minimum": 1 },
                        "max_open_bundles": { "type": "integer", "minimum": 1 },
                        "max_positions_per_exchange": { "type": "integer", "minimum": 1 },
                        "max_active_bundles_per_symbol": { "type": "integer", "minimum": 1 },
                        "symbol_cooldown_secs": { "type": "integer", "minimum": 0 },
                        "max_hold_secs": { "type": "integer", "minimum": 1 },
                        "close_on_max_hold_requires_profit": { "type": "boolean" }
                    }
                },
                "slippage_capture": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "target_notional_usdt": { "type": "number", "minimum": 0.0 },
                        "min_open_spread_pct": { "type": "number", "minimum": 0.0 },
                        "min_open_net_profit_pct": { "type": "number", "minimum": 0.0 },
                        "max_open_spread_pct": { "type": "number", "minimum": 0.0 },
                        "startup_skip_spread_secs": { "type": "integer", "minimum": 0, "default": 10 },
                        "max_signal_age_ms": { "type": "integer", "minimum": 1, "default": 3000 },
                        "maker_price_offset_pct": { "type": "number", "minimum": 0.0 },
                        "hedge_taker_slippage_pct": { "type": "number", "minimum": 0.0 },
                        "close_taker_slippage_pct": { "type": "number", "minimum": 0.0 },
                        "close_min_net_profit_pct": { "type": "number", "minimum": 0.0 },
                        "max_hold_secs": { "type": "integer", "minimum": 1 },
                        "close_on_max_hold_requires_profit": { "type": "boolean" },
                        "maker_order_timeout_ms": { "type": "integer", "minimum": 1, "default": 1000 },
                        "max_concurrent_maker_orders": { "type": "integer", "minimum": 1, "default": 1 },
                        "cancel_unfilled_maker": { "type": "boolean" },
                        "max_maker_top_depth_usdt": { "type": "number", "minimum": 0.0 },
                        "hedge_top_of_book_capacity_ratio": { "type": "number", "exclusiveMinimum": 0.0, "maximum": 1.0 },
                        "enforce_hedge_top_depth": { "type": "boolean" },
                        "orderbook_stale_ms": { "type": "integer", "minimum": 1 },
                        "min_orderbook_levels": { "type": "integer", "minimum": 1 },
                        "max_open_bundles": { "type": "integer", "minimum": 1 },
                        "max_positions_per_exchange": { "type": "integer", "minimum": 1 },
                        "max_active_bundles_per_symbol": { "type": "integer", "minimum": 1 },
                        "symbol_cooldown_secs": { "type": "integer", "minimum": 0 }
                    }
                },
                "logging": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "persist_only_key_events": { "type": "boolean" },
                        "max_file_bytes": { "type": "integer", "minimum": 1 },
                        "retained_files": { "type": "integer", "minimum": 1 }
                    }
                },
                "volatility_universe": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "enabled": { "type": "boolean" },
                        "top_gainers_per_exchange": { "type": "integer", "minimum": 0 },
                        "top_losers_per_exchange": { "type": "integer", "minimum": 0 },
                        "min_abs_change_pct": { "type": "number", "minimum": 0.0 },
                        "min_quote_volume_usdt": { "type": "number", "minimum": 0.0 },
                        "refresh_secs": { "type": "integer", "minimum": 1 },
                        "max_dynamic_symbols": { "type": "integer", "minimum": 0 },
                        "monitor_orderbook": { "type": "boolean" }
                    }
                },
                "dry_run": { "type": "boolean", "default": true }
            }
        }),
    }
}

pub fn snapshot_schema() -> StrategySnapshotSchema {
    StrategySnapshotSchema {
        schema_version: 1,
        json_schema: common_snapshot_schema(),
    }
}

fn common_snapshot_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["migrated_from", "handled_events"],
        "properties": {
            "migrated_from": { "type": "string" },
            "handled_events": { "type": "integer", "minimum": 0 },
            "started_at": { "type": ["string", "null"], "format": "date-time" },
            "last_event_at": { "type": ["string", "null"], "format": "date-time" },
            "configured_venues": {
                "type": "array",
                "items": { "type": "string" }
            },
            "configured_symbols": { "type": "integer", "minimum": 0 },
            "execution_module": { "type": "string" },
            "excluded_bases": {
                "type": "array",
                "items": { "type": "string" }
            },
            "target_notional_usdt": { "type": "string" },
            "min_open_spread_pct": { "type": "string" },
            "min_open_net_profit_pct": { "type": "string" },
            "max_open_spread_pct": { "type": "string" },
            "close_min_net_profit_pct": { "type": "string" },
            "slippage_capture_maker_price_offset_pct": { "type": "string" },
            "slippage_capture_maker_order_timeout_ms": { "type": "integer", "minimum": 1 },
            "slippage_capture_startup_skip_spread_secs": { "type": "integer", "minimum": 0 },
            "orderbook_stale_ms": { "type": "integer", "minimum": 1 },
            "high_volatility_universe_enabled": { "type": "boolean" },
            "high_volatility_refresh_secs": { "type": "integer", "minimum": 1 },
            "max_consecutive_losses": { "type": "integer", "minimum": 1 }
        }
    })
}

fn runtime_command_schemas() -> Vec<StrategyCommandSchema> {
    ["pause", "resume", "stop", "rescan", "reload_config"]
        .into_iter()
        .map(|command_kind| StrategyCommandSchema {
            command_kind: command_kind.to_string(),
            description: Some(format!(
                "Cross-exchange arbitrage runtime {command_kind} command"
            )),
            payload_schema: json!({
                "type": "object",
                "additionalProperties": true
            }),
        })
        .collect()
}

fn event_timestamp(event: &StrategyEvent) -> DateTime<Utc> {
    match event {
        StrategyEvent::Started(event) | StrategyEvent::Stopping(event) => event.occurred_at,
        StrategyEvent::Execution(event) => event.occurred_at,
        StrategyEvent::MarketData(event) => event.received_at,
        StrategyEvent::Account(event) => event.received_at,
        StrategyEvent::OperatorCommand(command) => command.requested_at,
        StrategyEvent::Timer(event) => event.fired_at,
    }
}

fn risk_capability(
    capability: RiskCapability,
    description: impl Into<String>,
) -> RiskCapabilityDeclaration {
    RiskCapabilityDeclaration {
        capability,
        description: Some(description.into()),
        limits: json!({ "configured_per_instance": true }),
    }
}

fn account_permissions(permissions: &[AccountPermission]) -> Vec<RequiredAccountPermission> {
    permissions
        .iter()
        .cloned()
        .map(|permission| RequiredAccountPermission {
            account_id: None,
            permission,
            reason: Some("Required by configured strategy runtime".to_string()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustcta_strategy_sdk::{
        ExecutionCancelAck, ExecutionCancelCommand, ExecutionIntent, ExecutionIntentAck,
        ExecutionOrderAck, ExecutionOrderCommand, MarketDataChannel, MarketType, SdkResult,
        StrategyExecutionClient,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

    struct NoopExecutionClient;

    #[test]
    fn config_market_type_should_keep_futures_distinct_from_perpetual() {
        for (market_type, expected) in [
            ("perpetual", MarketType::Perpetual),
            ("perp", MarketType::Perpetual),
            ("swap", MarketType::Perpetual),
            ("futures", MarketType::Futures),
            ("future", MarketType::Futures),
        ] {
            let config = CrossExchangeArbitrageConfig::from_runtime_value(&json!({
                "venues": ["binance", "bitget"],
                "symbols": ["DRIFT/USDT"],
                "market_type": market_type
            }));
            assert_eq!(config.market_type, expected);
        }
    }

    #[test]
    fn config_should_parse_min_open_maker_taker_net_edge() {
        let config = CrossExchangeArbitrageConfig::from_runtime_value(&json!({
            "thresholds": {
                "min_open_raw_spread": 0.004,
                "min_open_maker_taker_net_edge": 0.0045
            }
        }));

        assert_eq!(config.dual_taker.min_open_spread_pct, 0.004);
        assert_eq!(config.dual_taker.min_open_net_profit_pct, 0.0045);
    }

    #[test]
    fn config_should_parse_close_on_max_hold_requires_profit() {
        let config = CrossExchangeArbitrageConfig::from_runtime_value(&json!({
            "dual_taker": {
                "close_on_max_hold_requires_profit": true
            }
        }));

        assert!(config.dual_taker.close_on_max_hold_requires_profit);
    }

    #[test]
    fn config_should_parse_max_open_bundles() {
        let config = CrossExchangeArbitrageConfig::from_runtime_value(&json!({
            "risk": {
                "max_open_bundles": 3
            }
        }));

        assert_eq!(config.dual_taker.max_open_bundles, 3);
    }

    #[async_trait]
    impl StrategyExecutionClient for NoopExecutionClient {
        async fn submit_order(
            &self,
            command: ExecutionOrderCommand,
        ) -> SdkResult<ExecutionOrderAck> {
            Ok(ExecutionOrderAck {
                schema_version: command.schema_version,
                accepted: true,
                client_order_id: command.client_order_id,
                execution_order_id: None,
                reason: None,
                received_at: Utc::now(),
            })
        }

        async fn cancel_order(
            &self,
            command: ExecutionCancelCommand,
        ) -> SdkResult<ExecutionCancelAck> {
            Ok(ExecutionCancelAck {
                schema_version: command.schema_version,
                accepted: true,
                client_order_id: command.client_order_id,
                execution_order_id: command.execution_order_id,
                reason: None,
                received_at: Utc::now(),
            })
        }

        async fn submit_raw_intent(
            &self,
            intent: ExecutionIntent,
        ) -> SdkResult<ExecutionIntentAck> {
            Ok(ExecutionIntentAck {
                schema_version: intent.schema_version,
                accepted: true,
                intent_kind: intent.intent_kind,
                reason: None,
                received_at: Utc::now(),
                payload: Value::Null,
            })
        }
    }

    #[derive(Default)]
    struct ConcurrentProbeExecutionClient {
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        orders: Mutex<Vec<ExecutionOrderCommand>>,
    }

    #[async_trait]
    impl StrategyExecutionClient for ConcurrentProbeExecutionClient {
        async fn submit_order(
            &self,
            command: ExecutionOrderCommand,
        ) -> SdkResult<ExecutionOrderAck> {
            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.orders
                .lock()
                .expect("probe orders lock should not be poisoned")
                .push(command.clone());
            Ok(ExecutionOrderAck {
                schema_version: command.schema_version,
                accepted: true,
                client_order_id: command.client_order_id,
                execution_order_id: Some("accepted".to_string()),
                reason: None,
                received_at: Utc::now(),
            })
        }

        async fn cancel_order(
            &self,
            command: ExecutionCancelCommand,
        ) -> SdkResult<ExecutionCancelAck> {
            Ok(ExecutionCancelAck {
                schema_version: command.schema_version,
                accepted: false,
                client_order_id: command.client_order_id,
                execution_order_id: command.execution_order_id,
                reason: Some("not used by concurrent submission probe".to_string()),
                received_at: Utc::now(),
            })
        }

        async fn submit_raw_intent(
            &self,
            intent: ExecutionIntent,
        ) -> SdkResult<ExecutionIntentAck> {
            Ok(ExecutionIntentAck {
                schema_version: intent.schema_version,
                accepted: false,
                intent_kind: intent.intent_kind,
                reason: Some("not used by concurrent submission probe".to_string()),
                received_at: Utc::now(),
                payload: Value::Null,
            })
        }
    }

    #[test]
    fn spec_should_expose_config_and_snapshot_schemas() {
        let spec = strategy_spec();
        assert_eq!(spec.strategy_kind, STRATEGY_KIND);
        assert_eq!(spec.config_schema.schema_version, 1);
        assert_eq!(spec.config_schema.json_schema["type"], json!("object"));
        assert!(spec.config_schema.json_schema["required"]
            .as_array()
            .is_some_and(|required| !required.is_empty()));
        assert_eq!(spec.snapshot_schema.schema_version, 1);
        assert_eq!(spec.snapshot_schema.json_schema["type"], json!("object"));
        assert!(spec.snapshot_schema.json_schema["properties"]["handled_events"].is_object());
        assert_eq!(
            spec.supported_commands
                .iter()
                .map(|command| command.command_kind.as_str())
                .collect::<Vec<_>>(),
            vec!["pause", "resume", "stop", "rescan", "reload_config"]
        );
        assert!(!spec.market_data_subscriptions.is_empty());
        assert_eq!(
            spec.market_data_subscriptions[0].channels,
            vec![
                MarketDataChannel::OrderBookDepth,
                MarketDataChannel::Custom("fastest_l1_10ms".to_string())
            ]
        );
        assert_eq!(
            spec.market_data_subscriptions[0].market_type,
            MarketType::Perpetual
        );
        assert_eq!(spec.metadata["runtime_contract_migration"], json!(true));
        assert_eq!(spec.metadata["remaining_legacy_modules"], json!([]));
        assert_secret_free(&serde_json::to_value(spec).expect("spec should serialize"));
    }

    #[test]
    fn config_parse_and_market_data_subscription_should_use_sdk_contract() {
        let config: CrossExchangeArbitrageConfig = serde_json::from_value(json!({
            "venues": ["binance", "bitget"],
            "symbols": ["drift/usdt"],
            "min_profit_bps": 1.5,
            "max_position_notional_quote": "25",
            "dry_run": true
        }))
        .expect("config should parse");

        let subscriptions = cross_exchange_market_data_subscriptions(&config);

        assert_eq!(config.symbols, vec!["drift/usdt"]);
        assert_eq!(subscriptions.len(), 2);
        assert_eq!(subscriptions[0].exchange_id, "binance");
        assert_eq!(subscriptions[0].symbol, "DRIFT/USDT");
        assert_eq!(subscriptions[0].market_type, MarketType::Perpetual);
        assert_eq!(
            subscriptions[0].channels,
            vec![
                MarketDataChannel::OrderBookDepth,
                MarketDataChannel::Custom("fastest_l1_10ms".to_string())
            ]
        );
    }

    #[test]
    fn config_should_filter_excluded_major_symbols_before_subscription() {
        let config: CrossExchangeArbitrageConfig = serde_json::from_value(json!({
            "venues": ["binance", "gate", "bitget"],
            "symbols": ["BTC/USDT", "eth_usdt", "BNBUSDT", "EDGE/USDT"],
            "dry_run": true
        }))
        .expect("config should parse");

        assert_eq!(config.active_symbols(), vec!["EDGE/USDT"]);
        assert_eq!(cross_exchange_market_data_subscriptions(&config).len(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_should_be_secret_free() {
        let mut runtime = CrossExchangeArbitrageRuntime::new();
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            json!({
                "api_key": "must-not-leak",
                "secret": "must-not-leak",
                "symbols": ["BTC/USDT"]
            }),
            execution,
        );

        runtime.start(ctx).await.expect("runtime should start");
        let snapshot = runtime.snapshot().await.expect("snapshot should build");

        assert_eq!(snapshot.strategy_kind, STRATEGY_KIND);
        assert_eq!(snapshot.status, StrategyStatus::Running);
        assert_secret_free(&snapshot.payload);
        assert_secret_free(&serde_json::to_value(snapshot).expect("snapshot should serialize"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_reload_config_value_should_update_snapshot_venues() {
        let mut runtime = CrossExchangeArbitrageRuntime::new();
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "cross_arb_live",
            "run-1",
            json!({
                "enabled_exchanges": ["binance"],
                "enabled_symbols": ["EDGE/USDT"]
            }),
            execution,
        );
        runtime.start(ctx).await.expect("runtime should start");
        runtime.reload_config_value(&json!({
            "enabled_exchanges": ["gate", "bitget"],
            "enabled_symbols": ["EDGE/USDT"]
        }));

        let snapshot = runtime.snapshot().await.expect("snapshot should build");

        assert_eq!(
            snapshot.payload["configured_venues"],
            json!(["gate", "bitget"])
        );
        assert_eq!(snapshot.payload["handled_events"], json!(1));
        assert!(snapshot.payload["last_event_at"].is_string());
    }

    #[test]
    fn dry_run_execution_intent_should_be_adapter_free_and_idempotent() {
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            serde_json::to_value(CrossExchangeArbitrageConfig::default())
                .expect("config should serialize"),
            execution,
        );
        let requested_at = Utc::now();
        let intent = cross_exchange_execution_intent(
            &ctx,
            "bundle-1",
            requested_at,
            json!({
                "dry_run": true,
                "route": {
                    "long_exchange": "binance",
                    "short_exchange": "bitget",
                    "symbol": "BTC/USDT"
                }
            }),
        );

        assert_eq!(
            intent.intent_kind,
            "cross_exchange_arbitrage_execution_plan"
        );
        assert_eq!(intent.tenant_id, "tenant-1");
        assert_eq!(intent.account_id, "account-1");
        assert_eq!(intent.idempotency_key, "strategy-1:run-1:bundle-1");
        assert_eq!(intent.payload["dry_run"], json!(true));
        assert_secret_free(&serde_json::to_value(intent).expect("intent should serialize"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn taker_order_pair_submission_should_submit_two_legs_concurrently() {
        let probe = Arc::new(ConcurrentProbeExecutionClient::default());
        let execution: Arc<dyn StrategyExecutionClient> = probe.clone();
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            json!({}),
            execution,
        );
        let symbol = CanonicalSymbol::new("EDGE", "USDT");
        let first = TakerOrderDraft {
            exchange: ExchangeId::new("binance"),
            canonical_symbol: symbol.clone(),
            side: OrderSide::Buy,
            base_quantity: 0.055,
            quantity: 0.055,
            quantity_unit: QuantityUnit::Base,
            contract_size: 1.0,
            reference_price: 100.0,
            worst_acceptable_price: 100.05,
            reduce_only: false,
            role: TakerOrderRole::OpenLong,
        };
        let second = TakerOrderDraft {
            exchange: ExchangeId::new("gate"),
            canonical_symbol: symbol,
            side: OrderSide::Sell,
            base_quantity: 0.055,
            quantity: 55.0,
            quantity_unit: QuantityUnit::Contracts,
            contract_size: 0.001,
            reference_price: 100.6,
            worst_acceptable_price: 100.54,
            reduce_only: false,
            role: TakerOrderRole::OpenShort,
        };

        let submitted = submit_taker_order_pair_concurrently(
            &ctx,
            "bundle-1",
            &first,
            &second,
            "cross-arb-live",
            Utc::now(),
        )
        .await;

        assert!(submitted.both_accepted());
        assert_eq!(probe.max_in_flight.load(Ordering::SeqCst), 2);
        let orders = probe
            .orders
            .lock()
            .expect("probe orders lock should not be poisoned");
        assert_eq!(orders.len(), 2);
        assert!(orders
            .iter()
            .all(|order| order.order_type == OrderType::ImmediateOrCancel));
        assert!(orders
            .iter()
            .all(|order| order.time_in_force == Some(TimeInForce::ImmediateOrCancel)));
        assert!(orders
            .iter()
            .any(|order| order.metadata["quantity_unit"] == json!("contracts")));
        assert!(orders
            .iter()
            .all(|order| order.metadata["planned_execution_price"].is_string()));
    }

    #[test]
    fn slippage_capture_commands_should_encode_maker_timeout_and_hedge_trigger() {
        let execution: Arc<dyn StrategyExecutionClient> = Arc::new(NoopExecutionClient);
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            json!({}),
            execution,
        );
        let symbol = CanonicalSymbol::new("EDGE", "USDT");
        let maker = SlippageCaptureMakerOrderDraft {
            exchange: ExchangeId::new("binance"),
            canonical_symbol: symbol.clone(),
            side: OrderSide::Sell,
            base_quantity: 0.055,
            quantity: 0.055,
            quantity_unit: QuantityUnit::Base,
            contract_size: 1.0,
            top_of_book_price: 100.6,
            limit_price: 100.7,
            reduce_only: false,
            post_only: false,
            auto_cancel_after_ms: 1_000,
            role: SlippageCaptureOrderRole::OpenMakerShort,
        };
        let hedge = SlippageCaptureHedgePlan {
            exchange: ExchangeId::new("gate"),
            canonical_symbol: symbol.clone(),
            side: OrderSide::Buy,
            reference_price: 100.0,
            filled_maker_base_quantity: 0.055,
            order: TakerOrderDraft {
                exchange: ExchangeId::new("gate"),
                canonical_symbol: symbol,
                side: OrderSide::Buy,
                base_quantity: 0.055,
                quantity: 0.055,
                quantity_unit: QuantityUnit::Base,
                contract_size: 1.0,
                reference_price: 100.0,
                worst_acceptable_price: 100.05,
                reduce_only: false,
                role: TakerOrderRole::OpenLong,
            },
            trigger: "on_maker_fill_private_stream_or_rest_fill_sync".to_string(),
        };
        let requested_at = Utc::now();

        let maker_command = slippage_capture_maker_order_command(
            &ctx,
            "bundle-slip",
            &maker,
            "cross-arb-live",
            requested_at,
        );
        let hedge_command = slippage_capture_hedge_order_command(
            &ctx,
            "bundle-slip",
            &hedge,
            "cross-arb-live",
            requested_at,
        );
        let cancel_command = slippage_capture_maker_cancel_command(
            &ctx,
            "bundle-slip",
            &maker,
            maker_command.client_order_id.clone(),
            "cross-arb-live",
            requested_at,
        );

        assert_eq!(maker_command.order_type, OrderType::Limit);
        assert_eq!(
            maker_command.time_in_force,
            Some(TimeInForce::GoodTilCanceled)
        );
        assert_eq!(
            maker_command.metadata["execution_style"],
            json!("slippage_capture_maker_open")
        );
        assert_eq!(maker_command.metadata["auto_cancel_after_ms"], json!(1000));
        assert_eq!(
            maker_command.metadata["hedge_after_fill_required"],
            json!(true)
        );
        assert_eq!(hedge_command.order_type, OrderType::ImmediateOrCancel);
        assert_eq!(
            hedge_command.metadata["execution_style"],
            json!("slippage_capture_taker_hedge")
        );
        assert_eq!(
            hedge_command.metadata["hedge_trigger"],
            json!("on_maker_fill_private_stream_or_rest_fill_sync")
        );
        assert_eq!(
            cancel_command.metadata["cancel_reason"],
            json!("maker_order_timeout_or_unfilled")
        );
    }

    #[test]
    fn manifest_should_not_depend_on_exchange_adapters() {
        let manifest = include_str!("../Cargo.toml");
        assert!(manifest.contains("rustcta-strategy-sdk.workspace = true"));
        for forbidden in [
            "rustcta-exchange-api",
            "rustcta-exchange-gateway",
            "legacy exchange adapter path",
            "gateio",
            "kucoin",
            "okx",
            "binance",
            "bitget",
            "mexc",
        ] {
            assert!(
                !manifest.contains(forbidden),
                "{forbidden} should not appear in manifest"
            );
        }
    }

    #[test]
    fn runtime_contract_should_be_adapter_free_and_order_safe() {
        let contract = build_runtime_contract(&CrossExchangeArbitrageConfig::default(), Utc::now());

        assert_eq!(contract.strategy_kind, STRATEGY_KIND);
        assert!(!contract.live_orders_enabled_by_default);
        assert!(!contract.dashboard_snapshot.live_orders_enabled);
        assert!(contract.market_data_provider.adapter_free);
        assert!(!contract.market_data_provider.concrete_adapter_dependency);
        assert!(contract.execution_provider.adapter_free);
        assert!(!contract.execution_provider.concrete_adapter_dependency);
        assert!(contract
            .tasks
            .iter()
            .any(|task| task.task_kind == "publish_dashboard_snapshot"));
        assert_secret_free(&serde_json::to_value(contract).expect("contract should serialize"));
    }

    #[test]
    fn app_runtime_cycle_should_record_snapshot_storage_and_notifications_without_orders() {
        let mut runtime = CrossArbAppRuntime::default();
        let cycle = runtime.run_cycle(
            CrossArbRuntimeInput {
                market_snapshots: 3,
                opportunities: 1,
                execution_intents: 2,
                open_bundles: 1,
                pending_orders: 2,
                ..Default::default()
            },
            Utc::now(),
        );

        assert!(!cycle.execution.live_orders_enabled);
        assert_eq!(cycle.execution.requested_intents, 2);
        assert_eq!(cycle.execution.submitted_intents, 0);
        assert_eq!(cycle.dashboard_snapshot.open_bundles, 1);
        assert_eq!(cycle.dashboard_snapshot.pending_orders, 2);
        assert!(cycle
            .storage_events
            .iter()
            .any(|event| event.event_kind == "market_snapshots"));
        assert_eq!(cycle.notifications.len(), 1);
        assert_secret_free(&serde_json::to_value(cycle).expect("cycle should serialize"));
    }

    #[test]
    fn app_runtime_read_model_should_replace_local_ws_support_without_endpoint_payloads() {
        let captured_at = Utc::now();
        let mut runtime = CrossArbAppRuntime::default();
        let cycle = runtime.run_cycle(
            CrossArbRuntimeInput {
                market_snapshots: 1,
                opportunities: 1,
                market_snapshot_rows: vec![CrossArbMarketSnapshotRow {
                    exchange: "binance".to_string(),
                    symbol: "BTC/USDT".to_string(),
                    bid_quote: "65000.0".to_string(),
                    ask_quote: "65001.0".to_string(),
                    captured_at,
                }],
                opportunity_rows: vec![CrossArbOpportunityRow {
                    opportunity_id: "opp-1".to_string(),
                    symbol: "BTC/USDT".to_string(),
                    long_exchange: "binance".to_string(),
                    short_exchange: "bitget".to_string(),
                    net_edge_bps: "4.2".to_string(),
                }],
                route_health_rows: vec![CrossArbRouteHealthRow {
                    exchange: "binance".to_string(),
                    status: "ok".to_string(),
                    last_error: None,
                }],
                ..Default::default()
            },
            captured_at,
        );

        assert_eq!(cycle.dashboard_snapshot.market_snapshots.len(), 1);
        assert_eq!(cycle.dashboard_snapshot.opportunities.len(), 1);
        assert_eq!(cycle.dashboard_snapshot.route_health.len(), 1);
        let value = serde_json::to_value(cycle).expect("cycle should serialize");
        assert_secret_free(&value);
        let serialized = serde_json::to_string(&value).expect("serialized read model");
        for forbidden in ["wss://", "ws://", "subscribe", "signature"] {
            assert!(
                !serialized.contains(forbidden),
                "app-local read model should not embed concrete websocket payload {forbidden}"
            );
        }
    }

    #[test]
    fn migrated_core_should_model_routes_and_sides() {
        let route = StrategyRoute {
            long_exchange: ExchangeId::new("binance"),
            short_exchange: ExchangeId::new("bitget"),
            maker_exchange: ExchangeId::new("binance"),
            taker_exchange: ExchangeId::new("bitget"),
            maker_side: OrderSide::Buy,
            taker_side: OrderSide::Sell,
            maker_leg_kind: MakerLegKind::LongMakerBuy,
        };
        let state = SimulatedBundleState {
            bundle_id: "bundle-1".to_string(),
            opportunity_id: "opp-1".to_string(),
            status: SimulatedBundleStatus::Observing,
            route,
            target_notional_usdt: 100.0,
            opened_at: None,
            updated_at: Utc::now(),
        };

        assert_eq!(OrderSide::Buy.opposite(), OrderSide::Sell);
        assert_eq!(state.route.long_exchange.as_str(), "binance");
        assert_eq!(state.route.taker_side, OrderSide::Sell);
    }

    #[test]
    fn migrated_core_fee_model_should_allow_negative_maker_fee() {
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            ExchangeId::new("binance"),
            ExchangeFeeRates {
                maker: -0.0001,
                taker: 0.0005,
            },
        );
        let model = FeeModel::new(
            ExchangeFeeRates {
                maker: 0.0002,
                taker: 0.0005,
            },
            overrides,
        );

        assert_eq!(
            model.fee_amount(&ExchangeId::new("binance"), FeeRole::Maker, 100.0),
            -0.01
        );
        let breakdown = model.estimate_maker_taker_round_trip(
            &ExchangeId::new("binance"),
            &ExchangeId::new("bitget"),
            100.0,
        );
        assert!((breakdown.open_fee() - 0.04).abs() < 1e-9);
        assert!((breakdown.total_normal_fee() - 0.08).abs() < 1e-9);
    }

    #[test]
    fn migrated_core_funding_should_apply_long_short_direction() {
        let model = FundingModel::default();
        let estimate = model.estimate_pair(100.0, 0.0003, 100.0, 0.0005);

        assert!((estimate.long_leg_funding + 0.03).abs() < 1e-9);
        assert!((estimate.short_leg_funding - 0.05).abs() < 1e-9);
        assert!((estimate.net_funding - 0.02).abs() < 1e-9);

        let adverse = model.estimate_pair(100.0, 0.001, 100.0, -0.001);
        assert!(adverse.net_funding < 0.0);
        assert!(adverse.dangerous);
    }

    #[test]
    fn migrated_core_funding_should_record_settlement_pnl() {
        let mut ledger = FundingSettlementLedger::default();
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let now = Utc::now();

        ledger.record(core::FundingModel::settle_leg(
            "bundle-1",
            ExchangeId::new("binance"),
            symbol.clone(),
            PositionSide::Long,
            100.0,
            0.0003,
            Some(65_000.0),
            now,
        ));
        ledger.record(core::FundingModel::settle_leg(
            "bundle-1",
            ExchangeId::new("bitget"),
            symbol,
            PositionSide::Short,
            100.0,
            0.0005,
            Some(65_010.0),
            now,
        ));

        assert_eq!(ledger.settlements().len(), 2);
        assert!((ledger.total_pnl_for_bundle("bundle-1") - 0.02).abs() < 1e-9);
        assert!((ledger.total_pnl_usdt() - 0.02).abs() < 1e-9);
    }

    fn assert_secret_free(value: &Value) {
        let encoded = value.to_string().to_ascii_lowercase();
        for forbidden in [
            "api_key",
            "secret",
            "passphrase",
            "password",
            "token",
            "credential",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "{forbidden} should not appear in serialized runtime output"
            );
        }
    }
}

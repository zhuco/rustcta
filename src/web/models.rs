use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::control::spot_control::SpotControlReadModel;
use crate::exchanges::unified::{MarketType, SymbolRule};
use crate::execution::FeeSource;
use crate::execution::{
    BalanceMismatchSeverity, BalanceReconciliationReport, LiveDryRunOrderPlan,
    OrderReconciliationConfig, OrderReconciliationResult,
};
use crate::live_preflight::{LivePreflightReport, LiveReadinessDecision, SmallLiveGateReport};
use crate::risk::{HedgePolicyReadModel, KillSwitchState};
use crate::scanner::FiveExchangeScannerReadModel;
use crate::strategies::arbitrage_core::{
    ArbitrageOpportunityAnalysis, ArbitrageOpportunityQuery, ArbitrageRelationship,
    ArbitrageStatisticsSnapshot,
};

fn default_bind_addr() -> String {
    "127.0.0.1:8080".to_string()
}

fn default_true() -> bool {
    true
}

fn default_token_env() -> String {
    "RUSTCTA_MONITOR_TOKEN".to_string()
}

fn default_recent_limit() -> usize {
    500
}

fn default_refresh_interval_ms() -> u64 {
    1_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,
    #[serde(default)]
    pub expose_publicly: bool,
    #[serde(default = "default_true")]
    pub require_token: bool,
    #[serde(default = "default_token_env")]
    pub token_env: String,
    #[serde(default = "default_recent_limit")]
    pub max_recent_opportunities: usize,
    #[serde(default = "default_recent_limit")]
    pub max_recent_trades: usize,
    #[serde(default = "default_recent_limit")]
    pub max_recent_risk_events: usize,
    #[serde(default = "default_refresh_interval_ms")]
    pub refresh_interval_ms: u64,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_addr: default_bind_addr(),
            expose_publicly: false,
            require_token: true,
            token_env: default_token_env(),
            max_recent_opportunities: default_recent_limit(),
            max_recent_trades: default_recent_limit(),
            max_recent_risk_events: default_recent_limit(),
            refresh_interval_ms: default_refresh_interval_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusView {
    pub service_name: String,
    pub version: String,
    pub started_at: DateTime<Utc>,
    pub uptime_seconds: i64,
    pub trading_mode: String,
    pub live_trading_enabled: bool,
    pub dry_run: bool,
    pub strategy_name: String,
    pub strategy_status: String,
    pub last_loop_at: Option<DateTime<Utc>>,
    pub error_count: u64,
    pub warning_count: u64,
    pub live_preflight_enabled: bool,
    pub live_readiness_decision: Option<LiveReadinessDecision>,
    pub live_preflight_last_run_at: Option<DateTime<Utc>>,
    pub live_preflight_fail_count: usize,
    pub live_preflight_warn_count: usize,
    pub kill_switch_active: bool,
    pub small_live_gate_status: Option<String>,
    pub live_dry_run_last_plan_at: Option<DateTime<Utc>>,
    pub balance_reconciliation_status: Option<BalanceMismatchSeverity>,
    pub order_reconciliation_ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyStatusView {
    pub strategy_name: String,
    pub status: String,
    pub last_loop_at: Option<DateTime<Utc>>,
    pub error_count: u64,
    pub warning_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeHealthView {
    pub exchange: String,
    pub market_type: Option<MarketType>,
    pub connected: bool,
    pub public_ws_connected: bool,
    pub private_ws_connected: Option<bool>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub last_book_update_at: Option<DateTime<Utc>>,
    pub stale_symbol_count: usize,
    pub fresh_symbol_count: usize,
    pub reconnect_count: u64,
    pub parse_error_count: u64,
    pub sequence_gap_count: u64,
    pub heartbeat_timeout_count: u64,
    pub avg_latency_ms: Option<f64>,
    pub max_latency_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookView {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub exchange_symbol: String,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread: Option<f64>,
    pub book_age_ms: i64,
    pub latency_ms: Option<i64>,
    pub is_stale: bool,
    pub stale_reason: Option<String>,
    pub source: String,
    pub sequence: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityView {
    pub timestamp: DateTime<Utc>,
    pub opportunity_id: String,
    pub symbol: String,
    pub relationship_type: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_price: f64,
    pub sell_price: f64,
    pub raw_spread_bps: f64,
    pub fee_bps: f64,
    pub net_spread_bps: f64,
    pub estimated_net_pnl: f64,
    pub accepted: bool,
    pub rejection_reason: Option<String>,
    pub buy_book_age_ms: i64,
    pub sell_book_age_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeView {
    pub timestamp: DateTime<Utc>,
    pub trade_id: String,
    pub symbol: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub quantity: f64,
    pub buy_avg_price: f64,
    pub sell_avg_price: f64,
    pub gross_pnl: f64,
    pub total_fee: f64,
    pub net_pnl: f64,
    pub execution_mode: String,
    pub paper_or_live: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryView {
    pub exchange: String,
    pub market_type: MarketType,
    pub asset: String,
    pub total: f64,
    pub available: f64,
    pub locked_by_exchange: f64,
    pub locally_reserved: f64,
    pub effective_available: f64,
    pub unmanaged_quantity: f64,
    pub valuation_usdt: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeView {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: Option<String>,
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
    pub source: FeeSource,
    pub platform_discount_enabled: bool,
    pub platform_token: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DisabledView {
    pub symbols: Vec<DisabledSymbolView>,
    pub exchanges: Vec<DisabledExchangeView>,
    pub exchange_symbols: Vec<DisabledExchangeSymbolView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisabledSymbolView {
    pub symbol: String,
    pub status: String,
    pub reason: String,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisabledExchangeView {
    pub exchange: String,
    pub status: String,
    pub reason: String,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisabledExchangeSymbolView {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub status: String,
    pub reason: String,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnmanagedPositionView {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub asset: String,
    pub quantity: f64,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecorderHealthView {
    pub book_recording_enabled: bool,
    pub opportunity_recording_enabled: bool,
    pub trade_recording_enabled: bool,
    pub dropped_book_events: u64,
    pub dropped_opportunity_events: u64,
    pub dropped_trade_events: u64,
    pub last_write_at: Option<DateTime<Utc>>,
    pub output_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskEventView {
    pub timestamp: DateTime<Utc>,
    pub event_type: String,
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    pub severity: String,
    pub reason: String,
    pub details: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConfigSummaryView {
    pub enabled_exchanges: Vec<String>,
    pub enabled_symbols: Vec<String>,
    pub trading_mode: String,
    pub live_trading_enabled: bool,
    pub dry_run: bool,
    pub max_notional_per_trade: Option<f64>,
    pub max_notional_per_symbol: Option<f64>,
    pub max_total_notional: Option<f64>,
    pub fee_config_summary: Option<String>,
    pub disabled_config_summary: Option<String>,
    pub secrets_redacted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardReadModel {
    pub service_name: String,
    pub version: String,
    pub started_at: DateTime<Utc>,
    pub trading_mode: String,
    pub live_trading_enabled: bool,
    pub dry_run: bool,
    pub strategy: StrategyStatusView,
    pub exchanges: Vec<ExchangeHealthView>,
    pub books: Vec<BookView>,
    pub spot_symbol_rules: Vec<SymbolRule>,
    pub opportunities: Vec<OpportunityView>,
    pub trades: Vec<TradeView>,
    pub inventory: Vec<InventoryView>,
    pub fees: Vec<FeeView>,
    pub disabled: DisabledView,
    pub unmanaged_positions: Vec<UnmanagedPositionView>,
    pub recorder: RecorderHealthView,
    pub risk_events: Vec<RiskEventView>,
    pub config_summary: ConfigSummaryView,
    pub live_preflight_enabled: bool,
    pub live_preflight: Option<LivePreflightReport>,
    pub live_dry_run_orders: Vec<LiveDryRunOrderPlan>,
    pub order_reconciliation_config: OrderReconciliationConfig,
    pub order_reconciliation_status: Option<OrderReconciliationResult>,
    pub balance_reconciliation: Option<BalanceReconciliationReport>,
    pub kill_switch: Option<KillSwitchState>,
    pub small_live_gate: Option<SmallLiveGateReport>,
    pub arbitrage_relationships: Vec<ArbitrageRelationship>,
    pub arbitrage_opportunities: Vec<ArbitrageOpportunityAnalysis>,
    pub arbitrage_statistics: Vec<ArbitrageStatisticsSnapshot>,
    pub spot_control: Option<SpotControlReadModel>,
    pub five_exchange_scanner: FiveExchangeScannerReadModel,
    pub hedge_policy: HedgePolicyReadModel,
}

impl Default for DashboardReadModel {
    fn default() -> Self {
        Self {
            service_name: "RustCTA".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            started_at: Utc::now(),
            trading_mode: "paper".to_string(),
            live_trading_enabled: false,
            dry_run: true,
            strategy: StrategyStatusView {
                strategy_name: "unknown".to_string(),
                status: "starting".to_string(),
                last_loop_at: None,
                error_count: 0,
                warning_count: 0,
            },
            exchanges: Vec::new(),
            books: Vec::new(),
            spot_symbol_rules: Vec::new(),
            opportunities: Vec::new(),
            trades: Vec::new(),
            inventory: Vec::new(),
            fees: Vec::new(),
            disabled: DisabledView::default(),
            unmanaged_positions: Vec::new(),
            recorder: RecorderHealthView::default(),
            risk_events: Vec::new(),
            config_summary: ConfigSummaryView {
                secrets_redacted: true,
                ..ConfigSummaryView::default()
            },
            live_preflight_enabled: false,
            live_preflight: None,
            live_dry_run_orders: Vec::new(),
            order_reconciliation_config: OrderReconciliationConfig::default(),
            order_reconciliation_status: None,
            balance_reconciliation: None,
            kill_switch: Some(KillSwitchState {
                enabled: true,
                active: false,
                reason: None,
                triggered_by: None,
                triggered_at: None,
                allow_paper_trading: true,
                allow_live_dry_run: true,
                allow_live_orders: false,
            }),
            small_live_gate: None,
            arbitrage_relationships: Vec::new(),
            arbitrage_opportunities: Vec::new(),
            arbitrage_statistics: Vec::new(),
            spot_control: None,
            five_exchange_scanner: FiveExchangeScannerReadModel::default(),
            hedge_policy: HedgePolicyReadModel::default(),
        }
    }
}

pub type ArbitrageQuery = ArbitrageOpportunityQuery;

#[derive(Debug, Clone, Deserialize)]
pub struct BookQuery {
    pub exchange: Option<String>,
    pub market_type: Option<MarketType>,
    pub symbol: Option<String>,
    #[serde(default)]
    pub stale_only: bool,
}

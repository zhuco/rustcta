use serde::{Deserialize, Serialize};

use super::EnableMode;

fn default_command_store_path() -> String {
    "data/control_commands.jsonl".to_string()
}

fn default_audit_store_path() -> String {
    "data/control_audit.jsonl".to_string()
}

fn default_lifecycle_store_path() -> String {
    "data/managed_spot_symbols.jsonl".to_string()
}

fn default_true() -> bool {
    true
}

fn default_max_book_age_ms() -> u64 {
    1_000
}

fn default_maximum_slippage_bps() -> f64 {
    30.0
}

fn default_maximum_liquidation_loss_usdt() -> f64 {
    2.0
}

fn default_reprice_interval_ms() -> u64 {
    5_000
}

fn default_order_timeout_ms() -> u64 {
    15_000
}

fn default_maximum_duration_seconds() -> u64 {
    3_600
}

fn default_minimum_order_notional_usdt() -> f64 {
    1.0
}

fn default_maximum_active_orders_per_exchange() -> usize {
    1
}

fn default_balances_interval_ms() -> u64 {
    5_000
}

fn default_open_orders_interval_ms() -> u64 {
    3_000
}

fn default_recent_fills_interval_ms() -> u64 {
    5_000
}

fn default_symbol_rules_interval_seconds() -> u64 {
    3_600
}

fn default_fee_interval_seconds() -> u64 {
    1_800
}

fn default_reconciliation_interval_ms() -> u64 {
    5_000
}

fn default_snapshot_interval_ms() -> u64 {
    1_000
}

fn default_maximum_concurrent_requests_per_exchange() -> usize {
    2
}

fn default_minimum_request_spacing_ms() -> u64 {
    100
}

fn default_jitter_ms() -> u64 {
    100
}

fn default_exponential_backoff_initial_ms() -> u64 {
    500
}

fn default_exponential_backoff_max_ms() -> u64 {
    30_000
}

fn default_pause_after_rate_limit_ms() -> u64 {
    60_000
}

fn default_max_balance_age_ms() -> u64 {
    10_000
}

fn default_max_open_orders_age_ms() -> u64 {
    10_000
}

fn default_max_fee_age_seconds() -> u64 {
    3_600
}

fn default_max_symbol_rule_age_seconds() -> u64 {
    86_400
}

fn default_max_component_time_skew_ms() -> u64 {
    10_000
}

fn default_snapshot_store_path() -> String {
    "data/spot_control_snapshots.jsonl".to_string()
}

fn default_snapshot_store_backend() -> String {
    "jsonl".to_string()
}

fn default_maximum_retained_snapshots_per_symbol() -> usize {
    10_000
}

fn default_refresh_timeout_ms() -> u64 {
    2_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotSymbolControlConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub require_write_auth: bool,
    #[serde(default = "default_command_store_path")]
    pub command_store_path: String,
    #[serde(default = "default_audit_store_path")]
    pub audit_store_path: String,
    #[serde(default = "default_lifecycle_store_path")]
    pub lifecycle_store_path: String,
    #[serde(default = "default_true")]
    pub require_liquidation_confirmation: bool,
    #[serde(default = "default_enable_mode")]
    pub default_enable_mode: EnableMode,
    #[serde(default)]
    pub allow_future_small_live: bool,
    #[serde(default)]
    pub enable_validation: EnableValidationConfig,
    #[serde(default)]
    pub disable_defaults: DisableDefaultsConfig,
    #[serde(default)]
    pub market_liquidation: MarketLiquidationConfig,
    #[serde(default)]
    pub passive_liquidation: PassiveLiquidationConfig,
    #[serde(default)]
    pub runtime_snapshot: super::SpotControlSnapshotConfig,
    #[serde(default)]
    pub runtime_publisher: SpotControlRuntimePublisherConfig,
}

impl Default for SpotSymbolControlConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            require_write_auth: true,
            command_store_path: default_command_store_path(),
            audit_store_path: default_audit_store_path(),
            lifecycle_store_path: default_lifecycle_store_path(),
            require_liquidation_confirmation: true,
            default_enable_mode: default_enable_mode(),
            allow_future_small_live: false,
            enable_validation: EnableValidationConfig::default(),
            disable_defaults: DisableDefaultsConfig::default(),
            market_liquidation: MarketLiquidationConfig::default(),
            passive_liquidation: PassiveLiquidationConfig::default(),
            runtime_snapshot: super::SpotControlSnapshotConfig::default(),
            runtime_publisher: SpotControlRuntimePublisherConfig::default(),
        }
    }
}

fn default_enable_mode() -> EnableMode {
    EnableMode::LiveDryRun
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnableValidationConfig {
    #[serde(default = "default_true")]
    pub require_fresh_books: bool,
    #[serde(default = "default_max_book_age_ms")]
    pub max_book_age_ms: u64,
    #[serde(default = "default_true")]
    pub require_fee_model: bool,
    #[serde(default = "default_true")]
    pub require_symbol_rules: bool,
    #[serde(default = "default_true")]
    pub require_inventory_for_future_small_live: bool,
}

impl Default for EnableValidationConfig {
    fn default() -> Self {
        Self {
            require_fresh_books: true,
            max_book_age_ms: default_max_book_age_ms(),
            require_fee_model: true,
            require_symbol_rules: true,
            require_inventory_for_future_small_live: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisableDefaultsConfig {
    #[serde(default = "default_true")]
    pub cancel_active_orders: bool,
    #[serde(default = "default_true")]
    pub include_managed_inventory_only: bool,
    #[serde(default = "default_maximum_slippage_bps")]
    pub maximum_slippage_bps: f64,
    #[serde(default = "default_maximum_liquidation_loss_usdt")]
    pub maximum_liquidation_loss_usdt: f64,
}

impl Default for DisableDefaultsConfig {
    fn default() -> Self {
        Self {
            cancel_active_orders: true,
            include_managed_inventory_only: true,
            maximum_slippage_bps: default_maximum_slippage_bps(),
            maximum_liquidation_loss_usdt: default_maximum_liquidation_loss_usdt(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketLiquidationConfig {
    #[serde(default = "default_true")]
    pub use_ioc_limit_only: bool,
    #[serde(default)]
    pub allow_unbounded_market_order: bool,
    #[serde(default = "default_true")]
    pub stop_on_any_failure: bool,
}

impl Default for MarketLiquidationConfig {
    fn default() -> Self {
        Self {
            use_ioc_limit_only: true,
            allow_unbounded_market_order: false,
            stop_on_any_failure: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PassiveLiquidationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_price_mode")]
    pub price_mode: String,
    #[serde(default = "default_true")]
    pub post_only_required: bool,
    #[serde(default = "default_reprice_interval_ms")]
    pub reprice_interval_ms: u64,
    #[serde(default = "default_order_timeout_ms")]
    pub order_timeout_ms: u64,
    #[serde(default = "default_maximum_duration_seconds")]
    pub maximum_duration_seconds: u64,
    #[serde(default = "default_maximum_active_orders_per_exchange")]
    pub maximum_active_orders_per_exchange: usize,
    #[serde(default = "default_maximum_liquidation_loss_usdt")]
    pub maximum_liquidation_loss_usdt: f64,
    #[serde(default = "default_minimum_order_notional_usdt")]
    pub minimum_order_notional_usdt: f64,
    #[serde(default = "default_true")]
    pub stop_on_partial_fill_reconciliation_error: bool,
}

impl Default for PassiveLiquidationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            price_mode: default_price_mode(),
            post_only_required: true,
            reprice_interval_ms: default_reprice_interval_ms(),
            order_timeout_ms: default_order_timeout_ms(),
            maximum_duration_seconds: default_maximum_duration_seconds(),
            maximum_active_orders_per_exchange: default_maximum_active_orders_per_exchange(),
            maximum_liquidation_loss_usdt: default_maximum_liquidation_loss_usdt(),
            minimum_order_notional_usdt: default_minimum_order_notional_usdt(),
            stop_on_partial_fill_reconciliation_error: true,
        }
    }
}

fn default_price_mode() -> String {
    "best_ask".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimePublisherPollingConfig {
    #[serde(default = "default_true")]
    pub poll_balances: bool,
    #[serde(default = "default_true")]
    pub poll_open_orders: bool,
    #[serde(default = "default_true")]
    pub poll_recent_fills: bool,
    #[serde(default = "default_true")]
    pub poll_symbol_rules: bool,
    #[serde(default = "default_true")]
    pub poll_fees: bool,
    #[serde(default = "default_balances_interval_ms")]
    pub balances_interval_ms: u64,
    #[serde(default = "default_open_orders_interval_ms")]
    pub open_orders_interval_ms: u64,
    #[serde(default = "default_recent_fills_interval_ms")]
    pub recent_fills_interval_ms: u64,
    #[serde(default = "default_symbol_rules_interval_seconds")]
    pub symbol_rules_interval_seconds: u64,
    #[serde(default = "default_fee_interval_seconds")]
    pub fee_interval_seconds: u64,
    #[serde(default = "default_reconciliation_interval_ms")]
    pub reconciliation_interval_ms: u64,
    #[serde(default = "default_snapshot_interval_ms")]
    pub snapshot_interval_ms: u64,
}

impl Default for RuntimePublisherPollingConfig {
    fn default() -> Self {
        Self {
            poll_balances: true,
            poll_open_orders: true,
            poll_recent_fills: true,
            poll_symbol_rules: true,
            poll_fees: true,
            balances_interval_ms: default_balances_interval_ms(),
            open_orders_interval_ms: default_open_orders_interval_ms(),
            recent_fills_interval_ms: default_recent_fills_interval_ms(),
            symbol_rules_interval_seconds: default_symbol_rules_interval_seconds(),
            fee_interval_seconds: default_fee_interval_seconds(),
            reconciliation_interval_ms: default_reconciliation_interval_ms(),
            snapshot_interval_ms: default_snapshot_interval_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimePublisherLimitsConfig {
    #[serde(default = "default_maximum_concurrent_requests_per_exchange")]
    pub maximum_concurrent_requests_per_exchange: usize,
    #[serde(default = "default_minimum_request_spacing_ms")]
    pub minimum_request_spacing_ms: u64,
    #[serde(default = "default_jitter_ms")]
    pub jitter_ms: u64,
    #[serde(default = "default_exponential_backoff_initial_ms")]
    pub exponential_backoff_initial_ms: u64,
    #[serde(default = "default_exponential_backoff_max_ms")]
    pub exponential_backoff_max_ms: u64,
    #[serde(default = "default_pause_after_rate_limit_ms")]
    pub pause_after_rate_limit_ms: u64,
}

impl Default for RuntimePublisherLimitsConfig {
    fn default() -> Self {
        Self {
            maximum_concurrent_requests_per_exchange:
                default_maximum_concurrent_requests_per_exchange(),
            minimum_request_spacing_ms: default_minimum_request_spacing_ms(),
            jitter_ms: default_jitter_ms(),
            exponential_backoff_initial_ms: default_exponential_backoff_initial_ms(),
            exponential_backoff_max_ms: default_exponential_backoff_max_ms(),
            pause_after_rate_limit_ms: default_pause_after_rate_limit_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimePublisherConsistencyConfig {
    #[serde(default = "default_max_book_age_ms")]
    pub max_book_age_ms: u64,
    #[serde(default = "default_max_balance_age_ms")]
    pub max_balance_age_ms: u64,
    #[serde(default = "default_max_open_orders_age_ms")]
    pub max_open_orders_age_ms: u64,
    #[serde(default = "default_max_fee_age_seconds")]
    pub max_fee_age_seconds: u64,
    #[serde(default = "default_max_symbol_rule_age_seconds")]
    pub max_symbol_rule_age_seconds: u64,
    #[serde(default = "default_max_component_time_skew_ms")]
    pub max_component_time_skew_ms: u64,
}

impl Default for RuntimePublisherConsistencyConfig {
    fn default() -> Self {
        Self {
            max_book_age_ms: default_max_book_age_ms(),
            max_balance_age_ms: default_max_balance_age_ms(),
            max_open_orders_age_ms: default_max_open_orders_age_ms(),
            max_fee_age_seconds: default_max_fee_age_seconds(),
            max_symbol_rule_age_seconds: default_max_symbol_rule_age_seconds(),
            max_component_time_skew_ms: default_max_component_time_skew_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimePublisherSnapshotStoreConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_snapshot_store_backend")]
    pub backend: String,
    #[serde(default = "default_snapshot_store_path")]
    pub path: String,
    #[serde(default = "default_true")]
    pub require_persistence_for_write_commands: bool,
    #[serde(default = "default_maximum_retained_snapshots_per_symbol")]
    pub maximum_retained_snapshots_per_symbol: usize,
}

impl Default for RuntimePublisherSnapshotStoreConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            backend: default_snapshot_store_backend(),
            path: default_snapshot_store_path(),
            require_persistence_for_write_commands: true,
            maximum_retained_snapshots_per_symbol: default_maximum_retained_snapshots_per_symbol(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotControlRuntimePublisherConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub polling: RuntimePublisherPollingConfig,
    #[serde(default)]
    pub limits: RuntimePublisherLimitsConfig,
    #[serde(default)]
    pub consistency: RuntimePublisherConsistencyConfig,
    #[serde(default)]
    pub snapshot_store: RuntimePublisherSnapshotStoreConfig,
    #[serde(default = "default_refresh_timeout_ms")]
    pub refresh_timeout_ms: u64,
}

impl Default for SpotControlRuntimePublisherConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            polling: RuntimePublisherPollingConfig::default(),
            limits: RuntimePublisherLimitsConfig::default(),
            consistency: RuntimePublisherConsistencyConfig::default(),
            snapshot_store: RuntimePublisherSnapshotStoreConfig::default(),
            refresh_timeout_ms: default_refresh_timeout_ms(),
        }
    }
}

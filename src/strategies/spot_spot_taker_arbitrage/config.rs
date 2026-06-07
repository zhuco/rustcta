use std::collections::{BTreeSet, HashMap};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::control::spot_control::SpotSymbolControlConfig;
use crate::exchanges::bitget::FeeOverride as BitgetFeeOverride;
use crate::exchanges::coinex::FeeOverride as CoinExFeeOverride;
use crate::exchanges::gateio::FeeOverride as GateIoFeeOverride;
use crate::exchanges::kucoin::KuCoinSpotConfig;
use crate::exchanges::mexc::FeeOverride as MexcFeeOverride;
use crate::execution::{LiveDryRunConfig, OrderReconciliationConfig};
use crate::live_preflight::LivePreflightConfig;
use crate::live_preflight::SmallLiveGateConfig;
use crate::risk::KillSwitchConfig;
use crate::strategies::arbitrage_core::ArbitrageScannerConfig;
use crate::web::MonitoringConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpotSpotTakerArbitrageConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_trading_mode")]
    pub trading_mode: String,
    pub exchanges: Vec<String>,
    pub symbols: Vec<String>,
    #[serde(default = "default_quote_asset")]
    pub quote_asset: String,
    pub max_notional_per_trade: f64,
    pub min_notional_per_trade: f64,
    pub max_notional_per_symbol: f64,
    pub max_total_notional: f64,
    #[serde(default = "default_max_enabled_arbitrage_symbols")]
    pub max_enabled_arbitrage_symbols: usize,
    #[serde(default = "default_initial_entry_notional_usdt")]
    pub initial_entry_notional_usdt: f64,
    #[serde(default = "default_spread_duration_threshold_seconds")]
    pub spread_duration_threshold_seconds: u64,
    #[serde(default = "default_entry_order_timeout_seconds")]
    pub entry_order_timeout_seconds: u64,
    #[serde(default = "default_entry_maker_retries")]
    pub entry_maker_retries: u32,
    #[serde(default = "default_active_taker_notional_usdt")]
    pub active_taker_notional_usdt: f64,
    #[serde(default = "default_inactivity_exit_seconds")]
    pub inactivity_exit_seconds: u64,
    #[serde(default = "default_exit_order_timeout_seconds")]
    pub exit_order_timeout_seconds: u64,
    #[serde(default = "default_exit_maker_retries")]
    pub exit_maker_retries: u32,
    #[serde(default = "default_min_raw_spread_bps")]
    pub min_raw_spread_bps: f64,
    pub min_net_spread_bps: f64,
    #[serde(default = "default_max_raw_spread_bps")]
    pub max_raw_spread_bps: f64,
    pub taker_fee_bps_override: Option<f64>,
    #[serde(default = "default_fee_config_path")]
    pub fee_config_path: String,
    #[serde(default = "default_disabled_registry_path")]
    pub disabled_registry_path: String,
    #[serde(default)]
    pub slippage_bps: f64,
    #[serde(default)]
    pub safety_buffer_bps: f64,
    #[serde(default = "default_stale_book_ms")]
    pub stale_book_ms: u64,
    #[serde(default = "default_max_book_latency_ms")]
    pub max_book_latency_ms: u64,
    pub min_depth_notional: f64,
    #[serde(default = "default_one")]
    pub max_active_opportunities_per_symbol: usize,
    #[serde(default = "default_cooldown_ms")]
    pub cooldown_ms_after_trade: u64,
    #[serde(default)]
    pub enable_database_recording: bool,
    #[serde(default = "default_true")]
    pub enable_csv_recording: bool,
    #[serde(default = "default_report_interval_seconds")]
    pub report_interval_seconds: u64,
    #[serde(default = "default_true")]
    pub dry_run: bool,
    #[serde(default)]
    pub live_trading_enabled: bool,
    #[serde(default = "default_scan_interval_ms")]
    pub scan_interval_ms: u64,
    #[serde(default = "default_orderbook_depth")]
    pub orderbook_depth: u16,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_max_daily_loss")]
    pub max_daily_loss: f64,
    #[serde(default = "default_max_trade_loss")]
    pub max_trade_loss: f64,
    #[serde(default = "default_max_consecutive_rejections")]
    pub max_consecutive_rejections: u32,
    #[serde(default = "default_jsonl_path")]
    pub jsonl_path: String,
    #[serde(default = "default_csv_path")]
    pub csv_path: String,
    #[serde(default)]
    pub market_data_mode: MarketDataMode,
    #[serde(default)]
    pub websocket: WebsocketMarketDataConfig,
    #[serde(default)]
    pub rest_polling: RestPollingMarketDataConfig,
    #[serde(default)]
    pub replay: ReplayConfig,
    #[serde(default)]
    pub monitoring: MonitoringConfig,
    #[serde(default)]
    pub spot_symbol_control: SpotSymbolControlConfig,
    #[serde(default)]
    pub live_preflight: LivePreflightConfig,
    #[serde(default)]
    pub live_dry_run: LiveDryRunConfig,
    #[serde(default)]
    pub order_reconciliation: OrderReconciliationConfig,
    #[serde(default)]
    pub kill_switch: KillSwitchConfig,
    #[serde(default)]
    pub small_live_gate: SmallLiveGateConfig,
    #[serde(default)]
    pub arbitrage_scanner: ArbitrageScannerConfig,
    #[serde(default)]
    pub inventory_rebalance: InventoryRebalanceConfig,
    #[serde(default)]
    pub initial_balances: HashMap<String, HashMap<String, f64>>,
    #[serde(default)]
    pub mexc: VenueRuntimeConfig,
    #[serde(default)]
    pub coinex: VenueRuntimeConfig,
    #[serde(default)]
    pub gateio: VenueRuntimeConfig,
    #[serde(default)]
    pub bitget: VenueRuntimeConfig,
    #[serde(default)]
    pub kucoin: KuCoinSpotConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InventoryRebalanceConfig {
    pub enabled: bool,
    pub allow_market_rebalance: bool,
    pub allow_transfer_rebalance: bool,
    pub allow_profit_covered_recovery: bool,
    pub allow_emergency_recovery: bool,
    pub allow_auto_initial_entry: bool,
    pub allow_auto_exit: bool,
    pub allow_lossy_rebalance_when_blocked: bool,
    pub target_total_notional_usdt: f64,
    pub min_cycles_buffer: u32,
    pub target_cycles_buffer: u32,
    pub no_loss_safety_bps: f64,
    pub slippage_buffer_bps: f64,
    pub profit_floor_usdt: f64,
    pub max_profit_use_ratio: f64,
    pub max_rebalance_notional_usdt: f64,
    pub max_blocked_rebalance_loss_usdt: f64,
    pub emergency_after_seconds: u64,
    pub emergency_max_exposure_usdt: f64,
    pub emergency_adverse_bps: f64,
}

impl Default for InventoryRebalanceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allow_market_rebalance: true,
            allow_transfer_rebalance: false,
            allow_profit_covered_recovery: false,
            allow_emergency_recovery: false,
            allow_auto_initial_entry: false,
            allow_auto_exit: false,
            allow_lossy_rebalance_when_blocked: false,
            target_total_notional_usdt: 20.0,
            min_cycles_buffer: 3,
            target_cycles_buffer: 5,
            no_loss_safety_bps: 8.0,
            slippage_buffer_bps: 5.0,
            profit_floor_usdt: 0.02,
            max_profit_use_ratio: 0.5,
            max_rebalance_notional_usdt: 5.0,
            max_blocked_rebalance_loss_usdt: 0.05,
            emergency_after_seconds: 20,
            emergency_max_exposure_usdt: 15.0,
            emergency_adverse_bps: 30.0,
        }
    }
}

impl InventoryRebalanceConfig {
    pub fn required_no_loss_bps(&self) -> f64 {
        self.no_loss_safety_bps.max(0.0) + self.slippage_buffer_bps.max(0.0)
    }

    pub fn available_profit_budget(&self, symbol_realized_pnl: f64) -> f64 {
        if !self.allow_profit_covered_recovery {
            return 0.0;
        }
        ((symbol_realized_pnl - self.profit_floor_usdt).max(0.0)
            * self.max_profit_use_ratio.clamp(0.0, 1.0))
        .max(0.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataMode {
    RestPolling,
    #[default]
    WebsocketCache,
    Replay,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketMarketDataConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub exchanges: Vec<String>,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default = "default_orderbook_depth")]
    pub depth: u16,
    #[serde(default = "default_ws_stale_book_ms")]
    pub stale_book_ms: u64,
    #[serde(default = "default_reconnect_interval_ms")]
    pub reconnect_interval_ms: u64,
    #[serde(default)]
    pub max_reconnect_attempts: u32,
    #[serde(default)]
    pub record_books: bool,
    #[serde(default = "default_book_recording_path")]
    pub book_recording_path: String,
    #[serde(default = "default_true")]
    pub record_top_of_book_only: bool,
    #[serde(default = "default_heartbeat_timeout_ms")]
    pub heartbeat_timeout_ms: u64,
    #[serde(default)]
    pub log_raw_messages: bool,
}

impl Default for WebsocketMarketDataConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            exchanges: Vec::new(),
            symbols: Vec::new(),
            depth: default_orderbook_depth(),
            stale_book_ms: default_ws_stale_book_ms(),
            reconnect_interval_ms: default_reconnect_interval_ms(),
            max_reconnect_attempts: 0,
            record_books: false,
            book_recording_path: default_book_recording_path(),
            record_top_of_book_only: true,
            heartbeat_timeout_ms: default_heartbeat_timeout_ms(),
            log_raw_messages: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestPollingMarketDataConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_scan_interval_ms")]
    pub interval_ms: u64,
}

impl Default for RestPollingMarketDataConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_ms: default_scan_interval_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_book_recording_path")]
    pub input_path: String,
    #[serde(default = "default_replay_speed")]
    pub speed: String,
    #[serde(default)]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default = "default_replay_output_path")]
    pub output_path: String,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            input_path: default_book_recording_path(),
            speed: default_replay_speed(),
            start_time: None,
            end_time: None,
            output_path: default_replay_output_path(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueRuntimeConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub api_secret: String,
    #[serde(default)]
    pub passphrase: String,
    #[serde(default)]
    pub base_url: String,
    #[serde(default)]
    pub websocket_url: String,
    #[serde(default)]
    pub fee_override: Option<VenueFeeOverride>,
    #[serde(default)]
    pub symbol_mappings: HashMap<String, String>,
}

impl Default for VenueRuntimeConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            api_secret: String::new(),
            passphrase: String::new(),
            base_url: String::new(),
            websocket_url: String::new(),
            fee_override: None,
            symbol_mappings: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct VenueFeeOverride {
    pub maker_fee_rate: f64,
    pub taker_fee_rate: f64,
}

impl From<VenueFeeOverride> for MexcFeeOverride {
    fn from(value: VenueFeeOverride) -> Self {
        Self {
            maker_fee_rate: value.maker_fee_rate,
            taker_fee_rate: value.taker_fee_rate,
        }
    }
}

impl From<VenueFeeOverride> for CoinExFeeOverride {
    fn from(value: VenueFeeOverride) -> Self {
        Self {
            maker_fee_rate: value.maker_fee_rate,
            taker_fee_rate: value.taker_fee_rate,
        }
    }
}

impl From<VenueFeeOverride> for GateIoFeeOverride {
    fn from(value: VenueFeeOverride) -> Self {
        Self {
            maker_fee_rate: value.maker_fee_rate,
            taker_fee_rate: value.taker_fee_rate,
        }
    }
}

impl From<VenueFeeOverride> for BitgetFeeOverride {
    fn from(value: VenueFeeOverride) -> Self {
        Self {
            maker_fee_rate: value.maker_fee_rate,
            taker_fee_rate: value.taker_fee_rate,
        }
    }
}

impl SpotSpotTakerArbitrageConfig {
    pub fn validate_safe_mode(&self) -> Result<()> {
        let mode = self.trading_mode.trim().to_ascii_lowercase();
        if mode != "paper" && mode != "live_dry_run" && mode != "live" {
            return Err(anyhow!(
                "spot_spot_taker_arbitrage only supports trading_mode: paper, live_dry_run, or live"
            ));
        }
        if mode == "live" {
            if !self.live_trading_enabled {
                return Err(anyhow!(
                    "trading_mode=live requires live_trading_enabled=true"
                ));
            }
            if self.dry_run {
                return Err(anyhow!("trading_mode=live requires dry_run=false"));
            }
        } else if self.live_trading_enabled {
            return Err(anyhow!(
                "live_trading_enabled=true requires trading_mode=live"
            ));
        } else if !self.dry_run {
            return Err(anyhow!("paper and live_dry_run modes require dry_run=true"));
        }
        self.live_dry_run.validate()?;
        if self.inventory_rebalance.enabled {
            if self.inventory_rebalance.target_total_notional_usdt <= 0.0
                || self.inventory_rebalance.max_profit_use_ratio < 0.0
                || self.inventory_rebalance.max_profit_use_ratio > 1.0
                || self.inventory_rebalance.max_rebalance_notional_usdt <= 0.0
                || self.inventory_rebalance.target_cycles_buffer
                    < self.inventory_rebalance.min_cycles_buffer
            {
                return Err(anyhow!("invalid inventory_rebalance settings"));
            }
        }
        if mode == "live_dry_run" && !self.live_dry_run.enabled {
            return Err(anyhow!(
                "trading_mode=live_dry_run requires live_dry_run.enabled=true"
            ));
        }
        let exchanges = self
            .exchanges
            .iter()
            .map(|exchange| exchange.trim().to_ascii_lowercase())
            .collect::<Vec<_>>();
        let has_mexc_coinex = exchanges.iter().any(|exchange| exchange == "mexc")
            && exchanges.iter().any(|exchange| exchange == "coinex");
        let has_gateio_bitget = exchanges
            .iter()
            .any(|exchange| matches!(exchange.as_str(), "gateio" | "gate" | "gate.io"))
            && exchanges.iter().any(|exchange| exchange == "bitget");
        if !has_mexc_coinex && !has_gateio_bitget {
            return Err(anyhow!(
                "exchanges must include either mexc+coinex or gateio+bitget"
            ));
        }
        if self.symbols.is_empty() {
            return Err(anyhow!("symbols must not be empty"));
        }
        if self.max_notional_per_trade <= 0.0
            || self.min_notional_per_trade <= 0.0
            || self.max_notional_per_trade < self.min_notional_per_trade
        {
            return Err(anyhow!("invalid per-trade notional limits"));
        }
        if self.max_notional_per_symbol <= 0.0 || self.max_total_notional <= 0.0 {
            return Err(anyhow!("max notional limits must be positive"));
        }
        if self.initial_entry_notional_usdt <= 0.0
            || self.active_taker_notional_usdt <= 0.0
            || self.spread_duration_threshold_seconds == 0
            || self.entry_order_timeout_seconds == 0
            || self.inactivity_exit_seconds == 0
            || self.exit_order_timeout_seconds == 0
        {
            return Err(anyhow!("invalid spot arbitrage lifecycle settings"));
        }
        if self.max_raw_spread_bps <= 0.0 {
            return Err(anyhow!("max_raw_spread_bps must be positive"));
        }
        if mode == "live" && self.max_raw_spread_bps > 1_000.0 + 1e-12 {
            return Err(anyhow!(
                "trading_mode=live requires max_raw_spread_bps <= 1000 so raw spot spreads above 10% are filtered"
            ));
        }
        if mode == "live"
            && (self.max_enabled_arbitrage_symbols == 0 || self.max_enabled_arbitrage_symbols > 5)
        {
            return Err(anyhow!(
                "trading_mode=live requires max_enabled_arbitrage_symbols between 1 and 5"
            ));
        }
        if mode == "live" {
            if !self.small_live_gate.enabled || !self.small_live_gate.explicit_live_confirmation {
                return Err(anyhow!(
                    "trading_mode=live requires small_live_gate.enabled=true and explicit_live_confirmation=true"
                ));
            }
            if self.small_live_gate.max_notional_per_order <= 0.0
                || self.small_live_gate.max_total_notional <= 0.0
                || self.small_live_gate.max_total_notional
                    < self.small_live_gate.max_notional_per_order
            {
                return Err(anyhow!("invalid small_live_gate notional limits"));
            }
            let monitored_symbols = normalized_symbol_set(&self.symbols);
            let live_symbols = normalized_symbol_set(&self.small_live_gate.enabled_symbols);
            if live_symbols.is_empty() {
                return Err(anyhow!(
                    "trading_mode=live requires small_live_gate.enabled_symbols to be explicit"
                ));
            }
            if !live_symbols.is_subset(&monitored_symbols) {
                return Err(anyhow!(
                    "small_live_gate.enabled_symbols must be a subset of monitored symbols"
                ));
            }
            if self.live_preflight.enabled
                && !self.live_preflight.symbols.is_empty()
                && normalized_symbol_set(&self.live_preflight.symbols) != live_symbols
            {
                return Err(anyhow!(
                    "live_preflight.symbols must match small_live_gate.enabled_symbols in live mode"
                ));
            }
        }
        if self.market_data_mode == MarketDataMode::Replay && !self.replay.enabled {
            return Err(anyhow!(
                "market_data_mode=replay requires replay.enabled=true"
            ));
        }
        if self.market_data_mode == MarketDataMode::WebsocketCache && !self.websocket.enabled {
            return Err(anyhow!(
                "market_data_mode=websocket_cache requires websocket.enabled=true"
            ));
        }
        if self.market_data_mode == MarketDataMode::WebsocketCache
            && !self.websocket.symbols.is_empty()
            && normalized_symbol_set(&self.websocket.symbols)
                != normalized_symbol_set(&self.symbols)
        {
            return Err(anyhow!(
                "websocket.symbols must be empty or match symbols exactly; otherwise scanned symbols may not be subscribed"
            ));
        }
        Ok(())
    }

    pub fn taker_fee_bps(&self, exchange: &str) -> f64 {
        if let Some(value) = self.taker_fee_bps_override {
            return value;
        }
        let fee = match exchange {
            "mexc" => self.mexc.fee_override,
            "coinex" => self.coinex.fee_override,
            "gateio" | "gate" | "gate.io" => self.gateio.fee_override,
            "bitget" => self.bitget.fee_override,
            _ => None,
        };
        fee.map(|fee| fee.taker_fee_rate * 10_000.0).unwrap_or(10.0)
    }

    pub fn recording_config(&self) -> RecorderConfig {
        RecorderConfig {
            enable_database_recording: self.enable_database_recording,
            enable_csv_recording: self.enable_csv_recording,
            jsonl_path: self.jsonl_path.clone(),
            csv_path: self.csv_path.clone(),
        }
    }
}

fn normalized_symbol_set(symbols: &[String]) -> BTreeSet<String> {
    symbols
        .iter()
        .map(|symbol| {
            symbol
                .trim()
                .replace(['-', '_', '/'], "")
                .to_ascii_uppercase()
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct RecorderConfig {
    pub enable_database_recording: bool,
    pub enable_csv_recording: bool,
    pub jsonl_path: String,
    pub csv_path: String,
}

fn default_true() -> bool {
    true
}
fn default_trading_mode() -> String {
    "paper".to_string()
}
fn default_quote_asset() -> String {
    "USDT".to_string()
}
fn default_min_raw_spread_bps() -> f64 {
    30.0
}
fn default_max_raw_spread_bps() -> f64 {
    1_000.0
}
fn default_fee_config_path() -> String {
    "config/fees.yml".to_string()
}
fn default_disabled_registry_path() -> String {
    "config/disabled_symbols.yml".to_string()
}
fn default_stale_book_ms() -> u64 {
    10_000
}
fn default_max_book_latency_ms() -> u64 {
    10_000
}
fn default_one() -> usize {
    1
}

fn default_max_enabled_arbitrage_symbols() -> usize {
    5
}

fn default_initial_entry_notional_usdt() -> f64 {
    10.0
}
fn default_spread_duration_threshold_seconds() -> u64 {
    30
}
fn default_entry_order_timeout_seconds() -> u64 {
    30
}
fn default_entry_maker_retries() -> u32 {
    5
}
fn default_active_taker_notional_usdt() -> f64 {
    1.2
}
fn default_inactivity_exit_seconds() -> u64 {
    3_600
}
fn default_exit_order_timeout_seconds() -> u64 {
    60
}
fn default_exit_maker_retries() -> u32 {
    10
}
fn default_cooldown_ms() -> u64 {
    5_000
}
fn default_report_interval_seconds() -> u64 {
    30
}
fn default_scan_interval_ms() -> u64 {
    1_000
}
fn default_orderbook_depth() -> u16 {
    5
}
fn default_request_timeout_ms() -> u64 {
    10_000
}
fn default_max_daily_loss() -> f64 {
    100.0
}
fn default_max_trade_loss() -> f64 {
    10.0
}
fn default_max_consecutive_rejections() -> u32 {
    20
}
fn default_jsonl_path() -> String {
    "logs/spot_spot_taker_arbitrage/events.jsonl".to_string()
}
fn default_csv_path() -> String {
    "logs/spot_spot_taker_arbitrage/events.csv".to_string()
}
fn default_ws_stale_book_ms() -> u64 {
    1_000
}
fn default_reconnect_interval_ms() -> u64 {
    1_000
}
fn default_book_recording_path() -> String {
    "data/market_books.jsonl".to_string()
}
fn default_heartbeat_timeout_ms() -> u64 {
    10_000
}
fn default_replay_speed() -> String {
    "max".to_string()
}
fn default_replay_output_path() -> String {
    "data/replay_report.jsonl".to_string()
}

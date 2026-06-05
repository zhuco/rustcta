use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::control::spot_control::SpotSymbolControlConfig;
use crate::exchanges::bitget::FeeOverride as BitgetFeeOverride;
use crate::exchanges::coinex::FeeOverride as CoinExFeeOverride;
use crate::exchanges::gateio::FeeOverride as GateIoFeeOverride;
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
    pub min_net_spread_bps: f64,
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
    pub initial_balances: HashMap<String, HashMap<String, f64>>,
    #[serde(default)]
    pub mexc: VenueRuntimeConfig,
    #[serde(default)]
    pub coinex: VenueRuntimeConfig,
    #[serde(default)]
    pub gateio: VenueRuntimeConfig,
    #[serde(default)]
    pub bitget: VenueRuntimeConfig,
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
            exchanges: vec!["mexc".to_string(), "coinex".to_string()],
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
        if mode != "paper" && mode != "live_dry_run" {
            return Err(anyhow!(
                "spot_spot_taker_arbitrage only supports trading_mode: paper or live_dry_run"
            ));
        }
        if self.live_trading_enabled {
            return Err(anyhow!(
                "spot_spot_taker_arbitrage refuses to start with live_trading_enabled=true"
            ));
        }
        if !self.dry_run {
            return Err(anyhow!(
                "spot_spot_taker_arbitrage requires dry_run=true in this implementation"
            ));
        }
        self.live_dry_run.validate()?;
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

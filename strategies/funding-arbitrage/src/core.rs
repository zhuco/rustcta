use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingCoreConfig {
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default)]
    pub universe: UniverseConfig,
    #[serde(default)]
    pub selection: SelectionConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub notifications: NotificationConfig,
}

impl Default for FundingCoreConfig {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            universe: UniverseConfig::default(),
            selection: SelectionConfig::default(),
            execution: ExecutionConfig::default(),
            notifications: NotificationConfig::default(),
        }
    }
}

impl FundingCoreConfig {
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        if self.universe.enabled_exchanges.is_empty() {
            return Err(ConfigValidationError::NoExchanges);
        }
        if self.selection.per_exchange_limit == 0 {
            return Err(ConfigValidationError::InvalidPerExchangeLimit);
        }
        if !self.selection.min_funding_rate.is_finite() || self.selection.min_funding_rate >= 0.0 {
            return Err(ConfigValidationError::InvalidFundingThreshold);
        }
        if self.selection.max_funding_snapshot_age_ms <= 0 {
            return Err(ConfigValidationError::InvalidSnapshotAge);
        }
        if !matches!(
            self.mode.trim().to_ascii_lowercase().as_str(),
            "observe" | "live"
        ) {
            return Err(ConfigValidationError::InvalidMode);
        }
        if self.is_live_mode() {
            if !self.selection.require_next_funding_time
                || self.selection.max_seconds_to_settlement_at_scan.is_none()
            {
                return Err(ConfigValidationError::InvalidLiveSettlementWindow);
            }
            self.execution.validate()?;
        }
        Ok(())
    }

    pub fn is_live_mode(&self) -> bool {
        self.mode.trim().eq_ignore_ascii_case("live")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniverseConfig {
    #[serde(default = "default_exchanges")]
    pub enabled_exchanges: Vec<String>,
    #[serde(default = "default_quote_asset")]
    pub quote_asset: String,
    #[serde(default = "default_contract_type")]
    pub contract_type: String,
    #[serde(default)]
    pub symbol_allowlist: Vec<String>,
    #[serde(default)]
    pub symbol_blocklist: Vec<String>,
}

impl Default for UniverseConfig {
    fn default() -> Self {
        Self {
            enabled_exchanges: default_exchanges(),
            quote_asset: default_quote_asset(),
            contract_type: default_contract_type(),
            symbol_allowlist: Vec::new(),
            symbol_blocklist: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectionConfig {
    #[serde(default = "default_per_exchange_limit")]
    pub per_exchange_limit: usize,
    #[serde(default = "default_min_funding_rate")]
    pub min_funding_rate: f64,
    #[serde(default = "default_true")]
    pub require_next_funding_time: bool,
    #[serde(default = "default_max_funding_snapshot_age_ms")]
    pub max_funding_snapshot_age_ms: i64,
    #[serde(default)]
    pub min_seconds_to_settlement_at_scan: Option<i64>,
    #[serde(default)]
    pub max_seconds_to_settlement_at_scan: Option<i64>,
}

impl Default for SelectionConfig {
    fn default() -> Self {
        Self {
            per_exchange_limit: default_per_exchange_limit(),
            min_funding_rate: default_min_funding_rate(),
            require_next_funding_time: true,
            max_funding_snapshot_age_ms: default_max_funding_snapshot_age_ms(),
            min_seconds_to_settlement_at_scan: None,
            max_seconds_to_settlement_at_scan: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_scan_minute")]
    pub scan_minute: u32,
    #[serde(default = "default_notional_usdt")]
    pub notional_usdt: f64,
    #[serde(default = "default_open_seconds_before_settlement")]
    pub open_seconds_before_settlement: i64,
    #[serde(default = "default_close_seconds_after_settlement")]
    pub close_seconds_after_settlement: i64,
    #[serde(default = "default_close_limit_timeout_secs")]
    pub close_limit_timeout_secs: u64,
    #[serde(default = "default_close_limit_max_retries")]
    pub close_limit_max_retries: u32,
    #[serde(default = "default_max_slippage_pct")]
    pub max_slippage_pct: f64,
    #[serde(default = "default_live_position_side")]
    pub position_side: String,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            scan_minute: default_scan_minute(),
            notional_usdt: default_notional_usdt(),
            open_seconds_before_settlement: default_open_seconds_before_settlement(),
            close_seconds_after_settlement: default_close_seconds_after_settlement(),
            close_limit_timeout_secs: default_close_limit_timeout_secs(),
            close_limit_max_retries: default_close_limit_max_retries(),
            max_slippage_pct: default_max_slippage_pct(),
            position_side: default_live_position_side(),
        }
    }
}

impl ExecutionConfig {
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        if self.scan_minute > 59 {
            return Err(ConfigValidationError::InvalidExecution);
        }
        if !self.notional_usdt.is_finite() || self.notional_usdt <= 0.0 {
            return Err(ConfigValidationError::InvalidExecution);
        }
        if self.notional_usdt > 20.0 {
            return Err(ConfigValidationError::InvalidExecution);
        }
        if self.open_seconds_before_settlement < 0 || self.close_seconds_after_settlement < 0 {
            return Err(ConfigValidationError::InvalidExecution);
        }
        if self.close_limit_timeout_secs == 0 || self.close_limit_timeout_secs > 1800 {
            return Err(ConfigValidationError::InvalidExecution);
        }
        if self.close_limit_max_retries == 0 || self.close_limit_max_retries > 10 {
            return Err(ConfigValidationError::InvalidExecution);
        }
        if !self.max_slippage_pct.is_finite()
            || self.max_slippage_pct < 0.0
            || self.max_slippage_pct > 0.02
        {
            return Err(ConfigValidationError::InvalidExecution);
        }
        if !matches!(
            self.position_side.trim().to_ascii_lowercase().as_str(),
            "long"
        ) {
            return Err(ConfigValidationError::InvalidExecution);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_wecom_webhook_env")]
    pub wecom_webhook_env: String,
    #[serde(default)]
    pub wecom_webhook_url: Option<String>,
}

impl Default for NotificationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            wecom_webhook_env: default_wecom_webhook_env(),
            wecom_webhook_url: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigValidationError {
    InvalidMode,
    NoExchanges,
    InvalidPerExchangeLimit,
    InvalidFundingThreshold,
    InvalidSnapshotAge,
    InvalidExecution,
    InvalidLiveSettlementWindow,
}

impl std::fmt::Display for ConfigValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::InvalidMode => "mode must be observe or live",
            Self::NoExchanges => "funding-rate arbitrage requires at least one enabled exchange",
            Self::InvalidPerExchangeLimit => {
                "selection.per_exchange_limit must be greater than zero"
            }
            Self::InvalidFundingThreshold => {
                "selection.min_funding_rate must be a finite negative number"
            }
            Self::InvalidSnapshotAge => {
                "selection.max_funding_snapshot_age_ms must be greater than zero"
            }
            Self::InvalidExecution => "execution config is invalid",
            Self::InvalidLiveSettlementWindow => {
                "live mode requires require_next_funding_time=true and max_seconds_to_settlement_at_scan"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ConfigValidationError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingSymbol {
    pub base: String,
    pub quote: String,
}

impl FundingSymbol {
    pub fn new(base: impl Into<String>, quote: impl Into<String>) -> Self {
        Self {
            base: base.into(),
            quote: quote.into(),
        }
    }

    pub fn as_pair(&self) -> String {
        format!(
            "{}/{}",
            self.base.to_ascii_uppercase(),
            self.quote.to_ascii_uppercase()
        )
    }
}

impl std::fmt::Display for FundingSymbol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.as_pair())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FundingSnapshot {
    pub exchange: String,
    pub canonical_symbol: FundingSymbol,
    pub exchange_symbol: Option<String>,
    pub funding_rate: f64,
    pub predicted_funding_rate: Option<f64>,
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub recv_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FundingInstrument {
    pub exchange: String,
    pub canonical_symbol: FundingSymbol,
    pub exchange_symbol: String,
    pub contract_type: String,
    pub status: String,
    pub contract_size: f64,
    pub quantity_step: f64,
    pub min_qty: f64,
    pub min_notional: f64,
}

impl FundingInstrument {
    pub fn is_tradeable_usdt_perpetual(&self) -> bool {
        self.status.eq_ignore_ascii_case("trading")
            && self
                .contract_type
                .to_ascii_lowercase()
                .contains("perpetual")
            && self.canonical_symbol.quote.eq_ignore_ascii_case("USDT")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FundingCandidate {
    pub exchange: String,
    pub canonical_symbol: FundingSymbol,
    pub exchange_symbol: Option<String>,
    pub funding_rate: f64,
    pub funding_rate_pct: f64,
    pub predicted_funding_rate: Option<f64>,
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub seconds_to_settlement: Option<i64>,
    pub snapshot_age_ms: i64,
    pub qualifies: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExchangeFundingSelection {
    pub exchange: String,
    pub selected: Option<FundingCandidate>,
    pub scanned_symbols: usize,
    pub funding_snapshots: usize,
    pub eligible_candidates: usize,
    pub skipped_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExchangeScanError {
    pub exchange: String,
    pub stage: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FundingScanReport {
    pub generated_at: DateTime<Utc>,
    pub threshold: f64,
    pub threshold_pct: f64,
    pub selections: Vec<ExchangeFundingSelection>,
    pub errors: Vec<ExchangeScanError>,
}

pub fn select_exchange_funding(
    exchange: impl Into<String>,
    snapshots: impl IntoIterator<Item = FundingSnapshot>,
    instruments: &[FundingInstrument],
    config: &FundingCoreConfig,
    now: DateTime<Utc>,
) -> ExchangeFundingSelection {
    let exchange = exchange.into();
    let scanned_symbols = eligible_symbols(instruments, config).len();
    let snapshots = snapshots.into_iter().collect::<Vec<_>>();
    let snapshot_count = snapshots.len();
    let mut candidates = snapshots
        .into_iter()
        .filter(|snapshot| symbol_allowed(&snapshot.canonical_symbol, config))
        .filter_map(|snapshot| candidate_from_snapshot(snapshot, config, now))
        .filter(|candidate| candidate_orderable(candidate, instruments, config))
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.funding_rate
            .partial_cmp(&right.funding_rate)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let eligible_candidates = candidates
        .iter()
        .filter(|candidate| candidate.qualifies)
        .count();
    let selected = candidates.into_iter().find(|candidate| candidate.qualifies);
    let skipped_reason = selected
        .is_none()
        .then(|| selection_skip_reason(snapshot_count, eligible_candidates));

    ExchangeFundingSelection {
        exchange,
        selected,
        scanned_symbols,
        funding_snapshots: snapshot_count,
        eligible_candidates,
        skipped_reason,
    }
}

pub fn candidate_from_snapshot(
    snapshot: FundingSnapshot,
    config: &FundingCoreConfig,
    now: DateTime<Utc>,
) -> Option<FundingCandidate> {
    if snapshot
        .exchange_symbol
        .as_ref()
        .is_some_and(|symbol| !exchange_symbol_supported(symbol))
    {
        return None;
    }

    let snapshot_age_ms = now
        .signed_duration_since(snapshot.recv_ts)
        .num_milliseconds()
        .max(0);
    if snapshot_age_ms > config.selection.max_funding_snapshot_age_ms {
        return None;
    }

    let seconds_to_settlement = snapshot
        .next_funding_time
        .map(|time| time.signed_duration_since(now).num_seconds());
    if config.selection.require_next_funding_time && seconds_to_settlement.is_none() {
        return None;
    }
    if let Some(min_seconds) = config.selection.min_seconds_to_settlement_at_scan {
        if seconds_to_settlement.is_some_and(|seconds| seconds < min_seconds) {
            return None;
        }
    }
    if let Some(max_seconds) = config.selection.max_seconds_to_settlement_at_scan {
        if seconds_to_settlement.is_some_and(|seconds| seconds > max_seconds) {
            return None;
        }
    }

    let qualifies = snapshot.funding_rate <= config.selection.min_funding_rate;
    Some(FundingCandidate {
        exchange: snapshot.exchange,
        canonical_symbol: snapshot.canonical_symbol,
        exchange_symbol: snapshot.exchange_symbol,
        funding_rate: snapshot.funding_rate,
        funding_rate_pct: snapshot.funding_rate * 100.0,
        predicted_funding_rate: snapshot.predicted_funding_rate,
        mark_price: snapshot.mark_price,
        index_price: snapshot.index_price,
        next_funding_time: snapshot.next_funding_time,
        seconds_to_settlement,
        snapshot_age_ms,
        qualifies,
    })
}

pub fn symbol_allowed(symbol: &FundingSymbol, config: &FundingCoreConfig) -> bool {
    if !symbol
        .quote
        .eq_ignore_ascii_case(&config.universe.quote_asset)
    {
        return false;
    }
    if !canonical_symbol_supported(symbol) {
        return false;
    }
    let allowlist = normalized_symbol_set(&config.universe.symbol_allowlist);
    let blocklist = normalized_symbol_set(&config.universe.symbol_blocklist);
    let normalized = normalize_symbol(&symbol.as_pair());
    if blocklist.contains(&normalized) {
        return false;
    }
    allowlist.is_empty() || allowlist.contains(&normalized)
}

pub fn candidate_orderable(
    candidate: &FundingCandidate,
    instruments: &[FundingInstrument],
    config: &FundingCoreConfig,
) -> bool {
    let Some(instrument) = instruments.iter().find(|instrument| {
        instrument.is_tradeable_usdt_perpetual()
            && instrument.canonical_symbol == candidate.canonical_symbol
            && candidate
                .exchange_symbol
                .as_ref()
                .map(|symbol| instrument.exchange_symbol == *symbol)
                .unwrap_or(true)
    }) else {
        return true;
    };
    let Some(price) = candidate.mark_price.or(candidate.index_price) else {
        return true;
    };
    planned_quantity(config.execution.notional_usdt, price, instrument).is_some()
}

pub fn planned_quantity(notional: f64, price: f64, instrument: &FundingInstrument) -> Option<f64> {
    if !notional.is_finite() || notional <= 0.0 || !price.is_finite() || price <= 0.0 {
        return None;
    }
    let contract_size = if instrument.contract_size.is_finite() && instrument.contract_size > 0.0 {
        instrument.contract_size
    } else {
        1.0
    };
    let quantity_step = if instrument.quantity_step.is_finite() && instrument.quantity_step > 0.0 {
        instrument.quantity_step
    } else {
        1.0
    };
    let min_quantity_for_notional = if instrument.min_notional > 0.0 {
        instrument.min_notional / (price * contract_size)
    } else {
        0.0
    };
    let raw_quantity = (notional / (price * contract_size))
        .max(instrument.min_qty)
        .max(min_quantity_for_notional);
    let steps = (raw_quantity / quantity_step).ceil().max(1.0);
    let quantity = steps * quantity_step;
    let planned_notional = quantity * price * contract_size;
    (planned_notional <= notional * 1.2).then_some(quantity)
}

pub fn eligible_symbols(
    instruments: &[FundingInstrument],
    config: &FundingCoreConfig,
) -> Vec<FundingSymbol> {
    let mut symbols = instruments
        .iter()
        .filter(|instrument| instrument.is_tradeable_usdt_perpetual())
        .filter(|instrument| symbol_allowed(&instrument.canonical_symbol, config))
        .map(|instrument| instrument.canonical_symbol.clone())
        .collect::<Vec<_>>();
    symbols.sort_by(|left, right| left.as_pair().cmp(&right.as_pair()));
    symbols.dedup();
    symbols
}

pub fn build_startup_markdown(report: &FundingScanReport) -> String {
    let generated_at = report.generated_at.format("%Y-%m-%d %H:%M:%S UTC");
    let mut content = format!(
        "### Funding Arbitrage Startup Observation\n\
         > Time: {}\n\
         > Mode: observe only, no orders\n\
         > Threshold: `{:.4}%`\n\n",
        generated_at, report.threshold_pct
    );

    if report.selections.is_empty() {
        content.push_str("No exchanges completed scanning.\n");
    } else {
        content.push_str("### Exchange Candidates\n\n");
        for selection in &report.selections {
            match &selection.selected {
                Some(candidate) => {
                    let next = candidate
                        .next_funding_time
                        .map(|time| time.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| "unknown".to_string());
                    let settle = candidate
                        .seconds_to_settlement
                        .map(|seconds| format!("{seconds}s"))
                        .unwrap_or_else(|| "unknown".to_string());
                    let mark = candidate
                        .mark_price
                        .map(|price| format!("{price:.8}"))
                        .unwrap_or_else(|| "-".to_string());
                    content.push_str(&format!(
                        "- `{}` `{}` funding `{:+.4}%`, settlement `{}`, remaining `{}`, mark `{}`, snapshot `{}`ms\n",
                        selection.exchange,
                        candidate.canonical_symbol,
                        candidate.funding_rate_pct,
                        next,
                        settle,
                        mark,
                        candidate.snapshot_age_ms,
                    ));
                }
                None => {
                    content.push_str(&format!(
                        "- `{}` no hit: {}, scanned symbols={} funding_snapshots={}\n",
                        selection.exchange,
                        selection
                            .skipped_reason
                            .as_deref()
                            .unwrap_or("unknown reason"),
                        selection.scanned_symbols,
                        selection.funding_snapshots,
                    ));
                }
            }
        }
    }

    if !report.errors.is_empty() {
        content.push_str("\n### Scan Errors\n\n");
        for error in &report.errors {
            content.push_str(&format!(
                "- `{}` `{}`: {}\n",
                error.exchange, error.stage, error.message
            ));
        }
    }
    content.push_str("\n> This version does not submit any orders.\n");
    content
}

fn canonical_symbol_supported(symbol: &FundingSymbol) -> bool {
    ascii_asset_token(&symbol.base) && ascii_asset_token(&symbol.quote)
}

fn exchange_symbol_supported(symbol: &str) -> bool {
    let value = symbol.trim();
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '/'))
}

fn ascii_asset_token(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty() && value.chars().all(|ch| ch.is_ascii_alphanumeric())
}

fn normalized_symbol_set(values: &[String]) -> HashSet<String> {
    values.iter().map(|value| normalize_symbol(value)).collect()
}

fn normalize_symbol(value: &str) -> String {
    value
        .trim()
        .to_ascii_uppercase()
        .replace('-', "")
        .replace('_', "")
        .replace('/', "")
        .replace("SWAP", "")
}

fn selection_skip_reason(snapshot_count: usize, eligible_candidates: usize) -> String {
    if snapshot_count == 0 {
        "no funding snapshots loaded".to_string()
    } else if eligible_candidates == 0 {
        "no candidate met threshold and filters".to_string()
    } else {
        "no selected candidate".to_string()
    }
}

fn default_mode() -> String {
    "observe".to_string()
}

fn default_exchanges() -> Vec<String> {
    vec![
        "binance".to_string(),
        "bitget".to_string(),
        "gate".to_string(),
    ]
}

fn default_quote_asset() -> String {
    "USDT".to_string()
}

fn default_contract_type() -> String {
    "perpetual".to_string()
}

fn default_per_exchange_limit() -> usize {
    1
}

fn default_min_funding_rate() -> f64 {
    -0.005
}

fn default_max_funding_snapshot_age_ms() -> i64 {
    5_000
}

fn default_wecom_webhook_env() -> String {
    "FUNDING_ARB_WECOM_WEBHOOK_URL".to_string()
}

fn default_true() -> bool {
    true
}

fn default_scan_minute() -> u32 {
    55
}

fn default_notional_usdt() -> f64 {
    10.0
}

fn default_open_seconds_before_settlement() -> i64 {
    1
}

fn default_close_seconds_after_settlement() -> i64 {
    1
}

fn default_close_limit_timeout_secs() -> u64 {
    300
}

fn default_close_limit_max_retries() -> u32 {
    3
}

fn default_max_slippage_pct() -> f64 {
    0.003
}

fn default_live_position_side() -> String {
    "long".to_string()
}

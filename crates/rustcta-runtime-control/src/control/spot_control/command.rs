use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{DisableMode, EnableMode, EnabledDirection, SymbolOperationType};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CommandStatus {
    Requested,
    Validated,
    Rejected,
    Queued,
    Running,
    AwaitingConfirmation,
    Completed,
    Failed,
    ManualInterventionRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ValidationError {
    pub code: String,
    pub message: String,
    pub critical: bool,
}

impl ValidationError {
    pub fn critical(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            critical: true,
        }
    }

    pub fn warning(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            critical: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlCommand {
    pub command_id: String,
    pub idempotency_key: String,
    pub operation: SymbolOperationType,
    pub symbol: String,
    pub payload: Value,
    pub requested_by: String,
    pub requested_at: DateTime<Utc>,
    pub expected_version: u64,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    pub status: CommandStatus,
    #[serde(default)]
    pub validation_errors: Vec<ValidationError>,
    #[serde(default)]
    pub started_at_optional: Option<DateTime<Utc>>,
    #[serde(default)]
    pub completed_at_optional: Option<DateTime<Utc>>,
    #[serde(default)]
    pub result_summary_optional: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlCommandResponse {
    pub command_id: String,
    pub status: CommandStatus,
    pub symbol: String,
    pub lifecycle_state: Option<super::SpotSymbolLifecycleState>,
    pub current_version: Option<u64>,
    #[serde(default)]
    pub validation_errors: Vec<ValidationError>,
    #[serde(default)]
    pub result_summary: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnableSymbolRequest {
    pub symbol: String,
    pub mode: EnableMode,
    pub selected_exchanges: Vec<String>,
    pub allowed_directions: Vec<EnabledDirection>,
    #[serde(default)]
    pub max_notional_per_trade: Option<f64>,
    #[serde(default)]
    pub max_total_symbol_exposure: Option<f64>,
    #[serde(default = "default_quote_asset")]
    pub quote_asset: String,
    #[serde(default = "default_requested_by")]
    pub requested_by: String,
    pub expected_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisableSymbolRequest {
    pub symbol: String,
    pub selected_exchanges: Vec<String>,
    pub mode: DisableMode,
    #[serde(default = "default_true")]
    pub cancel_active_orders: bool,
    #[serde(default = "default_true")]
    pub include_managed_inventory_only: bool,
    #[serde(default)]
    pub maximum_liquidation_loss_usdt: Option<f64>,
    #[serde(default)]
    pub maximum_slippage_bps: Option<f64>,
    #[serde(default = "default_requested_by")]
    pub requested_by: String,
    pub expected_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedSymbolRequest {
    #[serde(default = "default_requested_by")]
    pub requested_by: String,
    pub expected_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkDustUnmanagedRequest {
    pub exchange: String,
    pub asset: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default = "default_requested_by")]
    pub requested_by: String,
    pub expected_version: u64,
}

fn default_true() -> bool {
    true
}

fn default_requested_by() -> String {
    "operator".to_string()
}

fn default_quote_asset() -> String {
    "USDT".to_string()
}

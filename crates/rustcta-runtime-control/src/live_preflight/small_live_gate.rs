use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

fn default_max_notional_per_order() -> f64 {
    20.0
}

fn default_max_total_notional() -> f64 {
    50.0
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SmallLiveGateConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub explicit_live_confirmation: bool,
    #[serde(default = "default_max_notional_per_order")]
    pub max_notional_per_order: f64,
    #[serde(default = "default_max_total_notional")]
    pub max_total_notional: f64,
    #[serde(default)]
    pub enabled_symbols: Vec<String>,
    #[serde(default)]
    pub enabled_exchanges: Vec<String>,
}

impl Default for SmallLiveGateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            explicit_live_confirmation: false,
            max_notional_per_order: default_max_notional_per_order(),
            max_total_notional: default_max_total_notional(),
            enabled_symbols: Vec::new(),
            enabled_exchanges: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SmallLiveGateStatus {
    ReadyForSmallLive,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmallLiveGateCheck {
    pub name: String,
    pub passed: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SmallLiveGateReport {
    pub timestamp: DateTime<Utc>,
    pub status: SmallLiveGateStatus,
    pub checks: Vec<SmallLiveGateCheck>,
    pub blockers: Vec<String>,
    pub does_not_start_live_trading: bool,
}

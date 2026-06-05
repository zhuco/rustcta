use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeOperationalRole {
    ScanOnly,
    ReadOnlyValidated,
    LiveDryRunEligible,
    FutureLiveExecutionCandidate,
    LiveExecutionEnabled,
    Disabled,
}

impl ExchangeOperationalRole {
    pub fn scan_enabled(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    pub fn authenticated_read_allowed(self) -> bool {
        matches!(
            self,
            Self::ReadOnlyValidated
                | Self::LiveDryRunEligible
                | Self::FutureLiveExecutionCandidate
                | Self::LiveExecutionEnabled
        )
    }

    pub fn live_dry_run_eligible(self) -> bool {
        matches!(
            self,
            Self::LiveDryRunEligible
                | Self::FutureLiveExecutionCandidate
                | Self::LiveExecutionEnabled
        )
    }

    pub fn future_live_candidate(self) -> bool {
        matches!(
            self,
            Self::FutureLiveExecutionCandidate | Self::LiveExecutionEnabled
        )
    }

    pub fn currently_live_executable(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeRoleConfig {
    #[serde(default = "default_exchange_roles")]
    pub exchange_roles: BTreeMap<String, ExchangeOperationalRole>,
}

impl Default for ExchangeRoleConfig {
    fn default() -> Self {
        Self {
            exchange_roles: default_exchange_roles(),
        }
    }
}

pub fn default_exchange_roles() -> BTreeMap<String, ExchangeOperationalRole> {
    BTreeMap::from([
        (
            "gateio".to_string(),
            ExchangeOperationalRole::FutureLiveExecutionCandidate,
        ),
        (
            "bitget".to_string(),
            ExchangeOperationalRole::FutureLiveExecutionCandidate,
        ),
        ("mexc".to_string(), ExchangeOperationalRole::ScanOnly),
        ("coinex".to_string(), ExchangeOperationalRole::ScanOnly),
        ("kucoin".to_string(), ExchangeOperationalRole::ScanOnly),
    ])
}

pub fn normalize_exchange_name(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gate.io" => "gateio".to_string(),
        other => other.to_string(),
    }
}

pub fn role_for(
    roles: &BTreeMap<String, ExchangeOperationalRole>,
    exchange: &str,
) -> ExchangeOperationalRole {
    roles
        .get(&normalize_exchange_name(exchange))
        .copied()
        .unwrap_or(ExchangeOperationalRole::Disabled)
}

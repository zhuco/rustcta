use serde::{Deserialize, Serialize};

fn default_max_snapshot_age_ms() -> u64 {
    2_000
}
fn default_max_book_age_ms() -> u64 {
    1_000
}
fn default_max_balance_age_ms() -> u64 {
    5_000
}
fn default_max_symbol_rule_age_seconds() -> u64 {
    86_400
}
fn default_max_fee_age_seconds() -> u64 {
    3_600
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotControlSnapshotConfig {
    #[serde(default = "default_max_snapshot_age_ms")]
    pub max_snapshot_age_ms: u64,
    #[serde(default = "default_max_book_age_ms")]
    pub max_book_age_ms: u64,
    #[serde(default = "default_max_balance_age_ms")]
    pub max_balance_age_ms: u64,
    #[serde(default = "default_max_symbol_rule_age_seconds")]
    pub max_symbol_rule_age_seconds: u64,
    #[serde(default = "default_max_fee_age_seconds")]
    pub max_fee_age_seconds: u64,
    #[serde(default = "default_true")]
    pub require_exchange_health: bool,
    #[serde(default = "default_true")]
    pub require_balance_reconciliation_clean: bool,
    #[serde(default = "default_true")]
    pub fail_on_unknown_order_ownership: bool,
    #[serde(default = "default_true")]
    pub fail_on_unknown_inventory_ownership: bool,
}

impl Default for SpotControlSnapshotConfig {
    fn default() -> Self {
        Self {
            max_snapshot_age_ms: default_max_snapshot_age_ms(),
            max_book_age_ms: default_max_book_age_ms(),
            max_balance_age_ms: default_max_balance_age_ms(),
            max_symbol_rule_age_seconds: default_max_symbol_rule_age_seconds(),
            max_fee_age_seconds: default_max_fee_age_seconds(),
            require_exchange_health: true,
            require_balance_reconciliation_clean: true,
            fail_on_unknown_order_ownership: true,
            fail_on_unknown_inventory_ownership: true,
        }
    }
}

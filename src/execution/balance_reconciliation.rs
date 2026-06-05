use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{AssetBalance, MarketType};
use crate::risk::UnmanagedPosition;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BalanceMismatchSeverity {
    Info,
    Warn,
    Error,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetReconciliationStatus {
    pub exchange: String,
    pub market_type: MarketType,
    pub asset: String,
    pub total: f64,
    pub available: f64,
    pub locked_by_exchange: f64,
    pub locally_reserved: f64,
    pub effective_available: f64,
    pub unmanaged_quantity: f64,
    pub severity: BalanceMismatchSeverity,
    pub messages: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceReconciliationReport {
    pub timestamp: DateTime<Utc>,
    pub statuses: Vec<AssetReconciliationStatus>,
    pub max_severity: BalanceMismatchSeverity,
    pub clean: bool,
}

pub fn reconcile_balances(
    exchange: &str,
    market_type: MarketType,
    balances: &[AssetBalance],
    unmanaged_positions: &[UnmanagedPosition],
) -> BalanceReconciliationReport {
    let statuses = balances
        .iter()
        .map(|balance| reconcile_asset(exchange, market_type, balance, unmanaged_positions))
        .collect::<Vec<_>>();
    let max_severity = statuses
        .iter()
        .map(|status| status.severity)
        .max()
        .unwrap_or(BalanceMismatchSeverity::Info);
    BalanceReconciliationReport {
        timestamp: Utc::now(),
        clean: max_severity <= BalanceMismatchSeverity::Info,
        statuses,
        max_severity,
    }
}

pub fn reconcile_dashboard_inventory(
    inventory: &[crate::web::InventoryView],
) -> BalanceReconciliationReport {
    let statuses = inventory
        .iter()
        .map(|item| {
            let mut messages = Vec::new();
            let mut severity = BalanceMismatchSeverity::Info;
            if item.effective_available < -1e-12 {
                severity = severity.max(BalanceMismatchSeverity::Critical);
                messages.push("negative effective available".to_string());
            }
            if item.locally_reserved > item.available + 1e-12 {
                severity = severity.max(BalanceMismatchSeverity::Critical);
                messages.push("locally reserved exceeds available".to_string());
            }
            if item.unmanaged_quantity > 0.0 {
                let unmanaged_severity = if item.unmanaged_quantity > item.effective_available {
                    BalanceMismatchSeverity::Critical
                } else {
                    BalanceMismatchSeverity::Warn
                };
                severity = severity.max(unmanaged_severity);
                messages.push("unmanaged inventory overlaps tradable inventory".to_string());
            }
            if messages.is_empty() {
                messages.push("balance reconciliation clean".to_string());
            }
            AssetReconciliationStatus {
                exchange: item.exchange.clone(),
                market_type: item.market_type,
                asset: item.asset.clone(),
                total: item.total,
                available: item.available,
                locked_by_exchange: item.locked_by_exchange,
                locally_reserved: item.locally_reserved,
                effective_available: item.effective_available,
                unmanaged_quantity: item.unmanaged_quantity,
                severity,
                messages,
            }
        })
        .collect::<Vec<_>>();
    let max_severity = statuses
        .iter()
        .map(|status| status.severity)
        .max()
        .unwrap_or(BalanceMismatchSeverity::Info);
    BalanceReconciliationReport {
        timestamp: Utc::now(),
        clean: max_severity <= BalanceMismatchSeverity::Info,
        statuses,
        max_severity,
    }
}

fn reconcile_asset(
    exchange: &str,
    market_type: MarketType,
    balance: &AssetBalance,
    unmanaged_positions: &[UnmanagedPosition],
) -> AssetReconciliationStatus {
    let unmanaged_quantity = unmanaged_positions
        .iter()
        .filter(|position| {
            position.exchange.eq_ignore_ascii_case(exchange)
                && position.market_type == market_type
                && position.asset.eq_ignore_ascii_case(&balance.asset)
        })
        .map(|position| position.quantity.max(0.0))
        .sum::<f64>();
    let effective_available = balance.available - balance.locally_reserved;
    let mut severity = BalanceMismatchSeverity::Info;
    let mut messages = Vec::new();
    if effective_available < -1e-12 {
        severity = severity.max(BalanceMismatchSeverity::Critical);
        messages.push("negative effective available".to_string());
    }
    if balance.locally_reserved > balance.available + 1e-12 {
        severity = severity.max(BalanceMismatchSeverity::Critical);
        messages.push("locally reserved exceeds available".to_string());
    }
    if unmanaged_quantity > 0.0 {
        let unmanaged_severity = if unmanaged_quantity > effective_available.max(0.0) {
            BalanceMismatchSeverity::Critical
        } else {
            BalanceMismatchSeverity::Warn
        };
        severity = severity.max(unmanaged_severity);
        messages.push("unmanaged inventory overlaps tradable inventory".to_string());
    }
    if (balance.total - balance.available - balance.locked_by_exchange).abs() > 1e-8 {
        severity = severity.max(BalanceMismatchSeverity::Warn);
        messages.push("total differs from available plus exchange locked".to_string());
    }
    if messages.is_empty() {
        messages.push("balance reconciliation clean".to_string());
    }
    AssetReconciliationStatus {
        exchange: exchange.trim().to_ascii_lowercase(),
        market_type,
        asset: balance.asset.to_ascii_uppercase(),
        total: balance.total,
        available: balance.available,
        locked_by_exchange: balance.locked_by_exchange.max(balance.locked),
        locally_reserved: balance.locally_reserved,
        effective_available,
        unmanaged_quantity,
        severity,
        messages,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unmanaged(asset: &str, quantity: f64) -> UnmanagedPosition {
        UnmanagedPosition {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            asset: asset.to_string(),
            quantity,
            reason: "manual".to_string(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn available_balance_matches_reservation() {
        let balance = AssetBalance::new("USDT", 10.0, 10.0, 0.0).with_reservation(2.0);
        let report = reconcile_balances("mexc", MarketType::Spot, &[balance], &[]);
        assert_eq!(report.max_severity, BalanceMismatchSeverity::Info);
        assert_eq!(report.statuses[0].effective_available, 8.0);
    }

    #[test]
    fn locally_reserved_greater_than_available_is_critical() {
        let balance = AssetBalance::new("USDT", 10.0, 10.0, 0.0).with_reservation(12.0);
        let report = reconcile_balances("mexc", MarketType::Spot, &[balance], &[]);
        assert_eq!(report.max_severity, BalanceMismatchSeverity::Critical);
    }

    #[test]
    fn unmanaged_position_overlap_warns_or_criticals() {
        let balance = AssetBalance::new("BTC", 1.0, 1.0, 0.0);
        let report = reconcile_balances(
            "mexc",
            MarketType::Spot,
            &[balance],
            &[unmanaged("BTC", 0.5)],
        );
        assert_eq!(report.max_severity, BalanceMismatchSeverity::Warn);
        let balance = AssetBalance::new("BTC", 1.0, 0.1, 0.9);
        let report = reconcile_balances(
            "mexc",
            MarketType::Spot,
            &[balance],
            &[unmanaged("BTC", 0.5)],
        );
        assert_eq!(report.max_severity, BalanceMismatchSeverity::Critical);
    }

    #[test]
    fn negative_effective_available_is_critical() {
        let balance = AssetBalance::new("USDT", 10.0, 1.0, 9.0).with_reservation(2.0);
        let report = reconcile_balances("mexc", MarketType::Spot, &[balance], &[]);
        assert_eq!(report.max_severity, BalanceMismatchSeverity::Critical);
    }
}

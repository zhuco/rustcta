use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::execution::{
    BalanceMismatchSeverity, BalanceReconciliationReport, LiveDryRunOrderPlan,
    OrderReconciliationConfig,
};
use crate::risk::KillSwitchState;

use super::{LivePreflightReport, LiveReadinessDecision};

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    #[serde(default = "default_env_var")]
    pub env_var: String,
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
            env_var: default_env_var(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SmallLiveGateStatus {
    ReadyForSmallLive,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmallLiveGateCheck {
    pub name: String,
    pub passed: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmallLiveGateReport {
    pub timestamp: DateTime<Utc>,
    pub status: SmallLiveGateStatus,
    pub checks: Vec<SmallLiveGateCheck>,
    pub blockers: Vec<String>,
    pub does_not_start_live_trading: bool,
}

#[derive(Debug, Clone)]
pub struct SmallLiveGateInput<'a> {
    pub config: &'a SmallLiveGateConfig,
    pub preflight: Option<&'a LivePreflightReport>,
    pub live_dry_run_orders: &'a [LiveDryRunOrderPlan],
    pub kill_switch: &'a KillSwitchState,
    pub balance_reconciliation: Option<&'a BalanceReconciliationReport>,
    pub order_reconciliation: &'a OrderReconciliationConfig,
    pub recorder_enabled: bool,
    pub dashboard_enabled: bool,
    pub fee_model_available: bool,
    pub books_fresh: bool,
    pub disabled_symbol_overlap: bool,
    pub unmanaged_inventory_overlap: bool,
}

pub fn evaluate_small_live_gate(input: SmallLiveGateInput<'_>) -> SmallLiveGateReport {
    let mut checks = Vec::new();
    push(
        &mut checks,
        "preflight_ready",
        input
            .preflight
            .is_some_and(|report| report.decision == LiveReadinessDecision::ReadyForSmallLive),
        "live_preflight decision must be ReadyForSmallLive",
    );
    push(
        &mut checks,
        "live_dry_run_valid_orders",
        input
            .live_dry_run_orders
            .iter()
            .any(|plan| plan.validation_result.passed && !plan.would_submit),
        "at least one valid live dry-run plan is required",
    );
    push(
        &mut checks,
        "kill_switch_allows_live_orders",
        input.kill_switch.allow_live_orders && !input.kill_switch.active,
        "kill switch must explicitly allow live orders",
    );
    push(
        &mut checks,
        "notional_per_order_limit",
        input.config.max_notional_per_order > 0.0 && input.config.max_notional_per_order <= 5.0,
        "max_notional_per_order must be <= 5 USDT",
    );
    push(
        &mut checks,
        "total_notional_limit",
        input.config.max_total_notional > 0.0 && input.config.max_total_notional <= 50.0,
        "max_total_notional must be <= 50 USDT",
    );
    push(
        &mut checks,
        "explicit_symbols",
        !input.config.enabled_symbols.is_empty()
            && input
                .config
                .enabled_symbols
                .iter()
                .all(|symbol| symbol != "*" && !symbol.eq_ignore_ascii_case("all")),
        "enabled symbols must be explicit",
    );
    push(
        &mut checks,
        "explicit_exchanges",
        !input.config.enabled_exchanges.is_empty(),
        "enabled exchanges must be explicit",
    );
    push(
        &mut checks,
        "no_disabled_symbol_overlap",
        !input.disabled_symbol_overlap,
        "disabled symbols must not overlap enabled set",
    );
    push(
        &mut checks,
        "no_unmanaged_inventory_overlap",
        !input.unmanaged_inventory_overlap,
        "unmanaged inventory must not overlap tradable inventory",
    );
    push(
        &mut checks,
        "books_fresh",
        input.books_fresh,
        "books must be fresh",
    );
    push(
        &mut checks,
        "balances_sufficient",
        input
            .balance_reconciliation
            .is_some_and(|report| report.max_severity < BalanceMismatchSeverity::Error),
        "balance reconciliation must be clean or warning-only",
    );
    push(
        &mut checks,
        "fee_model_available",
        input.fee_model_available,
        "fee model must be loaded",
    );
    push(
        &mut checks,
        "order_reconciliation_enabled",
        input.order_reconciliation.enabled,
        "REST order reconciliation must be enabled",
    );
    push(
        &mut checks,
        "recorder_enabled",
        input.recorder_enabled,
        "recorder must be enabled",
    );
    push(
        &mut checks,
        "dashboard_enabled",
        input.dashboard_enabled,
        "dashboard must be enabled",
    );
    push(
        &mut checks,
        "explicit_live_confirmation",
        input.config.explicit_live_confirmation,
        "explicit live confirmation flag must be present",
    );
    let env_present = std::env::var(&input.config.env_var)
        .map(|value| value == "true")
        .unwrap_or(false);
    push(
        &mut checks,
        "env_var_present",
        env_present,
        format!("{} must equal true", input.config.env_var),
    );

    let blockers = checks
        .iter()
        .filter(|check| !check.passed)
        .map(|check| format!("{}: {}", check.name, check.message))
        .collect::<Vec<_>>();
    SmallLiveGateReport {
        timestamp: Utc::now(),
        status: if blockers.is_empty() {
            SmallLiveGateStatus::ReadyForSmallLive
        } else {
            SmallLiveGateStatus::Blocked
        },
        checks,
        blockers,
        does_not_start_live_trading: true,
    }
}

fn push(
    checks: &mut Vec<SmallLiveGateCheck>,
    name: &str,
    passed: bool,
    message: impl Into<String>,
) {
    checks.push(SmallLiveGateCheck {
        name: name.to_string(),
        passed,
        message: message.into(),
    });
}

fn default_max_notional_per_order() -> f64 {
    5.0
}

fn default_max_total_notional() -> f64 {
    50.0
}

fn default_env_var() -> String {
    "RUSTCTA_ENABLE_SMALL_LIVE".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchanges::unified::{
        MarketType, OrderRequest, OrderSide, OrderType, PositionSide, SymbolRule, SymbolStatus,
        TimeInForce,
    };
    use crate::execution::{
        FeeCalculation, FeeRole, FeeSource, LiveDryRunValidationResult, OrderReconciliationConfig,
    };
    use crate::live_preflight::{LivePreflightConfig, LivePreflightReport};
    use std::collections::HashMap;

    fn preflight(decision: LiveReadinessDecision) -> LivePreflightReport {
        LivePreflightReport {
            timestamp: Utc::now(),
            decision,
            target_mode: "live_dry_run".to_string(),
            checks: Vec::new(),
            pass_count: 0,
            warn_count: 0,
            fail_count: 0,
            skipped_count: 0,
            unknown_count: 0,
            critical_failures: Vec::new(),
            warnings: Vec::new(),
            suggested_next_actions: Vec::new(),
            per_exchange_readiness: HashMap::new(),
            per_symbol_readiness: HashMap::new(),
            config_summary: LivePreflightConfig::default(),
        }
    }

    fn plan() -> LiveDryRunOrderPlan {
        let rule = SymbolRule {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            internal_symbol: "BTCUSDT".to_string(),
            exchange_symbol: "BTCUSDT".to_string(),
            base_asset: "BTC".to_string(),
            quote_asset: "USDT".to_string(),
            price_precision: 2,
            quantity_precision: 4,
            tick_size: 0.01,
            step_size: 0.0001,
            min_quantity: 0.0001,
            min_notional: 5.0,
            max_quantity: None,
            supported_order_types: vec![OrderType::IOC],
            supported_time_in_force: vec![TimeInForce::IOC],
            status: SymbolStatus::Trading,
            raw_metadata: None,
        };
        let request = OrderRequest {
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::IOC,
            time_in_force: Some(TimeInForce::IOC),
            quantity: 0.1,
            price: Some(100.0),
            client_order_id: Some("cid".to_string()),
            reduce_only: false,
        };
        LiveDryRunOrderPlan {
            plan_id: "p1".to_string(),
            timestamp: Utc::now(),
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            symbol: "BTCUSDT".to_string(),
            exchange_symbol: "BTCUSDT".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::IOC,
            time_in_force: Some(TimeInForce::IOC),
            price: Some(100.0),
            quantity: 0.1,
            notional: 10.0,
            client_order_id: Some("cid".to_string()),
            fee_estimate: FeeCalculation {
                exchange: "mexc".to_string(),
                market_type: MarketType::Spot,
                symbol: Some("BTCUSDT".to_string()),
                role: FeeRole::Taker,
                fee_asset_mode: crate::execution::FeeAssetMode::Quote,
                fee_asset: Some("USDT".to_string()),
                notional: 10.0,
                raw_fee_bps: 5.0,
                effective_fee_bps: 5.0,
                fee_amount: 0.005,
                source: FeeSource::Fallback,
                platform_discount_applied: false,
            },
            required_balance_asset: "USDT".to_string(),
            required_balance_amount: 10.005,
            symbol_rule_snapshot: rule,
            validation_result: LiveDryRunValidationResult::pass(),
            would_submit: false,
            rejection_reason: None,
            order_request: request,
        }
    }

    fn ready_input<'a>(
        config: &'a SmallLiveGateConfig,
        preflight: &'a LivePreflightReport,
        plan: &'a [LiveDryRunOrderPlan],
        kill_switch: &'a KillSwitchState,
        balance: &'a BalanceReconciliationReport,
        order_recon: &'a OrderReconciliationConfig,
    ) -> SmallLiveGateInput<'a> {
        SmallLiveGateInput {
            config,
            preflight: Some(preflight),
            live_dry_run_orders: plan,
            kill_switch,
            balance_reconciliation: Some(balance),
            order_reconciliation: order_recon,
            recorder_enabled: true,
            dashboard_enabled: true,
            fee_model_available: true,
            books_fresh: true,
            disabled_symbol_overlap: false,
            unmanaged_inventory_overlap: false,
        }
    }

    #[test]
    fn blocks_if_env_var_missing() {
        std::env::remove_var("RUSTCTA_ENABLE_SMALL_LIVE");
        let config = SmallLiveGateConfig {
            enabled: true,
            explicit_live_confirmation: true,
            enabled_symbols: vec!["BTCUSDT".to_string()],
            enabled_exchanges: vec!["mexc".to_string()],
            ..SmallLiveGateConfig::default()
        };
        let plans = vec![plan()];
        let preflight = preflight(LiveReadinessDecision::ReadyForSmallLive);
        let kill = KillSwitchState {
            enabled: true,
            active: false,
            reason: None,
            triggered_by: None,
            triggered_at: None,
            allow_paper_trading: true,
            allow_live_dry_run: true,
            allow_live_orders: true,
        };
        let balance = BalanceReconciliationReport {
            timestamp: Utc::now(),
            statuses: Vec::new(),
            max_severity: BalanceMismatchSeverity::Info,
            clean: true,
        };
        let report = evaluate_small_live_gate(ready_input(
            &config,
            &preflight,
            &plans,
            &kill,
            &balance,
            &OrderReconciliationConfig::default(),
        ));
        assert_eq!(report.status, SmallLiveGateStatus::Blocked);
        assert!(report.does_not_start_live_trading);
    }

    #[test]
    fn blocks_if_preflight_blocked() {
        let config = SmallLiveGateConfig {
            explicit_live_confirmation: true,
            enabled_symbols: vec!["BTCUSDT".to_string()],
            enabled_exchanges: vec!["mexc".to_string()],
            ..SmallLiveGateConfig::default()
        };
        let plans = vec![plan()];
        let preflight = preflight(LiveReadinessDecision::Blocked);
        let kill = KillSwitchState {
            enabled: true,
            active: false,
            reason: None,
            triggered_by: None,
            triggered_at: None,
            allow_paper_trading: true,
            allow_live_dry_run: true,
            allow_live_orders: true,
        };
        let balance = BalanceReconciliationReport {
            timestamp: Utc::now(),
            statuses: Vec::new(),
            max_severity: BalanceMismatchSeverity::Info,
            clean: true,
        };
        let report = evaluate_small_live_gate(ready_input(
            &config,
            &preflight,
            &plans,
            &kill,
            &balance,
            &OrderReconciliationConfig::default(),
        ));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("preflight_ready")));
    }

    #[test]
    fn blocks_if_notional_too_large() {
        let config = SmallLiveGateConfig {
            explicit_live_confirmation: true,
            max_notional_per_order: 50.0,
            enabled_symbols: vec!["BTCUSDT".to_string()],
            enabled_exchanges: vec!["mexc".to_string()],
            ..SmallLiveGateConfig::default()
        };
        let plans = vec![plan()];
        let preflight = preflight(LiveReadinessDecision::ReadyForSmallLive);
        let kill = KillSwitchState {
            enabled: true,
            active: false,
            reason: None,
            triggered_by: None,
            triggered_at: None,
            allow_paper_trading: true,
            allow_live_dry_run: true,
            allow_live_orders: true,
        };
        let balance = BalanceReconciliationReport {
            timestamp: Utc::now(),
            statuses: Vec::new(),
            max_severity: BalanceMismatchSeverity::Info,
            clean: true,
        };
        let report = evaluate_small_live_gate(ready_input(
            &config,
            &preflight,
            &plans,
            &kill,
            &balance,
            &OrderReconciliationConfig::default(),
        ));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("notional_per_order_limit")));
    }
}

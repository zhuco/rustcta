use std::collections::HashMap;

use chrono::Utc;

use super::{
    LivePreflightCheck, LivePreflightCheckStatus, LivePreflightConfig, LivePreflightReport,
    LivePreflightSeverity, LiveReadinessDecision,
};

pub fn build_report(
    config: LivePreflightConfig,
    checks: Vec<LivePreflightCheck>,
) -> LivePreflightReport {
    let pass_count = checks
        .iter()
        .filter(|check| check.status == LivePreflightCheckStatus::Pass)
        .count();
    let warn_count = checks
        .iter()
        .filter(|check| check.status == LivePreflightCheckStatus::Warn)
        .count();
    let fail_count = checks
        .iter()
        .filter(|check| check.status == LivePreflightCheckStatus::Fail)
        .count();
    let skipped_count = checks
        .iter()
        .filter(|check| check.status == LivePreflightCheckStatus::Skipped)
        .count();
    let unknown_count = checks
        .iter()
        .filter(|check| check.status == LivePreflightCheckStatus::Unknown)
        .count();
    let critical_failures = checks
        .iter()
        .filter(|check| {
            check.status == LivePreflightCheckStatus::Fail
                && matches!(
                    check.severity,
                    LivePreflightSeverity::Critical | LivePreflightSeverity::Error
                )
        })
        .map(|check| check.message.clone())
        .collect::<Vec<_>>();
    let warnings = checks
        .iter()
        .filter(|check| {
            matches!(
                check.status,
                LivePreflightCheckStatus::Warn | LivePreflightCheckStatus::Unknown
            )
        })
        .map(|check| check.message.clone())
        .collect::<Vec<_>>();

    let live_target = config.target_mode.eq_ignore_ascii_case("live")
        || config
            .target_mode
            .eq_ignore_ascii_case("small_live_taker_taker");
    let decision = if fail_count > 0 || !critical_failures.is_empty() {
        LiveReadinessDecision::Blocked
    } else if live_target && config.require_api_key_trade_permission {
        LiveReadinessDecision::ReadyForSmallLive
    } else if warn_count > 0 || unknown_count > 0 {
        LiveReadinessDecision::ReadyForLiveDryRun
    } else if config.require_api_key_trade_permission {
        LiveReadinessDecision::ReadyForSmallLive
    } else {
        LiveReadinessDecision::ReadyForLiveDryRun
    };

    LivePreflightReport {
        timestamp: Utc::now(),
        decision,
        target_mode: config.target_mode.clone(),
        per_exchange_readiness: aggregate_by_exchange(&checks),
        per_symbol_readiness: aggregate_by_symbol(&checks),
        suggested_next_actions: suggested_next_actions(&checks),
        checks,
        pass_count,
        warn_count,
        fail_count,
        skipped_count,
        unknown_count,
        critical_failures,
        warnings,
        config_summary: config,
    }
}

pub fn render_human_report(report: &LivePreflightReport) -> String {
    let mut output = String::new();
    output.push_str("LIVE PREFLIGHT REPORT\n");
    output.push_str(&format!("Decision: {:?}\n", report.decision));
    output.push_str(&format!(
        "Checks: pass={} warn={} fail={} skipped={} unknown={}\n\n",
        report.pass_count,
        report.warn_count,
        report.fail_count,
        report.skipped_count,
        report.unknown_count
    ));
    output.push_str("Critical failures:\n");
    if report.critical_failures.is_empty() {
        output.push_str("- none\n");
    } else {
        for item in &report.critical_failures {
            output.push_str(&format!("- {item}\n"));
        }
    }
    output.push_str("\nWarnings:\n");
    if report.warnings.is_empty() {
        output.push_str("- none\n");
    } else {
        for item in &report.warnings {
            output.push_str(&format!("- {item}\n"));
        }
    }
    output.push_str("\nReady:\n");
    let ready = report
        .checks
        .iter()
        .filter(|check| check.status == LivePreflightCheckStatus::Pass)
        .map(|check| check.name.clone())
        .collect::<Vec<_>>();
    if ready.is_empty() {
        output.push_str("- none\n");
    } else {
        for item in ready {
            output.push_str(&format!("- {item}\n"));
        }
    }
    output.push_str("\nSuggested next actions:\n");
    if report.suggested_next_actions.is_empty() {
        output.push_str("- none\n");
    } else {
        for item in &report.suggested_next_actions {
            output.push_str(&format!("- {item}\n"));
        }
    }
    output
}

fn aggregate_by_exchange(
    checks: &[LivePreflightCheck],
) -> HashMap<String, LivePreflightCheckStatus> {
    let mut readiness = HashMap::new();
    for check in checks {
        let Some(exchange) = &check.exchange else {
            continue;
        };
        let entry = readiness
            .entry(exchange.clone())
            .or_insert(LivePreflightCheckStatus::Pass);
        *entry = worst_status(*entry, check.status);
    }
    readiness
}

fn aggregate_by_symbol(checks: &[LivePreflightCheck]) -> HashMap<String, LivePreflightCheckStatus> {
    let mut readiness = HashMap::new();
    for check in checks {
        let Some(symbol) = &check.symbol else {
            continue;
        };
        let key = match &check.exchange {
            Some(exchange) => format!("{exchange}:{symbol}"),
            None => symbol.clone(),
        };
        let entry = readiness
            .entry(key)
            .or_insert(LivePreflightCheckStatus::Pass);
        *entry = worst_status(*entry, check.status);
    }
    readiness
}

fn worst_status(
    left: LivePreflightCheckStatus,
    right: LivePreflightCheckStatus,
) -> LivePreflightCheckStatus {
    use LivePreflightCheckStatus::*;
    match (left, right) {
        (Fail, _) | (_, Fail) => Fail,
        (Warn, _) | (_, Warn) => Warn,
        (Unknown, _) | (_, Unknown) => Unknown,
        (Skipped, _) | (_, Skipped) => Skipped,
        _ => Pass,
    }
}

fn suggested_next_actions(checks: &[LivePreflightCheck]) -> Vec<String> {
    let mut actions = Vec::new();
    for check in checks {
        if check.status == LivePreflightCheckStatus::Fail {
            actions.push(format!("Fix {}: {}", check.name, check.message));
        } else if check.status == LivePreflightCheckStatus::Warn {
            actions.push(format!("Review {}: {}", check.name, check.message));
        }
    }
    actions.sort();
    actions.dedup();
    actions
}

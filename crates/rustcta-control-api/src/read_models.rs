use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::{
    ControlApiStateSnapshot, FeeSummaryView, FeeVenueSummary, JsonRowsView, LogEventView, LogLevel,
    LogStreamView, OpportunitiesView, RiskEventView, RiskSeverity, RiskStatus, RiskSummaryView,
    StrategySnapshotEnvelope, StrategySnapshotSource, SymbolScannerView, SymbolsView,
    CONTROL_API_SCHEMA_VERSION,
};

pub fn apply_legacy_dashboard_snapshot(
    mut snapshot: ControlApiStateSnapshot,
    legacy: &Value,
) -> ControlApiStateSnapshot {
    snapshot.risk = risk_summary_from_legacy_snapshot(legacy);
    snapshot.fees = fee_summary_from_legacy_snapshot(legacy);
    snapshot.logs = log_stream_from_legacy_snapshot(legacy);
    snapshot.inventory = json_rows_from_legacy_snapshot(legacy, "inventory");
    snapshot.books = json_rows_from_legacy_snapshot(legacy, "books");
    snapshot.exchanges = json_rows_from_legacy_snapshot(legacy, "exchanges");
    snapshot.recent_trades = json_rows_from_legacy_snapshot(legacy, "trades");
    snapshot.recent_opportunities = json_rows_from_legacy_snapshot(legacy, "opportunities");
    snapshot.opportunities = opportunities_from_legacy_snapshot(legacy);
    snapshot.symbols = symbols_from_legacy_snapshot(legacy);
    snapshot.strategy_snapshots = strategy_snapshots_from_legacy_snapshot(legacy);
    snapshot
}

pub fn json_rows_from_legacy_snapshot(legacy: &Value, field: &str) -> JsonRowsView {
    JsonRowsView {
        schema_version: CONTROL_API_SCHEMA_VERSION,
        rows: legacy
            .get(field)
            .and_then(Value::as_array)
            .map(|rows| rows.iter().map(secret_free_value).collect())
            .unwrap_or_default(),
    }
}

pub fn risk_summary_from_legacy_snapshot(legacy: &Value) -> RiskSummaryView {
    let risk_events = legacy
        .get("risk_events")
        .and_then(Value::as_array)
        .map(|events| {
            events
                .iter()
                .enumerate()
                .filter_map(|(index, event)| risk_event_from_legacy(index, event))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let kill_switch_active = legacy
        .pointer("/kill_switch/active")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let live_orders_enabled = legacy
        .get("live_trading_enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let live_preflight_enabled = legacy
        .get("live_preflight_enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let blocked = kill_switch_active || unresolved_critical_event_count(&risk_events) > 0;
    let warning = risk_events
        .iter()
        .any(|event| event.resolved_at.is_none() && event.severity == RiskSeverity::Warning);
    let status = if blocked {
        RiskStatus::Blocked
    } else if warning {
        RiskStatus::Warning
    } else if live_preflight_enabled || live_orders_enabled {
        RiskStatus::Ready
    } else {
        RiskStatus::Unknown
    };

    RiskSummaryView {
        schema_version: CONTROL_API_SCHEMA_VERSION,
        status,
        kill_switch_active,
        live_orders_enabled,
        open_risk_event_count: risk_events
            .iter()
            .filter(|event| event.resolved_at.is_none())
            .count(),
        last_event_at: risk_events.iter().map(|event| event.occurred_at).max(),
        checks: Vec::new(),
        events: risk_events,
    }
}

pub fn fee_summary_from_legacy_snapshot(legacy: &Value) -> FeeSummaryView {
    let venues = legacy
        .get("fees")
        .and_then(Value::as_array)
        .map(|fees| {
            fees.iter()
                .map(|fee| FeeVenueSummary {
                    exchange_id: string_field(fee, &["exchange", "exchange_id"])
                        .unwrap_or_else(|| "unknown".to_string()),
                    market: string_field(fee, &["market_type", "market"]),
                    maker_fee_rate: number_field(fee, &["maker_fee_rate", "maker_rate"]),
                    taker_fee_rate: number_field(fee, &["taker_fee_rate", "taker_rate"]),
                    fee_paid_usdt: number_field(fee, &["fee_paid_usdt", "total_fee_usdt"])
                        .unwrap_or_default(),
                    rebate_usdt: number_field(fee, &["rebate_usdt", "realized_rebate_usdt"])
                        .unwrap_or_default(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    FeeSummaryView {
        schema_version: CONTROL_API_SCHEMA_VERSION,
        generated_at: legacy
            .get("generated_at")
            .and_then(Value::as_str)
            .and_then(parse_timestamp),
        total_fee_usdt: venues.iter().map(|venue| venue.fee_paid_usdt).sum(),
        realized_rebate_usdt: venues.iter().map(|venue| venue.rebate_usdt).sum(),
        venues,
    }
}

pub fn log_stream_from_legacy_snapshot(legacy: &Value) -> LogStreamView {
    let mut events = Vec::new();
    if let Some(risk_events) = legacy.get("risk_events").and_then(Value::as_array) {
        for (index, event) in risk_events.iter().enumerate() {
            if let Some(risk_event) = risk_event_from_legacy(index, event) {
                events.push(LogEventView {
                    log_id: format!("risk-{}", risk_event.event_id),
                    level: log_level_for_risk_severity(risk_event.severity),
                    target: Some(risk_event.scope),
                    message: risk_event.message,
                    occurred_at: risk_event.occurred_at,
                });
            }
        }
    }
    LogStreamView {
        schema_version: CONTROL_API_SCHEMA_VERSION,
        events,
    }
}

pub fn opportunities_from_legacy_snapshot(legacy: &Value) -> OpportunitiesView {
    OpportunitiesView {
        schema_version: CONTROL_API_SCHEMA_VERSION,
        recent: legacy_array_values(legacy, "opportunities"),
        arbitrage: legacy_array_values(legacy, "arbitrage_opportunities"),
        statistics: legacy
            .get("arbitrage_statistics")
            .map(secret_free_value)
            .unwrap_or_else(|| Value::Object(Default::default())),
    }
}

pub fn symbols_from_legacy_snapshot(legacy: &Value) -> SymbolsView {
    SymbolsView {
        schema_version: CONTROL_API_SCHEMA_VERSION,
        symbol_rules: legacy
            .get("spot_symbol_rules")
            .map(secret_free_value)
            .unwrap_or_else(|| Value::Array(Vec::new())),
        spot_control: legacy
            .get("spot_control")
            .map(secret_free_value)
            .unwrap_or_else(|| Value::Object(Default::default())),
        scanner: SymbolScannerView {
            symbol_coverage: legacy
                .pointer("/five_exchange_scanner/symbol_coverage")
                .map(secret_free_value)
                .unwrap_or_else(|| Value::Array(Vec::new())),
            recommendations: legacy
                .pointer("/five_exchange_scanner/recommendations")
                .map(secret_free_value)
                .unwrap_or_else(|| Value::Array(Vec::new())),
        },
    }
}

pub fn strategy_snapshots_from_legacy_snapshot(legacy: &Value) -> Vec<StrategySnapshotEnvelope> {
    let generated_at = legacy
        .get("generated_at")
        .and_then(Value::as_str)
        .and_then(parse_timestamp)
        .unwrap_or_else(Utc::now);
    let mut snapshots = Vec::new();

    let spot_detail = secret_free_object([
        ("status", legacy.get("status").cloned()),
        ("config_summary", legacy.get("config_summary").cloned()),
        ("spot_control", legacy.get("spot_control").cloned()),
        (
            "runtime_publisher",
            legacy.get("runtime_publisher_health").cloned(),
        ),
        ("disabled", legacy.get("disabled").cloned()),
        ("live_preflight", legacy.get("live_preflight").cloned()),
        (
            "live_dry_run_orders",
            legacy.get("live_dry_run_orders").cloned(),
        ),
        (
            "order_reconciliation",
            legacy
                .get("order_reconciliation_status")
                .cloned()
                .or_else(|| legacy.get("order_reconciliation").cloned()),
        ),
        (
            "balance_reconciliation",
            legacy.get("balance_reconciliation").cloned(),
        ),
    ]);
    if !spot_detail
        .as_object()
        .is_some_and(serde_json::Map::is_empty)
    {
        snapshots.push(StrategySnapshotEnvelope {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            strategy_id: "spot-arb-local".to_string(),
            strategy_kind: "spot_spot_taker_arbitrage".to_string(),
            run_id: None,
            status: None,
            generated_at,
            source: StrategySnapshotSource::LegacyDashboard,
            detail: spot_detail,
        });
    }

    let cross_detail = secret_free_object([
        ("status", legacy.get("status").cloned()),
        ("config_summary", legacy.get("config_summary").cloned()),
        (
            "arbitrage_relationships",
            legacy.get("arbitrage_relationships").cloned(),
        ),
        (
            "arbitrage_opportunities",
            legacy.get("arbitrage_opportunities").cloned(),
        ),
        (
            "arbitrage_statistics",
            legacy.get("arbitrage_statistics").cloned(),
        ),
        (
            "market_snapshots",
            legacy
                .get("cross_arb_market_snapshots")
                .cloned()
                .or_else(|| legacy.get("market_snapshots").cloned()),
        ),
        (
            "instrument_feasibility",
            legacy.get("instrument_feasibility").cloned(),
        ),
        ("scanner", legacy.get("five_exchange_scanner").cloned()),
        ("hedge_policy", legacy.get("hedge_policy").cloned()),
    ]);
    if !cross_detail
        .as_object()
        .is_some_and(serde_json::Map::is_empty)
    {
        snapshots.push(StrategySnapshotEnvelope {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            strategy_id: "contract-arb-local".to_string(),
            strategy_kind: "cross_exchange_arbitrage".to_string(),
            run_id: None,
            status: None,
            generated_at,
            source: StrategySnapshotSource::LegacyDashboard,
            detail: cross_detail,
        });
    }

    snapshots
}

fn legacy_array_values(legacy: &Value, field: &str) -> Vec<Value> {
    legacy
        .get(field)
        .and_then(Value::as_array)
        .map(|rows| rows.iter().map(secret_free_value).collect())
        .unwrap_or_default()
}

fn risk_event_from_legacy(index: usize, event: &Value) -> Option<RiskEventView> {
    let occurred_at = event
        .get("timestamp")
        .or_else(|| event.get("occurred_at"))
        .and_then(Value::as_str)
        .and_then(parse_timestamp)?;
    let severity = string_field(event, &["severity"])
        .as_deref()
        .map(risk_severity)
        .unwrap_or(RiskSeverity::Info);
    let event_type =
        string_field(event, &["event_type", "type"]).unwrap_or_else(|| "risk_event".to_string());
    let symbol = string_field(event, &["symbol"]);
    let exchange = string_field(event, &["exchange"]);
    let scope = [exchange.as_deref(), symbol.as_deref()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(":");
    let scope = if scope.is_empty() {
        "workspace".to_string()
    } else {
        scope
    };
    let reason = string_field(event, &["reason", "message"]).unwrap_or_else(|| event_type.clone());
    let details = string_field(event, &["details"]);
    let message = details
        .filter(|details| !details.trim().is_empty())
        .map(|details| format!("{reason}: {details}"))
        .unwrap_or(reason);

    Some(RiskEventView {
        event_id: format!(
            "{}-{}",
            occurred_at.timestamp_millis(),
            string_field(event, &["event_type", "type"]).unwrap_or_else(|| index.to_string())
        ),
        severity,
        scope,
        message,
        occurred_at,
        resolved_at: event
            .get("resolved_at")
            .and_then(Value::as_str)
            .and_then(parse_timestamp),
    })
}

fn unresolved_critical_event_count(events: &[RiskEventView]) -> usize {
    events
        .iter()
        .filter(|event| event.resolved_at.is_none() && event.severity == RiskSeverity::Critical)
        .count()
}

fn risk_severity(value: &str) -> RiskSeverity {
    match value.trim().to_ascii_lowercase().as_str() {
        "critical" | "fatal" => RiskSeverity::Critical,
        "error" | "fail" | "failed" => RiskSeverity::Error,
        "warn" | "warning" => RiskSeverity::Warning,
        _ => RiskSeverity::Info,
    }
}

fn log_level_for_risk_severity(severity: RiskSeverity) -> LogLevel {
    match severity {
        RiskSeverity::Critical | RiskSeverity::Error => LogLevel::Error,
        RiskSeverity::Warning => LogLevel::Warn,
        RiskSeverity::Info => LogLevel::Info,
    }
}

fn string_field(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(Value::as_str)
            .map(|text| text.to_string())
    })
}

fn number_field(value: &Value, fields: &[&str]) -> Option<f64> {
    fields.iter().find_map(|field| {
        value.get(*field).and_then(|item| match item {
            Value::Number(number) => number.as_f64(),
            Value::String(text) => text.parse::<f64>().ok(),
            _ => None,
        })
    })
}

fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .ok()
}

fn secret_free_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .filter(|(key, _)| !is_sensitive_legacy_field(key))
                .map(|(key, value)| (key.clone(), secret_free_value(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(secret_free_value).collect()),
        value => value.clone(),
    }
}

fn secret_free_object<const N: usize>(fields: [(&str, Option<Value>); N]) -> Value {
    let object = fields
        .into_iter()
        .filter_map(|(field, value)| {
            value.map(|value| (field.to_string(), secret_free_value(&value)))
        })
        .collect();
    Value::Object(object)
}

fn is_sensitive_legacy_field(field: &str) -> bool {
    let lower = field.to_ascii_lowercase();
    let key_parts = ["api", "key"];
    let secret_parts = ["api", "secret"];
    let raw_key_field = key_parts.join("_");
    let raw_secret_field = secret_parts.join("_");
    let pass_field = ["pass", "phrase"].join("");
    let auth_field = ["author", "ization"].join("");

    lower == raw_key_field
        || lower == raw_secret_field
        || lower == pass_field
        || lower == auth_field
        || lower == "secret"
        || lower == "token"
        || lower == "access_token"
        || lower == "refresh_token"
}

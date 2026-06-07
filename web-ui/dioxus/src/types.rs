use serde_json::{json, Value};

use crate::i18n::s;
use crate::utils::{
    as_array, bool_at, canonical_exchange_name, exchange_field_value, fallback_exchange_schemas,
    normalize_exchange_api_key_schemas, text_at,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Language {
    Zh,
    En,
}

impl Language {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::Zh => "zh",
            Self::En => "en",
        }
    }

    pub(crate) fn is_zh(self) -> bool {
        matches!(self, Self::Zh)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ControlPanelView {
    #[default]
    Workspace,
    Overview,
    SpotArb,
    CrossArb,
    FundingArb,
    Exchanges,
    Symbols,
    Plans,
    Risk,
    Runtime,
    Config,
    Logs,
    ApiKeys,
}

impl ControlPanelView {
    pub(crate) const ALL: [Self; 13] = [
        Self::Workspace,
        Self::Overview,
        Self::SpotArb,
        Self::CrossArb,
        Self::FundingArb,
        Self::Exchanges,
        Self::Symbols,
        Self::Plans,
        Self::Risk,
        Self::Runtime,
        Self::Config,
        Self::Logs,
        Self::ApiKeys,
    ];

    pub(crate) fn nav_label_key(self) -> &'static str {
        match self {
            Self::Workspace => "nav_workspace",
            Self::Overview => "nav_overview",
            Self::SpotArb => "nav_spot_arb",
            Self::CrossArb => "nav_cross_arb",
            Self::FundingArb => "nav_funding_arb",
            Self::Exchanges => "nav_exchanges",
            Self::Symbols => "nav_symbols",
            Self::Plans => "nav_plans",
            Self::Risk => "nav_risk",
            Self::Runtime => "nav_runtime",
            Self::Config => "nav_config",
            Self::Logs => "nav_logs",
            Self::ApiKeys => "nav_api_keys",
        }
    }

    pub(crate) fn route_id(self) -> &'static str {
        match self {
            Self::Workspace => "workspace",
            Self::Overview => "overview",
            Self::SpotArb => "spot-arb",
            Self::CrossArb => "cross-arb",
            Self::FundingArb => "funding-arb",
            Self::Exchanges => "exchanges",
            Self::Symbols => "symbols",
            Self::Plans => "plans",
            Self::Risk => "risk",
            Self::Runtime => "runtime",
            Self::Config => "config",
            Self::Logs => "logs",
            Self::ApiKeys => "api-keys",
        }
    }

    pub(crate) fn from_route_id(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "workspace" => Some(Self::Workspace),
            "overview" => Some(Self::Overview),
            "spot-arb" | "spot_arb" => Some(Self::SpotArb),
            "cross-arb" | "cross_arb" => Some(Self::CrossArb),
            "funding-arb" | "funding_arb" => Some(Self::FundingArb),
            "exchanges" => Some(Self::Exchanges),
            "symbols" => Some(Self::Symbols),
            "plans" => Some(Self::Plans),
            "risk" => Some(Self::Risk),
            "runtime" => Some(Self::Runtime),
            "config" => Some(Self::Config),
            "logs" => Some(Self::Logs),
            "api-keys" | "api_keys" => Some(Self::ApiKeys),
            _ => None,
        }
    }
}

#[derive(Clone, Default, PartialEq)]
pub(crate) struct DashboardData {
    pub(crate) status: Value,
    pub(crate) config: Value,
    pub(crate) exchanges: Value,
    pub(crate) books: Value,
    pub(crate) symbols: Value,
    pub(crate) opportunities: Value,
    pub(crate) dry_run_plans: Value,
    pub(crate) risk: Value,
    pub(crate) logs: Value,
    pub(crate) health: Value,
    pub(crate) runtime_publisher: Value,
    pub(crate) inventory: Value,
    pub(crate) fees: Value,
    pub(crate) disabled: Value,
    pub(crate) scanner: Value,
    pub(crate) hedge_policy: Value,
    pub(crate) control_symbols: Value,
    pub(crate) control_audit: Value,
    pub(crate) spot_arb: Value,
    pub(crate) cross_arb: Value,
    pub(crate) api_keys: Value,
    pub(crate) strategy_logs: Value,
    pub(crate) balance_history: Value,
    pub(crate) workspace: Value,
    pub(crate) strategies: Value,
    pub(crate) processes: Value,
    pub(crate) agents: Value,
    pub(crate) gateway_status: Value,
    pub(crate) credentials_status: Value,
}

#[derive(Clone, Default, PartialEq)]
pub(crate) struct WorkspaceFetchData {
    pub(crate) workspace: Value,
    pub(crate) strategies: Value,
    pub(crate) processes: Value,
    pub(crate) agents: Value,
    pub(crate) gateway_status: Value,
    pub(crate) credentials_status: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WorkspaceSummaryData {
    pub(crate) strategy_count: String,
    pub(crate) process_count: String,
    pub(crate) agent_count: String,
    pub(crate) risk_status: String,
}

impl WorkspaceSummaryData {
    pub(crate) fn from_value(value: &Value, lang: Language) -> Self {
        Self {
            strategy_count: text_at(value, "strategy_count", lang),
            process_count: text_at(value, "process_count", lang),
            agent_count: text_at(value, "agent_count", lang),
            risk_status: text_at(value, "risk_status", lang),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StrategyWorkspaceRowData {
    pub(crate) strategy_id: String,
    pub(crate) strategy_kind: String,
    pub(crate) status: String,
    pub(crate) config_path: String,
    pub(crate) run_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProcessWorkspaceRowData {
    pub(crate) strategy_id: String,
    pub(crate) status: String,
    pub(crate) process_id: String,
    pub(crate) started_at: String,
    pub(crate) last_heartbeat_at: String,
    pub(crate) restart_count: String,
    pub(crate) last_exit_code: String,
    pub(crate) last_error: String,
    pub(crate) log_configured: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AgentWorkspaceRowData {
    pub(crate) agent_id: String,
    pub(crate) tenant_id: String,
    pub(crate) status: String,
    pub(crate) last_heartbeat_at: String,
    pub(crate) capabilities: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GatewayWorkspaceData {
    pub(crate) status: String,
    pub(crate) agent_id: String,
    pub(crate) endpoint: String,
    pub(crate) last_heartbeat_at: String,
    pub(crate) exchange_count: usize,
    pub(crate) exchanges: Vec<GatewayExchangeRowData>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GatewayExchangeRowData {
    pub(crate) exchange_id: String,
    pub(crate) market: String,
    pub(crate) status: String,
    pub(crate) last_error: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CredentialStatusRowData {
    pub(crate) slot_id: String,
    pub(crate) exchange_id: String,
    pub(crate) tenant_id: String,
    pub(crate) configured: bool,
    pub(crate) last_verified_at: String,
    pub(crate) health: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExchangeCredentialField {
    pub(crate) name: String,
    pub(crate) label: String,
    pub(crate) required: bool,
}

impl ExchangeCredentialField {
    fn from_value(value: &Value, lang: Language) -> Self {
        Self {
            name: text_at(value, "field", Language::En),
            label: text_at(value, "label", lang),
            required: value
                .get("required")
                .and_then(Value::as_bool)
                .unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExchangeCredentialSchema {
    pub(crate) exchange: String,
    pub(crate) label: String,
    pub(crate) fields: Vec<ExchangeCredentialField>,
}

impl ExchangeCredentialSchema {
    pub(crate) fn from_supported_rows(value: &Value, lang: Language) -> Vec<Self> {
        let mut rows = as_array(value);
        if rows.is_empty() {
            rows = fallback_exchange_schemas();
        }
        normalize_exchange_api_key_schemas(rows)
            .into_iter()
            .map(|row| Self::from_value(&row, lang))
            .collect()
    }

    fn from_value(value: &Value, lang: Language) -> Self {
        Self {
            exchange: text_at(value, "exchange", Language::En),
            label: text_at(value, "label", lang),
            fields: as_array(value.get("fields").unwrap_or(&Value::Null))
                .iter()
                .map(|field| ExchangeCredentialField::from_value(field, lang))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExchangeCredentialAccountRow {
    pub(crate) exchange: String,
    pub(crate) label: String,
    pub(crate) account_id: String,
    pub(crate) account_label: String,
    pub(crate) credential_namespace: String,
    pub(crate) row_key: String,
    pub(crate) exchange_account_id: String,
    pub(crate) wallet_address: String,
    pub(crate) is_vault_address: String,
    pub(crate) fields: Vec<ExchangeCredentialAccountField>,
    pub(crate) enabled: bool,
    pub(crate) default_account: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AccountManagerAccountRow {
    pub(crate) account_id: String,
    pub(crate) name: String,
    pub(crate) exchange: String,
    pub(crate) account_type: String,
    pub(crate) description: String,
    pub(crate) env_prefix: String,
    pub(crate) credential_namespace: String,
    pub(crate) enabled: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CredentialAccountOption {
    pub(crate) value: String,
    pub(crate) label: String,
    pub(crate) env_prefix: String,
    pub(crate) credential_namespace: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExchangeCredentialPanelData {
    pub(crate) enabled_exchange_text: String,
    pub(crate) restart_required: bool,
    pub(crate) store_path: String,
    pub(crate) account_manager_rows: Vec<AccountManagerAccountRow>,
    pub(crate) exchange_rows: Vec<ExchangeCredentialAccountRow>,
    pub(crate) supported_rows: Vec<ExchangeCredentialSchema>,
    pub(crate) configured_account_count: usize,
}

impl ExchangeCredentialPanelData {
    pub(crate) fn from_status(value: &Value, lang: Language) -> Self {
        let enabled_exchange_text = value
            .get("enabled_exchanges")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|item| crate::utils::value_text(item, lang))
                    .filter(|item| !item.trim().is_empty() && item != "-")
                    .collect::<Vec<_>>()
                    .join(" / ")
            })
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "-".to_string());
        let exchange_rows = ExchangeCredentialAccountRow::from_rows(
            value.get("exchanges").unwrap_or(&Value::Null),
            lang,
        );
        let account_manager_rows = AccountManagerAccountRow::from_rows(
            value
                .get("account_manager_accounts")
                .unwrap_or(&Value::Null),
            lang,
        );
        let configured_account_count = exchange_rows
            .iter()
            .filter(|row| row.fields.iter().all(|field| field.configured))
            .count();
        Self {
            enabled_exchange_text,
            restart_required: bool_at(value, "restart_required"),
            store_path: text_at(value, "store_path", lang),
            account_manager_rows,
            supported_rows: ExchangeCredentialSchema::from_supported_rows(
                value.get("supported_exchanges").unwrap_or(&Value::Null),
                lang,
            ),
            exchange_rows,
            configured_account_count,
        }
    }
}

impl CredentialAccountOption {
    pub(crate) fn for_exchange(
        api_keys: &Value,
        exchange: &str,
        current_account: &str,
        lang: Language,
    ) -> Vec<Self> {
        let mut options = Vec::new();
        for row in AccountManagerAccountRow::from_rows(
            api_keys
                .get("account_manager_accounts")
                .unwrap_or(&Value::Null),
            lang,
        )
        .into_iter()
        .filter(|row| row.exchange.eq_ignore_ascii_case(exchange) && row.enabled)
        {
            let account_id = if row.account_id.trim().is_empty() || row.account_id == "-" {
                "default".to_string()
            } else {
                row.account_id
            };
            if options
                .iter()
                .any(|option: &CredentialAccountOption| option.value == account_id)
            {
                continue;
            }
            options.push(Self {
                value: account_id.clone(),
                label: account_id,
                env_prefix: row.env_prefix,
                credential_namespace: row.credential_namespace,
            });
        }
        let _ = current_account;
        options.sort_by(|left, right| {
            let left_default = left.value.eq_ignore_ascii_case("default");
            let right_default = right.value.eq_ignore_ascii_case("default");
            right_default
                .cmp(&left_default)
                .then_with(|| left.value.to_lowercase().cmp(&right.value.to_lowercase()))
        });
        options
    }
}

impl AccountManagerAccountRow {
    pub(crate) fn from_rows(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value)
            .into_iter()
            .map(|row| Self {
                account_id: text_at(&row, "account_id", Language::En),
                name: text_at(&row, "name", lang),
                exchange: text_at(&row, "exchange", Language::En),
                account_type: text_at(&row, "account_type", lang),
                description: text_at(&row, "description", lang),
                env_prefix: text_at(&row, "env_prefix", Language::En),
                credential_namespace: text_at(&row, "credential_namespace", Language::En),
                enabled: bool_at(&row, "enabled"),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExchangeCredentialAccountField {
    pub(crate) name: String,
    pub(crate) label: String,
    pub(crate) required: bool,
    pub(crate) configured: bool,
    pub(crate) source: String,
    pub(crate) display_value: String,
}

impl ExchangeCredentialAccountField {
    fn from_value(value: &Value, lang: Language) -> Self {
        let masked = text_at(value, "masked", lang);
        let plain_value = text_at(value, "value", lang);
        let display_value = if masked != "-" {
            masked
        } else if plain_value != "-" {
            plain_value
        } else {
            "-".to_string()
        };
        Self {
            name: text_at(value, "field", Language::En),
            label: text_at(value, "label", lang),
            required: bool_at(value, "required"),
            configured: bool_at(value, "configured"),
            source: text_at(value, "source", lang),
            display_value,
        }
    }
}

impl ExchangeCredentialAccountRow {
    pub(crate) fn from_rows(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value)
            .into_iter()
            .map(|row| {
                let exchange = text_at(&row, "exchange", lang);
                let account_id = text_at(&row, "account_id", lang);
                let raw_fields = as_array(row.get("fields").unwrap_or(&Value::Null));
                Self {
                    row_key: format!("{exchange}:{account_id}"),
                    exchange,
                    label: text_at(&row, "label", lang),
                    account_id,
                    account_label: text_at(&row, "account_label", lang),
                    credential_namespace: text_at(&row, "credential_namespace", Language::En),
                    exchange_account_id: exchange_field_value(&raw_fields, "account_id"),
                    wallet_address: exchange_field_value(&raw_fields, "wallet_address"),
                    is_vault_address: exchange_field_value(&raw_fields, "is_vault_address"),
                    fields: raw_fields
                        .iter()
                        .map(|field| ExchangeCredentialAccountField::from_value(field, lang))
                        .collect(),
                    enabled: bool_at(&row, "enabled"),
                    default_account: bool_at(&row, "is_default_account"),
                }
            })
            .collect()
    }
}

impl StrategyWorkspaceRowData {
    pub(crate) fn from_rows(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value)
            .into_iter()
            .map(|row| Self {
                strategy_id: text_at(&row, "strategy_id", lang),
                strategy_kind: text_at(&row, "strategy_kind", lang),
                status: text_at(&row, "status", lang),
                config_path: text_at(&row, "config_path", lang),
                run_id: text_at(&row, "run_id", lang),
            })
            .collect()
    }
}

impl ProcessWorkspaceRowData {
    pub(crate) fn from_rows(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value)
            .into_iter()
            .map(|row| Self {
                strategy_id: text_at(&row, "strategy_id", lang),
                status: text_at(&row, "status", lang),
                process_id: text_at(&row, "process_id", lang),
                started_at: text_at(&row, "started_at", lang),
                last_heartbeat_at: text_at(&row, "last_heartbeat_at", lang),
                restart_count: text_at(&row, "restart_count", lang),
                last_exit_code: text_at(&row, "last_exit_code", lang),
                last_error: text_at(&row, "last_error", lang),
                log_configured: bool_at(&row, "log_configured"),
            })
            .collect()
    }
}

impl AgentWorkspaceRowData {
    pub(crate) fn from_rows(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value)
            .into_iter()
            .map(|row| Self {
                agent_id: text_at(&row, "agent_id", lang),
                tenant_id: text_at(&row, "tenant_id", lang),
                status: text_at(&row, "status", lang),
                last_heartbeat_at: text_at(&row, "last_heartbeat_at", lang),
                capabilities: row
                    .get("capabilities")
                    .and_then(Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| "-".to_string()),
            })
            .collect()
    }
}

impl GatewayWorkspaceData {
    pub(crate) fn from_value(value: &Value, lang: Language) -> Self {
        let gateway = value.get("gateway").unwrap_or(value);
        let exchanges = GatewayExchangeRowData::from_rows(
            gateway
                .get("connected_exchanges")
                .unwrap_or(&Value::Array(Vec::new())),
            lang,
        );
        Self {
            status: text_at(gateway, "status", lang),
            agent_id: text_at(gateway, "agent_id", lang),
            endpoint: text_at(gateway, "endpoint", lang),
            last_heartbeat_at: text_at(gateway, "last_heartbeat_at", lang),
            exchange_count: exchanges.len(),
            exchanges,
        }
    }
}

impl GatewayExchangeRowData {
    fn from_rows(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value)
            .into_iter()
            .map(|row| Self {
                exchange_id: text_at(&row, "exchange_id", lang),
                market: text_at(&row, "market", lang),
                status: text_at(&row, "status", lang),
                last_error: text_at(&row, "last_error", lang),
            })
            .collect()
    }
}

impl CredentialStatusRowData {
    pub(crate) fn from_response(value: &Value, lang: Language) -> Vec<Self> {
        let slots = as_array(value.get("slots").unwrap_or(&Value::Null));
        if !slots.is_empty() {
            return slots
                .into_iter()
                .map(|row| Self {
                    slot_id: text_at(&row, "slot_id", lang),
                    exchange_id: text_at(&row, "exchange_id", lang),
                    tenant_id: text_at(&row, "tenant_id", lang),
                    configured: bool_at(&row, "configured"),
                    last_verified_at: text_at(&row, "last_verified_at", lang),
                    health: text_at(&row, "health", lang),
                })
                .collect();
        }
        ExchangeCredentialAccountRow::from_rows(
            value.get("exchanges").unwrap_or(&Value::Null),
            lang,
        )
        .into_iter()
        .map(|row| {
            let required_fields = row.fields.iter().filter(|field| field.required).count();
            let configured = if required_fields == 0 {
                row.fields.iter().all(|field| field.configured)
            } else {
                row.fields
                    .iter()
                    .filter(|field| field.required)
                    .all(|field| field.configured)
            };
            Self {
                slot_id: row.row_key,
                exchange_id: row.exchange,
                tenant_id: "-".to_string(),
                configured,
                last_verified_at: "-".to_string(),
                health: if configured {
                    "healthy".to_string()
                } else {
                    "missing".to_string()
                },
            }
        })
        .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BalanceSummary {
    pub(crate) exchange: String,
    pub(crate) total_usdt: f64,
    pub(crate) available_usdt: f64,
    pub(crate) asset_count: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RuntimeTimingRow {
    pub(crate) exchange: String,
    pub(crate) latency_ms: Option<f64>,
    pub(crate) request_failures: u64,
    pub(crate) rate_limit_events: u64,
    pub(crate) last_successful_request: String,
    pub(crate) backoff_until: String,
    pub(crate) last_error: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StrategyRuntimeOverviewData {
    pub(crate) active_strategy_count: usize,
    pub(crate) strategy_name: String,
    pub(crate) mode_text: String,
    pub(crate) strategy_ready: bool,
    pub(crate) selected_count: usize,
    pub(crate) candidate_count: usize,
    pub(crate) runtime_running: String,
    pub(crate) snapshot_counts: String,
    pub(crate) last_snapshot_at: String,
}

impl StrategyRuntimeOverviewData {
    pub(crate) fn from_values(
        status: &Value,
        runtime_publisher: &Value,
        spot_arb: &Value,
        lang: Language,
    ) -> Self {
        let strategy_name = text_at(status, "strategy_name", lang);
        let active_strategy_count = if strategy_name == "-" { 0 } else { 1 };
        let selected_count = spot_arb
            .get("selected_symbols")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or_default();
        let candidate_count = spot_arb
            .get("candidate_symbols")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or_default();
        Self {
            active_strategy_count,
            strategy_name,
            mode_text: format!(
                "{} / {}",
                text_at(status, "trading_mode", lang),
                text_at(status, "dry_run", lang)
            ),
            strategy_ready: !bool_at(status, "kill_switch_active"),
            selected_count,
            candidate_count,
            runtime_running: text_at(runtime_publisher, "running", lang),
            snapshot_counts: format!(
                "{} / {}",
                text_at(runtime_publisher, "snapshots_generated", lang),
                text_at(runtime_publisher, "snapshots_persisted", lang)
            ),
            last_snapshot_at: text_at(runtime_publisher, "last_snapshot_at", lang),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExchangeConnectionChipData {
    pub(crate) exchange: String,
    pub(crate) connected: bool,
}

impl ExchangeConnectionChipData {
    pub(crate) fn from_rows(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value)
            .into_iter()
            .map(|row| Self {
                exchange: text_at(&row, "exchange", lang),
                connected: bool_at(&row, "connected"),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BalanceTrendData {
    pub(crate) chart_points: String,
    pub(crate) change: f64,
}

impl BalanceTrendData {
    pub(crate) fn from_history(value: &Value) -> Self {
        let profit_points = crate::utils::profit_history_values(value);
        Self {
            chart_points: crate::utils::sparkline_points(&profit_points, 640.0, 180.0, 14.0),
            change: profit_points.last().copied().unwrap_or_default(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.chart_points.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExchangeHealthRowData {
    pub(crate) exchange: String,
    pub(crate) connected: bool,
    pub(crate) fresh_symbol_count: String,
    pub(crate) stale_symbol_count: String,
    pub(crate) avg_latency_ms: String,
    pub(crate) last_book_update_at: String,
}

impl ExchangeHealthRowData {
    pub(crate) fn from_rows(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value)
            .into_iter()
            .map(|row| Self {
                exchange: text_at(&row, "exchange", lang),
                connected: bool_at(&row, "connected"),
                fresh_symbol_count: text_at(&row, "fresh_symbol_count", lang),
                stale_symbol_count: text_at(&row, "stale_symbol_count", lang),
                avg_latency_ms: text_at(&row, "avg_latency_ms", lang),
                last_book_update_at: text_at(&row, "last_book_update_at", lang),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BookSnapshotRowData {
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) best_bid: String,
    pub(crate) best_ask: String,
    pub(crate) book_age_ms: String,
    pub(crate) source: String,
    pub(crate) is_stale: bool,
}

impl BookSnapshotRowData {
    pub(crate) fn from_rows(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value)
            .into_iter()
            .map(|row| Self {
                exchange: text_at(&row, "exchange", lang),
                symbol: text_at(&row, "symbol", lang),
                best_bid: text_at(&row, "best_bid", lang),
                best_ask: text_at(&row, "best_ask", lang),
                book_age_ms: text_at(&row, "book_age_ms", lang),
                source: text_at(&row, "source", lang),
                is_stale: bool_at(&row, "is_stale"),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SymbolRuleRowData {
    pub(crate) internal_symbol: String,
    pub(crate) exchange: String,
    pub(crate) base_asset: String,
    pub(crate) quote_asset: String,
}

impl SymbolRuleRowData {
    pub(crate) fn from_response(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value.get("symbol_rules").unwrap_or(&Value::Null))
            .into_iter()
            .map(|row| Self {
                internal_symbol: text_at(&row, "internal_symbol", lang),
                exchange: text_at(&row, "exchange", lang),
                base_asset: text_at(&row, "base_asset", lang),
                quote_asset: text_at(&row, "quote_asset", lang),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SymbolOpportunityRowData {
    pub(crate) symbol: String,
    pub(crate) net_spread_bps: String,
    pub(crate) risk_adjusted_score: String,
    pub(crate) warnings: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RecentOpportunityRowData {
    pub(crate) timestamp: String,
    pub(crate) symbol: String,
    pub(crate) buy_exchange: String,
    pub(crate) sell_exchange: String,
    pub(crate) spread_bps: String,
    pub(crate) profit: String,
}

impl SymbolOpportunityRowData {
    pub(crate) fn from_response(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value.get("arbitrage").unwrap_or(&Value::Null))
            .into_iter()
            .map(|row| Self {
                symbol: text_at(&row, "symbol", lang),
                net_spread_bps: text_at(&row, "net_spread_bps", lang),
                risk_adjusted_score: text_at(&row, "risk_adjusted_score", lang),
                warnings: text_at(&row, "warnings", lang),
            })
            .collect()
    }
}

impl RecentOpportunityRowData {
    pub(crate) fn from_response(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value.get("recent").unwrap_or(&Value::Null))
            .into_iter()
            .map(|row| Self {
                timestamp: text_at(&row, "timestamp", lang),
                symbol: text_at(&row, "symbol", lang),
                buy_exchange: text_at(&row, "buy_exchange", lang),
                sell_exchange: text_at(&row, "sell_exchange", lang),
                spread_bps: crate::utils::bps_text(&row, "spread_bps"),
                profit: crate::utils::money_text(&row, "profit_usdt"),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DryRunPlanRowData {
    pub(crate) timestamp: String,
    pub(crate) intent: String,
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) side: String,
    pub(crate) notional: String,
    pub(crate) quantity: String,
    pub(crate) would_submit: bool,
}

impl DryRunPlanRowData {
    pub(crate) fn from_rows(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value)
            .into_iter()
            .map(|row| Self {
                timestamp: text_at(&row, "timestamp", lang),
                intent: text_at(&row, "intent", lang),
                exchange: text_at(&row, "exchange", lang),
                symbol: text_at(&row, "symbol", lang),
                side: text_at(&row, "side", lang),
                notional: crate::utils::format_usdt(crate::utils::numeric_at(&row, "notional")),
                quantity: text_at(&row, "quantity", lang),
                would_submit: bool_at(&row, "would_submit"),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RiskPanelData {
    pub(crate) kill_switch_active: bool,
    pub(crate) auto_stop_active: bool,
    pub(crate) stop_reason: String,
    pub(crate) triggered_at: String,
    pub(crate) auto_stop_summary: Option<String>,
    pub(crate) events: Vec<RiskEventRowData>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RiskEventRowData {
    pub(crate) timestamp: String,
    pub(crate) severity: String,
    pub(crate) symbol: String,
    pub(crate) reason: String,
    pub(crate) details: String,
    pub(crate) event_type: String,
}

impl RiskPanelData {
    pub(crate) fn from_value(value: &Value, lang: Language) -> Self {
        let kill_switch = value.get("kill_switch").unwrap_or(&Value::Null);
        let events = RiskEventRowData::from_rows(
            value
                .get("risk_events")
                .or_else(|| value.get("events"))
                .unwrap_or(&Value::Null),
            lang,
        );
        let auto_stop_summary = events
            .iter()
            .rev()
            .find(|row| row.event_type.eq_ignore_ascii_case("risk_auto_stop"))
            .map(|row| format!("{} {}", row.symbol, row.details));
        Self {
            kill_switch_active: bool_at(kill_switch, "active"),
            auto_stop_active: auto_stop_summary.is_some(),
            stop_reason: text_at(kill_switch, "reason", lang),
            triggered_at: text_at(kill_switch, "triggered_at", lang),
            auto_stop_summary,
            events,
        }
    }
}

impl RiskEventRowData {
    fn from_rows(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value)
            .into_iter()
            .map(|row| Self {
                timestamp: text_at(&row, "timestamp", lang),
                severity: text_at(&row, "severity", lang),
                symbol: text_at(&row, "symbol", lang),
                reason: text_at(&row, "reason", lang),
                details: text_at(&row, "details", lang),
                event_type: text_at(&row, "event_type", lang),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InventoryAssetRowData {
    pub(crate) exchange: String,
    pub(crate) asset: String,
    pub(crate) total: String,
    pub(crate) available: String,
    pub(crate) locally_reserved: String,
}

impl InventoryAssetRowData {
    pub(crate) fn from_rows(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value)
            .into_iter()
            .map(|row| Self {
                exchange: text_at(&row, "exchange", lang),
                asset: text_at(&row, "asset", lang),
                total: text_at(&row, "total", lang),
                available: text_at(&row, "available", lang),
                locally_reserved: text_at(&row, "locally_reserved", lang),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FeeScheduleRowData {
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) maker_fee_bps: String,
    pub(crate) taker_fee_bps: String,
    pub(crate) source: String,
}

impl FeeScheduleRowData {
    pub(crate) fn from_rows(value: &Value, lang: Language) -> Vec<Self> {
        as_array(value)
            .into_iter()
            .map(|row| Self {
                exchange: text_at(&row, "exchange", lang),
                symbol: text_at(&row, "symbol", lang),
                maker_fee_bps: text_at(&row, "maker_fee_bps", lang),
                taker_fee_bps: text_at(&row, "taker_fee_bps", lang),
                source: text_at(&row, "source", lang),
            })
            .collect()
    }

    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| Self {
                exchange: text_at(row, "exchange", lang),
                symbol: text_at(row, "symbol", lang),
                maker_fee_bps: text_at(row, "maker_fee_bps", lang),
                taker_fee_bps: text_at(row, "taker_fee_bps", lang),
                source: text_at(row, "source", lang),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RiskDebugPanelData {
    pub(crate) raw: String,
    pub(crate) live_preflight: String,
    pub(crate) small_live_gate: String,
}

impl RiskDebugPanelData {
    pub(crate) fn from_value(value: &Value) -> Self {
        Self {
            raw: crate::utils::pretty(value),
            live_preflight: crate::utils::pretty(
                value.get("live_preflight").unwrap_or(&Value::Null),
            ),
            small_live_gate: crate::utils::pretty(
                value.get("small_live_gate").unwrap_or(&Value::Null),
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RuntimePublisherPanelData {
    pub(crate) status: String,
    pub(crate) exchanges: String,
    pub(crate) components: String,
    pub(crate) errors: String,
    pub(crate) audit: String,
    pub(crate) runtime_health: String,
}

impl RuntimePublisherPanelData {
    pub(crate) fn from_values(runtime_publisher: &Value, health: &Value, audit: &Value) -> Self {
        Self {
            status: crate::utils::pretty(runtime_publisher),
            exchanges: crate::utils::pretty(
                runtime_publisher.get("exchanges").unwrap_or(&Value::Null),
            ),
            components: crate::utils::pretty(
                runtime_publisher.get("components").unwrap_or(&Value::Null),
            ),
            errors: crate::utils::pretty(runtime_publisher.get("errors").unwrap_or(&Value::Null)),
            audit: crate::utils::pretty(audit),
            runtime_health: crate::utils::pretty(health.get("runtime").unwrap_or(&Value::Null)),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DebugJsonPanelData {
    pub(crate) primary: String,
    pub(crate) secondary: String,
}

impl DebugJsonPanelData {
    pub(crate) fn from_values(primary: &Value, secondary: &Value) -> Self {
        Self {
            primary: crate::utils::pretty(primary),
            secondary: crate::utils::pretty(secondary),
        }
    }

    pub(crate) fn from_primary(primary: &Value) -> Self {
        Self {
            primary: crate::utils::pretty(primary),
            secondary: String::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StrategyLogPanelData {
    pub(crate) page_size: usize,
}

impl StrategyLogPanelData {
    pub(crate) fn from_value(value: &Value) -> Self {
        Self {
            page_size: value
                .get("page_size")
                .and_then(Value::as_u64)
                .map(|value| value.clamp(20, 100) as usize)
                .unwrap_or(50),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SymbolControlUpdate {
    pub(crate) control_symbols: Option<Value>,
}

impl SymbolControlUpdate {
    pub(crate) fn from_response(value: &Value) -> Self {
        Self {
            control_symbols: value.get("symbols").cloned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbSourceData {
    pub(crate) summary: Value,
    pub(crate) settings: Value,
    pub(crate) opportunities: Vec<Value>,
    pub(crate) signals: Vec<Value>,
    pub(crate) hedge_records: Vec<Value>,
    pub(crate) repair_tasks: Vec<Value>,
    pub(crate) market_snapshots: Vec<Value>,
    pub(crate) private_events: Vec<Value>,
    pub(crate) risk_events: Vec<Value>,
    pub(crate) instruments: Vec<Value>,
    pub(crate) instrument_feasibility: Value,
    pub(crate) position_bundles: Vec<Value>,
    pub(crate) open_orders: Vec<Value>,
    pub(crate) arbitrage_results: Vec<Value>,
    pub(crate) profit_summary: Value,
    pub(crate) account_console: Vec<Value>,
    pub(crate) account_readiness: Value,
    pub(crate) strategy_readiness: Value,
    pub(crate) exchanges: Vec<String>,
    pub(crate) symbols: Vec<String>,
}

impl CrossArbSourceData {
    pub(crate) fn from_dashboard(cross_arb: &Value) -> Self {
        let summary = cross_arb.get("summary").cloned().unwrap_or(Value::Null);
        let settings = cross_arb.get("settings").cloned().unwrap_or(Value::Null);
        let opportunities = rows_at(cross_arb, "opportunities");
        let configured_exchanges = summary
            .get("enabled_exchanges")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(canonical_exchange_name)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let inferred_exchanges = crate::utils::cross_arb_string_set(
            &opportunities,
            &[
                "long_exchange",
                "short_exchange",
                "maker_exchange",
                "taker_exchange",
            ],
        );
        let configured_symbols = settings
            .get("symbols")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .filter(|symbol| !symbol.trim().is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let inferred_symbols =
            crate::utils::cross_arb_string_set(&opportunities, &["canonical_symbol", "symbol"]);
        Self {
            summary,
            settings,
            signals: rows_at(cross_arb, "signals"),
            hedge_records: rows_at(cross_arb, "hedge_records"),
            repair_tasks: rows_at(cross_arb, "hedge_repair_tasks"),
            market_snapshots: rows_at(cross_arb, "market_snapshots"),
            private_events: rows_at(cross_arb, "private_events"),
            risk_events: rows_at(cross_arb, "risk_events"),
            instruments: rows_at(cross_arb, "instruments"),
            instrument_feasibility: cross_arb
                .get("instrument_feasibility")
                .cloned()
                .unwrap_or(Value::Null),
            position_bundles: rows_at(cross_arb, "position_bundles"),
            open_orders: rows_at(cross_arb, "open_orders"),
            arbitrage_results: rows_at(cross_arb, "arbitrage_results"),
            profit_summary: cross_arb
                .get("profit_summary")
                .cloned()
                .unwrap_or(Value::Null),
            account_console: rows_at(cross_arb, "account_console"),
            account_readiness: cross_arb
                .get("account_readiness")
                .cloned()
                .unwrap_or(Value::Null),
            strategy_readiness: cross_arb
                .get("strategy_readiness")
                .cloned()
                .unwrap_or(Value::Null),
            exchanges: if configured_exchanges.is_empty() {
                inferred_exchanges
            } else {
                configured_exchanges
            },
            symbols: if configured_symbols.is_empty() {
                inferred_symbols
            } else {
                configured_symbols
            },
            opportunities,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SpotArbSourceData {
    pub(crate) books: Value,
    pub(crate) opportunities: Value,
    pub(crate) plans: Value,
    pub(crate) candidates: Vec<Value>,
    pub(crate) selected: Vec<Value>,
    pub(crate) exchange_console: Vec<Value>,
    pub(crate) account_console: Vec<Value>,
    pub(crate) inventory: Vec<Value>,
    pub(crate) position_console: Vec<Value>,
    pub(crate) fees: Vec<Value>,
    pub(crate) arbitrage_slot_status: Value,
    pub(crate) active_slot_symbols: Vec<Value>,
    pub(crate) liquidation_rows: Vec<Value>,
    pub(crate) initial_entry_status: Value,
    pub(crate) initial_entry_rows: Vec<Value>,
    pub(crate) blocked_slot_symbols: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SpotArbDerivedData {
    pub(crate) books: Vec<Value>,
    pub(crate) opportunities: Vec<Value>,
    pub(crate) best_opportunities: Vec<Value>,
    pub(crate) plans: Vec<Value>,
    pub(crate) detail_books: Vec<Value>,
    pub(crate) detail_opportunities: Vec<Value>,
    pub(crate) detail_plans: Vec<Value>,
    pub(crate) active_books: usize,
    pub(crate) stale_books: usize,
    pub(crate) accepted_spread_count: usize,
}

pub(crate) struct SpotArbDashboardInput<'a> {
    pub(crate) spot_arb: &'a Value,
    pub(crate) books: Value,
    pub(crate) opportunities: Value,
    pub(crate) plans: Value,
    pub(crate) inventory: &'a Value,
    pub(crate) exchanges: &'a Value,
    pub(crate) fees: &'a Value,
    pub(crate) config: &'a Value,
    pub(crate) lang: Language,
}

impl SpotArbDerivedData {
    pub(crate) fn from_source(
        source: &SpotArbSourceData,
        detail_symbol_key: &str,
        lang: Language,
    ) -> Self {
        let mut books = as_array(&source.books)
            .into_iter()
            .filter(crate::utils::is_spot_book)
            .collect::<Vec<_>>();
        books.sort_by(|left, right| {
            text_at(left, "symbol", lang)
                .cmp(&text_at(right, "symbol", lang))
                .then(text_at(left, "exchange", lang).cmp(&text_at(right, "exchange", lang)))
        });
        let mut opportunities = as_array(&source.opportunities)
            .into_iter()
            .filter(crate::utils::is_spot_spot_opp)
            .collect::<Vec<_>>();
        opportunities.sort_by(|left, right| {
            crate::utils::numeric_at(right, "tt_immediate_net_bps")
                .partial_cmp(&crate::utils::numeric_at(left, "tt_immediate_net_bps"))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut plans = as_array(&source.plans);
        plans.reverse();
        let detail_books = rows_for_symbol(&books, detail_symbol_key, lang);
        let detail_opportunities = rows_for_symbol(&opportunities, detail_symbol_key, lang);
        let detail_plans = rows_for_symbol(&plans, detail_symbol_key, lang);
        let active_books = books.iter().filter(|row| !bool_at(row, "is_stale")).count();
        let stale_books = books.iter().filter(|row| bool_at(row, "is_stale")).count();
        let accepted_spread_count = opportunities
            .iter()
            .filter(|row| bool_at(row, "accepted"))
            .count();
        Self {
            best_opportunities: crate::utils::best_opportunity_per_symbol(&opportunities, lang),
            books,
            opportunities,
            plans,
            detail_books,
            detail_opportunities,
            detail_plans,
            active_books,
            stale_books,
            accepted_spread_count,
        }
    }
}

impl SpotArbSourceData {
    pub(crate) fn from_dashboard(input: SpotArbDashboardInput<'_>) -> Self {
        let SpotArbDashboardInput {
            spot_arb,
            books,
            opportunities,
            plans,
            inventory,
            exchanges,
            fees,
            config,
            lang,
        } = input;
        let source_books = if spot_arb.get("books").is_some() {
            spot_arb.get("books").cloned().unwrap_or(Value::Null)
        } else {
            books
        };
        let source_opps = if spot_arb.get("opportunities").is_some() {
            spot_arb
                .get("opportunities")
                .cloned()
                .unwrap_or_else(|| json!([]))
        } else {
            opportunities
                .get("arbitrage")
                .cloned()
                .unwrap_or_else(|| json!([]))
        };
        let source_plans = if spot_arb.get("dry_run_plans").is_some() {
            spot_arb
                .get("dry_run_plans")
                .cloned()
                .unwrap_or_else(|| json!([]))
        } else {
            plans
        };
        let enabled_exchanges = crate::utils::enabled_exchange_set(config);
        let raw_exchange_rows = as_array(exchanges)
            .into_iter()
            .filter(|row| crate::utils::row_matches_enabled_exchange(row, &enabled_exchanges))
            .collect::<Vec<_>>();
        let balances = crate::utils::balance_summaries_for_enabled(inventory, &enabled_exchanges);
        let exchange_console = {
            let rows = rows_at(spot_arb, "exchange_console");
            if rows.is_empty() {
                raw_exchange_rows.clone()
            } else {
                rows
            }
        };
        let account_console = {
            let rows = rows_at(spot_arb, "account_console");
            if rows.is_empty() {
                balances
                    .iter()
                    .map(|row| {
                        json!({
                            "exchange": row.exchange,
                            "account_id": "default",
                            "enabled": true,
                            "connected": crate::utils::exchange_connected(&raw_exchange_rows, &row.exchange),
                            "usdt_total": row.total_usdt,
                            "usdt_available": row.available_usdt,
                            "total_equity_usdt": row.total_usdt,
                            "coin_balance_usdt": 0.0,
                            "asset_count": row.asset_count,
                            "last_book_update_at": crate::utils::exchange_last_update(&raw_exchange_rows, &row.exchange, lang),
                        })
                    })
                    .collect::<Vec<_>>()
            } else {
                rows
            }
        };
        let inventory_rows = as_array(inventory)
            .into_iter()
            .filter(|row| crate::utils::row_matches_enabled_exchange(row, &enabled_exchanges))
            .collect::<Vec<_>>();
        let position_console = {
            let rows = rows_at(spot_arb, "position_console");
            if rows.is_empty() {
                inventory_rows.clone()
            } else {
                rows
            }
        };
        let fee_rows = as_array(fees)
            .into_iter()
            .filter(|row| crate::utils::row_matches_enabled_exchange(row, &enabled_exchanges))
            .collect::<Vec<_>>();
        let arbitrage_slot_status = spot_arb
            .get("arbitrage_slot_status")
            .cloned()
            .unwrap_or(Value::Null);
        let initial_entry_status = spot_arb
            .get("initial_entry_status")
            .cloned()
            .unwrap_or(Value::Null);
        Self {
            books: source_books,
            opportunities: source_opps,
            plans: source_plans,
            candidates: rows_at(spot_arb, "candidate_symbols"),
            selected: rows_at(spot_arb, "selected_symbols"),
            exchange_console,
            account_console,
            inventory: inventory_rows,
            position_console,
            fees: fee_rows,
            active_slot_symbols: as_array(
                arbitrage_slot_status
                    .get("active_symbols")
                    .unwrap_or(&Value::Null),
            ),
            liquidation_rows: rows_at(spot_arb, "liquidation_plans"),
            initial_entry_rows: as_array(initial_entry_status.get("rows").unwrap_or(&Value::Null)),
            blocked_slot_symbols: as_array(
                arbitrage_slot_status
                    .get("blocked_symbols")
                    .unwrap_or(&Value::Null),
            ),
            arbitrage_slot_status,
            initial_entry_status,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbPanelData {
    pub(crate) target_refresh_ms: String,
    pub(crate) latest_event_at: String,
    pub(crate) data_source: String,
    pub(crate) event_dir: String,
    pub(crate) event_file_count: usize,
    pub(crate) valid_events: String,
    pub(crate) parse_errors: String,
    pub(crate) can_open_opportunities: String,
    pub(crate) open_signals: String,
    pub(crate) order_events: String,
    pub(crate) estimated_edge_usdt: f64,
    pub(crate) realized_close_pct: f64,
    pub(crate) realized_profit_usdt: f64,
    pub(crate) source_tone: &'static str,
    pub(crate) source_label: String,
    pub(crate) settings_path: String,
    pub(crate) settings_symbol_count: usize,
    pub(crate) instrument_known_symbols: String,
    pub(crate) instrument_symbol_count: String,
    pub(crate) instrument_feasible_symbols: String,
    pub(crate) instrument_feasible_instruments: String,
    pub(crate) instrument_known_instruments: String,
    pub(crate) instrument_required_notional_max: String,
    pub(crate) closed_arbitrages: String,
    pub(crate) win_rate: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbEventSummaryData {
    pub(crate) fill_events: usize,
    pub(crate) balance_events: usize,
    pub(crate) error_events: usize,
    pub(crate) private_balance_rows: Vec<Value>,
}

impl CrossArbEventSummaryData {
    pub(crate) fn from_events(
        private_events: &[Value],
        risk_events: &[Value],
        exchanges: &[String],
        lang: Language,
    ) -> Self {
        let enabled_exchanges = exchanges
            .iter()
            .map(|exchange| canonical_exchange_name(exchange))
            .collect::<std::collections::BTreeSet<_>>();
        let fill_events = private_events
            .iter()
            .filter(|row| text_at(row, "private_kind", lang).eq_ignore_ascii_case("fill"))
            .count();
        let private_balance_rows = private_events
            .iter()
            .filter(|row| text_at(row, "private_kind", lang).eq_ignore_ascii_case("balance"))
            .filter(|row| crate::utils::row_matches_enabled_exchange(row, &enabled_exchanges))
            .cloned()
            .collect::<Vec<_>>();
        let error_events = private_events
            .iter()
            .filter(|row| {
                text_at(row, "private_kind", lang).eq_ignore_ascii_case("error")
                    || text_at(row, "kind", lang).eq_ignore_ascii_case("error")
            })
            .count()
            + risk_events.len();
        Self {
            fill_events,
            balance_events: private_balance_rows.len(),
            error_events,
            private_balance_rows,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbReadinessData {
    pub(crate) account_text: String,
    pub(crate) account_label: String,
    pub(crate) account_class: &'static str,
    pub(crate) strategy_text: String,
    pub(crate) strategy_label: String,
    pub(crate) strategy_class: &'static str,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbSettingsFormData {
    pub(crate) binance_enabled: bool,
    pub(crate) bitget_enabled: bool,
    pub(crate) gate_enabled: bool,
    pub(crate) binance_account: String,
    pub(crate) bitget_account: String,
    pub(crate) gate_account: String,
    pub(crate) binance_env: String,
    pub(crate) bitget_env: String,
    pub(crate) gate_env: String,
    pub(crate) symbols_text: String,
    pub(crate) target_notional: String,
    pub(crate) max_notional: String,
    pub(crate) max_positions: String,
    pub(crate) min_open_spread: String,
    pub(crate) min_net_edge: String,
    pub(crate) close_profit: String,
    pub(crate) close_spread: String,
    pub(crate) execution_profile: String,
}

impl CrossArbSettingsFormData {
    pub(crate) fn from_settings(settings: &Value) -> Self {
        Self {
            binance_enabled: cross_arb_setting_exchange_enabled(settings, "binance"),
            bitget_enabled: cross_arb_setting_exchange_enabled(settings, "bitget"),
            gate_enabled: cross_arb_setting_exchange_enabled(settings, "gate"),
            binance_account: cross_arb_setting_exchange_text(settings, "binance", "account_id"),
            bitget_account: cross_arb_setting_exchange_text(settings, "bitget", "account_id"),
            gate_account: cross_arb_setting_exchange_text(settings, "gate", "account_id"),
            binance_env: cross_arb_setting_exchange_text(settings, "binance", "env_prefix"),
            bitget_env: cross_arb_setting_exchange_text(settings, "bitget", "env_prefix"),
            gate_env: cross_arb_setting_exchange_text(settings, "gate", "env_prefix"),
            symbols_text: cross_arb_symbols_text(settings),
            target_notional: number_setting_text(settings, "target_notional_usdt", 5.0),
            max_notional: number_setting_text(settings, "max_notional_usdt", 5.2),
            max_positions: number_setting_text(settings, "max_positions_per_exchange", 10.0),
            min_open_spread: number_setting_text(settings, "min_open_raw_spread", 0.005),
            min_net_edge: number_setting_text(settings, "min_open_maker_taker_net_edge", 0.005),
            close_profit: number_setting_text(settings, "lock_profit_dual_taker_pct", 0.0005),
            close_spread: number_setting_text(settings, "max_close_spread_pct", 0.0005),
            execution_profile: cross_arb_execution_profile(settings),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FundingArbExchangeFormData {
    pub(crate) exchange: String,
    pub(crate) enabled: bool,
    pub(crate) account_id: String,
    pub(crate) env_prefix: String,
    pub(crate) demo_trading: bool,
    pub(crate) private_ws_enabled: bool,
    pub(crate) position_mode: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FundingArbSettingsFormData {
    pub(crate) path: String,
    pub(crate) strategy_id: String,
    pub(crate) mode: String,
    pub(crate) per_exchange_limit: String,
    pub(crate) min_funding_rate: String,
    pub(crate) require_next_funding_time: bool,
    pub(crate) max_funding_snapshot_age_ms: String,
    pub(crate) min_seconds_to_settlement_at_scan: String,
    pub(crate) max_seconds_to_settlement_at_scan: String,
    pub(crate) scan_minute: String,
    pub(crate) notional_usdt: String,
    pub(crate) open_seconds_before_settlement: String,
    pub(crate) close_seconds_after_settlement: String,
    pub(crate) order_readback_delay_secs: String,
    pub(crate) close_limit_timeout_secs: String,
    pub(crate) close_limit_max_retries: String,
    pub(crate) max_slippage_pct: String,
    pub(crate) allow_existing_symbol_position: bool,
    pub(crate) symbol_allowlist_text: String,
    pub(crate) symbol_blocklist_text: String,
    pub(crate) exchanges: Vec<FundingArbExchangeFormData>,
}

impl FundingArbSettingsFormData {
    pub(crate) fn from_settings(settings: &Value, lang: Language) -> Self {
        let exchanges = rows_at(settings, "exchanges")
            .into_iter()
            .map(|row| FundingArbExchangeFormData {
                exchange: text_at(&row, "exchange", Language::En),
                enabled: bool_at(&row, "enabled"),
                account_id: text_at(&row, "account_id", lang),
                env_prefix: text_at(&row, "env_prefix", Language::En),
                demo_trading: bool_at(&row, "demo_trading"),
                private_ws_enabled: bool_at(&row, "private_ws_enabled"),
                position_mode: text_at(&row, "position_mode", Language::En),
            })
            .collect::<Vec<_>>();
        Self {
            path: text_at(settings, "path", lang),
            strategy_id: text_at(settings, "strategy_id", Language::En),
            mode: text_at(settings, "mode", Language::En),
            per_exchange_limit: number_setting_text(settings, "per_exchange_limit", 1.0),
            min_funding_rate: number_setting_text(settings, "min_funding_rate", -0.005),
            require_next_funding_time: bool_at(settings, "require_next_funding_time"),
            max_funding_snapshot_age_ms: number_setting_text(
                settings,
                "max_funding_snapshot_age_ms",
                5000.0,
            ),
            min_seconds_to_settlement_at_scan: optional_number_setting_text(
                settings,
                "min_seconds_to_settlement_at_scan",
            ),
            max_seconds_to_settlement_at_scan: optional_number_setting_text(
                settings,
                "max_seconds_to_settlement_at_scan",
            ),
            scan_minute: number_setting_text(settings, "scan_minute", 55.0),
            notional_usdt: number_setting_text(settings, "notional_usdt", 10.0),
            open_seconds_before_settlement: number_setting_text(
                settings,
                "open_seconds_before_settlement",
                1.0,
            ),
            close_seconds_after_settlement: number_setting_text(
                settings,
                "close_seconds_after_settlement",
                1.0,
            ),
            order_readback_delay_secs: number_setting_text(
                settings,
                "order_readback_delay_secs",
                3.0,
            ),
            close_limit_timeout_secs: number_setting_text(
                settings,
                "close_limit_timeout_secs",
                300.0,
            ),
            close_limit_max_retries: number_setting_text(settings, "close_limit_max_retries", 3.0),
            max_slippage_pct: number_setting_text(settings, "max_slippage_pct", 0.003),
            allow_existing_symbol_position: bool_at(settings, "allow_existing_symbol_position"),
            symbol_allowlist_text: value_array_text(settings, "symbol_allowlist"),
            symbol_blocklist_text: value_array_text(settings, "symbol_blocklist"),
            exchanges,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbSaveResultData {
    pub(crate) strategy_status: String,
    pub(crate) skipped: bool,
}

impl CrossArbSaveResultData {
    pub(crate) fn from_response(value: &Value) -> Self {
        let restart = value.get("restart").unwrap_or(&Value::Null);
        Self {
            strategy_status: text_at(restart, "strategy_status", Language::En),
            skipped: bool_at(restart, "skipped"),
        }
    }
}

impl CrossArbReadinessData {
    pub(crate) fn from_values(
        account_readiness: &Value,
        strategy_readiness: &Value,
        lang: Language,
    ) -> Self {
        let account_ready = bool_at(account_readiness, "all_required_credentials_configured");
        let strategy_ready = bool_at(strategy_readiness, "live_small_ready");
        Self {
            account_text: format!(
                "{} / {}",
                text_at(account_readiness, "ready_accounts", lang),
                text_at(account_readiness, "enabled_accounts", lang)
            ),
            account_label: if account_ready {
                s(lang, "all_credentials_ready")
            } else {
                format!(
                    "{}: {}",
                    s(lang, "missing_credentials"),
                    text_at(account_readiness, "missing_accounts", lang)
                )
            },
            account_class: if account_ready { "pill" } else { "pill warn" },
            strategy_text: format!(
                "{} / {}",
                text_at(strategy_readiness, "instrument_pairs", lang),
                text_at(strategy_readiness, "expected_exchange_symbol_pairs", lang)
            ),
            strategy_label: if strategy_ready {
                s(lang, "ready")
            } else {
                let missing_rules =
                    crate::utils::numeric_at(strategy_readiness, "missing_instrument_pairs")
                        as usize;
                let missing_books =
                    crate::utils::numeric_at(strategy_readiness, "missing_market_snapshot_pairs")
                        as usize;
                if missing_rules > 0 || missing_books > 0 {
                    format!(
                        "{}: {} / {}",
                        s(lang, "missing_pairs"),
                        missing_rules,
                        missing_books
                    )
                } else {
                    let blockers = strategy_readiness
                        .get("blockers")
                        .and_then(Value::as_array)
                        .map(Vec::len)
                        .unwrap_or_default();
                    format!("{}: {}", s(lang, "readiness_blockers"), blockers)
                }
            },
            strategy_class: if strategy_ready { "pill" } else { "pill warn" },
        }
    }
}

impl CrossArbPanelData {
    pub(crate) fn from_values(
        cross_arb: &Value,
        summary: &Value,
        settings: &Value,
        profit_summary: &Value,
        instrument_feasibility: &Value,
        lang: Language,
    ) -> Self {
        let data_source = text_at(cross_arb, "data_source", lang);
        let online = bool_at(cross_arb, "online");
        Self {
            target_refresh_ms: text_at(cross_arb, "target_refresh_ms", lang),
            latest_event_at: text_at(cross_arb, "latest_event_at", lang),
            data_source: data_source.clone(),
            event_dir: text_at(cross_arb, "event_dir", lang),
            event_file_count: cross_arb
                .get("files")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default(),
            valid_events: text_at(summary, "valid_events", lang),
            parse_errors: text_at(summary, "parse_errors", lang),
            can_open_opportunities: text_at(summary, "can_open_opportunities", lang),
            open_signals: text_at(summary, "open_signals", lang),
            order_events: text_at(summary, "order_events", lang),
            estimated_edge_usdt: crate::utils::numeric_at(summary, "estimated_edge_usdt"),
            realized_close_pct: crate::utils::numeric_at(summary, "realized_close_pct"),
            realized_profit_usdt: crate::utils::numeric_at(profit_summary, "realized_profit_usdt"),
            source_tone: if online {
                "pill"
            } else if data_source == "historical_events" {
                "pill warn"
            } else {
                "pill bad"
            },
            source_label: if online {
                s(lang, "online")
            } else if data_source == "historical_events" {
                s(lang, "historical_mode")
            } else {
                s(lang, "offline")
            },
            settings_path: text_at(settings, "path", lang),
            settings_symbol_count: settings
                .get("symbols")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default(),
            instrument_known_symbols: text_at(instrument_feasibility, "known_symbols", lang),
            instrument_symbol_count: text_at(instrument_feasibility, "symbol_count", lang),
            instrument_feasible_symbols: text_at(instrument_feasibility, "feasible_symbols", lang),
            instrument_feasible_instruments: text_at(
                instrument_feasibility,
                "feasible_instruments",
                lang,
            ),
            instrument_known_instruments: text_at(
                instrument_feasibility,
                "known_instruments",
                lang,
            ),
            instrument_required_notional_max: crate::utils::money_text(
                instrument_feasibility,
                "required_notional_max_usdt",
            ),
            closed_arbitrages: text_at(profit_summary, "closed_arbitrages", lang),
            win_rate: crate::utils::format_pct(crate::utils::numeric_at(
                profit_summary,
                "win_rate",
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbAccountRowData {
    pub(crate) exchange: String,
    pub(crate) account_id: String,
    pub(crate) credential_namespace: String,
    pub(crate) credential_text: String,
    pub(crate) credential_class: &'static str,
    pub(crate) status_text: String,
    pub(crate) status_class: &'static str,
    pub(crate) total: String,
    pub(crate) available: String,
    pub(crate) recorded_at: String,
    pub(crate) credentials_ready: bool,
    pub(crate) credential_exchange: String,
    pub(crate) credential_account: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbExchangeConsoleRowData {
    pub(crate) exchange: String,
    pub(crate) maker_volume: String,
    pub(crate) taker_volume: String,
    pub(crate) order_count: usize,
    pub(crate) taker_success_rate: String,
    pub(crate) latency: String,
}

impl CrossArbExchangeConsoleRowData {
    pub(crate) fn from_exchanges(
        exchanges: &[String],
        private_events: &[Value],
        lang: Language,
    ) -> Vec<Self> {
        exchanges
            .iter()
            .map(|exchange| Self {
                exchange: exchange.clone(),
                maker_volume: crate::utils::cross_arb_exchange_volume(
                    private_events,
                    exchange,
                    "Maker",
                ),
                taker_volume: crate::utils::cross_arb_exchange_volume(
                    private_events,
                    exchange,
                    "Taker",
                ),
                order_count: crate::utils::cross_arb_exchange_orders(private_events, exchange),
                taker_success_rate: crate::utils::cross_arb_exchange_taker_success(
                    private_events,
                    exchange,
                ),
                latency: crate::utils::cross_arb_exchange_latency(private_events, exchange, lang),
            })
            .collect()
    }
}

impl CrossArbAccountRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| {
                let exchange = text_at(row, "exchange", lang);
                let account_id = text_at(row, "account_id", lang);
                let credentials = row.get("credentials").unwrap_or(&Value::Null);
                let credentials_ready = row
                    .get("credentials")
                    .map(|credentials| bool_at(credentials, "configured_all_required"))
                    .unwrap_or(true);
                let credential_text = cross_arb_credential_summary_text(credentials, lang);
                let credential_class = if bool_at(credentials, "configured_all_required") {
                    "pill"
                } else if bool_at(credentials, "supported") {
                    "pill warn"
                } else {
                    "pill neutral"
                };
                let raw_status = text_at(row, "status", lang);
                let status_text = if raw_status.trim().is_empty() || raw_status == "-" {
                    s(lang, "configured")
                } else {
                    raw_status
                };
                let status_class = if status_text == "online" {
                    "pill"
                } else {
                    "pill neutral"
                };
                let credential_account = if account_id.trim().is_empty() || account_id == "-" {
                    "default".to_string()
                } else {
                    account_id.clone()
                };
                Self {
                    exchange: exchange.clone(),
                    account_id,
                    credential_namespace: text_at(row, "credential_namespace", lang),
                    credential_text,
                    credential_class,
                    status_text,
                    status_class,
                    total: crate::utils::money_text(row, "total"),
                    available: crate::utils::money_text(row, "available"),
                    recorded_at: text_at(row, "recorded_at", lang),
                    credentials_ready,
                    credential_exchange: exchange,
                    credential_account,
                }
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbOpportunityRowData {
    pub(crate) canonical_symbol: String,
    pub(crate) route: String,
    pub(crate) maker: String,
    pub(crate) taker: String,
    pub(crate) raw_open_spread: f64,
    pub(crate) raw_open_spread_text: String,
    pub(crate) maker_taker_net_edge: f64,
    pub(crate) maker_taker_net_edge_text: String,
    pub(crate) target_notional_usdt: String,
    pub(crate) executable_notional_usdt: String,
    pub(crate) fees: String,
    pub(crate) expected_funding_usdt: String,
    pub(crate) book_age_ms: String,
    pub(crate) can_open: bool,
    pub(crate) reject_reasons: String,
}

impl CrossArbOpportunityRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| {
                let raw_open_spread = crate::utils::numeric_at(row, "raw_open_spread");
                let maker_taker_net_edge = crate::utils::numeric_at(row, "maker_taker_net_edge");
                Self {
                    canonical_symbol: text_at(row, "canonical_symbol", lang),
                    route: format!(
                        "{} / {}",
                        text_at(row, "long_exchange", lang),
                        text_at(row, "short_exchange", lang)
                    ),
                    maker: format!(
                        "{} {}",
                        text_at(row, "maker_exchange", lang),
                        text_at(row, "maker_side", lang)
                    ),
                    taker: format!(
                        "{} {}",
                        text_at(row, "taker_exchange", lang),
                        text_at(row, "taker_side", lang)
                    ),
                    raw_open_spread,
                    raw_open_spread_text: crate::utils::format_pct(raw_open_spread),
                    maker_taker_net_edge,
                    maker_taker_net_edge_text: crate::utils::format_pct(maker_taker_net_edge),
                    target_notional_usdt: crate::utils::money_text(row, "target_notional_usdt"),
                    executable_notional_usdt: crate::utils::money_text(
                        row,
                        "executable_notional_usdt",
                    ),
                    fees: format!(
                        "{} / {}",
                        crate::utils::money_text(row, "open_fee_est_usdt"),
                        crate::utils::money_text(row, "close_fee_est_usdt")
                    ),
                    expected_funding_usdt: crate::utils::money_text(row, "expected_funding_usdt"),
                    book_age_ms: text_at(row, "book_age_ms", lang),
                    can_open: bool_at(row, "can_open"),
                    reject_reasons: text_at(row, "reject_reasons", lang),
                }
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbMarketSnapshotRowData {
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) best_bid: String,
    pub(crate) best_ask: String,
    pub(crate) book_age_ms: String,
    pub(crate) sequence: String,
    pub(crate) recorded_at: String,
}

impl CrossArbMarketSnapshotRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| Self {
                exchange: text_at(row, "exchange", lang),
                symbol: text_at(row, "symbol", lang),
                best_bid: crate::utils::price_text(row, "best_bid"),
                best_ask: crate::utils::price_text(row, "best_ask"),
                book_age_ms: text_at(row, "book_age_ms", lang),
                sequence: text_at(row, "sequence", lang),
                recorded_at: text_at(row, "recorded_at", lang),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbInstrumentRowData {
    pub(crate) exchange: String,
    pub(crate) canonical_symbol: String,
    pub(crate) exchange_symbol: String,
    pub(crate) price_tick: String,
    pub(crate) quantity_step: String,
    pub(crate) min_qty: String,
    pub(crate) min_notional: String,
    pub(crate) required_notional: String,
    pub(crate) headroom: String,
    pub(crate) headroom_value: f64,
    pub(crate) feasibility_label: String,
    pub(crate) feasibility_class: &'static str,
    pub(crate) status: String,
}

impl CrossArbInstrumentRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| {
                let feasibility = row.get("feasibility").unwrap_or(&Value::Null);
                let known = bool_at(feasibility, "known");
                let fits_max = bool_at(feasibility, "fits_max");
                let headroom_value = feasibility
                    .get("headroom_usdt")
                    .and_then(Value::as_f64)
                    .unwrap_or_default();
                Self {
                    exchange: text_at(row, "exchange", lang),
                    canonical_symbol: text_at(row, "canonical_symbol", lang),
                    exchange_symbol: row
                        .get("exchange_symbol")
                        .and_then(|value| value.get("symbol"))
                        .map(|value| crate::utils::value_text(value, lang))
                        .filter(|value| value != "-")
                        .unwrap_or_else(|| text_at(row, "exchange_symbol", lang)),
                    price_tick: text_at(row, "price_tick", lang),
                    quantity_step: text_at(row, "quantity_step", lang),
                    min_qty: text_at(row, "min_qty", lang),
                    min_notional: crate::utils::money_text(row, "min_notional"),
                    required_notional: feasibility
                        .get("required_notional_usdt")
                        .and_then(Value::as_f64)
                        .filter(|value| *value > 0.0)
                        .map(crate::utils::format_usdt)
                        .unwrap_or_else(|| "-".to_string()),
                    headroom: feasibility
                        .get("headroom_usdt")
                        .and_then(Value::as_f64)
                        .map(crate::utils::signed_usdt)
                        .unwrap_or_else(|| "-".to_string()),
                    headroom_value,
                    feasibility_label: if !known {
                        s(lang, "unknown")
                    } else if fits_max {
                        s(lang, "feasible")
                    } else {
                        s(lang, "infeasible")
                    },
                    feasibility_class: if !known {
                        "pill neutral"
                    } else if fits_max {
                        "pill"
                    } else {
                        "pill bad"
                    },
                    status: text_at(row, "status", lang),
                }
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbPositionBundleRowData {
    pub(crate) bundle_id: String,
    pub(crate) symbol: String,
    pub(crate) long_exchange: String,
    pub(crate) short_exchange: String,
    pub(crate) status: String,
    pub(crate) entry_net_edge_pct: f64,
    pub(crate) entry_net_edge_pct_text: String,
    pub(crate) close_profit_now: f64,
    pub(crate) close_profit_now_text: String,
    pub(crate) close_threshold_pct: String,
    pub(crate) closeable: bool,
    pub(crate) close_route: String,
    pub(crate) updated_at: String,
}

impl CrossArbPositionBundleRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| {
                let entry_net_edge_pct = crate::utils::numeric_at(row, "entry_net_edge_pct");
                let close_profit_now = crate::utils::cross_arb_close_profit_now(row);
                Self {
                    bundle_id: text_at(row, "bundle_id", lang),
                    symbol: text_at(row, "symbol", lang),
                    long_exchange: text_at(row, "long_exchange", lang),
                    short_exchange: text_at(row, "short_exchange", lang),
                    status: text_at(row, "status", lang),
                    entry_net_edge_pct,
                    entry_net_edge_pct_text: crate::utils::format_pct(entry_net_edge_pct),
                    close_profit_now,
                    close_profit_now_text: crate::utils::format_pct(close_profit_now),
                    close_threshold_pct: crate::utils::format_pct(crate::utils::numeric_at(
                        row,
                        "close_threshold_pct",
                    )),
                    closeable: bool_at(row, "closeable"),
                    close_route: crate::utils::cross_arb_close_route(row, lang),
                    updated_at: text_at(row, "updated_at", lang),
                }
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbOpenOrderRowData {
    pub(crate) source: String,
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) side: String,
    pub(crate) price: String,
    pub(crate) remaining_qty: String,
    pub(crate) notional: String,
    pub(crate) status: String,
    pub(crate) order_ref: String,
}

impl CrossArbOpenOrderRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| Self {
                source: crate::utils::order_source_label(row, lang),
                exchange: text_at(row, "exchange", lang),
                symbol: text_at(row, "symbol", lang),
                side: text_at(row, "side", lang),
                price: crate::utils::price_text(row, "price"),
                remaining_qty: text_or_fallback(text_at(row, "remaining_qty", lang), || {
                    text_at(row, "remaining_quantity", lang)
                }),
                notional: crate::utils::money_text(row, "notional"),
                status: text_at(row, "status", lang),
                order_ref: text_or_fallback(text_at(row, "client_order_id", lang), || {
                    text_at(row, "order_id", lang)
                }),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbResultRowData {
    pub(crate) bundle_id: String,
    pub(crate) symbol: String,
    pub(crate) route: String,
    pub(crate) target_notional_usdt: String,
    pub(crate) realized_profit_usdt: f64,
    pub(crate) realized_pnl: String,
    pub(crate) cumulative_profit_usdt: f64,
    pub(crate) cumulative_profit: String,
    pub(crate) updated_at: String,
}

impl CrossArbResultRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| {
                let realized_profit_usdt = crate::utils::numeric_at(row, "realized_profit_usdt");
                let cumulative_profit_usdt =
                    crate::utils::numeric_at(row, "cumulative_profit_usdt");
                Self {
                    bundle_id: text_at(row, "bundle_id", lang),
                    symbol: text_at(row, "symbol", lang),
                    route: format!(
                        "{} / {}",
                        text_at(row, "long_exchange", lang),
                        text_at(row, "short_exchange", lang)
                    ),
                    target_notional_usdt: crate::utils::money_text(row, "target_notional_usdt"),
                    realized_profit_usdt,
                    realized_pnl: format!(
                        "{} / {}",
                        crate::utils::signed_usdt(realized_profit_usdt),
                        crate::utils::format_pct(crate::utils::numeric_at(
                            row,
                            "close_net_profit_pct",
                        ))
                    ),
                    cumulative_profit_usdt,
                    cumulative_profit: crate::utils::signed_usdt(cumulative_profit_usdt),
                    updated_at: text_at(row, "updated_at", lang),
                }
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbRepairTaskRowData {
    pub(crate) status: String,
    pub(crate) failed_exchange: String,
    pub(crate) side: String,
    pub(crate) quantity: String,
    pub(crate) attempt_count: String,
    pub(crate) last_error: String,
}

impl CrossArbRepairTaskRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| Self {
                status: text_at(row, "status", lang),
                failed_exchange: text_at(row, "failed_exchange", lang),
                side: format!(
                    "{} {}",
                    text_at(row, "side", lang),
                    text_at(row, "position_side", lang)
                ),
                quantity: text_at(row, "quantity", lang),
                attempt_count: text_at(row, "attempt_count", lang),
                last_error: text_at(row, "last_error", lang),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SpotExchangeConsoleRowData {
    pub(crate) exchange: String,
    pub(crate) enabled: bool,
    pub(crate) public_ws_connected: bool,
    pub(crate) private_ws_connected: bool,
    pub(crate) fresh_symbol_count: String,
    pub(crate) stale_symbol_count: String,
    pub(crate) usdt_total: String,
    pub(crate) usdt_available: String,
    pub(crate) coin_assets: String,
    pub(crate) fee_aux_asset: String,
    pub(crate) avg_latency_ms: String,
    pub(crate) max_latency_ms: String,
    pub(crate) parse_error_count: String,
    pub(crate) sequence_gap_count: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SpotArbPanelData {
    pub(crate) target_refresh_ms: String,
    pub(crate) target_exchanges_text: String,
    pub(crate) subscription_limit: String,
    pub(crate) candidate_limit: String,
    pub(crate) active_window_seconds: String,
    pub(crate) selected_count: usize,
    pub(crate) candidate_count: usize,
    pub(crate) used_slots: String,
    pub(crate) remaining_slots: String,
    pub(crate) cfg_monitored_symbols: usize,
    pub(crate) cfg_max_arbitrage_symbols: u64,
    pub(crate) cfg_notional: f64,
    pub(crate) cfg_initial_entry: f64,
    pub(crate) cfg_threshold_percent: f64,
    pub(crate) trading_mode: String,
    pub(crate) initial_completed: String,
    pub(crate) initial_target: String,
    pub(crate) initial_observed: String,
    pub(crate) initial_failed: String,
    pub(crate) initial_pending: String,
}

impl SpotArbPanelData {
    pub(crate) fn from_values(
        spot_arb: &Value,
        config: &Value,
        arbitrage_slot_status: &Value,
        initial_entry_status: &Value,
        selected_count: usize,
        candidate_count: usize,
        lang: Language,
    ) -> Self {
        let target_exchanges_text = spot_arb
            .get("target_exchanges")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|item| crate::utils::value_text(item, lang))
                    .filter(|item| !item.trim().is_empty() && item != "-")
                    .collect::<Vec<_>>()
                    .join(" / ")
            })
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "-".to_string());
        let cfg_threshold_bps = crate::utils::config_threshold_bps(config).unwrap_or(30.0);
        Self {
            target_refresh_ms: text_at(spot_arb, "target_refresh_ms", lang),
            target_exchanges_text,
            subscription_limit: text_at(spot_arb, "subscription_limit", lang),
            candidate_limit: text_at(spot_arb, "candidate_limit", lang),
            active_window_seconds: text_at(arbitrage_slot_status, "active_window_seconds", lang),
            selected_count,
            candidate_count,
            used_slots: format!(
                "{} / {}",
                text_at(arbitrage_slot_status, "used", lang),
                text_at(arbitrage_slot_status, "limit", lang)
            ),
            remaining_slots: text_at(arbitrage_slot_status, "remaining", lang),
            cfg_monitored_symbols: crate::utils::config_monitored_symbol_count(config),
            cfg_max_arbitrage_symbols: crate::utils::config_max_arbitrage_symbols(config)
                .unwrap_or(5),
            cfg_notional: crate::utils::config_notional(config).unwrap_or(5.0),
            cfg_initial_entry: crate::utils::config_initial_entry_notional(config).unwrap_or(5.0),
            cfg_threshold_percent: cfg_threshold_bps / 100.0,
            trading_mode: text_at(config, "trading_mode", lang),
            initial_completed: text_at(initial_entry_status, "completed", lang),
            initial_target: text_at(initial_entry_status, "target", lang),
            initial_observed: text_at(initial_entry_status, "observed", lang),
            initial_failed: text_at(initial_entry_status, "failed", lang),
            initial_pending: text_at(initial_entry_status, "pending", lang),
        }
    }
}

impl SpotExchangeConsoleRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| Self {
                exchange: text_at(row, "exchange", lang),
                enabled: bool_at(row, "enabled"),
                public_ws_connected: bool_at(row, "public_ws_connected"),
                private_ws_connected: bool_at(row, "private_ws_connected"),
                fresh_symbol_count: text_at(row, "fresh_symbol_count", lang),
                stale_symbol_count: text_at(row, "stale_symbol_count", lang),
                usdt_total: crate::utils::format_usdt(crate::utils::numeric_at(row, "usdt_total")),
                usdt_available: crate::utils::format_usdt(crate::utils::numeric_at(
                    row,
                    "usdt_available",
                )),
                coin_assets: crate::utils::coin_assets_text(row, lang),
                fee_aux_asset: crate::utils::fee_aux_asset_text(row, lang),
                avg_latency_ms: text_at(row, "avg_latency_ms", lang),
                max_latency_ms: text_at(row, "max_latency_ms", lang),
                parse_error_count: text_at(row, "parse_error_count", lang),
                sequence_gap_count: text_at(row, "sequence_gap_count", lang),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SpotAccountConsoleRowData {
    pub(crate) exchange: String,
    pub(crate) account_label: String,
    pub(crate) enabled: bool,
    pub(crate) connected: bool,
    pub(crate) quote_asset: String,
    pub(crate) usdt_total: String,
    pub(crate) usdt_available: String,
    pub(crate) coin_balance_usdt: String,
    pub(crate) coin_assets: String,
    pub(crate) fee_aux_asset: String,
    pub(crate) avg_latency_ms: String,
    pub(crate) total_equity_usdt: String,
    pub(crate) last_book_update_at: String,
}

impl SpotAccountConsoleRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| Self {
                exchange: text_at(row, "exchange", lang),
                account_label: text_at(row, "account_label", lang),
                enabled: bool_at(row, "enabled"),
                connected: bool_at(row, "connected"),
                quote_asset: text_at(row, "quote_asset", lang),
                usdt_total: crate::utils::format_usdt(crate::utils::numeric_at(row, "usdt_total")),
                usdt_available: crate::utils::format_usdt(crate::utils::numeric_at(
                    row,
                    "usdt_available",
                )),
                coin_balance_usdt: crate::utils::format_usdt(crate::utils::numeric_at(
                    row,
                    "coin_balance_usdt",
                )),
                coin_assets: crate::utils::coin_assets_text(row, lang),
                fee_aux_asset: crate::utils::fee_aux_asset_text(row, lang),
                avg_latency_ms: text_at(row, "avg_latency_ms", lang),
                total_equity_usdt: crate::utils::format_usdt(crate::utils::numeric_at(
                    row,
                    "total_equity_usdt",
                )),
                last_book_update_at: text_at(row, "last_book_update_at", lang),
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SpotBookRowData {
    pub(crate) symbol: String,
    pub(crate) exchange: String,
    pub(crate) best_bid: String,
    pub(crate) best_ask: String,
    pub(crate) spread_bps: String,
    pub(crate) book_age_ms: String,
    pub(crate) latency_ms: String,
    pub(crate) source: String,
    pub(crate) sequence: String,
    pub(crate) stale_reason: String,
}

impl SpotBookRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter().map(|row| Self::from_value(row, lang)).collect()
    }

    fn from_value(row: &Value, lang: Language) -> Self {
        Self {
            symbol: text_at(row, "symbol", lang),
            exchange: text_at(row, "exchange", lang),
            best_bid: crate::utils::price_text(row, "best_bid"),
            best_ask: crate::utils::price_text(row, "best_ask"),
            spread_bps: crate::utils::book_spread_bps_text(row),
            book_age_ms: text_at(row, "book_age_ms", lang),
            latency_ms: text_at(row, "latency_ms", lang),
            source: text_at(row, "source", lang),
            sequence: text_at(row, "sequence", lang),
            stale_reason: text_at(row, "stale_reason", lang),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SpotOpportunityRowData {
    pub(crate) symbol: String,
    pub(crate) route: String,
    pub(crate) buy_vwap: String,
    pub(crate) sell_vwap: String,
    pub(crate) tt_immediate_net_bps: f64,
    pub(crate) tt_immediate_net_bps_text: String,
    pub(crate) tt_immediate_net_pnl: f64,
    pub(crate) tt_immediate_net_pnl_text: String,
    pub(crate) required_capital_usdt: String,
    pub(crate) confidence: String,
    pub(crate) expected_return_on_capital: f64,
    pub(crate) expected_return_on_capital_text: String,
    pub(crate) risk_adjusted_score: f64,
    pub(crate) risk_adjusted_score_text: String,
    pub(crate) accepted: bool,
    pub(crate) rejection_reasons: String,
    pub(crate) warnings: String,
}

impl SpotOpportunityRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter().map(|row| Self::from_value(row, lang)).collect()
    }

    fn from_value(row: &Value, lang: Language) -> Self {
        let tt_immediate_net_bps = crate::utils::numeric_at(row, "tt_immediate_net_bps");
        let tt_immediate_net_pnl = crate::utils::numeric_at(row, "tt_immediate_net_pnl");
        let expected_return_on_capital =
            crate::utils::numeric_at(row, "expected_return_on_capital");
        let risk_adjusted_score = crate::utils::numeric_at(row, "risk_adjusted_score");
        Self {
            symbol: text_at(row, "symbol", lang),
            route: format!(
                "{} -> {}",
                text_at(row, "buy_exchange", lang),
                text_at(row, "sell_exchange", lang)
            ),
            buy_vwap: crate::utils::price_text(row, "buy_vwap"),
            sell_vwap: crate::utils::price_text(row, "sell_vwap"),
            tt_immediate_net_bps,
            tt_immediate_net_bps_text: crate::utils::bps_text(row, "tt_immediate_net_bps"),
            tt_immediate_net_pnl,
            tt_immediate_net_pnl_text: crate::utils::money_text(row, "tt_immediate_net_pnl"),
            required_capital_usdt: crate::utils::money_text(row, "required_capital_usdt"),
            confidence: text_at(row, "confidence", lang),
            expected_return_on_capital,
            expected_return_on_capital_text: crate::utils::format_pct(expected_return_on_capital),
            risk_adjusted_score,
            risk_adjusted_score_text: format!("{risk_adjusted_score:.2}"),
            accepted: bool_at(row, "accepted"),
            rejection_reasons: text_at(row, "rejection_reasons", lang),
            warnings: text_at(row, "warnings", lang),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SpotOrderRowData {
    pub(crate) timestamp: String,
    pub(crate) source: String,
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) side: String,
    pub(crate) remaining_qty: String,
    pub(crate) price: String,
    pub(crate) notional: String,
    pub(crate) status: String,
    pub(crate) order_ref: String,
}

impl SpotOrderRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter().map(|row| Self::from_value(row, lang)).collect()
    }

    fn from_value(row: &Value, lang: Language) -> Self {
        Self {
            timestamp: text_at(row, "timestamp", lang),
            source: crate::utils::order_source_label(row, lang),
            exchange: text_at(row, "exchange", lang),
            symbol: text_at(row, "symbol", lang),
            side: text_at(row, "side", lang),
            remaining_qty: crate::utils::order_remaining_text(row, lang),
            price: crate::utils::price_text(row, "price"),
            notional: crate::utils::money_text(row, "notional"),
            status: text_at(row, "status", lang),
            order_ref: crate::utils::order_ref_text(row, lang),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InitialEntryBlockerRowData {
    pub(crate) symbol: String,
    pub(crate) rejection_reason: String,
}

impl InitialEntryBlockerRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .filter(|row| !bool_at(row, "passed"))
            .map(|row| Self {
                symbol: text_at(row, "symbol", lang),
                rejection_reason: text_at(row, "rejection_reason", lang),
            })
            .collect()
    }
}

fn cross_arb_credential_summary_text(credentials: &Value, lang: Language) -> String {
    if !bool_at(credentials, "supported") {
        return s(lang, "unknown");
    }
    let summary = format!(
        "{} / {}",
        text_at(credentials, "configured", lang),
        text_at(credentials, "required", lang)
    );
    if bool_at(credentials, "configured_all_required") {
        return summary;
    }
    let missing = cross_arb_missing_credential_text(credentials);
    if missing.is_empty() {
        summary
    } else {
        format!("{summary} · {missing}")
    }
}

fn cross_arb_missing_credential_text(credentials: &Value) -> String {
    credentials
        .get("missing_required")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .take(3)
                .map(|item| {
                    let label = text_or_fallback(text_at(item, "display", Language::En), || {
                        text_at(item, "code", Language::En)
                    });
                    let keys = item
                        .get("expected_env_parts")
                        .and_then(Value::as_array)
                        .map(|keys| {
                            keys.iter()
                                .filter_map(|parts| {
                                    parts.as_array().map(|parts| {
                                        parts
                                            .iter()
                                            .filter_map(Value::as_str)
                                            .collect::<Vec<_>>()
                                            .join("_")
                                    })
                                })
                                .take(2)
                                .collect::<Vec<_>>()
                                .join("/")
                        })
                        .unwrap_or_default();
                    if keys.is_empty() {
                        label
                    } else {
                        format!("{label}: {keys}")
                    }
                })
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_default()
}

fn text_or_fallback(value: String, fallback: impl FnOnce() -> String) -> String {
    if value.trim().is_empty() || value == "-" {
        fallback()
    } else {
        value
    }
}

fn rows_at(value: &Value, key: &str) -> Vec<Value> {
    as_array(value.get(key).unwrap_or(&Value::Null))
}

fn rows_for_symbol(rows: &[Value], symbol_key: &str, lang: Language) -> Vec<Value> {
    rows.iter()
        .filter(|row| {
            crate::utils::normalize_symbol_text(&text_at(row, "symbol", lang)) == symbol_key
        })
        .cloned()
        .collect()
}

fn cross_arb_setting_exchange<'a>(settings: &'a Value, exchange: &str) -> Option<&'a Value> {
    settings
        .get("exchanges")
        .and_then(Value::as_array)
        .and_then(|rows| {
            rows.iter()
                .find(|row| text_at(row, "exchange", Language::En).eq_ignore_ascii_case(exchange))
        })
}

fn cross_arb_setting_exchange_enabled(settings: &Value, exchange: &str) -> bool {
    cross_arb_setting_exchange(settings, exchange)
        .map(|row| bool_at(row, "enabled"))
        .unwrap_or_else(|| matches!(exchange, "binance" | "bitget" | "gate"))
}

fn cross_arb_setting_exchange_text(settings: &Value, exchange: &str, key: &str) -> String {
    cross_arb_setting_exchange(settings, exchange)
        .map(|row| text_at(row, key, Language::En))
        .filter(|value| value != "-")
        .unwrap_or_default()
}

fn cross_arb_symbols_text(settings: &Value) -> String {
    settings
        .get("symbols")
        .and_then(Value::as_array)
        .map(|symbols| {
            symbols
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn value_array_text(settings: &Value, key: &str) -> String {
    settings
        .get(key)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| crate::utils::value_text(item, Language::En))
                .filter(|value| !value.trim().is_empty() && value != "-")
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn number_setting_text(settings: &Value, key: &str, default: f64) -> String {
    let value = crate::utils::numeric_at(settings, key);
    if value.is_finite() {
        value.to_string()
    } else {
        default.to_string()
    }
}

fn optional_number_setting_text(settings: &Value, key: &str) -> String {
    let value = settings.get(key).unwrap_or(&Value::Null);
    if value.is_null() {
        String::new()
    } else {
        crate::utils::value_text(value, Language::En)
    }
}

fn cross_arb_execution_profile(settings: &Value) -> String {
    let mode = text_at(settings, "mode", Language::En).to_ascii_lowercase();
    if mode == "livesmall" && bool_at(settings, "execution_dry_run") {
        "live_small_dry_run".to_string()
    } else {
        "simulation".to_string()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExchangeStatisticRow {
    pub(crate) exchange: String,
    pub(crate) connected: bool,
    pub(crate) day_volume: f64,
    pub(crate) taker_volume: f64,
    pub(crate) maker_volume: f64,
    pub(crate) volume_share_pct: f64,
    pub(crate) api_calls: String,
    pub(crate) ws_data: String,
    pub(crate) private_ws: String,
    pub(crate) latency: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CoinConsoleRow {
    pub(crate) symbol: String,
    pub(crate) state: String,
    pub(crate) control_state: String,
    pub(crate) subscribed: bool,
    pub(crate) disabled: bool,
    pub(crate) arbitraging: bool,
    pub(crate) exiting: bool,
    pub(crate) controllable: bool,
    pub(crate) capital: f64,
    pub(crate) exchanges: String,
    pub(crate) volume: f64,
    pub(crate) est_profit: f64,
    pub(crate) realized_pnl: f64,
    pub(crate) pnl_1h: f64,
    pub(crate) pnl_8h: f64,
    pub(crate) pnl_24h: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AnomalyTradeRow {
    pub(crate) symbol: String,
    pub(crate) inst_id_key: String,
    pub(crate) price: String,
    pub(crate) reason: String,
    pub(crate) class_name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SymbolExchangeDetailRow {
    pub(crate) exchange: String,
    pub(crate) bid: String,
    pub(crate) ask: String,
    pub(crate) spread_bps: String,
    pub(crate) age_ms: String,
    pub(crate) order_count: usize,
    pub(crate) notional: f64,
    pub(crate) state: String,
    pub(crate) class_name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PositionRow {
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) asset: String,
    pub(crate) total: String,
    pub(crate) available: String,
    pub(crate) reserved: String,
    pub(crate) locked: String,
    pub(crate) valuation: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AuxAssetRow {
    pub(crate) exchange: String,
    pub(crate) asset: String,
    pub(crate) total: String,
    pub(crate) available: String,
    pub(crate) valuation: String,
    pub(crate) configured: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LogRow {
    pub(crate) timestamp: String,
    pub(crate) level: String,
    pub(crate) source: String,
    pub(crate) message: String,
    pub(crate) class_name: String,
}

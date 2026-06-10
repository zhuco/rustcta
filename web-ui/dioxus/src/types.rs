use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

use crate::i18n::s;
use crate::utils::{
    as_array, bool_at, canonical_exchange_name, exchange_field_value, fallback_exchange_schemas,
    normalize_exchange_api_key_schemas, text_at, value_text,
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
    SpotArb,
    CrossArb,
    ExchangeLatency,
    FundingArb,
    ApiKeys,
}

impl ControlPanelView {
    pub(crate) const ALL: [Self; 6] = [
        Self::Workspace,
        Self::SpotArb,
        Self::CrossArb,
        Self::ApiKeys,
        Self::ExchangeLatency,
        Self::FundingArb,
    ];

    pub(crate) fn nav_label_key(self) -> &'static str {
        match self {
            Self::Workspace => "nav_workspace",
            Self::SpotArb => "nav_spot_arb",
            Self::CrossArb => "nav_cross_arb",
            Self::ExchangeLatency => "nav_exchange_latency",
            Self::FundingArb => "nav_funding_arb",
            Self::ApiKeys => "nav_api_keys",
        }
    }

    pub(crate) fn route_id(self) -> &'static str {
        match self {
            Self::Workspace => "workspace",
            Self::SpotArb => "spot-arb",
            Self::CrossArb => "cross-arb",
            Self::ExchangeLatency => "exchange-latency",
            Self::FundingArb => "funding-arb",
            Self::ApiKeys => "api-keys",
        }
    }

    pub(crate) fn from_route_id(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "workspace" => Some(Self::Workspace),
            "spot-arb" | "spot_arb" => Some(Self::SpotArb),
            "cross-arb" | "cross_arb" => Some(Self::CrossArb),
            "exchange-latency" | "exchange_latency" => Some(Self::ExchangeLatency),
            "funding-arb" | "funding_arb" => Some(Self::FundingArb),
            "api-keys" | "api_keys" => Some(Self::ApiKeys),
            "legacy" | "overview" | "exchanges" | "symbols" | "plans" | "risk" | "runtime"
            | "config" | "logs" => Some(Self::Workspace),
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
pub(crate) struct ExchangeLatencyTestData {
    pub(crate) mode: String,
    pub(crate) batch_size: String,
    pub(crate) timeout_ms: String,
    pub(crate) exchange_count: usize,
    pub(crate) elapsed_ms: String,
    pub(crate) rows: Vec<ExchangeLatencyRowData>,
}

impl ExchangeLatencyTestData {
    pub(crate) fn from_value(value: &Value, lang: Language) -> Self {
        Self {
            mode: text_at(value, "mode", lang),
            batch_size: text_at(value, "batch_size", lang),
            timeout_ms: text_at(value, "timeout_ms", lang),
            exchange_count: value
                .get("exchange_count")
                .and_then(Value::as_u64)
                .unwrap_or_default() as usize,
            elapsed_ms: text_at(value, "elapsed_ms", lang),
            rows: as_array(value.get("rows").unwrap_or(&Value::Null))
                .iter()
                .map(|row| ExchangeLatencyRowData::from_value(row, lang))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExchangeLatencyRowData {
    pub(crate) exchange: String,
    pub(crate) label: String,
    pub(crate) endpoint_source: String,
    pub(crate) rest_status: String,
    pub(crate) rest_latency_ms: String,
    pub(crate) rest_endpoint: String,
    pub(crate) rest_error: String,
    pub(crate) ws_status: String,
    pub(crate) ws_latency_ms: String,
    pub(crate) ws_endpoint: String,
    pub(crate) ws_error: String,
}

impl ExchangeLatencyRowData {
    fn from_value(value: &Value, lang: Language) -> Self {
        let rest = value.get("rest").unwrap_or(&Value::Null);
        let websocket = value.get("websocket").unwrap_or(&Value::Null);
        Self {
            exchange: text_at(value, "exchange", lang),
            label: text_at(value, "label", lang),
            endpoint_source: text_at(value, "endpoint_source", lang),
            rest_status: text_at(rest, "status", lang),
            rest_latency_ms: latency_text(rest),
            rest_endpoint: text_at(rest, "endpoint", lang),
            rest_error: text_at(rest, "error", lang),
            ws_status: text_at(websocket, "status", lang),
            ws_latency_ms: latency_text(websocket),
            ws_endpoint: text_at(websocket, "endpoint", lang),
            ws_error: text_at(websocket, "error", lang),
        }
    }
}

fn latency_text(value: &Value) -> String {
    value
        .get("latency_ms")
        .and_then(Value::as_u64)
        .map(|latency| format!("{latency} ms"))
        .unwrap_or_else(|| "-".to_string())
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
        let target_exchange = canonical_exchange_name(exchange);
        for row in AccountManagerAccountRow::from_rows(
            api_keys
                .get("account_manager_accounts")
                .unwrap_or(&Value::Null),
            lang,
        )
        .into_iter()
        .filter(|row| {
            exchange_matches_alias(&row.exchange, exchange, &target_exchange) && row.enabled
        }) {
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
        let current_account = normalize_account_id(current_account);
        if !current_account.is_empty()
            && !options
                .iter()
                .any(|option: &CredentialAccountOption| option.value == current_account)
        {
            options.push(Self {
                value: current_account.clone(),
                label: current_account,
                env_prefix: String::new(),
                credential_namespace: String::new(),
            });
        }
        if options.is_empty() {
            options.push(Self {
                value: "default".to_string(),
                label: "default".to_string(),
                env_prefix: String::new(),
                credential_namespace: String::new(),
            });
        }
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

fn exchange_matches_alias(row_exchange: &str, exchange: &str, target_exchange: &str) -> bool {
    row_exchange.eq_ignore_ascii_case(exchange)
        || canonical_exchange_name(row_exchange) == target_exchange
}

fn normalize_account_id(account_id: &str) -> String {
    let account_id = account_id.trim();
    if account_id.is_empty() || account_id == "-" {
        String::new()
    } else {
        account_id.to_string()
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
        let name = text_at(value, "field", Language::En);
        let masked = text_at(value, "masked", lang);
        let plain_value = text_at(value, "value", lang);
        let display_value = if masked != "-" {
            masked
        } else if !credential_field_is_sensitive(&name) && plain_value != "-" {
            plain_value
        } else if bool_at(value, "configured") {
            "configured".to_string()
        } else {
            "-".to_string()
        };
        Self {
            name,
            label: text_at(value, "label", lang),
            required: bool_at(value, "required"),
            configured: bool_at(value, "configured"),
            source: text_at(value, "source", lang),
            display_value,
        }
    }
}

fn credential_field_is_sensitive(name: &str) -> bool {
    let name = name.trim().to_ascii_lowercase();
    name.contains("secret")
        || name.contains("passphrase")
        || name.contains("password")
        || name.contains("private_key")
        || name == "api_key"
        || name == "key"
        || name == "token"
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
    pub(crate) exchange_status: Vec<Value>,
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
            exchange_status: rows_at(cross_arb, "exchange_status"),
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
    pub(crate) event_dir: String,
    pub(crate) event_file_count: usize,
    pub(crate) valid_events: String,
    pub(crate) parse_errors: String,
    pub(crate) can_open_opportunities: String,
    pub(crate) market_can_open_opportunities: String,
    pub(crate) open_signals: String,
    pub(crate) order_events: String,
    pub(crate) estimated_edge_usdt: f64,
    pub(crate) realized_close_pct: f64,
    pub(crate) realized_profit_usdt: f64,
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
        let balance_events = private_events
            .iter()
            .filter(|row| text_at(row, "private_kind", lang).eq_ignore_ascii_case("balance"))
            .count();
        let private_balance_rows = private_events
            .iter()
            .filter(|row| private_user_event_kind(row, lang))
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
            balance_events,
            error_events,
            private_balance_rows,
        }
    }
}

fn private_user_event_kind(row: &Value, lang: Language) -> bool {
    matches!(
        text_at(row, "private_kind", lang)
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "balance" | "fill" | "order" | "trade"
    )
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SystemResourcePanelData {
    pub(crate) cpu: String,
    pub(crate) memory: String,
    pub(crate) process_cpu: String,
    pub(crate) process_memory: String,
}

impl SystemResourcePanelData {
    pub(crate) fn from_status(status: &Value) -> Self {
        let system = status.get("system").unwrap_or(&Value::Null);
        let memory_used = system.get("memory_used_bytes").and_then(Value::as_u64);
        let memory_total = system.get("memory_total_bytes").and_then(Value::as_u64);
        let memory_usage = system.get("memory_usage_pct").and_then(Value::as_f64);
        Self {
            cpu: percent_value_text(system.get("cpu_usage_pct").and_then(Value::as_f64)),
            memory: match (memory_usage, memory_used, memory_total) {
                (Some(usage), Some(_), Some(total)) => {
                    format!("{usage:.1}% / {}", bytes_text(total))
                }
                (Some(usage), _, _) => format!("{usage:.1}%"),
                (_, Some(used), Some(total)) => {
                    format!("{} / {}", bytes_text(used), bytes_text(total))
                }
                _ => "-".to_string(),
            },
            process_cpu: percent_value_text(
                system.get("process_cpu_usage_pct").and_then(Value::as_f64),
            ),
            process_memory: system
                .get("process_memory_bytes")
                .and_then(Value::as_u64)
                .map(bytes_text)
                .unwrap_or_else(|| "-".to_string()),
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
pub(crate) struct CrossArbExchangeFormData {
    pub(crate) exchange: String,
    pub(crate) label: String,
    pub(crate) enabled: bool,
    pub(crate) account_id: String,
    pub(crate) env_prefix: String,
    pub(crate) private_rest_enabled: bool,
    pub(crate) private_ws_enabled: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbSettingsFormData {
    pub(crate) exchanges: Vec<CrossArbExchangeFormData>,
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
    pub(crate) fn from_settings(
        settings: &Value,
        api_keys: &Value,
        seed_exchanges: &[String],
        lang: Language,
    ) -> Self {
        Self {
            exchanges: cross_arb_exchange_form_rows(settings, api_keys, seed_exchanges, lang),
            symbols_text: cross_arb_symbols_text(settings),
            target_notional: number_setting_text(settings, "target_notional_usdt", 5.0),
            max_notional: number_setting_text(settings, "max_notional_usdt", 5.2),
            max_positions: number_setting_text_any(
                settings,
                &["max_open_bundles", "max_positions_per_exchange"],
                10.0,
            ),
            min_open_spread: pct_setting_text_any(
                settings,
                &[
                    "min_open_raw_spread",
                    "min_open_spread_pct",
                    "min_open_raw_spread_pct",
                ],
                0.005,
            ),
            min_net_edge: pct_setting_text_any(
                settings,
                &[
                    "min_open_maker_taker_net_edge",
                    "min_open_net_profit_pct",
                    "min_open_net_edge_pct",
                ],
                0.002,
            ),
            close_profit: pct_setting_text_any(
                settings,
                &["close_min_net_profit_pct", "lock_profit_dual_taker_pct"],
                0.002,
            ),
            close_spread: pct_setting_text_any(
                settings,
                &["expected_close_spread_pct", "max_close_spread_pct"],
                0.002,
            ),
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
    pub(crate) fn from_values(account_readiness: &Value, lang: Language) -> Self {
        let account_ready = bool_at(account_readiness, "all_required_credentials_configured");
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
            strategy_text: String::new(),
            strategy_label: String::new(),
            strategy_class: "pill neutral",
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
        Self {
            target_refresh_ms: text_at(cross_arb, "target_refresh_ms", lang),
            latest_event_at: text_at(cross_arb, "latest_event_at", lang),
            event_dir: text_at(cross_arb, "event_dir", lang),
            event_file_count: cross_arb
                .get("files")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default(),
            valid_events: text_at(summary, "valid_events", lang),
            parse_errors: text_at(summary, "parse_errors", lang),
            can_open_opportunities: text_at(summary, "can_open_opportunities", lang),
            market_can_open_opportunities: text_at(summary, "market_can_open_opportunities", lang),
            open_signals: text_at(summary, "open_signals", lang),
            order_events: text_at(summary, "order_events", lang),
            estimated_edge_usdt: crate::utils::numeric_at(summary, "estimated_edge_usdt"),
            realized_close_pct: crate::utils::numeric_at(summary, "realized_close_pct"),
            realized_profit_usdt: crate::utils::numeric_at(profit_summary, "realized_profit_usdt"),
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
            win_rate: {
                let pct = first_numeric(profit_summary, &["win_rate_pct", "win_rate"]);
                if profit_summary.get("win_rate_pct").is_some() {
                    crate::utils::format_pct(pct / 100.0)
                } else {
                    crate::utils::format_pct(pct)
                }
            },
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
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CrossArbExchangeStatusCardData {
    pub(crate) exchange: String,
    pub(crate) status_class: &'static str,
    pub(crate) status_label: String,
    pub(crate) subscriptions: String,
    pub(crate) server_offset: String,
    pub(crate) reconnects: String,
}

impl CrossArbExchangeConsoleRowData {
    pub(crate) fn from_exchanges(exchanges: &[String], private_events: &[Value]) -> Vec<Self> {
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
            })
            .collect()
    }
}

impl CrossArbExchangeStatusCardData {
    pub(crate) fn from_values(
        exchanges: &[String],
        exchange_status: &[Value],
        console_rows: &[CrossArbExchangeConsoleRowData],
        private_events: &[Value],
        symbol_count: usize,
        lang: Language,
    ) -> Vec<Self> {
        exchanges
            .iter()
            .map(|exchange| {
                let status = exchange_status.iter().find(|row| {
                    canonical_exchange_name(&text_at(row, "exchange", Language::En))
                        == canonical_exchange_name(exchange)
                });
                let fallback_console = console_rows.iter().find(|row| {
                    canonical_exchange_name(&row.exchange) == canonical_exchange_name(exchange)
                });
                let subscriptions = status
                    .and_then(|row| row.get("streamed_symbol_count"))
                    .map(|value| value_text(value, lang))
                    .filter(|value| value != "-")
                    .unwrap_or_else(|| symbol_count.to_string());
                let server_offset = status
                    .and_then(|row| row.get("server_time_offset_ms").and_then(Value::as_i64))
                    .map(|value| format!("{value}ms"))
                    .unwrap_or_else(|| "-".to_string());
                let reconnects = status
                    .and_then(|row| row.get("disconnect_count"))
                    .map(|value| value_text(value, lang))
                    .filter(|value| value != "-")
                    .unwrap_or_else(|| "0".to_string());
                let private_connected = private_events.iter().any(|row| {
                    canonical_exchange_name(&text_at(row, "exchange", Language::En))
                        == canonical_exchange_name(exchange)
                        && private_user_event_kind(row, Language::En)
                });
                let online = status.is_some()
                    || fallback_console
                        .map(|row| row.order_count > 0)
                        .unwrap_or(false)
                    || private_connected;
                Self {
                    exchange: exchange.clone(),
                    status_class: if online { "pill" } else { "pill warn" },
                    status_label: if online {
                        s(lang, "online")
                    } else {
                        s(lang, "offline")
                    },
                    subscriptions,
                    server_offset,
                    reconnects,
                }
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
                let private_connected = private_user_event_kind(row, lang);
                let status_text = if private_connected {
                    s(lang, "online")
                } else if raw_status.trim().is_empty() || raw_status == "-" {
                    s(lang, "configured")
                } else {
                    raw_status
                };
                let status_class =
                    if private_connected || status_text.eq_ignore_ascii_case("online") {
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
                    total: first_money_text(
                        row,
                        &[
                            "account_console_balance_usdt",
                            "account_console_balance",
                            "console_balance_usdt",
                            "console_balance",
                            "total_equity_usdt",
                            "account_equity_usdt",
                            "account_equity",
                            "equity",
                            "usdt_total",
                            "total",
                        ],
                    ),
                    available: first_money_text(
                        row,
                        &[
                            "available_equity_usdt",
                            "available_equity",
                            "usdt_available",
                            "available",
                            "free",
                        ],
                    ),
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
    pub(crate) long_entry_price: String,
    pub(crate) short_entry_price: String,
    pub(crate) raw_book_prices: String,
    pub(crate) raw_open_spread: f64,
    pub(crate) raw_open_spread_text: String,
    pub(crate) maker_taker_net_edge: f64,
    pub(crate) maker_taker_net_edge_text: String,
    pub(crate) target_notional_usdt: String,
    pub(crate) executable_notional_usdt: String,
    pub(crate) fees: String,
    pub(crate) expected_funding_usdt: String,
    pub(crate) book_age_ms: String,
    pub(crate) market_can_open: bool,
    pub(crate) can_open: bool,
    pub(crate) reject_reasons: String,
}

impl CrossArbOpportunityRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| {
                let raw_open_spread = first_numeric(
                    row,
                    &[
                        "raw_open_spread_pct",
                        "raw_spread_pct",
                        "raw_open_spread",
                        "spread_pct",
                        "spread",
                    ],
                );
                let maker_taker_net_edge = first_numeric(
                    row,
                    &[
                        "maker_taker_net_edge",
                        "expected_net_profit_pct",
                        "net_profit_pct",
                    ],
                );
                let long_exchange = text_at(row, "long_exchange", lang);
                let short_exchange = text_at(row, "short_exchange", lang);
                let maker_exchange = text_or_fallback(text_at(row, "maker_exchange", lang), || {
                    long_exchange.clone()
                });
                let maker_side =
                    text_or_fallback(text_at(row, "maker_side", lang), || "open_long".to_string());
                let taker_exchange = text_or_fallback(text_at(row, "taker_exchange", lang), || {
                    short_exchange.clone()
                });
                let taker_side = text_or_fallback(text_at(row, "taker_side", lang), || {
                    "open_short".to_string()
                });
                Self {
                    canonical_symbol: text_at(row, "canonical_symbol", lang),
                    route: format!("{long_exchange} / {short_exchange}"),
                    maker: format!("{maker_exchange} {maker_side}"),
                    taker: format!("{taker_exchange} {taker_side}"),
                    long_entry_price: first_price_text(row, &["long_entry_price", "long_price"]),
                    short_entry_price: first_price_text(row, &["short_entry_price", "short_price"]),
                    raw_book_prices: format!(
                        "{} / {}",
                        first_price_text(row, &["long_best_ask_price", "long_entry_price"]),
                        first_price_text(row, &["short_best_bid_price", "short_entry_price"])
                    ),
                    raw_open_spread,
                    raw_open_spread_text: crate::utils::format_pct(raw_open_spread),
                    maker_taker_net_edge,
                    maker_taker_net_edge_text: crate::utils::format_pct(maker_taker_net_edge),
                    target_notional_usdt: first_money_text(
                        row,
                        &[
                            "target_notional_usdt",
                            "long_notional_usdt",
                            "short_notional_usdt",
                        ],
                    ),
                    executable_notional_usdt: first_money_text(
                        row,
                        &[
                            "executable_notional_usdt",
                            "executable_top_depth_usdt",
                            "long_notional_usdt",
                        ],
                    ),
                    fees: format!(
                        "{} / {}",
                        first_money_text(row, &["open_fee_est_usdt", "estimated_open_fee_usdt"]),
                        first_money_text(
                            row,
                            &["close_fee_est_usdt", "estimated_round_trip_fee_usdt"]
                        )
                    ),
                    expected_funding_usdt: crate::utils::money_text(row, "expected_funding_usdt"),
                    book_age_ms: text_or_fallback(text_at(row, "book_age_ms", lang), || {
                        text_at(row, "age_ms", lang)
                    }),
                    market_can_open: bool_at(row, "market_can_open"),
                    can_open: bool_at(row, "can_open"),
                    reject_reasons: text_or_fallback(text_at(row, "reject_reasons", lang), || {
                        text_at(row, "failure_reason", lang)
                    }),
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
    pub(crate) price_precision: String,
    pub(crate) quantity_precision: String,
    pub(crate) min_order: String,
    pub(crate) funding_rate: String,
    pub(crate) status: String,
}

impl CrossArbInstrumentRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| {
                Self {
                    exchange: text_at(row, "exchange", lang),
                    canonical_symbol: text_at(row, "canonical_symbol", lang),
                    exchange_symbol: row
                        .get("exchange_symbol")
                        .and_then(|value| value.get("symbol"))
                        .map(|value| crate::utils::value_text(value, lang))
                        .filter(|value| value != "-")
                        .unwrap_or_else(|| text_at(row, "exchange_symbol", lang)),
                    price_precision: text_or_fallback(text_at(row, "price_precision", lang), || {
                        text_at(row, "price_tick", lang)
                    }),
                    quantity_precision: text_or_fallback(
                        text_at(row, "quantity_precision", lang),
                        || text_at(row, "quantity_step", lang),
                    ),
                    min_order: first_nonempty_text(
                        row,
                        &[
                            "min_order",
                            "min_order_qty",
                            "min_base_quantity",
                            "min_qty",
                            "min_notional",
                        ],
                        lang,
                    ),
                    funding_rate: row
                        .get("funding_rate")
                        .and_then(Value::as_f64)
                        .map(crate::utils::format_pct)
                        .unwrap_or_else(|| text_at(row, "funding_rate", lang)),
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
    pub(crate) entry_prices: String,
    pub(crate) entry_net_edge_pct: f64,
    pub(crate) entry_net_edge_pct_text: String,
    pub(crate) close_prices: String,
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
                let entry_net_edge_pct =
                    first_numeric(row, &["entry_net_edge_pct", "planned_spread_pct"]);
                let close_profit_now = first_numeric(
                    row,
                    &[
                        "close_net_profit_pct",
                        "close_candidate_profit_pct",
                        "net_profit_pct",
                    ],
                );
                let closeable = bool_at(row, "closeable")
                    || bool_at(row, "close_ready")
                    || text_at(row, "close_reason", Language::En) != "-";
                Self {
                    bundle_id: text_at(row, "bundle_id", lang),
                    symbol: text_or_fallback(text_at(row, "symbol", lang), || {
                        text_at(row, "canonical_symbol", lang)
                    }),
                    long_exchange: text_at(row, "long_exchange", lang),
                    short_exchange: text_at(row, "short_exchange", lang),
                    status: text_or_fallback(text_at(row, "status", lang), || "open".to_string()),
                    entry_prices: format!(
                        "{} / {}",
                        first_price_text(row, &["long_entry_price"]),
                        first_price_text(row, &["short_entry_price"])
                    ),
                    entry_net_edge_pct,
                    entry_net_edge_pct_text: crate::utils::format_pct(entry_net_edge_pct),
                    close_prices: format!(
                        "{} / {}",
                        first_price_text(row, &["long_close_price"]),
                        first_price_text(row, &["short_close_price"])
                    ),
                    close_profit_now,
                    close_profit_now_text: crate::utils::format_pct(close_profit_now),
                    close_threshold_pct: crate::utils::format_pct(first_numeric(
                        row,
                        &["close_threshold_pct", "close_min_net_profit_pct"],
                    )),
                    closeable,
                    close_route: crate::utils::cross_arb_close_route(row, lang),
                    updated_at: text_or_fallback(text_at(row, "updated_at", lang), || {
                        text_at(row, "evaluated_at", lang)
                    }),
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
    pub(crate) symbol: String,
    pub(crate) route: String,
    pub(crate) status: String,
    pub(crate) open_expected_spread_pct: f64,
    pub(crate) open_expected_spread: String,
    pub(crate) open_actual_spread_pct: f64,
    pub(crate) open_actual_spread: String,
    pub(crate) close_expected_spread_pct: f64,
    pub(crate) close_expected_spread: String,
    pub(crate) close_actual_spread_pct: f64,
    pub(crate) close_actual_spread: String,
    pub(crate) realized_profit_usdt: f64,
    pub(crate) realized_pnl: String,
    pub(crate) opened_at: String,
    pub(crate) closed_at: String,
    pub(crate) four_order_elapsed_ms: String,
}

impl CrossArbResultRowData {
    pub(crate) fn from_value_rows(rows: &[Value], lang: Language) -> Vec<Self> {
        rows.iter()
            .map(|row| {
                let realized_profit_usdt = first_numeric(
                    row,
                    &[
                        "realized_profit_usdt",
                        "actual_pnl_usdt",
                        "pnl_usdt",
                        "profit_usdt",
                    ],
                );
                let open_expected_spread_pct = first_numeric(
                    row,
                    &[
                        "open_expected_spread_pct",
                        "planned_spread_pct",
                        "entry_expected_spread_pct",
                        "raw_open_spread_pct",
                        "spread_pct",
                    ],
                );
                let open_actual_spread_pct =
                    first_numeric(row, &["open_actual_spread_pct", "actual_open_spread_pct"]);
                let close_expected_spread_pct = first_numeric(
                    row,
                    &[
                        "close_expected_spread_pct",
                        "expected_close_spread_pct",
                        "close_candidate_profit_pct",
                    ],
                );
                let close_actual_spread_pct =
                    first_numeric(row, &["close_actual_spread_pct", "actual_close_spread_pct"]);
                Self {
                    symbol: text_or_fallback(text_at(row, "symbol", lang), || {
                        text_at(row, "canonical_symbol", lang)
                    }),
                    route: format!(
                        "{} / {}",
                        text_or_fallback(text_at(row, "long_exchange", lang), || {
                            text_at(row, "exchange", lang)
                        }),
                        text_or_fallback(text_at(row, "short_exchange", lang), || "-".to_string())
                    ),
                    status: text_or_fallback(text_at(row, "status", lang), || {
                        text_at(row, "lifecycle", lang)
                    }),
                    open_expected_spread_pct,
                    open_expected_spread: crate::utils::format_pct(open_expected_spread_pct),
                    open_actual_spread_pct,
                    open_actual_spread: crate::utils::format_pct(open_actual_spread_pct),
                    close_expected_spread_pct,
                    close_expected_spread: crate::utils::format_pct(close_expected_spread_pct),
                    close_actual_spread_pct,
                    close_actual_spread: crate::utils::format_pct(close_actual_spread_pct),
                    realized_profit_usdt,
                    realized_pnl: crate::utils::signed_usdt(realized_profit_usdt),
                    opened_at: first_beijing_time_text(
                        row,
                        &["opened_at", "open_time", "planned_at"],
                    ),
                    closed_at: first_beijing_time_text(
                        row,
                        &["closed_at", "close_time", "recorded_at", "updated_at"],
                    ),
                    four_order_elapsed_ms: four_order_elapsed_ms_text(row),
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

fn first_nonempty_text(value: &Value, keys: &[&str], lang: Language) -> String {
    keys.iter()
        .map(|key| text_at(value, key, lang))
        .find(|text| {
            let trimmed = text.trim();
            !trimmed.is_empty() && trimmed != "-"
        })
        .unwrap_or_else(|| "-".to_string())
}

fn first_numeric(value: &Value, keys: &[&str]) -> f64 {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_f64))
        .unwrap_or(0.0)
}

fn first_price_text(value: &Value, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_f64))
        .map(crate::utils::format_price)
        .unwrap_or_else(|| "-".to_string())
}

fn first_money_text(value: &Value, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_f64))
        .map(|value| format!("{value:.4}"))
        .unwrap_or_else(|| "-".to_string())
}

fn first_beijing_time_text(value: &Value, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| {
            value
                .get(*key)
                .and_then(crate::utils::format_beijing_time_value)
        })
        .unwrap_or_else(|| "-".to_string())
}

fn four_order_elapsed_ms_text(row: &Value) -> String {
    for key in [
        "four_order_elapsed_ms",
        "four_orders_elapsed_ms",
        "orders_elapsed_ms",
    ] {
        if let Some(text) = row.get(key).and_then(Value::as_str) {
            let text = text.trim();
            if !text.is_empty() && text != "-" {
                return text.to_string();
            }
        }
    }
    first_numeric_option(row, &["order_elapsed_ms", "execution_elapsed_ms"])
        .map(format_ms)
        .or_else(|| elapsed_ms_from_order_legs(row))
        .unwrap_or_else(|| "-".to_string())
}

fn first_numeric_option(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_f64))
        .filter(|value| value.is_finite())
}

fn elapsed_ms_from_order_legs(row: &Value) -> Option<String> {
    let mut times = Vec::new();
    collect_leg_times(row.get("open_legs"), &mut times);
    collect_leg_times(row.get("legs"), &mut times);
    if times.len() < 4 {
        collect_leg_times(row.get("normal_close_legs"), &mut times);
    }
    if times.len() < 4 {
        return None;
    }
    let min = times.iter().copied().fold(f64::INFINITY, f64::min);
    let max = times.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    (min.is_finite() && max.is_finite() && max >= min).then(|| format_ms(max - min))
}

fn collect_leg_times(value: Option<&Value>, times: &mut Vec<f64>) {
    let Some(Value::Array(legs)) = value else {
        return;
    };
    for leg in legs {
        if let Some(timestamp) = ["submitted_at", "acked_at", "filled_at"]
            .iter()
            .rev()
            .find_map(|key| leg.get(*key).and_then(timestamp_ms_from_value))
        {
            times.push(timestamp);
        }
    }
}

fn timestamp_ms_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(value) => value.as_f64().and_then(normalize_timestamp_ms),
        Value::String(value) => {
            let value = value.trim();
            if value.is_empty() || value == "-" || value.eq_ignore_ascii_case("null") {
                return None;
            }
            if let Ok(timestamp) = value.parse::<f64>() {
                if let Some(timestamp_ms) = normalize_timestamp_ms(timestamp) {
                    return Some(timestamp_ms);
                }
            }
            let timestamp_ms = js_sys::Date::parse(value);
            timestamp_ms.is_finite().then_some(timestamp_ms)
        }
        _ => None,
    }
}

fn normalize_timestamp_ms(value: f64) -> Option<f64> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    let abs = value.abs();
    if (1_000_000_000_000.0..100_000_000_000_000.0).contains(&abs) {
        Some(value)
    } else if (1_000_000_000.0..100_000_000_000.0).contains(&abs) {
        Some(value * 1000.0)
    } else {
        None
    }
}

fn format_ms(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.0}")
    } else {
        "-".to_string()
    }
}

fn rows_at(value: &Value, key: &str) -> Vec<Value> {
    match value.get(key).unwrap_or(&Value::Null) {
        Value::Array(rows) => rows.clone(),
        Value::Object(rows) => rows
            .iter()
            .filter_map(|(exchange, row)| {
                let mut object = row.as_object().cloned()?;
                object
                    .entry("exchange".to_string())
                    .or_insert_with(|| Value::String(exchange.clone()));
                Some(Value::Object(object))
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn rows_for_symbol(rows: &[Value], symbol_key: &str, lang: Language) -> Vec<Value> {
    rows.iter()
        .filter(|row| {
            crate::utils::normalize_symbol_text(&text_at(row, "symbol", lang)) == symbol_key
        })
        .cloned()
        .collect()
}

fn cross_arb_exchange_form_rows(
    settings: &Value,
    api_keys: &Value,
    seed_exchanges: &[String],
    lang: Language,
) -> Vec<CrossArbExchangeFormData> {
    let supported_rows = ExchangeCredentialSchema::from_supported_rows(
        api_keys.get("supported_exchanges").unwrap_or(&Value::Null),
        lang,
    );
    let supported_labels = supported_rows
        .iter()
        .map(|row| (exchange_key(&row.exchange), row.label.clone()))
        .collect::<BTreeMap<_, _>>();
    let account_rows = AccountManagerAccountRow::from_rows(
        api_keys
            .get("account_manager_accounts")
            .unwrap_or(&Value::Null),
        lang,
    );
    let settings_rows = rows_at(settings, "exchanges");
    let has_settings_rows = !settings_rows.is_empty();
    let seed_exchange_keys = seed_exchanges
        .iter()
        .map(|exchange| exchange_key(exchange))
        .filter(|exchange| !exchange.is_empty())
        .collect::<BTreeSet<_>>();
    let mut rows = BTreeMap::<String, CrossArbExchangeFormData>::new();
    let mut order = Vec::<String>::new();

    for row in settings_rows {
        let exchange = text_at(&row, "exchange", Language::En);
        let key = exchange_key(&exchange);
        if key.is_empty() {
            continue;
        }
        let label = exchange_label(&exchange, &supported_labels);
        insert_cross_arb_exchange_form_row(
            &mut rows,
            &mut order,
            key,
            CrossArbExchangeFormData {
                exchange,
                label,
                enabled: bool_at(&row, "enabled"),
                account_id: normalize_account_id(&text_at(&row, "account_id", Language::En)),
                env_prefix: optional_text_at(&row, "env_prefix"),
                private_rest_enabled: row
                    .get("private_rest_enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                private_ws_enabled: row
                    .get("private_ws_enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
            },
        );
    }

    for account in account_rows.iter().filter(|row| row.enabled) {
        let key = exchange_key(&account.exchange);
        if key.is_empty() {
            continue;
        }
        let account_id = normalize_account_id(&account.account_id);
        let label = exchange_label(&account.exchange, &supported_labels);
        let enabled = !has_settings_rows
            && (seed_exchange_keys.contains(&key) || default_cross_arb_exchange(&account.exchange));
        if let Some(existing) = rows.get_mut(&key) {
            if existing.account_id.is_empty() && !account_id.is_empty() {
                existing.account_id = account_id;
            }
            if existing.env_prefix.is_empty() && !account.env_prefix.trim().is_empty() {
                existing.env_prefix = account.env_prefix.clone();
            }
            if existing.label == existing.exchange {
                existing.label = label;
            }
            continue;
        }
        insert_cross_arb_exchange_form_row(
            &mut rows,
            &mut order,
            key,
            CrossArbExchangeFormData {
                exchange: account.exchange.clone(),
                label,
                enabled,
                account_id,
                env_prefix: account.env_prefix.clone(),
                private_rest_enabled: true,
                private_ws_enabled: true,
            },
        );
    }

    for exchange in seed_exchanges {
        let key = exchange_key(exchange);
        if key.is_empty() {
            continue;
        }
        let enabled = !has_settings_rows;
        let label = exchange_label(exchange, &supported_labels);
        insert_cross_arb_exchange_form_row(
            &mut rows,
            &mut order,
            key,
            CrossArbExchangeFormData {
                exchange: exchange.clone(),
                label,
                enabled,
                account_id: String::new(),
                env_prefix: String::new(),
                private_rest_enabled: true,
                private_ws_enabled: true,
            },
        );
    }

    for row in supported_rows {
        let key = exchange_key(&row.exchange);
        if key.is_empty() {
            continue;
        }
        insert_cross_arb_exchange_form_row(
            &mut rows,
            &mut order,
            key,
            CrossArbExchangeFormData {
                exchange: row.exchange,
                label: row.label,
                enabled: false,
                account_id: String::new(),
                env_prefix: String::new(),
                private_rest_enabled: true,
                private_ws_enabled: true,
            },
        );
    }

    for exchange in ["binance", "okx", "bybit", "bitget", "gate"] {
        let key = exchange_key(exchange);
        if rows.contains_key(&key) {
            continue;
        }
        insert_cross_arb_exchange_form_row(
            &mut rows,
            &mut order,
            key,
            CrossArbExchangeFormData {
                exchange: exchange.to_string(),
                label: exchange_label(exchange, &supported_labels),
                enabled: !has_settings_rows && matches!(exchange, "binance" | "bitget" | "gate"),
                account_id: String::new(),
                env_prefix: String::new(),
                private_rest_enabled: true,
                private_ws_enabled: true,
            },
        );
    }

    order
        .into_iter()
        .filter_map(|key| rows.remove(&key))
        .collect()
}

fn insert_cross_arb_exchange_form_row(
    rows: &mut BTreeMap<String, CrossArbExchangeFormData>,
    order: &mut Vec<String>,
    key: String,
    row: CrossArbExchangeFormData,
) {
    if rows.contains_key(&key) {
        return;
    }
    order.push(key.clone());
    rows.insert(key, row);
}

fn exchange_key(exchange: &str) -> String {
    let exchange = exchange.trim();
    if exchange.is_empty() || exchange == "-" {
        String::new()
    } else {
        canonical_exchange_name(exchange)
    }
}

fn exchange_label(exchange: &str, supported_labels: &BTreeMap<String, String>) -> String {
    supported_labels
        .get(&exchange_key(exchange))
        .cloned()
        .filter(|label| !label.trim().is_empty() && label != "-")
        .unwrap_or_else(|| match exchange.trim().to_ascii_lowercase().as_str() {
            "gate" | "gateio" | "gate.io" => "Gate.io".to_string(),
            "okx" | "okx_spot" => "OKX".to_string(),
            "bybit" => "Bybit".to_string(),
            "bitget" => "Bitget".to_string(),
            "binance" | "binance_spot" => "Binance".to_string(),
            _ => exchange.to_string(),
        })
}

fn default_cross_arb_exchange(exchange: &str) -> bool {
    matches!(
        exchange_key(exchange).as_str(),
        "binance" | "bitget" | "gateio"
    )
}

fn optional_text_at(value: &Value, key: &str) -> String {
    let text = text_at(value, key, Language::En);
    if text == "-" {
        String::new()
    } else {
        text
    }
}

fn percent_value_text(value: Option<f64>) -> String {
    value
        .filter(|value| value.is_finite())
        .map(|value| format!("{value:.1}%"))
        .unwrap_or_else(|| "-".to_string())
}

fn bytes_text(value: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let value = value as f64;
    if value >= GIB {
        format!("{:.2}G", value / GIB)
    } else if value >= MIB {
        format!("{:.0}M", value / MIB)
    } else if value >= KIB {
        format!("{:.0}K", value / KIB)
    } else {
        format!("{value:.0}B")
    }
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
    number_setting_text_any(settings, &[key], default)
}

fn number_setting_text_any(settings: &Value, keys: &[&str], default: f64) -> String {
    let value = keys
        .iter()
        .find_map(|key| settings.get(*key).and_then(Value::as_f64))
        .unwrap_or(default);
    if value.is_finite() {
        value.to_string()
    } else {
        default.to_string()
    }
}

fn pct_setting_text_any(settings: &Value, keys: &[&str], default: f64) -> String {
    let value = keys
        .iter()
        .find_map(|key| settings.get(*key).and_then(Value::as_f64))
        .unwrap_or(default);
    if value.is_finite() {
        trim_number(value * 100.0)
    } else {
        trim_number(default * 100.0)
    }
}

fn trim_number(value: f64) -> String {
    let mut text = format!("{value:.8}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    if text.is_empty() {
        "0".to_string()
    } else {
        text
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
    let execution = settings.get("execution").unwrap_or(&Value::Null);
    if bool_at(execution, "live_orders_enabled") {
        let place_path =
            text_or_fallback(text_at(execution, "order_place_path", Language::En), || {
                "rest".to_string()
            });
        let market_data = text_or_fallback(
            text_at(execution, "market_data_source", Language::En),
            || "websocket".to_string(),
        );
        return format!("live orders / {place_path} / {market_data}");
    }
    if bool_at(execution, "dry_run") {
        return "dry_run".to_string();
    }
    let mode = text_at(settings, "mode", Language::En).to_ascii_lowercase();
    if mode == "livesmall" && bool_at(settings, "execution_dry_run") {
        "live_small_dry_run".to_string()
    } else {
        text_or_fallback(text_at(settings, "execution_profile", Language::En), || {
            "simulation".to_string()
        })
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

use gloo_net::http::Request;
use serde_json::json;
use serde_json::Value;

use crate::types::{DashboardData, WorkspaceFetchData};
use crate::utils::{as_array, bool_at, path_segment, record_balance_history, text_at};

const API_CONTROL_PREFIX: &str = "/api/control";
const API_INVENTORY: &str = "/api/inventory";
const API_BALANCE_HISTORY: &str = "/api/balance-history";
const API_WORKSPACE: &str = "/api/workspace";
const API_STRATEGIES: &str = "/api/strategies";
const API_PROCESSES: &str = "/api/processes";
const API_AGENTS: &str = "/api/agents";
const API_GATEWAY_STATUS: &str = "/api/gateway/status";
const API_CREDENTIALS_STATUS: &str = "/api/credentials/status";
const API_STATUS: &str = "/api/status";
const API_CONFIG: &str = "/api/config";
const API_EXCHANGES: &str = "/api/exchanges";
const API_BOOKS: &str = "/api/books";
const API_SYMBOLS: &str = "/api/symbols";
const API_OPPORTUNITIES: &str = "/api/opportunities";
const API_DRY_RUN_PLANS: &str = "/api/dry_run_plans";
const API_RISK: &str = "/api/risk";
const API_LOGS: &str = "/api/logs";
const API_HEALTH: &str = "/api/health";
const API_RUNTIME_PUBLISHER_STATUS: &str = "/api/control/runtime-publisher/status";
const API_RUNTIME_PUBLISHER_EXCHANGES: &str = "/api/control/runtime-publisher/exchanges";
const API_RUNTIME_PUBLISHER_COMPONENTS: &str = "/api/control/runtime-publisher/components";
const API_RUNTIME_PUBLISHER_ERRORS: &str = "/api/control/runtime-publisher/errors";
const API_FEES: &str = "/api/fees";
const API_DISABLED: &str = "/api/disabled";
const API_SCANNER_RECOMMENDATIONS: &str = "/api/scanner/recommendations";
const API_HEDGE_POLICY_STATUS: &str = "/api/hedge-policy/status";
const API_CONTROL_SYMBOLS: &str = "/api/control/symbols";
const API_CONTROL_AUDIT: &str = "/api/control/audit";
const API_SPOT_ARB_DASHBOARD: &str = "/api/spot-arb/dashboard";
const API_CROSS_ARB_DASHBOARD: &str = "/api/cross-arb/dashboard";
const API_EXCHANGE_API_KEYS: &str = "/api/exchange-api-keys";
const API_STRATEGY_LOGS: &str = "/api/strategy-logs";
const API_STRATEGY_CONFIG: &str = "/api/strategy-config";
const API_CROSS_ARB_SETTINGS: &str = "/api/cross-arb/settings";
const API_CROSS_ARB_INSTRUMENTS: &str = "/api/cross-arb/instruments";
const API_CROSS_ARB_MARKET_SNAPSHOTS: &str = "/api/cross-arb/market-snapshots";

pub(crate) struct DashboardFetch {
    pub(crate) data: DashboardData,
    pub(crate) updated: usize,
    pub(crate) errors: Vec<String>,
}

pub(crate) struct StrategyLiveFetch {
    pub(crate) spot_arb: Value,
    pub(crate) cross_arb: Value,
    pub(crate) strategy_logs: Value,
    pub(crate) updated: usize,
    pub(crate) errors: Vec<String>,
}

pub(crate) struct SpotSymbolControlRefresh {
    pub(crate) control_symbols: Value,
    pub(crate) disabled: Value,
    pub(crate) errors: Vec<String>,
}

pub(crate) struct SpotExchangeControlRefresh {
    pub(crate) exchanges: Value,
    pub(crate) disabled: Value,
    pub(crate) errors: Vec<String>,
}

pub(crate) struct CrossArbInstrumentFetch {
    pub(crate) rows: Vec<Value>,
    pub(crate) feasibility: Value,
    pub(crate) coverage_ok: bool,
}

pub(crate) struct CrossArbMarketSnapshotFetch {
    pub(crate) rows: Vec<Value>,
    pub(crate) coverage_ok: bool,
}

pub(crate) struct StrategyConfigDraft {
    pub(crate) path: String,
    pub(crate) content: String,
}

fn control_command_endpoint(command: &str) -> String {
    format!("{API_CONTROL_PREFIX}/{}", path_segment(command))
}

fn exchange_control_endpoint(exchange: &str, command: &str) -> String {
    format!(
        "{API_CONTROL_PREFIX}/exchanges/{}/{}",
        path_segment(exchange),
        path_segment(command)
    )
}

fn symbol_control_endpoint(symbol: &str, command: &str) -> String {
    format!(
        "{API_CONTROL_PREFIX}/symbols/{}/{}",
        path_segment(symbol),
        path_segment(command)
    )
}

fn strategy_command_endpoint(strategy_id: &str) -> String {
    format!("{API_STRATEGIES}/{}/command", path_segment(strategy_id))
}

fn strategy_config_endpoint(strategy_id: &str) -> String {
    format!("{API_STRATEGIES}/{}/config", path_segment(strategy_id))
}

pub(crate) async fn create_strategy(token: &str, body: &Value) -> Result<Value, String> {
    api_post_json(API_STRATEGIES, token, body).await
}

pub(crate) async fn post_control_command(token: &str, command: &str) -> Result<Value, String> {
    api_post(&control_command_endpoint(command), token).await
}

pub(crate) async fn send_strategy_command(
    token: &str,
    strategy_id: &str,
    body: &Value,
) -> Result<Value, String> {
    api_post_json(&strategy_command_endpoint(strategy_id), token, body).await
}

pub(crate) async fn fetch_strategy_config_for_strategy(
    token: &str,
    strategy_id: &str,
    lang: crate::types::Language,
) -> Result<StrategyConfigDraft, String> {
    let value = api_get(&strategy_config_endpoint(strategy_id), token).await?;
    Ok(StrategyConfigDraft {
        path: text_at(&value, "path", lang),
        content: text_at(&value, "content", lang),
    })
}

pub(crate) async fn save_strategy_config_for_strategy(
    token: &str,
    strategy_id: &str,
    content: String,
    restart: bool,
) -> Result<Value, String> {
    let body = json!({ "content": content, "restart": restart });
    api_post_json(&strategy_config_endpoint(strategy_id), token, &body).await
}

pub(crate) async fn post_exchange_control(
    token: &str,
    exchange: &str,
    command: &str,
) -> Result<Value, String> {
    api_post(&exchange_control_endpoint(exchange, command), token).await
}

pub(crate) async fn post_symbol_control(
    token: &str,
    symbol: &str,
    command: &str,
) -> Result<Value, String> {
    api_post(&symbol_control_endpoint(symbol, command), token).await
}

pub(crate) async fn fetch_strategy_live_data(
    token: &str,
    previous: DashboardData,
) -> StrategyLiveFetch {
    let mut updated = 0usize;
    let mut errors = Vec::new();
    StrategyLiveFetch {
        spot_arb: fetch_or_previous(
            API_SPOT_ARB_DASHBOARD,
            token,
            previous.spot_arb,
            &mut updated,
            &mut errors,
        )
        .await,
        cross_arb: fetch_or_previous(
            API_CROSS_ARB_DASHBOARD,
            token,
            previous.cross_arb,
            &mut updated,
            &mut errors,
        )
        .await,
        strategy_logs: fetch_or_previous(
            API_STRATEGY_LOGS,
            token,
            previous.strategy_logs,
            &mut updated,
            &mut errors,
        )
        .await,
        updated,
        errors,
    }
}

pub(crate) async fn fetch_workspace_data(
    token: &str,
    previous: WorkspaceFetchData,
    updated: &mut usize,
    errors: &mut Vec<String>,
) -> WorkspaceFetchData {
    WorkspaceFetchData {
        workspace: fetch_or_previous(API_WORKSPACE, token, previous.workspace, updated, errors)
            .await,
        strategies: fetch_or_previous(API_STRATEGIES, token, previous.strategies, updated, errors)
            .await,
        processes: fetch_or_previous(API_PROCESSES, token, previous.processes, updated, errors)
            .await,
        agents: fetch_or_previous(API_AGENTS, token, previous.agents, updated, errors).await,
        gateway_status: fetch_or_previous(
            API_GATEWAY_STATUS,
            token,
            previous.gateway_status,
            updated,
            errors,
        )
        .await,
        credentials_status: fetch_or_previous(
            API_CREDENTIALS_STATUS,
            token,
            previous.credentials_status,
            updated,
            errors,
        )
        .await,
    }
}

pub(crate) async fn fetch_dashboard(token: &str, previous: DashboardData) -> DashboardFetch {
    let mut updated = 0usize;
    let mut errors = Vec::new();
    let workspace_data = fetch_workspace_data(
        token,
        WorkspaceFetchData {
            workspace: previous.workspace.clone(),
            strategies: previous.strategies.clone(),
            processes: previous.processes.clone(),
            agents: previous.agents.clone(),
            gateway_status: previous.gateway_status.clone(),
            credentials_status: previous.credentials_status.clone(),
        },
        &mut updated,
        &mut errors,
    )
    .await;
    let inventory = fetch_or_previous(
        API_INVENTORY,
        token,
        previous.inventory.clone(),
        &mut updated,
        &mut errors,
    )
    .await;
    let local_balance_history = if inventory == previous.inventory {
        previous.balance_history.clone()
    } else {
        record_balance_history(&inventory)
    };
    let balance_history = match api_get(API_BALANCE_HISTORY, token).await {
        Ok(value)
            if value
                .as_array()
                .map(|items| !items.is_empty())
                .unwrap_or(false) =>
        {
            updated += 1;
            value
        }
        Ok(_) => local_balance_history,
        Err(error) => {
            errors.push(error);
            local_balance_history
        }
    };
    let credentials_status = workspace_data.credentials_status.clone();
    let api_keys = fetch_credential_schema_fallback(
        token,
        previous.api_keys,
        credentials_status.clone(),
        &mut updated,
        &mut errors,
    )
    .await;
    DashboardFetch {
        data: DashboardData {
            workspace: workspace_data.workspace,
            strategies: workspace_data.strategies,
            processes: workspace_data.processes,
            agents: workspace_data.agents,
            gateway_status: workspace_data.gateway_status,
            credentials_status,
            status: fetch_or_previous(
                API_STATUS,
                token,
                previous.status,
                &mut updated,
                &mut errors,
            )
            .await,
            config: fetch_or_previous(
                API_CONFIG,
                token,
                previous.config,
                &mut updated,
                &mut errors,
            )
            .await,
            exchanges: fetch_or_previous(
                API_EXCHANGES,
                token,
                previous.exchanges,
                &mut updated,
                &mut errors,
            )
            .await,
            books: fetch_or_previous(API_BOOKS, token, previous.books, &mut updated, &mut errors)
                .await,
            symbols: fetch_or_previous(
                API_SYMBOLS,
                token,
                previous.symbols,
                &mut updated,
                &mut errors,
            )
            .await,
            opportunities: fetch_or_previous(
                API_OPPORTUNITIES,
                token,
                previous.opportunities,
                &mut updated,
                &mut errors,
            )
            .await,
            dry_run_plans: fetch_or_previous(
                API_DRY_RUN_PLANS,
                token,
                previous.dry_run_plans,
                &mut updated,
                &mut errors,
            )
            .await,
            risk: fetch_or_previous(API_RISK, token, previous.risk, &mut updated, &mut errors)
                .await,
            logs: fetch_or_previous(API_LOGS, token, previous.logs, &mut updated, &mut errors)
                .await,
            health: fetch_or_previous(
                API_HEALTH,
                token,
                previous.health,
                &mut updated,
                &mut errors,
            )
            .await,
            runtime_publisher: fetch_runtime_publisher_status(
                API_RUNTIME_PUBLISHER_STATUS,
                token,
                previous.runtime_publisher,
                &mut updated,
                &mut errors,
            )
            .await,
            inventory,
            fees: fetch_or_previous(API_FEES, token, previous.fees, &mut updated, &mut errors)
                .await,
            disabled: fetch_or_previous(
                API_DISABLED,
                token,
                previous.disabled,
                &mut updated,
                &mut errors,
            )
            .await,
            scanner: fetch_or_previous(
                API_SCANNER_RECOMMENDATIONS,
                token,
                previous.scanner,
                &mut updated,
                &mut errors,
            )
            .await,
            hedge_policy: fetch_or_previous(
                API_HEDGE_POLICY_STATUS,
                token,
                previous.hedge_policy,
                &mut updated,
                &mut errors,
            )
            .await,
            control_symbols: fetch_or_previous(
                API_CONTROL_SYMBOLS,
                token,
                previous.control_symbols,
                &mut updated,
                &mut errors,
            )
            .await,
            control_audit: fetch_or_previous(
                API_CONTROL_AUDIT,
                token,
                previous.control_audit,
                &mut updated,
                &mut errors,
            )
            .await,
            spot_arb: fetch_or_previous(
                API_SPOT_ARB_DASHBOARD,
                token,
                previous.spot_arb,
                &mut updated,
                &mut errors,
            )
            .await,
            cross_arb: fetch_or_previous(
                API_CROSS_ARB_DASHBOARD,
                token,
                previous.cross_arb,
                &mut updated,
                &mut errors,
            )
            .await,
            api_keys,
            strategy_logs: fetch_or_previous(
                API_STRATEGY_LOGS,
                token,
                previous.strategy_logs,
                &mut updated,
                &mut errors,
            )
            .await,
            balance_history,
        },
        updated,
        errors,
    }
}

pub(crate) async fn fetch_credential_status(token: &str) -> Result<Value, String> {
    match api_get(API_CREDENTIALS_STATUS, token).await {
        Ok(value) => Ok(value),
        Err(status_error) => match api_get(API_EXCHANGE_API_KEYS, token).await {
            Ok(value) => Ok(value),
            Err(legacy_error) => Err(format!("{status_error}; legacy fallback: {legacy_error}")),
        },
    }
}

pub(crate) async fn fetch_strategy_config_draft(
    token: &str,
    lang: crate::types::Language,
) -> Result<StrategyConfigDraft, String> {
    let value = api_get(API_STRATEGY_CONFIG, token).await?;
    Ok(StrategyConfigDraft {
        path: text_at(&value, "path", lang),
        content: text_at(&value, "content", lang),
    })
}

pub(crate) async fn save_strategy_config_draft(
    token: &str,
    content: String,
    restart: bool,
) -> Result<Value, String> {
    let body = json!({ "content": content, "restart": restart });
    api_post_json(API_STRATEGY_CONFIG, token, &body).await
}

pub(crate) async fn refresh_spot_symbol_control(
    token: &str,
    previous_control_symbols: Value,
    previous_disabled: Value,
) -> SpotSymbolControlRefresh {
    let mut updated = 0usize;
    let mut errors = Vec::new();
    SpotSymbolControlRefresh {
        control_symbols: fetch_or_previous(
            API_CONTROL_SYMBOLS,
            token,
            previous_control_symbols,
            &mut updated,
            &mut errors,
        )
        .await,
        disabled: fetch_or_previous(
            API_DISABLED,
            token,
            previous_disabled,
            &mut updated,
            &mut errors,
        )
        .await,
        errors,
    }
}

pub(crate) async fn refresh_spot_exchange_control(
    token: &str,
    previous_exchanges: Value,
    previous_disabled: Value,
) -> SpotExchangeControlRefresh {
    let mut updated = 0usize;
    let mut errors = Vec::new();
    SpotExchangeControlRefresh {
        exchanges: fetch_or_previous(
            API_EXCHANGES,
            token,
            previous_exchanges,
            &mut updated,
            &mut errors,
        )
        .await,
        disabled: fetch_or_previous(
            API_DISABLED,
            token,
            previous_disabled,
            &mut updated,
            &mut errors,
        )
        .await,
        errors,
    }
}

pub(crate) async fn fetch_cross_arb_settings(token: &str) -> Result<Value, String> {
    api_get(API_CROSS_ARB_SETTINGS, token).await
}

pub(crate) async fn save_cross_arb_settings(token: &str, body: &Value) -> Result<Value, String> {
    api_post_json(API_CROSS_ARB_SETTINGS, token, body).await
}

pub(crate) async fn fetch_cross_arb_instrument_data(
    token: &str,
) -> Result<CrossArbInstrumentFetch, String> {
    let value = api_get(API_CROSS_ARB_INSTRUMENTS, token).await?;
    Ok(CrossArbInstrumentFetch {
        rows: as_array(value.get("instruments").unwrap_or(&Value::Null)),
        feasibility: value.get("feasibility").cloned().unwrap_or(Value::Null),
        coverage_ok: bool_at(&value, "coverage_ok"),
    })
}

pub(crate) async fn fetch_cross_arb_market_snapshot_data(
    token: &str,
) -> Result<CrossArbMarketSnapshotFetch, String> {
    let value = api_get(API_CROSS_ARB_MARKET_SNAPSHOTS, token).await?;
    Ok(CrossArbMarketSnapshotFetch {
        rows: as_array(value.get("snapshots").unwrap_or(&Value::Null)),
        coverage_ok: bool_at(&value, "coverage_ok"),
    })
}

async fn fetch_or_previous(
    path: &str,
    token: &str,
    previous: Value,
    updated: &mut usize,
    errors: &mut Vec<String>,
) -> Value {
    match api_get(path, token).await {
        Ok(value) => {
            *updated += 1;
            value
        }
        Err(error) => {
            errors.push(error);
            previous
        }
    }
}

async fn fetch_credential_schema_fallback(
    token: &str,
    previous: Value,
    status_value: Value,
    updated: &mut usize,
    errors: &mut Vec<String>,
) -> Value {
    match api_get(API_EXCHANGE_API_KEYS, token).await {
        Ok(value) => {
            *updated += 1;
            value
        }
        Err(error) => {
            if status_value.is_null() {
                errors.push(error);
                previous
            } else {
                status_value
            }
        }
    }
}

async fn fetch_runtime_publisher_status(
    path: &str,
    token: &str,
    previous: Value,
    updated: &mut usize,
    errors: &mut Vec<String>,
) -> Value {
    let status = fetch_or_previous(path, token, previous.clone(), updated, errors).await;
    let exchanges = fetch_or_previous(
        API_RUNTIME_PUBLISHER_EXCHANGES,
        token,
        status.get("exchanges").cloned().unwrap_or(Value::Null),
        updated,
        errors,
    )
    .await;
    let components = fetch_or_previous(
        API_RUNTIME_PUBLISHER_COMPONENTS,
        token,
        status.get("components").cloned().unwrap_or(Value::Null),
        updated,
        errors,
    )
    .await;
    let errors_value = fetch_or_previous(
        API_RUNTIME_PUBLISHER_ERRORS,
        token,
        status.get("errors").cloned().unwrap_or(Value::Null),
        updated,
        errors,
    )
    .await;
    let mut merged = status;
    if let Some(map) = merged.as_object_mut() {
        map.insert("exchanges".to_string(), exchanges);
        map.insert("components".to_string(), components);
        map.insert("errors".to_string(), errors_value);
    } else {
        merged = json!({
            "status": merged,
            "exchanges": exchanges,
            "components": components,
            "errors": errors_value,
        });
    }
    merged
}

async fn api_get(path: &str, token: &str) -> Result<Value, String> {
    let bearer = format!("Bearer {token}");
    let response = Request::get(path)
        .header("Authorization", &bearer)
        .send()
        .await
        .map_err(|error| format!("请求 {path} 失败：{error}"))?;
    if !response.ok() {
        return Err(api_error_message(path, response).await);
    }
    response
        .json::<Value>()
        .await
        .map_err(|error| format!("解析 {path} 响应失败：{error}"))
}

async fn api_post(path: &str, token: &str) -> Result<Value, String> {
    let bearer = format!("Bearer {token}");
    let response = Request::post(path)
        .header("Authorization", &bearer)
        .send()
        .await
        .map_err(|error| format!("提交 {path} 失败：{error}"))?;
    if !response.ok() {
        return Err(api_error_message(path, response).await);
    }
    response
        .json::<Value>()
        .await
        .map_err(|error| format!("解析 {path} 提交响应失败：{error}"))
}

async fn api_post_json(path: &str, token: &str, body: &Value) -> Result<Value, String> {
    let bearer = format!("Bearer {token}");
    let response = Request::post(path)
        .header("Authorization", &bearer)
        .json(body)
        .map_err(|error| format!("编码 {path} 提交内容失败：{error}"))?
        .send()
        .await
        .map_err(|error| format!("提交 {path} 失败：{error}"))?;
    if !response.ok() {
        return Err(api_error_message(path, response).await);
    }
    response
        .json::<Value>()
        .await
        .map_err(|error| format!("解析 {path} 提交响应失败：{error}"))
}

async fn api_error_message(path: &str, response: gloo_net::http::Response) -> String {
    let status = response.status();
    if status == 401 {
        return format!("{path} 返回 HTTP 401：认证 token 缺失、错误或已过期；请把 run/local_control_api_token.txt 里的当前 token 重新粘贴到页面左侧 Auth token 输入框");
    }
    match response.json::<Value>().await {
        Ok(value) => {
            if let Some(error) = value.get("error").and_then(Value::as_str) {
                format!("{path} 返回 HTTP {status}：{error}")
            } else {
                format!("{path} 返回 HTTP {status}：{value}")
            }
        }
        Err(error) => format!("{path} 返回 HTTP {status}，且响应内容无法解析：{error}"),
    }
}

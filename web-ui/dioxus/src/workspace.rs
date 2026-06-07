use dioxus::prelude::*;
use serde_json::json;

use crate::api::{
    create_strategy, fetch_strategy_config_for_strategy, save_strategy_config_for_strategy,
    send_strategy_command,
};
use crate::i18n::{s, t};
use crate::types::{
    AgentWorkspaceRowData, CredentialStatusRowData, DashboardData, GatewayWorkspaceData, Language,
    ProcessWorkspaceRowData, StrategyWorkspaceRowData, WorkspaceSummaryData,
};
use crate::ui::Metric;
use crate::utils::compact;

#[component]
pub(crate) fn StrategyWorkspacePanel(
    data: Signal<DashboardData>,
    token: String,
    message: Signal<String>,
    lang: Language,
) -> Element {
    let default_preset = StrategyCreatePreset::DEFAULT;
    let mut strategy_id = use_signal(|| default_preset.strategy_id.to_string());
    let mut strategy_kind = use_signal(|| default_preset.kind.to_string());
    let mut config_path = use_signal(|| default_preset.config_path.to_string());
    let mut log_path = use_signal(|| default_preset.log_path.to_string());
    let mut command = use_signal(|| default_preset.command.to_string());
    let mut args = use_signal(|| default_preset.args.to_string());
    let mut selected_strategy_id = use_signal(String::new);
    let mut selected_config_path = use_signal(String::new);
    let mut selected_config_content = use_signal(String::new);
    let strategies = StrategyWorkspaceRowData::from_rows(&data().strategies, lang);
    if selected_strategy_id().is_empty() {
        if let Some(row) = strategies.first() {
            selected_strategy_id.set(row.strategy_id.clone());
            selected_config_path.set(row.config_path.clone());
        }
    }
    let processes = ProcessWorkspaceRowData::from_rows(&data().processes, lang);
    let workspace = WorkspaceSummaryData::from_value(&data().workspace, lang);
    let agents = AgentWorkspaceRowData::from_rows(&data().agents, lang);
    let gateway = GatewayWorkspaceData::from_value(&data().gateway_status, lang);
    let credentials = CredentialStatusRowData::from_response(&data().credentials_status, lang);
    let create_strategy = {
        let token = token.clone();
        let mut message = message;
        move |_| {
            let token_value = token.clone();
            let body = json!({
                "strategy_id": strategy_id(),
                "strategy_kind": strategy_kind(),
                "tenant_id": "local",
                "config_path": config_path(),
                "log_path": log_path(),
                "command": command(),
                "args": args().split_whitespace().map(ToString::to_string).collect::<Vec<_>>(),
                "working_dir": ".",
            });
            wasm_bindgen_futures::spawn_local(async move {
                match create_strategy(&token_value, &body).await {
                    Ok(value) => message.set(format!(
                        "{}: {}",
                        t(lang, "workspace_strategy_created"),
                        compact(&value)
                    )),
                    Err(error) => message.set(error),
                }
            });
        }
    };
    let load_selected_config = {
        let token = token.clone();
        let mut message = message;
        move |_| {
            let token_value = token.clone();
            let strategy_id = selected_strategy_id();
            if strategy_id.trim().is_empty() {
                message.set(s(lang, "workspace_select_strategy").to_string());
                return;
            }
            wasm_bindgen_futures::spawn_local(async move {
                match fetch_strategy_config_for_strategy(&token_value, &strategy_id, lang).await {
                    Ok(draft) => {
                        selected_config_path.set(draft.path);
                        selected_config_content.set(draft.content);
                        message.set(t(lang, "config_loaded").to_string());
                    }
                    Err(error) => message.set(error),
                }
            });
        }
    };
    rsx! {
        section { id: "workspace", class: "strategy-workspace",
            div { class: "section-title",
                h2 { {s(lang, "nav_workspace")} }
                p { {s(lang, "workspace_subtitle")} }
            }
            div { class: "metrics-row",
                Metric { label: s(lang, "workspace_strategy_count"), value: workspace.strategy_count.clone() }
                Metric { label: s(lang, "workspace_process_count"), value: workspace.process_count.clone() }
                Metric { label: "Agent".to_string(), value: workspace.agent_count.clone() }
                Metric { label: s(lang, "workspace_risk_status"), value: workspace.risk_status.clone() }
            }
            div { class: "two",
                div { class: "panel",
                    div { class: "panel-title-row",
                        h3 { {s(lang, "workspace_strategies")} }
                        span { class: "muted", "{strategies.len()} items" }
                    }
                    div { class: "table-wrap",
                        table {
                            thead {
                                tr {
                                    th { "ID" }
                                    th { {s(lang, "workspace_strategy_type")} }
                                    th { {s(lang, "workspace_strategy_status")} }
                                    th { {s(lang, "workspace_strategy_config")} }
                                    th { "Run" }
                                    th { {s(lang, "workspace_actions")} }
                                }
                            }
                            tbody {
                                if strategies.is_empty() {
                                    tr {
                                        td { colspan: "6", {s(lang, "workspace_empty_strategies")} }
                                    }
                                }
                                for row in strategies.iter() {
                                    StrategyWorkspaceRow {
                                        row: row.clone(),
                                        token: token.clone(),
                                        message,
                                        lang,
                                        selected_strategy_id,
                                        selected_config_path,
                                        selected_config_content
                                    }
                                }
                            }
                        }
                    }
                    div { class: "panel-title-row subpanel-title",
                        h3 { {s(lang, "workspace_processes")} }
                        span { class: "muted", "{processes.len()} items" }
                    }
                    div { class: "table-wrap compact-table",
                        table {
                            thead {
                                tr {
                                    th { "ID" }
                                    th { {s(lang, "workspace_strategy_status")} }
                                    th { {s(lang, "workspace_process_id")} }
                                    th { {s(lang, "workspace_started_at")} }
                                    th { {s(lang, "workspace_last_heartbeat")} }
                                    th { {s(lang, "workspace_restart_count")} }
                                    th { {s(lang, "workspace_last_exit_code")} }
                                    th { {s(lang, "workspace_log_configured")} }
                                    th { {s(lang, "last_error")} }
                                }
                            }
                            tbody {
                                if processes.is_empty() {
                                    tr {
                                        td { colspan: "9", {s(lang, "workspace_empty_processes")} }
                                    }
                                }
                                for row in processes.iter() {
                                    tr {
                                        td { "{row.strategy_id}" }
                                        td { span { class: workspace_status_class(&row.status), "{row.status}" } }
                                        td { "{row.process_id}" }
                                        td { "{row.started_at}" }
                                        td { "{row.last_heartbeat_at}" }
                                        td { "{row.restart_count}" }
                                        td { "{row.last_exit_code}" }
                                        td { span { class: if row.log_configured { "status-pill good" } else { "status-pill warn" }, "{row.log_configured}" } }
                                        td { "{row.last_error}" }
                                    }
                                }
                            }
                        }
                    }
                }
                div { class: "panel",
                    h3 { {s(lang, "workspace_create_strategy")} }
                    div { class: "strategy-create-form",
                        label { class: "form-field",
                            span { {s(lang, "workspace_strategy_id")} }
                            input {
                                value: "{strategy_id()}",
                                oninput: move |event| strategy_id.set(event.value())
                            }
                        }
                        label { class: "form-field",
                            span { {s(lang, "workspace_strategy_kind")} }
                            select {
                                value: "{strategy_kind()}",
                                onchange: move |event| {
                                    let value = event.value();
                                    strategy_kind.set(value.clone());
                                    if let Some(preset) = StrategyCreatePreset::for_kind(&value) {
                                        config_path.set(preset.config_path.to_string());
                                        log_path.set(preset.log_path.to_string());
                                        command.set(preset.command.to_string());
                                        args.set(preset.args.to_string());
                                    }
                                },
                                for preset in StrategyCreatePreset::ALL {
                                    option { value: preset.kind, "{preset.kind}" }
                                }
                            }
                        }
                        label { class: "form-field",
                            span { {s(lang, "workspace_config_path")} }
                            input {
                                value: "{config_path()}",
                                oninput: move |event| config_path.set(event.value())
                            }
                        }
                        label { class: "form-field",
                            span { {s(lang, "workspace_log_path")} }
                            input {
                                value: "{log_path()}",
                                oninput: move |event| log_path.set(event.value())
                            }
                        }
                        label { class: "form-field",
                            span { {s(lang, "workspace_command")} }
                            input {
                                value: "{command()}",
                                oninput: move |event| command.set(event.value())
                            }
                        }
                        label { class: "form-field",
                            span { {s(lang, "workspace_args")} }
                            input {
                                value: "{args()}",
                                oninput: move |event| args.set(event.value())
                            }
                        }
                        button { class: "button primary", onclick: create_strategy, {s(lang, "workspace_create_strategy")} }
                    }
                    div { class: "config-editor-panel",
                        div { class: "panel-title-row subpanel-title",
                            h3 { {s(lang, "workspace_strategy_config")} }
                            span { class: "muted", "{selected_strategy_id()}" }
                        }
                        label { class: "form-field",
                            span { {s(lang, "workspace_selected_strategy")} }
                            select {
                                value: "{selected_strategy_id()}",
                                onchange: move |event| {
                                    let selected = event.value();
                                    selected_strategy_id.set(selected.clone());
                                    if let Some(row) = strategies.iter().find(|row| row.strategy_id == selected) {
                                        selected_config_path.set(row.config_path.clone());
                                    }
                                    selected_config_content.set(String::new());
                                },
                                for row in strategies.iter() {
                                    option { value: "{row.strategy_id}", "{row.strategy_id}" }
                                }
                            }
                        }
                        div { class: "config-grid",
                            div { span { {s(lang, "workspace_config_path")} } strong { "{selected_config_path()}" } }
                        }
                        textarea {
                            class: "config-editor",
                            value: "{selected_config_content()}",
                            placeholder: s(lang, "workspace_load_strategy_config_hint"),
                            oninput: move |event| selected_config_content.set(event.value())
                        }
                        div { class: "actions",
                            button { class: "button", onclick: load_selected_config, {s(lang, "load_config")} }
                            button {
                                class: "button",
                                onclick: save_strategy_config_click(token.clone(), selected_strategy_id, selected_config_content, message, lang, false),
                                {s(lang, "save_config")}
                            }
                            button {
                                class: "button primary",
                                onclick: save_strategy_config_click(token.clone(), selected_strategy_id, selected_config_content, message, lang, true),
                                {s(lang, "save_and_restart")}
                            }
                        }
                    }
                    div { class: "workspace-side-summary",
                        h3 { {s(lang, "workspace_agents_gateway")} }
                        div { class: "metrics-row compact-metrics",
                            Metric { label: s(lang, "workspace_agent_count"), value: agents.len().to_string() }
                            Metric { label: s(lang, "workspace_gateway_status"), value: gateway.status.clone() }
                            Metric { label: s(lang, "workspace_gateway_exchanges"), value: gateway.exchange_count.to_string() }
                        }
                        div { class: "mini-grid",
                            div { span { {s(lang, "workspace_agent")} } strong { "{gateway.agent_id}" } }
                            div { span { {s(lang, "workspace_endpoint")} } strong { "{gateway.endpoint}" } }
                            div { span { {s(lang, "workspace_last_heartbeat")} } strong { "{gateway.last_heartbeat_at}" } }
                        }
                        div { class: "table-wrap compact-table",
                            table {
                                thead {
                                    tr {
                                        th { {s(lang, "workspace_agent")} }
                                        th { {s(lang, "workspace_tenant")} }
                                        th { {s(lang, "workspace_strategy_status")} }
                                        th { {s(lang, "workspace_last_heartbeat")} }
                                    }
                                }
                                tbody {
                                    if agents.is_empty() {
                                        tr { td { colspan: "4", {s(lang, "workspace_empty_agents")} } }
                                    }
                                    for row in agents.iter() {
                                        tr {
                                            td { "{row.agent_id}" }
                                            td { "{row.tenant_id}" }
                                            td { span { class: workspace_status_class(&row.status), "{row.status}" } }
                                            td { "{row.last_heartbeat_at}" }
                                        }
                                    }
                                }
                            }
                        }
                        div { class: "table-wrap compact-table",
                            table {
                                thead {
                                    tr {
                                        th { {s(lang, "exchange")} }
                                        th { {s(lang, "market")} }
                                        th { {s(lang, "workspace_strategy_status")} }
                                        th { {s(lang, "last_error")} }
                                    }
                                }
                                tbody {
                                    if gateway.exchanges.is_empty() {
                                        tr { td { colspan: "4", {s(lang, "workspace_empty_gateway_exchanges")} } }
                                    }
                                    for row in gateway.exchanges.iter() {
                                        tr {
                                            td { "{row.exchange_id}" }
                                            td { "{row.market}" }
                                            td { span { class: workspace_status_class(&row.status), "{row.status}" } }
                                            td { "{row.last_error}" }
                                        }
                                    }
                                }
                            }
                        }
                        h3 { {s(lang, "workspace_credentials_status")} }
                        div { class: "table-wrap compact-table",
                            table {
                                thead {
                                    tr {
                                        th { {s(lang, "workspace_credential_slot")} }
                                        th { {s(lang, "exchange")} }
                                        th { {s(lang, "workspace_tenant")} }
                                        th { {s(lang, "configured")} }
                                        th { {s(lang, "health")} }
                                        th { {s(lang, "workspace_last_verified")} }
                                    }
                                }
                                tbody {
                                    if credentials.is_empty() {
                                        tr { td { colspan: "6", {s(lang, "workspace_empty_credentials")} } }
                                    }
                                    for row in credentials.iter() {
                                        tr {
                                            td { "{row.slot_id}" }
                                            td { "{row.exchange_id}" }
                                            td { "{row.tenant_id}" }
                                            td { span { class: if row.configured { "status-pill good" } else { "status-pill bad" }, "{row.configured}" } }
                                            td { span { class: workspace_status_class(&row.health), "{row.health}" } }
                                            td { "{row.last_verified_at}" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct StrategyCreatePreset {
    kind: &'static str,
    strategy_id: &'static str,
    config_path: &'static str,
    log_path: &'static str,
    command: &'static str,
    args: &'static str,
}

impl StrategyCreatePreset {
    const DEFAULT: Self = Self::SPOT_ARB;

    const SPOT_ARB: Self = Self {
        kind: "spot_spot_taker_arbitrage",
        strategy_id: "spot-arb-local",
        config_path: "config/spot_spot_taker_arbitrage.yml",
        log_path: "logs/control_panel/spot-arb-local.log",
        command: "cargo",
        args: "run --bin rustcta -- --strategy spot_spot_taker_arbitrage --config config/spot_spot_taker_arbitrage.yml",
    };

    const CROSS_ARB: Self = Self {
        kind: "cross_exchange_arbitrage",
        strategy_id: "contract-arb-local",
        config_path: "config/cross_exchange_arbitrage_usdt.yml",
        log_path: "logs/control_panel/contract-arb-local.log",
        command: "cargo",
        args: "run --bin cross_arb_live -- --config config/cross_exchange_arbitrage_usdt.yml --skip-private-audit --run",
    };

    const FUNDING_ARB: Self = Self {
        kind: "funding_arbitrage",
        strategy_id: "funding-arb-local",
        config_path: "config/funding_rate_arbitrage_live_usdt.yml",
        log_path: "logs/control_panel/funding-arb-local.log",
        command: "cargo",
        args: "run --bin funding_arb_live -- --config config/funding_rate_arbitrage_live_usdt.yml --confirm-live-order",
    };

    const CUSTOM: Self = Self {
        kind: "custom",
        strategy_id: "custom-local",
        config_path: "",
        log_path: "logs/custom-local.log",
        command: "cargo",
        args: "",
    };

    const ALL: [Self; 4] = [
        Self::SPOT_ARB,
        Self::CROSS_ARB,
        Self::FUNDING_ARB,
        Self::CUSTOM,
    ];

    fn for_kind(kind: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|preset| preset.kind == kind)
    }
}

#[component]
fn StrategyWorkspaceRow(
    row: StrategyWorkspaceRowData,
    token: String,
    message: Signal<String>,
    lang: Language,
    mut selected_strategy_id: Signal<String>,
    mut selected_config_path: Signal<String>,
    mut selected_config_content: Signal<String>,
) -> Element {
    let strategy_id = row.strategy_id.clone();
    let status = row.status.clone();
    let row_config_path = row.config_path.clone();
    let row_strategy_id = strategy_id.clone();
    let start = strategy_lifecycle_click(
        strategy_id.clone(),
        StrategyLifecycleCommand::Start,
        token.clone(),
        message,
        lang,
    );
    let stop = strategy_lifecycle_click(
        strategy_id.clone(),
        StrategyLifecycleCommand::Stop,
        token.clone(),
        message,
        lang,
    );
    let restart = strategy_lifecycle_click(
        strategy_id.clone(),
        StrategyLifecycleCommand::Restart,
        token.clone(),
        message,
        lang,
    );
    rsx! {
        tr {
            class: if selected_strategy_id() == row_strategy_id { "clickable-row active" } else { "clickable-row" },
            onclick: move |_| {
                selected_strategy_id.set(row_strategy_id.clone());
                selected_config_path.set(row_config_path.clone());
                selected_config_content.set(String::new());
            },
            td { "{strategy_id}" }
            td { "{row.strategy_kind}" }
            td { span { class: strategy_status_class(&status), "{status}" } }
            td { "{row.config_path}" }
            td { "{row.run_id}" }
            td {
                div { class: "inline-actions",
                    button { class: "mini-button primary", onclick: start, {s(lang, "workspace_start")} }
                    button { class: "mini-button", onclick: stop, {s(lang, "workspace_stop")} }
                    button { class: "mini-button", onclick: restart, {s(lang, "workspace_restart")} }
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum StrategyLifecycleCommand {
    Start,
    Stop,
    Restart,
}

impl StrategyLifecycleCommand {
    fn api_value(self) -> &'static str {
        match self {
            Self::Start => "Start",
            Self::Stop => "Stop",
            Self::Restart => "Restart",
        }
    }

    fn id_part(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Restart => "restart",
        }
    }
}

fn strategy_lifecycle_click(
    strategy_id: String,
    command: StrategyLifecycleCommand,
    token: String,
    mut message: Signal<String>,
    lang: Language,
) -> impl FnMut(Event<MouseData>) + 'static {
    move |_| {
        let token_value = token.clone();
        let strategy_id_value = strategy_id.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let now = js_sys::Date::new_0()
                .to_iso_string()
                .as_string()
                .unwrap_or_default();
            let command_id = format!("web-{}-{}", command.id_part(), js_sys::Date::now() as u64);
            let body = json!({
                "schema_version": 1,
                "command_id": command_id,
                "strategy_id": strategy_id_value,
                "run_id": format!("run-{}", js_sys::Date::now() as u64),
                "command": command.api_value(),
                "requested_by": "web",
                "idempotency_key": command_id,
                "requested_at": now,
            });
            match send_strategy_command(&token_value, &strategy_id_value, &body).await {
                Ok(value) => message.set(format!(
                    "{}: {}",
                    t(lang, "workspace_strategy_command_accepted"),
                    compact(&value)
                )),
                Err(error) => message.set(error),
            }
        });
    }
}

fn save_strategy_config_click(
    token: String,
    selected_strategy_id: Signal<String>,
    selected_config_content: Signal<String>,
    mut message: Signal<String>,
    lang: Language,
    restart: bool,
) -> impl FnMut(Event<MouseData>) + 'static {
    move |_| {
        let token_value = token.clone();
        let strategy_id = selected_strategy_id();
        let content = selected_config_content();
        if strategy_id.trim().is_empty() {
            message.set(s(lang, "workspace_select_strategy").to_string());
            return;
        }
        wasm_bindgen_futures::spawn_local(async move {
            match save_strategy_config_for_strategy(&token_value, &strategy_id, content, restart)
                .await
            {
                Ok(value) => {
                    message.set(format!("{}: {}", t(lang, "config_saved"), compact(&value)))
                }
                Err(error) => message.set(error),
            }
        });
    }
}

fn strategy_status_class(status: &str) -> &'static str {
    match status {
        "Running" | "running" => "status-pill good",
        "Starting" | "starting" => "status-pill warn",
        "Failed" | "failed" => "status-pill bad",
        _ => "status-pill",
    }
}

fn workspace_status_class(status: &str) -> &'static str {
    match status {
        "online" | "connected" | "healthy" | "running" | "true" => "status-pill good",
        "degraded" | "starting" | "unknown" => "status-pill warn",
        "offline" | "disconnected" | "missing" | "error" | "failed" | "false" => "status-pill bad",
        _ => "status-pill",
    }
}

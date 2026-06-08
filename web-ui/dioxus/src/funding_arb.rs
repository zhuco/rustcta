use dioxus::prelude::*;
use serde_json::{json, Value};

use crate::api::{fetch_funding_arb_settings, save_funding_arb_settings, send_strategy_command};
use crate::cross_arb::{env_prefix_for_account, CrossArbAccountSelect};
use crate::i18n::{s, t};
use crate::storage::save_active_view;
use crate::types::{
    ControlPanelView, CredentialAccountOption, FundingArbSettingsFormData, Language,
};
use crate::ui::Metric;
use crate::utils::{compact, text_at};

const STRATEGY_ID: &str = "funding-arb-local";

#[component]
pub(crate) fn FundingArbPanel(
    api_keys: Value,
    processes: Value,
    token: String,
    mut message: Signal<String>,
    lang: Language,
    mut active_view: Signal<ControlPanelView>,
    mut api_key_exchange: Signal<String>,
    mut api_key_account: Signal<String>,
    mut api_key_namespace: Signal<String>,
) -> Element {
    let mut settings = use_signal(|| Value::Null);
    let initial = FundingArbSettingsFormData::from_settings(&settings(), lang);
    let config_path = use_signal(|| initial.path.clone());
    let mut mode = use_signal(|| initial.mode.clone());
    let per_exchange_limit = use_signal(|| initial.per_exchange_limit.clone());
    let min_funding_rate = use_signal(|| initial.min_funding_rate.clone());
    let mut require_next_funding_time = use_signal(|| initial.require_next_funding_time);
    let max_funding_snapshot_age_ms = use_signal(|| initial.max_funding_snapshot_age_ms.clone());
    let min_seconds_to_settlement = use_signal(|| {
        if initial.min_seconds_to_settlement_at_scan.is_empty() {
            "2".to_string()
        } else {
            initial.min_seconds_to_settlement_at_scan.clone()
        }
    });
    let max_seconds_to_settlement = use_signal(|| {
        if initial.max_seconds_to_settlement_at_scan.is_empty() {
            "600".to_string()
        } else {
            initial.max_seconds_to_settlement_at_scan.clone()
        }
    });
    let scan_minute = use_signal(|| initial.scan_minute.clone());
    let notional_usdt = use_signal(|| initial.notional_usdt.clone());
    let open_seconds = use_signal(|| initial.open_seconds_before_settlement.clone());
    let close_seconds = use_signal(|| initial.close_seconds_after_settlement.clone());
    let readback_delay = use_signal(|| initial.order_readback_delay_secs.clone());
    let close_timeout = use_signal(|| initial.close_limit_timeout_secs.clone());
    let close_retries = use_signal(|| initial.close_limit_max_retries.clone());
    let max_slippage = use_signal(|| initial.max_slippage_pct.clone());
    let mut allow_existing_position = use_signal(|| initial.allow_existing_symbol_position);
    let mut symbol_allowlist = use_signal(|| initial.symbol_allowlist_text.clone());
    let mut symbol_blocklist = use_signal(|| initial.symbol_blocklist_text.clone());
    let binance_enabled = use_signal(|| funding_exchange_enabled(&initial, "binance"));
    let bitget_enabled = use_signal(|| funding_exchange_enabled(&initial, "bitget"));
    let gate_enabled = use_signal(|| funding_exchange_enabled(&initial, "gate"));
    let binance_account = use_signal(|| funding_exchange_text(&initial, "binance", "account"));
    let bitget_account = use_signal(|| funding_exchange_text(&initial, "bitget", "account"));
    let gate_account = use_signal(|| funding_exchange_text(&initial, "gate", "account"));
    let binance_env = use_signal(|| funding_exchange_text(&initial, "binance", "env"));
    let bitget_env = use_signal(|| funding_exchange_text(&initial, "bitget", "env"));
    let gate_env = use_signal(|| funding_exchange_text(&initial, "gate", "env"));
    let binance_options =
        CredentialAccountOption::for_exchange(&api_keys, "binance", &binance_account(), lang);
    let bitget_options =
        CredentialAccountOption::for_exchange(&api_keys, "bitget", &bitget_account(), lang);
    let gate_options =
        CredentialAccountOption::for_exchange(&api_keys, "gate", &gate_account(), lang);
    let binance_select_options = binance_options.clone();
    let bitget_select_options = bitget_options.clone();
    let gate_select_options = gate_options.clone();
    let process = funding_process(&processes);
    let process_status = text_at(&process, "status", lang);
    let process_id = text_at(&process, "process_id", lang);
    let restart_count = text_at(&process, "restart_count", lang);
    {
        let token = token.clone();
        use_future(move || {
            let token_value = token.clone();
            async move {
                if !settings().is_null() {
                    return;
                }
                match fetch_funding_arb_settings(&token_value).await {
                    Ok(value) => apply_loaded_settings(
                        &value,
                        lang,
                        config_path,
                        mode,
                        per_exchange_limit,
                        min_funding_rate,
                        require_next_funding_time,
                        max_funding_snapshot_age_ms,
                        min_seconds_to_settlement,
                        max_seconds_to_settlement,
                        scan_minute,
                        notional_usdt,
                        open_seconds,
                        close_seconds,
                        readback_delay,
                        close_timeout,
                        close_retries,
                        max_slippage,
                        allow_existing_position,
                        symbol_allowlist,
                        symbol_blocklist,
                        binance_enabled,
                        bitget_enabled,
                        gate_enabled,
                        binance_account,
                        bitget_account,
                        gate_account,
                        binance_env,
                        bitget_env,
                        gate_env,
                        settings,
                    ),
                    Err(error) => message.set(error),
                }
            }
        });
    }
    let load_settings = {
        let token = token.clone();
        move |_| {
            let token_value = token.clone();
            wasm_bindgen_futures::spawn_local(async move {
                match fetch_funding_arb_settings(&token_value).await {
                    Ok(value) => {
                        apply_loaded_settings(
                            &value,
                            lang,
                            config_path,
                            mode,
                            per_exchange_limit,
                            min_funding_rate,
                            require_next_funding_time,
                            max_funding_snapshot_age_ms,
                            min_seconds_to_settlement,
                            max_seconds_to_settlement,
                            scan_minute,
                            notional_usdt,
                            open_seconds,
                            close_seconds,
                            readback_delay,
                            close_timeout,
                            close_retries,
                            max_slippage,
                            allow_existing_position,
                            symbol_allowlist,
                            symbol_blocklist,
                            binance_enabled,
                            bitget_enabled,
                            gate_enabled,
                            binance_account,
                            bitget_account,
                            gate_account,
                            binance_env,
                            bitget_env,
                            gate_env,
                            settings,
                        );
                        message.set(t(lang, "config_loaded").to_string());
                    }
                    Err(error) => message.set(error),
                }
            });
        }
    };
    let save_settings = {
        let token = token.clone();
        move |_| {
            let token_value = token.clone();
            let body = json!({
                "mode": mode(),
                "per_exchange_limit": parse_usize(&per_exchange_limit(), 1),
                "min_funding_rate": parse_f64(&min_funding_rate(), -0.005),
                "require_next_funding_time": require_next_funding_time(),
                "max_funding_snapshot_age_ms": parse_i64(&max_funding_snapshot_age_ms(), 5000),
                "min_seconds_to_settlement_at_scan": parse_optional_i64(&min_seconds_to_settlement()),
                "max_seconds_to_settlement_at_scan": parse_optional_i64(&max_seconds_to_settlement()),
                "scan_minute": parse_u32(&scan_minute(), 55),
                "notional_usdt": parse_f64(&notional_usdt(), 10.0),
                "open_seconds_before_settlement": parse_i64(&open_seconds(), 1),
                "close_seconds_after_settlement": parse_i64(&close_seconds(), 1),
                "order_readback_delay_secs": parse_u64(&readback_delay(), 3),
                "close_limit_timeout_secs": parse_u64(&close_timeout(), 300),
                "close_limit_max_retries": parse_u32(&close_retries(), 3),
                "max_slippage_pct": parse_f64(&max_slippage(), 0.003),
                "allow_existing_symbol_position": allow_existing_position(),
                "position_side": "long",
                "symbol_allowlist": split_lines(&symbol_allowlist()),
                "symbol_blocklist": split_lines(&symbol_blocklist()),
                "exchanges": [
                    { "exchange": "binance", "enabled": binance_enabled(), "account_id": binance_account(), "env_prefix": binance_env(), "demo_trading": false, "private_ws_enabled": false, "position_mode": "hedge" },
                    { "exchange": "bitget", "enabled": bitget_enabled(), "account_id": bitget_account(), "env_prefix": bitget_env(), "demo_trading": false, "private_ws_enabled": false, "position_mode": "hedge" },
                    { "exchange": "gate", "enabled": gate_enabled(), "account_id": gate_account(), "env_prefix": gate_env(), "demo_trading": false, "private_ws_enabled": false, "position_mode": "hedge" }
                ]
            });
            wasm_bindgen_futures::spawn_local(async move {
                match save_funding_arb_settings(&token_value, &body).await {
                    Ok(value) => {
                        settings.set(value.get("settings").cloned().unwrap_or(Value::Null));
                        message.set(format!(
                            "{}: {}",
                            t(lang, "config_saved_restarting"),
                            compact(&value)
                        ));
                    }
                    Err(error) => message.set(error),
                }
            });
        }
    };
    rsx! {
        section { id: "funding-arb", class: "cross-arb",
            div { class: "section-title",
                div {
                    h2 { {s(lang, "funding_arb_title")} }
                    p { class: "muted", "{config_path()}" }
                }
                div { class: "row-actions",
                    button { class: "button", onclick: funding_lifecycle_click("Start", token.clone(), message, lang), {s(lang, "workspace_start")} }
                    button { class: "button", onclick: funding_lifecycle_click("Stop", token.clone(), message, lang), {s(lang, "workspace_stop")} }
                    button { class: "button primary", onclick: funding_lifecycle_click("Restart", token.clone(), message, lang), {s(lang, "workspace_restart")} }
                }
            }
            div { class: "grid cross-metrics",
                Metric { label: s(lang, "workspace_strategy_status"), value: process_status }
                Metric { label: s(lang, "workspace_process_id"), value: process_id }
                Metric { label: s(lang, "workspace_restart_count"), value: restart_count }
                Metric { label: s(lang, "mode"), value: mode() }
                Metric { label: s(lang, "per_arb_notional"), value: format!("{} USDT", notional_usdt()) }
                Metric { label: s(lang, "min_funding_rate"), value: min_funding_rate() }
            }
            div { class: "panel cross-settings",
                div { class: "panel-title-row",
                    div {
                        h2 { {s(lang, "strategy_parameters")} }
                        p { class: "muted", {s(lang, "funding_arb_subtitle")} }
                    }
                    div { class: "row-actions",
                        button { class: "button", onclick: load_settings, {s(lang, "load_config")} }
                        button { class: "button primary", onclick: save_settings, {s(lang, "save_and_restart")} }
                    }
                }
                div { class: "cross-settings-grid",
                    div { class: "settings-form",
                        FundingExchangeEditor {
                            exchange_label: "Binance".to_string(),
                            enabled: binance_enabled,
                            account: binance_account,
                            env_prefix: binance_env,
                            options: binance_options,
                            select_options: binance_select_options,
                            active_view,
                            api_key_exchange,
                            api_key_account,
                            api_key_namespace,
                            lang,
                            message
                        }
                        FundingExchangeEditor {
                            exchange_label: "Bitget".to_string(),
                            enabled: bitget_enabled,
                            account: bitget_account,
                            env_prefix: bitget_env,
                            options: bitget_options,
                            select_options: bitget_select_options,
                            active_view,
                            api_key_exchange,
                            api_key_account,
                            api_key_namespace,
                            lang,
                            message
                        }
                        FundingExchangeEditor {
                            exchange_label: "Gate.io".to_string(),
                            enabled: gate_enabled,
                            account: gate_account,
                            env_prefix: gate_env,
                            options: gate_options,
                            select_options: gate_select_options,
                            active_view,
                            api_key_exchange,
                            api_key_account,
                            api_key_namespace,
                            lang,
                            message
                        }
                    }
                    div { class: "settings-form compact-settings",
                        label { class: "form-field",
                            span { {s(lang, "mode")} }
                            select {
                                value: "{mode()}",
                                onchange: move |event| mode.set(event.value()),
                                option { value: "observe", "observe" }
                                option { value: "live", "live" }
                            }
                        }
                        NumberField { label: s(lang, "per_arb_notional"), value: notional_usdt }
                        NumberField { label: s(lang, "min_funding_rate"), value: min_funding_rate }
                        NumberField { label: s(lang, "per_exchange_limit"), value: per_exchange_limit }
                        NumberField { label: s(lang, "scan_minute"), value: scan_minute }
                        NumberField { label: s(lang, "max_slippage_pct"), value: max_slippage }
                        label { class: "check-row",
                            input { r#type: "checkbox", checked: "{require_next_funding_time()}", onchange: move |event| require_next_funding_time.set(event.checked()) }
                            span { {s(lang, "require_next_funding_time")} }
                        }
                        label { class: "check-row",
                            input { r#type: "checkbox", checked: "{allow_existing_position()}", onchange: move |event| allow_existing_position.set(event.checked()) }
                            span { {s(lang, "allow_existing_symbol_position")} }
                        }
                    }
                    div { class: "settings-form compact-settings",
                        NumberField { label: s(lang, "max_funding_snapshot_age_ms"), value: max_funding_snapshot_age_ms }
                        NumberField { label: s(lang, "min_seconds_to_settlement"), value: min_seconds_to_settlement }
                        NumberField { label: s(lang, "max_seconds_to_settlement"), value: max_seconds_to_settlement }
                        NumberField { label: s(lang, "open_seconds_before_settlement"), value: open_seconds }
                        NumberField { label: s(lang, "close_seconds_after_settlement"), value: close_seconds }
                        NumberField { label: s(lang, "order_readback_delay_secs"), value: readback_delay }
                        NumberField { label: s(lang, "close_limit_timeout_secs"), value: close_timeout }
                        NumberField { label: s(lang, "close_limit_max_retries"), value: close_retries }
                    }
                    label { class: "form-field symbol-editor",
                        span { {s(lang, "symbol_allowlist")} }
                        textarea { value: "{symbol_allowlist()}", oninput: move |event| symbol_allowlist.set(event.value()) }
                    }
                    label { class: "form-field symbol-editor",
                        span { {s(lang, "symbol_blocklist")} }
                        textarea { value: "{symbol_blocklist()}", oninput: move |event| symbol_blocklist.set(event.value()) }
                    }
                }
            }
        }
    }
}

#[component]
fn FundingExchangeEditor(
    exchange_label: String,
    mut enabled: Signal<bool>,
    mut account: Signal<String>,
    mut env_prefix: Signal<String>,
    options: Vec<CredentialAccountOption>,
    select_options: Vec<CredentialAccountOption>,
    mut active_view: Signal<ControlPanelView>,
    mut api_key_exchange: Signal<String>,
    mut api_key_account: Signal<String>,
    mut api_key_namespace: Signal<String>,
    lang: Language,
    mut message: Signal<String>,
) -> Element {
    let credential_exchange = exchange_label
        .trim()
        .trim_end_matches(".io")
        .to_ascii_lowercase();
    let account_select_options = select_options.clone();
    let credential_select_options = select_options.clone();
    rsx! {
        label { class: "check-row",
            input { r#type: "checkbox", checked: "{enabled()}", onchange: move |event| enabled.set(event.checked()) }
            span { "{exchange_label}" }
        }
        label { class: "form-field",
            span { "Account" }
            CrossArbAccountSelect {
                value: account(),
                options,
                on_change: move |value: String| {
                    if let Some(prefix) = env_prefix_for_account(&account_select_options, &value) {
                        env_prefix.set(prefix);
                    }
                    account.set(value);
                }
            }
        }
        label { class: "form-field",
            span { "Env" }
            input { value: "{env_prefix()}", oninput: move |event| env_prefix.set(event.value()) }
        }
        button {
            class: "mini-button",
            onclick: move |_| {
                let account_value = account();
                let selected_account = if account_value.trim().is_empty() {
                    "default".to_string()
                } else {
                    account_value
                };
                let namespace = credential_namespace_for_account(&credential_select_options, &selected_account)
                    .unwrap_or_else(|| env_prefix());
                api_key_exchange.set(credential_exchange.clone());
                api_key_account.set(selected_account);
                api_key_namespace.set(namespace);
                save_active_view(ControlPanelView::ApiKeys);
                active_view.set(ControlPanelView::ApiKeys);
                message.set(t(lang, "api_keys_loaded_for_edit").to_string());
            },
            {s(lang, "configure_credentials")}
        }
    }
}

#[component]
fn NumberField(label: String, mut value: Signal<String>) -> Element {
    rsx! {
        label { class: "form-field",
            span { "{label}" }
            input { value: "{value()}", oninput: move |event| value.set(event.value()) }
        }
    }
}

fn funding_lifecycle_click(
    command: &'static str,
    token: String,
    mut message: Signal<String>,
    lang: Language,
) -> impl FnMut(Event<MouseData>) + 'static {
    move |_| {
        let token_value = token.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let now = js_sys::Date::new_0()
                .to_iso_string()
                .as_string()
                .unwrap_or_default();
            let command_id = format!(
                "web-funding-{}-{}",
                command.to_ascii_lowercase(),
                js_sys::Date::now() as u64
            );
            let body = json!({
                "schema_version": 1,
                "command_id": command_id,
                "strategy_id": STRATEGY_ID,
                "run_id": format!("run-{}", js_sys::Date::now() as u64),
                "command": command,
                "requested_by": "web",
                "idempotency_key": command_id,
                "requested_at": now,
            });
            match send_strategy_command(&token_value, STRATEGY_ID, &body).await {
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

fn apply_loaded_settings(
    value: &Value,
    lang: Language,
    mut config_path: Signal<String>,
    mut mode: Signal<String>,
    mut per_exchange_limit: Signal<String>,
    mut min_funding_rate: Signal<String>,
    mut require_next_funding_time: Signal<bool>,
    mut max_funding_snapshot_age_ms: Signal<String>,
    mut min_seconds_to_settlement: Signal<String>,
    mut max_seconds_to_settlement: Signal<String>,
    mut scan_minute: Signal<String>,
    mut notional_usdt: Signal<String>,
    mut open_seconds: Signal<String>,
    mut close_seconds: Signal<String>,
    mut readback_delay: Signal<String>,
    mut close_timeout: Signal<String>,
    mut close_retries: Signal<String>,
    mut max_slippage: Signal<String>,
    mut allow_existing_position: Signal<bool>,
    mut symbol_allowlist: Signal<String>,
    mut symbol_blocklist: Signal<String>,
    mut binance_enabled: Signal<bool>,
    mut bitget_enabled: Signal<bool>,
    mut gate_enabled: Signal<bool>,
    mut binance_account: Signal<String>,
    mut bitget_account: Signal<String>,
    mut gate_account: Signal<String>,
    mut binance_env: Signal<String>,
    mut bitget_env: Signal<String>,
    mut gate_env: Signal<String>,
    mut settings: Signal<Value>,
) {
    let form = FundingArbSettingsFormData::from_settings(value, lang);
    let binance_enabled_value = funding_exchange_enabled(&form, "binance");
    let bitget_enabled_value = funding_exchange_enabled(&form, "bitget");
    let gate_enabled_value = funding_exchange_enabled(&form, "gate");
    let binance_account_value = funding_exchange_text(&form, "binance", "account");
    let bitget_account_value = funding_exchange_text(&form, "bitget", "account");
    let gate_account_value = funding_exchange_text(&form, "gate", "account");
    let binance_env_value = funding_exchange_text(&form, "binance", "env");
    let bitget_env_value = funding_exchange_text(&form, "bitget", "env");
    let gate_env_value = funding_exchange_text(&form, "gate", "env");
    config_path.set(form.path);
    mode.set(form.mode);
    per_exchange_limit.set(form.per_exchange_limit);
    min_funding_rate.set(form.min_funding_rate);
    require_next_funding_time.set(form.require_next_funding_time);
    max_funding_snapshot_age_ms.set(form.max_funding_snapshot_age_ms);
    min_seconds_to_settlement.set(default_text(form.min_seconds_to_settlement_at_scan, "2"));
    max_seconds_to_settlement.set(default_text(form.max_seconds_to_settlement_at_scan, "600"));
    scan_minute.set(form.scan_minute);
    notional_usdt.set(form.notional_usdt);
    open_seconds.set(form.open_seconds_before_settlement);
    close_seconds.set(form.close_seconds_after_settlement);
    readback_delay.set(form.order_readback_delay_secs);
    close_timeout.set(form.close_limit_timeout_secs);
    close_retries.set(form.close_limit_max_retries);
    max_slippage.set(form.max_slippage_pct);
    allow_existing_position.set(form.allow_existing_symbol_position);
    symbol_allowlist.set(form.symbol_allowlist_text);
    symbol_blocklist.set(form.symbol_blocklist_text);
    binance_enabled.set(binance_enabled_value);
    bitget_enabled.set(bitget_enabled_value);
    gate_enabled.set(gate_enabled_value);
    binance_account.set(binance_account_value);
    bitget_account.set(bitget_account_value);
    gate_account.set(gate_account_value);
    binance_env.set(binance_env_value);
    bitget_env.set(bitget_env_value);
    gate_env.set(gate_env_value);
    settings.set(value.clone());
}

fn funding_process(processes: &Value) -> Value {
    processes
        .as_array()
        .and_then(|rows| {
            rows.iter()
                .find(|row| text_at(row, "strategy_id", Language::En) == STRATEGY_ID)
        })
        .cloned()
        .unwrap_or(Value::Null)
}

fn funding_exchange_enabled(form: &FundingArbSettingsFormData, exchange: &str) -> bool {
    form.exchanges
        .iter()
        .find(|row| row.exchange.eq_ignore_ascii_case(exchange))
        .map(|row| row.enabled)
        .unwrap_or_else(|| matches!(exchange, "bitget" | "gate"))
}

fn funding_exchange_text(form: &FundingArbSettingsFormData, exchange: &str, field: &str) -> String {
    form.exchanges
        .iter()
        .find(|row| row.exchange.eq_ignore_ascii_case(exchange))
        .map(|row| match field {
            "account" => row.account_id.clone(),
            "env" => row.env_prefix.clone(),
            _ => String::new(),
        })
        .unwrap_or_default()
}

fn credential_namespace_for_account(
    options: &[CredentialAccountOption],
    account_id: &str,
) -> Option<String> {
    options
        .iter()
        .find(|option| option.value.eq_ignore_ascii_case(account_id))
        .and_then(|option| {
            let namespace = option.credential_namespace.trim();
            (!namespace.is_empty() && namespace != "-").then(|| namespace.to_string())
        })
}

fn split_lines(value: &str) -> Vec<String> {
    value
        .lines()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn default_text(value: String, default: &str) -> String {
    if value.trim().is_empty() || value == "-" {
        default.to_string()
    } else {
        value
    }
}

fn parse_f64(value: &str, default: f64) -> f64 {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .unwrap_or(default)
}

fn parse_i64(value: &str, default: i64) -> i64 {
    value.trim().parse::<i64>().unwrap_or(default)
}

fn parse_optional_i64(value: &str) -> Option<i64> {
    value.trim().parse::<i64>().ok()
}

fn parse_u64(value: &str, default: u64) -> u64 {
    value.trim().parse::<u64>().unwrap_or(default)
}

fn parse_u32(value: &str, default: u32) -> u32 {
    value.trim().parse::<u32>().unwrap_or(default)
}

fn parse_usize(value: &str, default: usize) -> usize {
    value.trim().parse::<usize>().unwrap_or(default)
}

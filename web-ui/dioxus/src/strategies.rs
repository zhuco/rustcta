use std::collections::BTreeMap;

use dioxus::prelude::*;
use serde_json::{json, Map, Value};

use crate::api::{
    create_strategy, delete_strategy, fetch_dashboard, fetch_strategy_templates,
    send_strategy_command,
};
use crate::i18n::s;
use crate::types::{
    DashboardData, ExchangeCredentialAccountRow, ExchangeCredentialPanelData, Language,
    ProcessWorkspaceRowData, StrategyWorkspaceRowData,
};
use crate::utils::{as_array, bool_at, text_at};

const SYMBOL_QUOTES: [&str; 5] = ["USDT", "USDC", "USD", "BTC", "ETH"];

#[component]
pub(crate) fn StrategiesPanel(
    data: Signal<DashboardData>,
    token: String,
    mut message: Signal<String>,
    lang: Language,
) -> Element {
    let mut templates = use_signal(fallback_strategy_templates);
    let mut selected_template_id = use_signal(|| "unified_arbitrage".to_string());
    let mut strategy_id = use_signal(|| "unified_arb_live".to_string());
    let tenant_id = use_signal(|| "local".to_string());
    let mut run_mode = use_signal(|| "dry_run".to_string());
    let mut save_enable = use_signal(|| false);
    let mut exchange_accounts = use_signal(|| json!({}));
    let mut form_values = use_signal(|| json!({}));
    let mut save_busy = use_signal(|| false);
    let mut show_create = use_signal(|| false);
    let mut editing_strategy = use_signal(String::new);
    let mut create_card_position = use_signal(|| (112.0_f64, 48.0_f64));
    let mut create_card_drag = use_signal(|| None::<(f64, f64, f64, f64)>);

    let fetch_token = token.clone();
    use_future(move || {
        let fetch_token = fetch_token.clone();
        async move {
            match fetch_strategy_templates(&fetch_token).await {
                Ok(value) => templates.set(value),
                Err(error) => message.set(error),
            }
        }
    });

    let snapshot = data();
    let template_items = template_rows(&templates());
    let strategy_cards = strategy_card_rows(&snapshot, &template_items, lang);
    let selected_template = selected_template(&template_items, &selected_template_id())
        .cloned()
        .unwrap_or_else(default_template);
    let credential_data = ExchangeCredentialPanelData::from_status(&snapshot.api_keys, lang);
    let account_rows = usable_strategy_accounts(&snapshot.api_keys, &credential_data, lang);
    let account_options = account_options(&account_rows);
    let selected_strategy_kind = text_at(&selected_template, "strategy_kind", Language::En);
    let common_fields = as_array(
        selected_template
            .get("common_fields")
            .unwrap_or(&Value::Null),
    );
    let strategy_fields = as_array(
        selected_template
            .get("strategy_fields")
            .unwrap_or(&Value::Null),
    );
    let exchange_slots = as_array(
        selected_template
            .get("exchange_slots")
            .unwrap_or(&Value::Null),
    );
    let multi_account_template = template_uses_multi_accounts(&selected_template);
    let can_submit = strategy_form_can_submit(
        &strategy_id(),
        &exchange_accounts(),
        &exchange_slots,
        multi_account_template,
    );
    let token_for_save = token.clone();

    rsx! {
        section { class: "strategy-console strategy-canvas-page",
            div {
                class: "strategy-canvas",
                onmousemove: move |event| {
                    if let Some((start_x, start_y, start_left, start_top)) = create_card_drag() {
                        let point = event.map(|data| data.client_coordinates());
                        let left = snap_card_axis(start_left + point.x - start_x, 12.0, 560.0);
                        let top = snap_card_axis(start_top + point.y - start_y, 44.0, 420.0);
                        create_card_position.set((left, top));
                    }
                },
                onmouseup: move |_| create_card_drag.set(None),
                onmouseleave: move |_| create_card_drag.set(None),
                div { class: "strategy-canvas-toolbar",
                    button {
                        class: "button primary strategy-add-button",
                        onclick: move |_| {
                            editing_strategy.set(String::new());
                            strategy_id.set(default_strategy_id_for_template(&selected_template_id()));
                            form_values.set(json!({}));
                            exchange_accounts.set(json!({}));
                            create_card_position.set((112.0, 48.0));
                            show_create.set(true);
                        },
                        "+ 添加"
                    }
                }

                if show_create() {
                    div {
                        class: "strategy-create-shell strategy-canvas-create",
                        style: create_card_style(create_card_position()),
                        div { class: "strategy-simple-layout",
                            div { class: "strategy-simple-main",
                                div { class: "strategy-edit-card strategy-create-card",
                                    div {
                                        class: "strategy-create-head",
                                        onmousedown: move |event| {
                                            let point = event.map(|data| data.client_coordinates());
                                            let (left, top) = create_card_position();
                                            create_card_drag.set(Some((point.x, point.y, left, top)));
                                        },
                                        div {
                                            strong { "添加卡片" }
                                            span { "{text_at(&selected_template, \"label\", lang)}" }
                                        }
                                        button {
                                            class: "button strategy-create-close",
                                            onclick: move |_| show_create.set(false),
                                            "取消"
                                        }
                                    }
                                    div { class: "strategy-simple-section strategy-primary-fields",
                                        div { class: "strategy-priority-grid",
                                            label {
                                                span { "策略" }
                                                select {
                                                    value: "{selected_template_id()}",
                                                    onchange: move |event| {
                                                        let template_id = event.value();
                                                        selected_template_id.set(template_id.clone());
                                                        strategy_id.set(default_strategy_id_for_template(&template_id));
                                                        editing_strategy.set(String::new());
                                                        form_values.set(json!({}));
                                                        exchange_accounts.set(json!({}));
                                                    },
                                                    for template in template_items.iter() {
                                                        {
                                                            let template_id = text_at(template, "template_id", Language::En);
                                                            let template_label = text_at(template, "label", lang);
                                                            rsx! {
                                                                option {
                                                                    value: "{template_id}",
                                                                    "{template_label}"
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            label {
                                                span { "ID" }
                                                input {
                                                    value: "{strategy_id()}",
                                                    oninput: move |event| strategy_id.set(event.value())
                                                }
                                            }

                                            div { class: "strategy-account-selector",
                                                if account_options.is_empty() {
                                                    div { class: "empty-state compact", {s(lang, "no_connected_exchanges")} }
                                                } else if multi_account_template {
                                                    div { class: "strategy-account-inline",
                                                        span { "账户" }
                                                        div { class: "account-dropdown-field",
                                                        details { class: "account-dropdown",
                                                            summary {
                                                                span { "{multi_account_summary(&exchange_accounts(), &account_options)}" }
                                                            }
                                                            div { class: "account-dropdown-menu",
                                                                for option in account_options.iter() {
                                                                    {
                                                                        let selected = selected_exchange_accounts(&exchange_accounts(), "venues")
                                                                            .iter()
                                                                            .any(|value| value == &option.value);
                                                                        let option_value = option.value.clone();
                                                                        rsx! {
                                                                            label { class: "account-dropdown-option",
                                                                                input {
                                                                                    r#type: "checkbox",
                                                                                    checked: selected,
                                                                                    onchange: move |event| {
                                                                                        set_account_selection(exchange_accounts, "venues", option_value.clone(), event.checked());
                                                                                    }
                                                                                }
                                                                                span { "{option.label}" }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        }
                                                    }
                                                } else {
                                                    div { class: "strategy-slot-selectors",
                                                        for slot in exchange_slots.iter() {
                                                            {
                                                                let slot_id = text_at(slot, "slot_id", Language::En);
                                                                let slot_label = text_at(slot, "label", lang);
                                                                let current = selected_exchange_account(&exchange_accounts(), &slot_id);
                                                                rsx! {
                                                                    label {
                                                                        span { "{slot_label}" }
                                                                        select {
                                                                            value: "{current}",
                                                                            onchange: move |event| {
                                                                                set_object_value(exchange_accounts, &slot_id, event.value());
                                                                            },
                                                                            option { value: "", "选择已连接账户" }
                                                                            for option in account_options.iter() {
                                                                                option {
                                                                                    value: "{option.value}",
                                                                                    "{option.label}"
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
                                            label { class: "strategy-mode-switch strategy-mode-field",
                                                span { "运行模式" }
                                                select {
                                                    value: "{run_mode()}",
                                                    onchange: move |event| run_mode.set(event.value()),
                                                    option { value: "dry_run", "模拟运行" }
                                                    option { value: "small_live", "小仓实盘" }
                                                    option { value: "live", "实盘" }
                                                }
                                            }
                                        }
                                    }

                                    div { class: "strategy-simple-section",
                                        div { class: "strategy-compact-grid two strategy-param-grid",
                                            for field in common_fields.iter() {
                                                {render_strategy_field(field, form_values, lang)}
                                            }
                                            for field in strategy_fields.iter() {
                                                {render_strategy_field(field, form_values, lang)}
                                            }
                                        }
                                    }

                                    div { class: "strategy-simple-submit",
                                        label { class: "checkbox-row",
                                            input {
                                                r#type: "checkbox",
                                                checked: save_enable(),
                                                onchange: move |event| save_enable.set(event.checked())
                                            }
                                            span { "保存后启动" }
                                        }
                                        button {
                                            class: "button primary",
                                            disabled: save_busy() || !can_submit,
                                            onclick: move |_| {
                                                let token_value = token_for_save.clone();
                                                let mut data_for_save = data;
                                                let current_strategy_id = strategy_id();
                                                let current_tenant_id = tenant_id();
                                                let current_run_mode = run_mode();
                                                let current_template_id = selected_template_id();
                                                let current_strategy_kind = selected_strategy_kind.clone();
                                                let current_exchange_accounts = normalized_exchange_accounts(
                                                    &current_template_id,
                                                    &exchange_accounts(),
                                                );
                                                let current_values = form_values_with_defaults(&common_fields, &strategy_fields, &form_values());
                                                let should_enable = save_enable();
                                                if current_strategy_id.trim().is_empty() || !can_submit {
                                                    message.set(s(lang, "strategy_form_invalid"));
                                                    return;
                                                }
                                                save_busy.set(true);
                                                spawn(async move {
                                                    let config_path = format!("config/generated/{current_strategy_id}.yml");
                                                    let body = json!({
                                                        "strategy_id": current_strategy_id,
                                                        "strategy_kind": current_strategy_kind,
                                                        "template_id": current_template_id,
                                                        "tenant_id": current_tenant_id,
                                                        "run_mode": current_run_mode,
                                                        "config_path": config_path,
                                                        "log_path": format!("logs/supervisor/{current_strategy_id}.log"),
                                                        "working_dir": ".",
                                                        "exchange_accounts": current_exchange_accounts,
                                                        "risk": current_values.get("risk").cloned().unwrap_or_else(|| json!({})),
                                                        "params": current_values.get("params").cloned().unwrap_or_else(|| json!({})),
                                                    });
                                                    match create_strategy(&token_value, &body).await {
                                                        Ok(_) if should_enable => {
                                                            let command = lifecycle_command_payload(&current_strategy_id, "Start");
                                                            match send_strategy_command(&token_value, &current_strategy_id, &command).await {
                                                                Ok(_) => message.set(format!("{} 启动命令已提交。", s(lang, "strategy_saved"))),
                                                                Err(error) => message.set(format!("{} {error}", s(lang, "strategy_saved_start_failed"))),
                                                            }
                                                            let refreshed = fetch_dashboard(&token_value, data_for_save()).await;
                                                            data_for_save.set(refreshed.data);
                                                        }
                                                        Ok(_) => {
                                                            let refreshed = fetch_dashboard(&token_value, data_for_save()).await;
                                                            data_for_save.set(refreshed.data);
                                                            show_create.set(false);
                                                            save_enable.set(false);
                                                            message.set(format!("{} 已保存为暂停卡片，可在策略卡片中启动。", s(lang, "strategy_saved")));
                                                        }
                                                        Err(error) => message.set(error),
                                                    }
                                                    save_busy.set(false);
                                                });
                                            },
                                            if save_enable() {
                                                "保存并启动"
                                            } else {
                                                "保存"
                                            }
                                        }
                                    }
                                }
                            }

                        }
                    }
                }

                div { class: "strategy-canvas-grid",
                    for card in strategy_cards.iter() {
                        {
                            let card_id = card.strategy_id.clone();
                            let edit_id = card.strategy_id.clone();
                            let delete_id = card.strategy_id.clone();
                            let command = if card.enabled { "Stop" } else { "Start" };
                            let command_label = if card.enabled { "停止" } else { "启动" };
                            let template_id = template_id_for_kind(&template_items, &card.strategy_kind);
                            let default_id = default_strategy_id_for_template(&template_id);
                            let token_for_command = token.clone();
                            let token_for_delete = token.clone();
                            let mut data_for_delete = data;
                            rsx! {
                                div { class: "strategy-config-card strategy-canvas-card",
                                    div { class: "strategy-config-card-head",
                                        div {
                                            strong { "{card.name}" }
                                            span { "{card.strategy_kind}" }
                                        }
                                        span { class: card.status_class.as_str(), "{card.status_label}" }
                                    }
                                    div { class: "strategy-card-meta",
                                        div { span { "交易对" } strong { "{card.symbol_text}" } }
                                        div { span { "交易所" } strong { "{card.exchange_text}" } }
                                        div { span { "ID" } strong { "{card.strategy_id}" } }
                                        div { span { "配置" } strong { "{card.config_path}" } }
                                        div { span { "运行号" } strong { "{card.run_id}" } }
                                        div { span { "心跳" } strong { "{card.last_heartbeat}" } }
                                        div { span { "进程" } strong { "{card.process_text}" } }
                                        div { span { "日志" } strong { "{card.log_text}" } }
                                    }
                                    div { class: "strategy-card-actions",
                                        button {
                                            class: if card.enabled { "button danger" } else { "button primary" },
                                            onclick: move |_| {
                                                let token_value = token_for_command.clone();
                                                let strategy_id_value = card_id.clone();
                                                let payload = lifecycle_command_payload(&strategy_id_value, command);
                                                spawn(async move {
                                                    match send_strategy_command(&token_value, &strategy_id_value, &payload).await {
                                                        Ok(_) => message.set(format!("{strategy_id_value} 命令已提交。")),
                                                        Err(error) => message.set(error),
                                                    }
                                                });
                                            },
                                            "{command_label}"
                                        }
                                        button {
                                            class: "button",
                                            onclick: move |_| {
                                                selected_template_id.set(template_id.clone());
                                                strategy_id.set(if edit_id.is_empty() { default_id.clone() } else { edit_id.clone() });
                                                editing_strategy.set(edit_id.clone());
                                                form_values.set(json!({}));
                                                exchange_accounts.set(json!({}));
                                                show_create.set(true);
                                                message.set(format!("{edit_id} 已载入配置表单。"));
                                            },
                                            "编辑"
                                        }
                                        button {
                                            class: "button danger",
                                            onclick: move |_| {
                                                let token_value = token_for_delete.clone();
                                                let strategy_id_value = delete_id.clone();
                                                let previous = data_for_delete();
                                                spawn(async move {
                                                    match delete_strategy(&token_value, &strategy_id_value).await {
                                                        Ok(_) => {
                                                            let result = fetch_dashboard(&token_value, previous).await;
                                                            data_for_delete.set(result.data);
                                                            message.set(format!("{strategy_id_value} 配置已删除。"));
                                                        }
                                                        Err(error) => message.set(error),
                                                    }
                                                });
                                            },
                                            "删除"
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if strategy_cards.is_empty() {
                        div { class: "empty-state strategy-empty-state", "暂无策略配置。" }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct StrategyCardRow {
    strategy_id: String,
    name: String,
    strategy_kind: String,
    status_label: String,
    status_class: String,
    enabled: bool,
    config_path: String,
    run_id: String,
    last_heartbeat: String,
    symbol_text: String,
    exchange_text: String,
    process_text: String,
    log_text: String,
}

#[derive(Clone, Debug, PartialEq)]
struct AccountOption {
    value: String,
    label: String,
}

fn strategy_card_rows(
    data: &DashboardData,
    templates: &[Value],
    lang: Language,
) -> Vec<StrategyCardRow> {
    let process_rows = ProcessWorkspaceRowData::from_rows(&data.processes, lang)
        .into_iter()
        .map(|row| (row.strategy_id.clone(), row))
        .collect::<BTreeMap<_, _>>();
    let mut rows = StrategyWorkspaceRowData::from_rows(&data.strategies, lang)
        .into_iter()
        .map(|strategy| {
            let process = process_rows.get(&strategy.strategy_id);
            let status = process
                .map(|row| row.status.clone())
                .filter(|value| value != "-")
                .unwrap_or_else(|| strategy.status.clone());
            let enabled = status_is_enabled(&status);
            let process_text = process
                .map(|row| row.process_id.clone())
                .filter(|value| value != "-")
                .unwrap_or_else(|| "-".to_string());
            let log_text = process
                .map(|row| {
                    if row.log_configured {
                        "已配置"
                    } else {
                        "未配置"
                    }
                })
                .unwrap_or("-");
            let strategy_kind = strategy.strategy_kind.clone();
            let symbol_text = default_symbol_text_for_kind(&strategy_kind);
            let exchange_text = default_exchange_text_for_kind(&strategy_kind);
            StrategyCardRow {
                name: strategy_label(templates, &strategy_kind, &strategy.strategy_id, lang),
                strategy_id: strategy.strategy_id,
                strategy_kind,
                status_label: enabled_label(enabled, &status),
                status_class: status_class(enabled, &status).to_string(),
                enabled,
                symbol_text,
                exchange_text,
                config_path: compact_path(&strategy.config_path),
                run_id: compact_text(&strategy.run_id, 30),
                last_heartbeat: process
                    .map(|row| row.last_heartbeat_at.clone())
                    .filter(|value| value != "-")
                    .unwrap_or_else(|| "-".to_string()),
                process_text,
                log_text: log_text.to_string(),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.strategy_id.cmp(&right.strategy_id));
    rows
}

fn create_card_style(position: (f64, f64)) -> String {
    format!(
        "left: {:.0}px; top: {:.0}px;",
        position.0.max(12.0),
        position.1.max(44.0)
    )
}

fn snap_card_axis(value: f64, min: f64, max: f64) -> f64 {
    let snapped = (value / 8.0).round() * 8.0;
    snapped.clamp(min, max)
}

fn compact_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "-" {
        return "-".to_string();
    }
    trimmed
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| trimmed.to_string())
}

fn compact_text(value: &str, max_chars: usize) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "-" {
        return "-".to_string();
    }
    if trimmed.chars().count() <= max_chars {
        return trimmed.to_string();
    }
    let mut text = trimmed
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    text.push('…');
    text
}

fn default_symbol_text_for_kind(strategy_kind: &str) -> String {
    match strategy_kind {
        "hedged_grid" => "BTC/USDT".to_string(),
        "unified_arbitrage" => "BTC/USDT, ETH/USDT".to_string(),
        _ => "-".to_string(),
    }
}

fn default_exchange_text_for_kind(strategy_kind: &str) -> String {
    match strategy_kind {
        "hedged_grid" => "单账户合约".to_string(),
        "unified_arbitrage" => "双交易所合约".to_string(),
        _ => "配置内账户".to_string(),
    }
}

fn strategy_label(
    templates: &[Value],
    strategy_kind: &str,
    strategy_id: &str,
    lang: Language,
) -> String {
    templates
        .iter()
        .find(|template| text_at(template, "strategy_kind", Language::En) == strategy_kind)
        .map(|template| text_at(template, "label", lang))
        .filter(|label| label != "-")
        .map(|label| format!("{label} / {strategy_id}"))
        .unwrap_or_else(|| strategy_id.to_string())
}

fn status_is_enabled(status: &str) -> bool {
    let status = status.to_ascii_lowercase();
    status.contains("running")
        || status.contains("starting")
        || status.contains("active")
        || status.contains("运行")
        || status.contains("启动")
}

fn enabled_label(enabled: bool, status: &str) -> String {
    if enabled {
        "已启用".to_string()
    } else if status.trim().is_empty() || status == "-" {
        "未启用".to_string()
    } else {
        "未启用".to_string()
    }
}

fn status_class(enabled: bool, status: &str) -> &'static str {
    let status = status.to_ascii_lowercase();
    if enabled {
        "status-pill good"
    } else if status.contains("fail") || status.contains("error") {
        "status-pill bad"
    } else {
        "status-pill warn"
    }
}

fn render_strategy_field(field: &Value, form_values: Signal<Value>, lang: Language) -> Element {
    let field_id = text_at(field, "field_id", Language::En);
    let label = text_at(field, "label", lang);
    let unit = text_at(field, "unit", lang);
    let input_type = text_at(field, "input_type", Language::En);
    let value = form_field_value(&form_values(), &field_id)
        .unwrap_or_else(|| text_at(field, "default_value", Language::En));
    if input_type == "select" {
        let options = as_array(field.get("options").unwrap_or(&Value::Null));
        return rsx! {
            label {
                span {
                    "{label}"
                    if unit != "-" {
                        small { " {unit}" }
                    }
                }
                select {
                    value: "{value}",
                    onchange: move |event| {
                        set_form_value(form_values, &field_id, event.value());
                    },
                    for option in options.iter() {
                        {
                            let option_value = text_at(option, "value", Language::En);
                            let option_label = text_at(option, "label", lang);
                            rsx! {
                                option {
                                    value: "{option_value}",
                                    "{option_label}"
                                }
                            }
                        }
                    }
                }
            }
        };
    }
    if input_type == "checkbox" {
        return rsx! {
            label { class: "checkbox-field",
                input {
                    r#type: "checkbox",
                    checked: truthy_text(&value),
                    onchange: move |event| {
                        set_form_value(form_values, &field_id, event.checked().to_string());
                    }
                }
                span {
                    "{label}"
                    if unit != "-" {
                        small { " {unit}" }
                    }
                }
            }
        };
    }
    rsx! {
        label {
            span {
                "{label}"
                if unit != "-" {
                    small { " {unit}" }
                }
            }
            input {
                r#type: "{input_type}",
                value: "{value}",
                oninput: move |event| {
                    set_form_value(form_values, &field_id, event.value());
                }
            }
        }
    }
}

fn template_rows(value: &Value) -> Vec<Value> {
    as_array(value.get("templates").unwrap_or(value))
}

fn selected_template<'a>(templates: &'a [Value], template_id: &str) -> Option<&'a Value> {
    templates
        .iter()
        .find(|item| text_at(item, "template_id", Language::En) == template_id)
        .or_else(|| templates.first())
}

fn template_uses_multi_accounts(_template: &Value) -> bool {
    false
}

fn strategy_form_can_submit(
    strategy_id: &str,
    exchange_accounts: &Value,
    exchange_slots: &[Value],
    multi_account_template: bool,
) -> bool {
    if strategy_id.trim().is_empty() {
        return false;
    }
    if multi_account_template {
        return !selected_exchange_accounts(exchange_accounts, "venues").is_empty();
    }
    exchange_slots.iter().all(|slot| {
        !bool_at(slot, "required")
            || !selected_exchange_account(
                exchange_accounts,
                &text_at(slot, "slot_id", Language::En),
            )
            .trim()
            .is_empty()
    })
}

fn default_template() -> Value {
    template_rows(&fallback_strategy_templates())
        .into_iter()
        .next()
        .unwrap_or_else(|| json!({}))
}

fn fallback_strategy_templates() -> Value {
    json!({
        "templates": [
            {
                "template_id": "hedged_grid",
                "strategy_kind": "hedged_grid",
                "label": "对冲网格",
                "description": "单账户多空双开网格。",
                "exchange_slots": [
                    { "slot_id": "execution", "label": "交易所", "market_type": "perpetual", "required": true }
                ],
                "common_fields": [],
                "strategy_fields": [
                    { "field_id": "symbol", "label": "交易对", "input_type": "text", "default_value": "BTC/USDT", "required": true },
                    {
                        "field_id": "grid_spacing_mode",
                        "label": "类型",
                        "input_type": "select",
                        "default_value": "pct",
                        "required": true,
                        "options": [
                            { "value": "pct", "label": "等比" },
                            { "value": "abs", "label": "等差" }
                        ]
                    },
                    { "field_id": "grid_spacing", "label": "间距", "input_type": "number", "default_value": "0.25", "unit": "%/U", "required": true },
                    { "field_id": "grid_order_count", "label": "挂单", "input_type": "number", "default_value": "8", "required": true },
                    { "field_id": "order_notional_usdt", "label": "每格USDT", "input_type": "number", "default_value": "50", "required": true }
                ]
            },
            {
                "template_id": "unified_arbitrage",
                "strategy_kind": "unified_arbitrage",
                "label": "跨所合约对冲",
                "description": "两个永续交易所之间按开仓净边际和平仓净利阈值执行双边对冲。",
                "exchange_slots": [
                    { "slot_id": "long", "label": "做多交易所", "market_type": "perpetual", "required": true },
                    { "slot_id": "short", "label": "做空交易所", "market_type": "perpetual", "required": true }
                ],
                "common_fields": common_fallback_fields(),
                "strategy_fields": [
                    { "field_id": "symbols", "label": "交易对列表", "input_type": "text", "default_value": "BTC/USDT,ETH/USDT,SOL/USDT", "required": true },
                    { "field_id": "min_open_raw_spread_pct", "label": "开仓原始价差", "input_type": "number", "default_value": "0.12", "unit": "%", "required": true },
                    { "field_id": "min_open_net_edge_pct", "label": "开仓净边际", "input_type": "number", "default_value": "0.04", "unit": "%", "required": true },
                    { "field_id": "close_min_net_profit_pct", "label": "平仓净利", "input_type": "number", "default_value": "0.03", "unit": "%", "required": true },
                    { "field_id": "max_close_spread_pct", "label": "最大平仓价差", "input_type": "number", "default_value": "0.08", "unit": "%", "required": true }
                ]
            },
        ]
    })
}

fn common_fallback_fields() -> Value {
    json!([
        { "field_id": "max_total_notional_usdt", "label": "最大总名义本金", "input_type": "number", "default_value": "1000", "unit": "USDT", "required": true },
        { "field_id": "max_single_order_usdt", "label": "每单金额", "input_type": "number", "default_value": "50", "unit": "USDT", "required": true },
        { "field_id": "max_slippage_pct", "label": "最大滑点", "input_type": "number", "default_value": "0.08", "unit": "%", "required": true },
        { "field_id": "cooldown_ms", "label": "下单冷却", "input_type": "number", "default_value": "500", "unit": "ms", "required": true }
    ])
}

fn template_id_for_kind(templates: &[Value], strategy_kind: &str) -> String {
    templates
        .iter()
        .find(|template| text_at(template, "strategy_kind", Language::En) == strategy_kind)
        .map(|template| text_at(template, "template_id", Language::En))
        .filter(|value| value != "-")
        .unwrap_or_else(|| match strategy_kind {
            "hedged_grid" => "hedged_grid".to_string(),
            "unified_arbitrage" => "unified_arbitrage".to_string(),
            value => value.to_string(),
        })
}

fn default_strategy_id_for_template(template_id: &str) -> String {
    match template_id {
        "hedged_grid" => "hedged_grid_live".to_string(),
        "unified_arbitrage" => "unified_arb_live".to_string(),
        value => format!("{value}_live"),
    }
}

fn usable_strategy_accounts(
    api_keys: &Value,
    credential_data: &ExchangeCredentialPanelData,
    lang: Language,
) -> Vec<ExchangeCredentialAccountRow> {
    let mut rows = Vec::new();
    merge_strategy_account_rows(
        &mut rows,
        ExchangeCredentialAccountRow::from_rows(
            api_keys.get("supported_exchanges").unwrap_or(&Value::Null),
            lang,
        ),
    );
    merge_strategy_account_rows(&mut rows, credential_data.exchange_rows.clone());
    rows.retain(account_credentials_configured);
    rows.sort_by(|left, right| {
        account_connection_rank(right)
            .cmp(&account_connection_rank(left))
            .then_with(|| left.label.to_lowercase().cmp(&right.label.to_lowercase()))
            .then_with(|| {
                left.account_id
                    .to_lowercase()
                    .cmp(&right.account_id.to_lowercase())
            })
    });
    rows
}

fn merge_strategy_account_rows(
    rows: &mut Vec<ExchangeCredentialAccountRow>,
    incoming: Vec<ExchangeCredentialAccountRow>,
) {
    for row in incoming {
        let key = account_value(&row);
        if let Some(index) = rows
            .iter()
            .position(|existing| account_value(existing).eq_ignore_ascii_case(&key))
        {
            rows[index] = row;
        } else {
            rows.push(row);
        }
    }
}

fn account_credentials_configured(row: &ExchangeCredentialAccountRow) -> bool {
    let required_count = row.fields.iter().filter(|field| field.required).count();
    if required_count == 0 {
        return row.fields.iter().all(|field| field.configured);
    }
    row.fields
        .iter()
        .filter(|field| field.required)
        .all(|field| field.configured)
}

fn account_connection_rank(row: &ExchangeCredentialAccountRow) -> u8 {
    let status = row.connection_status.to_ascii_lowercase();
    if status.contains("ok")
        || status.contains("connected")
        || status.contains("healthy")
        || status.contains("已连接")
    {
        2
    } else if account_credentials_configured(row) {
        1
    } else {
        0
    }
}

fn account_options(rows: &[ExchangeCredentialAccountRow]) -> Vec<AccountOption> {
    rows.iter()
        .map(|row| AccountOption {
            value: account_value(row),
            label: format!("{} · {}", row.label, account_status_text(row)),
        })
        .collect()
}

fn account_status_text(row: &ExchangeCredentialAccountRow) -> &'static str {
    let status = row.connection_status.to_ascii_lowercase();
    if status.contains("ok") || status.contains("connected") || status.contains("已连接") {
        "已验证"
    } else if account_credentials_configured(row) {
        "已配置"
    } else {
        "未配置"
    }
}

fn account_value(row: &ExchangeCredentialAccountRow) -> String {
    format!(
        "{}:{}:{}",
        row.exchange, row.account_id, row.credential_namespace
    )
}

fn selected_exchange_account(value: &Value, slot_id: &str) -> String {
    value
        .get(slot_id)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn selected_exchange_accounts(value: &Value, slot_id: &str) -> Vec<String> {
    match value.get(slot_id) {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(ToString::to_string)
            .collect(),
        Some(Value::String(item)) if !item.trim().is_empty() => vec![item.trim().to_string()],
        _ => Vec::new(),
    }
}

fn multi_account_summary(value: &Value, options: &[AccountOption]) -> String {
    let selected = selected_exchange_accounts(value, "venues");
    if selected.is_empty() {
        return "选择已连接账户".to_string();
    }
    let labels = options
        .iter()
        .filter(|option| selected.iter().any(|value| value == &option.value))
        .map(|option| option.label.clone())
        .collect::<Vec<_>>();
    if labels.is_empty() {
        return format!("已选择 {} 个账户", selected.len());
    }
    if labels.len() <= 2 {
        return labels.join("、");
    }
    format!("已选择 {} 个账户", labels.len())
}

fn set_account_selection(mut signal: Signal<Value>, key: &str, value: String, selected: bool) {
    let mut next = signal();
    if !next.is_object() {
        next = json!({});
    }
    let mut values = selected_exchange_accounts(&next, key);
    if selected {
        if !values.iter().any(|item| item == &value) {
            values.push(value);
        }
    } else {
        values.retain(|item| item != &value);
    }
    if let Some(map) = next.as_object_mut() {
        map.insert(
            key.to_string(),
            Value::Array(values.into_iter().map(Value::String).collect()),
        );
    }
    signal.set(next);
}

fn normalized_exchange_accounts(template_id: &str, value: &Value) -> Value {
    if template_id == "hedged_grid" {
        let execution = selected_exchange_account(value, "execution");
        if !execution.trim().is_empty() {
            return json!({
                "execution": execution,
            });
        }
    }
    if template_id == "unified_arbitrage" {
        return normalize_slot_accounts(value, &["long", "short"]);
    }
    value.clone()
}

fn normalize_slot_accounts(value: &Value, slots: &[&str]) -> Value {
    let mut next = value.clone();
    if !next.is_object() {
        next = json!({});
    }
    if let Some(map) = next.as_object_mut() {
        for slot in slots {
            let account = selected_exchange_account(value, slot);
            if !account.trim().is_empty() {
                map.insert(
                    (*slot).to_string(),
                    Value::String(normalize_account_value(&account)),
                );
            }
        }
    }
    next
}

fn normalize_account_value(value: &str) -> String {
    let mut parts = value.split(':').map(str::trim).collect::<Vec<_>>();
    if let Some(exchange) = parts.first_mut() {
        *exchange = match exchange
            .to_ascii_lowercase()
            .replace(['-', '_', ' '], "")
            .as_str()
        {
            "binance" | "binancespot" | "binanceusdm" => "binance",
            "okx" | "okex" => "okx",
            "bybit" | "bybitunified" => "bybit",
            "bitget" => "bitget",
            "gate" | "gateio" => "gateio",
            _ => *exchange,
        };
    }
    parts.join(":")
}

fn set_object_value(mut signal: Signal<Value>, key: &str, value: String) {
    let mut next = signal();
    if !next.is_object() {
        next = json!({});
    }
    if let Some(map) = next.as_object_mut() {
        map.insert(key.to_string(), Value::String(value));
    }
    signal.set(next);
}

fn form_field_value(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_string)
}

fn set_form_value(signal: Signal<Value>, key: &str, value: String) {
    set_object_value(signal, key, value);
}

fn truthy_text(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "true" | "1" | "yes" | "on"
    )
}

fn form_values_with_defaults(
    common_fields: &[Value],
    strategy_fields: &[Value],
    values: &Value,
) -> Value {
    let mut risk = Map::new();
    for field in common_fields {
        let key = text_at(field, "field_id", Language::En);
        let value = form_field_value(values, &key)
            .unwrap_or_else(|| text_at(field, "default_value", Language::En));
        risk.insert(
            key.clone(),
            Value::String(normalize_strategy_field_value(&key, &value)),
        );
    }
    let mut params = Map::new();
    for field in strategy_fields {
        let key = text_at(field, "field_id", Language::En);
        let value = form_field_value(values, &key)
            .unwrap_or_else(|| text_at(field, "default_value", Language::En));
        params.insert(
            key.clone(),
            Value::String(normalize_strategy_field_value(&key, &value)),
        );
    }
    json!({
        "risk": Value::Object(risk),
        "params": Value::Object(params),
    })
}

fn normalize_strategy_field_value(key: &str, value: &str) -> String {
    match key {
        "symbol" => normalize_symbol_for_storage(value),
        "symbols" => normalize_symbol_list_for_storage(value),
        _ => value.trim().to_string(),
    }
}

fn normalize_symbol_list_for_storage(value: &str) -> String {
    value
        .split([',', '\n', ';'])
        .map(normalize_symbol_for_storage)
        .filter(|symbol| !symbol.is_empty())
        .collect::<Vec<_>>()
        .join(",")
}

fn normalize_symbol_for_storage(value: &str) -> String {
    let value = value.trim().to_ascii_uppercase();
    if value.is_empty() || value == "-" {
        return String::new();
    }
    let compact = value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>();
    for quote in SYMBOL_QUOTES {
        if let Some(base) = compact.strip_suffix(quote) {
            if !base.is_empty() {
                return format!("{base}/{quote}");
            }
        }
    }
    value.replace('-', "/").replace('_', "/")
}

fn lifecycle_command_payload(strategy_id: &str, command: &str) -> Value {
    let now = js_sys::Date::new_0()
        .to_iso_string()
        .as_string()
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".to_string());
    let nonce = js_sys::Math::random().to_string().replace('.', "");
    json!({
        "schema_version": 1,
        "command_id": format!("web-{}-{nonce}", command.to_ascii_lowercase()),
        "strategy_id": strategy_id,
        "run_id": Value::Null,
        "command": command,
        "requested_by": "web",
        "idempotency_key": format!("web-{command}-{strategy_id}-{nonce}"),
        "requested_at": now,
    })
}

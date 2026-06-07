use dioxus::prelude::*;
use gloo_timers::future::TimeoutFuture;
use serde_json::json;

use crate::api::{fetch_credential_status, save_exchange_api_keys};
use crate::i18n::s;
use crate::types::{
    CredentialStatusRowData, DashboardData, ExchangeCredentialAccountRow,
    ExchangeCredentialPanelData, ExchangeCredentialSchema, Language,
};
use crate::ui::StatusPill;

#[component]
pub(crate) fn ApiKeysPanel(
    data: Signal<DashboardData>,
    token: String,
    mut message: Signal<String>,
    lang: Language,
    selected_exchange: String,
    selected_account: String,
    selected_namespace: String,
) -> Element {
    let _ = selected_namespace;
    let credentials_status = data().credentials_status;
    let legacy_status = data().api_keys;
    let status_rows = CredentialStatusRowData::from_response(&credentials_status, lang);
    let panel_data = ExchangeCredentialPanelData::from_status(&legacy_status, lang);
    let selected_slot = selected_credential_slot(&selected_exchange, &selected_account);
    let initial_exchange = if selected_exchange.trim().is_empty() {
        "binance".to_string()
    } else {
        selected_exchange.clone()
    };
    let initial_account = if selected_account.trim().is_empty() {
        "default".to_string()
    } else {
        selected_account.clone()
    };
    let mut form_exchange = use_signal(move || initial_exchange);
    let mut form_account = use_signal(move || initial_account);
    let mut form_exchange_account_id = use_signal(String::new);
    let mut form_api_key = use_signal(String::new);
    let mut form_api_secret = use_signal(String::new);
    let mut form_passphrase = use_signal(String::new);
    let mut saved_api_key_hint = use_signal(String::new);
    let mut saved_api_secret_hint = use_signal(String::new);
    let mut saved_passphrase_hint = use_signal(String::new);
    let mut save_busy = use_signal(|| false);
    let mut delete_busy = use_signal(|| false);
    let active_schema =
        credential_schema_for_exchange(&panel_data.supported_rows, &form_exchange());
    let has_account_id = schema_has_field(active_schema, "account_id");
    let has_passphrase = schema_has_field(active_schema, "passphrase");
    let form_exchange_value = form_exchange();
    let form_account_value = form_account();
    let account_manager_options = panel_data
        .account_manager_rows
        .iter()
        .filter(|row| row.exchange.eq_ignore_ascii_case(&form_exchange_value) && row.enabled)
        .cloned()
        .collect::<Vec<_>>();
    let account_manager_select_options = account_manager_options.clone();
    let save_account_options = account_manager_options.clone();
    let save_token = token.clone();
    let mut refresh_data = data;
    let refresh_token = token.clone();
    let mut refresh_message = message;
    use_future(move || {
        let token_value = refresh_token.clone();
        async move {
            loop {
                if !token_value.is_empty() {
                    match fetch_credential_status(&token_value).await {
                        Ok(status) => {
                            let mut next = refresh_data();
                            if status.get("slots").is_some() {
                                next.credentials_status = status;
                            } else {
                                next.api_keys = status;
                            }
                            refresh_data.set(next);
                        }
                        Err(error) => refresh_message.set(error),
                    }
                }
                TimeoutFuture::new(10_000).await;
            }
        }
    });

    rsx! {
        section { id: "api-keys", class: "panel api-keys credential-status-panel",
            div { class: "panel-title-row",
                div {
                    h2 { {s(lang, "api_key_management")} }
                    p { class: "muted", {s(lang, "api_key_note")} }
                }
                div { class: "row-actions",
                    if panel_data.restart_required {
                        span { class: "pill warn", {s(lang, "restart_required")} }
                    }
                    span { class: "pill neutral", {s(lang, "credential_manage_enabled")} }
                }
            }
            div { class: "runtime-strip api-key-summary-strip",
                div { span { {s(lang, "enabled_exchanges")} } strong { "{panel_data.enabled_exchange_text}" } }
                div { span { {s(lang, "credential_slots")} } strong { "{status_rows.len()}" } }
                div { span { {s(lang, "supported_exchanges")} } strong { "{panel_data.supported_rows.len()}" } }
                div { span { {s(lang, "configured")} } strong { "{configured_count(&status_rows, &panel_data)}" } }
            }
            div { class: "api-key-section api-key-entry-section credential-boundary",
                div { class: "api-key-section-title",
                    h3 { {s(lang, "add_exchange_section")} }
                    span { class: "muted", "{selected_slot}" }
                }
                p { class: "muted", {s(lang, "add_exchange_hint")} }
                div { class: "credential-form-grid",
                    label {
                        span { {s(lang, "exchange")} }
                        select {
                            value: "{form_exchange_value}",
                            onchange: move |event| {
                                form_exchange.set(event.value());
                                form_account.set("default".to_string());
                                saved_api_key_hint.set(String::new());
                                saved_api_secret_hint.set(String::new());
                                saved_passphrase_hint.set(String::new());
                            },
                            for schema in panel_data.supported_rows.iter() {
                                option { value: "{schema.exchange}", "{schema.label}" }
                            }
                        }
                    }
                    label {
                        span { {s(lang, "account")} }
                        if account_manager_options.is_empty() {
                            select {
                                disabled: true,
                                option { value: "", "{s(lang, \"no_account_manager_accounts\")}" }
                            }
                        } else {
                            select {
                                value: "{form_account_value}",
                                onchange: move |event| {
                                    let account_id = event.value();
                                    form_account.set(account_id.clone());
                                    if let Some(row) = account_manager_select_options
                                        .iter()
                                        .find(|row| row.account_id.eq_ignore_ascii_case(&account_id))
                                    {
                                        form_account.set(row.account_id.clone());
                                    }
                                },
                                option { value: "default", "default" }
                                for row in account_manager_options.iter() {
                                    option {
                                        value: "{row.account_id}",
                                        "{row.account_id}"
                                    }
                                }
                            }
                        }
                    }
                    if has_account_id {
                        label {
                            span { {s(lang, "exchange_account_id")} }
                            input {
                                value: "{form_exchange_account_id()}",
                                placeholder: "{s(lang, \"exchange_account_id_placeholder\")}",
                                oninput: move |event| form_exchange_account_id.set(event.value())
                            }
                        }
                    }
                    label {
                        span { {s(lang, "api_key")} }
                        input {
                            value: "{form_api_key()}",
                            placeholder: "{credential_input_placeholder(lang, \"api_key\", &saved_api_key_hint())}",
                            autocomplete: "off",
                            oninput: move |event| form_api_key.set(event.value())
                        }
                    }
                    label {
                        span { {s(lang, "api_secret")} }
                        input {
                            r#type: "password",
                            value: "{form_api_secret()}",
                            placeholder: "{credential_input_placeholder(lang, \"api_secret\", &saved_api_secret_hint())}",
                            autocomplete: "off",
                            oninput: move |event| form_api_secret.set(event.value())
                        }
                    }
                    if has_passphrase {
                        label {
                            span { {s(lang, "passphrase")} }
                            input {
                                r#type: "password",
                                value: "{form_passphrase()}",
                                placeholder: "{credential_input_placeholder(lang, \"passphrase\", &saved_passphrase_hint())}",
                                autocomplete: "off",
                                oninput: move |event| form_passphrase.set(event.value())
                            }
                        }
                    }
                }
                div { class: "row-actions",
                    button {
                        class: "button",
                        disabled: save_busy(),
                        onclick: move |_| {
                            let token_value = save_token.clone();
                            if token_value.trim().is_empty() {
                                message.set(s(lang, "auth_token_required_error"));
                                return;
                            }
                            let exchange = form_exchange();
                            let account_id = normalized_account_input(&form_account());
                            if !save_account_options
                                .iter()
                                .any(|row| row.account_id.eq_ignore_ascii_case(&account_id))
                            {
                                message.set(s(lang, "account_not_in_manager_error"));
                                return;
                            }
                            if !valid_account_identifier(&account_id) {
                                message.set(s(lang, "account_identifier_invalid"));
                                return;
                            }
                            let body = json!({
                                "exchange": exchange,
                                "account_id": account_id,
                                "exchange_account_id": optional_text(form_exchange_account_id()),
                                "api_key": optional_text(form_api_key()),
                                "api_secret": optional_text(form_api_secret()),
                                "passphrase": optional_text(form_passphrase()),
                            });
                            save_busy.set(true);
                            spawn(async move {
                                match save_exchange_api_keys(&token_value, &body).await {
                                    Ok(value) => {
                                        let mut next = data();
                                        if let Some(status) = value.get("status") {
                                            next.api_keys = status.clone();
                                        }
                                        data.set(next);
                                        saved_api_key_hint.set(String::new());
                                        saved_api_secret_hint.set(String::new());
                                        saved_passphrase_hint.set(String::new());
                                        form_api_secret.set(String::new());
                                        form_passphrase.set(String::new());
                                        message.set(s(lang, "api_keys_saved"));
                                    }
                                    Err(error) => message.set(error),
                                }
                                save_busy.set(false);
                            });
                        },
                        {s(lang, "save_keys")}
                    }
                    button {
                        class: "mini-button",
                        onclick: move |_| {
                            form_exchange_account_id.set(String::new());
                            form_api_key.set(String::new());
                            form_api_secret.set(String::new());
                            form_passphrase.set(String::new());
                            saved_api_key_hint.set(String::new());
                            saved_api_secret_hint.set(String::new());
                            saved_passphrase_hint.set(String::new());
                            message.set(s(lang, "api_keys_cleared"));
                        },
                        {s(lang, "clear_keys")}
                    }
                    span { class: "muted", {s(lang, "credential_secret_hint")} }
                }
            }
            div { class: "api-key-section",
                h3 { {s(lang, "workspace_credentials_status")} }
                if status_rows.is_empty() {
                    div { class: "empty-state", {s(lang, "workspace_empty_credentials")} }
                } else {
                    div { class: "table-wrap",
                        table {
                            thead { tr {
                                th { {s(lang, "workspace_credential_slot")} }
                                th { {s(lang, "exchange")} }
                                th { {s(lang, "workspace_tenant")} }
                                th { {s(lang, "configured")} }
                                th { {s(lang, "health")} }
                                th { {s(lang, "workspace_last_verified")} }
                            } }
                            tbody {
                                for row in status_rows.iter() {
                                    tr {
                                        td { "{row.slot_id}" }
                                        td { "{row.exchange_id}" }
                                        td { "{row.tenant_id}" }
                                        td { StatusPill { value: row.configured, lang } }
                                        td { span { class: credential_health_class(&row.health), "{row.health}" } }
                                        td { "{row.last_verified_at}" }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "api-key-section supported-exchanges",
                h3 { {s(lang, "credential_requirements")} }
                div { class: "table-wrap compact-table",
                    table {
                        thead { tr {
                            th { {s(lang, "exchange")} }
                            th { {s(lang, "fields")} }
                            th { {s(lang, "credential_setup_state")} }
                        } }
                        tbody {
                            if panel_data.supported_rows.is_empty() {
                                tr { td { colspan: "3", {s(lang, "no_credential_requirements")} } }
                            }
                            for item in panel_data.supported_rows.iter() {
                                tr {
                                    td { "{item.label}" }
                                    td {
                                        for field in item.fields.iter() {
                                            span {
                                                class: if field.required { "field-chip warn" } else { "field-chip" },
                                                "{field.label}"
                                                if field.required {
                                                    " *"
                                                }
                                            }
                                        }
                                    }
                                    td {
                                        span { class: "muted", {s(lang, "credential_setup_external")} }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "api-key-section",
                h3 { {s(lang, "connected_exchange_accounts")} }
                if panel_data.exchange_rows.is_empty() {
                    div { class: "empty-state", {s(lang, "no_connected_exchanges")} }
                } else {
                    div { class: "table-wrap",
                        table {
                            thead { tr {
                                th { {s(lang, "exchange")} }
                                th { {s(lang, "strategy_enabled")} }
                                th { {s(lang, "account")} }
                                th { {s(lang, "fields")} }
                                th { {s(lang, "field_values")} }
                                th { {s(lang, "source")} }
                                th { {s(lang, "configured")} }
                                th { {s(lang, "actions")} }
                            } }
                            tbody {
                                for row in panel_data.exchange_rows.iter() {
                                    {
                                        let edit_exchange = row.exchange.clone();
                                        let edit_account = row.account_id.clone();
                                        let edit_exchange_account_id = row.exchange_account_id.clone();
                                        let edit_api_key_hint = credential_display_value(row, "api_key");
                                        let edit_api_secret_hint = credential_display_value(row, "api_secret");
                                        let edit_passphrase_hint = credential_display_value(row, "passphrase");
                                        let delete_exchange = row.exchange.clone();
                                        let delete_account = row.account_id.clone();
                                        let delete_credential_namespace = row.credential_namespace.clone();
                                        let delete_token = token.clone();
                                        rsx! {
                                    tr {
                                        td { "{row.label}" }
                                        td { StatusPill { value: row.enabled, lang } }
                                        td {
                                            span { class: if row.default_account { "pill neutral" } else { "pill" }, "{row.account_id}" }
                                        }
                                        td {
                                            for field in row.fields.iter() {
                                                span {
                                                    class: if field.required { "field-chip warn" } else { "field-chip" },
                                                    "{field.label}"
                                                    if field.required {
                                                        " *"
                                                    }
                                                }
                                            }
                                        }
                                        td {
                                            for field in row.fields.iter() {
                                                span { class: "field-chip secret-chip", "{field.display_value}" }
                                            }
                                        }
                                        td {
                                            for field in row.fields.iter() {
                                                span { class: "field-chip", "{field.source}" }
                                            }
                                        }
                                        td {
                                            for field in row.fields.iter() {
                                                StatusPill { value: field.configured, lang }
                                            }
                                        }
                                        td {
                                            div { class: "row-actions compact-actions",
                                                button {
                                                    class: "mini-button",
                                                    onclick: move |_| {
                                                        form_exchange.set(edit_exchange.clone());
                                                        form_account.set(edit_account.clone());
                                                        form_exchange_account_id.set(clean_display_value(&edit_exchange_account_id));
                                                        form_api_key.set(String::new());
                                                        form_api_secret.set(String::new());
                                                        form_passphrase.set(String::new());
                                                        saved_api_key_hint.set(clean_display_value(&edit_api_key_hint));
                                                        saved_api_secret_hint.set(clean_display_value(&edit_api_secret_hint));
                                                        saved_passphrase_hint.set(clean_display_value(&edit_passphrase_hint));
                                                        message.set(s(lang, "api_keys_loaded_for_edit"));
                                                    },
                                                    {s(lang, "edit")}
                                                }
                                                button {
                                                    class: "mini-button danger",
                                                    disabled: delete_busy(),
                                                    onclick: move |_| {
                                                        let token_value = delete_token.clone();
                                                        if token_value.trim().is_empty() {
                                                            message.set(s(lang, "auth_token_required_error"));
                                                            return;
                                                        }
                                                        let exchange = delete_exchange.clone();
                                                        let account = delete_account.clone();
                                                        let credential_namespace = delete_credential_namespace.clone();
                                                        delete_busy.set(true);
                                                        spawn(async move {
                                                            let body = json!({
                                                                "exchange": exchange,
                                                                "account_id": account,
                                                                "credential_namespace": optional_text(credential_namespace),
                                                                "clear": true,
                                                            });
                                                            match save_exchange_api_keys(&token_value, &body).await {
                                                                Ok(value) => {
                                                                    let mut next = data();
                                                                    if let Some(status) = value.get("status") {
                                                                        next.api_keys = status.clone();
                                                                    }
                                                                    data.set(next);
                                                                    message.set(s(lang, "api_keys_deleted"));
                                                                }
                                                                Err(error) => message.set(error),
                                                            }
                                                            delete_busy.set(false);
                                                        });
                                                    },
                                                    {s(lang, "delete_exchange")}
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
            }
        }
    }
}

fn credential_schema_for_exchange<'a>(
    rows: &'a [ExchangeCredentialSchema],
    exchange: &str,
) -> Option<&'a ExchangeCredentialSchema> {
    rows.iter()
        .find(|row| row.exchange.eq_ignore_ascii_case(exchange))
        .or_else(|| rows.first())
}

fn schema_has_field(schema: Option<&ExchangeCredentialSchema>, field: &str) -> bool {
    schema
        .map(|schema| schema.fields.iter().any(|item| item.name == field))
        .unwrap_or_default()
}

fn optional_text(value: String) -> Option<String> {
    let value = value.trim().to_string();
    (!value.is_empty()).then_some(value)
}

fn credential_display_value(row: &ExchangeCredentialAccountRow, field_name: &str) -> String {
    row.fields
        .iter()
        .find(|field| field.name == field_name)
        .map(|field| field.display_value.clone())
        .unwrap_or_default()
}

fn credential_input_placeholder(
    lang: Language,
    label_key: &'static str,
    saved_hint: &str,
) -> String {
    let saved_hint = saved_hint.trim();
    if saved_hint.is_empty() {
        s(lang, label_key)
    } else {
        format!("{} {saved_hint}", s(lang, "credential_saved_placeholder"))
    }
}

fn normalized_account_input(value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        "default".to_string()
    } else {
        value.to_string()
    }
}

fn valid_account_identifier(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn clean_display_value(value: &str) -> String {
    let value = value.trim();
    if value == "-" || value.starts_with('*') {
        String::new()
    } else {
        value.to_string()
    }
}

fn configured_count(
    rows: &[CredentialStatusRowData],
    panel_data: &ExchangeCredentialPanelData,
) -> usize {
    if rows.is_empty() {
        panel_data.configured_account_count
    } else {
        rows.iter().filter(|row| row.configured).count()
    }
}

fn selected_credential_slot(exchange: &str, account: &str) -> String {
    let exchange = exchange.trim();
    let account = account.trim();
    if exchange.is_empty() && account.is_empty() {
        return "-".to_string();
    }
    if account.is_empty() {
        exchange.to_string()
    } else {
        format!("{exchange}:{account}")
    }
}

fn credential_health_class(status: &str) -> &'static str {
    match status {
        "healthy" | "configured" | "true" => "status-pill good",
        "missing" | "unknown" => "status-pill warn",
        "degraded" | "error" | "false" => "status-pill bad",
        _ => "status-pill",
    }
}

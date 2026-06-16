use dioxus::prelude::*;
use gloo_timers::future::TimeoutFuture;
use serde_json::{json, Value};

use crate::api::{
    delete_exchange_api_keys, fetch_credential_status, fetch_exchange_api_keys,
    save_exchange_api_keys, test_exchange_api_keys,
};
use crate::i18n::s;
use crate::types::{
    AccountManagerAccountRow, DashboardData, ExchangeCredentialAccountField,
    ExchangeCredentialAccountRow, ExchangeCredentialField, ExchangeCredentialPanelData,
    ExchangeCredentialSchema, Language,
};
use crate::utils::{as_array, text_at};

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
    let exchange_api_keys = data().api_keys;
    let inventory = data().inventory;
    let panel_data = ExchangeCredentialPanelData::from_status(&exchange_api_keys, lang);
    let list_rows = credential_exchange_list(&panel_data, &exchange_api_keys, &inventory, lang);
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
    let initial_namespace = selected_namespace.trim().to_string();
    let mut editing_row_key = use_signal(String::new);
    let mut form_exchange = use_signal(move || initial_exchange);
    let mut form_account = use_signal(move || initial_account);
    let mut form_namespace = use_signal(move || initial_namespace);
    let mut form_exchange_account_id = use_signal(String::new);
    let mut form_api_key = use_signal(String::new);
    let mut form_api_secret = use_signal(String::new);
    let mut form_passphrase = use_signal(String::new);
    let mut saved_api_key_hint = use_signal(String::new);
    let mut saved_api_secret_hint = use_signal(String::new);
    let mut saved_passphrase_hint = use_signal(String::new);
    let mut save_busy = use_signal(|| false);
    let mut test_busy = use_signal(String::new);
    let active_row = list_rows
        .iter()
        .find(|row| row.row_key == editing_row_key())
        .cloned();
    let has_account_id = active_row
        .as_ref()
        .map(|row| credential_list_has_field(row, "account_id"))
        .unwrap_or_else(|| {
            schema_has_field(
                credential_schema_for_exchange(&panel_data.supported_rows, &form_exchange()),
                "account_id",
            )
        });
    let has_passphrase = active_row
        .as_ref()
        .map(|row| credential_list_has_field(row, "passphrase"))
        .unwrap_or_else(|| {
            schema_has_field(
                credential_schema_for_exchange(&panel_data.supported_rows, &form_exchange()),
                "passphrase",
            )
        });
    let panel_message = message();
    let mut refresh_data = data;
    let refresh_token = token.clone();
    let mut refresh_message = message;
    use_future(move || {
        let token_value = refresh_token.clone();
        async move {
            loop {
                match fetch_credential_status(&token_value).await {
                    Ok(status) => {
                        let mut next = refresh_data();
                        next.credentials_status = status;
                        refresh_data.set(next);
                    }
                    Err(error) => refresh_message.set(error),
                }
                match fetch_exchange_api_keys(&token_value).await {
                    Ok(status) => {
                        let mut next = refresh_data();
                        next.api_keys = status;
                        refresh_data.set(next);
                    }
                    Err(error) => refresh_message.set(error),
                }
                TimeoutFuture::new(10_000).await;
            }
        }
    });

    rsx! {
        section { id: "api-keys", class: "panel api-keys exchange-list-panel",
            div { class: "panel-title-row",
                div {
                    h2 { {exchange_list_title(lang)} }
                    p { class: "muted", {exchange_list_subtitle(lang)} }
                }
                div { class: "row-actions",
                    if panel_data.restart_required {
                        span { class: "pill warn", {s(lang, "restart_required")} }
                    }
                    span { class: "pill neutral", "{credential_list_count_text(lang, list_rows.len())}" }
                }
            }
            if !panel_message.trim().is_empty() {
                div { class: "inline-status-message", "{panel_message}" }
            }
            div { class: "api-key-section exchange-list-section",
                if list_rows.is_empty() {
                    div { class: "empty-state", {s(lang, "no_connected_exchanges")} }
                } else {
                    div { class: "exchange-account-list credential-exchange-list",
                        for item in list_rows.iter() {
                            {
                                let test_token = token.clone();
                                let exchange = item.exchange.clone();
                                let account_id = item.account_id.clone();
                                let namespace = item.namespace.clone();
                                let test_row_key = item.row_key.clone();
                                let clear_row_key = item.row_key.clone();
                                let label = item.list_label.clone();
                                let edit_label = item.list_label.clone();
                                let edit_exchange = item.exchange.clone();
                                let edit_account_id = item.account_id.clone();
                                let edit_namespace = item.namespace.clone();
                                let edit_row_key = item.row_key.clone();
                                let edit_exchange_account_id = item.exchange_account_id.clone();
                                let edit_fields = item.fields.clone();
                                let double_click_row_key = edit_row_key.clone();
                                let double_click_exchange = edit_exchange.clone();
                                let double_click_account_id = edit_account_id.clone();
                                let double_click_namespace = edit_namespace.clone();
                                let double_click_exchange_account_id = edit_exchange_account_id.clone();
                                let double_click_fields = edit_fields.clone();
                                let save_token = token.clone();
                                let save_label = item.list_label.clone();
                                let save_row_key = item.row_key.clone();
                                let delete_token = token.clone();
                                let delete_exchange = item.exchange.clone();
                                let delete_account_id = item.account_id.clone();
                                let delete_namespace = item.namespace.clone();
                                let delete_label = item.list_label.clone();
                                let row_configured = item.configured;
                                let account_meta = credential_account_meta(item);
                                rsx! {
                                    div {
                                        class: "{credential_list_row_class(item, editing_row_key() == item.row_key)}",
                                        title: "{credential_edit_title(lang)}",
                                        ondoubleclick: move |_| {
                                            editing_row_key.set(double_click_row_key.clone());
                                            form_exchange.set(double_click_exchange.clone());
                                            form_account.set(double_click_account_id.clone());
                                            form_namespace.set(double_click_namespace.clone());
                                            form_exchange_account_id.set(clean_display_value(&double_click_exchange_account_id));
                                            form_api_key.set(String::new());
                                            form_api_secret.set(String::new());
                                            form_passphrase.set(String::new());
                                            saved_api_key_hint.set(saved_hint_for_field(&double_click_fields, "api_key", lang));
                                            saved_api_secret_hint.set(saved_hint_for_field(&double_click_fields, "api_secret", lang));
                                            saved_passphrase_hint.set(saved_hint_for_field(&double_click_fields, "passphrase", lang));
                                        },
                                        div { class: "exchange-account-main",
                                            strong { "{item.list_label}" }
                                            span { "{account_meta}" }
                                        }
                                        div { class: "exchange-account-status",
                                            span { class: credential_state_class(item), "{credential_state_label(lang, item)}" }
                                            small { "{item.last_tested_at}" }
                                        }
                                        div { class: "credential-field-strip",
                                            for field in item.fields.iter() {
                                                span {
                                                    class: credential_field_chip_class(field),
                                                    "{field.label}"
                                                    if field.required {
                                                        " *"
                                                    }
                                                }
                                            }
                                        }
                                        if item.unified_account {
                                            div { class: "exchange-account-list-balance unified-balance",
                                                span { {s(lang, "unified_balance")} }
                                                strong { "{item.unified_balance}" }
                                            }
                                        } else {
                                            div { class: "exchange-account-list-balance",
                                                span { {s(lang, "spot_balance")} }
                                                strong { "{item.spot_balance}" }
                                            }
                                            div { class: "exchange-account-list-balance",
                                                span { {s(lang, "perp_balance")} }
                                                strong { "{item.perp_balance}" }
                                            }
                                        }
                                        div { class: "exchange-account-actions",
                                            button {
                                                class: "mini-button",
                                                onclick: move |_| {
                                                    editing_row_key.set(edit_row_key.clone());
                                                    form_exchange.set(edit_exchange.clone());
                                                    form_account.set(edit_account_id.clone());
                                                    form_namespace.set(edit_namespace.clone());
                                                    form_exchange_account_id.set(clean_display_value(&edit_exchange_account_id));
                                                    form_api_key.set(String::new());
                                                    form_api_secret.set(String::new());
                                                    form_passphrase.set(String::new());
                                                    saved_api_key_hint.set(saved_hint_for_field(&edit_fields, "api_key", lang));
                                                    saved_api_secret_hint.set(saved_hint_for_field(&edit_fields, "api_secret", lang));
                                                    saved_passphrase_hint.set(saved_hint_for_field(&edit_fields, "passphrase", lang));
                                                    message.set(format!("{edit_label} {}", edit_loaded_text(lang)));
                                                },
                                                {s(lang, "edit")}
                                            }
                                            button {
                                                class: "mini-button",
                                                disabled: test_busy() == test_row_key || !row_configured,
                                                onclick: move |_| {
                                                    let token_value = test_token.clone();
                                                    let exchange_value = exchange.clone();
                                                    let account_value = account_id.clone();
                                                    let namespace_value = namespace.clone();
                                                    let row_key_value = test_row_key.clone();
                                                    let label_value = label.clone();
                                                    test_busy.set(test_row_key.clone());
                                                    spawn(async move {
                                                        let body = json!({
                                                            "exchange": exchange_value,
                                                            "account_id": account_value,
                                                            "credential_namespace": namespace_value,
                                                        });
                                                        match test_exchange_api_keys(
                                                            &token_value,
                                                            &body,
                                                        )
                                                        .await
                                                        {
                                                            Ok(value) => {
                                                                let mut next = data();
                                                                if let Some(status) = value.get("status") {
                                                                    next.api_keys = status.clone();
                                                                }
                                                                data.set(next);
                                                                let ok = value.get("accepted").and_then(Value::as_bool).unwrap_or(false);
                                                                if ok {
                                                                    message.set(format!("{label_value} {}", s(lang, "connection_test_passed")));
                                                                } else {
                                                                    let error = value.get("check")
                                                                        .and_then(|check| check.get("error"))
                                                                        .and_then(Value::as_str)
                                                                        .unwrap_or("connection_test_failed");
                                                                    message.set(format!("{label_value} {}", error));
                                                                }
                                                            }
                                                            Err(error) => message.set(error),
                                                        }
                                                        if test_busy() == row_key_value {
                                                            test_busy.set(String::new());
                                                        }
                                                    });
                                                },
                                                if test_busy() == item.row_key {
                                                    {s(lang, "testing_connection")}
                                                } else {
                                                    {s(lang, "connection_test")}
                                                }
                                            }
                                            if item.last_error != "-" {
                                                small { class: "exchange-account-error", "{item.last_error}" }
                                            }
                                        }
                                        if editing_row_key() == item.row_key {
                                            div { class: "exchange-inline-editor",
                                                div { class: "credential-form-grid inline-credential-form",
                                                    if has_account_id {
                                                        label {
                                                            span { {s(lang, "exchange_account_id")} }
                                                            input {
                                                                value: "{form_exchange_account_id()}",
                                                                placeholder: "{credential_input_placeholder(lang, \"exchange_account_id\", &saved_hint_for_field(&item.fields, \"account_id\", lang))}",
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
                                                div { class: "row-actions inline-credential-actions",
                                                    button {
                                                        class: "mini-button primary",
                                                        disabled: save_busy(),
                                                        onclick: move |_| {
                                                            let token_value = save_token.clone();
                                                            let exchange = form_exchange();
                                                            let account_id = normalized_account_input(&form_account());
                                                            if !valid_account_identifier(&account_id) {
                                                                message.set(s(lang, "account_identifier_invalid"));
                                                                return;
                                                            }
                                                            let body = json!({
                                                                "exchange": exchange,
                                                                "account_id": account_id,
                                                                "credential_namespace": optional_text(form_namespace()),
                                                                "exchange_account_id": optional_text(form_exchange_account_id()),
                                                                "api_key": optional_text(form_api_key()),
                                                                "api_secret": optional_text(form_api_secret()),
                                                                "passphrase": optional_text(form_passphrase()),
                                                            });
                                                            let test_body = body.clone();
                                                            let label_value = save_label.clone();
                                                            let row_key_value = save_row_key.clone();
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
                                                                        form_api_key.set(String::new());
                                                                        form_api_secret.set(String::new());
                                                                        form_passphrase.set(String::new());
                                                                        test_busy.set(row_key_value.clone());
                                                                        match test_exchange_api_keys(&token_value, &test_body).await {
                                                                            Ok(test_value) => {
                                                                                let mut next = data();
                                                                                if let Some(status) = test_value.get("status") {
                                                                                    next.api_keys = status.clone();
                                                                                }
                                                                                data.set(next);
                                                                                let ok = test_value.get("accepted").and_then(Value::as_bool).unwrap_or(false);
                                                                                if ok {
                                                                                    editing_row_key.set(String::new());
                                                                                    message.set(format!("{label_value} {}", s(lang, "connection_test_passed")));
                                                                                } else {
                                                                                    let error = exchange_test_error_text(&test_value);
                                                                                    message.set(format!("{label_value} {}", connection_test_failed_text(lang, &error)));
                                                                                }
                                                                            }
                                                                            Err(error) => {
                                                                                message.set(format!("{label_value} {}", connection_test_failed_text(lang, &error)));
                                                                            }
                                                                        }
                                                                        if test_busy() == row_key_value {
                                                                            test_busy.set(String::new());
                                                                        }
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
                                                    button {
                                                        class: "mini-button danger",
                                                        disabled: test_busy() == clear_row_key || !row_configured,
                                                        onclick: move |_| {
                                                            let token_value = delete_token.clone();
                                                            let exchange_value = delete_exchange.clone();
                                                            let account_value = delete_account_id.clone();
                                                            let namespace_value = delete_namespace.clone();
                                                            let label_value = delete_label.clone();
                                                            let row_key_value = clear_row_key.clone();
                                                            test_busy.set(clear_row_key.clone());
                                                            spawn(async move {
                                                                let body = json!({
                                                                    "exchange": exchange_value,
                                                                    "account_id": account_value,
                                                                    "credential_namespace": namespace_value,
                                                                });
                                                                match delete_exchange_api_keys(&token_value, &body).await {
                                                                    Ok(value) => {
                                                                        let mut next = data();
                                                                        if let Some(status) = value.get("status") {
                                                                            next.api_keys = status.clone();
                                                                        }
                                                                        data.set(next);
                                                                        message.set(format!("{label_value} {}", s(lang, "api_keys_deleted")));
                                                                    }
                                                                    Err(error) => message.set(error),
                                                                }
                                                                if test_busy() == row_key_value {
                                                                    test_busy.set(String::new());
                                                                }
                                                            });
                                                        },
                                                        {s(lang, "delete")}
                                                    }
                                                    button {
                                                        class: "mini-button",
                                                        onclick: move |_| editing_row_key.set(String::new()),
                                                        {cancel_text(lang)}
                                                    }
                                                    span { class: "muted", {s(lang, "credential_secret_hint")} }
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

fn exchange_test_error_text(value: &Value) -> String {
    value
        .get("check")
        .and_then(|check| check.get("error"))
        .and_then(Value::as_str)
        .or_else(|| value.get("error").and_then(Value::as_str))
        .unwrap_or("connection_test_failed")
        .to_string()
}

fn connection_test_failed_text(lang: Language, error: &str) -> String {
    match lang {
        Language::Zh => format!("连接测试失败：{error}"),
        Language::En => format!("connection test failed: {error}"),
    }
}

fn optional_text(value: String) -> Option<String> {
    let value = value.trim().to_string();
    (!value.is_empty()).then_some(value)
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
    if value == "-" || value.starts_with('*') || value.contains("...") {
        String::new()
    } else {
        value.to_string()
    }
}

#[derive(Clone, Debug, PartialEq)]
struct CredentialExchangeListItem {
    row_key: String,
    exchange: String,
    list_label: String,
    account_id: String,
    namespace: String,
    exchange_account_id: String,
    fields: Vec<CredentialExchangeListField>,
    unified_balance: String,
    unified_account: bool,
    spot_balance: String,
    perp_balance: String,
    connection_status: String,
    last_tested_at: String,
    last_error: String,
    configured: bool,
    verified: bool,
}

impl CredentialExchangeListItem {
    fn from_account_row(row: &ExchangeCredentialAccountRow, inventory: &Value) -> Self {
        let (spot, perp, unified) = account_balances(&row.exchange, inventory);
        let spot = row.spot_total_usdt.unwrap_or(spot);
        let perp = row.perp_total_usdt.unwrap_or(perp);
        let unified = row.unified_total_usdt.unwrap_or(unified);
        let unified_account = row.unified_total_usdt.is_some() || unified > 0.0;
        let fields = row
            .fields
            .iter()
            .map(CredentialExchangeListField::from_account_field)
            .collect::<Vec<_>>();
        let configured = credential_fields_configured(&fields);
        let verified = configured && row.connection_status == "ok";
        let account_id = normalized_account_input(&row.account_id);
        let list_label = credential_list_label(&row.label, &account_id);
        Self {
            row_key: credential_row_key(&row.exchange, &account_id),
            exchange: row.exchange.clone(),
            list_label,
            account_id,
            namespace: credential_namespace_text(&row.credential_namespace, &row.exchange),
            exchange_account_id: row.exchange_account_id.clone(),
            fields,
            unified_balance: balance_text(row.unified_total_usdt, unified),
            unified_account,
            spot_balance: balance_text(row.spot_total_usdt, spot),
            perp_balance: balance_text(row.perp_total_usdt, perp),
            connection_status: row.connection_status.clone(),
            last_tested_at: row.last_tested_at.clone(),
            last_error: row.last_error.clone(),
            configured,
            verified,
        }
    }

    fn from_schema(
        schema: &ExchangeCredentialSchema,
        account_id: &str,
        namespace: &str,
        inventory: &Value,
    ) -> Self {
        let (spot, perp, unified) = account_balances(&schema.exchange, inventory);
        let fields = schema
            .fields
            .iter()
            .map(CredentialExchangeListField::from_schema_field)
            .collect::<Vec<_>>();
        let account_id = normalized_account_input(account_id);
        Self {
            row_key: credential_row_key(&schema.exchange, &account_id),
            exchange: schema.exchange.clone(),
            list_label: credential_list_label(&schema.label, &account_id),
            account_id,
            namespace: credential_namespace_text(namespace, &schema.exchange),
            exchange_account_id: String::new(),
            fields,
            unified_balance: balance_text(None, unified),
            unified_account: unified > 0.0,
            spot_balance: balance_text(None, spot),
            perp_balance: balance_text(None, perp),
            connection_status: "untested".to_string(),
            last_tested_at: "-".to_string(),
            last_error: "-".to_string(),
            configured: false,
            verified: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct CredentialExchangeListField {
    name: String,
    label: String,
    required: bool,
    configured: bool,
    display_value: String,
}

impl CredentialExchangeListField {
    fn from_account_field(field: &ExchangeCredentialAccountField) -> Self {
        Self {
            name: field.name.clone(),
            label: field.label.clone(),
            required: field.required,
            configured: field.configured,
            display_value: field.display_value.clone(),
        }
    }

    fn from_schema_field(field: &ExchangeCredentialField) -> Self {
        Self {
            name: field.name.clone(),
            label: field.label.clone(),
            required: field.required,
            configured: false,
            display_value: "-".to_string(),
        }
    }
}

fn credential_exchange_list(
    panel_data: &ExchangeCredentialPanelData,
    raw_status: &Value,
    inventory: &Value,
    lang: Language,
) -> Vec<CredentialExchangeListItem> {
    let mut items = Vec::new();
    for row in ExchangeCredentialAccountRow::from_rows(
        raw_status
            .get("supported_exchanges")
            .unwrap_or(&Value::Null),
        lang,
    ) {
        upsert_credential_item(
            &mut items,
            CredentialExchangeListItem::from_account_row(&row, inventory),
        );
    }
    for row in panel_data.exchange_rows.iter() {
        upsert_credential_item(
            &mut items,
            CredentialExchangeListItem::from_account_row(row, inventory),
        );
    }
    for account in panel_data
        .account_manager_rows
        .iter()
        .filter(|row| row.enabled)
    {
        if items.iter().any(|item| {
            item.exchange.eq_ignore_ascii_case(&account.exchange)
                && item.account_id.eq_ignore_ascii_case(&account.account_id)
        }) {
            continue;
        }
        if let Some(item) =
            credential_item_from_manager(account, &panel_data.supported_rows, inventory)
        {
            upsert_credential_item(&mut items, item);
        }
    }
    if items.is_empty() {
        for schema in panel_data.supported_rows.iter() {
            upsert_credential_item(
                &mut items,
                CredentialExchangeListItem::from_schema(
                    schema,
                    "default",
                    &schema.exchange.to_ascii_uppercase(),
                    inventory,
                ),
            );
        }
    }
    items.sort_by(|left, right| {
        right
            .verified
            .cmp(&left.verified)
            .then_with(|| right.configured.cmp(&left.configured))
            .then_with(|| {
                left.list_label
                    .to_lowercase()
                    .cmp(&right.list_label.to_lowercase())
            })
            .then_with(|| {
                left.account_id
                    .to_lowercase()
                    .cmp(&right.account_id.to_lowercase())
            })
    });
    items
}

fn upsert_credential_item(
    items: &mut Vec<CredentialExchangeListItem>,
    item: CredentialExchangeListItem,
) {
    if let Some(index) = items
        .iter()
        .position(|existing| existing.row_key.eq_ignore_ascii_case(&item.row_key))
    {
        items[index] = item;
    } else {
        items.push(item);
    }
}

fn credential_item_from_manager(
    account: &AccountManagerAccountRow,
    schemas: &[ExchangeCredentialSchema],
    inventory: &Value,
) -> Option<CredentialExchangeListItem> {
    let schema = schemas
        .iter()
        .find(|schema| schema.exchange.eq_ignore_ascii_case(&account.exchange))?;
    Some(CredentialExchangeListItem::from_schema(
        schema,
        &account.account_id,
        &account.credential_namespace,
        inventory,
    ))
}

fn credential_row_key(exchange: &str, account_id: &str) -> String {
    format!(
        "{}:{}",
        exchange.trim(),
        normalized_account_input(account_id)
    )
}

fn credential_namespace_text(namespace: &str, exchange: &str) -> String {
    let namespace = namespace.trim();
    if namespace.is_empty() || namespace == "-" {
        exchange.to_ascii_uppercase()
    } else {
        namespace.to_string()
    }
}

fn credential_list_label(label: &str, account_id: &str) -> String {
    if account_id.eq_ignore_ascii_case("default") {
        label.to_string()
    } else {
        format!("{label} / {account_id}")
    }
}

fn credential_account_meta(row: &CredentialExchangeListItem) -> String {
    if row.account_id.eq_ignore_ascii_case("default") {
        format!("标准账户 / {}", row.namespace)
    } else {
        format!("{} / {}", row.account_id, row.namespace)
    }
}

fn credential_fields_configured(fields: &[CredentialExchangeListField]) -> bool {
    if fields.is_empty() {
        return false;
    }
    let required_count = fields.iter().filter(|field| field.required).count();
    if required_count == 0 {
        return fields.iter().all(|field| field.configured);
    }
    fields
        .iter()
        .filter(|field| field.required)
        .all(|field| field.configured)
}

fn credential_list_has_field(item: &CredentialExchangeListItem, name: &str) -> bool {
    item.fields
        .iter()
        .any(|field| field.name.eq_ignore_ascii_case(name))
}

fn saved_hint_for_field(
    fields: &[CredentialExchangeListField],
    name: &str,
    lang: Language,
) -> String {
    let Some(field) = fields
        .iter()
        .find(|field| field.name.eq_ignore_ascii_case(name) && field.configured)
    else {
        return String::new();
    };
    let value = field.display_value.trim();
    if value.is_empty() || value == "-" || value == "configured" {
        s(lang, "configured")
    } else {
        value.to_string()
    }
}

fn credential_state_class(item: &CredentialExchangeListItem) -> &'static str {
    if item.verified {
        "status-pill good"
    } else if item.configured && item.connection_status == "error" {
        "status-pill bad"
    } else if item.configured {
        "status-pill warn"
    } else {
        "status-pill neutral"
    }
}

fn credential_state_label(lang: Language, item: &CredentialExchangeListItem) -> &'static str {
    match (
        lang,
        item.verified,
        item.configured,
        item.connection_status.as_str(),
    ) {
        (Language::Zh, true, _, _) => "已验证",
        (Language::Zh, _, true, "error") => "验证失败",
        (Language::Zh, _, true, _) => "已填入",
        (Language::Zh, _, false, _) => "未填入",
        (Language::En, true, _, _) => "Verified",
        (Language::En, _, true, "error") => "Verify failed",
        (Language::En, _, true, _) => "Configured",
        (Language::En, _, false, _) => "Missing",
    }
}

fn credential_field_chip_class(field: &CredentialExchangeListField) -> &'static str {
    if field.configured {
        "field-chip good"
    } else if field.required {
        "field-chip warn"
    } else {
        "field-chip"
    }
}

fn credential_list_row_class(item: &CredentialExchangeListItem, active: bool) -> String {
    let state = if item.verified {
        "verified"
    } else if item.configured && item.connection_status == "error" {
        "error"
    } else if item.configured {
        "configured"
    } else {
        "missing"
    };
    if active {
        format!("exchange-account-list-row credential-exchange-row {state} active")
    } else {
        format!("exchange-account-list-row credential-exchange-row {state}")
    }
}

fn exchange_list_title(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "交易所列表",
        Language::En => "Exchange List",
    }
}

fn exchange_list_subtitle(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "API key / 连接状态",
        Language::En => "API keys / connection status",
    }
}

fn credential_edit_title(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "双击修改 API key",
        Language::En => "Double-click to edit API keys",
    }
}

fn edit_loaded_text(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "已进入行内编辑。",
        Language::En => "opened for inline editing.",
    }
}

fn cancel_text(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "取消",
        Language::En => "Cancel",
    }
}

fn credential_list_count_text(lang: Language, count: usize) -> String {
    match lang {
        Language::Zh => format!("{count} 个"),
        Language::En => format!("{count} exchanges"),
    }
}

fn account_balances(exchange: &str, inventory: &Value) -> (f64, f64, f64) {
    let mut spot = 0.0;
    let mut perp = 0.0;
    let mut unified = 0.0;
    for row in as_array(inventory) {
        let row_exchange = text_at(&row, "exchange", Language::En);
        if !row_exchange.eq_ignore_ascii_case(exchange) {
            continue;
        }
        let value = row
            .get("valuation_usdt")
            .and_then(Value::as_f64)
            .or_else(|| {
                let asset = row.get("asset").and_then(Value::as_str)?;
                (asset.eq_ignore_ascii_case("USDT") || asset.eq_ignore_ascii_case("USDC"))
                    .then(|| row.get("total").and_then(Value::as_f64))
                    .flatten()
            })
            .unwrap_or_default();
        let market = text_at(&row, "market_type", Language::En).to_ascii_lowercase();
        if market.contains("unified") {
            unified += value;
        } else if market.contains("perp") || market.contains("future") || market.contains("swap") {
            perp += value;
        } else {
            spot += value;
        }
    }
    (spot, perp, unified)
}

fn balance_text(known: Option<f64>, fallback: f64) -> String {
    if let Some(value) = known {
        return money_text(value);
    }
    if fallback == 0.0 {
        "-".to_string()
    } else {
        money_text(fallback)
    }
}

fn money_text(value: f64) -> String {
    if value.abs() < 0.005 {
        "0.00 USDT".to_string()
    } else {
        format!("{value:.2} USDT")
    }
}

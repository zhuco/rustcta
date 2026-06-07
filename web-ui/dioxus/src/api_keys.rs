use dioxus::prelude::*;
use gloo_timers::future::TimeoutFuture;

use crate::api::{
    delete_exchange_credential_slot, fetch_exchange_api_keys, save_exchange_credential_draft,
};
use crate::i18n::{s, t};
use crate::types::{
    DashboardData, ExchangeCredentialPanelData, ExchangeCredentialSaveDraft,
    ExchangeCredentialSchema, Language,
};
use crate::ui::StatusPill;
use crate::utils::*;

#[component]
pub(crate) fn ApiKeysPanel(
    data: Signal<DashboardData>,
    token: String,
    mut message: Signal<String>,
    lang: Language,
    selected_exchange: String,
    selected_account: String,
) -> Element {
    let mut exchange = use_signal(move || selected_exchange.clone());
    let initial_account_id = selected_account.clone();
    let initial_account_label = selected_account.clone();
    let mut account_id = use_signal(move || initial_account_id.clone());
    let mut account_label = use_signal(move || initial_account_label.clone());
    let mut exchange_account_id = use_signal(String::new);
    let mut wallet_address = use_signal(String::new);
    let mut is_vault_address = use_signal(|| "false".to_string());
    let mut api_key = use_signal(String::new);
    let mut api_secret = use_signal(String::new);
    let mut passphrase = use_signal(String::new);
    let mut selected_account_key = use_signal(String::new);
    let api_keys = data().api_keys;
    let panel_data = ExchangeCredentialPanelData::from_status(&api_keys, lang);
    let active_exchange =
        ExchangeCredentialSchema::active_exchange(&panel_data.supported_rows, &exchange());
    let selected_schema =
        ExchangeCredentialSchema::selected(&panel_data.supported_rows, &active_exchange);
    let selected_label = selected_schema
        .as_ref()
        .map(|schema| schema.label.clone())
        .unwrap_or_else(|| active_exchange.clone());
    let needs_exchange_account_id = selected_schema
        .as_ref()
        .map(|schema| schema.field_exists("account_id"))
        .unwrap_or_default();
    let needs_wallet_address = selected_schema
        .as_ref()
        .map(|schema| schema.field_exists("wallet_address"))
        .unwrap_or_default();
    let needs_is_vault_address = selected_schema
        .as_ref()
        .map(|schema| schema.field_exists("is_vault_address"))
        .unwrap_or_default();
    let needs_api_key = selected_schema
        .as_ref()
        .map(|schema| schema.field_exists("api_key"))
        .unwrap_or_default();
    let needs_api_secret = selected_schema
        .as_ref()
        .map(|schema| schema.field_exists("api_secret"))
        .unwrap_or_default();
    let needs_passphrase = selected_schema
        .as_ref()
        .map(|schema| schema.field_exists("passphrase"))
        .unwrap_or_default();
    let exchange_account_label = selected_schema
        .as_ref()
        .map(|schema| schema.field_label("account_id", lang))
        .unwrap_or_else(|| s(lang, "exchange_account_id"));
    let wallet_address_label = selected_schema
        .as_ref()
        .map(|schema| schema.field_label("wallet_address", lang))
        .unwrap_or_else(|| s(lang, "exchange_account_id"));
    let is_vault_address_label = selected_schema
        .as_ref()
        .map(|schema| schema.field_label("is_vault_address", lang))
        .unwrap_or_else(|| s(lang, "exchange_account_id"));
    let refresh_token = token.clone();
    let mut refresh_data = data;
    let mut refresh_message = message;
    use_future(move || {
        let token_value = refresh_token.clone();
        async move {
            loop {
                if !token_value.is_empty() {
                    match fetch_exchange_api_keys(&token_value).await {
                        Ok(status) => {
                            let mut next = refresh_data();
                            next.api_keys = status;
                            refresh_data.set(next);
                        }
                        Err(error) => refresh_message.set(error),
                    }
                }
                TimeoutFuture::new(10_000).await;
            }
        }
    });
    let save_token = token.clone();
    let mut save_data = data;
    let save_exchange = active_exchange.clone();
    let save = move |_| {
        let token_value = save_token.clone();
        let exchange_value = save_exchange.clone();
        let account_label_value = account_label();
        let account_value = if selected_account_key().is_empty() {
            console_account_id_for_label(&account_label_value)
        } else {
            account_id()
        };
        let exchange_account_value = exchange_account_id();
        let wallet_address_value = wallet_address();
        let is_vault_address_value = is_vault_address();
        let key_value = api_key();
        let secret_value = api_secret();
        let passphrase_value = passphrase();
        wasm_bindgen_futures::spawn_local(async move {
            let draft = ExchangeCredentialSaveDraft {
                exchange: exchange_value,
                account_id: account_value,
                account_label: account_label_value,
                exchange_account_id: exchange_account_value,
                wallet_address: wallet_address_value,
                is_vault_address: is_vault_address_value,
                api_key: key_value,
                api_secret: secret_value,
                passphrase: passphrase_value,
            };
            match save_exchange_credential_draft(&token_value, draft).await {
                Ok(update) => {
                    if !update.status.is_null() {
                        let mut next = save_data();
                        next.api_keys = update.status;
                        save_data.set(next);
                    }
                    message.set(t(lang, "api_keys_saved").to_string());
                }
                Err(error) => message.set(error),
            }
        });
    };

    rsx! {
        section { id: "api-keys", class: "panel api-keys",
            div { class: "panel-title-row",
                div {
                    h2 { {s(lang, "api_key_management")} }
                    p { class: "muted", {s(lang, "api_key_note")} }
                }
                div { class: "row-actions",
                    if panel_data.restart_required {
                        span { class: "pill warn", {s(lang, "restart_required")} }
                    }
                    span { class: "pill neutral", "{panel_data.store_path}" }
                }
            }
            div { class: "runtime-strip api-key-summary-strip",
                div { span { {s(lang, "enabled_exchanges")} } strong { "{panel_data.enabled_exchange_text}" } }
                div { span { {s(lang, "supported_exchanges")} } strong { "{panel_data.supported_rows.len()}" } }
                div { span { {s(lang, "connected_exchange_accounts")} } strong { "{panel_data.exchange_rows.len()}" } }
                div { span { {s(lang, "configured")} } strong { "{panel_data.configured_account_count}" } }
            }
            div { class: "api-key-section api-key-entry-section",
                div { class: "api-key-section-title",
                    h3 { {s(lang, "add_exchange_section")} }
                    span { class: "muted", {s(lang, "add_exchange_hint")} }
                }
                div { class: "api-key-form",
                    label { class: "form-field",
                        span { {s(lang, "exchange")} }
                        select {
                            value: "{active_exchange}",
                            onchange: move |event| {
                                selected_account_key.set(String::new());
                                account_id.set(String::new());
                                account_label.set(String::new());
                                exchange_account_id.set(String::new());
                                wallet_address.set(String::new());
                                is_vault_address.set("false".to_string());
                                api_key.set(String::new());
                                api_secret.set(String::new());
                                passphrase.set(String::new());
                                exchange.set(event.value());
                            },
                            for item in panel_data.supported_rows.iter() {
                                option {
                                    value: "{item.exchange}",
                                    "{item.label}"
                                }
                            }
                        }
                    }
                    label { class: "form-field",
                        span { {s(lang, "account_name")} }
                        input {
                            value: "{account_label()}",
                            placeholder: s(lang, "account_placeholder"),
                            oninput: move |event| {
                                selected_account_key.set(String::new());
                                account_id.set(String::new());
                                account_label.set(event.value());
                            }
                        }
                    }
                    if needs_exchange_account_id {
                        label { class: "form-field",
                            span { "{exchange_account_label}" }
                            input {
                                value: "{exchange_account_id()}",
                                placeholder: s(lang, "exchange_account_id_placeholder"),
                                oninput: move |event| exchange_account_id.set(event.value())
                            }
                        }
                    }
                    if needs_wallet_address {
                        label { class: "form-field",
                            span { "{wallet_address_label}" }
                            input {
                                value: "{wallet_address()}",
                                placeholder: s(lang, "hyperliquid_wallet_placeholder"),
                                oninput: move |event| wallet_address.set(event.value())
                            }
                        }
                    }
                    if needs_is_vault_address {
                        label { class: "form-field",
                            span { "{is_vault_address_label}" }
                            select {
                                value: "{is_vault_address()}",
                                onchange: move |event| is_vault_address.set(event.value()),
                                option { value: "false", "false" }
                                option { value: "true", "true" }
                            }
                        }
                    }
                    if needs_api_key {
                        label { class: "form-field",
                            span { {s(lang, "api_key")} }
                            input {
                                r#type: "password",
                                value: "{api_key()}",
                                placeholder: format!("{} {}", selected_label, s(lang, "api_key")),
                                oninput: move |event| api_key.set(event.value())
                            }
                        }
                    }
                    if needs_api_secret {
                        label { class: "form-field",
                            span { {s(lang, "api_secret")} }
                            input {
                                r#type: "password",
                                value: "{api_secret()}",
                                placeholder: format!("{} {}", selected_label, s(lang, "api_secret")),
                                oninput: move |event| api_secret.set(event.value())
                            }
                        }
                    }
                    if needs_passphrase {
                        label { class: "form-field",
                            span { {s(lang, "passphrase")} }
                            input {
                                r#type: "password",
                                value: "{passphrase()}",
                                placeholder: format!("{} {}", selected_label, s(lang, "passphrase")),
                                oninput: move |event| passphrase.set(event.value())
                            }
                        }
                    }
                    div { class: "actions",
                        button {
                            class: "button primary",
                            onclick: save,
                            if selected_account_key().is_empty() {
                                {s(lang, "add_exchange")}
                            } else {
                                {s(lang, "update_exchange")}
                            }
                        }
                    }
                }
            }
            div { class: "api-key-section supported-exchanges",
                h3 { {s(lang, "supported_exchanges")} }
                div { class: "table-wrap compact-table",
                    table {
                        thead { tr {
                            th { {s(lang, "exchange")} }
                            th { {s(lang, "fields")} }
                            th { {s(lang, "actions")} }
                        } }
                        tbody {
                            for item in panel_data.supported_rows.iter() {
                                {
                                    let row_exchange_for_select = item.exchange.clone();
                                    let row_fields = item.fields.clone();
                                    rsx! {
                                        tr {
                                            td { "{item.label}" }
                                            td {
                                                for field in row_fields.iter() {
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
                                                button {
                                                    class: "mini-button primary",
                                                    onclick: move |_| {
                                                        exchange.set(row_exchange_for_select.clone());
                                                        account_id.set("default".to_string());
                                                        account_label.set(String::new());
                                                        exchange_account_id.set(String::new());
                                                        wallet_address.set(String::new());
                                                        is_vault_address.set("false".to_string());
                                                        api_key.set(String::new());
                                                        api_secret.set(String::new());
                                                        passphrase.set(String::new());
                                                        selected_account_key.set(String::new());
                                                        message.set(t(lang, "api_keys_loaded_for_edit").to_string());
                                                    },
                                                    {s(lang, "configure_credentials")}
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
                                        let row_key = row.row_key.clone();
                                        let row_fields = row.fields.clone();
                                        let row_exchange_for_load = row.exchange.clone();
                                        let row_account_for_load = row.account_id.clone();
                                        let row_account_label_for_load = row.account_label.clone();
                                        let row_key_for_load = row_key.clone();
                                        let row_exchange_account_id = row.exchange_account_id.clone();
                                        let row_wallet_address = row.wallet_address.clone();
                                        let row_is_vault_address = row.is_vault_address.clone();
                                        let delete_token = token.clone();
                                        let mut delete_data = data;
                                        let mut row_message = message;
                                        let row_exchange_for_delete = row.exchange.clone();
                                        let row_account_for_delete = row.account_id.clone();
                                        rsx! {
                                            tr {
                                                class: if selected_account_key() == row_key { "clickable-row active" } else { "clickable-row" },
                                                onclick: move |_| {
                                                    exchange.set(row_exchange_for_load.clone());
                                                    account_id.set(row_account_for_load.clone());
                                                    account_label.set(row_account_label_for_load.clone());
                                                    exchange_account_id.set(row_exchange_account_id.clone());
                                                    wallet_address.set(row_wallet_address.clone());
                                                    is_vault_address.set(if row_is_vault_address.trim().eq_ignore_ascii_case("true") { "true".to_string() } else { "false".to_string() });
                                                    api_key.set(String::new());
                                                    api_secret.set(String::new());
                                                    passphrase.set(String::new());
                                                    selected_account_key.set(row_key_for_load.clone());
                                                    message.set(t(lang, "api_keys_loaded_for_edit").to_string());
                                                },
                                                td { "{row.label}" }
                                                td { StatusPill { value: row.enabled, lang } }
                                                td {
                                                    span { class: if row.default_account { "pill neutral" } else { "pill" }, "{row.account_label}" }
                                                }
                                                td {
                                                    for field in row_fields.iter() {
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
                                                    for field in row_fields.iter() {
                                                        span { class: "field-chip secret-chip", "{field.display_value}" }
                                                    }
                                                }
                                                td {
                                                    for field in row_fields.iter() {
                                                        span { class: "field-chip", "{field.source}" }
                                                    }
                                                }
                                                td {
                                                    for field in row_fields.iter() {
                                                        StatusPill { value: field.configured, lang }
                                                    }
                                                }
                                                td {
                                                    button {
                                                        class: "button danger",
                                                        onclick: move |event| {
                                                            event.stop_propagation();
                                                            let token_value = delete_token.clone();
                                                            let exchange_value = row_exchange_for_delete.clone();
                                                            let account_value = row_account_for_delete.clone();
                                                            wasm_bindgen_futures::spawn_local(async move {
                                                                match delete_exchange_credential_slot(&token_value, &exchange_value, &account_value).await {
                                                                    Ok(update) => {
                                                                        if !update.status.is_null() {
                                                                            let mut next = delete_data();
                                                                            next.api_keys = update.status;
                                                                            delete_data.set(next);
                                                                        }
                                                                        row_message.set(t(lang, "api_keys_deleted").to_string());
                                                                    }
                                                                    Err(error) => row_message.set(error),
                                                                }
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

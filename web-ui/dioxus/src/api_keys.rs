use dioxus::prelude::*;
use gloo_timers::future::TimeoutFuture;

use crate::api::fetch_credential_status;
use crate::i18n::s;
use crate::types::{CredentialStatusRowData, DashboardData, ExchangeCredentialPanelData, Language};
use crate::ui::StatusPill;

#[component]
pub(crate) fn ApiKeysPanel(
    data: Signal<DashboardData>,
    token: String,
    mut message: Signal<String>,
    lang: Language,
    selected_exchange: String,
    selected_account: String,
) -> Element {
    let credentials_status = data().credentials_status;
    let legacy_status = data().api_keys;
    let status_rows = CredentialStatusRowData::from_response(&credentials_status, lang);
    let panel_data = ExchangeCredentialPanelData::from_status(&legacy_status, lang);
    let selected_slot = selected_credential_slot(&selected_exchange, &selected_account);
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
                    span { class: "pill neutral", {s(lang, "credential_status_only")} }
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
                    h3 { {s(lang, "credential_setup_state")} }
                    span { class: "muted", "{selected_slot}" }
                }
                p { class: "muted", {s(lang, "credential_setup_hint")} }
                div { class: "credential-setup-grid",
                    div { span { {s(lang, "exchange")} } strong { "{selected_exchange}" } }
                    div { span { {s(lang, "account")} } strong { "{selected_account}" } }
                    div { span { {s(lang, "data_source")} } strong { {credential_source_label(&credentials_status, &legacy_status, lang)} } }
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
                            } }
                            tbody {
                                for row in panel_data.exchange_rows.iter() {
                                    tr {
                                        td { "{row.label}" }
                                        td { StatusPill { value: row.enabled, lang } }
                                        td {
                                            span { class: if row.default_account { "pill neutral" } else { "pill" }, "{row.account_label}" }
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

fn credential_source_label(
    credentials_status: &serde_json::Value,
    legacy_status: &serde_json::Value,
    lang: Language,
) -> String {
    if credentials_status.get("slots").is_some() {
        "/api/credentials/status".to_string()
    } else if legacy_status.get("exchanges").is_some() {
        s(lang, "legacy_credential_status")
    } else {
        "-".to_string()
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

use dioxus::prelude::*;
use serde_json::{json, Value};

use crate::api::run_exchange_latency_test;
use crate::i18n::{s, t};
use crate::types::{ExchangeLatencyTestData, Language};
use crate::utils::{as_array, text_at};

#[component]
pub(crate) fn ExchangeLatencyPanel(
    cross_arb: Value,
    token: String,
    mut message: Signal<String>,
    lang: Language,
) -> Element {
    let mut latency_mode = use_signal(|| "batched".to_string());
    let mut latency_batch_size = use_signal(|| "5".to_string());
    let mut latency_timeout_ms = use_signal(|| "3000".to_string());
    let mut latency_running = use_signal(|| false);
    let mut latency_result = use_signal(|| Value::Null);
    let exchanges = exchange_names(&cross_arb);
    let gateway_endpoint_rows = gateway_latency_endpoint_rows(&cross_arb, &exchanges);
    let run_latency_token = token.clone();
    let run_latency_test = move |_| {
        if latency_running() {
            return;
        }
        let token_value = run_latency_token.clone();
        let mode = latency_mode();
        let batch_size = parse_usize_or_default(&latency_batch_size(), 5).clamp(1, 50);
        let timeout_ms = parse_u64_or_default(&latency_timeout_ms(), 3_000).clamp(500, 15_000);
        let endpoint_rows = gateway_endpoint_rows.clone();
        latency_running.set(true);
        message.set(t(lang, "exchange_latency_running").to_string());
        wasm_bindgen_futures::spawn_local(async move {
            match run_exchange_latency_test(
                &token_value,
                &mode,
                batch_size,
                timeout_ms,
                Value::Array(endpoint_rows),
            )
            .await
            {
                Ok(value) => {
                    latency_result.set(value);
                    message.set(t(lang, "exchange_latency_finished").to_string());
                }
                Err(error) => message.set(error),
            }
            latency_running.set(false);
        });
    };

    rsx! {
        section { id: "exchange-latency", class: "exchange-latency-page",
            div { class: "section-title",
                div {
                    h2 { {s(lang, "exchange_latency_test")} }
                    p { class: "muted", {s(lang, "exchange_latency_note")} }
                }
            }
            div { class: "panel exchange-latency-panel",
                div { class: "panel-title-row",
                    div { h2 { {s(lang, "exchange_latency_test")} } }
                    div { class: "row-actions latency-actions",
                        label { class: "compact-field",
                            span { {s(lang, "test_mode")} }
                            select {
                                value: "{latency_mode()}",
                                onchange: move |event| latency_mode.set(event.value()),
                                option { value: "batched", {s(lang, "test_batched")} }
                                option { value: "all_concurrent", {s(lang, "test_all_concurrent")} }
                            }
                        }
                        label { class: "compact-field numeric-compact-field",
                            span { {s(lang, "batch_size")} }
                            input {
                                value: "{latency_batch_size()}",
                                oninput: move |event| latency_batch_size.set(event.value())
                            }
                        }
                        label { class: "compact-field numeric-compact-field",
                            span { {s(lang, "timeout_ms")} }
                            input {
                                value: "{latency_timeout_ms()}",
                                oninput: move |event| latency_timeout_ms.set(event.value())
                            }
                        }
                        button {
                            class: "button primary",
                            disabled: "{latency_running()}",
                            onclick: run_latency_test,
                            if latency_running() {
                                {s(lang, "testing")}
                            } else {
                                {s(lang, "run_latency_test")}
                            }
                        }
                    }
                }
                if latency_result().is_null() {
                    div { class: "empty-state", {s(lang, "exchange_latency_empty")} }
                } else {
                    {
                        let latency_data = ExchangeLatencyTestData::from_value(&latency_result(), lang);
                        rsx! {
                            div { class: "runtime-strip latency-summary-strip",
                                div { span { {s(lang, "exchange")} } strong { "{latency_data.exchange_count}" } }
                                div { span { {s(lang, "test_mode")} } strong { "{latency_data.mode}" } }
                                div { span { {s(lang, "batch_size")} } strong { "{latency_data.batch_size}" } }
                                div { span { {s(lang, "timeout_ms")} } strong { "{latency_data.timeout_ms}" } }
                                div { span { {s(lang, "elapsed_ms")} } strong { "{latency_data.elapsed_ms}" } }
                            }
                            div { class: "table-wrap compact-table latency-table no-wrap-table",
                                table {
                                    thead { tr {
                                        th { {s(lang, "exchange")} }
                                        th { {s(lang, "endpoint_source")} }
                                        th { "REST" }
                                        th { {s(lang, "latency")} }
                                        th { "WebSocket" }
                                        th { {s(lang, "latency")} }
                                        th { {s(lang, "details")} }
                                    } }
                                    tbody {
                                        for row in latency_data.rows.iter() {
                                            tr {
                                                td { "{row.label}" }
                                                td { "{row.endpoint_source}" }
                                                td { span { class: latency_status_class(&row.rest_status), "{row.rest_status}" } }
                                                td { "{row.rest_latency_ms}" }
                                                td { span { class: latency_status_class(&row.ws_status), "{row.ws_status}" } }
                                                td { "{row.ws_latency_ms}" }
                                                td { class: "reason-cell latency-detail-cell",
                                                    div { "{row.rest_endpoint}" }
                                                    if !row.rest_error.is_empty() {
                                                        div { class: "muted", "REST: {row.rest_error}" }
                                                    }
                                                    div { "{row.ws_endpoint}" }
                                                    if !row.ws_error.is_empty() {
                                                        div { class: "muted", "WS: {row.ws_error}" }
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

fn latency_status_class(status: &str) -> &'static str {
    match status {
        "ok" => "pill",
        "timeout" => "pill warn",
        _ => "pill bad",
    }
}

fn parse_usize_or_default(value: &str, default: usize) -> usize {
    value
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn parse_u64_or_default(value: &str, default: u64) -> u64 {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn exchange_names(cross_arb: &Value) -> Vec<String> {
    let dashboard = cross_arb
        .get("cross_arb_dashboard")
        .or_else(|| cross_arb.get("data"))
        .unwrap_or(cross_arb);
    as_array(dashboard.get("exchange_status").unwrap_or(&Value::Null))
        .iter()
        .filter_map(|row| {
            row.get("exchange")
                .or_else(|| row.get("exchange_id"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .collect()
}

fn gateway_latency_endpoint_rows(cross_arb: &Value, exchanges: &[String]) -> Vec<Value> {
    let dashboard = cross_arb
        .get("cross_arb_dashboard")
        .or_else(|| cross_arb.get("data"))
        .unwrap_or(cross_arb);
    let mut rows = Vec::new();
    for source in [
        dashboard.get("exchange_status"),
        dashboard.get("account_console"),
        dashboard.get("instruments"),
    ]
    .into_iter()
    .flatten()
    {
        for row in as_array(source) {
            if let Some(exchange) = row
                .get("exchange")
                .or_else(|| row.get("exchange_id"))
                .and_then(Value::as_str)
            {
                let rest_url = first_text_field(
                    &row,
                    &[
                        "rest_url",
                        "rest_base_url",
                        "public_rest_url",
                        "private_rest_base_url",
                    ],
                );
                let ws_url = first_text_field(
                    &row,
                    &["ws_url", "websocket_url", "public_ws_url", "private_ws_url"],
                );
                if rest_url.is_some() || ws_url.is_some() {
                    rows.push(json!({
                        "exchange": exchange,
                        "rest_url": rest_url.unwrap_or_default(),
                        "ws_url": ws_url.unwrap_or_default(),
                    }));
                }
            }
        }
    }
    if rows.is_empty() {
        rows.extend(
            exchanges
                .iter()
                .map(|exchange| json!({ "exchange": exchange })),
        );
    }
    rows
}

fn first_text_field(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty() && *text != "-")
            .map(ToString::to_string)
            .or_else(|| {
                let text = text_at(value, field, Language::En);
                (!text.is_empty() && text != "-").then_some(text)
            })
    })
}

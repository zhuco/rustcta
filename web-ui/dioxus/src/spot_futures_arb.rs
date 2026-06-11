use dioxus::prelude::*;
use serde_json::{json, Value};

use crate::api::send_strategy_command;
use crate::i18n::{s, t};
use crate::types::Language;
use crate::ui::Metric;
use crate::utils::{as_array, compact, text_at};

const STRATEGY_ID: &str = "spot_futures_arb_live";

#[component]
pub(crate) fn SpotFuturesArbPanel(
    spot_futures_arb: Value,
    processes: Value,
    token: String,
    mut message: Signal<String>,
    lang: Language,
) -> Element {
    let settings = spot_futures_arb
        .get("settings")
        .cloned()
        .unwrap_or(Value::Null);
    let market = settings.get("market").cloned().unwrap_or(Value::Null);
    let universe = settings.get("universe").cloned().unwrap_or(Value::Null);
    let selection = settings.get("selection").cloned().unwrap_or(Value::Null);
    let funding = settings.get("funding").cloned().unwrap_or(Value::Null);
    let execution = settings.get("execution").cloned().unwrap_or(Value::Null);
    let sizing = settings.get("sizing").cloned().unwrap_or(Value::Null);
    let risk = settings.get("risk").cloned().unwrap_or(Value::Null);
    let contract = spot_futures_arb
        .get("runtime_contract")
        .cloned()
        .unwrap_or(Value::Null);
    let market_scan = spot_futures_arb
        .get("market_scan")
        .cloned()
        .unwrap_or(Value::Null);
    let preflight = spot_futures_arb
        .get("preflight")
        .cloned()
        .unwrap_or(Value::Null);
    let reconcile = spot_futures_arb
        .get("reconcile")
        .cloned()
        .unwrap_or(Value::Null);
    let execution_cycle = spot_futures_arb
        .get("execution_cycle")
        .cloned()
        .unwrap_or(Value::Null);
    let dashboard = contract
        .get("dashboard_snapshot")
        .cloned()
        .unwrap_or(Value::Null);
    let active_symbols = string_array(
        spot_futures_arb
            .get("active_symbols")
            .or_else(|| dashboard.get("active_symbols")),
    );
    let excluded_bases = string_array(universe.get("excluded_bases"));
    let spot_exchanges = string_array(universe.get("enabled_spot_exchanges"));
    let perp_exchanges = string_array(universe.get("enabled_perp_exchanges"));
    let process = spot_futures_process(&processes);
    let process_status = text_at(&process, "status", lang);
    let process_id = text_at(&process, "process_id", lang);
    let restart_count = text_at(&process, "restart_count", lang);

    rsx! {
        section { id: "spot-futures-arb", class: "cross-arb",
            div { class: "section-title",
                div {
                    h2 { {s(lang, "spot_futures_arb_title")} }
                    p { class: "muted", {s(lang, "spot_futures_arb_subtitle")} }
                }
                div { class: "row-actions",
                    button { class: "button", onclick: lifecycle_click("pause", token.clone(), message, lang), {s(lang, "pause")} }
                    button { class: "button", onclick: lifecycle_click("resume", token.clone(), message, lang), {s(lang, "resume")} }
                    button { class: "button danger", onclick: lifecycle_click("close_only", token.clone(), message, lang), "Close-only" }
                }
            }

            div { class: "grid cross-metrics",
                Metric { label: s(lang, "workspace_strategy_status"), value: process_status }
                Metric { label: s(lang, "workspace_process_id"), value: process_id }
                Metric { label: s(lang, "workspace_restart_count"), value: restart_count }
                Metric { label: s(lang, "mode"), value: text_at(&settings, "mode", lang) }
                Metric { label: s(lang, "per_arb_notional"), value: format!("{} USDT", text_at(&sizing, "target_notional_usdt", lang)) }
                Metric { label: "Min net edge".to_string(), value: format!("{} bps", text_at(&selection, "min_open_net_edge_bps", lang)) }
            }

            div { class: "grid two-columns",
                div { class: "panel",
                    h2 { "Live Preflight" }
                    dl { class: "definition-list",
                        dt { "Ready for live" } dd { {text_at(&preflight, "ready_for_live", lang)} }
                        dt { "Blocks live" } dd { {text_at(&preflight, "blocks_live", lang)} }
                        dt { "Exchanges" } dd { {text_at(&preflight, "exchange_count", lang)} }
                        dt { "Checked at" } dd { {text_at(&preflight, "checked_at", lang)} }
                        dt { "Missing" } dd { {join_json_strings(preflight.get("missing_requirements"))} }
                    }
                }
                div { class: "panel",
                    h2 { "Market Scan" }
                    dl { class: "definition-list",
                        dt { "Routes" } dd { {text_at(&market_scan, "route_count", lang)} }
                        dt { "Accepted" } dd { {text_at(&market_scan, "accepted_count", lang)} }
                        dt { "Rejected" } dd { {text_at(&market_scan, "rejected_count", lang)} }
                        dt { "Errors" } dd { {text_at(&market_scan, "error_count", lang)} }
                        dt { "Scanned at" } dd { {text_at(&market_scan, "scanned_at", lang)} }
                    }
                }
            }

            div { class: "grid two-columns",
                div { class: "panel",
                    h2 { "Execution Cycle" }
                    dl { class: "definition-list",
                        dt { "Mode" } dd { {text_at(&execution_cycle, "mode", lang)} }
                        dt { "Blocked" } dd { {text_at(&execution_cycle, "blocked_reason", lang)} }
                        dt { "Candidates" } dd { {text_at(&execution_cycle, "candidate_count", lang)} }
                        dt { "Planned" } dd { {text_at(&execution_cycle, "planned_count", lang)} }
                        dt { "Submitted" } dd { {text_at(&execution_cycle, "submitted_count", lang)} }
                        dt { "Order accepts" } dd { {text_at(&execution_cycle, "accepted_count", lang)} }
                        dt { "Order rejects" } dd { {text_at(&execution_cycle, "rejected_count", lang)} }
                        dt { "Readback events" } dd { {as_array(execution_cycle.get("readback_events").unwrap_or(&Value::Null)).len().to_string()} }
                    }
                }
                div { class: "panel warning-panel",
                    h2 { {s(lang, "spot_futures_arb_note")} }
                    dl { class: "definition-list",
                        dt { "Strategy" } dd { "strategy_kind=spot_futures_arbitrage; strategy_id={STRATEGY_ID}" }
                        dt { "Live orders" } dd { {text_at(&execution_cycle, "live_orders_enabled", lang)} }
                        dt { "Reconcile blocks" } dd { {text_at(&reconcile, "blocks_new_entries", lang)} }
                        dt { "Spot residual" } dd { {format!("{} USDT", text_at(&reconcile, "residual_spot_total_notional_usdt", lang))} }
                        dt { "Perp residual" } dd { {format!("{} USDT", text_at(&reconcile, "residual_perp_total_notional_usdt", lang))} }
                        dt { "Reconcile errors" } dd { {join_json_strings(reconcile.get("errors"))} }
                    }
                }
            }

            div { class: "grid two-columns",
                div { class: "panel",
                    h2 { "Universe" }
                    dl { class: "definition-list",
                        dt { "Quote" } dd { {text_at(&market, "quote_asset", lang)} }
                        dt { "Spot exchanges" } dd { {join_or_dash(&spot_exchanges)} }
                        dt { "Perp exchanges" } dd { {join_or_dash(&perp_exchanges)} }
                        dt { "Excluded bases" } dd { {join_or_dash(&excluded_bases)} }
                        dt { "Active symbols" } dd { {join_or_dash(&active_symbols)} }
                    }
                }
                div { class: "panel",
                    h2 { "Funding / Edge" }
                    dl { class: "definition-list",
                        dt { "Funding enabled" } dd { {text_at(&funding, "enabled", lang)} }
                        dt { "Expected holding hours" } dd { {text_at(&funding, "expected_holding_hours", lang)} }
                        dt { "Funding max age ms" } dd { {text_at(&funding, "max_funding_snapshot_age_ms", lang)} }
                        dt { "No-open adverse mins" } dd { {text_at(&funding, "no_open_before_adverse_funding_mins", lang)} }
                        dt { "Min funding bps" } dd { {text_at(&funding, "min_expected_funding_bps", lang)} }
                    }
                }
            }

            div { class: "grid two-columns",
                div { class: "panel",
                    h2 { "Execution" }
                    dl { class: "definition-list",
                        dt { "Open style" } dd { "spot_maker_then_perp_taker" }
                        dt { "Live submit" } dd { {text_at(&execution, "allow_live_order_submission", lang)} }
                        dt { "Readback hedge" } dd { {text_at(&execution, "enable_spot_fill_readback_hedge", lang)} }
                        dt { "Spot maker TTL" } dd { {format!("{} ms", text_at(&execution, "spot_maker_order_ttl_ms", lang))} }
                        dt { "Post-only required" } dd { {text_at(&execution, "spot_post_only_required", lang)} }
                        dt { "Perp hedge slippage" } dd { {text_at(&execution, "perp_hedge_slippage_pct", lang)} }
                        dt { "Close style" } dd { "dual_taker_reduce_only" }
                    }
                }
                div { class: "panel",
                    h2 { "Risk" }
                    dl { class: "definition-list",
                        dt { "Start paused" } dd { {text_at(&risk, "start_paused_new_entries", lang)} }
                        dt { "Close only" } dd { {text_at(&risk, "start_close_only", lang)} }
                        dt { "Max bundles" } dd { {text_at(&risk, "max_open_bundles", lang)} }
                        dt { "Max total notional" } dd { {format!("{} USDT", text_at(&risk, "max_total_notional_usdt", lang))} }
                        dt { "Max unhedged spot" } dd { {format!("{} USDT", text_at(&risk, "max_unhedged_spot_notional_usdt", lang))} }
                    }
                }
            }

            div { class: "panel",
                h2 { "Runtime Contract" }
                div { class: "table-wrap",
                    table {
                        thead { tr { th { "Task" } th { "Provider" } th { "Dashboard" } } }
                        tbody {
                            for task in as_array(contract.get("tasks").unwrap_or(&Value::Null)) {
                                tr {
                                    td { {text_at(&task, "task_kind", lang)} }
                                    td { {text_at(&task, "provider_boundary", lang)} }
                                    td { {text_at(&task, "emits_dashboard_snapshot", lang)} }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn lifecycle_click(
    command: &'static str,
    token: String,
    mut message: Signal<String>,
    lang: Language,
) -> impl FnMut(Event<MouseData>) + 'static {
    move |_| {
        let token_value = token.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let body = json!({
                "schema_version": 1,
                "command_kind": command,
                "payload": {},
            });
            match send_strategy_command(&token_value, STRATEGY_ID, &body).await {
                Ok(value) => message.set(format!(
                    "{} {}: {}",
                    command,
                    t(lang, "queued"),
                    compact(&value)
                )),
                Err(error) => message.set(error),
            }
        });
    }
}

fn spot_futures_process(processes: &Value) -> Value {
    as_array(processes)
        .into_iter()
        .find(|process| {
            text_at(process, "strategy_id", Language::En) == STRATEGY_ID
                || text_at(process, "strategy_kind", Language::En) == "spot_futures_arbitrage"
        })
        .unwrap_or(Value::Null)
}

fn string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn join_or_dash(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(", ")
    }
}

fn join_json_strings(value: Option<&Value>) -> String {
    let values = string_array(value);
    join_or_dash(&values)
}

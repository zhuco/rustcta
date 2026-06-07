use dioxus::prelude::*;

use crate::i18n::s;
use crate::types::{
    BalanceSummary, BalanceTrendData, DashboardData, ExchangeConnectionChipData, Language,
    RuntimeTimingRow, StrategyRuntimeOverviewData,
};
use crate::ui::StatusPill;
use crate::utils::*;

#[component]
pub(crate) fn Overview(data: DashboardData, lang: Language) -> Element {
    let balances = balance_summaries(&data.inventory);
    let timing_rows = runtime_timing_rows(&data.runtime_publisher, &data.exchanges, lang);
    let strategy_runtime = StrategyRuntimeOverviewData::from_values(
        &data.status,
        &data.runtime_publisher,
        &data.spot_arb,
        lang,
    );
    let exchange_chips = ExchangeConnectionChipData::from_rows(&data.exchanges, lang);
    rsx! {
        section { id: "overview", class: "overview",
            div { class: "two overview-main",
                StrategyRuntimePanel {
                    data: strategy_runtime,
                    lang,
                }
            }
            div { class: "two overview-main",
                ExchangeBalancePanel { balances, exchanges: exchange_chips, lang }
                RuntimeTimingPanel { rows: timing_rows, lang }
            }
        }
    }
}

#[component]
pub(crate) fn BalanceTrendPanel(data: BalanceTrendData, lang: Language) -> Element {
    rsx! {
        div { class: "balance-trend",
            div { class: "panel-title-row",
                h2 { {s(lang, "strategy_profit_curve")} }
                span { class: profit_class(data.change), "{signed_usdt(data.change)}" }
            }
            div { class: "chart-frame",
                if data.is_empty() {
                    div { class: "empty-state", {s(lang, "no_balance_history")} }
                } else {
                    svg { class: "balance-chart", view_box: "0 0 640 180", preserve_aspect_ratio: "none",
                        polyline { points: "{data.chart_points}", fill: "none", stroke: "#1f7a8c", stroke_width: "4", stroke_linecap: "round", stroke_linejoin: "round" }
                        line { x1: "14", y1: "166", x2: "626", y2: "166", stroke: "#d8e0e8", stroke_width: "1" }
                    }
                }
            }
        }
    }
}

#[component]
fn StrategyRuntimePanel(data: StrategyRuntimeOverviewData, lang: Language) -> Element {
    rsx! {
        div { class: "panel strategy-panel",
            div { class: "panel-title-row",
                h2 { {s(lang, "running_strategies")} }
                span { class: "pill", "{data.active_strategy_count} {s(lang, \"active\")}" }
            }
            div { class: "strategy-list",
                div { class: "strategy-item",
                    div {
                        strong { "{data.strategy_name}" }
                        span { "{data.mode_text}" }
                    }
                    StatusPill { value: data.strategy_ready, lang }
                }
                div { class: "strategy-item",
                    div {
                        strong { {s(lang, "spot_arb_title")} }
                        span { "{data.selected_count} {s(lang, \"subscribed_pairs\")} / {data.candidate_count} {s(lang, \"candidate_pairs\")}" }
                    }
                    StatusPill { value: data.selected_count > 0, lang }
                }
            }
            div { class: "runtime-strip",
                div {
                    span { {s(lang, "runtime_publisher")} }
                    strong { "{data.runtime_running}" }
                }
                div {
                    span { {s(lang, "snapshots")} }
                    strong { "{data.snapshot_counts}" }
                }
                div {
                    span { {s(lang, "last_snapshot")} }
                    strong { "{data.last_snapshot_at}" }
                }
            }
        }
    }
}

#[component]
fn ExchangeBalancePanel(
    balances: Vec<BalanceSummary>,
    exchanges: Vec<ExchangeConnectionChipData>,
    lang: Language,
) -> Element {
    let max_total = balances
        .iter()
        .map(|row| row.total_usdt)
        .fold(0.0_f64, f64::max);
    rsx! {
        div { class: "panel",
            h2 { {s(lang, "exchange_balances")} }
            div { class: "balance-list",
                for row in balances.iter() {
                    div { class: "balance-row",
                        div { class: "balance-meta",
                            strong { "{row.exchange}" }
                            span { "{row.asset_count} {s(lang, \"assets\")}" }
                        }
                        div { class: "balance-bar",
                            span { style: "width: {percent_width(row.total_usdt, max_total)}%;" }
                        }
                        div { class: "balance-values",
                            strong { "{format_usdt(row.total_usdt)}" }
                            span { "{s(lang, \"available\")}: {format_usdt(row.available_usdt)}" }
                        }
                    }
                }
                if balances.is_empty() {
                    div { class: "empty-state", {s(lang, "no_inventory")} }
                }
            }
            div { class: "connection-strip",
                for row in exchanges.iter() {
                    span { class: if row.connected { "pill" } else { "pill bad" },
                        "{row.exchange}"
                    }
                }
            }
        }
    }
}

#[component]
fn RuntimeTimingPanel(rows: Vec<RuntimeTimingRow>, lang: Language) -> Element {
    rsx! {
        div { class: "panel",
            h2 { {s(lang, "thread_timing")} }
            div { class: "table-wrap compact-table",
                table {
                    thead { tr {
                        th { {s(lang, "exchange")} }
                        th { {s(lang, "latency")} }
                        th { {s(lang, "failures")} }
                        th { {s(lang, "rate_limits")} }
                        th { {s(lang, "last_success")} }
                        th { {s(lang, "backoff")} }
                    } }
                    tbody {
                        for row in rows.iter().take(12) {
                            tr {
                                td { "{row.exchange}" }
                                td { {row.latency_ms.map(format_ms).unwrap_or_else(|| "-".to_string())} }
                                td { "{row.request_failures}" }
                                td { "{row.rate_limit_events}" }
                                td { "{row.last_successful_request}" }
                                td { "{row.backoff_until}" }
                            }
                        }
                    }
                }
            }
            if rows.is_empty() {
                div { class: "empty-state", {s(lang, "no_runtime_timing")} }
            }
        }
    }
}

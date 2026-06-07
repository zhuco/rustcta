use dioxus::prelude::*;
use serde_json::Value;

use crate::i18n::s;
use crate::types::{
    BookSnapshotRowData, DebugJsonPanelData, DryRunPlanRowData, ExchangeHealthRowData,
    FeeScheduleRowData, InventoryAssetRowData, Language, RecentOpportunityRowData,
    RiskDebugPanelData, RiskPanelData, RuntimePublisherPanelData, SymbolOpportunityRowData,
    SymbolRuleRowData,
};
use crate::ui::StatusPill;

#[component]
pub(crate) fn ExchangePanel(exchanges: Value, lang: Language) -> Element {
    let rows = ExchangeHealthRowData::from_rows(&exchanges, lang);
    rsx! {
        section { id: "exchanges", class: "panel",
            h2 { {s(lang, "exchange_health")} }
            div { class: "table-wrap",
                table {
                    thead { tr {
                        th { {s(lang, "exchange")} } th { {s(lang, "connected")} } th { {s(lang, "fresh")} } th { {s(lang, "stale")} } th { {s(lang, "latency")} } th { {s(lang, "last_book")} }
                    } }
                    tbody {
                        for row in rows.iter() {
                            tr {
                                td { "{row.exchange}" }
                                td { StatusPill { value: row.connected, lang } }
                                td { "{row.fresh_symbol_count}" }
                                td { "{row.stale_symbol_count}" }
                                td { "{row.avg_latency_ms}" }
                                td { "{row.last_book_update_at}" }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
pub(crate) fn BookPanel(books: Value, lang: Language) -> Element {
    let rows = BookSnapshotRowData::from_rows(&books, lang);
    rsx! {
        section { id: "books", class: "panel",
            h2 { {s(lang, "order_books")} }
            div { class: "table-wrap",
                table {
                    thead { tr {
                        th { {s(lang, "exchange")} } th { {s(lang, "symbol")} } th { {s(lang, "bid")} } th { {s(lang, "ask")} } th { {s(lang, "age_ms")} } th { {s(lang, "source")} } th { {s(lang, "stale")} }
                    } }
                    tbody {
                        for row in rows.iter().take(40) {
                            tr {
                                td { "{row.exchange}" }
                                td { "{row.symbol}" }
                                td { "{row.best_bid}" }
                                td { "{row.best_ask}" }
                                td { "{row.book_age_ms}" }
                                td { "{row.source}" }
                                td { StatusPill { value: row.is_stale, lang } }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
pub(crate) fn SymbolPanel(symbols: Value, opportunities: Value, lang: Language) -> Element {
    let symbol_rows = SymbolRuleRowData::from_response(&symbols, lang);
    let opp_rows = SymbolOpportunityRowData::from_response(&opportunities, lang);
    let recent_rows = RecentOpportunityRowData::from_response(&opportunities, lang);
    rsx! {
        section { id: "symbols", class: "stack",
            div { class: "two",
            div { class: "panel",
                h2 { {s(lang, "symbols")} }
                div { class: "table-wrap",
                    table {
                        thead { tr { th { {s(lang, "symbol")} } th { {s(lang, "exchange")} } th { {s(lang, "base")} } th { {s(lang, "quote")} } } }
                        tbody {
                            for row in symbol_rows.iter().take(20) {
                                tr {
                                    td { "{row.internal_symbol}" }
                                    td { "{row.exchange}" }
                                    td { "{row.base_asset}" }
                                    td { "{row.quote_asset}" }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "panel",
                h2 { {s(lang, "opportunities")} }
                div { class: "table-wrap",
                    table {
                        thead { tr { th { {s(lang, "symbol")} } th { {s(lang, "spread_bps")} } th { {s(lang, "score")} } th { {s(lang, "warnings")} } } }
                        tbody {
                            for row in opp_rows.iter().rev().take(20) {
                                tr {
                                    td { "{row.symbol}" }
                                    td { "{row.net_spread_bps}" }
                                    td { "{row.risk_adjusted_score}" }
                                    td { "{row.warnings}" }
                                }
                            }
                        }
                    }
                }
            }
            }
            div { class: "panel",
                h2 { {s(lang, "recent_opportunities")} }
                div { class: "table-wrap compact-table",
                    table {
                        thead { tr {
                            th { {s(lang, "time")} } th { {s(lang, "symbol")} } th { {s(lang, "buy_exchange")} } th { {s(lang, "sell_exchange")} } th { {s(lang, "spread_bps")} } th { {s(lang, "profit")} }
                        } }
                        tbody {
                            for row in recent_rows.iter().rev().take(20) {
                                tr {
                                    td { "{row.timestamp}" }
                                    td { "{row.symbol}" }
                                    td { "{row.buy_exchange}" }
                                    td { "{row.sell_exchange}" }
                                    td { "{row.spread_bps}" }
                                    td { "{row.profit}" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
pub(crate) fn DryRunPanel(plans: Value, lang: Language) -> Element {
    let rows = DryRunPlanRowData::from_rows(&plans, lang);
    rsx! {
        section { id: "plans", class: "panel",
            h2 { {s(lang, "dry_run_order_plans")} }
            div { class: "table-wrap",
                table {
                    thead { tr {
                        th { {s(lang, "time")} } th { {s(lang, "plan_intent")} } th { {s(lang, "exchange")} } th { {s(lang, "symbol")} } th { {s(lang, "side")} } th { {s(lang, "notional")} } th { {s(lang, "qty")} } th { {s(lang, "would_submit")} }
                    } }
                    tbody {
                        for row in rows.iter().rev().take(30) {
                            tr {
                                td { "{row.timestamp}" }
                                td { "{row.intent}" }
                                td { "{row.exchange}" }
                                td { "{row.symbol}" }
                                td { "{row.side}" }
                                td { "{row.notional}" }
                                td { "{row.quantity}" }
                                td { StatusPill { value: row.would_submit, lang } }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
pub(crate) fn RiskPanel(risk: Value, lang: Language) -> Element {
    let data = RiskPanelData::from_value(&risk, lang);
    let debug = RiskDebugPanelData::from_value(&risk);
    rsx! {
        section { id: "risk", class: "two",
            div { class: "panel",
                h2 { {s(lang, "risk_gates")} }
                div { class: "config-grid",
                    div { span { {s(lang, "metric_kill_switch")} } strong { if data.kill_switch_active { "ACTIVE" } else { "OK" } } }
                    div { span { {s(lang, "auto_stop")} } strong { if data.auto_stop_active { "YES" } else { "NO" } } }
                    div { span { {s(lang, "stop_reason")} } strong { "{data.stop_reason}" } }
                    div { span { {s(lang, "update_time")} } strong { "{data.triggered_at}" } }
                }
                if let Some(summary) = data.auto_stop_summary.as_ref() {
                    div { class: "fee-strip",
                        span { class: "chip warn", "{summary}" }
                    }
                }
                h2 { {s(lang, "recent_risk_events")} }
                div { class: "table-wrap compact-table",
                    table {
                        thead { tr {
                            th { {s(lang, "time")} } th { {s(lang, "severity")} } th { {s(lang, "symbol")} } th { {s(lang, "reason")} } th { {s(lang, "details")} }
                        } }
                        tbody {
                            for row in data.events.iter().rev().take(8) {
                                tr {
                                    td { "{row.timestamp}" }
                                    td { "{row.severity}" }
                                    td { "{row.symbol}" }
                                    td { "{row.reason}" }
                                    td { "{row.details}" }
                                }
                            }
                        }
                    }
                }
                pre { "{debug.raw}" }
            }
            div { class: "panel",
                h2 { "LivePreflight / SmallLiveGate" }
                pre { "{debug.live_preflight}" }
                pre { "{debug.small_live_gate}" }
            }
        }
    }
}

#[component]
pub(crate) fn RuntimePublisherPanel(
    runtime_publisher: Value,
    health: Value,
    audit: Value,
    lang: Language,
) -> Element {
    let data = RuntimePublisherPanelData::from_values(&runtime_publisher, &health, &audit);
    rsx! {
        section { id: "runtime", class: "two",
            div { class: "panel",
                h2 { "RuntimePublisher" }
                pre { "{data.status}" }
            }
            div { class: "panel",
                h2 { {s(lang, "runtime_status")} }
                pre { "{data.runtime_health}" }
            }
            div { class: "panel",
                h2 { {s(lang, "exchange_health")} }
                pre { "{data.exchanges}" }
            }
            div { class: "panel",
                h2 { {s(lang, "components")} }
                pre { "{data.components}" }
            }
            div { class: "panel",
                h2 { {s(lang, "errors")} }
                pre { "{data.errors}" }
            }
            div { class: "panel",
                h2 { {s(lang, "control_audit")} }
                pre { "{data.audit}" }
            }
        }
    }
}

#[component]
pub(crate) fn InventoryPanel(
    inventory: Value,
    fees: Value,
    disabled: Value,
    lang: Language,
) -> Element {
    let inventory_rows = InventoryAssetRowData::from_rows(&inventory, lang);
    let fee_rows = FeeScheduleRowData::from_rows(&fees, lang);
    let disabled_debug = DebugJsonPanelData::from_primary(&disabled);
    rsx! {
        section { id: "inventory", class: "two",
            div { class: "panel",
                h2 { {s(lang, "inventory")} }
                div { class: "table-wrap",
                    table {
                        thead { tr { th { {s(lang, "exchange")} } th { {s(lang, "asset")} } th { {s(lang, "total")} } th { {s(lang, "available")} } th { {s(lang, "reserved")} } } }
                        tbody {
                            for row in inventory_rows.iter().take(40) {
                                tr {
                                    td { "{row.exchange}" }
                                    td { "{row.asset}" }
                                    td { "{row.total}" }
                                    td { "{row.available}" }
                                    td { "{row.locally_reserved}" }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "panel",
                h2 { {s(lang, "fees_disabled")} }
                div { class: "table-wrap",
                    table {
                        thead { tr { th { {s(lang, "exchange")} } th { {s(lang, "symbol")} } th { {s(lang, "maker_bps")} } th { {s(lang, "taker_bps")} } th { {s(lang, "source")} } } }
                        tbody {
                            for row in fee_rows.iter().take(30) {
                                tr {
                                    td { "{row.exchange}" }
                                    td { "{row.symbol}" }
                                    td { "{row.maker_fee_bps}" }
                                    td { "{row.taker_fee_bps}" }
                                    td { "{row.source}" }
                                }
                            }
                        }
                    }
                }
                pre { "{disabled_debug.primary}" }
            }
        }
    }
}

#[component]
pub(crate) fn ScannerPanel(scanner: Value, hedge_policy: Value, lang: Language) -> Element {
    let debug = DebugJsonPanelData::from_values(&scanner, &hedge_policy);
    rsx! {
        section { id: "scanner", class: "two",
            div { class: "panel",
                h2 { {s(lang, "scanner")} }
                pre { "{debug.primary}" }
            }
            div { class: "panel",
                h2 { {s(lang, "hedge_policy")} }
                pre { "{debug.secondary}" }
            }
        }
    }
}

#[component]
pub(crate) fn ConfigPanel(config: Value, control_symbols: Value, lang: Language) -> Element {
    let debug = DebugJsonPanelData::from_values(&config, &control_symbols);
    rsx! {
        section { id: "config", class: "two",
            div { class: "panel",
                h2 { {s(lang, "config_summary")} }
                pre { "{debug.primary}" }
            }
            div { class: "panel",
                h2 { {s(lang, "symbol_control")} }
                pre { "{debug.secondary}" }
            }
        }
    }
}

#[component]
pub(crate) fn LogsPanel(logs: Value, health: Value, lang: Language) -> Element {
    let debug = DebugJsonPanelData::from_values(&logs, &health);
    rsx! {
        section { id: "logs", class: "two",
            div { class: "panel",
                h2 { {s(lang, "runtime_logs")} }
                pre { "{debug.primary}" }
            }
            div { class: "panel",
                h2 { {s(lang, "control_api_health")} }
                pre { "{debug.secondary}" }
            }
        }
    }
}

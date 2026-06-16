use std::collections::BTreeMap;

use dioxus::prelude::*;
use serde_json::Value;

use crate::api_keys::ApiKeysPanel;
use crate::i18n::s;
use crate::types::{DashboardData, ExchangeCredentialPanelData, Language};
use crate::utils::{
    as_array, compact, format_usdt, profit_class, signed_usdt, sparkline_points,
    strategy_log_rows_for_category, text_at,
};

const DAY_MS: f64 = 24.0 * 60.0 * 60.0 * 1000.0;

#[component]
pub(crate) fn TradeHistoryPanel(data: DashboardData, lang: Language) -> Element {
    let mut active_curve = use_signal(|| ProfitCurveRange::SevenDays);
    let curves = profit_curves(&data.balance_history, lang);
    let selected_curve = curves
        .iter()
        .find(|curve| curve.range == active_curve())
        .cloned()
        .unwrap_or_else(|| curves[0].clone());
    let rows = trade_history_rows(&data, lang);

    rsx! {
        section { class: "console-page trade-history-page",
            div { class: "panel profit-chart-panel",
                div { class: "profit-chart-head",
                    div {
                        h2 { "利润走势" }
                        p { class: "muted", "7 天 / 30 天 / 累计" }
                    }
                    div { class: "profit-tabs",
                        for curve in curves.iter() {
                            {
                                let range = curve.range;
                                rsx! {
                                    button {
                                        class: if active_curve() == range { "profit-tab active" } else { "profit-tab" },
                                        onclick: move |_| active_curve.set(range),
                                        "{curve.label}"
                                    }
                                }
                            }
                        }
                    }
                }
                div { class: "profit-chart-summary",
                    span { "{selected_curve.label}" }
                    strong { class: profit_class(selected_curve.value), "{signed_usdt(selected_curve.value)}" }
                }
                div { class: "profit-chart-frame",
                    if selected_curve.points.is_empty() {
                        div { class: "empty-state", "暂无收益曲线" }
                    } else {
                        svg { class: "balance-chart", view_box: "0 0 720 220", preserve_aspect_ratio: "none",
                            polyline {
                                points: "{selected_curve.points}",
                                fill: "none",
                                stroke: "#1f7a8c",
                                stroke_width: "4",
                                stroke_linecap: "round",
                                stroke_linejoin: "round"
                            }
                            line {
                                x1: "16",
                                y1: "204",
                                x2: "704",
                                y2: "204",
                                stroke: "#d8e0e8",
                                stroke_width: "1"
                            }
                        }
                    }
                }
            }
            div { class: "panel history-table-panel",
                div { class: "panel-title-row",
                    h2 { "历史交易" }
                    span { class: "pill neutral", "{rows.len()} 条" }
                }
                div { class: "table-wrap console-table-wrap",
                    table { class: "console-table",
                        thead {
                            tr {
                                th { "币种" }
                                th { "利润" }
                                th { "交易所" }
                                th { "时间" }
                                th { "详情" }
                            }
                        }
                        tbody {
                            for row in rows.iter() {
                                tr {
                                    td { "{row.symbol}" }
                                    td { strong { class: profit_class(row.profit_value), "{row.profit}" } }
                                    td { "{row.exchange}" }
                                    td { "{row.time}" }
                                    td { class: "detail-cell", "{row.detail}" }
                                }
                            }
                        }
                    }
                    if rows.is_empty() {
                        div { class: "empty-state", "暂无历史收益、亏损或资金费率结算记录。" }
                    }
                }
            }
        }
    }
}

#[component]
pub(crate) fn ExchangeConsolePanel(
    data: Signal<DashboardData>,
    token: String,
    message: Signal<String>,
    lang: Language,
) -> Element {
    rsx! {
        section { class: "console-page exchange-console-page",
            ApiKeysPanel {
                data,
                token,
                message,
                lang,
                selected_exchange: "binance".to_string(),
                selected_account: "default".to_string(),
                selected_namespace: String::new()
            }
        }
    }
}

#[component]
pub(crate) fn LogsConsolePanel(data: DashboardData, lang: Language) -> Element {
    let rows = combined_log_rows(&data, lang);
    let source = combined_log_source_text(lang);

    rsx! {
        section { class: "console-page logs-console-page",
            div { class: "panel log-panel",
                div { class: "panel-title-row",
                    div {
                        h2 { "日志" }
                        p { class: "muted", "{source}" }
                    }
                    span { class: "pill neutral", "{rows.len()} 条" }
                }
                LogTable { rows }
            }
        }
    }
}

fn combined_log_source_text(lang: Language) -> &'static str {
    match lang {
        Language::Zh => "策略日志 / 运行日志",
        Language::En => "Strategy logs / runtime logs",
    }
}

fn combined_log_rows(data: &DashboardData, lang: Language) -> Vec<crate::types::LogRow> {
    let mut rows = strategy_log_rows_for_category(&data.strategy_logs, "all", lang)
        .into_iter()
        .filter(|row| !is_empty_log_row(row))
        .collect::<Vec<_>>();
    rows.extend(
        strategy_log_rows_for_category(&data.logs, "all", lang)
            .into_iter()
            .filter(|row| !is_empty_log_row(row)),
    );
    if rows.is_empty() {
        rows.push(crate::types::LogRow {
            timestamp: "-".to_string(),
            level: "INFO".to_string(),
            source: "logs".to_string(),
            message: s(lang, "no_exception_logs"),
            class_name: "log-info".to_string(),
        });
    }
    rows.truncate(200);
    rows
}

fn is_empty_log_row(row: &crate::types::LogRow) -> bool {
    row.timestamp == "-"
        && row.class_name == "log-info"
        && matches!(
            row.message.as_str(),
            "暂无异常日志。" | "No exception logs."
        )
}

#[component]
fn LogTable(rows: Vec<crate::types::LogRow>) -> Element {
    rsx! {
        div { class: "table-wrap console-table-wrap log-table-wrap",
            table { class: "console-table log-table",
                thead {
                    tr {
                        th { "时间" }
                        th { "级别" }
                        th { "来源" }
                        th { "内容" }
                    }
                }
                tbody {
                    for row in rows.iter() {
                        tr { class: "{row.class_name}",
                            td { "{row.timestamp}" }
                            td { "{row.level}" }
                            td { "{row.source}" }
                            td { class: "detail-cell", "{row.message}" }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ProfitCurve {
    range: ProfitCurveRange,
    label: String,
    points: String,
    value: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProfitCurveRange {
    SevenDays,
    ThirtyDays,
    Cumulative,
}

#[derive(Clone, Debug, PartialEq)]
struct TradeHistoryRow {
    symbol: String,
    profit: String,
    profit_value: f64,
    exchange: String,
    time: String,
    detail: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct ExchangeBalanceAccumulator {
    label: String,
    spot: f64,
    perp: f64,
    asset_count: usize,
    status: String,
    updated_at: String,
}

#[derive(Clone, Debug, PartialEq)]
struct ExchangeBalanceRow {
    name: String,
    spot_balance: String,
    perp_balance: String,
    asset_count: usize,
    status: String,
    status_class: String,
    updated_at: String,
}

fn profit_curves(history: &Value, lang: Language) -> Vec<ProfitCurve> {
    let labels = match lang {
        Language::Zh => ["最近7天", "最近30天", "累计"],
        Language::En => ["7 Days", "30 Days", "Cumulative"],
    };
    vec![
        profit_curve(
            history,
            ProfitCurveRange::SevenDays,
            labels[0],
            Some(7.0 * DAY_MS),
        ),
        profit_curve(
            history,
            ProfitCurveRange::ThirtyDays,
            labels[1],
            Some(30.0 * DAY_MS),
        ),
        profit_curve(history, ProfitCurveRange::Cumulative, labels[2], None),
    ]
}

fn profit_curve(
    history: &Value,
    range: ProfitCurveRange,
    label: &str,
    window_ms: Option<f64>,
) -> ProfitCurve {
    let mut points = profit_points(history);
    if let Some(window_ms) = window_ms {
        let cutoff = js_sys::Date::now() - window_ms;
        let filtered = points
            .iter()
            .copied()
            .filter(|(timestamp, _)| *timestamp >= cutoff)
            .collect::<Vec<_>>();
        if !filtered.is_empty() {
            points = filtered;
        }
    }
    let values = points.iter().map(|(_, value)| *value).collect::<Vec<_>>();
    let value = match (values.first(), values.last(), window_ms) {
        (Some(first), Some(last), Some(_)) => last - first,
        (_, Some(last), None) => *last,
        _ => 0.0,
    };
    ProfitCurve {
        range,
        label: label.to_string(),
        points: sparkline_points(&values, 720.0, 220.0, 16.0),
        value,
    }
}

fn profit_points(history: &Value) -> Vec<(f64, f64)> {
    let rows = as_array(history);
    let first_total = rows
        .first()
        .and_then(|row| row.get("total_usdt").and_then(Value::as_f64))
        .unwrap_or_default();
    rows.iter()
        .enumerate()
        .filter_map(|(index, row)| {
            let timestamp = row
                .get("timestamp_ms")
                .and_then(Value::as_f64)
                .unwrap_or(index as f64);
            let profit = row.get("profit_usdt").and_then(Value::as_f64).or_else(|| {
                row.get("total_usdt")
                    .and_then(Value::as_f64)
                    .map(|total| total - first_total)
            })?;
            Some((timestamp, profit))
        })
        .collect()
}

fn trade_history_rows(data: &DashboardData, lang: Language) -> Vec<TradeHistoryRow> {
    let mut rows = Vec::new();
    collect_recent_trade_rows(&data.recent_trades, &mut rows, lang);
    collect_balance_profit_rows(&data.balance_history, &mut rows, lang);
    collect_history_rows(&data.strategy_logs, &mut rows, lang);
    collect_history_rows(&data.logs, &mut rows, lang);
    if rows.is_empty() {
        collect_plan_rows(&data.dry_run_plans, &mut rows, lang);
    }
    rows.truncate(80);
    rows
}

fn collect_balance_profit_rows(value: &Value, rows: &mut Vec<TradeHistoryRow>, lang: Language) {
    for item in as_array(value).iter().rev() {
        let Some(profit_value) = first_numeric(item, &["event_profit_usdt"]) else {
            continue;
        };
        rows.push(TradeHistoryRow {
            symbol: first_text(item, &["symbol", "instrument", "asset"], lang),
            profit: signed_usdt(profit_value),
            profit_value,
            exchange: exchange_text(item, lang),
            time: first_text(
                item,
                &["timestamp", "recorded_at", "time", "timestamp_ms"],
                lang,
            ),
            detail: first_text(item, &["detail", "lifecycle", "event_kind"], lang),
        });
        if rows.len() >= 80 {
            break;
        }
    }
}

fn collect_recent_trade_rows(value: &Value, rows: &mut Vec<TradeHistoryRow>, lang: Language) {
    for item in as_array(value).iter().rev() {
        let profit_value = first_numeric(
            item,
            &[
                "profit_usdt",
                "net_profit_usdt",
                "realized_pnl",
                "pnl",
                "funding_fee",
                "funding_pnl",
                "fee_usdt",
            ],
        )
        .unwrap_or_default();
        let profit = if profit_value.abs() > f64::EPSILON {
            signed_usdt(profit_value)
        } else {
            "-".to_string()
        };
        let detail = first_text(
            item,
            &[
                "message", "detail", "details", "side", "intent", "order_id", "trade_id",
            ],
            lang,
        );
        rows.push(TradeHistoryRow {
            symbol: first_text(
                item,
                &["symbol", "instrument", "inst_id", "asset", "coin"],
                lang,
            ),
            profit,
            profit_value,
            exchange: exchange_text(item, lang),
            time: first_text(
                item,
                &[
                    "timestamp",
                    "recorded_at",
                    "time",
                    "created_at",
                    "closed_at",
                    "settled_at",
                ],
                lang,
            ),
            detail: if detail == "-" { compact(item) } else { detail },
        });
        if rows.len() >= 80 {
            break;
        }
    }
}

fn collect_history_rows(value: &Value, rows: &mut Vec<TradeHistoryRow>, lang: Language) {
    if let Some(events) = value.get("events").and_then(Value::as_array) {
        for event in events.iter().rev() {
            if let Some(row) = trade_history_row(event, lang) {
                rows.push(row);
            }
        }
        return;
    }
    if let Some(lines) = value.get("lines").and_then(Value::as_array) {
        for line in lines.iter().rev() {
            if let Some(row) = trade_history_row(line, lang) {
                rows.push(row);
            }
        }
        return;
    }
    if let Some(items) = value.as_array() {
        for item in items.iter().rev() {
            if let Some(row) = trade_history_row(item, lang) {
                rows.push(row);
            }
        }
    }
}

fn collect_plan_rows(value: &Value, rows: &mut Vec<TradeHistoryRow>, lang: Language) {
    for item in as_array(value).iter().rev() {
        let detail = first_text(item, &["intent", "plan_intent", "side"], lang);
        rows.push(TradeHistoryRow {
            symbol: first_text(item, &["symbol", "instrument", "asset"], lang),
            profit: "-".to_string(),
            profit_value: 0.0,
            exchange: first_text(item, &["exchange", "venue"], lang),
            time: first_text(item, &["timestamp", "created_at", "time"], lang),
            detail,
        });
        if rows.len() >= 40 {
            break;
        }
    }
}

fn trade_history_row(value: &Value, lang: Language) -> Option<TradeHistoryRow> {
    let haystack = compact(value).to_ascii_lowercase();
    let is_history = haystack.contains("profit")
        || haystack.contains("pnl")
        || haystack.contains("funding")
        || haystack.contains("settlement")
        || haystack.contains("fill")
        || haystack.contains("close")
        || haystack.contains("trade");
    if !is_history {
        return None;
    }
    let profit_value = first_numeric(
        value,
        &[
            "profit_usdt",
            "net_profit_usdt",
            "realized_pnl",
            "pnl",
            "funding_fee",
            "funding_pnl",
            "fee_usdt",
        ],
    )
    .unwrap_or_default();
    let profit = if profit_value.abs() > f64::EPSILON {
        signed_usdt(profit_value)
    } else {
        "-".to_string()
    };
    let message = first_text(
        value,
        &["message", "detail", "details", "reason", "event", "intent"],
        lang,
    );
    Some(TradeHistoryRow {
        symbol: first_text(
            value,
            &["symbol", "instrument", "inst_id", "asset", "coin"],
            lang,
        ),
        profit,
        profit_value,
        exchange: exchange_text(value, lang),
        time: first_text(
            value,
            &[
                "timestamp",
                "recorded_at",
                "time",
                "created_at",
                "closed_at",
                "settled_at",
            ],
            lang,
        ),
        detail: if message == "-" {
            compact(value)
        } else {
            message
        },
    })
}

fn exchange_console_rows(data: &DashboardData, lang: Language) -> Vec<ExchangeBalanceRow> {
    let mut rows: BTreeMap<String, ExchangeBalanceAccumulator> = BTreeMap::new();
    for item in as_array(&data.inventory) {
        let exchange = text_at(&item, "exchange", Language::En);
        if exchange == "-" {
            continue;
        }
        let value = item
            .get("valuation_usdt")
            .and_then(Value::as_f64)
            .or_else(|| {
                let asset = item.get("asset").and_then(Value::as_str)?;
                (asset.eq_ignore_ascii_case("USDT") || asset.eq_ignore_ascii_case("USDC"))
                    .then(|| item.get("total").and_then(Value::as_f64))
                    .flatten()
            })
            .unwrap_or_default();
        let market = text_at(&item, "market_type", Language::En).to_ascii_lowercase();
        let entry = rows
            .entry(exchange.clone())
            .or_insert_with(|| ExchangeBalanceAccumulator {
                label: exchange.clone(),
                updated_at: first_text(&item, &["timestamp", "updated_at", "recorded_at"], lang),
                ..ExchangeBalanceAccumulator::default()
            });
        if market.contains("perp")
            || market.contains("future")
            || market.contains("swap")
            || market.contains("contract")
        {
            entry.perp += value;
        } else {
            entry.spot += value;
        }
        entry.asset_count += 1;
    }

    let credential_data = ExchangeCredentialPanelData::from_status(&data.api_keys, lang);
    for account in credential_data.exchange_rows {
        let exchange = account.exchange.clone();
        let entry = rows
            .entry(exchange.clone())
            .or_insert_with(|| ExchangeBalanceAccumulator {
                label: account.label.clone(),
                ..ExchangeBalanceAccumulator::default()
            });
        if entry.label.trim().is_empty() || entry.label == "-" {
            entry.label = account.label;
        }
        if let Some(spot) = account.spot_total_usdt {
            if entry.spot.abs() <= f64::EPSILON {
                entry.spot = spot;
            }
        }
        if let Some(perp) = account.perp_total_usdt {
            if entry.perp.abs() <= f64::EPSILON {
                entry.perp = perp;
            }
        }
        if account.connection_status != "-" {
            entry.status = account.connection_status;
        }
        if account.last_tested_at != "-" {
            entry.updated_at = account.last_tested_at;
        }
    }

    for item in as_array(&data.exchanges) {
        let exchange = text_at(&item, "exchange", Language::En);
        if exchange == "-" {
            continue;
        }
        let entry = rows
            .entry(exchange.clone())
            .or_insert_with(|| ExchangeBalanceAccumulator {
                label: exchange.clone(),
                ..ExchangeBalanceAccumulator::default()
            });
        if entry.status.trim().is_empty() {
            entry.status = text_at(&item, "status", lang);
        }
        if entry.updated_at.trim().is_empty() || entry.updated_at == "-" {
            entry.updated_at = first_text(&item, &["last_update", "last_message_at"], lang);
        }
    }

    rows.into_values()
        .map(|row| {
            let status = if row.status.trim().is_empty() || row.status == "-" {
                "unknown".to_string()
            } else {
                row.status
            };
            let lower = status.to_ascii_lowercase();
            let status_class = if lower.contains("ok")
                || lower.contains("connect")
                || lower.contains("healthy")
                || status.contains("已连接")
            {
                "status-pill good"
            } else if lower.contains("fail") || lower.contains("error") || lower.contains("missing")
            {
                "status-pill bad"
            } else {
                "status-pill warn"
            }
            .to_string();
            ExchangeBalanceRow {
                name: row.label,
                spot_balance: format_usdt(row.spot),
                perp_balance: format_usdt(row.perp),
                asset_count: row.asset_count,
                status,
                status_class,
                updated_at: if row.updated_at.trim().is_empty() {
                    "-".to_string()
                } else {
                    row.updated_at
                },
            }
        })
        .collect()
}

fn exchange_text(value: &Value, lang: Language) -> String {
    let direct = first_text(
        value,
        &[
            "exchange",
            "venue",
            "buy_exchange",
            "sell_exchange",
            "long_exchange",
            "short_exchange",
        ],
        lang,
    );
    if direct != "-" {
        return direct;
    }
    let long_exchange = text_at(value, "long_exchange", lang);
    let short_exchange = text_at(value, "short_exchange", lang);
    if long_exchange != "-" || short_exchange != "-" {
        return format!("{long_exchange} / {short_exchange}");
    }
    "-".to_string()
}

fn first_text(value: &Value, keys: &[&str], lang: Language) -> String {
    keys.iter()
        .map(|key| text_at(value, key, lang))
        .find(|value| !value.trim().is_empty() && value != "-")
        .unwrap_or_else(|| "-".to_string())
}

fn first_numeric(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_f64))
        .or_else(|| {
            value.as_object().and_then(|map| {
                map.values().find_map(|child| {
                    if child.is_object() {
                        first_numeric(child, keys)
                    } else {
                        None
                    }
                })
            })
        })
}

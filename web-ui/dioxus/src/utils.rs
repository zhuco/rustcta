use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value};

use crate::i18n::t;
use crate::storage::storage;
use crate::types::{
    AnomalyTradeRow, AuxAssetRow, BalanceSummary, CoinConsoleRow, ExchangeStatisticRow, Language,
    LogRow, PositionRow, RuntimeTimingRow, SymbolExchangeDetailRow,
};

const BALANCE_HISTORY_KEY: &str = "rustcta_control_balance_history_v1";
const ACTIVE_ARBITRAGE_PLAN_WINDOW_MS: f64 = 600_000.0;
const BEIJING_OFFSET_MS: f64 = 8.0 * 60.0 * 60.0 * 1000.0;

pub(crate) fn as_array(value: &Value) -> Vec<Value> {
    value.as_array().cloned().unwrap_or_default()
}

pub(crate) fn canonical_exchange_name(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "gate" | "gateio" | "gate.io" => "gateio".to_string(),
        "binance_spot" => "binance".to_string(),
        "okx_spot" => "okx".to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn enabled_exchange_set(config: &Value) -> BTreeSet<String> {
    config
        .get("enabled_exchanges")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(canonical_exchange_name)
                .filter(|exchange| !exchange.is_empty())
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default()
}

pub(crate) fn row_matches_enabled_exchange(row: &Value, enabled: &BTreeSet<String>) -> bool {
    enabled.is_empty()
        || enabled.contains(&canonical_exchange_name(&text_at(
            row,
            "exchange",
            Language::En,
        )))
}

pub(crate) fn balance_summaries_for_enabled(
    inventory: &Value,
    enabled: &BTreeSet<String>,
) -> Vec<BalanceSummary> {
    let mut rows = balance_summaries(inventory)
        .into_iter()
        .filter(|row| {
            enabled.is_empty() || enabled.contains(&canonical_exchange_name(&row.exchange))
        })
        .collect::<Vec<_>>();
    let existing = rows
        .iter()
        .map(|row| canonical_exchange_name(&row.exchange))
        .collect::<BTreeSet<_>>();
    for exchange in enabled {
        if !existing.contains(exchange) {
            rows.push(BalanceSummary {
                exchange: exchange.clone(),
                total_usdt: 0.0,
                available_usdt: 0.0,
                asset_count: 0,
            });
        }
    }
    rows.sort_by(|left, right| left.exchange.cmp(&right.exchange));
    rows
}

pub(crate) fn fallback_exchange_schemas() -> Vec<Value> {
    vec![
        json!({"exchange":"binance","label":"Binance","fields":[{"field":"api_key","label":"API Key"},{"field":"api_secret","label":"API Secret"}]}),
        json!({"exchange":"okx","label":"OKX","fields":[{"field":"api_key","label":"API Key"},{"field":"api_secret","label":"API Secret"},{"field":"passphrase","label":"Passphrase"}]}),
        json!({"exchange":"bitget","label":"Bitget","fields":[{"field":"api_key","label":"API Key"},{"field":"api_secret","label":"API Secret"},{"field":"passphrase","label":"Passphrase"}]}),
        json!({"exchange":"gate","label":"Gate.io","fields":[{"field":"account_id","label":"Gate User ID / Account ID"},{"field":"api_key","label":"API Key"},{"field":"api_secret","label":"API Secret"}]}),
        json!({"exchange":"bybit","label":"Bybit","fields":[{"field":"api_key","label":"API Key"},{"field":"api_secret","label":"API Secret"}]}),
        json!({"exchange":"mexc","label":"MEXC","fields":[{"field":"api_key","label":"API Key"},{"field":"api_secret","label":"API Secret"}]}),
        json!({"exchange":"coinex","label":"CoinEx","fields":[{"field":"api_key","label":"API Key"},{"field":"api_secret","label":"API Secret"}]}),
        json!({"exchange":"kucoin","label":"KuCoin","fields":[{"field":"api_key","label":"API Key"},{"field":"api_secret","label":"API Secret"},{"field":"passphrase","label":"API Passphrase"}]}),
        json!({"exchange":"htx","label":"HTX","fields":[{"field":"api_key","label":"API Key"},{"field":"api_secret","label":"API Secret"}]}),
        json!({"exchange":"bitmart","label":"BitMart","fields":[{"field":"api_key","label":"API Key"},{"field":"api_secret","label":"API Secret"},{"field":"passphrase","label":"Memo"}]}),
        json!({"exchange":"hyperliquid","label":"Hyperliquid","fields":[{"field":"api_key","label":"Wallet / API Key","required":true},{"field":"api_secret","label":"Private Key","required":true}]}),
    ]
}

pub(crate) fn normalize_exchange_api_key_schemas(mut rows: Vec<Value>) -> Vec<Value> {
    let has_hyperliquid = rows.iter().any(|row| {
        canonical_exchange_name(&text_at(row, "exchange", Language::En)) == "hyperliquid"
    });
    if !has_hyperliquid {
        rows.push(hyperliquid_exchange_schema());
    }
    rows
}

fn hyperliquid_exchange_schema() -> Value {
    json!({
        "exchange": "hyperliquid",
        "label": "Hyperliquid",
        "fields": [
            {"field": "api_key", "label": "Wallet / API Key", "required": true},
            {"field": "api_secret", "label": "Private Key", "required": true}
        ]
    })
}

pub(crate) fn exchange_field_value(fields: &[Value], field_name: &str) -> String {
    fields
        .iter()
        .find(|field| field.get("field").and_then(Value::as_str) == Some(field_name))
        .and_then(|field| field.get("value"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

pub(crate) fn balance_summaries(inventory: &Value) -> Vec<BalanceSummary> {
    let mut by_exchange: BTreeMap<String, BalanceSummary> = BTreeMap::new();
    for row in as_array(inventory) {
        let exchange = row
            .get("exchange")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let total = row
            .get("valuation_usdt")
            .and_then(Value::as_f64)
            .or_else(|| {
                let asset = row.get("asset").and_then(Value::as_str)?;
                if asset.eq_ignore_ascii_case("USDT") || asset.eq_ignore_ascii_case("USDC") {
                    row.get("total").and_then(Value::as_f64)
                } else {
                    None
                }
            })
            .unwrap_or(0.0);
        let available = row
            .get("valuation_usdt")
            .and_then(Value::as_f64)
            .map(|valuation| {
                let total_qty = row.get("total").and_then(Value::as_f64).unwrap_or_default();
                let available_qty = row
                    .get("effective_available")
                    .or_else(|| row.get("available"))
                    .and_then(Value::as_f64)
                    .unwrap_or_default();
                if total_qty > 0.0 {
                    valuation * (available_qty / total_qty).clamp(0.0, 1.0)
                } else {
                    0.0
                }
            })
            .or_else(|| {
                let asset = row.get("asset").and_then(Value::as_str)?;
                if asset.eq_ignore_ascii_case("USDT") || asset.eq_ignore_ascii_case("USDC") {
                    row.get("available").and_then(Value::as_f64)
                } else {
                    None
                }
            })
            .unwrap_or(0.0);
        let entry = by_exchange
            .entry(exchange.clone())
            .or_insert_with(|| BalanceSummary {
                exchange,
                total_usdt: 0.0,
                available_usdt: 0.0,
                asset_count: 0,
            });
        entry.total_usdt += total;
        entry.available_usdt += available;
        entry.asset_count += 1;
    }
    let mut rows = by_exchange.into_values().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .total_usdt
            .partial_cmp(&left.total_usdt)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
}

pub(crate) fn best_opportunity_per_symbol(rows: &[Value], lang: Language) -> Vec<Value> {
    let mut by_symbol: BTreeMap<String, Value> = BTreeMap::new();
    for row in rows {
        let symbol = text_at(row, "symbol", lang);
        match by_symbol.get(&symbol) {
            Some(existing)
                if numeric_at(existing, "tt_immediate_net_bps")
                    >= numeric_at(row, "tt_immediate_net_bps") => {}
            _ => {
                by_symbol.insert(symbol, row.clone());
            }
        }
    }
    let mut best = by_symbol.into_values().collect::<Vec<_>>();
    best.sort_by(|left, right| {
        numeric_at(right, "tt_immediate_net_bps")
            .partial_cmp(&numeric_at(left, "tt_immediate_net_bps"))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    best
}

pub(crate) fn runtime_timing_rows(
    runtime_publisher: &Value,
    exchanges: &Value,
    lang: Language,
) -> Vec<RuntimeTimingRow> {
    let mut rows = runtime_publisher
        .get("per_exchange_health")
        .and_then(Value::as_object)
        .map(|items| {
            items
                .values()
                .map(|item| RuntimeTimingRow {
                    exchange: text_at(item, "exchange", lang),
                    latency_ms: item.get("request_latency_ms").and_then(Value::as_f64),
                    request_failures: item
                        .get("request_failures")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                    rate_limit_events: item
                        .get("rate_limit_events")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                    last_successful_request: text_at(item, "last_successful_request", lang),
                    backoff_until: text_at(item, "backoff_until", lang),
                    last_error: text_at(item, "last_error", lang),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if rows.is_empty() {
        rows = as_array(exchanges)
            .into_iter()
            .map(|item| RuntimeTimingRow {
                exchange: text_at(&item, "exchange", lang),
                latency_ms: item.get("avg_latency_ms").and_then(Value::as_f64),
                request_failures: item
                    .get("parse_error_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                rate_limit_events: 0,
                last_successful_request: text_at(&item, "last_message_at", lang),
                backoff_until: "-".to_string(),
                last_error: "-".to_string(),
            })
            .collect();
    }
    rows.sort_by(|left, right| {
        right
            .latency_ms
            .unwrap_or_default()
            .partial_cmp(&left.latency_ms.unwrap_or_default())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn coin_console_rows(
    _candidates: &[Value],
    selected: &[Value],
    opportunities: &[Value],
    plans: &[Value],
    inventory: &[Value],
    disabled: &BTreeSet<String>,
    control_symbols: &Value,
    active_symbols: &[Value],
    liquidation_plans: &[Value],
    lang: Language,
) -> Vec<CoinConsoleRow> {
    let selected_set = selected
        .iter()
        .filter_map(Value::as_str)
        .map(normalize_symbol_text)
        .collect::<BTreeSet<_>>();
    let mut symbols = BTreeSet::<String>::new();
    for symbol in active_symbols.iter().filter_map(Value::as_str) {
        symbols.insert(symbol.to_string());
    }
    for row in plans {
        if let Some(symbol) = row.get("symbol").and_then(Value::as_str) {
            if is_active_arbitrage_plan(row) || plan_is_exit_workflow(row) {
                symbols.insert(symbol.to_string());
            }
        }
    }
    for row in as_array(control_symbols) {
        if let Some(symbol) = row
            .get("internal_symbol")
            .or_else(|| row.get("symbol"))
            .and_then(Value::as_str)
        {
            if control_symbol_state_is_managed_console(&row) {
                symbols.insert(symbol.to_string());
            }
        }
    }
    for row in liquidation_plans {
        if let Some(symbol) = row.get("symbol").and_then(Value::as_str) {
            symbols.insert(symbol.to_string());
        }
    }
    for row in inventory {
        if numeric_at(row, "total").abs() <= f64::EPSILON
            && numeric_at(row, "effective_available").abs() <= f64::EPSILON
        {
            continue;
        }
        let symbol = position_symbol_text(row, lang);
        if symbol != "-" {
            let key = normalize_symbol_text(&symbol);
            if disabled.contains(&key)
                || control_symbol_state(control_symbols, &symbol).is_some_and(|state| {
                    control_state_blocks_symbol(&state) || control_state_is_exit_workflow(&state)
                })
            {
                symbols.insert(symbol);
            }
        }
    }
    let mut rows = symbols
        .into_iter()
        .map(|symbol| {
            let symbol_key = normalize_symbol_text(&symbol);
            let symbol_opps = opportunities
                .iter()
                .filter(|row| {
                    row.get("symbol")
                        .and_then(Value::as_str)
                        .map(normalize_symbol_text)
                        == Some(symbol_key.clone())
                })
                .collect::<Vec<_>>();
            let symbol_plans = plans
                .iter()
                .filter(|row| {
                    row.get("symbol")
                        .and_then(Value::as_str)
                        .map(normalize_symbol_text)
                        == Some(symbol_key.clone())
                        && is_active_arbitrage_plan(row)
                })
                .collect::<Vec<_>>();
            let mut exchanges = BTreeSet::new();
            for row in &symbol_opps {
                if let Some(value) = row.get("buy_exchange").and_then(Value::as_str) {
                    exchanges.insert(value.to_string());
                }
                if let Some(value) = row.get("sell_exchange").and_then(Value::as_str) {
                    exchanges.insert(value.to_string());
                }
            }
            for row in &symbol_plans {
                if let Some(value) = row.get("exchange").and_then(Value::as_str) {
                    exchanges.insert(value.to_string());
                }
            }
            let capital = symbol_opps
                .iter()
                .map(|row| numeric_at(row, "required_capital_usdt"))
                .fold(0.0, f64::max);
            let volume = symbol_plans
                .iter()
                .map(|row| numeric_at(row, "notional"))
                .sum::<f64>();
            let est_profit = symbol_opps
                .iter()
                .filter(|row| bool_at(row, "accepted"))
                .map(|row| numeric_at(row, "tt_immediate_net_pnl"))
                .sum::<f64>();
            let base = symbol_base(&symbol);
            let realized_pnl = inventory
                .iter()
                .filter(|row| row.get("asset").and_then(Value::as_str) == Some(base.as_str()))
                .map(|row| numeric_at(row, "realized_pnl_usdt"))
                .sum::<f64>();
            let disabled = disabled.contains(&symbol_key);
            let control_state = control_symbol_state(control_symbols, &symbol);
            let control_disabled = control_state
                .as_deref()
                .is_some_and(control_state_blocks_symbol);
            let subscribed = selected_set.contains(&normalize_symbol_text(&symbol));
            let has_plans = !symbol_plans.is_empty();
            let exiting = control_state
                .as_deref()
                .is_some_and(control_state_is_exit_workflow)
                || disabled
                || plans.iter().any(|row| {
                    row.get("symbol")
                        .and_then(Value::as_str)
                        .map(normalize_symbol_text)
                        == Some(symbol_key.clone())
                        && plan_is_exit_workflow(row)
                })
                || liquidation_plans.iter().any(|row| {
                    row.get("symbol")
                        .and_then(Value::as_str)
                        .map(normalize_symbol_text)
                        == Some(symbol_key.clone())
                });
            let arbitraging = active_symbols
                .iter()
                .filter_map(Value::as_str)
                .map(normalize_symbol_text)
                .any(|item| item == symbol_key)
                || has_plans
                || control_state
                    .as_deref()
                    .is_some_and(|state| state.eq_ignore_ascii_case("active"));
            let controllable = control_state.is_some() || has_plans || subscribed || exiting;
            let state = if let Some(control_state) = control_state.as_deref() {
                control_state_label(control_state, lang)
            } else if disabled {
                t(lang, "stopped").to_string()
            } else if exiting {
                t(lang, "exiting").to_string()
            } else if arbitraging {
                t(lang, "arbitraging").to_string()
            } else if subscribed {
                t(lang, "subscribed").to_string()
            } else {
                t(lang, "candidate").to_string()
            };
            CoinConsoleRow {
                symbol,
                state,
                control_state: control_state.unwrap_or_else(|| "-".to_string()),
                subscribed,
                disabled: disabled || control_disabled,
                arbitraging,
                exiting,
                controllable,
                capital,
                exchanges: if exchanges.is_empty() {
                    "-".to_string()
                } else {
                    exchanges.into_iter().collect::<Vec<_>>().join(" / ")
                },
                volume,
                est_profit,
                realized_pnl,
                pnl_1h: 0.0,
                pnl_8h: 0.0,
                pnl_24h: 0.0,
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .est_profit
            .partial_cmp(&left.est_profit)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(left.symbol.cmp(&right.symbol))
    });
    rows
}

fn is_active_arbitrage_plan(row: &Value) -> bool {
    if row
        .get("intent")
        .and_then(Value::as_str)
        .unwrap_or("arbitrage")
        != "arbitrage"
    {
        return false;
    }
    let timestamp_ms = row
        .get("timestamp")
        .and_then(Value::as_str)
        .map(js_sys::Date::parse)
        .filter(|value| value.is_finite());
    timestamp_ms
        .map(|timestamp_ms| js_sys::Date::now() - timestamp_ms <= ACTIVE_ARBITRAGE_PLAN_WINDOW_MS)
        .unwrap_or(true)
}

fn plan_is_exit_workflow(row: &Value) -> bool {
    let text = format!(
        "{} {} {}",
        text_at(row, "intent", Language::En),
        text_at(row, "source", Language::En),
        text_at(row, "status", Language::En)
    )
    .to_ascii_lowercase();
    text.contains("liquidat")
        || text.contains("disable_exit")
        || text.contains("control_stop")
        || text.contains("exit")
}

fn control_symbol_state_is_managed_console(row: &Value) -> bool {
    row.get("lifecycle_state")
        .or_else(|| row.get("state"))
        .or_else(|| row.get("status"))
        .and_then(Value::as_str)
        .is_some_and(|state| {
            state.eq_ignore_ascii_case("active") || control_state_is_exit_workflow(state)
        })
}

fn control_symbol_state(control_symbols: &Value, symbol: &str) -> Option<String> {
    as_array(control_symbols).into_iter().find_map(|row| {
        let row_symbol = row
            .get("internal_symbol")
            .or_else(|| row.get("symbol"))
            .and_then(Value::as_str)?;
        if normalize_symbol_text(row_symbol) != normalize_symbol_text(symbol) {
            return None;
        }
        row.get("lifecycle_state")
            .or_else(|| row.get("state"))
            .or_else(|| row.get("status"))
            .and_then(Value::as_str)
            .map(str::to_string)
    })
}

fn control_state_blocks_symbol(state: &str) -> bool {
    matches!(
        state.to_ascii_lowercase().as_str(),
        "disabled"
            | "paused"
            | "frozen"
            | "disabledwithinventory"
            | "disabledclean"
            | "dustremaining"
            | "inventoryreviewrequired"
            | "manualinterventionrequired"
            | "failed"
            | "disablerequested"
            | "cancelingorders"
            | "liquidationplanning"
            | "liquidatingmarket"
            | "liquidatingpassive"
    )
}

fn control_state_is_exit_workflow(state: &str) -> bool {
    matches!(
        state.to_ascii_lowercase().as_str(),
        "disabledwithinventory"
            | "dustremaining"
            | "inventoryreviewrequired"
            | "manualinterventionrequired"
            | "failed"
            | "disablerequested"
            | "cancelingorders"
            | "liquidationplanning"
            | "liquidatingmarket"
            | "liquidatingpassive"
            | "passiveaskliquidate"
            | "marketliquidate"
    )
}

fn control_state_label(state: &str, lang: Language) -> String {
    match state.to_ascii_lowercase().as_str() {
        "active" => t(lang, "arbitraging").to_string(),
        "paused" => t(lang, "stopped").to_string(),
        "disablerequested"
        | "cancelingorders"
        | "liquidationplanning"
        | "liquidatingmarket"
        | "liquidatingpassive" => t(lang, "stop_liquidating").to_string(),
        "disabled" | "disabledclean" | "disabledwithinventory" => t(lang, "disable").to_string(),
        "frozen" => "Frozen".to_string(),
        "failed" => "Failed".to_string(),
        "manualinterventionrequired" => "Manual".to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn normalize_symbol_text(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}

pub(crate) fn position_rows(inventory: &[Value], lang: Language) -> Vec<PositionRow> {
    let mut rows = inventory
        .iter()
        .filter(|row| numeric_at(row, "total").abs() > 0.0)
        .map(|row| PositionRow {
            exchange: text_at(row, "exchange", lang),
            asset: text_at(row, "asset", lang),
            symbol: position_symbol_text(row, lang),
            total: text_at(row, "total", lang),
            available: row
                .get("effective_available")
                .or_else(|| row.get("available"))
                .map(|value| value_text(value, lang))
                .unwrap_or_else(|| "-".to_string()),
            reserved: text_at(row, "locally_reserved", lang),
            locked: text_at(row, "locked_by_exchange", lang),
            valuation: row
                .get("valuation_usdt")
                .and_then(Value::as_f64)
                .map(format_usdt)
                .unwrap_or_else(|| "-".to_string()),
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.exchange
            .cmp(&right.exchange)
            .then(left.symbol.cmp(&right.symbol))
            .then(left.asset.cmp(&right.asset))
    });
    rows
}

fn position_symbol_text(row: &Value, lang: Language) -> String {
    let asset = text_at(row, "asset", lang).trim().to_ascii_uppercase();
    if asset.is_empty() || matches!(asset.as_str(), "USDT" | "USDC" | "USD") {
        return "-".to_string();
    }
    row.get("symbol")
        .or_else(|| row.get("internal_symbol"))
        .map(|value| value_text(value, lang))
        .filter(|value| !value.trim().is_empty() && value != "-")
        .unwrap_or_else(|| format!("{asset}USDT"))
}

pub(crate) fn aux_asset_rows(
    inventory: &[Value],
    exchange_rows: &[Value],
    lang: Language,
) -> Vec<AuxAssetRow> {
    let mut rows = exchange_rows
        .iter()
        .filter_map(|exchange_row| {
            let exchange = text_at(exchange_row, "exchange", lang);
            let asset = exchange_row
                .get("fee_aux_asset")
                .and_then(Value::as_str)
                .filter(|asset| !asset.trim().is_empty())
                .map(str::to_string)
                .or_else(|| aux_asset_for_exchange(&exchange).map(str::to_string))?;
            let inventory_row = inventory.iter().find(|row| {
                row.get("exchange")
                    .and_then(Value::as_str)
                    .is_some_and(|value| exchange_matches(value, &exchange))
                    && row
                        .get("asset")
                        .and_then(Value::as_str)
                        .is_some_and(|value| value.eq_ignore_ascii_case(&asset))
            });
            Some(AuxAssetRow {
                exchange,
                asset,
                total: inventory_row
                    .map(|row| text_at(row, "total", lang))
                    .or_else(|| {
                        exchange_row
                            .get("fee_aux_total")
                            .map(|value| value_text(value, lang))
                    })
                    .unwrap_or_else(|| "0".to_string()),
                available: inventory_row
                    .and_then(|row| {
                        row.get("effective_available")
                            .or_else(|| row.get("available"))
                    })
                    .map(|value| value_text(value, lang))
                    .or_else(|| {
                        exchange_row
                            .get("fee_aux_available")
                            .map(|value| value_text(value, lang))
                    })
                    .unwrap_or_else(|| "0".to_string()),
                valuation: inventory_row
                    .and_then(|row| row.get("valuation_usdt").and_then(Value::as_f64))
                    .or_else(|| {
                        exchange_row
                            .get("fee_aux_valuation_usdt")
                            .and_then(Value::as_f64)
                    })
                    .map(format_usdt)
                    .unwrap_or_else(|| "-".to_string()),
                configured: inventory_row
                    .map(|row| numeric_at(row, "total").abs() > 0.0)
                    .unwrap_or_else(|| numeric_at(exchange_row, "fee_aux_total").abs() > 0.0),
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.exchange.cmp(&right.exchange));
    rows
}

pub(crate) fn exchange_statistic_rows(
    exchange_rows: &[Value],
    plans: &[Value],
    books: &[Value],
) -> Vec<ExchangeStatisticRow> {
    let mut exchanges = BTreeSet::new();
    for row in exchange_rows {
        exchanges.insert(canonical_exchange_name(&text_at(
            row,
            "exchange",
            Language::En,
        )));
    }
    for row in plans {
        exchanges.insert(canonical_exchange_name(&text_at(
            row,
            "exchange",
            Language::En,
        )));
    }
    for row in books {
        exchanges.insert(canonical_exchange_name(&text_at(
            row,
            "exchange",
            Language::En,
        )));
    }
    exchanges.retain(|exchange| !exchange.is_empty() && exchange != "-");

    let total_volume = plans
        .iter()
        .map(plan_notional)
        .filter(|value| value.is_finite())
        .sum::<f64>();

    let mut rows = exchanges
        .into_iter()
        .map(|exchange| {
            let exchange_plan_rows = plans
                .iter()
                .filter(|row| {
                    canonical_exchange_name(&text_at(row, "exchange", Language::En)) == exchange
                })
                .collect::<Vec<_>>();
            let day_volume = exchange_plan_rows
                .iter()
                .map(|row| plan_notional(row))
                .sum::<f64>();
            let maker_volume = exchange_plan_rows
                .iter()
                .filter(|row| order_role(row).contains("maker"))
                .map(|row| plan_notional(row))
                .sum::<f64>();
            let taker_volume = (day_volume - maker_volume).max(0.0);
            let health = exchange_rows.iter().find(|row| {
                canonical_exchange_name(&text_at(row, "exchange", Language::En)) == exchange
            });
            let connected = health.map(|row| bool_at(row, "connected")).unwrap_or(false)
                || health.map(|row| bool_at(row, "enabled")).unwrap_or(false);
            let fresh = health
                .map(|row| numeric_at(row, "fresh_symbol_count") as usize)
                .unwrap_or_else(|| exchange_book_count(books, &exchange, false));
            let stale = health
                .map(|row| numeric_at(row, "stale_symbol_count") as usize)
                .unwrap_or_else(|| exchange_book_count(books, &exchange, true));
            let latency_ms = health.and_then(|row| {
                row.get("avg_latency_ms")
                    .and_then(Value::as_f64)
                    .filter(|value| value.is_finite())
            });
            let asset_count = health
                .map(|row| numeric_at(row, "asset_count") as usize)
                .unwrap_or_default();
            let reconnects = health
                .map(|row| numeric_at(row, "reconnect_count") as u64)
                .unwrap_or_default();
            let parse_errors = health
                .map(|row| numeric_at(row, "parse_error_count") as u64)
                .unwrap_or_default();
            ExchangeStatisticRow {
                exchange,
                connected,
                day_volume,
                taker_volume,
                maker_volume,
                volume_share_pct: if total_volume > 0.0 {
                    day_volume / total_volume * 100.0
                } else {
                    0.0
                },
                api_calls: if reconnects == 0 && parse_errors == 0 {
                    "-".to_string()
                } else {
                    format!("{reconnects}|{parse_errors}")
                },
                ws_data: format!("{fresh}|{stale}"),
                private_ws: if connected || asset_count > 0 || !exchange_plan_rows.is_empty() {
                    format!(
                        "{}|{}|{}",
                        exchange_plan_rows.len(),
                        asset_count,
                        latency_ms.map(format_ms).unwrap_or_else(|| "-".to_string())
                    )
                } else {
                    "-".to_string()
                },
                latency: latency_ms.map(format_ms).unwrap_or_else(|| "-".to_string()),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .day_volume
            .partial_cmp(&left.day_volume)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(left.exchange.cmp(&right.exchange))
    });
    rows
}

pub(crate) fn spot_anomaly_rows(
    books: &[Value],
    plans: &[Value],
    opportunities: &[Value],
    disabled_symbols: &BTreeSet<String>,
    lang: Language,
) -> Vec<AnomalyTradeRow> {
    let mut rows = Vec::new();
    for row in plans.iter().filter(|row| plan_is_rejected(row)).take(40) {
        let symbol = text_at(row, "symbol", lang);
        rows.push(AnomalyTradeRow {
            inst_id_key: format!("{}@{}", text_at(row, "exchange", lang), symbol),
            price: price_text(row, "price"),
            reason: anomaly_text(row, lang)
                .filter(|value| value != "-")
                .unwrap_or_else(|| t(lang, "order_rejected").to_string()),
            class_name: "danger".to_string(),
            symbol,
        });
    }
    for row in books.iter().filter(|row| bool_at(row, "is_stale")).take(40) {
        let symbol = text_at(row, "symbol", lang);
        rows.push(AnomalyTradeRow {
            inst_id_key: format!("{}@{}", text_at(row, "exchange", lang), symbol),
            price: format!(
                "{} / {}",
                price_text(row, "best_bid"),
                price_text(row, "best_ask")
            ),
            reason: format!(
                "{}: {} ms",
                t(lang, "stale_book"),
                text_at(row, "book_age_ms", lang)
            ),
            class_name: "warn".to_string(),
            symbol,
        });
    }
    for row in opportunities
        .iter()
        .filter(|row| !bool_at(row, "accepted"))
        .take(40)
    {
        if let Some(reason) = anomaly_text(row, lang).filter(|value| value != "-") {
            let symbol = text_at(row, "symbol", lang);
            rows.push(AnomalyTradeRow {
                inst_id_key: format!(
                    "{}>{}@{}",
                    text_at(row, "buy_exchange", lang),
                    text_at(row, "sell_exchange", lang),
                    symbol
                ),
                price: format!(
                    "{} / {}",
                    price_text(row, "buy_vwap"),
                    price_text(row, "sell_vwap")
                ),
                reason,
                class_name: "warn".to_string(),
                symbol,
            });
        }
    }
    for symbol in disabled_symbols.iter().take(40) {
        rows.push(AnomalyTradeRow {
            symbol: symbol.clone(),
            inst_id_key: symbol.clone(),
            price: "-".to_string(),
            reason: t(lang, "symbol_disabled_reason").to_string(),
            class_name: "danger".to_string(),
        });
    }
    rows.sort_by(|left, right| {
        anomaly_rank(&left.class_name)
            .cmp(&anomaly_rank(&right.class_name))
            .then(left.symbol.cmp(&right.symbol))
    });
    rows.dedup_by(|left, right| {
        left.symbol == right.symbol
            && left.inst_id_key == right.inst_id_key
            && left.reason == right.reason
    });
    rows.truncate(80);
    rows
}

pub(crate) fn symbol_exchange_detail_rows(
    symbol_key: &str,
    books: &[&Value],
    plans: &[&Value],
    exchange_rows: &[Value],
    lang: Language,
) -> Vec<SymbolExchangeDetailRow> {
    if symbol_key.is_empty() {
        return Vec::new();
    }
    let mut exchanges = BTreeSet::new();
    for row in books {
        exchanges.insert(canonical_exchange_name(&text_at(
            row,
            "exchange",
            Language::En,
        )));
    }
    for row in plans {
        exchanges.insert(canonical_exchange_name(&text_at(
            row,
            "exchange",
            Language::En,
        )));
    }
    for row in exchange_rows {
        let exchange = canonical_exchange_name(&text_at(row, "exchange", Language::En));
        if !exchange.is_empty() && exchange != "-" {
            exchanges.insert(exchange);
        }
    }
    exchanges.retain(|exchange| !exchange.is_empty() && exchange != "-");

    let mut rows = exchanges
        .into_iter()
        .map(|exchange| {
            let book = books.iter().find(|row| {
                canonical_exchange_name(&text_at(row, "exchange", Language::En)) == exchange
            });
            let exchange_plans = plans
                .iter()
                .filter(|row| {
                    canonical_exchange_name(&text_at(row, "exchange", Language::En)) == exchange
                })
                .collect::<Vec<_>>();
            let health = exchange_rows.iter().find(|row| {
                canonical_exchange_name(&text_at(row, "exchange", Language::En)) == exchange
            });
            let book_stale = book.map(|row| bool_at(row, "is_stale")).unwrap_or(true);
            let connected = health.map(|row| bool_at(row, "connected")).unwrap_or(false)
                || health.map(|row| bool_at(row, "enabled")).unwrap_or(false);
            let order_count = exchange_plans.len();
            let state = if book.is_some() && !book_stale {
                t(lang, "fresh").to_string()
            } else if book.is_some() {
                t(lang, "stale").to_string()
            } else if connected || order_count > 0 {
                t(lang, "pending").to_string()
            } else {
                t(lang, "offline").to_string()
            };
            let class_name = if book.is_some() && !book_stale {
                "good"
            } else if book.is_some() || connected || order_count > 0 {
                "warn"
            } else {
                "bad"
            }
            .to_string();
            SymbolExchangeDetailRow {
                exchange,
                bid: book
                    .map(|row| price_text(row, "best_bid"))
                    .unwrap_or_else(|| "-".to_string()),
                ask: book
                    .map(|row| price_text(row, "best_ask"))
                    .unwrap_or_else(|| "-".to_string()),
                spread_bps: book
                    .map(|row| book_spread_bps_text(row))
                    .unwrap_or_else(|| "-".to_string()),
                age_ms: book
                    .map(|row| text_at(row, "book_age_ms", lang))
                    .unwrap_or_else(|| "-".to_string()),
                order_count,
                notional: exchange_plans
                    .iter()
                    .map(|row| plan_notional(row))
                    .sum::<f64>(),
                state,
                class_name,
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        symbol_exchange_rank(&left.class_name)
            .cmp(&symbol_exchange_rank(&right.class_name))
            .then(left.exchange.cmp(&right.exchange))
    });
    rows
}

fn symbol_exchange_rank(class_name: &str) -> u8 {
    match class_name {
        "good" => 0,
        "warn" => 1,
        _ => 2,
    }
}

fn exchange_book_count(books: &[Value], exchange: &str, stale: bool) -> usize {
    books
        .iter()
        .filter(|row| canonical_exchange_name(&text_at(row, "exchange", Language::En)) == exchange)
        .filter(|row| bool_at(row, "is_stale") == stale)
        .count()
}

fn plan_notional(row: &Value) -> f64 {
    numeric_at(row, "notional").max(numeric_at(row, "quote_quantity"))
}

fn order_role(row: &Value) -> String {
    format!(
        "{} {}",
        text_at(row, "liquidity_role", Language::En),
        text_at(row, "source", Language::En)
    )
    .to_ascii_lowercase()
}

fn plan_is_rejected(row: &Value) -> bool {
    text_at(row, "status", Language::En)
        .to_ascii_lowercase()
        .contains("reject")
        || row
            .get("rejection_reason")
            .map(|value| value_text(value, Language::En))
            .is_some_and(|value| !value.trim().is_empty() && value != "-")
}

fn anomaly_text(row: &Value, lang: Language) -> Option<String> {
    for key in [
        "rejection_reason",
        "reject_reasons",
        "warnings",
        "reason",
        "last_error",
    ] {
        if let Some(value) = row.get(key) {
            let text = match value {
                Value::Array(items) => items
                    .iter()
                    .map(|item| value_text(item, lang))
                    .filter(|item| !item.trim().is_empty() && item != "-")
                    .collect::<Vec<_>>()
                    .join(", "),
                _ => value_text(value, lang),
            };
            if !text.trim().is_empty() && text != "-" && text != "0" {
                return Some(text);
            }
        }
    }
    None
}

fn anomaly_rank(class_name: &str) -> u8 {
    match class_name {
        "danger" => 0,
        "warn" => 1,
        _ => 2,
    }
}

pub(crate) fn path_segment(value: &str) -> String {
    let mut escaped = String::new();
    for byte in value.as_bytes() {
        let keep = matches!(
            byte,
            b'A'..=b'Z'
                | b'a'..=b'z'
                | b'0'..=b'9'
                | b'-'
                | b'_'
                | b'.'
                | b'~'
        );
        if keep {
            escaped.push(*byte as char);
        } else {
            escaped.push_str(&format!("%{byte:02X}"));
        }
    }
    escaped
}

fn aux_asset_for_exchange(exchange: &str) -> Option<&'static str> {
    match normalize_exchange_key(exchange).as_str() {
        "binance" | "binancespot" => Some("BNB"),
        "okx" | "okxspot" => Some("OKB"),
        "gate" | "gateio" | "gate.io" => Some("GT"),
        "bitget" => Some("BGB"),
        "kucoin" => Some("KCS"),
        "mexc" => Some("MX"),
        "htx" | "huobi" => Some("HTX"),
        "bitmart" => Some("BMX"),
        _ => None,
    }
}

fn exchange_matches(left: &str, right: &str) -> bool {
    normalize_exchange_key(left) == normalize_exchange_key(right)
}

fn normalize_exchange_key(exchange: &str) -> String {
    exchange
        .to_ascii_lowercase()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect()
}

pub(crate) fn strategy_log_rows_for_category(
    logs: &Value,
    category: &str,
    lang: Language,
) -> Vec<LogRow> {
    let source_label = strategy_log_source_label(logs, lang);
    if let Some(events) = logs.get("events").and_then(Value::as_array) {
        let mut rows = events
            .iter()
            .rev()
            .filter(|event| category == "all" || strategy_log_event_matches(event, category))
            .map(|item| {
                let level = text_at(item, "level", lang);
                let message = item
                    .get("message")
                    .or_else(|| item.get("error"))
                    .or_else(|| item.get("warning"))
                    .map(|value| value_text(value, lang))
                    .unwrap_or_else(|| compact(item));
                let message = strategy_log_display_message(&message, lang);
                let timestamp = log_timestamp_text(item).unwrap_or_else(|| "-".to_string());
                LogRow {
                    class_name: log_class(&level, &message).to_string(),
                    timestamp,
                    level: if level == "-" {
                        "INFO".to_string()
                    } else {
                        level
                    },
                    source: source_label.clone(),
                    message,
                }
            })
            .collect::<Vec<_>>();
        if rows.is_empty() {
            rows.push(LogRow {
                timestamp: "-".to_string(),
                level: "INFO".to_string(),
                source: source_label,
                message: t(lang, "no_exception_logs").to_string(),
                class_name: "log-info".to_string(),
            });
        }
        return rows;
    }
    let source = match category {
        "all" => logs.get("lines"),
        other => logs
            .get("categories")
            .and_then(|categories| categories.get(other)),
    }
    .or_else(|| logs.get("lines"));
    let mut rows = Vec::new();
    if let Some(Value::Array(items)) = source {
        for item in items.iter().rev() {
            let level = text_at(item, "level", lang);
            let message = item
                .get("message")
                .or_else(|| item.get("error"))
                .or_else(|| item.get("warning"))
                .map(|value| value_text(value, lang))
                .unwrap_or_else(|| compact(item));
            let message = strategy_log_display_message(&message, lang);
            let timestamp = log_timestamp_text(item).unwrap_or_else(|| "-".to_string());
            rows.push(LogRow {
                class_name: log_class(&level, &message).to_string(),
                timestamp,
                level: if level == "-" {
                    "INFO".to_string()
                } else {
                    level
                },
                source: source_label.clone(),
                message,
            });
        }
    } else if let Some(source) = source {
        collect_log_value(&mut rows, &source_label, source, lang);
    }
    if rows.is_empty() {
        rows.push(LogRow {
            timestamp: "-".to_string(),
            level: "INFO".to_string(),
            source: source_label,
            message: t(lang, "no_exception_logs").to_string(),
            class_name: "log-info".to_string(),
        });
    }
    rows
}

pub(crate) fn strategy_log_source_text(logs: &Value, lang: Language) -> String {
    let label = strategy_log_source_label(logs, lang);
    if logs.get("events").is_some()
        || logs.get("configured").is_some()
        || logs.get("readable").is_some()
        || logs.get("event_count").is_some()
    {
        let target = text_at(logs, "target", lang);
        let path = text_at(logs, "path", lang);
        let source = if target != "-" {
            target
        } else if path != "-" {
            path
        } else {
            "-".to_string()
        };
        let mut parts = vec![format!("{label}: {source}")];
        if let Some(configured) = logs.get("configured").and_then(Value::as_bool) {
            parts.push(format!(
                "{} {}",
                t(lang, "configured"),
                bool_text(configured, lang)
            ));
        }
        if let Some(readable) = logs.get("readable").and_then(Value::as_bool) {
            let readable_label = match lang {
                Language::Zh => "可读",
                Language::En => "Readable",
            };
            parts.push(format!("{readable_label} {}", bool_text(readable, lang)));
        }
        let event_count = logs.get("event_count").and_then(Value::as_u64).or_else(|| {
            logs.get("events")
                .and_then(Value::as_array)
                .map(|events| events.len() as u64)
        });
        if let Some(event_count) = event_count {
            parts.push(format!("{} {event_count}", t(lang, "event_count")));
        }
        let read_error = text_at(logs, "read_error", lang);
        if read_error != "-" {
            parts.push(format!("error {read_error}"));
        }
        return parts.join(" · ");
    }
    let path = text_at(logs, "path", lang);
    let pointer = text_at(logs, "source_pointer", lang);
    if pointer == "-" {
        format!("{label}: {path}")
    } else {
        format!("{label}: {path} ({pointer})")
    }
}

fn strategy_log_source_label(logs: &Value, lang: Language) -> String {
    let source = text_at(logs, "source", Language::En).to_ascii_lowercase();
    let path = text_at(logs, "path", Language::En).to_ascii_lowercase();
    let target = text_at(logs, "target", Language::En).to_ascii_lowercase();
    let haystack = format!("{source} {path} {target}");
    let is_cross_arb = haystack.contains("cross_arb")
        || haystack.contains("cross-arb")
        || haystack.contains("cross_exchange")
        || haystack.contains("cross-exchange");
    let is_funding_arb = haystack.contains("funding");
    let is_spot_arb = haystack.contains("spot_arb")
        || haystack.contains("spot_spot")
        || haystack.contains("strategy_runtime")
        || haystack.contains("control_panel/strategy_");
    match (lang, is_cross_arb, is_funding_arb, is_spot_arb) {
        (Language::Zh, true, _, _) => "跨所合约套利".to_string(),
        (Language::Zh, _, true, _) => "资金费率套利".to_string(),
        (Language::Zh, _, _, true) => "现货套利".to_string(),
        (Language::Zh, _, _, _) => "策略日志".to_string(),
        (Language::En, true, _, _) => "cross-arb".to_string(),
        (Language::En, _, true, _) => "funding-arb".to_string(),
        (Language::En, _, _, true) => "spot-arb".to_string(),
        (Language::En, _, _, _) => "strategy".to_string(),
    }
}

fn strategy_log_display_message(message: &str, lang: Language) -> String {
    if !lang.is_zh() {
        return message.to_string();
    }
    translate_cross_arb_trade_event_message(message)
        .or_else(|| translate_strategy_log_message(message))
        .unwrap_or_else(|| message.to_string())
}

fn translate_cross_arb_trade_event_message(message: &str) -> Option<String> {
    let lower = message.to_ascii_lowercase();
    let action = kv_log_value(message, "action").unwrap_or_else(|| "-".to_string());
    let lifecycle = kv_log_value(message, "lifecycle").unwrap_or_else(|| action.clone());
    if !lower.contains("cross-arb trade event")
        && !lower.contains("cross-arb emergency close")
        && !action.starts_with("cross_arb_")
        && !lifecycle.starts_with("cross_arb_")
    {
        return None;
    }
    let symbol = kv_log_value(message, "symbol")
        .or_else(|| kv_log_value(message, "canonical_symbol"))
        .unwrap_or_else(|| "-".to_string());
    let operation = zh_cross_arb_operation(&action, &lifecycle);
    let reason = kv_log_value(message, "failure_reason")
        .or_else(|| kv_log_value(message, "emergency_trigger_reason"))
        .unwrap_or_default();
    let reason = zh_cross_arb_failure_reason(&reason);
    let reason = if reason.is_empty() {
        "无".to_string()
    } else {
        reason
    };
    Some(format!(
        "策略日志=跨所合约套利 | 交易对={} | 执行操作={} | 原因={}",
        symbol, operation, reason
    ))
}

fn translate_strategy_log_message(message: &str) -> Option<String> {
    let lower = message.to_ascii_lowercase();
    if lower.contains("spot_spot_taker_arbitrage is disabled in config") {
        return Some("现货套利策略已在配置中禁用。".to_string());
    }
    if lower.contains("spot_spot_taker_arbitrage replay complete") {
        return Some(format!(
            "回放完成：事件={}，机会={}，通过={}，净收益={}",
            kv_token(message, "events=").unwrap_or("-"),
            kv_token(message, "opportunities=").unwrap_or("-"),
            kv_token(message, "accepted=").unwrap_or("-"),
            kv_token(message, "net_pnl=").unwrap_or("-")
        ));
    }
    if lower.contains("spot_spot_taker_arbitrage loaded") && lower.contains("common symbols") {
        return Some(format!(
            "现货套利已加载 {} 个两边交易所都支持的交易对。",
            token_after(message, "loaded").unwrap_or("-")
        ));
    }
    if lower.contains("initial entry completed") && lower.contains("symbol is now arbitraging") {
        return Some(format!(
            "初期建仓完成：{} 已进入套利管理。",
            token_after(message, "completed").unwrap_or("-")
        ));
    }
    if lower.contains("initial entry detected existing")
        && lower.contains("symbol is now arbitraging")
    {
        return Some(format!(
            "检测到已有双边底仓：{} 已进入套利管理。",
            token_after(message, "existing").unwrap_or("-")
        ));
    }
    if lower.contains("orderbook fetch failed") {
        return Some(format!(
            "盘口获取失败：交易对={}，MEXC 成功={}，CoinEx 成功={}。",
            kv_token(message, "symbol=").unwrap_or("-"),
            zh_bool(kv_token(message, "mexc_ok=").unwrap_or("-")),
            zh_bool(kv_token(message, "coinex_ok=").unwrap_or("-"))
        ));
    }
    if lower.contains("spot live order blocked by validation") {
        return Some(format!(
            "实盘现货订单被校验拦截：计划={}，交易所={}，交易对={}，原因={}。",
            kv_token(message, "plan_id=").unwrap_or("-"),
            kv_token(message, "exchange=").unwrap_or("-"),
            kv_token(message, "symbol=").unwrap_or("-"),
            kv_rest(message, "reason=").unwrap_or("-")
        ));
    }
    if lower.contains("spot live order submitted") {
        return Some(format!(
            "实盘现货订单已提交：计划={}，交易所={}，交易对={}，方向={}，数量={}，价格={}，订单={}，状态={}。",
            kv_token(message, "plan_id=").unwrap_or("-"),
            kv_token(message, "exchange=").unwrap_or("-"),
            kv_token(message, "symbol=").unwrap_or("-"),
            kv_token(message, "side=").unwrap_or("-"),
            kv_token(message, "quantity=").unwrap_or("-"),
            kv_token(message, "price=").unwrap_or("-"),
            kv_token(message, "order_id=").unwrap_or("-"),
            kv_token(message, "status=").unwrap_or("-")
        ));
    }
    if lower.contains("spot live order submit failed") {
        return Some(format!(
            "实盘现货订单提交失败：计划={}，交易所={}，交易对={}，方向={}，错误={}。",
            kv_token(message, "plan_id=").unwrap_or("-"),
            kv_token(message, "exchange=").unwrap_or("-"),
            kv_token(message, "symbol=").unwrap_or("-"),
            kv_token(message, "side=").unwrap_or("-"),
            text_after_last(message, ": ").unwrap_or("-")
        ));
    }
    if lower.contains("spot live order poll failed") {
        return Some(format!(
            "实盘现货订单轮询失败：计划={}，交易所={}，交易对={}，订单={}，错误={}。",
            kv_token(message, "plan_id=").unwrap_or("-"),
            kv_token(message, "exchange=").unwrap_or("-"),
            kv_token(message, "symbol=").unwrap_or("-"),
            kv_token(message, "order_id=").unwrap_or("-"),
            text_after_last(message, ": ").unwrap_or("-")
        ));
    }
    if lower.contains("runtime publisher requested but spot control service is unavailable") {
        return Some("已配置运行时发布器，但现货控制服务不可用。".to_string());
    }
    if lower.contains("control liquidation skipped")
        && lower.contains("runtime snapshot unavailable")
    {
        return Some(format!(
            "控制清仓跳过 {}：缺少运行时快照。",
            token_after(message, "skipped")
                .unwrap_or("-")
                .trim_end_matches(':')
        ));
    }
    if lower.contains("control liquidation skipped") && lower.contains("symbol rules unavailable") {
        return Some(format!(
            "控制清仓跳过 {}：缺少交易规则。",
            token_after(message, "skipped")
                .unwrap_or("-")
                .trim_end_matches(':')
        ));
    }
    if lower.contains("control liquidation lifecycle completion rejected") {
        return Some(format!(
            "控制清仓生命周期完成被拒绝：交易对={}，错误={}。",
            kv_token(message, "symbol=").unwrap_or("-"),
            kv_rest(message, "errors=").unwrap_or("-")
        ));
    }
    if lower.contains("control liquidation completed") {
        return Some(format!(
            "控制清仓完成：交易对={}，最终状态={}。",
            kv_token(message, "symbol=").unwrap_or("-"),
            kv_token(message, "final_state=").unwrap_or("-")
        ));
    }
    if lower.contains("initial entry live dry-run skipped")
        && lower.contains("symbol rules are unavailable")
    {
        return Some(format!(
            "初期建仓干跑跳过 {}：缺少交易规则。",
            token_after(message, "skipped").unwrap_or("-")
        ));
    }
    if lower.contains("initial entry live dry-run skipped")
        && lower.contains("both entry books are required")
    {
        return Some(format!(
            "初期建仓干跑跳过 {}：需要两个交易所的可用盘口。",
            token_after(message, "skipped").unwrap_or("-")
        ));
    }
    if lower.contains("initial entry live dry-run plan rejected") {
        return Some(format!(
            "初期建仓干跑计划被拒绝：交易对={}，交易所={}，原因={}。",
            token_after(message, "rejected").unwrap_or("-"),
            token_after(message, "on")
                .unwrap_or("-")
                .trim_end_matches(':'),
            text_after_last(message, ": ").unwrap_or("-")
        ));
    }
    if lower.contains("preflight skipped unsupported spot exchange") {
        return Some(format!(
            "预检跳过不支持的现货交易所：{}。",
            message.split_whitespace().last().unwrap_or("-")
        ));
    }
    if lower.contains("preflight failed to fetch rest orderbook") {
        return Some(format!(
            "预检获取 REST 盘口失败：交易所={}，交易对={}，错误={}。",
            kv_token(message, "exchange=").unwrap_or("-"),
            kv_token(message, "symbol=")
                .unwrap_or("-")
                .trim_end_matches(':'),
            text_after_last(message, ": ").unwrap_or("-")
        ));
    }
    if lower.contains("failed to record arbitrage analytics jsonl") {
        return Some(format!(
            "套利分析 JSONL 写入失败：{}。",
            text_after_last(message, ": ").unwrap_or("-")
        ));
    }
    if lower.contains("control command accepted") {
        return Some(format!(
            "控制命令已接收：命令 ID={}，类型={}，详情={}。",
            kv_token(message, "command_id=").unwrap_or("-"),
            kv_token(message, "type=").unwrap_or("-"),
            kv_rest(message, "details=").unwrap_or("-")
        ));
    }
    if lower.contains("failed to reload spot control lifecycle store") {
        return Some(format!(
            "重新加载现货生命周期状态失败：{}。",
            text_after_last(message, ": ").unwrap_or("-")
        ));
    }
    if lower.contains("spot_spot_taker_arbitrage recorder jsonl error") {
        return Some(format!(
            "现货套利记录器写入 JSONL 失败：{}。",
            text_after_last(message, ": ").unwrap_or("-")
        ));
    }
    if lower.contains("spot_spot_taker_arbitrage recorder csv error") {
        return Some(format!(
            "现货套利记录器写入 CSV 失败：{}。",
            text_after_last(message, ": ").unwrap_or("-")
        ));
    }
    None
}

fn kv_log_value(message: &str, key: &str) -> Option<String> {
    let marker = format!("{key}=");
    let rest = message.split_once(&marker)?.1.trim_start();
    if rest.is_empty() {
        return None;
    }
    if let Some(stripped) = rest.strip_prefix('"') {
        let mut escaped = false;
        let mut value = String::new();
        for ch in stripped.chars() {
            if escaped {
                value.push(ch);
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                break;
            } else {
                value.push(ch);
            }
        }
        return normalized_log_value(value);
    }
    normalized_log_value(
        rest.split_whitespace()
            .next()
            .unwrap_or_default()
            .trim_matches('"')
            .trim_end_matches(',')
            .to_string(),
    )
}

fn normalized_log_value(value: String) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value == "-" || value.eq_ignore_ascii_case("null") {
        None
    } else {
        Some(value.to_string())
    }
}

fn zh_cross_arb_operation(action: &str, lifecycle: &str) -> String {
    let value = if lifecycle != "-" && !lifecycle.is_empty() {
        lifecycle
    } else {
        action
    };
    let lower = value.to_ascii_lowercase();
    let text = match lower.as_str() {
        "cross_arb_open_decision_audit" => "开仓决策审核",
        "cross_arb_open" | "open" => "开仓执行",
        "cross_arb_close" | "close" => "平仓执行",
        "cross_arb_emergency_close" | "emergency_close" => "应急平仓",
        "cross_arb_order" | "order" => "订单提交",
        "cross_arb_fill" | "fill" => "成交回报",
        "cross_arb_pair_execution" | "pair_execution" => "双腿执行",
        _ if lower.contains("open_decision") => "开仓决策审核",
        _ if lower.contains("emergency_close") => "应急平仓",
        _ if lower.contains("close") => "平仓执行",
        _ if lower.contains("open") => "开仓执行",
        _ if lower.contains("fill") => "成交回报",
        _ if lower.contains("order") => "订单提交",
        _ => value,
    };
    text.to_string()
}

fn zh_cross_arb_failure_reason(reason: &str) -> String {
    let reason = reason.trim();
    if reason.is_empty() || reason == "-" {
        return String::new();
    }
    let segments = reason
        .split(';')
        .map(zh_cross_arb_failure_reason_segment)
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() > 1 {
        return segments.join("; ");
    }
    if let Some(segment) = segments.into_iter().next() {
        return segment;
    }
    String::new()
}

fn zh_cross_arb_failure_reason_segment(reason: &str) -> String {
    let reason = reason.trim();
    if reason.is_empty() || reason == "-" {
        return String::new();
    }
    let lower = reason.to_ascii_lowercase();
    let text = match lower.as_str() {
        "below_min_net_profit" => "低于最低净利润阈值",
        "insufficient_top_depth" => "盘口顶层深度不足",
        "close-only control is enabled" => "已启用只平仓控制，禁止新开仓",
        "new entries are paused by control config" => "控制配置已暂停新开仓",
        "new entries are stopped by runtime deadline" => "运行时截止时间已停止新开仓",
        "invalid_long_book" => "多头盘口无效",
        "invalid_short_book" => "空头盘口无效",
        "stale_long_book" => "多头盘口过期",
        "stale_short_book" => "空头盘口过期",
        "above_max_spread" => "价差超过最大允许阈值",
        "strategy_halted" => "策略已暂停",
        "max_open_bundles" => "已达到最大开仓组合数",
        "symbol_already_active" => "交易对已有活跃套利组合",
        "symbol_cooling_down" => "交易对处于冷却期",
        "exchange_position_limit" => "触发交易所持仓限制",
        "private websocket did not confirm fill before timeout; rest readback is disabled for the trading hot path" => {
            "私有 WebSocket 未在超时前确认成交，且热路径禁用了 REST 回读"
        }
        _ if lower.contains("private websocket did not confirm fill before timeout") => {
            "私有 WebSocket 未在超时前确认成交"
        }
        _ if lower.contains("status=expired")
            && lower.contains("accepted=true")
            && lower.contains("error=-") =>
        {
            return format!(
                "{} 已被交易所接受，但 IOC 限价单过期未成交",
                reason
                    .split_whitespace()
                    .take(2)
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }
        _ if lower.contains("open route") && lower.contains("is cooling down until") => {
            return reason
                .replace("open route", "开仓路线")
                .replace("is cooling down until", "冷却中，直到")
                .replace("after recent_partial_close_emergency_repair", "原因：最近部分平仓后应急修复")
                .replace("after recent_incomplete_open", "原因：最近开仓不完整")
                .replace("after single_leg_open_fill", "原因：最近单腿开仓成交")
                .replace("after completed close", "原因：最近完成平仓");
        }
        _ if lower.contains("live execution halted because unmanaged exchange positions were detected") =>
        {
            return reason.replace(
                "live execution halted because unmanaged exchange positions were detected",
                "检测到交易所存在程序未管理仓位，实盘执行已暂停",
            );
        }
        _ if lower.contains("short leg notional is below exchange minimum") => {
            "空头腿名义金额低于交易所最低下单额"
        }
        _ if lower.contains("long leg notional is below exchange minimum") => {
            "多头腿名义金额低于交易所最低下单额"
        }
        _ if lower.contains("quantity is below exchange minimum") => "数量低于交易所最低下单要求",
        _ if lower.contains("top-of-book executable depth")
            && lower.contains("is below target notional") =>
        {
            return reason
                .replace("top-of-book executable depth", "盘口顶层可执行深度")
                .replace("is below target notional", "低于目标名义金额");
        }
        _ if lower.contains("raw spread")
            && lower.contains("is below min open raw spread") =>
        {
            return reason
                .replace("raw spread", "原始价差")
                .replace("is below min open raw spread", "低于开仓最低原始价差")
                .replace("live ", "实盘");
        }
        _ if lower.contains("expected net edge")
            && lower.contains("is below min open net edge") =>
        {
            return reason
                .replace("expected net edge", "预期净边际")
                .replace("is below min open net edge", "低于开仓最低净边际")
                .replace("live ", "实盘");
        }
        _ if lower.contains("live raw spread")
            && lower.contains("is below execution quality min") =>
        {
            return reason
                .replace("live raw spread", "实盘原始价差")
                .replace("is below execution quality min", "低于执行质量最低要求");
        }
        _ if lower.contains("live expected net edge")
            && lower.contains("is below execution quality min") =>
        {
            return reason
                .replace("live expected net edge", "实盘预期净边际")
                .replace("is below execution quality min", "低于执行质量最低要求");
        }
        _ if lower.contains("display-only row has no executable order drafts") => {
            "该机会仅展示，无可执行订单草稿"
        }
        _ if lower.contains("private_ws_confirmation_timeout") => "私有 WebSocket 成交确认超时",
        _ if lower.contains("emergency close not filled") => "应急平仓未成交",
        _ if lower.contains("not filled") => "订单未成交",
        _ if lower.contains("submit_failed") => "订单提交失败",
        _ if lower.contains("rejected") => "订单被拒绝",
        _ if lower.contains("timeout") => "执行超时",
        _ => reason,
    };
    text.to_string()
}

fn kv_token<'a>(message: &'a str, key: &str) -> Option<&'a str> {
    message
        .split_once(key)
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .filter(|value| !value.is_empty())
}

fn kv_rest<'a>(message: &'a str, key: &str) -> Option<&'a str> {
    message
        .split_once(key)
        .map(|(_, rest)| rest.trim())
        .filter(|value| !value.is_empty())
}

fn token_after<'a>(message: &'a str, marker: &str) -> Option<&'a str> {
    message
        .split_whitespace()
        .skip_while(|token| *token != marker)
        .nth(1)
}

fn text_after_last<'a>(message: &'a str, marker: &str) -> Option<&'a str> {
    message
        .rsplit_once(marker)
        .map(|(_, rest)| rest.trim())
        .filter(|value| !value.is_empty())
}

fn zh_bool(value: &str) -> &str {
    match value {
        "true" => "是",
        "false" => "否",
        other => other,
    }
}

pub(crate) fn strategy_log_count(logs: &Value, category: &str) -> usize {
    logs.get("counts")
        .and_then(|counts| counts.get(category))
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .or_else(|| {
            if let Some(events) = logs.get("events").and_then(Value::as_array) {
                return Some(if category == "all" {
                    events.len()
                } else {
                    events
                        .iter()
                        .filter(|event| strategy_log_event_matches(event, category))
                        .count()
                });
            }
            let source = if category == "all" {
                logs.get("lines")
            } else {
                logs.get("categories")
                    .and_then(|categories| categories.get(category))
            };
            source.and_then(Value::as_array).map(Vec::len)
        })
        .unwrap_or_default()
}

fn strategy_log_event_matches(event: &Value, category: &str) -> bool {
    let level = text_at(event, "level", Language::En).to_ascii_lowercase();
    let message = text_at(event, "message", Language::En).to_ascii_lowercase();
    match category {
        "error" => level == "error" || message.contains("error") || message.contains("failed"),
        "warn" => level == "warn" || level == "warning" || message.contains("warn"),
        "trade" => {
            message.contains("order")
                || message.contains("fill")
                || message.contains("trade")
                || message.contains("pnl")
        }
        "control" => message.contains("control") || message.contains("command"),
        "balance" => message.contains("balance") || message.contains("equity"),
        "market" => {
            message.contains("book") || message.contains("market") || message.contains("spread")
        }
        "info" => level == "info" || level == "-",
        _ => false,
    }
}

pub(crate) fn collect_log_value(
    rows: &mut Vec<LogRow>,
    source: &str,
    value: &Value,
    lang: Language,
) {
    match value {
        Value::Array(items) => {
            for item in items.iter().rev().take(80) {
                let level = text_at(item, "level", lang);
                let message = item
                    .get("message")
                    .or_else(|| item.get("error"))
                    .or_else(|| item.get("warning"))
                    .map(|value| value_text(value, lang))
                    .unwrap_or_else(|| compact(item));
                let timestamp = log_timestamp_text(item).unwrap_or_else(|| "-".to_string());
                rows.push(LogRow {
                    class_name: log_class(&level, &message).to_string(),
                    timestamp,
                    level: if level == "-" {
                        "INFO".to_string()
                    } else {
                        level
                    },
                    source: source.to_string(),
                    message,
                });
            }
        }
        Value::Object(map) => {
            for (key, item) in map.iter().take(80) {
                if item.is_array() {
                    collect_log_value(rows, key, item, lang);
                } else if item.is_object() {
                    let message = display_compact(item);
                    let timestamp = log_timestamp_text(item).unwrap_or_else(|| "-".to_string());
                    rows.push(LogRow {
                        class_name: log_class(key, &message).to_string(),
                        timestamp,
                        level: key.to_ascii_uppercase(),
                        source: source.to_string(),
                        message,
                    });
                }
            }
        }
        _ => {}
    }
}

pub(crate) fn disabled_symbol_set(disabled: &Value, control_symbols: &Value) -> BTreeSet<String> {
    let mut symbols = BTreeSet::new();
    collect_symbol_strings(disabled, &mut symbols);
    for row in as_array(control_symbols) {
        let state = row
            .get("lifecycle_state")
            .or_else(|| row.get("state"))
            .or_else(|| row.get("status"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if control_state_blocks_symbol(&state) {
            if let Some(symbol) = row
                .get("internal_symbol")
                .or_else(|| row.get("symbol"))
                .and_then(Value::as_str)
            {
                symbols.insert(normalize_symbol_text(symbol));
            }
        }
    }
    symbols
}

pub(crate) fn collect_symbol_strings(value: &Value, output: &mut BTreeSet<String>) {
    match value {
        Value::String(value) if looks_like_symbol(value) => {
            output.insert(value.clone());
        }
        Value::Array(values) => {
            for value in values {
                collect_symbol_strings(value, output);
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                if looks_like_symbol(key) {
                    output.insert(key.clone());
                }
                collect_symbol_strings(value, output);
            }
        }
        _ => {}
    }
}

pub(crate) fn exchange_connected(exchange_rows: &[Value], exchange: &str) -> bool {
    exchange_rows.iter().any(|row| {
        row.get("exchange").and_then(Value::as_str) == Some(exchange) && bool_at(row, "connected")
    })
}

pub(crate) fn exchange_last_update(
    exchange_rows: &[Value],
    exchange: &str,
    lang: Language,
) -> String {
    exchange_rows
        .iter()
        .find(|row| row.get("exchange").and_then(Value::as_str) == Some(exchange))
        .map(|row| {
            let value = text_at(row, "last_book_update_at", lang);
            if value == "-" {
                text_at(row, "last_message_at", lang)
            } else {
                value
            }
        })
        .unwrap_or_else(|| "-".to_string())
}

pub(crate) fn residual_asset_valuation_usdt(inventory: &[Value]) -> f64 {
    inventory
        .iter()
        .filter(|row| {
            row.get("asset")
                .and_then(Value::as_str)
                .is_some_and(|asset| !matches!(asset, "USDT" | "USDC" | "USD"))
        })
        .map(|row| numeric_at(row, "valuation_usdt"))
        .sum()
}

pub(crate) fn config_threshold_bps(config: &Value) -> Option<f64> {
    find_numeric_key(
        config,
        &[
            "min_raw_spread_bps",
            "min_net_spread_bps",
            "arbitrage_threshold_bps",
            "threshold_bps",
        ],
    )
}

pub(crate) fn config_notional(config: &Value) -> Option<f64> {
    find_numeric_key(
        config,
        &[
            "max_notional_per_trade",
            "default_pair_capital_usdt",
            "entry_notional_usdt",
            "notional_usdt",
        ],
    )
}

pub(crate) fn config_initial_entry_notional(config: &Value) -> Option<f64> {
    find_numeric_key(
        config,
        &[
            "initial_entry_notional_usdt",
            "initial_entry_notional",
            "entry_notional_usdt",
        ],
    )
}

pub(crate) fn config_max_arbitrage_symbols(config: &Value) -> Option<u64> {
    find_integer_key(
        config,
        &[
            "max_enabled_arbitrage_symbols",
            "max_arbitrage_symbols",
            "max_symbol_count",
        ],
    )
}

pub(crate) fn config_monitored_symbol_count(config: &Value) -> usize {
    config
        .get("enabled_symbols")
        .or_else(|| config.get("symbols"))
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default()
}

pub(crate) fn find_numeric_key(value: &Value, keys: &[&str]) -> Option<f64> {
    match value {
        Value::Object(map) => {
            for key in keys {
                if let Some(value) = map.get(*key).and_then(Value::as_f64) {
                    return Some(value);
                }
            }
            map.values().find_map(|value| find_numeric_key(value, keys))
        }
        Value::Array(values) => values
            .iter()
            .find_map(|value| find_numeric_key(value, keys)),
        _ => None,
    }
}

pub(crate) fn find_integer_key(value: &Value, keys: &[&str]) -> Option<u64> {
    match value {
        Value::Object(map) => {
            for key in keys {
                if let Some(value) = map.get(*key).and_then(Value::as_u64) {
                    return Some(value);
                }
            }
            map.values().find_map(|value| find_integer_key(value, keys))
        }
        Value::Array(values) => values
            .iter()
            .find_map(|value| find_integer_key(value, keys)),
        _ => None,
    }
}

pub(crate) fn page_count(len: usize, page_size: usize) -> usize {
    len.max(1).div_ceil(page_size.max(1))
}

pub(crate) fn page_start(page: usize, page_size: usize, len: usize) -> usize {
    page.saturating_mul(page_size).min(len)
}

pub(crate) fn symbol_base(symbol: &str) -> String {
    for quote in ["USDT", "USDC", "USD", "BTC", "ETH"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            return base.to_string();
        }
    }
    symbol.to_string()
}

pub(crate) fn looks_like_symbol(value: &str) -> bool {
    value.ends_with("USDT") || value.ends_with("USDC") || value.ends_with("USD")
}

pub(crate) fn log_class(level: &str, message: &str) -> &'static str {
    let text = format!("{level} {message}").to_ascii_lowercase();
    if text.contains("error") || text.contains("panic") || text.contains("fail") {
        "log-error"
    } else if text.contains("actual_pnl_usdt=-")
        || text.contains("realized_profit_usdt=-")
        || text.contains("pnl=-")
        || text.contains("亏损")
        || text.contains("loss")
    {
        "log-loss"
    } else if text.contains("actual_pnl_usdt=")
        || text.contains("realized_profit_usdt=")
        || text.contains(" profit")
        || text.contains("盈利")
    {
        "log-profit"
    } else if text.contains("warn") || text.contains("stale") || text.contains("risk") {
        "log-warn"
    } else if text.contains("accepted") || text.contains("connected") || text.contains("ok") {
        "log-success"
    } else {
        "log-info"
    }
}

pub(crate) fn record_balance_history(inventory: &Value) -> Value {
    let mut history = storage()
        .and_then(|storage| storage.get_item(BALANCE_HISTORY_KEY).ok().flatten())
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
        .unwrap_or_else(|| json!([]));
    let balances = balance_summaries(inventory);
    let total = balances.iter().map(|row| row.total_usdt).sum::<f64>();
    let exchanges = balances
        .iter()
        .map(|row| json!({ "exchange": row.exchange, "total_usdt": row.total_usdt }))
        .collect::<Vec<_>>();
    let point = json!({
        "timestamp_ms": js_sys::Date::now(),
        "total_usdt": total,
        "exchanges": exchanges,
    });
    let mut values = history.as_array().cloned().unwrap_or_default();
    let first_total = values
        .first()
        .and_then(|row| row.get("total_usdt").and_then(Value::as_f64))
        .unwrap_or(total);
    let point = with_profit_usdt(point, first_total);
    let should_append = values
        .last()
        .and_then(|last| last.get("total_usdt").and_then(Value::as_f64))
        .map(|last| (last - total).abs() > 0.01 || values.len() < 2)
        .unwrap_or(true);
    if should_append {
        values.push(point);
    }
    let extra = values.len().saturating_sub(240);
    if extra > 0 {
        values.drain(0..extra);
    }
    history = Value::Array(values);
    if let Some(storage) = storage() {
        let _ = storage.set_item(BALANCE_HISTORY_KEY, &compact(&history));
    }
    history
}

pub(crate) fn profit_history_values(history: &Value) -> Vec<f64> {
    let values = history.as_array().cloned().unwrap_or_default();
    let first_total = values
        .first()
        .and_then(|item| item.get("total_usdt").and_then(Value::as_f64))
        .unwrap_or_default();
    values
        .iter()
        .filter_map(|item| {
            item.get("profit_usdt").and_then(Value::as_f64).or_else(|| {
                item.get("total_usdt")
                    .and_then(Value::as_f64)
                    .map(|total| total - first_total)
            })
        })
        .collect()
}

fn with_profit_usdt(mut point: Value, first_total: f64) -> Value {
    if let Some(total) = point.get("total_usdt").and_then(Value::as_f64) {
        point["profit_usdt"] = json!(total - first_total);
    }
    point
}

pub(crate) fn sparkline_points(values: &[f64], width: f64, height: f64, pad: f64) -> String {
    if values.is_empty() {
        return String::new();
    }
    if values.len() == 1 {
        let y = height / 2.0;
        return format!("{pad:.2},{y:.2} {:.2},{y:.2}", width - pad);
    }
    let min = values.iter().copied().fold(f64::INFINITY, f64::min);
    let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let range = (max - min).abs().max(0.000_001);
    let step = (width - pad * 2.0) / (values.len().saturating_sub(1) as f64);
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let x = pad + step * index as f64;
            let normalized = (*value - min) / range;
            let y = height - pad - normalized * (height - pad * 2.0);
            format!("{x:.2},{y:.2}")
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn text_at(value: &Value, key: &str, lang: Language) -> String {
    match value.get(key) {
        Some(value) if is_time_key(key) => {
            format_beijing_time_value(value).unwrap_or_else(|| value_text(value, lang))
        }
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => bool_text(*value, lang).to_string(),
        Some(Value::Array(values)) => format!("{}", values.len()),
        Some(Value::Object(values)) => format!("{} {}", values.len(), t(lang, "fields")),
        _ => "-".to_string(),
    }
}

pub(crate) fn bool_at(value: &Value, key: &str) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(false)
}

pub(crate) fn numeric_at(value: &Value, key: &str) -> f64 {
    value.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

pub(crate) fn optional_numeric_at(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(Value::as_f64)
}

pub(crate) fn value_text(value: &Value, lang: Language) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => bool_text(*value, lang).to_string(),
        Value::Array(values) => format!("{}", values.len()),
        Value::Object(values) => format!("{} {}", values.len(), t(lang, "fields")),
        _ => "-".to_string(),
    }
}

pub(crate) fn price_text(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(format_price)
        .unwrap_or_else(|| "-".to_string())
}

pub(crate) fn money_text(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|value| format!("{value:.4}"))
        .unwrap_or_else(|| "-".to_string())
}

pub(crate) fn coin_assets_text(row: &Value, lang: Language) -> String {
    let assets = row
        .get("coin_assets")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if assets.is_empty() {
        return "-".to_string();
    }
    assets
        .iter()
        .take(4)
        .map(|asset| {
            format!(
                "{} {} / {}",
                text_at(asset, "asset", lang),
                compact_amount(numeric_at(asset, "total")),
                compact_amount(numeric_at(asset, "effective_available"))
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn fee_aux_asset_text(row: &Value, lang: Language) -> String {
    let asset = text_at(row, "fee_aux_asset", lang);
    if asset == "-" {
        return "-".to_string();
    }
    format!(
        "{} {} / {}",
        asset,
        compact_amount(numeric_at(row, "fee_aux_total")),
        compact_amount(numeric_at(row, "fee_aux_available"))
    )
}

pub(crate) fn order_source_label(row: &Value, lang: Language) -> String {
    match row
        .get("source")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "exchange_open_order" => t(lang, "online_order").to_string(),
        "live_dry_run_plan" => t(lang, "dry_run_plan").to_string(),
        value if !value.trim().is_empty() => value.to_string(),
        _ => "-".to_string(),
    }
}

pub(crate) fn order_remaining_text(row: &Value, lang: Language) -> String {
    row.get("remaining_quantity")
        .or_else(|| row.get("quantity"))
        .map(|value| value_text(value, lang))
        .unwrap_or_else(|| "-".to_string())
}

pub(crate) fn order_ref_text(row: &Value, lang: Language) -> String {
    row.get("order_id")
        .or_else(|| row.get("client_order_id"))
        .map(|value| value_text(value, lang))
        .filter(|value| !value.trim().is_empty() && value != "null" && value != "-")
        .unwrap_or_else(|| "-".to_string())
}

fn compact_amount(value: f64) -> String {
    if value.abs() >= 1_000_000.0 {
        format!("{:.4}M", value / 1_000_000.0)
    } else if value.abs() >= 1_000.0 {
        format!("{:.4}K", value / 1_000.0)
    } else if value.abs() >= 1.0 {
        format!("{value:.6}")
    } else if value.abs() > 0.0 {
        format!("{value:.8}")
    } else {
        "0".to_string()
    }
}

pub(crate) fn format_usdt(value: f64) -> String {
    if value.abs() >= 1_000_000.0 {
        format!("${:.2}M", value / 1_000_000.0)
    } else if value.abs() >= 1_000.0 {
        format!("${:.2}K", value / 1_000.0)
    } else {
        format!("${value:.2}")
    }
}

pub(crate) fn format_usdt_precise(value: f64) -> String {
    format!("${value:.6}")
}

pub(crate) fn signed_usdt(value: f64) -> String {
    if value > 0.0 {
        format!("+{}", format_usdt(value))
    } else if value < 0.0 {
        format!("-{}", format_usdt(value.abs()))
    } else {
        format_usdt(0.0)
    }
}

pub(crate) fn signed_usdt_precise(value: f64) -> String {
    if value > 0.0 {
        format!("+{}", format_usdt_precise(value))
    } else if value < 0.0 {
        format!("-{}", format_usdt_precise(value.abs()))
    } else {
        format_usdt_precise(0.0)
    }
}

pub(crate) fn format_ms(value: f64) -> String {
    if value >= 1_000.0 {
        format!("{:.2}s", value / 1_000.0)
    } else {
        format!("{value:.0} ms")
    }
}

pub(crate) fn percent_width(value: f64, max: f64) -> String {
    if max <= 0.0 {
        "0".to_string()
    } else {
        format!("{:.2}", (value / max * 100.0).clamp(2.0, 100.0))
    }
}

pub(crate) fn bps_text(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(|value| format!("{value:.2}"))
        .unwrap_or_else(|| "-".to_string())
}

pub(crate) fn book_spread_bps_text(value: &Value) -> String {
    let bid = numeric_at(value, "best_bid");
    let ask = numeric_at(value, "best_ask");
    if bid <= 0.0 || ask <= 0.0 {
        return "-".to_string();
    }
    format!("{:.2}", (ask - bid) / ask * 10_000.0)
}

pub(crate) fn format_pct(value: f64) -> String {
    format!("{:.4}%", value * 100.0)
}

pub(crate) fn cross_arb_close_profit_now(row: &Value) -> f64 {
    optional_numeric_at(row, "close_candidate_profit_pct")
        .or_else(|| optional_numeric_at(row, "close_net_profit_pct"))
        .unwrap_or(0.0)
}

pub(crate) fn cross_arb_close_route(row: &Value, lang: Language) -> String {
    let exchange = text_at(row, "close_maker_exchange", lang);
    let side = text_at(row, "close_maker_side", lang);
    let price = optional_numeric_at(row, "close_maker_price")
        .map(format_usdt)
        .unwrap_or_else(|| "-".to_string());
    if exchange == "-" && side == "-" && price == "-" {
        "-".to_string()
    } else {
        format!("{exchange} {side} @ {price}")
    }
}

pub(crate) fn format_price(value: f64) -> String {
    if value.abs() >= 100.0 {
        format!("{value:.2}")
    } else if value.abs() >= 1.0 {
        format!("{value:.4}")
    } else {
        format!("{value:.8}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

pub(crate) fn profit_class(value: f64) -> &'static str {
    if value > 0.0 {
        "profit positive"
    } else if value < 0.0 {
        "profit negative"
    } else {
        "profit"
    }
}

pub(crate) fn is_spot_book(value: &Value) -> bool {
    let exchange = value
        .get("exchange")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let market_type = value
        .get("market_type")
        .and_then(Value::as_str)
        .unwrap_or("spot")
        .to_ascii_lowercase();
    market_type == "spot"
        && matches!(
            exchange.as_str(),
            "gateio" | "bitget" | "mexc" | "coinex" | "kucoin"
        )
}

pub(crate) fn cross_arb_string_set(rows: &[Value], keys: &[&str]) -> Vec<String> {
    rows.iter()
        .flat_map(|row| {
            keys.iter()
                .filter_map(|key| row.get(*key).and_then(Value::as_str))
                .map(str::to_string)
        })
        .filter(|value| !value.is_empty() && value != "-")
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(crate) fn cross_arb_exchange_volume(rows: &[Value], exchange: &str, liquidity: &str) -> String {
    format_usdt(
        rows.iter()
            .filter(|row| text_at(row, "exchange", Language::En).eq_ignore_ascii_case(exchange))
            .filter(|row| {
                let role = text_at(row, "liquidity_role", Language::En);
                role.is_empty() || role == "-" || role.eq_ignore_ascii_case(liquidity)
            })
            .map(|row| {
                numeric_at(row, "quote_quantity")
                    .max(numeric_at(row, "notional"))
                    .max(numeric_at(row, "total"))
            })
            .sum::<f64>(),
    )
}

pub(crate) fn cross_arb_exchange_orders(rows: &[Value], exchange: &str) -> usize {
    rows.iter()
        .filter(|row| text_at(row, "exchange", Language::En).eq_ignore_ascii_case(exchange))
        .filter(|row| text_at(row, "private_kind", Language::En).eq_ignore_ascii_case("fill"))
        .count()
}

pub(crate) fn cross_arb_exchange_taker_success(rows: &[Value], exchange: &str) -> String {
    let fills = rows
        .iter()
        .filter(|row| text_at(row, "exchange", Language::En).eq_ignore_ascii_case(exchange))
        .filter(|row| text_at(row, "private_kind", Language::En).eq_ignore_ascii_case("fill"))
        .collect::<Vec<_>>();
    let taker = fills
        .iter()
        .filter(|row| text_at(row, "liquidity_role", Language::En).eq_ignore_ascii_case("taker"))
        .count();
    let errors = rows
        .iter()
        .filter(|row| text_at(row, "exchange", Language::En).eq_ignore_ascii_case(exchange))
        .filter(|row| text_at(row, "private_kind", Language::En).eq_ignore_ascii_case("error"))
        .count();
    if taker + errors == 0 {
        "-".to_string()
    } else {
        format!("{:.1}%", taker as f64 / (taker + errors) as f64 * 100.0)
    }
}

pub(crate) fn cross_arb_symbol_capital(rows: &[Value], symbol: &str) -> f64 {
    rows.iter()
        .filter(|row| cross_arb_row_symbol(row).eq_ignore_ascii_case(symbol))
        .map(|row| {
            numeric_at(row, "executable_notional_usdt").max(numeric_at(row, "target_notional_usdt"))
        })
        .fold(0.0, f64::max)
}

pub(crate) fn cross_arb_symbol_exchanges(rows: &[Value], symbol: &str) -> String {
    let values = rows
        .iter()
        .filter(|row| cross_arb_row_symbol(row).eq_ignore_ascii_case(symbol))
        .flat_map(|row| {
            [
                text_at(row, "long_exchange", Language::En),
                text_at(row, "short_exchange", Language::En),
            ]
        })
        .filter(|value| !value.is_empty() && value != "-")
        .collect::<std::collections::BTreeSet<_>>();
    if values.is_empty() {
        "-".to_string()
    } else {
        values.into_iter().collect::<Vec<_>>().join(" / ")
    }
}

pub(crate) fn cross_arb_symbol_volume(rows: &[Value], symbol: &str) -> f64 {
    rows.iter()
        .filter(|row| cross_arb_row_symbol(row).eq_ignore_ascii_case(symbol))
        .map(|row| numeric_at(row, "quote_quantity").max(numeric_at(row, "notional")))
        .sum()
}

pub(crate) fn cross_arb_symbol_est(rows: &[Value], symbol: &str) -> f64 {
    rows.iter()
        .filter(|row| {
            text_at(row, "action", Language::En).eq_ignore_ascii_case("open")
                && text_at(row, "opportunity_id", Language::En)
                    .to_ascii_lowercase()
                    .contains(&symbol.replace('/', "").to_ascii_lowercase())
        })
        .map(|row| numeric_at(row, "expected_edge") * numeric_at(row, "max_notional_usdt"))
        .sum()
}

pub(crate) fn cross_arb_symbol_realized_pct(rows: &[Value], symbol: &str) -> String {
    let value = rows
        .iter()
        .filter(|row| cross_arb_row_symbol(row).eq_ignore_ascii_case(symbol))
        .map(|row| numeric_at(row, "close_net_profit_pct"))
        .sum::<f64>();
    format_pct(value)
}

fn cross_arb_row_symbol(row: &Value) -> String {
    text_at(row, "canonical_symbol", Language::En)
        .trim_matches('-')
        .to_string()
        .if_empty_then(|| text_at(row, "symbol", Language::En))
}

trait EmptyStringFallback {
    fn if_empty_then(self, fallback: impl FnOnce() -> String) -> String;
}

impl EmptyStringFallback for String {
    fn if_empty_then(self, fallback: impl FnOnce() -> String) -> String {
        if self.is_empty() {
            fallback()
        } else {
            self
        }
    }
}

pub(crate) fn is_spot_spot_opp(value: &Value) -> bool {
    let relationship = value
        .get("relationship_type")
        .and_then(Value::as_str)
        .unwrap_or("spot_spot")
        .to_ascii_lowercase();
    let buy_exchange = value
        .get("buy_exchange")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let sell_exchange = value
        .get("sell_exchange")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    relationship == "spot_spot"
        && matches!(
            buy_exchange.as_str(),
            "gateio" | "bitget" | "mexc" | "coinex" | "kucoin"
        )
        && matches!(
            sell_exchange.as_str(),
            "gateio" | "bitget" | "mexc" | "coinex" | "kucoin"
        )
}

pub(crate) fn bool_text(value: bool, lang: Language) -> &'static str {
    if value {
        t(lang, "yes")
    } else {
        t(lang, "no")
    }
}

pub(crate) fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(&display_value(value)).unwrap_or_else(|_| "{}".to_string())
}

pub(crate) fn compact(value: &Value) -> String {
    serde_json::to_string(&display_value(value)).unwrap_or_else(|_| "{}".to_string())
}

fn display_compact(value: &Value) -> String {
    serde_json::to_string(&display_value(value)).unwrap_or_else(|_| "{}".to_string())
}

fn display_value(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(display_value).collect()),
        Value::Object(values) => {
            let mut output = Map::new();
            for (key, value) in values {
                let value = if should_redact_display_key(key, values) {
                    Value::String("[redacted]".to_string())
                } else if is_time_key(key) {
                    format_beijing_time_value(value)
                        .map(Value::String)
                        .unwrap_or_else(|| display_value(value))
                } else {
                    display_value(value)
                };
                output.insert(key.clone(), value);
            }
            Value::Object(output)
        }
        _ => value.clone(),
    }
}

fn should_redact_display_key(key: &str, values: &Map<String, Value>) -> bool {
    let key = key.trim().to_ascii_lowercase();
    matches!(
        key.as_str(),
        "api_key"
            | "apikey"
            | "api_secret"
            | "apisecret"
            | "secret"
            | "passphrase"
            | "password"
            | "private_key"
            | "authorization"
            | "bearer_token"
            | "auth_token"
            | "access_token"
            | "refresh_token"
    ) || (key == "value" && object_is_sensitive_credential_field(values))
}

fn object_is_sensitive_credential_field(values: &Map<String, Value>) -> bool {
    let Some(field) = values.get("field").and_then(Value::as_str) else {
        return false;
    };
    let field = field.trim().to_ascii_lowercase();
    field.contains("secret")
        || field.contains("passphrase")
        || field.contains("password")
        || field.contains("private_key")
        || field == "api_key"
        || field == "key"
        || field == "token"
}

fn log_timestamp_text(value: &Value) -> Option<String> {
    let field_timestamp = [
        "timestamp",
        "occurred_at",
        "recorded_at",
        "updated_at",
        "created_at",
        "triggered_at",
        "last_snapshot_at",
        "last_book_update_at",
        "last_message_at",
        "timestamp_ms",
    ]
    .iter()
    .find_map(|key| value.get(*key).and_then(format_beijing_time_value));
    field_timestamp.or_else(|| {
        value
            .get("message")
            .and_then(Value::as_str)
            .and_then(log_timestamp_from_message)
    })
}

fn log_timestamp_from_message(message: &str) -> Option<String> {
    let trimmed = message.trim_start();
    if trimmed.is_empty() {
        return None;
    }
    let trimmed = trimmed.trim_start_matches('[');
    for length in [35usize, 30, 29, 25, 24, 23, 20, 19] {
        if trimmed.len() < length {
            continue;
        }
        let candidate = trimmed[..length]
            .trim_end_matches(']')
            .trim_end_matches(',')
            .trim();
        if let Some(timestamp) = format_beijing_time_text(candidate) {
            return Some(timestamp);
        }
    }
    None
}

fn is_time_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    if matches!(key.as_str(), "timestamp_ms" | "timestamp" | "time") {
        return true;
    }
    if key.ends_with("_ms") {
        return false;
    }
    key.ends_with("_at")
        || key.ends_with("_time")
        || key.ends_with("_until")
        || key.contains("timestamp")
}

pub(crate) fn format_beijing_time_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => format_beijing_time_text(value),
        Value::Number(value) => value
            .as_f64()
            .and_then(normalize_timestamp_ms)
            .map(format_beijing_timestamp_ms),
        _ => None,
    }
}

fn format_beijing_time_text(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value == "-" || value.eq_ignore_ascii_case("null") {
        return None;
    }
    if let Ok(timestamp) = value.parse::<f64>() {
        if let Some(timestamp_ms) = normalize_timestamp_ms(timestamp) {
            return Some(format_beijing_timestamp_ms(timestamp_ms));
        }
    }
    let timestamp_ms = js_sys::Date::parse(value);
    timestamp_ms
        .is_finite()
        .then(|| format_beijing_timestamp_ms(timestamp_ms))
}

fn normalize_timestamp_ms(value: f64) -> Option<f64> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    let abs = value.abs();
    if (1_000_000_000_000.0..100_000_000_000_000.0).contains(&abs) {
        Some(value)
    } else if (1_000_000_000.0..100_000_000_000.0).contains(&abs) {
        Some(value * 1000.0)
    } else {
        None
    }
}

fn format_beijing_timestamp_ms(timestamp_ms: f64) -> String {
    let timestamp = js_sys::Number::from(timestamp_ms + BEIJING_OFFSET_MS);
    let date = js_sys::Date::new(timestamp.as_ref());
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
        date.get_utc_full_year(),
        date.get_utc_month() + 1,
        date.get_utc_date(),
        date.get_utc_hours(),
        date.get_utc_minutes(),
        date.get_utc_seconds(),
        date.get_utc_milliseconds(),
    )
}

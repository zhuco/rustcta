use dioxus::prelude::*;
use serde_json::{json, Value};

use crate::api::{
    delete_cross_arb_exchange_config, fetch_cross_arb_exchange_config,
    fetch_cross_arb_instrument_data, fetch_cross_arb_settings, request_cross_arb_position_close,
    save_cross_arb_exchange_config, save_cross_arb_settings,
};
use crate::i18n::{s, t};
use crate::storage::save_active_view;
use crate::types::{
    ControlPanelView, CredentialAccountOption, CrossArbAccountRowData, CrossArbEventSummaryData,
    CrossArbExchangeConsoleRowData, CrossArbExchangeFormData, CrossArbExchangeStatusCardData,
    CrossArbInstrumentRowData, CrossArbOpportunityRowData, CrossArbPanelData,
    CrossArbPositionBundleRowData, CrossArbReadinessData, CrossArbResultRowData,
    CrossArbSaveResultData, CrossArbSettingsFormData, CrossArbSourceData, Language,
    StrategyLogPanelData,
};
use crate::types::SystemResourcePanelData;
use crate::ui::{LogPager, Metric, Pager, StatusPill};
use crate::utils::*;

#[component]
pub(crate) fn CrossArbPanel(
    cross_arb: Value,
    api_keys: Value,
    status: Value,
    processes: Value,
    strategy_logs: Value,
    token: String,
    mut message: Signal<String>,
    lang: Language,
    mut active_view: Signal<ControlPanelView>,
    mut api_key_exchange: Signal<String>,
    mut api_key_account: Signal<String>,
    mut api_key_namespace: Signal<String>,
) -> Element {
    let _ = &processes;
    let mut hedge_page = use_signal(|| 0usize);
    let mut symbol_page = use_signal(|| 0usize);
    let mut console_tab = use_signal(|| "exchange".to_string());
    let mut position_tab = use_signal(|| "positions".to_string());
    let mut strategy_log_tab = use_signal(|| "all".to_string());
    let mut strategy_log_page = use_signal(|| 0usize);
    let mut settings_dialog_open = use_signal(|| false);
    let source = CrossArbSourceData::from_dashboard(&cross_arb);
    let summary = source.summary.clone();
    let settings = source.settings.clone();
    let initial_settings =
        CrossArbSettingsFormData::from_settings(&settings, &api_keys, &source.exchanges, lang);
    let mut exchange_settings = use_signal(|| initial_settings.exchanges.clone());
    let mut symbol_text = use_signal(|| initial_settings.symbols_text.clone());
    let mut hedge_notional = use_signal(|| initial_settings.target_notional.clone());
    let mut max_hedge_notional = use_signal(|| initial_settings.max_notional.clone());
    let mut max_positions = use_signal(|| initial_settings.max_positions.clone());
    let mut min_open_spread = use_signal(|| initial_settings.min_open_spread.clone());
    let mut min_net_edge = use_signal(|| initial_settings.min_net_edge.clone());
    let mut close_profit = use_signal(|| initial_settings.close_profit.clone());
    let mut close_spread = use_signal(|| initial_settings.close_spread.clone());
    let mut execution_profile = use_signal(|| initial_settings.execution_profile.clone());
    let live_trading_enabled = use_signal(|| cross_arb_live_trading_enabled(&settings));
    let live_trading_pending = use_signal(|| false);
    let mut exchange_config_loaded = use_signal(|| false);
    let mut exchange_config_dirty = use_signal(|| false);
    let load_exchange_config_token = token.clone();
    let load_exchange_config_api_keys = api_keys.clone();
    let load_exchange_config_seed = source.exchanges.clone();
    let _exchange_config_bootstrap = use_future(move || {
        let token_value = load_exchange_config_token.clone();
        let api_keys_value = load_exchange_config_api_keys.clone();
        let seed_exchanges = load_exchange_config_seed.clone();
        async move {
            match fetch_cross_arb_exchange_config(&token_value).await {
                Ok(value) => {
                    let form = CrossArbSettingsFormData::from_settings(
                        &value,
                        &api_keys_value,
                        &seed_exchanges,
                        lang,
                    );
                    exchange_settings.set(form.exchanges);
                    exchange_config_loaded.set(true);
                    exchange_config_dirty.set(false);
                    if value
                        .get("cleaned_on_read")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                    {
                        let cleaned = value
                            .get("cleaned_invalid_count")
                            .and_then(Value::as_u64)
                            .unwrap_or_default();
                        message.set(format!("{}: {}", t(lang, "config_cleaned"), cleaned));
                    }
                }
                Err(error) => {
                    if !optional_exchange_config_error(&error) {
                        message.set(error);
                    }
                }
            }
        }
    });
    let opportunities = source.opportunities.clone();
    let hedge_records = source.hedge_records.clone();
    let private_events = source.private_events.clone();
    let risk_events = source.risk_events.clone();
    let instruments = source.instruments.clone();
    let mut instrument_rows = use_signal(|| instruments.clone());
    let mut instrument_feasibility = use_signal(|| source.instrument_feasibility.clone());
    let position_bundles = source.position_bundles.clone();
    let arbitrage_results = source.arbitrage_results.clone();
    let profit_summary = source.profit_summary.clone();
    let account_console = source.account_console.clone();
    let account_readiness = source.account_readiness.clone();
    let exchanges = source.exchanges.clone();
    let symbols = source.symbols.clone();
    let event_summary =
        CrossArbEventSummaryData::from_events(&private_events, &risk_events, &exchanges, lang);
    let balance_rows = if account_console.is_empty() {
        event_summary.private_balance_rows.clone()
    } else {
        account_console.clone()
    };
    let account_rows = CrossArbAccountRowData::from_value_rows(&balance_rows, lang);
    let exchange_console_rows =
        CrossArbExchangeConsoleRowData::from_exchanges(&exchanges, &private_events);
    let exchange_status_cards = CrossArbExchangeStatusCardData::from_values(
        &exchanges,
        &source.exchange_status,
        &exchange_console_rows,
        &private_events,
        symbols.len(),
        lang,
    );
    let mut opportunity_rows = CrossArbOpportunityRowData::from_value_rows(&opportunities, lang);
    opportunity_rows.sort_by(|left, right| {
        right
            .raw_open_spread
            .partial_cmp(&left.raw_open_spread)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut seen_opportunity_symbols = std::collections::BTreeSet::new();
    opportunity_rows.retain(|row| seen_opportunity_symbols.insert(row.canonical_symbol.clone()));
    opportunity_rows.truncate(5);
    let mut sorted_position_bundles = position_bundles.clone();
    sort_cross_arb_rows_by_opened_at_desc(&mut sorted_position_bundles);
    let position_bundle_rows =
        CrossArbPositionBundleRowData::from_value_rows(&sorted_position_bundles, lang);
    let mut sorted_arbitrage_results = arbitrage_results.clone();
    sort_cross_arb_rows_by_opened_at_desc(&mut sorted_arbitrage_results);
    let arbitrage_result_rows =
        CrossArbResultRowData::from_value_rows(&sorted_arbitrage_results, lang);
    let strategy_logs_view = strategy_logs.clone();
    let strategy_log_rows =
        strategy_log_rows_for_category(&strategy_logs_view, &strategy_log_tab(), lang);
    let readiness = CrossArbReadinessData::from_values(&account_readiness, lang);
    let panel_data = CrossArbPanelData::from_values(
        &cross_arb,
        &summary,
        &settings,
        &profit_summary,
        &instrument_feasibility(),
        lang,
    );
    let resource_data = SystemResourcePanelData::from_status(&status);
    let hedge_page_size = 18usize;
    let symbol_page_size = 40usize;
    let strategy_log_panel_data = StrategyLogPanelData::from_value(&strategy_logs_view);
    let strategy_log_page_size = strategy_log_panel_data.page_size;
    let hedge_total_pages = page_count(position_bundle_rows.len(), hedge_page_size);
    let symbol_total_pages = page_count(symbols.len(), symbol_page_size);
    let strategy_log_total_pages = page_count(strategy_log_rows.len(), strategy_log_page_size);
    let hedge_start = page_start(hedge_page(), hedge_page_size, position_bundle_rows.len());
    let symbol_start = page_start(symbol_page(), symbol_page_size, symbols.len());
    let strategy_log_start = page_start(
        strategy_log_page(),
        strategy_log_page_size,
        strategy_log_rows.len(),
    );
    let strategy_log_source = strategy_log_source_text(&strategy_logs_view, lang);
    let summary_execution_profile = initial_settings.execution_profile.clone();
    let summary_hedge_notional = initial_settings.target_notional.clone();
    let summary_max_positions = initial_settings.max_positions.clone();
    let load_settings_token = token.clone();
    let load_settings_api_keys = api_keys.clone();
    let load_settings_exchanges = exchanges.clone();
    let _load_settings = move |_: Event<MouseData>| {
        let token_value = load_settings_token.clone();
        let api_keys_value = load_settings_api_keys.clone();
        let seed_exchanges = load_settings_exchanges.clone();
        wasm_bindgen_futures::spawn_local(async move {
            match fetch_cross_arb_settings(&token_value).await {
                Ok(value) => {
                    let form = CrossArbSettingsFormData::from_settings(
                        &value,
                        &api_keys_value,
                        &seed_exchanges,
                        lang,
                    );
                    apply_cross_arb_settings_form(
                        form,
                        symbol_text,
                        hedge_notional,
                        max_hedge_notional,
                        max_positions,
                        min_open_spread,
                        min_net_edge,
                        close_profit,
                        close_spread,
                        execution_profile,
                    );
                    match fetch_cross_arb_exchange_config(&token_value).await {
                        Ok(exchange_value) => {
                            let exchange_form = CrossArbSettingsFormData::from_settings(
                                &exchange_value,
                                &api_keys_value,
                                &seed_exchanges,
                                lang,
                            );
                            exchange_settings.set(exchange_form.exchanges);
                            exchange_config_loaded.set(true);
                            exchange_config_dirty.set(false);
                            message.set(t(lang, "config_loaded").to_string());
                        }
                        Err(error) => {
                            exchange_config_loaded.set(false);
                            if optional_exchange_config_error(&error) {
                                message.set(t(lang, "exchange_config_not_loaded").to_string());
                            } else {
                                message.set(error);
                            }
                        }
                    }
                }
                Err(error) => message.set(error),
            }
        });
    };
    let open_settings_token = token.clone();
    let open_settings_api_keys = api_keys.clone();
    let open_settings_exchanges = exchanges.clone();
    let _open_settings_dialog = move |_: Event<MouseData>| {
        let token_value = open_settings_token.clone();
        let api_keys_value = open_settings_api_keys.clone();
        let seed_exchanges = open_settings_exchanges.clone();
        wasm_bindgen_futures::spawn_local(async move {
            match fetch_cross_arb_settings(&token_value).await {
                Ok(value) => {
                    let form = CrossArbSettingsFormData::from_settings(
                        &value,
                        &api_keys_value,
                        &seed_exchanges,
                        lang,
                    );
                    apply_cross_arb_settings_form(
                        form,
                        symbol_text,
                        hedge_notional,
                        max_hedge_notional,
                        max_positions,
                        min_open_spread,
                        min_net_edge,
                        close_profit,
                        close_spread,
                        execution_profile,
                    );
                }
                Err(error) => {
                    message.set(error);
                    return;
                }
            }
            match fetch_cross_arb_exchange_config(&token_value).await {
                Ok(exchange_value) => {
                    let exchange_form = CrossArbSettingsFormData::from_settings(
                        &exchange_value,
                        &api_keys_value,
                        &seed_exchanges,
                        lang,
                    );
                    exchange_settings.set(exchange_form.exchanges);
                    exchange_config_loaded.set(true);
                    exchange_config_dirty.set(false);
                    settings_dialog_open.set(true);
                    message.set(t(lang, "config_loaded").to_string());
                }
                Err(error) => {
                    exchange_config_loaded.set(false);
                    if optional_exchange_config_error(&error) {
                        message.set(t(lang, "exchange_config_not_loaded").to_string());
                    } else {
                        message.set(error);
                    }
                }
            }
        });
    };
    let load_instruments_token = token.clone();
    let load_instruments = move |_| {
        let token_value = load_instruments_token.clone();
        wasm_bindgen_futures::spawn_local(async move {
            match fetch_cross_arb_instrument_data(&token_value).await {
                Ok(value) => {
                    let count = value.rows.len();
                    let coverage_ok = value.coverage_ok;
                    instrument_feasibility.set(value.feasibility);
                    instrument_rows.set(value.rows);
                    message.set(format!(
                        "{}: {} {}",
                        t(lang, "contract_rules_loaded"),
                        count,
                        if coverage_ok { "OK" } else { "CHECK" }
                    ));
                }
                Err(error) => message.set(error),
            }
        });
    };
    let save_settings_token = token.clone();
    let save_settings_api_keys = api_keys.clone();
    let save_settings_seed_exchanges = exchanges.clone();
    let save_settings = move |_| {
        let token_value = save_settings_token.clone();
        let api_keys_value = save_settings_api_keys.clone();
        let seed_exchanges = save_settings_seed_exchanges.clone();
        let symbols = split_symbol_text(&symbol_text());
        let target_notional = parse_number_or_default(&hedge_notional(), 5.0);
        let max_notional =
            parse_number_or_default(&max_hedge_notional(), target_notional).max(target_notional);
        let max_positions_value = parse_usize_or_default(&max_positions(), 10);
        let min_open_spread_value = parse_pct_or_default(&min_open_spread(), 0.5);
        let min_net_edge_value = parse_pct_or_default(&min_net_edge(), 0.2);
        let close_profit_value = parse_pct_or_default(&close_profit(), 0.2);
        let close_spread_value = parse_pct_or_default(&close_spread(), 0.2);
        let exchange_rows = exchange_settings();
        let save_exchange_config = exchange_config_dirty();
        if save_exchange_config && !exchange_config_loaded() {
            message.set(t(lang, "exchange_config_not_loaded").to_string());
            return;
        }
        let enabled_exchange_count = exchange_rows
            .iter()
            .filter(|row| row.enabled)
            .count()
            .max(1) as f64;
        let exchange_payload = exchange_rows
            .iter()
            .map(|row| {
                json!({
                    "exchange": row.exchange.clone(),
                    "enabled": row.enabled,
                    "account_id": row.account_id.clone(),
                    "env_prefix": row.env_prefix.clone(),
                    "private_rest_enabled": row.private_rest_enabled,
                    "private_ws_enabled": row.private_ws_enabled,
                })
            })
            .collect::<Vec<_>>();
        let exchange_body = save_exchange_config.then(|| {
            json!({
                "strategy_id": "cross_arb_live",
                "apply": true,
                "replace": false,
                "exchanges": exchange_payload,
            })
        });
        let body = json!({
            "symbols": symbols,
            "target_symbol_count": symbols.len(),
            "min_notional_usdt": target_notional,
            "target_notional_usdt": target_notional,
            "max_notional_usdt": max_notional,
            "max_positions_per_exchange": max_positions_value,
            "max_open_bundles": max_positions_value,
            "max_open_positions": max_positions_value * 2,
            "max_symbol_notional_usdt": max_notional,
            "max_notional_per_symbol_usdt": max_notional,
            "max_notional_per_exchange_usdt": max_notional * max_positions_value as f64,
            "max_total_notional_usdt": max_notional * max_positions_value as f64 * enabled_exchange_count,
            "min_open_raw_spread": min_open_spread_value,
            "min_open_spread_pct": min_open_spread_value,
            "min_open_maker_taker_net_edge": min_net_edge_value,
            "min_open_net_profit_pct": min_net_edge_value,
            "min_open_net_edge_pct": min_net_edge_value,
            "min_open_executable_depth_ratio": 1.2,
            "close_min_net_profit_pct": close_profit_value,
            "lock_profit_dual_taker_pct": close_profit_value,
            "expected_close_spread_pct": close_spread_value,
            "max_close_spread_pct": close_spread_value,
            "execution_profile": execution_profile()
        });
        wasm_bindgen_futures::spawn_local(async move {
            if let Some(exchange_body) = exchange_body {
                match save_cross_arb_exchange_config(&token_value, &exchange_body).await {
                    Ok(exchange_value) => {
                        let exchange_form = CrossArbSettingsFormData::from_settings(
                            &exchange_value,
                            &api_keys_value,
                            &seed_exchanges,
                            lang,
                        );
                        exchange_settings.set(exchange_form.exchanges);
                        exchange_config_loaded.set(true);
                        exchange_config_dirty.set(false);
                    }
                    Err(error) => {
                        message.set(error);
                        return;
                    }
                }
            }
            match save_cross_arb_settings(&token_value, &body).await {
                Ok(value) => {
                    let settings_value = value
                        .get("settings")
                        .cloned()
                        .unwrap_or_else(|| value.clone());
                    let form = CrossArbSettingsFormData::from_settings(
                        &settings_value,
                        &api_keys_value,
                        &seed_exchanges,
                        lang,
                    );
                    apply_cross_arb_settings_form(
                        form,
                        symbol_text,
                        hedge_notional,
                        max_hedge_notional,
                        max_positions,
                        min_open_spread,
                        min_net_edge,
                        close_profit,
                        close_spread,
                        execution_profile,
                    );
                    settings_dialog_open.set(false);
                    message.set(format!(
                        "{} {}",
                        t(lang, "exchange_config_saved"),
                        cross_arb_save_result_message(&value, lang)
                    ));
                }
                Err(error) => message.set(error),
            }
        });
    };
    let set_live_trading_token = token.clone();
    let toggle_live_trading = move |_| {
        let token_value = set_live_trading_token.clone();
        let next_enabled = !live_trading_enabled();
        let mut live_trading_enabled_value = live_trading_enabled;
        let mut live_trading_pending_value = live_trading_pending;
        wasm_bindgen_futures::spawn_local(async move {
            live_trading_pending_value.set(true);
            let body = json!({
                "strategy_id": "cross_arb_live",
                "apply": true,
                "trading_enabled": next_enabled,
            });
            match save_cross_arb_settings(&token_value, &body).await {
                Ok(value) => {
                    let confirmed_enabled = value
                        .get("settings")
                        .map(cross_arb_live_trading_enabled)
                        .unwrap_or(next_enabled);
                    live_trading_enabled_value.set(confirmed_enabled);
                    let key = if next_enabled {
                        "live_trading_enabled"
                    } else {
                        "live_trading_disabled"
                    };
                    message.set(format!(
                        "{} {}",
                        t(lang, key),
                        cross_arb_save_result_message(&value, lang)
                    ));
                }
                Err(error) => message.set(error),
            }
            live_trading_pending_value.set(false);
        });
    };
    rsx! {
        section { id: "cross-arb", class: "cross-arb",
            div { class: "section-title",
                div {
                    h2 { {s(lang, "cross_arb_title")} }
                    p { class: "muted", {s(lang, "cross_arb_subtitle")} }
                }
                div { class: "refresh-badge",
                    span { {s(lang, "refresh_target")} }
                    strong { "{panel_data.target_refresh_ms} ms" }
                }
            }
            div { class: "spot-control-strip cross-control-strip",
                button {
                    class: if live_trading_enabled() { "button danger" } else { "button primary" },
                    disabled: live_trading_pending(),
                    onclick: toggle_live_trading,
                    {if live_trading_pending() {
                        if live_trading_enabled() { s(lang, "live_trading_disabling") } else { s(lang, "live_trading_enabling") }
                    } else if live_trading_enabled() { s(lang, "live_trading_disable") } else { s(lang, "live_trading_enable") }}
                }
                span { class: if live_trading_enabled() { "pill" } else { "pill warn" }, {if live_trading_enabled() { s(lang, "live_trading_on") } else { s(lang, "live_trading_off") }} }
            }
            div { class: "grid cross-metrics cross-ops-metrics",
                Metric { label: s(lang, "monitored_pairs"), value: symbols.len().to_string() }
                Metric { label: s(lang, "hedge_records"), value: panel_data.closed_arbitrages.clone() }
                Metric { label: s(lang, "cumulative_profit"), value: signed_usdt(panel_data.realized_profit_usdt) }
                Metric { label: s(lang, "system_cpu"), value: resource_data.cpu.clone() }
                Metric { label: s(lang, "system_memory"), value: resource_data.memory.clone() }
                Metric { label: s(lang, "process_resource"), value: format!("{} / {}", resource_data.process_cpu, resource_data.process_memory) }
            }
            div { class: "panel cross-settings",
                div { class: "panel-title-row",
                    div {
                        h2 { {s(lang, "strategy_parameters")} }
                        p { class: "muted", "{panel_data.settings_path} · {panel_data.settings_symbol_count} {s(lang, \"monitored_pairs\")}" }
                    }
                }
                div { class: "settings-summary-strip",
                    div {
                        span { {s(lang, "enabled_exchanges")} }
                        strong { "{enabled_exchange_summary(&exchange_settings())}" }
                    }
                    div {
                        span { {s(lang, "execution_profile")} }
                        strong { "{summary_execution_profile}" }
                    }
                    div {
                        span { {s(lang, "per_arb_notional")} }
                        strong { "{summary_hedge_notional}" }
                    }
                    div {
                        span { {s(lang, "max_positions")} }
                        strong { "{summary_max_positions}" }
                    }
                }
            }
            if settings_dialog_open() {
                div { class: "modal-backdrop",
                    div { class: "config-dialog", role: "dialog", "aria-modal": "true",
                        div { class: "config-dialog-head",
                            div {
                                h2 { {s(lang, "edit_config")} }
                                p { class: "muted", "{panel_data.settings_path}" }
                            }
                            div { class: "row-actions",
                                button { class: "button primary", onclick: save_settings, {s(lang, "save_config")} }
                                button { class: "button", onclick: move |_| settings_dialog_open.set(false), {s(lang, "close")} }
                            }
                        }
                        div { class: "cross-settings-grid config-dialog-body",
                            div { class: "settings-form exchange-config-list",
                                div { class: "panel-title-row compact-title-row",
                                    h3 { {s(lang, "target_exchanges")} }
                                    button {
                                        class: "button",
                                        onclick: move |_| {
                                            append_exchange_config_row(exchange_settings);
                                            exchange_config_dirty.set(true);
                                        },
                                        {s(lang, "add_config_exchange")}
                                    }
                                }
                                for (index, row) in exchange_settings().iter().enumerate() {
                                    {
                                        let row_exchange = row.exchange.clone();
                                        let row_label = row.label.clone();
                                        let row_enabled = row.enabled;
                                        let row_account = row.account_id.clone();
                                        let row_env = row.env_prefix.clone();
                                        let account_options = CredentialAccountOption::for_exchange(&api_keys, &row_exchange, &row_account, lang);
                                        let account_select_options = account_options.clone();
                                        let delete_token = token.clone();
                                        let delete_api_keys = api_keys.clone();
                                        let delete_seed_exchanges = exchanges.clone();
                                        rsx! {
                                            div { class: "exchange-config-row",
                                                label { class: "form-field",
                                                    span { {s(lang, "exchange")} }
                                                    input {
                                                        value: "{row_exchange}",
                                                        oninput: move |event| {
                                                            update_exchange_name(exchange_settings, index, event.value());
                                                            exchange_config_dirty.set(true);
                                                        }
                                                    }
                                                }
                                                label { class: "check-row",
                                                    input {
                                                        r#type: "checkbox",
                                                        checked: "{row_enabled}",
                                                        onchange: move |event| {
                                                            update_exchange_enabled(exchange_settings, index, event.checked());
                                                            exchange_config_dirty.set(true);
                                                        }
                                                    }
                                                    span { "{row_label}" }
                                                }
                                                label { class: "form-field",
                                                    span { {s(lang, "account")} }
                                                    CrossArbAccountSelect {
                                                        value: row_account.clone(),
                                                        options: account_options,
                                                        on_change: move |value: String| {
                                                            update_exchange_account(exchange_settings, index, value, &account_select_options);
                                                            exchange_config_dirty.set(true);
                                                        }
                                                    }
                                                }
                                                label { class: "form-field",
                                                    span { {s(lang, "credential_namespace")} }
                                                    input {
                                                        value: "{row_env}",
                                                        oninput: move |event| {
                                                            update_exchange_env(exchange_settings, index, event.value());
                                                            exchange_config_dirty.set(true);
                                                        }
                                                    }
                                                }
                                                div { class: "row-actions exchange-row-actions",
                                                    button {
                                                        class: "mini-button danger",
                                                        onclick: move |_| {
                                                                delete_exchange_config_row(
                                                                    row_exchange.clone(),
                                                                    delete_token.clone(),
                                                                    delete_api_keys.clone(),
                                                                    delete_seed_exchanges.clone(),
                                                                    exchange_settings,
                                                                    exchange_config_loaded,
                                                                    exchange_config_dirty,
                                                                    message,
                                                                    lang,
                                                            );
                                                        },
                                                        {s(lang, "delete")}
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            div { class: "settings-form compact-settings",
                                h3 { {s(lang, "trade_config_console")} }
                                label { class: "form-field",
                                    span { {s(lang, "execution_profile")} }
                                    select {
                                        value: "{execution_profile()}",
                                        onchange: move |event| execution_profile.set(event.value()),
                                        option { value: "live_small_dry_run", {s(lang, "live_small_dry_run")} }
                                        option { value: "simulation", {s(lang, "simulation")} }
                                    }
                                }
                                label { class: "form-field",
                                    span { {s(lang, "per_arb_notional")} }
                                    input { value: "{hedge_notional()}", oninput: move |event| hedge_notional.set(event.value()) }
                                }
                                label { class: "form-field",
                                    span { {s(lang, "max_arb_notional")} }
                                    input { value: "{max_hedge_notional()}", oninput: move |event| max_hedge_notional.set(event.value()) }
                                }
                                label { class: "form-field",
                                    span { {s(lang, "max_positions")} }
                                    input { value: "{max_positions()}", oninput: move |event| max_positions.set(event.value()) }
                                }
                                label { class: "form-field",
                                    span { {s(lang, "open_raw_spread_pct")} }
                                    input { value: "{min_open_spread()}", oninput: move |event| min_open_spread.set(event.value()) }
                                }
                                label { class: "form-field",
                                    span { {s(lang, "open_net_spread_pct")} }
                                    input { value: "{min_net_edge()}", oninput: move |event| min_net_edge.set(event.value()) }
                                }
                                label { class: "form-field",
                                    span { {s(lang, "close_profit_pct")} }
                                    input { value: "{close_profit()}", oninput: move |event| close_profit.set(event.value()) }
                                }
                                label { class: "form-field",
                                    span { {s(lang, "expected_close_spread_pct")} }
                                    input { value: "{close_spread()}", oninput: move |event| close_spread.set(event.value()) }
                                }
                            }
                            label { class: "form-field symbol-editor",
                                span { {s(lang, "monitored_pairs")} }
                                textarea {
                                    value: "{symbol_text()}",
                                    oninput: move |event| symbol_text.set(event.value())
                                }
                            }
                        }
                    }
                }
            }
            div { class: "cross-arb-arbitrage-stack",
                    div { class: "panel",
                    div { class: "panel-title-row",
                        h2 { {s(lang, "contract_opportunities")} }
                    }
                    div { class: "table-wrap compact-table fixed-opportunity-table",
                        table {
                            thead { tr {
                                th { {s(lang, "symbol")} } th { {s(lang, "route")} } th { {s(lang, "maker")} } th { {s(lang, "taker")} } th { {s(lang, "long_entry_price")} } th { {s(lang, "short_entry_price")} } th { {s(lang, "raw_book_prices")} } th { {s(lang, "raw_spread")} } th { {s(lang, "expected_edge")} } th { {s(lang, "target_notional")} } th { {s(lang, "executable_notional")} } th { {s(lang, "fees")} } th { {s(lang, "funding")} } th { {s(lang, "age_ms")} } th { {s(lang, "market_can_open")} } th { {s(lang, "can_open")} } th { {s(lang, "warnings")} }
                            } }
                            tbody {
                                for row in opportunity_rows.iter() {
                                    tr {
                                        td { "{row.canonical_symbol}" }
                                        td { "{row.route}" }
                                        td { "{row.maker}" }
                                        td { "{row.taker}" }
                                        td { class: "numeric-cell", "{row.long_entry_price}" }
                                        td { class: "numeric-cell", "{row.short_entry_price}" }
                                        td { class: "numeric-cell", "{row.raw_book_prices}" }
                                        td { class: profit_class(row.raw_open_spread), "{row.raw_open_spread_text}" }
                                        td { class: profit_class(row.maker_taker_net_edge), "{row.maker_taker_net_edge_text}" }
                                        td { "{row.target_notional_usdt}" }
                                        td { "{row.executable_notional_usdt}" }
                                        td { "{row.fees}" }
                                        td { "{row.expected_funding_usdt}" }
                                        td { "{row.book_age_ms}" }
                                        td { StatusPill { value: row.market_can_open, lang } }
                                        td { StatusPill { value: row.can_open, lang } }
                                        td { class: "reason-cell", ReasonTags { text: row.reject_reasons.clone(), lang } }
                                    }
                                }
                            }
                        }
                    }
                }
                div { class: "panel",
                    h2 { {s(lang, "arbitrage_results")} }
                    div { class: "runtime-strip cross-runtime-strip",
                        div { span { {s(lang, "closed_arbitrages")} } strong { "{panel_data.closed_arbitrages}" } }
                        div { span { {s(lang, "win_rate")} } strong { "{panel_data.win_rate}" } }
                        div { span { {s(lang, "cumulative_profit")} } strong { class: profit_class(panel_data.realized_profit_usdt), "{signed_usdt(panel_data.realized_profit_usdt)}" } }
                    }
                    div { class: "table-wrap compact-table fixed-result-table no-wrap-table",
                        table {
                            thead { tr {
                                th { {s(lang, "symbol")} }
                                th { {s(lang, "route")} }
                                th { {s(lang, "status")} }
                                th { {s(lang, "open_expected_spread")} }
                                th { {s(lang, "open_actual_spread")} }
                                th { {s(lang, "close_expected_spread")} }
                                th { {s(lang, "close_actual_spread")} }
                                th { "PnL" }
                                th { {s(lang, "open_time")} }
                                th { {s(lang, "close_time")} }
                                th { {s(lang, "four_order_elapsed_ms")} }
                            } }
                            tbody {
                                for row in arbitrage_result_rows.iter().take(40) {
                                    tr {
                                        td { "{row.symbol}" }
                                        td { "{row.route}" }
                                        td { "{row.status}" }
                                        td { class: profit_class(row.open_expected_spread_pct), "{row.open_expected_spread}" }
                                        td { class: profit_class(row.open_actual_spread_pct), "{row.open_actual_spread}" }
                                        td { class: profit_class(row.close_expected_spread_pct), "{row.close_expected_spread}" }
                                        td { class: profit_class(row.close_actual_spread_pct), "{row.close_actual_spread}" }
                                        td { class: profit_class(row.realized_profit_usdt), "{row.realized_pnl}" }
                                        td { "{row.opened_at}" }
                                        td { "{row.closed_at}" }
                                        td { "{row.four_order_elapsed_ms}" }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "panel tab-panel fixed-tab-panel cross-console-tabs",
                div { class: "tab-bar",
                    button { class: if console_tab() == "exchange" { "tab-button active" } else { "tab-button" }, onclick: move |_| console_tab.set("exchange".to_string()), {s(lang, "exchange_console")} }
                    button { class: if console_tab() == "account" { "tab-button active" } else { "tab-button" }, onclick: move |_| console_tab.set("account".to_string()), {s(lang, "account_console")} }
                    button { class: if console_tab() == "coin" { "tab-button active" } else { "tab-button" }, onclick: move |_| console_tab.set("coin".to_string()), {s(lang, "coin_console")} }
                }
                if console_tab() == "exchange" {
                    div { class: "tab-content fixed-tab-content",
                        div { class: "exchange-card-grid compact-exchange-card-grid",
                            for card in exchange_status_cards.iter() {
                                div { class: "exchange-status-card",
                                    div { class: "exchange-card-head",
                                        strong { "{card.exchange}" }
                                        span { class: card.status_class, "{card.status_label}" }
                                    }
                                    dl {
                                        div { dt { {s(lang, "subscriptions")} } dd { "{card.subscriptions}" } }
                                        div { dt { {s(lang, "server_offset")} } dd { "{card.server_offset}" } }
                                        div { dt { {s(lang, "reconnects")} } dd { "{card.reconnects}" } }
                                    }
                                }
                            }
                        }
                        div { class: "table-wrap compact-table fixed-tab-table",
                            table {
                                thead { tr {
                                    th { {s(lang, "exchange")} } th { {s(lang, "status")} } th { {s(lang, "maker_volume")} } th { {s(lang, "taker_volume")} } th { {s(lang, "order_count")} } th { {s(lang, "taker_success_rate")} }
                                } }
                                tbody {
                                    for row in exchange_console_rows.iter() {
                                        tr {
                                            td { "{row.exchange}" }
                                            td { span { class: "pill", {s(lang, "active")} } }
                                            td { "{row.maker_volume}" }
                                            td { "{row.taker_volume}" }
                                            td { "{row.order_count}" }
                                            td { "{row.taker_success_rate}" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if console_tab() == "account" {
                    div { class: "tab-content fixed-tab-content",
                        div { class: "panel-title-row tab-title-row",
                            span { class: readiness.account_class, "{readiness.account_label}" }
                        }
                        div { class: "table-wrap compact-table fixed-tab-table",
                            table {
                                thead { tr {
                                    th { {s(lang, "exchange")} } th { {s(lang, "account")} } th { {s(lang, "credential_namespace")} } th { {s(lang, "credential_status")} } th { {s(lang, "status")} } th { {s(lang, "equity")} } th { {s(lang, "quote_equity")} } th { {s(lang, "update_time")} } th { {s(lang, "actions")} }
                                } }
                                tbody {
                                    for row in account_rows.iter().take(40) {
                                        {
                                            let credential_exchange = row.credential_exchange.clone();
                                            let credential_account = row.credential_account.clone();
                                            let credential_namespace = row.credential_namespace.clone();
                                            rsx! {
                                        tr {
                                            td { "{row.exchange}" }
                                            td { "{row.account_id}" }
                                            td { "{row.credential_namespace}" }
                                            td { span { class: row.credential_class, "{row.credential_text}" } }
                                            td { span { class: row.status_class, "{row.status_text}" } }
                                            td { "{row.total}" }
                                            td { "{row.available}" }
                                            td { "{row.recorded_at}" }
                                            td {
                                                if !row.credentials_ready {
                                                    button {
                                                        class: "mini-button primary",
                                                        onclick: move |_| {
                                                            api_key_exchange.set(credential_exchange.clone());
                                                            api_key_account.set(credential_account.clone());
                                                            api_key_namespace.set(credential_namespace.clone());
                                                            save_active_view(ControlPanelView::ApiKeys);
                                                            active_view.set(ControlPanelView::ApiKeys);
                                                            message.set(t(lang, "api_keys_loaded_for_edit").to_string());
                                                        },
                                                        {s(lang, "configure_credentials")}
                                                    }
                                                } else {
                                                    span { class: "muted", "-" }
                                                }
                                            }
                                        }
                                            }
                                        }
                                    }
                                    if balance_rows.is_empty() {
                                        for exchange in exchanges.iter() {
                                            tr {
                                                td { "{exchange}" }
                                                td { "default" }
                                                td { "-" }
                                                td { span { class: "pill warn", {s(lang, "offline")} } }
                                                td { span { class: "pill warn", {s(lang, "offline")} } }
                                                td { "$0.00" }
                                                td { "$0.00" }
                                                td { "-" }
                                                td { "-" }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if console_tab() == "coin" {
                    div { class: "tab-content fixed-tab-content",
                        div { class: "panel-title-row tab-title-row",
                            Pager { page: symbol_page(), total_pages: symbol_total_pages, on_prev: move |_| symbol_page.set(symbol_page().saturating_sub(1)), on_next: move |_| symbol_page.set((symbol_page() + 1).min(symbol_total_pages.saturating_sub(1))), lang }
                        }
                        div { class: "table-wrap compact-table fixed-tab-table",
                            table {
                                thead { tr {
                                    th { {s(lang, "symbol")} } th { {s(lang, "status")} } th { {s(lang, "occupied_capital")} } th { {s(lang, "arb_exchanges")} } th { {s(lang, "trade_volume")} } th { {s(lang, "realized_pnl")} }
                                } }
                                tbody {
                                    for symbol in symbols.iter().skip(symbol_start).take(symbol_page_size) {
                                        tr {
                                            td { "{symbol}" }
                                            td { span { class: "pill", {s(lang, "active")} } }
                                            td { "{format_usdt(cross_arb_symbol_capital(&opportunities, symbol))}" }
                                            td { "{cross_arb_symbol_exchanges(&opportunities, symbol)}" }
                                            td { "{format_usdt(cross_arb_symbol_volume(&private_events, symbol))}" }
                                            td { "{cross_arb_symbol_realized_pct(&hedge_records, symbol)}" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "panel tab-panel fixed-tab-panel cross-position-tabs",
                div { class: "tab-bar",
                    button { class: if position_tab() == "positions" { "tab-button active" } else { "tab-button" }, onclick: move |_| position_tab.set("positions".to_string()), {s(lang, "positions")} }
                    button { class: if position_tab() == "rules" { "tab-button active" } else { "tab-button" }, onclick: move |_| position_tab.set("rules".to_string()), {s(lang, "contract_rules")} }
                }
                if position_tab() == "positions" {
                    div { class: "tab-content fixed-tab-content",
                        div { class: "panel-title-row tab-title-row",
                            Pager { page: hedge_page(), total_pages: hedge_total_pages, on_prev: move |_| hedge_page.set(hedge_page().saturating_sub(1)), on_next: move |_| hedge_page.set((hedge_page() + 1).min(hedge_total_pages.saturating_sub(1))), lang }
                        }
                        div { class: "table-wrap compact-table fixed-tab-table",
                            table {
                                thead { tr {
                                    th { "Bundle" } th { {s(lang, "symbol")} } th { {s(lang, "long_exchange")} } th { {s(lang, "short_exchange")} } th { {s(lang, "status")} } th { {s(lang, "entry_price")} } th { {s(lang, "expected_edge")} } th { {s(lang, "close_price")} } th { {s(lang, "close_now")} } th { {s(lang, "close_threshold")} } th { {s(lang, "close_ready")} } th { {s(lang, "close_route")} } th { {s(lang, "update_time")} } th { {s(lang, "actions")} }
                                } }
                                tbody {
                                    for row in position_bundle_rows.iter().skip(hedge_start).take(hedge_page_size) {
                                        {
                                            let close_row = row.clone();
                                            let close_token = token.clone();
                                            let mut close_message = message;
                                            rsx! {
                                        tr {
                                            td { "{row.bundle_id}" }
                                            td { "{row.symbol}" }
                                            td { "{row.long_exchange}" }
                                            td { "{row.short_exchange}" }
                                            td { "{row.status}" }
                                            td { "{row.entry_prices}" }
                                            td { class: profit_class(row.entry_net_edge_pct), "{row.entry_net_edge_pct_text}" }
                                            td { "{row.close_prices}" }
                                            td { class: profit_class(row.close_profit_now), "{row.close_profit_now_text}" }
                                            td { "{row.close_threshold_pct}" }
                                            td { StatusPill { value: row.closeable, lang } }
                                            td { "{row.close_route}" }
                                            td { "{row.updated_at}" }
                                            td {
                                                button {
                                                    class: "mini-button danger",
                                                    onclick: move |_| {
                                                        let row = close_row.clone();
                                                        let token_value = close_token.clone();
                                                        wasm_bindgen_futures::spawn_local(async move {
                                                            let confirmed = web_sys::window()
                                                                .and_then(|window| {
                                                                    window
                                                                        .confirm_with_message(&format!(
                                                                            "Confirm market close {} on {} / {}?",
                                                                            row.symbol, row.long_exchange, row.short_exchange
                                                                        ))
                                                                        .ok()
                                                                })
                                                                .unwrap_or(false);
                                                            if !confirmed {
                                                                return;
                                                            }
                                                            match request_cross_arb_position_close(
                                                                &token_value,
                                                                &row.bundle_id,
                                                                &row.symbol,
                                                                &row.long_exchange,
                                                                &row.short_exchange,
                                                            )
                                                            .await
                                                            {
                                                                Ok(_) => close_message.set(t(lang, "manual_close_queued").to_string()),
                                                                Err(error) => close_message.set(error),
                                                            }
                                                        });
                                                    },
                                                    {s(lang, "market_close")}
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
                if position_tab() == "rules" {
                    div { class: "tab-content fixed-tab-content rules-tab-content",
                        div { class: "panel-title-row tab-title-row",
                            button { class: "mini-button", onclick: load_instruments, {s(lang, "load_contract_rules")} }
                        }
                        div { class: "runtime-strip cross-runtime-strip",
                            div {
                                span { {s(lang, "known_symbols")} }
                                strong { "{panel_data.instrument_known_symbols} / {panel_data.instrument_symbol_count}" }
                            }
                            div {
                                span { {s(lang, "feasible_symbols")} }
                                strong { "{panel_data.instrument_feasible_symbols} / {panel_data.instrument_symbol_count}" }
                            }
                            div {
                                span { {s(lang, "feasible_instruments")} }
                                strong { "{panel_data.instrument_feasible_instruments} / {panel_data.instrument_known_instruments}" }
                            }
                            div {
                                span { {s(lang, "required_notional_max")} }
                                strong { "{panel_data.instrument_required_notional_max}" }
                            }
                        }
                        div { class: "table-wrap compact-table fixed-tab-table",
                            table {
                                thead { tr {
                                    th { {s(lang, "exchange")} } th { {s(lang, "symbol")} } th { {s(lang, "exchange_symbol")} } th { {s(lang, "price_precision")} } th { {s(lang, "quantity_precision")} } th { {s(lang, "min_order")} } th { {s(lang, "funding_rate")} } th { {s(lang, "status")} }
                                } }
                                tbody {
                                    for row in CrossArbInstrumentRowData::from_value_rows(&instrument_rows(), lang).iter().take(120) {
                                        tr {
                                            td { "{row.exchange}" }
                                            td { "{row.canonical_symbol}" }
                                            td { "{row.exchange_symbol}" }
                                            td { "{row.price_precision}" }
                                            td { "{row.quantity_precision}" }
                                            td { "{row.min_order}" }
                                            td { "{row.funding_rate}" }
                                            td { "{row.status}" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "panel",
                h2 { {s(lang, "runtime_health")} }
                div { class: "runtime-strip cross-runtime-strip",
                    div { span { "Fill" } strong { "{event_summary.fill_events}" } }
                    div { span { "Balance" } strong { "{event_summary.balance_events}" } }
                    div { span { "Error" } strong { "{event_summary.error_events}" } }
                }
                div { class: "symbol-cloud",
                    for exchange in exchanges.iter() {
                        span { class: "chip active", "{exchange}" }
                    }
                }
            }
            div { class: "panel wide-log-panel",
                div { class: "panel-title-row",
                    div {
                        h2 { {s(lang, "strategy_all_logs")} }
                        p { class: "muted", "{s(lang, \"log_source\")}: {strategy_log_source}" }
                    }
                    LogPager {
                        page: strategy_log_page(),
                        total_pages: strategy_log_total_pages,
                        on_prev: move |_| strategy_log_page.set(strategy_log_page().saturating_sub(1)),
                        on_next: move |_| strategy_log_page.set((strategy_log_page() + 1).min(strategy_log_total_pages.saturating_sub(1))),
                        on_jump: move |page: usize| strategy_log_page.set(page.min(strategy_log_total_pages.saturating_sub(1))),
                        lang,
                    }
                }
                div { class: "tab-bar log-tab-bar",
                    for (key, label_key) in [
                        ("all", "log_all"),
                        ("error", "log_error"),
                        ("warn", "log_warn"),
                        ("trade", "log_trade"),
                        ("control", "log_control"),
                        ("balance", "log_balance"),
                        ("market", "log_market"),
                        ("info", "log_info"),
                    ] {
                        button {
                            class: if strategy_log_tab() == key { "tab-button active" } else { "tab-button" },
                            onclick: move |_| {
                                strategy_log_tab.set(key.to_string());
                                strategy_log_page.set(0);
                            },
                            "{s(lang, label_key)} {strategy_log_count(&strategy_logs_view, key)}"
                        }
                    }
                }
                div { class: "log-list strategy-log-list",
                    for row in strategy_log_rows.iter().skip(strategy_log_start).take(strategy_log_page_size) {
                        div { class: "log-line {row.class_name}",
                            span { "{row.timestamp}" }
                            strong { "{row.source}" }
                            p { "{row.message}" }
                        }
                    }
                }
            }
        }
    }
}

fn sort_cross_arb_rows_by_opened_at_desc(rows: &mut [Value]) {
    rows.sort_by(|left, right| cross_arb_open_sort_key(right).cmp(&cross_arb_open_sort_key(left)));
}

fn cross_arb_open_sort_key(row: &Value) -> String {
    ["opened_at", "open_time", "planned_at", "recorded_at"]
        .iter()
        .find_map(|key| row.get(*key).and_then(Value::as_str))
        .unwrap_or_default()
        .to_string()
}

#[component]
pub(crate) fn CrossArbAccountSelect(
    value: String,
    options: Vec<CredentialAccountOption>,
    on_change: EventHandler<String>,
) -> Element {
    rsx! {
        select {
            value: "{value}",
            onchange: move |event| on_change.call(event.value()),
            for option in options.iter() {
                option { value: "{option.value}", "{option.label}" }
            }
        }
    }
}

#[component]
fn ReasonTags(text: String, lang: Language) -> Element {
    let title = reason_title(&text);
    let tags = compact_reason_tags(&text, lang);
    let more_count = tags.len().saturating_sub(3);
    rsx! {
        div { class: "reason-tags", title: "{title}",
            if tags.is_empty() {
                span { class: "muted", "-" }
            } else {
                for tag in tags.iter().take(3) {
                    span { class: "reason-tag", "{tag}" }
                }
                if more_count > 0 {
                    span { class: "reason-tag reason-more", "+{more_count}" }
                }
            }
        }
    }
}

pub(crate) fn env_prefix_for_account(
    options: &[CredentialAccountOption],
    account_id: &str,
) -> Option<String> {
    options
        .iter()
        .find(|option| option.value.eq_ignore_ascii_case(account_id))
        .and_then(|option| {
            let env_prefix = option.env_prefix.trim();
            (!env_prefix.is_empty() && env_prefix != "-").then(|| env_prefix.to_string())
        })
}

fn append_exchange_config_row(mut exchange_settings: Signal<Vec<CrossArbExchangeFormData>>) {
    let mut rows = exchange_settings();
    rows.push(CrossArbExchangeFormData {
        exchange: String::new(),
        label: String::new(),
        enabled: true,
        account_id: String::new(),
        env_prefix: String::new(),
        private_rest_enabled: true,
        private_ws_enabled: true,
    });
    exchange_settings.set(rows);
}

fn update_exchange_name(
    mut exchange_settings: Signal<Vec<CrossArbExchangeFormData>>,
    index: usize,
    exchange: String,
) {
    let mut rows = exchange_settings();
    if let Some(row) = rows.get_mut(index) {
        row.exchange = exchange.trim().to_string();
        row.label = if row.exchange.is_empty() {
            String::new()
        } else {
            row.exchange.clone()
        };
    }
    exchange_settings.set(rows);
}

fn update_exchange_enabled(
    mut exchange_settings: Signal<Vec<CrossArbExchangeFormData>>,
    index: usize,
    enabled: bool,
) {
    let mut rows = exchange_settings();
    if let Some(row) = rows.get_mut(index) {
        row.enabled = enabled;
    }
    exchange_settings.set(rows);
}

fn update_exchange_account(
    mut exchange_settings: Signal<Vec<CrossArbExchangeFormData>>,
    index: usize,
    account_id: String,
    options: &[CredentialAccountOption],
) {
    let mut rows = exchange_settings();
    if let Some(row) = rows.get_mut(index) {
        if let Some(env_prefix) = env_prefix_for_account(options, &account_id) {
            row.env_prefix = env_prefix;
        }
        row.account_id = account_id;
    }
    exchange_settings.set(rows);
}

fn update_exchange_env(
    mut exchange_settings: Signal<Vec<CrossArbExchangeFormData>>,
    index: usize,
    env_prefix: String,
) {
    let mut rows = exchange_settings();
    if let Some(row) = rows.get_mut(index) {
        row.env_prefix = env_prefix;
    }
    exchange_settings.set(rows);
}

fn delete_exchange_config_row(
    exchange: String,
    token: String,
    api_keys: Value,
    seed_exchanges: Vec<String>,
    mut exchange_settings: Signal<Vec<CrossArbExchangeFormData>>,
    mut exchange_config_loaded: Signal<bool>,
    mut exchange_config_dirty: Signal<bool>,
    mut message: Signal<String>,
    lang: Language,
) {
    if exchange.trim().is_empty() {
        let mut rows = exchange_settings();
        rows.retain(|row| !row.exchange.trim().is_empty());
        exchange_settings.set(rows);
        exchange_config_dirty.set(true);
        return;
    }
    wasm_bindgen_futures::spawn_local(async move {
        match delete_cross_arb_exchange_config(&token, &exchange).await {
            Ok(value) => {
                let form = CrossArbSettingsFormData::from_settings(
                    &value,
                    &api_keys,
                    &seed_exchanges,
                    lang,
                );
                exchange_settings.set(form.exchanges);
                exchange_config_loaded.set(true);
                exchange_config_dirty.set(false);
                message.set(t(lang, "exchange_config_deleted").to_string());
            }
            Err(error) => message.set(error),
        }
    });
}

fn enabled_exchange_summary(rows: &[CrossArbExchangeFormData]) -> String {
    let labels = rows
        .iter()
        .filter(|row| row.enabled)
        .map(|row| {
            if row.label.trim().is_empty() || row.label == "-" {
                row.exchange.clone()
            } else {
                row.label.clone()
            }
        })
        .collect::<Vec<_>>();
    if labels.is_empty() {
        "-".to_string()
    } else {
        labels.join(" / ")
    }
}

fn compact_reason_tags(text: &str, lang: Language) -> Vec<String> {
    split_reason_text(text)
        .into_iter()
        .map(|reason| localized_reason_label(&reason, lang))
        .fold(Vec::new(), |mut tags, tag| {
            if !tags.iter().any(|existing| existing == &tag) {
                tags.push(tag);
            }
            tags
        })
}

fn split_reason_text(text: &str) -> Vec<String> {
    text.split([';', '\n'])
        .map(str::trim)
        .filter(|item| !item.is_empty() && *item != "-")
        .map(ToString::to_string)
        .collect()
}

fn reason_title(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "-" {
        "-".to_string()
    } else {
        trimmed.to_string()
    }
}

fn localized_reason_label(reason: &str, lang: Language) -> String {
    let lower = reason.trim().to_ascii_lowercase();
    let label = if lower.contains("agreement_required") {
        ("需开通协议", "Agreement needed")
    } else if lower.contains("close-only") || lower == "close_only" {
        ("只平仓", "Close-only")
    } else if lower.contains("max_consecutive_losses") || lower.contains("consecutive loss") {
        ("亏损保护", "Loss guard")
    } else if lower.contains("same symbol already has an active open bundle") {
        ("同币种已有仓位", "Symbol occupied")
    } else if lower.contains("open route") && lower.contains("cooling down") {
        ("路线冷却", "Route cooldown")
    } else if lower.contains("symbol is cooling down") {
        ("币种冷却", "Symbol cooldown")
    } else if lower.contains("market data provider is not connected") {
        ("行情未连接", "Market data down")
    } else if lower.contains("startup position takeover") {
        ("接管未启用", "Takeover off")
    } else if lower.contains("start_paused_new_entries") || lower.contains("new entries are paused")
    {
        ("开仓暂停", "Entry paused")
    } else if lower.contains("runtime deadline") {
        ("运行到期", "Runtime ended")
    } else if lower.contains("manual intervention") {
        ("需人工处理", "Manual action")
    } else if lower.contains("all live routes are disabled") {
        ("路线禁用", "Routes disabled")
    } else if lower.contains("new entries disabled on")
        || lower.contains("new entries disabled by config")
        || lower.contains("disabled_exchange")
        || lower.contains("disabled by config")
    {
        ("配置禁开", "Entry disabled")
    } else if lower.contains("display-only opportunity") || lower.contains("display-only row") {
        ("未下单", "Not submitted")
    } else if lower.contains("not evaluated") {
        ("待评估", "Pending")
    } else if lower.contains("live top-of-book is stale")
        || lower.contains("snapshot is stale")
        || lower.contains("top-of-book is stale")
    {
        ("行情过期", "Stale book")
    } else if lower.contains("top-of-book executable depth") {
        ("深度不足", "Depth low")
    } else if lower.contains("raw spread") && lower.contains("below") {
        ("价差不足", "Spread low")
    } else if lower.contains("raw spread") && lower.contains("above") {
        ("价差过大", "Spread high")
    } else if lower.contains("expected net edge") && lower.contains("below") {
        ("净价差不足", "Net edge low")
    } else if lower.contains("net profit") && lower.contains("below") {
        ("净收益不足", "Profit low")
    } else if lower.contains("notional") && lower.contains("below exchange minimum") {
        ("金额过小", "Notional low")
    } else if lower.contains("quantity") && lower.contains("below exchange minimum") {
        ("数量过小", "Qty low")
    } else if lower.contains("minimum") {
        ("低于最小下单", "Below minimum")
    } else if lower.contains("private rest is not enabled") {
        ("下单 REST 未启用", "REST off")
    } else if lower.contains("private websocket") {
        ("用户 WS 未启用", "Private WS off")
    } else if lower.contains("place_order is not supported") {
        ("不支持下单", "Place order off")
    } else if lower.contains("cancel_order is not supported") {
        ("不支持撤单", "Cancel off")
    } else if lower.contains("adapter is not loaded") {
        ("适配器未加载", "Adapter missing")
    } else if lower.contains("raw intents") {
        ("下单通道未接", "Order route off")
    } else if lower.contains("not filled") {
        ("未完全成交", "Not filled")
    } else if lower.contains("ioc") && (lower.contains("cancelled") || lower.contains("canceled")) {
        ("IOC 取消", "IOC cancelled")
    } else if lower.contains("cancelled") || lower.contains("canceled") {
        ("订单取消", "Cancelled")
    } else if lower.contains("timeout") || lower.contains("timed out") {
        ("请求超时", "Timeout")
    } else if lower.contains("insufficient") || lower.contains("balance") {
        ("余额不足", "Balance low")
    } else if lower.contains("authentication")
        || lower.contains("credential")
        || lower.contains("api key")
    {
        ("API 认证异常", "API auth")
    } else if lower.contains("rate limit") || lower.contains("too many requests") {
        ("限频", "Rate limit")
    } else if lower.contains("network") || lower.contains("connection") || lower.contains("connect")
    {
        ("连接异常", "Connection")
    } else {
        return fallback_reason_label(reason, lang);
    };
    if lang.is_zh() {
        label.0.to_string()
    } else {
        label.1.to_string()
    }
}

fn fallback_reason_label(reason: &str, lang: Language) -> String {
    let trimmed = reason.trim();
    if trimmed.is_ascii() {
        if lang.is_zh() {
            "待排查".to_string()
        } else {
            "Check needed".to_string()
        }
    } else {
        trimmed.chars().take(12).collect()
    }
}

fn optional_exchange_config_error(error: &str) -> bool {
    error.contains("HTTP 404")
        || error.contains("strategy_config_not_configured")
        || error.contains("command_queue_not_configured")
}

fn cross_arb_runtime_text(processes: &Value, cross_arb: &Value) -> String {
    let started_at = cross_arb
        .get("started_at")
        .and_then(timestamp_ms_from_value)
        .or_else(|| {
            as_array(processes).into_iter().find_map(|row| {
                let strategy_kind = text_at(&row, "strategy_kind", Language::En);
                let strategy_id = text_at(&row, "strategy_id", Language::En);
                if strategy_kind == "cross_exchange_arbitrage"
                    || strategy_id.to_ascii_lowercase().contains("cross_arb")
                {
                    row.get("started_at").and_then(timestamp_ms_from_value)
                } else {
                    None
                }
            })
        });
    started_at
        .map(|started_at| format_elapsed_ms(js_sys::Date::now() - started_at))
        .unwrap_or_else(|| "-".to_string())
}

fn timestamp_ms_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(value) => value.as_f64().and_then(normalize_timestamp_ms),
        Value::String(value) => {
            let value = value.trim();
            if value.is_empty() || value == "-" || value.eq_ignore_ascii_case("null") {
                return None;
            }
            if let Ok(timestamp) = value.parse::<f64>() {
                if let Some(timestamp_ms) = normalize_timestamp_ms(timestamp) {
                    return Some(timestamp_ms);
                }
            }
            let timestamp_ms = js_sys::Date::parse(value);
            timestamp_ms.is_finite().then_some(timestamp_ms)
        }
        _ => None,
    }
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

fn format_elapsed_ms(value: f64) -> String {
    if !value.is_finite() || value < 0.0 {
        return "-".to_string();
    }
    format!("{:.3}s", value / 1000.0)
}

fn cross_arb_save_result_message(value: &Value, lang: Language) -> String {
    let result = CrossArbSaveResultData::from_response(value);
    let suffix = if result.strategy_status == "started" {
        s(lang, "monitor_started")
    } else if result.strategy_status == "stopped" {
        s(lang, "monitor_stopped")
    } else if result.skipped {
        s(lang, "restart_skipped")
    } else {
        s(lang, "restart_status_unknown")
    };
    format!("{} {}", t(lang, "config_saved_restarting"), suffix)
}

fn apply_cross_arb_settings_form(
    form: CrossArbSettingsFormData,
    mut symbol_text: Signal<String>,
    mut hedge_notional: Signal<String>,
    mut max_hedge_notional: Signal<String>,
    mut max_positions: Signal<String>,
    mut min_open_spread: Signal<String>,
    mut min_net_edge: Signal<String>,
    mut close_profit: Signal<String>,
    mut close_spread: Signal<String>,
    mut execution_profile: Signal<String>,
) {
    symbol_text.set(form.symbols_text);
    hedge_notional.set(form.target_notional);
    max_hedge_notional.set(form.max_notional);
    max_positions.set(form.max_positions);
    min_open_spread.set(form.min_open_spread);
    min_net_edge.set(form.min_net_edge);
    close_profit.set(form.close_profit);
    close_spread.set(form.close_spread);
    execution_profile.set(form.execution_profile);
}

fn cross_arb_live_trading_enabled(settings: &Value) -> bool {
    let execution = settings.get("execution").unwrap_or(&Value::Null);
    bool_at(execution, "trading_enabled")
        || bool_at(execution, "live_orders_enabled")
        || bool_at(settings, "trading_enabled")
}

fn parse_number_or_default(value: &str, default: f64) -> f64 {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(default)
}

fn parse_pct_or_default(value: &str, default_pct: f64) -> f64 {
    value
        .trim()
        .trim_end_matches('%')
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value >= 0.0)
        .map(|value| value / 100.0)
        .unwrap_or(default_pct / 100.0)
}

fn parse_usize_or_default(value: &str, default: usize) -> usize {
    value
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn split_symbol_text(value: &str) -> Vec<String> {
    value
        .split(|ch: char| ch == ',' || ch == '\n' || ch == '\r' || ch.is_whitespace())
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            let upper = item.to_ascii_uppercase().replace(['_', '-'], "/");
            if upper.contains('/') {
                upper
            } else if let Some(base) = upper.strip_suffix("USDT") {
                format!("{base}/USDT")
            } else {
                upper
            }
        })
        .collect()
}

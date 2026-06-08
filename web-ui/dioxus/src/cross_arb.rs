use dioxus::prelude::*;
use serde_json::{json, Value};

use crate::api::{
    delete_cross_arb_exchange_config, fetch_cross_arb_exchange_config,
    fetch_cross_arb_instrument_data, fetch_cross_arb_market_snapshot_data,
    fetch_cross_arb_settings, save_cross_arb_exchange_config, save_cross_arb_settings,
};
use crate::i18n::{s, t};
use crate::storage::save_active_view;
use crate::types::{
    ControlPanelView, CredentialAccountOption, CrossArbAccountRowData, CrossArbEventSummaryData,
    CrossArbExchangeConsoleRowData, CrossArbExchangeFormData, CrossArbExchangeStatusCardData,
    CrossArbInstrumentRowData, CrossArbMarketSnapshotRowData, CrossArbOpenOrderRowData,
    CrossArbOpportunityRowData, CrossArbPanelData, CrossArbPositionBundleRowData,
    CrossArbReadinessData, CrossArbRepairTaskRowData, CrossArbResultRowData,
    CrossArbSaveResultData, CrossArbSettingsFormData, CrossArbSourceData, Language,
    SystemResourcePanelData,
};
use crate::ui::{control_click, ControlActionTarget, Metric, Pager, StatusPill};
use crate::utils::*;

#[component]
pub(crate) fn CrossArbPanel(
    cross_arb: Value,
    api_keys: Value,
    status: Value,
    processes: Value,
    token: String,
    mut message: Signal<String>,
    lang: Language,
    mut active_view: Signal<ControlPanelView>,
    mut api_key_exchange: Signal<String>,
    mut api_key_account: Signal<String>,
    mut api_key_namespace: Signal<String>,
) -> Element {
    let mut opp_page = use_signal(|| 0usize);
    let mut hedge_page = use_signal(|| 0usize);
    let mut symbol_page = use_signal(|| 0usize);
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
    let signals = source.signals.clone();
    let hedge_records = source.hedge_records.clone();
    let repair_tasks = source.repair_tasks.clone();
    let market_snapshots = source.market_snapshots.clone();
    let mut market_snapshot_rows = use_signal(|| market_snapshots.clone());
    let private_events = source.private_events.clone();
    let risk_events = source.risk_events.clone();
    let instruments = source.instruments.clone();
    let mut instrument_rows = use_signal(|| instruments.clone());
    let mut instrument_feasibility = use_signal(|| source.instrument_feasibility.clone());
    let position_bundles = source.position_bundles.clone();
    let open_orders = source.open_orders.clone();
    let arbitrage_results = source.arbitrage_results.clone();
    let profit_summary = source.profit_summary.clone();
    let account_console = source.account_console.clone();
    let account_readiness = source.account_readiness.clone();
    let strategy_readiness = source.strategy_readiness.clone();
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
        CrossArbExchangeConsoleRowData::from_exchanges(&exchanges, &private_events, lang);
    let exchange_status_cards = CrossArbExchangeStatusCardData::from_values(
        &exchanges,
        &source.exchange_status,
        &market_snapshot_rows(),
        &exchange_console_rows,
        symbols.len(),
        lang,
    );
    let opportunity_rows = CrossArbOpportunityRowData::from_value_rows(&opportunities, lang);
    let position_bundle_rows =
        CrossArbPositionBundleRowData::from_value_rows(&position_bundles, lang);
    let open_order_rows = CrossArbOpenOrderRowData::from_value_rows(&open_orders, lang);
    let arbitrage_result_rows = CrossArbResultRowData::from_value_rows(&arbitrage_results, lang);
    let repair_task_rows = CrossArbRepairTaskRowData::from_value_rows(&repair_tasks, lang);
    let readiness =
        CrossArbReadinessData::from_values(&account_readiness, &strategy_readiness, lang);
    let panel_data = CrossArbPanelData::from_values(
        &cross_arb,
        &summary,
        &settings,
        &profit_summary,
        &instrument_feasibility(),
        lang,
    );
    let system_resources = SystemResourcePanelData::from_status(&status);
    let runtime_text = cross_arb_runtime_text(&processes, &cross_arb);
    let opp_page_size = 24usize;
    let hedge_page_size = 18usize;
    let symbol_page_size = 40usize;
    let opp_total_pages = page_count(opportunity_rows.len(), opp_page_size);
    let hedge_total_pages = page_count(position_bundle_rows.len(), hedge_page_size);
    let symbol_total_pages = page_count(symbols.len(), symbol_page_size);
    let opp_start = page_start(opp_page(), opp_page_size, opportunity_rows.len());
    let hedge_start = page_start(hedge_page(), hedge_page_size, position_bundle_rows.len());
    let symbol_start = page_start(symbol_page(), symbol_page_size, symbols.len());
    let load_settings_token = token.clone();
    let load_settings_api_keys = api_keys.clone();
    let load_settings_exchanges = exchanges.clone();
    let load_settings = move |_| {
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
                    exchange_settings.set(form.exchanges);
                    symbol_text.set(form.symbols_text);
                    hedge_notional.set(form.target_notional);
                    max_hedge_notional.set(form.max_notional);
                    max_positions.set(form.max_positions);
                    min_open_spread.set(form.min_open_spread);
                    min_net_edge.set(form.min_net_edge);
                    close_profit.set(form.close_profit);
                    close_spread.set(form.close_spread);
                    execution_profile.set(form.execution_profile);
                    if let Ok(exchange_value) = fetch_cross_arb_exchange_config(&token_value).await
                    {
                        let exchange_form = CrossArbSettingsFormData::from_settings(
                            &exchange_value,
                            &api_keys_value,
                            &seed_exchanges,
                            lang,
                        );
                        exchange_settings.set(exchange_form.exchanges);
                    }
                    message.set(t(lang, "config_loaded").to_string());
                }
                Err(error) => message.set(error),
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
    let load_market_snapshots_token = token.clone();
    let load_market_snapshots = move |_| {
        let token_value = load_market_snapshots_token.clone();
        wasm_bindgen_futures::spawn_local(async move {
            match fetch_cross_arb_market_snapshot_data(&token_value).await {
                Ok(value) => {
                    let count = value.rows.len();
                    let coverage_ok = value.coverage_ok;
                    market_snapshot_rows.set(value.rows);
                    message.set(format!(
                        "{}: {} {}",
                        t(lang, "market_snapshots_loaded"),
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
        let min_open_spread_value = parse_number_or_default(&min_open_spread(), 0.005);
        let min_net_edge_value = parse_number_or_default(&min_net_edge(), 0.005);
        let close_profit_value = parse_number_or_default(&close_profit(), 0.0005);
        let close_spread_value = parse_number_or_default(&close_spread(), 0.0005);
        let exchange_rows = exchange_settings();
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
        let exchange_body = json!({
            "strategy_id": "cross_arb_live",
            "apply": true,
            "exchanges": exchange_payload,
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
            "min_open_maker_taker_net_edge": min_net_edge_value,
            "lock_profit_dual_taker_pct": close_profit_value,
            "max_close_spread_pct": close_spread_value,
            "execution_profile": execution_profile()
        });
        wasm_bindgen_futures::spawn_local(async move {
            match save_cross_arb_exchange_config(&token_value, &exchange_body).await {
                Ok(exchange_value) => {
                    let exchange_form = CrossArbSettingsFormData::from_settings(
                        &exchange_value,
                        &api_keys_value,
                        &seed_exchanges,
                        lang,
                    );
                    exchange_settings.set(exchange_form.exchanges);
                    match save_cross_arb_settings(&token_value, &body).await {
                        Ok(value) => {
                            settings_dialog_open.set(false);
                            message.set(format!(
                                "{} {}",
                                t(lang, "exchange_config_saved"),
                                cross_arb_save_result_message(&value, lang)
                            ));
                        }
                        Err(error) => message.set(error),
                    }
                }
                Err(error) => message.set(error),
            }
        });
    };
    let pause_strategy = control_click(
        ControlActionTarget::Global { command: "pause" },
        token.clone(),
        message,
        lang,
        "pause_strategy",
    );
    let resume_strategy = control_click(
        ControlActionTarget::Global { command: "resume" },
        token.clone(),
        message,
        lang,
        "resume_strategy",
    );
    let kill_strategy = control_click(
        ControlActionTarget::Global {
            command: "kill_switch",
        },
        token.clone(),
        message,
        lang,
        "kill_switch",
    );
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
                button { class: "button", onclick: pause_strategy, {s(lang, "pause_strategy")} }
                button { class: "button primary", onclick: resume_strategy, {s(lang, "resume_strategy")} }
                button { class: "button danger", onclick: kill_strategy, {s(lang, "kill_switch")} }
                span { class: panel_data.source_tone, "{panel_data.source_label}" }
                span { class: "muted", "{s(lang, \"latest_event\")}: {panel_data.latest_event_at}" }
            }
            div { class: "grid cross-metrics cross-ops-metrics",
                Metric { label: s(lang, "monitored_pairs"), value: symbols.len().to_string() }
                Metric { label: s(lang, "exchange"), value: exchanges.len().to_string() }
                Metric { label: s(lang, "realtime_quotes"), value: market_snapshot_rows().len().to_string() }
                Metric { label: s(lang, "running_time"), value: runtime_text.clone() }
                Metric { label: s(lang, "server_cpu"), value: system_resources.cpu.clone() }
                Metric { label: s(lang, "server_memory"), value: system_resources.memory.clone() }
                Metric { label: s(lang, "process_cpu"), value: system_resources.process_cpu.clone() }
                Metric { label: s(lang, "process_memory"), value: system_resources.process_memory.clone() }
            }
            div { class: "grid cross-metrics cross-detail-metrics",
                Metric { label: s(lang, "data_source"), value: panel_data.data_source.clone() }
                Metric { label: s(lang, "event_count"), value: panel_data.valid_events.clone() }
                Metric { label: s(lang, "parse_errors"), value: panel_data.parse_errors.clone() }
                Metric { label: s(lang, "opportunity_count"), value: format!("{} / {}", panel_data.can_open_opportunities, opportunities.len()) }
                Metric { label: s(lang, "signal_count"), value: format!("{} / {}", panel_data.open_signals, signals.len()) }
                Metric { label: s(lang, "hedge_records"), value: hedge_records.len().to_string() }
                Metric { label: s(lang, "repair_tasks"), value: repair_tasks.len().to_string() }
                Metric { label: s(lang, "strategy_estimated_profit"), value: signed_usdt(panel_data.estimated_edge_usdt) }
                Metric { label: s(lang, "open_orders"), value: open_orders.len().to_string() }
                Metric { label: s(lang, "cumulative_profit"), value: signed_usdt(panel_data.realized_profit_usdt) }
                Metric { label: s(lang, "strategy_readiness"), value: readiness.strategy_text.clone() }
                Metric { label: s(lang, "credential_readiness"), value: readiness.account_text.clone() }
            }
            div { class: "panel cross-settings",
                div { class: "panel-title-row",
                    div {
                        h2 { {s(lang, "strategy_parameters")} }
                        p { class: "muted", "{panel_data.settings_path} · {panel_data.settings_symbol_count} {s(lang, \"monitored_pairs\")}" }
                    }
                    div { class: "row-actions",
                        span { class: readiness.strategy_class, "{readiness.strategy_label}" }
                        button { class: "button", onclick: load_settings, {s(lang, "load_config")} }
                        button { class: "button primary", onclick: move |_| settings_dialog_open.set(true), {s(lang, "edit_config")} }
                    }
                }
                div { class: "settings-summary-strip",
                    div {
                        span { {s(lang, "enabled_exchanges")} }
                        strong { "{enabled_exchange_summary(&exchange_settings())}" }
                    }
                    div {
                        span { {s(lang, "execution_profile")} }
                        strong { "{execution_profile()}" }
                    }
                    div {
                        span { {s(lang, "per_arb_notional")} }
                        strong { "{hedge_notional()}" }
                    }
                    div {
                        span { {s(lang, "max_positions")} }
                        strong { "{max_positions()}" }
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
                                        onclick: move |_| append_exchange_config_row(exchange_settings),
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
                                                        oninput: move |event| update_exchange_name(exchange_settings, index, event.value())
                                                    }
                                                }
                                                label { class: "check-row",
                                                    input {
                                                        r#type: "checkbox",
                                                        checked: "{row_enabled}",
                                                        onchange: move |event| update_exchange_enabled(exchange_settings, index, event.checked())
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
                                                        }
                                                    }
                                                }
                                                label { class: "form-field",
                                                    span { {s(lang, "credential_namespace")} }
                                                    input {
                                                        value: "{row_env}",
                                                        oninput: move |event| update_exchange_env(exchange_settings, index, event.value())
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
                                    span { {s(lang, "raw_spread")} }
                                    input { value: "{min_open_spread()}", oninput: move |event| min_open_spread.set(event.value()) }
                                }
                                label { class: "form-field",
                                    span { {s(lang, "expected_edge")} }
                                    input { value: "{min_net_edge()}", oninput: move |event| min_net_edge.set(event.value()) }
                                }
                                label { class: "form-field",
                                    span { {s(lang, "close_profit")} }
                                    input { value: "{close_profit()}", oninput: move |event| close_profit.set(event.value()) }
                                }
                                label { class: "form-field",
                                    span { {s(lang, "close_spread")} }
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
            div { class: "panel exchange-status-panel",
                h2 { {s(lang, "exchange_console")} }
                div { class: "exchange-card-grid",
                    for card in exchange_status_cards.iter() {
                        div { class: "exchange-status-card",
                            div { class: "exchange-card-head",
                                strong { "{card.exchange}" }
                                span { class: card.status_class, "{card.status_label}" }
                            }
                            dl {
                                div { dt { {s(lang, "subscriptions")} } dd { "{card.subscriptions}" } }
                                div { dt { {s(lang, "messages")} } dd { "{card.messages}" } }
                                div { dt { {s(lang, "latency")} } dd { "{card.latency}" } }
                                div { dt { {s(lang, "server_offset")} } dd { "{card.server_offset}" } }
                                div { dt { {s(lang, "book_count")} } dd { "{card.book_count}" } }
                                div { dt { {s(lang, "last_update")} } dd { "{card.last_update}" } }
                                div { dt { {s(lang, "reconnects")} } dd { "{card.reconnects}" } }
                            }
                        }
                    }
                }
            }
            div { class: "two spot-main",
                div { class: "panel",
                    h2 { {s(lang, "exchange_console")} }
                    div { class: "table-wrap compact-table",
                        table {
                            thead { tr {
                                th { {s(lang, "exchange")} } th { {s(lang, "status")} } th { {s(lang, "maker_volume")} } th { {s(lang, "taker_volume")} } th { {s(lang, "order_count")} } th { {s(lang, "taker_success_rate")} } th { {s(lang, "latency")} } th { {s(lang, "actions")} }
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
                                        td { "{row.latency}" }
                                        td {
                                            div { class: "row-actions",
                                                button { class: "mini-button", onclick: control_click(ControlActionTarget::Exchange { exchange: row.exchange.clone(), command: "pause" }, token.clone(), message, lang, "exchange_pause"), {s(lang, "stop")} }
                                                button { class: "mini-button primary", onclick: control_click(ControlActionTarget::Exchange { exchange: row.exchange.clone(), command: "resume" }, token.clone(), message, lang, "exchange_resume"), {s(lang, "enable")} }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                div { class: "panel",
                    div { class: "panel-title-row",
                        h2 { {s(lang, "account_console")} }
                        span { class: readiness.account_class, "{readiness.account_label}" }
                    }
                    div { class: "table-wrap compact-table",
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
            div { class: "panel",
                div { class: "panel-title-row",
                    h2 { {s(lang, "contract_opportunities")} }
                    Pager { page: opp_page(), total_pages: opp_total_pages, on_prev: move |_| opp_page.set(opp_page().saturating_sub(1)), on_next: move |_| opp_page.set((opp_page() + 1).min(opp_total_pages.saturating_sub(1))), lang }
                }
                div { class: "table-wrap compact-table",
                    table {
                        thead { tr {
                            th { {s(lang, "symbol")} } th { {s(lang, "route")} } th { {s(lang, "maker")} } th { {s(lang, "taker")} } th { {s(lang, "raw_spread")} } th { {s(lang, "expected_edge")} } th { {s(lang, "target_notional")} } th { {s(lang, "executable_notional")} } th { {s(lang, "fees")} } th { {s(lang, "funding")} } th { {s(lang, "age_ms")} } th { {s(lang, "can_open")} } th { {s(lang, "warnings")} }
                        } }
                        tbody {
                            for row in opportunity_rows.iter().skip(opp_start).take(opp_page_size) {
                                tr {
                                    td { "{row.canonical_symbol}" }
                                    td { "{row.route}" }
                                    td { "{row.maker}" }
                                    td { "{row.taker}" }
                                    td { class: profit_class(row.raw_open_spread), "{row.raw_open_spread_text}" }
                                    td { class: profit_class(row.maker_taker_net_edge), "{row.maker_taker_net_edge_text}" }
                                    td { "{row.target_notional_usdt}" }
                                    td { "{row.executable_notional_usdt}" }
                                    td { "{row.fees}" }
                                    td { "{row.expected_funding_usdt}" }
                                    td { "{row.book_age_ms}" }
                                    td { StatusPill { value: row.can_open, lang } }
                                    td { "{row.reject_reasons}" }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "two spot-main",
                div { class: "panel",
                    div { class: "panel-title-row",
                        h2 { {s(lang, "coin_console")} }
                        Pager { page: symbol_page(), total_pages: symbol_total_pages, on_prev: move |_| symbol_page.set(symbol_page().saturating_sub(1)), on_next: move |_| symbol_page.set((symbol_page() + 1).min(symbol_total_pages.saturating_sub(1))), lang }
                    }
                    div { class: "table-wrap compact-table",
                        table {
                            thead { tr {
                                th { {s(lang, "symbol")} } th { {s(lang, "status")} } th { {s(lang, "occupied_capital")} } th { {s(lang, "arb_exchanges")} } th { {s(lang, "trade_volume")} } th { {s(lang, "est_profit")} } th { {s(lang, "realized_pnl")} } th { {s(lang, "actions")} }
                            } }
                            tbody {
                                for symbol in symbols.iter().skip(symbol_start).take(symbol_page_size) {
                                    tr {
                                        td { "{symbol}" }
                                        td { span { class: "pill", {s(lang, "active")} } }
                                        td { "{format_usdt(cross_arb_symbol_capital(&opportunities, symbol))}" }
                                        td { "{cross_arb_symbol_exchanges(&opportunities, symbol)}" }
                                        td { "{format_usdt(cross_arb_symbol_volume(&private_events, symbol))}" }
                                        td { class: profit_class(cross_arb_symbol_est(&signals, symbol)), "{signed_usdt(cross_arb_symbol_est(&signals, symbol))}" }
                                        td { "{cross_arb_symbol_realized_pct(&hedge_records, symbol)}" }
                                        td {
                                            div { class: "row-actions",
                                                button { class: "mini-button", onclick: control_click(ControlActionTarget::Symbol { symbol: symbol.clone(), command: "pause" }, token.clone(), message, lang, "symbol_pause"), {s(lang, "pause")} }
                                                button { class: "mini-button primary", onclick: control_click(ControlActionTarget::Symbol { symbol: symbol.clone(), command: "resume" }, token.clone(), message, lang, "symbol_resume"), {s(lang, "start")} }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                div { class: "panel",
                    div { class: "panel-title-row",
                        h2 { {s(lang, "market_snapshots")} }
                        button { class: "mini-button", onclick: load_market_snapshots, {s(lang, "load_market_snapshots")} }
                    }
                    div { class: "table-wrap compact-table",
                        table {
                            thead { tr {
                                th { {s(lang, "exchange")} } th { {s(lang, "symbol")} } th { {s(lang, "bid")} } th { {s(lang, "ask")} } th { {s(lang, "age_ms")} } th { {s(lang, "sequence")} } th { {s(lang, "recorded_at")} }
                            } }
                            tbody {
                                for row in CrossArbMarketSnapshotRowData::from_value_rows(&market_snapshot_rows(), lang).iter().take(120) {
                                    tr {
                                        td { "{row.exchange}" }
                                        td { "{row.symbol}" }
                                        td { "{row.best_bid}" }
                                        td { "{row.best_ask}" }
                                        td { "{row.book_age_ms}" }
                                        td { "{row.sequence}" }
                                        td { "{row.recorded_at}" }
                                    }
                                }
                            }
                        }
                    }
                }
                div { class: "panel",
                    div { class: "panel-title-row",
                        h2 { {s(lang, "contract_rules")} }
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
                    div { class: "table-wrap compact-table",
                        table {
                            thead { tr {
                                th { {s(lang, "exchange")} } th { {s(lang, "symbol")} } th { {s(lang, "exchange_symbol")} } th { {s(lang, "price_tick")} } th { {s(lang, "quantity_step")} } th { {s(lang, "min_qty")} } th { {s(lang, "min_notional")} } th { {s(lang, "required_notional")} } th { {s(lang, "headroom")} } th { {s(lang, "feasibility")} } th { {s(lang, "status")} }
                            } }
                            tbody {
                                for row in CrossArbInstrumentRowData::from_value_rows(&instrument_rows(), lang).iter().take(120) {
                                    tr {
                                        td { "{row.exchange}" }
                                        td { "{row.canonical_symbol}" }
                                        td { "{row.exchange_symbol}" }
                                        td { "{row.price_tick}" }
                                        td { "{row.quantity_step}" }
                                        td { "{row.min_qty}" }
                                        td { "{row.min_notional}" }
                                        td { "{row.required_notional}" }
                                        td { class: profit_class(row.headroom_value), "{row.headroom}" }
                                        td { span { class: row.feasibility_class, "{row.feasibility_label}" } }
                                        td { "{row.status}" }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "panel",
                div { class: "panel-title-row",
                    h2 { {s(lang, "positions")} }
                    Pager { page: hedge_page(), total_pages: hedge_total_pages, on_prev: move |_| hedge_page.set(hedge_page().saturating_sub(1)), on_next: move |_| hedge_page.set((hedge_page() + 1).min(hedge_total_pages.saturating_sub(1))), lang }
                }
                div { class: "table-wrap compact-table",
                    table {
                        thead { tr {
                            th { "Bundle" } th { {s(lang, "symbol")} } th { {s(lang, "long_exchange")} } th { {s(lang, "short_exchange")} } th { {s(lang, "status")} } th { {s(lang, "expected_edge")} } th { {s(lang, "close_now")} } th { {s(lang, "close_threshold")} } th { {s(lang, "close_ready")} } th { {s(lang, "close_route")} } th { {s(lang, "update_time")} }
                        } }
                        tbody {
                            for row in position_bundle_rows.iter().skip(hedge_start).take(hedge_page_size) {
                                tr {
                                    td { "{row.bundle_id}" }
                                    td { "{row.symbol}" }
                                    td { "{row.long_exchange}" }
                                    td { "{row.short_exchange}" }
                                    td { "{row.status}" }
                                    td { class: profit_class(row.entry_net_edge_pct), "{row.entry_net_edge_pct_text}" }
                                    td { class: profit_class(row.close_profit_now), "{row.close_profit_now_text}" }
                                    td { "{row.close_threshold_pct}" }
                                    td { StatusPill { value: row.closeable, lang } }
                                    td { "{row.close_route}" }
                                    td { "{row.updated_at}" }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "two spot-main",
                div { class: "panel",
                    h2 { {s(lang, "open_orders")} }
                    div { class: "table-wrap compact-table",
                        table {
                            thead { tr {
                                th { {s(lang, "source")} } th { {s(lang, "exchange")} } th { {s(lang, "symbol")} } th { {s(lang, "side")} } th { {s(lang, "price")} } th { {s(lang, "remaining_qty")} } th { {s(lang, "notional")} } th { {s(lang, "status")} } th { {s(lang, "order_ref")} }
                            } }
                            tbody {
                                for row in open_order_rows.iter().take(40) {
                                    tr {
                                        td { "{row.source}" }
                                        td { "{row.exchange}" }
                                        td { "{row.symbol}" }
                                        td { "{row.side}" }
                                        td { "{row.price}" }
                                        td { "{row.remaining_qty}" }
                                        td { "{row.notional}" }
                                        td { "{row.status}" }
                                        td { "{row.order_ref}" }
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
                    div { class: "table-wrap compact-table",
                        table {
                            thead { tr {
                                th { "Bundle" } th { {s(lang, "symbol")} } th { {s(lang, "route")} } th { {s(lang, "target_notional")} } th { {s(lang, "realized_pnl")} } th { {s(lang, "cumulative_profit")} } th { {s(lang, "update_time")} }
                            } }
                            tbody {
                                for row in arbitrage_result_rows.iter().take(40) {
                                    tr {
                                        td { "{row.bundle_id}" }
                                        td { "{row.symbol}" }
                                        td { "{row.route}" }
                                        td { "{row.target_notional_usdt}" }
                                        td { class: profit_class(row.realized_profit_usdt), "{row.realized_pnl}" }
                                        td { class: profit_class(row.cumulative_profit_usdt), "{row.cumulative_profit}" }
                                        td { "{row.updated_at}" }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "two spot-main",
                div { class: "panel",
                    h2 { {s(lang, "repair_tasks_panel")} }
                    div { class: "table-wrap compact-table",
                        table {
                            thead { tr {
                                th { {s(lang, "status")} } th { {s(lang, "exchange")} } th { {s(lang, "side")} } th { {s(lang, "qty")} } th { {s(lang, "failures")} } th { {s(lang, "warnings")} }
                            } }
                            tbody {
                                for row in repair_task_rows.iter().take(40) {
                                    tr {
                                        td { "{row.status}" }
                                        td { "{row.failed_exchange}" }
                                        td { "{row.side}" }
                                        td { "{row.quantity}" }
                                        td { "{row.attempt_count}" }
                                        td { "{row.last_error}" }
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
            }
        }
    }
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
    mut message: Signal<String>,
    lang: Language,
) {
    if exchange.trim().is_empty() {
        let mut rows = exchange_settings();
        rows.retain(|row| !row.exchange.trim().is_empty());
        exchange_settings.set(rows);
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

fn parse_number_or_default(value: &str, default: f64) -> f64 {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(default)
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

use dioxus::prelude::*;
use serde_json::Value;

use crate::api::{
    post_exchange_control, post_symbol_control, refresh_spot_exchange_control,
    refresh_spot_symbol_control,
};
use crate::i18n::{s, t};
use crate::overview::BalanceTrendPanel;
use crate::types::{
    BalanceTrendData, DashboardData, FeeScheduleRowData, InitialEntryBlockerRowData, Language,
    SpotAccountConsoleRowData, SpotArbDashboardInput, SpotArbDerivedData, SpotArbPanelData,
    SpotArbSourceData, SpotBookRowData, SpotExchangeConsoleRowData, SpotOpportunityRowData,
    SpotOrderRowData, StrategyLogPanelData, SymbolControlUpdate,
};
use crate::ui::{control_click, ControlActionTarget, LogPager, Pager, StatusPill};
use crate::utils::*;

#[component]
pub(crate) fn SpotArbPanel(
    data: Signal<DashboardData>,
    spot_arb: Value,
    books: Value,
    opportunities: Value,
    plans: Value,
    inventory: Value,
    exchanges: Value,
    fees: Value,
    logs: Value,
    health: Value,
    risk: Value,
    config: Value,
    disabled: Value,
    control_symbols: Value,
    balance_history: Value,
    strategy_logs: Value,
    token: String,
    mut message: Signal<String>,
    lang: Language,
) -> Element {
    let data_signal = data;
    let mut symbol_page = use_signal(|| 0usize);
    let mut plan_page = use_signal(|| 0usize);
    let mut console_tab = use_signal(|| "config".to_string());
    let mut market_tab = use_signal(|| "subscriptions".to_string());
    let mut symbol_filter = use_signal(|| "all".to_string());
    let mut symbol_search = use_signal(String::new);
    let mut selected_symbol = use_signal(String::new);
    let mut strategy_log_tab = use_signal(|| "all".to_string());
    let mut strategy_log_page = use_signal(|| 0usize);
    let source = SpotArbSourceData::from_dashboard(SpotArbDashboardInput {
        spot_arb: &spot_arb,
        books,
        opportunities,
        plans,
        inventory: &inventory,
        exchanges: &exchanges,
        fees: &fees,
        config: &config,
        lang,
    });
    let source_token = token.clone();
    let candidates = source.candidates.clone();
    let selected = source.selected.clone();
    let balances = balance_summaries_for_enabled(&inventory, &enabled_exchange_set(&config));
    let exchange_console_rows = source.exchange_console.clone();
    let account_console_rows = source.account_console.clone();
    let total_equity_usdt = balances.iter().map(|row| row.total_usdt).sum::<f64>();
    let available_equity_usdt = balances.iter().map(|row| row.available_usdt).sum::<f64>();
    let inventory_rows = source.inventory.clone();
    let position_source_rows = source.position_console.clone();
    let residual_assets_usdt = residual_asset_valuation_usdt(&inventory_rows);
    let fee_rows = source.fees.clone();
    let fee_view_rows = FeeScheduleRowData::from_value_rows(&fee_rows, lang);
    let arbitrage_slot_status = source.arbitrage_slot_status.clone();
    let active_slot_symbols = source.active_slot_symbols.clone();
    let liquidation_rows = source.liquidation_rows.clone();
    let disabled_symbols = disabled_symbol_set(&disabled, &control_symbols);
    let base_derived = SpotArbDerivedData::from_source(&source, "", lang);
    let symbol_rows = coin_console_rows(
        &candidates,
        &selected,
        &base_derived.opportunities,
        &base_derived.plans,
        &inventory_rows,
        &disabled_symbols,
        &control_symbols,
        &active_slot_symbols,
        &liquidation_rows,
        lang,
    );
    let active_symbol_count = symbol_rows.iter().filter(|row| row.arbitraging).count();
    let exiting_symbol_count = symbol_rows.iter().filter(|row| row.exiting).count();
    let anomaly_symbol_count = symbol_rows
        .iter()
        .filter(|row| {
            row.disabled || row.est_profit < 0.0 || row.realized_pnl < 0.0 || row.pnl_24h < 0.0
        })
        .count();
    let symbol_query = symbol_search().trim().to_ascii_uppercase();
    let symbol_filter_value = symbol_filter();
    let filtered_symbol_rows = symbol_rows
        .iter()
        .filter(|row| {
            if !symbol_query.is_empty() {
                let haystack = format!("{} {} {}", row.symbol, row.exchanges, row.control_state)
                    .to_ascii_uppercase();
                if !haystack.contains(&symbol_query) {
                    return false;
                }
            }
            match symbol_filter_value.as_str() {
                "arbitraging" => row.arbitraging,
                "exiting" => row.exiting,
                "anomaly" => {
                    row.disabled
                        || row.est_profit < 0.0
                        || row.realized_pnl < 0.0
                        || row.pnl_24h < 0.0
                }
                _ => true,
            }
        })
        .cloned()
        .collect::<Vec<_>>();
    let selected_symbol_value = selected_symbol().trim().to_string();
    let detail_symbol = if selected_symbol_value.is_empty() {
        filtered_symbol_rows
            .first()
            .map(|row| row.symbol.clone())
            .unwrap_or_default()
    } else {
        selected_symbol_value
    };
    let detail_symbol_key = normalize_symbol_text(&detail_symbol);
    let derived = SpotArbDerivedData::from_source(&source, &detail_symbol_key, lang);
    let detail_book_refs = derived.detail_books.iter().collect::<Vec<_>>();
    let detail_plan_refs = derived.detail_plans.iter().collect::<Vec<_>>();
    let detail_exchange_rows = symbol_exchange_detail_rows(
        &detail_symbol_key,
        &detail_book_refs,
        &detail_plan_refs,
        &exchange_console_rows,
        lang,
    );
    let exchange_console_view_rows =
        SpotExchangeConsoleRowData::from_value_rows(&exchange_console_rows, lang);
    let account_console_view_rows =
        SpotAccountConsoleRowData::from_value_rows(&account_console_rows, lang);
    let book_view_rows = SpotBookRowData::from_value_rows(&derived.books, lang);
    let detail_book_view_rows = SpotBookRowData::from_value_rows(&derived.detail_books, lang);
    let detail_opp_view_rows =
        SpotOpportunityRowData::from_value_rows(&derived.detail_opportunities, lang);
    let detail_plan_view_rows = SpotOrderRowData::from_value_rows(&derived.detail_plans, lang);
    let best_opp_view_rows =
        SpotOpportunityRowData::from_value_rows(&derived.best_opportunities, lang);
    let plan_view_rows = SpotOrderRowData::from_value_rows(&derived.plans, lang);
    let position_rows = position_rows(&position_source_rows, lang);
    let strategy_log_rows =
        strategy_log_rows_for_category(&strategy_logs, &strategy_log_tab(), lang);
    let aux_rows = aux_asset_rows(&inventory_rows, &exchange_console_rows, lang);
    let exchange_stat_rows =
        exchange_statistic_rows(&exchange_console_rows, &derived.plans, &derived.books);
    let active_books = derived.active_books;
    let stale_books = derived.stale_books;
    let accepted_spread_count = derived.accepted_spread_count;
    let managed_symbol_count = symbol_rows.len();
    let symbol_page_size = 20usize;
    let plan_page_size = 18usize;
    let strategy_log_panel_data = StrategyLogPanelData::from_value(&strategy_logs);
    let strategy_log_page_size = strategy_log_panel_data.page_size;
    let symbol_total_pages = page_count(filtered_symbol_rows.len(), symbol_page_size);
    let plan_total_pages = page_count(derived.plans.len(), plan_page_size);
    let strategy_log_total_pages = page_count(strategy_log_rows.len(), strategy_log_page_size);
    let symbol_start = page_start(symbol_page(), symbol_page_size, filtered_symbol_rows.len());
    let plan_start = page_start(plan_page(), plan_page_size, derived.plans.len());
    let strategy_log_start = page_start(
        strategy_log_page(),
        strategy_log_page_size,
        strategy_log_rows.len(),
    );
    let strategy_log_source = strategy_log_source_text(&strategy_logs, lang);
    let anomaly_rows = spot_anomaly_rows(
        &derived.books,
        &derived.plans,
        &derived.opportunities,
        &disabled_symbols,
        lang,
    );
    let initial_entry_status = source.initial_entry_status.clone();
    let initial_entry_rows = source.initial_entry_rows.clone();
    let initial_entry_blocker_rows =
        InitialEntryBlockerRowData::from_value_rows(&initial_entry_rows, lang);
    let blocked_slot_symbols = source.blocked_slot_symbols.clone();
    let panel_data = SpotArbPanelData::from_values(
        &spot_arb,
        &config,
        &arbitrage_slot_status,
        &initial_entry_status,
        selected.len(),
        candidates.len(),
        lang,
    );
    let pause_strategy = control_click(
        ControlActionTarget::Global { command: "pause" },
        source_token.clone(),
        message,
        lang,
        "pause_strategy",
    );
    let resume_strategy = control_click(
        ControlActionTarget::Global { command: "resume" },
        source_token.clone(),
        message,
        lang,
        "resume_strategy",
    );
    let kill_strategy = control_click(
        ControlActionTarget::Global {
            command: "kill_switch",
        },
        source_token.clone(),
        message,
        lang,
        "kill_switch",
    );
    let bottom_pause_strategy = control_click(
        ControlActionTarget::Global { command: "pause" },
        source_token.clone(),
        message,
        lang,
        "pause_strategy",
    );
    let bottom_resume_strategy = control_click(
        ControlActionTarget::Global { command: "resume" },
        source_token.clone(),
        message,
        lang,
        "resume_strategy",
    );
    let bottom_kill_strategy = control_click(
        ControlActionTarget::Global {
            command: "kill_switch",
        },
        source_token.clone(),
        message,
        lang,
        "kill_switch",
    );
    let symbol_action = |symbol: String, command: &'static str, label_key: &'static str| {
        let token_value = source_token.clone();
        let mut action_message = message;
        let mut action_data = data_signal;
        move |_| {
            let symbol_value = symbol.clone();
            let token_value = token_value.clone();
            wasm_bindgen_futures::spawn_local(async move {
                match post_symbol_control(&token_value, &symbol_value, command).await {
                    Ok(value) => {
                        let mut next = action_data();
                        if let Some(control_symbols) =
                            SymbolControlUpdate::from_response(&value).control_symbols
                        {
                            next.control_symbols = control_symbols.clone();
                        }
                        let refresh = refresh_spot_symbol_control(
                            &token_value,
                            next.control_symbols.clone(),
                            next.disabled.clone(),
                        )
                        .await;
                        next.control_symbols = refresh.control_symbols;
                        next.disabled = refresh.disabled;
                        let refresh_error = refresh.errors.first().cloned();
                        if let Some(error) = refresh_error {
                            action_message.set(format!(
                                "{}: {}; {}",
                                t(lang, label_key),
                                compact(&value),
                                error
                            ));
                        } else {
                            action_message.set(format!(
                                "{}: {}",
                                t(lang, label_key),
                                compact(&value)
                            ));
                        }
                        action_data.set(next);
                    }
                    Err(error) => action_message.set(error),
                }
            });
        }
    };
    let exchange_action = |exchange: String, command: &'static str, label_key: &'static str| {
        let token_value = source_token.clone();
        let mut action_message = message;
        let mut action_data = data_signal;
        move |_| {
            let exchange_value = exchange.clone();
            let token_value = token_value.clone();
            wasm_bindgen_futures::spawn_local(async move {
                match post_exchange_control(&token_value, &exchange_value, command).await {
                    Ok(value) => {
                        let mut next = action_data();
                        let refresh = refresh_spot_exchange_control(
                            &token_value,
                            next.exchanges.clone(),
                            next.disabled.clone(),
                        )
                        .await;
                        next.exchanges = refresh.exchanges;
                        next.disabled = refresh.disabled;
                        let refresh_error = refresh.errors.first().cloned();
                        if let Some(error) = refresh_error {
                            action_message.set(format!(
                                "{} {}: {}; {}",
                                t(lang, label_key),
                                t(lang, "queued"),
                                compact(&value),
                                error
                            ));
                        } else {
                            action_message.set(format!(
                                "{} {}: {}",
                                t(lang, label_key),
                                t(lang, "queued"),
                                compact(&value)
                            ));
                        }
                        action_data.set(next);
                    }
                    Err(error) => action_message.set(error),
                }
            });
        }
    };
    rsx! {
        section { id: "spot-arb", class: "spot-arb",
            div { class: "spot-control-strip",
                button { class: "button", onclick: pause_strategy, {s(lang, "pause_strategy")} }
                button { class: "button primary", onclick: resume_strategy, {s(lang, "resume_strategy")} }
                button { class: "button danger", onclick: kill_strategy, {s(lang, "kill_switch")} }
                span { class: "pill neutral", {s(lang, "credential_setup_external")} }
                span { class: "pill neutral", "{s(lang, \"refresh_target\")}: {panel_data.target_refresh_ms} ms" }
                span { class: "muted", {s(lang, "spot_control_note")} }
            }
            div { class: "runtime-strip spot-runtime-strip",
                div { span { {s(lang, "target_exchanges")} } strong { "{panel_data.target_exchanges_text}" } }
                div { span { {s(lang, "subscription_limit")} } strong { "{panel_data.subscription_limit}" } }
                div { span { {s(lang, "candidate_limit")} } strong { "{panel_data.candidate_limit}" } }
                div { span { {s(lang, "active_window")} } strong { "{panel_data.active_window_seconds}s" } }
            }
            div { class: "industrial-status-grid",
                div { class: "status-cell",
                    span { {s(lang, "subscribed_pairs")} }
                    strong { "{panel_data.selected_count}" }
                }
                div { class: "status-cell",
                    span { {s(lang, "candidate_pairs")} }
                    strong { "{panel_data.candidate_count}" }
                }
                div { class: "status-cell",
                    span { {s(lang, "active_books")} }
                    strong { "{active_books}" }
                }
                div { class: if stale_books == 0 { "status-cell" } else { "status-cell warn" },
                    span { {s(lang, "stale_books")} }
                    strong { "{stale_books}" }
                }
                div { class: "status-cell",
                    span { {s(lang, "accepted_spreads")} }
                    strong { "{accepted_spread_count}" }
                }
                div { class: "status-cell",
                    span { {s(lang, "coin_console")} }
                    strong { "{managed_symbol_count}" }
                }
                div { class: "status-cell",
                    span { {s(lang, "strategy_orders_count")} }
                    strong { "{derived.plans.len()}" }
                }
                div { class: "status-cell",
                    span { {s(lang, "total_equity")} }
                    strong { "{format_usdt(total_equity_usdt)}" }
                }
            }
            BalanceTrendPanel { data: BalanceTrendData::from_history(&balance_history), lang }
            div { class: "panel exchange-statistics-panel",
                div { class: "panel-title-row",
                    h2 { {s(lang, "exchange_statistics")} }
                    span { class: "pill neutral", "{exchange_stat_rows.len()} {s(lang, \"exchange\")}" }
                }
                div { class: "table-wrap compact-table",
                    table {
                        thead { tr {
                            th { {s(lang, "exchange")} }
                            th { {s(lang, "status")} }
                            th { {s(lang, "day_volume")} }
                            th { {s(lang, "taker_maker_volume")} }
                            th { {s(lang, "volume_share")} }
                            th { {s(lang, "api_calls")} }
                            th { {s(lang, "ws_data")} }
                            th { {s(lang, "private_ws_status")} }
                            th { {s(lang, "latency")} }
                        } }
                        tbody {
                            for row in exchange_stat_rows.iter() {
                                tr {
                                    td { "{row.exchange}" }
                                    td { StatusPill { value: row.connected, lang } }
                                    td { "{format_usdt(row.day_volume)}" }
                                    td { "{format_usdt(row.taker_volume)} / {format_usdt(row.maker_volume)}" }
                                    td { "{row.volume_share_pct:.2}" }
                                    td { "{row.api_calls}" }
                                    td { "{row.ws_data}" }
                                    td { "{row.private_ws}" }
                                    td { "{row.latency}" }
                                }
                            }
                            if exchange_stat_rows.is_empty() {
                                tr {
                                    td { colspan: "9", class: "empty-state", {s(lang, "no_runtime_timing")} }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "panel tab-panel",
                div { class: "tab-bar",
                    button { class: if console_tab() == "config" { "tab-button active" } else { "tab-button" }, onclick: move |_| console_tab.set("config".to_string()), {s(lang, "trade_config_console")} }
                    button { class: if console_tab() == "exchange" { "tab-button active" } else { "tab-button" }, onclick: move |_| console_tab.set("exchange".to_string()), {s(lang, "exchange_console")} }
                    button { class: if console_tab() == "account" { "tab-button active" } else { "tab-button" }, onclick: move |_| console_tab.set("account".to_string()), {s(lang, "account_console")} }
                }
                if console_tab() == "config" {
                    div { class: "tab-content",
                    div { class: "config-grid",
                        div { span { {s(lang, "monitored_pairs")} } strong { "{panel_data.cfg_monitored_symbols}" } }
                        div { span { {s(lang, "max_arbitrage_pairs")} } strong { "{panel_data.cfg_max_arbitrage_symbols}" } }
                        div { span { {s(lang, "used_slots")} } strong { "{panel_data.used_slots}" } }
                        div { span { {s(lang, "remaining_slots")} } strong { "{panel_data.remaining_slots}" } }
                        div { span { {s(lang, "per_arb_notional")} } strong { "{format_usdt(panel_data.cfg_notional)}" } }
                        div { span { {s(lang, "initial_entry_notional")} } strong { "{format_usdt(panel_data.cfg_initial_entry)}" } }
                        div { span { {s(lang, "initial_entry_progress")} } strong { "{panel_data.initial_completed} / {panel_data.initial_target}" } }
                        div { span { {s(lang, "initial_entry_observed")} } strong { "{panel_data.initial_observed}" } }
                        div { span { {s(lang, "failed")} } strong { "{panel_data.initial_failed}" } }
                        div { span { {s(lang, "pending")} } strong { "{panel_data.initial_pending}" } }
                        div { span { {s(lang, "arb_threshold")} } strong { "{panel_data.cfg_threshold_percent:.2}%" } }
                        div { span { {s(lang, "mode")} } strong { "{panel_data.trading_mode}" } }
                    }
                    div { class: "fee-strip",
                        for row in initial_entry_blocker_rows.iter().take(5) {
                            span { class: "chip warn", "{row.symbol} {row.rejection_reason}" }
                        }
                    }
                    div { class: "fee-strip",
                        for symbol in blocked_slot_symbols.iter().take(8) {
                            span { class: "chip warn", "{s(lang, \"blocked_pairs\")}: {value_text(symbol, lang)}" }
                        }
                    }
                    div { class: "fee-strip",
                        for row in fee_view_rows.iter().take(12) {
                            span { class: "chip", "{row.exchange} {row.symbol} M:{row.maker_fee_bps} T:{row.taker_fee_bps}" }
                        }
                    }
                    }
                }
                if console_tab() == "exchange" {
                    div { class: "tab-content",
                    div { class: "panel-title-row tab-title-row",
                        span { class: "muted", {s(lang, "exchange_console_note")} }
                    }
                div { class: "table-wrap compact-table",
                    table {
                        thead { tr {
                            th { {s(lang, "exchange")} } th { {s(lang, "status")} } th { {s(lang, "public_ws")} } th { {s(lang, "private_ws")} } th { {s(lang, "fresh")} } th { {s(lang, "stale")} } th { "USDT" } th { {s(lang, "available")} } th { {s(lang, "coin_assets")} } th { {s(lang, "fee_aux_asset")} } th { {s(lang, "latency")} } th { {s(lang, "max_latency")} } th { {s(lang, "parse_errors")} } th { {s(lang, "sequence_gaps")} } th { {s(lang, "actions")} }
                        } }
                        tbody {
                            for row in exchange_console_view_rows.iter() {
                                tr {
                                    td { "{row.exchange}" }
                                    td { StatusPill { value: row.enabled, lang } }
                                    td { StatusPill { value: row.public_ws_connected, lang } }
                                    td { StatusPill { value: row.private_ws_connected, lang } }
                                    td { "{row.fresh_symbol_count}" }
                                    td { "{row.stale_symbol_count}" }
                                    td { "{row.usdt_total}" }
                                    td { "{row.usdt_available}" }
                                    td { "{row.coin_assets}" }
                                    td { "{row.fee_aux_asset}" }
                                    td { "{row.avg_latency_ms}" }
                                    td { "{row.max_latency_ms}" }
                                    td { "{row.parse_error_count}" }
                                    td { "{row.sequence_gap_count}" }
                                    td {
                                        div { class: "row-actions",
                                            button { class: "mini-button", onclick: exchange_action(row.exchange.clone(), "pause", "exchange_pause"), {s(lang, "stop")} }
                                            button { class: "mini-button primary", onclick: exchange_action(row.exchange.clone(), "resume", "exchange_resume"), {s(lang, "enable")} }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                    }
            }
                if console_tab() == "account" {
                    div { class: "tab-content",
                div { class: "config-grid",
                    div { span { {s(lang, "total_equity")} } strong { "{format_usdt(total_equity_usdt)}" } }
                    div { span { {s(lang, "available_equity")} } strong { "{format_usdt(available_equity_usdt)}" } }
                    div { span { {s(lang, "residual_assets")} } strong { "{format_usdt(residual_assets_usdt)}" } }
                }
                div { class: "table-wrap compact-table",
                    table {
                        thead { tr {
                            th { {s(lang, "exchange")} } th { {s(lang, "account_label")} } th { {s(lang, "enabled_account")} } th { {s(lang, "status")} } th { {s(lang, "quote_asset")} } th { "USDT" } th { {s(lang, "available_equity")} } th { {s(lang, "coin_balance")} } th { {s(lang, "coin_assets")} } th { {s(lang, "fee_aux_asset")} } th { {s(lang, "latency")} } th { {s(lang, "equity")} } th { {s(lang, "update_time")} }
                        } }
                        tbody {
                            for row in account_console_view_rows.iter() {
                                tr {
                                    td { "{row.exchange}" }
                                    td { "{row.account_label}" }
                                    td { StatusPill { value: row.enabled, lang } }
                                    td { StatusPill { value: row.connected, lang } }
                                    td { "{row.quote_asset}" }
                                    td { "{row.usdt_total}" }
                                    td { "{row.usdt_available}" }
                                    td { "{row.coin_balance_usdt}" }
                                    td { "{row.coin_assets}" }
                                    td { "{row.fee_aux_asset}" }
                                    td { "{row.avg_latency_ms}" }
                                    td { "{row.total_equity_usdt}" }
                                    td { "{row.last_book_update_at}" }
                                }
                            }
                        }
                    }
                }
                    }
                }
            }
            div { class: "panel tab-panel",
                div { class: "tab-bar",
                    button { class: if market_tab() == "subscriptions" { "tab-button active" } else { "tab-button" }, onclick: move |_| market_tab.set("subscriptions".to_string()), {s(lang, "subscribed_spot_pairs")} }
                    button { class: if market_tab() == "books" { "tab-button active" } else { "tab-button" }, onclick: move |_| market_tab.set("books".to_string()), {s(lang, "spot_book_health")} }
                    button { class: if market_tab() == "coins" { "tab-button active" } else { "tab-button" }, onclick: move |_| market_tab.set("coins".to_string()), {s(lang, "coin_console")} }
                    button { class: if market_tab() == "positions" { "tab-button active" } else { "tab-button" }, onclick: move |_| market_tab.set("positions".to_string()), {s(lang, "positions")} }
                }
                if market_tab() == "subscriptions" {
                    div { class: "tab-content",
                        div { class: "symbol-chips",
                            for symbol in selected.iter().take(5) {
                                span { class: "chip active", "{value_text(symbol, lang)}" }
                            }
                            if selected.is_empty() {
                                span { class: "muted", {s(lang, "no_subscriptions")} }
                            }
                        }
                        h3 { {s(lang, "candidate_pool")} }
                        div { class: "symbol-cloud",
                            for symbol in candidates.iter().take(200) {
                                span { class: "chip", "{value_text(symbol, lang)}" }
                            }
                        }
                    }
                }
                if market_tab() == "books" {
                    div { class: "tab-content",
                        div { class: "book-summary",
                            span { class: "pill", "{active_books} {s(lang, \"fresh\")}" }
                            span { class: if stale_books == 0 { "pill" } else { "pill warn" }, "{stale_books} {s(lang, \"stale\")}" }
                        }
                        div { class: "table-wrap compact-table",
                            table {
                                thead { tr {
                                    th { {s(lang, "symbol")} } th { {s(lang, "exchange")} } th { {s(lang, "bid")} } th { {s(lang, "ask")} } th { {s(lang, "spread_bps")} } th { {s(lang, "age_ms")} } th { {s(lang, "latency")} } th { {s(lang, "source")} } th { {s(lang, "sequence")} } th { {s(lang, "stale_reason")} }
                                } }
                                tbody {
                                    for row in book_view_rows.iter().take(80) {
                                        tr {
                                            td { "{row.symbol}" }
                                            td { "{row.exchange}" }
                                            td { "{row.best_bid}" }
                                            td { "{row.best_ask}" }
                                            td { "{row.spread_bps}" }
                                            td { "{row.book_age_ms}" }
                                            td { "{row.latency_ms}" }
                                            td { "{row.source}" }
                                            td { "{row.sequence}" }
                                            td { "{row.stale_reason}" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if market_tab() == "coins" {
                    div { class: "tab-content",
                        div { class: "operator-filter-bar",
                            div { class: "status-filter-tabs",
                                for (key, label_key, count) in [
                                    ("all", "filter_all", symbol_rows.len()),
                                    ("arbitraging", "filter_arbitraging", active_symbol_count),
                                    ("exiting", "filter_exiting", exiting_symbol_count),
                                    ("anomaly", "filter_anomaly", anomaly_symbol_count),
                                ] {
                                    button {
                                        class: if symbol_filter() == key { "filter-button active" } else { "filter-button" },
                                        onclick: move |_| {
                                            symbol_filter.set(key.to_string());
                                            symbol_page.set(0);
                                        },
                                        "{s(lang, label_key)} {count}"
                                    }
                                }
                            }
                            label { class: "table-search",
                                input {
                                    value: "{symbol_search()}",
                                    placeholder: s(lang, "symbol_search"),
                                    oninput: move |event| {
                                        symbol_search.set(event.value());
                                        symbol_page.set(0);
                                    }
                                }
                            }
                            span { class: "pill neutral", "{s(lang, \"visible_rows\")}: {filtered_symbol_rows.len()}" }
                        }
                        div { class: "panel-title-row",
                            Pager { page: symbol_page(), total_pages: symbol_total_pages, on_prev: move |_| symbol_page.set(symbol_page().saturating_sub(1)), on_next: move |_| symbol_page.set((symbol_page() + 1).min(symbol_total_pages.saturating_sub(1))), lang }
                        }
                        div { class: "table-wrap compact-table",
                            table {
                                thead { tr {
                                    th { {s(lang, "symbol")} } th { {s(lang, "status")} } th { "Lifecycle" } th { {s(lang, "occupied_capital")} } th { {s(lang, "arb_exchanges")} } th { {s(lang, "trade_volume")} } th { {s(lang, "est_profit")} } th { {s(lang, "realized_pnl")} } th { "1H" } th { "8H" } th { "24H" } th { {s(lang, "actions")} }
                            } }
                            tbody {
                                    if filtered_symbol_rows.is_empty() {
                                        tr {
                                            td { colspan: "12", class: "empty-state", {s(lang, "no_managed_arb_pairs")} }
                                        }
                                    }
                                    for row in filtered_symbol_rows.iter().skip(symbol_start).take(symbol_page_size) {
                                        tr {
                                            class: if normalize_symbol_text(&row.symbol) == detail_symbol_key.as_str() { "clickable-row active" } else { "clickable-row" },
                                            onclick: {
                                                let symbol = row.symbol.clone();
                                                move |_| selected_symbol.set(symbol.clone())
                                            },
                                            td { "{row.symbol}" }
                                            td { span { class: if row.disabled { "pill bad" } else if row.subscribed { "pill" } else { "pill warn" }, "{row.state}" } }
                                            td { "{row.control_state}" }
                                            td { "{format_usdt(row.capital)}" }
                                            td { "{row.exchanges}" }
                                            td { "{format_usdt(row.volume)}" }
                                            td { class: profit_class(row.est_profit), "{signed_usdt(row.est_profit)}" }
                                            td { class: profit_class(row.realized_pnl), "{signed_usdt(row.realized_pnl)}" }
                                            td { class: profit_class(row.pnl_1h), "{signed_usdt(row.pnl_1h)}" }
                                            td { class: profit_class(row.pnl_8h), "{signed_usdt(row.pnl_8h)}" }
                                            td { class: profit_class(row.pnl_24h), "{signed_usdt(row.pnl_24h)}" }
                                            td {
                                                if row.controllable {
                                                div {
                                                    class: "row-actions",
                                                    onclick: move |event| event.stop_propagation(),
                                                    button { class: "mini-button", onclick: symbol_action(row.symbol.clone(), "pause", "symbol_pause"), {s(lang, "pause")} }
                                                    button { class: "mini-button primary", onclick: symbol_action(row.symbol.clone(), "resume", "symbol_resume"), {s(lang, "start")} }
                                                    button { class: "mini-button danger", onclick: symbol_action(row.symbol.clone(), "disable", "symbol_disable"), {s(lang, "stop_liquidate")} }
                                                }
                                                } else {
                                                    span { class: "muted", "-" }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        div { class: "symbol-detail-panel",
                            div { class: "panel-title-row",
                                div {
                                    h3 { {s(lang, "selected_symbol_detail")} }
                                    p { class: "muted",
                                        if detail_symbol.is_empty() {
                                            {s(lang, "select_symbol_hint")}
                                        } else {
                                            "{detail_symbol}"
                                        }
                                    }
                                }
                                div { class: "row-actions",
                                    span { class: "pill neutral", "{s(lang, \"related_books\")}: {derived.detail_books.len()}" }
                                    span { class: "pill neutral", "{s(lang, \"related_spreads\")}: {derived.detail_opportunities.len()}" }
                                    span { class: "pill neutral", "{s(lang, \"related_orders\")}: {derived.detail_plans.len()}" }
                                }
                            }
                            div { class: "symbol-exchange-strip",
                                for row in detail_exchange_rows.iter() {
                                    div { class: "exchange-mini-card {row.class_name}",
                                        div { class: "exchange-mini-head",
                                            strong { "{row.exchange}" }
                                            span { "{row.state}" }
                                        }
                                        div { class: "exchange-mini-grid",
                                            span { {s(lang, "bid")} } strong { "{row.bid}" }
                                            span { {s(lang, "ask")} } strong { "{row.ask}" }
                                            span { {s(lang, "spread_bps")} } strong { "{row.spread_bps}" }
                                            span { {s(lang, "age_ms")} } strong { "{row.age_ms}" }
                                            span { {s(lang, "order_count")} } strong { "{row.order_count}" }
                                            span { {s(lang, "notional")} } strong { "{format_usdt(row.notional)}" }
                                        }
                                    }
                                }
                                if detail_exchange_rows.is_empty() {
                                    div { class: "empty-state", {s(lang, "select_symbol_hint")} }
                                }
                            }
                            div { class: "symbol-detail-grid",
                                div { class: "detail-box",
                                    h3 { {s(lang, "related_books")} }
                                    div { class: "table-wrap compact-table detail-table",
                                        table {
                                            thead { tr { th { {s(lang, "exchange")} } th { {s(lang, "bid")} } th { {s(lang, "ask")} } th { {s(lang, "spread_bps")} } th { {s(lang, "age_ms")} } } }
                                            tbody {
                                                for row in detail_book_view_rows.iter().take(12) {
                                                    tr {
                                                        td { "{row.exchange}" }
                                                        td { "{row.best_bid}" }
                                                        td { "{row.best_ask}" }
                                                        td { "{row.spread_bps}" }
                                                        td { "{row.book_age_ms}" }
                                                    }
                                                }
                                                if derived.detail_books.is_empty() {
                                                    tr { td { colspan: "5", class: "empty-state", "-" } }
                                                }
                                            }
                                        }
                                    }
                                }
                                div { class: "detail-box",
                                    h3 { {s(lang, "related_spreads")} }
                                    div { class: "table-wrap compact-table detail-table",
                                        table {
                                            thead { tr { th { {s(lang, "route")} } th { {s(lang, "tt_net_bps")} } th { {s(lang, "est_profit")} } th { {s(lang, "accepted")} } } }
                                            tbody {
                                                for row in detail_opp_view_rows.iter().take(12) {
                                                    tr {
                                                        td { "{row.route}" }
                                                        td { class: profit_class(row.tt_immediate_net_bps), "{row.tt_immediate_net_bps_text}" }
                                                        td { class: profit_class(row.tt_immediate_net_pnl), "{row.tt_immediate_net_pnl_text}" }
                                                        td { StatusPill { value: row.accepted, lang } }
                                                    }
                                                }
                                                if derived.detail_opportunities.is_empty() {
                                                    tr { td { colspan: "4", class: "empty-state", "-" } }
                                                }
                                            }
                                        }
                                    }
                                }
                                div { class: "detail-box",
                                    h3 { {s(lang, "related_orders")} }
                                    div { class: "table-wrap compact-table detail-table",
                                        table {
                                            thead { tr { th { {s(lang, "exchange")} } th { {s(lang, "side")} } th { {s(lang, "remaining_qty")} } th { {s(lang, "price")} } th { {s(lang, "status")} } } }
                                            tbody {
                                                for row in detail_plan_view_rows.iter().take(12) {
                                                    tr {
                                                        td { "{row.exchange}" }
                                                        td { "{row.side}" }
                                                        td { "{row.remaining_qty}" }
                                                        td { "{row.price}" }
                                                        td { "{row.status}" }
                                                    }
                                                }
                                                if derived.detail_plans.is_empty() {
                                                    tr { td { colspan: "5", class: "empty-state", "-" } }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if market_tab() == "positions" {
                    div { class: "tab-content",
                        div { class: "table-wrap compact-table",
                            table {
                                thead { tr { th { {s(lang, "exchange")} } th { {s(lang, "symbol")} } th { {s(lang, "asset")} } th { {s(lang, "total")} } th { {s(lang, "available")} } th { {s(lang, "reserved")} } th { {s(lang, "locked")} } th { {s(lang, "equity")} } } }
                                tbody {
                                    for row in position_rows.iter().take(80) {
                                        tr {
                                            td { "{row.exchange}" }
                                            td { "{row.symbol}" }
                                            td { "{row.asset}" }
                                            td { "{row.total}" }
                                            td { "{row.available}" }
                                            td { "{row.reserved}" }
                                            td { "{row.locked}" }
                                            td { "{row.valuation}" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "panel spread-analysis-panel",
                h2 { {s(lang, "spread_analysis")} }
                div { class: "table-wrap spread-analysis-table",
                    table {
                        thead { tr {
                            th { {s(lang, "symbol")} } th { {s(lang, "route")} } th { {s(lang, "buy_vwap")} } th { {s(lang, "sell_vwap")} } th { {s(lang, "tt_net_bps")} } th { {s(lang, "est_profit")} } th { {s(lang, "capital")} } th { {s(lang, "confidence")} } th { {s(lang, "expected_roc")} } th { {s(lang, "risk_score")} } th { {s(lang, "accepted")} } th { {s(lang, "rejection_reasons")} } th { {s(lang, "warnings")} }
                        } }
                        tbody {
                            for row in best_opp_view_rows.iter().take(10) {
                                tr {
                                    td { "{row.symbol}" }
                                    td { "{row.route}" }
                                    td { "{row.buy_vwap}" }
                                    td { "{row.sell_vwap}" }
                                    td { class: profit_class(row.tt_immediate_net_bps), "{row.tt_immediate_net_bps_text}" }
                                    td { class: profit_class(row.tt_immediate_net_pnl), "{row.tt_immediate_net_pnl_text}" }
                                    td { "{row.required_capital_usdt}" }
                                    td { "{row.confidence}" }
                                    td { class: profit_class(row.expected_return_on_capital), "{row.expected_return_on_capital_text}" }
                                    td { class: profit_class(row.risk_adjusted_score), "{row.risk_adjusted_score_text}" }
                                    td { StatusPill { value: row.accepted, lang } }
                                    td { "{row.rejection_reasons}" }
                                    td { "{row.warnings}" }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "two spot-main",
                div { class: "panel",
                    div { class: "panel-title-row",
                        h2 { {s(lang, "strategy_orders")} }
                        Pager { page: plan_page(), total_pages: plan_total_pages, on_prev: move |_| plan_page.set(plan_page().saturating_sub(1)), on_next: move |_| plan_page.set((plan_page() + 1).min(plan_total_pages.saturating_sub(1))), lang }
                    }
                    div { class: "table-wrap compact-table",
                        table {
                            thead { tr {
                                th { {s(lang, "time")} } th { {s(lang, "source")} } th { {s(lang, "exchange")} } th { {s(lang, "symbol")} } th { {s(lang, "side")} } th { {s(lang, "remaining_qty")} } th { {s(lang, "price")} } th { {s(lang, "notional")} } th { {s(lang, "status")} } th { {s(lang, "order_ref")} }
                            } }
                            tbody {
                                for row in plan_view_rows.iter().skip(plan_start).take(plan_page_size) {
                                    tr {
                                        td { "{row.timestamp}" }
                                        td { "{row.source}" }
                                        td { "{row.exchange}" }
                                        td { "{row.symbol}" }
                                        td { "{row.side}" }
                                        td { "{row.remaining_qty}" }
                                        td { "{row.price}" }
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
                    h2 { {s(lang, "aux_disabled_symbols")} }
                    div { class: "table-wrap compact-table",
                        table {
                            thead { tr {
                                th { {s(lang, "exchange")} } th { {s(lang, "asset")} } th { {s(lang, "total")} } th { {s(lang, "available")} } th { {s(lang, "equity")} } th { {s(lang, "status")} }
                            } }
                            tbody {
                                for row in aux_rows.iter() {
                                    tr {
                                        td { "{row.exchange}" }
                                        td { "{row.asset}" }
                                        td { "{row.total}" }
                                        td { "{row.available}" }
                                        td { "{row.valuation}" }
                                        td { StatusPill { value: row.configured, lang } }
                                    }
                                }
                            }
                        }
                    }
                    if aux_rows.is_empty() {
                        div { class: "empty-state", {s(lang, "no_aux_assets")} }
                    }
                    div { class: "symbol-cloud stopped-symbols",
                        for symbol in disabled_symbols.iter() {
                            span { class: "chip stopped", "{symbol}" }
                        }
                    }
                }
            }
            div { class: "panel anomaly-panel",
                div { class: "panel-title-row",
                    h2 { {s(lang, "anomaly_trade_info")} }
                    span { class: if anomaly_rows.is_empty() { "pill" } else { "pill warn" }, "{anomaly_rows.len()}" }
                }
                div { class: "table-wrap compact-table anomaly-table",
                    table {
                        thead { tr {
                            th { {s(lang, "symbol")} }
                            th { {s(lang, "inst_id_key")} }
                            th { {s(lang, "current_price")} }
                            th { {s(lang, "anomaly_reason")} }
                        } }
                        tbody {
                            for row in anomaly_rows.iter().take(24) {
                                tr { class: "anomaly-row {row.class_name}",
                                    td { "{row.symbol}" }
                                    td { "{row.inst_id_key}" }
                                    td { "{row.price}" }
                                    td { "{row.reason}" }
                                }
                            }
                            if anomaly_rows.is_empty() {
                                tr {
                                    td { colspan: "4", class: "empty-state", {s(lang, "no_exception_logs")} }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "panel global-control-panel",
                div { class: "global-control-grid",
                    div { class: "control-group",
                        span { {s(lang, "global_trade_status")} }
                        strong { class: "status-dot-text",
                            span { class: if stale_books == 0 && anomaly_rows.is_empty() { "status-dot" } else { "status-dot warn" } }
                            if stale_books == 0 && anomaly_rows.is_empty() {
                                {s(lang, "running")}
                            } else {
                                {s(lang, "warnings")}
                            }
                        }
                    }
                    div { class: "control-group",
                        span { {s(lang, "global_control")} }
                        div { class: "row-actions",
                            button { class: "mini-button", onclick: bottom_pause_strategy, {s(lang, "pause_strategy")} }
                            button { class: "mini-button primary", onclick: bottom_resume_strategy, {s(lang, "resume_strategy")} }
                        }
                    }
                    div { class: "control-group",
                        span { {s(lang, "risk_operations")} }
                        div { class: "row-actions",
                            button { class: "mini-button danger", onclick: bottom_kill_strategy, {s(lang, "kill_switch")} }
                        }
                    }
                    div { class: "control-group",
                        span { {s(lang, "batch_operations")} }
                        div { class: "row-actions",
                            span { class: "pill neutral", "{selected.len()} {s(lang, \"subscribed_pairs\")}" }
                            span { class: "pill neutral", "{disabled_symbols.len()} {s(lang, \"stopped\")}" }
                        }
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
                            "{s(lang, label_key)} {strategy_log_count(&strategy_logs, key)}"
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

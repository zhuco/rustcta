use dioxus::prelude::*;
use gloo_timers::future::TimeoutFuture;

use crate::api::{
    fetch_dashboard, fetch_strategy_live_data, post_control_command, DashboardFetch,
    StrategyLiveFetch,
};
use crate::api_keys::ApiKeysPanel;
use crate::cross_arb::CrossArbPanel;
use crate::funding_arb::FundingArbPanel;
use crate::i18n::{command_label, s, t};
use crate::spot_arb::SpotArbPanel;
use crate::storage::{
    load_active_view, load_language, load_token, save_active_view, save_language, save_token,
};
use crate::types::{ControlPanelView, DashboardData, Language};
use crate::ui::ControlActionPanel;
use crate::workspace::StrategyWorkspacePanel;

#[derive(Clone, Copy)]
struct ActiveViewContext {
    data: Signal<DashboardData>,
    message: Signal<String>,
    active_view: Signal<ControlPanelView>,
    api_key_exchange: Signal<String>,
    api_key_account: Signal<String>,
    api_key_namespace: Signal<String>,
}

#[derive(Clone, Copy)]
struct DashboardRefreshContext {
    data: Signal<DashboardData>,
    refresh_health: Signal<String>,
    refresh_error_count: Signal<usize>,
    last_refresh: Signal<String>,
    message: Signal<String>,
}

#[component]
pub(crate) fn App() -> Element {
    let mut token = use_signal(load_token);
    let mut language = use_signal(load_language);
    let data = use_signal(DashboardData::default);
    let mut message = use_signal(String::new);
    let refresh_health = use_signal(|| "-".to_string());
    let refresh_error_count = use_signal(|| 0usize);
    let mut auto_refresh = use_signal(|| true);
    let last_refresh = use_signal(|| "-".to_string());
    let mut active_view = use_signal(load_active_view);
    let api_key_exchange = use_signal(|| "gate".to_string());
    let api_key_account = use_signal(|| "default".to_string());
    let api_key_namespace = use_signal(String::new);
    let view_context = ActiveViewContext {
        data,
        message,
        active_view,
        api_key_exchange,
        api_key_account,
        api_key_namespace,
    };
    let refresh_context = DashboardRefreshContext {
        data,
        refresh_health,
        refresh_error_count,
        last_refresh,
        message,
    };

    let refresh = move |_| {
        let token_value = token();
        let lang = language();
        let refresh_context = refresh_context;
        wasm_bindgen_futures::spawn_local(async move {
            let result = fetch_dashboard(&token_value, (refresh_context.data)()).await;
            apply_dashboard_fetch(result, lang, true, refresh_context);
        });
    };

    let pause = command_handler("pause", token, language, message);
    let resume = command_handler("resume", token, language, message);
    let kill = command_handler("kill_switch", token, language, message);
    let lang = language();
    let _dashboard_refresh = use_future(move || {
        let refresh_context = refresh_context;
        async move {
            loop {
                let token_value = token();
                if auto_refresh() {
                    let result = fetch_dashboard(&token_value, (refresh_context.data)()).await;
                    apply_dashboard_fetch(result, lang, false, refresh_context);
                }
                TimeoutFuture::new(15_000).await;
            }
        }
    });
    let _strategy_live_refresh = use_future(move || {
        let data = data;
        let message = message;
        let refresh_error_count = refresh_error_count;
        async move {
            loop {
                let token_value = token();
                if auto_refresh() {
                    let result = fetch_strategy_live_data(&token_value, data()).await;
                    apply_strategy_live_fetch(result, data, refresh_error_count, message);
                }
                TimeoutFuture::new(1000).await;
            }
        }
    });

    rsx! {
        document::Stylesheet {
            href: asset!("/assets/main.css", AssetOptions::css().with_static_head(true))
        }
        div { class: "shell",
            aside { class: "sidebar",
                div { class: "brand",
                    strong { "RustCTA Control" }
                    span { {s(lang, "brand_subtitle")} }
                }
                div { class: "language-switch", "aria-label": s(lang, "language"),
                    button {
                        class: if lang.is_zh() { "lang-button active" } else { "lang-button" },
                        onclick: move |_| {
                            save_language(Language::Zh);
                            language.set(Language::Zh);
                            message.set(t(Language::Zh, "language_set").to_string());
                        },
                        "中文"
                    }
                    button {
                        class: if lang == Language::En { "lang-button active" } else { "lang-button" },
                        onclick: move |_| {
                            save_language(Language::En);
                            language.set(Language::En);
                            message.set(t(Language::En, "language_set").to_string());
                        },
                        "English"
                    }
                }
                label { class: "token",
                    span { {s(lang, "auth_token")} }
                    input {
                        r#type: "password",
                        value: "{token()}",
                        placeholder: s(lang, "bearer_token"),
                        oninput: move |event| {
                            let value = event.value();
                            save_token(&value);
                            token.set(value);
                        }
                    }
                }
                nav { class: "nav",
                    for view in ControlPanelView::ALL {
                        button {
                            class: nav_class(active_view(), view),
                            onclick: move |_| {
                                save_active_view(view);
                                active_view.set(view);
                            },
                            {s(lang, view.nav_label_key())}
                        }
                    }
                }
                p { class: "muted", {s(lang, "frontend_boundary")} }
            }
            main { class: "main",
                div { class: "topbar",
                    div {
                        h1 { {s(lang, "control_panel")} }
                    }
                    div { class: "actions",
                        span {
                            class: if refresh_error_count() == 0 { "refresh-health" } else { "refresh-health warn" },
                            "{refresh_health()}"
                        }
                        span { class: "refresh-time", "{s(lang, \"last_refresh\")}: {last_refresh()}" }
                        if !message().is_empty() {
                            span { class: "topbar-message", "{message()}" }
                        }
                        button {
                            class: if auto_refresh() { "button refresh-toggle active" } else { "button refresh-toggle" },
                            onclick: move |_| auto_refresh.set(!auto_refresh()),
                            if auto_refresh() {
                                {s(lang, "pause_refresh")}
                            } else {
                                {s(lang, "resume_refresh")}
                            }
                        }
                        button { class: "button primary", onclick: refresh, {s(lang, "refresh")} }
                        button { class: "button", onclick: pause, {s(lang, "pause")} }
                        button { class: "button", onclick: resume, {s(lang, "resume")} }
                        button { class: "button danger", onclick: kill, {s(lang, "kill_switch")} }
                    }
                }
                if is_auth_error(&message()) {
                    div { class: "modal-backdrop",
                        div { class: "error-dialog", role: "alertdialog", "aria-modal": "true",
                            div { class: "error-dialog-title", "认证 Token 错误" }
                            p { "{message()}" }
                            div { class: "row-actions",
                                button {
                                    class: "button danger",
                                    onclick: move |_| message.set(String::new()),
                                    "OK"
                                }
                            }
                        }
                    }
                }
                {render_active_view(active_view(), view_context, token(), lang)}
            }
        }
    }
}

fn is_auth_error(message: &str) -> bool {
    message.contains("AUTH_TOKEN_ERROR") || message.contains("HTTP 401")
}

fn render_active_view(
    view: ControlPanelView,
    context: ActiveViewContext,
    token: String,
    lang: Language,
) -> Element {
    let ActiveViewContext {
        data,
        message,
        active_view,
        api_key_exchange,
        api_key_account,
        api_key_namespace,
    } = context;
    match view {
        ControlPanelView::Workspace => {
            rsx! { StrategyWorkspacePanel { data, token, message, lang } }
        }
        ControlPanelView::SpotArb => {
            let snapshot = data();
            rsx! {
                SpotArbPanel {
                    data,
                    spot_arb: snapshot.spot_arb,
                    books: snapshot.books,
                    opportunities: snapshot.opportunities,
                    plans: snapshot.dry_run_plans,
                    inventory: snapshot.inventory,
                    exchanges: snapshot.exchanges,
                    fees: snapshot.fees,
                    logs: snapshot.logs,
                    health: snapshot.health,
                    risk: snapshot.risk,
                    config: snapshot.config,
                    disabled: snapshot.disabled,
                    control_symbols: snapshot.control_symbols,
                    balance_history: snapshot.balance_history,
                    strategy_logs: snapshot.strategy_logs,
                    token,
                    message,
                    lang
                }
            }
        }
        ControlPanelView::CrossArb => {
            let snapshot = data();
            rsx! {
                CrossArbPanel {
                    cross_arb: snapshot.cross_arb,
                    api_keys: snapshot.api_keys,
                    status: snapshot.status,
                    processes: snapshot.processes,
                    token,
                    message,
                    lang,
                    active_view,
                    api_key_exchange,
                    api_key_account,
                    api_key_namespace
                }
                ControlActionPanel { lang }
            }
        }
        ControlPanelView::FundingArb => {
            let snapshot = data();
            rsx! {
                FundingArbPanel {
                    api_keys: snapshot.api_keys,
                    processes: snapshot.processes,
                    token,
                    message,
                    lang,
                    active_view,
                    api_key_exchange,
                    api_key_account,
                    api_key_namespace
                }
            }
        }
        ControlPanelView::ApiKeys => {
            rsx! {
                ApiKeysPanel {
                    data,
                    token,
                    message,
                    lang,
                    selected_exchange: api_key_exchange(),
                    selected_account: api_key_account(),
                    selected_namespace: api_key_namespace()
                }
                ControlActionPanel { lang }
            }
        }
    }
}

fn nav_class(active: ControlPanelView, target: ControlPanelView) -> &'static str {
    if active == target {
        "nav-link active"
    } else {
        "nav-link"
    }
}

fn command_handler(
    command: &'static str,
    token: Signal<String>,
    language: Signal<Language>,
    mut message: Signal<String>,
) -> impl FnMut(Event<MouseData>) + 'static {
    move |_| {
        let token_value = token();
        let lang = language();
        wasm_bindgen_futures::spawn_local(async move {
            match post_control_command(&token_value, command).await {
                Ok(value) => message.set(format!(
                    "{} {}: {}",
                    command_label(lang, command),
                    t(lang, "queued"),
                    crate::utils::compact(&value)
                )),
                Err(error) => message.set(error),
            }
        });
    }
}

fn apply_dashboard_fetch(
    result: DashboardFetch,
    lang: Language,
    announce_success: bool,
    context: DashboardRefreshContext,
) {
    let DashboardRefreshContext {
        mut data,
        mut refresh_health,
        mut refresh_error_count,
        mut last_refresh,
        mut message,
    } = context;
    let updated = result.updated;
    let error_count = result.errors.len();
    let first_error = result.errors.first().cloned();
    data.set(result.data);
    refresh_error_count.set(error_count);
    refresh_health.set(refresh_health_text(lang, updated, error_count));
    last_refresh.set(refresh_time_text());
    if error_count == 0 {
        if announce_success {
            message.set(t(lang, "dashboard_refreshed").to_string());
        }
    } else {
        message.set(format!(
            "{} {}",
            t(lang, "refresh_partial"),
            first_error.unwrap_or_default()
        ));
    }
}

fn apply_strategy_live_fetch(
    result: StrategyLiveFetch,
    mut data: Signal<DashboardData>,
    mut refresh_error_count: Signal<usize>,
    mut message: Signal<String>,
) {
    let StrategyLiveFetch {
        spot_arb,
        cross_arb,
        strategy_logs,
        updated,
        errors,
    } = result;
    if updated > 0 {
        let mut next = data();
        next.spot_arb = spot_arb;
        next.cross_arb = cross_arb;
        next.strategy_logs = strategy_logs;
        data.set(next);
    }
    if errors.is_empty() {
        return;
    }
    refresh_error_count.set(refresh_error_count() + errors.len());
    if let Some(error) = errors.first() {
        message.set(error.clone());
    }
}

fn refresh_health_text(lang: Language, updated: usize, stale: usize) -> String {
    if stale == 0 {
        format!("{}: {updated}", t(lang, "refresh_sources"))
    } else {
        format!(
            "{}: {updated} / {}: {stale}",
            t(lang, "refresh_sources"),
            t(lang, "stale_sources")
        )
    }
}

fn refresh_time_text() -> String {
    let date = js_sys::Date::new_0();
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        date.get_hours(),
        date.get_minutes(),
        date.get_seconds(),
        date.get_milliseconds()
    )
}

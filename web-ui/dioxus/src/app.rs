use dioxus::prelude::*;
use gloo_timers::future::TimeoutFuture;

use crate::api::{fetch_dashboard, DashboardFetch};
use crate::console_pages::{ExchangeConsolePanel, LogsConsolePanel, TradeHistoryPanel};
use crate::i18n::{s, t};
use crate::storage::{load_active_view, load_language, save_active_view, save_language};
use crate::strategies::StrategiesPanel;
use crate::types::{ControlPanelView, DashboardData, Language};

#[derive(Clone, Copy)]
struct ActiveViewContext {
    data: Signal<DashboardData>,
    message: Signal<String>,
    active_view: Signal<ControlPanelView>,
}

#[derive(Clone, Copy)]
struct DashboardRefreshContext {
    data: Signal<DashboardData>,
    message: Signal<String>,
}

#[component]
pub(crate) fn App() -> Element {
    let token = use_signal(String::new);
    let mut language = use_signal(load_language);
    let data = use_signal(DashboardData::default);
    let mut message = use_signal(String::new);
    let mut active_view = use_signal(load_active_view);
    let view_context = ActiveViewContext {
        data,
        message,
        active_view,
    };
    let refresh_context = DashboardRefreshContext { data, message };

    let lang = language();
    let _dashboard_refresh = use_future(move || {
        let refresh_context = refresh_context;
        async move {
            loop {
                let token_value = token();
                let result = fetch_dashboard(&token_value, (refresh_context.data)()).await;
                apply_dashboard_fetch(result, lang, refresh_context);
                TimeoutFuture::new(15_000).await;
            }
        }
    });

    rsx! {
        document::Stylesheet {
            href: asset!("/assets/main.css", AssetOptions::css().with_static_head(true))
        }
        div { class: "shell top-shell",
            header { class: "app-header",
                div { class: "brand",
                    strong { "RustCTA Control" }
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
                div { class: "header-tools",
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
                }
            }
            main { class: "main",
                {render_active_view(active_view(), view_context, token(), lang)}
            }
        }
    }
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
        active_view: _active_view,
    } = context;
    match view {
        ControlPanelView::StrategyConsole => {
            rsx! {
                StrategiesPanel {
                    data,
                    token,
                    message,
                    lang
                }
            }
        }
        ControlPanelView::TradeHistory => {
            let snapshot = data();
            rsx! {
                TradeHistoryPanel {
                    data: snapshot,
                    lang
                }
            }
        }
        ControlPanelView::ExchangeConsole => {
            rsx! {
                ExchangeConsolePanel {
                    data,
                    token,
                    message,
                    lang
                }
            }
        }
        ControlPanelView::Logs => {
            let snapshot = data();
            rsx! {
                LogsConsolePanel {
                    data: snapshot,
                    lang
                }
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

fn apply_dashboard_fetch(result: DashboardFetch, lang: Language, context: DashboardRefreshContext) {
    let DashboardRefreshContext {
        mut data,
        mut message,
    } = context;
    let error_count = result.errors.len();
    let first_error = result.errors.first().cloned();
    data.set(result.data);
    if error_count > 0 {
        message.set(format!(
            "{} {}",
            t(lang, "refresh_partial"),
            first_error.unwrap_or_default()
        ));
    }
}

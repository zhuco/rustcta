use dioxus::prelude::*;

use crate::api::{post_control_command, post_exchange_control, post_symbol_control};
use crate::i18n::{s, t};
use crate::types::Language;
use crate::utils::compact;

#[derive(Clone)]
pub(crate) enum ControlActionTarget {
    Global {
        command: &'static str,
    },
    Exchange {
        exchange: String,
        command: &'static str,
    },
    Symbol {
        symbol: String,
        command: &'static str,
    },
}

pub(crate) fn control_click(
    target: ControlActionTarget,
    token: String,
    mut message: Signal<String>,
    lang: Language,
    label_key: &'static str,
) -> impl FnMut(Event<MouseData>) + 'static {
    move |_| {
        let token_value = token.clone();
        let target_value = target.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let result = match target_value {
                ControlActionTarget::Global { command } => {
                    post_control_command(&token_value, command).await
                }
                ControlActionTarget::Exchange { exchange, command } => {
                    post_exchange_control(&token_value, &exchange, command).await
                }
                ControlActionTarget::Symbol { symbol, command } => {
                    post_symbol_control(&token_value, &symbol, command).await
                }
            };
            match result {
                Ok(value) => message.set(format!(
                    "{} {}: {}",
                    t(lang, label_key),
                    t(lang, "queued"),
                    compact(&value)
                )),
                Err(error) => message.set(error),
            }
        });
    }
}

#[component]
pub(crate) fn Metric(label: String, value: String) -> Element {
    rsx! {
        div { class: "panel metric",
            span { "{label}" }
            strong { "{value}" }
        }
    }
}

#[component]
pub(crate) fn ControlActionPanel(lang: Language) -> Element {
    rsx! {
        section { class: "panel warning-panel",
            h2 { {s(lang, "control_actions")} }
            p { {s(lang, "queued_warning")} }
            p { class: "muted", {s(lang, "no_browser_orders")} }
        }
    }
}

#[component]
pub(crate) fn StatusPill(value: bool, lang: Language) -> Element {
    let class = if value { "pill" } else { "pill bad" };
    let text = if value { t(lang, "yes") } else { t(lang, "no") };
    rsx! { span { class: "{class}", "{text}" } }
}

#[component]
pub(crate) fn Pager(
    page: usize,
    total_pages: usize,
    on_prev: EventHandler<Event<MouseData>>,
    on_next: EventHandler<Event<MouseData>>,
    lang: Language,
) -> Element {
    let display_page = page.saturating_add(1).min(total_pages.max(1));
    rsx! {
        div { class: "pager",
            button { class: "mini-button", disabled: page == 0, onclick: move |event| on_prev.call(event), {s(lang, "prev_page")} }
            span { "{display_page} / {total_pages.max(1)}" }
            button { class: "mini-button", disabled: page + 1 >= total_pages.max(1), onclick: move |event| on_next.call(event), {s(lang, "next_page")} }
        }
    }
}

#[component]
pub(crate) fn LogPager(
    page: usize,
    total_pages: usize,
    on_prev: EventHandler<Event<MouseData>>,
    on_next: EventHandler<Event<MouseData>>,
    on_jump: EventHandler<usize>,
    lang: Language,
) -> Element {
    let display_page = page.saturating_add(1).min(total_pages.max(1));
    rsx! {
        div { class: "pager log-pager",
            button { class: "mini-button", disabled: page == 0, onclick: move |event| on_prev.call(event), {s(lang, "prev_page")} }
            span { "{display_page} / {total_pages.max(1)}" }
            label { class: "page-jump",
                span { {s(lang, "jump_page")} }
                input {
                    r#type: "number",
                    min: "1",
                    max: "{total_pages.max(1)}",
                    value: "{display_page}",
                    oninput: move |event| {
                        if let Ok(page_number) = event.value().parse::<usize>() {
                            if page_number > 0 {
                                on_jump.call(page_number - 1);
                            }
                        }
                    }
                }
            }
            button { class: "mini-button", disabled: page + 1 >= total_pages.max(1), onclick: move |event| on_next.call(event), {s(lang, "next_page")} }
        }
    }
}

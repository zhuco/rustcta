use crate::types::{ControlPanelView, Language};

const LANGUAGE_KEY: &str = "rustcta_control_ui_language";
const ACTIVE_VIEW_KEY: &str = "rustcta_control_active_view";

pub(crate) fn load_language() -> Language {
    match storage()
        .and_then(|storage| storage.get_item(LANGUAGE_KEY).ok().flatten())
        .as_deref()
    {
        Some("en") => Language::En,
        _ => Language::Zh,
    }
}

pub(crate) fn load_active_view() -> ControlPanelView {
    active_view_from_hash()
        .or_else(|| {
            storage()
                .and_then(|storage| storage.get_item(ACTIVE_VIEW_KEY).ok().flatten())
                .and_then(|value| ControlPanelView::from_route_id(&value))
        })
        .unwrap_or_default()
}

pub(crate) fn save_language(language: Language) {
    if let Some(storage) = storage() {
        let _ = storage.set_item(LANGUAGE_KEY, language.code());
    }
}

pub(crate) fn save_active_view(view: ControlPanelView) {
    if let Some(storage) = storage() {
        let _ = storage.set_item(ACTIVE_VIEW_KEY, view.route_id());
    }
    if let Some(window) = web_sys::window() {
        let _ = window.location().set_hash(view.route_id());
    }
}

pub(crate) fn storage() -> Option<web_sys::Storage> {
    web_sys::window().and_then(|window| window.local_storage().ok().flatten())
}

fn active_view_from_hash() -> Option<ControlPanelView> {
    web_sys::window()
        .and_then(|window| window.location().hash().ok())
        .and_then(|hash| {
            let value = hash
                .trim_start_matches('#')
                .trim_start_matches('/')
                .strip_prefix("view=")
                .unwrap_or_else(|| hash.trim_start_matches('#').trim_start_matches('/'))
                .to_string();
            ControlPanelView::from_route_id(&value)
        })
}

use crate::types::Language;

const TOKEN_KEY: &str = "rustcta_control_api_token";
const LANGUAGE_KEY: &str = "rustcta_control_ui_language";

pub(crate) fn load_token() -> String {
    storage()
        .and_then(|storage| storage.get_item(TOKEN_KEY).ok().flatten())
        .unwrap_or_default()
}

pub(crate) fn load_language() -> Language {
    match storage()
        .and_then(|storage| storage.get_item(LANGUAGE_KEY).ok().flatten())
        .as_deref()
    {
        Some("en") => Language::En,
        _ => Language::Zh,
    }
}

pub(crate) fn save_token(value: &str) {
    if let Some(storage) = storage() {
        let _ = storage.set_item(TOKEN_KEY, value);
    }
}

pub(crate) fn save_language(language: Language) {
    if let Some(storage) = storage() {
        let _ = storage.set_item(LANGUAGE_KEY, language.code());
    }
}

pub(crate) fn storage() -> Option<web_sys::Storage> {
    web_sys::window().and_then(|window| window.local_storage().ok().flatten())
}

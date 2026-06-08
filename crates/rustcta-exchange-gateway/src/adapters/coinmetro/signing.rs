#![cfg_attr(not(test), allow(dead_code))]

pub fn bearer_header(token: &str) -> Option<String> {
    let token = token.trim();
    if token.is_empty() {
        None
    } else if token.to_ascii_lowercase().starts_with("bearer ") {
        Some(token.to_string())
    } else {
        Some(format!("Bearer {token}"))
    }
}

pub fn websocket_token_query(device_id: Option<&str>, token: &str) -> Option<String> {
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    let Some(device_id) = device_id.map(str::trim).filter(|value| !value.is_empty()) else {
        return Some(token.to_string());
    };
    Some(format!("{device_id}:{token}"))
}

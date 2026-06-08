#![cfg_attr(not(test), allow(dead_code))]

pub fn mercado_bearer_authorization(token: &str) -> String {
    format!("Bearer {token}")
}

pub fn mercado_bearer_request_fingerprint(method: &str, path: &str, body: &str) -> String {
    format!("{} {path} {body}", method.to_ascii_uppercase())
}

#![cfg_attr(not(test), allow(dead_code))]

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn foxbit_rest_prehash(
    timestamp_ms: &str,
    method: &str,
    request_path: &str,
    query_string: &str,
    raw_body: &str,
) -> String {
    format!(
        "{}{}{}{}{}",
        timestamp_ms,
        method.to_ascii_uppercase(),
        request_path,
        query_string,
        raw_body
    )
}

pub fn foxbit_hmac_sha256_hex(secret: &str, payload: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC-SHA256 accepts any key length");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn foxbit_private_ws_login_prehash(timestamp_ms: &str) -> String {
    format!("{timestamp_ms}login")
}

pub fn foxbit_signed_headers(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: &str,
    method: &str,
    request_path: &str,
    query_string: &str,
    raw_body: &str,
    receive_window_ms: Option<u64>,
) -> Vec<(String, String)> {
    let prehash = foxbit_rest_prehash(timestamp_ms, method, request_path, query_string, raw_body);
    let mut headers = vec![
        ("X-FB-ACCESS-KEY".to_string(), api_key.to_string()),
        (
            "X-FB-ACCESS-TIMESTAMP".to_string(),
            timestamp_ms.to_string(),
        ),
        (
            "X-FB-ACCESS-SIGNATURE".to_string(),
            foxbit_hmac_sha256_hex(api_secret, &prehash),
        ),
    ];
    if let Some(receive_window_ms) = receive_window_ms {
        headers.push((
            "X-FB-RECEIVE-WINDOW".to_string(),
            receive_window_ms.to_string(),
        ));
    }
    headers
}

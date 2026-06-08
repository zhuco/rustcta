use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha512;

pub fn build_payload(symbol_path: &str, body: &serde_json::Value, nonce: &str) -> String {
    let payload = json!({
        "symbol": symbol_path,
        "timeStamp_nonce": nonce,
        "body": body,
    });
    base64::engine::general_purpose::STANDARD
        .encode(serde_json::to_string(&payload).expect("payload serializes"))
}

pub fn sign_payload(api_secret: &str, payload: &str) -> String {
    let mut mac =
        Hmac::<Sha512>::new_from_slice(api_secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn signed_headers(
    api_key: &str,
    api_secret: &str,
    symbol_path: &str,
    body: &serde_json::Value,
    nonce: &str,
) -> Vec<(String, String)> {
    let payload = build_payload(symbol_path, body, nonce);
    let signature = sign_payload(api_secret, &payload);
    vec![
        ("X-BITBNS-APIKEY".to_string(), api_key.to_string()),
        ("X-BITBNS-PAYLOAD".to_string(), payload),
        ("X-BITBNS-SIGNATURE".to_string(), signature),
    ]
}

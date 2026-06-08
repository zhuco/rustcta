#![cfg_attr(not(test), allow(dead_code))]

use sha2::{Digest, Sha256};

pub fn paradex_auth_canonical_payload(
    account: &str,
    timestamp_ms: &str,
    expiration_ms: &str,
) -> String {
    format!(
        "paradex.auth:account={}:timestamp={}:expiration={}",
        account.trim().to_ascii_lowercase(),
        timestamp_ms.trim(),
        expiration_ms.trim()
    )
}

pub fn paradex_order_canonical_payload(
    market: &str,
    side: &str,
    order_type: &str,
    size: &str,
    price: Option<&str>,
    nonce: &str,
) -> String {
    format!(
        "paradex.order:market={}:side={}:type={}:size={}:price={}:nonce={}",
        market.trim().to_ascii_uppercase(),
        side.trim().to_ascii_uppercase(),
        order_type.trim().to_ascii_uppercase(),
        size.trim(),
        price.unwrap_or("").trim(),
        nonce.trim()
    )
}

pub fn fixture_signature_digest(private_key_label: &str, canonical_payload: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(private_key_label.trim().as_bytes());
    hasher.update(b":");
    hasher.update(canonical_payload.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn bearer_header(jwt_token: &str) -> Option<String> {
    let token = jwt_token.trim();
    if token.is_empty() {
        None
    } else if token.to_ascii_lowercase().starts_with("bearer ") {
        Some(token.to_string())
    } else {
        Some(format!("Bearer {token}"))
    }
}

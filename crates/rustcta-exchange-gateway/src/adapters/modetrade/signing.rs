#![cfg_attr(not(test), allow(dead_code))]

pub const MODETRADE_ORDERLY_SIGNING_BOUNDARY: &str =
    "modetrade uses Orderly Ed25519 account headers; private REST is spec-only until account key handling is audited";

pub fn modetrade_orderly_canonical_payload(
    timestamp_ms: i64,
    method: &str,
    path_with_query: &str,
    body: &str,
) -> String {
    format!(
        "{timestamp_ms}{}{}{}",
        method.trim().to_ascii_uppercase(),
        normalized_path(path_with_query),
        body
    )
}

pub fn modetrade_orderly_signing_boundary() -> &'static str {
    MODETRADE_ORDERLY_SIGNING_BOUNDARY
}

fn normalized_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}

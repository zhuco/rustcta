use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn rest_hs256_payload(
    method: &str,
    request_path: &str,
    query: Option<&str>,
    body: Option<&str>,
    timestamp_ms: u64,
    window_ms: Option<u64>,
) -> String {
    let mut payload = format!("{}{}", method.to_ascii_uppercase(), request_path);
    if let Some(query) = query.filter(|value| !value.is_empty()) {
        payload.push('?');
        payload.push_str(query);
    }
    if let Some(body) = body.filter(|value| !value.is_empty()) {
        payload.push_str(body);
    }
    payload.push_str(&timestamp_ms.to_string());
    if let Some(window_ms) = window_ms {
        payload.push_str(&window_ms.to_string());
    }
    payload
}

pub fn ws_hs256_payload(timestamp_ms: u64, window_ms: Option<u64>) -> String {
    let mut payload = timestamp_ms.to_string();
    if let Some(window_ms) = window_ms {
        payload.push_str(&window_ms.to_string());
    }
    payload
}

pub fn sign_hs256_hex(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid FMFW.io API secret length: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn hs256_authorization_header(
    api_key: &str,
    signature: &str,
    timestamp_ms: u64,
    window_ms: Option<u64>,
) -> String {
    let data = if let Some(window_ms) = window_ms {
        format!("{api_key}:{signature}:{timestamp_ms}:{window_ms}")
    } else {
        format!("{api_key}:{signature}:{timestamp_ms}")
    };
    format!("HS256 {}", general_purpose::STANDARD.encode(data))
}

pub fn build_hs256_authorization_header(
    api_key: &str,
    api_secret: &str,
    method: &str,
    request_path: &str,
    body: Option<&str>,
    timestamp_ms: u64,
    window_ms: Option<u64>,
) -> ExchangeApiResult<String> {
    let payload = rest_hs256_payload(method, request_path, None, body, timestamp_ms, window_ms);
    let signature = sign_hs256_hex(api_secret, &payload)?;
    Ok(hs256_authorization_header(
        api_key,
        &signature,
        timestamp_ms,
        window_ms,
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        build_hs256_authorization_header, rest_hs256_payload, sign_hs256_hex, ws_hs256_payload,
    };

    #[test]
    fn fmfwio_signing_should_match_fixed_rest_vector() {
        let payload = rest_hs256_payload(
            "POST",
            "/api/3/spot/order",
            None,
            Some("client_order_id=cli-fmfwio-1&price=0.046016&quantity=0.063&side=sell&symbol=ETHBTC&type=limit"),
            1_710_000_000_000,
            Some(10_000),
        );
        assert_eq!(
            payload,
            "POST/api/3/spot/orderclient_order_id=cli-fmfwio-1&price=0.046016&quantity=0.063&side=sell&symbol=ETHBTC&type=limit171000000000010000"
        );
        assert_eq!(
            sign_hs256_hex("test-secret", &payload).expect("signature"),
            "b3f62b36441bf2714f0363d913b3ddc4e033673b6f0ba6f094183722211a44b6"
        );
    }

    #[test]
    fn fmfwio_signing_should_match_fixed_ws_vector() {
        let payload = ws_hs256_payload(1_710_000_000_000, Some(10_000));
        assert_eq!(payload, "171000000000010000");
        assert_eq!(
            sign_hs256_hex("test-secret", &payload).expect("signature"),
            "47caebaef064195539b50b7c53dcd103ff3ee8face5b2a4bbccb1234b6a54a7e"
        );
    }

    #[test]
    fn fmfwio_authorization_header_should_not_expose_plain_secret() {
        let header = build_hs256_authorization_header(
            "test-key",
            "test-secret",
            "POST",
            "/api/3/spot/order",
            Some("client_order_id=cli-fmfwio-1&price=0.046016&quantity=0.063&side=sell&symbol=ETHBTC&type=limit"),
            1_710_000_000_000,
            Some(10_000),
        )
        .expect("auth header");
        assert!(header.starts_with("HS256 "));
        assert!(!header.contains("test-secret"));
    }
}

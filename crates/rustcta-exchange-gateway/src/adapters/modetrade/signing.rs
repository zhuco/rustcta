#![cfg_attr(not(test), allow(dead_code))]
#![cfg_attr(not(test), allow(dead_code))]

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub const MODETRADE_ORDERLY_SIGNING_BOUNDARY: &str =
    "modetrade uses Orderly Ed25519 account headers; private REST requires explicit account id, orderly key and base58/hex orderly secret";

#[derive(Debug, Clone)]
pub struct OrderlyAuth {
    pub account_id: String,
    pub orderly_key: String,
    pub orderly_secret: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedHeaders {
    pub timestamp_ms: i64,
    pub orderly_account_id: String,
    pub orderly_key: String,
    pub orderly_signature: String,
    pub canonical_payload: String,
}

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

pub fn sign_orderly_request(
    auth: &OrderlyAuth,
    timestamp_ms: i64,
    method: &str,
    path_with_query: &str,
    body: &str,
) -> ExchangeApiResult<SignedHeaders> {
    let secret = decode_orderly_secret(&auth.orderly_secret)?;
    let signing_key = SigningKey::from_bytes(&secret);
    let canonical_payload =
        modetrade_orderly_canonical_payload(timestamp_ms, method, path_with_query, body);
    let signature = signing_key.sign(canonical_payload.as_bytes());
    Ok(SignedHeaders {
        timestamp_ms,
        orderly_account_id: auth.account_id.clone(),
        orderly_key: auth.orderly_key.clone(),
        orderly_signature: URL_SAFE_NO_PAD.encode(signature.to_bytes()),
        canonical_payload,
    })
}

fn decode_orderly_secret(secret: &str) -> ExchangeApiResult<[u8; 32]> {
    let trimmed = secret
        .trim()
        .strip_prefix("ed25519:")
        .unwrap_or_else(|| secret.trim());
    let bytes = if trimmed.len() == 64 && trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        hex::decode(trimmed).map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid hex orderly secret: {error}"),
        })?
    } else {
        decode_base58(trimmed)?
    };
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| ExchangeApiError::InvalidRequest {
            message: format!(
                "modetrade orderly secret must decode to 32 bytes, got {} bytes",
                bytes.len()
            ),
        })
}

fn decode_base58(value: &str) -> ExchangeApiResult<Vec<u8>> {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if value.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "modetrade orderly secret must not be empty".to_string(),
        });
    }
    let mut bytes = vec![0u8];
    for ch in value.bytes() {
        let carry = ALPHABET
            .iter()
            .position(|candidate| *candidate == ch)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "invalid base58 character in modetrade orderly secret".to_string(),
            })? as u32;
        let mut carry = carry;
        for byte in bytes.iter_mut().rev() {
            let value = (*byte as u32) * 58 + carry;
            *byte = (value & 0xff) as u8;
            carry = value >> 8;
        }
        while carry > 0 {
            bytes.insert(0, (carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    for ch in value.bytes() {
        if ch == b'1' {
            bytes.insert(0, 0);
        } else {
            break;
        }
    }
    Ok(bytes)
}

fn normalized_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}

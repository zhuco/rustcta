#![cfg_attr(not(test), allow(dead_code))]

use base64::Engine as _;
use ed25519_dalek::{Signer, SigningKey};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{Map, Value};

const BASE58_ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

pub fn pacifica_signing_payload(
    operation_type: &str,
    timestamp_ms: i64,
    expiry_window_ms: u64,
    operation_data: Value,
) -> ExchangeApiResult<String> {
    let mut root = Map::new();
    root.insert("data".to_string(), sort_json(operation_data)?);
    root.insert(
        "expiry_window".to_string(),
        Value::Number(expiry_window_ms.into()),
    );
    root.insert("timestamp".to_string(), Value::Number(timestamp_ms.into()));
    root.insert(
        "type".to_string(),
        Value::String(operation_type.to_string()),
    );
    serde_json::to_string(&Value::Object(root)).map_err(|error| ExchangeApiError::Serialization {
        message: format!("failed to encode Pacifica signing payload: {error}"),
    })
}

pub fn sign_pacifica_payload(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let key_bytes = decode_secret_key(secret)?;
    let signing_key = SigningKey::from_bytes(&key_bytes);
    let signature = signing_key.sign(payload.as_bytes());
    Ok(base58_encode(&signature.to_bytes()))
}

fn sort_json(value: Value) -> ExchangeApiResult<Value> {
    match value {
        Value::Object(object) => {
            let mut sorted = Map::new();
            let mut keys = object.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                let value =
                    object
                        .get(&key)
                        .cloned()
                        .ok_or_else(|| ExchangeApiError::Serialization {
                            message: format!("missing Pacifica signing key {key}"),
                        })?;
                sorted.insert(key, sort_json(value)?);
            }
            Ok(Value::Object(sorted))
        }
        Value::Array(values) => values
            .into_iter()
            .map(sort_json)
            .collect::<ExchangeApiResult<Vec<_>>>()
            .map(Value::Array),
        other => Ok(other),
    }
}

fn decode_secret_key(secret: &str) -> ExchangeApiResult<[u8; 32]> {
    let trimmed = secret.trim();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(trimmed)
        .or_else(|_| hex::decode(trimmed))
        .or_else(|_| base58_decode(trimmed))
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid Pacifica Ed25519 secret: {error}"),
        })?;
    let bytes = decoded
        .get(..32)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "Pacifica Ed25519 secret must decode to at least 32 bytes".to_string(),
        })?;
    bytes
        .try_into()
        .map_err(|_| ExchangeApiError::InvalidRequest {
            message: "Pacifica Ed25519 secret must decode to 32 bytes".to_string(),
        })
}

fn base58_encode(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    let zeros = bytes.iter().take_while(|byte| **byte == 0).count();
    let mut digits = Vec::<u8>::new();
    for byte in &bytes[zeros..] {
        let mut carry = *byte as u32;
        for digit in &mut digits {
            let value = (*digit as u32) * 256 + carry;
            *digit = (value % 58) as u8;
            carry = value / 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let mut encoded = String::new();
    encoded.extend(std::iter::repeat('1').take(zeros));
    encoded.extend(
        digits
            .iter()
            .rev()
            .map(|digit| BASE58_ALPHABET[*digit as usize] as char),
    );
    encoded
}

fn base58_decode(value: &str) -> Result<Vec<u8>, String> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let mut bytes = Vec::<u8>::new();
    for character in value.bytes() {
        let position = BASE58_ALPHABET
            .iter()
            .position(|candidate| *candidate == character)
            .ok_or_else(|| format!("invalid base58 character {}", character as char))?;
        let mut carry = position as u32;
        for byte in &mut bytes {
            let next = (*byte as u32) * 58 + carry;
            *byte = (next & 0xff) as u8;
            carry = next >> 8;
        }
        while carry > 0 {
            bytes.push((carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    let zeros = value.bytes().take_while(|byte| *byte == b'1').count();
    bytes.extend(std::iter::repeat(0).take(zeros));
    bytes.reverse();
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use base64::{engine::general_purpose, Engine as _};
    use serde_json::json;

    use super::{base58_decode, base58_encode, pacifica_signing_payload, sign_pacifica_payload};

    #[test]
    fn pacifica_payload_should_sort_recursively_and_sign() {
        let payload = pacifica_signing_payload(
            "create_order",
            1_748_970_123_456,
            5_000,
            json!({
                "symbol": "BTC",
                "price": "100000",
                "amount": "0.1",
                "side": "bid",
                "tif": "GTC",
                "reduce_only": false,
                "client_order_id": "12345678-1234-1234-1234-123456789abc"
            }),
        )
        .expect("payload");
        assert_eq!(
            payload,
            r#"{"data":{"amount":"0.1","client_order_id":"12345678-1234-1234-1234-123456789abc","price":"100000","reduce_only":false,"side":"bid","symbol":"BTC","tif":"GTC"},"expiry_window":5000,"timestamp":1748970123456,"type":"create_order"}"#
        );
        let secret = general_purpose::STANDARD.encode([7_u8; 32]);
        let signature = sign_pacifica_payload(&secret, &payload).expect("signature");
        assert_eq!(
            sign_pacifica_payload("US517G5965aydkZ46HS38QLi7UQiSojurfbQfKCELFx", &payload)
                .expect("base58 signature"),
            signature
        );
    }

    #[test]
    fn base58_should_round_trip_leading_zero_bytes() {
        let bytes = [0_u8, 0, 7, 8, 9, 255];
        let encoded = base58_encode(&bytes);
        assert!(encoded.starts_with("11"));
        assert_eq!(base58_decode(&encoded).expect("decode"), bytes);
    }
}

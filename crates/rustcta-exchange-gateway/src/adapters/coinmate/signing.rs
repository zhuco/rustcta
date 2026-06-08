use std::collections::BTreeMap;

use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn coinmate_signature_payload(nonce: &str, client_id: &str, public_key: &str) -> String {
    format!("{nonce}{client_id}{public_key}")
}

pub fn coinmate_hmac_signature(private_key: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha256::new_from_slice(private_key.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Coinmate private key: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()).to_ascii_uppercase())
}

pub fn coinmate_signed_auth_fields(
    client_id: &str,
    public_key: &str,
    private_key: &str,
    nonce: &str,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    let payload = coinmate_signature_payload(nonce, client_id, public_key);
    let signature = coinmate_hmac_signature(private_key, &payload)?;
    Ok(BTreeMap::from([
        ("clientId".to_string(), client_id.to_string()),
        ("publicKey".to_string(), public_key.to_string()),
        ("nonce".to_string(), nonce.to_string()),
        ("signature".to_string(), signature),
    ]))
}

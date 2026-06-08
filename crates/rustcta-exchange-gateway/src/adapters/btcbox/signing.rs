use std::collections::BTreeMap;

use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn canonical_form(params: &BTreeMap<String, String>) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        serializer.append_pair(key, value);
    }
    serializer.finish()
}

pub fn md5_hex(value: &str) -> String {
    format!("{:x}", md5::compute(value.as_bytes()))
}

pub fn sign_canonical_form(
    private_key: &str,
    params: &BTreeMap<String, String>,
) -> ExchangeApiResult<String> {
    let canonical = canonical_form(params);
    sign_canonical_payload(private_key, &canonical)
}

pub fn sign_canonical_payload(private_key: &str, canonical: &str) -> ExchangeApiResult<String> {
    let hmac_key = md5_hex(private_key);
    let mut mac = HmacSha256::new_from_slice(hmac_key.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid BTCBOX private key for signing: {error}"),
        }
    })?;
    mac.update(canonical.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

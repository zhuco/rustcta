use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha512;

type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bit2cSignedForm {
    pub encoded_body: String,
    pub signature: String,
}

pub fn canonical_form<K: AsRef<str>, V: AsRef<str>>(params: &[(K, V)], nonce: u64) -> String {
    let mut pairs = params
        .iter()
        .map(|(key, value)| (key.as_ref().to_string(), value.as_ref().to_string()))
        .collect::<Vec<_>>();
    pairs.push(("nonce".to_string(), nonce.to_string()));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn sign_base64(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha512::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Bit2C API secret length: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        mac.finalize().into_bytes(),
    ))
}

pub fn signed_form<K: AsRef<str>, V: AsRef<str>>(
    secret: &str,
    params: &[(K, V)],
    nonce: u64,
) -> ExchangeApiResult<Bit2cSignedForm> {
    let encoded_body = canonical_form(params, nonce);
    let signature = sign_base64(secret, &encoded_body)?;
    Ok(Bit2cSignedForm {
        encoded_body,
        signature,
    })
}

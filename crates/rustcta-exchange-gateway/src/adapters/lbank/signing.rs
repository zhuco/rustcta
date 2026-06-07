use std::collections::HashMap;

use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub const SIGNATURE_METHOD: &str = "HmacSHA256";
pub const DEFAULT_ECHOSTR: &str = "rustctaLBankGatewayEchostr20260607";

pub fn sign_lbank_params(
    secret: &str,
    params: &HashMap<String, String>,
) -> ExchangeApiResult<String> {
    let parameter_string = build_query_string(params);
    let prepared = format!("{:x}", md5::compute(parameter_string.as_bytes())).to_ascii_uppercase();
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid LBank API secret length: {error}"),
        }
    })?;
    mac.update(prepared.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn build_query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

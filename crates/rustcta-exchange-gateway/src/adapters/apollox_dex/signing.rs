#![cfg_attr(not(test), allow(dead_code))]

use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;
use std::collections::BTreeMap;

pub fn apollox_sign_query(secret: &str, query: &str) -> ExchangeApiResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid ApolloX DEX API secret length: {error}"),
        }
    })?;
    mac.update(query.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn apollox_query_string(params: impl IntoIterator<Item = (String, String)>) -> String {
    let ordered = params.into_iter().collect::<BTreeMap<_, _>>();
    ordered
        .into_iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(&key),
                urlencoding::encode(&value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

pub fn apollox_signed_query(
    secret: &str,
    params: impl IntoIterator<Item = (String, String)>,
) -> ExchangeApiResult<(String, String)> {
    let query = apollox_query_string(params);
    let signature = apollox_sign_query(secret, &query)?;
    Ok((query, signature))
}

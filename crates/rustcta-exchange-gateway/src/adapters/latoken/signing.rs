use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::{Sha256, Sha384, Sha512};

type HmacSha256 = Hmac<Sha256>;
type HmacSha384 = Hmac<Sha384>;
type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatokenDigest {
    Sha256,
    Sha384,
    Sha512,
}

impl LatokenDigest {
    pub fn header_value(self) -> &'static str {
        match self {
            Self::Sha256 => "HMAC-SHA256",
            Self::Sha384 => "HMAC-SHA384",
            Self::Sha512 => "HMAC-SHA512",
        }
    }
}

pub fn form_encode(params: &[(String, String)]) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn rest_signing_payload(
    method: &str,
    path: &str,
    query_params: &[(String, String)],
    body_params: &[(String, String)],
) -> String {
    format!(
        "{}{}{}{}",
        method.to_ascii_uppercase(),
        path,
        form_encode(query_params),
        form_encode(body_params)
    )
}

pub fn rest_signature(
    api_secret: &str,
    payload: &str,
    digest: LatokenDigest,
) -> ExchangeApiResult<String> {
    let api_secret = api_secret.trim();
    if api_secret.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "LATOKEN signature requires non-empty API secret".to_string(),
        });
    }
    match digest {
        LatokenDigest::Sha256 => {
            let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes()).map_err(|error| {
                ExchangeApiError::InvalidRequest {
                    message: format!("invalid LATOKEN API secret length: {error}"),
                }
            })?;
            mac.update(payload.as_bytes());
            Ok(hex::encode(mac.finalize().into_bytes()))
        }
        LatokenDigest::Sha384 => {
            let mut mac = HmacSha384::new_from_slice(api_secret.as_bytes()).map_err(|error| {
                ExchangeApiError::InvalidRequest {
                    message: format!("invalid LATOKEN API secret length: {error}"),
                }
            })?;
            mac.update(payload.as_bytes());
            Ok(hex::encode(mac.finalize().into_bytes()))
        }
        LatokenDigest::Sha512 => {
            let mut mac = HmacSha512::new_from_slice(api_secret.as_bytes()).map_err(|error| {
                ExchangeApiError::InvalidRequest {
                    message: format!("invalid LATOKEN API secret length: {error}"),
                }
            })?;
            mac.update(payload.as_bytes());
            Ok(hex::encode(mac.finalize().into_bytes()))
        }
    }
}

pub fn signed_rest_headers(
    api_key: &str,
    api_secret: &str,
    method: &str,
    path: &str,
    query_params: &[(String, String)],
    body_params: &[(String, String)],
    digest: LatokenDigest,
) -> ExchangeApiResult<LatokenSignedHeaders> {
    let api_key = api_key.trim();
    if api_key.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "LATOKEN signed REST requires non-empty API key".to_string(),
        });
    }
    let payload = rest_signing_payload(method, path, query_params, body_params);
    let signature = rest_signature(api_secret, &payload, digest)?;
    Ok(LatokenSignedHeaders {
        api_key: api_key.to_string(),
        signature,
        digest: digest.header_value().to_string(),
        payload,
    })
}

pub fn ws_auth_signature(
    api_secret: &str,
    timestamp_ms: &str,
    digest: LatokenDigest,
) -> ExchangeApiResult<String> {
    if timestamp_ms.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "LATOKEN WS signature requires X-LA-SIGDATA timestamp".to_string(),
        });
    }
    rest_signature(api_secret, timestamp_ms, digest)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatokenSignedHeaders {
    pub api_key: String,
    pub signature: String,
    pub digest: String,
    pub payload: String,
}

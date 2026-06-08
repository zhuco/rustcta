use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn cex_rest_signature(
    nonce: &str,
    user_id: &str,
    api_key: &str,
    api_secret: &str,
) -> ExchangeApiResult<String> {
    let payload = format!("{nonce}{user_id}{api_key}");
    hmac_sha256_hex(api_secret, &payload, HexCase::Upper)
}

pub fn cex_ws_signature(
    timestamp_seconds: &str,
    api_key: &str,
    api_secret: &str,
) -> ExchangeApiResult<String> {
    let payload = format!("{timestamp_seconds}{api_key}");
    hmac_sha256_hex(api_secret, &payload, HexCase::Lower)
}

fn hmac_sha256_hex(
    api_secret: &str,
    payload: &str,
    hex_case: HexCase,
) -> ExchangeApiResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid CEX.IO API secret length: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());
    Ok(match hex_case {
        HexCase::Lower => signature,
        HexCase::Upper => signature.to_ascii_uppercase(),
    })
}

enum HexCase {
    Lower,
    Upper,
}

#[cfg(test)]
mod tests {
    use super::{cex_rest_signature, cex_ws_signature};

    #[test]
    fn cex_ws_signature_should_match_official_vector() {
        let signature = cex_ws_signature(
            "1448034533",
            "1WZbtMTbMbo2NsW12vOz9IuPM",
            "1IuUeW4IEWatK87zBTENHj1T17s",
        )
        .expect("signature");
        assert_eq!(
            signature,
            "7d581adb01ad22f1ed38e1159a7f08ac5d83906ae1a42fe17e7d977786fe9694"
        );
    }

    #[test]
    fn cex_rest_signature_should_be_uppercase_hmac_sha256() {
        let signature =
            cex_rest_signature("1700000000000", "user123", "fixture-key", "fixture-secret")
                .expect("signature");
        assert_eq!(signature.len(), 64);
        assert!(signature.chars().all(|ch| ch.is_ascii_hexdigit()));
        assert_eq!(signature, signature.to_ascii_uppercase());
    }
}

use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;
use std::collections::HashMap;

pub fn build_query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

pub fn sign_query(secret: &str, query: &str) -> ExchangeApiResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Toobit API secret length: {error}"),
        }
    })?;
    mac.update(query.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{build_query_string, sign_query};
    use crate::signing_spec::SigningVector;

    #[test]
    fn toobit_signing_should_sort_urlencode_and_hmac_query_vector() {
        let params = HashMap::from([
            ("symbol".to_string(), "BTCUSDT".to_string()),
            ("timestamp".to_string(), "1700000000000".to_string()),
            ("recvWindow".to_string(), "5000".to_string()),
        ]);
        let query = build_query_string(&params);
        assert_eq!(
            query,
            "recvWindow=5000&symbol=BTCUSDT&timestamp=1700000000000"
        );
        assert_eq!(
            sign_query("secret", &query).expect("signature"),
            "5e1ff144a940ffd87f51175de5ebe2f76d4e8fe1951e3c81b001375485bf3430"
        );
    }

    #[test]
    fn toobit_signing_should_verify_fixture_vector() {
        let vector: SigningVector = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/toobit/signing_vectors/rest_hmac_sha256.json"
        ))
        .expect("signing vector fixture");
        assert_eq!(vector.exchange, "toobit");
        vector.verify().expect("signing vector");
    }
}

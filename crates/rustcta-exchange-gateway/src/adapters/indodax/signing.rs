use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha512;
use std::collections::HashMap;

type HmacSha512 = Hmac<Sha512>;

pub fn build_form_body(params: &HashMap<String, String>) -> String {
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

pub fn sign_form(secret: &str, form_body: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha512::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Indodax API secret length: {error}"),
        }
    })?;
    mac.update(form_body.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{build_form_body, sign_form};

    #[test]
    fn indodax_signing_should_sort_urlencode_and_hmac_sha512_form() {
        let params = HashMap::from([
            ("method".to_string(), "getInfo".to_string()),
            ("nonce".to_string(), "1700000000000".to_string()),
            ("pair".to_string(), "btc_idr".to_string()),
        ]);
        let body = build_form_body(&params);
        assert_eq!(body, "method=getInfo&nonce=1700000000000&pair=btc_idr");
        assert_eq!(
            sign_form("secret", &body).expect("signature"),
            "523e6caea8203708f0ccbfaf83e4ddf520aff5f6c4b1e942e5b6807b86cd619d78a32fba6af296a890e8bdc0dbcdac28dd35d6ed9a723d78267acc62aae501e6"
        );
    }
}

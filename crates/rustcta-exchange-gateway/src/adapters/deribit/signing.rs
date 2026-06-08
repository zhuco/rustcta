use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct DeribitPrivateCredentials {
    pub client_id: String,
    pub client_secret: String,
}

pub fn authorization_query(client_id: &str, client_secret: &str) -> Vec<(String, String)> {
    vec![
        ("grant_type".to_string(), "client_credentials".to_string()),
        ("client_id".to_string(), client_id.to_string()),
        ("client_secret".to_string(), client_secret.to_string()),
    ]
}

pub fn sign_deribit_hmac(
    client_secret: &str,
    timestamp_ms: i64,
    nonce: &str,
    request_data: &str,
) -> String {
    let payload = format!("{timestamp_ms}\n{nonce}\n{request_data}");
    let mut mac =
        HmacSha256::new_from_slice(client_secret.as_bytes()).expect("HMAC supports any key length");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::{authorization_query, sign_deribit_hmac};

    #[test]
    fn deribit_hmac_should_use_timestamp_nonce_request_data_prehash() {
        let signature = sign_deribit_hmac("secret", 1_700_000_000_000, "nonce-1", "");
        assert_eq!(
            signature,
            "54561f3d258310bc01755054186e43678c8a1b0419b87956c23a92f44b31e95b"
        );
    }

    #[test]
    fn deribit_client_credentials_query_should_not_hide_required_fields() {
        let query = authorization_query("client", "secret");
        assert_eq!(
            query[0],
            ("grant_type".to_string(), "client_credentials".to_string())
        );
        assert_eq!(query[1], ("client_id".to_string(), "client".to_string()));
        assert_eq!(
            query[2],
            ("client_secret".to_string(), "secret".to_string())
        );
    }
}

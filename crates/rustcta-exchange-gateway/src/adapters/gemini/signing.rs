use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use serde_json::{Map, Value};
use sha2::Sha384;

type HmacSha384 = Hmac<Sha384>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeminiSignedPayload {
    pub payload_json: String,
    pub payload_base64: String,
    pub signature_hex: String,
}

pub fn sign_payload(secret: &str, payload_json: &str) -> GeminiSignedPayload {
    let payload_base64 = general_purpose::STANDARD.encode(payload_json.as_bytes());
    let mut mac =
        HmacSha384::new_from_slice(secret.as_bytes()).expect("HMAC-SHA384 accepts any key length");
    mac.update(payload_base64.as_bytes());
    GeminiSignedPayload {
        payload_json: payload_json.to_string(),
        payload_base64,
        signature_hex: hex::encode(mac.finalize().into_bytes()),
    }
}

pub fn private_payload(request_path: &str, nonce: &str, extra: Map<String, Value>) -> String {
    let mut payload = Map::new();
    payload.insert(
        "request".to_string(),
        Value::String(request_path.to_string()),
    );
    payload.insert("nonce".to_string(), Value::String(nonce.to_string()));
    for (key, value) in extra {
        payload.insert(key, value);
    }
    serde_json::to_string(&Value::Object(payload)).expect("Gemini payload is serializable")
}

pub fn sign_private_request(
    secret: &str,
    request_path: &str,
    nonce: &str,
    extra: Map<String, Value>,
) -> GeminiSignedPayload {
    sign_payload(secret, &private_payload(request_path, nonce, extra))
}

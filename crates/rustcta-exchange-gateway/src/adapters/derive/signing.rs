#![cfg_attr(not(test), allow(dead_code))]

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn derive_json_rpc_payload(method: &str, params_json: &str, id: u64) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"{}","params":{}}}"#,
        method.trim(),
        params_json.trim()
    )
}

pub fn derive_session_canonical_payload(
    method: &str,
    subaccount_id: &str,
    timestamp_ms: &str,
    params_json: &str,
) -> String {
    format!(
        "derive.session:method={}:subaccount_id={}:timestamp={}:params={}",
        method.trim(),
        subaccount_id.trim(),
        timestamp_ms.trim(),
        params_json.trim()
    )
}

pub fn derive_fixture_hmac(secret: &str, canonical_payload: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key");
    mac.update(canonical_payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

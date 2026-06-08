use serde_json::json;

use super::signing::{build_payload, sign_payload, signed_headers};

#[test]
fn bitbns_signing_vector_should_match_official_sdk_shape() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitbns/signing_vectors/private_post_headers.json"
    ))
    .expect("signing fixture");
    let body = json!({
        "quantity": "0.01",
        "rate": "65000",
        "side": "BUY"
    });
    let payload = build_payload("/placeOrder/BTC", &body, "1700000000000");
    let signature = sign_payload("fixture-secret", &payload);

    assert_eq!(payload, fixture["payload"].as_str().expect("payload"));
    assert_eq!(
        signature,
        fixture["expected_signature"]
            .as_str()
            .expect("expected signature")
    );

    let headers = signed_headers(
        "fixture-key",
        "fixture-secret",
        "/placeOrder/BTC",
        &body,
        "1700000000000",
    );
    assert_eq!(
        headers
            .iter()
            .find(|(key, _)| key == "X-BITBNS-APIKEY")
            .map(|(_, value)| value.as_str()),
        Some("fixture-key")
    );
    assert!(!headers
        .iter()
        .any(|(_, value)| value.contains("fixture-secret")));
}

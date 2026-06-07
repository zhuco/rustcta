use super::*;

#[test]
fn secret_detector_should_block_raw_secret_fields() {
    assert!(secret_key_name_is_forbidden("api_secret"));
    assert!(secret_key_name_is_forbidden("Authorization"));
    assert!(secret_key_name_is_forbidden("private-key"));
    assert!(!secret_key_name_is_forbidden("configured"));
}

#[test]
fn secret_detector_should_reject_nested_request_and_response_payloads() {
    let value = json!({
        "payload": {
            "nested": [
                { "api_secret": "must-not-cross" }
            ]
        }
    });

    assert!(matches!(
        crate::security::ensure_secret_free_value(&value, "response"),
        Err(GatewayError::SecretPayloadRejected {
            direction: "response"
        })
    ));
}

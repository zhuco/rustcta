use super::*;

#[tokio::test]
async fn router_should_expose_health_status_and_typed_request() {
    let app = gateway_router(Arc::new(MockExchangeGateway::with_exchanges(
        "mock",
        vec![exchange_id()],
    )));

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("health response");
    assert_eq!(health.status(), StatusCode::OK);

    let status = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/status")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("status response");
    assert_eq!(status.status(), StatusCode::OK);

    let request = serde_json::to_value(GatewayProtocolRequest {
        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
        request_id: "req-status".to_string(),
        tenant_id: tenant_id(),
        account_id: Some(account_id()),
        operation: GatewayOperation::GetStatus,
        payload: GatewayRequestPayload::GetStatus(GetStatusRequest::default()),
        requested_at: Utc::now(),
    })
    .expect("request json");
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/gateway/request")
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .expect("request"),
        )
        .await
        .expect("gateway response");

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let value: Value = serde_json::from_slice(&bytes).expect("json body");
    assert_eq!(value["operation"], "get_status");
    assert_eq!(value["accepted"], true);
}

#[tokio::test]
async fn router_should_reject_secret_like_request_payloads_before_deserialize() {
    let app = gateway_router(Arc::new(MockExchangeGateway::new("mock")));
    let request = json!({
        "schema_version": GATEWAY_PROTOCOL_SCHEMA_VERSION,
        "request_id": "req-secret",
        "tenant_id": "tenant",
        "account_id": "account",
        "operation": "get_status",
        "payload": {
            "payload_type": "get_status",
            "payload": {
                "schema_version": GATEWAY_PROTOCOL_SCHEMA_VERSION,
                "include_exchanges": true,
                "api_secret": "must-not-cross"
            }
        },
        "requested_at": Utc::now(),
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/gateway/request")
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .expect("request"),
        )
        .await
        .expect("gateway response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let value: Value = serde_json::from_slice(&bytes).expect("json body");
    assert!(value["error"]
        .as_str()
        .expect("error string")
        .contains("secret-like"));
}

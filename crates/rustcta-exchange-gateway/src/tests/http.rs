use super::*;

struct RateLimitedGateway;

#[async_trait::async_trait]
impl LocalGateway for RateLimitedGateway {
    async fn status(&self) -> Result<GatewayStatus, GatewayError> {
        Err(GatewayError::Exchange {
            exchange: "binance".to_string(),
            kind: ExchangeErrorKind::RateLimited,
            message: "too many requests".to_string(),
            code: Some("429".to_string()),
            retry_after_ms: Some(2500),
        })
    }

    async fn handle_typed(
        &self,
        _request: GatewayProtocolRequest,
    ) -> Result<GatewayProtocolResponse, GatewayError> {
        Err(GatewayError::Exchange {
            exchange: "binance".to_string(),
            kind: ExchangeErrorKind::RateLimited,
            message: "too many requests".to_string(),
            code: Some("429".to_string()),
            retry_after_ms: Some(2500),
        })
    }
}

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
async fn router_should_classify_exchange_errors_with_retry_metadata() {
    let app = gateway_router(Arc::new(RateLimitedGateway));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/status")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("status response");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let value: Value = serde_json::from_slice(&bytes).expect("json body");

    assert_eq!(value["kind"], "rate_limited");
    assert_eq!(value["retryable"], true);
    assert_eq!(value["requires_reconciliation"], false);
    assert_eq!(value["retry_after_ms"], 2500);
}

#[test]
fn gateway_error_should_classify_transport_errors_as_retryable_network() {
    let error = GatewayError::from(rustcta_exchange_api::ExchangeApiError::Transport {
        message: "connection reset".to_string(),
    });

    assert_eq!(error.kind(), ExchangeErrorKind::Network);
    assert!(error.is_retryable());
}

#[test]
fn gateway_error_should_classify_serialization_errors_as_decode() {
    let error = GatewayError::from(rustcta_exchange_api::ExchangeApiError::Serialization {
        message: "invalid json".to_string(),
    });

    assert_eq!(error.kind(), ExchangeErrorKind::Decode);
    assert!(!error.is_retryable());
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
    assert_eq!(value["kind"], "invalid_request");
    assert_eq!(value["retryable"], false);
    assert_eq!(value["requires_reconciliation"], false);
}

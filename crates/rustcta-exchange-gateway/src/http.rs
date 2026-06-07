use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use rustcta_exchange_api::ExchangeErrorKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::security::{ensure_secret_free_serializable, ensure_secret_free_value};
use crate::{
    GatewayError, GatewayHealth, GatewayProtocolResponse, GatewayStatus, LocalGateway,
    GATEWAY_PROTOCOL_SCHEMA_VERSION,
};

#[derive(Clone)]
struct GatewayHttpState {
    gateway: Arc<dyn LocalGateway>,
}

pub fn gateway_router(gateway: Arc<dyn LocalGateway>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/gateway/request", post(gateway_request))
        .with_state(GatewayHttpState { gateway })
}

async fn health() -> Json<GatewayHealth> {
    Json(GatewayHealth::ok(Utc::now()))
}

async fn status(
    State(state): State<GatewayHttpState>,
) -> Result<Json<GatewayStatus>, GatewayHttpError> {
    let status = state.gateway.status().await?;
    ensure_secret_free_serializable(&status, "response")?;
    Ok(Json(status))
}

async fn gateway_request(
    State(state): State<GatewayHttpState>,
    Json(value): Json<Value>,
) -> Result<Json<GatewayProtocolResponse>, GatewayHttpError> {
    ensure_secret_free_value(&value, "request")?;
    let request = serde_json::from_value(value).map_err(|error| GatewayError::InvalidPayload {
        message: error.to_string(),
    })?;
    let response = state.gateway.handle_typed(request).await?;
    ensure_secret_free_serializable(&response, "response")?;
    Ok(Json(response))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct GatewayHttpErrorBody {
    schema_version: u16,
    error: String,
    kind: ExchangeErrorKind,
    retryable: bool,
    requires_reconciliation: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_ms: Option<u64>,
    responded_at: DateTime<Utc>,
}

struct GatewayHttpError {
    status: StatusCode,
    body: GatewayHttpErrorBody,
}

impl From<GatewayError> for GatewayHttpError {
    fn from(error: GatewayError) -> Self {
        let kind = error.kind();
        let retry_after_ms = error.retry_after_ms();
        let status = match &error {
            GatewayError::MissingCredentials { .. } => StatusCode::FORBIDDEN,
            GatewayError::UnsupportedOperation { .. } => StatusCode::NOT_IMPLEMENTED,
            GatewayError::Rejected(_) | GatewayError::InvalidPayload { .. } => {
                StatusCode::BAD_REQUEST
            }
            GatewayError::SecretPayloadRejected {
                direction: "response",
            } => StatusCode::INTERNAL_SERVER_ERROR,
            GatewayError::SecretPayloadRejected { .. } => StatusCode::BAD_REQUEST,
            GatewayError::Exchange { kind, .. } => status_for_exchange_error(*kind),
        };
        Self {
            status,
            body: GatewayHttpErrorBody {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                error: error.to_string(),
                kind,
                retryable: kind.is_retryable(),
                requires_reconciliation: kind.requires_reconciliation(),
                retry_after_ms,
                responded_at: Utc::now(),
            },
        }
    }
}

fn status_for_exchange_error(kind: ExchangeErrorKind) -> StatusCode {
    match kind {
        ExchangeErrorKind::Authentication => StatusCode::UNAUTHORIZED,
        ExchangeErrorKind::Permission => StatusCode::FORBIDDEN,
        ExchangeErrorKind::Unsupported | ExchangeErrorKind::UnsupportedCapability => {
            StatusCode::NOT_IMPLEMENTED
        }
        ExchangeErrorKind::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        ExchangeErrorKind::ExchangeUnavailable | ExchangeErrorKind::Maintenance => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        ExchangeErrorKind::Network | ExchangeErrorKind::Timeout => StatusCode::BAD_GATEWAY,
        ExchangeErrorKind::Internal | ExchangeErrorKind::Unknown => StatusCode::BAD_GATEWAY,
        ExchangeErrorKind::Decode => StatusCode::BAD_GATEWAY,
        _ => StatusCode::BAD_REQUEST,
    }
}

impl IntoResponse for GatewayHttpError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

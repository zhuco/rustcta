use thiserror::Error;

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("gateway credentials are not configured for {exchange}")]
    MissingCredentials { exchange: String },
    #[error("gateway operation is unsupported: {operation}")]
    UnsupportedOperation { operation: String },
    #[error("gateway rejected request: {0}")]
    Rejected(String),
    #[error("gateway rejected {direction} containing secret-like fields")]
    SecretPayloadRejected { direction: &'static str },
    #[error("gateway payload is invalid: {message}")]
    InvalidPayload { message: String },
}

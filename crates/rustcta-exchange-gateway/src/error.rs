use thiserror::Error;

use rustcta_exchange_api::{ExchangeApiError, ExchangeErrorKind, ExchangeErrorMapping};
use rustcta_types::ExchangeError;

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
    #[error("exchange {exchange} returned {kind:?}: {message}")]
    Exchange {
        exchange: String,
        kind: ExchangeErrorKind,
        message: String,
        code: Option<String>,
        retry_after_ms: Option<u64>,
    },
}

impl GatewayError {
    pub fn kind(&self) -> ExchangeErrorKind {
        match self {
            Self::MissingCredentials { .. } => ExchangeErrorKind::Authentication,
            Self::UnsupportedOperation { .. } => ExchangeErrorKind::Unsupported,
            Self::Rejected(_) | Self::InvalidPayload { .. } => ExchangeErrorKind::InvalidRequest,
            Self::SecretPayloadRejected {
                direction: "response",
            } => ExchangeErrorKind::Internal,
            Self::SecretPayloadRejected { .. } => ExchangeErrorKind::InvalidRequest,
            Self::Exchange { kind, .. } => *kind,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.kind().is_retryable()
    }

    pub fn retry_after_ms(&self) -> Option<u64> {
        match self {
            Self::Exchange { retry_after_ms, .. } => *retry_after_ms,
            _ => None,
        }
    }
}

impl From<ExchangeError> for GatewayError {
    fn from(error: ExchangeError) -> Self {
        let mapping = ExchangeErrorMapping::from(error);
        Self::Exchange {
            exchange: mapping.exchange.as_str().to_string(),
            kind: mapping.kind,
            message: mapping.message,
            code: mapping.code,
            retry_after_ms: mapping
                .retry_after
                .and_then(|retry_after| retry_after.millis()),
        }
    }
}

impl From<ExchangeApiError> for GatewayError {
    fn from(error: ExchangeApiError) -> Self {
        match error {
            ExchangeApiError::Unsupported { operation } => Self::UnsupportedOperation {
                operation: operation.to_string(),
            },
            ExchangeApiError::InvalidRequest { message } => Self::Rejected(message),
            ExchangeApiError::Serialization { message } => Self::Exchange {
                exchange: "unknown".to_string(),
                kind: ExchangeErrorKind::Decode,
                message,
                code: None,
                retry_after_ms: None,
            },
            ExchangeApiError::Transport { message } => Self::Exchange {
                exchange: "unknown".to_string(),
                kind: ExchangeErrorKind::Network,
                message,
                code: None,
                retry_after_ms: None,
            },
            ExchangeApiError::Exchange(error) => Self::from(error),
        }
    }
}

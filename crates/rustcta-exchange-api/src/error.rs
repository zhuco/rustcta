use thiserror::Error;

use crate::{ExchangeError, OrderState};

pub type ExchangeApiResult<T> = Result<T, ExchangeApiError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeErrorKind {
    Unsupported,
    UnsupportedCapability,
    Authentication,
    Permission,
    RateLimited,
    Network,
    Timeout,
    ExchangeUnavailable,
    Maintenance,
    InvalidRequest,
    InvalidSymbol,
    InvalidPrecision,
    MinNotionalViolation,
    InsufficientBalance,
    InsufficientPosition,
    DuplicateClientOrderId,
    InvalidClientOrderId,
    OrderNotFound,
    OrderRejected,
    RiskRejected,
    StaleMarketData,
    UnknownOrderState,
    Decode,
    Internal,
    Unknown,
}

impl ExchangeErrorKind {
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::RateLimited
                | Self::Network
                | Self::Timeout
                | Self::ExchangeUnavailable
                | Self::Maintenance
                | Self::UnknownOrderState
        )
    }

    pub fn requires_reconciliation(self) -> bool {
        matches!(
            self,
            Self::UnknownOrderState
                | Self::DuplicateClientOrderId
                | Self::OrderNotFound
                | Self::OrderRejected
        )
    }

    pub fn is_authentication_error(self) -> bool {
        matches!(self, Self::Authentication | Self::Permission)
    }
}

impl From<rustcta_types::ExchangeErrorClass> for ExchangeErrorKind {
    fn from(class: rustcta_types::ExchangeErrorClass) -> Self {
        match class {
            rustcta_types::ExchangeErrorClass::Unsupported => Self::Unsupported,
            rustcta_types::ExchangeErrorClass::UnsupportedCapability => Self::UnsupportedCapability,
            rustcta_types::ExchangeErrorClass::Authentication => Self::Authentication,
            rustcta_types::ExchangeErrorClass::Permission => Self::Permission,
            rustcta_types::ExchangeErrorClass::RateLimited => Self::RateLimited,
            rustcta_types::ExchangeErrorClass::Network => Self::Network,
            rustcta_types::ExchangeErrorClass::Timeout => Self::Timeout,
            rustcta_types::ExchangeErrorClass::ExchangeUnavailable => Self::ExchangeUnavailable,
            rustcta_types::ExchangeErrorClass::Maintenance => Self::Maintenance,
            rustcta_types::ExchangeErrorClass::InvalidRequest => Self::InvalidRequest,
            rustcta_types::ExchangeErrorClass::InvalidSymbol => Self::InvalidSymbol,
            rustcta_types::ExchangeErrorClass::InvalidPrecision => Self::InvalidPrecision,
            rustcta_types::ExchangeErrorClass::MinNotionalViolation => Self::MinNotionalViolation,
            rustcta_types::ExchangeErrorClass::InsufficientBalance => Self::InsufficientBalance,
            rustcta_types::ExchangeErrorClass::InsufficientPosition => Self::InsufficientPosition,
            rustcta_types::ExchangeErrorClass::DuplicateClientOrderId => {
                Self::DuplicateClientOrderId
            }
            rustcta_types::ExchangeErrorClass::InvalidClientOrderId => Self::InvalidClientOrderId,
            rustcta_types::ExchangeErrorClass::OrderNotFound => Self::OrderNotFound,
            rustcta_types::ExchangeErrorClass::OrderRejected => Self::OrderRejected,
            rustcta_types::ExchangeErrorClass::RiskRejected => Self::RiskRejected,
            rustcta_types::ExchangeErrorClass::StaleMarketData => Self::StaleMarketData,
            rustcta_types::ExchangeErrorClass::UnknownOrderState => Self::UnknownOrderState,
            rustcta_types::ExchangeErrorClass::Decode => Self::Decode,
            rustcta_types::ExchangeErrorClass::Internal => Self::Internal,
            rustcta_types::ExchangeErrorClass::Unknown => Self::Unknown,
        }
    }
}

impl From<ExchangeErrorKind> for rustcta_types::ExchangeErrorClass {
    fn from(kind: ExchangeErrorKind) -> Self {
        match kind {
            ExchangeErrorKind::Unsupported => Self::Unsupported,
            ExchangeErrorKind::UnsupportedCapability => Self::UnsupportedCapability,
            ExchangeErrorKind::Authentication => Self::Authentication,
            ExchangeErrorKind::Permission => Self::Permission,
            ExchangeErrorKind::RateLimited => Self::RateLimited,
            ExchangeErrorKind::Network => Self::Network,
            ExchangeErrorKind::Timeout => Self::Timeout,
            ExchangeErrorKind::ExchangeUnavailable => Self::ExchangeUnavailable,
            ExchangeErrorKind::Maintenance => Self::Maintenance,
            ExchangeErrorKind::InvalidRequest => Self::InvalidRequest,
            ExchangeErrorKind::InvalidSymbol => Self::InvalidSymbol,
            ExchangeErrorKind::InvalidPrecision => Self::InvalidPrecision,
            ExchangeErrorKind::MinNotionalViolation => Self::MinNotionalViolation,
            ExchangeErrorKind::InsufficientBalance => Self::InsufficientBalance,
            ExchangeErrorKind::InsufficientPosition => Self::InsufficientPosition,
            ExchangeErrorKind::DuplicateClientOrderId => Self::DuplicateClientOrderId,
            ExchangeErrorKind::InvalidClientOrderId => Self::InvalidClientOrderId,
            ExchangeErrorKind::OrderNotFound => Self::OrderNotFound,
            ExchangeErrorKind::OrderRejected => Self::OrderRejected,
            ExchangeErrorKind::RiskRejected => Self::RiskRejected,
            ExchangeErrorKind::StaleMarketData => Self::StaleMarketData,
            ExchangeErrorKind::UnknownOrderState => Self::UnknownOrderState,
            ExchangeErrorKind::Decode => Self::Decode,
            ExchangeErrorKind::Internal => Self::Internal,
            ExchangeErrorKind::Unknown => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryAfter {
    Millis(u64),
    Header(String),
    Unspecified,
}

impl RetryAfter {
    pub fn millis(&self) -> Option<u64> {
        match self {
            Self::Millis(value) => Some(*value),
            Self::Header(_) | Self::Unspecified => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExchangeErrorMapping {
    pub exchange: rustcta_types::ExchangeId,
    pub kind: ExchangeErrorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<RetryAfter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<serde_json::Value>,
}

impl ExchangeErrorMapping {
    pub fn new(
        exchange: rustcta_types::ExchangeId,
        kind: ExchangeErrorKind,
        message: impl Into<String>,
    ) -> Self {
        Self {
            exchange,
            kind,
            code: None,
            message: message.into(),
            retry_after: None,
            raw: None,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.kind.is_retryable()
    }

    pub fn requires_reconciliation(&self) -> bool {
        self.kind.requires_reconciliation()
    }
}

impl From<ExchangeError> for ExchangeErrorMapping {
    fn from(error: ExchangeError) -> Self {
        Self {
            exchange: error.exchange_id,
            kind: ExchangeErrorKind::from(error.class),
            code: error.code,
            message: error.message,
            retry_after: error.retry_after_ms.map(RetryAfter::Millis),
            raw: error.raw,
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OrderLevelError {
    pub kind: ExchangeErrorKind,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exchange_order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<RetryAfter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<serde_json::Value>,
}

impl OrderLevelError {
    pub fn new(kind: ExchangeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            code: None,
            client_order_id: None,
            exchange_order_id: None,
            retry_after: None,
            raw: None,
        }
    }

    pub fn with_order(mut self, order: &OrderState) -> Self {
        self.client_order_id = order.client_order_id.clone();
        self.exchange_order_id = order.exchange_order_id.clone();
        self
    }

    pub fn is_retryable(&self) -> bool {
        self.kind.is_retryable()
    }
}

#[derive(Debug, Error)]
pub enum ExchangeApiError {
    #[error("exchange error: {0:?}")]
    Exchange(ExchangeError),
    #[error("unsupported exchange API operation: {operation}")]
    Unsupported { operation: &'static str },
    #[error("invalid exchange API request: {message}")]
    InvalidRequest { message: String },
    #[error("serialization error: {message}")]
    Serialization { message: String },
    #[error("transport error: {message}")]
    Transport { message: String },
}

impl From<ExchangeError> for ExchangeApiError {
    fn from(error: ExchangeError) -> Self {
        Self::Exchange(error)
    }
}

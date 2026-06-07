use thiserror::Error;

use crate::ExchangeError;

pub type ExchangeApiResult<T> = Result<T, ExchangeApiError>;

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

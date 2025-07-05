use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Network error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("JSON parsing error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("URL parsing error: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("WebSocket error: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HMAC error")]
    HmacError,

    #[error("Binance API error: {code}, {msg}")]
    BinanceError { code: i64, msg: String },

    #[error("Other error: {0}")]
    Other(String),

    #[error("Unknown error")]
    Unknown,
}

impl From<hmac::digest::InvalidLength> for AppError {
    fn from(_: hmac::digest::InvalidLength) -> Self {
        AppError::HmacError
    }
}

use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HibachiSigningSide {
    Bid,
    Ask,
}

impl HibachiSigningSide {
    fn wire_value(self) -> u32 {
        match self {
            Self::Ask => 0,
            Self::Bid => 1,
        }
    }
}

pub fn hibachi_create_order_payload_hex(
    nonce: u64,
    contract_id: u32,
    quantity_scaled: u64,
    side: HibachiSigningSide,
    price_scaled: Option<u64>,
    max_fees_scaled: u64,
) -> String {
    let mut bytes = Vec::with_capacity(40);
    bytes.extend_from_slice(&nonce.to_be_bytes());
    bytes.extend_from_slice(&contract_id.to_be_bytes());
    bytes.extend_from_slice(&quantity_scaled.to_be_bytes());
    bytes.extend_from_slice(&side.wire_value().to_be_bytes());
    bytes.extend_from_slice(&price_scaled.unwrap_or_default().to_be_bytes());
    bytes.extend_from_slice(&max_fees_scaled.to_be_bytes());
    hex::encode(bytes)
}

pub fn hibachi_cancel_order_payload_hex(order_id_or_nonce: u64) -> String {
    hex::encode(order_id_or_nonce.to_be_bytes())
}

pub fn hibachi_hmac_sha256_signature_hex(
    secret: &str,
    payload_hex: &str,
) -> ExchangeApiResult<String> {
    let bytes = hex::decode(payload_hex.trim()).map_err(validation_error)?;
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(validation_error)?;
    mac.update(&bytes);
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn hibachi_private_write_boundary() -> &'static str {
    "hibachi.trade_write_request_spec_only"
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

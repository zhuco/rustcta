#![cfg_attr(not(test), allow(dead_code))]

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub const BSX_EIP712_ORDER_BOUNDARY: &str =
    "bsx REST order writes require EIP-712 Order(sender,size,price,nonce,productIndex,orderSide) signatures; adapter keeps writes request-spec-only";

pub fn bsx_ws_auth_message(api_key: &str, timestamp_ns: i128) -> String {
    format!("{},{}", api_key.trim(), timestamp_ns)
}

pub fn bsx_ws_auth_signature(api_key: &str, api_secret: &str, timestamp_ns: i128) -> String {
    let message = bsx_ws_auth_message(api_key, timestamp_ns);
    let mut mac =
        HmacSha256::new_from_slice(api_secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn bsx_eip712_order_boundary() -> &'static str {
    BSX_EIP712_ORDER_BOUNDARY
}

#![cfg_attr(not(test), allow(dead_code))]

pub const WAVESEXCHANGE_PRIVATE_WRITE_BOUNDARY: &str =
    "wavesexchange private writes require Waves order Curve25519 signatures; no HMAC API-key signing";

pub fn wavesexchange_private_write_boundary() -> &'static str {
    WAVESEXCHANGE_PRIVATE_WRITE_BOUNDARY
}

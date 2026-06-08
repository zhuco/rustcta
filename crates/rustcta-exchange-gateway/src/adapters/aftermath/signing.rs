#![cfg_attr(not(test), allow(dead_code))]

pub const AFTERMATH_PRIVATE_WRITE_BOUNDARY: &str =
    "aftermath private writes require Sui transaction build/sign/submit; no HMAC API-key signing";

pub fn aftermath_private_write_boundary() -> &'static str {
    AFTERMATH_PRIVATE_WRITE_BOUNDARY
}

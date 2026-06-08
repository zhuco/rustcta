#![cfg_attr(not(test), allow(dead_code))]

pub const D8X_SIGNING_BOUNDARY: &str =
    "d8x trading and account reads require EVM wallet or ethers signer contract calls; no HMAC/API-key signing is exposed";

pub fn d8x_contract_call_boundary(chain_id: u64, contract: &str, method: &str) -> String {
    format!(
        "chain_id={chain_id};contract={};method={};signer=eip155-wallet",
        normalized_address(contract),
        method.trim()
    )
}

pub fn d8x_signing_boundary() -> &'static str {
    D8X_SIGNING_BOUNDARY
}

fn normalized_address(address: &str) -> String {
    address.trim().to_ascii_lowercase()
}

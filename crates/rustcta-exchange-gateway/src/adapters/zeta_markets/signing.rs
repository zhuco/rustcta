#![cfg_attr(not(test), allow(dead_code))]

pub const ZETA_MARKETS_SIGNING_BOUNDARY: &str =
    "zeta_markets trading used wallet-signed Solana SDK transactions; the venue ceased operations and this adapter never handles private keys";

pub fn zeta_markets_solana_transaction_boundary(
    program: &str,
    instruction: &str,
    market: &str,
    wallet_scope: &str,
) -> String {
    format!(
        "program={};instruction={};market={};wallet_scope={};runtime=unsupported",
        program.trim(),
        instruction.trim(),
        market.trim(),
        wallet_scope.trim()
    )
}

pub fn zeta_markets_signing_boundary() -> &'static str {
    ZETA_MARKETS_SIGNING_BOUNDARY
}

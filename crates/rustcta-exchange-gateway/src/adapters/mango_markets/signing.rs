#![cfg_attr(not(test), allow(dead_code))]

pub const MANGO_MARKETS_SIGNING_BOUNDARY: &str =
    "mango_markets order writes are Solana transactions signed by a wallet; no API secret or server-side signing is supported by this adapter";

pub fn mango_markets_transaction_boundary() -> &'static str {
    MANGO_MARKETS_SIGNING_BOUNDARY
}

pub fn mango_markets_instruction_fingerprint(
    program_id: &str,
    group: &str,
    account: &str,
    instruction: &str,
) -> String {
    format!(
        "program={} group={} account={} instruction={}",
        program_id.trim(),
        group.trim(),
        account.trim(),
        instruction.trim()
    )
}

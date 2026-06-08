# derive_chain_perps Gateway Profile

Task C-47 covers `derive_chain_perps`, a Derive Chain settlement profile for perpetuals. It does not duplicate the existing `derive` adapter; runtime Derive Exchange API coverage remains owned by `derive`, while this profile records chain/profile differences and settlement audit boundaries.

Official sources reviewed:
- Derive docs: https://docs.derive.xyz/
- Pricing mechanism: https://docs.derive.xyz/whitepaper-v3/pricing-mechanism
- Funding rate: https://docs.derive.xyz/whitepaper-v3/funding-rate
- Derive SDK examples: https://github.com/deriveprotocol/sdk

The profile records `https://api.derive.xyz`, `https://rpc.lyra.finance`, and Derive Chain id `957` as audit metadata only. No REST, WebSocket, indexer, or wallet-signing surface is promoted as a live exchange-gateway runtime in this task.

Runtime boundary:
- Product: `MarketType::Perpetual`
- Chain: Derive Chain, chain id `957`
- Public REST/WS: unsupported unverified
- Private reads: unsupported unverified
- Orders/cancels/batch: unsupported unverified
- Account model: Derive Chain wallet profile, not API key/HMAC

Non-compile validation:
- `python scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/derive_chain_perps/endpoint_mapping.yaml`
- JSON fixture parse for `tests/fixtures/exchanges/derive_chain_perps/**/*.json`
- `rustfmt --check` on touched Rust files
- template-residue grep for copied venue names and stale task IDs

Per task instruction, do not run cargo build/check/test for this profile.

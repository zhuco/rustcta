# derive_chain_perps Gateway Profile

Task C-47 covers `derive_chain_perps`, a Derive Chain settlement profile for perpetuals. It does not duplicate the existing `derive` adapter; runtime Derive Exchange API coverage remains owned by `derive`, while this profile records chain/profile differences and settlement audit boundaries.

Official sources reviewed:
- Derive docs: https://docs.derive.xyz/
- Pricing mechanism: https://docs.derive.xyz/whitepaper-v3/pricing-mechanism
- Funding rate: https://docs.derive.xyz/whitepaper-v3/funding-rate
- Derive SDK examples: https://github.com/deriveprotocol/sdk

The profile records `https://api.derive.xyz`, `https://rpc.lyra.finance`, and Derive Chain id `957` as audit metadata only. No REST, WebSocket, indexer, or wallet-signing surface is promoted as a live exchange-gateway runtime in this task.

Product lines:
- Perpetual: `MarketType::Perpetual` settlement/profile boundary.
- Spot: 交易所不支持现货（本 `derive_chain_perps` profile 口径）。Derive 平台其他产品线不由此 profile 承接。

Runtime boundary:
- Product: `MarketType::Perpetual`
- Chain: Derive Chain, chain id `957`
- Public WS: 交易所不支持公共 WS 行情（本 profile 口径）；Derive Exchange 官方 WS runtime 归 `derive` adapter，不在 `derive_chain_perps` 重复实现
- Public REST: unsupported unverified
- Private reads: unsupported unverified
- Orders/cancels/batch: unsupported unverified
- Account model: Derive Chain wallet profile, not API key/HMAC

Core trading official detail:
- Source: [核心交易官方核验 P3 第四批](../核心交易官方核验_P3_第四批.md)
- 写法：`交易所不支持当前 profile 交易/私有接口 runtime`。
- 原因：本 profile 只记录 Derive Chain settlement/profile boundary；Derive Exchange REST/WS/order runtime 归 `derive` adapter，不在这里重复实现。

Position official detail:
- Source: [仓位接口官方核验 P1 第二批](../仓位接口官方核验_P1_第二批.md)
- 写法：`交易所不支持当前仓位接口 runtime`。
- 原因：本 profile 不重复承接 Derive Exchange account/position runtime；仓位能力看 `derive` adapter。

Non-compile validation:
- `python scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/derive_chain_perps/endpoint_mapping.yaml`
- JSON fixture parse for `tests/fixtures/exchanges/derive_chain_perps/**/*.json`
- `rustfmt --check` on touched Rust files
- template-residue grep for copied venue names and stale task IDs

Per task instruction, do not run cargo build/check/test for this profile.

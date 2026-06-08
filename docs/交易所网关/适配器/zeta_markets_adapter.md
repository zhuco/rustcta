# Zeta Markets Gateway Adapter

Status date: 2026-06-08

## Scope

`zeta_markets` is a conservative C-45 adapter for the original Zeta Markets Solana derivatives venue. The public Zeta documentation states that the original protocol ceased operations in May 2025, so this adapter is scan-only. It maps legacy public REST market metadata and order book fixtures, and keeps private reads, wallet signing, order writes, batch operations and stream runtime explicitly `Unsupported`.

## Official References

| Item | URL | Adapter use |
| --- | --- | --- |
| Main docs | `https://docs.zeta.markets/` | Shutdown and Solana protocol boundary audit |
| REST API reference | `https://zetamarkets.apidocumentation.com/` | Legacy public data API paths |
| Data API base URL | `https://api.zeta.markets` | Public symbols and order book fixtures |
| Devnet historical base URL | `https://dex-devnet-webserver-ecs.zeta.markets` | Documented only; not enabled by default |

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Perpetual futures | `Perpetual` | Legacy public REST fixture only; original Zeta Markets stopped operations in 2025-05. |
| Spot | n/a | 交易所不支持现货。 |
| Current trading/private API/public WS runtime | n/a | 交易所不支持当前交易/私有接口/公共 WS runtime；只保留 legacy scan。 |

## Capability

| Operation | Support | Notes |
| --- | --- | --- |
| `get_symbol_rules` | native public REST | Parses `/prices/symbols` style fixture into perpetual `SymbolRules`. |
| `get_order_book` | native public REST | Parses `/v2/orderbook?ticker_id={symbol}` snapshot fixture. |
| balances / positions / fees | unsupported | Require wallet-owned Solana margin account and SDK audit. |
| place / cancel / amend / batch | unsupported | Venue is shut down; no private keys or Solana transactions are built. |
| WebSocket | unsupported | 交易所不支持当前公共 WS runtime；fallback is REST polling only for legacy fixtures. |

## Unsupported Boundary

The adapter never accepts wallet private keys, seed phrases or production signing material. Historical Solana SDK transaction construction is documented only through a sanitized signing-boundary fixture. Options-specific semantics remain audit-only because the gateway trait does not expose a lossless option contract model.

Official core trading verification:

[核心交易官方核验 P2 第三批](../核心交易官方核验_P2_第三批.md) confirms this is not a live trading implementation gap. Official Zeta docs state the original venue ceased operations in 2025-05, and Final Epoch material says trading was disabled after 2025-05-01 04:00 UTC. Keep this adapter as `交易所不支持当前交易/私有接口 runtime`; do not add live place/cancel runtime for the legacy Zeta venue.

Official position verification:

[仓位接口官方核验 P1 第二批](../仓位接口官方核验_P1_第二批.md) confirms balances/positions/private account runtime remain unsupported for this legacy scan adapter. Keep `get_positions` as `交易所不支持当前仓位接口 runtime`; do not add live private position runtime for the stopped venue.

## Validation

Allowed validation commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/zeta_markets/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway zeta_markets --lib --message-format short
cargo test -p rustcta-gateway zeta_markets --message-format short
```

Do not run `cargo build` for this task.

# ApolloX DEX Gateway Adapter

Status date: 2026-06-08

Adapter id: `apollox_dex`

## Scope

`apollox_dex` targets APX/ApolloX V1 orderbook perpetual contracts. The V1 API is Binance Futures style: public REST market metadata and order book snapshots are enabled, while signed private REST is kept as offline request-spec/signing-vector coverage only.

Out of scope:

- ApolloX spot API family at `https://www.apollox.finance/api/v1`.
- APX V2 fully on-chain trading, wallet signing, gas, chain profiles and contract execution.
- Broker SDK proxy/UI paths, deposits, withdrawals, transfers and custody operations.
- Stable V1 testnet routing; no stable REST/WS testnet base is declared.

## Official Sources

| Topic | Source |
| --- | --- |
| V1 API docs | `https://apollox-finance.gitbook.io/apollox-finance/api/api-documentation.md` |
| Product overview | `https://apollox-finance.gitbook.io/apollox-finance` |
| V2 trading docs | `https://apollox-finance.gitbook.io/apollox-finance/welcome/trading-on-v2.md` |
| API GitHub mirror | `https://github.com/apollox-finance/apollox-finance-api-docs` |
| Broker SDK boundary | `https://github.com/apollox-finance/broker-web-sdk` |

## Endpoints

| Capability | Runtime | Endpoint |
| --- | --- | --- |
| Symbol rules | Native public REST | `GET /fapi/v1/exchangeInfo` |
| Order book snapshot | Native public REST | `GET /fapi/v1/depth` |
| Funding / premium index | Spec-only | `GET /fapi/v1/premiumIndex`, `GET /fapi/v1/fundingRate` |
| Balances / positions | Request-spec only | `GET /fapi/v2/balance`, `GET /fapi/v2/positionRisk` |
| Place / cancel / cancel-all | Request-spec only | `POST/DELETE /fapi/v1/order`, `DELETE /fapi/v1/allOpenOrders` |
| Open orders / fills | Request-spec only | `GET /fapi/v1/openOrders`, `GET /fapi/v1/userTrades` |

## Auth

Signed `TRADE` and `USER_DATA` endpoints use `X-MBX-APIKEY` plus HMAC-SHA256 over the canonical query/body parameters. Fixtures use sanitized `test-key` and `test-secret`; no real credential or account identifier is present.

Private REST runtime remains disabled even if credentials are present. This avoids treating V1 account availability, region restrictions and write permissions as validated.

## WebSocket

Public WS base is `wss://fstream.apollox.finance`. The adapter includes subscription payload fixtures for Binance-style streams such as `btcusdt@depth`. Runtime WS is disabled until snapshot/delta resync, 24-hour reconnect and stream limits are verified.

Private WS uses listen keys from `POST|PUT|DELETE /fapi/v1/listenKey` in the docs, but runtime private streams remain unsupported and require REST reconciliation fallback.

## Capability Boundary

- `MarketType::Perpetual` only.
- Public REST symbol rules and order book snapshot are enabled.
- Private REST, private WS, batch, cancel-all and writes are request-spec only or `Unsupported`.
- APX V2 on-chain trading is explicitly `Unsupported`.

## Validation

Allowed validation commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/apollox_dex/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway apollox_dex --lib --message-format short
cargo test -p rustcta-gateway apollox_dex --message-format short
```

Do not run `cargo build`, release builds or live trading commands for this task.

# Hibachi Gateway Adapter

Status date: 2026-06-08

Task: A-22 / AI-R22 from `docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`.

## Product Scope

- Adapter id: `hibachi`
- Product: USDT-settled perpetual contracts
- Implemented runtime surface: public REST market metadata, public REST order book snapshot, public fee readback from exchange info.
- Offline-only surface: private write request specs and exchange-managed HMAC signing vector.
- Explicitly closed: real place/cancel/amend/batch/cancel-all, private REST read runtime, private WebSocket runtime, withdrawals/transfers, trustless/non-custodial signing runtime.

## Official Sources

| Area | Source |
| --- | --- |
| Market REST | `https://hibachi-docs.redocly.app/marketapi/market` |
| Account/trade REST | `https://hibachi-docs.redocly.app/accountapi/trade` |
| Authorization | `https://hibachi-docs.redocly.app/api/authorization` |
| Signing | `https://hibachi-docs.redocly.app/api/signing` |
| SDK WS defaults | `hibachi-xyz` Python SDK `api_ws_market.py`, `api_ws_account.py`, `api_ws_trade.py` |

Observed base URLs:

- Market/data REST: `https://data-api.hibachi.xyz`
- Account/trade REST: `https://api.hibachi.xyz`
- Market WS: `wss://data-api.hibachi.xyz/ws/market`
- Account WS: `wss://api.hibachi.xyz/ws/account`
- Trade WS: `wss://api.hibachi.xyz/ws/trade`

No public testnet/sandbox URL was confirmed during this implementation.

## Endpoint Mapping

Machine-readable mapping: `crates/rustcta-exchange-gateway/src/adapters/hibachi/endpoint_mapping.yaml`.

Key endpoints:

- `GET /market/exchange-info`: contract metadata, increments, min order size, min notional, fee config.
- `GET /market/data/orderbook`: order book snapshot by `symbol`, `depth`, and `granularity`.
- `GET /market/data/prices`: mark/trade/spot price and funding estimate, mapped as spec-only.
- `GET /market/data/open-interest`: open interest, mapped as spec-only.
- `GET /trade/account/info`, `GET /trade/orders`, `GET /trade/order`, `GET /trade/account/trades`: audited private read endpoints, runtime closed by default.
- `POST/DELETE/PATCH /trade/order`, `DELETE/POST /trade/orders`: write endpoints kept request-spec only.

## Signing Boundary

Hibachi documents order signing using an account model with `accountId`, `nonce`, symbol/contract fields, and a `signature` field. The adapter includes an offline exchange-managed HMAC-SHA256 vector for the documented byte-packing shape:

- Fixture: `tests/fixtures/exchanges/hibachi/signing_vectors/exchange_managed_hmac.json`
- Request spec: `tests/fixtures/exchanges/hibachi/request_specs/place_order_limit.json`

Trustless/non-custodial signing and any key registration flow are not enabled in runtime. The adapter returns `hibachi.trade_write_request_spec_only` and related Unsupported boundaries for write methods.

## WebSocket Boundary

The SDK exposes market/account/trade WS clients. This adapter only builds deterministic payloads and documents heartbeat/reconnect policy:

- Subscribe: `{ "method": "subscribe", "channel": "orderbook/{symbol}" }`
- Unsubscribe: `{ "method": "unsubscribe", "channel": "orderbook/{symbol}" }`
- Keepalive: `{ "method": "ping" }`
- Account stream URL shape: `wss://api.hibachi.xyz/ws/account?accountId={accountId}`

Production WS runtime, sequence-gap handling, checksum validation, auth renewal, and private reconciliation are follow-up work.

## Capabilities

| Capability | Status | Notes |
| --- | --- | --- |
| `MarketType::Perpetual` | Native | Public metadata and book snapshot only. |
| Symbol rules | Native | From `futureContracts` in `/market/exchange-info`. |
| Order book snapshot | Native | From `/market/data/orderbook`. |
| Fees | Native public readback | Maker/taker defaults from `feeConfig`. |
| Balances/positions/open orders/fills | Unsupported runtime | Request-spec/audit boundary exists, no live read. |
| Place/cancel/amend/batch/cancel-all | Unsupported runtime | Offline request-spec and signing vector only. |
| Public/private WS runtime | Unsupported | Payload fixtures and URLs only. |
| Client order id | Unsupported | Account API notes nonce/order id identifiers and says client-id support is future work. |
| Testnet | Unsupported/unverified | No public sandbox base URL confirmed. |

## Validation

Allowed commands for this adapter:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/hibachi/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway hibachi --lib --message-format short
cargo test -p rustcta-gateway hibachi --message-format short
```

Do not run `cargo build`, release builds, app/web builds, live order placement, live cancel, withdrawal, transfer, or long-running private streams for this task.

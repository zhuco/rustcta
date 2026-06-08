# ApeX Gateway Adapter

Status date: 2026-06-08

Adapter id: `apex`

Task 7 status: ApeX Omni perpetual-first public REST G1 plus audited private
read/write request-spec boundaries. Private order placement remains unsupported
because ApeX order creation requires zkLink/L2 payload signatures in addition
to API-key HMAC headers.

## Product Lines

| Product | MarketType | Current status |
| --- | --- | --- |
| Omni perpetual | `Perpetual` | Primary Task 7 scope; public symbol-rules/order-book REST implemented. |
| Spot | n/a | Unsupported in this gateway task. |
| RWA / prediction / transfer / withdrawal | n/a | Unsupported; outside trading runtime scope. |

REST base URLs:

- Mainnet: `https://omni.apex.exchange`
- Testnet: `https://testnet.omni.apex.exchange`

The runtime endpoint paths include `/api/v3/...`; keep the configured base URL
host-only to avoid double-prefixing `/api`. The official SDK has a different QA
host constant, so testnet host selection must stay configurable.

WS base URLs:

- Public: `wss://quote.omni.apex.exchange/realtime_public?v=2&timestamp=<ts>`
- Private: `wss://quote.omni.apex.exchange/realtime_private?v=2&timestamp=<ts>`

## Authentication

Private REST requires:

- `APEX-SIGNATURE`
- `APEX-TIMESTAMP`
- `APEX-API-KEY`
- `APEX-PASSPHRASE`

The API-key signature message is:

`timestamp + method + path + dataString`

GET requests sign the path/query. POST requests sign an alphabetically sorted
form body. The HMAC-SHA256 output is Base64 encoded.

Orders, transfers, and withdrawals require an additional zkKeys/L2 signature in
the request body. The gateway does not implement that signer yet, so private
writes are not promoted.

## Endpoint Mapping

Mapping file:
`crates/rustcta-exchange-gateway/src/adapters/apex/endpoint_mapping.yaml`

Implemented for public REST:

- `GET /api/v3/symbols`
- `GET /api/v3/depth`

Covered as private request-spec/signing fixtures:

- `GET /api/v3/account`
- `GET /api/v3/account-balance`
- `GET /api/v3/open-orders`
- `GET /api/v3/order`
- `GET /api/v3/fills`
- `POST /api/v3/delete-order`

`POST /api/v3/order` is documented but unsupported by the adapter until
deterministic zkLink/L2 order signing vectors exist in Rust.

Native amend and batch place/cancel endpoints were not found in the audited
docs and remain unsupported.

## WebSocket

Public WS uses payloads such as:

```json
{"op":"subscribe","args":["orderBook200.H.BTCUSDT"]}
```

Heartbeat uses client `ping` roughly every 15 seconds.

Private WS logs in with an `op = "login"` message whose `args` contains a JSON
string with API key fields, signature, and `ws_zk_accounts_v3` topics. Runtime
private WS routing remains disabled until auth and reconciliation are tested.

## Fixtures

Fixtures live under `tests/fixtures/exchanges/apex/`:

- `symbols_perpetual.json`
- `depth_btcusdt.json`
- `account_positions.json`
- `request_specs/get_open_orders.json`
- `request_specs/get_account_balance.json`
- `request_specs/get_account.json`
- `request_specs/query_order.json`
- `request_specs/get_fills.json`
- `request_specs/cancel_order.json`
- `request_specs/cancel_all_orders.json`
- `request_specs/place_order_unsupported_zk.json`
- `signing_vectors/private_get_open_orders.json`
- `ws_public_orderbook.json`
- `ws_private_login.json`
- `unsupported_boundary.json`

## Unsupported Boundary

The current adapter is not live-trade-enabled. `place_order` returns
`Unsupported("apex.place_order_requires_zklink_l2_signature")`. Market orders
also require a signed price/worst-price field and are unsupported.

Before promotion, add parser tests for configs/depth/account/fills, implement
mocked transport, validate API-key HMAC against official SDK vectors, and add a
Rust zkLink/L2 signer with deterministic official vectors.

## Validation

Allowed validation commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/apex/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway apex --lib --message-format short
```

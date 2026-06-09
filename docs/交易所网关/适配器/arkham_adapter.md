# Arkham Gateway Adapter

A-03 scope from `docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`:
`arkham` is treated as an Arkham Exchange trading adapter, not as an Arkham
Intel on-chain analytics adapter.

## Official Sources

| Area | Source |
| --- | --- |
| Exchange API overview, REST base URL and signing | https://arkm.com/docs |
| Exchange API limits | https://arkm.com/limits-api |
| Arkham Intel API boundary | https://arkm.com/api/docs |

## Product Boundary

Arkham has two relevant API families:

- Arkham Exchange API at `https://arkm.com/api`, covering spot, perpetual
  market data, account state and order management.
- Arkham Intel API at `https://api.arkm.com`, covering blockchain analytics,
  entities, labels, transfers and alerts.

Only the Exchange API is in this gateway adapter. Intel API endpoints are not
mapped as trading capabilities.

## Coverage

| Area | Status |
| --- | --- |
| Product line | Spot and USDT perpetual |
| Public REST | `GET /public/pairs` and `GET /public/book` parser/transport |
| Private REST | guarded read-only order query, open orders and fills runtime; balances, positions, fees and writes remain offline request-spec only |
| WebSocket | public subscribe/unsubscribe/auth/heartbeat fixtures; live private WS disabled |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/arkham/endpoint_mapping.yaml` |
| Fixtures | `tests/fixtures/exchanges/arkham/` |
| Config example | `config/arkham_gateway_example.yml`, disabled by default |

## Authentication

Private REST uses these headers:

- `Arkham-Api-Key`
- `Arkham-Expires`
- `Arkham-Signature`
- `Arkham-Broker-Id`

The signature is base64 HMAC-SHA256 over
`api_key + expires + method + path + body`, using the base64-decoded API
secret. The fixture
`tests/fixtures/exchanges/arkham/signing_vectors/rest_hmac_sha256.json`
covers the offline vector.

## Runtime Boundary

Public REST can fetch symbol rules and order book snapshots when enabled.
Private REST readbacks for query order, open orders and recent fills fail closed
unless `ARKHAM_PRIVATE_REST_ENABLED`/`RUSTCTA_ARKHAM_PRIVATE_REST_ENABLED` and
API credentials are configured. The request specs document the native fields
without enabling live order mutation. Private WebSocket is kept behind REST
reconciliation until a separate live-dry-run validation verifies auth, channels
and replay behavior.

Unsupported:

- Arkham Intel API as trading adapter data.
- Withdrawals, transfers and deposit address creation.
- Trigger orders and leverage mutation.
- Private WebSocket live auth.
- Batch place/cancel. Native cancel-all is spec-only.

## Official WebSocket Order Book Detail

Arkham Exchange official material confirms a RESTful HTTP API and a Websocket
API with WebSocket request limits, but the publicly accessible pages reviewed in
this batch did not expose stable order-book channel, interval, depth,
sequence/checksum, or subscribe payload details. The mapping therefore records
`orderbook_channel: unverified`, no fixed interval published, depth:
unspecified / 未给固定档位, and `project_unimplemented` runtime boundary. Do not
enable runtime before the full official Arkham Exchange WS reference is
captured. Source batch:
[WebSocket 官方核验 P6 补充交易所盘口细项](../WebSocket官方核验_P6_补充交易所盘口细项.md).

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/arkham/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway arkham --lib --message-format short
cargo test -p rustcta-gateway arkham --message-format short
```

Do not run `cargo build` for this task.

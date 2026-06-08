# AscendEX Gateway Adapter

Status date: 2026-06-08

Adapter id: `ascendex`

Implementation status: Spot + futures/perpetual REST, endpoint mapping,
request-spec fixtures, signing vectors, parser fixtures, and WebSocket
payload/parser support are implemented behind `rustcta-exchange-api::ExchangeClient`.
The gateway returns subscription ids and validates payload/parser behavior; a
production long-running WebSocket supervisor is still a platform follow-up.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot/Cash | `Spot` | Public/private REST + public/private WS specs |
| USDT futures/perpetual | `Perpetual` | Public/private REST + public/private WS specs |
| Testnet | n/a | Futures sandbox is official; Spot sandbox is not declared as a stable product line |

REST base URL: `https://ascendex.com`

Official futures sandbox base URL: `https://api-test.ascendex-sandbox.com`.
The adapter keeps `rest_base_url` configurable and defaults to production.

Machine-readable endpoint and capability metadata lives at
`crates/rustcta-exchange-gateway/src/adapters/ascendex/endpoint_mapping.yaml`.
That mapping declares `capabilities_v2`-equivalent metadata for batch atomicity,
history pagination, stream heartbeat/auth renewal, credential scope, rate-limit
planning, reconciliation, and unsupported gaps. The runtime `capabilities()`
response preserves the legacy gateway capability fields and private stream
capability structure.

## Authentication

Private requests use:

- Headers: `x-auth-key`, `x-auth-timestamp`, `x-auth-signature`
- Timestamp: UTC milliseconds
- Signature: Base64 `HMAC-SHA256(secret, "<timestamp>+<api-path>")`

AscendEX signs a documented API path suffix, not the complete URL. Examples:
Spot order uses `order`, Spot balance uses `balance`, and futures order uses
`v2/futures/order`.

Private REST also requires `ASCENDEX_ACCOUNT_GROUP`, because account-scoped
paths are prefixed as `/<accountGroup>/api/pro/...`.

## Endpoint Mapping

The YAML mapping is the authoritative checklist for task 15 delivery: endpoint
paths, sign paths, account-group requirements, WS URLs/channels, heartbeat
policy, auth renewal, pagination, rate-limit plan, reconciliation plan, batch
limits, and unsupported gaps.

| Standard capability | Spot endpoint | Futures endpoint |
| --- | --- | --- |
| symbol rules | `GET /api/pro/v1/cash/products` | `GET /api/pro/v2/futures/contract` |
| order book | `GET /api/pro/v1/depth` | `GET /api/pro/v1/depth` scan-only fallback |
| balances | `GET /{group}/api/pro/v1/cash/balance` | `GET /{group}/api/pro/v2/futures/position` collateral snapshot |
| positions | Unsupported | `GET /{group}/api/pro/v2/futures/position` |
| fees | `GET /{group}/api/pro/v1/spot/fee` | `GET /{group}/api/pro/v1/futures/fee` |
| place order | `POST /{group}/api/pro/v1/cash/order` | `POST /{group}/api/pro/v2/futures/order` |
| cancel order | `DELETE /{group}/api/pro/v1/cash/order` | `DELETE /{group}/api/pro/v2/futures/order` |
| cancel all | `DELETE /{group}/api/pro/v1/cash/order/all` | `DELETE /{group}/api/pro/v2/futures/order/all` |
| batch place/cancel | `POST/DELETE /{group}/api/pro/v1/cash/order/batch` | `POST/DELETE /{group}/api/pro/v2/futures/order/batch` |
| query order | `GET /{group}/api/pro/v1/cash/order/status` | `GET /{group}/api/pro/v2/futures/order/status` |
| open orders | `GET /{group}/api/pro/v1/cash/order/open` | `GET /{group}/api/pro/v2/futures/order/open` |
| fills | `GET /{group}/api/pro/v1/cash/order/hist/current` order aggregate fallback | `GET /{group}/api/pro/v2/futures/order/hist/current` order aggregate fallback |
| public WS | `wss://.../api/pro/v1/stream` channels `depth`, `trades`, `bbo`, `bar` | `wss://.../api/pro/v2/stream` same standard channel mapping |
| private WS | `wss://.../{group}/api/pro/v1/stream`, auth prehash `stream`, `order:cash` | `wss://.../{group}/api/pro/v2/stream`, auth prehash `v2/stream`, `futures-order`; account/balance/position standard event parsing is not advertised yet |
| heartbeat | JSON ping/pong payloads | JSON ping/pong payloads |

Advanced native features such as amend, order lists, stop orders, leverage
setting, margin type, position mode, and private account/balance/position stream
event parsing are not advertised in `ExchangeClientCapabilities` in this
adapter pass.

## Reconciliation And Batch Boundaries

- REST reconciliation after private stream gaps uses open orders, order status,
  recent-fill/order-history fallback, and balances/positions where the product
  supports them.
- Batch place/cancel is native for Spot and futures, capped at 10 items, requires
  one market type per request, and is declared partial/non-atomic.
- Private streams require account-group credentials and relogin/resubscribe on
  reconnect. There is no listen-key renewal flow.
- Unsupported gaps are explicit: quote-sized market orders, amend/order-list
  workflows, stop orders, leverage/margin/position-mode mutation, and Spot
  positions.
- Pagination is limited to current order-history reads with `n <= 500`; cursor
  based historical traversal is not advertised.
- Live-dry-run guards such as kill-switch, disabled-symbol, and max-notional are
  expected from shared runtime configuration, not from this adapter directory.

## Error Classification

The transport maps AscendEX `code != 0`, `status=Err`, and non-2xx HTTP
responses into `ExchangeApiError::Exchange`. Classification covers
authentication, permission, rate limits, exchange unavailable, insufficient
balance, min-notional violations, invalid precision, duplicate client order id,
order not found, and invalid symbol using documented code/message families.

## Fixture Coverage

AscendEX-specific fixtures live under `tests/fixtures/exchanges/ascendex/`.
They include legacy `request_specs.json`, legacy `signing_vectors.json`,
standard `request_specs/*.json`, standard `signing_vectors/*.json`, public symbol and
contract responses, order book snapshots, balances, positions, order ack, order
history fill fallback, exchange error, public/private WS messages, empty
response, and missing-field decode error inputs. The signing vectors cover REST
`order`, Spot WS `stream`, and futures REST `v2/futures/order`.

No fixture contains real API keys, secrets, account ids, exchange order ids, or
client order ids.

## Runtime Policies

Public WS support is declared as spec payload plus parser runtime capability for
`depth`, `trades`, `bbo`, and `bar` channels. Private WS support is declared for
auth plus order/fill event parsing on `order:cash` and `futures-order`; account,
balance, and position stream event normalization remains unsupported.

Heartbeat policy is JSON ping/pong, 15 second ping interval, 30 second pong
timeout, and 30 second stale-message timeout. Auth renewal is reconnect and
re-auth; AscendEX does not use a listen-key lease in this adapter.

Batch place/cancel is native for Spot and futures, max 10 items per request,
exchange-defined non-atomic behavior, with partial failures reconciled item by
item through query/open-order reads.

## Validation

Offline request-spec/parser tests cover endpoint mapping declarations, external
request-spec fixtures, public Spot rules, public futures contracts, order book
snapshots, private balances, futures positions, signed order lifecycle requests,
fees, cancel-all, futures order/batch paths, query/open-order readbacks,
recent-fill fallback from order history, public/private WS subscription/auth
payloads, ping/pong payloads, public depth stream parsing, private order stream
parsing, private stream capability boundaries, unsupported private behavior
without credentials, error classification, empty parser response, missing-field
parser error, secret-free signed request paths/query/body, and HMAC signing
fixture vectors.

Validation commands attempted:

- `cargo fmt --all`
- `CARGO_TARGET_DIR=/tmp/rustcta_ascendex_target cargo test -p rustcta-exchange-gateway ascendex --lib -- --nocapture`
- `CARGO_TARGET_DIR=/tmp/rustcta_gateway_app_target cargo test -p rustcta-gateway config_should_ -- --nocapture`
- `cargo check -p rustcta-exchange-gateway --lib`

Current result: targeted formatting passes, `cargo check -p rustcta-exchange-gateway --lib`
passes with existing warnings, the filtered AscendEX adapter test run passes the
AscendEX tests, and the gateway app config filter passes 9 tests.

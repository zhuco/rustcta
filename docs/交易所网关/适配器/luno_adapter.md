# Luno Gateway Adapter

Task 18 scope: `luno` is a multi-region Spot adapter. It keeps fiat quote
markets explicit and avoids payment, wallet-address and withdrawal surfaces.

## Official Sources

| Area | Source |
| --- | --- |
| REST and authentication reference | https://www.luno.com/en/developers/api |
| API reference alternate locale | https://www.luno.com/developers/api |

## Coverage

| Area | Status |
| --- | --- |
| Product line | Spot only |
| Public REST | `GET /api/1/tickers`, `GET /api/1/orderbook_top` parser and transport |
| Private REST | Balance, fee, order, cancel, query, open-order and fill endpoints are request-spec/fixture only |
| WebSocket | Pair-scoped market stream URL helper, API-key credential payload helper, order-book sequence policy and fixtures |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/luno/endpoint_mapping.yaml` |
| Fixtures | `tests/fixtures/exchanges/luno/` |
| Config example | `config/luno_gateway_example.yml`, disabled by default |

## Fiat And Region Boundary

Luno markets vary by customer region. The fixtures intentionally cover
representative fiat quote markets from the task: `XBTZAR`, `XBTMYR`, `XBTNGN`
and `XBTIDR`. The adapter normalizes Luno `XBT` to canonical `BTC` while
preserving the venue symbol for requests.

Before live use, operators must enable only markets that are available for the
account region. This task does not infer availability from a global pair list.

## Official Public WS Order Book Details

Luno market stream connects to `wss://ws.luno.com/api/1/stream/:pair` and starts
by sending API key credentials, so it is not an anonymous public market stream.
The server sends the current order book, then sends updates as quickly as
possible. Official docs do not give a fixed millisecond interval or depth
parameter. Updates carry strict sequence numbers; any out-of-sequence update
requires closing the connection and reconnecting to reinitialise state.

| Field | Luno public order-book WS |
| --- | --- |
| URL | `wss://ws.luno.com/api/1/stream/{pair}`; example `XBTZAR` |
| Auth | Required initial JSON payload with `api_key_id` and `api_key_secret` |
| Initial state | Full current order book with `sequence`, `asks`, `bids`, `status`, `timestamp`; levels are order-id based |
| Updates | Atomic messages with `sequence`, `trade_updates`, `create_update`, `delete_update`, `status_update`, `timestamp` |
| Interval | Unknown fixed interval; official text says updates are sent as quickly as possible |
| Depth | `depth: unspecified`; no documented fixed depth; initial message is current full order book state |
| Sequence | Strict `sequence`; update `n` applies to local state `n-1` |
| Checksum | None documented; checksum risk remains |
| Reconnect/resync | Close and reconnect on gap/regression/out-of-order, parse error, network error or keepalive timeout; REST `GET /api/1/orderbook` can provide fallback snapshot state |

Sanitized fixtures:

| Fixture | Purpose |
| --- | --- |
| `tests/fixtures/exchanges/luno/ws/market_stream_initial_orderbook.json` | Credential-gated first full book state |
| `tests/fixtures/exchanges/luno/ws/market_stream_update.json` | Contiguous update and reconnect-on-gap policy evidence |

## Unsupported Funding And Payment Surfaces

Standard futures, perpetuals, options, and margin trading are
`交易所不支持合约` under the current official API scope.

The adapter does not implement beneficiaries, fiat withdrawals, fiat deposits,
bank payment rails, crypto address creation or send/transfer endpoints. Those
features remain documented boundaries and are excluded from runtime capabilities.

## Authentication

Luno private REST uses HTTP Basic authentication with the key id as the username
and the key secret as the password. The offline signing fixture is
`tests/fixtures/exchanges/luno/signing_vectors/basic_auth.json`.

## Runtime Boundary

Public REST is implemented for symbol rules and top-of-book snapshots. Private
read-only order/fill REST is implemented for `query_order`, `get_open_orders`,
and `get_recent_fills`; those methods fail closed unless
`LUNO_PRIVATE_REST_ENABLED` and Basic-auth credentials are present.

Private write surfaces remain offline request-spec only. `place_order` and
`cancel_order` still return explicit `Unsupported` errors because the adapter
does not have a non-live-write or dry-run-safe runtime path with post-write
reconciliation.

The public WS helper remains a policy/descriptor layer rather than a promoted
live stream consumer. Runtime promotion still needs credential lifecycle,
keepalive timeout handling, order-id level book application and reconnect
backoff. Shared checklist/template reconciliation should mark Luno P3 regional
spot public WS structured details as complete, but this task intentionally does
not edit those shared files.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/luno/endpoint_mapping.yaml
cargo test -p rustcta-exchange-gateway luno --lib --message-format short
cargo test -p rustcta-gateway luno --message-format short
```

Allowed broader checks after both Task 18 adapters:

```bash
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
```

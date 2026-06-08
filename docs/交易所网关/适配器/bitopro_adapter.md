# BitoPro Gateway Adapter

Status date: 2026-06-08

`bitopro` covers BitoPro Spot markets, with TWD quote markets treated as
first-class Spot pairs. Venue symbols are lowercase underscore pairs such as
`btc_twd`; public WebSocket paths use uppercase pairs such as `BTC_TWD`.

## Official Sources

| Area | Source |
| --- | --- |
| API overview | <https://www.bitopro.com/ns/api-docs> |
| REST docs | <https://github.com/bitoex/bitopro-official-api-docs/tree/master/api/v3> |
| Public WS docs | <https://github.com/bitoex/bitopro-official-api-docs/tree/master/ws/public> |
| Private WS docs | <https://github.com/bitoex/bitopro-official-api-docs/tree/master/ws/private> |

## Base URLs

| Surface | URL |
| --- | --- |
| REST v3 | `https://api.bitopro.com/v3` |
| WebSocket | `wss://stream.bitopro.com:443/ws` |
| Testnet | Not found in reviewed official docs |

## Authentication

Private REST and private WebSocket handshakes use:

- `X-BITOPRO-APIKEY`
- `X-BITOPRO-PAYLOAD`
- `X-BITOPRO-SIGNATURE`

`X-BITOPRO-PAYLOAD` is Base64-encoded JSON. For `GET` and `DELETE`, the JSON is
`{"identity":"USER_EMAIL","nonce":TIMESTAMP}`. For `POST`, the JSON is the
request body. The signature is lowercase hex HMAC-SHA384 over the Base64 payload
using the API secret.

The adapter therefore needs an API identity in addition to key and secret for
private read paths.

## Coverage

| Area | Status |
| --- | --- |
| Public REST | `get_symbol_rules` via `/provisioning/trading-pairs`; `get_order_book` via `/order-book/{pair}` |
| Private read REST | Balances, query order, open orders, and recent fills are signed and parser-tested |
| Private write REST | Place, cancel, cancel-all, batch place, and batch cancel are request-spec/signing fixtures only; runtime returns `Unsupported` |
| WebSocket | URL-based public stream specs, private handshake auth header specs, heartbeat policy, parser fixtures |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/bitopro/endpoint_mapping.yaml` |
| Fixtures | `tests/fixtures/exchanges/bitopro/` |
| Config | `config/bitopro_gateway_example.yml`, disabled by default |

## Rate Limits

BitoPro documents general public and auth limits of 600 requests per minute.
Endpoint-specific limits are reflected in the endpoint mapping, including
create order 1200/min/IP and UID, cancel one 900/min/IP and UID, open orders
5/s/IP and UID, batch create 90/min/IP and UID, batch cancel 2/s/IP, and
cancel-all 2/s/IP and UID.

## Unsupported Boundary

The adapter does not expose perpetuals, futures, margin, leverage, positions,
funding, mark price, open interest, risk tiers, wallet deposits, withdrawals,
transfers, order amend, order lists, or dead-man/cancel-all-after.

Private writes are deliberately limited to offline request-spec/signing
verification. They must not be promoted to live trading without a separate
credentialed live-dry-run task.

## Validation

Allowed commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitopro/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bitopro --lib --message-format short
cargo test -p rustcta-gateway bitopro --message-format short
```

Do not run `cargo build`, release builds, app/web builds, or live order actions
as part of this adapter task.

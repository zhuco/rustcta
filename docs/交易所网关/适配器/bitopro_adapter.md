# BitoPro Gateway Adapter

Status date: 2026-06-09

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
| Private write REST | Batch place, batch cancel, and cancel-all have signed runtime with mock/parser coverage behind explicit private REST credentials; single place/cancel remain request-spec/signing fixtures only |
| WebSocket | URL-based public stream specs, private handshake auth header specs, heartbeat policy, parser fixtures |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/bitopro/endpoint_mapping.yaml` |
| Fixtures | `tests/fixtures/exchanges/bitopro/` |
| Config | `config/bitopro_gateway_example.yml`, disabled by default |

## Official Public WS Order Book Details

Official public order book stream uses
`wss://stream.bitopro.com:443/ws/v1/pub/order-books/{PAIR}:{limit}`. It pushes
the full order book every second when updated. Default limit is 5; valid limits
are 1, 5, 10, 20, 30, and 50. Messages include `eventID` and timestamp, but the
reviewed official docs do not expose a sequence/checksum suitable for delta book
continuity.

| Feed | Product | URL / request | Interval | Limits | Semantics | Sequence / checksum | Reconnect / resync |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Public order book | Spot | URL path subscription: `/v1/pub/order-books/{PAIR}:{limit}`; multiple pairs use `pairs=` query | 1s when book changed | Default 5; supported 1/5/10/20/30/50 | Snapshot-only full order book; no delta stream | No continuity sequence or checksum; `eventID` is not used as a book sequence | Fetch REST `/order-book/{pair}` snapshot, then reconnect/resubscribe |
| Public trades | Spot | `/v1/pub/trades/{PAIR}` | Venue-driven | Not applicable | Trade stream only | Not an order-book continuity source | Reconnect/resubscribe |
| Public ticker | Spot | `/v1/pub/tickers/{PAIR}` | Venue-driven | Not applicable | Ticker stream only | Not an order-book continuity source | Reconnect/resubscribe |

The adapter exposes the public order book as `OrderBookSnapshot` only. It
rejects `OrderBookDelta` because the official stream publishes complete books
without replayable sequence/checksum fields. For local recovery, consumers
should discard the old local book and reload REST `get_order_book` before
resuming the WS snapshot feed.

## Rate Limits

BitoPro documents general public and auth limits of 600 requests per minute.
Endpoint-specific limits are reflected in the endpoint mapping, including
create order 1200/min/IP and UID, cancel one 900/min/IP and UID, open orders
5/s/IP and UID, batch create 90/min/IP and UID, batch cancel 2/s/IP, and
cancel-all 2/s/IP and UID.

## Unsupported Boundary

Current official Open API scope does not show standard futures, perpetuals, or
options. This adapter therefore writes standard contract trading as
`交易所不支持合约`.

费率项目未实现/未启用：BitoPro public limitations-and-fees/VIP schedule 已作为离线配置源记录到 `tests/fixtures/exchanges/bitopro/request_specs/get_fees_source_boundary.json`，适用产品线为 Spot。该边界只能作为 fee table/config source；生产账户有效费率仍需 VIP level config、pair scope、fee table version 或明确 account override，不能用零费率占位替代。shared `get_fees` runtime 仍未启用，剩 VIP/config loader、maker/taker parser 和 `FeeRateSnapshot` 映射。

The adapter does not expose margin, leverage, positions, funding, mark price,
open interest, risk tiers, wallet deposits, withdrawals, transfers, order amend,
order lists, or dead-man/cancel-all-after.

Batch place (`POST /orders/batch`), batch cancel (`PUT /orders`), and
cancel-all (`DELETE /orders/all` or pair-scoped `DELETE /orders/{pair}`) are now
exposed through the shared runtime when private REST credentials and API
identity are explicitly configured. Focused tests cover signed request bodies or
DELETE headers, success response parsing, and fallback ack/cancelled states
without live exchange writes.

Advanced-order boundary: native batch place (`POST /orders/batch`), batch
cancel (`PUT /orders`), and cancel-all (`DELETE /orders/all` or
`DELETE /orders/{pair}`) are runtime-backed behind the private REST guard.
In-place amend and OCO/order-list semantics are explicitly unsupported because
no equivalent BitoPro v3 endpoint was found in the reviewed official docs.

Remaining public WS boundaries: no order-book delta runtime, no sequence-gap
detector, no checksum validation, and no high-frequency/BBO order-book feed
beyond the 1s snapshot stream documented above.

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

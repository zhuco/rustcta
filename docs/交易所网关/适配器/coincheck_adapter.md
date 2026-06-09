# Coincheck Gateway Adapter

Status: Task 15 offline adapter, 2026-06-08.

`coincheck` is a Japan spot adapter for Coincheck Exchange API trading
surfaces.

## Scope

- REST base URL: `https://coincheck.com`.
- WebSocket URL: `wss://ws-api.coincheck.com/`.
- Market type: Spot only.
- Symbols: lowercase underscore pairs such as `btc_jpy`.

## Authentication

Private REST uses `ACCESS-KEY`, `ACCESS-NONCE`, and `ACCESS-SIGNATURE`.
The signature payload is `nonce + full_request_url + request_body`, signed
with HMAC-SHA256 and hex encoded.

## Implemented Offline Surface

- Public REST: conservative static pair rules and `/api/order_books` snapshots.
- Private REST: balances, place order, quote-sized market buy, cancel order,
  open orders, and recent transaction fills.
- Batch place/cancel are gateway-composed sequential calls.
- Public WebSocket subscribe/unsubscribe payloads for `{pair}-trades` and
  `{pair}-orderbook`.
- Private order/fill synchronization uses REST reconciliation.

## Official Public WS Order Book Details

Coincheck public WebSocket uses `wss://ws-api.coincheck.com` and subscribes with
`{"type":"subscribe","channel":"btc_jpy-orderbook"}`. Official docs say public
WS data is pushed approximately every 0.1 seconds when a trade occurs, and the
order book channel sends order book differences. The reviewed docs do not expose
a fixed depth parameter, sequence, or checksum; runtime should periodically
resync from REST and apply stale-book checks.

| Channel | Status | Subscription | Interval | Depth | Sequence/checksum | Rebuild |
| --- | --- | --- | --- | --- | --- | --- |
| `{pair}-orderbook` | Native public WS payload helper | JSON `subscribe` / `unsubscribe` | Approx. 0.1s when trades occur; not a fixed heartbeat cadence | Not documented | No sequence or checksum documented | Start from `GET /api/order_books`; rebuild after reconnect, stale stream, parse error, or suspected message loss |
| `{pair}-trades` | Native public WS payload helper | JSON `subscribe` / `unsubscribe` | Approx. 0.1s when trades occur | N/A | N/A | N/A |

The fixture `tests/fixtures/exchanges/coincheck/ws/public_orderbook_diff.json`
records the unsequenced order book difference shape and zero-amount delete
boundary used by local-book consumers.

## Unsupported Boundary

Standard futures, perpetuals, and options are `交易所不支持合约` under the current
official Exchange API scope.

Private WebSocket, single order query, cancel-all, amend order, fees, lending,
deposit, withdraw, bank transfer, margin, futures, and leverage are unsupported
in this runtime adapter.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/coincheck/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway coincheck --lib --message-format short
```

## Fee Boundary

交易所不支持当前费率接口 runtime：当前 profile 只记录 fees unsupported boundary。

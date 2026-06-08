# Bitvavo Gateway Adapter

Status date: 2026-06-08.

Bitvavo is implemented as a Spot-only gateway adapter for European/EUR markets. REST base URL defaults to `https://api.bitvavo.com/v2`; WebSocket base URL defaults to `wss://ws.bitvavo.com/v2`.

Contract/Futures/Options: 交易所不支持合约（当前官方 API 口径）。The official
Bitvavo API docs cover Exchange REST/WS/FIX, market data and institutional
account APIs, with no stable futures/perpetual/option trading interface found
in this verification batch.

## Implemented Surface

- Public REST: markets and order book snapshots.
- Private REST request construction: balances, account fees, place order, cancel order, cancel all, query order, open orders, fills.
- WebSocket specs: public `book` subscription, private authenticate payload, private `account` subscription payload.
- Signing: `Bitvavo-Access-*` headers with HMAC-SHA256 over `timestamp + method + path_with_query + body`; private WS auth signs `timestamp + GET + /websocket`.

## Public WebSocket Order Book

Official `book` subscription pushes order book changes and uses `nonce` as the ordered book version. Local book management should initialize from REST/WS `getBook` with a chosen depth, then require each `book` event nonce to be exactly previous nonce + 1; otherwise resubscribe and rebuild. The reviewed official docs do not publish a fixed millisecond interval or checksum.

## Capability Matrix

| Surface | Runtime |
| --- | --- |
| Products | Spot only; contract/futures/options 交易所不支持 |
| Public REST | markets, order book snapshot |
| Private REST | balances, account fees, order lifecycle, open orders, recent fills |
| Public WS | native `book` subscription payload |
| Private WS | native auth plus `account` subscription payload |
| Order types | market, limit, post-only |
| Batch | not advertised; native batch endpoint is not mapped |
| Positions/reduce-only/amend | Unsupported |

## Endpoint Mapping

`crates/rustcta-exchange-gateway/src/adapters/bitvavo/endpoint_mapping.yaml` maps:

- `GET /markets`
- `GET /{market}/book`
- `GET /balance`
- `GET /account/fees`
- `POST /order`
- `DELETE /order`
- `DELETE /orders`
- `GET /order`
- `GET /ordersOpen`
- `GET /trades`

Private REST operations require request specs under `tests/fixtures/exchanges/bitvavo/request_specs/`.

## Rate Limits

The adapter tags public REST as `rest_ip`, private account reads as `key`, and order writes/cancels as `orders` in the endpoint mapping. Runtime does not implement a separate local throttler; callers should enforce Bitvavo's current published rate limits outside this adapter.

## Unsupported Boundary

Fiat payment methods, bank deposits, withdrawals, transfers, staking, ledger-writing funding operations, FIX runtime, and institutional account management are not connected to runtime. Contract/futures/options are recorded as `交易所不支持合约` under the current official API surface. Amend order remains `Unsupported` because the shared request lacks Bitvavo `operatorId` semantics. The executable boundary fixture is `tests/fixtures/exchanges/bitvavo/unsupported_boundary.json`.

## Official References

| Topic | URL |
| --- | --- |
| REST API | `https://docs.bitvavo.com/docs/rest-api/` |
| WebSocket API | `https://docs.bitvavo.com/docs/websocket-api/` |
| Authentication | `https://docs.bitvavo.com/docs/authentication/` |

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitvavo/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bitvavo --lib --message-format short
```

`cargo build` is intentionally not part of Task 14 validation.

# Coinmate Adapter

Task A-16 adds a conservative Coinmate gateway adapter for European spot markets.

## Scope

- Products: spot only.
- Native symbol format: underscore pairs, for example `BTC_EUR` and `BTC_CZK`.
- REST base URL: `https://coinmate.io/api`.
- WebSocket v2 URL: `wss://coinmate.io/api/websocket`.
- Public REST is implemented for trading-pair rules and order-book snapshots.
- Private REST readbacks for `query_order`, `get_open_orders`, and `get_recent_fills` are implemented behind `COINMATE_PRIVATE_REST_ENABLED` plus `clientId`/`publicKey`/private key credentials.
- Private REST writes, balances, fees, and private WebSocket remain offline request-spec/payload fixtures only.

## Official API Notes

- Official REST documentation: <https://coinmate.docs.apiary.io/>.
- Official API examples: <https://github.com/coinmate-io/coinmate-api-examples>.
- Public WebSocket demo: <https://coinmate.io/static/api_demo/pushpin/api/public.html>.
- Private WebSocket demo: <https://coinmate.io/static/api_demo/pushpin/api/private.html>.
- Private REST auth uses `clientId`, optional `publicKey`, `nonce`, and uppercase hex HMAC-SHA256 signature over `nonce + clientId + publicKey`.
- Nonces must be strictly increasing per key pair, so parallel private calls require a serialized nonce source.
- REST limit is documented as 100 requests per minute, with IP-ban risk if exceeded.

## Endpoint Coverage

- Public rules: `GET /tradingPairs`.
- Public order book: `GET /orderBook?currencyPair=BTC_EUR&groupByPriceLimit=false`.
- Private read runtime: `/openOrders`, `/orderById`, `/tradeHistory`.
- Private read request-specs only: `/balances`, `/traderFees`.
- Private write request-specs: `/buyLimit`, `/sellLimit`, `/cancelOrder`, `/cancelAllOpenOrders`, `/replaceByBuyLimit`, `/replaceBySellLimit`.
- WebSocket payload helpers cover public `trades-{PAIR}`, `order_book-{PAIR}`, `statistics-{PAIR}` and private user channel auth shapes.

## Public WebSocket Order Book

Coinmate public WebSocket v2 exposes `order_book-{PAIR}` through the documented
pushpin demo shape. The reviewed official docs do not publish a fixed cadence,
depth selector, sequence field, or checksum. The adapter therefore treats this
as a payload/parser surface with REST snapshot recovery, not as a strictly
sequenced local-book runtime.

| Channel | Status | Cadence | Depth | Sequence/checksum | Rebuild |
| --- | --- | --- | --- | --- | --- |
| `order_book-{PAIR}` | Payload/parser ready | No fixed ms documented | No fixed depth or selector documented | No official sequence or checksum | Reconnect/resubscribe and rebuild from REST `GET /orderBook?currencyPair={PAIR}` |

Fixture `tests/fixtures/exchanges/coinmate/ws/public_order_book.json` covers
the order-book payload shape.

## Unsupported Boundary

- Futures, perpetuals, options, margin, leverage, positions, funding, mark price, open interest, and risk tiers are `交易所不支持合约` under the current official API/product scope.
- Withdrawals, transfers, bank-wire withdrawal, deposit addresses, and fiat movement are outside the trading gateway.
- Stop-loss and hidden order parameters are unsupported because the official docs say those fields were removed/disabled.
- `place_order`, `cancel_order`, `cancelAllOpenOrders`, balances, and fees remain offline/spec-only in shared runtime for this pass.
- `amend_order` has offline request-spec and parser coverage through Coinmate buy/sell replace-limit routes. Shared runtime still returns `coinmate.replace_order_offline_requires_side_price_nonce_and_reconciliation_guard` because `AmendOrderRequest` does not carry replacement side/price, and live writes still need serialized nonce, dry-run guard, and REST/WS reconciliation.
- Native batch place, list batch cancel, and OCO/OTO order-list are unsupported; `cancelAllOpenOrders` is treated as native cancel-all only.
- WebSocket order book has no documented sequence/checksum contract, so REST order-book snapshot is the reconciliation fallback.
- Official public WebSocket exposes `order_book-{PAIR}` through the Coinmate pushpin demo, but no fixed push interval or depth parameter was found in the reviewed docs.

官方核验见 [产品线官方核验 P5 区域现货 CEX 第二批](../产品线官方核验_P5_区域现货_CEX第二批.md)。

## Verification

Allowed checks:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/coinmate/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway coinmate --lib --message-format short
cargo test -p rustcta-gateway coinmate --message-format short
```

Do not run `cargo build`.

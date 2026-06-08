# Coinmate Adapter

Task A-16 adds a conservative Coinmate gateway adapter for European spot markets.

## Scope

- Products: spot only.
- Native symbol format: underscore pairs, for example `BTC_EUR` and `BTC_CZK`.
- REST base URL: `https://coinmate.io/api`.
- WebSocket v2 URL: `wss://coinmate.io/api/websocket`.
- Public REST is implemented for trading-pair rules and order-book snapshots.
- Private REST and private WebSocket are offline request-spec/payload fixtures only.

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
- Private read request-specs: `/balances`, `/traderFees`, `/openOrders`, `/orderById`, `/tradeHistory`.
- Private write request-specs: `/buyLimit`, `/sellLimit`, `/cancelOrder`, `/cancelAllOpenOrders`, `/replaceByBuyLimit`.
- WebSocket payload helpers cover public `trades-{PAIR}`, `order_book-{PAIR}`, `statistics-{PAIR}` and private user channel auth shapes.

## Unsupported Boundary

- Futures, perpetuals, options, margin, leverage, positions, funding, mark price, open interest, and risk tiers are `交易所不支持合约` under the current official API/product scope.
- Withdrawals, transfers, bank-wire withdrawal, deposit addresses, and fiat movement are outside the trading gateway.
- Stop-loss and hidden order parameters are unsupported because the official docs say those fields were removed/disabled.
- Native batch place and list batch cancel are unsupported; `cancelAllOpenOrders` is treated as native cancel-all only.
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

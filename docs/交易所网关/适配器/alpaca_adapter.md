# Alpaca Gateway Adapter

Alpaca is wired as a Broker crypto spot adapter. It targets Alpaca Broker REST for account and order operations and Alpaca crypto market-data REST/WebSocket for public order-book data.

## Scope

- Exchange id: `alpaca`
- Market type: spot crypto only
- Default Broker REST: `https://broker-api.sandbox.alpaca.markets`
- Default market-data REST: `https://data.sandbox.alpaca.markets`
- Default market-data WebSocket: `wss://stream.data.sandbox.alpaca.markets/v1beta3/crypto/us`
- Auth: APCA key/secret headers. The adapter does not sign requests with HMAC.

## Supported Gateway Operations

- `get_symbol_rules`: Broker `/v1/assets?asset_class=crypto&status=active`
- `get_order_book`: market-data `/v1beta3/crypto/{loc}/latest/orderbooks`
- `get_balances`: Broker trading account readback
- `get_positions`: Broker crypto positions
- `place_order`: crypto market/limit/stop-limit boundaries mapped to Broker order create
- `place_quote_market_order`: Broker market order with `notional`
- `cancel_order`, `cancel_all_orders`, `query_order`, `get_open_orders`
- Public WebSocket auth, subscribe, unsubscribe, and order-book parser fixtures

## Boundaries

- Private order updates are not exposed as WebSocket. Alpaca Broker documents trade events as SSE at `/v2/events/trades`, so this adapter marks private streams unsupported.
- Alpaca crypto market-data WebSocket has no adapter-level heartbeat contract; reconnects should resync through REST order-book snapshots.
- Symbol-scoped cancel-all is unsupported because the Broker cancel-all endpoint is account-scoped.
- Recent fills remain unsupported until the account-activity pagination and filter semantics are mapped losslessly.
- Crypto margin, shorting, and leverage are unsupported.
- Public market data requires valid Alpaca credentials.

## Environment

Use `RUSTCTA_GATEWAY_ADAPTERS=alpaca` to load the adapter through `apps/gateway`.

Required for public market data and private Broker REST:

- `RUSTCTA_ALPACA_API_KEY`
- `RUSTCTA_ALPACA_API_SECRET`

Optional REST overrides:

- `RUSTCTA_ALPACA_REST_BASE_URL`: applies to both Broker and market-data REST
- `RUSTCTA_ALPACA_BROKER_REST_BASE_URL`: Broker REST only
- `RUSTCTA_ALPACA_MARKET_DATA_REST_BASE_URL`: market-data REST only

Requests that operate on a Broker account require `RequestContext.account_id`.

## Validation

Use the task-approved checks only:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/alpaca/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway alpaca --lib --message-format short
cargo test -p rustcta-gateway alpaca --message-format short
```

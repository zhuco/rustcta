# Indodax Adapter

Scope: spot IDR markets only.

Implemented public REST:

- `GET /api/pairs` for IDR symbol rules.
- `GET /api/{pair}/depth` for order book snapshots.

Implemented private REST:

- `POST /tapi` with `method=getInfo` for balances.
- `POST /tapi` with `method=trade` for spot market/limit orders.
- `POST /tapi` with `method=cancelOrder` for single-order cancel by exchange order ID.
- `POST /tapi` with `method=getOrder`, `openOrders`, and `tradeHistory` for order/fill parsers.

Signing:

- Private requests are form-encoded.
- Headers are `Key` and `Sign`.
- `Sign` is lowercase hex HMAC-SHA512 over the exact form body containing `method`, `nonce`, and operation parameters.

Boundaries:

- Runtime support is limited to `MarketType::Spot`.
- Public/private streams are unsupported; the adapter is REST-only.
- Client order IDs, amend orders, order lists, cancel-all, positions, leverage/margin, reduce-only, post-only, stop orders, IOC/FOK, transfers, deposits, withdrawals, fiat banking, and payment APIs are explicitly unsupported.
- Fiat IDR is treated only as a spot quote asset in balances/orders; fiat payment and withdrawal flows are not represented in runtime APIs.
- Generic cancel and batch-cancel are unsupported because Indodax `cancelOrder` requires buy/sell side context that `CancelOrderRequest` does not carry.

Validation:

- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/indodax/endpoint_mapping.yaml`
- `python3 -m json.tool tests/fixtures/exchanges/indodax/...`
- `cargo test -p rustcta-exchange-gateway indodax --lib --message-format short` once unrelated parallel adapters compile.

# Bitfinex Gateway Adapter

Status: `rustcta-exchange-gateway` Bitfinex V2 adapter for Spot, margin trading and derivatives/perpetual boundaries. Private order writes are offline request-spec covered; margin funding, withdrawals and transfers stay outside trading runtime.

## Scope

- Adapter id: `bitfinex`
- Public REST: `https://api-pub.bitfinex.com`
- Private REST: `https://api.bitfinex.com`
- Public WebSocket: `wss://api-pub.bitfinex.com/ws/2`
- Private WebSocket: `wss://api.bitfinex.com/ws/2`
- Market types: `Spot`, `Margin`, `Perpetual`
- Symbols: Bitfinex exchange symbols are prefixed trading symbols such as `tBTCUSD`; derivative examples include `tBTCF0:USTF0`.
- Sandbox: no stable public sandbox URL is configured by default.

Official docs reviewed on 2026-06-08:

- REST auth: https://docs.bitfinex.com/docs/rest-auth
- REST configs: https://docs.bitfinex.com/reference/rest-public-conf
- REST book: https://docs.bitfinex.com/reference/rest-public-book
- Orders: https://docs.bitfinex.com/reference/rest-auth-submit-order, https://docs.bitfinex.com/reference/rest-auth-cancel-order, https://docs.bitfinex.com/reference/rest-auth-cancel-orders-multiple
- Wallets/positions: https://docs.bitfinex.com/reference/rest-auth-wallets, https://docs.bitfinex.com/reference/rest-auth-positions
- Derivatives boundary: https://docs.bitfinex.com/docs/derivatives
- WS auth: https://docs.bitfinex.com/docs/ws-auth

## Endpoint Mapping

The machine-readable mapping lives at `crates/rustcta-exchange-gateway/src/adapters/bitfinex/endpoint_mapping.yaml`.
The runtime `capabilities_v2` surface now mirrors this mapping for public/private REST, stream fallback policy, order/fill history, native batch cancel and cancel-all.

| Gateway capability | Bitfinex endpoint | Status |
| --- | --- | --- |
| Spot symbols | `GET /v2/conf/pub:list:pair:exchange` | Native parser |
| Margin symbols | `GET /v2/conf/pub:list:pair:margin` | Native parser |
| Derivatives symbols | `GET /v2/conf/pub:list:pair:futures` | Native parser, mapped to `Perpetual` |
| Order book | `GET /v2/book/{symbol}/P0?len=N` | Native snapshot parser |
| Balances | `POST /v2/auth/r/wallets` | Signed REST |
| Positions | `POST /v2/auth/r/positions` | Margin/perpetual signed REST |
| Place order | `POST /v2/auth/w/order/submit` | Signed REST for Spot/margin/perp order lifecycle |
| Cancel order | `POST /v2/auth/w/order/cancel` | Exchange-order-id runtime path |
| Batch cancel/cancel all | `POST /v2/auth/w/order/cancel/multi` | Native cancel multi/all; partial results require reconciliation |
| Query/open orders | `POST /v2/auth/r/orders`, `POST /v2/auth/r/orders/{symbol}` | Signed REST |
| Recent fills | `POST /v2/auth/r/trades/{symbol}/hist` | Signed REST |
| Public WS | `book`, `trades`, `ticker`, `candles` subscriptions | Payload/parser helpers |
| Private WS | `auth` channel 0 account info | Auth payload, orders/wallets/positions parser helpers |

## Signing

Private REST uses the Bitfinex V2 headers:

```text
bfx-nonce: increasing numeric nonce
bfx-apikey: API key
bfx-signature: HMAC-SHA384_HEX("/api" + path + nonce + compact_json_body, api_secret)
content-type: application/json
```

Private WebSocket auth signs `AUTH{nonce}` with HMAC-SHA384 and sends `event=auth`, `apiKey`, `authNonce`, `authPayload`, and `authSig`.

## WebSocket Policy

Public sockets are spec/parser ready. The adapter emits subscribe/unsubscribe payloads and treats Bitfinex `hb` messages as heartbeat. Order-book resync uses REST snapshots after reconnect or sequence gaps.

Private sockets authenticate once per connection. Channel 0 account events can carry orders, trades, wallets and positions. The runtime policy is REST reconciliation after reconnect before trusting private stream state.

## Unsupported Boundaries

- Margin loans, funding offers, funding credits/loans and funding books are `Unsupported` for trading runtime.
- Withdrawals and wallet transfers/conversions are `Unsupported`; derivatives wallet funding must be handled outside this adapter runtime.
- Client-id cancel/query is `Unsupported` unless a future shared request adds Bitfinex `cid_date`.
- Amend order is `Unsupported` in the shared trait because safe Bitfinex update mapping needs side and verified amount semantics.
- Batch place/order multi-op remains `Unsupported` until partial response semantics are mapped to the shared batch report model.
- Quote-sized market orders are `Unsupported`; Bitfinex order submit uses signed base amount.

## Fixtures And Validation

Fixtures live under `tests/fixtures/exchanges/bitfinex/` and cover public parser payloads, private REST request specs, deterministic HMAC-SHA384 signing vectors, and WS auth/book/order samples.

Recommended targeted validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitfinex/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bitfinex --lib --message-format short
```

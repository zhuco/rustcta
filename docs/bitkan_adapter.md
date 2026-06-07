# BitKan Gateway Adapter

Status date: 2026-06-07

Adapter id: `bitkan`

Implementation status: conservative gateway registration is implemented behind
`rustcta_exchange_api::ExchangeClient`. The adapter is intentionally disabled
for unverified REST and real WebSocket trading surfaces until a stable official
BitKan OpenAPI/signing/channel specification is available for implementation
and request-spec tests.

Current code covers:

- `bitkan` named adapter registration and `BitkanGatewayConfig` export.
- `apps/gateway` wiring for `RUSTCTA_BITKAN_REST_BASE_URL`,
  `RUSTCTA_BITKAN_API_KEY`, and `RUSTCTA_BITKAN_API_SECRET`.
- Spot + perpetual product-scope declaration.
- Public WebSocket subscription payload helper and heartbeat helper.
- Explicit `Unsupported` responses for every unverified public/private REST,
  batch, cancel-all, query/fill, and private WebSocket capability.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Product scope declared; REST/WS trading capabilities remain `Unsupported` until official endpoints are verified. |
| Contract/perpetual | `Perpetual` | Product scope declared; public/private REST and WS trading capabilities remain `Unsupported` until official endpoints are verified. |
| Testnet | n/a | Unsupported; no stable public sandbox host verified. |

Default REST base URL: `https://bitkan.com`
Default public WS URL: `wss://bitkan.com/ws`

Both URLs are placeholders for configuration continuity, not claims that the
current adapter can use those hosts for production trading. Override them only
after official API documentation has been verified.

## Authentication

The gateway can read:

- `RUSTCTA_BITKAN_API_KEY` or `BITKAN_API_KEY`
- `RUSTCTA_BITKAN_API_SECRET` or `BITKAN_API_SECRET`

Private REST remains disabled by default and the `apps/gateway` config builder
keeps `enabled_private_rest = false` even when credentials are present. This is
intentional: no official request signing scheme has been verified for this
adapter.

## Endpoint Mapping

| Standard capability | BitKan endpoint/spec | Current implementation |
| --- | --- | --- |
| Spot symbol rules | Not verified | `Unsupported("bitkan.symbol_rules_unverified")` |
| Perpetual symbol rules | Not verified | `Unsupported("bitkan.symbol_rules_unverified")` |
| Order book snapshot | Not verified | `Unsupported("bitkan.order_book_unverified")` |
| Balances | Not verified | `Unsupported("bitkan.balances_unverified")` |
| Positions | Not verified | `Unsupported("bitkan.positions_unverified")` |
| Fee rate | Not verified | `Unsupported("bitkan.fees_unverified")` |
| Place order | Not verified | `Unsupported("bitkan.place_order_unverified")` |
| Quote-sized market order | Not verified | `Unsupported("bitkan.quote_market_order_unverified")` |
| Cancel order | Not verified | `Unsupported("bitkan.cancel_order_unverified")` |
| Amend order | Not verified | `Unsupported("bitkan.amend_order_unverified")` |
| OCO/OTO order list | Not verified | `Unsupported("bitkan.order_list_unverified")` |
| Batch place | Not verified | `Unsupported("bitkan.batch_place_orders_unverified")` |
| Batch cancel | Not verified | `Unsupported("bitkan.batch_cancel_orders_unverified")` |
| Cancel all | Not verified | `Unsupported("bitkan.cancel_all_orders_unverified")` |
| Query/open orders | Not verified | `Unsupported("bitkan.query_order_unverified")`, `Unsupported("bitkan.open_orders_unverified")` |
| Recent fills | Not verified | `Unsupported("bitkan.recent_fills_unverified")` |
| Public WebSocket | Unverified helper only | Payload helper builds `{"op":"subscribe","channel":"..."}` but `subscribe_public_stream` returns `Unsupported("bitkan.public_streams_unverified")` unless explicitly enabled for future experiments. |
| Private WebSocket | Not verified | `Unsupported("bitkan.private_streams_unverified")` and unsupported private stream capabilities. |
| Heartbeat | Unverified helper only | Helper builds `{"op":"ping"}` and documents 30s ping / 45s pong timeout / 60s stale-message policy. |

## Capability Contract

Default `capabilities()` returns:

- `market_types = [Spot, Perpetual]`
- `supports_public_rest = false`
- `supports_private_rest = false`
- `supports_public_streams = false`
- `supports_private_streams = false`
- all order lifecycle, batch, cancel-all, fee, balance, position, fill, amend,
  quote-market, and order-list flags set to `false`

This keeps runtime routing honest while preserving a registered adapter shell
for later endpoint-by-endpoint upgrades.

## Upgrade Gate

Before enabling any BitKan trading feature, update this document with:

- official REST and WebSocket base URLs
- authentication headers/query/body signing string and timestamp semantics
- Spot market and order-book endpoints
- private balance/order/fill endpoints
- contract/perpetual market, position, and order endpoints
- native or composed batch place/cancel semantics
- WebSocket public/private channel names, auth payloads, sequence/checksum
  behavior, heartbeat protocol, and reconnect/resubscribe rules
- read-only live report proving account/order/fill endpoints do not submit
  orders

Do not implement against website-only paths or undocumented browser traffic.

## Task 30 Toolchain Status

- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/bitkan/endpoint_mapping.yaml`.
- Capabilities v2: `toolchain.rs` explicitly marks public REST, private REST, public WS, private WS, batch, cancel-all, order history and fills history as unsupported with unverified reasons.
- Fixtures: `tests/fixtures/exchanges/bitkan/` records unsupported boundary, empty response, error response and missing-field samples for future parser upgrades.
- Request-spec/signing: the signing-vector fixture is a negative boundary case because no official signing scheme has been verified; private REST remains disabled by default.
- WS policy: helper-only public subscribe/ping payloads are covered by tests, but live subscribe/private streams remain `Unsupported`.
- Rate-limit/pagination/reconciliation/batch: mapping records unsupported buckets, unsupported pagination and no reconciliation fallback. This adapter is not scan-only or trade-enabled.
- Live boundary: no BitKan capability may be promoted until official REST/WS endpoints, signing, heartbeat and read-only reconciliation are verified; keep the adapter behind the exchange kill-switch until that evidence exists.

## Validation

- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitkan/endpoint_mapping.yaml`
  passed.
- `rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/bitkan/*.rs crates/rustcta-exchange-gateway/src/adapters/mod.rs crates/rustcta-exchange-gateway/src/lib.rs apps/gateway/src/config.rs`
- `CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway bitkan --lib --message-format short`
  passed: 4 BitKan tests passed with 748 filtered out.
- `CARGO_TARGET_DIR=target/gateway-clean-check cargo check -p rustcta-exchange-gateway --lib --message-format short`
  passed with existing workspace warnings.
- `CARGO_TARGET_DIR=target/gateway-clean-check cargo check -p rustcta-gateway --message-format short`
  passed with existing workspace warnings.

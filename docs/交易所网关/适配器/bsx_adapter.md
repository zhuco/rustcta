# BSX Gateway Adapter

## Scope

- Adapter id: `bsx`
- Product line: Base perpetuals
- Current capability: public REST `products` and order book snapshot; private REST/WS and order writes are request-spec-only or Unsupported.
- Default trading posture: disabled. No live private write path is enabled.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Perpetual | `Perpetual` | Current adapter scope; public products and order-book snapshot are mapped. |
| Spot | n/a | 项目未实现 Spot。BSX 官方有 spot/perpetuals exchange 线索，当前 `bsx` adapter 只接 Base perpetuals，不能写成交易所不支持。 |

## Official Materials

| Area | URL | Notes |
| --- | --- | --- |
| REST overview | https://api-docs.bsx.exchange/reference/rest-overview | Production REST host `https://api.bsx.exchange` |
| Products | https://api-docs.bsx.exchange/reference/get-products | Product metadata source for `get_symbol_rules` |
| Product book | https://api-docs.bsx.exchange/reference/get-product-book | Public order book snapshot source |
| Order write | https://api-docs.bsx.exchange/reference/post-order | Requires account headers and EIP-712 order signature |
| WebSocket overview | https://api-docs.bsx.exchange/reference/websocket-overview | Production WS host `wss://ws.bsx.exchange/ws` |

## Endpoint Mapping

The authoritative mapping lives in
`crates/rustcta-exchange-gateway/src/adapters/bsx/endpoint_mapping.yaml`.

| Gateway operation | BSX endpoint | Support | Fixture |
| --- | --- | --- | --- |
| `get_symbol_rules` | `GET /products` | native public REST | `tests/fixtures/exchanges/bsx/products.json` |
| `get_order_book` | `GET /products/{product_id}/book` | native public REST | `tests/fixtures/exchanges/bsx/orderbook.json` |
| `get_open_orders` | `GET /orders` | request-spec-only | `tests/fixtures/exchanges/bsx/request_specs/open_orders_spec_only.json` |
| `place_order` | `POST /orders` | request-spec-only / Unsupported runtime | `tests/fixtures/exchanges/bsx/request_specs/place_order_unsupported.json` |
| `cancel_order` | `DELETE /orders/{order_id}` | request-spec-only / Unsupported runtime | `tests/fixtures/exchanges/bsx/request_specs/cancel_order_unsupported.json` |
| private write/batch/cancel-all | account scoped REST | Unsupported runtime | `tests/fixtures/exchanges/bsx/unsupported_boundary.json` |

## Authentication And Signing

Public REST is unauthenticated. Private/account endpoints use BSX account headers such as
`BSX-KEY`, `BSX-SIGNATURE` and `BSX-TIMESTAMP`; websocket auth fixtures model an HMAC-SHA256
payload of `api_key,timestamp_ns`.

Order writes require an EIP-712 order signature over BSX order fields. The adapter records the
boundary in `tests/fixtures/exchanges/bsx/signing_vectors/eip712_order_boundary.json` but does not
construct live order signatures or submit private writes.

## Capability Boundary

- Market type is restricted to `perpetual`.
- `supports_public_rest`, `supports_symbol_rules` and `supports_order_book_snapshot` are enabled
  when public REST is enabled.
- `supports_private_rest`, `supports_place_order`, `supports_cancel_order`,
  `supports_batch_place_order`, `supports_private_streams` and `supports_public_streams` remain
  false.
- Public websocket helpers generate fixture payloads only; no runtime connection/resync loop is
  enabled in this adapter.
- Balances, positions, fees, open orders, fills, query order, cancel and order writes require a
  follow-up account-permission and EIP-712/cancel semantics audit.

## Configuration

See `config/bsx_gateway_example.yml`. It is disabled by default and contains placeholder credential
keys only.

Relevant environment variables:

- `RUSTCTA_BSX_REST_BASE_URL`
- `RUSTCTA_BSX_PUBLIC_WS_URL`
- `RUSTCTA_BSX_API_KEY`
- `RUSTCTA_BSX_API_SECRET`
- `RUSTCTA_BSX_WALLET_ADDRESS`
- `RUSTCTA_BSX_SIGNER_ADDRESS`
- `RUSTCTA_BSX_PRIVATE_REST_ENABLED` defaults to false
- `RUSTCTA_BSX_PRIVATE_STREAMS_ENABLED` defaults to false

## Validation

Allowed validation commands for this task:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bsx/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bsx --lib --message-format short
cargo test -p rustcta-gateway bsx --message-format short
```

Do not run `cargo build` for this task.

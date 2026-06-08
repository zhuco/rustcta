# P2B Gateway Adapter

Status date: 2026-06-08

Adapter id: `p2b`

Implementation status: A-32 scan-only Spot adapter. Public REST support is
implemented for Spot market metadata and two-sided order-book snapshots. Private
REST, trading, batch, account readbacks, and WebSocket runtime are deliberately
kept `Unsupported` until read-only credentials and write request semantics are
validated beyond offline request specs.

## Official Materials

| Area | Source | Adapter use |
| --- | --- | --- |
| REST base URL | `https://api.p2pb2b.com` in the P2B API docs. | Default `rest_base_url`. |
| Public markets | `GET /api/v2/public/markets`. | `get_symbol_rules` parser fixture. |
| Public order book | `GET /api/v2/public/book?market=...&side=...&offset=0&limit=...`. | `get_order_book` issues one buy request and one sell request, then merges the two sides into a snapshot. |
| Private REST | `POST /api/v2/account/balances`, `/api/v2/order/new`, `/api/v2/order/cancel`, `/api/v2/orders`, `/api/v2/account/market_deals`. | Request-spec and signing fixtures only; runtime stays unsupported. |
| Auth | Private examples use `X-TXC-APIKEY`, `X-TXC-PAYLOAD`, and `X-TXC-SIGNATURE`. | `signing.rs` builds base64 JSON payload and HMAC-SHA512 hex signature from sanitized fixture keys. |
| WebSocket | P2B WSS public docs list `wss://apiws.p2pb2b.com/`, `server.ping`, and `depth.subscribe`. | 项目未实现公共 WS runtime；官方 depth limit 1-100，increment 每 1s，full flag 每 60s。 |

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST G1 for symbol rules and order book. |
| P2P | n/a | Unsupported; not a central exchange order-book/trading surface. |
| Perpetual/futures | n/a | `交易所不支持合约`; P6 official verification found only spot/P2P API surfaces. |
| Testnet | n/a | Unsupported; no stable public sandbox host verified. |

Default REST base URL: `https://api.p2pb2b.com`

## Endpoint Mapping

| Gateway capability | P2B endpoint | Current status |
| --- | --- | --- |
| Symbol rules | `GET /api/v2/public/markets` | Implemented for Spot parser fixtures. |
| Order book | `GET /api/v2/public/book` with `side=buy` and `side=sell` | Implemented for snapshot parser fixtures. |
| Balances | `POST /api/v2/account/balances` | Request-spec-only; runtime returns `Unsupported("p2b.balances_request_spec_only")`. |
| Place order | `POST /api/v2/order/new` | Request-spec-only; runtime returns `Unsupported("p2b.place_order_request_spec_only")`. |
| Cancel order | `POST /api/v2/order/cancel` | Request-spec-only; runtime returns `Unsupported("p2b.cancel_order_request_spec_only")`. |
| Open orders | `POST /api/v2/orders` | Request-spec-only REST reconciliation candidate. |
| Recent fills | `POST /api/v2/account/market_deals` | Request-spec-only REST reconciliation candidate. |
| Batch place/cancel | Not verified | Unsupported. |
| WebSocket | `wss://apiws.p2pb2b.com/` | Public/private streams unsupported in runtime; official public depth support is a `项目未实现公共 WS 行情` task. |

## Authentication

Private REST examples require a JSON body containing `request` and `nonce`.
P2B signs the base64-encoded JSON payload with HMAC-SHA512 and sends:

- `X-TXC-APIKEY`
- `X-TXC-PAYLOAD`
- `X-TXC-SIGNATURE`

Fixtures:

- signing vector: `tests/fixtures/exchanges/p2b/signing_vectors/private_headers.json`
- request specs: `tests/fixtures/exchanges/p2b/request_specs/*.json`

Private REST remains disabled by default even when credentials are present.
The adapter config can read `P2B_SPOT_API_KEY`/`P2B_SPOT_API_SECRET`,
`P2B_API_KEY`/`P2B_API_SECRET`, or `P2PB2B_API_KEY`/`P2PB2B_API_SECRET` for
offline request specs.

## Capability Boundary

Default `capabilities()` returns:

- `market_types = [Spot]`
- public REST, symbol rules, and order-book snapshots supported
- private REST, balances, fees, trading, cancel, batch, cancel-all, query,
  open orders, fills, public streams, and private streams unsupported
- `capabilities_v2.private_rest` unsupported with a request-spec-only reason

This adapter must not be promoted to live-dry-run until a separate validation
task proves private read-only endpoints with sanitized credentials and confirms
write request semantics without real order placement.

## Validation

Non-compiling validation for this task:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/p2b/endpoint_mapping.yaml
rustfmt --edition 2021 --check crates/rustcta-exchange-gateway/src/adapters/p2b/*.rs
```

Per the user request for A-32, do not run `cargo build` and do not run
compile-driving `cargo check` in this pass. When compilation is explicitly
allowed later, run:

```bash
cargo test -p rustcta-exchange-gateway p2b --lib --message-format short
cargo test -p rustcta-gateway p2b --message-format short
```

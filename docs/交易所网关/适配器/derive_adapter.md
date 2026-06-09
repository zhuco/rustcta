# Derive Gateway Adapter

Status: Task 8 offline-verifiable adapter for Derive options/perp DeFi scope.

## Scope

- Products: options and perpetuals.
- Spot: 项目未实现 Spot。Derive 官方文档说明平台支持 spot trading，当前 `derive` adapter 只接 options/perpetuals。
- JSON-RPC base URL: `https://api.derive.xyz`.
- WebSocket URL: `wss://api.derive.xyz/ws`.
- Public scope: instrument metadata, order book/ticker/trade channel payloads.
- Private scope: session/account model, balances/positions/open-orders/order request specs, and offline session-signing fixtures.

Spot 边界写入 `spot_product status: project_unimplemented`：当前只映射 Derive options/perpetual JSON-RPC。补 Spot 前需要 spot instrument filter、spot order book/ticker/trades、spot balances、spot create/cancel/query/open/fills lifecycle 和 parser fixtures；不要和 `derive_chain_perps` profile 混用。

## Signing And Account Model

Derive private methods use a wallet/subaccount/session-key model. The adapter keeps the model explicit:

- `wallet`: owner wallet identifier, configured only through env/config.
- `subaccount_id`: target trading subaccount.
- `session_key` and `session_secret`: scoped session credential placeholders.
- Private JSON-RPC methods such as `private/order`, `private/cancel`, `private/get_open_orders`, and `private/get_positions` require session-key authorization.

Fixtures use `fixture-subaccount`, `fixture-session-key`, and deterministic HMAC sentinel material. No real wallet, session token, or chain account fixture is committed.

## Endpoint Mapping

Machine-readable mapping:
`crates/rustcta-exchange-gateway/src/adapters/derive/endpoint_mapping.yaml`.

Covered operations:

- `symbol_rules`: JSON-RPC `public/get_all_instruments` / instruments metadata.
- `order_book`: public orderbook reference / JSON-RPC route.
- `balances`: private collateral/balance route boundary.
- `positions`: JSON-RPC `private/get_positions`.
- `place_order`: JSON-RPC `private/order`, offline request-spec only.
- `cancel_order`: JSON-RPC `private/cancel`, offline request-spec only.
- `amend_order`: JSON-RPC `private/replace`, offline request-spec/signing/source/parser boundary only.
- `open_orders`: JSON-RPC `private/get_open_orders`.

## Capability Boundary

The runtime adapter advertises public REST and public WS specs. Private reads are gated on a configured subaccount/session key. Private writes and private WebSocket runtime return explicit `Unsupported` until live-dry-run promotion validates session-key signing, permission levels, and REST reconciliation.

Options and perp metadata are adapter-specific in `tests/fixtures/exchanges/derive/instruments.json`; no shared trait was expanded.

WebSocket fixtures cover subscribe, unsubscribe, heartbeat ping/pong, login/auth boundary, and private order event parser samples. Private stream disconnects require JSON-RPC REST reconciliation over balances, positions, open orders, and trade history before live promotion.

Official public WS includes orderbook/ticker/trades channels at
`wss://api.derive.xyz/ws`, but the reviewed public docs do not provide a fixed
millisecond interval, fixed depth parameter, stable sequence field, or checksum
for orderbook reconstruction. Current project support is native/spec-level; the
mapping explicitly records no fixed ms, `depth: unspecified`/no fixed depth,
sequence/checksum risk, and JSON-RPC REST/reference orderbook snapshot as the
rebuild path. Source batch:
[WebSocket 官方核验 P5 衍生品/链上盘口细项](../WebSocket官方核验_P5_衍生品链上盘口细项.md).

| Channel | Status | Cadence/depth | Sequence/checksum | Rebuild |
| --- | --- | --- | --- | --- |
| `orderbook` | Native/spec-level public WS | no fixed ms; depth unspecified/no fixed depth | no stable sequence field or checksum documented | Use JSON-RPC `public/get_orderbook` snapshot/reference data, then resubscribe after reconnect, stale stream, parse error or suspected message loss |

Unsupported or deferred:

- Live order placement/cancel.
- Admin-only cancel-all promotion.
- `amend_order` is `project_unimplemented`: Derive documents `private/replace`,
  and `tests/fixtures/exchanges/derive/request_specs/amend_order_replace.json`
  pins the offline JSON-RPC request shape. The replace boundary is also backed by
  `tests/fixtures/exchanges/derive/signing_vectors/session_replace_hmac.json`,
  `tests/fixtures/exchanges/derive/request_specs/amend_order_replace_source_boundary.json`,
  and `tests/fixtures/exchanges/derive/parser/amend_order_replace_ack.json`.
  Shared amend runtime remains disabled because production session/JWT/Stark
  signing, full replace field mapping from the shared amend request, response
  reconciliation and live dry-run guard are not implemented.
- Native batch place/cancel and OCO/OTO/order-list remain explicit unsupported
  boundaries in `endpoint_mapping.yaml` until a lossless shared mapping is
  verified.
- Shared option Greeks/risk model.

## Validation

Allowed targeted commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/derive/endpoint_mapping.yaml
cargo test -p rustcta-exchange-gateway derive --lib --message-format short
```

## Fee Boundary

交易所不支持当前费率接口 runtime：fee 信息只是 adapter-specific metadata，未归一到 shared fee snapshot。

## P2 Product Line Boundary (2026-06-09)

`spot_product` is an official-source project boundary, not an exchange-unsupported row. Derive spot trading exists in the JSON-RPC API family, while this adapter is scoped to option/perpetual instruments, order book, and private session/account boundaries.

Do not promote Spot runtime from option/perpetual JSON-RPC handling or from `derive_chain_perps`. Promotion requires spot instrument filtering, spot order-book/ticker/trade public parsers, spot balances private specs, spot order lifecycle create/cancel/get-orders/trades private specs, session/JWT/Stark auth, product-scope guards, and reconciliation.

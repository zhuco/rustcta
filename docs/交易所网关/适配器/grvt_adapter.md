# GRVT Gateway Adapter

Status date: 2026-06-08

Adapter id: `grvt`

Implementation status: conservative Task 9 gateway registration with G0/G1
audit artifacts plus guarded P2 private readbacks. GRVT public market-data and
trading documentation is mature enough to map endpoints; `query_order`,
`get_open_orders`, and `get_recent_fills` are enabled only behind explicit
session-cookie/account runtime flags. Write, account, fee, position, and
authenticated WebSocket runtime remain disabled.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Perpetual | `Perpetual` | Declared product scope; public REST/WS endpoints are mapped as spec/parser-only. |
| Options | `Option` | Declared product scope for audit; option chain/greeks/order semantics are adapter-specific and not mapped into the shared trading trait. |
| Spot | n/a | 交易所不支持现货（当前 adapter/官方主线口径；如官方 spot 交易面上线需重核）。 |

Default mainnet URLs:

- Market data REST: `https://market-data.grvt.io/full`
- Trading REST: `https://trades.grvt.io/full`
- Auth: `https://edge.grvt.io/auth`
- Public WS: `wss://market-data.grvt.io/ws/full`
- Private WS: `wss://trades.grvt.io/ws/full`

Testnet URLs:

- Market data REST: `https://market-data.testnet.grvt.io/full`
- Trading REST: `https://trades.testnet.grvt.io/full`
- Auth: `https://edge.testnet.grvt.io/auth`

## Authentication

GRVT private REST/WS is not simple per-request HMAC. API-key login returns a
session cookie and `X-Grvt-Account-Id`; write flows additionally require
transaction/order signing with an EIP-712 or external signer boundary. The
gateway config can hold placeholder `api_key`, `session_cookie`, and
`account_id` fields. Private readback REST remains fail-closed unless
`GRVT_PRIVATE_REST_ENABLED` or `RUSTCTA_GRVT_PRIVATE_REST_ENABLED` is set and
session cookie plus account id are configured.

No wallet private key, real session cookie, account id, sub-account id, API key,
or order id is committed in fixtures.

## Endpoint Mapping

Machine-readable mapping:
`crates/rustcta-exchange-gateway/src/adapters/grvt/endpoint_mapping.yaml`.

Current runtime behavior:

- `symbol_rules`: `Unsupported("grvt.symbol_rules_session_spec_only")`
- `order_book`: `Unsupported("grvt.order_book_session_spec_only")`
- `positions`: `Unsupported("grvt.positions_requires_session_cookie_external_signer_subaccount_scope_parser_reconciliation")`
- `place_order`: `Unsupported("grvt.place_order_session_spec_only")`
- `query_order`: guarded session-cookie runtime for `POST /v1/order`
- `get_open_orders`: guarded session-cookie runtime for `POST /v1/open_orders`
- `get_recent_fills`: guarded session-cookie runtime for `POST /v1/fills`
- `bulk_orders`: offline request-spec boundary for `POST /v2/bulk_orders`; runtime remains `Unsupported("grvt.bulk_orders_requires_session_renewal_eip712_signer_partial_parser_and_dry_run_guard")`
- `amend_order` / `batch_place_orders` / `batch_cancel_orders`: 官方支持但项目未实现；mapping 指向 `spec_only` request-spec fixture，已补 fixture-only ack/partial parser 和 `capabilities_v2` runtime-disabled 边界，仍 pending EIP-712/external signer、session auth、write reconciliation 和 dry-run guard。
- public WS subscribe helper: JSON-RPC payload only
- private WS: `Unsupported("grvt.private_stream_session_spec_only")`

GRVT public order-book WS sequence rule is recorded in fixtures: snapshots use
sequence `0`; deltas are expected to increase by one, and gaps require
reconnect/resubscribe. This task does not claim production low-latency runtime.

## Official Core Trading Detail

官方核验见 [核心交易官方核验 P3 第四批](../核心交易官方核验_P3_第四批.md)。GRVT trading/auth 资料支持 session、EIP-712/external signer、create/cancel/bulk orders。

因此下单/撤单/全撤/改单/批量是 `官方支持，项目未实现/未启用`。`query_order`、`get_open_orders`、`get_recent_fills` 已提升为 guarded private REST readback runtime；`/v1/amend_order` 和 `/v2/bulk_orders` 已固定离线 request-spec、ack/partial parser fixture 与 `capabilities_v2` 边界，矩阵应写 `离线` 或 `项目未实现`，不代表共享 write runtime 已启用；补交易接口前必须完成 session-cookie auth、EIP-712/external signer runtime、create/cancel/cancel-all write reconciliation 和 live-dry-run guard。order-list/OCO 仍按交易所不支持边界记录。

## Official Position Detail

官方核验见 [仓位接口官方核验 P0 第一批](../仓位接口官方核验_P0_第一批.md)。GRVT API docs 有 positions request schema，trading/auth 资料使用 session/account 模型。

因此 GRVT 仓位接口写 `官方支持，已有离线 request-spec 边界，shared runtime 项目未实现/未启用`。当前已固定 `tests/fixtures/exchanges/grvt/request_specs/positions_session_spec_only.json`，覆盖 `POST /v1/positions`、session cookie、account id 和 sub-account id 的脱敏 source boundary。补仓位 runtime 前仍必须完成 session-cookie auth smoke、positions parser、account/sub-account scope、private WS/readback reconciliation 和 read-only runtime gate。

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。GRVT market data WS 支持 `v1.book.s` snapshot-style 和 `v1.book.d` delta-style orderbook。官方 selector 格式表示 500ms、50 档。

public WS endpoint 可用 `wss://market-data.grvt.io/ws/full` 或 `/ws/lite`。payload 有 `sequence_number` 和 `prev_sequence_number`，未见 checksum；断档要重新订阅 `v1.book.s` snapshot/`v1.book.d` delta，必要时回 REST market data snapshot。

## Unsupported Boundary

The following are explicitly not enabled:

- 现货交易：交易所不支持现货（当前 adapter/官方主线口径；如官方 spot 交易面上线需重核）。
- Options chain/greeks/trading through shared Spot/Perp fields.
- Private write REST and bulk orders until EIP-712/external signer fixtures
  prove request construction.
- Transfers, withdrawals, vault, referral and builder APIs.
- Private WebSocket runtime until session-cookie renewal and reconciliation
  fallbacks are implemented.
- Cancel-on-disconnect/dead-man switch until runtime semantics are verified.

## Fixtures

- `tests/fixtures/exchanges/grvt/request_specs/public_book.json`
- `tests/fixtures/exchanges/grvt/request_specs/positions_session_spec_only.json`
- `tests/fixtures/exchanges/grvt/request_specs/open_orders_session_spec_only.json`
- `tests/fixtures/exchanges/grvt/request_specs/query_order_session_spec_only.json`
- `tests/fixtures/exchanges/grvt/request_specs/recent_fills_session_spec_only.json`
- `tests/fixtures/exchanges/grvt/open_orders_success.json`
- `tests/fixtures/exchanges/grvt/order_success.json`
- `tests/fixtures/exchanges/grvt/recent_fills_success.json`
- `tests/fixtures/exchanges/grvt/request_specs/create_order_unsupported.json`
- `tests/fixtures/exchanges/grvt/request_specs/cancel_order_unsupported.json`
- `tests/fixtures/exchanges/grvt/request_specs/cancel_all_orders_unsupported.json`
- `tests/fixtures/exchanges/grvt/request_specs/amend_order_session_spec_only.json`
- `tests/fixtures/exchanges/grvt/request_specs/bulk_orders_session_spec_only.json`
- `tests/fixtures/exchanges/grvt/parser/amend_order_ack.json`
- `tests/fixtures/exchanges/grvt/parser/bulk_orders_ack.json`
- `tests/fixtures/exchanges/grvt/parser/bulk_cancel_orders_ack.json`
- `tests/fixtures/exchanges/grvt/signing_vectors/session_cookie_boundary.json`
- `tests/fixtures/exchanges/grvt/ws/book_delta.json`
- `tests/fixtures/exchanges/grvt/ws/private_stream_boundary.json`
- `tests/fixtures/exchanges/grvt/instruments.json`
- `tests/fixtures/exchanges/grvt/orderbook.json`
- unsupported/empty/error/missing-field boundary fixtures

## Validation

Allowed validation commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/grvt/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway grvt --lib --message-format short
```

## Fee Boundary

GRVT active maker/taker fee tier model 可作为配置型费率来源。Mapping 已记录公开 active tier 离线边界 fixture：`tests/fixtures/exchanges/grvt/fees_tier_boundary.json`，覆盖 Perpetual/Option fee tier source。

当前 shared `get_fees` runtime 仍属项目未实现/未启用；补齐前需完成 fee tier config source、session/account scope audit for account-effective tiers 和 private readback parser。公开 tier table 只可作为 backtest/config source，生产 effective fee 需要 account tier readback 或显式 override，不能用默认零费率占位。

账户/余额已补离线边界：GRVT session/account balance readback 已固定 `request_specs/balances_session_spec_only.json` source/request boundary，矩阵按 `get_balances=离线` 记录。共享 `get_balances` runtime 仍需完成 session-cookie auth smoke、account/sub-account scope guard、balance parser 和 private WS/readback reconciliation。
## P2 Core Trading Boundary (2026-06-09)

P2 core readbacks `query_order`, `get_open_orders`, and `get_recent_fills` are guarded session-cookie runtime. P2 writes `place_order`, `cancel_order`, and `cancel_all_orders` remain offline/spec-only session/EIP-712 boundaries blocked on external signer integration, write reconciliation, and dry-run guard.

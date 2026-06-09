# Lighter Gateway Adapter

Status date: 2026-06-08

Adapter id: `lighter`

Implementation status: conservative Task 9 gateway registration with G0/G1
audit artifacts. Public REST and WebSocket documents are sufficient for
endpoint/session specs, but low-latency production runtime and private signed
transactions are not enabled.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Perpetual | `Perpetual` | Declared product scope; REST/WS endpoints are mapped as spec/parser-only. |
| Spot | n/a | 项目未实现 Spot。官方文档出现 spot market/stat 结构，当前 `lighter` adapter 未接 spot，不能写成交易所不支持。 |
| Options | n/a | Unsupported. |

Spot 边界写入 `spot_product status: project_unimplemented`：当前 adapter 只接 perpetual market index、order book 和 signed tx request-spec。补 Spot 前需要 spot market stats/metadata、spot order book/trades、spot account model、spot `sendTx` builder、nonce 和 sub-account parser。

Default URLs:

- REST: `https://mainnet.zklighter.elliot.ai/api/v1`
- WebSocket: `wss://mainnet.zklighter.elliot.ai/stream`
- Testnet WebSocket: `wss://testnet.zklighter.elliot.ai/stream`
- Testnet REST is documented as the analogous testnet host in SDK examples and
  remains marked for follow-up smoke verification.

## Order Book Boundary

Lighter's public order-book WebSocket uses `order_book/{MARKET_INDEX}`. The
feed sends a full snapshot at subscription and then state changes. Updates are
batched roughly every 50 ms.

Integrity rules:

- `begin_nonce == previous nonce` is the continuity check.
- 官方未给固定档位；mapping records this as no fixed depth.
- `offset` is API-server local, can jump on reconnect and is not guaranteed
  continuous.
- No CRC/checksum field is documented. Checksum support is explicitly
  unsupported.
- On nonce gap, discard local state and resubscribe for a fresh snapshot; REST
  `/orderBookOrders?market_id&limit<=250` can be used later as a cold-start or
  reconciliation source after parser promotion.

This task ships parser/session fixtures only; it does not claim production
runtime behavior. Mapping now records the 50ms high-frequency interval, no
fixed depth boundary, nonce continuity, gap resubscribe rule and no-checksum
boundary as structured fields. Source batch:
[WebSocket 官方核验 P5 衍生品/链上盘口细项](../WebSocket官方核验_P5_衍生品链上盘口细项.md).

## Authentication

Private read REST uses a bearer auth token plus account index and is guarded by
`LIGHTER_PRIVATE_REST_ENABLED`/`RUSTCTA_LIGHTER_PRIVATE_REST_ENABLED`.
`query_order`, `get_open_orders` and `get_recent_fills` now use guarded
read-only REST. Trading writes are not ordinary HMAC REST
requests: orders, cancels and modifies require SDK-compatible signed
transactions, an API-key private key, API key index and per-key nonce handling,
then submission through `sendTx`/`sendTxBatch`.

The adapter does not store API private keys and does not synthesize signed txs.
Withdrawals, transfers, priority transactions, public pools, referral,
notification and account tier mutation are out of trading runtime scope.

## Official Core Trading Detail

官方核心交易核验见 [核心交易官方核验 P2 第三批](../核心交易官方核验_P2_第三批.md)。Lighter API Trading 文档列出 Trading REST `/orders`、`/batchOrders`，WebSocket `orderUpdates`、`fills`，并说明支持单笔/批量下单撤单；示例使用 client-generated id、limit order 和 `GTC`。

当前 adapter 只有 spec/parser 边界，没有 SDK-compatible signed tx、API-key private key/index/nonce 管理和 live-dry-run gate。因此这是 `项目未实现 signed tx 核心交易`，不是 `交易所不支持下单/撤单`。

高级订单边界同样按 `官方支持，项目未实现/未启用` 记录：`/sendTx` modify、`/sendTxBatch` batch place/cancel 和 `/batchOrders` 资料只作为 mapping/request-spec 线索；`amend_order`、`batch_place_orders`、`batch_cancel_orders` 已固定 `spec_only` request fixture、fixture-only ack parser 和 `capabilities_v2` native/partial runtime-disabled metadata，仍 pending SDK-compatible signed tx、API-key private key/index/nonce、post-write reconciliation 和 dry-run guard。order-list/OCO 继续按交易所不支持处理。

## Official Position Detail

官方核验见 [仓位接口官方核验 P0 第一批](../仓位接口官方核验_P0_第一批.md)。Lighter SDK/API 的 `AccountApi.account` 返回 account data；账号/sub-account、auth token、WS account updates 都依赖 signer/auth token。

因此 Lighter 仓位接口写 `官方支持，已有离线 request-spec 边界，shared runtime 项目未实现/未启用`。当前已固定 `tests/fixtures/exchanges/lighter/request_specs/account_positions_readonly.json`，覆盖 `AccountApi.account`/`GET /account`、`account_index`、bearer auth token 和 `account_all_positions` 对账来源。补仓位 runtime 前仍必须完成 read-only auth token smoke、positions parser、sub-account index 映射、WS/REST reconciliation 和 runtime gate。

账户/余额已补离线边界：`AccountApi.account` / `GET /account` balance/equity readback 已固定 `request_specs/account_balances_readonly.json`，矩阵按 `get_balances=离线` 记录。当前 shared `get_balances` runtime 尚未完成 bearer token smoke、account/sub-account parser、precision mapping 和 WS/REST reconciliation。

## Endpoint Mapping

Machine-readable mapping:
`crates/rustcta-exchange-gateway/src/adapters/lighter/endpoint_mapping.yaml`.

Current runtime behavior:

- `symbol_rules`: `Unsupported("lighter.symbol_rules_session_spec_only")`
- `order_book`: `Unsupported("lighter.order_book_session_spec_only")`
- `query_order`: guarded `GET /accountActiveOrders` readback; disabled without private REST flag, bearer token and account index
- `get_open_orders`: guarded `GET /accountActiveOrders` readback; disabled without private REST flag, bearer token and account index
- `get_recent_fills`: guarded `GET /trades` readback; disabled without private REST flag, bearer token and account index
- `positions`: `Unsupported("lighter.positions_requires_auth_token_session_renewal_subaccount_parser_reconciliation")`
- `place_order`: `Unsupported("lighter.send_tx_signing_unverified")`
- `amend_order`: `Unsupported("lighter.amend_requires_signed_tx_builder_nonce_and_reconciliation")`
- `batch_place_orders`: `Unsupported("lighter.batch_place_requires_signed_tx_batch_builder_nonce_partial_parser_and_dry_run_guard")`
- `batch_cancel_orders`: `Unsupported("lighter.batch_cancel_requires_signed_tx_batch_builder_nonce_partial_parser_and_dry_run_guard")`
- public WS subscribe helper: payload only
- private WS: `Unsupported("lighter.private_stream_session_spec_only")`

## Fixtures

- `tests/fixtures/exchanges/lighter/request_specs/order_book_orders.json`
- `tests/fixtures/exchanges/lighter/request_specs/account_active_orders_readonly.json`
- `tests/fixtures/exchanges/lighter/request_specs/account_positions_readonly.json`
- `tests/fixtures/exchanges/lighter/request_specs/trades_readonly.json`
- `tests/fixtures/exchanges/lighter/request_specs/send_tx_unsupported.json`
- `tests/fixtures/exchanges/lighter/request_specs/send_tx_modify_request_spec_only.json`
- `tests/fixtures/exchanges/lighter/request_specs/send_tx_batch_request_spec_only.json`
- `tests/fixtures/exchanges/lighter/request_specs/send_tx_batch_cancel_request_spec_only.json`
- `tests/fixtures/exchanges/lighter/parser/amend_order_ack.json`
- `tests/fixtures/exchanges/lighter/parser/account_active_orders_success.json`
- `tests/fixtures/exchanges/lighter/parser/trades_success.json`
- `tests/fixtures/exchanges/lighter/parser/batch_place_orders_ack.json`
- `tests/fixtures/exchanges/lighter/parser/batch_cancel_orders_ack.json`
- `tests/fixtures/exchanges/lighter/signing_vectors/signed_tx_boundary.json`
- `tests/fixtures/exchanges/lighter/unsupported_boundary.json` (`advanced_order_boundaries` separates `/sendTx` modify and `/sendTxBatch`/`/batchOrders` project-unimplemented boundaries from true unsupported order-list/OCO)
- `tests/fixtures/exchanges/lighter/ws/order_book_snapshot.json`
- `tests/fixtures/exchanges/lighter/ws/order_book_update.json`
- `tests/fixtures/exchanges/lighter/ws/order_book_gap.json`
- `tests/fixtures/exchanges/lighter/order_books.json`
- `tests/fixtures/exchanges/lighter/order_book_orders.json`
- unsupported/empty/error/missing-field boundary fixtures

## Validation

Allowed validation commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/lighter/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway lighter --lib --message-format short
```

## Fee Boundary

Lighter standard account zero-fee/default source 可作为配置型费率来源；当前已记录离线配置源边界 `tests/fixtures/exchanges/lighter/request_specs/get_fees_config_source.json`，覆盖 Perpetual/Spot 的 standard account 口径。Premium/API/HFT 账户可能有 override，因此生产 runtime 必须有 account type/tier guard 或显式 fee override；未验证 auth token 不能当 account-effective readback。shared `get_fees` runtime 仍属项目未实现/未启用，剩 premium override config、account/sub-account scope guard 和 `FeeRateSnapshot` 映射。
## P2 Core Trading Boundary (2026-06-09)

P2 readback runtime is guarded for `query_order`, `get_open_orders` and `get_recent_fills` through bearer-token REST plus account index. Write operations remain offline/spec-only for place/cancel/cancel-all because runtime promotion is blocked on SDK-compatible signed tx builder, API-key private key/index/nonce handling, reconciliation, and dry-run guard.

## P2 Product Line Boundary (2026-06-09)

`spot_product` is an official-source project boundary, not an exchange-unsupported row. Lighter exposes spot market stats/structures and trading documentation, while this adapter is scoped to perpetual market indexes, order-book specs, and signed-transaction boundaries.

Do not promote Spot runtime from the perpetual profile. Promotion requires spot market metadata/index mapping, spot order-book/trade public parsers, spot account/sub-account private readback, spot `sendTx` action encoding, nonce handling, order lifecycle, and reconciliation guards.

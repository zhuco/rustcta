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
- `offset` is API-server local, can jump on reconnect and is not guaranteed
  continuous.
- No CRC/checksum field is documented. Checksum support is explicitly
  unsupported.
- On nonce gap, discard local state and resubscribe for a fresh snapshot; REST
  `/orderBookOrders?market_id&limit<=250` can be used later as a cold-start or
  reconciliation source after parser promotion.

This task ships parser/session fixtures only; it does not claim production
runtime behavior. Mapping still needs to promote the 50ms interval, nonce
continuity, gap resubscribe rule and no-checksum boundary into structured fields.
Source batch:
[WebSocket 官方核验 P5 衍生品/链上盘口细项](../WebSocket官方核验_P5_衍生品链上盘口细项.md).

## Authentication

Private read REST/WS uses auth tokens. Trading writes are not ordinary HMAC REST
requests: orders, cancels and modifies require SDK-compatible signed
transactions, an API-key private key, API key index and per-key nonce handling,
then submission through `sendTx`/`sendTxBatch`.

The adapter does not store API private keys and does not synthesize signed txs.
Withdrawals, transfers, priority transactions, public pools, referral,
notification and account tier mutation are out of trading runtime scope.

## Official Core Trading Detail

官方核心交易核验见 [核心交易官方核验 P2 第三批](../核心交易官方核验_P2_第三批.md)。Lighter API Trading 文档列出 Trading REST `/orders`、`/batchOrders`，WebSocket `orderUpdates`、`fills`，并说明支持单笔/批量下单撤单；示例使用 client-generated id、limit order 和 `GTC`。

当前 adapter 只有 spec/parser 边界，没有 SDK-compatible signed tx、API-key private key/index/nonce 管理和 live-dry-run gate。因此这是 `项目未实现 signed tx 核心交易`，不是 `交易所不支持下单/撤单`。

## Official Position Detail

官方核验见 [仓位接口官方核验 P0 第一批](../仓位接口官方核验_P0_第一批.md)。Lighter SDK/API 的 `AccountApi.account` 返回 account data；账号/sub-account、auth token、WS account updates 都依赖 signer/auth token。

因此 Lighter 仓位接口写 `官方支持，项目未实现/未启用`。补仓位前必须完成 `AccountApi.account` request spec、read-only auth token、positions parser、sub-account index 映射和 WS/REST reconciliation。

## Endpoint Mapping

Machine-readable mapping:
`crates/rustcta-exchange-gateway/src/adapters/lighter/endpoint_mapping.yaml`.

Current runtime behavior:

- `symbol_rules`: `Unsupported("lighter.symbol_rules_session_spec_only")`
- `order_book`: `Unsupported("lighter.order_book_session_spec_only")`
- `positions`: `Unsupported("lighter.positions_auth_token_unverified")`
- `place_order`: `Unsupported("lighter.send_tx_signing_unverified")`
- `batch_place_orders`: `Unsupported("lighter.send_tx_batch_signing_unverified")`
- public WS subscribe helper: payload only
- private WS: `Unsupported("lighter.private_stream_session_spec_only")`

## Fixtures

- `tests/fixtures/exchanges/lighter/request_specs/order_book_orders.json`
- `tests/fixtures/exchanges/lighter/request_specs/account_active_orders_readonly.json`
- `tests/fixtures/exchanges/lighter/request_specs/account_positions_readonly.json`
- `tests/fixtures/exchanges/lighter/request_specs/trades_readonly.json`
- `tests/fixtures/exchanges/lighter/request_specs/send_tx_unsupported.json`
- `tests/fixtures/exchanges/lighter/request_specs/send_tx_batch_unsupported.json`
- `tests/fixtures/exchanges/lighter/signing_vectors/signed_tx_boundary.json`
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

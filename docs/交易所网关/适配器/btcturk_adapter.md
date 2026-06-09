# BTCTurk Gateway Adapter

Task 18 scope: `btcturk` is a Turkey-focused Spot adapter. The adapter
normalizes venue symbols to compact uppercase symbols such as `BTCTRY` and
keeps TRY fiat-market behavior explicit.

## Official Sources

| Area | Source |
| --- | --- |
| REST base URL and API overview | https://docs.btcturk.com/ |
| Private REST authentication | https://docs.btcturk.com/docs/authentication/authentication-v1 |
| Submit order fields | https://docs.btcturk.com/docs/private-endpoints/submit-order |

## Coverage

| Area | Status |
| --- | --- |
| Product line | Spot only; standard futures/perpetual/options are `交易所不支持合约` under the current official API scope |
| Public REST | `GET /api/v2/server/exchangeinfo`, `GET /api/v2/orderbook` parser and transport |
| Private REST | Credential-gated `query_order`, `get_open_orders`, and `get_recent_fills`; order, cancel, and balance remain request-spec/fixture only |
| WebSocket | Public subscribe payload helper and private auth payload helper |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/btcturk/endpoint_mapping.yaml` |
| Fixtures | `tests/fixtures/exchanges/btcturk/` |
| Config example | `config/btcturk_gateway_example.yml`, disabled by default |

## Fiat And Region Boundary

BTCTurk is treated as a Turkish spot venue. The adapter fixtures cover TRY
markets (`BTCTRY`, `ETHTRY`) plus a crypto/stable quote example (`BTCUSDT`) so
symbol normalization does not assume USDT-only markets.

The gateway does not implement fiat withdrawal, fiat deposit, bank payment rail,
crypto deposit-address, transfer or funding-account operations. These surfaces
remain outside runtime scope even if the exchange API exposes them elsewhere.

## Authentication

Private REST uses the API public key header plus a millisecond timestamp and a
base64 HMAC-SHA256 signature over `api_key + timestamp`, using the base64-decoded
secret. The implementation keeps this in `signing.rs` and verifies it with
`tests/fixtures/exchanges/btcturk/signing_vectors/rest_hmac_sha256.json`.

## Runtime Boundary

Public REST is implemented for symbol rules and order book snapshots. Private
order/fill readbacks are promoted only for signed, read-only REST calls:
`query_order`, `get_open_orders`, and `get_recent_fills` fail closed unless
`BTCTURK_PRIVATE_REST_ENABLED` and API key/secret credentials are present.

Private writes remain offline request-spec only. `place_order`, `cancel_order`,
and quote market order submit paths return explicit `Unsupported` errors
because no non-live-write/dry-run-safe runtime plus post-write reconciliation is
enabled for BtcTurk.

Advanced order runtime is explicitly unsupported for shared `amend_order`,
`place_order_list`/OCO/OTO, native `batch_place_orders`, native
`batch_cancel_orders`, and audit-only `cancel_all_orders`. The unsupported
boundary fixture pins the runtime operation names and keeps native batch
capabilities disabled.

官方核验见 [产品线官方核验 P5 区域现货 CEX 第二批](../产品线官方核验_P5_区域现货_CEX第二批.md)。当前 BtcTurk 官方 API 资料未见标准 futures/perpetual/options，单交易所文档写 `交易所不支持合约`。

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P7 补充交易所盘口细项二](../WebSocket官方核验_P7_补充交易所盘口细项二.md)。BtcTurk public WS endpoint 是 `wss://ws-feed-pro.btcturk.com`，订阅消息为 `[151,{"type":151,"channel":"orderbook","event":"BTCTRY","join":true}]`；差量订单簿使用 `channel=obdiff`。

官方未给固定推流毫秒、固定 WS depth 或 checksum。`orderbook` 推全量模型，`obdiff` 推变化模型；两者都有 `CS` ChangeSet 顺序值，但官方说明当前没有服务端连续性控制。套利 book-cache 应以 REST `GET /api/v2/orderbook` 初始化，收到差量异常或 `CS` 不连续时重订阅/REST 重建。

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/btcturk/endpoint_mapping.yaml
cargo test -p rustcta-exchange-gateway btcturk --lib --message-format short
cargo test -p rustcta-gateway btcturk --message-format short
```

Allowed broader checks after both Task 18 adapters:

```bash
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
```

## Fee Boundary

BtcTurk Global fee structure 可作为配置型 maker/taker 费率来源。Mapping 已记录公开费率表离线边界 fixture：`tests/fixtures/exchanges/btcturk/fees_table_boundary.json`，适用 Spot profile。

当前 shared `get_fees` runtime 仍属项目未实现/未启用；补齐前需完成 fee table config source、刷新策略、region/profile guard 和显式 tier override。默认 fee table 只能作为配置源，不代表 account-effective readback。

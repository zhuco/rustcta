# Bitso Adapter

Task 16 adds a conservative Bitso gateway adapter for Latin America spot markets.

## Scope

- Products: spot only.
- Native symbol format: `major_minor`, for example `btc_mxn` and `btc_brl`.
- Canonical symbols preserve fiat quotes such as `BTC/MXN`, `BTC/BRL`, `ETH/ARS`, and `USDC/COP`.
- Public REST and public WebSocket are represented by request/payload helpers and fixtures.
- Private REST order, balance, open order, and fill surfaces are offline request-spec only until sandbox/live validation promotes them.

## Official Public WS Order Book Details

Bitso WebSocket connects to `wss://ws.bitso.com` and subscribes with JSON
messages such as `{"action":"subscribe","book":"btc_mxn","type":"diff-orders"}`.
Official channels include `orders`, which maintains the top 20 asks and bids,
and `diff-orders`, which carries full order book mutations. `diff-orders`
includes a strictly increasing `sequence`; a gap means a dropped message and the
book must be rebuilt from the REST order book snapshot. Official docs do not
state a fixed millisecond push interval.

| Surface | Structured Detail |
| --- | --- |
| Public WS URL/protocol | `wss://ws.bitso.com`, JSON WebSocket subscribe payload. |
| Snapshot channel | `orders`, top 20 bids/asks. |
| Delta channel | `diff-orders`, full order book mutations. |
| Depth | `orders` publishes top 20 asks/bids; `diff-orders` carries full order book mutations. |
| Interval | Unknown/no fixed millisecond interval documented. |
| Sequence | `diff-orders.sequence` must advance contiguously; a skip or regression requires rebuild. |
| Checksum | Unsupported/not documented. |
| Resync | Fetch REST `/order_book` snapshot, keep the snapshot `sequence`, replay buffered `diff-orders` with later contiguous sequences, and rebuild on reconnect/gap. |

## Boundaries

- Fiat ledger is read-audit-only.
- Official Margin Trading [WIP] is `项目未实现 Margin [WIP]` in this adapter.
- Standard futures/perpetual/options are `交易所不支持合约` under the current official API scope.
- `endpoint_mapping.yaml` keeps that distinction explicit: `margin_product`
  is `status: project_unimplemented` with `official_gap: margin_trading_wip`;
  `contract_product` is `unsupported` only for standard contracts.
- Status recommendation: keep Margin Trading [WIP] as
  `project_unimplemented` until API stability, region/account eligibility,
  collateral/balance/position/risk parsers, product-scoped order lifecycle, and
  reconciliation/dry-run gates are complete.
- Withdrawals, bank payments, SPEI, card operations, transfers, and margin funding are unsupported.
- Shared amend, OCO/OTO/order-list, native batch place, and native batch cancel are unsupported in the current Spot gateway mapping.
- Private streams are not promoted; private state should reconcile through REST request specs.
- Public stream runtime remains payload-helper/policy only; no live local order book runtime is promoted in this adapter.
- Fixtures use placeholder keys and synthetic IDs only.

## Verification

Allowed checks:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitso/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo test -p rustcta-exchange-gateway bitso --lib --message-format short
```

## Fee Boundary

Bitso signed `GET /api/v3/fees` 可作为每个 book 的 customer maker/taker 费率来源。Mapping 已记录离线 request spec、签名向量和响应样例：

- `tests/fixtures/exchanges/bitso/request_specs/get_fees.json`
- `tests/fixtures/exchanges/bitso/signing_vectors/rest_get_fees.json`
- `tests/fixtures/exchanges/bitso/fees_success.json`

当前 shared `get_fees` runtime 仍属项目未实现/未启用；补齐前需完成 maker/taker parser、book-symbol 映射和 read-only credential guard。

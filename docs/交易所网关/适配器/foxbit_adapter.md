# Foxbit Adapter

A-21 from `docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`
adds a conservative Foxbit gateway adapter for Brazil spot markets.

## Scope

- Products: Foxbit Exchange spot only.
- Official API family: REST/WS v3.
- REST base URL: `https://api.foxbit.com.br/rest/v3`.
- Public WS URL: `wss://api.foxbit.com.br/ws/v3/public`.
- Private WS URL: `wss://api.foxbit.com.br/ws/v3/private`.
- Native symbols use lowercase compact form, for example `btcbrl`.
- Canonical symbols preserve fiat quotes such as `BTC/BRL`.
- G1 public REST covers market discovery and order book snapshots from
  `/markets` and `/markets/{market_symbol}/orderbook`.
- Private REST readbacks for `query_order`, `get_open_orders`, and
  `get_recent_fills` are runtime-gated by `FOXBIT_PRIVATE_REST_ENABLED` plus
  API key/secret. Private writes remain request-spec and signing-vector only.

## Signing

Private REST uses `X-FB-ACCESS-KEY`, `X-FB-ACCESS-TIMESTAMP`, and
`X-FB-ACCESS-SIGNATURE`. The HMAC fixture signs:

`timestamp + method + request_path + query_string + raw_body`

with HMAC-SHA256 and hex output. `X-FB-RECEIVE-WINDOW` is modeled as optional.
The docs also describe Ed25519, but this adapter only validates the HMAC path
offline.

Private WS login uses HMAC-SHA256 over `timestamp + "login"`.

## Boundaries

- Foxbit Invest / OTC and Prime Desk RFQ APIs are excluded.
- Prediction/category-specific products are excluded until product scope is
  split from ordinary spot.
- Deposits, withdrawals, transfers, and bank payment flows are unsupported.
- Standard futures/perpetual/options are `交易所不支持合约` under the current official Foxbit Exchange API scope.
- Rispar collateral loan / margin-call help-center content is not a central exchange contracts position API.
- Private order placement/cancel, order-list, batch writes, balances, and fees
  remain offline/unsupported in runtime. Read-only order query, open orders,
  and recent fills fail closed unless the private REST guard and credentials
  are configured.
- P4 advanced order capabilities are unsupported in the current runtime:
  `amend_order`, `place_order_list`/OCO/OTO, `batch_place_orders`, and
  `batch_cancel_orders` are explicit unsupported boundaries.
- Private WS account events are not promoted; private state should reconcile
  through REST specs until read-only validation is completed.
- No official REST/WS v3 sandbox endpoint was found in the reviewed docs.

官方核验见 [产品线官方核验 P6 剩余区域现货 CEX](../产品线官方核验_P6_剩余区域现货_CEX.md)。

## Endpoint Mapping

Machine-readable mapping is in:

`crates/rustcta-exchange-gateway/src/adapters/foxbit/endpoint_mapping.yaml`

It marks public market/orderbook endpoints as spec-only verified fixtures,
private REST order/fill readbacks as guarded runtime, and private writes/WS as
offline specs or auth-payload-only.

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。Foxbit REST/WS v3 有 public orderbook subscribe payload，public WS URL 为 `wss://api.foxbit.com.br/ws/v3/public`。

本批未在公开资料核到固定推流毫秒、固定 depth、固定 sequence 或 checksum，矩阵边界记录为无固定 ms、无固定 depth、no sequence、no checksum。项目已有 orderbook payload variant `100/250/500/1000` 证据，但这些参数需要继续确认语义；实盘前必须用 REST `/markets/{market_symbol}/orderbook` 做 snapshot fallback。

## Verification

Allowed checks:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/foxbit/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway foxbit --lib --message-format short
cargo test -p rustcta-gateway foxbit --message-format short
```

Do not run `cargo build` for this task.

## Fee Boundary

交易所不支持当前费率接口 runtime：当前已核资料没有稳定 trading fee-rate endpoint；成交手续费金额不能等同 maker/taker rate readback。

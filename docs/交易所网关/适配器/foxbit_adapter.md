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
- Private REST order/read surfaces are request-spec and signing-vector only
  until read-only/live-dry-run validation promotes them.

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
- Private order placement/cancel/query/open orders/fills remain offline
  request-spec only.
- Private WS account events are not promoted; private state should reconcile
  through REST specs until read-only validation is completed.
- No official REST/WS v3 sandbox endpoint was found in the reviewed docs.

官方核验见 [产品线官方核验 P6 剩余区域现货 CEX](../产品线官方核验_P6_剩余区域现货_CEX.md)。

## Endpoint Mapping

Machine-readable mapping is in:

`crates/rustcta-exchange-gateway/src/adapters/foxbit/endpoint_mapping.yaml`

It marks public market/orderbook endpoints as spec-only verified fixtures and
private REST/WS as offline specs or auth-payload-only.

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P8 补充交易所盘口细项三](../WebSocket官方核验_P8_补充交易所盘口细项三.md)。Foxbit REST/WS v3 有 public orderbook subscribe payload，public WS URL 为 `wss://api.foxbit.com.br/ws/v3/public`。

本批未在公开资料核到固定推流毫秒、固定 sequence 或 checksum。项目已有 `orderbook-100/250/500/1000` payload 证据，但这些参数需要继续确认是档位还是其他模式；实盘前必须用 REST `/markets/{market_symbol}/orderbook` 做 snapshot fallback。

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

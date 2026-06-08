# BIT.TEAM Gateway Adapter

Status date: 2026-06-08

Adapter id: `bitteam`

Implementation status: A-09 scan-only Spot adapter. Public REST support is
implemented for Spot pair metadata and order-book snapshots. Private REST,
trading, batch, account readbacks, and WebSocket runtime are deliberately kept
`Unsupported` until read-only credentials and write request semantics are
validated beyond offline request specs.

## Official Materials

| Area | Source | Adapter use |
| --- | --- | --- |
| Product scope | `https://bit.team/docs` describes BIT.TEAM Spot and P2P products and CCXT/API support. | Adapter declares `MarketType::Spot` only. |
| Swagger/OpenAPI | `https://bit.team/trade/api/documentation` and `https://bit.team/trade/api/documentation.json` list CCXT pairs, balance, order, order book, orders, and trades endpoints. | Public pairs/orderbook are enabled; private endpoints are request-spec-only. |
| Auth | Swagger description documents HTTP Basic auth using `base64(key1:key2)`. | `signing.rs` builds a Basic authorization vector with sanitized fixture keys. |
| Spot guide | `https://bit.team/docs/api/spot_api_new/currencies_rates` links the Spot API section. | Used only as product/API navigation evidence. |

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST G1 for symbol rules and order book. |
| P2P | n/a | Unsupported; not a central exchange order-book/trading surface. |
| Perpetual/futures/options | n/a | `交易所不支持合约`; current official docs/API list Spot/P2P and CCXT-style spot endpoints, with no stable standard contracts product scope verified for this adapter. |
| Testnet | n/a | Unsupported; no stable public sandbox host verified. |

官方核验见 [产品线官方核验 P5 区域现货 CEX 第二批](../产品线官方核验_P5_区域现货_CEX第二批.md)。P2P 不属于中心化订单簿交易 surface；标准 futures/perpetual/options 写 `交易所不支持合约`。

Default REST base URL: `https://bit.team`

## Endpoint Mapping

| Gateway capability | BIT.TEAM endpoint | Current status |
| --- | --- | --- |
| Symbol rules | `GET /trade/api/ccxt/pairs` | Implemented for Spot parser fixtures. |
| Order book | `GET /trade/api/orderbooks/{symbol}` | Implemented for snapshot parser fixtures. |
| Balances | `GET /trade/api/ccxt/balance` | Request-spec-only; runtime returns `Unsupported("bitteam.balances_request_spec_only")`. |
| Place order | `POST /trade/api/ccxt/ordercreate` | Request-spec-only; runtime returns `Unsupported("bitteam.place_order_request_spec_only")`. |
| Cancel order | `POST /trade/api/ccxt/cancelorder` | Request-spec-only; runtime returns `Unsupported("bitteam.cancel_order_request_spec_only")`. |
| Open orders | `GET /trade/api/ccxt/ordersOfUser` | Request-spec-only REST reconciliation candidate. |
| Recent fills | `GET /trade/api/ccxt/tradesOfUser` | Request-spec-only REST reconciliation candidate. |
| Batch place/cancel | Not verified | Unsupported. |
| WebSocket | 交易所不支持公共 WS 行情 | 当前官方 docs/OpenAPI 只给 REST/CCXT-style endpoints，未给 WSS endpoint 和订单簿订阅规格；REST reconciliation fallback documented for future private promotion. |

## Authentication

BIT.TEAM documents HTTP Basic authorization for CCXT methods. The adapter has
`basic_authorization_header(api_key, api_secret)` and a sanitized signing vector:

- fixture: `tests/fixtures/exchanges/bitteam/signing_vectors/basic_auth.json`
- request specs: `tests/fixtures/exchanges/bitteam/request_specs/*.json`

Private REST remains disabled by default even when credentials are present.
The adapter config can read `BITTEAM_SPOT_API_KEY`/`BITTEAM_SPOT_API_SECRET`
or `BITTEAM_API_KEY`/`BITTEAM_API_SECRET` for offline request specs, but
`apps/gateway` env wiring is not enabled for this scan-only task.

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

Non-compiling validation used for this task:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitteam/endpoint_mapping.yaml
```

When compilation is explicitly allowed, run:

```bash
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bitteam --lib --message-format short
```

Do not run `cargo build` for this task. `cargo test -p rustcta-gateway bitteam`
is only relevant if a future change wires Bitteam into `apps/gateway` env config.

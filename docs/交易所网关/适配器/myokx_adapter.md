# MyOKX Gateway Adapter

Status date: 2026-06-08

This adapter covers task A-27 from
`docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`.

## Scope

- Adapter id: `myokx`
- Profile: OKX EEA / MyOKX profile
- Product mapped to `ExchangeClient`: Spot only
- REST base URL: `https://eea.okx.com`
- Public WebSocket: `wss://wseea.okx.com:8443/ws/v5/public`
- Private WebSocket recorded for auth-shape audit only: `wss://wseea.okx.com:8443/ws/v5/private`

Official references:

- MyOKX API docs: `https://my.okx.com/docs-v5/en/`
- OKX V5 public instruments: `https://my.okx.com/docs-v5/en/#public-data-rest-api-get-instruments`
- OKX V5 order book: `https://my.okx.com/docs-v5/en/#order-book-trading-market-data-get-order-book`
- CCXT 4.5.56 MyOKX profile: `https://raw.githubusercontent.com/ccxt/ccxt/v4.5.56/ts/src/myokx.ts`

The gateway implements this as a profile over the existing `okx` Spot REST
adapter. `OkxGatewayConfig` carries the runtime exchange id, so `myokx` requests
are served as `ExchangeId("myokx")` without copying OKX request or parser code.

## Implemented Gateway Surface

- Named gateway registration for `myokx`, `my_okx`, `my-okx`, `okx_eea`,
  and `okx-eea`.
- Public Spot symbol rules through `GET /api/v5/public/instruments`.
- Public Spot order book snapshots through `GET /api/v5/market/books`.
- OKX-style public WebSocket subscribe/unsubscribe payload fixtures and
  heartbeat policy for the MyOKX EEA WebSocket host.
- OKX-style private WebSocket login signing vector for offline shape validation.
- Disabled config example at `config/myokx_gateway_example.yml`.
- Sanitized fixtures under `tests/fixtures/exchanges/myokx/`.

## Unsupported Boundary

- Swaps, futures, options, margin, leverage, margin mode, position mode, and
  position readback are unsupported in the current runtime for this profile.
- Runtime private REST reads, runtime private REST writes, private WebSocket
  login/subscriptions, real order placement, real cancels, batch operations,
  transfers, withdrawals, and funding actions are unsupported.
- The adapter does not read MyOKX credentials. Private request and WebSocket
  auth shapes are retained only as sanitized offline fixtures until EEA
  credential permissions and regional product scope are audited.

## Official Product-Line Boundary

P6 official verification uses `my.okx.com/docs-v5`: the regional OKX v5 docs
include Spot/Margin, Swap, Futures, Options, Events, positions, leverage,
funding, and open interest. These are `项目未实现` for this `myokx` profile or
should be delegated to the main `okx` adapter after a regional scope audit; they
must not be documented as `交易所不支持合约`.

## Endpoint Mapping

`crates/rustcta-exchange-gateway/src/adapters/myokx/endpoint_mapping.yaml`
records:

- EEA REST and WebSocket base URLs.
- Spot public REST endpoints inherited from OKX V5.
- Derivatives and private trading as explicit `Unsupported`.
- REST order book snapshot as the public WebSocket resync path.

## Validation

Allowed commands for this task:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/myokx/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway myokx --lib --message-format short
cargo test -p rustcta-gateway myokx --message-format short
```

Do not run `cargo build` or any live trading command for this adapter.

# Aftermath Gateway Adapter

Status date: 2026-06-08

Aftermath is a Sui on-chain protocol. This adapter is intentionally scan-only: it implements public perpetual REST reads through the official CCXT-compatible API surface and keeps account reads, order writes, and private streams disabled until the gateway has a native Sui transaction build/sign/submit boundary.

## Scope

- Adapter id: `aftermath`
- Market type: `perpetual`
- Native public REST:
  - `GET /api/ccxt/markets` -> `get_symbol_rules`
  - `POST /api/ccxt/orderbook` with `chId` -> `get_order_book`
- Public WebSocket helpers are payload/fixture-only for `/api/perpetuals/ws/updates`.
- Private reads and writes return `ExchangeApiError::Unsupported`.

## Official References

- OpenAPI: `https://aftermath.finance/api/openapi/spec.json`
- CCXT API docs: `https://docs.aftermath.finance/for-developers/api/ccxt`
- Perpetuals SDK source reference: `https://github.com/AftermathFinance/aftermath-ts-sdk/blob/main/src/packages/perpetuals/perpetuals.ts`

## Auth And Signing Boundary

The public CCXT endpoints do not require API-key signing. Aftermath also documents an optional wallet-message access token flow, represented only as an optional bearer token on public requests.

Trading operations are not HMAC/API-key requests. They require Sui transaction blocks and wallet/account-cap authorization, so this adapter does not synthesize or sign private write payloads. Fixtures under `tests/fixtures/exchanges/aftermath/signing_vectors/` assert that boundary.

## Base URLs And Limits

- REST base URL: `https://aftermath.finance`
- Testnet REST URL recorded for audit only: `https://testnet.aftermath.finance`
- Public WS URL recorded for payload fixtures only: `wss://aftermath.finance/api/perpetuals/ws/updates`
- Public CCXT rate bucket: `1000` requests per `10s`, represented as `aftermath_public_ccxt`.

## Endpoint Mapping And Capabilities

`endpoint_mapping.yaml` marks `symbol_rules` and `order_book` as native public REST for perpetual markets. `ticker`, `trades`, and `ohlcv` are fixture/parser-only because the current gateway interface does not expose them through this adapter. Balances, positions, fees, order writes, batch writes, query/open orders, recent fills, and private streams are explicitly `unsupported`.

Runtime capabilities follow that mapping: public REST is enabled for scan-only symbol rules and order-book snapshots; private REST, private streams, public stream runtime, trading, batch trading, reduce-only, post-only, and client order id support remain disabled.

## Validation

Use the task-approved offline checks:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/aftermath/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway aftermath --lib --message-format short
cargo test -p rustcta-gateway aftermath --message-format short
```

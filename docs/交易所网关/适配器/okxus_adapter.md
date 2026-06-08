# OKX US Gateway Adapter

Status: `rustcta-exchange-gateway` OKX US profile adapter for OKX v5 Spot public REST. Private trading is deliberately disabled.

## Product Boundary

- Adapter id: `okxus`.
- Profile: OKX v5 regional profile, reusing the existing `okx` parser, signing shape, and transport semantics.
- Products: Spot public market data only.
- REST base URL: `https://us.okx.com`.
- Public WS URL recorded for audit: `wss://wsus.okx.com:8443/ws/v5/public`.
- Private WS URL recorded for audit only: `wss://wsus.okx.com:8443/ws/v5/private`.

Official references:

- OKX US Risk & Compliance: https://app.okx.com/en-us/help/us-risk-and-compliance-disclosures
- OKX API FAQ: https://www.okx.com/en-us/help/api-faq
- OKX API docs: https://app.okx.com/docs-v5/en/
- OKX US Terms of Service: https://www.okx.com/en-us/help/terms-of-service-us
- OKX API Agreement: https://www.okx.com/en-us/help/okx-api-agreement

The official API docs use the same OKX v5 `/api/v5` REST surface and list U.S. domains for U.S. users. The U.S. terms and disclosures frame the U.S. venue as spot digital asset trading plus related buy/sell/convert services, so this adapter keeps non-spot and private trading surfaces closed until a separate credential and product-scope audit is completed.

P6 official product-line verification also found OKX U.S. help material for
perpetual futures, event contracts, and spot/margin trading. Those surfaces are
`项目未实现 Perpetual Futures/Event Contracts/Margin boundary` for this adapter
until regional API eligibility, credential scope, and product availability are
audited; do not document them as `交易所不支持合约`.

## Implemented Gateway Surface

- Named gateway registration for `okxus`, `okx_us`, and `okx-us`.
- Public REST symbol rules through `/api/v5/public/instruments`, using the OKX v5 parser with `ExchangeId("okxus")`.
- Public REST order book snapshots through `/api/v5/market/books`, using the OKX v5 parser with U.S. exchange identity.
- WebSocket subscription payload fixture for `books5` on the U.S. public stream host.
- Fixture-backed boundary audit under `tests/fixtures/exchanges/okxus/`.
- Disabled config example at `config/okxus_gateway_example.yml`.

## Unsupported Boundary

- Private REST reads, private REST writes, private WebSocket auth/subscriptions, real order placement, real cancels, batch operations, leverage, margin mode, position mode, derivatives, funding, mark/index/open-interest, deposits, withdrawals, transfers, fiat banking, convert/OTC, Earn, bots/copy/algo/order-list flows, and dead-man/cancel-all-after are `Unsupported`.
- The adapter does not read `RUSTCTA_OKXUS_API_KEY`, `RUSTCTA_OKXUS_API_SECRET`, `RUSTCTA_OKXUS_API_PASSPHRASE`, or global OKX credentials. This prevents accidental live trading on a regional profile whose credential scope has not been verified.
- No web page endpoints, unofficial APIs, live orders, live cancels, withdrawals, or transfers are used.

## Official Core Trading Detail

官方核心交易核验见 [核心交易官方核验 P1 第二批](../核心交易官方核验_P1_第二批.md)。OKX US profile 复用 OKX V5 Trade API 语义，官方/区域资料有 order/cancel 交易接口线索；Spot 至少支持 limit/market 和 `clOrdId`，但 eligibility、API domain 和产品 scope 需要单独审计。

当前 adapter 禁用 private REST/WS 和真实下单撤单；这是 `项目未实现/未启用区域 private trading`，不是 `交易所不支持下单/撤单`。后续必须先补区域 private REST specs、signing vectors、credential scope guard 和 parser。

## Endpoint Mapping

`crates/rustcta-exchange-gateway/src/adapters/okxus/endpoint_mapping.yaml` records:

- OKX US REST and WebSocket base URLs.
- OKX v5 Spot public market-data endpoints as supported/spec-covered.
- Private account/order/fills endpoints as explicit `Unsupported`.
- REST reconciliation fallback only for public stream resync; private reconciliation remains unsupported.

## Official WebSocket Order Book Detail

P9 official verification treats this as an OKX V5 regional profile. Public
market data supports `bbo-tbt` at 10ms, `books5` at 100ms, and `books` at
100ms; TBT depth channels are 10ms but require login/VIP eligibility. The U.S.
public host is `wss://wsus.okx.com:8443/ws/v5/public`. Mapping must record the
regional host, `books5`/`bbo-tbt`, 10ms/100ms intervals, OKX
sequence/checksum semantics, REST `/api/v5/market/books` resync, and regional
product eligibility caveats.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/okxus/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway okxus --lib --message-format short
cargo test -p rustcta-gateway okxus --message-format short
```

`cargo build` is intentionally not part of this task.

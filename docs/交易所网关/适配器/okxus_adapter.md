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

The official API docs use the same OKX v5 `/api/v5` REST surface and list U.S. domains for U.S. users. The current adapter still exposes only spot public REST at runtime, so non-spot product lines stay disabled until a dedicated OKX US product-scope audit verifies API-enabled access for each product line.

2026 US help materials also expose Event Contracts and futures concepts, and OKX v5 API docs expose the shared margin/derivatives account and instrument semantics. Because the current adapter wires only spot public REST, non-spot products are recorded as `项目未实现` regional product boundaries pending US domain, credential scope, state eligibility, account-mode, and API product-scope audit.
Mapping now records `contract_product`, `market_type_non_spot`,
`margin_product`, `futures_product`, `perpetual_product`, and
`event_contract_product` as `status: project_unimplemented` with
`boundary: project_unimplemented_product_line`, all bound to
`tests/fixtures/exchanges/okxus/request_specs/product_line_source_boundary.json`
with required audit gaps for US domain, credential scope, state eligibility,
account mode, product metadata, order lifecycle, and reconciliation. This
is a regional `project_unimplemented` boundary, not an unsupported judgment.
Status recommendation: keep Margin, Perpetual, Futures, non-spot market scope,
and Event Contracts disabled until US eligibility, credential permissions,
API product scope, metadata, lifecycle, positions, settlement, and
reconciliation are audited.
preserves the regional product gap without enabling private or non-spot runtime.

## Implemented Gateway Surface

- Named gateway registration for `okxus`, `okx_us`, and `okx-us`.
- Public REST symbol rules through `/api/v5/public/instruments`, using the OKX v5 parser with `ExchangeId("okxus")`.
- Public REST order book snapshots through `/api/v5/market/books`, using the OKX v5 parser with U.S. exchange identity.
- WebSocket subscription payload fixture for `books5` on the U.S. public stream host.
- Fixture-backed boundary audit under `tests/fixtures/exchanges/okxus/`.
- Disabled config example at `config/okxus_gateway_example.yml`.

## Unsupported Boundary

- Private REST reads, private REST writes, private WebSocket auth/subscriptions, real order placement, real cancels, batch operations, leverage, position mode, funding, mark/index/open-interest, deposits, withdrawals, transfers, fiat banking, convert/OTC, Earn, bots/copy/algo/order-list flows, and dead-man/cancel-all-after are `Unsupported` or disabled until credential/scope audit.
- Margin, perpetual, expiry futures, options, and event-contract product lines are `项目未实现` regional product boundaries, not fully implemented runtime surfaces; the source boundary fixture is `tests/fixtures/exchanges/okxus/request_specs/product_line_source_boundary.json`.
- The adapter does not read `RUSTCTA_OKXUS_API_KEY`, `RUSTCTA_OKXUS_API_SECRET`, `RUSTCTA_OKXUS_API_PASSPHRASE`, or global OKX credentials. This prevents accidental live trading on a regional profile whose credential scope has not been verified.
- No web page endpoints, unofficial APIs, live orders, live cancels, withdrawals, or transfers are used.

## Official Core Trading Detail

官方核心交易核验见 [核心交易官方核验 P1 第二批](../核心交易官方核验_P1_第二批.md)。OKX US profile 复用 OKX V5 Trade API 语义，官方/区域资料有 order/cancel 交易接口线索；Spot 至少支持 limit/market 和 `clOrdId`，但 eligibility、API domain 和产品 scope 需要单独审计。

当前 adapter 禁用 private REST/WS 和真实下单撤单；这是 `项目未实现/未启用区域 private trading`，不是 `交易所不支持下单/撤单`。后续必须先补区域 private REST specs、signing vectors、credential scope guard 和 parser。

账户/余额已补离线边界：OKX V5 `GET /api/v5/account/balance` 已固定 OKX US host request-spec `request_specs/get_balances_account_balance.json`，矩阵按 `get_balances=离线` 记录。`okxus` shared `get_balances` runtime 尚未完成 US domain、credential scope、state/product eligibility、account-mode policy、parser 和 read-only reconciliation。

## Endpoint Mapping

`crates/rustcta-exchange-gateway/src/adapters/okxus/endpoint_mapping.yaml` records:

- OKX US REST and WebSocket base URLs.
- OKX v5 Spot public market-data endpoints as supported/spec-covered.
- Private account/order/fills endpoints as explicit `Unsupported` with `project_unimplemented` boundaries for the US credential, scope, signing, parser, eligibility, and dry-run audit.
- Non-spot product lines as regional `project_unimplemented` boundaries pending US domain, credential scope, state eligibility, account-mode, and API product-scope audit.
- REST reconciliation fallback only for public stream resync; private reconciliation remains unsupported.

## Official WebSocket Order Book Detail

P9 official verification treats this as an OKX V5 regional profile. Public
market data supports `bbo-tbt` depth1 at 10ms, `books5` depth5 at 100ms, and
`books` depth400 at 100ms; `books-l2-tbt` depth400 and `books50-l2-tbt` depth50
are 10ms depth channels but require login/VIP eligibility. The U.S. public host
is `wss://wsus.okx.com:8443/ws/v5/public`. The mapping records the regional
host, `books`/`books5`/`bbo-tbt`, 10ms/100ms intervals, OKX
`seqId`/`prevSeqId` and `checksum` semantics, REST `/api/v5/market/books`
resync, and regional product eligibility caveats.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/okxus/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway okxus --lib --message-format short
cargo test -p rustcta-gateway okxus --message-format short
```

`cargo build` is intentionally not part of this task.

## Fee Boundary

费率来源在 OKX US/OKX V5 account fee 资料中存在；当前已补 OKX US host `GET /api/v5/account/trade-fee` 的 `request_specs/get_fees_trade_fee.json` 离线 request-spec 边界。`okxus` shared `get_fees` runtime 仍属项目未实现/未启用；补齐前需完成 US domain、credential scope、state/product eligibility、instType/instId policy 和 parser 审计。
## P2 Core Trading Boundary (2026-06-09)

P2 core trading now reuses the OKX V5 signed private REST runtime on the OKX US host when explicit OKXUS credentials/passphrase and private REST enable flag are configured. place/cancel/cancel-all/query/open/fills are matrix `原生`; non-spot products remain separate US product-scope gaps.

# Bybit EU Gateway Adapter

Status: `rustcta-exchange-gateway` Bybit EU profile adapter for Bybit V5 public REST/WebSocket plus guarded private REST read/write operations when explicit EU credentials and the private REST enable flag are configured.

## Product Boundary

- Adapter id: `bybiteu`.
- Profile: Bybit V5 local-site profile, reusing the existing `bybit` parser, signing shape, and transport semantics.
- Products: Spot, USDT/USDC linear perpetual, and futures where the EU site exposes the same V5 market-data categories.
- REST base URL: `https://api.bybit.eu`.
- Public WS URL: `wss://stream.bybit.eu/v5/public/linear`.
- Private WS URL recorded for audit only: `wss://stream.bybit.eu/v5/private`.

Official references:

- Bybit V5 integration guidance: https://bybit-exchange.github.io/docs/v5/guide
- Bybit V5 WebSocket connect: https://bybit-exchange.github.io/docs/v5/ws/connect
- Bybit V5 market instruments: https://bybit-exchange.github.io/docs/v5/market/instrument

The V5 integration guidance lists `https://api.bybit.eu` for EEA users and states that the EU site API is limited to broker third-party application connectivity. The WebSocket guide documents Bybit V5 public/private stream shapes and regional local-site host handling. This adapter therefore treats Bybit EU as a regulatory profile, not a separate exchange implementation.

## Implemented Gateway Surface

- Named gateway registration for `bybiteu`, `bybit_eu`, and `bybit-eu`.
- Public REST symbol rules through `/v5/market/instruments-info`, using the Bybit V5 parser.
- Public REST order book snapshots through `/v5/market/orderbook`, using the Bybit V5 parser.
- Public WebSocket subscription payload specs for Bybit V5 topics on the EU stream host.
- Fixture-backed boundary audit under `tests/fixtures/exchanges/bybiteu/`.
- Disabled config example at `config/bybiteu_gateway_example.yml`.

## Unsupported Boundary

- Private REST order create/cancel/cancel-all/query/open/fills and position readback reuse Bybit V5 signed runtime on the EU host only when BYBITEU/BYBIT_EU credentials and the private REST enable flag are configured. Default no-credential environments fail closed. P4 advanced order surfaces remain explicitly bounded: `amend_order`, OCO/OTO/order-list, native `batch_place_orders`, native `batch_cancel_orders`, and audit-only `cancel_all_orders` are pinned by `unsupported_boundary.json` and default-config tests as `Unsupported` until EU broker scope, dry-run safety, and post-write reconciliation are audited. Leverage, margin mode, position mode, and dead-man/cancel-all-after remain `Unsupported`.
- 费率项目未实现/未启用：Bybit V5 `GET /v5/account/fee-rate` 语义可复用到 EU profile；当前已补 `request_specs/get_fees_fee_rate.json` 离线 request-spec 边界，但还缺 EU domain、credential scope、broker application audit、category/symbol parser 和 read-only guard，不能把共享 `get_fees` 写成已启用。
- 账户/余额已补离线边界：Bybit V5 `GET /v5/account/wallet-balance` 已固定 EU host request-spec `request_specs/get_balances_wallet_balance.json`，矩阵按 `get_balances=离线` 记录；仍需 EU domain/scope guard、broker application audit、accountType policy、balance parser 和 read-only reconciliation。
- The adapter does not read `RUSTCTA_BYBITEU_API_KEY`, `RUSTCTA_BYBITEU_API_SECRET`, or Bybit global credentials. This prevents accidental live trading on a regulatory profile whose credential scope has not been verified.
- No web page endpoints, unofficial APIs, live orders, live cancels, withdrawals, or transfers are used.

## Official Core Trading Detail

P0 core-trading verification confirms Bybit EU UTA help material maps order
creation to `/v5/order/create` and order deletion to `/v5/order/cancel`. Bybit
V5 official docs support Spot, Margin, linear/inverse derivatives, and options
order creation, cancel, and cancel-all. Supported TIF values include `GTC`,
`IOC`, `FOK`, `PostOnly`, and `RPI`; `orderLinkId` is supported up to 36
characters. This adapter keeps private trading disabled until regional API
eligibility and credential scope are audited, so the correct status is
`项目未实现核心交易接口`, not `交易所不支持下单/撤单`.

## Official Position Detail

官方核验见 [仓位接口官方核验 P0 第一批](../仓位接口官方核验_P0_第一批.md)。Bybit V5 position API 支持 `GET /v5/position/list`，category 覆盖 linear、inverse、option，支持 `symbol`、`baseCoin`、`settleCoin` 和 cursor pagination。

因此 Bybit EU 仓位接口现在写 `运行`：`get_positions` 复用 Bybit V5 guarded signed REST runtime，默认无凭证或未显式开启 private REST 时 fail-closed；启用后请求 EU host `/v5/position/list`，使用 `limit=50` 和 `result.nextPageCursor` 拉取后续页，并复用 Bybit position parser 映射 side、entry/mark/liquidation price、unrealized PnL 和 leverage。当前测试只证明 mock REST/request-spec/parser/reconciliation guard，不代表 live credential 已验证。

## Endpoint Mapping

`crates/rustcta-exchange-gateway/src/adapters/bybiteu/endpoint_mapping.yaml` records:

- EU REST and WebSocket base URLs.
- Bybit V5 public market-data endpoints as supported/spec-covered.
- Private order/fills endpoints as guarded Bybit V5 runtime or request-spec-only boundaries depending on operation.
- Private position readback via `/v5/position/list` as guarded runtime with cursor readback tests.
- REST reconciliation fallback only for public stream resync; private reconciliation remains unsupported.

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P7 补充交易所盘口细项二](../WebSocket官方核验_P7_补充交易所盘口细项二.md)。Bybit EU 按 Bybit V5 public orderbook 语义处理，EU public WS host 使用 `wss://stream.bybit.eu/v5/public/{category}`，topic 为 `orderbook.{depth}.{symbol}`。

Linear/inverse/spot 支持 1/50/200/1000 档，对应 10ms/20ms/100ms/200ms；option 支持 25/100 档，对应 20ms/100ms。消息有 `u`、cross `seq` 和 `cts`；snapshot 后推 delta，收到新 snapshot 必须 reset 本地 book。L1 是 snapshot-only，3 秒无变化会重复 snapshot。

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bybiteu/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bybiteu --lib --message-format short
cargo test -p rustcta-gateway bybiteu --message-format short
```

`cargo build` is intentionally not part of this task.
## P2 Core Trading Boundary (2026-06-09)

P2 core trading now reuses the Bybit V5 signed private REST runtime on the EU host when explicit BYBITEU/BYBIT_EU credentials and private REST enable flag are configured. place/cancel/cancel-all/query/open/fills are matrix `原生`; default no-credential environments still fail closed through the existing private REST guard.

## P2 Position Runtime (2026-06-09)

`get_positions` now reuses the Bybit V5 signed private REST runtime on the EU host with explicit credential/enable gating. Focused tests cover the first request-spec page, cursor follow-up, signed header redaction, exchange id `bybiteu`, and parser output; matrix status is `运行`.

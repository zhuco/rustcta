# CoinSpot Adapter

Scope: Australian AUD spot REST only.

Base URLs:

- Public REST: `https://www.coinspot.com.au/pubapi/v2/*`
- Private REST: `https://www.coinspot.com.au/api/v2/*`
- Read-only private REST: `https://www.coinspot.com.au/api/v2/ro/*`

Implemented:

- Public REST symbol rules from `/pubapi/v2/markets`.
- Public order book snapshots from `/pubapi/v2/orders/open`.
- Private HMAC-SHA512 JSON signing with `nonce`, `key`, and `sign`.
- Private balances, fees, open orders, recent fills, limit buy/sell, market buy/sell, and quote-value market buy request construction.
- Offline request specs, signing vectors, and parser fixtures under `tests/fixtures/exchanges/coinspot/`.

Official Core Trading Detail:

官方核心交易核验见 [核心交易官方核验 P1 第二批](../核心交易官方核验_P1_第二批.md)。CoinSpot API/V2 页面列出 place buy/sell order、cancel buy/sell order、market buy/sell/cancel 和 advanced buy stop/limit。

当前项目下单、open orders 和 recent fills 已有运行证据；通用 cancel/query 仍是项目缺口。`cancel_order` 已补 side-specific `/my/{buy|sell}/cancel` 离线 request-spec，但 runtime 仍缺 side/order-context 选择、parser 和 cancel/readback reconciliation；`query_order` 依赖 open/completed/history readback 对单笔订单归一，未补 parser 前继续写 `项目未实现`，不能写成 `交易所不支持撤单/查单`。

Explicit boundaries:

- Non-AUD markets are rejected as unsupported.
- Streams are unsupported；当前官方 API/V2 API 未见公共订单簿 WS，写 `交易所不支持公共 WS 行情`，并在 YAML 记录 `websocket.public.support: unsupported`。
- Positions are unsupported because CoinSpot is spot-only.
- Standard futures/perpetual/options/margin are `交易所不支持合约` under the current official API scope.
- Cancel and query-order are project-unimplemented because CoinSpot exposes side-specific cancel/readback surfaces that are not safely mapped from generic gateway requests without side/order-context guarantees. Amend, order-list, batch place/cancel, and generic cancel-all remain unsupported boundaries for the current runtime.
- Fill history filters and pagination are unsupported.
- Fiat deposit, withdrawal, and payment APIs are documentation boundaries only and are not runtime gateway operations.

## P4 Advanced Order Unsupported Boundary (2026-06-09)

`amend_order`、`place_order_list`、`batch_place_orders`、`batch_cancel_orders`、`cancel_all_orders` 不提升 runtime。原因是 CoinSpot 原生 order/cancel/readback 依赖 buy/sell side 或订单上下文，shared advanced-order request 无法安全携带这些上下文；generic batch 或 OCO/OTO 会产生错侧撤单/改单风险。

可执行证据：

- `endpoint_mapping.yaml` 的五个高级订单 operation 均是 `support: unsupported`。
- `tests/fixtures/exchanges/coinspot/unsupported_boundary.json` 固定 side-specific API 边界、capability flags 和 runtime `Unsupported.operation` 名称。
- `private_tests.rs` 覆盖 capabilities flags、batch capability unsupported 和实际 runtime guard。

官方核验见 [产品线官方核验 P6 剩余区域现货 CEX](../产品线官方核验_P6_剩余区域现货_CEX.md)。

Validation:

- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/coinspot/endpoint_mapping.yaml`
- `python3 -m json.tool tests/fixtures/exchanges/coinspot/...`
- `cargo test -p rustcta-exchange-gateway coinspot --lib --message-format short` once unrelated parallel adapters compile.
## P2 Core Trading Boundary (2026-06-09)

P2 core cancel/query are explicit unsupported shared-runtime boundaries. Native CoinSpot cancel/query surfaces are buy/sell side-context dependent, while the shared request model does not carry preserved side/order context; generic runtime would risk wrong-side cancellation or readback.

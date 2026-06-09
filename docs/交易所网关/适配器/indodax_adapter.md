# Indodax Adapter

Scope: spot IDR markets only.

Implemented public REST:

- `GET /api/pairs` for IDR symbol rules.
- `GET /api/{pair}/depth` for order book snapshots.

Implemented private REST:

- `POST /tapi` with `method=getInfo` for balances.
- `POST /tapi` with `method=trade` for spot market/limit orders.
- `POST /tapi` with `method=cancelOrder` for single-order cancel by exchange order ID.
- `POST /tapi` with `method=getOrder`, `openOrders`, and `tradeHistory` for order/fill parsers.

Signing:

- Private requests are form-encoded.
- Headers are `Key` and `Sign`.
- `Sign` is lowercase hex HMAC-SHA512 over the exact form body containing `method`, `nonce`, and operation parameters.

Official Core Trading Detail:

官方核心交易核验见 [核心交易官方核验 P1 第二批](../核心交易官方核验_P1_第二批.md)。Indodax TAPI/PDF 支持 `trade`、`cancelOrder`、`openOrders`、`orderHistory`、`tradeHistory`；当前项目下单已有运行证据，但通用撤单仍写 unsupported。

这不是交易所不支持撤单，而是项目需要把 `cancelOrder` 的 side/pair/order id 上下文补进 runtime、request spec、parser 和撤单后对账。

Boundaries:

- Runtime support is limited to `MarketType::Spot`.
- Official P6 product-line verification found only IDR spot public/private API
  surfaces; standard futures/perpetual/options are `交易所不支持合约`.
- Public/private streams are unsupported; the adapter is REST-only. 当前官方 PDF/GitHub API 未见公共订单簿 WS，写 `交易所不支持公共 WS 行情`，并在 YAML 记录 `websocket.public.support: unsupported`。
- Client order IDs, amend orders, order lists, cancel-all, positions, leverage/margin, reduce-only, post-only, stop orders, IOC/FOK, transfers, deposits, withdrawals, fiat banking, and payment APIs are explicitly unsupported.
- Fiat IDR is treated only as a spot quote asset in balances/orders; fiat payment and withdrawal flows are not represented in runtime APIs.
- Generic cancel and batch-cancel are unsupported because Indodax `cancelOrder` requires buy/sell side context that `CancelOrderRequest` does not carry.

Validation:

- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/indodax/endpoint_mapping.yaml`
- `python3 -m json.tool tests/fixtures/exchanges/indodax/...`
- `cargo test -p rustcta-exchange-gateway indodax --lib --message-format short` once unrelated parallel adapters compile.

## Fee Boundary

交易所不支持当前费率接口 runtime：当前 Indodax TAPI profile 不暴露稳定 fee runtime。
## P2 Core Trading Boundary (2026-06-09)

P2 core cancel_order is an explicit unsupported shared-runtime boundary. Native Indodax cancelOrder requires buy/sell side, and the shared CancelOrderRequest has no side field; the internal helper no longer defaults to buy.

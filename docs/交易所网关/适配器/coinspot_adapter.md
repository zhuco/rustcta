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

当前项目下单已有运行证据，但通用 cancel 在矩阵里缺实现证据；这应写 `项目未实现/证据未补齐撤单`，不能写成 `交易所不支持撤单`。后续要补 side-specific cancel runtime/fixture，或修正 mapping evidence 并完成 buy/sell cancel 对账。

Explicit boundaries:

- Non-AUD markets are rejected as unsupported.
- Streams are unsupported；当前官方 API/V2 API 未见公共订单簿 WS，写 `交易所不支持公共 WS 行情`。
- Positions are unsupported because CoinSpot is spot-only.
- Standard futures/perpetual/options/margin are `交易所不支持合约` under the current official API scope.
- Cancel, amend, query-order, and batch operations are unsupported because CoinSpot exposes side-specific order APIs that are not safely mapped from generic gateway requests without side/order-context guarantees.
- Fill history filters and pagination are unsupported.
- Fiat deposit, withdrawal, and payment APIs are documentation boundaries only and are not runtime gateway operations.

官方核验见 [产品线官方核验 P6 剩余区域现货 CEX](../产品线官方核验_P6_剩余区域现货_CEX.md)。

Validation:

- `python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/coinspot/endpoint_mapping.yaml`
- `python3 -m json.tool tests/fixtures/exchanges/coinspot/...`
- `cargo test -p rustcta-exchange-gateway coinspot --lib --message-format short` once unrelated parallel adapters compile.

# HTX Gateway Adapter

Status: `rustcta-exchange-gateway` Spot + USDT perpetual REST adapter with offline public/private WebSocket payload and parser coverage.

## Scope

- Adapter id: `htx`
- CCXT id: `htx`
- Spot REST: `https://api.huobi.pro`
- USDT perpetual REST: `https://api.hbdm.com`
- Spot public WebSocket: `wss://api.huobi.pro/ws`
- USDT perpetual public WebSocket: `wss://api.hbdm.com/linear-swap-ws`
- Spot private WebSocket: `wss://api.huobi.pro/ws/v2`
- USDT perpetual private WebSocket: `wss://api.hbdm.com/linear-swap-notification`
- Market types: `Spot`, `Perpetual`

## Endpoint Mapping

- Spot symbol rules: `GET /v1/common/symbols`
- Perpetual symbol rules: `GET /linear-swap-api/v1/swap_contract_info`
- Spot order book: `GET /market/depth`
- Perpetual order book: `GET /linear-swap-ex/market/depth`
- Spot balances: `GET /v1/account/accounts/{account-id}/balance`; requires `spot_account_id`
- Perpetual balances/positions: `POST /linear-swap-api/v1/swap_cross_account_info`, `/swap_cross_position_info`
- Perpetual orders: `POST /linear-swap-api/v1/swap_cross_order`, `/swap_cross_cancel`, `/swap_cross_cancelall`
- Spot private order reads: `GET /v1/order/orders/{order-id}`, `/v1/order/openOrders`, `/v1/order/matchresults`
- Perpetual private order reads: `POST /linear-swap-api/v1/swap_cross_order_info`, `/swap_cross_openorders`, `/linear-swap-api/v3/swap_cross_matchresults`
- Perpetual batch orders: `POST /linear-swap-api/v1/swap_cross_batchorder`; batch cancel uses `/swap_cross_cancel`
- Funding rate endpoint is documented in mapping as `spec_only` because the shared gateway trait does not currently expose a funding-rate method.

## Authentication

REST auth uses query parameters `AccessKeyId`, `SignatureMethod=HmacSHA256`, `SignatureVersion=2`, `Timestamp`, and `Signature`.

The V2 signature payload is:

```text
METHOD
host
path
sorted_urlencoded_query
```

The payload is signed with HMAC-SHA256 and Base64 encoded. Signed GET requests include business query parameters in the signed sorted query. Private USDT-swap WebSocket auth uses the same scheme with the configured WS host/path. Spot private WebSocket auth uses the v2.1 payload on `/ws/v2`.

## Explicit Boundaries

- Huobi is implemented as a legacy profile that reuses HTX code but registers under exchange id `huobi`.
- No production WebSocket task runner is started by this adapter; it exposes subscription/auth payloads and parser fixtures for the shared runtime.
- Spot batch orders, spot cancel-all, amend order, order lists, transfer/withdraw, and leverage/margin/position-mode mutation APIs remain explicit `Unsupported` or documented-only boundaries in the gateway API.
- Spot private balances require `HTX_SPOT_ACCOUNT_ID`; the adapter will not infer an account id.
- Live private REST should remain disabled until read-only preflight and live-dry-run are completed with venue-issued scoped API keys.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/htx/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway htx --lib --message-format short
```

## References

- Huobi Spot API Reference: <https://huobiapi.github.io/docs/spot/v1/en/>
- Huobi USDT Margined Contracts API Reference: <https://huobiapi.github.io/docs/usdt_swap/v1/en/>
- HTX USDT-margined swaps API notes: <https://www.htx.com/support/900001603466>

# Deribit Gateway Adapter

Deribit is implemented as a `rustcta-exchange-gateway` adapter for options,
futures, and perpetual contracts. The unified gateway surface covers public REST
symbol rules/order books and private REST balances, positions, order lifecycle,
open orders, fills, and private WebSocket order/position events.

Spot is 项目未实现 Spot, not `交易所不支持现货`: Deribit official API
instrument metadata includes `kind = "spot"` and support docs list Spot
Instruments, while this adapter currently declares only options, futures, and
perpetual contracts.

Spot 边界写入 `spot_product status: project_unimplemented`：当前只接 options/futures/perpetual。补 Spot 前需要 `public/get_instruments kind=spot` 过滤、spot order book/ticker parser，以及可映射到 shared gateway 的 spot private order/open-order/user-trade/account lifecycle。

Options-specific data is intentionally adapter-specific:

- option chain: `DeribitOptionContract`
- greeks: `DeribitGreeksSnapshot`
- settlements: `DeribitSettlementEvent`

These helpers use fixtures under `tests/fixtures/exchanges/deribit/` and do not
extend the shared `ExchangeClient` trait.

Base URLs:

- REST: `https://www.deribit.com`
- WebSocket: `wss://www.deribit.com/ws/api/v2`

Authentication:

- REST uses Deribit OAuth client credentials through `/api/v2/public/auth`, then
  bearer tokens for private endpoints.
- WebSocket uses JSON-RPC `public/auth` before private subscriptions.
- Example config is disabled by default in `config/deribit_gateway_example.yml`.

Public order book WebSocket uses `public/subscribe` channel
`book.{instrument}.{group}.{depth}.{interval}`. The mapping records supported
depths `1`, `10`, and `20`, intervals `raw`, `100ms`, and `agg2`, plus
`change_id`/`prev_change_id` continuity with REST `/api/v2/public/get_order_book`
snapshot rebuild after gaps.

Boundaries:

- Portfolio margin, option settlement operations, transfers, and withdrawals are
  read-only audit boundaries and are not wired into trading runtime.
- Batch place/cancel is composed from single-order endpoints because the common
  gateway surface does not rely on a native atomic Deribit batch primitive.
- Advanced-order matrix entries declare `batch_place_orders` and
  `batch_cancel_orders` as `composed` over `/api/v2/private/buy|sell` and
  `/api/v2/private/cancel`. `amend_order` is native runtime over
  `/api/v2/private/edit`; `place_order_list` remains an explicit unsupported
  shared-model boundary for this P4 audit pass.

Validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/deribit/endpoint_mapping.yaml
cargo test -p rustcta-exchange-gateway deribit --lib --message-format short
```

## P2 Product Line Boundary (2026-06-09)

`spot_product` is an official-source project boundary, not an exchange-unsupported row. Deribit exposes instruments with `kind=spot`, while this adapter is scoped to options, futures, and perpetual instruments/order lifecycle.

Do not promote Spot runtime from derivative parsers. Promotion requires `kind=spot` instrument filtering, spot order-book/ticker public parsers, private spot buy/sell/open-orders/user-trades specs, balance scope audit, and spot order reconciliation.

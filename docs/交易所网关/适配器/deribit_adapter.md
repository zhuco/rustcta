# Deribit Gateway Adapter

Deribit is implemented as a `rustcta-exchange-gateway` adapter for options,
futures, and perpetual contracts. The unified gateway surface covers public REST
symbol rules/order books and private REST balances, positions, order lifecycle,
open orders, fills, and private WebSocket order/position events.

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

Boundaries:

- Portfolio margin, option settlement operations, transfers, and withdrawals are
  read-only audit boundaries and are not wired into trading runtime.
- Batch place/cancel is composed from single-order endpoints because the common
  gateway surface does not rely on a native atomic Deribit batch primitive.

Validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/deribit/endpoint_mapping.yaml
cargo test -p rustcta-exchange-gateway deribit --lib --message-format short
```

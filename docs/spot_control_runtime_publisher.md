# Spot Control Runtime Publisher

`SpotControlRuntimePublisher` is the production-oriented server-side runtime feed for Spot symbol control. It is optional and disabled by default. When enabled, it polls authoritative private account state through `ExchangeClient` and reads public market state from the existing WebSocket `BookCache`.

The publisher builds `SpotControlRuntimeSnapshot` values with the existing `SpotControlSnapshotBuilder`, persists them by `snapshot_id`, and publishes them to the control read model. Browser request bodies are operator intent only; they are never accepted as balances, fees, books, orders, reservations, or ownership state.

Runtime sources:

- `ExchangeClient::get_balances` for account-wide balances.
- `ExchangeClient::get_open_orders(None)` where supported for account-wide open orders.
- `ExchangeClient::get_recent_fills(symbol)` where supported for ownership/reconciliation hints.
- `ExchangeClient::load_symbol_rules` and `get_fee_rate` as refresh/probe surfaces.
- `UnifiedSymbolRegistry`, `BookCache`, `BookHealth`, `FeeModel`, `DisabledRegistry`, `BalanceReservationManager`, reconciliation reports, `KillSwitch`, `LivePreflight`, `SmallLiveGate`, and recorder health.

The publisher never calls `place_order`, `cancel_order`, or any liquidation execution path. Live order submission remains blocked.

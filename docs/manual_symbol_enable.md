# Manual Symbol Enable

Operators must explicitly enable each Spot to Spot symbol. Automatic discovery must not enable trading.

Example:

```json
{
  "symbol": "AURORAUSDT",
  "mode": "LiveDryRun",
  "selected_exchanges": ["gateio", "bitget"],
  "allowed_directions": [
    { "buy_exchange": "gateio", "sell_exchange": "bitget" },
    { "buy_exchange": "bitget", "sell_exchange": "gateio" }
  ],
  "max_notional_per_trade": 3,
  "max_total_symbol_exposure": 10,
  "expected_version": 0
}
```

Wildcard exchange selection is forbidden. Validation checks adapter support, Spot support, symbol existence, tradable status, symbol mapping, tick and step rules, min quantity, min notional, fresh public book, fee model, balance snapshot if available, DisabledRegistry, unmanaged conflicts, KillSwitch, LivePreflight, and SmallLiveGate for `FutureSmallLive`.

Inventory is reviewed before activation. `ObserveOnly`, `Paper`, and `LiveDryRun` may proceed with inventory warnings. `FutureSmallLive` requires sufficient real inventory for every enabled direction and is disabled by default.

# Manual Symbol Disable

Disable starts by moving the symbol to `DisableRequested`, which blocks new arbitrage opportunities immediately. The workflow acquires a symbol operation lock, identifies strategy orders, optionally cancels them, loads balances, excludes unmanaged inventory, and builds a freeze or liquidation plan.

Modes:

- `FreezeOnly`: stop openings, optionally cancel strategy maker orders, preserve managed and unmanaged inventory.
- `MarketLiquidate`: build slippage-protected IOC limit sell plans. Unsafe unbounded market orders are forbidden.
- `PassiveAskLiquidate`: build PostOnly passive sell sessions near best ask. Normal limit downgrade is forbidden.

If cancellation, reconciliation, order status, loss, slippage, stale book, or lock handling becomes unknown, the workflow escalates to `ManualInterventionRequired`.

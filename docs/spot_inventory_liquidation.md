# Spot Inventory Liquidation

Liquidation only considers managed sellable base inventory:

`base_available - base_unmanaged - base_reserved`

Unmanaged positions are never sold automatically. Dust is recorded instead of repeatedly submitting invalid orders. `DisabledClean` means no managed sellable inventory remains; `DustRemaining` is an acceptable completed disable state.

`MarketLiquidate` uses IOC limit sell plans with worst allowed price derived from maximum slippage. IOC limit orders are safer than unbounded market orders because they cap price impact and prevent unlimited crossing through a thin or stale book.

`PassiveAskLiquidate` requires PostOnly support. PostOnly must not be silently downgraded because a crossing downgrade can convert a passive liquidation into a taker sale with unexpected slippage and fees.

Current implementation produces dry-run liquidation plans and sessions only. Real submission remains blocked.

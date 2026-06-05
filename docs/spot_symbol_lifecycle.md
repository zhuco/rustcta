# Spot Symbol Lifecycle

The monitoring dashboard is read-only telemetry. The spot control plane adds authenticated operator writes for Spot to Spot symbol lifecycle management.

Each managed spot symbol has one authoritative `SpotSymbolLifecycleState`: `Discovered`, `Disabled`, `EnableRequested`, `EnableValidating`, `InventoryReviewRequired`, `Ready`, `Active`, `Paused`, `DisableRequested`, `CancelingOrders`, `Frozen`, `LiquidationPlanning`, `LiquidatingMarket`, `LiquidatingPassive`, `DustRemaining`, `DisabledWithInventory`, `DisabledClean`, `Failed`, or `ManualInterventionRequired`.

Only `Active` may create new arbitrage opportunities. `Paused`, `Frozen`, liquidation, failed, and manual-intervention states block openings immediately. Invalid transitions fail safely and are audited.

Lifecycle state is persisted in JSONL at `spot_symbol_control.lifecycle_store_path`. Commands and audits are separately persisted so restart does not forget requested operations.

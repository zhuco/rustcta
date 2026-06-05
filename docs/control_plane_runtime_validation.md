# Control Plane Runtime Validation

Enable, disable, inventory review, and liquidation preview use authoritative runtime snapshots.

Direction readiness evaluates:

- buy quote available
- sell base available
- buy and sell book freshness
- buy and sell symbol tradability
- fee source quality
- DisabledRegistry state
- operation lock state
- KillSwitch and live readiness gates

`ObserveOnly` may continue with limited data. `LiveDryRun` requires symbol rules and valid books. `FutureSmallLive` must not be marked ready from fallback or manually assumed data.

Liquidation previews use managed sellable inventory only, fresh bid-side depth for IOC plans, PostOnly support for passive plans, symbol precision and minimums, and effective fees. `would_submit_order` remains false.

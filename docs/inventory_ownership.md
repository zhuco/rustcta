# Inventory Ownership

Spot control may only manage inventory that is explicitly attributable to the controlled Spot to Spot strategy.

Ownership formula:

```text
effective_sellable_managed_quantity =
  max(spot_control_managed_quantity - exchange_locked - locally_reserved, 0)
```

`spot_control_managed_quantity` excludes unmanaged positions and other-strategy owned inventory. Unknown ownership is not liquidated and should move the workflow to inventory review or manual intervention.

The dashboard exposes total balance, exchange locked quantity, local reservations, unmanaged quantity, other-strategy quantity, managed quantity, and effective sellable managed quantity.

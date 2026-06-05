# Unified Account Capital Model

The capital model supports:

- Separate spot and futures accounts.
- Unified accounts.
- Cross-margin accounts.
- Isolated-margin accounts.

Required capital includes spot cash, spot inventory, perpetual initial margin, maintenance margin, liquidation buffer, collateral requirement, fragmented-capital penalty, and any explicit unified-account benefit.

Unified collateral is only used when exchange capability explicitly supports it. Collateral haircuts are applied and unknown capability falls back to conservative separate-account requirements. The model does not treat every asset as full-value collateral and does not assume profit on one venue can cover loss on another venue.

Return on capital is:

```text
expected_lifecycle_net_pnl / effective_total_capital_required_usdt
```

Return on capital per hour is:

```text
expected_return_on_capital / expected_holding_hours
```

Unified accounts can improve capital efficiency, but they still carry mark-price, index-price, margin, liquidation, borrow, and collateral-haircut risk.

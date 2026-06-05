# SpotPerp Basis Model

SpotPerp relationships compare a spot leg with a perpetual leg, commonly buy spot and short perpetual when the perp trades rich.

Entry basis is:

```text
(perp_sell_vwap - spot_buy_vwap) / spot_buy_vwap * 10000
```

Entry basis is not immediately realized profit. It is an open lifecycle exposure that depends on convergence, funding, fees, exit execution, margin risk, and residual risk.

The expected lifecycle model includes:

- Expected convergence from entry basis to expected exit basis.
- Expected funding income or cost during the configured holding period.
- Spot and perp entry fees.
- Expected spot and perp exit fees.
- Entry and exit slippage.
- Margin risk cost and liquidation buffer.
- Residual risk buffer and safety buffer.

Funding improves the expected return only when the held direction receives funding. Missing funding data lowers confidence and should be treated conservatively.

# Market-Regime Hedge Recommendation

The market-regime classifier is explainable and rule-based. Inputs may include BTC moving-average state, drawdown, realized volatility, funding levels, breadth, inventory PnL, and concentration.

Missing data produces `Unknown` with low confidence. The model does not claim exact bull-market stage certainty.

Example behavior:

- Strong uptrend: no or low hedge, subject to inventory limits.
- Moderate uptrend: partial hedge only if concentration or volatility is elevated.
- Weakening trend or high volatility: higher hedge consideration.
- Downtrend: strong hedge consideration or inventory reduction.

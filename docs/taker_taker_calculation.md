# TakerTaker Calculation

TakerTaker is the executable baseline because both legs use immediately available order book liquidity.

Buy pricing:

- Consume asks from lowest ask upward.
- Calculate executable quantity, notional, VWAP, worst price, levels consumed, and slippage versus ask1.

Sell pricing:

- Consume bids from highest bid downward.
- Calculate executable quantity, notional, VWAP, worst price, levels consumed, and slippage versus bid1.

The raw executable spread is:

```text
(sell_vwap - buy_vwap) / buy_vwap * 10000
```

The immediate net spread is:

```text
raw_executable_spread_bps
- buy_taker_fee_bps
- sell_taker_fee_bps
- buy_slippage_bps
- sell_slippage_bps
- latency_buffer_bps
- residual_risk_buffer_bps
```

The opportunity is rejected if either side cannot fully execute the same quantity, if a book is stale, if depth is insufficient, if symbol rules fail, or if step-size rounding differs beyond tolerance.

For SpotSpot, immediate net may represent a cross-exchange inventory arbitrage entry, but inventory rebalance cost still belongs in lifecycle analytics. For SpotPerp and PerpPerp, immediate entry spread is basis or spread exposure, not realized profit.

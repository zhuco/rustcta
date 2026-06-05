# PerpPerp Spread Model

PerpPerp relationships compare two perpetual markets, typically long the cheaper perp and short the richer perp.

Entry spread is:

```text
(short_perp_sell_vwap - long_perp_buy_vwap) / long_perp_buy_vwap * 10000
```

The lifecycle estimate includes four fee legs: long entry, short entry, long exit, and short exit. It also includes entry slippage, expected exit slippage, funding-rate difference, margin risk, liquidation buffers, residual loss, and a safety buffer.

Funding can dominate small spreads. The short leg may receive or pay funding depending on exchange sign convention and market state, and the long leg has the opposite exposure. Missing or stale funding data lowers confidence.

PerpPerp capital is fragmented across venues unless explicit unified-account support and collateral rules are known. Profit on one exchange is not assumed to cover loss or margin stress on another exchange.

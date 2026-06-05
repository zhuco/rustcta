# Exchange Pair Statistics

Statistics are tracked per directed pair, symbol, and target notional. Direction matters: Gate.io buy to Bitget sell is distinct from Bitget buy to Gate.io sell.

The model tracks observations, fresh-book observations, positive TakerTaker count and rate, average/median/p90/max net bps, depth failures, stale-book count, fee fallback count, and MakerTaker theoretical/expected positives.

Low sample counts remain low confidence. Estimated opportunity PnL is not realized profit.

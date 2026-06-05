# Arbitrage Relationship Analytics

The unified arbitrage analytics layer evaluates SpotSpot, SpotPerp, and PerpPerp relationships from normalized exchange snapshots. It is a scanning, recording, dashboard, paper, and `live_dry_run` tool only. It does not submit orders and it does not enable maker/taker execution.

The shared model lives in `src/strategies/arbitrage_core/` and separates:

- `ArbitrageRelationshipType`: `SpotSpot`, `SpotPerp`, `PerpPerp`.
- `ExecutionModeCandidate`: `TakerTaker`, `MakerBuyTakerSell`, `TakerBuyMakerSell`.
- `MarketLeg`: normalized exchange, market type, internal symbol, exchange symbol, side, order book, fee rate, symbol rule, optional funding, and optional margin data.
- `ArbitrageRelationship`: buy leg, sell leg, base/quote assets, and optional settlement asset.

The scanner evaluates every configured relationship and records accepted and rejected opportunities. A rejection is still valuable because it explains which constraint blocked the opportunity: stale books, insufficient depth, symbol-rule mismatch, quantity-rounding mismatch, fee fallback, low confidence, or configured minimum thresholds.

Last trade and mid prices are not used for profit calculations. They are not executable. Taker analysis consumes asks for buys and bids for sells, then computes VWAP for the configured notional or quantity.

Dashboard fields should be interpreted as follows:

- Raw executable spread is a price relationship, not profit.
- TakerTaker immediate net is the executable entry baseline after fees, slippage, latency, and residual buffers.
- TakerTaker lifecycle expected net is an estimate that includes exit, rebalance, funding, margin, and safety costs.
- MakerTaker theoretical net is non-executable and only shows what a passive quote plus taker hedge could look like.
- MakerTaker expected net applies fill probability, adverse selection, residual loss, cancel/replace cost, and inventory risk.

Early trial data should be used to validate book quality, fee assumptions, depth stability, fillability estimates, and lifecycle assumptions before any live order path is considered.

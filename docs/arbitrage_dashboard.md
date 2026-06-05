# Arbitrage Dashboard

The monitoring backend exposes arbitrage analytics as read-only endpoints:

- `GET /api/arbitrage/relationships`
- `GET /api/arbitrage/opportunities`
- `GET /api/arbitrage/opportunities/{id}`
- `GET /api/arbitrage/rankings`
- `GET /api/arbitrage/fees`
- `GET /api/arbitrage/capital-efficiency`
- `GET /api/arbitrage/statistics`

Opportunity rows include relationship type, symbol, buy/sell venue, market type, buy VWAP, sell VWAP, raw executable spread, TakerTaker immediate net, TakerTaker lifecycle expected net, MakerTaker theoretical net, MakerTaker expected net, entry fees, expected exit fees, funding, rebalance cost, required capital, return on capital, confidence, score components, book age, warnings, and rejection reasons.

Supported filters include relationship type, symbol, exchange, minimum TT net bps, minimum lifecycle expected net bps, minimum return on capital per hour, confidence level, accepted/rejected, and fresh-only books.

Visual interpretation:

- Raw spread should not be styled as profit.
- TT immediate net should be visually distinct from raw spread.
- Lifecycle expected net should be marked as an estimate.
- MakerTaker theoretical should be marked non-executable.
- MakerTaker expected should show confidence.

Operators should watch rejected records as closely as accepted records during early trials. Rejections expose missing depth, stale books, low fee confidence, and model assumptions before capital is at risk.

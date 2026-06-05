# Five-Exchange Spot Scanner

The five-exchange scanner evaluates Spot to Spot opportunities across Gate.io, Bitget, MEXC, CoinEx, and KuCoin. It is analytics-only. It does not submit orders, cancel orders, enable symbols, or execute MakerTaker.

The scanner uses WebSocket-fed books from `src/data::BookCache`, exchange
symbol rules, and `FeeModel`. It does not subscribe to exchange streams itself;
the data layer owns acquisition and freshness. For each common symbol it
evaluates every directed buy/sell exchange pair and the configured notionals:
1, 2, 5, 10, 25, and 50 USDT.

TakerTaker uses executable VWAP:

- Buy leg consumes asks.
- Sell leg consumes bids.
- Both legs use the same executable quantity.
- Fees, stale-book warnings, shallow depth, rebalance cost, and residual-risk estimates reduce the result.

MakerTaker values are theoretical and expected-value analytics only. A profitable MakerTaker expected value does not make an opportunity executable.

Gate.io and Bitget are the only future live-dry-run candidate pair. MEXC, CoinEx, and KuCoin are scan-only in this task.

# Symbol Management

RustCTA keeps the existing market symbol utilities, and adds a unified exchange symbol registry in
`src/exchanges/symbol_registry.rs` for Spot and Perpetual order validation.

`SymbolKey` is keyed by:

- exchange
- market type
- internal symbol

The registry also exposes typed projections:

- `SpotSymbol` for spot instruments
- `PerpetualSymbol` for perpetual instruments
- `SymbolInstrument` when code wants one enum over both

Spot example:

- internal symbol: `BTCUSDT`
- base asset: `BTC`
- quote asset: `USDT`
- market type: `Spot`
- MEXC symbol: `BTCUSDT`
- OKX symbol: `BTC-USDT`
- GateIO symbol: `BTC_USDT`

Perpetual example:

- internal symbol: `BTCUSDT`
- base asset: `BTC`
- quote asset: `USDT`
- settlement asset and contract size live in `SymbolRule.raw_metadata` until all adapters expose a
  richer common instrument model.
- OKX swap symbol: `BTC-USDT-SWAP`

The registry supports:

- `internal_to_exchange(exchange, market_type, internal_symbol)`
- `exchange_to_internal(exchange, market_type, exchange_symbol)`
- symbol rule lookup
- supported symbols by exchange and market type
- order validation against symbol status, order type, time-in-force, tick size, step size, minimum
  quantity, minimum notional, and client order ID policy
- exchange-scoped order validation through `validate_order_for_exchange()`

Optional overrides are documented in `config/symbol_mappings.yml`.

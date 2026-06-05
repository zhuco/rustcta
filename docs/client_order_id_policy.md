# Client Order ID Policy

RustCTA now centralizes client order ID generation in `src/exchanges/client_order_id.rs`.

The shared generator is the default path for new unified order flows. If a caller supplies a
`client_order_id`, adapters should validate it with `validate_client_order_id()` before sending it
to an exchange.

Format uses conservative exchange and market prefixes:

- `BN_SPT_ARB_<timestamp>_<counter><suffix>`
- `BN_PERP_ARB_<timestamp>_<counter><suffix>`
- `MX_SPT_ARB_<timestamp>_<counter><suffix>`
- `CX_SPT_ARB_<timestamp>_<counter><suffix>`
- `OKX_PERP_ARB_<timestamp>_<counter><suffix>`

When a venue policy has a short maximum length, the generator automatically compacts the ID while
preserving exchange, market, strategy, timestamp, and a random suffix.

Current policy notes:

- Policies are conservative where exact venue-specific production constraints are not encoded.
- Live exchange policies currently require uppercase ASCII alphanumeric plus underscore.
- Paper uses a separate compatibility policy that allows lowercase hyphenated RustCTA idempotency
  IDs such as `crossarb-ls-mk-1-deadbeef`.
- Duplicate client IDs are treated as unsafe and should be avoided globally.
- MEXC Spot, CoinEx Spot, Binance Spot, OKX Spot, Paper, and the legacy compatibility wrapper use
  this central generator when an order request omits `client_order_id`.
- Caller-provided IDs are validated before submission; invalid values are rejected before an API
  request is built.
- Validation is currently format-based. Prefix-to-exchange matching is intentionally not enforced
  yet so existing explicit IDs can migrate gradually.

To add a new exchange, extend `policy_for()` with the venue's max length, allowed characters,
market support, and documented duplicate behavior.

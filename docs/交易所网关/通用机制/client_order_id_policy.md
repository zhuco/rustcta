# Client Order ID Policy

RustCTA now centralizes client order ID capability and reconciliation policy in
`crates/rustcta-exchange-api/src/order.rs` and
`crates/rustcta-exchange-gateway/src/reconciliation.rs`.

`ClientOrderIdPolicy` defines the shared validation envelope. If a caller
supplies a `client_order_id`, adapters should validate it against venue
constraints before sending it to an exchange.

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
- Venue adapters should either preserve the caller-supplied `client_order_id`
  or explicitly declare `supports_client_order_id=false`.
- Caller-provided IDs are validated before submission; invalid values are rejected before an API
  request is built.
- Validation is currently format-based. Prefix-to-exchange matching is intentionally not enforced
  yet so existing explicit IDs can migrate gradually.

To add a new exchange, document the venue's max length, allowed characters,
market support, and duplicate behavior in its adapter capability profile and
fixtures.

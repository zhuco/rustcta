# Bitget Read-Only Validation

Create a Bitget Spot API key with read/account permissions only. Disable trading and withdrawal permissions for this validation key. The Bitget adapter requires a passphrase for authenticated requests.

The Bitget test validates:

- `get_balances` normalization from Spot account assets.
- `get_open_orders` mapping for IDs, client IDs where present, side, type, price, quantity, filled quantity, remaining quantity, and status.
- `get_recent_fills` mapping when supported and populated.
- Public Spot order-book WebSocket snapshots into `BookCache`.
- Local reservation separation from exchange-locked balance.
- Preservation of last-known balances on temporary poll failure.

Run:

```bash
ENABLE_LIVE_READONLY_TESTS=true \
BITGET_API_KEY=... \
BITGET_API_SECRET=... \
BITGET_API_PASSPHRASE=... \
LIVE_TEST_SYMBOL=BTCUSDT \
cargo test --all-features --test live_readonly_bitget -- --ignored --nocapture
```

The test never probes trade permission by sending an order. If permission introspection is unavailable, permission status is recorded as `unknown`, not `pass`.

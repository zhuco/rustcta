# Gate.io Read-Only Validation

Create a Gate.io Spot API key with read/account permissions only. Disable trading and withdrawal permissions for this validation key. If Gate.io does not expose permission introspection through the adapter, the report marks permission status as `unknown`; the operator must verify permissions in the Gate.io UI before trusting the key.

The Gate.io test validates:

- `get_balances` normalization from Spot account balances.
- `get_open_orders` mapping for IDs, client IDs where present, side, type, price, quantity, filled quantity, remaining quantity, and status.
- `get_recent_fills` mapping when the endpoint returns fills.
- Public Spot order-book WebSocket snapshots into `BookCache`.
- Local reservation separation from exchange-locked balance.
- Preservation of last-known balances on temporary poll failure.

Run:

```bash
ENABLE_LIVE_READONLY_TESTS=true \
GATEIO_API_KEY=... \
GATEIO_API_SECRET=... \
LIVE_TEST_SYMBOL=BTCUSDT \
cargo test --all-features --test live_readonly_gateio -- --ignored --nocapture
```

No order is created to manufacture open-order or fill data. If the account has no open orders or no recent fills, the test passes with a warning and records that the mapping was not fully observed.

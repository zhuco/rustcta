# Live Read-Only Exchange Validation

These tests validate real Gate.io Spot and Bitget Spot account semantics before any live execution path is implemented. They are ignored by default and are read-only only.

Run only with explicit opt-in:

```bash
ENABLE_LIVE_READONLY_TESTS=true \
LIVE_TEST_SYMBOL=BTCUSDT \
LIVE_TEST_DURATION_SECONDS=120 \
cargo test --all-features --test live_readonly_gateio -- --ignored --nocapture
```

```bash
ENABLE_LIVE_READONLY_TESTS=true \
LIVE_TEST_SYMBOL=BTCUSDT \
LIVE_TEST_DURATION_SECONDS=120 \
cargo test --all-features --test live_readonly_bitget -- --ignored --nocapture
```

```bash
ENABLE_LIVE_READONLY_TESTS=true \
LIVE_TEST_SYMBOL=BTCUSDT \
LIVE_TEST_DURATION_SECONDS=300 \
cargo test --all-features --test live_runtime_publisher -- --ignored --nocapture
```

Required opt-in:

- `ENABLE_LIVE_READONLY_TESTS=true`

Credentials:

- Gate.io: `GATEIO_API_KEY`, `GATEIO_API_SECRET`
- Bitget: `BITGET_API_KEY`, `BITGET_API_SECRET`, `BITGET_API_PASSPHRASE`

Optional settings:

- `LIVE_TEST_SYMBOL`, default `BTCUSDT`
- `LIVE_TEST_MARKET_TYPE`, only `spot` is accepted
- `LIVE_TEST_DURATION_SECONDS`, default `120`
- `LIVE_TEST_OUTPUT_DIR`, default `data/live_readonly_tests`
- `LIVE_TEST_REQUIRE_NO_WITHDRAW_PERMISSION`, default `true`
- `LIVE_TEST_MAX_REST_REQUESTS_PER_MINUTE`, default `30`

The tests call read-only account endpoints for balances, open orders, fees, symbol metadata, and recent fills where supported. They subscribe to public order-book WebSockets and feed the shared `BookCache`. They do not place, cancel, modify, transfer, or withdraw.

Reports are written to `data/live_readonly_tests/` and are sanitized. They intentionally omit exact balance quantities, credentials, signatures, authorization headers, and raw private payloads.

Warnings mean the venue semantics could not be fully observed, for example no open orders or no recent fills. Critical errors mean an assumption used by the control plane was violated or the mutation guard detected a prohibited method call.

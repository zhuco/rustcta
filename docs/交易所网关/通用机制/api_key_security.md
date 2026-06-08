# API Key Security

Use dedicated validation keys for live read-only tests. Do not reuse execution keys.

Required key properties:

- Account/read permission enabled.
- Spot trade permission disabled.
- Withdrawal permission disabled.
- Transfer permission disabled where the exchange separates it.
- IP allowlisting enabled when practical.
- Key rotated after validation if it was exposed to a shared environment.

The validation harness never prints or persists API keys, secrets, passphrases, signatures, authorization headers, or private raw payloads. Reports contain status, warnings, counts, and latency summaries only.

If `LIVE_TEST_REQUIRE_NO_WITHDRAW_PERMISSION=true`, a detected withdrawal permission is critical. If permission introspection is unsupported, the report records `unknown`; the operator must verify permissions manually in the exchange UI.

Before any small live execution work begins, the read-only reports must show:

- Credentials valid for read-only account endpoints.
- Withdrawal permission not enabled, or manually verified disabled when introspection is unavailable.
- No mutation calls detected.
- No false zero balance after poll failures.
- No unknown/manual inventory treated as liquidatable.
- Runtime snapshots persisted and replayable without network calls.

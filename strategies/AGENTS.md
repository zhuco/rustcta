# Strategy Crate Rules

Strategies under this directory are independent industrial strategy crates.

Current migration focus:

- The active workstream is extracting strategy crates and stabilizing the
  strategy/runtime boundary for the industrial workspace.
- Do not add exchange endpoint support, venue-specific API behavior, signing,
  credential handling, or concrete adapter code from strategy tasks. Exchange
  API details are scheduled separately.
- During the split, prefer preserving strategy behavior while making ownership,
  manifests, schemas, and snapshots explicit.

Rules:

- Use `rustcta-strategy-sdk` as the entrypoint.
- Submit orders through the strategy context/execution client.
- Depend on generic libraries and `rustcta-strategy-sdk`; avoid direct platform
  crates unless a documented SDK contract change requires it.
- Do not import concrete exchange adapter modules.
- Do not depend on `rustcta-exchange-api`, `rustcta-exchange-gateway`,
  `rustcta-execution-router`, `rustcta-control-api`, `rustcta-core-compat`, or
  the legacy root `rustcta` package.
- Do not read `.env` or exchange API credentials.
- Do not bypass execution router, risk gates, or idempotency identity.
- Do not encode venue-specific REST/WebSocket endpoints, request signing, or
  adapter quirks in strategies. Model strategy intent and let the platform
  route it.
- Every strategy must expose a `StrategySpec`, config schema, snapshot schema,
  and secret-free snapshots before it is production-eligible.

Ownership:

- Each subdirectory owns one strategy crate, its manifest, config schema,
  snapshot schema, runtime state, and strategy-local tests.
- Shared strategy ergonomics belong in `rustcta-strategy-sdk`, not copied
  between strategy crates.
- Cross-strategy behavior must first be expressed as an SDK/platform contract
  before strategies consume it.

Parallel work guidance:

- One AI task should change one strategy crate at a time unless the task is a
  mechanical SDK contract migration across all strategies.
- Strategy tasks may update `strategies/AGENTS.md` or boundary checks when they
  discover a repeatable ownership rule.
- If a strategy change requires exchange API shape changes or concrete adapter
  behavior, stop at the SDK/platform contract and hand off the venue-specific
  work.

Required checks:

```bash
cargo check -p <strategy-crate>
cargo test -p <strategy-crate>
scripts/check_industrial_boundaries.sh
```

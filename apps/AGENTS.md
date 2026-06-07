# App Boundary Rules

Apps are thin executable composition layers.

Current migration focus:

- The active workstream is the industrial directory split and app/crate
  composition boundary.
- Do not add exchange endpoint support, signing behavior, venue quirks, or
  adapter implementation details in apps. Exchange/API details are a separate
  workstream.
- App changes should mainly wire existing crates, parse process-level
  configuration, and expose runnable binaries for the new layout.

Rules:

- Keep business logic in `crates/`.
- Apps may parse CLI/env and start services.
- Gateway app may start credential-owning gateway services.
- Supervisor app may manage strategy processes.
- Control API app may serve HTTP routes and static assets.
- Apps should not define core domain models.
- Apps should not depend on strategy crates directly.
- Only `apps/gateway` should depend on `rustcta-exchange-gateway`.
- Apps must not import legacy `rustcta::exchanges`, `crate::exchanges`, or
  concrete adapter modules.
- Apps must not reach into `src/` legacy modules. Use workspace crates.
- Temporary exception: `apps/backtest` may depend on the legacy root `rustcta`
  package only to call `rustcta::backtest::runtime` while backtest code still
  lives under `src/backtest/*`. Do not use this exception for exchange modules,
  strategies, gateway internals, or any other app. Retire it when
  `crates/rustcta-backtest` is extracted.

Ownership:

- `apps/gateway`: process entrypoint for the exchange gateway crate. It owns
  bind address/service startup only; credential loading and adapters stay in
  `rustcta-exchange-gateway`.
- `apps/supervisor`: process entrypoint for strategy process lifecycle through
  `rustcta-supervisor`.
- `apps/control-api`: HTTP process entrypoint for `rustcta-control-api` and
  static UI serving, without raw secret exposure.
- `apps/cli`: operator/developer CLI composition only. Put reusable behavior in
  platform crates before wiring it here.
- `apps/backtest`: offline backtest/research command entrypoint. It may keep a
  narrow compatibility bridge to `rustcta::backtest::runtime` until the
  backtest library is extracted, but it must not add exchange API behavior.

Parallel work guidance:

- Keep app tasks narrow: one binary, its manifest, and the public crate API it
  composes.
- If an app needs business logic, move that logic to the owning crate first and
  keep the app as wiring.
- If an app appears to need a concrete exchange adapter, hand off to the
  gateway/API workstream instead of implementing adapter details here.

Required checks:

```bash
cargo check -p <app-crate>
scripts/check_industrial_boundaries.sh
```

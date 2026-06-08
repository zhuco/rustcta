# Crate Boundary Rules

This directory contains the target industrial RustCTA platform crates.

Current migration focus:

- The active workstream is directory and architecture refactoring for the
  industrial workspace split.
- Do not expand concrete exchange API coverage, venue endpoints, request
  signing, or adapter behavior as part of this refactor. Treat those details as
  a separately scheduled exchange/API workstream.
- Keep crate moves and boundary changes mechanical and reviewable. When a
  boundary needs a new contract, add the smallest platform-facing trait/model
  first and leave venue-specific implementation details in the gateway lane.

General rules:

- Keep crates small and dependency-directed.
- Do not import concrete strategy crates from platform API crates.
- Do not import concrete exchange adapters outside the exchange gateway.
- Do not read `.env` or exchange secrets outside gateway/admin code.
- Persisted or networked models must carry schema/version identity.
- Mutation commands must include tenant, account, strategy, run, command, and
  idempotency identity.
- New industrial crates must not depend on the legacy root `rustcta` package or
  `src/` modules. Use explicit workspace crates and temporary compatibility
  exports only where ownership is documented.

Dependency direction:

- `rustcta-types` is the bottom shared domain layer.
- API/contract crates (`rustcta-exchange-api`, `rustcta-execution-api`,
  `rustcta-event-ledger`) may depend on shared types, but not on apps,
  strategies, routers, gateways, adapters, or compatibility shims.
- `rustcta-exchange-gateway` owns credential-bearing exchange connectivity and
  concrete venue adapters behind `rustcta-exchange-api`.
- `rustcta-execution-router` may depend on execution contracts, exchange
  contracts, event ledger contracts, and the gateway. It owns routing, risk
  gates, idempotency, dry-run, and reconciliation behavior.
- `rustcta-strategy-sdk` is the strategy-facing safe runtime surface. It must
  not depend on the exchange gateway or concrete adapters.
- `rustcta-control-api` is a presentation/control boundary. It may expose
  secret-free status and lifecycle command views, but it must not own order
  routing, strategy logic, or adapter details.
- `retired-core-compat` is temporary migration glue. Do not add new business
  logic there, and do not make new apps or strategies rely on it unless a
  migration handoff explicitly requires it.

Ownership:

- `rustcta-types`: stable shared domain types only.
- `rustcta-exchange-api`: exchange client contracts and request/response
  models only.
- `rustcta-exchange-gateway`: credential-owning gateway, venue adapters,
  signing, rate limits, exchange stream normalization.
- `rustcta-execution-api`: order/cancel/fill/risk/reconciliation protocol.
- `rustcta-execution-router`: dry-run, risk gates, idempotency, order routing,
  reconciliation.
- `rustcta-event-ledger`: append-only event, order, fill, account, audit, and
  replay contracts.
- `rustcta-strategy-sdk`: safe strategy runtime and context interfaces.
- `rustcta-supervisor`: owner for process lifecycle. It includes run/process
  registry records, snapshots, and the first OS child-process start/stop/restart
  implementation. Log tailing APIs, runtime snapshot collection, crash recovery,
  and durable process registry storage remain in this crate.
- `rustcta-control-api`: web/control API view models and routes only.
- `retired-core-compat`: temporary compatibility exports during migration.

Parallel work guidance:

- One AI task should own one crate lane at a time. Cross-crate changes should
  be limited to public contract adjustments plus the smallest downstream
  compile fixes needed for that contract.
- If a change touches exchange contracts or gateway adapter behavior, stop and
  split that into the exchange/API workstream instead of mixing it with
  directory restructuring.
- Prefer adding boundary tests or `scripts/check_industrial_boundaries.sh`
  rules over relying on comments when a new ownership rule can be checked
  cheaply.

Required checks for crate changes:

```bash
cargo check -p <crate>
cargo test -p <crate>
scripts/check_industrial_boundaries.sh
```

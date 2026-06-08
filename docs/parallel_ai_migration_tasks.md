# Parallel AI Migration Tasks

Status date: 2026-06-07

This document splits the remaining industrial workspace migration into 10
parallel AI task packets. Each packet is intended to be assigned to one AI in a
separate working branch/session.

Primary references:

- `docs/industrial_workspace_migration_status.md`
- `docs/industrial_directory_migration_plan.md`
- `docs/control_web_directory_migration_plan.md`
- `docs/backtest_app_migration_plan.md`
- `docs/tools_ops_migration_plan.md`
- `docs/architecture_module_layout.md`

## Global Rules For All 10 AIs

Every AI must follow these rules before editing code:

1. Run `git status --short` and identify unrelated dirty files.
2. Read the relevant plan documents listed in its task.
3. Keep changes inside the task's allowed file scope.
4. Do not revert or overwrite changes made by other agents.
5. Preserve retired binary names and route compatibility unless the task
   explicitly says otherwise.
6. Do not promote raw exchange credential write/delete behavior into the new
   public control API.
7. Do not change exchange signing, REST contract details, websocket protocol
   details, or live order semantics unless the task explicitly owns that area.
8. Prefer thin app binaries and reusable crate modules.
9. Add focused tests for every migrated route, command, DTO, or behavior.
10. End with validation commands and a short migration note.

Recommended common validation:

```bash
cargo fmt --check
cargo test --all-features
cargo clippy --all-targets --all-features
```

If a task is intentionally narrower, it may run focused checks first, but the
AI must say clearly whether full validation passed or was not run.

## Coordination Matrix

| Task | Area | Primary owner | Must coordinate with |
| --- | --- | --- | --- |
| 1 | Control API read DTO/routes | `crates/rustcta-control-api` | Tasks 2, 3 |
| 2 | Control API app wiring/static hosting | `apps/control-api` | Tasks 1, 3 |
| 3 | Dioxus workspace UI | `web-ui/dioxus` | Tasks 1, 2 |
| 4 | Tools/ops low-risk commands | `tools/ops`, thin root wrappers | Tasks 5, 6 |
| 5 | Tools/ops canary/audit/admin safety migration | `tools/ops`, helper crates | Tasks 4, 7 |
| 6 | Industrial CLI and boundary checks | `apps/cli`, scripts/docs | Tasks 4, 5, 10 |
| 7 | Execution router and ledger mutation coverage | `crates/rustcta-execution-*`, `rustcta-event-ledger` | Tasks 1, 5, 8 |
| 8 | Supervisor runtime heartbeat/snapshot/recovery | `crates/rustcta-supervisor`, `apps/supervisor` | Tasks 1, 2, 7 |
| 9 | Strategy crate SDK migration | `strategies/*`, `retired strategy tree/*` compat | Tasks 7, 8 |
| 10 | CI, compatibility retirement audit, docs | scripts/docs/root manifests | All tasks |

## Task 1: Control API Secret-Free Read Route Extraction

Goal: move the next batch of legacy dashboard read models/routes from
`retired root bin directory/control_api.rs` into `crates/rustcta-control-api` without moving local
side effects or raw credential writes.

Allowed scope:

- `crates/rustcta-control-api/src/models.rs`
- `crates/rustcta-control-api/src/read_models.rs`
- `crates/rustcta-control-api/src/routes.rs`
- `crates/rustcta-control-api/src/router.rs`
- `crates/rustcta-control-api/src/state.rs`
- tests in `crates/rustcta-control-api/src/lib.rs` or sibling test modules
- docs describing the new route contract

Target routes/read models:

- status/config summary routes already present but missing legacy parity fields
- exchanges/books/inventory/risk/fees/logs read-only summaries
- selected strategy dashboard summaries as generic `StrategySnapshotEnvelope`
- events/audit read-only query behavior
- credential status/schema only, with redacted fields

Do not migrate:

- `/api/exchange-api-keys` POST/DELETE or any raw key/secret/passphrase write
- strategy YAML editing
- restart script invocation
- concrete exchange adapter probing
- local file mutation beyond test fixtures

Acceptance:

```bash
cargo test -p rustcta-control-api --all-features
cargo clippy -p rustcta-control-api --all-targets --all-features
cargo fmt --check
```

Prompt to assign:

```text
You are AI-1. Complete Task 1 from docs/parallel_ai_migration_tasks.md.
Read docs/control_web_directory_migration_plan.md and
docs/industrial_workspace_migration_status.md first. Extract only secret-free
read DTOs/routes into crates/rustcta-control-api. Do not touch raw credential
write/delete behavior, exchange adapter logic, or app/static hosting. Add
focused route/model tests and run the acceptance commands listed for Task 1.
```

## Task 2: Control API App Wiring And Static Hosting Parity

Goal: make `apps/control-api` a stronger process wrapper for the new
`rustcta-control-api` crate while keeping it thin.

Allowed scope:

- `apps/control-api/**`
- process wiring helpers in `crates/rustcta-control-api` only if Task 1 agrees
- docs for app flags/env vars
- test fixtures under the app crate

Target work:

- bind/env parsing tests
- optional static SPA hosting parity with the legacy local console
- legacy dashboard snapshot file following
- supervisor registry file following
- audit JSONL file following
- strategy log tail wiring from supervisor registry metadata
- safe failure behavior for missing files

Do not migrate:

- route DTO ownership that belongs to Task 1
- Dioxus UI code
- raw API key update/delete endpoints
- concrete exchange or strategy runtime code

Acceptance:

```bash
cargo test -p rustcta-control-api-app --all-features
cargo test -p rustcta-control-api --all-features
cargo fmt --check
```

Prompt to assign:

```text
You are AI-2. Complete Task 2 from docs/parallel_ai_migration_tasks.md.
Focus on apps/control-api process wiring and static hosting/snapshot following
parity. Keep apps/control-api thin. Coordinate route/DTO assumptions with
Task 1's public API contract. Do not touch Dioxus UI, exchange adapters, raw
credential writes, or strategy runtime logic. Add app-level tests and run the
Task 2 acceptance commands.
```

## Task 3: Dioxus Multi-Strategy Workspace UI Migration

Goal: convert `web-ui/dioxus` from a mostly legacy single-console panel into a
generic operator workspace that consumes the new control API routes where
available.

Allowed scope:

- `web-ui/dioxus/**`
- docs for panel route usage
- generated frontend build artifacts only if the repo already tracks them

Target work:

- workspace/agent/process/strategy list as the primary UI shell
- strategy detail panels driven by generic route contracts
- status/config/risk/log/events pages switched to new control API routes
- credential UI changed to status/schema/setup state only
- remove frontend raw secret DTO storage and raw key update assumptions
- maintain compatibility fallback for legacy local console routes where needed

Do not migrate:

- Rust control API DTOs without coordinating with Task 1
- local credential write/delete behavior
- exchange adapter or live order behavior

Acceptance:

```bash
cargo test -p rustcta-control-api --all-features
cargo fmt --check
```

Also run the web build command used by the project if available, for example:

```bash
cd web-ui/dioxus && cargo build --release
```

Prompt to assign:

```text
You are AI-3. Complete Task 3 from docs/parallel_ai_migration_tasks.md.
Read docs/control_web_directory_migration_plan.md and
docs/dioxus_control_panel.md first. Migrate the Dioxus UI toward a generic
multi-strategy workspace using the new control API read routes where available.
Do not add raw secret write/delete flows. Coordinate route names and DTO shapes
with Task 1 and Task 2. Run the Task 3 acceptance commands and report any web
build limitations.
```

## Task 4: Tools/Ops Low-Risk Command Completion

Goal: finish low-risk `tools/ops` migrations that can run without importing
the legacy root crate and without private account mutation.

Allowed scope:

- `tools/ops/**`
- thin compatibility wrappers in `retired root bin directory/*.rs`
- `crates/rustcta-reporting/**` for root-free reporting helpers
- docs/tools ops migration notes

Target candidates:

- `hyperliquid_self_test` if it can be made root-free and non-mutating
- additional render-only reporter commands
- help/summary parity for already migrated smart-money/probe/symbol commands
- wrapper consistency through `retired root bin directory/common/legacy_tools_ops_shim.rs`

Do not migrate:

- live order canaries
- private audit/admin commands
- root-dependent account provider bootstrap
- any command that would create live network/private side effects during tests

Acceptance:

```bash
cargo test -p rustcta-tools-ops --all-features
cargo clippy -p rustcta-tools-ops --all-targets --all-features
cargo run -p rustcta-tools-ops -- verify-retired-src
cargo run -p rustcta-tools-ops -- verify-retired-src
cargo fmt --check
```

Prompt to assign:

```text
You are AI-4. Complete Task 4 from docs/parallel_ai_migration_tasks.md.
Read docs/tools_ops_migration_plan.md first. Finish only root-free, low-risk
tools/ops commands and thin legacy wrappers. Do not migrate canary/audit/admin
commands or anything that requires private account mutation. Add parity tests
and run the Task 4 acceptance commands.
```

## Task 5: Tools/Ops Canary, Audit, And Admin Safety Migration

Goal: design and implement the next safe slice for live-canary, audit, and
order-admin tools by extracting non-root helpers and adding strict dry-run/help
contract tests.

Allowed scope:

- `tools/ops/**`
- helper crates that do not depend on root `rustcta`
- thin root wrappers in `retired root bin directory/*.rs`
- docs for safety gates and command compatibility

Target commands:

- `exchange_order_canary`
- `bitget_order_canary`
- `bitget_spot_order_canary`
- `cross_arb_account_audit`
- `cross_arb_fee_audit`
- `cross_arb_order_admin`

Migration rule:

- Start with CLI parsing, validation, dry-run planning, output rendering, and
  safety gate tests.
- Keep live execution in legacy root until non-root providers and explicit
  confirmation semantics are proven equivalent.

Do not change:

- exchange REST signing
- live order placement/cancel semantics
- confirmation flags
- output fields used by runbooks

Acceptance:

```bash
cargo test -p rustcta-tools-ops --all-features
cargo run -p rustcta-tools-ops -- verify-retired-src
cargo run -p rustcta-tools-ops -- verify-retired-src
cargo fmt --check
```

Prompt to assign:

```text
You are AI-5. Complete Task 5 from docs/parallel_ai_migration_tasks.md.
Read docs/tools_ops_migration_plan.md first. Extract only safe non-root helper
logic for canary/audit/admin commands: parsing, validation, dry-run planning,
output rendering, and safety gates. Preserve live behavior in legacy wrappers
unless fully proven equivalent. Do not alter exchange API details or live order
semantics. Add tests and run the Task 5 acceptance commands.
```

## Task 6: Industrial CLI Command Tree And Boundary Checks

Goal: make `apps/cli` the operator command surface for migration inventory,
ledger, supervisor, ops, and existing preflight bridge commands, with smoke
coverage.

Allowed scope:

- `apps/cli/**`
- scripts/boundary checks
- docs for CLI command tree
- read-only integration tests

Target work:

- command tree documentation and Clap help tests
- smoke tests for `migration`, `ledger`, `supervisor`, `ops`
- bridge command for existing `cross-arb preflight` without live network paths
- boundary check that fails on unclassified new `retired root bin directory/*.rs`
- boundary check that app crates do not import gateway-private modules

Do not migrate:

- tools command implementation owned by Tasks 4/5
- control API route behavior
- exchange gateway features
- live private account paths

Acceptance:

```bash
cargo test -p rustcta-industrial-app --all-features
scripts/check_industrial_boundaries.sh
RUSTCTA_STRICT_STRATEGY_MIGRATION=1 scripts/check_industrial_boundaries.sh
cargo fmt --check
```

Prompt to assign:

```text
You are AI-6. Complete Task 6 from docs/parallel_ai_migration_tasks.md.
Focus on apps/cli command tree coverage and migration boundary checks. Do not
implement tools command internals, control API routes, exchange gateway
features, or live private paths. Add smoke/help tests and run the Task 6
acceptance commands.
```

## Task 7: Execution Router And Event Ledger Mutation Coverage

Goal: move more execution mutation decisions through typed execution-router and
event-ledger contracts, including legacy/runtime mutation paths that still need
auditable records.

Allowed scope:

- `crates/rustcta-execution-api/**`
- `crates/rustcta-execution-router/**`
- `crates/rustcta-event-ledger/**`
- root execution modules only as compatibility adapters
- focused tests

Target work:

- execution router support for fee model, live-dry-run, reservation,
  idempotency, reconciliation, and rejection event records
- append ledger events for remaining legacy mutation paths where adapters exist
- JSONL replay tests for command/ack/fill/account/audit ordering
- secret-field rejection before ledger writes

Do not change:

- exchange adapter request signing or private REST endpoints
- live order semantics
- strategy runtime orchestration owned by Task 9
- production database backend choice

Acceptance:

```bash
cargo test -p rustcta-execution-api --all-features
cargo test -p rustcta-execution-router --all-features
cargo test -p rustcta-event-ledger --all-features
cargo fmt --check
```

Prompt to assign:

```text
You are AI-7. Complete Task 7 from docs/parallel_ai_migration_tasks.md.
Move more mutation decisions into typed execution-router and event-ledger
contracts without changing exchange adapter behavior or live order semantics.
Add focused ledger/replay/secret-rejection tests. Do not choose a production
database backend. Run the Task 7 acceptance commands.
```

## Task 8: Supervisor Heartbeat, Snapshot, And Recovery Foundation

Goal: extend supervisor beyond start/stop/restart into runtime heartbeat,
snapshot collection, and local recovery policy foundations.

Allowed scope:

- `crates/rustcta-supervisor/**`
- `apps/supervisor/**`
- `config/supervisor/**`
- docs for process specs and recovery policy

Target work:

- heartbeat ingestion model from supervised runtimes
- runtime snapshot metadata in supervisor records
- crash/restart policy configuration and tests
- registry validation for deployment specs
- read-only HTTP views for heartbeat/snapshot/recovery status
- local-only lifecycle mutation groundwork if authenticated and explicitly
  separated from read-only mode

Do not migrate:

- strategy runtime internals
- control API DTOs except stable supervisor read model inputs
- remote unauthenticated lifecycle mutation APIs
- deployment-specific production decisions not in config

Acceptance:

```bash
cargo test -p rustcta-supervisor --all-features
cargo test -p rustcta-supervisor-app --all-features
cargo fmt --check
```

Prompt to assign:

```text
You are AI-8. Complete Task 8 from docs/parallel_ai_migration_tasks.md.
Read docs/supervisor_process_specs.md if present and
docs/industrial_workspace_migration_status.md first. Extend supervisor
heartbeat/snapshot/recovery foundations while keeping read-only mode safe. Do
not move strategy runtime internals or add unauthenticated remote lifecycle
mutations. Add tests and run the Task 8 acceptance commands.
```

## Task 9: Strategy Crate SDK Migration

Goal: continue moving adapter-free strategy core/runtime slices into
independent `strategies/*` crates using `rustcta-strategy-sdk`, while keeping
legacy `retired strategy tree/*` compatibility paths green.

Allowed scope:

- `strategies/*/**`
- `crates/rustcta-strategy-sdk/**`
- `retired strategy tree/*` compatibility adapters only when needed
- strategy-specific tests

Target work:

- pick one or two strategy crates per batch, not all at once
- move adapter-free planning, risk, state, sizing, signal, and DTO logic
- add SDK-facing tests that do not instantiate concrete exchange adapters
- leave market-data/execution/storage orchestration in legacy until its
  boundary is explicit
- keep `RUSTCTA_STRICT_STRATEGY_MIGRATION=1` passing

Do not change:

- concrete exchange clients
- live order submission behavior
- control API/web UI routes
- backtest root-heavy extraction except compile fixes

Acceptance:

```bash
cargo test --workspace --all-features
RUSTCTA_STRICT_STRATEGY_MIGRATION=1 scripts/check_industrial_boundaries.sh
cargo fmt --check
```

Prompt to assign:

```text
You are AI-9. Complete Task 9 from docs/parallel_ai_migration_tasks.md.
Continue adapter-free strategy crate migration using rustcta-strategy-sdk.
Choose a bounded strategy slice, keep legacy retired strategy tree compatibility
paths compiling, and avoid concrete exchange/live execution behavior changes.
Run the Task 9 acceptance commands.
```

## Task 10: CI Gates, Compatibility Retirement Audit, And Final Docs

Goal: make the migration measurable and prevent regressions after Tasks 1-9.

Allowed scope:

- scripts and CI configuration
- root `Cargo.toml` bin metadata if needed
- docs
- compatibility inventory tests

Target work:

- full workspace check/test/clippy gate plan
- web UI release build gate
- classified legacy bin inventory gate
- docs/runbook list for old command names and new command names
- compatibility retirement matrix: keep wrapper, warn, alias, or remove later
- local end-to-end paper run checklist through gateway, execution router,
  supervisor, control API, and web UI
- document intentionally paused areas: root-heavy backtest extraction,
  production ledger backend, raw credential administration path

Do not change:

- feature behavior owned by Tasks 1-9
- exchange gateway features
- live order semantics
- raw credential write/delete promotion

Acceptance:

```bash
cargo fmt --check
cargo test --all-features
cargo clippy --all-targets --all-features
scripts/check_industrial_boundaries.sh
```

Prompt to assign:

```text
You are AI-10. Complete Task 10 from docs/parallel_ai_migration_tasks.md.
Focus on CI/boundary gates, compatibility retirement audit, and final migration
docs. Do not implement feature behavior owned by other tasks. Preserve legacy
compatibility names until the docs define a retirement milestone. Run the Task
10 acceptance commands and summarize remaining intentionally paused areas.
```

## Suggested Execution Order

Parallel start:

1. Task 4
2. Task 6
3. Task 8
4. Task 10 docs/checks draft

Coordinate before merging:

1. Task 1 and Task 2 should agree on route names and state provider shape.
2. Task 3 should wait for Task 1 route names or use compatibility fallback.
3. Task 7 should coordinate with Task 5 for canary/admin audit events.
4. Task 9 should coordinate with Task 7 before moving any runtime mutation path.

High-risk tasks:

- Task 3, because UI and control API DTOs can drift.
- Task 5, because canary/admin commands can touch live exchanges.
- Task 7, because mutation audit coverage must not change execution semantics.
- Task 9, because strategy runtime logic is still coupled to legacy modules.

Paused unless explicitly assigned later:

- Further root-heavy backtest extraction beyond compile fixes.
- Exchange gateway feature work and gateway-private adapter changes.
- Raw credential key write/delete promotion into public control API.
- Production event-ledger database backend selection.
- Full strategy live runtime relocation without SDK/execution/supervisor
  boundary agreement.

## Task 11: Final Migration Closure Audit

Task 11 must run only after Tasks 1-10 have been merged into the same branch and
their validation results are available. It is not a parallel task.

Goal: decide whether the migration is ready for deletion/entrypoint retirement,
then either perform only safe cleanup or document blockers that prevent final
closure.

Allowed scope:

- docs
- scripts and CI gates
- root/app/tool manifest entrypoints
- compatibility wrappers that are proven redundant by tests and inventory
- empty directories or unreferenced backup files

Required audit commands:

```bash
git status --short
cargo run -q -p rustcta-tools-ops -- verify-retired-src
cargo run -q -p rustcta-tools-ops -- verify-retired-src
scripts/check_industrial_boundaries.sh
RUSTCTA_STRICT_STRATEGY_MIGRATION=1 scripts/check_industrial_boundaries.sh
rg -n "exchange-api-keys|/api/control/symbols|/api/spot-arb/dashboard|/api/cross-arb/dashboard|/api/strategy-config|/api/cross-arb/settings" web-ui/dioxus/src crates/rustcta-control-api/src apps/control-api/src
```

Closure is allowed only if all of these are true:

- `verify-retired-src` marks the old entrypoint as `alias` or `remove_later`, not
  `keep_wrapper` or `warn`.
- the replacement command exists and has help/contract tests.
- operator docs and scripts no longer require the old command name.
- deleting the old file does not remove live-order confirmation gates,
  credential safety behavior, or local-only compatibility behavior.
- full validation passes after the deletion.

If any old entrypoint is still `keep_wrapper` or `warn`, Task 11 must not delete
it. It should update docs with the blocker and retirement milestone instead.

Safe cleanup examples:

- remove empty directories and deleted-file references;
- delete unreferenced backup files already outside any compatibility path;
- update docs to point to new workspace commands first;
- mark legacy command status as `alias`, `warn`, `keep_wrapper`, or
  `remove_later` with a concrete milestone;
- add or tighten boundary checks so future unclassified `retired root bin directory/*.rs` files
  fail CI.

Do not do in Task 11:

- delete `retired root bin directory/control_api.rs` while it still owns raw local credential
  env-store write/delete behavior, static UI compatibility, command queue
  aliases, or legacy read routes;
- delete live canary/admin/audit binaries while they still own confirmation
  flags, private account access, or live mutation safety gates;
- delete strategy runtime binaries while supervisor specs still launch them;
- remove Dioxus legacy endpoint fallback while the new generic route contract is
  incomplete;
- remove root-heavy backtest compatibility entries before scripts/runbooks have
  switched to `retired-backtest`.

Acceptance:

```bash
cargo fmt --check
cargo test --all-features
cargo clippy --all-targets --all-features
scripts/check_industrial_boundaries.sh
RUSTCTA_STRICT_STRATEGY_MIGRATION=1 scripts/check_industrial_boundaries.sh
```

Prompt to assign:

```text
You are AI-11. Complete Task 11 from docs/parallel_ai_migration_tasks.md.
First audit whether Tasks 1-10 are actually closure-ready. Do not delete legacy
entrypoints that verify-retired-src marks keep_wrapper or warn. Only remove files
that are proven unreferenced and outside compatibility paths. Update final docs,
entrypoint runbooks, and boundary checks. Run the Task 11 acceptance commands
and clearly list any blockers that prevent final deletion.
```

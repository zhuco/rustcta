# Legacy `src/` Full Workspace Migration Parallel Plan

Status date: 2026-06-08

本文档用于把剩余 legacy `src/` 目录一次性收口到 workspace
边界。目标不是继续整理目录，而是让 10-20 个 AI/开发任务可以并行执行，
最终让 `src/` 不再承载业务实现。

## 结论

长期目标：可以把所有还需要的功能迁到 workspace 的 `crates/`、
`apps/`、`tools/`、`strategies/`、`web-ui/` 中。

当前状态：`retired backtest tree` 和 `src/smart_money` 已删除；不能直接删除
`src/web` 或仍服务 live runtime 的 control/exchange/strategy 目录。

- `retired backtest tree` 已由 `retired-backtest` / `apps/backtest` 接管；root
  `backtest` 和 `retired_short_ladder_grid` 只保留 manifest 显式兼容 bin。
- `src/smart_money` 已由 `crates/rustcta-smart-money` 接管；旧 root facade
  和 smart-money legacy tool shims 已删除。
- `src/web` 不是单纯旧页面，它还是 legacy runtime 的 sanitized
  dashboard snapshot producer。删除前必须让 runtime 状态通过
  `rustcta-control-api`、event ledger、supervisor/process registry 或新的
  typed snapshot provider 发布。
- `retired root bin directory/control_api.rs` 仍拥有大量旧控制台 route、本地文件副作用、
  strategy config 编辑、raw key env-store、SSE/轮询和静态资源默认入口。
- `retired exchange tree` 是 legacy live runtime 的真实交易/行情接入路径；
  新的 `rustcta-exchange-gateway` 覆盖了 gateway adapter 边界，但还没有
  替换所有 live strategy、canary、audit、readonly harness 的旧调用。
- `retired strategy tree` 中的新策略 crate 目前多数只完成 adapter-free core
  slice；实际 live runtime orchestration、market data、execution、storage、
  notification 仍在 legacy `retired strategy tree/*`。

因此后续不能做无边界“大删目录”。正确收口方式是：保留跨所套利、
资金费率、对冲网格仍可能依赖的 live runtime 路径；其余已 root-free
的 wrapper 和目录可以按 gate 删除。

## Hard Rules

所有并行任务必须遵守：

- 不新增交易所 endpoint、签名、REST/WS 行为。交易所能力扩展属于独立
  gateway workstream。
- 不把 raw secret/key/passphrase 写入新的 control API、web UI、strategy
  crate 或普通 app。
- 不让 `apps/*` 直接依赖 strategy crate；不让非 gateway app 依赖
  `rustcta-exchange-gateway`。
- 不让 `tools/ops` 依赖 legacy root `rustcta`。如果需要 root 行为，先抽
  到非 root helper crate。
- 不让 `strategies/*` 依赖 legacy root、exchange gateway、execution
  router 或 concrete adapter。
- 不提前删除 `src/*` 目录。删除必须由最后的 cleanup task 统一完成，并且
  通过全仓 gates。
- 共享文件只由 coordinator 或指定任务修改：root `Cargo.toml`、
  `src/lib.rs`、`scripts/check_industrial_boundaries.sh`、CI workflow、
  `docs/README.md`。

基础验收命令：

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
scripts/check_industrial_boundaries.sh
```

单任务可以先跑 focused checks，但合并前必须说明未跑的全仓 gate。

## Current Legacy Inventory

当前 `src/` 已删除 backtest 和 smart-money 两个完成迁移的区域。最大遗留区域：

| Area | Approx. size | 当前判断 |
| --- | ---: | --- |
| `retired strategy tree` | ~79k lines | 未完成迁移。策略 crate 有 core slice，但 live runtime 仍在 legacy。 |
| `retired exchange tree` | ~76k lines | 未完成迁移。gateway 不等于旧 live adapter 全替换。 |
| `retired root bin directory` | 17 direct bins | 保留 control、cross/funding runtime、live canary/audit/admin、backtest compat 等已分类入口。 |
| `retired backtest tree` | 0 | 已迁到 `crates/retired-backtest` 并删除 legacy tree。 |
| `src/execution` | ~10k lines | router/api 已有新 crate，但 user stream、fee model、reconciliation 等仍在 root。 |
| `src/control` | ~8k lines | spot control runtime/snapshot/command 仍在 root。 |
| `src/core` / `src/cta` / `src/utils` | ~7k lines | root compatibility glue、account manager、config/logger/http helpers 尚未拆完。 |
| `src/web` | ~850 lines | legacy snapshot producer，不是纯旧 UI。 |
| `src/smart_money` | 0 | 已迁到 `crates/rustcta-smart-money`；legacy root facade 已删除。 |

## Parallel Task Matrix

建议把任务拆成 18 个 lane。每个 lane 独立分支执行；共享文件由 AI-00
集中合并。

### AI-00 Coordinator / Boundary Owner

Scope:

- `scripts/check_industrial_boundaries.sh`
- `.github/workflows/*`
- root `Cargo.toml`
- `src/lib.rs`
- final deletion PR

Responsibilities:

- 维护 legacy module/binary inventory。
- 给每个完成迁移的目录添加 boundary check，禁止后续回流到 `src/`。
- 统一处理 root manifest target 删除、workspace member 调整和 CI gates。
- 最后执行 `src/` 删除。

Do not:

- 不迁具体业务逻辑。
- 不改交易所 adapter 行为。

Acceptance:

```bash
cargo run -q -p rustcta-tools-ops -- verify-retired-src
scripts/check_industrial_boundaries.sh
cargo test --workspace --all-features
```

### AI-01 Backtest Runtime Extraction

Target:

- `crates/retired-backtest`
- `apps/backtest`
- legacy bridge in `retired backtest tree/runtime/*`
- tests `tests/backtest_cli.rs`, `tests/backtest_runtime.rs`

Goal:

- 移除 `crates/retired-backtest` 的 `legacy-runtime` 依赖。
- 让 `retired-backtest` 拥有 `BacktestCli`、`BacktestCommand`、
  `execute_cli`、`BacktestRuntimeConfig`、stdout renderer、runtime dispatch。

Do not:

- 不迁 live exchange adapter。
- 不改变 CLI flags/output schema。

Done when:

- `apps/backtest/Cargo.toml` 不再启用 `legacy-runtime`。
- `crates/retired-backtest/Cargo.toml` 不再依赖 root `rustcta`。
- `tests/backtest_cli.rs` 和 `tests/backtest_runtime.rs` import 新 crate。

Acceptance:

```bash
cargo test -p retired-backtest
cargo test -p retired-backtest-app
cargo test --test backtest_cli --test backtest_runtime
cargo run -q -p retired-backtest-app --bin retired-backtest -- --help
```

### AI-02 Backtest Strategy / Research Extraction

Target:

- `retired backtest tree/strategy/*`
- `retired root bin directory/retired_short_ladder_grid.rs`
- `crates/retired-backtest`

Goal:

- 把 mean-reversion、trend-factor、short-ladder、MTF grid 等回测策略实现迁到
  `retired-backtest` 或相关 strategy crate 的 offline module。
- root `retired backtest tree/strategy/*` 只剩 compatibility include 或空 wrapper。

Do not:

- 不改 live strategy runtime。
- 不改变报告字段、CSV/Markdown 输出。

Acceptance:

```bash
cargo test -p retired-backtest
cargo test --test backtest_mean_reversion --test backtest_trend_factor
cargo run -q -p retired-backtest-app --bin retired-backtest -- short-ladder-mtf-grid --help
```

### AI-03 Backtest Data Acquisition Cutover

Target:

- `retired backtest tree/data/*`
- `crates/retired-backtest`
- optional new crate only if needed: `crates/rustcta-market-data`

Goal:

- 把 kline/depth/trade capture、dataset acquisition、exchange metadata 的剩余
  root-heavy 代码迁出 `retired backtest tree`。
- 网络采集路径必须通过 gateway/public market-data contract 或明确的
  root-free provider trait。

Do not:

- 不新增交易所 endpoint。
- 不把 root `retired exchange tree/*` 搬进 backtest crate。

Acceptance:

```bash
cargo test -p retired-backtest
cargo test --test backtest_depth_capture --test backtest_trade_capture
cargo test --test backtest_exchange_metadata
```

### AI-04 Control API Legacy Route Extraction

Target:

- `retired root bin directory/control_api.rs`
- `crates/rustcta-control-api/src/*`
- `apps/control-api/src/*`

Goal:

- 把 legacy dashboard read routes 迁到 `rustcta-control-api` 的 secret-free
  DTO/route/read-model。
- 优先迁：disabled state、dry-run plans、preflight/reconciliation summaries、
  spot/cross dashboard、scanner、hedge-policy、runtime-publisher、symbol-control。

Do not:

- 不迁 raw `/api/exchange-api-keys` POST/DELETE。
- 不把 exchange registry 或 strategy YAML parser 放进 control API crate。

Acceptance:

```bash
cargo test -p rustcta-control-api
cargo check -p rustcta-control-api-app
scripts/control_api_smoke_test.sh
scripts/check_industrial_boundaries.sh
```

### AI-05 Control API Local Side Effects Split

Target:

- `retired root bin directory/control_api.rs`
- `apps/control-api`
- optional new crate: `crates/rustcta-local-agent`

Goal:

- 把 strategy config edit、restart script、command queue、balance/profit
  history、本地文件 IO 从 monolithic retired binary 拆到 app/local-agent 边界。
- 新 public API 只能返回 accepted/audit/status，不直接暴露 raw local path。

Do not:

- 不提升 raw key write/delete 到新 API。
- 不让 SaaS/public API 执行本地 restart script。

Acceptance:

```bash
cargo test -p rustcta-control-api
cargo check -p rustcta-control-api-app
scripts/check_industrial_boundaries.sh
```

### AI-06 `src/web` Snapshot Producer Replacement

Target:

- `src/web/*`
- legacy runtime publishers
- `crates/rustcta-control-api`
- `crates/rustcta-event-ledger`
- `crates/rustcta-supervisor`

Goal:

- 让 runtime 状态不再写 `src/web::DashboardReadModel`。
- 用 typed snapshot/event ledger/supervisor registry/control API read-model
  替换 `MonitoringState` 和 snapshot writer。

Do not:

- 不迁前端 UI。
- 不把 legacy `DashboardReadModel` 作为长期 public DTO。

Done when:

- `rg 'crate::web|legacy root crate path web' src tests crates apps strategies tools` 只剩
  planned compatibility 或为空。
- `src/web` 可以删除。

Acceptance:

```bash
cargo test -p rustcta-control-api
cargo test -p rustcta-event-ledger
cargo check --workspace --all-targets
```

### AI-07 Dioxus Workspace UI Cutover

Target:

- `web-ui/dioxus/src/*`
- `web-ui/dioxus/assets/main.css`
- `apps/control-api`

Goal:

- UI 第一屏切到 workspace/agents/processes/strategies/gateway。
- legacy dashboard panels 改为 strategy detail 或 legacy fallback。
- 移除 raw key/secret/passphrase browser state。

Do not:

- 不直接调用 legacy `control_api` raw credential routes。
- 不把 UI 类型当作 Rust public DTO。

Acceptance:

```bash
cd web-ui/dioxus
cargo build --release
```

### AI-08 Execution Router Full Cutover

Target:

- `src/execution/*`
- `crates/rustcta-execution-api`
- `crates/rustcta-execution-router`
- `crates/rustcta-event-ledger`

Goal:

- 迁移 fee model、idempotency、live-dry-run、reservation、bundle、order/fill
  reconciliation、user stream event normalization。
- legacy runtime mutation path 必须写 event ledger。

Do not:

- 不实例化 concrete exchange adapter。
- 不绕过 gateway/execution API。

Acceptance:

```bash
cargo test -p rustcta-execution-api
cargo test -p rustcta-execution-router
cargo test -p rustcta-event-ledger
```

### AI-09 Market/Data Runtime Extraction

Target:

- `src/market/*`
- `src/data/*`
- optional new crate: `crates/rustcta-market-data`

Goal:

- 抽出 order book cache、book health、WS supervisor、instrument metadata、
  symbol routing、VWAP、funding/trade events。
- 策略 runtime 和 gateway/live readonly 都通过新 market-data contract 使用。

Do not:

- 不新增 venue-specific WS endpoint。
- 不把 `retired exchange tree/market_adapters` 直接复制到 generic market crate。

Acceptance:

```bash
cargo test --test live_runtime_publisher
cargo test --test live_websocket_books
cargo check --workspace --all-targets
```

### AI-10 Legacy Exchange Runtime Cutover

Target:

- `retired exchange tree/*`
- `crates/rustcta-exchange-api`
- `crates/rustcta-exchange-gateway`
- `apps/gateway`

Goal:

- 让 live strategies、readonly harness、canaries/audits/admin 不再直接依赖
  legacy `retired exchange tree::*` concrete clients。
- 旧 `ExchangeClient`/`TradingAdapter` 语义通过 gateway client 或新的
  adapter-free provider trait 消费。

Do not:

- 不新增交易所覆盖。
- 不改变签名和 live order 行为，除非已有 fixture/contract 锁定。

Acceptance:

```bash
cargo test -p rustcta-exchange-api
cargo test -p rustcta-exchange-gateway
cargo test -p rustcta-gateway
cargo test --test live_readonly_harness
```

### AI-11 Strategy Runtime: Cross / Funding

Target:

- `retired strategy tree/cross_exchange_arbitrage/*`
- `retired strategy tree/funding_rate_arbitrage/*`
- `strategies/cross-exchange-arbitrage`
- `strategies/funding-arbitrage`
- `retired root bin directory/cross_arb_live.rs`
- `retired root bin directory/funding_arb_live.rs`

Goal:

- 把 live/observe runtime、task orchestration、market-data provider、
  execution provider、storage、dashboard snapshot、notification 迁到 strategy
  crate/app runtime contract。
- root bins 变成 supervised compatibility wrappers。

Do not:

- 不直接依赖 concrete exchange adapter。
- 不打开新的 live order 默认行为。

Acceptance:

```bash
cargo test -p rustcta-strategy-cross-exchange-arbitrage
cargo test -p rustcta-strategy-funding-arbitrage
cargo check --bin cross_arb_live --bin funding_arb_live
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- --validate-spec config/supervisor/cross_arb_live.spec.json
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- --validate-spec config/supervisor/funding_arb_live.spec.json
```

### AI-12 Strategy Runtime: Spot-Spot

Target:

- `retired strategy tree/spot_spot_taker_arbitrage/*`
- `strategies/spot-spot-arbitrage`
- live readonly tests

Goal:

- 迁移 config loading、inventory ownership、fee integration、paper/live
  execution、lifecycle、replay、websocket market data、control-plane integration。

Do not:

- 不直接 import `legacy root crate path exchanges::{binance, okx, ...}`。
- 不改变 dry-run/live safety flags。

Acceptance:

```bash
cargo test -p rustcta-strategy-spot-spot-arbitrage
cargo test --test spot_spot_live_config
cargo test --test five_exchange_spot_scanner
```

### AI-13 Strategy Runtime: Trend / Market Maker / Grids

Target:

- `retired strategy tree/trend/*`
- `retired strategy tree/poisson_market_maker/*`
- `retired strategy tree/hedged_grid/*`
- `retired strategy tree/range_grid/*`
- `retired strategy tree/short_ladder_live/*`
- matching `strategies/*` crates

Goal:

- 把 async market feed、execution calls、position/user stream sync、monitoring、
  alerting、task orchestration 迁到 strategy crate + SDK contracts。

Do not:

- 不把 exchange adapter、gateway private module、env secret 读入 strategy crate。

Acceptance:

```bash
cargo test -p rustcta-strategy-trend
cargo test -p rustcta-strategy-poisson-market-maker
cargo test -p rustcta-strategy-hedged-grid
cargo test -p rustcta-strategy-range-grid
cargo test -p rustcta-strategy-short-ladder-live
RUSTCTA_STRICT_STRATEGY_MIGRATION=1 scripts/check_industrial_boundaries.sh
```

### AI-14 Control / Risk / Scanner Extraction

Target:

- `src/control/spot_control/*`
- `src/risk/*`
- `src/scanner/*`
- optional new crate: `crates/rustcta-runtime-control`

Goal:

- 迁移 spot control command/snapshot/lifecycle/operation lock/snapshot replay。
- 迁移 kill switch、disabled registry、five-exchange scanner read models。

Do not:

- 不让 control/risk crate 直接拥有 exchange clients。

Acceptance:

```bash
cargo test --test live_readonly_harness
cargo test --test five_exchange_spot_scanner
cargo check --workspace --all-targets
```

### AI-15 Tools Ops Root-Dependent Commands

Target:

- `retired root bin directory/account_position_reporter.rs`
- `retired root bin directory/hyperliquid_self_test.rs`
- canary/audit/admin binaries
- `tools/ops`
- helper crates such as `rustcta-reporting`

Goal:

- 把 live reporter/private audit/admin/canary 的非 root helper 抽到 crates。
- `tools/ops` 只拥有 bounded operator command/safety plan，不直接依赖 root。
- legacy bins 变成 wrapper 或明确保留到最后。

Do not:

- 不把 live mutation 默认接入新 tools command。
- 不删除确认门：`--execute`、`--confirm-live-order`、notional caps。

Acceptance:

```bash
cargo test -p rustcta-tools-ops
cargo test -p rustcta-reporting
cargo run -q -p rustcta-tools-ops -- verify-retired-src
cargo run -q -p rustcta-tools-ops -- verify-retired-src
```

### AI-16 Smart Money Extraction

Target:

- `src/smart_money/*`
- `tools/ops`
- optional new crate: `crates/rustcta-smart-money`

Goal:

- 把 smart-money domain/config/pipeline/replay/portfolio/scoring/storage 迁到
  root-free crate。
- 工具入口只做 CLI composition。

Do not:

- 不把交易所私钥读取、下单、长期 daemon 行为塞进 tools。

Acceptance:

```bash
cargo test --test smart_money_core --test smart_money_pipeline --test smart_money_binance_replay
cargo test -p rustcta-tools-ops
```

### AI-17 Core / CTA / Utils Extraction

Target:

- `src/core/*`
- `src/cta/*`
- `src/utils/*`
- `src/analysis/*`
- foundation crates or new small crates

Goal:

- 把 account manager、config loading、logger/tracing、http/ws helpers、time sync、
  request manager、monitoring、analysis trade collector 迁到 root-free crates。
- 删除 broad `pub use` re-export 依赖。

Do not:

- 不让 foundation crate 反向依赖 app/strategy/gateway implementation。

Acceptance:

```bash
cargo check --workspace --all-targets
cargo test --workspace --all-features
```

### AI-18 Test Import / Compatibility Cleanup

Target:

- `tests/*`
- root `src/lib.rs`
- `Cargo.toml`

Goal:

- 把所有 `use legacy root crate path ...` 测试 import 改为新 crate import。
- root crate 只保留 compatibility bins，随后由 AI-00 统一删除。

Do not:

- 不在测试里继续引用 legacy root 以绕过迁移。

Acceptance:

```bash
rg -n 'use legacy root crate path |legacy root crate path ' tests crates apps tools strategies --glob '*.rs'
cargo test --workspace --all-features
```

## Deletion Gates

目录删除必须按顺序执行，不能并行抢删。

1. Delete first:
   - `retired strategy tree/backup_pre_modularization` if still empty/untracked.
   - `retired strategy tree/oscillation_grid.rs` after confirming only migrated
     `rustcta-strategy-range-grid` owns the useful planner/fill semantics.
2. `retired backtest tree` deletion completed when:
   - `crates/retired-backtest` has no `legacy-runtime` feature.
   - `rg 'legacy root crate path backtest|crate::backtest' tests crates apps tools strategies`
     returns empty.
   - `cargo test -p retired-backtest -p retired-backtest-app` passes.
3. Delete `src/web` only when:
   - `rg 'legacy root crate path web|crate::web' src tests crates apps tools strategies`
     returns empty.
   - new control API and runtime snapshot/event path cover status, config,
     exchanges, books, inventory, risk, fees, logs, symbols, opportunities,
     trades, runtime publisher, preflight, reconciliation and disabled state.
4. Delete root `retired root bin directory/*.rs` only when:
   - replacement command exists.
   - operator docs/scripts no longer invoke old binary.
   - compatibility matrix marks the entry `remove_later` and AI-00 confirms
     retirement milestone is met.
5. Delete `retired exchange tree`, `src/execution`, `src/market`, `src/data`,
   `src/control`, `src/risk`, `src/scanner` only after live strategy/runtime
   paths use gateway/execution/market/control crates.
6. Delete `retired strategy tree` only after all strategy crates own runtime
   orchestration, not just adapter-free core.
7. Delete root `src/lib.rs` and root package only after every test and app no
   longer imports `legacy root crate path `.

Final deletion acceptance:

```bash
rg -n 'legacy root crate path |crate::(backtest|web|exchanges|execution|market|strategies|control|risk|scanner|smart_money)' \
  tests crates apps tools strategies web-ui --glob '*.rs' --glob '*.toml'
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
scripts/check_industrial_boundaries.sh
cd web-ui/dioxus && cargo build --release
```

## Merge Protocol For 10-20 Parallel AIs

- 每个 AI 只认领一个 lane，不跨 lane 改共享文件。
- 每个 AI 开工前运行 `git status --short`，不得覆盖他人改动。
- 每个 AI 在提交说明中列出：
  - migrated source paths
  - new owner crate/app/tool
  - old compatibility path
  - deletion blocker removed or remaining
  - validation commands
- 共享文件改动通过 AI-00 合并：
  - workspace manifests
  - CI workflow
  - boundary script
  - docs index
  - final root package removal
- 如果两个 lane 都需要同一个 public contract，先由 AI-00 合并最小 trait/DTO，
  再让 downstream lane 使用。
- live order/admin/private audit 任务必须默认 dry-run/read-only，不能把真实
  live mutation 放进 routine CI。

## Expected End State

最终状态应该是：

- `crates/` 拥有 reusable platform/domain logic。
- `apps/` 只负责 process startup/env/CLI/http/static wiring。
- `tools/ops` 只负责 operator tools、migration/audit/reporting command。
- `strategies/*` 拥有完整策略 runtime/core/snapshot/config schema，并通过
  `rustcta-strategy-sdk` 和 execution/gateway/control contract 工作。
- `web-ui/dioxus` 是唯一 Web panel。
- legacy root `rustcta` package 被删除，或只在一个短期 release 中作为空的
  compatibility package 存在。
- `src/` 不再承载业务代码；最终可以删除。

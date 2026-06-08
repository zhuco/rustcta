# Control API + Web Panel Directory Migration Plan

本文档只描述目录边界和迁移任务，不要求立即改 Rust 源码。目标是把当前单策略本地控制台，演进成支持多策略、多进程、SaaS 控制面和本地 agent 的 operator workspace。

## 目标形态

Operator workspace 的长期边界如下：

- `crates/rustcta-control-api/` 是控制面 API 契约和路由层：只暴露 operator read model、command/audit shape、多策略/多进程状态、gateway/agent 状态和安全的配置摘要。
- `apps/control-api/` 是控制 API 进程入口：负责读取环境、绑定监听地址、装配 state provider、静态资源服务和本地部署 glue。
- `web-ui/dioxus/` 是唯一 Web panel UI：保留 Dioxus 组件、样式、i18n、本地 token 存储和 API client；它不拥有交易所、策略执行或密钥存储逻辑。
- `src/web/` 暂时作为 legacy runtime monitoring/read-model 兼容层：继续给现有策略 runtime 生成 sanitized snapshot，直到策略 runtime 与 control API 契约完全解耦。
- `retired root bin directory/control_api.rs` 暂时作为 legacy local console 入口：冻结为兼容桥，逐步把可复用 API shape 下沉到 `crates/rustcta-control-api/`，把本地副作用留给 app/local-agent 边界。

核心原则：

- Control API 和 Web UI 不 import concrete exchange adapters，例如 `legacy root crate path exchanges::registry`、`binance`、`okx`、`gateio`、`bitget`、`mexc`、`coinex` 等。
- Control API 和 Web UI 不定义、保存或返回 raw secret shape，例如 `api_key`、`api_secret`、`secret`、`passphrase`、`authorization`。公开契约只能有 redacted credential status、credential slot id、health/error code。
- SaaS 控制面只看多租户/多 agent 的状态和命令审计；交易所密钥、实际下单、adapter 实例化只在本地 agent/gateway/execution 边界。
- Strategy-specific dashboard 可以作为 typed summary + versioned opaque detail 扩展，但 workspace shell、process control、risk/readiness/audit 必须是跨策略通用 shape。

## 当前边界观察

### `src/web/`

当前职责：

- `models.rs` 定义 `MonitoringConfig`、`DashboardReadModel`、`StatusView`、`ExchangeHealthView`、`BookView`、`InventoryView`、`FeeView`、`DisabledView`、`ConfigSummaryView` 等监控 read model。
- `state.rs` 提供 `MonitoringState` 和一批 `publish_*` 方法，现有策略 runtime、preflight、spot control、reconciliation 直接写入这些模型。
- `server.rs` 写 sanitized dashboard snapshot 到 `data/control_api/dashboard_snapshot.json`，并有 secret-like key 检查。

问题：

- `src/web/models.rs` 直接依赖主 crate 内部类型：`control::spot_control`、`exchanges::unified`、`execution`、`live_preflight`、`risk`、`scanner`、`strategies::arbitrage_core`。
- `src/web/state.rs` 是 runtime 内存状态，不是可复用 control API 契约。
- 多个 runtime 模块仍依赖 `crate::web::*`，所以不能作为第一步移动或删除。

迁移定位：

- 暂时保留 `src/web/`，只把它看作 legacy runtime snapshot producer。
- 新 `crates/rustcta-control-api/` 不应直接复用 `src/web::DashboardReadModel` 作为长期 public contract；过渡期可以在 app/local adapter 层读取 JSON snapshot 并转换为新 DTO。

### `retired root bin directory/control_api.rs`

当前职责：

- 作为本地 separated control panel 的 retired binary，读取 snapshot、服务 `web-ui/dioxus/dist`、提供大量 `/api/*` 兼容端点。
- 包含 dashboard read endpoints：`/api/status`、`/api/config`、`/api/exchanges`、`/api/books`、`/api/inventory`、`/api/risk`、`/api/spot-arb/dashboard`、`/api/cross-arb/dashboard` 等。
- 包含 command endpoints：`/api/control/pause`、`/api/control/resume`、`/api/control/kill_switch`、exchange/symbol pause/resume/disable/cancel/liquidation 等。
- 包含本地副作用：编辑 strategy YAML、执行 restart script、写 `control_commands.jsonl`、写 balance/profit history。
- 包含 credential/env-store 管理：`/api/exchange-api-keys`，当前 request shape 有 raw key/secret/passphrase 字段。
- 包含 concrete exchange/strategy coupling：直接使用 `exchange_registry::market_adapter`、`SpotSpotTakerArbitrageConfig`、`CrossExchangeArbitrageConfig`。

问题：

- 文件同时承担 API 契约、HTTP route、本地文件 IO、配置编辑、credential 管理、strategy-specific dashboard 和 exchange adapter probing。
- 它可以继续支撑本地控制台，但不应作为 SaaS/local-agent 长期边界。

迁移定位：

- 保留为 legacy compatibility binary，短期不要继续扩大。
- 可迁移的是 route shape/read model shape/command acknowledgement shape；不可直接迁移的是 raw secret update、concrete adapter probing、strategy YAML parser、restart script invocation。

### `crates/rustcta-control-api/`

当前职责：

- 已经是独立 crate，当前暴露 generic multi-strategy/process API：
  - `/api/health`
  - `/api/strategies`
  - `/api/strategies/:id`
  - `/api/strategies/:id/snapshot`
  - `/api/strategies/:id/command`
  - `/api/processes`
  - `/api/gateway/status`
  - `/api/events`
- `ControlApiStateSnapshot` 包含 `schema_version`、`generated_at`、`strategies`、`gateway`、`agents`。
- 依赖 `rustcta-supervisor` 和 `rustcta-exchange-gateway`，没有直接依赖主 crate concrete exchange adapters。

问题：

- 目前只有最小 in-memory state 和 stub route，尚未覆盖 Dioxus legacy 面板需要的大多数 read model。
- DTO 仍复用 `StrategyProcess` 作为过渡期 process/strategy read model，但
  gateway 已经改为本地 `GatewayStatusView` public DTO，避免控制 API crate
  直接依赖 gateway crate 的实现状态。

迁移定位：

- 这是目标 control API contract 的落点。
- 下一步应继续扩展 route/model/service 模块，而不是继续把 logic 加到
  `retired root bin directory/control_api.rs`。

### `apps/control-api/`

当前职责：

- 最小 binary wrapper：读取 `RUSTCTA_CONTROL_API_BIND`，初始化空 `ControlApiState`，启动 axum router。

迁移定位：

- 负责 runtime wiring，不定义 public API shape。
- 未来可以装配 snapshot reader、supervisor client、event ledger、agent connector、static asset service。
- 允许处理本地路径、bind address、TLS/reverse-proxy 配置，但不 import concrete exchange adapters，不解析 raw strategy secret。

### `web-ui/dioxus/`

当前职责：

- Dioxus frontend，保留 `api.rs`、`app.rs`、`components.rs`、`i18n.rs`、`storage.rs`、`types.rs`、`utils.rs`。
- 当前 `api.rs` 用 `serde_json::Value` 调 legacy `/api/*` endpoints。
- UI 有 overview、spot arb、cross arb、exchange、symbol、plans、risk、runtime、config、logs、api keys 等面板。
- 当前仍调用 legacy endpoints，例如 `/api/exchange-api-keys`、`/api/strategy-config`、`/api/cross-arb/settings`、`/api/control/symbols`。

迁移定位：

- UI 代码继续留在 `web-ui/dioxus/`。
- UI 应从单策略 dashboard 转成 workspace shell：workspace/agent/process/strategy list first，然后进入 strategy detail panels。
- UI 可以显示 redacted credential status 和 setup state，但不应保留 raw exchange secret DTO、本地存储密钥或 exchange-specific adapter schema。

## Read Model / API Shape 归属

### 应进入 `crates/rustcta-control-api/`

这些是跨策略、跨进程、可 SaaS/local agent 复用的 public contract：

- Workspace/agent:
  - `WorkspaceSummary`
  - `AgentSummary`
  - `AgentConnectionStatus`
  - `AgentCapabilityView`
  - `AgentHeartbeatView`
- Process/supervisor:
  - `ProcessSummary`
  - `ProcessDetail`
  - `ProcessHealthView`
  - `StrategyProcessView`
  - `LifecycleCommandRequest`
  - `LifecycleCommandAccepted`
  - `LifecycleCommandRecord`
- Strategy:
  - `StrategySummary`
  - `StrategyDetail`
  - `StrategyRuntimeStatus`
  - `StrategyConfigSummary`
  - `StrategySnapshotEnvelope`
  - `StrategyLogEvent`
  - `StrategyKind`
  - `RunId` / `StrategyId` / `AgentId` wrapper shape
- Gateway/execution read model:
  - `GatewayStatusView`
  - `ExchangeConnectionView` using exchange id strings, not adapter types
  - `MarketDataHealthView`
  - `AccountBalanceSummaryView`
  - `OrderSummaryView`
  - `FillSummaryView`
  - `ExecutionModeView`
- Risk/readiness:
  - `RiskSummaryView`
  - `RiskEventView`
  - `KillSwitchView`
  - `LivePreflightSummaryView`
  - `ReadinessCheckView`
  - `OrderReconciliationSummaryView`
  - `BalanceReconciliationSummaryView`
- Operator command/audit:
  - `OperatorCommandRequest`
  - `OperatorCommandAccepted`
  - `OperatorCommandAuditRecord`
  - `CommandTarget` with workspace/agent/process/strategy/exchange/symbol scopes
  - `CommandSafetyFlags` with `would_submit_order` and `applied_to_runtime`
- Control/read-only strategy panels:
  - `ControlSymbolSummary`
  - `ControlSymbolDetail`
  - `SymbolLifecycleView`
  - `DisabledScopeView`
  - `RuntimePublisherHealthView`
- Arbitrage/scanner summary shape:
  - `OpportunitySummary`
  - `ArbitrageStatisticsView`
  - `ScannerRecommendationView`
  - `CapitalEfficiencyView`
  - Strategy-specific details may live inside `StrategySnapshotEnvelope.detail: serde_json::Value` until the strategy crates publish stable DTOs.
- Credential status only:
  - `CredentialSlotStatus`
  - `CredentialHealthView`
  - `CredentialRequirementView`
  - No raw credential update DTO with secret-bearing fields.

Suggested module layout:

```text
crates/rustcta-control-api/src/
  lib.rs
  router.rs
  state.rs
  auth.rs
  models/
    mod.rs
    workspace.rs
    agent.rs
    process.rs
    strategy.rs
    gateway.rs
    market.rs
    execution.rs
    risk.rs
    control.rs
    audit.rs
    credentials.rs
  routes/
    mod.rs
    health.rs
    workspace.rs
    agents.rs
    processes.rs
    strategies.rs
    gateway.rs
    commands.rs
    events.rs
```

Current implementation has landed the first version of this split:

```text
crates/rustcta-control-api/src/
  lib.rs
  models.rs
  router.rs
  routes.rs
  state.rs
```

The finer `models/*` and `routes/*` directory split remains a later cleanup
once the DTO set expands beyond the current workspace/agent/process/strategy/
gateway/credential skeleton.

Route shape target:

```text
GET  /api/health
GET  /api/workspace
GET  /api/agents
GET  /api/agents/:agent_id
GET  /api/processes
GET  /api/processes/:process_id
GET  /api/strategies
GET  /api/strategies/:strategy_id
GET  /api/strategies/:strategy_id/snapshot
GET  /api/strategies/:strategy_id/logs
GET  /api/strategy-logs
POST /api/commands
GET  /api/commands/:command_id
GET  /api/audit
GET  /api/gateway/status
GET  /api/events
GET  /api/credentials/status
```

Current implementation status:

- `GET /api/workspace` exists and returns generic workspace counts plus
  `GatewayStatusView`.
- `GET /api/agents` exists and returns generic agent summaries.
- `GET /api/agents/:agent_id` exists and returns one generic agent summary or
  `404` when the agent is absent.
- `GET /api/processes/:process_id` exists and returns one sanitized
  `StrategyProcessView` or `404` when the process is absent.
- `POST /api/commands` exists as a generic lifecycle command acceptance route.
- `GET /api/commands/:command_id` exists and returns the accepted lifecycle
  command record from the in-memory command log or `404` when absent.
- `GET /api/credentials/status` exists and returns redacted/status-only slots.
- The new `rustcta-control-api` crate does not expose `/api/exchange-api-keys`
  or raw key/secret/passphrase update DTOs. Legacy credential editing remains
  frozen outside the new control API boundary until it can be replaced by a
  gateway/agent-owned credential service.
- `GET /api/risk`, `GET /api/risk/events`, `GET /api/fees`, and
  `GET /api/logs` exist as generic, secret-free read-model contracts. Empty
  local state returns stable empty/default views.
- `GET /api/strategy-logs` exists as a read-only migration bridge for the
  legacy Dioxus log panel. It tails only an explicitly configured local log path,
  limits bytes/lines, redacts sensitive marker lines, and does not return the
  host file path.
- `GET /api/processes/:id/logs` and `GET /api/strategies/:id/logs` exist as
  generic multi-process log tail routes. They use the selected process
  `log_path` from supervisor state internally, but public process/strategy
  responses expose only `log_configured` instead of host file paths.
- `rustcta-control-api` no longer depends on `rustcta-exchange-gateway`; gateway
  status is represented as a local public DTO until a source adapter is wired
  by `apps/control-api`.
- `apps/control-api` now uses `ControlApiState::empty_local()` so app startup
  wiring stays thin and does not define DTO defaults itself.
- `rustcta-control-api` has a JSON-value legacy dashboard snapshot adapter for
  risk, fee, and log summaries. It intentionally parses sanitized snapshot JSON
  instead of importing `src/web::DashboardReadModel` or legacy exchange/strategy
  types.
- `apps/control-api` can wire that adapter with
  `RUSTCTA_CONTROL_API_LEGACY_SNAPSHOT_PATH`. The path is re-read on request so
  the new API follows the latest sanitized legacy snapshot during migration.
- `apps/control-api` can follow a supervisor registry snapshot with
  `RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH`. Supervisor processes are
  composed with the legacy dashboard snapshot so multi-process state is not
  lost when risk/fee/log summaries still come from the legacy snapshot writer.
- `apps/control-api` can publish a local agent summary with
  `RUSTCTA_CONTROL_API_AGENT_ID`, `RUSTCTA_CONTROL_API_TENANT_ID`, and
  `RUSTCTA_CONTROL_API_AGENT_CAPABILITIES`. This gives `/api/workspace`,
  `/api/agents`, and `/api/agents/:agent_id` a non-empty local deployment
  source without introducing a remote agent protocol into the API crate.
- `apps/control-api` can wire the strategy log migration bridge with
  `RUSTCTA_CONTROL_API_STRATEGY_LOG_PATH`,
  `RUSTCTA_CONTROL_API_STRATEGY_LOG_TAIL_LINES`, and
  `RUSTCTA_CONTROL_API_STRATEGY_LOG_TAIL_BYTES`. These settings are process
  wiring only; the public API crate still exposes a generic DTO rather than
  legacy log discovery policy.
- `ControlApiState` can attach a `rustcta-event-ledger` store. Accepted
  lifecycle commands are recorded in memory and, when a store is configured,
  appended as secret-free `OperatorCommandEvent` records before the HTTP route
  returns success.
- `GET /api/events` returns the in-memory command list plus durable
  `ledger_events` replayed from the configured store.
- `GET /api/audit` exists and returns audit/operator-command events replayed
  from the configured store.
- `apps/control-api` can wire a durable JSONL command-audit ledger with
  `RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH`, while leaving the library free of
  local file-path policy. The same JSONL store is used for command audit writes
  and event/audit reads.
- `apps/control-api` can optionally serve the built Dioxus panel with
  `RUSTCTA_CONTROL_API_STATIC_DIR`, falling back to `index.html` for SPA
  routes. Static-file policy lives in the app boundary, not in
  `crates/rustcta-control-api`.

Legacy endpoint aliases can remain during migration, but new UI work should target the generic routes above.

### 应留在 `web-ui/dioxus/`

这些属于前端体验和本地 browser state：

- `web-ui/dioxus/src/app.rs`: workspace shell、navigation、refresh orchestration。
- `web-ui/dioxus/src/api.rs`: HTTP client；迁移后应拆成 typed client，legacy client 只做兼容。
- `web-ui/dioxus/src/components.rs`: UI components；后续按 workspace/process/strategy/risk/logs/credentials 拆分。
- `web-ui/dioxus/src/types.rs`: UI-only view state、language enum、table rows、form state；公共 DTO 应尽量来自 control API crate 的 generated/bindings mirror 或明确命名为 UI adapter type。
- `web-ui/dioxus/src/i18n.rs`: 文案。
- `web-ui/dioxus/src/storage.rs`: browser token/language 等 UI preference；不得存 exchange credentials。
- `web-ui/dioxus/assets/main.css`: 样式。
- `web-ui/dioxus/dist/`、`target/`: 构建产物，不作为源码迁移对象。

UI 迁移方向：

- 第一屏从单策略 dashboard 改为 operator workspace：agents、processes、strategies、gateway health。
- Strategy detail 内再挂 spot/cross/scanner/risk/runtime panels。
- `/api/exchange-api-keys` 面板改为 credential status/setup handoff。长期 UI 不保留 raw key/secret/passphrase state，不写 local storage。
- `/api/strategy-config` raw YAML 编辑器降级为 local-only legacy panel；长期目标是 config schema/draft/review command，不是浏览器直接编辑运行时 YAML。

### Legacy `src/web/` 暂时保留

保留内容：

- `src/web/models.rs`
  - `MonitoringConfig`
  - `DashboardReadModel`
  - 当前 runtime 需要的 `*View`
  - `status_from_model`
- `src/web/state.rs`
  - `MonitoringState`
  - `publish_*` methods
  - runtime snapshot 内存聚合
- `src/web/server.rs`
  - sanitized snapshot writer
  - secret-like key guard
- `src/web/mod.rs`
  - compatibility exports

不应在这里继续新增：

- 新 control API route。
- 新 web panel public contract。
- exchange credential 写入 shape。
- concrete adapter probing。

后续当 strategy runtime 已经改为通过 event ledger/gateway/supervisor 发布 typed status 后，再把 `src/web/` 缩小为兼容 adapter 或删除。

## Security / Boundary Rules

强制边界：

- `crates/rustcta-control-api/` 不依赖 root `rustcta` crate，不依赖 `retired exchange tree/*`，不调用 exchange registry。
- `apps/control-api/` 不实例化 concrete exchange adapter；如果需要交易所状态，只读 `rustcta-exchange-gateway`、supervisor、event ledger 或 local agent provider。
- `web-ui/dioxus/` 不 import Rust exchange crates，不定义 raw credential persistence，不在 local storage 保存 exchange secrets。
- Public API response 不返回 raw secret-like fields。字段名也应避免 `api_key`、`api_secret`、`secret`、`passphrase`、`authorization`，除非是 HTTP auth 文档上下文。
- Strategy config summary 必须 redacted；raw YAML 编辑和 restart script 只允许作为 legacy local mode，不能成为 SaaS contract。
- Cross-arb instrument discovery 不能由 control API 直接调用 `exchange_registry::market_adapter`；应由 gateway/agent 产出 read model，control API 只读取结果。

建议边界检查：

```text
scripts/check_industrial_boundaries.sh
  - forbid `legacy root crate path exchanges` in crates/rustcta-control-api
  - forbid concrete exchange module names in crates/rustcta-control-api and web-ui/dioxus/src
  - forbid raw secret field names in public DTO files
  - forbid `exchange_registry::` outside gateway/local adapter paths
```

## 并行后续任务

### 任务 A：Control API DTO 模块拆分

Scope:

- `crates/rustcta-control-api/src/models/mod.rs`
- `crates/rustcta-control-api/src/models/workspace.rs`
- `crates/rustcta-control-api/src/models/agent.rs`
- `crates/rustcta-control-api/src/models/process.rs`
- `crates/rustcta-control-api/src/models/strategy.rs`
- `crates/rustcta-control-api/src/models/gateway.rs`
- `crates/rustcta-control-api/src/models/risk.rs`
- `crates/rustcta-control-api/src/models/control.rs`
- `crates/rustcta-control-api/src/models/audit.rs`
- `crates/rustcta-control-api/src/models/credentials.rs`

Deliverable:

- 把 public DTO 从 `lib.rs` 拆出来。
- DTO 只使用 primitive/string/newtype/serde/chrono，不引用 concrete exchange adapter。
- `credentials.rs` 只能包含 redacted/status shape。

可并行性：

- 可与 UI shell、app wiring、legacy endpoint inventory 并行。
- 不应与 route split 同时改同一个 `lib.rs` 大块；先合并 DTO 再合并 routes 更稳。

### 任务 B：Control API route/service 拆分

Scope:

- `crates/rustcta-control-api/src/router.rs`
- `crates/rustcta-control-api/src/state.rs`
- `crates/rustcta-control-api/src/routes/health.rs`
- `crates/rustcta-control-api/src/routes/workspace.rs`
- `crates/rustcta-control-api/src/routes/agents.rs`
- `crates/rustcta-control-api/src/routes/processes.rs`
- `crates/rustcta-control-api/src/routes/strategies.rs`
- `crates/rustcta-control-api/src/routes/gateway.rs`
- `crates/rustcta-control-api/src/routes/commands.rs`
- `crates/rustcta-control-api/src/routes/events.rs`

Deliverable:

- `lib.rs` 只 re-export public router/state/models。
- 保留现有 `/api/strategies/*`、`/api/processes`、`/api/gateway/status` route 行为。
- 新增 generic `/api/workspace`、`/api/agents`、`/api/commands` skeleton。

可并行性：

- 可与 UI typed client 并行，只要 route shape 先在文档/DTO 中冻结。

### 任务 C：App wiring 与 retired binary 分界

Scope:

- `apps/control-api/src/main.rs`
- `apps/control-api/Cargo.toml`
- `scripts/separated_control_panel.sh`
- `docs/dioxus_control_panel.md`

Deliverable:

- `apps/control-api` 成为新 control API 默认入口。
- 静态资源服务、bind/env、snapshot source provider 放 app 层。
- `retired root bin directory/control_api.rs` 标记为 legacy compatibility path，脚本保留开关以便回退。

注意：

- 不要把 `retired root bin directory/control_api.rs` 的 exchange registry、API key env-store、strategy YAML parser 直接搬进 `crates/rustcta-control-api`。

### 任务 D：Legacy dashboard snapshot adapter

Scope:

- `src/web/models.rs`
- `src/web/server.rs`
- 新增时优先放 app/local adapter 层，例如 `apps/control-api/src/legacy_snapshot.rs`

Deliverable:

- 过渡期读取 `data/control_api/dashboard_snapshot.json`，转换为 `StrategySnapshotEnvelope` 或 `StrategyDetail`。
- `src/web` 仍只负责 runtime 写 snapshot。
- 新 contract 与 legacy `DashboardReadModel` 解耦，避免长期 public API 被主 crate 内部类型锁死。

可并行性：

- 可与 DTO/route split 并行，但需要等 `StrategySnapshotEnvelope` shape 稳定。

### 任务 E：Dioxus workspace shell

Scope:

- `web-ui/dioxus/src/app.rs`
- `web-ui/dioxus/src/components.rs`
- `web-ui/dioxus/src/types.rs`
- `web-ui/dioxus/assets/main.css`

Deliverable:

- 第一屏显示 workspace summary、agents、processes、strategies、gateway status。
- 原 spot/cross/risk/runtime panels 移到 strategy detail 下。
- 保留 legacy panel 入口，但显式通过 legacy client 调用旧 `/api/*`。

可并行性：

- 可与 API DTO/route split 并行。
- 合并前只需要 mock JSON 或当前 crate skeleton route。

### 任务 F：Dioxus typed API client

Scope:

- `web-ui/dioxus/src/api.rs`
- `web-ui/dioxus/src/types.rs`
- 可选新增 `web-ui/dioxus/src/control_api_types.rs`

Deliverable:

- 把当前全量 `serde_json::Value` fetching 收敛为 `WorkspaceFetch`、`StrategyFetch`、`GatewayFetch` 等 typed client。
- legacy endpoints 放到单独 `legacy_*` functions，便于之后删除。
- 不在 UI type 中保留 raw `api_key` / `api_secret` / `passphrase` 字段。

可并行性：

- 可与 workspace shell 并行，但两者会碰 `api.rs/types.rs`，建议先合并 typed client skeleton。

### 任务 G：Credential boundary replacement

Scope:

- `retired root bin directory/control_api.rs` legacy `/api/exchange-api-keys` 保留但冻结。
- `crates/rustcta-control-api/src/models/credentials.rs`
- `web-ui/dioxus/src/components.rs` 中 `ApiKeysPanel`
- 后续 local agent/credential service 目录，具体目录另行确定。

Deliverable:

- 新 contract 只有 credential status/health/required fields，不包含 raw secret update request。
- UI 显示 credential slots、configured/missing/error 状态。
- Secret 写入改为 local-agent-only handoff，不进入 SaaS control API contract。

可并行性：

- 可与 route split 并行。
- 不应和 exchange adapter 开发混在一个 PR。

### 任务 H：Config editing 降级与 schema 化

Scope:

- `retired root bin directory/control_api.rs` legacy `/api/strategy-config` 保留。
- `crates/rustcta-control-api/src/models/strategy.rs`
- `web-ui/dioxus/src/components.rs` 中 `ConfigPanel`

Deliverable:

- 新 control API 暴露 `StrategyConfigSummary`、`ConfigSchemaView`、`ConfigDraftCommand`。
- Raw YAML editor 只保留在 local legacy mode。
- Restart script invocation 留在 app/local-agent，不进入 crate contract。

可并行性：

- 可与 credential replacement 并行。

### 任务 I：Boundary checks

Scope:

- `scripts/check_industrial_boundaries.sh`
- 根 `Cargo.toml`
- `crates/rustcta-control-api/Cargo.toml`
- `web-ui/dioxus/Cargo.toml`

Deliverable:

- CI/本地脚本检查 control API 和 web UI 不引用 concrete adapters。
- 检查 public DTO 不新增 secret-like field。
- 检查 `crates/rustcta-control-api` 不依赖 root `rustcta` crate。

可并行性：

- 可独立执行，建议在 DTO/route split 后马上加。

## 建议迁移顺序

1. 先冻结新 public route/DTO 名称：`workspace`、`agents`、`processes`、`strategies`、`gateway`、`commands`、`audit`、`credentials/status`。
2. 拆 `crates/rustcta-control-api` 的 DTO 和 routes，让 `apps/control-api` 继续能启动。
3. 加 boundary checks，防止 `retired root bin directory/control_api.rs` 的 adapter/secret coupling 被搬进新 crate。
4. 给 `apps/control-api` 加 legacy snapshot adapter，支持读取现有 `src/web` snapshot。
5. Dioxus 改成 workspace shell + typed client，legacy panels 暂时挂在 strategy detail 下。
6. credential/config 两块从 legacy local console 中剥离：新 API 只暴露 redacted status/schema/command，实际 secret/config write 交给 local agent。
7. 当新 control API + Dioxus 覆盖主要操作路径后，脚本切默认入口到 `apps/control-api`，`retired root bin directory/control_api.rs` 只保留回退窗口。

## 不在本轮做的事

- 不开发新的交易所 API。
- 不移动 concrete exchange adapter。
- 不把 `retired root bin directory/control_api.rs` 的全部逻辑机械拆进新 crate。
- 不删除 `src/web/`。
- 不把 raw secret DTO 引入 `crates/rustcta-control-api` 或 `web-ui/dioxus`。

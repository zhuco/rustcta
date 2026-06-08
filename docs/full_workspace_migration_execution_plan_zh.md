# RustCTA 全量迁移执行计划

状态日期：2026-06-08

本文档用于指导一次全量迁移，把仍在 legacy `src/` 中承载的业务实现收口到
workspace 边界：`crates/`、`apps/`、`tools/`、`strategies/`、`web-ui/`。

本轮迁移的两个关键决策：

- 回测系统不再作为目标系统迁移。`apps/backtest`、`crates/retired-backtest`、
  root `backtest` / `retired_short_ladder_grid`、相关 tests/docs/scripts 应进入
  退役流程，而不是继续补齐。
- 交易所网关仍保留为凭证和交易所连接边界，但不再作为大规模新功能
  workstream。已有 `rustcta-exchange-gateway` 实现优先复用；只做 active live
  runtime 需要的缺口审计和最小补齐。不能为了迁移重新扩张交易所 endpoint。

## 目标状态

- root `rustcta` package 不再承载业务实现，最终可以移除 legacy `src/`。
- live strategy 只能通过 strategy SDK、execution router、event ledger、
  supervisor、control API、gateway client 等边界交互。
- 只有 gateway/local agent 能持有或操作交易所凭证。
- Web/control/API 只暴露 secret-free read model 和受审计的 operator command。
- 回测相关包、bin、测试、文档从 active workspace 中移除。

## 全局规则

- 每个 AI 任务先运行 `git status --short`，不要覆盖其他任务的修改。
- 共享文件只由任务 1 或最终清理任务修改：root `Cargo.toml`、`Cargo.lock`、
  `src/lib.rs`、`scripts/check_industrial_boundaries.sh`、CI workflow、
  `docs/README.md`。
- 不新增交易所签名、REST、WS 或下单行为；只允许为 active runtime
  打通已确认缺口。
- 不把 raw API key、secret、passphrase 写入 control API、web UI、strategy
  crate、ordinary app 或 logs。
- `strategies/*` 不得依赖 legacy root `rustcta`、`rustcta-exchange-gateway`、
  `rustcta-execution-router` 或 concrete adapter。
- `tools/ops` 不得依赖 legacy root；需要复用行为时先抽到 root-free crate。
- 每个任务必须提交自己的 focused validation；合并前由任务 16 跑全仓 gates。

基础 gates：

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
scripts/check_industrial_boundaries.sh
```

## 任务清单

### 1. 迁移协调与库存冻结

目标：建立全量迁移的唯一事实来源，冻结 legacy 库存和共享文件编辑权。

范围：

- `docs/full_workspace_migration_execution_plan_zh.md`
- `scripts/check_industrial_boundaries.sh`
- root `Cargo.toml`
- `src/lib.rs`
- legacy bin matrix in `tools/ops`

执行：

- 重新生成并确认 `retired root bin directory/*.rs`、`src/*` 模块、workspace members、策略包、
  app 包、tool 包的库存。
- 给每个 legacy area 标记目标：迁移、保留 wrapper、删除、冻结。
- 在 boundary check 中阻止新代码回流到 `src/`。
- 为任务 2-15 分配文件所有权，避免多个 AI 同时改同一模块。

不做：

- 不迁业务逻辑。
- 不删除目录。

验收：

```bash
cargo run -q -p rustcta-tools-ops -- verify-retired-src
cargo run -q -p rustcta-tools-ops -- verify-retired-src
scripts/check_industrial_boundaries.sh
```

#### 任务 1 冻结结果

本节是任务 1 的当前库存冻结点。后续任务发现库存变化时，先回到任务 1
更新本节和 `tools/ops` legacy bin matrix，不要在业务任务中顺手改共享清单。

当前冻结基线：`src/` 目录已从工作树移除，root `Cargo.toml` 已是 virtual
workspace manifest，不再暴露 root package、root lib 或 root binary target。
因此当前 direct `retired root bin directory/*.rs` 文件库存为 0。`tools/ops/src/lib.rs` 的
`LEGACY_BIN_MIGRATIONS` 仍保留最后一批 17 个 legacy bin 的 retirement
matrix，用于审计历史兼容入口、替代命令和最终清理结果；这些 entry 不表示
当前文件仍存在。

| Historical legacy bin | 当前目标 | 处理任务 |
| --- | --- | --- |
| `account_position_reporter.rs` | tools/ops reporter wrapper | 13 |
| `backtest.rs` | backtest retired surface | 2 |
| `bitget_order_canary.rs` | tools/ops canary wrapper | 13 |
| `bitget_spot_order_canary.rs` | tools/ops canary wrapper | 13 |
| `control_api.rs` | control-api app compatibility | 6 |
| `cross_arb_account_audit.rs` | tools/ops audit wrapper | 10, 13 |
| `cross_arb_fee_audit.rs` | tools/ops audit wrapper | 10, 13 |
| `cross_arb_live.rs` | supervised strategy runtime | 10, 14 |
| `cross_arb_observe.rs` | supervised/CLI observe runtime | 10 |
| `cross_arb_order_admin.rs` | tools/ops admin wrapper | 10, 13 |
| `cross_arb_preflight.rs` | industrial CLI command | 10, 13 |
| `cross_arb_ws_opportunity_probe.rs` | tools/ops probe | 10, 13 |
| `exchange_order_canary.rs` | tools/ops canary wrapper | 13 |
| `funding_arb_live.rs` | supervised strategy runtime | 11, 14 |
| `funding_arb_observe.rs` | supervised/CLI observe runtime | 11, 13 |
| `hyperliquid_self_test.rs` | tools/ops probe wrapper | 13 |
| `retired_short_ladder_grid.rs` | backtest retired surface | 2 |

当前 direct `src/*` area 库存为 0，因为 `src/` 已缺席。下表记录最后一批
legacy area 的目标归属，作为防回流 allowlist 的历史说明；新增任何 `src/**`
业务文件都必须失败，除非任务 1 重新打开库存冻结。

| Historical legacy area | 目标 | 处理任务 |
| --- | --- | --- |
| `src/analysis` | root-free reporting/core helper 或删除未用分析 facade | 4, 16 |
| `retired root bin directory` | wrapper-only，最终删除 | 2, 6, 10, 11, 13, 14, 16 |
| `src/control` | `rustcta-runtime-control` / `rustcta-control-api` | 5, 6 |
| `src/core` | `rustcta-types` / root-free helper / `rustcta-core-compat` 过渡 | 4 |
| `src/cta` | account/runtime-neutral helper 或删除 | 4, 7 |
| `src/data` | market-data contracts/provider，禁止回测扩展 | 12 |
| `retired exchange tree` | gateway 缺口审计后迁出或冻结 wrapper | 3, 7, 16 |
| `src/execution` | `rustcta-execution-api` / router / ledger | 7 |
| `src/live_preflight` | `rustcta-runtime-control` / CLI preflight DTO | 5, 13 |
| `src/market` | `rustcta-types` / runtime snapshot market contracts | 4, 12 |
| `src/risk` | `rustcta-runtime-control` risk contracts | 5 |
| `src/scanner` | `rustcta-runtime-control` scanner read models | 5 |
| `src/smart_money` | already migrated; keep deleted/frozen until final cleanup | 16 |
| `retired strategy tree` | strategy crates or retired research-only code | 8, 9, 10, 11 |
| `src/utils` | root-free helper crates or delete unused helpers | 4 |
| `src/web` | control-api typed snapshot/read model | 6, 12, 15 |

当前 workspace member 冻结点来自 root `Cargo.toml`。Active members 包括
`crates/rustcta-{types,exchange-api,exchange-gateway,execution-api,
execution-router,event-ledger,strategy-sdk,supervisor,control-api,reporting,
smart-money}`、`apps/{gateway,supervisor,control-api,cli}`、`tools/ops`、以及
9 个 `strategies/*` crate。`crates/rustcta-core-compat` 和
`crates/rustcta-runtime-control` 目录当前存在但不在 root workspace members 中；
`rustcta-runtime-control` 仍被 `crates/rustcta-control-api` 作为 path dependency
引用，由任务 5 决定正式纳入 workspace 或合并到现有控制边界。
`apps/backtest` 和 `crates/retired-backtest` 当前 manifest 已缺席；backtest
retirement 已落到当前库存基线。

任务 2-15 文件所有权如下。未列入的共享文件默认仍归任务 1 或任务 16。
如果某个业务任务需要改 root `Cargo.toml`、`Cargo.lock`、`src/lib.rs`、
`scripts/check_industrial_boundaries.sh`、CI workflow 或 `docs/README.md`，
该任务只能提交明确的 manifest/check/docs diff 建议；实际共享文件合并由
任务 1 或任务 16 执行，除非任务 1 后续显式授权唯一例外。

| 任务 | 主要写范围 | 禁止顺手修改 |
| --- | --- | --- |
| 2 | 已退役 backtest surface 的剩余 docs/tests/scripts 引用审计；root manifest/lockfile 只提交 diff 建议，由任务 1/16 合并 | gateway、strategy live runtime、control API 业务行为 |
| 3 | `crates/rustcta-exchange-api`、`crates/rustcta-exchange-gateway`、`apps/gateway`、gateway docs | strategy crate 直接 adapter 依赖、control API secret routes |
| 4 | `src/core`、`src/utils`、`src/cta`、`src/market` pure contracts、`rustcta-types`、`rustcta-core-compat` | live adapter behavior、route behavior |
| 5 | `src/control`、`src/risk`、`src/scanner`、`src/live_preflight`、`rustcta-runtime-control` | remote mutation API、exchange signing |
| 6 | `retired root bin directory/control_api.rs`、`crates/rustcta-control-api`、`apps/control-api` | raw credential write/delete promotion |
| 7 | `src/execution`、`rustcta-execution-api`、`rustcta-execution-router`、`rustcta-event-ledger` | live order safety default changes |
| 8 | `rustcta-strategy-sdk`、strategy crate boundary tests/checks | concrete adapter or process ownership |
| 9 | `retired strategy tree/spot_spot_taker_arbitrage`、`strategies/spot-spot-arbitrage` | cross/funding runtime behavior |
| 10 | `cross_arb_*` bins/support modules、`strategies/cross-exchange-arbitrage`、related CLI/tools commands | funding/spot runtime behavior |
| 11 | funding/grid/trend/other strategy legacy dirs and corresponding strategy crates |回测/research-only resurrection |
| 12 | `src/data`、`src/market`、`src/web` snapshot/data contracts | new exchange WS topics |
| 13 | remaining operator tools/probes/canaries/reporters in `tools/ops` and `apps/cli` | tools dependency on legacy root |
| 14 | `rustcta-supervisor`、`apps/supervisor`、`config/supervisor/*.spec.json` | strategy implementation code |
| 15 | `web-ui/dioxus`、control panel API client/static serving docs | raw private payload UI or shell side effects |

Boundary freeze already expected by task 1:

- `scripts/check_industrial_boundaries.sh` must keep enforcing legacy bin
  classification, no strategy/root/gateway-private leakage, non-gateway app
  gateway isolation, tools no-root dependency, and raw credential route freeze.
- Reintroducing `src/` or any direct `retired root bin directory/*.rs` file is disallowed in the
  current baseline. If a future recovery task must restore a legacy file, task 1
  must first reopen the freeze and add an owner plus retirement milestone.
- Root `Cargo.toml` must remain a virtual workspace manifest without `[package]`
  or root `[[bin]]` targets.
- Root `src/lib.rs` must remain absent. If restored for emergency compatibility,
  it may only be a disabled facade and must not re-export business modules.

### 2. 回测系统退役

目标：停止继续迁移回测系统，并从 active workspace 中移除回测 surface。

范围：

- historical `apps/backtest`
- historical `crates/retired-backtest`
- historical root `Cargo.toml` backtest workspace member/dependency/bin
- historical `retired root bin directory/backtest.rs`
- historical `retired root bin directory/retired_short_ladder_grid.rs`
- remaining `tests/backtest_*.rs`
- `docs/backtest_app_migration_plan.md`
- README/docs/scripts 中的 backtest 命令引用

执行：

- 先用 `rg` 找全残余 backtest 命令、crate、test、doc、script 引用。
- 确认 `retired-backtest`、`retired-backtest-app`、root backtest bins
  没有重新进入 workspace 或 root manifest。
- 删除 backtest-only tests；如果某些测试实际覆盖通用数据结构，先迁到对应
  root-free crate 后再删 backtest 名称。
- 更新 docs：标记回测为 retired，不再作为迁移 blocker。
- 从 lockfile、CI 和 docs 中清理残余 backtest targets/references。

不做：

- 不继续抽 `retired backtest tree`。
- 不保留新的 `retired-backtest` 兼容 wrapper，除非某个 active binary 编译
  临时需要；这种 wrapper 必须在任务 16 删除。

验收：

```bash
rg -n "retired-backtest|apps/backtest|crates/retired-backtest|retired root bin directory/backtest|retired_short_ladder_grid|backtest_" Cargo.toml apps crates src tests docs scripts README.md
cargo check --workspace --all-targets
```

### 3. 交易所网关缺口审计

目标：确认现有 gateway 是否足够承接 active live runtime；只补迁移必须的缺口。

范围：

- `crates/rustcta-exchange-api`
- `crates/rustcta-exchange-gateway`
- `apps/gateway`
- `retired exchange tree`
- live strategy / canary / audit 中的 exchange 调用点
- `docs/交易所网关/*`

执行：

- 列出 active runtime 实际需要的能力：行情订阅、order book、余额、持仓、
  手续费、下单、撤单、批量、cancel-all、成交、订单查询、私有流。
- 对照 `rustcta-exchange-gateway` 已有 protocol/client/adapter 能力。
- 给缺口分级：必须补齐、可由旧 wrapper 暂时隔离、非 active 可删除。
- 只为必须缺口补 typed protocol/client/adapter glue；不新增交易所产品线。
- 明确 `retired exchange tree` 中哪些代码仍被 active runtime 调用，作为后续任务输入。

不做：

- 不重写 gateway。
- 不新增未被 active runtime 使用的交易所 endpoint。
- 不让 strategy crate 直接 import gateway adapters。

验收：

```bash
cargo test -p rustcta-exchange-api
cargo test -p rustcta-exchange-gateway
cargo check -p rustcta-gateway-app
scripts/check_industrial_boundaries.sh
```

### 4. Runtime-neutral 类型、配置和工具收口

目标：把仍被多处共享的纯类型、symbol、money、time、config、logging helper
从 legacy root 移到 root-free crate。

范围：

- `src/core`
- `src/utils`
- `src/cta/account_manager.rs`
- `src/market/*` 中的纯 contract
- `crates/rustcta-types`
- `crates/rustcta-core-compat`
- 必要时新增或整理 root-free helper module

执行：

- 识别无副作用、无 exchange client、无 app 状态的共享模块。
- 优先放入 `rustcta-types` 或已有专门 crate；`rustcta-core-compat` 只作为过渡
  compatibility，不作为长期业务聚合层。
- 更新 app/strategy/tool/control/execution 引用，禁止新代码从 `legacy root crate path *`
  取共享类型。
- 给已迁出的模块添加 boundary check。

不做：

- 不迁 live exchange adapter。
- 不迁 control API route。

验收：

```bash
cargo check --workspace --all-targets
scripts/check_industrial_boundaries.sh
```

### 5. Runtime control、risk、scanner 收口

目标：把运行控制、风险开关、scanner/read-model 合约从 legacy root 迁到
`rustcta-runtime-control` 或现有控制 crate。

范围：

- `src/control`
- `src/risk`
- `src/scanner`
- `src/live_preflight`
- `crates/rustcta-runtime-control`
- `crates/rustcta-control-api`
- `strategies/*` 的风险/控制声明

执行：

- 将 kill switch、disabled registry、hedge policy、spot control snapshot、
  publisher health、scanner coverage、live preflight DTO 迁到 root-free crate。
- 将 `crates/rustcta-runtime-control` 正式纳入 workspace 或合并到现有
  control/risk crate。
- control API 和策略 runtime 只依赖 root-free DTO，不依赖 `src/control`。
- legacy root 只保留 wrapper 到迁出实现，最终任务 16 删除。

不做：

- 不开放新的远程 mutation API。
- 不处理 exchange signing。

验收：

```bash
cargo test -p rustcta-runtime-control
cargo test -p rustcta-control-api
cargo check --workspace --all-targets
scripts/check_industrial_boundaries.sh
```

### 6. Control API 与 local side effects 拆分

目标：替换 `retired root bin directory/control_api.rs` 的 active control surface，并冻结旧 raw key
和本地副作用路径。

范围：

- `retired root bin directory/control_api.rs`
- `crates/rustcta-control-api`
- `apps/control-api`
- `web-ui/dioxus/src/api*.rs`
- supervisor registry / event ledger integration

执行：

- 将 legacy dashboard secret-free read routes 迁到 `rustcta-control-api`。
- 将 restart script、strategy config edit、本地文件写入、command queue 等副作用
  移到 `apps/control-api` local-agent 边界，所有 mutation 先写 audit ledger。
- raw exchange credential 写/删路径不迁入新 public API；后续由 gateway/local
  secret agent 独立接管。
- Web UI 全部改用新 control API routes，不再依赖 legacy route。

不做：

- 不把 legacy raw key store 升级为远程 API。
- 不把 YAML parser 或 exchange registry 塞进 control API library。

验收：

```bash
cargo test -p rustcta-control-api
cargo check -p rustcta-control-api-app
scripts/control_api_smoke_test.sh
scripts/check_industrial_boundaries.sh
```

### 7. Execution router、ledger、account reconciliation 收口

目标：让所有 active 下单/撤单/成交/账户同步路径通过 execution API、router、
event ledger 和 gateway client。

范围：

- `src/execution`
- `crates/rustcta-execution-api`
- `crates/rustcta-execution-router`
- `crates/rustcta-event-ledger`
- strategy execution plan builders
- canary/audit/admin commands

执行：

- 迁出 order command、cancel command、cancel-all、ack、fill、balance
  reconciliation、idempotency、fee readback 相关 root 代码。
- dry-run/live-dry-run/live mutation 都写 append-only ledger。
- live mutation 只能通过 gateway client；禁止 strategy 或 tool 直接调用
  concrete adapter。
- 保留确认 flag、notional gate、kill switch、audit event。

不做：

- 不改变 live order 默认安全语义。
- 不新增交易所 endpoint。

验收：

```bash
cargo test -p rustcta-execution-api
cargo test -p rustcta-execution-router
cargo test -p rustcta-event-ledger
scripts/check_industrial_boundaries.sh
```

### 8. Strategy SDK 边界加固

目标：让所有策略 crate 只表达策略配置、状态、订阅、信号、订单意图和
operator command schema，不拥有交易所连接和进程编排。

范围：

- `crates/rustcta-strategy-sdk`
- `strategies/*`
- `scripts/check_industrial_boundaries.sh`
- strategy crate tests

执行：

- 统一 StrategyRuntime、StrategySpec、StrategySnapshot、MarketDataSubscription、
  execution intent、risk declaration 的 SDK contract。
- 删除策略 crate 中的 wrapper-only legacy 标记，替换为 root-free runtime/core。
- boundary check 禁止 `legacy root crate path `、`retired exchange tree`、gateway adapter、router
  internal import。
- 每个策略 crate 至少有 config parse、snapshot、command schema、market data
  subscription、dry-run execution intent tests。

不做：

- 不在策略 crate 中启动进程或打开网络连接。
- 不在策略 crate 中持有凭证。

验收：

```bash
cargo test -p rustcta-strategy-sdk
cargo test -p rustcta-strategy-spot-spot-arbitrage
cargo test -p rustcta-strategy-cross-exchange-arbitrage
cargo test -p rustcta-strategy-funding-arbitrage
RUSTCTA_STRICT_STRATEGY_MIGRATION=1 scripts/check_industrial_boundaries.sh
```

### 9. 现货-现货套利 runtime 迁移

目标：把 `retired strategy tree/spot_spot_taker_arbitrage` 的 active runtime 收口到
`strategies/spot-spot-arbitrage` 和 supervisor/execution/gateway 边界。

范围：

- `retired strategy tree/spot_spot_taker_arbitrage`
- `strategies/spot-spot-arbitrage`
- spot live config/tests
- market data publisher/subscriber glue
- execution order plan glue

执行：

- 保留现有配置字段、风控 gate、dry-run/live-dry-run 语义。
- runtime crate 输出行情订阅、机会、订单计划、库存/费用/readiness snapshot。
- app/supervisor 层负责启动进程、读取配置、接 gateway/execution/router。
- 删除对 legacy `retired exchange tree`、`src/execution`、`src/web` 的直接依赖。

不做：

- 不改变策略逻辑阈值和交易方向。
- 不扩大支持交易所。

验收：

```bash
cargo test -p rustcta-strategy-spot-spot-arbitrage
cargo test --test spot_spot_live_config
cargo check --workspace --all-targets
scripts/check_industrial_boundaries.sh
```

### 10. 跨所套利 runtime 迁移

目标：把 `cross_arb_live`、`cross_arb_observe`、相关 account/fee/order admin
中可复用的 active runtime 收口到 `strategies/cross-exchange-arbitrage`、
CLI/tools、execution/router 和 supervisor。

范围：

- `retired root bin directory/cross_arb_live.rs`
- `retired root bin directory/cross_arb_observe.rs`
- `retired root bin directory/cross_arb_server/ws.rs`
- `retired root bin directory/cross_arb_preflight.rs`
- `retired root bin directory/cross_arb_account_audit.rs`
- `retired root bin directory/cross_arb_fee_audit.rs`
- `retired root bin directory/cross_arb_order_admin.rs`
- `strategies/cross-exchange-arbitrage`
- `apps/cli`
- `tools/ops`

执行：

- 策略 crate 只保留 root-free core/runtime contract。
- live process 由 supervisor spec 启动，execution mutation 通过 router/gateway。
- observe/preflight/audit/admin 迁到 CLI 或 tools，保留 flag/output/confirmation。
- `cross_arb_server/ws.rs` 改由 control API/websocket read model 或 app-local
  module 接管。

不做：

- 不把旧 monolithic live binary 整体复制到新 app。
- 不绕过 execution ledger 执行订单操作。

验收：

```bash
cargo test -p rustcta-strategy-cross-exchange-arbitrage
cargo test -p rustcta-industrial-cli
cargo test -p rustcta-tools-ops
cargo run -q -p rustcta-tools-ops -- verify-retired-src
scripts/check_industrial_boundaries.sh
```

### 11. 资金费率、网格、趋势与其他策略迁移

目标：把剩余 active strategy 的 root-heavy runtime 拆到对应 strategy crate，
非 active 策略只保留可编译 core 或直接退役。

范围：

- `retired strategy tree/funding_rate_arbitrage`
- `retired strategy tree/hedged_grid`
- `retired strategy tree/range_grid`
- `retired strategy tree/short_ladder_live`
- `retired strategy tree/trend`
- `retired strategy tree/mean_reversion`
- `retired strategy tree/avellaneda_stoikov`
- `retired strategy tree/poisson_market_maker`
- corresponding `strategies/*`

执行：

- 先区分 active live、operator/report-only、research-only、dead code。
- active live 迁 runtime contract、config、risk declaration、market data
  subscription、execution intent。
- research-only 或回测-only 代码随任务 2 退役，不再迁移。
- 删除 `backup_pre_modularization` 和无导出 runtime 的 legacy 策略文件。

不做：

- 不为了保留历史研究代码阻塞 live 迁移。
- 不在 strategy crate 接 concrete adapter。

验收：

```bash
cargo test -p rustcta-strategy-funding-arbitrage
cargo test -p rustcta-strategy-hedged-grid
cargo test -p rustcta-strategy-range-grid
cargo test -p rustcta-strategy-trend
RUSTCTA_STRICT_STRATEGY_MIGRATION=1 scripts/check_industrial_boundaries.sh
```

### 12. Market data 与 runtime snapshot 发布收口

目标：替换 legacy `src/data`、`src/market`、`src/web` 的行情缓存和 dashboard
snapshot producer。

范围：

- `src/data`
- `src/market`
- `src/web`
- `crates/rustcta-types`
- `crates/rustcta-runtime-control`
- `crates/rustcta-control-api`
- `strategies/*` market data subscriptions
- gateway public stream client

执行：

- 将 order book、trade、funding、symbol、precision、route health、publisher
  snapshot DTO 迁到 root-free contract。
- live 行情来源通过 gateway/public market-data client 或 root-free provider
  trait 注入。
- Control API 读取 typed runtime snapshot、supervisor registry、event ledger，
  不再依赖 `src/web` JSON snapshot producer。
- 删除 legacy websocket book/cache 直连路径，或只在 gateway 内保留。

不做：

- 不新增交易所 WS topic。
- 不把 dashboard DTO 和 adapter raw payload 混在一起。

验收：

```bash
cargo test -p rustcta-runtime-control
cargo test -p rustcta-control-api
cargo check --workspace --all-targets
scripts/check_industrial_boundaries.sh
```

### 13. Tools/ops 与工业 CLI 收口

目标：把所有 operator one-shot、canary、probe、reporter 从 `retired root bin directory` 迁到
`tools/ops` 或 `apps/cli`，root bin 只短期保留 wrapper。

范围：

- `retired root bin directory/account_position_reporter.rs`
- `retired root bin directory/bitget_order_canary.rs`
- `retired root bin directory/bitget_spot_order_canary.rs`
- `retired root bin directory/exchange_order_canary.rs`
- `retired root bin directory/hyperliquid_self_test.rs`
- `retired root bin directory/funding_arb_observe.rs`
- `retired root bin directory/cross_arb_ws_opportunity_probe.rs`
- `tools/ops`
- `apps/cli`

执行：

- read-only/report/probe 放 `tools/ops`。
- operator command/preflight/migration inventory 放 `apps/cli`。
- canary 必须保留原 confirmation flags、dry-run 默认值、notional limit、
  输出字段和 audit plan。
- root bin wrapper 只调用新命令，不能继续保留业务实现。

不做：

- 不让 tools 依赖 root `rustcta`。
- 不默认开启 live mutation。

验收：

```bash
cargo test -p rustcta-tools-ops
cargo test -p rustcta-industrial-cli
cargo run -q -p rustcta-tools-ops -- verify-retired-src
scripts/check_industrial_boundaries.sh
```

### 14. Supervisor 与 process spec 切换

目标：让 active live runtime 由 supervisor 管理，而不是 root launcher 或手工
运行 legacy bins。

范围：

- `crates/rustcta-supervisor`
- `apps/supervisor`
- `config/supervisor/*.spec.json`
- `src/main.rs`
- live runtime wrapper bins
- process logs and registry paths

执行：

- 为 spot-spot、cross-exchange、funding、reporter/canary 等 active 进程生成
  root-free supervisor specs。
- supervisor 负责 start/stop/restart/stale heartbeat/log path/registry。
- `src/main.rs` root launcher 标记为 deprecated wrapper，并在任务 16 删除。
- control API 从 supervisor registry 读 process state。

不做：

- 不让 supervisor 持有交易所凭证。
- 不把 strategy runtime 代码放进 supervisor crate。

验收：

```bash
cargo test -p rustcta-supervisor
cargo test -p rustcta-supervisor-app
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- --validate-spec config/supervisor/cross_arb_live.spec.json
scripts/check_industrial_boundaries.sh
```

### 15. Web UI 与 control panel 切换

目标：让 Dioxus control panel 完全使用新 control API，不再依赖 legacy
control_api binary 和 legacy dashboard routes。

范围：

- `web-ui/dioxus`
- `apps/control-api`
- `crates/rustcta-control-api`
- static asset serving
- docs/control panel runbooks

执行：

- 更新 API client DTO 到 `rustcta-control-api` 当前 route。
- 删除 legacy-only exchange key write/delete UI，改为 credential status 和
  gateway/local-agent 指引。
- process、strategy、gateway、risk、fees、logs、inventory、books、audit
  页面全部通过新 read model。
- static hosting 由 `apps/control-api` 或独立 web server 提供。

不做：

- 不把 raw private exchange payload 展示到 UI。
- 不在 UI 中执行本地 shell script。

验收：

```bash
cargo check --manifest-path web-ui/dioxus/Cargo.toml
cargo test -p rustcta-control-api
cargo check -p rustcta-control-api-app
scripts/control_api_smoke_test.sh
```

### 16. 最终兼容清理与 legacy `src/` 删除

目标：在所有 active runtime 已 root-free 后，删除 legacy root 实现和过渡 wrapper。

范围：

- root `Cargo.toml`
- `Cargo.lock`
- `src/`
- remaining legacy root bins
- boundary checks
- CI
- docs/README and runbooks

执行：

- 删除任务 2-15 已替换的 root bins、modules、compat includes、legacy-only tests。
- root package 如果不再需要，移除或改成空 compatibility crate；优先彻底删除
  legacy business implementation。
- 从 workspace、dependencies、features、CI 中移除 retired packages。
- 更新 docs，把旧命令列入 retired/deprecated，不再作为 active 操作入口。
- 强化 boundary check：禁止新增 `src/` 业务实现、禁止 root crate dependency。

不做：

- 不在最终清理中修业务逻辑；发现业务缺口退回对应任务。
- 不保留未分类 legacy bin。

验收：

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo run -q -p rustcta-tools-ops -- verify-retired-src
scripts/check_industrial_boundaries.sh
rg -n "legacy root crate path |retired exchange tree|retired strategy tree|retired backtest tree|retired-backtest|retired_short_ladder_grid" apps crates strategies tools web-ui tests docs scripts README.md
```

## 推荐执行顺序

1. 先执行任务 1，冻结库存和 shared file ownership。
2. 任务 2 可以紧接执行，因为回测已明确退役，可以减少后续编译面。
3. 任务 3 只做审计和最小缺口补齐，结论输出给任务 7、9、10、13。
4. 任务 4、5、6、7 可并行，但共享 DTO 需要由任务 1 协调。
5. 任务 8 先加严边界，再执行任务 9、10、11 的策略迁移。
6. 任务 12、14、15 在 active strategy runtime 能发布 typed snapshot 后收口。
7. 任务 16 最后统一删除 legacy `src/` 和 compatibility wrappers。

## 完成定义

全量迁移完成必须同时满足：

- active workspace 不包含 backtest system。
- active live runtime 不直接依赖 legacy `retired exchange tree`、`src/execution`、
  `src/control`、`src/web`、`retired strategy tree`。
- `strategies/*` 只依赖 strategy SDK 和通用类型。
- tools/CLI/control/web/supervisor 均不依赖 legacy root。
- gateway 是唯一 exchange credential owner。
- 全仓基础 gates 通过。

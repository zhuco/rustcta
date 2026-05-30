# Exchange Unification Remediation Plan

## Purpose

`rustcta_push_work` is the consolidation target for the current RustCTA codebase.
It contains the newer cross-exchange arbitrage execution stack, the
`private_perp` Bitget/Gate private trading implementation, server deployment
scripts, and the funding-rate arbitrage observer work.

The next engineering goal is not to add another live strategy quickly. The goal
is to make exchange access an industrial, shared platform layer so every
strategy uses the same directory structure, registry, capabilities, market data,
private trading, account state, and notification conventions.

## Current State

There are two local working directories:

- `rustcta`: older non-git working tree used for recent funding-rate observer
  development.
- `rustcta_push_work`: git working tree, server deployment source, and the
  newer cross-exchange arbitrage execution implementation.

`rustcta_push_work` should become the single source of truth. Do not overwrite
it wholesale with `rustcta`; copy only missing feature files or documentation
after reviewing conflicts.

Current exchange layering in `rustcta_push_work`:

- `src/market/adapter.rs` defines the shared public market-data interface.
- `src/execution/adapter.rs` defines the shared private trading interface.
- `src/exchanges/gateway.rs` defines the strategy-facing exchange gateway
  plugin boundary.
- `src/exchanges/registry.rs` owns global exchange construction and gateway
  lookup.
- `src/exchanges/config.rs` defines strategy-neutral runtime config traits used
  by the registry.
- `src/exchanges/adapters/*_market.rs` implements public market adapters.
- `src/exchanges/adapters/trading.rs` bridges legacy `core::Exchange` into
  `TradingAdapter` for Binance/OKX.
- `src/exchanges/adapters/private_perp.rs` implements Bitget/Gate/Bybit/MEXC/HTX
  private perpetual protocol and exposes it through `TradingAdapter`.
- `src/strategies/cross_exchange_arbitrage/runtime.rs` keeps compatibility
  re-exports for older call sites but no longer owns generic adapter
  construction logic.

The code is already unified at the `TradingAdapter` trait boundary, but the
construction and directory ownership are still being hardened. New strategies
should import exchange access from `exchanges::registry` or
`exchanges::gateway`, not from `strategies::cross_exchange_arbitrage`.

## Problems To Fix

1. Strategy-to-strategy dependency

   Funding-rate arbitrage and canary tools currently need concepts that are
   owned by cross-exchange arbitrage runtime code:

   - trading adapter construction;
   - private-perp exchange mapping;
   - private REST auth loading;
   - configured position mode.

   These are platform responsibilities, not cross-arbitrage responsibilities.

2. Mixed exchange ownership

   Public market adapters, legacy private adapters, private-perp protocol
   adapters, auth loading, and runtime-specific config are spread across:

   - `src/market`;
   - `src/execution`;
   - `src/exchanges/adapters`;
   - `src/strategies/cross_exchange_arbitrage/runtime.rs`;
   - standalone binaries.

3. Large private-perp file

   `src/exchanges/adapters/private_perp.rs` has protocol signing, REST request
   construction, response parsing, private WebSocket parsing, adapter
   implementation, and tests in one large file. This is hard to review and easy
   to break accidentally.

4. Config shape is strategy-specific

   Cross-arb exchange runtime config currently contains fields that should be
   exchange-platform config:

   - `env_prefix`;
   - `account_id`;
   - `demo_trading`;
   - private REST base URL;
   - private WebSocket URL;
   - position mode;
   - margin/leverage defaults.

5. Funding ledger and server clock are not first-class adapters

   Live funding-rate arbitrage needs funding settlement attribution and server
   clock alignment. These should not be implemented ad hoc inside one strategy.

## Target Architecture

The strategy-facing architecture should be:

```text
Strategy
  -> ExchangeGateway / ExchangeRegistry
       -> MarketDataAdapter
       -> TradingAdapter
       -> AccountAdapter
       -> FundingLedgerAdapter
       -> ExchangeClockAdapter
```

Strategies should only depend on shared traits and registry handles. They should
not instantiate exchange protocols directly.

Current gateway boundary:

```rust
pub trait ExchangeGateway: Send + Sync {
    fn exchange(&self) -> ExchangeId;
    fn capabilities(&self) -> ExchangeGatewayCapabilities;
    fn market_data(&self) -> Option<Box<dyn MarketDataAdapter + Send + Sync>>;
    fn trading(
        &self,
        auth: PrivateRestAuth,
        position_mode: PositionMode,
        instruments: Vec<InstrumentMeta>,
        private_rest_base_url: Option<&str>,
    ) -> Result<Arc<dyn TradingAdapter>>;
    fn private_perp_exchange(&self) -> Option<PrivatePerpExchange>;
}
```

The gateway boundary is deliberately additive. It wraps current market/trading
traits so that live strategies can keep running while `private_perp.rs` is split
behind this API.

Target directory structure:

```text
src/exchanges/
  mod.rs
  config.rs
  registry.rs
  capabilities.rs

  market/
    mod.rs
    binance.rs
    okx.rs
    bitget.rs
    gate.rs
    bybit.rs
    mexc.rs
    htx.rs

  trading/
    mod.rs
    registry.rs
    legacy_core.rs
    private_perp.rs

  private_perp/
    mod.rs
    common.rs
    transport.rs
    signer.rs
    websocket.rs
    bitget.rs
    gate.rs
    bybit.rs
    mexc.rs
    htx.rs
    tests.rs

  account/
    mod.rs
    balance.rs
    position.rs
    fee.rs
    clock.rs
    funding_ledger.rs
```

Keep `src/market` and `src/execution` as the trait/contract crates inside this
repository:

- `src/market` owns normalized public market contracts.
- `src/execution` owns normalized private trading contracts.
- `src/exchanges` owns exchange-specific implementations and registries.

## Registry Contract

Introduce a global `ExchangeRegistry`:

```rust
pub struct ExchangeRegistry {
    market: HashMap<ExchangeId, Arc<dyn MarketDataAdapter + Send + Sync>>,
    trading: HashMap<ExchangeId, Arc<dyn TradingAdapter>>,
    account: HashMap<ExchangeId, Arc<dyn AccountAdapter>>,
    funding_ledger: HashMap<ExchangeId, Arc<dyn FundingLedgerAdapter>>,
    clock: HashMap<ExchangeId, Arc<dyn ExchangeClockAdapter>>,
}
```

Required methods:

```rust
impl ExchangeRegistry {
    pub fn market(&self, exchange: &ExchangeId) -> Result<Arc<dyn MarketDataAdapter + Send + Sync>>;
    pub fn trading(&self, exchange: &ExchangeId) -> Result<Arc<dyn TradingAdapter>>;
    pub fn account(&self, exchange: &ExchangeId) -> Result<Arc<dyn AccountAdapter>>;
    pub fn funding_ledger(&self, exchange: &ExchangeId) -> Result<Arc<dyn FundingLedgerAdapter>>;
    pub fn clock(&self, exchange: &ExchangeId) -> Result<Arc<dyn ExchangeClockAdapter>>;
    pub fn capabilities(&self, exchange: &ExchangeId) -> ExchangeCapabilities;
}
```

Initial builder:

```rust
pub fn build_exchange_registry(
    config: &ExchangeRuntimeConfig,
    instruments: impl IntoIterator<Item = InstrumentMeta>,
) -> Result<ExchangeRegistry>;
```

The initial registry may support only market and trading adapters. Account,
funding ledger, and clock can be added as separate phases.

## Shared Config Contract

Create a strategy-neutral exchange runtime config:

```rust
pub struct ExchangeRuntimeConfig {
    pub enabled_exchanges: Vec<ExchangeId>,
    pub exchanges: HashMap<ExchangeId, ExchangeRuntimeEntry>,
}

pub struct ExchangeRuntimeEntry {
    pub enabled: bool,
    pub env_prefix: Option<String>,
    pub account_id: Option<String>,
    pub demo_trading: bool,
    pub private_rest_base_url: Option<String>,
    pub private_ws_url: Option<String>,
    pub private_ws_enabled: bool,
    pub position_mode: PositionMode,
    pub margin_mode: Option<MarginMode>,
    pub leverage: Option<u32>,
}
```

Cross-arb config and funding-arb config should either embed this structure or
provide conversion into it.

## Migration Phases

### Phase 0: Consolidate Working Tree

Use `rustcta_push_work` as the main tree.

Actions:

- Keep all current `rustcta_push_work` modified files intact.
- Copy only missing funding-rate observer files from `rustcta`.
- Copy funding-rate docs from `rustcta`.
- Do not copy `target/`, logs, or stale generated files.
- Run:

```bash
cargo fmt
cargo check --bin funding_arb_observe
cargo test funding_rate_arbitrage
```

Current status:

- Funding observer code has been copied into `rustcta_push_work`.
- `config/funding_rate_arbitrage_usdt.yml` exists in `rustcta_push_work`.
- `docs/funding_rate_arbitrage_development_plan.md` has been copied into
  `rustcta_push_work`.

### Phase 1: Move Adapter Builders Out Of Cross-Arb

Create:

```text
src/exchanges/registry.rs
src/exchanges/config.rs
```

Move or wrap these functions from
`src/strategies/cross_exchange_arbitrage/runtime.rs`:

- `build_trading_adapter_for_exchange`;
- `build_trading_adapter_for_exchange_with_instruments`;
- `private_perp_exchange`;
- `private_rest_auth_for_exchange`;
- `private_ws_auth_for_exchange`;
- `configured_position_mode`;
- `build_core_exchange_for_exchange`.

During this phase, keep compatibility re-exports in cross-arb runtime:

```rust
pub use crate::exchanges::registry::{
    build_trading_adapter_for_exchange,
    build_trading_adapter_for_exchange_with_instruments,
    configured_position_mode,
    private_perp_exchange,
    private_rest_auth_for_exchange,
};
```

This keeps existing binaries compiling while new code imports from
`crate::exchanges::registry`.

Validation:

```bash
cargo check --bin cross_arb_live --bin cross_arb_preflight --bin exchange_order_canary
cargo check --bin funding_arb_observe
cargo test cross_exchange_arbitrage runtime
```

Current status:

- `src/exchanges/config.rs` exists and defines `ExchangeRuntimeSettings` plus
  `ExchangeRegistryConfig`.
- `src/exchanges/registry.rs` exists and owns generic trading construction,
  auth loading, position mode parsing, private-perp exchange mapping, and core
  exchange construction.
- Cross-arb runtime re-exports the global registry functions for compatibility.

### Phase 2: Move Market Adapter Registry Out Of Binaries

Several binaries build market adapters locally. Replace repeated match blocks
with:

```rust
crate::exchanges::registry::market_adapter(exchange)
crate::exchanges::registry::market_adapters(&enabled_exchanges)
```

Affected files include:

- `src/bin/cross_arb_observe.rs`;
- `src/bin/cross_arb_preflight.rs`;
- `src/bin/cross_arb_server.rs`;
- `src/bin/cross_arb_live.rs`;
- `src/bin/funding_arb_observe.rs`;
- `src/bin/exchange_order_canary.rs`.

Validation:

```bash
cargo check --bin cross_arb_observe --bin cross_arb_preflight --bin cross_arb_server
cargo check --bin cross_arb_live --bin funding_arb_observe --bin exchange_order_canary
```

Current status:

- Funding-rate observer, cross-arb observe, preflight, server, live, and
  exchange canary use `exchanges::registry::market_adapter` either directly or
  through a small compatibility wrapper.
- `cross_arb_live` intentionally keeps its existing OKX market-adapter exclusion
  while delegating other venue construction to the global registry.

### Phase 2.5: Add Stable Gateway Boundary

Create:

```text
src/exchanges/gateway.rs
```

Registry requirements:

- expose `gateway_for_exchange(exchange) -> Option<Box<dyn ExchangeGateway>>`;
- expose `gateway_capabilities(exchange)`;
- implement core gateways for Binance/OKX;
- implement private-perp gateways for Bitget/Gate/Bybit/MEXC/HTX;
- keep `market_adapter`, `build_trading_adapter_for_exchange`, and
  `build_trading_adapter_for_exchange_with_instruments` signatures compatible.

Validation:

```bash
cargo test gateway_registry_should
cargo check --bin funding_arb_observe
cargo check --bin cross_arb_preflight --bin exchange_order_canary
```

Current status:

- The gateway trait and capability model exist.
- Registry construction now routes through gateway implementations while
  preserving the previous public helper function names.

### Phase 3: Split Private-Perp Module Without Changing API

Refactor only after Phase 2.5 validation is stable:

```text
src/exchanges/adapters/private_perp.rs
```

into:

```text
src/exchanges/private_perp/mod.rs
src/exchanges/private_perp/common.rs
src/exchanges/private_perp/transport.rs
src/exchanges/private_perp/signer.rs
src/exchanges/private_perp/websocket.rs
src/exchanges/private_perp/bitget.rs
src/exchanges/private_perp/gate.rs
src/exchanges/private_perp/bybit.rs
src/exchanges/private_perp/mexc.rs
src/exchanges/private_perp/htx.rs
src/exchanges/private_perp/tests.rs
```

Keep compatibility re-export:

```rust
// src/exchanges/adapters/private_perp.rs or adapters/mod.rs
pub use crate::exchanges::private_perp::*;
```

Do not change request payloads or response parsing in this phase. This is a file
movement and ownership refactor only.

Important guardrail:

- Do not split all 7000+ lines in a single behavior-changing patch.
- Move one boundary at a time: `common`/types first, then signer, then
  transport, then websocket, then venue protocol modules.
- After each move, keep `src/exchanges/adapters/private_perp.rs` as a re-export
  shim or compatibility module so existing canary and strategy imports continue
  to compile.

Validation:

```bash
cargo test private_perp
cargo check --bin bitget_order_canary --bin exchange_order_canary
cargo check --bin cross_arb_live
```

### Phase 4: Add Account, Funding Ledger, And Clock Adapters

Add traits:

```rust
pub trait AccountAdapter {
    async fn balances(&self) -> Result<Vec<ExchangeBalance>>;
    async fn positions(&self, symbol: Option<&ExchangeSymbol>) -> Result<Vec<ExchangePosition>>;
    async fn trade_fee(&self, symbol: &ExchangeSymbol) -> Result<TradeFeeSnapshot>;
    async fn symbol_account_config(&self, symbol: &ExchangeSymbol) -> Result<SymbolAccountConfig>;
}

pub trait FundingLedgerAdapter {
    async fn funding_events(&self, query: FundingLedgerQuery) -> Result<Vec<FundingLedgerEvent>>;
}

pub trait ExchangeClockAdapter {
    async fn server_time(&self) -> Result<DateTime<Utc>>;
}
```

Initially, `AccountAdapter` can be a thin wrapper over `TradingAdapter`.
Funding ledger and clock can use exchange-specific endpoints where available.

Validation:

```bash
cargo check --bin cross_arb_account_audit --bin cross_arb_fee_audit
cargo check --bin funding_arb_observe
```

### Phase 5: Convert Funding-Arb To Registry

After the registry is global:

- `funding_arb_observe` should use `ExchangeRegistry::market`.
- `funding_arb_preflight` should use `ExchangeRegistry::trading`,
  `AccountAdapter`, and `ExchangeClockAdapter`.
- `funding_arb_live` should use only registry-provided adapters.

Funding-arb must not import cross-arb runtime builders.

Validation:

```bash
cargo check --bin funding_arb_observe --bin funding_arb_preflight --bin funding_arb_live
cargo test funding_rate_arbitrage
```

## Dependency Rules

Allowed:

```text
strategies -> market contracts
strategies -> execution contracts
strategies -> exchanges registry
exchanges -> market contracts
exchanges -> execution contracts
```

Forbidden:

```text
funding_rate_arbitrage -> cross_exchange_arbitrage::runtime
any strategy -> exchanges::private_perp::BitgetPrivatePerpProtocol
any strategy -> raw REST signer/transport modules
any bin -> hand-written market adapter match blocks after Phase 2
```

## Acceptance Criteria

The cleanup is complete when:

- New strategy code constructs exchange access through `exchanges::registry` or
  `ExchangeGateway`; legacy strategy code may keep compatibility imports only
  during migration.
- No non-cross-arb strategy imports from `strategies::cross_exchange_arbitrage`.
- `cross_exchange_arbitrage::runtime` no longer owns generic exchange builder
  logic.
- Bitget/Gate private trading is available through the same registry path as
  Binance/OKX.
- Gateway capability tests cover Binance/OKX core gateways and
  Bitget/Gate/Bybit/MEXC/HTX private-perp gateways.
- `cargo check --all-targets` passes, ignoring only known unrelated warnings.
- `cargo test private_perp` and `cargo test funding_rate_arbitrage` pass.
- Server deployment scripts build and upload binaries without strategy-specific
  exchange construction hacks.

## Recommended Implementation Order

1. Create `src/exchanges/config.rs` with neutral config structs.
2. Create `src/exchanges/registry.rs` and move builder logic there.
3. Re-export moved functions from cross-arb runtime for compatibility.
4. Replace direct imports in canary/audit binaries with registry imports.
5. Replace market adapter match blocks with registry helpers.
6. Run full binary check for all cross-arb and funding-arb binaries.
7. Split `private_perp.rs` into modules after behavior is stable.
8. Add account/funding-ledger/clock traits.
9. Implement `funding_arb_preflight`.
10. Implement `funding_arb_live` only after the registry is stable.

## Operational Notes

- Do not stop or restart existing live services during refactor unless the
  operator explicitly asks.
- Keep small live-order tools such as `bitget_order_canary` and
  `exchange_order_canary`; they are useful adapter smoke tests.
- Config files containing webhook URLs or account-specific settings should be
  treated as deployment artifacts. Avoid moving secrets into code.
- Use `scripts/local/rustcta_server.sh deploy-bin <bin>` for binary upload and
  manual `rsync` for one-off config upload until deployment scripts gain a
  generic config upload command.

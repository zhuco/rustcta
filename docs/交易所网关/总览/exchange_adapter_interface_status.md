# Exchange Adapter Interface Status

This note records the current adapter layout after compatibility cleanup.

## Summary

Multi-exchange Spot arbitrage is supported through unified Spot clients and the
`spot_spot_taker_arbitrage` runtime. USDT perpetual arbitrage remains on the
execution gateway path used by unified arbitrage.

The old flat compatibility adapter directory has been removed; current adapter
code lives under `crates/rustcta-exchange-gateway/src/adapters/`.

## 2026-06-10 Cross-Arb Gateway Closure

The current `rustcta-exchange-gateway` path now exposes the shared API surface
needed by cross-exchange Spot, cross-exchange perpetual, and funding-rate
arbitrage for the OKX, Bybit, MEXC, and KuCoin/KuCoin Futures venue set.

| Venue | Gateway adapters | Shared funding API | Account control | Boundary |
| --- | --- | --- | --- | --- |
| OKX | `okx` | Native `get_funding_rates` on OKX mainnet derivatives | `set_leverage`, `set_position_mode` | `okxus`/`myokx` remain Spot-only profiles. |
| Bybit | `bybit` | Native `get_funding_rates` for V5 linear derivatives | `set_leverage`, `set_position_mode` | Position mode is account/category scoped as linear hedge/one-way. |
| MEXC | `mexc` | Native contract public funding endpoint | `set_leverage` | Position-mode mutation remains unsupported until verified. |
| KuCoin | `kucoin` + `kucoinfutures` | Native KuCoin Futures funding history endpoint | Not declared | Spot and Futures remain separate adapter/profile boundaries. |

The shared model additions are:

- `FundingRatesRequest`, `FundingRateSnapshot`, and `FundingRatesResponse`
  through `ExchangeClient::get_funding_rates`.
- Gateway protocol/client/helper/mock/readonly/provider routing for funding
  reads and perpetual account-control requests.
- `ExchangeAdapterProfile.position_mode_control`, so missing hedge-mode control
  is visible in adapter audits instead of being implied by order placement.

Focused validation run during closure:

```bash
cargo fmt
cargo check -p rustcta-exchange-gateway
cargo test -p rustcta-exchange-api capabilities -- --nocapture
cargo test -p rustcta-exchange-api exchange_adapter_profile_should_report_missing_features_and_inconsistencies -- --nocapture
cargo test -p rustcta-exchange-gateway bybit_get_funding_rates_should_send_public_funding_history_request -- --nocapture
cargo test -p rustcta-exchange-gateway bybit_set_position_mode_should_send_signed_v5_switch_mode_request -- --nocapture
cargo test -p rustcta-exchange-gateway okx_adapter_should_set_position_mode_with_standard_account_control_request -- --nocapture
cargo test -p rustcta-exchange-gateway mexc_adapter_should_declare_capabilities_v2_for_toolchain_audit -- --nocapture
cargo test -p rustcta-exchange-gateway kucoinfutures_public_stream_spec_should_normalize_symbol_and_ping_pong -- --nocapture
```

## Current Adapter Layout

| Area | Path | Role |
| --- | --- | --- |
| Unified client contracts | `crates/rustcta-exchange-api/` | Spot/Perpetual client model, requests, responses, user stream events |
| Venue modules | `crates/rustcta-exchange-gateway/src/adapters/<exchange>/` | Exchange-specific gateway adapters |
| Gateway client | `crates/rustcta-exchange-gateway/src/client.rs` | Local/in-process gateway routing and client wrapper |
| Gateway protocol | `crates/rustcta-exchange-gateway/src/protocol/` | Request validation and operation routing helpers |
| Trading adapter contract | `crates/rustcta-execution-api/src/lib.rs` | Execution-plane adapter trait and order/fill command models |
| Gateway app | `apps/gateway/` | Runnable gateway process |

## Spot Support

The Spot path is used by:

- `strategies/spot-spot-arbitrage/`
- `crates/rustcta-runtime-control/src/control/spot_control/`
- `crates/rustcta-execution-api/`
- `crates/rustcta-execution-router/`

Current Spot-related venue modules include Binance, OKX, Bitget, Gate.io, MEXC,
CoinEx, KuCoin, BitMEX public/private REST, and Paper. Capability depth varies by
venue; configs and tests should decide whether a venue is scan-only,
paper-capable, live-dry-run capable, or eligible for future live submission.

## Perpetual Support

The perpetual path uses:

- `TradingAdapter`
- `MarketDataAdapter`
- `ExchangeGateway`
- private perpetual protocol adapters

Current perpetual coverage includes Binance, OKX, Bitget, Gate, Bybit, MEXC,
HTX market-data/private-protocol paths where registered, and BitMEX public/private
REST symbol, book, account, position, order, and fill support in
`rustcta-exchange-gateway`.

## Compatibility Layer Cleanup

Removed:

- old flat adapter compatibility modules outside `crates/rustcta-exchange-gateway`
- stale strategy references to deleted legacy strategy families
- migration/remediation documents superseded by current architecture docs

Kept:

- `GatewayExchange`, because parts of the existing CTA stack still accept
  `core::exchange::Exchange`
- legacy core clients where active strategies still use them
- explicit conversion helpers between legacy and unified market/order types

New work should not add another broad compatibility facade. Add a narrow bridge
only when a current caller needs it and document why it cannot use the unified
contract directly.

## Validation Expectations

Before enabling a venue for executable arbitrage:

- verify symbol mapping and precision rules
- verify min quantity and min notional handling
- verify fee source and fallback source
- run read-only balance/orderbook validation
- run dry-run or live-dry-run order planning
- verify private stream or reconciliation behavior
- confirm disabled-symbol and kill-switch handling

For scan-only support, the minimum bar is correct symbol normalization, fresh
book data, fee annotation, and explicit non-executable labeling.

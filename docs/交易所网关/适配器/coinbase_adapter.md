# Coinbase Gateway Adapter

Status date: 2026-06-08

Adapter id: `coinbase`

`crates/rustcta-exchange-gateway/src/adapters/coinbase/` implements the
industrial `ExchangeClient` adapter for Coinbase Advanced Trade Spot and INTX
perpetual request-spec coverage.

## Scope

- Public REST: `/products` symbol rules and `/product_book` order book
  snapshots for Spot and INTX perpetual symbols.
- Private REST: accounts, INTX portfolio balances/positions, fees, order
  placement, trigger-bracket OCO order-list projection, composed batch place,
  native batch cancel, cancel-all via open-orders reconciliation, order query,
  open orders, and recent fills.
- Streams: public market data and private user channels with client ping
  heartbeat, reconnect/resubscribe requirement, and REST snapshot/fills
  reconciliation after reconnect.

## Credentials And Scope

The adapter accepts an externally minted bearer/JWT token through
`COINBASE_API_BEARER_TOKEN` or `COINBASE_JWT`. Local Coinbase JWT construction is
not implemented in this adapter. Private INTX balance/position reads also
require `COINBASE_INTX_PORTFOLIO_UUID`.

Required credential scopes are read-only for account/order/fill reconciliation
and trade for order placement/cancel/amend/batch. Withdrawal and transfer scopes
are not required and must not be enabled for live dry-run.

## Capability Notes

- `capabilities_v2` declares bearer/JWT credential scope, endpoint metadata,
  stream runtime, cursor/limit history capabilities, and batch semantics.
- Batch place is `ComposedSequential`, non-atomic, and aborts on the first
  failed single-order request.
- Batch cancel uses Coinbase's native `/orders/batch_cancel` endpoint and can
  surface partial per-item failures.
- Public order book streams require a REST snapshot resync on reconnect.
- Private stream auth renewal is declared as token refresh because the adapter
  consumes an externally minted token instead of managing key material.
- Algo/trigger behavior is limited to Coinbase trigger-bracket OCO projection;
  it remains adapter-specific and does not extend the shared trait.
- Live-dry-run promotion requires the shared runner controls: reconciliation
  enabled, kill-switch active, disabled-symbol filtering, and max-notional
  limits.

## Official Position Detail

仓位接口核验见 [仓位接口官方核验 P1 第二批](../仓位接口官方核验_P1_第二批.md)。Coinbase INTX perpetual positions 已由当前项目 `get_positions` runtime 覆盖；调用需要 `COINBASE_INTX_PORTFOLIO_UUID`。

## Validation

Targeted local validation:

```bash
python scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/coinbase/endpoint_mapping.yaml
TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/coinbase-task-check cargo test -p rustcta-exchange-gateway coinbase --lib --message-format short
```

Parser fixtures live under `tests/fixtures/exchanges/coinbase/` and cover
success, empty, error, and critical-field-missing responses used by the adapter
tests and endpoint mapping audit.
Latest targeted run passed 30 Coinbase tests with 722 filtered out and existing
workspace warnings. Local mock REST tests require an unsandboxed run when
`127.0.0.1` binding is blocked.

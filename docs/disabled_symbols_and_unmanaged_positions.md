# Disabled Symbols and Unmanaged Positions

RustCTA uses `src/risk/disabled_registry.rs` to centralize symbols, exchanges, exchange-symbols, and inventory that automated strategies must not use.

## Disable Scopes

The registry supports three rejection scopes:

- Global symbol disable: rejects all opportunities for an internal symbol.
- Exchange disable: rejects any opportunity involving an exchange.
- Exchange-symbol disable: rejects one symbol on one exchange and market type.

Expired disables are ignored. Missing config is allowed and produces an empty registry with an info log.

## Unmanaged Positions

Unmanaged positions represent inventory that exists on an exchange but should not be used by automated strategies. Paper inventory subtracts configured unmanaged quantities before opportunity checks and settlement simulation.

This is intended for residual balances, manually managed holdings, and positions that require operator intervention.

## Configuration

Default config path:

```yaml
disabled:
  symbols:
    - symbol: TURBOSUSDT
      reason: "manual disabled due to abnormal spread"
      expires_at: null

  exchanges:
    - exchange: coinex
      reason: "temporary maintenance"
      expires_at: null

  exchange_symbols:
    - exchange: mexc
      market_type: spot
      symbol: DKAUSDT
      reason: "oversold errors"
      expires_at: "2026-06-10T00:00:00Z"

unmanaged_positions:
  - exchange: coinex
    market_type: spot
    symbol: PONDUSDT
    asset: POND
    quantity: 5000
    reason: "legacy residual position, do not trade automatically"
    created_at: "2026-06-04T20:20:00Z"
```

See `config/disabled_symbols.yml` for the repository template.

## Strategy Usage

Strategies should check `DisabledRegistry` before accepting opportunities. Rejections should keep the structured reason and human-readable detail.

`spot_spot_taker_arbitrage` checks the registry for both legs before accepting an opportunity and records disabled rejection details in opportunity records. Replay and polling modes use the same registry behavior.

# Fee Model

RustCTA keeps fee data in the exchange/execution contracts and strategy-local
models:

- `crates/rustcta-exchange-api/src/market.rs` exposes `FeesRequest`,
  `FeesResponse`, and `FeeRateSnapshot`.
- `crates/rustcta-execution-api/src/lib.rs` records execution fee decisions.
- `strategies/spot-spot-arbitrage/src/fees.rs` owns the current Spot
  arbitrage `SpotFeeModel`.

Strategies should ask their shared fee model or exchange fee API for maker or
taker fees instead of hardcoding per-strategy rates.

## Priority

Fee lookup is keyed by exchange, market type, optional symbol, and liquidity role.

Priority is:

1. Exchange API fees when `prefer_exchange_api_fees: true`.
2. Symbol-specific config override.
3. VIP-level override.
4. Exchange API fees when present and not preferred ahead of config.
5. Exchange and market-type default.
6. Conservative fallback.

Unknown fees never default to zero. Missing or invalid config falls back to conservative defaults and logs a warning.

## Fee Sources

Each lookup returns raw and effective rates with a `FeeSource`:

- `ExchangeApi`
- `ConfigDefault`
- `SymbolOverride`
- `VipOverride`
- `PlatformTokenDiscount`
- `Fallback`

Opportunity records should persist the buy and sell fee sources so later analysis can distinguish live API fee data, config data, and fallback data.

## Platform Token Discounts

Platform-token discounts are disabled unless explicitly configured per exchange. This avoids assuming that an account has a token balance, discount setting, or exchange support.

When enabled, the configured multiplier is applied to both maker and taker fees. The effective rate source becomes `PlatformTokenDiscount`, while the raw rate remains available in the lookup result.

## Configuration

Default config path:

```yaml
fees:
  fallback:
    spot:
      maker_bps: 20
      taker_bps: 20
      fee_asset: quote

  defaults:
    mexc:
      spot:
        maker_bps: 0
        taker_bps: 5
        fee_asset: quote

  symbol_overrides:
    - exchange: mexc
      market_type: spot
      symbol: CUDISUSDT
      maker_bps: 0
      taker_bps: 3
      fee_asset: quote
      reason: "special campaign fee"

  platform_tokens:
    - exchange: binance
      token: BNB
      enabled: true
      discount_multiplier: 0.75
```

See `config/fees.yml` for the repository template.

## Strategy Usage

`strategies/spot-spot-arbitrage/` loads `SpotFeeModel` from `fee_config_path`,
then applies existing strategy fee overrides as compatibility overlays.
Opportunity detection uses the model for net spread and records:

- buy and sell fee bps
- buy and sell fee source
- platform discount flag
- estimated total fee
- gross and net estimated PnL

The strategy remains paper-only and does not place live orders.

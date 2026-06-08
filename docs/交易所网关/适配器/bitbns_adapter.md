# Bitbns Gateway Adapter

Status date: 2026-06-08

Adapter id: `bitbns`

Task A-07 scope from `docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`: India/region spot exchange, scan-only public REST first, with private API and regional constraints audited but not enabled.

## Official Sources

| Area | Source | Notes |
| --- | --- | --- |
| Official SDK/docs | `https://github.com/bitbns-official/node-bitbns-api` | Documents public/private methods and API access permissions. |
| Public REST paths | `https://github.com/bitbns-official/node-bitbns-api/blob/master/index.js` | Source maps public methods to `https://bitbns.com` paths. |
| Private signing | same SDK source | Base64 JSON payload signed with HMAC-SHA512 headers. |
| KYC/region | Bitbns FAQ / how-it-works / terms pages | Trading requires KYC; terms allow compliance holds and exclude sanctioned jurisdictions. |

## Product Lines

| Product | MarketType | Current adapter status |
| --- | --- | --- |
| INR/USDT spot | `Spot` | G1 public REST scan-only: symbol rules and order book snapshot. |
| Margin, swap, FIP, payment, withdrawal | n/a | Unsupported; not part of exchange trading runtime. |
| Futures/perpetual | n/a | Not enabled in this A-07 spot task. |

Default public REST base URL: `https://bitbns.com`

Documented private REST bases:

- `https://api.bitbns.com/api/trade/v1`
- `https://api.bitbns.com/api/trade/v2`

## Implemented Public REST

| Gateway operation | Bitbns path | Status |
| --- | --- | --- |
| `get_symbol_rules` | `GET /order/fetchMarkets/` | Implemented. Parser accepts object/array market shapes, inactive markets are filtered. |
| `get_order_book` | `GET /exchangeData/orderBook?coin={COIN}&market={INR|USDT}` | Implemented. Depth is locally truncated, matching official SDK behavior. |
| ticker/trades/OHLCV | `/order/getTickerWithVolume`, `/exchangeData/tradedetails`, `/exchangeData/ohlc` | Mapped in endpoint audit only; not exposed through current shared trait. |

Symbols normalize to `BASE_QUOTE` (`BTC_INR`, `ETH_USDT`). The adapter rejects non-Spot market types.

## Authentication Boundary

The official SDK signs private POST requests with:

- `X-BITBNS-APIKEY`
- `X-BITBNS-PAYLOAD`
- `X-BITBNS-SIGNATURE`

`X-BITBNS-PAYLOAD` is base64 JSON containing `symbol`, `timeStamp_nonce`, and `body`; signature is HMAC-SHA512 over that payload. The adapter includes an offline signing vector, but all private REST operations return explicit `Unsupported` until KYC/region/account readback validation is done.

## WebSocket Boundary

Official SDK Socket.IO helpers show:

- public order book: `https://ws{market}mv2.bitbns.com/?coin={COIN}`
- ticker: `https://ws{market}mv2.bitbns.com/?withTicker=true&onlyTicker=true`
- private executed orders: `https://wsorderv2.bitbns.com/?token={token}`

This adapter only ships payload/parser fixtures and heartbeat policy notes. Runtime public/private streams remain `Unsupported("bitbns.public_streams_spec_only")` and `Unsupported("bitbns.private_streams_disabled")`.

## Unsupported Boundaries

- Private balances, fees, open orders, query order and fills are disabled.
- Place/cancel/cancel-all/amend/order-list/batch operations are disabled.
- Public Socket.IO runtime is spec-only, not advertised as a stable stream.
- Private Socket.IO token stream is disabled.
- FIP, swap, deposits, withdrawals, bank rails, payment gateway and transfer APIs are outside the trading adapter.

## Limits And Region Notes

The SDK documents API usage counters (`readLimit`, `writeLimit`) but not a stable window. The config example uses conservative public throttling. SDK docs list minimum order values of `10 INR` and `0.1 USDT`; dynamic market fields are preferred when present.

Trading requires Bitbns account eligibility and KYC. Do not promote private REST or live dry-run until a separate read-only validation confirms credentials, region eligibility, signing, and readback responses without submitting orders.

## Local Artifacts

- Adapter: `crates/rustcta-exchange-gateway/src/adapters/bitbns/`
- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/bitbns/endpoint_mapping.yaml`
- Fixtures: `tests/fixtures/exchanges/bitbns/`
- Config example: `config/bitbns_gateway_example.yml`

## Validation

Current no-compile validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitbns/endpoint_mapping.yaml
```

Deferred compile-backed checks for when compilation is allowed:

```bash
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bitbns --lib --message-format short
cargo test -p rustcta-gateway bitbns --message-format short
```

Do not run `cargo build`, release builds, or live connectivity commands for this task.

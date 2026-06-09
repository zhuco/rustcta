# btcbox Adapter

Task A-12 covers BTCBOX as a Japanese Spot venue. This adapter exposes public
REST symbol discovery from `tickers`, order book snapshots from `depth`, and
guarded private REST readback for order query/open orders. Live writes remain
offline request-spec only.

## API Scope

| Area | Status |
| --- | --- |
| Product line | Spot JPY pairs only |
| Public REST base URL | `https://www.btcbox.co.jp/api/v1` |
| Public REST | `GET /tickers`, `GET /ticker?coin=...`, `GET /depth?coin=...`, parser-only `GET /orders` |
| Private REST | Documented form POST API; readback is runtime-gated by `BTCBOX_PRIVATE_REST_ENABLED` plus API key/secret |
| Signing | HMAC-SHA256 over URL-encoded form params, keyed by `md5(private_key)` |
| Rate limits | Private documented per endpoint; public has no fixed cap, so adapter mapping uses a conservative bucket |
| WebSocket | 交易所不支持公共 WS 行情；当前官方 API 资料是 HTTP Public/Private API |
| Service state | BTCBOX service suspension code `901` is treated as exchange unavailable |
| Testnet | No stable public testnet endpoint verified |
| Region/KYC | Japanese venue; private API use is assumed to require BTCBOX account approval/KYC |
| Standard contracts | `交易所不支持合约`; current official API docs list spot JPY ticker/depth/orders/trade endpoints only |

## Official Sources

| Source | Coverage |
| --- | --- |
| `https://blog.btcbox.jp/en/archives/8762` | Public REST, form POST private REST, signing, endpoint examples, error codes |
| `crates/rustcta-exchange-gateway/src/adapters/btcbox/endpoint_mapping.yaml` | Local gateway capability mapping, rate buckets, Unsupported boundary |

Official docs list public ticker keys such as `BTC_JPY`, `BCH_JPY`, `LTC_JPY`,
`ETH_JPY`, `DOGE_JPY`, `DOT_JPY`, and `TRX_JPY`. The order book docs confirm
`depth` for BTC/BCH/LTC/ETH. Because BTCBOX does not publish a dedicated market
metadata endpoint, symbol rules derive from `tickers` keys and leave precision
and min-size fields unset.

## Endpoint Mapping

- `get_symbol_rules`: native public REST `GET /tickers`, derives JPY spot symbols
  from ticker keys.
- `get_order_book`: native public REST `GET /depth?coin=<base>`, limited to
  BTC/BCH/LTC/ETH because those depth coins are the verified public docs scope.
- `public_trades`: documented `GET /orders`, left `Unsupported` in this scan-only
  task because no shared gateway method/parser is wired for it.
- `query_order`: guarded private REST `POST /trade_view`, exchange-order-id only.
- `get_open_orders`: guarded private REST `POST /trade_list`, symbol scoped.
- `get_balances`, `place_order`, and `cancel_order`: offline request-spec
  fixtures only; runtime returns `Unsupported`.

## Capability Boundary

Implemented:

- `get_symbol_rules` via `GET /tickers`
- `get_order_book` via `GET /depth?coin=<base>`
- `query_order` via `POST /trade_view` when private REST is explicitly enabled
- `get_open_orders` via `POST /trade_list` when private REST is explicitly enabled
- private request-spec/signing fixtures for `balance`, `trade_list`,
  `trade_view`, `trade_cancel`, and `trade_add`

Unsupported:

- private REST writes
- balances, fees, fills
- place/cancel runtime operations; offline request-spec fixtures exist only for documented single-order endpoints
- advanced orders stay explicitly unsupported: `amend_order`, `place_order_list`, `batch_place_orders`, `batch_cancel_orders`, and audit-only `cancel_all_orders`
- public/private streams：交易所不支持公共 WS 行情；REST `depth` is the polling fallback
- fiat deposits, withdrawals, bank transfers, wallet operations, and ledgers
- standard futures/perpetual/options: `交易所不支持合约`

官方核验见 [产品线官方核验 P5 区域现货 CEX 第二批](../产品线官方核验_P5_区域现货_CEX第二批.md)。

## Validation

Allowed commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/btcbox/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway btcbox --lib --message-format short
```

If app config wiring is modified:

```bash
cargo test -p rustcta-gateway btcbox --message-format short
```

Do not use `cargo build` for this task.

## Fee Boundary

交易所不支持当前费率接口 runtime：当前资料未验证到稳定 BTCBOX fee endpoint。

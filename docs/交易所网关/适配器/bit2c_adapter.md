# Bit2C Gateway Adapter

Status date: 2026-06-08

## Scope

Task A-06 implements `bit2c` as a conservative G1 Spot adapter for Bit2C Pro
NIS markets. The adapter covers public REST symbol rules and order-book
snapshots for the documented exchange pairs:

- `BtcNis` -> `BTC/NIS`
- `EthNis` -> `ETH/NIS`
- `LtcNis` -> `LTC/NIS`
- `UsdcNis` -> `USDC/NIS`

Broker conversion, merchant checkout, custody, withdrawals, OTC, tax services,
futures, perpetuals, margin, leverage, positions, funding, and open interest are
out of scope. 官方核验见 [产品线官方核验 P5 区域现货 CEX 第二批](../产品线官方核验_P5_区域现货_CEX第二批.md)；当前官方 API 未见标准 futures/perpetual/options，所以标准合约写 `交易所不支持合约`。

## Official Sources

| Area | Value |
| --- | --- |
| API docs | `https://bit2c.co.il/home/api?language=en-US` |
| REST base URL | `https://bit2c.co.il` |
| Public book | `GET /Exchanges/{pair}/orderbook.json` |
| Public top book | `GET /Exchanges/{pair}/orderbook-top.json` |
| Public ticker | `GET /Exchanges/{pair}/Ticker.json` |
| Public trades | `GET /Exchanges/{pair}/lasttrades`, `GET /Exchanges/{pair}/trades.json` |
| Private auth | `key` and `sign` headers, HMAC-SHA512 over form data plus increasing `nonce` |
| Testnet | Not found |
| WebSocket | 交易所不支持公共 WS 行情（当前官方 API 只见 REST public/private） |

## Endpoint Mapping

Machine-readable mapping lives at
`crates/rustcta-exchange-gateway/src/adapters/bit2c/endpoint_mapping.yaml`.

| Gateway operation | Bit2C endpoint | Runtime status |
| --- | --- | --- |
| `get_symbol_rules` | static enum from public pair docs | Native G1 |
| `get_order_book` | `GET /Exchanges/{pair}/orderbook.json` | Native G1 |
| `get_order_book_top` | `GET /Exchanges/{pair}/orderbook-top.json` | Spec only |
| `get_public_trades` | `GET /Exchanges/{pair}/lasttrades` | Spec only |
| `get_balances` | `GET /Account/Balance` | Request-spec only |
| `get_open_orders` | `GET /Order/MyOrders` | Request-spec only |
| `query_order` | `GET /Order/GetById` | Request-spec only |
| `place_order` | `POST /Order/AddOrder` | Request-spec only |
| `cancel_order` | `POST /Order/CancelOrder` | Request-spec only |

## Rate Limits

No official numeric REST quota was found. The mapping uses conservative local
buckets: public order-book snapshots are treated as one request per second,
matching the documented order-book cache note, and private request specs are
tagged under a separate low-rate private bucket. These are adapter safety
defaults, not claims about Bit2C's published limits.

## Capability Boundary

Implemented:

- `MarketType::Spot`
- `get_symbol_rules` from the official static pair enum
- `get_order_book` via public REST full order book
- Offline HMAC-SHA512 request-spec/signing fixture for private REST shape

Explicitly unsupported:

- Private REST runtime, including balances, order placement, cancellation, open
  orders, query order, fills, and fees
- WebSocket public/private streams：交易所不支持公共 WS 行情；REST orderbook/top book is the polling fallback
- Batch place/cancel, cancel-all, amend, order lists
- Any funds, withdrawal, merchant, broker, or OTC flow
- Standard futures/perpetual/options: `交易所不支持合约` under the current official API scope.

Private REST remains request-spec only because Bit2C does not publish an
official signature vector, no testnet was found, and live trading credentials
should not be required for this adapter task.

## Validation

Allowed commands for this task:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bit2c/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bit2c --lib --message-format short
cargo test -p rustcta-gateway bit2c --message-format short
```

Do not run `cargo build` or live trading commands.

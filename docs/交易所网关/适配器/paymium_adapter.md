# Paymium Gateway Adapter

Paymium is implemented as a BTC/EUR spot public REST first adapter for task A-33.

## Scope

- Exchange id: `paymium`
- Product: spot
- Promoted runtime: public REST `GET /data/eur/ticker` for BTC/EUR symbol metadata and `GET /data/eur/depth` for order book snapshots
- Deferred runtime: private REST trading, private balances, order history and socket.io streams

Paymium's public API documents ticker, trades and market depth without authentication. Authenticated user and trading endpoints require API-token/OAuth permissions, so this adapter keeps them as offline request specs until live validation and risk controls are promoted.

## Unsupported Boundaries

- Non BTC/EUR pairs
- Margin, futures, perpetuals, options, leverage, positions and funding are
  `交易所不支持合约` under the current official Paymium API scope.
- Native batch place/cancel, cancel-all, amend/replace and order lists
- Post-only/client order id semantics
- Withdrawals, deposit address creation, email transfers, payment requests and merchant APIs
- Socket.io public/user runtime: 官方支持 public socket.io `stream` with `bids`/`asks` price-level changes, but current project is `项目未实现公共 WS 行情`.

## Official Public Socket Boundary

官方 WS 细项核验见 [WebSocket 官方核验 P3 P2 公共 WS 缺口交易所](../WebSocket官方核验_P3_P2公共WS缺口交易所.md)。Paymium public socket uses socket.io v1.3 at
`https://paymium.com/public` with path `/ws/socket.io`. The `stream` event can
include changed `bids` and `asks`; amount `0` removes a price level. Official
docs do not provide fixed push interval, fixed depth, sequence, or checksum, so
the implementation needs REST depth initialization and full resync on reconnect.

## Validation

Only error checks were used for this task; no `cargo build` command is required.

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/paymium/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
CARGO_TARGET_DIR=target/paymium-error-check cargo check -p rustcta-exchange-gateway --lib --message-format short
```

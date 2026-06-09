# Paymium Gateway Adapter

Paymium is implemented as a BTC/EUR spot public REST first adapter for task A-33.

## Scope

- Exchange id: `paymium`
- Product: spot
- Promoted runtime: public REST `GET /data/eur/ticker` for BTC/EUR symbol metadata and `GET /data/eur/depth` for order book snapshots; private REST read-only `query_order`, `get_open_orders`, and `get_recent_fills`
- Public WebSocket: socket.io v1.3 public `stream`/`bids_asks` order book updates are implemented behind `PAYMIUM_PUBLIC_STREAMS_ENABLED`
- Deferred runtime: private REST writes, private balances, fees, and user socket streams

Paymium's public API documents ticker, trades and market depth without authentication. Authenticated user order readbacks now run only when `PAYMIUM_PRIVATE_REST_ENABLED` plus `PAYMIUM_API_KEY`/`PAYMIUM_API_SECRET` are configured. Trading mutations remain offline request specs.

## Official Core Trading Detail

官方核心交易核验见 [核心交易官方核验 P1 第二批](../核心交易官方核验_P1_第二批.md)。Paymium 支持 `POST /api/v1/user/orders`、订单取消、user orders/history；支持 `LimitOrder`、`MarketOrder` 和 buy/sell。

当前 adapter 提升 public REST 和 private read-only REST：`query_order`、`get_open_orders`、`get_recent_fills` 已接入 signed GET runtime，并在默认配置下通过 `PAYMIUM_PRIVATE_REST_ENABLED` fail closed。下单/撤单仍是 BTC/EUR request-spec/source 离线边界，不能写成 live runtime，也不能写成 `交易所不支持下单/撤单`；后续要补 live write risk controls/dry-run guard 后才能提升。

账户/余额已补离线边界：Paymium `GET /api/v1/user` user info 已绑定 `get_balances` request-spec、API-token/OAuth auth 形状和 BTC/EUR 响应样例；共享 `get_balances` runtime 仍需完成 private REST auth、BTC/EUR 余额 parser 和 read-only scope guard。

## Unsupported Boundaries

- Non BTC/EUR pairs
- Margin, futures, perpetuals, options, leverage, positions and funding are
  `交易所不支持合约` under the current official Paymium API scope.
- Native batch place/cancel, cancel-all, amend/replace and order lists
- Post-only/client order id semantics
- Withdrawals, deposit address creation, email transfers, payment requests and merchant APIs
- User socket runtime: user socket depends on private channel id/auth promotion and remains unsupported.

## Official Public Socket Boundary

官方 WS 细项核验见 [WebSocket 官方核验 P3 P2 公共 WS 缺口交易所](../WebSocket官方核验_P3_P2公共WS缺口交易所.md)。Paymium public socket uses socket.io v1.3 at
`https://paymium.com/public` with path `/ws/socket.io`. The `stream` event can
include changed `bids` and `asks`; amount `0` removes a price level. Official
docs do not provide fixed push interval, fixed depth (`depth: unspecified`), sequence, or checksum, so
the implementation initializes from REST `/api/v1/data/eur/depth` and performs a
full REST rebuild on reconnect, stale socket, parse error, or suspected message
loss. The public socket runtime remains env-gated by `PAYMIUM_PUBLIC_STREAMS_ENABLED`
because the feed has no documented sequence or checksum.

## Validation

Only error checks were used for this task; no `cargo build` command is required.

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/paymium/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo test -p rustcta-exchange-gateway paymium --lib --message-format short
cargo check -p rustcta-exchange-gateway --lib --message-format short
```

## Fee Boundary

交易所不支持当前费率接口 runtime：当前 public adapter 未见 fee 文档。

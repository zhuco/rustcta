# dYdX Gateway Adapter

Status date: 2026-06-08

## Scope

Adapter id: `dydx`

Product line: dYdX v4 perpetual markets. The gateway implements
`MarketType::Perpetual` public Indexer REST/WS plus private readback when a
wallet address and subaccount number are configured.

Spot is 项目未实现 Spot, not `交易所不支持现货`: dYdX announced Solana
spot trading on 2025-12-11, but this adapter still targets v4 perpetual Indexer
markets only.

Official references:

- dYdX docs: https://docs.dydx.xyz/
- Indexer API: https://docs.dydx.xyz/indexer-client/http
- WebSocket API: https://docs.dydx.xyz/indexer-client/websockets
- Chain/account integration: https://docs.dydx.xyz/interaction/connect-dydx

Base URLs:

| Environment | Indexer REST | Indexer WS |
| --- | --- | --- |
| Mainnet | `https://indexer.dydx.trade` | `wss://indexer.dydx.trade/v4/ws` |
| Testnet | `https://indexer.v4testnet.dydx.exchange` | `wss://indexer.v4testnet.dydx.exchange/v4/ws` |

## Implemented Gateway Surface

| Capability | Endpoint / channel | Status |
| --- | --- | --- |
| Symbol rules | `GET /v4/perpetualMarkets` | Native public Indexer REST parser |
| Order book | `GET /v4/orderbooks/perpetualMarket/{ticker}` | Native public Indexer REST parser |
| Balances | `GET /v4/addresses/{address}/subaccountNumber/{n}` | Native private Indexer read |
| Positions | `GET /v4/perpetualPositions` | Native private Indexer read |
| Open orders | `GET /v4/orders` | Native private Indexer read |
| Fills | `GET /v4/fills` | Native private Indexer read |
| Public WS | `v4_orderbook`, `v4_trades`, `v4_markets`, `v4_candles` | Subscription payloads |
| Private WS | `v4_subaccounts` | Subscription payloads and REST reconciliation fallback |

Official `v4_orderbook` subscription returns an initial response equivalent to
the REST orderbook content, then price-level updates. Messages carry
`message_id`, `version`, and `clobPairId`, but the official WS docs do not
declare a checksum or fixed push interval. The local book must reconnect and
resubscribe for a fresh initial snapshot after gaps or disconnects. Current
project support is native but mapping still needs interval/depth/rebuild fields.
Source batch:
[WebSocket 官方核验 P5 衍生品/链上盘口细项](../WebSocket官方核验_P5_衍生品链上盘口细项.md).

## Wallet / Subaccount / Signing Boundary

dYdX v4 private writes are validator/node transactions, not simple REST API key
requests. Correct signing needs wallet key material, account number, sequence,
chain id, subaccount number, authenticator policy, and transaction broadcast
handling. This gateway does not accept mnemonics or private keys in config and
does not expose private writes until that signing path is implemented and
audited.

Indexer private readback uses only wallet address plus subaccount number. That
does not prove order placement permission and must not be treated as trade
authorization.

## Official Core Trading Detail

官方核心交易核验见 [核心交易官方核验 P2 第三批](../核心交易官方核验_P2_第三批.md)。dYdX Chain 官方 client 示例覆盖 placing、replacing、canceling orders；Indexer 可查 subaccount orders、order、fills 和 positions。

当前 private writes 是 Node/validator wallet transaction signing 边界，不是普通 REST API key HMAC。项目未实现 account sequence、chain id、subaccount signing、order/cancel transaction broadcast 和 parser 前，必须保持 `Unsupported`；这应写 `项目未实现链上下单/撤单`，不能写成 `交易所不支持下单/撤单`。

## Unsupported / Follow-Ups

- Place/cancel/batch/cancel-all/amend/order-list are `Unsupported` until Node
  wallet transaction signing is implemented.
- Fee tiers are not mapped to the shared `FeeRateSnapshot` yet.
- Production WebSocket supervisor connection and resubscribe state are platform
  follow-ups; Indexer REST is the reconciliation fallback.
- Transfers, staking, validators, withdrawals, and bridge/account operations
  are outside runtime scope.

## Fixtures

- Public REST: `tests/fixtures/exchanges/dydx/perpetual_markets.json`,
  `tests/fixtures/exchanges/dydx/orderbook.json`
- Private read: `subaccount.json`, `positions.json`, `orders.json`,
  `fills.json`
- Request-spec: `request_specs/open_orders.json`
- Signing boundary: `signing_vectors/node_write_unsupported.json`
- WS payload: `ws/orderbook_subscribe.json`

## Registration / App Wiring

The adapter is registered in `crates/rustcta-exchange-gateway/src/adapters/mod.rs`
as `dydx`, `dydx_v4`, and `dydxv4`, and `DydxGatewayConfig` is re-exported from
the gateway crate. `apps/gateway/src/config.rs` wires
`RUSTCTA_DYDX_INDEXER_REST_BASE_URL`, `RUSTCTA_DYDX_NODE_REST_BASE_URL`,
`RUSTCTA_DYDX_WALLET_ADDRESS`, and `RUSTCTA_DYDX_SUBACCOUNT_NUMBER` with a
redacted config test. Node private writes remain disabled even when the app
receives wallet/subaccount readback configuration.

## Validation Commands

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/dydx/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway --test task5_dex_adapters --message-format short
cargo test -p rustcta-exchange-gateway dydx --lib --message-format short
```

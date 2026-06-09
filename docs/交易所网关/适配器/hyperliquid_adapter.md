# Hyperliquid Gateway Adapter

Status date: 2026-06-08

## Scope

Adapter id: `hyperliquid`

Product line: USDC-settled perpetuals on Hyperliquid. The gateway implements
`MarketType::Perpetual` only. Spot is 项目未实现 Spot, not
`交易所不支持现货`: official Hyperliquid docs expose `spotMeta`/spot asset
contexts and HIP-1 onchain spot order books. Margin, options, transfers,
withdrawals, subaccount transfers, and vault administration are out of runtime
scope.

Spot 边界写入 `spot_product status: project_unimplemented`：当前只接 perpetual metadata/book/account/order lifecycle。补 Spot 前需要 `spotMeta`/spot asset id mapping、HIP-1 spot `l2Book` parser、spot order/cancel action encoding、spot open-orders/fills/balance reconciliation 和 parser fixtures。

Official references:

- API docs: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Existing repository audit: `docs/交易所网关/总览/hyperliquid_api.md`

Base URLs:

| Environment | REST | WebSocket |
| --- | --- | --- |
| Mainnet | `https://api.hyperliquid.xyz` | `wss://api.hyperliquid.xyz/ws` |
| Testnet | `https://api.hyperliquid-testnet.xyz` | `wss://api.hyperliquid-testnet.xyz/ws` |

## Implemented Gateway Surface

| Capability | Endpoint / channel | Status |
| --- | --- | --- |
| Symbol rules | `POST /info`, `{"type":"meta"}` | Native public REST parser |
| Order book | `POST /info`, `{"type":"l2Book","coin":"BTC"}` | Native public REST parser |
| Balances | `POST /info`, `{"type":"clearinghouseState","user":"..."}` | Native private read when wallet address is configured |
| Positions | same `clearinghouseState` payload | Native private read when wallet address is configured |
| Open orders | `POST /info`, `{"type":"openOrders","user":"..."}` | Native private read |
| Fills | `userFills` / `userFillsByTime` | Native private read |
| Place order | `POST /exchange`, `action.type=order` | Native request construction and L1 signing when agent key is configured |
| Cancel order | `POST /exchange`, `action.type=cancel` or `cancelByCloid` | Native request construction and L1 signing |
| Batch place/cancel | multi-item `order` / `cancel` actions | Native, partial-failure semantics |
| Public WS | `l2Book`, `trades`, `bbo`, `candle` | Subscription payloads and heartbeat helper |
| Private WS | `orderUpdates`, `userFills`, `userEvents` | Subscription payloads and REST reconciliation fallback |

Official public WS supports `l2Book` and `bbo`. REST `/info` `l2Book` provides
the rebuild snapshot and is capped at 20 levels per side. The official docs do
not publish a fixed millisecond interval, traditional sequence, or checksum for
these feeds, so strategy/runtime code must use stale-book detection and REST
resync. Current project support is native but the mapping still needs no-fixed
ms, no-checksum, BBO/L2, and 20-level REST snapshot fields. Source batch:
[WebSocket 官方核验 P5 衍生品/链上盘口细项](../WebSocket官方核验_P5_衍生品链上盘口细项.md).

## Signing Boundary

Hyperliquid private writes are not HMAC API-key requests. They sign an L1 action:

1. Serialize the action with msgpack using map field ordering compatible with
   the existing legacy adapter.
2. Append nonce, vault marker/address, and optional expiry.
3. Keccak hash the payload.
4. Sign the EIP-712 `Agent(string source,bytes32 connectionId)` digest with a
   secp256k1 API wallet key.

The gateway accepts signing material only through environment/runtime config
(`HYPERLIQUID_AGENT_PRIVATE_KEY`) and the disabled example file does not contain
real keys, wallet addresses, vault addresses, or mnemonics.

## Unsupported / Follow-Ups

- `cancel_all_orders` is not mapped to shared semantics because Hyperliquid's
  schedule-cancel dead-man switch needs a separate runtime policy.
- Shared amend is not mapped yet; native `batchModify` has different semantics.
- 费率项目未实现/未启用：Hyperliquid 官方 fee schedule/user tier source 已记录到 `tests/fixtures/exchanges/hyperliquid/request_specs/get_fees_source_boundary.json`，覆盖 Perpetual、Spot 和 HIP-3 Perps。当前只作为离线协议/config source；生产 effective fee 需要 user fee tier readback、asset scope 和 discount/referral/staking guard，默认 schedule snapshot 只可用于 backtest/估算，不能当稳定 `get_fees` runtime。
- Leverage mutation, margin operations, transfers, vault operations, and
  withdrawals are explicitly outside this adapter runtime.
- Production WebSocket supervisor connection, order-book merge, and sequence
  continuity are platform follow-ups; REST `l2Book` is the resync source.

## Fixtures

- Public REST: `tests/fixtures/exchanges/hyperliquid/meta.json`,
  `tests/fixtures/exchanges/hyperliquid/l2_book.json`
- Private read: `clearinghouse_state.json`, `open_orders.json`, `fills.json`
- Request-spec: `request_specs/place_order_limit.json`
- Signing vector: `signing_vectors/l1_action_order.json`
- WS payload: `ws/l2_book_subscribe.json`

## Validation Commands

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/hyperliquid/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway --test task5_dex_adapters --message-format short
cargo test -p rustcta-exchange-gateway hyperliquid --lib --message-format short
```

## P2 Product Line Boundary (2026-06-09)

`spot_product` is an official-source project boundary, not an exchange-unsupported row. Hyperliquid exposes `spotMeta`, spot asset contexts, and HIP-1 spot order books, while this adapter is scoped to USDC-settled perpetual metadata/book/account/order lifecycle.

Do not promote Spot runtime from perpetual asset indexes or order actions. Promotion requires spotMeta asset-id mapping, HIP-1 spot `l2Book` public parsers, spot order/cancel action encoding, open-order/fill/balance private readback, and asset-scope reconciliation guards.

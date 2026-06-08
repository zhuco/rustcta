# Hyperliquid Gateway Adapter

Status date: 2026-06-08

## Scope

Adapter id: `hyperliquid`

Product line: USDC-settled perpetuals on Hyperliquid. The gateway implements
`MarketType::Perpetual` only. Spot, margin, options, transfers, withdrawals,
subaccount transfers, and vault administration are out of runtime scope.

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
- Fee tiers, leverage mutation, margin operations, transfers, vault operations,
  and withdrawals are explicitly outside this adapter runtime.
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

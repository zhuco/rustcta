# Coinbase Exchange Gateway Adapter

Status: Task 15 offline adapter, 2026-06-08.

`coinbaseexchange` is a Coinbase Exchange spot adapter. It is intentionally
separate from the existing `coinbase` adapter, which covers Advanced Trade
brokerage REST and INTX boundaries.

## Scope

- REST base URL: `https://api.exchange.coinbase.com`.
- Sandbox REST URL: `https://api-public.sandbox.exchange.coinbase.com`.
- WebSocket URL: `wss://ws-feed.exchange.coinbase.com`.
- Market type: Spot only.
- Symbols: dashed uppercase Exchange product ids such as `BTC-USD`.

## Authentication

Private REST uses `CB-ACCESS-KEY`, `CB-ACCESS-SIGN`,
`CB-ACCESS-TIMESTAMP`, and `CB-ACCESS-PASSPHRASE`. The signature payload is
`timestamp + method + request_path + body`; the API secret is base64-decoded,
the payload is signed with HMAC-SHA256, and the digest is base64-encoded.

## Implemented Offline Surface

- Public REST: products and level-2 product book snapshots.
- Private REST: balances, place order, quote-sized market buy, cancel order,
  symbol-scoped cancel-all, query order, open orders, fills, and fees.
- Batch place/cancel are gateway-composed sequential calls.
- WebSocket specs: public `level2`/`matches`/`ticker` channels and private
  authenticated `user` channel payloads.

## Unsupported Boundary

Advanced Trade `/api/v3/brokerage/*`, INTX perpetuals, transfers, deposits,
withdrawals, margin, leverage, and funding operations are unsupported in this
adapter.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/coinbaseexchange/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway coinbaseexchange --lib --message-format short
```

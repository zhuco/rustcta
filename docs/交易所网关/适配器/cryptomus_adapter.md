# Cryptomus Adapter

Task 18 scope: `cryptomus` is treated as a Spot exchange adapter only. Cryptomus
also exposes merchant payment, payout and wallet APIs; those rails are not
registered in the exchange gateway.

## Boundary

| Area | Status |
| --- | --- |
| Public REST | Market asset metadata and order book snapshot |
| Private REST | Spec-only account balance, fees, active/history order reads, limit place and cancel |
| Public WS | Payload helpers for depth, trades, ticker and heartbeat |
| Private WS | Spec-only subscription payloads; live activation requires one-time token handling |
| Payment/payout | Explicitly unsupported in this adapter |
| Withdraw/transfer | Explicitly unsupported |

## Endpoint Notes

- REST base URL: `https://api.cryptomus.com`.
- Public WS URL: `wss://api-ws.cryptomus.com/ws`.
- Trading pairs are sourced from the Exchange "List of available trading pairs"
  endpoint, not from merchant payment assets.
- Private REST signing uses `sign = md5(base64(json_body) + api_key)` and sends
  `userId` plus `sign` headers.
- Private REST and private streams are disabled by default even when credentials
  are present. This keeps the adapter scan-only unless a caller opts in through
  `CryptomusGatewayConfig`.
- Live market-order writes are gated as unsupported; only limit order request
  specs and explicit private REST paths are covered.

Official documentation used for this task:

- https://doc.cryptomus.com/methods/exchange/list-of-available-trading-pairs
- https://doc.cryptomus.com/methods/market-cap/orderbook
- https://doc.cryptomus.com/methods/exchange/list-of-active-orders
- https://doc.cryptomus.com/methods/exchange/wallet-balances
- https://doc.cryptomus.com/methods/exchange/websockets

## Fixtures

Fixtures live under `tests/fixtures/exchanges/cryptomus/`:

- `markets.json` and `orderbook.json` cover public parser behavior.
- `balances.json`, `orders_active.json`, `orders_history.json` and
  `tariffs.json` cover private parser behavior.
- `request_specs/` and `signing_vectors/` cover signed request shape.
- `unsupported_boundary.json` records payment/payout exclusion.
- `ws_public_depth.json` and `ws_private_order.json` document stream sample
  payloads.

## Validation

Run these checks for this adapter without compiling deployment binaries:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/cryptomus/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway cryptomus --lib --message-format short
```

If `apps/gateway/src/config.rs` changes, also run:

```bash
cargo test -p rustcta-gateway cryptomus --message-format short
```

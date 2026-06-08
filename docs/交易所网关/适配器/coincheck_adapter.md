# Coincheck Gateway Adapter

Status: Task 15 offline adapter, 2026-06-08.

`coincheck` is a Japan spot adapter for Coincheck Exchange API trading
surfaces.

## Scope

- REST base URL: `https://coincheck.com`.
- WebSocket URL: `wss://ws-api.coincheck.com/`.
- Market type: Spot only.
- Symbols: lowercase underscore pairs such as `btc_jpy`.

## Authentication

Private REST uses `ACCESS-KEY`, `ACCESS-NONCE`, and `ACCESS-SIGNATURE`.
The signature payload is `nonce + full_request_url + request_body`, signed
with HMAC-SHA256 and hex encoded.

## Implemented Offline Surface

- Public REST: conservative static pair rules and `/api/order_books` snapshots.
- Private REST: balances, place order, quote-sized market buy, cancel order,
  open orders, and recent transaction fills.
- Batch place/cancel are gateway-composed sequential calls.
- Public WebSocket subscribe/unsubscribe payloads for `{pair}-trades` and
  `{pair}-orderbook`.
- Private order/fill synchronization uses REST reconciliation.

## Unsupported Boundary

Private WebSocket, single order query, cancel-all, amend order, fees, lending,
deposit, withdraw, bank transfer, margin, futures, and leverage are unsupported
in this runtime adapter.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/coincheck/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway coincheck --lib --message-format short
```

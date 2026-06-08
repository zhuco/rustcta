# NovaDAX Adapter

Task A-29 adds a conservative NovaDAX gateway adapter for Brazil spot markets.

## Official Sources

| Area | Source |
| --- | --- |
| REST overview and authentication | <https://doc.novadax.com/en-US/> |
| Public market endpoints | <https://doc.novadax.com/en-US/> |
| Private order/account endpoints | <https://doc.novadax.com/en-US/> |
| Official SDK examples | <https://github.com/novadaxsdk/Python> |

## Scope

- Products: spot only.
- Native symbol format: `BASE_QUOTE`, for example `BTC_BRL`.
- Canonical symbols preserve fiat quotes such as `BTC/BRL`.
- REST base URL: `https://api.novadax.com`.
- Public WebSocket URL: `wss://api.novadax.com` for Socket.IO-style public channel payloads.
- Public REST is represented for symbol rules and order book snapshots.
- Private REST balances, order lifecycle, open orders, fills, native batch create/cancel, and cancel-by-symbol are documented as offline request-spec/signing fixtures only.
- Public WebSocket is treated as Socket.IO payload-helper scope until a production supervisor validates long-running sessions.

## Authentication

NovaDAX private REST uses `X-Nova-Access-Key`, `X-Nova-Signature`, and `X-Nova-Timestamp`.
The signed payload is method, path, sorted query string or MD5 body digest, and timestamp separated by newlines, signed with HMAC-SHA256.

Fixtures use placeholder keys and synthetic order IDs only.

## Rate Limits And Pagination

- Public REST is declared under `novadax_public` with a conservative local bucket of 10 requests per second per IP.
- Private read REST is declared under `novadax_private_read` with 5 requests per second per API key until live read-only validation confirms tighter limits.
- Private order REST is declared under `novadax_private_orders` with 5 requests per second per API key; write routes remain request-spec only.
- Open orders and fills use `page`/`limit` pagination; fixtures cap `limit` at `100` and start at `page=1`.

## Boundaries

- Fiat rails, deposits, withdrawals, transfers, tax reports, and KYC workflows are unsupported.
- Official P6 product-line verification found no standard futures, perpetual,
  options, or margin API in the current NovaDAX docs; standard contracts are
  `交易所不支持合约`.
- Private streams are not promoted; private state should reconcile through REST request specs.
- Live private REST remains disabled by default. Write APIs are request-spec only and must not place real orders in this task.

## Endpoint Mapping

The machine-readable mapping is:

```bash
crates/rustcta-exchange-gateway/src/adapters/novadax/endpoint_mapping.yaml
```

It covers public symbol rules/order book, private balances, place/cancel/query/open-orders/fills, native batch create/cancel, and cancel-by-symbol with conservative `spec_only` support.

## Verification

Allowed checks:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/novadax/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
```

The task instruction for this run says not to compile and only check errors, so `cargo test` is intentionally not part of the executed verification set.

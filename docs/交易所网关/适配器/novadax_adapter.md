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
- Private REST single order lifecycle, order query, open orders, fills, and native batch create/cancel are credential-gated runtime with mock signed request/parser coverage. Balances and cancel-by-symbol remain offline request-spec/signing fixtures.
- Public WebSocket is treated as Socket.IO payload-helper scope until a production supervisor validates long-running sessions.

## Authentication

NovaDAX private REST uses `X-Nova-Access-Key`, `X-Nova-Signature`, and `X-Nova-Timestamp`.
The signed payload is method, path, sorted query string or MD5 body digest, and timestamp separated by newlines, signed with HMAC-SHA256.

Fixtures use placeholder keys and synthetic order IDs only.

## Rate Limits And Pagination

- Public REST is declared under `novadax_public` with a conservative local bucket of 10 requests per second per IP.
- Private read REST is declared under `novadax_private_read` with 5 requests per second per API key until live read-only validation confirms tighter limits.
- Private order REST is declared under `novadax_private_orders` with 5 requests per second per API key; single place/cancel and native batch place/cancel are promoted to credential-gated runtime.
- Open orders and fills use `page`/`limit` pagination; fixtures cap `limit` at `100` and start at `page=1`.

## Boundaries

- Fiat rails, deposits, withdrawals, transfers, tax reports, and KYC workflows are unsupported.
- Official P6 product-line verification found no standard futures, perpetual,
  options, or margin API in the current NovaDAX docs; standard contracts are
  `交易所不支持合约`.
- Private streams are not promoted; private order state reconciles through guarded REST query/open-orders/fills readbacks when private REST is explicitly enabled.
- Live private REST remains disabled by default. Single and batch write APIs require explicit credentials and private REST enablement; tests use local mock REST only and do not assert live success.
- Cancel-all-by-symbol remains offline because shared `cancel_all_orders` needs an over-cancel safety policy, symbol/account guard, parser, and reconciliation before runtime promotion.

## Official WebSocket Order Book Detail

P9 official verification confirms NovaDAX public WebSocket uses Socket.IO over
`wss://api.novadax.com`. The order book topic is
`MARKET.{symbol}.DEPTH.LEVEL0`, subscribed with `SUBSCRIBE`; official docs state
depth data is sent once per second. No fixed depth parameter, sequence, or
checksum is documented. The mapping records Socket.IO transport, 1s push,
`MARKET.{symbol}.DEPTH.LEVEL0`, full-state replacement semantics, no-sequence/
no-checksum risk, and reconnect/re-subscribe as the snapshot rebuild path.

## Endpoint Mapping

The machine-readable mapping is:

```bash
crates/rustcta-exchange-gateway/src/adapters/novadax/endpoint_mapping.yaml
```

It covers public symbol rules/order book, private balances, place/cancel/query/open-orders/fills, native batch create/cancel, and cancel-by-symbol. Single place/cancel/query/open-orders/fills plus batch create/cancel are `native` because signed private REST runtime is mock-tested and credential-gated; balances and cancel-by-symbol remain conservative `spec_only` boundaries.

## Verification

Allowed checks:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/novadax/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
```

The task instruction for this run says not to compile and only check errors, so `cargo test` is intentionally not part of the executed verification set.

## Fee Boundary

NovaDAX fees/limits VIP table 可作为配置型费率来源；当前已记录离线配置源边界 `tests/fixtures/exchanges/novadax/request_specs/get_fees_config_source.json`，适用 Spot fee table。API 成交回报里的 fee amount/role 只能证明成交手续费金额，不等同 maker/taker fee-rate runtime。shared `get_fees` runtime 仍属项目未实现/未启用；补齐前需完成 fee table refresh、VIP level mapping、account-effective fee 边界或显式 override。

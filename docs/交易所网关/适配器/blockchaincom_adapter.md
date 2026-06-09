# Blockchain.com Gateway Adapter

Task A-11 scope: `blockchaincom` covers Blockchain.com Exchange spot markets.
Public REST is implemented for symbol rules and order book snapshots. Private
REST and private WebSocket are represented by sanitized offline request specs,
auth payload fixtures and parser coverage; runtime trading stays disabled.

## Official Sources

| Area | Source |
| --- | --- |
| REST v3 docs | https://api.blockchain.com/v3/ |
| Official REST client/spec repo | https://github.com/blockchain/lib-exchange-client |
| Exchange and WebSocket docs | https://exchange.blockchain.com/api |
| Official WebSocket docs repo | https://github.com/blockchain/docs-exchange-api |

## Coverage

| Area | Status |
| --- | --- |
| Product line | Exchange API Spot; platform perps/margin-like surfaces are separate and not connected by this adapter |
| Public REST | `GET /symbols`, `GET /l2/{symbol}` parser and transport |
| Private REST | `GET /accounts`, `GET /fees`, `GET /orders/{orderId}`, `GET /orders`, and `GET /fills` are credential-gated read runtime; private writes remain request-spec/fixture only |
| WebSocket | Public/private subscribe payloads, auth payload, heartbeat fixture, sequence-gap restart policy |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/blockchaincom/endpoint_mapping.yaml` |
| Fixtures | `tests/fixtures/exchanges/blockchaincom/` |
| Config example | `config/blockchaincom_gateway_example.yml`, disabled by default |

## Base URLs

| Surface | URL |
| --- | --- |
| REST | `https://api.blockchain.com/v3/exchange` |
| WebSocket | `wss://ws.blockchain.info/mercury-gateway/v1/ws` |
| WS Origin header | `https://exchange.blockchain.com` |

No official REST sandbox/testnet URL was found in the Exchange v3 docs, so the
adapter marks testnet support as unavailable.

## Authentication

REST private endpoints use the `X-API-Token` header. No REST HMAC, timestamp,
nonce, body hash or canonical signing string is documented. The signing fixture
therefore records a token-auth boundary rather than inventing an HMAC vector:
`tests/fixtures/exchanges/blockchaincom/signing_vectors/token_auth.json`.

WebSocket private auth uses an auth-channel subscribe payload with the API
secret token:

```json
{"action":"subscribe","channel":"auth","token":"<redacted>"}
```

## Capability Boundary

Implemented runtime methods:

- `get_symbol_rules`
- `get_order_book`
- `get_balances` when `BLOCKCHAINCOM_PRIVATE_REST_ENABLED` and API token are configured
- `get_fees` when `BLOCKCHAINCOM_PRIVATE_REST_ENABLED` and API token are configured
- `query_order`, `get_open_orders`, and `get_recent_fills` when `BLOCKCHAINCOM_PRIVATE_REST_ENABLED` and API token are configured

Offline request-spec only:

- `place_order`
- `cancel_order`
- `cancel_all_orders`

Unsupported:

- Standard derivatives, futures, perpetuals and margin position management through the Exchange API: `交易所不支持合约` under the current Exchange API scope.
- Blockchain.com app Perps / third-party Hyperliquid interface: `项目未实现` if the project decides to support it, and it must be designed separately from this Exchange Spot adapter.
- Mapping records `perps_third_party_product` as
  `status: project_unimplemented`, `official_gap: app_perps_third_party_interface`,
  and `scope: separate_from_exchange_spot_adapter`; Exchange v3 standard
  contracts remain `contract_product=unsupported`.
- Status recommendation: keep app Perps / third-party derivatives separate from
  the Exchange v3 Spot adapter until API ownership, credential scope, market
  metadata, positions/funding/settlement, private lifecycle, and reconciliation
  are designed.
- exchange funding surfaces such as deposit address creation, withdrawals and whitelist management
- wallet explorer, pay partner, brokerage quote/swap and lending APIs
- batch place/cancel, amend and order-list semantics

官方核验见 [产品线官方核验 P5 区域现货 CEX 第二批](../产品线官方核验_P5_区域现货_CEX第二批.md)。不要把 Blockchain.com app Perps 误并入 Exchange API Spot adapter；也不要把当前项目未接的第三方 perps interface 写成平台完全不支持。

## WebSocket Notes

Public channels include `heartbeat`, `l2`, `l3`, `prices`, `symbols`, `ticker`
and `trades`. Authenticated channels include `auth`, `balances` and `trading`.
Official docs state heartbeat updates are sent every 5 seconds after subscribing
to the heartbeat channel. Server messages carry `seqnum`; gaps require restarting
the WebSocket connection and resyncing public books via REST.

Official order book channels are `l2` for aggregated book and `l3` for
order-level book. Both use subscribe payloads with `channel` and `symbol`; the
docs do not give a fixed millisecond interval or checksum. The mapping records
`l2`/`l3` as L2/L3 order book channels, snapshot/update semantics, `seqnum` gap
detection, the 1200 messages/minute WebSocket message limit, no fixed ms, and
REST/WS snapshot rebuild. Source batch:
[WebSocket 官方核验 P6 补充交易所盘口细项](../WebSocket官方核验_P6_补充交易所盘口细项.md).

The WebSocket message limit documented by the exchange docs is 1200 messages per
minute. REST rate-limit numbers were not found in the official Exchange v3 docs,
so the endpoint mapping keeps REST limits as unknown instead of copying unrelated
Blockchain.com API limits.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/blockchaincom/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway blockchaincom --lib --message-format short
cargo test -p rustcta-gateway blockchaincom --message-format short
```

Do not run `cargo build` for this task.

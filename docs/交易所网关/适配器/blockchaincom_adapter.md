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
| Product line | Spot only |
| Public REST | `GET /symbols`, `GET /l2/{symbol}` parser and transport |
| Private REST | `GET /accounts`, `GET /fees`, order read/write and fills are request-spec/fixture only |
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

Offline request-spec only:

- `get_balances`
- `get_fees`
- `place_order`
- `cancel_order`
- `cancel_all_orders`
- `query_order`
- `get_open_orders`
- `get_recent_fills`

Unsupported:

- derivatives, futures, perpetuals and margin position management
- exchange funding surfaces such as deposit address creation, withdrawals and whitelist management
- wallet explorer, pay partner, brokerage quote/swap and lending APIs
- batch place/cancel, amend and order-list semantics

## WebSocket Notes

Public channels include `heartbeat`, `l2`, `l3`, `prices`, `symbols`, `ticker`
and `trades`. Authenticated channels include `auth`, `balances` and `trading`.
Official docs state heartbeat updates are sent every 5 seconds after subscribing
to the heartbeat channel. Server messages carry `seqnum`; gaps require restarting
the WebSocket connection and resyncing public books via REST.

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

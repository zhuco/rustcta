# Deepcoin Gateway Adapter

Status date: 2026-06-07

Adapter id: `deepcoin`

Implementation status: Spot + USDT perpetual adapter is implemented behind
`rustcta-exchange-api::ExchangeClient`. The standard gateway surface covers
public market data, private account readbacks, order lifecycle, quote-sized Spot
market orders, amend, batch place, batch cancel, fills, public WebSocket
subscription/parser specs, and private listenkey stream acquisition/parser
specs. The adapter also exposes Deepcoin-specific public/private WS session
helpers for initial subscribe requests, `ping`/`pong` heartbeat handling,
runtime-state decisions, text-message parsing into stream events, and private
listenkey renewal. OCO/OTO order lists, trigger/strategy orders, leverage
changes, margin-mode changes, position-mode changes, and close-position
extensions remain explicit follow-ups because they are outside the current
standard gateway trait.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Spot | `Spot` | Public REST + private REST + public/private WS specs |
| USDT perpetual | `Perpetual` | Public REST + private REST + public/private WS specs |
| Testnet | n/a | Unsupported; no stable public sandbox host verified |

REST base URL: `https://api.deepcoin.com`
Public Spot WS: `wss://stream.deepcoin.com/streamlet/trade/public/spot?platform=api&version=v2`
Public swap WS: `wss://stream.deepcoin.com/streamlet/trade/public/swap?platform=api&version=v2`
Private WS: `wss://stream.deepcoin.com/v1/private?listenKey=...`

## Authentication

Private requests use:

- Header: `DC-ACCESS-KEY`
- Header: `DC-ACCESS-SIGN`
- Header: `DC-ACCESS-TIMESTAMP`
- Header: `DC-ACCESS-PASSPHRASE`
- Signature: base64 `HMAC-SHA256(secret, timestamp + method + request_path + body)`

The adapter signs GET requests with the sorted query string included in
`request_path`, and signs POST requests with the JSON request body.

## Endpoint Mapping

| Standard capability | Deepcoin endpoint | Current implementation |
| --- | --- | --- |
| symbol rules | `GET /deepcoin/market/instruments` | `instType=SPOT` and `instType=SWAP`; filters USDT swaps into `MarketType::Perpetual` |
| order book | `GET /deepcoin/market/books` | Snapshot parser with `sz` clamped to 1..400 |
| balances | `GET /deepcoin/account/all-balances` | `accountType=spot` or `swapU`, asset filtering by `ccy` |
| positions | `GET /deepcoin/account/positions` | USDT perpetual positions, quantity/entry/mark/liquidation/PnL/leverage |
| fees | `GET /deepcoin/account/trade-fee` | Spot maker/taker and USDT swap makerU/takerU |
| place order | `POST /deepcoin/trade/order` | market/limit/post-only/IOC, client order id, quote-sized Spot market order, reduce-only for perp |
| cancel order | `POST /deepcoin/trade/cancel-order` | Requires exchange order id |
| amend order | `POST /deepcoin/trade/replace-order` | Native single-order replace by exchange order id; non-batch, atomic per request |
| order list | Unsupported | OCO/OTO order lists are not exposed through the current standard gateway trait |
| batch place | `POST /deepcoin/trade/batch-orders` | Native batch place, maximum 5 orders |
| batch cancel | `POST /deepcoin/trade/batch-cancel-order` | Native batch cancel by exchange order ids |
| cancel all | `POST /deepcoin/trade/swap/cancel-all` | Perpetual-only; Spot cancel-all returns `Unsupported` |
| query order | `GET /deepcoin/trade/order` | order id or client order id |
| open orders | `GET /deepcoin/trade/v2/orders-pending` | symbol-scoped or account pending orders |
| fills | `GET /deepcoin/trade/fills` | trade id/order id/fee/side/position side parser |
| public WebSocket | V2 `Action/Symbol/LocalNo/ResumeNo/Topic` channels | `book25`, `trade`, `market`, and `kline` subscribe/unsubscribe payloads; `ping`/`pong` heartbeat contract; session helper emits order-book stream events |
| private WebSocket | `GET /deepcoin/listenkey/acquire`, `GET /deepcoin/listenkey/extend`, `wss://stream.deepcoin.com/v1/private?listenKey=...` | signed listenkey acquisition/one-hour sliding-window extension; session helper handles heartbeat, renewal due checks, and order/trade/account/position push events |
| trigger/strategy orders | `POST /deepcoin/trade/trigger-order`, `POST /deepcoin/trade/dsl-trigger-order` | Not exposed by the current standard trait |

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P7 补充交易所盘口细项二](../WebSocket官方核验_P7_补充交易所盘口细项二.md)。DeepCoin public WS V2 的订单簿 topic 是 `book25`，官方描述为 25Level Incremental Market Data；Spot 和 swap public WS URL 分开。

订阅 payload 使用 V2 `Action/Symbol/LocalNo/ResumeNo/Topic` 结构。`book25` 是 25 档增量订单簿；官方未给固定推流毫秒，也未见 checksum，mapping 记录为 no fixed ms；`LocalNo`/`ResumeNo` 应作为会话连续性字段记录。断线或 gap 时用 REST `GET /deepcoin/market/books`，`sz` 1..400，重建 snapshot。

## Validation

Offline request-spec tests cover public Spot rules, public perpetual order book,
private readbacks, private order mutations, unsupported private behavior without
credentials, HMAC generation, public WS payload/parsers, private listenkey
acquisition/extension, heartbeat parsing, WS session state, and private WS push
parsers.

Validation notes:

- `rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/deepcoin/*.rs`
- `CARGO_TARGET_DIR=target/deepcoin-task-check cargo test -p rustcta-exchange-gateway deepcoin --lib -- --nocapture`
  passed: 16 tests passed, 0 failed, 385 filtered out.
- `CARGO_TARGET_DIR=target/deepcoin-task-check cargo test -p rustcta-gateway config_should_wire_deepcoin_private_gateway_adapter -- --nocapture`
  passed: 1 test passed, 0 failed.
- `CARGO_TARGET_DIR=target/deepcoin-task-check cargo check -p rustcta-exchange-gateway --lib`
  passed with existing warning backlog.

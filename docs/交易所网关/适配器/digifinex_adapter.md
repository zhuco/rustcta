# DigiFinex Gateway Adapter

Status: `ExchangeClient` adapter for DigiFinex Spot and perpetual swap REST plus
WebSocket subscription specs/parsers.

## Scope

Adapter id: `digifinex`.

Implemented capabilities:

- Spot and perpetual swap symbol rules and order book snapshots.
- Spot/swap balances, swap positions, fees, order lifecycle, quote-sized Spot
  market buy, native batch place/cancel, cancel-all via open-order sweep, query
  order, open orders, and recent fills when private REST credentials are set.
- HMAC-SHA256 private REST signing isolated in `signing.rs`.
- Public/private WebSocket subscription payloads, `server.ping` heartbeat
  payload/policy, public book parser, and private order/balance/position parser.

Unsupported boundaries:

- OCO/OTO order lists are not mapped because DigiFinex trigger/plan orders are
  not equivalent to the current Binance-style `OrderListRequest`.
- Amend order is explicit `Unsupported`; cancel-replace is not used as a fake
  amend.
- Perpetual quote-sized market order is unsupported.

## Endpoint Mapping

| Standard capability | Spot endpoint | Perpetual swap endpoint |
| --- | --- | --- |
| `get_symbol_rules` | `GET /v3/spot/markets` | `GET /swap/v2/public/instruments` |
| `get_order_book` | `GET /v3/spot/order_book` | `GET /swap/v2/public/order_book` |
| `get_balances` | `GET /v3/spot/assets` | `GET /swap/v2/account/balance` |
| `get_positions` | `Unsupported` | `GET /swap/v2/account/positions` |
| `place_order` | `POST /v3/spot/order/new` | `POST /swap/v2/trade/order` |
| `batch_place_orders` | `POST /v3/spot/order/batch_new` | `POST /swap/v2/trade/batch_order` |
| `cancel_order` | `POST /v3/spot/order/cancel` | `POST /swap/v2/trade/cancel_order` |
| `batch_cancel_orders` | `POST /v3/spot/order/cancel` with comma ids | `POST /swap/v2/trade/batch_cancel_order` |
| `query_order` | `GET /v3/spot/order` | `GET /swap/v2/trade/order_info` |
| `get_open_orders` | `GET /v3/spot/order/current` | `GET /swap/v2/trade/open_orders` |
| `get_recent_fills` | `GET /v3/spot/my_trades` | `GET /swap/v2/trade/fills` |
| WebSocket public | `depth/trades/ticker/kline.subscribe` | `orderbook/trades/ticker/kline.subscribe` |
| WebSocket private | `order/trade/balance.subscribe` | `order/trade/balance/position.subscribe` |

## Configuration

`config/digifinex_gateway_example.yml` provides a disabled private REST example.
Credentials are read from:

```bash
export DIGIFINEX_API_KEY=...
export DIGIFINEX_API_SECRET=...
```

Use read/trade scoped keys only and keep withdrawal permissions disabled.

## Task 20 Toolchain Status

- Endpoint mapping: `crates/rustcta-exchange-gateway/src/adapters/digifinex/endpoint_mapping.yaml`.
- Capabilities v2: `toolchain.rs` declares Spot/perpetual public REST, gated private REST, REST-fallback WS runtime, native partial batch place/cancel, REST reconciliation, credential scopes and 200-item history limits.
- Fixtures: `tests/fixtures/exchanges/digifinex/` covers success, empty response, error response and missing required fields; public parser tests read fixture files directly.
- Request-spec/signing: private tests assert signed request paths for order and batch routes; `signing.rs` has an HMAC-SHA256 vector.
- WS policy: public/private WS are spec/parser ready with server ping heartbeat and reconnect/resubscribe requirements; REST order book/open orders remain resync sources.
- Rate-limit/pagination/reconciliation/batch: endpoint mapping declares public/private buckets, limit pagination and open-orders REST reconciliation. Batch is native but partial/non-atomic and must stay within one market type.
- Live boundary: OCO/order-list, amend and perpetual quote-sized market orders remain explicit unsupported operations.

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P7 补充交易所盘口细项二](../WebSocket官方核验_P7_补充交易所盘口细项二.md)。DigiFinex Spot public WS 使用 JSON-RPC `depth.subscribe`，例如 `{"method":"depth.subscribe","id":1,"params":["ETH_USDT"]}`；perpetual swap 对应 `orderbook.subscribe`。

Spot `depth.update` 的 params 首个布尔值区分 complete result 和 last updated result，之后是 asks/bids 和 symbol。官方未给固定推流毫秒、固定 depth、sequence 或 checksum；断线或 complete/update 异常时应回 REST spot/swap order book 重建。

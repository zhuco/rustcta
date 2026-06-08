# WebSocket 官方核验 P5 衍生品/链上盘口细项

状态日期：2026-06-08

本批处理剩余队列中衍生品、链上永续和 Orderly profile 的公共订单簿 WebSocket 细项。重点仍是跨所套利需要的订阅方式、推流间隔、档位、sequence/checksum、snapshot 重建。这里是官方能力核验；项目当前实现仍以 [交易所功能盘点矩阵](交易所功能盘点矩阵.md) 为准。

## 核验结论

| adapter | 官方订单簿 WS 结论 | 推流间隔 | 档位 | 订阅方式 | sequence/checksum 与重建 | 当前项目处理 |
| --- | --- | --- | --- | --- | --- | --- |
| `aark` | Aark Perpetual Mode 由 Orderly Network 提供 CLOB/orderbook；公共盘口细项按 Orderly EVM profile 核验。 | Orderly `orderbookupdate` 每 200ms。 | 官方 public update 未给固定 depth；REST/request orderbook 返回当前 book。 | `wss://ws-evm.orderly.org/ws/stream/{account_id}`；订阅 `{symbol}@orderbookupdate`。 | 推送有 `ts`、`prevTs`，未见 checksum；价位数量为 0 表示删除；断线后需 request/REST snapshot 重建。 | 当前 payload fixture/spec-only；列入 `public_ws_struct`，不要写交易所不支持公共 WS。 |
| `modetrade` | Mode Trade 是 Orderly EVM profile；公共盘口细项同 Orderly。 | Orderly `orderbookupdate` 每 200ms。 | 官方 public update 未给固定 depth。 | 同 Orderly `{symbol}@orderbookupdate`。 | `prevTs` 用于相邻更新校验；未见 checksum。 | 当前 payload fixture/spec-only；列入 `public_ws_struct`。 |
| `woofipro` | WOOFi Pro 是 Orderly-powered perpetual venue；公共盘口细项同 Orderly。 | Orderly `orderbookupdate` 每 200ms。 | 官方 public update 未给固定 depth。 | 同 Orderly `{symbol}@orderbookupdate`；另有 ticker topic。 | `prevTs` 用于相邻更新校验；未见 checksum。 | 当前 spec_only；列入 `public_ws_struct`。 |
| `apex` | ApeX Omni public WS 支持 `orderBook{limit}.H.{symbol}`。 | 官方未给固定 ms；文档强调 high-frequency topic。 | `limit` 允许 25 或 200；首包 snapshot，后续 delta。 | Public WS `wss://quote.omni.apex.exchange/realtime_public`；`{"op":"subscribe","args":["orderBook200.H.BTC-USDT"]}`。 | 官方建议按 timestamp 或 ID 顺序应用，发现乱序/缺包后重建 snapshot；未见 checksum。 | 当前 native 但缺 interval/sequence 结构化字段；列入 `public_ws_struct`。 |
| `aster` | Aster Futures WS 是 Binance-style stream；支持 bookTicker、partial depth、diff depth。 | bookTicker real-time；partial/diff depth 可 100ms、250ms、500ms。 | partial depth 支持 5/10/20；REST snapshot 可取到 1000 用于重建。 | `wss://fstream.asterdex.com/ws/<stream>` 或 combined stream；如 `btcusdt@depth20@100ms`。 | diff depth 用 `U/u/pu`；REST snapshot `lastUpdateId` + delta buffer；`pu != previous u` 需重建。 | 当前 declared/payload helper，补 100ms/250ms/500ms、5/10/20 档、`U/u/pu`；列入 `public_ws_struct`。 |
| `blofin` | BloFin public WS 支持 `books` 和 `books5`。 | `books` 增量每 100ms；`books5` 快照有变化时每 100ms。 | `books` 初始 200 depth；`books5` 为 5 depth。 | Public WS `wss://openapi.blofin.com/ws/public`；JSON `op=subscribe`、`channel=books/books5`、`instId=BTC-USDT`。 | `books` 有 `prevSeqId`/`seqId`，prev 必须匹配上一条 seq；未见 checksum。 | 当前 native 但缺 interval/depth/seq 字段；列入 `public_ws_struct`。 |
| `bullish` | Bullish Multi-Order Book WS 支持多市场 L1/L2 订单簿。 | 官方写 live/realtime，未给固定 ms。 | L1 + L2；具体 REST hybrid order book 可用于 snapshot，WS 文档未给固定 depth 参数。 | `wss://api.exchange.bullish.com/trading-api/v1/market-data/orderbook`；JSON-RPC `subscribe`，topic `l1Orderbook`/`l2Orderbook`。 | L1 deprecated doc 明确 `sequenceNumber` 递增；乱序应断开重连；L2 hybrid 需 REST snapshot + WS update 重建。 | 当前 native，补 L1/L2、sequence/reconnect、无固定 ms/depth 字段；列入 `public_ws_struct`。 |
| `derive` | Derive WebSocket API 有 public orderbook/ticker/trades channel；官方 public currency docs也建议实时更新用 `ticker` 或 `orderbook` channel。 | 官方公开资料未给固定 ms。 | 官方未给固定 depth 参数。 | Public WS `wss://api.derive.xyz/ws`；JSON-RPC public orderbook route/channel。 | 本批未见稳定 sequence/checksum；应以 JSON-RPC REST/reference snapshot 作为重建路径。 | 当前 native，但缺 interval/depth/sequence 风险字段；列入 `public_ws_struct`。 |
| `dydx` | dYdX v4 Indexer WebSocket 支持 `v4_orderbook`。 | 官方未给固定 ms；由 Indexer 推送 channel data。 | Initial response 返回 REST `/v4/orderbooks/perpetualMarkets/{id}` 全量内容；后续为 price level 更新。 | `wss://indexer.../v4/ws`；subscribe `{"type":"subscribe","channel":"v4_orderbook","id":"BTC-USD"}`。 | 消息有 `message_id`、`version`、`clobPairId`，但官方 WS 文档未声明 checksum；断线后重新订阅并取 initial snapshot。 | 当前 native，但缺 interval/depth/rebuild 字段；列入 `public_ws_struct`。 |
| `hyperliquid` | 官方 WS 支持 `l2Book` 和 `bbo` subscriptions；REST `/info` `l2Book` snapshot 最多 20 levels/side。 | 官方未给固定 ms；按状态更新推送。 | `l2Book` REST snapshot 最多 20 levels/side；WS `bbo` 为 L1。 | Public WS `wss://api.hyperliquid.xyz/ws`；subscribe `l2Book` 或 `bbo`。 | 官方未见传统 sequence/checksum；本地 book 需要 REST `l2Book` resync 和 stale-message 保护。 | 当前 native，但缺“无固定 ms/无 checksum/20 levels REST snapshot”字段；列入 `public_ws_struct`。 |
| `lighter` | Lighter public WS 支持 `order_book/{MARKET_INDEX}`。 | 每 50ms 批量推送。 | 官方未给固定 depth 参数；首包完整 snapshot，后续只推变化。 | `wss://mainnet.zklighter.elliot.ai/stream`；`{"type":"subscribe","channel":"order_book/0"}`。 | 用 `begin_nonce == previous nonce` 校验连续性；offset 可能重连后跳变；checksum unsupported。 | 当前 parser_only，补 50ms、nonce continuity、snapshot/update 字段；列入 `public_ws_struct`，并加入高速盘口汇总。 |
| `paradex` | Paradex WS 支持 `order_book.{market_symbol}.{feed_type}@15@{refresh_rate}` 和 BBO/REST BBO。 | refresh_rate 允许 50ms 或 100ms。 | order_book 固定 path 中 `@15`，feed_type 可为 snapshot/deltas/interactive；price_tick 可选聚合。 | `wss://ws.api.prod.paradex.trade/v1/order_book.BTC-USD-PERP.snapshot@15@50ms`。 | 消息含 `seq_no`、`last_updated_at`、update_type、inserts/updates/deletes；未见 checksum。 | 当前 native，补 50/100ms、15 levels、seq_no 和 feed_type；列入 `public_ws_struct`，并加入高速盘口汇总。 |

## 高速盘口判断

| adapter | 行情等级 | 原因 |
| --- | --- | --- |
| `lighter` | `50ms 盘口候选` | 官方 order_book 每 50ms，首包 snapshot 后增量，并有 nonce 连续性校验。 |
| `paradex` | `50ms/100ms 盘口候选` | 官方 order_book refresh_rate 允许 50ms/100ms，消息带 `seq_no`。 |
| `aster` | `100ms + BBO 候选` | depth 和 partial depth 可 100ms，bookTicker real-time，`U/u/pu` 重建规则清楚。 |
| `blofin` | `100ms 候选` | `books` 200 depth 增量和 `books5` 快照均为 100ms，带 `prevSeqId/seqId`。 |
| `aark`, `modetrade`, `woofipro` | `200ms Orderly 候选` | Orderly `orderbookupdate` 每 200ms；无 checksum，需要 snapshot 兜底。 |
| `apex`, `bullish`, `derive`, `dydx`, `hyperliquid` | `实时但无固定毫秒` | 官方支持公共 orderbook/BBO WS，但未给固定 ms；实现时必须加强 stale book 和 snapshot rebuild。 |

## 官方资料

| 交易所/profile | 官方来源 |
| --- | --- |
| Orderly / Aark / Mode Trade / WOOFi Pro | <https://orderly.network/docs/build-on-omnichain/websocket-api/introduction>、<https://orderly.network/docs/build-on-omnichain/websocket-api/public/order-book-update>、<https://orderly.network/docs/build-on-omnichain/evm-api/websocket-api/public/request-orderbook> |
| ApeX | <https://api-docs.pro.apex.exchange/practice/index.html> |
| Aster | <https://docs.asterdex.com/product/aster-perpetuals/api/api-documentation> |
| BloFin | <https://docs.blofin.com/index.html> |
| Bullish | <https://docs.exchange.bullish.com/>、<https://docs.exchange.bullish.com/websocket/protocol/messages/index.html>、<https://docs.exchange.bullish.com/websocket/public/market-data/orderbook/index.html> |
| Derive | <https://docs.derive.xyz/>、<https://docs.derive.xyz/reference/public-get_all_currencies> |
| dYdX | <https://raw.githubusercontent.com/dydxprotocol/v4-chain/main/indexer/services/comlink/public/websocket-documentation.md> |
| Hyperliquid | <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions>、<https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint> |
| Lighter | <https://apidocs.lighter.xyz/docs/websocket-reference> |
| Paradex | <https://docs.paradex.trade/ws/web-socket-channels/order-book-market-symbol-feed-type-15-refresh-rate-price-tick/order-book-market-symbol-feed-type-15-refresh-rate-price-tick>、<https://docs.paradex.trade/api/prod/markets/get-bbo> |

## 下一步实现建议

1. 优先补 `lighter` 和 `paradex`，它们有明确 50ms 级别盘口和连续性字段。
2. 再补 `aster`、`blofin`，它们是 100ms 档，且有 Binance-style 或 seq 连续性重建规则。
3. Orderly profile 的 `aark`、`modetrade`、`woofipro` 可共享一套 `{symbol}@orderbookupdate` 200ms mapping，但要把 `{account_id}` 和 REST/request snapshot 边界写清楚。
4. `hyperliquid`、`dydx`、`derive` 虽然有 native WS，但官方未给固定 ms/checksum，策略侧必须做 stale book 超时和 REST snapshot 兜底。

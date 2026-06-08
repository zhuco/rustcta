# WebSocket 官方核验 P7 补充交易所盘口细项二

状态日期：2026-06-08

本批继续处理剩余队列里的公共订单簿 WebSocket 细项，覆盖 12 个 adapter/profile。重点是跨所套利必须记录的订阅方式、推流间隔、档位、sequence/checksum 和 snapshot 重建边界。这里是官方能力核验；项目当前实现仍以 [交易所功能盘点矩阵](交易所功能盘点矩阵.md) 为准。

## 核验结论

| adapter | 官方订单簿 WS 结论 | 推流间隔 | 档位 | 订阅方式 | sequence/checksum 与重建 | 当前项目处理 |
| --- | --- | --- | --- | --- | --- | --- |
| `bsx` | BSX `book` channel 支持订阅后 snapshot，之后推 order book update。 | 25ms；无变化不推；每分钟重新推 snapshot。 | 官方未给固定 depth 参数；snapshot 示例为多档全量。 | `{"op":"sub","channel":"book","product":"BTC-PERP"}`，主网 `wss://ws.bsx.exchange/ws`。 | payload 有 `timestamp` 和 `gsn`；未见 checksum。断档或重连后用首包/定时 snapshot 重建。 | 当前 spec_only；列入 `public_ws_struct`，补 25ms、`gsn`、snapshot/update 和 REST snapshot fallback。 |
| `btcturk` | BtcTurk WS `orderbook` channel 给全量 book；`obdiff` channel 给差量。 | 官方未给固定 ms；订阅后 server 持续推。 | REST orderbook 有 `limit`，WS 文档未给固定 depth。 | `[151,{"type":151,"channel":"orderbook","event":"BTCTRY","join":true}]`；diff 用 `channel=obdiff`。 | `CS` 是 ChangeSet 顺序值；官方写当前无控制机制。差量需要先有全量 orderbook，断档后重订阅/REST 重建。 | 当前 public_ws=native 但缺 channel；列入 `public_ws_struct`，补 orderbook/obdiff、`CS` 和重建。 |
| `bybiteu` | Bybit EU 沿用 Bybit V5 public orderbook topic。 | Linear/inverse/spot：1 档 10ms、50 档 20ms、200 档 100ms、1000 档 200ms；option：25 档 20ms、100 档 100ms。 | 1/50/200/1000；option 25/100。 | `orderbook.{depth}.{symbol}`，EU host 如 `wss://stream.bybit.eu/v5/public/linear`。 | 有 `u`、cross `seq`、`cts`；snapshot 后 delta，有新 snapshot 要 reset。L1 只有 snapshot。 | 当前 spec_only，已有 `orderbook.50`；列入 `public_ws_struct`，补 20ms/50 档、10ms L1 和 sequence 字段。 |
| `bydfi` | BYDFi Futures public WS 支持 incremental depth 和 limited depth。 | depth 支持 1000ms 或 100ms。 | limited depth 支持 10/50/100。 | URL path 或 JSON `SUBSCRIBE`；如 `BTC-USDT@depth@100ms`、`BTC-USDT@depth10@100ms`。 | payload 是 `depthUpdate`，含 event time 和 bids/asks；官方未见 update id/checksum。需要 REST depth snapshot + 断线重建。 | 当前 native，缺 channel/interval；列入 `public_ws_struct`，补 100ms、10/50/100 档和无 sequence/checksum 风险。 |
| `coinstore` | Spot WS 支持 `depth` stream；Futures Socket.IO 支持 `future_snapshot_depth`。 | Spot 官方写有变化推送，最小推送间隔 100ms。 | Spot channel 可用 `<symbol>@depth` 或项目已记录 `<symbol>@depth@20`；REST snapshot up to 100。Futures snapshot depth 未给固定 levels。 | Spot `{"op":"SUB","channel":["btcusdt@depth"],"id":1}`；Futures `future_snapshot_depth` Socket.IO subscribe。 | Spot server message 有 session 序号 `S`，可判断漏消息；未见 checksum。断线后 REST snapshot 重建。 | 当前 parser_only；列入 `public_ws_struct`，补 100ms、depth stream、session seq 和 futures snapshot depth 边界。 |
| `cointr` | CoinTR depth channel 支持 full book 和 1/5/15 档。 | 200-300ms。 | `books` full 400 levels；`books1`/`books5`/`books15`。 | `{"op":"subscribe","args":[{"instType":"SPOT","channel":"books5","instId":"BTCUSDT"}]}`；合约同公共 WS 语义需按 `instType` 区分。 | first snapshot 后 update；有 CRC32 checksum，按前 25 bids/asks 交错计算。 | 当前 declared，缺 interval/depth；列入 `public_ws_struct`，补 200-300ms、1/5/15/full 和 CRC32。 |
| `cryptomus` | Cryptomus Exchange WS 支持 `depth_subscribe`，按 `market:scale_index` 订阅。 | 官方未给固定 ms。 | 官方未给固定 levels；`scale_index` 控制价格聚合尺度。 | `{"id":0,"method":"depth_subscribe","params":["BTC_USDT:0"]}`，也支持 `all`。 | `depth_update` 有 `full_reload`；false 为 partial update，true 为全量刷新；未见 sequence/checksum。 | 当前 spec_only；列入 `public_ws_struct`，补 depth_subscribe、scale_index、full_reload 和无 sequence/checksum 风险。 |
| `d8x` | 本批未找到 D8X 稳定官方公共订单簿 WS 文档；可核验的是 REST `GET /coingecko/orderbook/{ticker_id}`。 | 交易所不支持当前公共 WS runtime。 | 交易所不支持当前公共 WS runtime；REST orderbook snapshot 作为兜底。 | 当前不启用 unverified `orderbook:{ticker_id}` payload fixture。 | 交易所不支持当前公共 WS runtime；链上/SDK 交易面需要单独 signer/indexer 审计。 | 当前 public/private WS runtime unsupported；列入 `public_ws_unsupported`，单交易所文档写 `交易所不支持当前公共 WS runtime`。 |
| `deepcoin` | DeepCoin public WS V2 支持 `book25` 25 档增量行情。 | 官方未给固定 ms。 | 25 levels；REST `GET /deepcoin/market/books` 支持 `sz` 1..400 snapshot。 | V2 `Action/Symbol/LocalNo/ResumeNo/Topic`，Topic `book25`；spot/swap WS URL 分开。 | 有 `LocalNo`/`ResumeNo` 会话字段；未见 checksum。断档后 REST snapshot + 重新订阅。 | 当前 public WS specs/parser；列入 `public_ws_struct`，补 book25、25 档、LocalNo/ResumeNo 和 REST snapshot。 |
| `delta` | Delta 新公共 WS 支持 `ob_l1`、`ob_l2` 和 `ob_updates`；legacy `l2_orderbook` 迁移到新 endpoint。 | `ob_l1` 100ms；`ob_l2` 500ms；`ob_updates` 100ms。 | `ob_l1` L1；`ob_l2` top 15；`ob_updates` full orderbook snapshot + incremental update。 | `{"type":"subscribe","payload":{"channels":[{"name":"ob_updates","symbols":["BTCUSD"]}]}}`。 | `ob_updates` 有 `seq` 和 `cs` CRC32；seq 必须 +1，不匹配需 resubscribe。 | 当前 native descriptor/缺 parser integration；列入 `public_ws_struct`，补新 endpoint、100ms/500ms、seq/checksum。 |
| `digifinex` | DigiFinex Spot WS 支持 `depth.subscribe`；Swap WS 支持 `orderbook.subscribe`。 | 官方未给固定 ms。 | 官方 WS 未给固定 depth 参数；REST snapshot 作为重建源。 | Spot JSON-RPC `{"method":"depth.subscribe","params":["ETH_USDT"]}`；swap `orderbook.subscribe`。 | Spot `depth.update` 首参数 true 表示 complete result，false 表示 last updated result；未见 sequence/checksum。 | 当前 spec/parser ready；列入 `public_ws_struct`，补 full/update 标记、无固定 ms/depth 和 REST snapshot。 |
| `exmo` | EXMO public WS 支持 order book snapshots 和 updates。 | 官方写 real-time，未给固定 ms。 | top-25 positions 和 top-400 changes。 | Public WS `wss://ws-api.exmo.com/v1/public`；`spot/order_book_snapshots`、`spot/order_book_updates`。 | 官方公开摘要未见 sequence/checksum；断线或 gap 后用 REST `order_book` snapshot 重建。 | 当前 spec_only；列入 `public_ws_struct`，补 25/400、snapshot/update 和无 sequence/checksum。 |

## 高速盘口判断

| adapter | 行情等级 | 原因 |
| --- | --- | --- |
| `bybiteu` | `10ms L1 / 20ms 50 档` | Bybit V5 orderbook 官方明确 1 档 10ms、50 档 20ms，并有 `u/seq/cts`。 |
| `bsx` | `25ms 增量` | 官方 `book` channel 25ms 推增量，每分钟 snapshot；适合列入低延迟候选但没有 checksum。 |
| `bydfi` | `100ms depth 候选` | Futures depth/limited depth 可订阅 `@100ms`，档位 10/50/100，但缺 sequence/checksum。 |
| `delta` | `100ms L1/增量候选` | 新公共 WS `ob_l1` 100ms、`ob_updates` 100ms，且有 `seq` 和 CRC32。 |
| `cointr` | `200-300ms + checksum` | depth channel 有 1/5/15/full 档和 CRC32，但不是 10/20/50ms。 |
| `coinstore`, `deepcoin`, `cryptomus`, `btcturk`, `digifinex`, `exmo` | `实时但无固定 10/20ms` | 官方支持订单簿 stream，但固定毫秒、档位或 sequence/checksum 不完整，需要 REST snapshot 兜底。 |
| `d8x` | `交易所不支持当前公共 WS runtime` | 当前只核到官方 REST orderbook；不启用未核验 payload fixture。 |

## 官方资料

| 交易所/profile | 官方来源 |
| --- | --- |
| BSX | <https://api-docs.bsx.exchange/reference/ws-overview>、<https://api-docs.bsx.exchange/reference/ws-orderbook> |
| BtcTurk | <https://docs.btcturk.com/docs/websocket-feed/authentication>、<https://docs.btcturk.com/docs/websocket-feed/channel-event-and-model>、<https://docs.btcturk.com/docs/websocket-feed/models/> |
| Bybit EU | <https://bybit-exchange.github.io/docs/v5/ws/connect>、<https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook> |
| BYDFi | <https://developers.bydfi.com/en/futures/websocket-market> |
| Coinstore | <https://coinstore-openapi.github.io/en/index.html>、<https://coinstore-openapi.github.io/en/futures.html> |
| CoinTR | <https://www.cointr.com/api-doc/common/websocket-intro>、<https://www.cointr.com/api-doc/spot/websocket/public/depth-channel> |
| Cryptomus | <https://doc.cryptomus.com/methods/exchange/websockets> |
| D8X | <https://d8x.gitbook.io/d8x/>、<https://drip.d8x.xyz/coingecko/orderbook/> |
| DeepCoin | <https://www.deepcoin.com/docs/publicWS/public>、<https://www.deepcoin.com/docs/DeepCoinMarket/marketBooks> |
| Delta Exchange | <https://docs.delta.exchange/>、<https://docs-global.delta.exchange/> |
| DigiFinex | <https://docs.digifinex.com/en-ww/spot/v1/websocket.html> |
| EXMO | <https://support.exmo.com/hc/en-us/articles/14338305227676-Websocket-API> |

## 下一步实现建议

1. 优先补 `bybiteu`、`bsx`、`bydfi`、`delta`，它们有明确 20ms/25ms/100ms 级别或 checksum/sequence 价值。
2. `cointr` 虽不是超低延迟，但 200-300ms + CRC32 对稳定 book-cache 有价值，适合排在无 checksum 的区域交易所之前。
3. `coinstore`、`deepcoin`、`cryptomus`、`digifinex`、`exmo` 先补结构化 mapping 和 fixture，再评估 runtime 质量。
4. `d8x` 当前按 `交易所不支持当前公共 WS runtime` 处理，避免把未核验 payload fixture 推入实盘套利。

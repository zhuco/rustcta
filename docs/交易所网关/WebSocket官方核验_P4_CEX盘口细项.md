# WebSocket 官方核验 P4 CEX 盘口细项

状态日期：2026-06-08

本批处理剩余队列中的 CEX 公共订单簿 WebSocket 细项。重点不是简单确认有没有 WebSocket，而是补齐跨所套利必须使用的字段：订阅方式、推流间隔、订单簿档位、L1/BBO、sequence/checksum、snapshot 重建。这里是官方能力核验；项目当前实现仍以 [交易所功能盘点矩阵](交易所功能盘点矩阵.md) 为准。

## 核验结论

| adapter | 官方订单簿 WS 结论 | 推流间隔 | 档位 | 订阅方式 | sequence/checksum 与重建 | 当前项目处理 |
| --- | --- | --- | --- | --- | --- | --- |
| `bitfinex` | 支持 V2 `book` 聚合订单簿；公共 URL 为 `wss://api-pub.bitfinex.com/ws/2`。 | `freq=F0` realtime；`freq=F1` 为 2s。 | `prec=P0/P1/P2/P3/P4`；`len=1/25/100/250`。 | JSON `event=subscribe`，`channel=book`，`symbol=tBTCUSD`，可带 `prec/freq/len/subId`。 | 首包 snapshot 后增量；`count=0` 删除价位；可用 `OB_CHECKSUM` 配置 CRC32 top25，可用 `SEQ_ALL` 加 sequence，bulk updates 需 conf flag。 | 当前 spec/payload 证据有，但缺结构化 `prec/freq/len/checksum/sequence/bulk` 字段；列入 `public_ws_struct`。 |
| `bitflyer` | Realtime API 支持 JSON-RPC 2.0 over WebSocket；订单簿分 snapshot 和 diff channel。 | 官方未给固定 ms；`lightning_board` 在板信息更新时推差分。 | 官方未给固定 depth 参数；snapshot/diff 都是 board 数据。 | `wss://ws.lightstream.bitflyer.com/json-rpc`；`subscribe` 到 `lightning_board_snapshot_{product_code}` 和 `lightning_board_{product_code}`。 | 单连接内消息顺序有保证；消息无传统 sequence/checksum；断线期间数据不能回补，需 REST `GET /v1/getboard` 或 snapshot channel 重建。 | 当前 native 但缺 channel/interval/depth 风险字段；列入 `public_ws_struct`。 |
| `bitmart` | Spot 公共 WS 支持 depth all、depth increase 和 BBO book ticker。 | `spot/depth/increase100` 最快 100ms；`spot/depth5/20/50` full depth 最快 500ms；book ticker 为 best bid/ask 变更实时推。 | full depth 5/20/50；incremental `increase100` 为 100 档。 | Public URL `wss://ws-manager-compress.bitmart.com/api?protocol=1.1`；`{"op":"subscribe","args":["spot/depth/increase100:BTC_USDT"]}` 或 `spot/depth5:BTC_USDT`。 | incremental 有 `version`、`type=snapshot/update`、`ms_t`；小于等于本地 version 的消息丢弃，snapshot 覆盖本地 book；未见 checksum。 | 当前 spec_only，补 `spot/depth/increase100` 100ms、depth5/20/50 500ms、bookTicker 和 version 重建；列入 `public_ws_struct`。 |
| `bitstamp` | 官方 API 指向 WebSocket v2 real-time 数据；项目已记录 `order_book_`、`diff_order_book_`、`live_trades_` channel。 | 官方可抓取资料未给固定 ms；按变更推送。 | 官方可抓取资料未给可选 depth；REST order book 是重建兜底。 | `wss://ws.bitstamp.net`；channel 形如 `order_book_btcusd`、`diff_order_book_btcusd`。 | 官方 API 2026-02-16 增加 order data endpoints 用于 WebSocket gap recovery；本批未见稳定 sequence/checksum 说明。 | 当前 declared/payload helper，补“无固定 ms/depth、gap recovery 需要 REST/order_data”的风险字段；列入 `public_ws_struct`。 |
| `bitvavo` | 支持 `book` subscription，order book 变化时推增量；支持本地 order book 管理流程。 | 官方未给固定 ms；变化时推送。 | `book` event 官方 FAQ 表示不限制深度；初始化 snapshot 可用 REST/WS `getBook` 并传 depth。 | `wss://ws.bitvavo.com/v2/`；JSON `action=subscribe`、`channels=[{name:book,markets:[BTC-EUR]}]`。 | `nonce` 是顺序 book version；snapshot nonce 作为本地版本，后续 event 必须正好 +1，否则重订阅并重新取 snapshot。无 checksum。 | 当前 native 但缺 interval/depth/nonce 重建字段；列入 `public_ws_struct`。 |
| `cryptocom` | Exchange v1 支持 `book.{instrument_name}.{depth}`；现货、衍生品共用 Exchange v1 API。 | Delta `SNAPSHOT_AND_UPDATE` 支持 `book_update_frequency=10` 或 `100` ms；snapshot 类型默认 500ms。 | depth 允许 10 或 50；默认旧 `book.{instrument_name}` 已废弃，必须显式 depth。 | JSON-RPC `subscribe`，`params.channels=["book.BTCUSD-PERP.10"]`，可传 `book_subscription_type` 和 `book_update_frequency`。 | 首包 `book` snapshot，后续 `book.update`；`u` 为 update sequence，`pu` 必须等于上一条 `u`，不匹配需重新 subscribe 获取新 snapshot；无 checksum。 | 当前 native 但缺 10ms/100ms、10/50 档、`u/pu` 字段；列入 `public_ws_struct`，并加入极速盘口候选。 |
| `phemex` | 支持 `orderbook.subscribe` 和 `orderbook_p.subscribe`，Spot/USDT 合约均有 orderbook stream。 | 默认/指定 depth 更新间隔约 20ms；参数 `true` 为约 120ms 聚合；full depth 为 100ms；snapshot 每 60s 自校验。 | depth 允许 0/1/5/10/30；0 表示 full book；默认 30。 | `{"method":"orderbook.subscribe","params":["BTCUSD",false,5]}` 或 `orderbook_p.subscribe`。 | 消息有 `sequence`、`type=snapshot/incremental`；数量为 0 删除价位；未声明 checksum。 | 当前 native 但缺 interval/depth/sequence 结构化字段；列入 `public_ws_struct`，并加入 20ms 极速盘口候选。 |
| `poloniex` | Spot 和 Futures V3 都支持 `book_lv2`。 | 官方写 real time，未给固定 ms。 | `book_lv2` 是 full 20 level snapshot，之后 first 20 levels 变化时推 update。 | Spot public URL `wss://ws.poloniex.com/ws/public`；Futures V3 public URL `wss://ws.poloniex.com/ws/v3/public`；subscribe `channel=["book_lv2"]`。 | Spot 用 `id/lastId`；Futures V3 用 `id/lid`；上一条 id 不匹配时说明丢包，必须重新订阅。无 checksum。 | 当前 native 但缺 20 档、real-time、id/lastId/lid 断档规则；列入 `public_ws_struct`。 |
| `coinex` | Spot WS 支持 `depth.subscribe`，可选择 full 或 incremental depth push。 | push delay 约 200ms；无变化不推；full market depth 每 1 分钟推一次。 | limit 可选 5/10/20/50；支持 merge interval 参数。 | `wss://socket.coinex.com/v2/spot`；method `depth.subscribe`，`market_list=[["BTCUSDT",10,"0",true]]`。 | `depth.checksum` 是 signed 32-bit CRC32；`is_full` 区分全量/增量；增量数量为 0 删除价位。 | 当前 native 但缺 channel/200ms/limit/checksum 结构化字段；列入 `public_ws_struct`。 |
| `coinmate` | 官方 public WebSocket demo 暴露 order book、trades、statistics channel；项目记录 `order_book-{PAIR}`。 | 官方 demo 未给固定 ms。 | 官方 demo 未给固定 depth 参数；REST `orderBook` 是兜底。 | `wss://coinmate.io/api/websocket`；订阅 `order_book-BTC_EUR` 等 channel。 | 官方未见 sequence/checksum 合约；断线或疑似丢包必须重新取 REST order book。 | 当前 payload_helper，补“无固定 ms/depth/sequence/checksum”的风险字段；列入 `public_ws_struct`。 |
| `coinmetro` | 官方 API docs repo 支持 WebSocket `pairs` query 订阅 bookUpdate，并推 tick BBO。 | 官方未给固定 ms。 | 官方未给固定 depth 参数；bookUpdate 是 price-level delta，REST book 初始化。 | `wss://api.coinmetro.com/ws?pairs=BTCEUR,LTCEUR`；demo 为 `wss://api.coinmetro.com/open/ws`。 | `bookUpdate` 有 `seqNumber` 和 CRC32 `checksum`；checksum 需要按官方 rounding 规则计算；`tick` 始终包含当前 ask/bid。 | 当前 native，补 checksum/seqNumber、tick BBO、无固定 ms/depth 字段；列入 `public_ws_struct`。 |
| `coindcx` | 官方 Socket.IO 支持 depth update；项目记录 spot `{symbol}@orderbook@50` 和 futures `{instrument}@orderbook@50-futures`。 | 官方现货示例未给固定 ms；`currentPrices@spot@10s` 只是价格统计，不是订单簿。 | 官方现货示例使用 `B-BTC_USDT@orderbook@20`；futures/public REST 与项目 mapping 记录 10/20/50 线索，需实现时统一 allowed depth。 | Socket.IO v2.4，`wss://stream.coindcx.com`；`socket.emit("join",{channelName:"B-BTC_USDT@orderbook@20"})`，事件 `depth-update`。 | 官方示例未见 sequence/checksum；需要 REST orderbook snapshot 作为重连/疑似丢包兜底。 | 当前 declared/native mapping，补 Socket.IO transport、20/50 depth 边界和无 sequence/checksum 风险；列入 `public_ws_struct`。 |

## 高速盘口判断

| adapter | 行情等级 | 原因 |
| --- | --- | --- |
| `cryptocom` | `10ms 盘口候选` | 官方 `book_update_frequency` 当前支持 delta 10ms，depth 10/50，且有 `u/pu` 断档重订阅规则。 |
| `phemex` | `20ms 盘口候选` | 官方 orderbook depth 0/1/5/10/30 可用约 20ms，带 `sequence`。 |
| `bitmart` | `100ms + BBO 候选` | incremental 100 档最快 100ms，book ticker 对 best bid/ask 变化实时推；无 checksum。 |
| `coinex` | `200ms 候选` | depth push 约 200ms，带 checksum；不是 10/20ms 第一批。 |
| `bitfinex`, `bitflyer`, `bitvavo`, `poloniex`, `coinmetro` | `实时但无固定毫秒` | 官方支持实时/变更推，但没有稳定固定 ms；其中 Bitfinex/Coinmetro/Bitvavo/Poloniex 的重建字段较清楚。 |
| `bitstamp`, `coinmate`, `coindcx` | `细项风险较高` | 可用 WS/Socket.IO，但本批未拿到固定 ms 和完整 sequence/checksum；实现时必须加强 REST snapshot/stale book 保护。 |

## 官方资料

| 交易所 | 官方来源 |
| --- | --- |
| Bitfinex | <https://docs.bitfinex.com/docs/ws-general>、<https://docs.bitfinex.com/reference/ws-public-books.md>、<https://docs.bitfinex.com/docs/ws-websocket-checksum> |
| bitFlyer | <https://bf-lightning-api.readme.io/docs/endpoint-json-rpc>、<https://bf-lightning-api.readme.io/docs/realtime-board-snapshot>、<https://bf-lightning-api.readme.io/docs/realtime-board> |
| BitMart | <https://developer-pro.bitmart.com/en/spot/> |
| Bitstamp | <https://www.bitstamp.net/api/>、<https://www.bitstamp.net/websocket/v2/> |
| Bitvavo | <https://docs.bitvavo.com/docs/websocket-api/book-subscription/>、<https://docs.bitvavo.com/docs/manage-order-book/> |
| Crypto.com | <https://exchange-docs.crypto.com/exchange/v1/rest-ws/index.html#book-instrument-name-depth> |
| Phemex | <https://phemex-docs.github.io/> |
| Poloniex | <https://api-docs.poloniex.com/spot/websocket/market-data>、<https://api-docs.poloniex.com/v3/futures/websocket/public/get-order-book-v2> |
| CoinEx | <https://docs.coinex.com/api/v2/spot/market/ws/market-depth> |
| Coinmate | <https://coinmate.io/static/api_demo/pushpin/api/public.html>、<https://coinmate.docs.apiary.io/> |
| Coinmetro | <https://github.com/CoinMetro/API-DOCS> |
| CoinDCX | <https://docs.coindcx.com/> |

## 下一步实现建议

1. 低延迟优先补 `cryptocom` 10ms delta 和 `phemex` 20ms orderbook；这两个都有明确 sequence/断档规则。
2. `bitmart` 同时补 `spot/bookTicker` 和 `spot/depth/increase100`，BBO 用于套利报价，100 档用于深度校验。
3. `bitfinex`、`coinex`、`coinmetro` 优先补 checksum 或 sequence 字段，能明显降低 stale book 风险。
4. `bitstamp`、`coinmate`、`coindcx` 必须写清“官方未给固定 ms/sequence/checksum”，策略侧要做 stale book 超时和 REST snapshot 兜底。

# WebSocket 官方核验 P9 剩余交易所盘口细项

状态日期：2026-06-08

本批清理剩余公共订单簿 WebSocket 细项，覆盖 14 个 adapter/profile。重点仍是跨所套利需要的订阅方式、推流间隔、档位、sequence/checksum 和 snapshot 重建边界。这里是官方能力核验；项目当前实现仍以 [交易所功能盘点矩阵](交易所功能盘点矩阵.md) 为准。

## 核验结论

| adapter | 官方订单簿 WS 结论 | 推流间隔 | 档位 | 订阅方式 | sequence/checksum 与重建 | 当前项目处理 |
| --- | --- | --- | --- | --- | --- | --- |
| `myokx` | MyOKX 区域 profile 走 OKX V5 public market data 语义；当前项目映射 `books`/`books5`。 | `bbo-tbt` 10ms；`books5` 100ms；`books` 100ms；TBT 深度通道 10ms 但需登录/VIP 限制。 | BBO、5 档、400 档、50 档 TBT。 | EEA host `wss://wseea.okx.com:8443/ws/v5/public`；JSON `{"op":"subscribe","args":[{"channel":"books5","instId":"BTC-USDT"}]}`。 | OKX 增量深度用 `seqId/prevSeqId` 和 checksum 语义；`books5` 是快照语义。断档用 REST `/api/v5/market/books` 重建。 | 当前 spec/payload 证据缺 interval/depth；列入 `public_ws_struct`，补 OKX V5 结构化字段和区域 host。 |
| `ndax` | NDAX WSGateway 支持 Level 1 和 Level 2 orderbook。 | 官方公开资料未给固定 ms。 | Level 1 是 BBO；Level 2 snapshot 可指定 `Depth`。 | `wss://api.ndax.io/WSGateway/`；JSON-RPC-like envelope，`n=SubscribeLevel1`、`n=SubscribeLevel2` 或 `n=GetL2Snapshot`。 | request envelope 有 client sequence `i`；公开资料未见 book checksum。断线后重新 `GetL2Snapshot`。 | 当前 payload_helper，缺 orderbook channel/interval/depth；列入 `public_ws_struct`。 |
| `novadax` | NovaDAX Socket.IO public WS 支持 `MARKET.{symbol}.DEPTH.LEVEL0`。 | 每秒一次。 | 官方称返回当前 asks/bids；未给可选档位。 | `wss://api.novadax.com`，Socket.IO websocket transport；`socket.emit("SUBSCRIBE", ["MARKET.BTC_USDT.DEPTH.LEVEL0"])`。 | 未见 sequence/checksum；消息带 timestamp。断线后重新订阅并以新快照替换。 | 当前 payload_helper，缺 interval/depth；列入 `public_ws_struct`，补 1s 和 Socket.IO 边界。 |
| `okxus` | OKX US 区域 profile 走 OKX V5 public market data 语义；当前项目映射 `books5`。 | `bbo-tbt` 10ms；`books5` 100ms；`books` 100ms；TBT 深度通道 10ms 但需登录/VIP 限制。 | BBO、5 档、400 档、50 档 TBT。 | U.S. host `wss://wsus.okx.com:8443/ws/v5/public`；JSON subscribe。 | 同 OKX V5：增量通道按 `seqId/prevSeqId` 与 checksum 重建；snapshot 通道按快照替换。 | 当前 spec_only，缺 interval/depth；列入 `public_ws_struct`，补区域 host 与限制通道说明。 |
| `onetrading` | One Trading public WS 支持 `ORDER_BOOK` snapshot/update 和 `BOOK_TICKER` BBO。 | 官方公开页未给固定 ms；按变更推送。 | `ORDER_BOOK` 全盘口快照/变更；`BOOK_TICKER` 是 L1/BBO。 | `wss://streams.fast.onetrading.com`；JSON `SUBSCRIBE` channels `ORDER_BOOK` 或 `BOOK_TICKER`。 | 消息有纳秒级 `time`；公开资料未见 sequence/checksum。断线后重新订阅拿 `ORDER_BOOK_SNAPSHOT`。 | 当前 spec/payload only，缺 interval/depth；列入 `public_ws_struct`，补 BBO 和无 sequence 风险。 |
| `oxfun` | OX.FUN public WS 支持 `depthUpdate:<marketCode>` 增量订单簿。 | 100ms。 | 官方未给固定最大档位；snapshot 后推绝对数量增量。 | `wss://api.ox.fun/v2/websocket`；JSON `{"op":"subscribe","args":["depthUpdate:BTC-USD-SWAP-LIN"]}`。 | `seqNum` 递增，payload 有 `checksum`；先等 `depthUpdate` snapshot，再应用 `depthUpdate-diff`。 | 当前 spec_only，缺 interval/depth/checksum 结构化字段；列入 `public_ws_struct`。 |
| `pacifica` | Pacifica public WS 支持 `book` 聚合订单簿和 `bbo` 最优买卖。 | `book` 每 100ms；`bbo` 顶层变化时推送。 | `book` 支持 `agg_level` 1/10/100/1000/10000；`bbo` 是 1 档。 | `wss://ws.pacifica.fi/ws`；JSON `{"method":"subscribe","params":{"source":"book","symbol":"SOL","agg_level":1}}` 或 `source=bbo`。 | `book`/`bbo` 有 `li` last order id，可按交易所级 nonce 排序；未见 checksum。 | 当前 spec_only，缺 interval；列入 `public_ws_struct`，补 100ms、BBO 和 `li`。 |
| `tapbit` | Tapbit Spot 和 USDT Perpetual public WS 都支持 orderBook topic。 | 官方未给固定 ms。 | 5/10/50/100/200。 | Spot `spot/orderBook.{instrument_id}.[depth]`；Perp `usdt/orderBook.{instrument_id}.[depth]`；JSON `op=subscribe`。 | payload 有严格递增 `version`，可检查连续性；未见 checksum。server ping 每 5s。 | 当前已有 spec，但缺 interval/sequence 结构化字段；列入 `public_ws_struct`。 |
| `tokocrypto` | Tokocrypto MBX public streams 支持 partial depth 和 diff depth。 | partial/diff 支持 1000ms 或 100ms。 | partial depth 5/10/20；REST snapshot 最大 5000，常用 1000 重建。 | `wss://stream-cloud.tokocrypto.site/stream`；raw `/ws/<symbol>@depth10@100ms` 或 combined `/stream?streams=...`。 | diff depth 有 `U/u`，REST snapshot 有 `lastUpdateId`；按 Binance-family buffer + snapshot 重建。 | 当前 spec_only，缺 channel/interval/depth；列入 `public_ws_struct`。 |
| `toobit` | Toobit Spot/USDT-M public WS 支持 `depth` snapshot 和 `diffDepth` 增量；USDT-M 另有 `wholeBookTicker` BBO。 | `depth` 300ms；`diffDepth` 每秒。 | `depth` 双边 300 档；REST depth 可按 limit 重建。 | `wss://stream.toobit.com/quote/ws/v1`；JSON `{"symbol":"BTCUSDT","topic":"depth","event":"sub","params":{"binary":false}}`。 | payload 有版本号 `v`，官方说明版本号不保证唯一；未见 checksum。断档用 REST `/quote/v1/depth`。 | 当前 native/spec，缺 interval/depth；列入 `public_ws_struct`，补 300ms/1s 和版本风险。 |
| `wavesexchange` | WX matcher public WS 支持 order book updates。 | 100ms。 | 订阅请求参数 `d` 指定 price depth；单连接当前最多 10 个 orderbook 订阅。 | `wss://matcher.waves.exchange/ws/v0`；JSON `{"T":"obs","S":"amount-price","d":10}`。 | 消息有 update id `U`；snapshot 含指定 depth，update 按 price level 绝对数量处理；未见 checksum。 | 当前 spec_only helper，缺 interval/depth；列入 `public_ws_struct`，补 100ms、`U` 和订阅上限。 |
| `weex` | WEEX Spot depth channel 支持自动 snapshot 后增量。 | 官方公开页未给固定 ms。 | depth level 15 或 200。 | `{"method":"SUBSCRIBE","params":["BTCUSDT@depth15"],"id":2}`。 | payload 有 `U/u`、`d=SNAPSHOT/CHANGED`；缺包时重新订阅获取 snapshot；未见 checksum。 | 当前 native 但缺 channel/interval；列入 `public_ws_struct`，补 15/200 和 update id。 |
| `xt` | XT Spot/Futures public WS 支持 `depth_update@symbol` 本地订单簿重建。 | 项目矩阵已有 1000ms 证据；官方本批未复核到更快固定 ms。 | REST snapshot 常用 limit 500；WS diff 覆盖绝对数量价位。 | Spot `wss://stream.xt.com/public`；Futures `wss://fstream.xt.com/ws/market`；topic `depth_update@btc_usdt`。 | Spot 用 `fi/i`，Futures 用 `fu/u`；按 REST snapshot `lastUpdateId` 和连续性规则处理。 | 当前 parser_only，缺 channel/depth；列入 `public_ws_struct`，补 snapshot + diff 重建规则。 |
| `zaif` | Zaif WebSocket 提供实时板信息、trades 和 last price。 | 官方未给固定 ms。 | 官方未给固定 depth。 | `wss://ws.zaif.jp/stream?currency_pair=btc_jpy`。 | payload 是 asks/bids/trades/timestamp；未见 sequence/checksum。重连后以新 WS 全量状态或 REST depth 重建。 | 当前 native，缺 interval/depth/sequence 字段；列入 `public_ws_struct`。 |

## 高速盘口判断

| adapter | 行情等级 | 原因 |
| --- | --- | --- |
| `myokx`, `okxus` | `10ms BBO / 100ms 5档候选` | OKX V5 `bbo-tbt` 10ms、`books5` 100ms；TBT 深度通道受登录/VIP 限制，区域 profile 要单独确认可用性。 |
| `oxfun`, `pacifica` | `100ms 候选` | OX.FUN incremental book 100ms 且有 `seqNum/checksum`；Pacifica `book` 100ms 且有 `li` ordering。 |
| `tokocrypto`, `tapbit`, `toobit`, `wavesexchange`, `xt`, `weex` | `可结构化但非 10ms/20ms` | 有明确订阅和重建字段，但固定间隔多为 100ms/300ms/1s，或官方未给固定 ms。 |
| `ndax`, `novadax`, `onetrading`, `zaif` | `实时/存在 WS 但细项不足` | 官方支持订单簿 WS，但公开资料缺固定毫秒、checksum 或完整 depth 参数。 |

## 官方资料

| 交易所/profile | 官方来源 |
| --- | --- |
| MyOKX / OKX V5 | <https://app.okx.com/docs-v5/trick_en/> |
| OKX US / OKX V5 | <https://app.okx.com/docs-v5/trick_en/> |
| NDAX | <https://github.com/NDAXlO/ndax-api-documentation> |
| NovaDAX | <https://doc.novadax.com/en-US/> |
| One Trading | <https://docs.onetrading.com/websocket>、<https://docs.onetrading.com/websocket/orderbook/snapshot>、<https://docs.onetrading.com/websocket/orderbook/update>、<https://docs.onetrading.com/websocket/book-ticker/introduction> |
| OX.FUN | <https://oxoxox.gitbook.io/ox-docs/api/websocket/subscriptions-public/incremental-order-book> |
| Pacifica | <https://docs.pacifica.fi/api-documentation/api/websocket/subscriptions/orderbook>、<https://docs.pacifica.fi/api-documentation/api/websocket/subscriptions/best-bid-offer-bbo> |
| Tapbit | <https://www.tapbit.com/openapi-docs/spot/ws/order_book/>、<https://www.tapbit.com/openapi-docs/usdt_perpetual/ws/order_book/> |
| Tokocrypto | <https://www.tokocrypto.com/apidocs/> |
| Toobit | <https://api-docs.toobit.com/api/spot-websocket-market-data>、<https://api-docs.toobit.com/api/usdt-m-websocket-market-data> |
| WavesExchange / WX Network | <https://docs.waves.exchange/en/waves-matcher/matcher-websocket-api-common-streams> |
| WEEX | <https://www.weex.com/api-doc/spot/Websocket/public/Depth-Channel> |
| XT | <https://github.com/XtApis/xt-api>、<https://doc.smnvbtp.cn/> |
| Zaif | <https://zaif-api-document.readthedocs.io/ja/latest/WebSocket_API.html> |

## 下一步实现建议

1. 优先把 `myokx`/`okxus` 的 `books5` 和 `bbo-tbt` 区域 host 写入 endpoint mapping；TBT 深度通道必须标登录/VIP 限制。
2. `oxfun` 和 `pacifica` 可直接补 100ms 结构化字段、sequence/checksum 或 `li` ordering，再补 fixture。
3. `tokocrypto`、`xt`、`weex` 的重建规则清楚，应补 REST snapshot + delta buffer 测试。
4. `ndax`、`novadax`、`onetrading`、`zaif` 先写清无固定 ms/checksum 风险，避免误标为极速盘口。

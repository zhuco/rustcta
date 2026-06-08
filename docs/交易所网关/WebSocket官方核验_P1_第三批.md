# WebSocket 官方核验 P1 第三批

状态日期：2026-06-08

本文件记录“项目已有公共 WebSocket 证据，但缺推流间隔、档位、订阅方式或 sequence/checksum 细项”的交易所。这里是官方资料核验，不代表项目已经完整实现；项目当前实现状态仍以 [交易所功能盘点矩阵](交易所功能盘点矩阵.md) 为准。

## 核验结论

| 交易所 | 产品线 | 官方订单簿 WS 结论 | 推流间隔 | 档位 | 订阅方式 | sequence/checksum | 项目下一步 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Hyperliquid | Perpetual/Spot mids | 官方支持 `l2Book` 和 `bbo`。 | 官方未给固定毫秒；`WsBook` 类型说明为按区块推 snapshot，且至少满足一定间隔才推。 | `bbo` 为 1 档；`l2Book` 返回双边 `levels`，官方未给固定档位参数。 | JSON subscribe：`{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}` 或 `bbo`。 | `WsBook` 有 `time`，没有显式 sequence/checksum；需要按 snapshot 替换或外部一致性检查。 | 当前项目已有 native `bbo`/`l2Book`，应把“官方未给固定 ms”和无 sequence 风险写入 mapping。 |
| Backpack | Spot/Perp | 官方支持 `bookTicker`、实时 `depth` 和聚合 `depth`。 | `depth.<symbol>` 实时；`depth.200ms`、`depth.600ms`、`depth.1000ms` 聚合。 | REST depth limit 5/10/20/50/100/500/1000；WS depth 推变化价位的绝对值。 | JSON `SUBSCRIBE`，stream 为 `bookTicker.<symbol>`、`depth.<symbol>`、`depth.200ms.<symbol>` 等。 | `bookTicker` 有 `u`；depth 有 `U`/`u`，需要 REST snapshot 后按连续 update id 应用。 | 当前项目已有 parser_only，补 200/600/1000ms 聚合通道、`U/u` 和 REST snapshot 重建规则。 |
| BitMEX | Perpetual/Spot | 官方支持 `orderBookL2_25`、`orderBook10`、`orderBookL2`、`quote`。 | 官方支持页当前写 `orderBookL2_25` 每 100ms；`orderBook10` 为每 tick；`orderBookL2` 为 full L2 diff。 | `orderBookL2_25` 25 档；`orderBook10` 10 档；`orderBookL2` 全量 L2。 | URL query 或 JSON subscribe：`orderBookL2_25:XBTUSD`、`orderBook10:XBTUSD`。 | WebSocket table diff 模型：partial/insert/update/delete；需维护本地 table。 | 当前项目只有档位证据，补 100ms、25/10/full 三类通道和 table diff 重建说明。 |
| Deribit | Futures/Perpetual/Option | 官方支持 `book.{instrument}.{group}.{depth}.{interval}` 聚合订单簿。 | `interval` 支持 `100ms` 或 `agg2`。 | `depth` 支持 1/10/20；`group` 支持 none/1/2/5/10/25/100/250 等。 | JSON-RPC `public/subscribe`，channel 如 `book.BTC-PERPETUAL.none.20.100ms`。 | 消息有 `change_id`；聚合 book 是 interval 内聚合结果。 | 当前项目已有 100ms 证据，补 channel、depth、group、change_id 到 mapping。 |
| OrangeX | Spot/Perpetual | 当前官方文档支持 `book.{instrument_name}.{interval}`。 | 当前官方枚举只看到 `raw`，未看到固定 100ms。 | `raw` 为订单簿增量；REST snapshot 可按 depth 取。 | JSON-RPC `public/subscribe`，channel 如 `book.BTC-USDT-PERPETUAL.raw`。 | 使用 `change_id`；REST snapshot 有 `version`，需按官方步骤 buffer + snapshot + replay。 | 当前项目矩阵有 100ms 证据，应复核旧证据来源；mapping 以当前官方 `raw` 和 change_id 重建为准。 |
| WhiteBIT | Spot/Perpetual | 官方支持 `depth_subscribe` 连续订单簿更新。 | 有变化时每 100ms 推增量；10s 无变化时推 full snapshot keepalive。 | 订阅传入 limit，示例为 100；按配置截断本地 book。 | JSON-RPC `depth_subscribe`，例如 `params:["ETH_BTC",100,"0",true]`。 | 有 `update_id`/`past_update_id`；断档需重新订阅重建。 | 当前项目 native 但缺 interval，应补 100ms、keepalive snapshot、update_id 连续性。 |
| WOO X | Spot/Perpetual | 官方支持 request orderbook、`orderbook`、`orderbookupdate`、`bbo`。 | request orderbook 实时；`orderbook` 1s；`orderbookupdate` 200ms；`bbo` 10ms。 | `orderbook`/`orderbook100` 为 100 levels；`bbo` 为 1 档。 | JSON subscribe topic：`SPOT_BTC_USDT@orderbookupdate`、`SPOT_BTC_USDT@bbo`。 | 示例有 `ts`/`prevTs`，未见明确 sequence/checksum；需要 REST/request orderbook 兜底。 | 当前项目已有 native 和 10 档证据，补 10ms BBO、200ms orderbookupdate、100 levels 和 `prevTs` 风险。 |
| Kraken | Spot | 官方 WebSocket v2 `book` channel 支持 L2 order book。 | 官方未给固定 ms，按市场事件推送。 | depth 支持 10/25/100/500/1000，默认 10。 | JSON subscribe：`{"method":"subscribe","params":{"channel":"book","symbol":["BTC/USD"],"depth":100}}`。 | snapshot/update 都有 CRC32 checksum top 10；需按 checksum guide 校验。 | 当前项目 public WS native 但矩阵缺 channel/depth/interval，补 Spot book depth 和 checksum。 |
| Kraken Futures | Futures/Perpetual | 官方 Futures WS `book` feed 支持 snapshot + delta。 | 官方未给固定 ms，按市场事件推送。 | 官方 book feed 未暴露 depth 参数。 | JSON subscribe：`{"event":"subscribe","feed":"book","product_ids":["PI_XBTUSD"]}`。 | snapshot/delta 都有 `seq` 和 timestamp。 | 补 futures book feed、`seq`、snapshot/delta 到 `kraken` 或 `krakenfutures` mapping。 |
| Coinbase Exchange | Spot | 官方支持 `level2`、`level2_batch`、`level3` 和 full channel。 | `level2_batch` 每 50ms 批量推送；`level2` 事件驱动。 | `level2` snapshot 表示 entire order book；无订阅档位参数。 | JSON subscribe：`channels:["level2"]` 或 `["level2_batch"]`。 | `level2` 保证 delivery；snapshot + `l2update`，full/level3 带 sequence。 | 当前项目已 native `level2`，补 50ms batch、full book、sequence/无 sequence 边界。 |
| Coinbase Advanced Trade | Spot/INTX | 官方支持 Advanced Trade `level2` channel。 | 官方未给固定 order book ms；ticker_batch 为 5s，heartbeats 每秒。 | `level2` 为 snapshot/update，未见 depth 参数。 | JSON subscribe：`{"type":"subscribe","product_ids":["BTC-USD"],"channel":"level2","jwt":"..."}`。 | level2 消息有 `sequence_num`/events 结构；官方建议 heartbeat 保持订阅。 | 当前 `coinbase` native 但矩阵缺 channel/interval，补 Advanced Trade level2 和 heartbeat 规则。 |

## 官方来源

| 交易所 | 官方来源 |
| --- | --- |
| Hyperliquid | <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions> |
| Backpack | <https://docs.backpack.exchange/> |
| BitMEX WS API | <https://www.bitmex.com/app/wsAPI> |
| BitMEX `orderBookL2_25` interval | <https://support.bitmex.com/hc/en-gb/articles/6910936268573-How-often-is-the-orderBookL2-25-throttled> |
| Deribit | <https://docs.deribit.com/subscriptions/orderbook/bookinstrument_namegroupdepthinterval> |
| OrangeX | <https://openapi-docs.orangex.com/> |
| WhiteBIT | <https://docs.whitebit.com/websocket/market-streams/depth> |
| WOO X | <https://docs.woox.io/products/woo-x> |
| Kraken Spot | <https://docs.kraken.com/api/docs/websocket-v2/book/> |
| Kraken Futures | <https://docs.kraken.com/api/docs/futures-api/websocket/book/> |
| Coinbase Exchange | <https://docs.cdp.coinbase.com/exchange/websocket-feed/channels> |
| Coinbase Advanced Trade | <https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels> |

## 下一步实现建议

1. `backpack`、`deribit`、`whitebit`、`woo` 的官方细项很明确，优先把 interval/depth/sequence 写进 endpoint mapping。
2. `coinbaseexchange` 和 `coinbase` 都已 native，但应分别记录 Exchange feed 与 Advanced Trade feed，避免混用 `level2` 语义。
3. `orangex` 当前官方文档只显示 `raw`，应复核项目里 100ms 证据来源；如果旧文档已失效，单交易所文档要标“官方当前未给固定 ms”。
4. `hyperliquid` 没有传统 sequence/checksum，策略侧不能把它等同于 Binance/OKX 这类增量 book。
5. `kraken` 与 `krakenfutures` 的产品线和 adapter 边界要分开写，Spot 有 CRC32，Futures 有 `seq`。

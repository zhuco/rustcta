# WebSocket 极速盘口能力汇总

状态日期：2026-06-08

本文件专门记录跨所套利最关心的公共订单簿 WebSocket：最快推流间隔、是否 1 档/BBO、可订阅档位、订阅方式和重建风险。这里是官方能力汇总，不代表项目已经实现；项目当前状态仍以 [交易所功能盘点矩阵](交易所功能盘点矩阵.md) 为准。

## 固定 10ms 或准 10ms 盘口

| 交易所 | 产品线 | 最快官方盘口 | 档位 | 订阅方式 | sequence/checksum | 项目处理 |
| --- | --- | --- | --- | --- | --- | --- |
| OKX | Spot/Swap/Futures/Option | `bbo-tbt` 10ms；`books-l2-tbt`/`books50-l2-tbt` 10ms 有登录/VIP 限制 | BBO、50、全量增量 | JSON subscribe | 需补 seq/checksum 细节 | `okx` 当前 public WS unsupported，先补 BBO/5 档；VIP 通道单独标限制。 |
| MyOKX / OKX US | Spot regional profiles | OKX V5 `bbo-tbt` 10ms；`books5` 100ms；TBT 深度通道 10ms 有登录/VIP 限制 | BBO、5、400、50 TBT | 区域 host + JSON subscribe | OKX 增量语义有 `seqId/prevSeqId` 和 checksum；snapshot 通道按快照替换 | `myokx`/`okxus` 先补区域 host、`books5` 和 `bbo-tbt`，VIP 通道单独标限制。 |
| Bybit | Spot/Linear/Inverse | `orderbook.1` 10ms；50 档 20ms；200 档 100ms；1000 档 200ms | 1/50/200/1000 | JSON topic `orderbook.{depth}.{symbol}` | 有 `u` 和 cross `seq`；L1 snapshot-only | 当前 spec_only，优先补 1 档 10ms 和 50 档 20ms。 |
| Bybit EU | Spot/Linear/Inverse | 沿用 Bybit V5：`orderbook.1` 10ms；50 档 20ms；200 档 100ms；1000 档 200ms | 1/50/200/1000 | EU host + JSON topic `orderbook.{depth}.{symbol}` | 有 `u`、cross `seq`、`cts`；L1 snapshot-only | `bybiteu` 当前 spec_only，补 EU host 下 10ms L1 和 20ms 50 档。 |
| Bitget | Spot | `books1` 10ms；`books`/`books5`/`books15` 200ms | 1/5/15/全量 | JSON subscribe `channel=books1/books5/...` | 有 `seq` 和 CRC32 | 当前 public WS unsupported，优先补 `books1`。 |
| MEXC | Spot | aggregated depth 和 bookTicker 可选 10ms/100ms | L1 bookTicker；partial 5/10/20；REST snapshot 1000 | JSON `SUBSCRIPTION` stream 字符串 | 有 `fromVersion`/`toVersion`，断档重建 | 当前 public WS unsupported，优先补 10ms bookTicker 和 aggregated depth。 |
| WOO X | Spot/Perpetual | `bbo` 10ms；`orderbookupdate` 200ms；`orderbook` 1s | BBO、100 levels | JSON topic `{symbol}@bbo`/`@orderbookupdate` | 示例有 `prevTs`，未见强 sequence/checksum | 当前 native 但缺结构化字段，补 10ms BBO 和 200ms update。 |
| KuCoin UTA | Spot/Futures | 2026-06-17 UTC 上线 `depth=increment@10ms` | 500 档增量 | JSON subscribe `channel=obu`，`depth=increment@10ms` | 按 UTA orderbook sequence 校准 | 这是未来上线能力；2026-06-17 前不能写成当前可用，2026-07-15 前要处理旧 increment 迁移。 |
| Crypto.com | Spot/Perpetual | `book` delta 可选 10ms 或 100ms | depth 10/50 | JSON-RPC `book.{instrument}.{depth}`，`SNAPSHOT_AND_UPDATE` | 有 `u/pu`；断档需重新 subscribe 获取 snapshot | 当前 native 但缺结构化字段，补 10ms/100ms、10/50 档和 `u/pu`。 |

## 固定 20ms/100ms 盘口补充

| 交易所 | 产品线 | 最快官方盘口 | 档位 | 订阅方式 | sequence/checksum | 项目处理 |
| --- | --- | --- | --- | --- | --- | --- |
| Phemex | Spot/Perpetual | orderbook depth 约 20ms；聚合参数约 120ms；full depth 100ms | 0/1/5/10/30，0 表示 full book | `orderbook.subscribe` / `orderbook_p.subscribe` | 有 `sequence`；未声明 checksum；每 60s snapshot 自校验 | 当前 native 但缺结构化字段，补 20ms、depth 参数和 sequence。 |
| BitMart | Spot | bookTicker BBO real-time；`spot/depth/increase100` 100ms；full depth 500ms | BBO、5/20/50 full、100 incremental | `spot/bookTicker`、`spot/depth/increase100`、`spot/depth5/20/50` | 有 `version`；未见 checksum | 当前 spec_only，补 BBO 和 100 档增量 book。 |
| Lighter | Perpetual/Spot | `order_book/{MARKET_INDEX}` 每 50ms | 官方未给固定 depth；首包 snapshot 后只推变化 | JSON subscribe `order_book/{market_id}` | `begin_nonce == previous nonce` 连续性；无 checksum | 当前 parser_only，补 50ms、nonce continuity 和 gap resubscribe。 |
| Paradex | Perpetual/Option/Spot | order_book refresh_rate 50ms 或 100ms | 固定 15 levels；feed_type snapshot/deltas/interactive | URL path `order_book.{market_symbol}.{feed_type}@15@{refresh_rate}` | 有 `seq_no`；未见 checksum | 当前 native，补 50/100ms、15 levels、feed_type 和 sequence。 |
| Aster | Perpetual | partial/diff depth 可 100ms、250ms、500ms；bookTicker real-time | partial depth 5/10/20；REST snapshot 可用于重建 | Binance-style stream `symbol@depth20@100ms` | `U/u/pu` + REST `lastUpdateId` | 当前 declared，补 100ms、5/10/20 档和重建规则。 |
| BloFin | Perpetual | `books` 增量 100ms；`books5` 有变化时 100ms | `books` 初始 200 depth；`books5` 5 depth | JSON subscribe `channel=books/books5` | `prevSeqId/seqId`；未见 checksum | 当前 native，补 100ms、200/5 档和断档重建。 |
| Orderly profile | Perpetual | `orderbookupdate` 每 200ms | public update 未给固定 depth；request orderbook 取 snapshot | `{symbol}@orderbookupdate` | `prevTs`；未见 checksum | `aark`/`modetrade`/`woofipro` 共享，补 endpoint/account_id 和 snapshot fallback。 |
| ApolloX DEX/APX | Perpetual | bookTicker real-time；partial/diff depth 可 100ms/250ms/500ms | partial 5/10/20；REST snapshot 5/10/20/50/100/500/1000 | Binance-style stream `<symbol>@depth20@100ms` / `<symbol>@bookTicker` | `U/u/pu` + REST `lastUpdateId` | 当前 spec_only，补 100ms、5/10/20 档和重建规则。 |
| BYDFi | Perpetual | depth/limited depth 可选 100ms 或 1000ms | incremental depth；limited depth 10/50/100 | path stream 或 JSON SUBSCRIBE，如 `BTC-USDT@depth10@100ms` | 未见 sequence/checksum；REST snapshot fallback | 当前 native 但缺结构化字段，补 100ms 和 10/50/100 档。 |
| Delta Exchange | Futures/Option/Perpetual | `ob_l1` 100ms；`ob_updates` 100ms；`ob_l2` 500ms | L1、15 档、全量增量 | JSON subscribe `ob_l1`/`ob_l2`/`ob_updates` | `ob_updates` 有 `seq` 和 CRC32 `cs`；断档 resubscribe | 当前 descriptor only，补新 public endpoint 和 checksum/sequence。 |
| Gemini | Spot | bookTicker real-time；partial/diff depth 可选 100ms 或 1s | BBO、partial 5/10/20、diff depth | `{symbol}@bookTicker`、`{symbol}@depth10@100ms`、`{symbol}@depth@100ms` | `lastUpdateId`、`U/u`；未见 checksum | 当前 native 但缺结构化字段，补 100ms、5/10/20 和 `U/u`。 |
| HitBTC / FMFW.io | Spot | `orderbook/top/100ms` BBO；`orderbook/D5-D20/100ms` partial | BBO、D5/D10/D20 | JSON-RPC `subscribe` channel `orderbook/top/100ms` 或 `orderbook/D5/100ms` | partial 有 sequence `s`；未见 checksum | 当前 spec_only，补 100ms BBO/partial 和 REST snapshot fallback。 |
| KuCoin Futures | Perpetual | increment real-time；5/50 档 100ms；UTA BBO real-time | increment、1/5/50 | classic `/contractMarket/level2*` 或 UTA `channel=obu` | `sequence`/`sequenceStart`/`sequenceEnd`；未见 checksum | 当前 native，补 channels、100ms 和 REST snapshot replay。 |
| OX.FUN | Option/Perpetual | `depthUpdate` incremental order book 100ms | 官方未给固定最大档位 | JSON `depthUpdate:<marketCode>` | `seqNum` 递增，payload 有 checksum；先 snapshot 后 diff | 当前 spec_only，补 100ms、seqNum/checksum 和断档 resubscribe。 |
| Pacifica | Perpetual | `book` 每 100ms；`bbo` 顶层变化推送 | `agg_level` 1/10/100/1000/10000；BBO 1 档 | JSON `source=book` 或 `source=bbo` | `li` last order id 可排序；未见 checksum | 当前 spec_only，补 100ms、BBO 和 `li` ordering。 |

## 高速但官方未给固定毫秒

| 交易所 | 产品线 | 官方写法 | 档位 | 订阅方式 | 项目处理 |
| --- | --- | --- | --- | --- | --- |
| Binance | Spot | bookTicker real-time；partial/diff depth 100ms 或 1000ms | bookTicker L1；partial 5/10/20；diff 全量增量 | URL stream 或 combined stream | public WS unsupported，补 100ms depth 和 bookTicker。 |
| Binance.US | Spot | bookTicker real-time；partial/diff depth 100ms 或 1000ms | bookTicker L1；partial 5/10/20 | URL stream 或 JSON subscribe | public WS unsupported，按 Binance Spot 同类补。 |
| KuCoin | Spot/Futures | BBO real time；5/50 档 100ms；increment real time | 1/5/50/increment | JSON subscribe `channel=obu` | native 但缺结构化字段；同时跟进 2026-06-17 的 10ms 500 档。 |
| HTX/Huobi | Spot | BBO tick-by-tick；MBP refresh 约 100ms | BBO、5/10/20/150/400 | JSON `sub` | 补 `seqNum`/`prevSeqNum` 和 BBO/MBP 档位。 |
| HTX/Huobi | USDT-M Swap/Futures | 高频 depth 每 30ms 检查，有变化才推 | 20/150 非合并档 | JSON `sub` | 补 `version`、snapshot/update 重建。 |
| Hyperliquid | Perpetual | `bbo`/`l2Book`，官方未给固定毫秒 | BBO、双边 levels | JSON subscribe | 没有传统 sequence/checksum，策略侧要标高风险。 |
| Backpack | Spot/Perp | bookTicker 和 realtime depth；聚合 depth 200/600/1000ms | L1、实时 depth、聚合 depth | JSON `SUBSCRIBE` | parser_only，补 `U/u` 和 REST snapshot 重建。 |
| Coinbase Exchange | Spot | `level2_batch` 50ms；`level2` 事件驱动 | full book，无档位参数 | JSON subscribe | native，补 50ms batch 和 sequence 边界。 |
| Coincheck | Spot | Public orderbook 约 0.1s when trade occurs | 官方未给固定 depth | JSON subscribe `[pair]-orderbook` | native，补 0.1s、无 sequence/checksum 和 REST snapshot 兜底。 |
| CoinEx | Spot | depth push 约 200ms；有变化才推；full market depth 每 1 分钟 | limit 5/10/20/50 | `depth.subscribe` | signed CRC32 checksum；`is_full` 区分全量/增量 | native，补 200ms、limit 和 checksum 字段。 |
| ApeX | Perpetual | high-frequency order book topic，官方未给固定毫秒 | 25/200 | `orderBook{limit}.H.{symbol}` | native，补 25/200 档、snapshot+delta 和乱序重建。 |
| Bullish | Spot/Perpetual | L1/L2 orderbook live realtime，官方未给固定毫秒 | L1/L2 | JSON-RPC `l1Orderbook`/`l2Orderbook` | native，补 sequenceNumber、reconnect 和 REST hybrid snapshot。 |
| dYdX | Perpetual | `v4_orderbook` channel data，官方未给固定毫秒 | initial snapshot + price level update | JSON subscribe `v4_orderbook` | native，补 message_id/version 和重订阅 snapshot 重建。 |
| BitTrade | Spot | BBO/depth 变化推送，官方未给固定毫秒 | BBO；depth step0-step5 | `market.<symbol>.bbo` / `market.<symbol>.depth.<type>` | native，补 BBO `seqId`、gzip 和 REST depth 重建。 |
| Bitunix | Spot/Perpetual | Futures 1/5/15 档 depth，官方未给固定毫秒 | `book1`/`book5`/`book15`/changed book | `depth_book1`/`depth_book5`/`depth_book15` | spec/parser ready，补无 sequence/checksum 和 REST depth fallback。 |
| Blockchain.com Exchange | Spot | L2/L3 order book，官方未给固定毫秒 | L2 聚合；L3 单订单级别 | `l2`/`l3` subscribe | spec_only，补 `seqnum`、snapshot/update 和断档重建。 |
| BigONE | Spot/Perpetual | MarketDepth real-time，官方未给固定毫秒 | 聚合 price levels；未给固定 depth 参数 | `subscribeMarketDepthRequest` | parser_only，补 `changeId/prevId` 和 protobuf 选项。 |
| BSX | Perpetual | `book` channel 每 25ms 推有变化的增量，每分钟 snapshot | 官方未给固定 depth 参数 | JSON `{"op":"sub","channel":"book","product":"BTC-PERP"}` | 有 `gsn`，未见 checksum | spec_only，补 25ms、`gsn`、snapshot/update 和 REST snapshot。 |
| CoinTR | Spot/Perpetual | depth channel 200-300ms | `books` full、`books1`/`books5`/`books15` | JSON subscribe `channel=books5` | CRC32 checksum；snapshot 后 update | declared，补 200-300ms、1/5/15/full 和 checksum。 |
| Coinstore | Spot/Perpetual | Spot depth stream 最小推送间隔 100ms | Spot depth；Futures `future_snapshot_depth` | Spot `SUB` depth channel；Futures Socket.IO subscribe | Spot session 序号 `S`，未见 checksum | parser_only，补 100ms、session seq 和 REST snapshot fallback。 |
| HashKey Global | Spot/Perpetual | depth topic 300ms | 最多 200 档 | JSON `topic=depth,event=sub` | 未见 sequence/checksum | spec/parser ready，补 300ms、200 档和 REST depth fallback。 |
| GRVT | Perpetual/Option | selector 示例 500ms | 示例 50 档 | JSON-RPC `stream=v1.book.s`, selector `instrument@500-50` | `sequence_number`/`prev_sequence_number`；未见 checksum | parser_only，补 selector、sequence 和 gap resubscribe。 |
| Kraken Futures | Perpetual | `book` feed snapshot+delta，官方未给固定毫秒 | 官方未给固定 depth | JSON `feed=book` | 有 `seq`；未见 checksum | native，补 `seq` 连续性和 snapshot/delta。 |
| One Trading | Spot/Futures boundary | `ORDER_BOOK` snapshot/update 和 `BOOK_TICKER` BBO，官方未给固定毫秒 | full order book update；BBO | JSON `SUBSCRIBE` channel | 未见 sequence/checksum；重新订阅拿 snapshot | spec_only，补 BBO 和无 sequence 风险。 |
| NDAX | Spot | Level1/Level2 WSGateway，官方未给固定毫秒 | L1/BBO；L2 snapshot 可指定 Depth | WSGateway `SubscribeLevel1`/`SubscribeLevel2` | 未见 book checksum；断线后重新 snapshot | payload_helper，补 Level1/Level2 和 Depth。 |
| NovaDAX | Spot | depth data 每秒一次 | 官方未给可选档位 | Socket.IO `MARKET.{symbol}.DEPTH.LEVEL0` | 未见 sequence/checksum | payload_helper，补 1s 和重新订阅全量替换。 |
| Tapbit | Spot/Perpetual | orderBook 变化推送，官方未给固定毫秒 | 5/10/50/100/200 | JSON `spot/orderBook.*` / `usdt/orderBook.*` | `version` 严格递增；未见 checksum | spec_only，补 version 连续性和 5s ping。 |
| Tokocrypto | Spot | partial/diff depth 可 100ms 或 1000ms | partial 5/10/20；diff depth | raw/combined stream | `U/u` + REST `lastUpdateId` | spec_only，补 Binance-family snapshot + delta buffer。 |
| Toobit | Spot/Perpetual | `depth` 300ms；`diffDepth` 每秒；USDT-M BBO 实时 | depth 双边 300 档；BBO | JSON `topic=depth/diffDepth/wholeBookTicker` | 版本 `v` 不保证唯一；未见 checksum | native，补 300ms/1s 和版本风险。 |
| WavesExchange | Spot | matcher order book updates 100ms | 订阅参数 `d` 指定 depth | JSON `obs/obu` | update id `U`；未见 checksum | spec_only，补 100ms、depth `d` 和订阅上限。 |
| WEEX | Spot/Perpetual | depth channel，官方未给固定毫秒 | 15/200 | JSON `<symbol>@depth15/200` | `U/u`，`SNAPSHOT/CHANGED`；缺包重订阅 | native，补 update id 和无固定 ms 风险。 |
| XT | Spot/Perpetual | `depth_update`，项目已有 1000ms 证据 | REST snapshot 常用 500 | topic `depth_update@symbol` | Spot `fi/i`，Futures `fu/u` | parser_only，补 snapshot + diff 重建。 |
| Zaif | Spot | 实时板信息，官方未给固定毫秒 | 官方未给固定 depth | URL query `stream?currency_pair=` | 未见 sequence/checksum | native，补 REST depth fallback。 |

## 落地字段

每个 adapter 的 `endpoint_mapping.yaml` 或单交易所文档至少要把下面字段写清楚：

```yaml
websocket:
  public:
    orderbook:
      subscription_mode: json_subscribe
      fastest_interval_ms: 10
      fastest_l1_channel: orderbook.1
      depth_levels: [1, 50, 200, 1000]
      sequence_fields: [u, seq]
      checksum: none
      resync: snapshot_then_delta
      reviewed_at: 2026-06-08
```

## 官方来源

| 交易所 | 官方来源 |
| --- | --- |
| OKX | <https://app.okx.com/docs-v5/trick_en/> |
| MyOKX / OKX US | <https://app.okx.com/docs-v5/trick_en/> |
| Bybit | <https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook> |
| Bitget | <https://www.bitget.com/api-doc/spot/websocket/public/Depth-Channel> |
| MEXC | <https://mexcdevelop.github.io/apidocs/spot_v3_en/> |
| WOO X | <https://docs.woox.io/products/woo-x> |
| KuCoin Orderbook | <https://www.kucoin.com/docs-new/3470221w0> |
| KuCoin 2026-06-08 公告 | <https://www.kucoin.com/announcement/en-api-upgrade-announcement-500-level-depth-feed-api-rate-limit> |
| Binance Spot | <https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md> |
| Coinbase Exchange | <https://docs.cdp.coinbase.com/exchange/websocket-feed/channels> |
| Coincheck | <https://coincheck.com/documents/exchange/api> |
| Crypto.com | <https://exchange-docs.crypto.com/exchange/v1/rest-ws/index.html#book-instrument-name-depth> |
| Phemex | <https://phemex-docs.github.io/> |
| BitMart | <https://developer-pro.bitmart.com/en/spot/> |
| CoinEx | <https://docs.coinex.com/api/v2/spot/market/ws/market-depth> |
| Orderly / Aark / Mode Trade / WOOFi Pro | <https://orderly.network/docs/build-on-omnichain/websocket-api/public/order-book-update> |
| ApeX | <https://api-docs.pro.apex.exchange/practice/index.html> |
| Aster | <https://docs.asterdex.com/product/aster-perpetuals/api/api-documentation> |
| BloFin | <https://docs.blofin.com/index.html> |
| Bullish | <https://docs.exchange.bullish.com/websocket/public/market-data/orderbook/index.html> |
| dYdX | <https://raw.githubusercontent.com/dydxprotocol/v4-chain/main/indexer/services/comlink/public/websocket-documentation.md> |
| Lighter | <https://apidocs.lighter.xyz/docs/websocket-reference> |
| Paradex | <https://docs.paradex.trade/ws/web-socket-channels/order-book-market-symbol-feed-type-15-refresh-rate-price-tick/order-book-market-symbol-feed-type-15-refresh-rate-price-tick> |
| ApolloX DEX/APX | <https://raw.githubusercontent.com/apollox-finance/apollox-finance-api-docs/master/apollox-finance-api.md> |
| BitTrade | <https://api-doc.bittrade.co.jp/> |
| Bitunix | <https://www.bitunix.com/api-docs/futures/websocket/public/depth%20channel.html> |
| Blockchain.com Exchange | <https://exchange.blockchain.com/api> |
| BigONE | <https://open.bigone.com/docs/spot/pusher> |
| BSX | <https://api-docs.bsx.exchange/reference/ws-orderbook> |
| BtcTurk | <https://docs.btcturk.com/docs/websocket-feed/models/> |
| Bybit EU | <https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook> |
| BYDFi | <https://developers.bydfi.com/en/futures/websocket-market> |
| Coinstore | <https://coinstore-openapi.github.io/en/index.html> |
| CoinTR | <https://www.cointr.com/api-doc/spot/websocket/public/depth-channel> |
| Cryptomus | <https://doc.cryptomus.com/methods/exchange/websockets> |
| DeepCoin | <https://www.deepcoin.com/docs/publicWS/public> |
| Delta Exchange | <https://docs.delta.exchange/> |
| DigiFinex | <https://docs.digifinex.com/en-ww/spot/v1/websocket.html> |
| EXMO | <https://support.exmo.com/hc/en-us/articles/14338305227676-Websocket-API> |
| Gemini | <https://developer.gemini.com/websocket/streams> |
| HitBTC | <https://api.hitbtc.com/> |
| FMFW.io | <https://api.fmfw.io/> |
| KuCoin Futures | <https://www.kucoin.com/docs-new/3470082w0>、<https://www.kucoin.com/docs-new/3470097w0>、<https://www.kucoin.com/docs-new/3470221w0> |
| HashKey Global | <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api> |
| GRVT | <https://api-docs.grvt.io/market_data_streams/> |
| Kraken Futures | <https://docs.kraken.com/api/docs/futures-api/websocket/book/> |
| NDAX | <https://github.com/NDAXlO/ndax-api-documentation> |
| NovaDAX | <https://doc.novadax.com/en-US/> |
| One Trading | <https://docs.onetrading.com/websocket> |
| OX.FUN | <https://oxoxox.gitbook.io/ox-docs/api/websocket/subscriptions-public/incremental-order-book> |
| Pacifica | <https://docs.pacifica.fi/api-documentation/api/websocket/subscriptions/orderbook> |
| Tapbit | <https://www.tapbit.com/openapi-docs/spot/ws/order_book/> |
| Tokocrypto | <https://www.tokocrypto.com/apidocs/> |
| Toobit | <https://api-docs.toobit.com/api/spot-websocket-market-data> |
| WavesExchange / WX Network | <https://docs.waves.exchange/en/waves-matcher/matcher-websocket-api-common-streams> |
| WEEX | <https://www.weex.com/api-doc/spot/Websocket/public/Depth-Channel> |
| XT | <https://github.com/XtApis/xt-api> |
| Zaif | <https://zaif-api-document.readthedocs.io/ja/latest/WebSocket_API.html> |

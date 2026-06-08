# WebSocket 官方核验 P8 补充交易所盘口细项三

状态日期：2026-06-08

本批继续处理剩余公共订单簿 WebSocket 细项，覆盖 12 个 adapter/profile。重点仍是跨所套利需要的订阅方式、推流间隔、档位、sequence/checksum 和 snapshot 重建边界。这里是官方能力核验；项目当前实现仍以 [交易所功能盘点矩阵](交易所功能盘点矩阵.md) 为准。

## 核验结论

| adapter | 官方订单簿 WS 结论 | 推流间隔 | 档位 | 订阅方式 | sequence/checksum 与重建 | 当前项目处理 |
| --- | --- | --- | --- | --- | --- | --- |
| `fmfwio` | FMFW.io 是 HitBTC v3 family API，public WS 支持 full/partial/top orderbook。 | partial/top 支持 100ms/500ms/1000ms；full 是 snapshot+update 事件。 | partial 支持 D5/D10/D20；top 是 L1/BBO。 | `wss://api.fmfw.io/api/3/ws/public`；JSON-RPC `subscribe`，channel 如 `orderbook/D5/100ms`、`orderbook/top/100ms`、`orderbook/full`。 | partial payload 有 sequence `s`；full orderbook 为 snapshot/update。未见 checksum；断档用 REST `/public/orderbook/{symbol}` 重建。 | 当前 spec_only，缺 orderbook channel；列入 `public_ws_struct`，补 100ms、D5/D10/D20、top BBO 和 sequence。 |
| `foxbit` | Foxbit REST/WS v3 支持 public orderbook subscribe payload；本批未在公开页核到固定推流毫秒。 | 官方公开资料未给固定 ms。 | 项目已记录 orderbook-100/250/500/1000；需继续核验这些是档位而非推流间隔。 | `wss://api.foxbit.com.br/ws/v3/public`；项目已有 `public_orderbook_subscribe` payload helper。 | 本批未见 sequence/checksum；断线后 REST `/markets/{market_symbol}/orderbook` 重建。 | 当前 payload_helper；列入 `public_ws_struct`，补“无固定 ms/sequence/checksum”风险和 depth 参数复核。 |
| `gemini` | Gemini public WS streams 支持 BBO、partial depth、diff depth。 | bookTicker real-time；partial depth 1s 或 100ms；diff depth 1s 或 100ms。 | partial depth 5/10/20；diff depth 可用 snapshot 参数要全量或 top N 初始 snapshot。 | `wss://ws.gemini.com`；stream 如 `{symbol}@bookTicker`、`{symbol}@depth10@100ms`、`{symbol}@depth@100ms`。 | bookTicker 有 `u`；diff depth 有 `U/u`；partial 有 `lastUpdateId`；未见 checksum。 | 当前 native 但缺细项；列入 `public_ws_struct`，补 100ms、5/10/20、bookTicker 和 `U/u`。 |
| `grvt` | GRVT market data WS 支持 `v1.book.s` snapshot-style 和 `v1.book.d` delta-style orderbook。 | selector 中 rate 示例 `@500`，可订 500ms；高频 delta channel 另需按官方 selector 扩展。 | snapshot selector 形如 `instrument@rate-depth`，示例 `BTC_USDT_Perp@500-50` 是 50 档。 | `wss://market-data.grvt.io/ws/full` 或 `/ws/lite`；JSON-RPC subscribe `stream=v1.book.s`。 | payload 有 `sequence_number` 和 `prev_sequence_number`；未见 checksum。断档需重新订阅或回 REST market data snapshot。 | 当前 parser_only；列入 `public_ws_struct`，补 500ms、50 档、sequence/prev_sequence。 |
| `hashkey_global` | HashKey Global public stream depth 支持 Spot/Perpetual symbol。 | 300ms。 | 最多 200 档；REST depth limit max 200。 | `wss://stream-glb.hashkey.com/quote/ws/v1`；`{"symbol":"BTCUSDT","topic":"depth","event":"sub","params":{"binary":false},"id":1}`。 | 官方公开页未见 sequence/checksum；心跳建议客户端每 10s ping。断线用 REST `/quote/v1/depth` 重建。 | 当前 spec/parser ready；列入 `public_ws_struct`，补 depth topic、300ms、200 档和无 checksum 风险。 |
| `hibachi` | Hibachi SDK/公开资料有 market WS `orderbook/{symbol}` payload，但官方公开页未给稳定 interval/depth/sequence/checksum。 | 官方公开资料未给固定 ms。 | REST orderbook 支持 `depth` 和 `granularity`；WS depth 参数未见公开说明。 | `wss://data-api.hibachi.xyz/ws/market`；`{"method":"subscribe","channel":"orderbook/{symbol}"}`。 | 本批未见 sequence/checksum；断线用 REST `/market/data/orderbook` 重建。 | 当前 payload_fixture_only；列入 `public_ws_struct`，补无固定 ms/depth/sequence 风险，不提升 runtime。 |
| `hitbtc` | HitBTC v3 public WS 支持 full/partial/top orderbook。 | partial/top 支持 100ms/500ms/1000ms；full 是 snapshot+update 事件。 | partial 支持 D5/D10/D20；top 是 L1/BBO。 | `wss://api.hitbtc.com/api/3/ws/public`；JSON-RPC `subscribe`，channel 如 `orderbook/D5/100ms`、`orderbook/top/100ms`、`orderbook/full`。 | partial payload 有 sequence `s`；full orderbook 为 snapshot/update。未见 checksum；断档用 REST `/public/orderbook/{symbol}` 重建。 | 当前 spec_only，缺 orderbook channel；列入 `public_ws_struct`，补 100ms、D5/D10/D20、top BBO 和 sequence。 |
| `hollaex` | HollaEx public WS 支持 `orderbook` channel，也可按 symbol 订阅 `orderbook:xht-usdt`。 | 官方未给固定 ms。 | 官方未给固定 depth；消息语义取决于白标 exchange 的 orderbook。 | HollaEx exchange `wss://<exchange>/stream`；library `client.connect(['orderbook:xht-usdt'])`。 | 官方公开页未见 sequence/checksum；白标 venue 差异大，断线后用 REST `/v2/orderbook` 重建。 | 当前 spec_only；列入 `public_ws_struct`，补 HollaEx profile 风险、无固定 ms/depth/checksum。 |
| `krakenfutures` | Kraken Futures public WS `book` feed 支持 snapshot + delta。 | 官方未给固定 ms；有变化推 delta。 | 官方未给固定 depth 参数；book snapshot 包含 bids/asks price levels。 | `wss://futures.kraken.com/ws/v1`；`{"event":"subscribe","feed":"book","product_ids":["PI_XBTUSD"]}`。 | snapshot/delta 都有 `seq`；未见 checksum。`seq` 不连续时重订阅/REST snapshot 重建。 | 当前 native 但缺细项；列入 `public_ws_struct`，补 `book` feed、`seq` 和 snapshot/delta。 |
| `kucoinfutures` | KuCoin Futures classic/pro WS 支持 increment、5 档、50 档和 UTA `obu`。 | increment real-time；level2Depth5/50 为 100ms；UTA BBO real-time、5/50 档 100ms、increment real-time。 | increment 全量增量；5/50；UTA depth 1/5/50/increment。 | classic topic `/contractMarket/level2:{symbol}`、`/contractMarket/level2Depth5:{symbol}`、`/contractMarket/level2Depth50:{symbol}`；UTA `channel=obu, tradeType=FUTURES`。 | classic/pro payload 有 `sequence`/`sequenceStart`/`sequenceEnd`；REST `/api/v1/level2/snapshot` 重建；未见 checksum。 | 当前 native，缺 channel/interval；列入 `public_ws_struct`，补 100ms 5/50、increment real-time 和 sequence 重建。 |
| `mango_markets` | Mango 当前 adapter 是 Solana/Mango scan-only profile；可确认 Solana RPC `accountSubscribe`，未确认中心化公共订单簿 WS。 | 交易所不支持当前公共订单簿 WS runtime。 | 交易所不支持当前公共订单簿 WS runtime。 | 仅可用 Solana `accountSubscribe` 监听账户变化，不等价于统一交易所 orderbook channel。 | Solana account notification 有 slot/context，但没有交易所级 orderbook sequence/checksum；OpenBook/Serum 重建需单独链上 indexer。 | 当前 payload fixture only；列入 `public_ws_unsupported`，单交易所文档写 `交易所不支持当前公共订单簿 WS runtime`。 |
| `mercado` | Mercado Bitcoin 官方 API v4 说明有 WebSocket API，但本批公开页只稳定核到 REST orderbook；项目已有 orderbook payload helper。 | 官方公开页未给固定 ms。 | 官方公开页未给 WS depth；REST orderbook `limit` max 1000。 | 当前保留项目 `orderbook` payload helper；需取得 WebSocket API 页面后再提升 runtime。 | 本批未见 sequence/checksum；断线或 payload gap 用 REST `/api/v4/{symbol}/orderbook` 重建。 | 当前 payload_helper；列入 `public_ws_struct`，补“官方 WS 入口存在但细项不足”风险和 REST fallback。 |

## 高速盘口判断

| adapter | 行情等级 | 原因 |
| --- | --- | --- |
| `fmfwio`, `hitbtc` | `100ms + BBO 候选` | HitBTC v3 family 支持 `orderbook/D5/100ms` 和 `orderbook/top/100ms`，有 sequence `s`，但无 checksum。 |
| `gemini` | `100ms + BBO 候选` | 新 stream 支持 bookTicker real-time、partial/diff depth 100ms、5/10/20 档和 `U/u`。 |
| `kucoinfutures` | `100ms/real-time 候选` | Futures increment real-time，5/50 档 100ms，序列重建规则清楚。 |
| `hashkey_global` | `300ms 中速盘口` | depth 300ms、最多 200 档，但缺 sequence/checksum。 |
| `grvt` | `500ms selector 候选` | 官方示例 `BTC_USDT_Perp@500-50`，有 sequence/prev_sequence。 |
| `krakenfutures` | `实时但无固定毫秒` | `book` snapshot+delta 有 `seq`，官方未给固定 ms/depth。 |
| `foxbit`, `hibachi`, `hollaex`, `mercado` | `实时/存在 WS 但细项不足` | 官方或项目资料有 orderbook WS/payload，但公开资料没有固定 ms、sequence/checksum 或 depth 细项。 |
| `mango_markets` | `交易所不支持当前公共订单簿 WS runtime` | 当前只确认 Solana `accountSubscribe`，不是可直接用于套利的交易所 orderbook stream。 |

## 官方资料

| 交易所/profile | 官方来源 |
| --- | --- |
| FMFW.io | <https://api.fmfw.io/> |
| Foxbit | <https://api.foxbit.com.br/rest/v3>、<https://api.foxbit.com.br/ws/v3/public> |
| Gemini | <https://developer.gemini.com/websocket/streams> |
| GRVT | <https://api-docs.grvt.io/market_data_streams/> |
| HashKey Global | <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>、<https://hashkeyglobal-apidoc.readme.io/reference/public-stream> |
| Hibachi | <https://pypi.org/project/hibachi-xyz/>、<https://www.postman.com/hibachi-xyz/hibachi-public/overview> |
| HitBTC | <https://api.hitbtc.com/> |
| HollaEx | <https://docs.hollaex.com/advanced/the-network-tool-library/functions/websocket>、<https://docs.hollaex.com/developers/api-guide> |
| Kraken Futures | <https://docs.kraken.com/api/docs/futures-api/websocket/book/> |
| KuCoin Futures | <https://www.kucoin.com/docs-new/3470082w0>、<https://www.kucoin.com/docs-new/3470083w0>、<https://www.kucoin.com/docs-new/3470097w0>、<https://www.kucoin.com/docs-new/3470221w0> |
| Mango Markets | <https://mango.markets/>、<https://solana.com/docs/rpc/websocket/accountsubscribe> |
| Mercado Bitcoin | <https://api.mercadobitcoin.net/> |

## 下一步实现建议

1. 优先补 `gemini`、`hitbtc`、`fmfwio`、`kucoinfutures`，它们有 100ms 或 real-time + sequence 证据。
2. `hashkey_global`、`grvt` 可作为中速盘口结构化任务，先补 depth/sequence 字段和 fixture。
3. `krakenfutures` 已有 native WS，补 `book` feed、`seq` 和 snapshot/delta parser 即可。
4. `foxbit`、`hibachi`、`hollaex`、`mercado` 先写清无固定 ms/sequence/checksum 风险，不应标成极速盘口。
5. `mango_markets` 当前不启用公共订单簿 WS runtime；如要接 OpenBook/Serum 盘口，需要独立链上 indexer 设计。

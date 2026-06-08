# WebSocket 官方核验 P6 补充交易所盘口细项

状态日期：2026-06-08

本批处理剩余队列中的补充 CEX、经纪商、链上永续和混合 profile 公共订单簿 WebSocket 细项。重点仍是跨所套利需要的订阅方式、推流间隔、档位、sequence/checksum、snapshot 重建。这里是官方能力核验；项目当前实现仍以 [交易所功能盘点矩阵](交易所功能盘点矩阵.md) 为准。

## 核验结论

| adapter | 官方订单簿 WS 结论 | 推流间隔 | 档位 | 订阅方式 | sequence/checksum 与重建 | 当前项目处理 |
| --- | --- | --- | --- | --- | --- | --- |
| `bitkan` | 当前未找到 BitKan 官方稳定 API 文档公开说明订单簿 WebSocket；官方站点/公开资料更像聚合交易/broker 入口。 | 交易所不支持公共 WS 行情。 | 交易所不支持公共 WS 行情。 | 交易所不支持公共 WS 行情。 | 交易所不支持公共 WS 行情；只能保留 REST/聚合行情或未来官方 API 重核。 | 当前 public_ws=unsupported；列入 `public_ws_unsupported`，单交易所文档写 `交易所不支持公共 WS 行情`。 |
| `aftermath` | Aftermath Perpetuals SDK 有 `/perpetuals/ws/updates`，支持 `orderbook` deltas。 | 官方 SDK 文档未给固定 ms。 | 官方 SDK 文档未给固定 depth。 | SDK `openUpdatesWebsocketStream` 后 `subscribeOrderbook({ marketId })` / `unsubscribeOrderbook({ marketId })`。 | 本批未见稳定 sequence/checksum；应以 REST `getOrderbook`/market snapshot 重建。 | 当前 spec_only；列入 `public_ws_struct`，补 orderbook delta、无固定 ms/depth/sequence 风险字段。 |
| `alpaca` | Alpaca Crypto Data API 支持 crypto `orderbooks` WebSocket stream。 | 官方未给固定 ms；同一 WS message 可能带多个 data points。 | 官方未给固定 depth；REST latest orderbooks 返回当前 asks/bids。 | `wss://stream.data.alpaca.markets/v1beta3/crypto/us`；先 auth，再 `{"action":"subscribe","orderbooks":["BTC/USD"]}`。 | 官方未见 sequence/checksum；断线后用 REST latest orderbooks 重建。 | 当前 native；列入 `public_ws_struct`，补 auth+subscribe、无固定 ms/depth/checksum 和 REST snapshot fallback。 |
| `apollox_dex` | APX/ApolloX Futures 是 Binance Futures-style stream，支持 bookTicker、partial depth、diff depth。 | bookTicker real-time；partial depth 可 250ms/500ms/100ms；diff depth 可 250ms/500ms/100ms。 | partial depth 5/10/20；REST depth 5/10/20/50/100/500/1000。 | raw/combined streams；如 `<symbol>@depth20@100ms`、`<symbol>@depth@100ms`、`<symbol>@bookTicker`。 | diff depth 用 `U/u/pu` + REST `lastUpdateId` 重建；未见 checksum。 | 当前 spec_only；列入 `public_ws_struct`，补 100ms、5/10/20、bookTicker 和 `U/u/pu`。 |
| `arkham` | Arkham Exchange 官方资料说明有 RESTful HTTP API 和 Websocket API，且有 WebSocket 请求限速；本批未在可访问公开页中核到订单簿 channel 细项。 | 官方公开页未给固定 ms。 | 官方公开页未给固定 depth。 | 官方公开页未给稳定 orderbook subscribe payload。 | 官方公开页未给 sequence/checksum；必须取得完整 Exchange API reference 后再启用 runtime。 | 当前 spec_only；列入 `public_ws_struct`，补“官方 WS 存在但订单簿细项待 API reference”风险字段。 |
| `biconomy` | Biconomy 官方 API repo 支持 `depth.subscribe`，推送 `depth.update`。 | 官方未给固定 ms。 | depth 支持 5/10/15/20/30/50/100；precision 支持多档。 | JSON-RPC `{"method":"depth.subscribe","params":["BTC_USDT",50,"0.01"],"id":...}`。 | `depth.update` 带 `is_full` 区分全量/增量；未见 sequence/checksum。 | 当前 spec/parser ready；列入 `public_ws_struct`，补 depth/precision、full/incremental 和无 sequence/checksum。 |
| `bigone` | BigONE Spot WS 支持 `subscribeMarketDepthRequest`，先 snapshot 后 update。 | 官方未给固定 ms；支持 JSON 或 protobuf，protobuf 官方建议用于高频交易。 | 官方未给固定 depth 参数；market depth model 是 price level 聚合。 | `wss://api.big.one/ws/v2`；`subscribeMarketDepthRequest`。 | `changeId` 和 `prevId` 连续；断档需重建 snapshot；未见 checksum。 | 当前 parser_only；列入 `public_ws_struct`，补 MarketDepth、snapshot/update、changeId/prevId 和 protobuf 选项。 |
| `bitbns` | 旧官方 Python repo 已废弃且说明该 repo 不覆盖 WebSockets；示例仅出现 `getOrderBookSocket`，未给稳定 WS endpoint/interval/depth/sequence。 | 官方稳定资料未给固定 ms。 | 官方稳定资料未给固定 depth。 | 稳定官方 payload 不足；本地 URL 只能保持 spec-only。 | 未见 sequence/checksum；应以 REST orderbook 重建。 | 当前 spec_only；列入 `public_ws_struct`，补“官方 SDK 旧资料/无稳定 WS 规格”风险字段，不提升 runtime。 |
| `bitrue` | Bitrue Spot 官方 WS 支持 `market_${symbol}_simple_depth_step0`。 | 官方未给 depth 固定推送 ms；server ping 15s。 | depth step0；官方未给固定 levels。 | `wss://ws.bitrue.com/market/ws`；`event=sub`，`channel=market_btcusdt_simple_depth_step0`。 | gzip payload；未见 sequence/checksum；断线后 REST depth 重建。 | 当前 parser_only；列入 `public_ws_struct`，补 channel、gzip、ping 15s、无固定 ms/depth/checksum。 |
| `bittrade` | BitTrade public WS 支持 depth 与 BBO。 | BBO/depth 为变化推送；官方未给固定 ms。 | depth type 为 step0-step5；BBO 为 L1。 | `wss://api-cloud.bittrade.co.jp/ws`；`sub=market.<symbol>.depth.<type>` 或 `market.<symbol>.bbo`。 | BBO 有 `seqId`；depth 文档未见 checksum；生产帧 gzip，需要 REST depth 重建。 | 当前 native；列入 `public_ws_struct`，补 depth/BBO channel、step0-step5、seqId 和 gzip。 |
| `bitunix` | Bitunix Futures public WS 支持 `depth_books`、`depth_book1`、`depth_book5`、`depth_book15`；Spot WS 是 request-response API。 | Futures depth 官方未给固定 ms；项目已有 15s client ping heartbeat 证据。 | Futures 1/5/15 或 changed book；Spot REST depth 另行 snapshot。 | Futures `wss://fapi.bitunix.com/public/` JSON subscribe；Spot `wss://openapi.bitunix.com:443/ws-api/v1` request-response。 | 未见 sequence/checksum；断线后 REST depth 重建。 | 当前 spec/parser ready；列入 `public_ws_struct`，补 futures depth channel、1/5/15 档和无 sequence/checksum。 |
| `blockchaincom` | Blockchain.com Exchange WS 支持 `l2` 和 `l3` order book channels。 | 官方未给固定 ms。 | `l2` 聚合订单簿；`l3` 单订单级别全量订单簿更新。 | `wss://ws.blockchain.info/mercury-gateway/v1/ws`；`{"action":"subscribe","channel":"l2/l3","symbol":"BTC-USD"}`。 | 消息带 `seqnum`；snapshot 后 apply updates；断档需重连并 REST/WS snapshot 重建；未见 checksum。 | 当前 spec_only；列入 `public_ws_struct`，补 l2/l3、seqnum、snapshot/update 和 message limit。 |

## 高速盘口判断

| adapter | 行情等级 | 原因 |
| --- | --- | --- |
| `apollox_dex` | `100ms + BBO 候选` | Binance-style partial/diff depth 可 100ms，bookTicker real-time，`U/u/pu` 重建规则清楚。 |
| `bittrade` | `L1/BBO 候选` | 官方有 `market.<symbol>.bbo`，BBO 带 `seqId`，但未给固定 ms。 |
| `bitunix` | `1/5/15 档候选` | Futures WS 明确 `book1/book5/book15`，但未给固定 ms，也未见 checksum。 |
| `blockchaincom` | `L2/L3 深度候选` | 官方有 L2 聚合和 L3 单订单级别 WS，带 `seqnum`，但未给固定 ms。 |
| `alpaca`, `aftermath`, `arkham`, `biconomy`, `bigone`, `bitbns`, `bitrue` | `实时但无固定毫秒` | 官方或官方 SDK 支持 orderbook WS/stream，但未给固定推流毫秒；实现时必须做 REST snapshot 兜底。 |
| `bitkan` | `交易所不支持公共 WS 行情` | 当前未找到稳定官方 API/WS 文档，不应接入 unverified runtime。 |

## 官方资料

| 交易所/profile | 官方来源 |
| --- | --- |
| BitKan | <https://www.bitkan.com/>、<https://coinmarketcap.com/exchanges/bitkan/> |
| Aftermath | <https://docs.aftermath.finance/for-developers/typescript-sdk/products/perpetuals/perpetuals> |
| Alpaca | <https://docs.alpaca.markets/us/v1.1/docs/real-time-crypto-pricing-data>、<https://docs.alpaca.markets/us/v1.1/docs/crypto-trading> |
| APX/ApolloX | <https://apollox-finance.gitbook.io/apollox-finance/api/api-documentation>、<https://raw.githubusercontent.com/apollox-finance/apollox-finance-api-docs/master/apollox-finance-api.md> |
| Arkham | <https://arkm.com/docs>、<https://arkm.com/limits-api> |
| Biconomy | <https://github.com/BiconomyOfficial/apidocs> |
| BigONE | <https://open.bigone.com/docs/spot/introduction>、<https://open.bigone.com/docs/spot/pusher> |
| Bitbns | <https://github.com/bitbns-official/python-bitbns-api> |
| Bitrue | <https://github.com/Bitrue-exchange/Spot-official-api-docs> |
| BitTrade | <https://api-doc.bittrade.co.jp/> |
| Bitunix | <https://www.bitunix.com/api-docs/futures/websocket/public/depth%20channel.html>、<https://www.bitunix.com/api-docs/spots/en_us/ws/> |
| Blockchain.com Exchange | <https://exchange.blockchain.com/api>、<https://api.blockchain.info/v3/> |

## 下一步实现建议

1. 优先补 `apollox_dex`，它有明确 100ms depth、bookTicker 和 `U/u/pu` 重建规则。
2. 再补 `blockchaincom`、`bigone`、`bittrade`，它们有 sequence 或 L2/L3/BBO 能力，适合套利行情但要补 snapshot rebuild。
3. `bitunix` futures depth 可补 1/5/15 档，但必须把“官方未给固定 ms/无 checksum”写进 mapping。
4. `biconomy`、`bitrue`、`alpaca`、`aftermath` 适合补结构化字段，不应标成 10ms/20ms 高速盘口。
5. `bitkan` 当前按 `交易所不支持公共 WS 行情` 处理，避免把未验证 payload helper 推到实盘。

# WebSocket 官方核验 P2 区域现货交易所

状态日期：2026-06-08

本文件核验一批区域现货交易所的公共订单簿 WebSocket。重点是跨所套利需要的订阅方式、推流间隔、档位、sequence/checksum、snapshot 重建风险。这里是官方能力核验，不代表当前项目已经完整实现；项目当前状态仍以 [交易所功能盘点矩阵](交易所功能盘点矩阵.md) 为准。

## 核验结论

| 交易所 | 产品线 | 官方订单簿 WS 结论 | 推流间隔 | 档位 | 订阅方式 | sequence/checksum | 项目下一步 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| bitbank | Spot | 支持 `depth_diff_{pair}` 和 `depth_whole_{pair}`。 | 官方未给固定 ms。 | `depth_whole` 正常模式 asks/bids 各 200 档；异常竞价模式围绕估算价最多 400 条。 | Socket.IO 4.x，`join-room` 订阅房间。 | `depth_diff.s` 与 `depth_whole.sequenceId` 单调递增但不保证连续；必须 buffer diff，收到 whole 后按 `s > sequenceId` 回放。 | 当前 native 但缺结构化字段，补 channel、200 档、无固定 ms、非连续 sequence 和 whole/diff 重建。 |
| Bithumb | Spot | 支持 `orderbook` Public WS，支持 snapshot/realtime 开关。 | 官方未给固定 ms。 | 返回 `orderbook_units`；官方有 `level` 聚合单位，不等同固定深度档位。 | JSON 数组，包含 ticket/type/format；Public URL `wss://ws-api.bithumb.com/websocket/v1`。 | 消息有 microseconds timestamp；未见 sequence/checksum。 | 当前 native 但缺 channel/interval/depth，补 orderbook、snapshot/realtime、无 fixed ms 和无 sequence 风险。 |
| BitoPro | Spot | 支持 order book stream，全量订单簿更新。 | 订单簿有更新时每秒推全量数据。 | 默认 5 档；可选 1/5/10/20/30/50。 | URL path：`wss://stream.bitopro.com:443/ws/v1/pub/order-books/{PAIR}:{limit}`，多 pair 用 query `pairs=`。 | 有 `eventID` 和 timestamp，未见可用于 book 连续性的 sequence/checksum。 | 当前 native 但缺 interval/depth，补 1s、1/5/10/20/30/50 档和 snapshot-only 风险。 |
| Bitso | Spot | 支持 `orders` top20 和 `diff-orders` 全量 order mutation。 | 官方未给固定 ms。 | `orders` 维护 top 20 asks/bids；`diff-orders` 可维护全量 book。 | JSON subscribe：`{action:"subscribe", book:"btc_mxn", type:"diff-orders"}`。 | `diff-orders.sequence` 是逐条递增整数；跳号表示丢消息，需要 REST order_book 重建。 | 当前 payload helper 但缺 channel/interval/depth，补 `orders`/`diff-orders`、top20、sequence replay。 |
| BTC Markets | Spot | WebSocket v2 支持 `tick`、`trade`、`orderbook`、private events。 | 官方未给固定 orderbook ms；heartbeat 5s。 | `orderbook` 事件最多 50 bids 和 50 asks。 | JSON subscribe：`messageType=subscribe`、`channels=["orderbook"]`、`marketIds=["BTC-AUD"]`。 | `orderbook` 事件未见 sequence/checksum；公共 book 应作为最新状态快照处理。 | 当前 spec_only，补 50 档、heartbeat、无 sequence 风险；最新入口 `docs.btcmarkets.net` 受 Cloudflare，旧官方 GitHub Wiki 作为 WS v2 细项证据。 |
| Coincheck | Spot | 支持 `[pair]-orderbook` Public WS，推订单簿差分。 | Public WS 数据在发生交易时约 0.1s 推送；trades 也写 0.1s。 | 官方未给固定档位参数；消息含 bids/asks 数组。 | JSON subscribe：`{"type":"subscribe","channel":"btc_jpy-orderbook"}`。 | 有 `last_update_at`，未见 sequence/checksum。 | 当前 native 但缺 interval/depth，补 0.1s、无固定档位和无 sequence 风险，定期 REST snapshot 兜底。 |
| Coinone | Spot | 支持 `ORDERBOOK` Public WS。首次订阅推最后订单簿一次，之后有变更才推。 | 官方未给固定 ms。 | 官方未给可选 depth 参数；示例含 16 档左右。 | JSON subscribe：`request_type=SUBSCRIBE`、`channel=ORDERBOOK`、topic 包含 quote/target。 | 有 order book `id`，值越大越新；未见 checksum。 | 当前 spec_only，补 ORDERBOOK topic、首包 snapshot、event-driven interval、id 边界。 |
| Independent Reserve | Spot | 官方 WebSockets 支持 `orderbook-{cryptocurrency}` 和 `ticker-{cryptocurrency}`，orderbook 推 NewOrder/OrderChanged/OrderCanceled。 | 官方写 realtime，未给固定 ms；heartbeat 当前 60s，且说明可能变化。 | 按订单事件维护 book，官方未给固定 depth 参数。 | 连接 query `?subscribe=orderbook-xbt` 或连接后发送 `Subscribe` 消息。 | 每个 channel/currency 有递增 `Nonce`，严格 +1；跳号表示丢事件，回退 REST/orderbook 重建。 | 当前 spec_only，补 channel、Nonce、heartbeat、REST 快照重建；不要当成固定档位 snapshot feed。 |
| Luno | Spot | 支持 market stream，先推当前订单簿，再尽快推 updates。 | 官方写 as quickly as possible，未给固定 ms。 | 初始全量订单簿按 order id；无固定 depth 参数。 | `wss://ws.luno.com/api/1/stream/:pair`，连接后发送 API key credentials。 | 每条 update 有严格递增 sequence；错序必须关闭并重连重新初始化。 | 当前 native 但缺细项；补“需要 API key 的 market stream”、sequence、全量初始 book、重连重建。 |
| Upbit | Spot | 支持 `orderbook` Public WS，snapshot/realtime 开关。 | 官方未给固定 ms。 | orderbook units 可按 `{code}.{count}` 指定 1/5/15/30；不支持则默认 30。 | JSON 数组，ticket + type object；区域化 URL 如 `wss://sg-api.upbit.com/websocket/v1`。 | 消息有 timestamp 和 stream_type，未见 sequence/checksum。 | 当前 native 但缺 channel/interval/depth，补 1/5/15/30、区域 URL、无 fixed ms 和无 checksum 风险。 |

## 套利优先级判断

| adapter | 行情等级 | 原因 |
| --- | --- | --- |
| `coincheck` | `百毫秒盘口候选` | 官方写约 0.1s，但没有 sequence/checksum，需 REST snapshot 兜底。 |
| `bitopro` | `慢速/未知` | 官方是更新时每秒全量 order book，不适合极速套利第一批。 |
| `bitbank`, `bitso`, `independentreserve`, `luno` | `实时但无固定毫秒` | sequence/Nonce/replay 规则较好，但官方未给固定推流间隔。 |
| `bithumb`, `btcmarkets`, `coinone`, `upbit` | `实时但无固定毫秒` | 官方支持 orderbook WS，但缺固定 ms；部分还缺 checksum/sequence。 |

## 官方来源

| 交易所 | 官方来源 |
| --- | --- |
| bitbank | <https://github.com/bitbankinc/bitbank-api-docs>、<https://raw.githubusercontent.com/bitbankinc/bitbank-api-docs/master/public-stream.md> |
| Bithumb | <https://apidocs.bithumb.com/llms.txt>、<https://apidocs.bithumb.com/reference/%ED%98%B8%EA%B0%80-orderbook.md>、<https://apidocs.bithumb.com/reference/%EC%9A%94%EC%B2%AD-%ED%8F%AC%EB%A7%B7.md> |
| BitoPro | <https://www.bitopro.com/ns/api-docs>、<https://raw.githubusercontent.com/bitoex/bitopro-official-api-docs/master/ws/public/order_book_stream.md> |
| Bitso | <https://docs.bitso.com/bitso-api/docs/general>、<https://docs.bitso.com/bitso-api/docs/diff-orders-channel> |
| BTC Markets | <https://docs.btcmarkets.net/>、<https://github.com/BTCMarkets/API/wiki/WebSocket-v2> |
| Coincheck | <https://coincheck.com/documents/exchange/api> |
| Coinone | <https://docs.coinone.co.kr/llms.txt>、<https://docs.coinone.co.kr/reference/public-websocket-1.md>、<https://docs.coinone.co.kr/reference/public-websocket-orderbook.md> |
| Independent Reserve | <https://www.independentreserve.com/sg/features/api>、<https://raw.githubusercontent.com/independentreserve/websockets/master/orderbook-ticker.md> |
| Luno | <https://www.luno.com/en/developers/api> |
| Upbit | <https://global-docs.upbit.com/reference/websocket-orderbook> |

## 下一步实现建议

1. 先补 `bitbank`、`bitso`、`independentreserve`、`luno` 的 sequence/Nonce 重建规则，这些能明确判断丢包。
2. `coincheck` 有 0.1s 盘口但缺 sequence/checksum，应作为轻量行情源，策略侧要做 stale book 和 REST snapshot 兜底。
3. `bithumb`、`coinone`、`upbit` 官方未给固定 ms，不能写成 10ms/20ms 候选；先落地订阅 payload、档位/聚合参数和无 checksum 风险。
4. `bitopro` 每秒全量 book 更适合监控或低频扫描，不放入极速套利第一批。

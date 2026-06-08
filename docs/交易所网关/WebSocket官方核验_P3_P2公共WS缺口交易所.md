# WebSocket 官方核验 P3 P2 公共 WS 缺口交易所

状态日期：2026-06-08

本批处理 [剩余官方核验队列](剩余官方核验队列.md) 中 P2 `public_ws_official_detail_check` 的长尾交易所。结论只针对公共订单簿行情 WS：官方支持但项目未接的写 `项目未实现公共 WS 行情`；官方当前资料没有稳定公共订单簿 WS 的写 `交易所不支持公共 WS 行情`；仍未拿到稳定官方规格的保留在剩余队列。

## 核验结论

| adapter | 官方公共 WS 结论 | 推流间隔 | 订单簿档位 | 订阅方式 | sequence/checksum 与重建 | 当前项目处理 |
| --- | --- | --- | --- | --- | --- | --- |
| `ascendex` | 官方支持 Spot 和 Futures 公共 WS 订单簿；`depth:<symbol>` L2，`bbo:<symbol>` L1。 | `depth` 默认 300ms throttle；BBO 有变化即推；snapshot 响应通常 1000-2000ms，只用于初始化。 | `depth-snapshot` 最多 500 档，另有 `depth-snapshot-top100`。 | `wss://ascendex.com/api/pro/v1/stream`、`/api/pro/v2/stream`；`{"op":"sub","ch":"depth:BTC/USDT"}`。 | `seqnum` 必须逐条 +1；断档后重新订阅并请求 snapshot。 | 项目已有 payload/parser 证据，但缺结构化 interval/depth/resync 字段；列入 `public_ws_struct`。 |
| `cex` | 官方 Spot Trading WS 支持 order book subscribe 和 increment。 | 官方未给固定 ms；按变更推送。 | 新 Spot WS 文档未给显式 depth 参数；旧 WS 有 depth 参数和 50 档 pair-room snapshot。 | `wss://trade.cex.io/api/spot/ws-public`；`order_book_subscribe`，后续 `order_book_increment`。 | 首包 snapshot 带 `seqId`，增量也带 `seqId`；需按序应用。 | 项目只有 payload/auth/pong helper，无公共 WS runtime；列入 `public_ws`。 |
| `coinsph` | 官方支持 Binance-like 公共 depth、partial depth、bookTicker。 | bookTicker real-time；diff depth 1000ms 或 `@100ms`；partial depth 5/10/20 档可 100ms，200 档 1000ms。 | partial depth 支持 5、10、20、200；diff depth 用 REST snapshot 限制决定可见深度。 | `wss://wsapi.pro.coins.ph/openapi/quote/stream?streams=btcphp@depth@100ms`，也支持 `@bookTicker`。 | `U/u` 更新 ID；先取 REST snapshot，再丢弃 `u <= lastUpdateId`，首个事件覆盖 `lastUpdateId+1`，后续 `U = prev.u + 1`。 | 项目未实现公共 WS runtime；列入 `public_ws`。 |
| `latoken` | 官方 STOMP WS 支持公共 book channel `/v1/book/{base}/{quote}`。 | 官方未给固定 ms。 | 官方未给固定档位；payload 是 price-level change。 | `wss://api.latoken.com/stomp`；STOMP subscribe 到 `/v1/book/{base}/{quote}`。 | STOMP body 含 `nonce` 和 `timestamp`；官方示例按 nonce 连续性检测，错位后重连订阅。 | 项目为 spec-only，runtime 未启用；列入 `public_ws`。 |
| `p2b` | 官方支持公共 WSS depth。 | `depth.update` 中 full flag `true` 每 60s，increment flag `false` 每 1s。 | `depth.subscribe` 的 limit 为 1-100。 | `wss://apiws.p2pb2b.com/`；`{"method":"depth.subscribe","params":["BTC_USDT",10,"0"],"id":1}`。 | 官方未给 sequence/checksum；需按 full/increment flag 设计 REST 或 WS 全量兜底。 | 项目只记录 unverified WSS，runtime 未启用；列入 `public_ws`。 |
| `paymium` | 官方支持 public socket.io stream，包含 `bids`/`asks` price-level changes。 | 官方未给固定 ms；有新数据时触发 `stream`。 | 官方未给固定档位；`bids`/`asks` 是变更价位，数量为 0 表示删除。 | socket.io v1.3，连接 `https://paymium.com/public`，path `/ws/socket.io`，监听 `stream`。 | 官方未给 sequence/checksum；需要 REST depth 初始化和断线全量重建。 | 项目未实现 socket.io runtime；列入 `public_ws`。 |
| `bit2c` | 当前官方 API 只见 REST public/private，未见稳定公共订单簿 WS。 | 交易所不支持公共 WS 行情。 | REST full orderbook 文档写 cache 1s，top book 为 real-time REST。 | 无官方 WSS 订阅规格。 | 无 WS sequence/checksum。 | 写 `交易所不支持公共 WS 行情`；REST snapshot/polling fallback。 |
| `bitteam` | 当前官方 docs/OpenAPI 只给 REST/CCXT-style endpoints，未给 WSS endpoint 和订单簿订阅规格。 | 交易所不支持公共 WS 行情。 | REST orderbook endpoint。 | 无稳定官方 WSS 订阅规格。 | 无 WS sequence/checksum。 | 写 `交易所不支持公共 WS 行情`；REST snapshot/polling fallback。 |
| `btcbox` | 当前官方 API 资料是 HTTP Public/Private API，未见稳定公共订单簿 WS。 | 交易所不支持公共 WS 行情。 | REST `depth`。 | 无官方 WSS 订阅规格。 | 无 WS sequence/checksum。 | 写 `交易所不支持公共 WS 行情`；REST snapshot/polling fallback。 |
| `cod3x` | Cod3x 当前官方资料是 perps trading terminal/routing layer，未建立 Cod3x-native 订单簿 WS API；下游 venue 走各自 adapter。 | 交易所不支持 Cod3x-native 公共 WS 行情。 | 不适用。 | 不适用。 | 不适用。 | 写 `交易所不支持公共 WS 行情`（Cod3x-native profile 口径）。 |
| `coinspot` | 官方 API/V2 API 是 public/private REST，未见公共订单簿 WS。 | 交易所不支持公共 WS 行情。 | REST open orders/orderbook。 | 无官方 WSS 订阅规格。 | 无 WS sequence/checksum。 | 写 `交易所不支持公共 WS 行情`；REST snapshot/polling fallback。 |
| `derive_chain_perps` | Derive Exchange 官方有 WS orderbook，但本 profile 只记录 Derive Chain settlement 边界，不承接 Derive Exchange runtime；runtime 归 `derive` adapter。 | 本 profile 交易所不支持公共 WS runtime。 | 不适用。 | 不适用。 | 不适用。 | 写 `交易所不支持公共 WS 行情`（profile 口径），避免重复实现。 |
| `equation` | 官方资料是 Arbitrum perpetual protocol、REST/Graph/EVM 示例；未给稳定 exchange-gateway 公共订单簿 WS。 | 交易所不支持公共 WS 行情。 | 不适用。 | 不适用。 | 不适用。 | 写 `交易所不支持公共 WS 行情`（当前 audit-only adapter 口径）。 |
| `indodax` | 官方 PDF/GitHub API 覆盖 Public API 和 TAPI REST，未见公共订单簿 WS。 | 交易所不支持公共 WS 行情。 | REST `GET /api/{pair}/depth`。 | 无官方 WSS 订阅规格。 | 无 WS sequence/checksum。 | 写 `交易所不支持公共 WS 行情`；REST snapshot/polling fallback。 |
| `yobit` | 官方 API v2/v3/TAPI/Defi API 覆盖 REST market data 和交易接口，未见公共订单簿 WS。 | 交易所不支持公共 WS 行情。 | REST `depth`。 | 无官方 WSS 订阅规格。 | 无 WS sequence/checksum。 | 写 `交易所不支持公共 WS 行情`；REST snapshot/polling fallback。 |
| `zebpay` | 当前官方 docs 覆盖 Spot/Futures REST market/book/orderBook，未见公共订单簿 WS stream。 | 交易所不支持公共 WS 行情。 | Spot REST book 15 档、book_long 50 档；Futures REST orderBook。 | 无官方 WSS 订阅规格。 | 无 WS sequence/checksum。 | 写 `交易所不支持公共 WS 行情`；REST snapshot/polling fallback。 |
| `zeta_markets` | 官方文档声明 Zeta Markets 已于 2025-05 停止运营；无当前公共 WS runtime。 | 交易所不支持当前公共 WS runtime。 | 不适用。 | 不适用。 | 不适用。 | 写 `交易所不支持当前公共 WS runtime`；只保留 legacy REST fixture。 |
| `bitkan` | 未找到稳定官方 OpenAPI/WS 规格；本地 adapter 也标为 placeholder/unverified。 | 暂不下结论。 | 暂不下结论。 | 暂不下结论。 | 暂不下结论。 | 保留在剩余核验队列，不写 `交易所不支持`。 |

## 高速盘口判断

- 本批没有官方明确 10ms/20ms L1/BBO 的交易所。
- `coinsph` 是本批最接近套利可用的低延迟盘口：`bookTicker` real-time，5/10/20 档 partial depth 可 `@100ms`。
- `ascendex` 默认 L2 `depth` 是 300ms，适合作为补充盘口；BBO 是变更推送但官方未给固定 ms。
- `p2b` 是 1s 增量、60s 全量；`paymium`/`cex`/`latoken` 没有固定 ms，需要实测延迟后再决定套利优先级。

## 官方资料

- AscendEX API Reference：https://ascendex.github.io/ascendex-pro-api/
- CEX.IO Spot Trading API：https://trade.cex.io/docs/
- Coins.ph WebSocket Streams：https://docs.coins.ph/web-socket-streams/
- LATOKEN WebSocket API：https://api.latoken.com/doc/ws/
- P2B WebSocket API Documentation：https://p2pb2b.zendesk.com/hc/en-us/articles/10223158843805-WebSocket-API-Documentation
- Paymium API：https://paymium.github.io/api-documentation/
- Bit2C API：https://bit2c.co.il/home/api?language=en-US
- BIT.TEAM docs/API：https://bit.team/docs、https://bit.team/trade/api/documentation.json
- BTCBOX API：https://blog.btcbox.jp/en/archives/9868
- CoinSpot API：https://www.coinspot.com.au/api
- Indodax API PDF：https://indodax.com/downloads/INDODAXCOM-API-DOCUMENTATION.pdf
- YoBit API：https://yobit.net/en/api/
- ZebPay docs：https://docs.zebpay.com/
- Zeta Markets docs：https://docs.zeta.markets/
- Derive docs：https://docs.derive.xyz/
- Cod3x docs：https://docs.cod3x.org/
- Equation docs：https://docs.equation.org/

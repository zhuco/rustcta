# WebSocket 官方核验 P0 第二批

状态日期：2026-06-08

本文件继续记录跨所套利优先级最高的公共订单簿 WebSocket 官方资料。这里是官方能力核验，不代表项目已经实现；项目当前实现状态仍以 [交易所功能盘点矩阵](交易所功能盘点矩阵.md) 为准。

## 核验结论

| 交易所 | 产品线 | 官方订单簿 WS 结论 | 推流间隔 | 档位 | 订阅方式 | sequence/checksum | 项目下一步 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| HTX/Huobi | Spot | 官方支持普通深度、MBP 增量、MBP refresh、BBO。 | 普通 `depth` 为 1s；`mbp.refresh` 约 100ms；BBO tick-by-tick；MBP 增量为事件驱动。 | MBP 增量 5/20/150/400；MBP refresh 5/10/20；BBO 1 档。 | JSON `sub`：`market.$symbol.depth.$type`、`market.$symbol.mbp.$levels`、`market.$symbol.mbp.refresh.$levels`、`market.$symbol.bbo`。 | MBP 增量有 `seqNum`/`prevSeqNum`；BBO 有 `seqId`。 | `htx`/`huobi` 项目已有 WS 声明和 parser 证据，但 mapping 缺结构化推流间隔、档位、sequence 字段。 |
| HTX/Huobi | USDT-M Swap/Futures | 官方支持普通深度、增量高频深度、BBO。 | 普通深度 tick 时间戳按 100ms 生成；高频增量 orderbook event 每 30ms 检查，有变化才推。 | 普通深度有 20/150 档聚合精度族；高频增量支持 20 或 150 个非合并档位。 | JSON `sub`：`market.$contract_code.depth.$type`、`market.$contract_code.depth.size_${size}.high_freq`、`market.$contract_code.bbo`。 | 高频增量有 `version`，首次或重连先推 snapshot，之后 update；必须维护本地订单簿。 | 补 `market.*.depth.size_20.high_freq` 和 `market.*.bbo` 的结构化 mapping、fixture 和重建规则。 |
| LBank | Spot | 官方支持 `depth` WebSocket 行情，同时支持一次性 request。 | 官方写“有更新后尽快推送”，未给固定 ms。 | 10/50/100。 | JSON `subscribe`：`{"action":"subscribe","subscribe":"depth","depth":"100","pair":"eth_btc"}`。 | 官方深度示例没有 sequence/checksum；需要 REST snapshot 兜底。 | 项目矩阵已识别 public WS native，但缺官方推流间隔和 sequence 缺失说明；mapping 应写明 `官方未给固定 ms`。 |
| CoinW | Spot | 官方支持 20 档 snapshot、100 档 snapshot、增量 order book。 | 官方写 real-time；未给固定 ms；订阅频率限制 none。 | Method 1 默认 20；Method 2 snapshot 100；增量不固定档位，推变化。 | Method 1 `spot/level2_20:BTC-USDT`；Method 2 JSON `type=depth_snapshot` 或 `type=depth`。 | snapshot 有 `seq`；增量有 `startSeq`/`endSeq`，每个价位也带 sequence。 | 项目矩阵为 parser_only，但 mapping 缺结构化 channel、interval、sequence；应优先补 Spot `depth_snapshot`/`depth`。 |
| CoinW | Futures | 官方支持合约订单簿 WebSocket。 | 官方写实时更新，未给固定 ms；订阅限频 10 次/2s/IP。 | 默认返回 100 个买单和 100 个卖单。 | JSON `sub`：`{"biz":"futures","pairCode":"BTC","type":"depth"}`，URL `wss://ws.futurescw.com/perpum`。 | 示例含 `ts`，未见明确 sequence/checksum 字段；需用 REST snapshot 校验。 | 项目矩阵为 parser_only，需补 Futures depth 100 档 fixture 和无 sequence 风险说明。 |
| Binance.US | Spot | 官方支持 bookTicker、partial depth 和 diff depth。 | bookTicker real-time；partial/diff depth 1000ms 或 100ms。 | partial depth 5/10/20；REST snapshot 可到 1000。 | URL stream 或 JSON subscribe：`<symbol>@depth<levels>@100ms`、`<symbol>@depth@100ms`。 | diff depth 使用 `U`/`u`，本地 book 需 REST snapshot + buffered events。 | 当前 `binanceus` 公共 WS 为 unsupported，应按 Binance Spot 同类实现补 stream mapping 和 parser。 |
| Binance | USD-M Futures | 官方支持 partial depth 和 diff depth。 | 250ms、500ms 或 100ms。 | partial depth 5/10/20；diff depth 为增量。 | URL stream：`<symbol>@depth<levels>@100ms`、`<symbol>@depth@100ms`。 | 使用 `U`/`u`/`pu`，本地 book 需按 futures 规则重建。 | 当前项目没有独立 USD-M adapter；需决定新建 `binanceusdm` 还是纳入 Binance 合约 adapter。 |
| Binance COIN-M | COIN-M Futures | 官方支持 partial depth 和 diff depth。 | 250ms、500ms 或 100ms。 | partial depth 5/10/20；diff depth 为增量。 | URL stream：`<symbol>@depth<levels>@100ms`、`<symbol>@depth@100ms`。 | 使用 `U`/`u`/`pu`，事件含 symbol 和 pair。 | 当前 `binancecoinm` 公共 WS 为 unsupported，应补 COIN-M depth stream parser 和 snapshot 重建。 |

## 官方来源

| 交易所 | 官方来源 |
| --- | --- |
| HTX/Huobi Spot | <https://huobiapi.github.io/docs/spot/v1/en/> |
| HTX/Huobi USDT-M | <https://huobiapi.github.io/docs/usdt_swap/v1/en/> |
| LBank Spot | <https://www.lbank.com/docs/index.html> |
| CoinW Spot snapshot | <https://www.coinw.com/api-doc/en/spot-trading/market/subscribe-order-book> |
| CoinW Spot incremental | <https://www.coinw.com/api-doc/en/spot-trading/market/subscribe-incremental-order-book> |
| CoinW Futures | <https://www.coinw.com/api-doc/futures-trading/market/subscribe-order-book> |
| Binance.US Spot | <https://github.com/binance-us/binance-us-api-docs/blob/master/web-socket-streams.md> |
| Binance USD-M Futures partial | <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams> |
| Binance USD-M Futures diff | <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams> |
| Binance COIN-M Futures partial | <https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams> |
| Binance COIN-M Futures diff | <https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams> |

## 下一步实现建议

1. 先补 `binanceus`、`binancecoinm`，因为官方能力清楚但当前项目公共 WS 明确 unsupported。
2. 再补 `htx`/`huobi` 的结构化 orderbook 字段：Spot 用 `mbp.refresh.20` 或 `mbp.20`，合约优先 `depth.size_20.high_freq`。
3. `lbank` 已经有 public WS native 证据，重点补“官方未给固定推流 ms”和 sequence 缺失说明，不要误写成已知 10ms/100ms。
4. `coinw` 已有 parser/session 证据，优先补 Spot `depth_snapshot` 和 Futures `depth` 的档位、sequence/无 sequence、REST resync 规则。
5. Binance USD-M Futures 官方能力已确认，但项目没有独立 adapter，需要先决定 adapter 边界。

# WebSocket 官方核验 P0 第一批

状态日期：2026-06-08

本文件记录第一批对跨所套利最关键的公共订单簿 WebSocket 官方核验结果。这里是官方资料核验，不等于项目已经实现；项目当前实现状态仍以 [交易所功能盘点矩阵](交易所功能盘点矩阵.md) 为准。

## 核验结论

| 交易所 | 产品线 | 官方订单簿 WS 结论 | 推流间隔 | 档位 | 订阅方式 | sequence/checksum | 项目下一步 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Binance | Spot | 官方支持 partial depth 和 diff depth；partial depth 可带 `@100ms`。 | partial depth 1000ms 或 100ms | 5/10/20；REST snapshot 可到 5000 | URL stream：`<symbol>@depth<levels>` 或 `<symbol>@depth<levels>@100ms` | diff depth 使用 update id；本地 book 需 REST snapshot + buffered events | 当前项目公共 WS 为 unsupported，应补 Spot depth/bookTicker stream mapping、parser 和 snapshot 重建。 |
| OKX | Spot/Swap/Futures/Option | 官方支持 `bbo-tbt`、`books5`、`books`、`books-l2-tbt`、`books50-l2-tbt`。 | `bbo-tbt` 10ms；`books5` 100ms；`books` 100ms；`books-l2-tbt`/`books50-l2-tbt` 10ms | BBO、5、50、full/incremental；部分 tbt channel 有 VIP/登录限制 | JSON subscribe channel | incremental channel 需要按序处理；需补 checksum/seq 字段细节 | 当前项目公共 WS 为 unsupported，应优先补 OKX books5、bbo-tbt，VIP 频道单独标限制。 |
| Bybit | Spot/Linear/Inverse/Option | 官方支持 `orderbook.{depth}.{symbol}`，snapshot + delta。 | Spot/linear/inverse：1=10ms、50=20ms、200=100ms、1000=200ms；option：25=20ms、100=100ms | 1/50/200/1000；option 25/100 | JSON subscribe/topic | 有 `seq` cross sequence；L1 snapshot-only，其他档有 snapshot/delta | 当前项目已有 spec_only orderbook.50，应补 1/50/200/1000 间隔字段和 seq/snapshot 规则。 |
| Bitget | Spot | 官方支持 `books`、`books1`、`books5`、`books15`。 | `books`/`books5`/`books15` 默认 200ms；`books1` 默认 10ms | all、1、5、15 | JSON subscribe args | 有 `seq`，支持 CRC32 checksum，`books` 初始 snapshot 后 update | 当前项目公共 WS 为 unsupported，应补 Spot books1/books5/books15 和 checksum/parser。 |
| Gate.io | Spot | 官方支持 `spot.order_book_update`。 | 20ms 或 100ms；20ms 对应 20 档，100ms 对应 100 档 | 20/100 | JSON subscribe payload：`["BTC_USDT", "100ms"]` | 官方文档说明增量 order book update；需继续补 sequence/checksum 字段 | 当前项目公共 WS 为 unsupported，应补 spot.order_book_update 20ms/100ms。 |
| MEXC | Spot | 官方支持 aggregated depth、partial depth、bookTicker。 | aggregated depth 100ms 或 10ms；bookTicker 100ms 或 10ms | partial depth 5/10/20；aggregated depth 为增量；snapshot REST limit 1000 | JSON subscription params | 有 fromVersion/toVersion 规则；断档需要 REST snapshot 重新初始化 | 当前项目公共 WS 为 unsupported，应补 10ms depth/bookTicker 与 snapshot 重建。 |
| KuCoin | Spot/Futures | 官方新版 orderbook channel 支持 BBO、5、50、increment；2026-06-08 官方公告称 UTA 将在 2026-06-17 UTC 上线 `depth=increment@10ms` 500 档。 | BBO real time；5/50 为 100ms；increment real time；`increment@10ms` 计划 2026-06-17 上线 | 1/5/50/increment；未来 500 档 | JSON subscribe `channel=obu`，`depth=1/5/50/increment`；未来 `depth=increment@10ms` | 有 starting/ending sequence；increment 需要 REST snapshot + replay；未来 10ms 为 snapshot + incremental model | 当前项目公共 WS 已 native 但矩阵缺结构化 channel；先补现有 `obu`，再在 2026-06-17 后补 UTA 10ms 500 档迁移。 |

## 官方来源

| 交易所 | 官方来源 |
| --- | --- |
| Binance | <https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md> |
| OKX | <https://app.okx.com/docs-v5/trick_en/> |
| Bybit | <https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook> |
| Bitget | <https://www.bitget.com/api-doc/spot/websocket/public/Depth-Channel> |
| Gate.io | <https://www.gate.com/docs/developers/apiv4/ws/en/> |
| MEXC | <https://mexcdevelop.github.io/apidocs/spot_v3_en/> |
| KuCoin | <https://www.kucoin.com/docs-new/3470221w0> |
| KuCoin 2026-06-08 UTA 500 档公告 | <https://www.kucoin.com/announcement/en-api-upgrade-announcement-500-level-depth-feed-api-rate-limit> |

## 需要写入 endpoint mapping 的结构化字段

建议后续在每个 adapter 的 `endpoint_mapping.yaml` 里补类似字段：

```yaml
websocket:
  public:
    support: native
    orderbook:
      subscription_mode: json_subscribe
      channels:
        - name: books1
          levels: 1
          push_interval_ms: 10
          message_type: snapshot
        - name: books5
          levels: 5
          push_interval_ms: 200
          message_type: snapshot
      sequence_field: seq
      checksum: crc32
      resync: rest_snapshot_then_replay_delta
      source:
        url: https://www.bitget.com/api-doc/spot/websocket/public/Depth-Channel
        reviewed_at: 2026-06-08
```

## 实现优先级

1. `binance`、`okx`、`bitget`、`gateio`、`mexc` 当前项目公共 WS 是 unsupported，先补这些主流现货交易所。
2. `bybit` 当前有 spec_only channel，先补推流间隔、档位、sequence 规则，再决定是否接 runtime。
3. `kucoin` 当前项目 public WS 已 native，但缺结构化档位/间隔字段，应先补 mapping 文档和 fixture；`depth=increment@10ms` 是 2026-06-17 UTC 后的新能力，2026-06-17 前不要写成当前已可用。
4. 每个交易所至少补一个 L1/BBO 或 5 档快照通道，再补 full/incremental order book。

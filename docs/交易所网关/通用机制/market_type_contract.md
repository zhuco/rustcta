# Market Type Contract

状态日期：2026-06-08

本文档约束交易所网关、策略配置、endpoint mapping、symbol rule 和运行时事件里的 `market_type` 语义。目标不是强行统一交易所外部命名，而是避免把交易所官方接口里的 `futures` namespace 和项目内部的合约类型混成同一个判断依据。

交易所命名不能直接决定内部类型。很多交易所会把永续放在 `futures`、`contract`、`swap`、`derivatives` 这样的产品线或 API namespace 下；项目内部仍按 instrument 语义归一化：没有到期/交割的是 `Perpetual`，有到期/交割的是 `Futures`。

## 规范定义

| 概念 | 规范内部值 | 规范配置值 | 含义 |
| --- | --- | --- | --- |
| 现货 | `MarketType::Spot` | `spot` | 即期买卖，不带合约仓位。 |
| 保证金 | `MarketType::Margin` | `margin` | 保证金现货或借贷交易；不能替代永续。 |
| 永续合约 | `MarketType::Perpetual` | `perpetual` | 无到期日的 perpetual swap/perp/USDT swap 合约。 |
| 交割合约 | `MarketType::Futures` | `futures` | 有到期日或交割周期的 dated futures/delivery futures；不是永续别名。 |
| 期权 | `MarketType::Option` | `option` | 期权产品线。 |

内部事件、symbol rule、订单请求、行情订阅和风控 key 使用规范内部值。交易所官方 URL、channel、endpoint、参数名和 adapter transport 方法可以保留交易所自己的命名。

endpoint mapping 和 adapter 文档应同时表达两层含义：

- `market_type`: 项目内部归一化类型，例如 `perpetual` 或 `futures`。
- `exchange_product_type` / `exchange_namespace` / notes: 交易所官方产品线或 API namespace，例如 Binance `fapi`/USD-M Futures、OKX `SWAP`、Gate `futures`、Bybit `linear`。

## 余额查询作用域

`BalancesRequest.market_type` 是可选字段，但它不是新增一种市场类型：

- `Some(MarketType::Spot)`：查询交易所现货/经典现货分区余额。
- `Some(MarketType::Perpetual)`：查询交易所永续合约分区余额。
- `None`：查询账户级余额快照。支持统一账户、组合保证金或 UTA 的交易所应把它映射到官方账户级接口；不支持或账号未开通时，调用方可以回退到显式 `Spot` + `Perpetual` 查询。

不要新增 `MarketType::Unified`，也不要让调用方靠响应里的 `MarketType::Perpetual` 推断统一账户。统一账户是账户/资金作用域，不是交易品种。当前 `ExchangeBalance` DTO 还没有独立的 balance scope 字段，账户级响应里的 `market_type` 只能作为交易所主产品线锚点保留；调用方必须按请求的 `market_type=None` 或上层检查模式判断它是账户级余额。

当前控制台余额检查策略是：

- Binance、Bybit：按统一账户查询并展示为统一账户余额。
- Bitget、Gate.io：先用 `market_type=None` 探测 UTA/Unified Account；若账号不支持或未开通，则回退经典现货和永续分区。
- 其它交易所：默认显式查询 `Spot` 和 `Perpetual` 分区。

## 兼容别名

当前仓库有历史兼容路径会把部分字符串映射到永续：

- `strategies/unified-arbitrage` 的运行时配置应使用 `perpetual` 表达永续合约。
- `crates/rustcta-runtime-control/src/live_preflight/config.rs` 会把 `perpetual`、`perp`、`future`、`futures` 解析为 `MarketType::Perpetual`。
- `crates/rustcta-runtime-control/src/risk/disabled_registry.rs` 会把 `perpetual`、`perp`、`future`、`futures` 解析为 `MarketType::Perpetual`。
这些兼容入口只服务交易所外部命名过渡。不要只凭 `market_type: futures` 判断它是永续；必须结合 adapter 的 instrument 规则或交易所产品线声明。

注意：通用 serde 直接反序列化 `rustcta_types::MarketType` 或 `rustcta_strategy_sdk::MarketType` 时，`futures` 会落到 `MarketType::Futures`，不会自动等价于 `Perpetual`。只有上面列出的兼容解析器会做别名归一化。

## Adapter 约束

- 永续 adapter 的能力声明、symbol rule、`ExchangeSymbol`、订单请求、行情订阅和事件模型归一化为 `MarketType::Perpetual`。
- endpoint mapping 里的内部 `market_types` 应表达归一化类型。交易所把永续放在 `futures` namespace 下时，在 exchange-specific 字段或 notes 中声明，不用把内部类型改成 `Futures`。
- `futures_rest_base_url`、`send_signed_futures_get`、`futures.order_book_update`、CoinW `biz=futures` 这类名称可以保留，因为它们是交易所官方 API namespace 或本地方法名。它们不改变归一化后的 `MarketType`。
- 同一交易所同时支持永续和交割合约时，必须按 instrument 是否有到期/交割信息拆分为 `Perpetual` 和 `Futures`，不能用一个 `futures` 产品线同时覆盖两者。
- 如果交易所把永续市场叫 `futures`，adapter parser 要在 symbol rule 或 instrument metadata 阶段完成归一化，不能把外部产品线名透传成内部 `MarketType::Futures`。

## 策略与配置约束

- 跨所 USDT 永续套利配置应声明策略目标是永续。现阶段兼容 `market_type: futures`，但 admission 和 adapter capability gate 需要基于归一化后的 `MarketType::Perpetual` 判断。
- live preflight、disabled symbol/position registry、fee lookup、symbol registry、book cache 和 execution router 的 key 都应使用归一化后的 `MarketType::Perpetual`。
- `MarketType::Futures` 只保留给未来交割合约实现。引入交割合约前，需要补充到期日、交割资产、合约面值、funding/settlement 差异和订单路由约束。

## 迁移规则

1. 不做全仓库机械重命名。先确认每个交易所的 instrument 是否有到期/交割。
2. 业务含义是永续的路径，内部归一化结果必须是 `MarketType::Perpetual`；交易所 namespace 仍可写 `futures`。
3. 新增测试时，主路径断言归一化后的内部类型；兼容测试可以保留 `futures`，但测试名和断言要说明它来自外部命名或 legacy alias。
4. 新增 adapter 时，先从官方文档确认产品是否为永续、交割或两者都有，再写入 endpoint mapping。
5. 如果内部 `market_type` 使用 `futures`，PR 说明必须写明这是交割合约，不是交易所把永续叫作 futures。

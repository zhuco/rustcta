# 交易所网关 CCXT 对标与 30 个新增交易所扩展计划

状态日期：2026-06-07

本文档先按 CCXT 全量交易所和 unified API 做差距分析，再把新增交易所网关开发拆成 `任务 1` 到 `任务 30` 作为执行子集。30 个任务三批推进，每批 10 个交易所，方便多 AI 并行开发；当前仓库已经接入或已有旧路径的交易所不放入这 30 个新增目标。

## 范围

目标是为 `rustcta-exchange-gateway` 增加标准化交易所适配器，并尽量对齐 Binance 已实现的大部分接口能力。每个新增交易所的第一优先级是可复用、可测试、可降级的标准网关接口，而不是一次性追齐交易所所有小众功能。

新增适配器默认按以下产品线验收：

- Spot：现货行情、账户、费用、订单生命周期、成交、公开流、私有流或 REST reconciliation fallback。
- Linear perpetual：优先 USDT 本位；交易所只提供 USDC 稳定币本位时，按同一套 `linear_perp` 抽象实现并在能力中标注。
- 不支持的原生能力必须显式返回 `Unsupported`，不能静默降级成错误语义不清的失败。

## CCXT 全量对标缺口总览

本节回答“对标 CCXT 还缺什么”。数据口径为 npm `ccxt@4.5.56` 导出的 `ccxt.exchanges` 数组，而不是 npm tarball 中可能保留的 historical/abstract 源文件列表；状态日期同本文档，即 2026-06-07。该版本 `package.json` 描述为覆盖 “more than 100 exchanges”，实际导出的 `ccxt.exchanges` 为 111 个 exchange id。当前仓库 `crates/rustcta-exchange-gateway/src/adapters/` 有 37 个 adapter 目录；按 `gateio -> gate/gateio`、`coinbase -> coinbase/coinbaseadvanced/coinbaseinternational`、`binance -> binance/binanceusdm`、`hashkey_global -> hashkey` 等合并口径，约覆盖 32 个 CCXT id，缺 79 个 CCXT id；另有 9 个本仓库已有但 CCXT 4.5.56 未导出的长尾 adapter：`biconomy`、`bitkan`、`bitunix`、`coindcx`、`coinstore`、`cointr`、`coinw`、`orangex`、`tapbit`。

按 CCXT 4.5.56 的 `has` 声明粗略归类，111 个交易所中约有 Spot 93 个、Swap 55 个、Future 20 个、Option 8 个、Margin 29 个。这个数字表示 CCXT 对应 exchange id 暴露了相关统一能力或市场类型，不等价于该交易所所有产品都适合真实交易；本仓库验收仍以官方文档、权限、限速、错误码和 live-dry-run 结果为准。

### 已覆盖 CCXT Id

| 覆盖类型 | CCXT id | 本仓库 adapter |
| --- | --- | --- |
| 直接覆盖 | `ascendex`, `backpack`, `bigone`, `binance`, `bingx`, `bitget`, `bitmex`, `bitrue`, `blofin`, `coinbase`, `coinex`, `cryptocom`, `deepcoin`, `digifinex`, `kraken`, `kucoin`, `lbank`, `mexc`, `okx`, `phemex`, `poloniex`, `toobit`, `weex`, `whitebit`, `woo`, `xt` | 同名或已注册等价 adapter |
| 别名覆盖 | `gate`, `gateio` | `gateio` |
| 别名覆盖 | `hashkey` | `hashkey_global` |
| 产品线合并覆盖 | `binanceusdm` | `binance` 的 USDT-M/linear perp 面 |
| 产品线合并覆盖 | `coinbaseadvanced`, `coinbaseinternational` | `coinbase` 的 Advanced Trade + INTX perp 面 |

注意：覆盖不等于 CCXT `has` 全量等价。很多 adapter 只覆盖 `ExchangeClient` 标准 trait 的交易面，缺 CCXT 的资金账户、借贷、充值提现、期权、历史账本、衍生品风控等扩展能力。后续每个 adapter 都要补 `ccxt_has_audit` 表，逐项标记 `Native`、`Composed`、`RestFallback`、`Unsupported`。

### 缺失 CCXT 交易所清单

| 优先级 | 缺失 CCXT id | 原因与建议 |
| --- | --- | --- |
| P0 主流交易/合约必须补 | `bybit`, `htx`, `huobi`, `bitmart`, `bitfinex`, `deribit`, `hyperliquid`, `krakenfutures`, `kucoinfutures`, `binancecoinm` | 这些直接影响主流现货/永续/期权覆盖。Bybit、HTX/Huobi、BitMart、Hyperliquid 在仓库可能有旧路径或非统一路径，优先迁移到 `ExchangeClient`；Deribit 补期权和线性/反向合约；Kraken/KuCoin futures 可从现有 spot adapter 拆产品线扩展；Binance coin-margined 用现有 Binance 结构扩展 inverse/coin-m futures。 |
| P1 主流现货和区域市场 | `bitstamp`, `bithumb`, `upbit`, `bitflyer`, `bitvavo`, `gemini`, `coinbaseexchange`, `coincheck`, `bitbank`, `bitso`, `btcturk`, `btcmarkets`, `independentreserve`, `indodax`, `coinone`, `coinsph`, `coinspot`, `mercado`, `luno` | 主要补法币区、日韩、欧洲和美洲现货深度。优先 public REST + spot private REST + public/private WS；合约能力通常不是第一目标。 |
| P1 衍生品/机构/链上撮合 | `bullish`, `delta`, `dydx`, `paradex`, `derive`, `grvt`, `lighter`, `oxfun`, `pacifica`, `apex`, `aster` | 用于期权、perp、链上订单簿和机构流动性覆盖。需要先确认签名、风控、地区限制和 testnet，再实现 `perp_risk_data`、positions、funding、open interest、WebSocket resync。 |
| P2 大量 CCXT 现货长尾 | `bit2c`, `bitbns`, `bitopro`, `bitteam`, `bittrade`, `blockchaincom`, `btcbox`, `cex`, `coinmate`, `coinmetro`, `cryptomus`, `exmo`, `foxbit`, `hitbtc`, `hollaex`, `latoken`, `ndax`, `novadax`, `onetrading`, `p2b`, `paymium`, `tokocrypto`, `wavesexchange`, `yobit`, `zaif`, `zebpay` | 先按 scan-only/public REST 接入，只有私有交易接口成熟且地区可用时再进入 live-dry-run。 |
| P2 特殊或低交易优先级 | `aftermath`, `alpaca`, `arkham`, `bybiteu`, `bydfi`, `hibachi`, `modetrade`, `woofipro` | 有些是品牌/地区/产品变体，有些偏 DeFi 或经纪接口。先判断是否能复用现有 adapter 或是否只作为 alias/register profile。 |
| P3 可延后或只做别名/只读 | `binanceus`, `okxus`, `bequant`, `fmfwio`, `myokx` | 可能是同 API 族、地区限制强、或与主 adapter 高度重合。优先做 config profile/alias，而不是复制 adapter。 |

完整缺失列表按 CCXT id 排序：`aftermath`, `alpaca`, `apex`, `arkham`, `aster`, `bequant`, `binancecoinm`, `binanceus`, `bit2c`, `bitbank`, `bitbns`, `bitfinex`, `bitflyer`, `bithumb`, `bitmart`, `bitopro`, `bitso`, `bitstamp`, `bitteam`, `bittrade`, `bitvavo`, `blockchaincom`, `btcbox`, `btcmarkets`, `btcturk`, `bullish`, `bybit`, `bybiteu`, `bydfi`, `cex`, `coinbaseexchange`, `coincheck`, `coinmate`, `coinmetro`, `coinone`, `coinsph`, `coinspot`, `cryptomus`, `delta`, `deribit`, `derive`, `dydx`, `exmo`, `fmfwio`, `foxbit`, `gemini`, `grvt`, `hibachi`, `hitbtc`, `hollaex`, `htx`, `huobi`, `hyperliquid`, `independentreserve`, `indodax`, `krakenfutures`, `kucoinfutures`, `latoken`, `lighter`, `luno`, `mercado`, `modetrade`, `myokx`, `ndax`, `novadax`, `okxus`, `onetrading`, `oxfun`, `p2b`, `pacifica`, `paradex`, `paymium`, `tokocrypto`, `upbit`, `wavesexchange`, `woofipro`, `yobit`, `zaif`, `zebpay`。

### 未接入交易所总台账

状态列用于后续逐个推进：`TODO` 表示尚未接入，`scan_only` 表示只读行情或 public REST 已接入，`request_spec` 表示私有请求构造和签名已离线验证，`live_dry_run` 表示已通过只读或小额演练，`done` 表示已进入标准 adapter 验收口径。

| # | CCXT id | 优先级 | 产品重点 | 初始接入建议 | 状态 |
| --- | --- | --- | --- | --- | --- |
| 1 | `aftermath` | P2 | DeFi/Sui 生态 | 先确认 CCXT 交易面是否适合网关；scan-only public first | TODO |
| 2 | `alpaca` | P2 | Broker/crypto | 按经纪接口处理，优先账户/订单模型和地区限制审计 | TODO |
| 3 | `apex` | P1 | Perp/DEX | 优先 perp public/private REST + WS，确认签名和链上账户模型 | TODO |
| 4 | `arkham` | P2 | 情报/交易平台 | 先做 API 产品边界审计，可能只做只读 profile | TODO |
| 5 | `aster` | P1 | Perp/DEX | 优先 linear perp、funding、positions、WS resync | TODO |
| 6 | `bequant` | P3 | HitBTC API 族 | 优先判断是否 alias/profile，不复制 adapter | TODO |
| 7 | `binancecoinm` | P0 | COIN-M futures | 从 Binance adapter 扩展 inverse/coin-m futures | TODO |
| 8 | `binanceus` | P3 | US spot | 优先 Binance profile/alias，处理地区和 endpoint 差异 | TODO |
| 9 | `bit2c` | P2 | 区域现货 | scan-only public REST，私有交易后置 | TODO |
| 10 | `bitbank` | P1 | 日本现货 | spot public/private + public/private WS | TODO |
| 11 | `bitbns` | P2 | 区域现货 | scan-only public REST，确认私有 API 可用性 | TODO |
| 12 | `bitfinex` | P0 | Spot/margin/derivatives | 主流深度，补 spot/margin/perp 和 WS | TODO |
| 13 | `bitflyer` | P1 | 日本现货/FX | spot + Lightning API，注意 region/product code | TODO |
| 14 | `bithumb` | P1 | 韩国现货 | spot public/private + KRW 市场能力 | TODO |
| 15 | `bitmart` | P0 | Spot/perp | 迁移旧路径到统一 ExchangeClient，补 WS 和 batch | TODO |
| 16 | `bitopro` | P2 | 台湾现货 | scan-only public REST 后补 spot private | TODO |
| 17 | `bitso` | P1 | 拉美现货 | spot public/private，法币市场和账本重点 | TODO |
| 18 | `bitstamp` | P1 | 欧美主流现货 | spot public/private + WS，机构稳定性优先 | TODO |
| 19 | `bitteam` | P2 | 长尾现货 | scan-only public REST | TODO |
| 20 | `bittrade` | P2 | 日本现货 | 先判断与 Huobi/HTX API 族关系，再接 profile | TODO |
| 21 | `bitvavo` | P1 | 欧洲现货 | spot public/private + WS，EUR 市场 | TODO |
| 22 | `blockchaincom` | P2 | 现货/经纪 | 先做 public REST 和账户模型审计 | TODO |
| 23 | `btcbox` | P2 | 日本现货 | scan-only public REST | TODO |
| 24 | `btcmarkets` | P1 | 澳洲现货 | spot public/private，AUD 市场和账本 | TODO |
| 25 | `btcturk` | P1 | 土耳其现货 | spot public/private，TRY 市场 | TODO |
| 26 | `bullish` | P1 | 机构现货/衍生品 | 优先 public/private REST + FIX/WS 可行性审计 | TODO |
| 27 | `bybit` | P0 | Spot/USDT perp/options | 迁移/补齐统一 adapter，优先 Binance 对标全能力 | TODO |
| 28 | `bybiteu` | P2 | Bybit EU profile | 优先 Bybit alias/profile，确认监管 endpoint 差异 | TODO |
| 29 | `bydfi` | P2 | Spot/perp | scan-only + 合约 API 审计 | TODO |
| 30 | `cex` | P2 | 老牌现货 | scan-only public REST 后补 spot private | TODO |
| 31 | `coinbaseexchange` | P1 | Coinbase Exchange | 优先 Coinbase profile/独立 Exchange API 差异 | TODO |
| 32 | `coincheck` | P1 | 日本现货 | spot public/private，日元市场 | TODO |
| 33 | `coinmate` | P2 | 欧洲现货 | scan-only public REST | TODO |
| 34 | `coinmetro` | P2 | 欧洲现货 | scan-only public REST 后补 private | TODO |
| 35 | `coinone` | P1 | 韩国现货 | spot public/private，KRW 市场 | TODO |
| 36 | `coinsph` | P1 | 菲律宾现货 | spot public/private，PHP 市场 | TODO |
| 37 | `coinspot` | P1 | 澳洲现货 | spot private 能力和 AUD 市场 | TODO |
| 38 | `cryptomus` | P2 | 支付/现货 | 先做 API 边界审计，可能偏支付而非交易 | TODO |
| 39 | `delta` | P1 | Options/perp | 期权、perp、greeks、funding、WS | TODO |
| 40 | `deribit` | P0 | Options/perp/futures | 期权首批，对 greeks/settlement/WS 私有流重点 | TODO |
| 41 | `derive` | P1 | Options/perp/DeFi | 链上/期权产品，先做签名和账户模型审计 | TODO |
| 42 | `dydx` | P1 | Perp/DEX | perp public/private + chain/account model + WS resync | TODO |
| 43 | `exmo` | P2 | 欧洲现货 | scan-only public REST 后补 spot private | TODO |
| 44 | `fmfwio` | P3 | HitBTC API 族 | 优先 alias/profile 或延后 | TODO |
| 45 | `foxbit` | P2 | 巴西现货 | scan-only public REST，BRL 市场 | TODO |
| 46 | `gemini` | P1 | 美国现货 | spot public/private + WS，账户和法币账本 | TODO |
| 47 | `grvt` | P1 | Perp/options | 先做 API 成熟度、testnet、签名审计 | TODO |
| 48 | `hibachi` | P2 | Perp/DEX | scan-only + perp API 审计 | TODO |
| 49 | `hitbtc` | P2 | 长尾现货 | public REST + WS，private 后置 | TODO |
| 50 | `hollaex` | P2 | 白标交易所框架 | 按 white-label profile 处理，避免泛化过度 | TODO |
| 51 | `htx` | P0 | Spot/perp | 迁移/补齐统一 adapter，HTX 与 Huobi 关系明确化 | TODO |
| 52 | `huobi` | P0 | Spot/perp legacy | 作为 HTX alias/profile 或兼容 adapter | TODO |
| 53 | `hyperliquid` | P0 | USDC perp/DEX | 迁移旧路径到统一 ExchangeClient，补私有流和风控数据 | TODO |
| 54 | `independentreserve` | P1 | 澳洲/新加坡现货 | spot public/private，法币账本 | TODO |
| 55 | `indodax` | P1 | 印尼现货 | spot public/private，IDR 市场 | TODO |
| 56 | `krakenfutures` | P0 | Futures/perp | 从 Kraken spot 拆 futures adapter，补 funding/positions/WS | TODO |
| 57 | `kucoinfutures` | P0 | Futures/perp | 从 KuCoin spot 拆 futures adapter，补合约私有接口 | TODO |
| 58 | `latoken` | P2 | 长尾现货 | scan-only public REST，private 谨慎 | TODO |
| 59 | `lighter` | P1 | Perp/DEX | 低延迟 perp，先做 WS/orderbook 可靠性审计 | TODO |
| 60 | `luno` | P1 | 区域现货 | spot public/private，多法币市场 | TODO |
| 61 | `mercado` | P1 | 拉美现货 | spot public/private，BRL/拉美账本 | TODO |
| 62 | `modetrade` | P2 | 特殊/DeFi | 先确认交易 API 价值和地区限制 | TODO |
| 63 | `myokx` | P3 | OKX profile | 优先 OKX alias/profile，不复制 adapter | TODO |
| 64 | `ndax` | P2 | 加拿大现货 | scan-only public REST 后补 private | TODO |
| 65 | `novadax` | P2 | 巴西现货 | scan-only public REST，BRL 市场 | 已由 `docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md` A-29 收口：gateway adapter、fixtures、文档、disabled config、named registration 已落地；私有写保持 request-spec-only |
| 66 | `okxus` | P3 | OKX US profile | 优先 OKX alias/profile，处理产品裁剪 | TODO |
| 67 | `onetrading` | P2 | 欧洲现货 | scan-only public REST 后补 private | TODO |
| 68 | `oxfun` | P1 | Perp/options | 合约/期权 API 审计，优先 public+private WS | TODO |
| 69 | `p2b` | P2 | 长尾现货 | scan-only public REST | TODO |
| 70 | `pacifica` | P1 | Perp/DEX | 先做 testnet、签名、WS resync 审计 | TODO |
| 71 | `paradex` | P1 | Perp/options/DEX | 链上账户、signing、WS、settlement 重点 | TODO |
| 72 | `paymium` | P2 | 欧洲现货 | scan-only public REST | TODO |
| 73 | `tokocrypto` | P2 | Binance 生态现货 | 优先判断 Binance API 族/profile 复用 | TODO |
| 74 | `upbit` | P1 | 韩国现货 | spot public/private + KRW 市场，严格限速 | TODO |
| 75 | `wavesexchange` | P2 | 链上现货 | 链上账户模型，scan-only 优先 | TODO |
| 76 | `woofipro` | P2 | DeFi/DEX | 先做 API 边界审计，可能只读 | TODO |
| 77 | `yobit` | P2 | 长尾现货 | scan-only public REST，private 高风险后置 | TODO |
| 78 | `zaif` | P2 | 日本现货 | scan-only public REST 后补 private | TODO |
| 79 | `zebpay` | P2 | 区域现货 | scan-only public REST 后补 private | TODO |

### 标准化请求与数据模型缺口

当前 `ExchangeClient` 是交易执行最小面：symbol rules、order book、balances、positions、fees、place/cancel/query/open/fills、批量下单/撤单、cancel-all、基础 public/private stream。对标 CCXT 后，通用标准化层还缺以下能力。

| 能力域 | CCXT 代表方法 | 当前缺口 | 建议标准化对象/方法 |
| --- | --- | --- | --- |
| 市场元数据完整性 | `loadMarkets`, `fetchMarkets`, `fetchCurrencies`, `fetchTradingLimits` | `SymbolRules` 还缺 contract size、settle asset、linear/inverse、expiry、strike、option type、precision mode、maker/taker 默认 fee、网络/资产元数据 | `MarketSnapshot`、`CurrencyNetwork`、`ContractSpec`、`MarketStatus`、`PrecisionMode`；把 spot/swap/future/option/margin 的元数据统一进 capability |
| 基础状态、时间和环境 | `fetchStatus`, `fetchTime`, `fetchCurrencies`, `setSandboxMode` | 没有统一的 exchange health、server time drift、维护状态、资产状态、sandbox/testnet/production profile；签名时间偏移和停机维护只能由 adapter 各自处理 | 新增 `get_exchange_status`、`get_server_time`、`get_time_offset`、`get_asset_status`、`ExchangeEnvironmentProfile`；启动时先校时再启用私有交易 |
| 行情 REST | `fetchTicker`, `fetchTickers`, `fetchBidsAsks`, `fetchTrades`, `fetchOHLCV`, `fetchMarkOHLCV`, `fetchIndexOHLCV`, `fetchPremiumIndexOHLCV` | 现在标准 trait 只有 order book；ticker、public trades、candles 多数是 adapter 专用 helper | 新增 `get_ticker(s)`、`get_public_trades`、`get_candles`、`get_book_ticker`、`get_index_mark_candles` |
| 永续风控数据 | `fetchFundingRate`, `fetchFundingRates`, `fetchFundingRateHistory`, `fetchFundingInterval(s)`, `fetchOpenInterest(s)`, `fetchOpenInterestHistory`, `fetchMarkPrice(s)`, `fetchLongShortRatio/History` | 各 adapter 文档零散，未进入统一 trait；funding/open interest/mark/index/long-short ratio 对策略是核心数据 | 新增 `get_funding_rate(s)`、`get_funding_history`、`get_funding_interval(s)`、`get_open_interest(s)`、`get_mark_index_price`、`get_long_short_ratio` |
| 高级订单 | `createStopOrder`, `createTriggerOrder`, `createStopLossOrder`, `createTakeProfitOrder`, `createTrailingAmountOrder`, `createTrailingPercentOrder`, `createMarketBuyOrderWithCost`, `createOrderWithTakeProfitAndStopLoss` | `OrderType` 只有 stop market/limit，缺 trigger 条件、TP/SL legs、trailing、quote-sized market buy、GTD/expire time、close-on-trigger、working type、self-trade prevention、client order id 幂等策略 | 新增 `TriggerOrderRequest`、`AttachedTpSl`、`TrailingSpec`、`QuoteSizedOrder`、`OrderExecutionPolicy`、`ClientOrderIdPolicy`；不要用 OCO/OTO 强行表示所有条件单 |
| 批量订单语义 | `createOrders`, `editOrders`, `cancelOrders`, `cancelOrdersForSymbols` | capability 只有 bool，无法表达 native/composed、atomic/non-atomic、最大条数、部分失败结构 | 新增 `BatchCapability { mode, atomicity, max_items, supports_partial_failure }` 和逐项 error response |
| 改单语义 | `editOrder`, `editOrders`, `editOrderWithClientOrderId` | 当前 `AmendOrderRequest` 只能改数量，缺价格、TIF、client id 策略和 keep-priority 语义 | 扩展 `AmendOrderRequest` 支持 `new_price`、`new_quantity`、`new_time_in_force`、`keep_priority`、`replace_client_order_id` |
| 订单历史和 reconciliation | `fetchOrders`, `fetchClosedOrders`, `fetchCanceledOrders`, `fetchOpenOrders`, `fetchMyTrades`, `fetchOrderTrades` | 当前查询接口偏实时交易闭环，缺历史分页游标、时间范围、订单状态过滤、成交归属订单的统一约束 | 新增 `OrderQuery`, `TradeQuery`, `PaginationCursor`, `OrderHistoryPage`；每个 adapter 明确按 id、client id、symbol、时间范围的支持矩阵 |
| 仓位/保证金控制 | `setLeverage`, `fetchLeverage(s)`, `setMarginMode`, `fetchMarginMode(s)`, `setPositionMode`, `fetchPositionMode`, `addMargin`, `reduceMargin`, `setMargin`, `fetchMarginAdjustmentHistory`, `closePosition`, `closeAllPositions` | 当前多数是 adapter 专用 helper 或 `Unsupported`，没有统一请求/响应；long/short side leverage、one-way/hedge mode 边界不清 | 新增 `set_leverage`、`get_leverage(s)`、`set_margin_mode`、`set_position_mode`、`adjust_position_margin`、`get_margin_adjustment_history`、`close_position(s)` |
| 杠杆档位、仓位历史和风险限额 | `fetchLeverageTiers`, `fetchMarketLeverageTiers`, `fetchPosition(s)`, `fetchPositionHistory`, `fetchPositionsHistory`, `fetchAccountPositions`, `fetchPositionADLRank`, `fetchPositionsRisk`, `fetchLiquidations`, `fetchMyLiquidations` | 风险限额、仓位历史、强平、ADL、最大杠杆无法统一给策略层，断线恢复和历史 PnL reconciliation 依赖 adapter 私有逻辑 | 新增 `LeverageTierSnapshot`、`RiskLimit`, `PositionSnapshot`, `PositionHistoryPage`, `LiquidationEvent`, `AdlRank` |
| 交易风控和 kill-switch | `cancelAllAfter`、交易所 dead-man switch 扩展 | 原生 countdown cancel-all、session scoped kill-switch、上层 supervisor kill-switch 没有统一声明，容易把本地组合能力误认为交易所原子能力 | 新增 `KillSwitchCapability`, `arm_cancel_all_after`, `disarm_cancel_all_after`；非原生能力只能标 `SupervisorComposed` |
| 账户账本和资金流水 | `fetchAccounts`, `fetchLedger`, `fetchLedgerEntry`, `fetchTransfers`, `transfer`, `fetchFundingHistory` | 目前只标准化余额和 fills，无法做完整资金 reconciliation；缺账户 id、subaccount id、portfolio margin account 模型 | 新增 `AccountDescriptor`, `LedgerEntry`, `TransferRequest/Response`, `AccountType`，覆盖 spot/funding/margin/futures/sub-account/portfolio |
| 充值提现和资产网络 | `fetchDepositAddress`, `fetchDepositAddresses`, `createDepositAddress`, `fetchDeposit`, `fetchDeposits`, `fetchWithdrawal`, `fetchWithdrawals`, `fetchTransactions`, `withdraw`, `fetchDepositWithdrawFee(s)` | 只有部分 adapter 有专用 helper；没有统一权限和安全边界；单条查询、聚合历史、network memo/tag 语义不统一 | 新增只读 `get_deposit_address(es)`, `get_deposit_withdraw_history`, `get_transaction`, `get_network_fees`；`withdraw` 默认不进交易 runtime，需独立高权限开关 |
| 借贷和保证金 | `borrowCrossMargin`, `repayCrossMargin`, `borrowIsolatedMargin`, `repayIsolatedMargin`, `fetchBorrowInterest`, `fetchCrossBorrowRate(s)`, `fetchIsolatedBorrowRate(s)`, `fetchBorrowRateHistory`, `fetchBorrowRatesPerSymbol` | `MarketType::Margin` 存在，但无标准借贷请求和利率模型；isolated margin 利率、历史利息、按 symbol 借贷上限无法统一 | 新增 `BorrowRequest`, `RepayRequest`, `BorrowRateSnapshot`, `BorrowRateHistoryPage`, `BorrowInterestRecord` |
| 期权 | `fetchOption`, `fetchOptionChain`, `fetchGreeks`, `fetchAllGreeks`, `fetchVolatilityHistory`, `fetchUnderlyingAssets`, `fetchSettlementHistory`, `fetchMySettlementHistory` | `MarketType::Option` 存在，但无 option contract、greeks、vol surface、交割/结算事件标准数据；option position/order reconciliation 口径未定义 | 新增 `OptionContractSpec`, `GreeksSnapshot`, `VolatilityPoint`, `SettlementEvent`；Deribit/OKX/Bybit/Delta 作为首批 |
| WebSocket runtime | CCXT Pro `watchOrderBook`, `watchOrderBookForSymbols`, `watchTrades`, `watchTradesForSymbols`, `watchTicker(s)`, `watchBidsAsks`, `watchOHLCV`, `watchOrders`, `watchMyTrades`, `watchBalance`, `watchPositions`, `watchLiquidations`, `watchDepositsWithdrawals` | 当前 trait 返回 subscription id，很多 adapter 只有 payload/parser helper；缺统一 runtime、批量订阅、退订、订阅状态、重连、resync、checksum 报告 | 新增 `StreamSession`, `StreamRuntimeEvent`, `SubscriptionAck`, `UnsubscribeRequest`, `ReconnectPolicy`, `ResyncReason`, `SequenceGap` |
| WebSocket 心跳和鉴权续期 | CCXT Pro watch loop 的 keepalive、交易所 ping/pong、listen key 或 token renewal | Binance listenKey keepalive、OKX login channel、Coinbase JWT、私有流 token 续期、server ping/client pong 语义差异大，目前无统一生命周期事件 | 新增 `HeartbeatPolicy`, `AuthRenewalPolicy`, `WsLivenessState`, `ListenKeyLease`；所有私有 WS 必须声明心跳方向、超时、续期、重登和订阅恢复流程 |
| WebSocket 交易通道 | `createOrderWs`, `editOrderWs`, `cancelOrderWs`, `cancelOrdersWs`, `watchOrders` | 部分交易所提供低延迟 WS 下单/撤单/改单，但当前只能走 REST；WS trading 与 REST 的 idempotency、ack/fill 顺序、错误返回不同 | 新增可选 `WsTradingCapability` 和 `place_order_ws`/`cancel_order_ws`/`amend_order_ws`；默认不替代 REST，只作为 latency-sensitive 能力 |
| 错误和限速 | CCXT typed errors、`rateLimit`, `has`, `requiredCredentials` | 已有错误分类，但缺 per-endpoint weight、retry-after、order-level partial failure、credential scope | 扩展 `ExchangeClientCapabilities` 和 `ExchangeError`，加入 endpoint weight、credential scopes、rate-limit bucket |
| 分页和回溯边界 | CCXT 大量 `since`, `limit`, `cursor`, `until`, `params` 分页方法 | 订单、成交、账本、充值提现、funding history、open interest history 的分页上限和时间窗口不统一，影响历史回补和 reconciliation | 新增 `PageRequest`, `PageCursor`, `HistoryWindowCapability`, `MaxLimitCapability`，每个列表方法暴露 `since/limit/cursor/days_back/max_limit` 支持边界 |

### 通用工具与基础设施台账

这些不是单个交易所 adapter，但必须先标准化，否则 79 个缺失交易所会重复实现签名、分页、WS 生命周期、错误映射和能力声明。

| 工具/模块 | 要解决的问题 | 最小交付 | 优先级 |
| --- | --- | --- | --- |
| CCXT 对标审计脚本 | 手工统计 CCXT id、`has` 能力和本仓库 adapter 容易漏算 | `scripts/audit_ccxt_gap.{js,sh}` 输出 covered/missing/non-ccxt、产品线统计、method has 统计，并写入 markdown 片段 | P0 |
| Adapter scaffold generator | 新交易所文件结构、config、signing、transport、parser、tests 重复创建 | `scripts/new_exchange_adapter` 生成目录、feature gate、registry stub、fixtures 目录、request-spec test stub | P0 |
| Endpoint mapping schema | 每个交易所 REST/WS endpoint、权限、限速、产品线分散写在文档里 | `crates/rustcta-exchange-gateway/schemas/exchange_endpoint_mapping.schema.json`，包含 method/path/auth/weight/product/testnet/ws channel | P0 |
| Request-spec 测试框架 | 私有接口不能靠真实下单验证签名和请求体 | 通用 matcher 校验 method/path/query/body/header/signature/timestamp；fixture 中脱敏 key/secret | P0 |
| Signing test vector harness | 各交易所 HMAC/RSA/Ed25519/JWT/API-key header 规则差异大 | 每个 signing.rs 必须有官方样例或本地固定样例，统一输出 canonical string 和 signature | P0 |
| Symbol normalization registry | `BTC/USDT`、`BTCUSDT`、`XBTUSD`、`BTC-USDT-SWAP` 等映射重复写 | `SymbolRegistry` 支持 canonical、exchange symbol、market type、settle asset、contract size、alias | P0 |
| Capability matrix schema | `supports_batch_orders: bool` 这类 bool 不能表达 native/composed/partial failure | `capabilities.yaml/json` 支持 `Native`、`Composed`、`RestFallback`、`WsOnly`、`Unsupported`、max items、atomicity | P0 |
| Error taxonomy mapper | CCXT 有 typed errors，本仓库需要可恢复/不可恢复/权限/限速/风控分类 | 通用 `ExchangeErrorKind` 映射表，覆盖 HTTP code、exchange code、retry-after、order-level partial failure | P0 |
| Rate-limit bucket runtime | 不同 endpoint weight、IP/key/user/order 限速不同 | `RateLimitPlan` 支持 per-endpoint weight、bucket key、burst、retry-after、429/418 处理 | P0 |
| Pagination toolkit | 历史订单、成交、账本、资金、funding/open interest 都要分页 | `PageRequest/PageCursor` + `HistoryBackfillPlan`，记录 since/limit/cursor/until/max window 支持 | P0 |
| REST reconciliation engine | 私有 WS 缺失或断线后需要用 REST 补订单/成交/余额/仓位 | 按 adapter capability 执行 open orders、closed orders、my trades、balances、positions 的补偿查询 | P0 |
| Public WS orderbook engine | depth diff、snapshot、checksum、sequence gap 各交易所不同 | 通用 orderbook state machine，adapter 只实现 snapshot/delta parser 和 checksum spec | P0 |
| WS session runtime | 当前很多 adapter 只有订阅 payload，没有统一连接生命周期 | `StreamSession` 统一 connect/auth/subscribe/unsubscribe/reconnect/resubscribe/resync/close 事件 | P0 |
| Heartbeat and lease manager | ping/pong、listenKey/JWT/token renewal 是私有流稳定性的核心 | `HeartbeatPolicy`、`AuthRenewalPolicy`、`ListenKeyLease`，统一超时、续期、重登、订阅恢复 | P0 |
| Private stream normalizer | 订单、成交、余额、仓位私有流格式差异大 | 标准 `OrderEvent`、`FillEvent`、`BalanceEvent`、`PositionEvent`，包含 event time、transaction time、sequence | P0 |
| Batch operation planner | 批量下单/撤单可能 native atomic、native partial 或本地组合 | `BatchPlanner` 基于 capability 拆分 batch，返回逐项成功/失败和全局 atomicity 标记 | P0 |
| Idempotency and client id policy | 重复 client order id、重试、超时后查询策略不统一 | `ClientOrderIdPolicy`、`RetryReconcilePolicy`，明确 duplicate、unknown status、timeout 的处理 | P0 |
| Sandbox/testnet profile manager | testnet endpoint、产品线、签名、symbol 与生产不同 | `ExchangeEnvironmentProfile` 分离 production/testnet/sandbox，禁止 dry-run 误发生产订单 | P0 |
| Fixture sanitizer | 真实响应含账户、地址、订单 id、资产余额，不能直接提交 | 脱敏工具保留字段形状和边界值，替换 key/id/address/amount 可选策略 | P1 |
| Live read-only validator | 接入后需要统一验证 server time、markets、book、fees、balances、open orders | `cargo run --bin exchange_live_probe -- --read-only` 输出 markdown/json 报告 | P1 |
| Small-order dry-run harness | 真实交易前要用小额订单验证 place/cancel/fills/reconciliation | 统一计划文件，支持 disabled symbols、max notional、kill-switch、post-only 小单 | P1 |
| Documentation generator | adapter 完成后文档、capability matrix、endpoint mapping 容易不同步 | 从 endpoint mapping 和 capability schema 生成 markdown 状态表 | P1 |

### 推荐推进顺序

1. 先补通用工具：CCXT 审计脚本、adapter scaffold、endpoint mapping schema、request-spec/signing harness、capability matrix、WS session runtime、heartbeat/lease manager。这些能让现有 37 个 adapter 和后续 79 个缺失交易所同时受益。
2. 再补标准化接口：ticker/trades/candles、funding/open interest/mark price、history pagination、batch capability 结构、REST reconciliation、WebSocket runtime 状态。
3. 再迁移主流缺失交易所：`bybit`、`htx/huobi`、`bitmart`、`hyperliquid`、`deribit`、`krakenfutures`、`kucoinfutures`、`bitfinex`、`bitstamp`、`bithumb/upbit`。
4. 对地区/品牌变体先做 alias/profile：`binanceus`、`okxus`、`coinbaseexchange`、`myokx`、`bybiteu`，确认 API 不同后再独立 adapter。
5. 对 CCXT 有但策略价值较低的长尾交易所，先做 scan-only public REST；只有私有交易文档成熟、权限可验证、错误码稳定时再升级到 live-dry-run。

## CCXT/Binance 对标口径

本计划用 CCXT 的统一方法名作为能力发现清单，用 Binance Spot 与 USD-M Futures 的接口行为作为验收基准。CCXT 只用于定义“应检查哪些能力”，不能替代官方文档；每个交易所最终仍以官方 REST/WS 规格、权限范围、限速、错误码和 live-dry-run 验证为准。

| CCXT 能力簇 | Binance 对标能力 | Gateway 验收口径 |
| --- | --- | --- |
| `loadMarkets` / `fetchMarkets` | Spot `/exchangeInfo`、USD-M `/fapi/v1/exchangeInfo` | `get_symbol_rules` 同时覆盖 canonical symbol、exchange symbol、tick size、lot size、min notional、合约面值、状态和 margin asset |
| `fetchOrderBook` / `fetchTicker` / `fetchTrades` / `fetchOHLCV` | depth、ticker、trades、klines | Spot 与 linear perp 均需 public REST parser；WebSocket 缺失时必须说明 REST fallback |
| `fetchBalance` / `fetchPositions` / `fetchTradingFees` | account、positionRisk、commission/fee | 私有 read-only 能力必须可 dry-run 构造请求，余额、仓位、手续费来源需标注 |
| `createOrder` / `createOrders` | single order、batchOrders | 单笔下单支持 market/limit/post-only/IOC/FOK 能力矩阵；批量下单必须标注 native atomic、native non-atomic 或 gateway composed |
| `cancelOrder` / `cancelOrders` / `cancelAllOrders` | single cancel、batch cancel、allOpenOrders | 单撤、批撤、symbol scoped cancel-all 全部要有 endpoint mapping；组合撤单不得伪装成原子能力 |
| `fetchOrder` / `fetchOpenOrders` / `fetchMyTrades` | order、openOrders、myTrades/userTrades | 用于 REST reconciliation，必须覆盖 order id、client order id、symbol、limit/time range 的支持边界 |
| `watchOrderBook` / `watchTrades` / `watchTicker` / `watchOHLCV` | public depth/trade/ticker/kline streams | 公开 WS 要输出订阅 payload、退订 payload、parser fixture、sequence/checksum/resync 策略 |
| `watchOrders` / `watchMyTrades` / `watchBalance` / `watchPositions` | user data stream、`ORDER_TRADE_UPDATE`、`ACCOUNT_UPDATE` | 私有 WS 要覆盖认证、订阅、订单/成交/余额/仓位 parser；缺 channel 时声明 REST reconciliation fallback |
| `setLeverage` / `setMarginMode` / `setPositionMode` | leverage、marginType、positionSide/dual | 合约高级能力只在官方原生支持且语义可无损映射时开启，否则 `Unsupported` |
| `fetchFundingRate` / `fetchFundingRateHistory` / `fetchOpenInterest` | premiumIndex、fundingRate、openInterest | 永续公开风控数据作为 perp public G3 验收项，缺失时要说明替代来源或能力空缺 |
| `cancelAllAfter` / dead-man switch | countdown cancel-all 或等价 kill-switch | 交易所原生支持才开启；否则由上层 supervisor kill-switch 实现，不写入 adapter 原子能力 |

“对标 Binance”不表示复制 Binance 特有语义。OCO/OTO、quote-sized market、post-only、hedge mode、listenKey、checksum、心跳和重连等能力必须按交易所原生机制逐项判断，可降级但必须显式记录。

## 已接入交易所不重复开发

以下交易所已经在当前仓库中有统一 Spot、私有永续、市场数据或旧适配器路径，本轮 30 个新增目标不再安排：

| 交易所 | 当前状态 | 本轮动作 |
| --- | --- | --- |
| Binance | Spot 与 USDT 永续大部分接口已实现，是本轮对标基准 | 不重复实现，只补齐标准和回归测试 |
| OKX | Spot 与 USDT 永续已有路径 | 不重复实现 |
| Bitget | Spot 与 USDT 永续已有路径 | 不重复实现 |
| Gate.io | Spot 与 USDT 永续已有路径 | 不重复实现 |
| MEXC | Spot 与 USDT 永续已有路径 | 不重复实现 |
| CoinEx | Spot 已有路径 | 不重复实现 |
| KuCoin | Spot 已有路径 | 不重复实现 |
| Bybit | USDT 永续市场数据/私有路径已有 | 不放入新增 30 个，后续单独补 Spot |
| HTX | USDT 永续市场数据/私有路径已有 | 不放入新增 30 个，后续单独补 Spot |
| BitMart | 旧适配器已有 | 不放入新增 30 个，后续做统一接口迁移 |
| Hyperliquid | 旧 USDC perp 适配器已有 | 不放入新增 30 个，后续做统一接口迁移 |
| Paper | 内部模拟交易适配器 | 不属于外部交易所 |

## 三批 30 个新增交易所

分批原则：

- 第一批优先高流动性、API 文档成熟、能支撑真实交易和回归验证的交易所。
- 第二批优先 altcoin/合约流动性较强、对跨所套利和扫描有增量价值的交易所。
- 第三批优先补广度，用于扩大行情发现、区域市场覆盖和后续长尾套利机会。
- 每个 AI 一次只认领一个任务编号和一个交易所目录，避免改同一批共享代码。确需改公共 trait、协议或类型时，先由协调者合并公共变更。

### 第一批：任务 1-10

| 任务编号 | 交易所 | 建议 adapter id | 产品目标 | 优先原因 |
| --- | --- | --- | --- | --- |
| 任务 1 | Kraken | `kraken` | Spot + Futures/linear where available | 合规和机构使用率高，Spot/Futures API 体系成熟 |
| 任务 2 | Coinbase / Coinbase International | `coinbase` | Coinbase Spot + International linear perp | 已新增 gateway adapter：Spot/INTX Perp 公共 REST、Spot/INTX 私有 REST、订单生命周期、组合式批量下单、原生批量撤单、WS request/parser/session specs、heartbeat 订阅和私有 user 事件 parser；真实 live 连接 supervisor 接入后续由统一 runtime 完成 |
| 任务 3 | Crypto.com Exchange | `cryptocom` | Spot + Futures | 主流现货和合约交易所，适合补充大盘流动性 |
| 任务 4 | Phemex | `phemex` | Spot + USDT perp | 合约 API 成熟，适合永续套利候选 |
| 任务 5 | BingX | `bingx` | Spot + USDT perp | 合约市场活跃，公共和私有接口都值得纳入 |
| 任务 6 | LBank | `lbank` | Spot + USDT perp | 已新增 gateway adapter：LBank Spot 公共/私有 REST、Spot 原生批量下单/组合批量撤单、Spot 公共/私有 WS subscription/session helper、ping/pong 心跳与 subscribeKey 生命周期 helper、USDT perp 公共 REST 与官方确认的 account/placeOrder 私有 REST、USDT perp 组合批量下单、MD5 + HMAC-SHA256 签名、request-spec/parser tests；perp 撤单/查单/开放订单/成交与杠杆/保证金/持仓模式因官方端点未确认，当前显式 `Unsupported` |
| 任务 7 | WhiteBIT | `whitebit` | Spot + Futures | 已新增 gateway adapter：Spot + perpetual 公共 REST、Spot/perp 私有 REST、Spot/perp 原生批量下单、批量撤单、公共/私有 WS subscription/session helper、ping/pong 心跳、标准 stream event parser、WhiteBIT v4 签名、request-spec/parser tests、默认 ignored 的只读 live-dry-run 探针 |
| 任务 8 | WOO X | `woo` | Spot + Perp | 已新增 gateway adapter：WOO X V3 Spot + Perp REST、V3 HMAC 签名、组合批量下单/撤单、Public/Private WS 订阅与 session/心跳/parser tests；live-dry-run 后续补齐 |
| 任务 9 | Toobit | `toobit` | Spot + USDT perp | 近期现货/永续量级进入主流候选池 |
| 任务 10 | BitMEX | `bitmex` | Spot + linear/inverse perp | 永续老牌交易所，合约模型可完善非 Binance 语义 |

#### 任务 6 LBank 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` LBank Spot + USDT perpetual gateway 初版，覆盖官方可确认的交易网关能力。当前覆盖 Spot/USDT perpetual REST base URL、Spot 公共/私有 WS URL、perp WS URL、Spot form private signing、perp JSON private signing、LBank `MD5(...).upper()` + HMAC-SHA256 签名、Spot symbol rules、Spot order book、Spot server time/currency pairs/ticker/book ticker/public trades/klines venue-specific helper、Spot balances、Spot fee rate、Spot place order、Spot quote-sized buy market order、Spot 原生 batch place、Spot 单撤/组合 batch cancel、Spot cancel-all、Spot query/open orders、Spot recent fills/order transaction detail、Spot public WS depth/trade/tick/kbar subscription spec、Spot public depth 标准 `OrderBookSnapshot` 转换、Spot private WS subscribe-key/orderUpdate/assetUpdate spec、private order 标准 `OrderUpdate` 转换、private balance 标准 `BalanceSnapshot` 转换、Spot WS session helper、ping/pong 心跳 payload/parser、标准 heartbeat stream event、subscribeKey create/refresh/destroy helper、USDT perpetual instruments、USDT perpetual order book、USDT perpetual account balance、USDT perpetual account payload 中 positions 的 best-effort parser、USDT perpetual limit placeOrder、USDT perpetual 组合 batch place、受保护的 USDT perpetual raw signed POST extension、request-spec/parser 测试、fixtures、config 示例和错误分类。

官方合约资料复核结论：LBank 合约文档与官方 Python connector 当前只确认 `/cfd/openApi/v1/prv/account` 和 `/cfd/openApi/v1/prv/placeOrder` 可实现级别路径/参数/签名；perp 撤单、查单、开放订单、成交、独立持仓、杠杆、保证金/持仓模式只在文案或错误码中出现，没有稳定路径/参数表。因此这些能力未硬编码伪实现，当前保持不声明支持或返回 `Unsupported`。

验证：`rustfmt --edition 2021 --check crates/rustcta-exchange-gateway/src/adapters/lbank/streams.rs crates/rustcta-exchange-gateway/src/adapters/lbank/public_tests.rs crates/rustcta-exchange-gateway/src/adapters/lbank/private_tests.rs` 已通过；`python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/lbank/endpoint_mapping.yaml` 已通过；`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway lbank --lib --message-format short` 已通过，21 个 LBank 测试通过，0 失败，731 filtered out；`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo check -p rustcta-exchange-gateway --lib --message-format short` 已通过，仅输出既有 workspace warnings。sandbox 内本地 mock REST 绑定 `127.0.0.1` 会被拦截，完整目标集已按需非 sandbox 重跑。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/lbank/`：config、signing、transport、public/private REST、Spot stream subscription spec、parser 和离线 mock REST 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`：adapter module/export、命名注册和 `AdapterBackedGateway` 注册函数。
- `crates/rustcta-exchange-gateway/src/lib.rs`：`LBankGatewayConfig` crate 导出。
- `apps/gateway/src/config.rs`：`lbank` adapter 注册、Spot/perp REST base URL env 和私有凭据 env wiring。
- `docs/交易所网关/适配器/lbank_adapter.md`、`config/lbank_gateway_example.yml`、`tests/fixtures/exchanges/lbank/`：endpoint mapping、能力边界、配置示例和离线响应样本。

Endpoint mapping：

| 标准能力 | LBank 原生接口 | 当前实现 |
| --- | --- | --- |
| Spot symbol rules | `GET /v2/accuracy.do` | 解析 `btc_usdt` 形态 symbol、price/quantity precision、min quantity、min notional |
| Spot order book | `GET /v2/depth.do` | snapshot parser，depth clamp 到 1..200 |
| Spot public market helpers | `GET /v2/timestamp.do`, `/v2/currencyPairs.do`, `/v2/ticker.do`, `/v2/supplement/ticker/bookTicker.do`, `/v2/trades.do`, `/v2/kline.do` | LBank-specific methods，覆盖 server time、交易对列表、ticker、book ticker、public trades、klines |
| Spot balances | `POST /v2/supplement/user_info_account.do` | 解析 `assetCode`/`assetAmt`/`usableAmt`/`locked` |
| Spot fees | `POST /v2/supplement/customer_trade_fee.do` | 按请求 symbol 解析 maker/taker fee |
| Spot order lifecycle | `POST /v2/supplement/create_order.do`, `/cancel_order.do`, `/cancel_order_by_symbol.do`, `/orders_info.do`, `/orders_info_no_deal.do` | limit/market/post-only/IOC/FOK、quote buy market、single cancel、symbol cancel-all、query/open orders |
| Spot batch place/cancel | `POST /v2/batch_create_order.do`; composed calls to `/v2/supplement/cancel_order.do` | 原生批量下单；批量撤单逐单复用已验证单撤路径，避免依赖未清楚标注的逗号批撤语义 |
| Spot fills | `POST /v2/order_transaction_detail.do`, `/v2/supplement/transaction_history.do` | 有 order id 时查订单成交明细；无 order id 时查近期成交流水 |
| Spot streams | `wss://www.lbkex.net/ws/V2/` | depth/trade/tick/kbar subscription spec；depth 转标准 `OrderBookSnapshot`；private subscribe-key + orderUpdate/assetUpdate spec；orderUpdate 转标准 `OrderUpdate`；assetUpdate 转标准 `BalanceSnapshot`；public/private WS session helper；ping/pong 心跳；subscribeKey create/refresh/destroy helper |
| USDT perp symbol rules | `GET /cfd/openApi/v1/pub/instrument` | 解析 `BTCUSDT` 形态 symbol、tick/step、min/max volume、min cost |
| USDT perp order book | `GET /cfd/openApi/v1/pub/marketOrder` | snapshot parser，depth clamp 到 1..100 |
| USDT perp account | `POST /cfd/openApi/v1/prv/account` | account balance 解析；positions 字段存在时 best-effort 解析 |
| USDT perp place order | `POST /cfd/openApi/v1/prv/placeOrder` | 官方确认的 limit order 参数映射：`clientOrderId`、`symbol`、`side`、`offsetFlag`、`orderPriceType`、`origType`、`price`、`volume` |
| USDT perp batch place | 组合调用 `POST /cfd/openApi/v1/prv/placeOrder` | 统一 `batch_place_orders` 对同质 perpetual 订单逐单提交，返回标准 batch response；不伪造未确认的原生 atomic batch |
| USDT perp raw signed extension | 受保护的 `POST /cfd/openApi/v1/prv/...` | `post_contract_signed_raw` 对人工确认但尚未强类型建模的 LBank contract 私有路径应用同一 JSON 签名并返回 raw payload；不改变标准能力声明 |

#### 任务 7 WhiteBIT 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Spot + Futures/linear perpetual 初版。当前覆盖官方 REST base URL、WhiteBIT v4 private `X-TXC-*` base64 payload + HMAC-SHA512 签名、Spot/Perp symbol rules、order book snapshot、Spot balances/fees/place/cancel/cancel-all/query/open orders/recent fills、perp balances/positions、Spot/Perp 下单、Spot/Perp 原生 batch place、组合 batch cancel、公共 WebSocket depth/trades/bookTicker/candles subscription/session helper、私有 WebSocket token/auth/orders/fills/balances/positions subscription/session helper、ping/pong 心跳、标准 order book/order/fill/balance/position/heartbeat stream event parser、request-spec/parser 测试、默认 ignored 的只读 live-dry-run 探针和错误分类。Order list、funding history、mark price dedicated endpoint、leverage/margin/position mode mutations 当前未在 gateway trait 初版接入，未声明支持或返回 `Unsupported`；真实 WhiteBIT 凭据环境验证通过 `WHITEBIT_LIVE_DRY_RUN=1` 显式运行，不会默认访问生产环境或提交订单。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/whitebit/`：config、signing、transport、public/private REST、public/private WebSocket session/parser、离线 mock REST/WS 测试和 ignored live-dry-run 只读探针。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`：adapter module/export、命名注册和 `AdapterBackedGateway` 注册函数。

Endpoint mapping：

| 标准能力 | WhiteBIT 原生接口 | 当前实现 |
| --- | --- | --- |
| spot/perp symbol rules | `GET /api/v4/public/markets`, `GET /api/v4/public/futures` | 解析 Spot `BASE_QUOTE` 与 perp `BASE_PERP`，含 precision、min amount、min/max total、fee metadata |
| spot/perp order book | `GET /api/v4/public/orderbook/{market}` | snapshot parser，支持 `limit` 1..100 与 level 0 |
| public streams | `depth_subscribe`, `trades_subscribe`, `bookTicker_subscribe`, `candles_subscribe` on `wss://api.whitebit.com/ws` | gateway subscription-spec、public WS session helper、ping/pong 心跳、order book/trade/ticker/candle parser；order book 与 heartbeat 转标准 `ExchangeStreamEvent` |
| spot balances | `POST /api/v4/trade-account/balance` | 解析 trade account available/locked/total |
| perp balances | `POST /api/v4/collateral-account/balance` | 解析 collateral balances，按 `MarketType::Perpetual` 标记 |
| spot fees | `POST /api/v4/market/fee` | 解析 maker/taker/futures maker/futures taker 字段，按请求 symbol 归一 |
| spot order lifecycle | `POST /api/v4/order/new`, `/order/market`, `/order/cancel`, `/order/cancel/all`, `/orders` | limit/market/post-only/IOC、quote-sized buy market、cancel、cancel-all、query/open orders；FOK 显式 `Unsupported` |
| spot/perp batch place/cancel | `POST /api/v4/order/bulk`, `/api/v4/order/collateral/bulk`, `/api/v4/order/cancel/bulk` | Spot/perp 原生批量下单；批量撤单按 WhiteBIT bulk cancel 响应解析标准 order state |
| spot fills | `POST /api/v4/trade-account/order` | 解析成交、手续费、成交时间；按订单维度 reconciliation |
| perp positions | `POST /api/v4/collateral-account/positions/open` | 解析数量、entry、mark、liquidation、PnL、leverage、LONG/SHORT/BOTH |
| perp order lifecycle | `/api/v4/order/collateral/market`, `/order/collateral/limit`, `/api/v4/order/cancel`, `/api/v4/orders` | 下单请求构造已映射 reduce-only/positionSide；支持 cancel/query/open orders，fills 使用 Spot trade-account order reconciliation；未确认的专用 funding/mark/leverage/margin/position-mode 仍显式 `Unsupported` |
| private streams | `/api/v4/profile/websocket_token` + `authorize`, `ordersPending_subscribe`, `deals_subscribe`, `balanceSpot_subscribe`, `balanceMargin_subscribe`, `positionsMargin_subscribe` | signed token 获取、authorize payload、private WS session helper、ping/pong 心跳、order/fill/balance/position 标准事件解析 |
| testnet | 官方文档未确认稳定 sandbox | `rest_base_url` 可配置，默认生产 URL；`whitebit_live_dry_run_should_probe_readonly_rest_and_private_ws_auth` 默认 ignored，需 `WHITEBIT_LIVE_DRY_RUN=1` 显式运行，且只读不下单 |

#### 任务 8 WOO X 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Spot + Perp REST/WS 初版，并补齐 WOO 专属 V3 algo order helper。当前覆盖官方 WOO X V3 REST base URL、生产/测试 base URL 可配置、`timestamp + METHOD + path_with_query + body` HMAC-SHA256 hex 签名、Spot/Perp symbol rules、order book snapshot、balances、positions、fee rate、place/cancel/cancel-all/amend/query/open orders、recent fills、quote-sized Spot market order、组合批量下单/组合批量撤单、cancel-all-after、futures leverage、position mode、account trading mode、default margin mode readback、V3 algo order create/amend/query/cancel/list/cancel-all helper、public/private WS subscription payload、private listen key、public/private WS session helper、PING/PONG 心跳、标准 stream event 转换、request-spec/parser 测试和错误分类。普通订单 batch 当前按已验证单笔端点顺序组合，非原子 native batch；Public `orderbookupdate` parser 不声明为严格 Binance diff-depth 等价，live consumer 仍需 snapshot buffering 和连续性校验。Algo order 因统一 `ExchangeClient` 尚无 trigger-order 模型，作为 WOO adapter 扩展方法提供；live-dry-run 仍需只读/测试环境凭据验证。

验证：`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/woo-task-check cargo test -p rustcta-exchange-gateway woo --lib --message-format short` 已通过，18 个 WOO 测试通过，0 失败，734 filtered out；sandbox 内本地 mock REST 绑定 `127.0.0.1` 会被拦截，完整目标集已按需非 sandbox 重跑。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/woo/`：config、signing、transport、public/private REST、parser 和离线 mock REST 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`：adapter module/export、命名注册和 `AdapterBackedGateway` 注册函数。
- `crates/rustcta-exchange-gateway/src/lib.rs`：`WooGatewayConfig` crate 导出。
- `docs/交易所网关/适配器/woo_adapter.md`、`config/woo_gateway_example.yml`：endpoint mapping、签名说明、配置示例和能力边界。

Endpoint mapping：

| 标准能力 | WOO X V3 原生接口 | 当前实现 |
| --- | --- | --- |
| spot/perp symbol rules | `GET /v3/public/instruments` | 解析 `SPOT_BASE_QUOTE` 与 `PERP_BASE_QUOTE`，含 tick、step、min/max quantity、min notional |
| spot/perp order book | `GET /v3/public/orderbook` | snapshot parser，支持 `maxLevel` 1..100，兼容 object level 格式 |
| spot/perp balances | `GET /v3/asset/balances` | 解析 `holding`、`availableBalance`、`frozen`，按请求 market type 标记 |
| perp positions | `GET /v3/futures/positions` | 兼容 `positions[]` 与 `rows[]`，解析数量、entry、mark、liquidation、PnL、positionSide |
| spot/perp fees | `GET /v3/trade/tradingFee` | 解析 maker/taker fee，按请求 symbol 归一 |
| spot/perp order lifecycle | `POST/GET/PUT/DELETE /v3/trade/order`, `GET /v3/trade/orders`, `DELETE /v3/trade/allOrders` | limit/market/post-only/IOC/FOK、quote market、amend quantity、cancel、cancel-all、query/open orders |
| spot/perp batch place/cancel | Composed `POST /v3/trade/order`, composed `DELETE /v3/trade/order` | 统一 `batch_place_orders`/`batch_cancel_orders` 支持；非原子、非 native batch，逐单返回标准 order state |
| spot/perp fills | `GET /v3/trade/transactionHistory` | 解析成交、手续费、maker/taker、realized PnL |
| private REST signing | `x-api-key`, `x-api-timestamp`, `x-api-signature` | 独立 `signing.rs`，hex HMAC-SHA256；测试断言 secret 不进入 path/query/body |
| advanced private controls | `/v3/trade/cancelAllAfter`, `/v3/futures/leverage`, `/v3/futures/positionMode`, `/v3/account/tradingMode`, `/v3/futures/defaultMarginMode` | cancel-all-after、杠杆、持仓模式、账户交易模式、默认保证金模式 helper；当前为 WOO adapter 扩展方法，不进入统一 trait |
| algo orders | `POST/PUT/GET/DELETE /v3/trade/algoOrder`, `GET/DELETE /v3/trade/algoOrders` | WOO 专属 helper 覆盖 stop-market/stop-limit trigger order create、amend、query、single cancel、list、bulk cancel；统一 trait 暂不声明通用 trigger-order capability |
| WebSocket | `wss://wss.woox.io/v3/public`, `POST /v3/account/listenKey`, `wss://wss.woox.io/v3/private?key=...` | public `trade/ticker/orderbook10/orderbookupdate/kline`；private `executionreport/balance/position/account`；session helper 支持 initial requests、PING/PONG、标准 order book/order/fill/balance/position/heartbeat 事件 |
| testnet/staging | `https://api.staging.woox.io` | `rest_base_url` 可配置，默认生产 URL；真实交易前必须 dry-run/live-dry-run |

#### 任务 4 Phemex 交付状态

状态：任务 4 的 Spot + USDT perpetual gateway 阶段交付已完成，但不等同于“Phemex 官方全部 endpoint 强类型全覆盖”。当前已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Spot + USDT perpetual REST 初版，并补充 Phemex 专用公开 REST、WebSocket request-spec/parser、USDT-M 仓位管理、账户历史、资金划转、兑换、新版充值提现 wrapper，以及受限 public/private raw REST passthrough。已覆盖官方 REST base URL、testnet/prod 可配置 base URL、public/private WS URL、REST `path + query + expiry + body` HMAC-SHA256 签名、WS `api_key + expiry` HMAC-SHA256 认证 payload、Spot/USDT 永续 symbol rules、order book snapshot、fullbook、server time、index sources、24h ticker、v1/v2/v3 ticker raw endpoint、public trades、nomics trades、perp mark/index/funding/open interest ticker 字段、funding history、legacy/v2/v2-list perp candles、products-plus、chain settings、real funding rates、trader performance info、balances、positions、Spot/USDT-M fee-rate、place、组合 batch-place、amend/cancel、USDT-M 原生 batch-cancel、cancel-all/query/open orders、recent fills、private funding fee history、closed positions、spot funds history、risk units、旧 wallet deposit address/deposit/withdraw history、新版 `/phemex-deposit/*` chain/address/history、新版 `/phemex-withdraw/*` withdraw history/create/cancel/asset info、`/assets/transfer`、spot/futures sub-account transfer、universal transfer、`/assets/quote`、`/assets/convert`、受限 raw signed GET/POST/POST JSON/PUT/DELETE 用于 margin、UTA、legacy contract 和其它未强类型建模官方 REST path、public WS orderbook/trade/kline/ticker 订阅与解析、private WS `wo.subscribe`/`aop_p.subscribe`/`ras_p.subscribe` 订阅与 Spot wallet/order/fill、USDT-M account/order/position、risk account parser、`switch_position_mode`、`set_one_way_leverage`、`set_hedged_leverage`、`assign_position_balance`、request-spec/parser 测试和错误分类。Spot REST candles 因官方 Spot REST section 无稳定等价端点，当前显式 `Unsupported`。order list、dead-man switch、手动 risk-limit 设置仍不声明完整统一交易能力；dead-man 当前未在官方 Phemex API Reference 找到等价 endpoint，手动 risk-limit 在 hedged contracts 文档中已废弃。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/phemex/`：config、signing、transport、public/private REST、parser 和离线 mock REST 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`：adapter module/export、命名注册和 `AdapterBackedGateway` 注册函数。
- `crates/rustcta-exchange-gateway/src/lib.rs`：`PhemexGatewayConfig` crate 导出。
- `apps/gateway/src/config.rs`：`phemex` adapter 注册、REST base URL env 和私有凭据 env wiring。
- `docs/交易所网关/适配器/phemex_adapter.md`、`config/phemex_gateway_example.yml`：endpoint mapping、能力边界、配置示例和离线验证说明。

#### 任务 3 Crypto.com Exchange 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Spot + perpetual REST/WS request-spec 初版。当前覆盖官方 Exchange v1 REST base URL、UAT/生产可配置 REST/WS URL、JSON-RPC 私有请求、`method + id + api_key + ordered params + nonce` HMAC-SHA256 签名、WS `public/auth` 签名 payload、Spot 与 perpetual symbol rules、order book、Spot balances、perpetual positions、Spot fee rate、Spot/perpetual place/cancel/cancel-all/amend/query/open orders/recent fills、perpetual reduce-only 下单、native batch place/cancel order list、advanced OCO/OTO order list、public WS book/trade/ticker/candlestick 订阅与解析、private WS user.order/user.trade/user.balance 订阅与解析、Crypto.com 专用 public/private WS session helper（initial requests、heartbeat response、runtime state、文本消息到标准事件转换）、request-spec/parser 测试和错误分类。`amend_order` 因官方 `private/amend-order` 同时要求 `new_price` 与 `new_quantity`，当前按统一接口先查当前订单价格，再以原价 + 新数量提交；`new_client_order_id` 不伪造支持。统一 `ExchangeClient` 级事件 receiver/stream、per-market capability 粒度、以及 trait 外的 mark price/funding/open interest/leverage/margin/position-mode/dead-man switch 仍不声明完整统一能力或显式返回 `Unsupported`。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/cryptocom/`：config、signing、transport、public/private REST、public/private WS request-spec/parser 和离线 mock 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`：adapter module/export、命名注册和 `AdapterBackedGateway` 注册函数。
- `crates/rustcta-exchange-gateway/src/lib.rs`：`CryptoComGatewayConfig` crate 导出。
- `apps/gateway/src/config.rs`：`cryptocom` gateway app env wiring，支持 `RUSTCTA_CRYPTOCOM_REST_BASE_URL`、`RUSTCTA_CRYPTOCOM_API_KEY`、`RUSTCTA_CRYPTOCOM_API_SECRET`。
- `config/spot_exchanges_example.yml`、`config/cryptocom_gateway_example.yml`、`docs/交易所网关/适配器/cryptocom_adapter.md`、`tests/fixtures/exchanges/cryptocom/`：disabled-by-default 配置示例、endpoint mapping、认证规则、WS 心跳说明、能力边界和离线 fixture 样本。

验证：`python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/cryptocom/endpoint_mapping.yaml` 已通过；`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/cryptocom-task-check cargo test -p rustcta-exchange-gateway cryptocom --lib --message-format short` 已通过，19 个 Crypto.com 测试通过，0 失败，733 filtered out；sandbox 内本地 mock REST 绑定 `127.0.0.1` 会被拦截，完整目标集已按需非 sandbox 重跑。

#### 任务 9 Toobit 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Toobit Spot + USDT-M perpetual REST 与 WebSocket request-spec/parser 接入，并保留旧路径的 `ToobitPerpClient`、USDT-M public market adapter 与通用 `private_perp` 入口。当前覆盖官方 REST base URL、`X-BB-APIKEY` HMAC query/body 签名、Spot/USDT-M symbol rules、order book、balances、positions、place/cancel/cancel-all/query/open orders、recent fills、Spot 原生 batch place/cancel、USDT-M 原生 batch place/cancel、USDT-M amend、USDT-M fees、Spot public depth/trade WS 订阅规格、Spot/USDT-M `markPrice` WS 订阅规格、listenKey private WS endpoint/parser、ping/pong 心跳 helper、错误分类和 request-spec/parser 测试。旧交易执行路径仍覆盖 USDT-M leverage 和 public market funding/mark price。官方未提供 Binance 等价能力的 Spot quote-sized order、Spot amend、Spot OCO/OTO order list、稳定 Spot fee-rate endpoint、USDT-M position-mode switch、USDT-M countdown cancel-all、USDT-M funding WS 和独立 testnet，当前显式 `Unsupported` 或文档化为不可对标项。

代码边界：

- `retired exchange tree/toobit/mod.rs`：Spot/USDT-M config、签名、REST transport、Spot public/private WS runtime、public/private parser、dry-run 和离线测试。
- `retired exchange tree/market_adapters/toobit.rs`：Toobit USDT-M public market adapter，覆盖 contracts、funding、mark price、depth snapshot 和 public WS payload parser。
- `retired exchange tree/private_perp/mod.rs`：Toobit USDT-M v2 futures order/batch/cancel/amend API request spec、`X-BB-APIKEY` query/body HMAC 签名、余额/仓位/成交/订单 parser、listenKey private WS endpoint/parser、private_perp adapter 注册与能力声明。
- `retired exchange tree/mod.rs`：adapter module/export 注册。
- `retired exchange tree/registry.rs`、`retired exchange tree/trading_adapters/mod.rs`：Toobit private perpetual gateway/trading support 注册。
- `retired exchange tree/client_order_id.rs`：Toobit Spot/Perpetual client order id policy。
- `crates/rustcta-exchange-gateway/src/adapters/toobit/`：config、signing、transport、public/private REST、public/private WS request-spec/parser、heartbeat helper、离线 mock REST/WS 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：`toobit` named adapter 注册和 `ToobitGatewayConfig` 导出。
- `docs/交易所网关/适配器/toobit_adapter.md`、`config/toobit_gateway_example.yml`：endpoint mapping、认证规则、配置示例和能力边界。
- `tests/fixtures/exchanges/toobit/`：exchange info、order book、balance、fills parser fixtures。
- `docs/交易所网关/总览/exchange_api_completion_matrix.md`：capability 状态行。

Endpoint mapping：

| 标准能力 | Toobit 原生接口 | 当前实现 |
| --- | --- | --- |
| spot/perp symbol rules | `GET /api/v1/exchangeInfo` | Spot `symbols` 与 USDT-M `contracts` parser，过滤 inverse contract |
| spot/perp order book | `GET /quote/v1/depth`, public WS `depth` | snapshot/parser runtime，保留 timestamp 和 best bid/ask |
| spot balances | `GET /api/v1/account` | 解析 `coin/total/free/locked` |
| spot order lifecycle | `/api/v1/spot/order`, `/api/v1/spot/openOrders` | place/cancel/cancel-all/query/open orders，dry-run 保护；Spot quote-sized、amend、OCO/OTO 官方无等价端点，保持 unsupported |
| spot fills | `GET /api/v1/account/trades` | 解析成交、手续费、maker/taker |
| spot private stream | `POST/PUT/DELETE /api/v1/userDataStream`, WS `/api/v1/ws/<listenKey>` | listenKey 创建/保活、user stream order/balance/fill parser 和 runtime |
| USDT-M public market adapter | `GET /api/v1/exchangeInfo`, `GET /quote/v1/depth`, `GET /api/v1/futures/fundingRate`, `GET /quote/v1/markPrice`, public WS `depth`/`markPrice` | 注册 `ToobitMarketAdapter`，支持 instruments、funding、mark price、REST depth snapshot、public WS subscription spec 和 parser fixtures |
| USDT-M private orders | `POST/DELETE/GET /api/v2/futures/order`, `POST /api/v2/futures/batch-orders`, `POST /api/v2/futures/order/update`, `GET /api/v2/futures/open-orders`, `GET /api/v2/futures/history-orders`, `DELETE /api/v2/futures/batch-orders`, `DELETE /api/v1/futures/cancelOrderByIds` | 统一 `ToobitPerpClient` 和 `private_perp` 支持 live place、batch-place、amend、cancel、batch-cancel、native symbol cancel-all、query、open/history readback |
| USDT-M private account | `GET /api/v1/futures/balance`, `GET /api/v1/futures/positions`, `GET /api/v1/futures/commissionRate`, `POST /api/v2/futures/leverage`, listenKey WS `/api/v1/ws/<listenKey>` | `ToobitPerpClient`/`private_perp` 支持 balances、positions、fees、fills、symbol account config、set leverage、private WS endpoint/parser；position-mode/countdown 保持显式 unsupported |
| testnet | 官方无独立外部 sandbox | 配置默认生产 URL，真实交易前必须 dry-run/live-dry-run |

#### 任务 10 BitMEX 交付状态

状态：已完成 `rustcta-exchange-gateway` 公共 REST、私有 REST 与 WebSocket 订阅/心跳接入。当前支持 BitMEX active instruments、L2 order book snapshot、余额、仓位、费用、单笔/批量下单、单笔/批量撤单、批量撤全、改单、查询、open orders、recent fills、公开/私有 WS subscription payload、auth payload、WS 心跳、public/private WS session helper、核心 WS 消息 parser 与标准 `ExchangeStreamEvent` 转换的 request-spec/parser 测试和 gateway 注册；quote-sized market、order list、StopLimit（统一请求缺少独立 stop trigger 字段）、资金费率/mark/index price 专用端点仍显式返回 `Unsupported` 或留作后续 G3 增强。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/bitmex/`：config、signing、transport、public/private parser、REST order/account/fill 实现、WS subscription spec、离线测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter 注册与 config 导出。
- `config/spot_exchanges_example.yml`、`config/bitmex_gateway_example.yml`：新增默认 disabled 的 BitMEX Spot/Perp 配置示例。

Endpoint mapping：

| 标准能力 | BitMEX 原生接口 | 当前实现 |
| --- | --- | --- |
| spot/perp symbol rules | `GET /api/v1/instrument/active` | 解析 symbol、Spot/Perpetual/Futures market type、base/quote、tick size、lot size、max qty |
| spot/perp order book | `GET /api/v1/orderBook/L2` | snapshot parser，按 side 分组排序，保留 exchange timestamp |
| authentication signature | HMAC-SHA256 `method + path + expires + body` | 独立 `signing.rs`，带固定向量测试和 signed request-spec 测试 |
| balances | `GET /api/v1/user/margin` | 解析 wallet/margin/available，归一资产和账户上下文 |
| positions | `GET /api/v1/position` | 解析 open position、side、entry/mark/liquidation/leverage |
| fees | `GET /api/v1/instrument/active` | 解析 maker/taker fee，source 标注为 instrument active |
| place order | `POST /api/v1/order`、`POST /api/v1/order/bulk` | limit/market/stop-market、post-only、reduce-only、IOC/FOK/GTC、client order id；批量下单走原生 bulk endpoint；StopLimit 因统一接口缺少独立 `stopPx` 字段保持 unsupported |
| cancel/query/open/fills | `DELETE /order`、`DELETE /order/all`、`GET /order`、`GET /execution/tradeHistory` | 单笔/批量撤单、撤全、查询、活动订单、成交 parser；解析手续费、client id、exchange id |
| amend order | `PUT /api/v1/order` | 支持 order id 或 client id 修改数量 |
| quote market / order list | 无标准等价或第一版未启用 | `Unsupported` |
| WebSocket | `wss://ws.bitmex.com/realtime` public/private channels | 支持 orderBookL2/trade/quote/tradeBin 与 order/execution/margin/position/wallet 订阅 payload、auth payload、ping/pong 心跳、public/private WS session helper、supervisor state decision、book/trade/order/execution/balance/position parser fixtures 和标准 stream event 转换 |

#### 任务 1 Kraken 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Kraken Spot + Futures perpetual REST、批量订单和 WebSocket session/parser 接入。当前覆盖 Spot/Futures REST base URL、Spot `API-Key/API-Sign` HMAC-SHA512 签名、Futures `APIKey/Nonce/Authent` HMAC-SHA512 签名、Spot/Futures symbol rules、order book snapshot、Spot/Futures balances、Futures positions、Spot fee-rate、place/cancel/cancel-all/query/open orders、Spot/Futures 原生批量下单、Spot/Futures 原生批量撤单、recent fills、公开 WS subscription request-spec、Spot/Futures public `book` 标准 `OrderBookSnapshot` stream event 转换、Spot 私有 WS token subscription request-spec、Futures 私有 WS challenge + signed subscription request-spec、Kraken 专用 public/private WS session helper、ping/pong/heartbeat/subscription-ack control parser、Spot private `executions` order/fill parser、Spot private `balances` parser、Futures private `open_orders`/`fills`/`balances`/`open_positions` 标准 stream event 转换、gateway/app 注册和离线 parser/request-spec 测试。amend order、order list、杠杆/保证金/持仓模式设置、生产 WebSocket supervisor 真实连接和 read-only live 报告当前显式不声明完成。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/kraken/`：config、Spot/Futures signing、transport、public/private parser、REST order/account/fill/batch 实现、WS subscription/session/heartbeat spec、离线 mock REST/WS 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：`kraken` named adapter 注册和 `KrakenGatewayConfig` 导出。
- `config/spot_exchanges_example.yml`：默认 disabled 的 Kraken Spot/Perp 配置示例。

Endpoint mapping：

| 标准能力 | Kraken Spot / Futures 原生接口 | 当前实现 |
| --- | --- | --- |
| spot symbol rules | Spot `GET /0/public/AssetPairs` | 解析 `wsname`/`altname`、tick、lot、min qty、min notional |
| futures symbol rules | Futures `GET /derivatives/api/v3/instruments` | 解析 perpetual instruments、base/quote、tick size、lot/contract size、min trade size |
| spot order book | Spot `GET /0/public/Depth` | snapshot parser，depth 归一到 1..500，保留交易所时间戳 |
| futures order book | Futures `GET /derivatives/api/v3/orderbook` | snapshot parser，解析 bid/ask、sequence 和交易所时间戳 |
| spot private REST signing | `API-Sign = HMAC-SHA512(uri_path + SHA256(nonce + body))` | 独立 `signing.rs`，官方向量测试和 request-spec 测试 |
| futures private REST signing | `Authent = HMAC-SHA512(query/body + nonce + endpoint path)` | 独立 `signing.rs`，header/body request-spec 测试 |
| spot balances | Spot `POST /0/private/BalanceEx`，fallback `Balance` | 解析 total/free/locked；签名请求 |
| futures balances | Futures `GET /accounts` | 解析 margin account balances，按 account context 标记 |
| futures positions | Futures `GET /openpositions` | 解析数量、entry、mark/liquidation、PnL、leverage、LONG/SHORT |
| spot fees | Spot `POST /0/private/TradeVolume` | 解析 maker/taker fee，source 标注为 TradeVolume |
| spot order lifecycle | Spot `AddOrder`、`CancelOrder`、`CancelAll`、`QueryOrders`、`OpenOrders`、`TradesHistory` | market/limit/post-only/IOC/FOK/GTC/GTX、client id、quote-sized market buy `viqc`、订单状态和成交 parser |
| futures order lifecycle | Futures `sendorder`、`cancelorder`、`cancelallorders`、`orders`、`fills` | market/limit/post-only/IOC/FOK/GTC、client id、reduce-only、cancel/query/open orders、recent fills |
| batch orders | Spot `AddOrderBatch`/`CancelOrderBatch`，Futures `batchorder` | Spot 校验 2..15 单且同一 pair、撤单最多 50 个 id；Futures batchorder 打包 send/cancel；返回标准 batch order/cancel 状态 |
| spot cancel all | Spot `POST /0/private/CancelAll` | 仅 account-wide；symbol-scoped 返回 `Unsupported` |
| public WebSocket | Spot v2 `book/trade/ticker/ohlc`，Futures `book/trade/ticker` | 生成标准 subscription payload 和 gateway subscription id；public WS session helper 输出 initial request、heartbeat request、runtime state、heartbeat stream event，并把 Spot/Futures `book` payload 转成标准 `ExchangeStreamEvent::OrderBookSnapshot`；共享事件模型暂无 public trade/ticker/candle 变体，保留 request-spec 能力 |
| private WebSocket | Spot `GetWebSocketsToken` + `executions`/`balances`；Futures challenge + signed subscribe | 支持 Spot token 获取、Spot subscription payload、Futures challenge 请求、challenge 签名、Futures `open_orders`/`fills`/`balances`/`open_positions` subscription payload、private WS session helper、ping/pong/heartbeat/subscription-ack control parser，并把 Spot/Futures order/fill/balance/position 消息转成标准 stream events |
| testnet | Kraken Spot 无统一 sandbox；Futures sandbox URL 可配置 | base URL 全部可配置，默认生产 URL；真实交易前必须 dry-run/live-dry-run |

验证：`rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/kraken/streams.rs crates/rustcta-exchange-gateway/src/adapters/kraken/stream_tests.rs crates/rustcta-exchange-gateway/src/adapters/kraken/private_parser.rs` 已通过；`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway kraken --lib --message-format short` 已通过 41 个 Kraken 测试，仍输出既有 workspace warnings。

#### 任务 5 BingX 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Spot + USDT
linear perpetual REST/WS request-spec/parser 接入。当前用
`MarketType::Perpetual` 表达 BingX USDT 永续，覆盖官方 REST base URL、
`X-BX-APIKEY` + HMAC-SHA256 query 签名、Spot/Perp symbol rules、order book
snapshot、balances、positions、fee-rate、place/cancel/cancel-all/query/open
orders、recent fills、Spot/Perp batch place、Spot/Perp batch cancel、perp
amend、Spot OCO order list、perp leverage、position mode、multi-assets mode、
margin type helper、public WS trade/ticker/depth/kline subscription spec、
private listen-key WS order/fill/account subscription spec、Ping/Pong 心跳、
public/private parser fixtures、gateway app env wiring、request-spec/parser
测试和错误分类。倒计时撤全、OTO order list、Spot amend、perp OCO/order list
当前显式不声明支持或返回 `Unsupported`。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/bingx/`：config、signing、
  transport、public/private parser、REST/WS request-spec 实现和离线测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`：注册 `bingx` named
  adapter 和 `BingxGatewayConfig`。
- `crates/rustcta-exchange-gateway/src/lib.rs`、`apps/gateway/src/config.rs`：
  `BingxGatewayConfig` 导出、gateway app `bingx` REST base URL 和私有凭据
  env wiring。
- `tests/fixtures/exchanges/bingx/`：symbol、contracts、orderbook、balance、
  position、order ack、fill、error fixture。
- `docs/交易所网关/适配器/bingx_adapter.md`、`config/bingx_gateway_example.yml`、
  `config/spot_exchanges_example.yml`：endpoint mapping、认证规则、错误分类、
  gateway app env 示例和能力限制。

验证记录：

- `CARGO_TARGET_DIR=target/bingx-task-check cargo check -p rustcta-exchange-gateway --lib`
  通过。
- `CARGO_TARGET_DIR=target/bingx-task-check cargo test -p rustcta-exchange-gateway bingx_ --lib`
  通过，6 passed / 1 ignored live readonly。
- `logs/bingx_readonly_public_2026-06-07.json` 已记录 BingX public
  readonly 实盘探测通过；当前环境缺少 `BINGX_API_KEY`/`BINGX_API_SECRET`，
  authenticated readonly live gate 已记录为 skipped。

### 第二批：任务 11-20

| 任务编号 | 交易所 | 建议 adapter id | 产品目标 | 优先原因 |
| --- | --- | --- | --- | --- |
| 任务 11 | Poloniex | `poloniex` | Spot + Futures | 历史交易所，Spot/Futures 覆盖较广 |
| 任务 12 | AscendEX | `ascendex` | Spot + Futures | altcoin 覆盖和合约产品有增量价值 |
| 任务 13 | XT.com | `xt` | Spot + USDT perp | 长尾交易对覆盖较多 |
| 任务 14 | CoinW | `coinw` | Spot + Futures | 合约市场活跃，适合永续扫描 |
| 任务 15 | Bitrue | `bitrue` | Spot + Futures | altcoin 现货和合约覆盖可补充流动性 |
| 任务 16 | Tapbit | `tapbit` | Spot + USDT perp | 合约市场候选，适合中优先级接入 |
| 任务 17 | WEEX | `weex` | Spot + Futures | 官方 API 标明现货和合约交易能力 |
| 任务 18 | OrangeX | `orangex` | Spot + USDT perp | 合约流动性候选，先做请求规格验证 |
| 任务 19 | Deepcoin | `deepcoin` | Spot + USDT perp | 合约产品活跃，适合补永续候选 |
| 任务 20 | Bitunix | `bitunix` | Spot + USDT perp | 新兴合约交易所，适合第二批验证 |

#### 任务 20 Bitunix 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Bitunix Spot + USDT perpetual REST/WebSocket request-spec 初版。当前覆盖官方 Spot REST base URL `https://openapi.bitunix.com`、Futures REST base URL `https://fapi.bitunix.com`、Bitunix 双 SHA256 header 签名、Spot/USDT perpetual symbol rules、order book snapshot、Spot/perp balances、perp positions、place/cancel/cancel-all/query/open orders、Spot/perp batch place、Spot/perp batch cancel、Spot quote-sized buy market order、perp amend、perp cancel-all、recent fills、Spot public WS request-response spec、Futures public/private WS subscribe/login spec、15 秒 ping 心跳策略、Futures depth/order/fill/balance/position stream parser、request-spec/parser 测试和错误分类。独立 WebSocket runtime loop、Spot private WebSocket、Spot client order id、Spot post-only/IOC/FOK、Spot amend order、order list、perp quote-sized market order、无 order id 的 Spot fills 和已验证 fee-rate endpoint 当前显式不声明支持或返回 `Unsupported`/fallback source marker；Bitunix Futures TP/SL、杠杆、保证金、持仓模式以 adapter 专用 helper 暴露，尚未进入统一 `ExchangeClient` 标准 trait。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/bitunix/`：config、signing、transport、public/private REST、WebSocket request specs/parser、parser 和离线 mock REST/WS 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `BitunixGatewayConfig` 导出。
- `config/bitunix_gateway_example.yml`、`docs/交易所网关/适配器/bitunix_adapter.md`：默认 disabled 配置示例、endpoint mapping、签名规则和能力边界。

Endpoint mapping：

| 标准能力 | Bitunix 原生接口 | 当前实现 |
| --- | --- | --- |
| spot symbol rules | `GET /api/spot/v1/common/coin_pair/list` | 解析紧凑 symbol、base/quote、precision、min price/volume、交易状态 |
| perp symbol rules | `GET /api/v1/futures/market/trading_pairs` | 解析 `BTCUSDT` 等 USDT-M 交易对，按 `MarketType::Perpetual` 标准化 |
| spot order book | `GET /api/spot/v1/market/depth` | snapshot parser，兼容 object level `price/volume` 格式 |
| perp order book | `GET /api/v1/futures/market/depth` | snapshot parser，兼容二维数组 level 格式，depth 归一到官方档位 |
| private REST signing | header `api-key/nonce/timestamp/sign` | `sign = SHA256(SHA256(nonce + timestamp + apiKey + sortedQuery + compactBody) + secret)`；测试断言 secret 不进入 path/query/body |
| spot/perp balances | `GET /api/spot/v1/user/account`, `GET /api/v1/futures/account` | Spot account 与 USDT margin account balance parser |
| perp positions | `GET /api/v1/futures/position/get_pending_positions` | 解析数量、entry、liquidation、PnL、leverage、LONG/SHORT/BOTH |
| order lifecycle | Spot `/order/place_order`, `/order/place_order/batch`, `/order/cancel`, `/order/detail`, `/order/pending/list`; Futures `/trade/place_order`, `/trade/batch_order`, `/trade/modify_order`, `/trade/cancel_orders`, `/trade/cancel_all_orders`, `/trade/get_order_detail`, `/trade/get_pending_orders` | Spot limit/market 数字枚举请求体、Spot 组合式 cancel-all；Perp limit/market/IOC/FOK/post-only、client id、reduce-only close 语义、perp amend quantity、batch place/cancel、cancel/query/open orders |
| Spot quote market | `POST /api/spot/v1/order/place_order` | 仅 Spot BUY quote-sized market，映射 `amount`；Spot SELL 和 Perp quote-sized market 显式 `Unsupported` |
| fills | Spot `/order/deal/list`, Futures `/trade/get_history_trades` | Spot 按 order id 查询成交；perp 支持 recent history filters |
| WebSocket | Spot `wss://openapi.bitunix.com:443/ws-api/v1`; Futures `wss://fapi.bitunix.com/public/` and `/private/` | Spot public depth/ticker/kline authenticated request-response payload；Futures public depth/trade/ticker/kline subscribe/unsubscribe，private login/order/balance/position subscribe，15 秒 ping/pong heartbeat spec，order push 同时解析标准 order update 与 fill |
| testnet | 官方文档未确认稳定 sandbox | base URL 可配置，默认生产 URL；真实交易前必须 dry-run/live-dry-run |

#### 任务 11 Poloniex 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Poloniex Spot + USDT perpetual REST 接入、批量订单能力和 WebSocket session 初版。当前覆盖官方 REST base URL、Spot/Perp WS URL、Poloniex V2 `key/signTimestamp/signature` HMAC-SHA256 base64 签名、Spot/Perp symbol rules、order book snapshot、balances、positions、Spot fee-rate、place/cancel/cancel-all/query/open orders、Spot amend、Spot/Perp batch place、Spot/Perp batch cancel、recent fills、public/private WS subscription payload、WS auth payload、ping/pong heartbeat payload、stream reconnect policy、public/private WS session state wrapper、public book/trade/ticker/candle parser、private order/fill/balance/position parser、perp position mode/leverage/isolated margin extension request routing、request-spec/parser/session 测试和错误分类。Order list、futures amend、perpetual fee readback 当前显式不声明支持或返回 `Unsupported`。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/poloniex/`：config、signing、transport、public/private REST、WebSocket request-spec/parser/session、parser 和离线 mock REST/WS 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `PoloniexGatewayConfig` 导出。
- `config/spot_exchanges_example.yml`、`docs/交易所网关/适配器/poloniex_adapter.md`：默认 disabled 配置示例、endpoint mapping、签名规则和能力边界。

Endpoint mapping：

| 标准能力 | Poloniex 原生接口 | 当前实现 |
| --- | --- | --- |
| spot symbol rules | `GET /markets` | 解析 `symbolTradeLimit`、base/quote、price/quantity precision、min/max quantity/notional、交易状态 |
| perp symbol rules | `GET /v3/market/allInstruments` | 解析 USDT perpetual instruments，按 `MarketType::Perpetual` 标准化 |
| spot order book | `GET /markets/{symbol}/orderBook` | snapshot parser，兼容 Poloniex flat level 格式，depth 归一到官方档位 |
| perp order book | `GET /v3/market/orderBook` | snapshot parser，解析 `symbol`、`limit`、bid/ask 和交易所时间戳 |
| private REST signing | `METHOD\npath\nsorted params` + `requestBody` | 独立 `signing.rs`，base64 HMAC-SHA256；测试断言 secret 不进入 path/query/body |
| balances | `GET /accounts/balances`, `GET /v3/account/balance` | Spot/Perp balances parser，按请求 market type 标记 |
| positions | `GET /v3/trade/position/opens` | 解析数量、entry、mark、liquidation、PnL、leverage、LONG/SHORT/BOTH |
| spot fee-rate | `GET /feeinfo` | 解析 maker/taker fee，按请求 symbol 归一 |
| order lifecycle | `POST /orders`, `PUT /orders/{id}`, `DELETE /orders/{id}`, `DELETE /orders`, `GET /orders/{id}`, `GET /orders`; `POST/DELETE/GET /v3/trade/order*`, `DELETE /v3/trade/allOrders` | limit/market/post-only/IOC/FOK、client order id、reduce-only/position side、Spot amend quantity/client id、cancel、cancel-all、query/open orders |
| batch orders | Spot `POST /orders/batch`, `DELETE /orders/cancelByIds`; Perp `POST /v3/trade/orders`, `DELETE /v3/trade/batchOrders` | Spot/Perp batch place 和 batch cancel；Spot 最大 20，Perp 最大 10，禁止混合 market type |
| fills | `GET /trades`, `GET /v3/trade/order/trades` | 解析成交、手续费、maker/taker、成交时间 |
| advanced order boundary | futures amend、OCO/OTO order list | 当前显式 `Unsupported`，不使用 cancel+replace 伪造 futures amend/order-list 语义 |
| WebSocket | Spot `wss://ws.poloniex.com/ws/public/private`; Futures V3 `wss://ws.poloniex.com/ws/v3/public/private` | public/private subscription specs、WS auth payload、ping/pong heartbeat payload、stream reconnect policy、public/private session state wrapper、book/trade/ticker/candle/order/fill/balance/position parser，并将 order book 和 heartbeat 转为统一 `ExchangeStreamEvent` |
| testnet | 官方文档未确认稳定 sandbox | `rest_base_url` 可配置，默认生产 URL；真实交易前必须 dry-run/live-dry-run |

#### 任务 12 AscendEX 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Spot/Cash + Futures/perpetual REST 和 WebSocket request-spec/parser 初版，并已接入 `apps/gateway` 本地 gateway 配置。当前覆盖官方 REST base URL、futures sandbox base URL 可配置、AscendEX `x-auth-key/x-auth-timestamp/x-auth-signature` Base64 HMAC-SHA256 签名、account group 私有路径、Spot/Futures symbol rules、order book snapshot、balances、futures positions、fee-rate、place/cancel/batch place/batch cancel/cancel-all/query/open orders、recent fills/order-history fallback、public/private WS subscription payload、WS auth payload、ping/pong heartbeat payload、public depth parser、private order/fill parser scope、request-spec/parser 测试和错误分类。Amend order、order list、stop order、杠杆/保证金/持仓模式设置、dedicated fills 端点、private account/balance/position WS 标准事件解析和真实长连接 supervisor runtime 当前显式不声明支持或作为平台层后续接入。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/ascendex/`：config、signing、transport、public/private REST、WebSocket specs/parsers、parser 和离线 mock REST/WS 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `AscendexGatewayConfig` 导出。
- `apps/gateway/src/config.rs`、`config/spot_exchanges_example.yml`、`docs/交易所网关/适配器/ascendex_adapter.md`：本地 gateway env wiring、默认 disabled 配置示例、endpoint mapping、签名规则和能力边界。

Endpoint mapping：

| 标准能力 | AscendEX 原生接口 | 当前实现 |
| --- | --- | --- |
| spot symbol rules | `GET /api/pro/v1/cash/products` | 解析 `symbol`、base/quote、tick/lot、min/max quantity/notional、precision |
| futures symbol rules | `GET /api/pro/v2/futures/contract` | 解析 `BTC-PERP` 类合约、settlement asset、price filter、lot size filter |
| spot/futures order book | `GET /api/pro/v1/depth` | snapshot parser，保留 `seqnum` 和 `ts`；futures depth 作为 scan-only fallback |
| private REST signing | `<timestamp>+<api-path>` | 独立 `signing.rs`，Base64 HMAC-SHA256；测试断言 secret 不进入 path/query/body |
| balances | `GET /{group}/api/pro/v1/cash/balance`, `/api/pro/v2/futures/position` | Spot balances 和 futures collateral snapshot，按请求 market type 标记 |
| positions | `GET /{group}/api/pro/v2/futures/position` | 解析数量、side、entry、mark、PnL、leverage |
| fee-rate | `GET /{group}/api/pro/v1/spot/fee`, `/futures/fee` | 解析 product fee maker/taker，按请求 symbol 归一 |
| order lifecycle | `POST/DELETE /{group}/api/pro/v1/cash/order`, `/api/pro/v2/futures/order`, `/order/batch` | limit/market/post-only/IOC/FOK、client order id、reduce-only、cancel、batch place/cancel、cancel-all、query/open orders |
| fills | `GET /{group}/api/pro/v1/cash/order/hist/current`, `/api/pro/v2/futures/order/hist/current` | 官方无独立 fills REST；第一版从已成交订单聚合字段生成 reconciliation fills |
| WebSocket | public/private stream endpoints | 支持 public depth/trades/bbo/bar subscription payload、private auth/order/futures-order subscription payload、ping/pong heartbeat payload、public depth parser 和 private order/fill parser scope；private account/balance/position 标准事件解析与真实长连接 runtime 后续接入 |
| testnet | futures sandbox `https://api-test.ascendex-sandbox.com` | `rest_base_url` 可配置；Spot sandbox 未作为稳定产品线声明 |

#### 任务 13 XT.com 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` XT.com Spot + USDT-M perpetual REST 与 WebSocket request-spec/parser 对标 Binance 统一 API 面。当前覆盖官方 Spot REST base URL `https://sapi.xt.com`、USDT-M Futures REST base URL `https://fapi.xt.com`、Spot 与 Futures 两套 `validate-*` HMAC-SHA256 签名、Spot/USDT-M symbol rules、order book snapshot、balances、perp positions、fee metadata、place/cancel/cancel-all/query/open orders、Spot quote-sized market buy、amend order、native batch place/cancel、recent fills、Spot/USDT-M public/private WS subscription specs、private listenKey 获取、ping/pong 心跳策略、public book/trade parser、private order/fill/balance/position parser、request-spec/parser 测试和错误分类。Binance-style OCO/OTO order list、杠杆/保证金/持仓模式设置、perpetual quote-sized market order 和 reduce-only 下单当前显式不声明支持或返回 `Unsupported`；XT Futures TP/SL/trigger-order API 与当前 `OrderListRequest` 的 Binance OCO/OTO 语义不等价，未用组合逻辑伪造。

验证：`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/xt-task-check cargo test -p rustcta-exchange-gateway adapters::xt:: --lib --message-format short` 已通过，18 个 XT adapter 测试通过，0 失败，1 个 live-readonly preflight 按设计 ignored，733 filtered out；sandbox 内本地 mock REST 绑定 `127.0.0.1` 会被拦截，完整目标集已按需非 sandbox 重跑。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/xt/`：config、signing、transport、public/private REST、parser 和离线 mock REST 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `XtGatewayConfig` 导出。
- `config/xt_gateway_example.yml`、`docs/交易所网关/适配器/xt_adapter.md`：默认 disabled-private-REST 配置示例、endpoint mapping、签名规则和能力边界。

Endpoint mapping：

| 标准能力 | XT.com 原生接口 | 当前实现 |
| --- | --- | --- |
| spot symbol rules | `GET /v4/public/symbol` | 解析 `symbol`、base/quote、price/quantity precision、filter tick/min/max/notional、maker/taker fee 和交易状态 |
| USDT-M symbol rules | `GET /future/market/v3/public/symbol/list` | 解析 USDT-M perpetual metadata，按 `MarketType::Perpetual` 标准化 |
| spot order book | `GET /v4/public/depth` | snapshot parser，depth 归一到 1..500 |
| USDT-M order book | `GET /future/market/v1/public/q/depth` | snapshot parser，解析 `a/b/t/u`，depth 归一到 1..50 |
| private REST signing | Spot `validate-algorithms/appkey/recvwindow/timestamp + #METHOD#path#query/body`；Futures `validate-appkey/timestamp + #path#query/body` | 独立 `signing.rs`，lowercase hex HMAC-SHA256；测试断言 secret 不进入 path/query/body |
| balances | `GET /v4/balances`, `GET /future/user/v1/balance/list` | Spot/Futures balances parser，按请求 market type 标记 |
| positions | `GET /future/user/v1/position/list` | 解析数量、entry、mark、PnL、leverage、LONG/SHORT |
| fee-rate | public symbol metadata | 解析 Spot `makerFeeRate/takerFeeRate` 与 Futures `makerFee/takerFee` |
| order lifecycle | `POST /v4/order`, `PUT/DELETE/GET /v4/order/{orderId}`, `POST/DELETE /v4/batch-order`, `GET/DELETE /v4/open-order`; `POST /future/trade/v1/order/create/update/cancel/cancel-batch/cancel-all/list-open-order`, `POST /future/trade/v2/order/atomic-create-batch`, `GET /future/trade/v1/order/detail` | limit/market/post-only/IOC/FOK、client order id、position side、Spot quote market buy、amend、native batch place/cancel、cancel、cancel-all、query/open orders |
| fills | `GET /v4/trade`, `GET /future/trade/v1/order/trade-list` | 解析成交、手续费、maker/taker、成交时间 |
| order list / OCO/OTO | Spot 无已验证原生 OCO/OTO；Futures TP/SL/trigger-order 与 Binance OCO/OTO 语义不等价 | 显式 `Unsupported`，不使用 cancel+replace 或 TP/SL 伪造 |
| WebSocket | Spot `wss://stream.xt.com/public/private`; Futures `wss://fstream.xt.com/ws/market`, `wss://fstream.xt.com/ws/user` | public/private subscription specs、Spot `/v4/ws-token`、Futures `/future/user/v1/user/listen-key`、text `ping`/`pong` heartbeat policy、book/trade/order/fill/balance/position parser；共享 stream supervisor 负责真实长连接和重连 |
| testnet | 官方文档未确认稳定 sandbox | base URL 可配置，默认生产 URL；真实交易前必须 dry-run/live-dry-run |

#### 任务 16 Tapbit 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Tapbit Spot + USDT perpetual REST + public WebSocket 订阅/解析规格。当前覆盖官方 Spot REST base URL `https://openapi.tapbit.com/spot`、USDT perpetual REST base URL `https://openapi.tapbit.com/swap`、public WebSocket `wss://ws-openapi.tapbit.com/stream/ws`、`ACCESS-KEY/ACCESS-SIGN/ACCESS-TIMESTAMP` HMAC-SHA256 hex 签名、Spot/USDT perpetual symbol rules、order book snapshot、Spot/perp server time、ticker、public trades、candles、perp funding-rate venue-specific helper、Spot/perp balances、perp positions、Spot public fee-rate、Spot/perp limit-GTC 下单、撤单、Spot 原生 batch place/cancel、perp batch place/cancel 顺序 fallback、组合式 cancel-all、query/open orders、perp recent fills、Spot/perp public WS order book/ticker subscribe payload、public WS order book/ticker/ack/ping/pong parser、public order book 标准 `OrderBookSnapshot` 事件转换、ping/pong 标准 `Heartbeat` 事件转换、官方 5/10/50/100/200 深度档位、5 秒 ping/`pong` 心跳和重连策略、request-spec/parser 测试和错误分类。Spot market/quote-sized market/post-only/IOC/FOK、client order id、Spot dedicated private fills、amend/order list/TP-SL/杠杆/保证金/持仓模式设置、public trades/candle WS topic、private WebSocket auth/subscription 和完整 socket task orchestration 当前显式不声明支持或返回 `Unsupported`。官方 Spot/USDT perpetual WebSocket 文档当前只列 public order book/ticker 与心跳行为，未发布 private order/account stream topic，因此不硬编码伪 private WS；ticker 仍为 typed parser 输出，因为共享 stream model 尚无 ticker 事件变体。

验证：`rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/tapbit/streams.rs crates/rustcta-exchange-gateway/src/adapters/tapbit/stream_tests.rs` 已通过；`python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/tapbit/endpoint_mapping.yaml` 已通过；`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway tapbit --lib --message-format short` 已通过，20 个 Tapbit 测试通过，0 失败，732 filtered out。sandbox 内本地 mock REST 绑定 `127.0.0.1` 会被拦截，完整目标集已按需非 sandbox 重跑。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/tapbit/`：config、signing、transport、public/private REST、parser 和离线 mock REST 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `TapbitGatewayConfig` 导出。
- `config/spot_exchanges_example.yml`、`docs/交易所网关/适配器/tapbit_adapter.md`：默认 disabled 配置示例、endpoint mapping、签名规则和能力边界。

Endpoint mapping：

| 标准能力 | Tapbit 原生接口 | 当前实现 |
| --- | --- | --- |
| spot symbol rules | `GET /api/spot/instruments/trade_pair_list` | 解析 `trade_pair_name`、base/quote、price/amount precision、min amount/notional、maker/taker fee |
| perp symbol rules | `GET /api/usdt/instruments/list` | 解析 `contract_code`、price tick、min/max amount，按 `MarketType::Perpetual` 标准化 |
| spot order book | `GET /api/spot/instruments/depth` | snapshot parser，depth 归一为 5/10/50/100/200 |
| perp order book | `GET /api/usdt/instruments/depth` | scan-only snapshot parser，`BTC-SWAP` depth 请求映射为 `BTC`，depth 归一为 5/10/50/100/200 |
| public market helpers | Spot `/api/spot/instruments/current/timestamp`, `/ticker_one`, `/trade_list`, `/candles`; USDT `/api/v1/usdt/time`, `/api/usdt/instruments/ticker_one`, `/trade_list`, `/candles`, `/funding_rate` | 覆盖 server time、ticker、public trades、candles、perp latest funding rate parser 和 request-spec 测试 |
| private REST signing | `timestamp + METHOD + path + ?query + body` | 独立 `signing.rs`，lowercase hex HMAC-SHA256；测试断言 secret 不进入 path/query/body |
| spot/perp balances | `GET /api/v1/spot/account/list`, `GET /api/v1/usdt/account` | 解析 total/available/frozen/equity，按请求 asset 过滤 |
| perp positions | `GET /api/v1/usdt/position_list` | 解析数量、entry、mark、PnL、leverage、LONG/SHORT/NET |
| spot fee-rate | `GET /api/spot/instruments/trade_pair_list` | 从公开交易对 metadata 读取 maker/taker fee |
| order lifecycle | Spot `POST /api/v1/spot/order`, `POST /api/v1/spot/cancel_order`, `POST /api/v1/spot/batch_cancel_order`, `GET /api/v1/spot/order_info`, `GET /api/v1/spot/open_order_list`; Perp `POST /api/v1/usdt/order`, `POST /api/v1/usdt/cancel_order`, `GET /api/v1/usdt/order_info`, `GET /api/v1/usdt/open_order_list` | Spot/perp limit-GTC、order-id cancel/query/open orders、perp reduce-only/position side；Spot cancel-all 先查 open orders 再按 id batch cancel；perp cancel-all 通过 open orders + 单撤组合 |
| batch place/cancel | Spot `POST /api/v1/spot/batch_order`, `POST /api/v1/spot/batch_cancel_order`; Perp `POST /api/v1/usdt/order`, `POST /api/v1/usdt/cancel_order` | Spot 原生批量下单/撤单；Perp 官方公开 USDT perpetual 文档未列 native batch endpoint，标准 batch place/cancel 通过已验证单笔端点顺序 fallback 并返回统一 batch response |
| fills | Spot 无独立私有 fills 端点；Perp `GET /api/v1/usdt/fills` | Spot 仅按订单字段 reconciliation；perp 解析成交、手续费、成交时间 |
| USDT perpetual private REST | `GET /api/v1/usdt/account`, `/position_list`, `/fills`, `POST /api/v1/usdt/order`, `/cancel_order`, `/order_info`, `/open_order_list` | 已启用 private REST request-spec/parser 基线 |
| WebSocket | `wss://ws-openapi.tapbit.com/stream/ws` | 已实现 Spot/perp public order book/ticker 订阅 payload、200 档 depth、order book/ticker/ack/ping/pong parser、public book 标准 `OrderBookSnapshot` 转换、ping/pong 标准 `Heartbeat` 转换与 5 秒 ping/`pong` 心跳/重连策略；private WS 和完整 socket task orchestration 显式 `Unsupported` |
| testnet | 官方文档未确认稳定 sandbox | base URL 可配置，默认生产 URL；真实交易前必须 dry-run/live-dry-run |

#### 任务 17 WEEX 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Spot + USDT-M Futures REST 初版，并补齐 WEEX WebSocket 订阅协议面。当前覆盖官方 Spot REST base URL `https://api-spot.weex.com`、Futures REST base URL `https://api-contract.weex.com`、WEEX V3 `ACCESS-*` header 认证、`timestamp + METHOD + path_with_query + body` HMAC-SHA256 Base64 签名、Spot/Futures symbol rules、order book snapshot、balances、Futures positions、fee-rate、place/cancel/cancel-all/query/open orders、recent fills、Spot/Futures native batch place/cancel、Spot/Futures public/private WebSocket subscribe payload、private WS header auth、心跳 PING/PONG helper、基础 stream parser、gateway app `RUSTCTA_WEEX_*` 环境变量注册/凭据注入、request-spec/parser 测试和错误分类。独立长连接 runtime loop、amend order、order list、杠杆/保证金/持仓模式设置和 reduce-only 下单当前显式不声明支持或返回 `Unsupported`。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/weex/`：config、signing、transport、public/private REST、parser 和离线 mock REST 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `WeexGatewayConfig` 导出。
- `apps/gateway/src/config.rs`：gateway app `weex` 注册、Spot/contract REST base URL override、`WEEX` 私有凭据/passphrase env wiring 和 redacted debug 覆盖。
- `config/spot_exchanges_example.yml`、`docs/交易所网关/适配器/weex_adapter.md`：默认 disabled 配置示例、endpoint mapping、签名规则和能力边界。

Endpoint mapping：

| 标准能力 | WEEX 原生接口 | 当前实现 |
| --- | --- | --- |
| spot/futures symbol rules | `GET /api/v3/exchangeInfo`, `GET /capi/v3/market/exchangeInfo` | 解析 `BTCUSDT` 等紧凑 symbol、base/quote、tick/step、min/max quantity、交易状态和 fee metadata |
| spot/futures order book | `GET /api/v3/market/depth`, `GET /capi/v3/market/depth` | snapshot parser，depth 归一到官方 `15` 或 `200` 档 |
| private REST signing | `ACCESS-KEY`, `ACCESS-SIGN`, `ACCESS-PASSPHRASE`, `ACCESS-TIMESTAMP` | 独立 `signing.rs`，Base64 HMAC-SHA256；测试断言 secret 不进入 path/query/body |
| balances | `GET /api/v2/account/assets`, `GET /capi/v3/account/balance` | Spot/Futures balances parser，按请求 market type 标记 |
| positions | `GET /capi/v3/account/position/allPosition` | 解析数量、side、entry/mark/liquidation、PnL、leverage |
| fee-rate | Spot 从 `exchangeInfo` fee metadata 解析；Futures `GET /capi/v3/account/commissionRate` | 解析 maker/taker fee，按请求 symbol 归一 |
| order lifecycle | `POST/DELETE/GET /api/v3/order`, `DELETE/GET /api/v3/openOrders`; `POST/DELETE/GET /capi/v3/order`, `/openOrders`, `/allOpenOrders` | limit/market/IOC/FOK、client order id、position side、cancel、cancel-all、query/open orders |
| batch place/cancel | Spot `POST /api/v2/trade/batch-orders`, `DELETE /api/v3/order/batch`; Futures `POST/DELETE /capi/v3/batchOrders` | 按单一 market type/symbol 校验后发送官方 native JSON batch body |
| fills | `GET /api/v3/myTrades`, `GET /capi/v3/userTrades` | 解析成交、手续费、maker/taker、成交时间 |
| WebSocket | Spot/Futures public/private WS | `subscribe_public_stream` / `subscribe_private_stream` 返回订阅 spec id；实现官方 `SUBSCRIBE`、private WS header auth、`PONG` helper 和基础 public/private stream parser；独立长连接 runtime loop 仍由上层 supervisor 接入 |
| simulation/testnet | Futures sim endpoints under `/capi/v3/sim/*` | 第一版未启用，默认生产 URL；真实交易前必须 dry-run/live-dry-run |

验证记录：

- `rustfmt --edition 2021 apps/gateway/src/config.rs crates/rustcta-exchange-gateway/src/adapters/weex/*.rs` 已通过。
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/weex-task-check cargo check -p rustcta-exchange-gateway --lib` 已通过（仅既有 warning）。
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/weex-task-check cargo test -p rustcta-exchange-gateway weex -- --nocapture` 已通过：11 passed，1 个 live-readonly 凭据门控测试 ignored。
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/weex-app-check cargo test -p rustcta-gateway config_should_wire_weex_private_gateway_adapter -- --nocapture` 已通过。
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/weex-app-check cargo test -p rustcta-gateway config_should_parse_adapters_and_redirection_urls_without_secret_fields -- --nocapture` 已通过。

#### 任务 18 OrangeX 交付状态

状态：已完成 `rustcta-exchange-gateway` Spot + USDT perpetual JSON-RPC REST 与 WebSocket request-spec/parser 接入。当前覆盖官方 REST/WS base URL、Spot/Perp symbol rules、order book snapshot、fee-rate metadata 读取、OAuth `client_signature` HMAC-SHA256 签名工具、`/public/auth` one-shot bearer token 获取、带 bearer token 的私有 REST request-spec 路由、gateway 组合 batch place/cancel、public WebSocket subscribe/unsubscribe 与 ping/pong heartbeat request-spec/control parser、private WebSocket `/private/subscribe`/`/private/unsubscribe` request-spec、OAuth `access_token` params 注入、user.changes/user.asset 私有事件 parser、订单/成交/余额/仓位标准 stream event 转换、永续 `adjust_perpetual_leverage` 与 `adjust_perpetual_margin_type` venue-specific helper、错误分类、named adapter 注册和离线 request-spec/parser 测试。普通交易 amend、order list、持仓模式设置当前未在官方普通交易 API 中确认等价接口，不声明支持或返回 `Unsupported`；完整长连接 supervisor/reconnect runtime 仍由 gateway 平台层接入。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/orangex/`：config、signing、transport、public/private parser、REST 实现和离线 mock REST 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `OrangeXGatewayConfig` 导出。
- `apps/gateway/src/config.rs`：`orangex` gateway app env wiring，支持 `RUSTCTA_ORANGEX_REST_BASE_URL`、`RUSTCTA_ORANGEX_CLIENT_ID`、`RUSTCTA_ORANGEX_CLIENT_SECRET`、`RUSTCTA_ORANGEX_ACCESS_TOKEN`、`RUSTCTA_ORANGEX_API_KEY`、`RUSTCTA_ORANGEX_API_SECRET`。
- `docs/交易所网关/适配器/orangex_adapter.md`、`config/orangex_gateway_example.yml`：endpoint mapping、OAuth 签名规则、配置示例和能力边界。

Endpoint mapping：

| 标准能力 | OrangeX 原生接口 | 当前实现 |
| --- | --- | --- |
| spot symbol rules | JSON-RPC `/public/get_instruments` | `currency=SPOT, kind=spot`；解析 `instrument_name`、tick/qty/min_notional、active 状态 |
| perp symbol rules | JSON-RPC `/public/get_instruments` | request-spec 使用 `currency=USDT, kind=perpetual`；parser 过滤 `*-PERPETUAL` 合约并映射 `MarketType::Perpetual` |
| order book | JSON-RPC `/public/get_order_book` | snapshot parser，depth 归一到 1..100，解析 bid/ask、`timestamp` 和 `version` |
| fee-rate | JSON-RPC `/public/get_instruments` | 从 `maker_commission`、`taker_commission` metadata 按请求 symbol 解析 |
| private REST auth | JSON-RPC `/public/auth` | `client_signature` HMAC 工具、固定向量测试和 one-shot bearer token 获取；支持预置 `ORANGEX_ACCESS_TOKEN` |
| private balances/positions | `/private/get_assets_info`、`/private/get_positions` | bearer token 配置后启用 request-spec 和 parser；无 token 显式 `Unsupported` |
| private order lifecycle | `/private/buy`、`/private/sell`、`/private/cancel`、open/query/fill methods | bearer token 配置后启用 request-spec；amend/order-list 仍 `Unsupported` |
| batch place/cancel | 未确认官方原生 batch endpoint | gateway 层顺序复用已签名单订单 place/cancel 路径，提供统一 batch API 面；非原子执行 |
| public WebSocket | `/public/subscribe`、`/public/unsubscribe`、`/public/ping`、text `PING` | order book、trades、ticker、candles subscription/unsubscription request-spec ack；subscription event/ack 与 ping/pong heartbeat control parser |
| private WebSocket | `/private/subscribe`、`/private/unsubscribe` | OAuth `access_token` 放入 JSON-RPC params；orders/fills/positions/account 映射 `user.changes.{kind}.{currency}.raw`，balances 映射 `user.asset.{asset_type}`；parser 转换为标准 stream events |
| perpetual leverage/margin | `/private/adjust_perpetual_leverage`、`/private/adjust_perpetual_margin_type` | venue-specific helper 覆盖 USDT perpetual 杠杆和 cross/isolated 保证金模式；不扩当前标准 `ExchangeClient` trait |

Validation：

- `rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/orangex/*.rs apps/gateway/src/config.rs` 已通过。
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo check -p rustcta-exchange-gateway --lib --message-format short` 已通过，仍有既有 warnings。
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway orangex --lib --message-format short` 已通过：22 passed，0 failed，729 filtered out，覆盖 OrangeX REST/auth/batch/WS/heartbeat/private stream/perpetual settings request-spec 与 parser；sandbox 内本地 mock REST 绑定 `127.0.0.1` 会被拦截，已按需非 sandbox 重跑。
- `CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/run/media/zhuhongwen/c600639e-ea8b-4df8-b2eb-52205f7a4f26/code/rustcta_push_work/.task18_audit_target cargo test -p rustcta-gateway config_should_wire_orangex_private_gateway_adapter -- --nocapture` 已通过。
- `CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/run/media/zhuhongwen/c600639e-ea8b-4df8-b2eb-52205f7a4f26/code/rustcta_push_work/.task18_audit_target cargo test -p rustcta-gateway config_should_parse_adapters_and_redirection_urls_without_secret_fields -- --nocapture` 已通过。

#### 任务 14 CoinW 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` CoinW Spot + USDT perpetual REST + WebSocket session/helper 初版。当前覆盖官方 REST base URL、Spot symbol rules、Spot 5/20 档 order book、USDT perpetual instruments、perpetual 20 档 order book、Spot balances/order lifecycle/quote-sized market order/cancel-all/query/open orders/recent fills、perp balances/positions/fee readback/order lifecycle/query/open orders/recent fills、gateway 组合 batch place/cancel、perp native batch cancel、Spot/Futures public/private WebSocket subscription spec、public/private WS session `initial_requests`、ping/pong heartbeat/reconnect policy、public order book parser、private order/balance/position parser 到标准 `ExchangeStreamEvent`、CoinW Spot MD5 大写签名、Futures HMAC-SHA256 base64 签名、错误分类、named adapter 注册和离线 request-spec/parser 测试。独立 WebSocket runtime loop、amend order、OCO/OTO order list、reduce-only 下单、杠杆/保证金/持仓模式设置当前不声明支持或返回 `Unsupported`。

验证：`rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/coinw/*.rs` 已通过；`CARGO_TARGET_DIR=target/coinw-task14-check cargo check -p rustcta-exchange-gateway --lib` 已通过；`CARGO_TARGET_DIR=target/coinw-task14-check cargo test -p rustcta-exchange-gateway coinw_ --lib` 已通过 19 个 CoinW 测试（含签名向量、REST request-spec/parser、WS subscription/session/heartbeat/parser）。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/coinw/`：config、signing、transport、public/private parser、REST 实现、WebSocket subscription/session/heartbeat/parser specs 和离线 mock REST/WS 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `CoinwGatewayConfig` 导出。
- `config/spot_exchanges_example.yml`、`docs/交易所网关/适配器/coinw_adapter.md`：默认 disabled 配置示例、endpoint mapping、签名规则和能力边界。

Endpoint mapping：

| 标准能力 | CoinW 原生接口 | 当前实现 |
| --- | --- | --- |
| spot symbol rules | `GET /api/v1/public?command=returnSymbol` | 解析 `currencyPair`、base/quote、price/count precision、min/max qty/notional、交易状态 |
| spot order book | `GET /api/v1/public?command=returnOrderBook` | snapshot parser，depth 归一到官方 `5` 或 `20` |
| perp symbol rules | `GET /v1/perpum/instruments` | 解析 `base/name/quote`、price precision、min size、oneLotSize、max position、online 状态 |
| perp order book | `GET /v1/perpumPublic/depth` | snapshot parser，解析 `p/m` 价量和 `ts` 时间戳 |
| spot balances | `POST /api/v1/private?command=returnCompleteBalances` | 解析 available/onOrders，总额、可用和冻结余额 |
| spot order lifecycle | `doTrade`, `cancelOrder`, `cancelAllOrder`, `returnOrderStatus`, `returnOpenOrders` | limit/market/quote-sized market、cancel、cancel-all、query/open orders；`out_trade_no` 映射 client order id |
| spot fills | `POST /api/v1/private?command=returnUTradeHistory` | 解析 tradeID、orderNumber、price/qty/notional、fee、成交时间 |
| perp balances/positions/fees | `GET /v1/perpum/account/getUserAssets`, `/positions`, `/positions/all`, `/account/fees` | 解析 USDT 账户、当前仓位、maker/taker fee |
| perp order lifecycle | `POST/GET/DELETE /v1/perpum/order`, `GET /orders/open`, `DELETE /batchOrders` | place/cancel/query/open/fills；cancel-all 通过 open-orders + batchOrders 组合 |
| batch place/cancel | gateway sequential fallback；perp native batch cancel | 标准 batch place/cancel 返回统一 batch response |
| WebSocket specs | Spot/Futures public/private streams | `subscribe_public_stream` / `subscribe_private_stream` 返回 subscription spec id；覆盖 Spot order book/trades/ticker/candles、perp depth/fills/ticker/candles、private order/assets/position 频道；提供 public/private session `initial_requests`、ping/pong heartbeat、reconnect policy、public order book parser 和 private order/balance/position 标准事件转换 |
| private REST signing | Spot MD5 uppercase；Futures base64 HMAC-SHA256 | 独立 `signing.rs`，带固定向量测试；request-spec 验证 secret 不进入请求 |
| runtime/amend/order-list/leverage | 独立 WS runtime、amend、OCO/OTO、杠杆/保证金/持仓模式 | 当前显式 `Unsupported` 或平台后续接入，不伪造 Binance 高级语义 |
| testnet | 官方文档未确认稳定 sandbox | `rest_base_url` 可配置，默认生产 URL；真实交易前必须后续补 dry-run/live-dry-run |

#### 任务 15 Bitrue 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Bitrue Spot + USDT perpetual REST 与 WebSocket request/parser/session specs。当前覆盖官方 Spot REST base URL、USDT-M Futures REST base URL、Spot `X-MBX-APIKEY` + HMAC-SHA256 query 签名、Futures `X-CH-APIKEY/X-CH-SIGN/X-CH-TS` payload 签名、Spot/Perp symbol rules、order book snapshot、balances、positions、fee-rate、place/cancel/query/open orders、recent fills、gateway 组合 batch place/cancel、symbol-scoped cancel-all、Spot public WS 官方 `event=sub` order book/trade/ticker/kline 频道与 `ping`/`pong` 心跳、Spot private `POST/PUT/DELETE /poseidon/api/v1/listenKey` 与 `user_order_update`/`user_balance_update` subscription/session spec、Futures private `POST/PUT/DELETE /user_stream/api/v1/listenKey` 与 `user_order_update`/`user_account_update` subscription/session spec、官方 futures `ORDER_TRADE_UPDATE` / `ACCOUNT_UPDATE` parser、public/private WS session helper（initial requests、heartbeat response、runtime state decision、文本消息到标准事件转换）、错误分类、named adapter 注册和离线 request-spec/parser 测试。Quote-sized Spot market order、Spot post-only/IOC/FOK/stop order、官方未确认的 futures public WS runtime、原生 batch endpoint、amend order、order list、杠杆/保证金/持仓模式设置当前显式不声明支持或返回 `Unsupported`。

验证：`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway bitrue --lib --message-format short` 已通过 21 个 Bitrue 测试（0 failed，696 filtered out）。全 crate 仍有大量第二/三批 adapter expansion 的 unused/dead-code warning，Bitrue 相关验证不依赖真实凭据或生产下单。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/bitrue/`：config、signing、transport、public/private parser、REST 实现、WebSocket subscription/session specs 和离线 request-spec/parser 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `BitrueGatewayConfig` 导出。
- `config/spot_exchanges_example.yml`、`docs/交易所网关/适配器/bitrue_adapter.md`：默认 disabled 配置示例、endpoint mapping、签名规则和能力边界。

Endpoint mapping：

| 标准能力 | Bitrue 原生接口 | 当前实现 |
| --- | --- | --- |
| spot symbol rules | `GET /api/v1/exchangeInfo` | 解析 `symbols[]`、PRICE_FILTER、LOT_SIZE、base/quote、precision、min/max qty、min notional |
| perp symbol rules | `GET /fapi/v1/contracts` | 解析 `E-BASE-QUOTE` USDT-M contracts，按 `MarketType::Perpetual` 标准化 |
| spot order book | `GET /api/v1/depth` | snapshot parser，Spot symbol 归一为 `BTCUSDT` |
| perp order book | `GET /fapi/v1/depth` | snapshot parser，perp contract 归一为 `E-BTC-USDT` |
| private REST signing | Spot query HMAC；Futures `timestamp + METHOD + path/query + body` HMAC | 独立 `signing.rs`，带固定向量测试；两套 header 不混用 |
| balances | `GET /api/v1/account`, `GET /fapi/v2/account` | Spot balances；Futures account balances，按请求 market type 标记 |
| positions | `GET /fapi/v2/account` | 从 `positionVos[].positions[]` 解析数量、entry、mark/index、liquidation/reduce price、PnL、leverage、BUY/SELL side |
| fee-rate | `GET /api/v1/account`, `GET /fapi/v2/commissionRate` | Spot account-level maker/taker commission 按整数 commission / 10000 转换为 rate；Futures open/close maker/taker fee |
| order lifecycle | `POST/DELETE/GET /api/v1/order`, `GET /api/v1/openOrders`; `POST /fapi/v2/order`, `/cancel`, `GET /fapi/v2/order`, `/openOrders` | Spot limit/market；Futures limit/market/post-only/IOC/FOK、client order id、reduce-only close mapping、cancel、query/open orders、组合 batch、组合 cancel-all |
| fills | `GET /api/v2/myTrades`, `GET /fapi/v2/myTrades` | 解析成交、手续费、maker/taker、成交时间；Spot `myTrades` 无 `side` 时使用 `isBuyer` 推导买卖方向 |
| WebSocket | Spot public WS、Spot private listenKey WS、Futures private listenKey WS | Spot public 官方 `event=sub` depth/trade/ticker/kline payload、`ping`/`pong` 心跳、`tick.buys/sells` depth parser；Spot private listenKey `POST/PUT/DELETE /poseidon/api/v1/listenKey` + `user_order_update/user_balance_update`；Futures private listenKey `POST/PUT/DELETE /user_stream/api/v1/listenKey` + `user_order_update/user_account_update` 与官方 `ORDER_TRADE_UPDATE` / `ACCOUNT_UPDATE` parser；session helper 输出 initial requests、heartbeat response 和标准 stream event；futures public WS 官方文档未确认，保持 best-effort/待 live-dry-run |
| testnet | 官方未确认独立 sandbox | Spot/Futures base URL 可配置，默认生产 URL；真实交易前必须 dry-run/live-dry-run |

#### 任务 19 Deepcoin 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Spot + USDT perpetual REST + WebSocket specs。当前覆盖官方 REST base URL、Deepcoin `DC-ACCESS-*` header + `timestamp + method + request_path + body` HMAC-SHA256 base64 签名、Spot/USDT 永续 symbol rules、order book snapshot、balances、positions、fee-rate、place/cancel/amend/batch-place/batch-cancel/query/open orders、perpetual cancel-all、recent fills、quote-sized Spot market order、public WS `book25/trade/market/kline` 订阅/退订 payload 与 parser、`ping`/`pong` heartbeat payload/parser、public/private WS session helper（initial requests、heartbeat request、runtime state decision、文本消息到标准事件转换）、private WS listenkey 获取/续期和 order/trade/account/position push parser、request-spec/parser 测试和错误分类。Order list、trigger/strategy order、Spot cancel-all、杠杆/保证金/持仓模式设置当前显式不声明支持或不在 `ExchangeClient` 标准 trait 内。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/deepcoin/`：config、signing、transport、public/private REST、public/private WS request specs/parser 和离线 mock 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `DeepcoinGatewayConfig` 导出。
- `docs/交易所网关/适配器/deepcoin_adapter.md`、`config/deepcoin_gateway_example.yml`：endpoint mapping、认证规则、配置示例和能力边界。

Endpoint mapping：

| 标准能力 | Deepcoin 原生接口 | 当前实现 |
| --- | --- | --- |
| spot/perp symbol rules | `GET /deepcoin/market/instruments` | `instType=SPOT/SWAP`，解析 tick/lot/min/max/状态，过滤 USDT 永续为 `MarketType::Perpetual` |
| spot/perp order book | `GET /deepcoin/market/books` | snapshot parser，`sz` 归一到 1..400 |
| private REST signing | `DC-ACCESS-KEY/SIGN/TIMESTAMP/PASSPHRASE` | 独立 `signing.rs`，base64 HMAC-SHA256；测试断言签名头存在且 secret 不进 path/query/body |
| balances | `GET /deepcoin/account/all-balances` | `accountType=spot/swapU`，按请求 market type 和资产过滤 |
| positions | `GET /deepcoin/account/positions` | 解析 USDT 永续数量、entry、mark/last、liquidation、PnL、leverage、LONG/SHORT |
| fee-rate | `GET /deepcoin/account/trade-fee` | Spot maker/taker 和 USDT 永续 makerU/takerU |
| order lifecycle | `POST /deepcoin/trade/order`, `POST /deepcoin/trade/cancel-order`, `POST /deepcoin/trade/replace-order`, `GET /deepcoin/trade/order`, `GET /deepcoin/trade/v2/orders-pending` | market/limit/post-only/IOC、client order id、Spot quote market、perp reduce-only/position side、cancel、amend quantity、query/open orders |
| batch orders | `POST /deepcoin/trade/batch-orders`, `POST /deepcoin/trade/batch-cancel-order` | 原生 batch place 最大 5 单；原生 batch cancel 按 exchange order id 批量撤单 |
| perp cancel-all | `POST /deepcoin/trade/swap/cancel-all` | 仅合约限价委托；Spot cancel-all 显式 `Unsupported` |
| fills | `GET /deepcoin/trade/fills` | 解析成交、手续费、maker/taker、成交时间 |
| public WebSocket | `book25`, `trade`, `market`, `kline` | V2 `Action/Symbol/LocalNo/ResumeNo/Topic` 订阅/退订 payload；`ping`/`pong` heartbeat；session helper 输出 order-book 标准 stream event |
| private WebSocket | `GET /deepcoin/listenkey/acquire`, `GET /deepcoin/listenkey/extend`, `wss://stream.deepcoin.com/v1/private?listenKey=...` | signed listenkey 获取/一小时滑动续期；session helper 支持 heartbeat、续期到期判断、order/trade/account/position 标准 stream event |
| testnet | 官方文档未确认稳定 sandbox | `rest_base_url` 可配置，默认生产 URL；真实交易前必须 read-only preflight 和 live-dry-run |

### 第三批：任务 21-30

| 任务编号 | 交易所 | 建议 adapter id | 产品目标 | 优先原因 |
| --- | --- | --- | --- | --- |
| 任务 21 | Backpack Exchange | `backpack` | Spot + Perp | 已完成：Spot/Perp REST、Ed25519 签名、原生 batch submit、组合 batch cancel、public/private WS 订阅/心跳 |
| 任务 22 | HashKey Global | `hashkey_global` | Spot + Futures where available | 合规区域市场覆盖 |
| 任务 23 | Biconomy Exchange | `biconomy` | Spot + Futures | 长尾交易对和合约候选 |
| 任务 24 | Coinstore | `coinstore` | Spot + Futures | 长尾交易对覆盖 |
| 任务 25 | DigiFinex | `digifinex` | Spot + Perpetual swap | 老牌 altcoin 交易所 |
| 任务 26 | BigONE | `bigone` | Spot + Contract where available | 区域和长尾市场补充 |
| 任务 27 | BloFin | `blofin` | Spot + Futures | 已完成：官方 OpenAPI 确认后实现 USDT 永续 REST + WS specs，并补齐资金费、K 线、杠杆/保证金/持仓模式、TPSL/Algo、划转和历史类专属接口；Spot trading 登记为 Unsupported |
| 任务 28 | CoinTR | `cointr` | Spot + Futures | 已完成：Spot + USDT futures REST、原生 batch place/cancel、public/private WS request/parser/session specs、15s ping/pong 心跳 |
| 任务 29 | CoinDCX | `coindcx` | Spot + Futures where API allows | 印度市场覆盖，需先确认 API 权限和地区限制 |
| 任务 30 | BitKan | `bitkan` | Spot + Contract where available | 已完成保守接入：named adapter/config/export/WS helper/Unsupported 边界；官方 OpenAPI 未确认前不声明交易能力 |

#### 任务 22 HashKey Global 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` HashKey Global Spot + futures REST 与 WebSocket request/parser/session specs。当前覆盖官方 Global REST/WS base URL、`X-HK-APIKEY` + query HMAC-SHA256 签名、Spot/Futures symbol rules、order book snapshot、balances、futures positions、fee-rate、place/cancel/query/open orders、recent fills、gateway 组合 batch place/cancel、symbol-scoped cancel-all、Spot/Futures public WS order book/trade/ticker/kline 订阅与 `ping`/`pong` 心跳、private listenKey URL 与 order/fill/balance/account/position subscription spec、public/private WS session helper（initial requests、heartbeat response、runtime state、文本消息到标准事件转换）、错误分类、named adapter 注册、gateway app `RUSTCTA_HASHKEY_GLOBAL_*` env wiring、配置示例和离线 request-spec/parser 测试。Quote-sized Spot market order、原生 atomic batch endpoint、amend order、order list、杠杆/保证金/持仓模式设置当前显式不声明支持或作为后续平台能力。

验证：`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/hashkey-task-check cargo test -p rustcta-exchange-gateway hashkey_global --lib --message-format short` 已通过，16 个 HashKey Global 测试通过，0 失败，736 filtered out；sandbox 内本地 mock REST 绑定 `127.0.0.1` 会被拦截，完整目标集已按需非 sandbox 重跑。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/hashkey_global/`：config、signing、transport、public/private parser、REST 实现、WebSocket subscription/session specs 和离线 request-spec/parser 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `HashKeyGlobalGatewayConfig` 导出。
- `apps/gateway/src/config.rs`：`hashkey_global` app adapter 注册、REST URL override 和 private credential env wiring。
- `config/hashkey_global_gateway_example.yml`、`docs/交易所网关/适配器/hashkey_global_adapter.md`：默认 disabled 配置示例、endpoint mapping、签名规则和能力边界。

Endpoint mapping：

| 标准能力 | HashKey Global 原生接口 | 当前实现 |
| --- | --- | --- |
| spot symbol rules | `GET /api/v1/exchangeInfo` | 解析 `symbols[]`、PRICE_FILTER、LOT_SIZE、base/quote、precision、min/max qty、min notional |
| futures symbol rules | `GET /api/v1/futures/exchangeInfo` | 解析紧凑 futures symbol，按 `MarketType::Perpetual` 标准化为 `BTCUSDT` |
| spot/futures order book | `GET /api/v1/depth`, `GET /api/v1/futures/depth` | snapshot parser，depth 归一到 1..1000 |
| private REST signing | `X-HK-APIKEY` + `timestamp/recvWindow/signature` query HMAC-SHA256 | 独立 `signing.rs`；request-spec 断言 signature 存在且 secret 不进入 path/query/body |
| balances/positions | `GET /api/v1/account`, `GET /api/v1/futures/account` | Spot balances；futures balances 和 positions，按请求 market type 标记 |
| fee-rate | `GET /api/v1/account`, `GET /api/v1/futures/commissionRate` | Spot account-level commission fallback；futures maker/taker commission |
| order lifecycle | `POST/DELETE/GET /api/v1/order`, `GET /api/v1/openOrders`; `POST/DELETE/GET /api/v1/futures/order`, `GET /api/v1/futures/openOrders` | Spot limit/market；futures limit/market/post-only/IOC/FOK、client id、reduce-only、position side、cancel、query/open orders |
| batch/cancel-all | gateway composed flow | batch place/cancel sequential fallback；cancel-all 先查 open orders 再逐单撤销 |
| fills | `GET /api/v2/myTrades`, `GET /api/v1/futures/myTrades` | 解析成交、手续费、maker/taker、成交时间；Spot `myTrades` 无 `side` 时使用 `isBuyer` 推导 |
| WebSocket | public quote WS + private listenKey WS | Spot/Futures public/private subscription specs、ping/pong heartbeat、public book/trade/ticker/candle parser、private order/fill/balance/account/position 标准事件转换 |

第三批里的部分交易所产品线、API 权限或地区可用性变化较快。实现前必须先做官方 API 文档和测试环境确认；如果没有可用的 Spot 或 linear perp API，该产品线只登记能力为 `Unsupported`，不硬凑实现。

#### 第三批余下任务统一验收矩阵

当前主文档已补齐 `任务 21 Backpack`、`任务 28 CoinTR` 和 `任务 30 BitKan` 的主计划文档、能力审计、endpoint mapping、配置示例和离线验收。Backpack/CoinTR 后续只保留生产前 read-only private key 预检与 live-dry-run promotion；BitKan 已完成保守 adapter 接入，但尚缺可验证官方 OpenAPI 后的 full trading/WS 升级，等待官方 REST/WS/签名规格确认后再提升 public/private REST、批量订单、取消和真实 WS 能力。

| 验收面 | CCXT 对标 | Binance 对标 | 余下任务要求 |
| --- | --- | --- | --- |
| Spot public/private REST | `fetchMarkets`、`fetchOrderBook`、`fetchTradingFees`、`fetchBalance`、`createOrder`、`cancelOrder`、`fetchOrder`、`fetchOpenOrders`、`fetchMyTrades` | `/api/v3/exchangeInfo`、`/depth`、`/account`、`/order`、`/openOrders`、`/myTrades` | 现货 symbol、book、balance、fee、下单、撤单、查单、开放订单、成交必须有 mapping 或 `Unsupported` |
| Linear perp public/private REST | `fetchPositions`、`createOrder`、`fetchFundingRate`、`fetchFundingRateHistory`、`fetchOpenInterest` | `/fapi/v1/exchangeInfo`、`/fapi/v2/positionRisk`、`/fapi/v1/order`、`/premiumIndex`、`/fundingRate`、`/openInterest` | USDT/USDC 永续优先覆盖 contracts、book、balance、position、order lifecycle、funding/mark/open interest |
| 批量提交订单 | `createOrders` | Spot/Futures batch order | 原生批量、组合批量、原子性、最大条数、部分失败语义必须写入 endpoint mapping、adapter 文档和测试；当前 bool capability 只表示是否开放标准方法 |
| 批量取消订单 | `cancelOrders`、`cancelAllOrders` | batch cancel、allOpenOrders | 批撤、symbol scoped cancel-all、全账户 cancel-all 分开验收；组合撤单必须在文档和测试中标注非原子，不能伪装成原生 batch |
| WebSocket 公开流 | `watchOrderBook`、`watchTrades`、`watchTicker`、`watchOHLCV` | depth/trade/ticker/kline | 输出订阅/退订 payload、parser fixture、sequence/checksum 策略、REST resync 入口 |
| WebSocket 私有流 | `watchOrders`、`watchMyTrades`、`watchBalance`、`watchPositions` | user data stream | 覆盖认证、订阅、订单/成交/余额/仓位 parser；缺私有流时使用 REST reconciliation fallback |
| 心跳和重连 | CCXT Pro keepalive 行为 | listenKey keepalive、ping/pong、stale reconnect | 明确 ping interval、pong timeout、stale timeout、server ping 响应、重连后 resubscribe/resync |
| 高级合约能力 | `setLeverage`、`setMarginMode`、`setPositionMode`、`cancelAllAfter` | leverage、marginType、dualSidePosition、countdownCancelAll | 只在官方原生支持时实现；否则 adapter 返回 `Unsupported`，由 supervisor/策略层兜底 |

#### 任务 21 Backpack Exchange 交付状态与余下计划

状态：仓库已新增 `backpack` adapter 和 `docs/交易所网关/适配器/backpack_adapter.md`，覆盖 Backpack Spot + Perp 的 REST、Ed25519 签名、symbol rules、order book、balances、positions、fees、single place/cancel/query/open/fills、quote market order、原生 batch place、组合式 batch cancel、symbol scoped cancel-all、public WS subscription specs、public trade/ticker/kline typed parser fixture、depth/bookTicker 标准 order book event 转换、私有 `account.orderUpdate`/`account.positionUpdate` signed subscribe payload、心跳 helper，以及 order/orderbook/position/heartbeat 标准事件 parser。单独 fill/balance/account 私有 WS channel 当前未在官方 WebSocket 规格中确认，保持显式 `Unsupported` 并通过 REST reconciliation 覆盖 fills/balances。主计划余下工作是接入统一 WebSocket supervisor 的真实连接、重连和 resync 流程，并补 read-only live report。

验证：`python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/backpack/endpoint_mapping.yaml` 已通过；`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/backpack-task-check cargo test -p rustcta-exchange-gateway backpack --lib --message-format short` 已通过，18 个 Backpack 测试通过，0 失败，734 filtered out；sandbox 内本地 mock REST 绑定 `127.0.0.1` 会被拦截，完整目标集已按需非 sandbox 重跑。

Endpoint mapping：

| 标准能力 | Backpack 原生接口/频道 | 当前实现与余下计划 |
| --- | --- | --- |
| Spot/perp symbol rules | `GET /api/v1/markets` | 已解析 `SPOT`/`PERP`、tick、step、min quantity/notional；余下补 CCXT `loadMarkets` 字段审计 |
| Spot/perp order book | `GET /api/v1/depth` | 已实现 snapshot parser；余下补 public WS depth 后的 REST resync 验收 |
| balances/positions/fees | `GET /api/v1/capital`、`GET /api/v1/position`、`GET /api/v1/account` | 已覆盖账户、perp 仓位、maker/taker bps；私有 WS positions 用 `account.positionUpdate`，余额用 REST reconciliation；余下补 read-only live report |
| order lifecycle | `POST/DELETE/GET /api/v1/order`、`GET /api/v1/orders` | 已覆盖 market/limit/post-only/IOC/FOK、numeric client id、reduce-only、query/open/fills；amend、OCO/OTO 继续 `Unsupported` |
| batch submit/cancel | `POST /api/v1/orders`；single cancel composed | 已实现原生 batch submit；batch cancel 是逐单组合撤单，必须在 capability 中保持非原子标注 |
| cancel-all | `DELETE /api/v1/orders` | 已支持 symbol scoped cancel-all；全账户 cancel-all 不默认开放 |
| perp public risk data | 官方待复核 | funding、funding history、open interest、mark/index dedicated public helpers 当前未在标准 adapter 中声明；若官方可确认则补 G3，否则显式 `Unsupported` |
| public WS | `trade`、`ticker`、`depth`、`bookTicker`、`kline` | 已有订阅/退订 payload；typed parser fixture 覆盖 trade/ticker/kline，depth/bookTicker 转标准 order book event；共享 `ExchangeStreamEvent` 暂无 public trade/ticker/candle 变体；无 checksum 确认前按 `BestEffortDelta` + REST snapshot resync 验收 |
| private WS | `account.orderUpdate`、`account.positionUpdate` | 已实现 `instruction=subscribe` Ed25519 signed subscribe payload、order/position parser tests；单独 fills/balances/account channel 因官方未确认保持 `Unsupported`，用 REST reconciliation |
| heartbeat | `PING`/`PONG` | 已有 heartbeat payload helper；统一 supervisor 需补 ping interval、pong timeout、stale timeout、断线重订阅和 resync 指标 |
| 高级合约能力 | 官方待复核 | leverage、margin mode、position mode、dead-man switch 当前不声明标准能力；只在官方原生支持且语义可无损映射时升级 |

#### 任务 28 CoinTR 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` CoinTR Spot + USDT futures REST 与 WebSocket request-spec/parser 初版，并已接入 named adapter 和 `apps/gateway` 配置入口。当前覆盖官方 REST base URL `https://api.cointr.com`、public/private WebSocket URL `wss://ws.cointr.com/v2/ws/public|private`、REST `ACCESS-*` header 私有认证、`timestamp + METHOD + path + query + body` Base64 HMAC-SHA256 签名、Spot/Futures symbol rules、order book snapshot、Spot/Futures balances、Futures positions、fee fallback、order lifecycle、Spot buy quote-sized market order、open orders、query order、recent fills、Spot/Futures native batch place、Spot/Futures native batch cancel、symbol-scoped cancel-all sweep、public WS `books5`/`trade`/`ticker`/`candle*` subscription specs、private WS order/fill/account/position channel specs、ping/pong heartbeat helper、public book 标准事件转换、public trade/ticker/candle typed parser、private order/fill/balance/position parser、named adapter 注册、gateway app `RUSTCTA_COINTR_*` env wiring、配置示例、endpoint mapping 文档和离线 request-spec/parser 测试。

官方文档复核结论：CoinTR 当前公开文档是 Bitget-like V2 API。已直接复核 domain、signature、Spot place/cancel order、WebSocket intro、Spot public WS trade/ticker/candle/depth 与 USDT futures public WS trade/ticker/candle/depth 页面；Spot 下单请求使用 `side=buy|sell`、`orderType=limit|market`、`force=gtc|ioc|fok|post_only`、`size`、`clientOid`，不是 Binance 风格 `quantity/type/newClientOrderId`。Public WS 按当前官方 envelope 使用 `op=subscribe` + `args[].instType/channel/instId`，Spot `instType=SPOT`，USDT futures `instType=USDT-FUTURES`。Spot batch cancel endpoint 按文档使用 `/api/v2/spot/trade/batch-cancel-order`。Readback 路由按 V2 族实现并有离线 mock 覆盖；生产使用前仍需要 read-only private key 预检确认账户权限和响应 envelope。

验证：

- `rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/cointr/streams.rs crates/rustcta-exchange-gateway/src/adapters/cointr/stream_tests.rs` 通过。
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway cointr --lib --message-format short` 通过：16 passed，0 failed。
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo check -p rustcta-exchange-gateway --lib --message-format short` 通过。
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo check -p rustcta-gateway --message-format short` 通过。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/cointr/`：config、signing、transport、public/private REST、parser、WebSocket subscription/session/heartbeat specs 和离线测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `CointrGatewayConfig` 导出。
- `apps/gateway/src/config.rs`：`cointr` app adapter 注册、REST URL override、API key/secret/passphrase env wiring 和 private adapter capabilities test。
- `docs/交易所网关/适配器/cointr_adapter.md`、`config/cointr_gateway_example.yml`：endpoint mapping、签名规则、配置示例和能力边界。

Endpoint mapping：

| 标准能力 | CoinTR 原生接口/频道 | 当前实现 |
| --- | --- | --- |
| Spot/Futures symbol rules | `GET /api/v2/spot/public/symbols`, `GET /api/v2/mix/market/contracts?productType=USDT-FUTURES` | 解析 base/quote、precision、tick/step、min quantity/min notional，按 `MarketType::Spot`/`Perpetual` 标准化 |
| Spot/Futures order book | `GET /api/v2/spot/market/orderbook`, `GET /api/v2/mix/market/orderbook` | snapshot parser，深度按 capability clamp |
| private signing | `ACCESS-KEY`, `ACCESS-SIGN`, `ACCESS-TIMESTAMP`, `ACCESS-PASSPHRASE`, `locale=en-US` | Base64 HMAC-SHA256；query string 按 key 排序后纳入签名 |
| balances/positions | `GET /api/v2/spot/account/assets`、`GET /api/v2/mix/account/accounts`、`GET /api/v2/mix/position/all-position` | 已覆盖 Spot/Futures balance 与 futures positions；Spot positions 返回空集合 |
| order lifecycle | Spot `/api/v2/spot/trade/place-order`, `/cancel-order`; Futures `/api/v2/mix/order/place-order`, `/cancel-order` | Spot limit/market/post-only/IOC/FOK、`clientOid`；Futures `USDT-FUTURES`、`marginCoin=USDT`、reduce-only、position side |
| quote market | Spot `/api/v2/spot/trade/place-order` market buy with `size` as quote amount | 已实现 Spot buy quote-sized market order；Spot sell 和 Futures quote-market 明确 `Unsupported` |
| batch place/cancel | Spot `/api/v2/spot/trade/batch-orders`, `/batch-cancel-order`; Futures `/api/v2/mix/order/batch-place-order`, `/batch-cancel-orders` | 原生 batch place/cancel；要求同一 market type |
| cancel-all | open-order sweep + single cancel | symbol-scoped sweep，避免未确认的 account-wide cancel 语义 |
| public WebSocket | `op=subscribe` + `books5`/`trade`/`ticker`/`candle*`; `instType=SPOT`/`USDT-FUTURES` | 订阅 payload 与 ping/pong heartbeat 已覆盖；order book 转标准事件，trade/ticker/candle 为 typed parser 输出。官方 depth push 含 timestamp/checksum 但无 sequence id；CRC32 校验、REST snapshot merge 与断线 resync 仍属统一 runtime 后续工作 |
| private WebSocket | login + `user.orders/user.fills/user.account/user.positions` | 已实现 signed login payload、订阅 payload、ping/pong heartbeat，以及 order/fill/account/position 标准事件 parser tests；真实私有 WS auth heartbeat 仍需 read-only live 验证 |
| 显式边界 | amend/order list、杠杆/保证金/持仓模式、mark/funding/open-interest helpers、dead-man switch | 当前不声明支持或返回 `Unsupported`；不使用伪实现 |

#### 任务 30 BitKan 保守接入状态与全量升级计划

状态：仓库已新增 `bitkan` 保守 adapter 目录、`docs/交易所网关/适配器/bitkan_adapter.md` 和 `config/bitkan_gateway_example.yml`，覆盖 named adapter 注册、`BitkanGatewayConfig` 导出、gateway app 配置入口、Spot/Perpetual market type 边界、public WS 订阅 payload helper、`{"op":"ping"}` 心跳 helper 和 30s ping / 45s pong timeout / 60s stale message reconnect policy。当前所有 Spot/Contract public/private REST 交易能力均保持 `false` capability 或 `bitkan.*_unverified` `Unsupported`，private WS 也保持 `bitkan.private_streams_unverified`。下一步必须先完成官方 API 与 CCXT `has` 能力复核；如果官方没有稳定交易 API、合约 API 或私有 WebSocket，则对应产品线继续登记 `Unsupported`，不得用网页接口或非公开协议硬凑。

实施计划：

| 阶段 | 目标 | 必须产出 |
| --- | --- | --- |
| G0 API/CCXT 复核 | 锁定 BitKan 官方 REST/WS 文档、base URL、认证字段、权限、testnet、限速、地区限制和 CCXT `bitkan` 能力面 | `docs/交易所网关/适配器/bitkan_adapter.md` endpoint mapping、签名样例、错误码表、能力矩阵；复核现有保守 adapter 的 `*_unverified` 边界；如官方 trading API 不可用，提交替代候选建议 |
| G1 Spot public REST | 对标 Binance Spot public 与 CCXT `fetchMarkets/fetchOrderBook/fetchTicker/fetchTrades/fetchOHLCV` | `get_symbol_rules`、`get_order_book`、ticker/trades/candles parser fixtures、request-spec tests |
| G2 Spot private REST | 对标 Binance Spot account/order 与 CCXT `fetchBalance/createOrder/cancelOrder/fetchOrder/fetchOpenOrders/fetchMyTrades` | balances、fees、place/cancel/query/open/fills、quote market 能力判断、dry-run request construction tests |
| G3 Contract/linear perp public REST | 对标 Binance USD-M public 与 CCXT swap/future market data | contracts、book、mark/index/funding/open interest、ticker/bookTicker/candles；无官方合约 API 时显式 `Unsupported` |
| G4 Contract/linear perp private REST | 对标 Binance USD-M private 与 CCXT positions/order lifecycle | futures balances、positions、place/cancel/query/open/fills、reduce-only、position side、leverage/margin/position mode 能力矩阵 |
| G5 Batch submit/cancel | 对标 CCXT `createOrders/cancelOrders` 与 Binance batch endpoints | 优先原生 batch；若只能组合，必须标注非原子、最大并发、部分失败语义、重试策略和 client id 幂等要求 |
| G6 WebSocket public/private | 对标 CCXT `watch*` 与 Binance streams | public depth/trade/ticker/kline，private orders/fills/balances/positions，替换当前未验证 channel helper，补订阅/退订 payload、auth、parser fixtures、sequence/checksum/resync |
| G7 心跳/高级风控 | 对标 Binance listenKey keepalive、ping/pong、countdownCancelAll | ping interval、pong timeout、stale timeout、server ping 响应、listen token 续期、dead-man/kill-switch 能力边界 |
| G8 接入与验收 | 升级现有 adapter、配置、测试和只读验证 | `bitkan` named adapter、config example、adapter docs 和 targeted tests 已完成；余下等待官方 OpenAPI 后补 fixtures、read-only live report；通过后才允许 `live_dry_run` |

BitKan 交付时必须至少给出以下明确结论：Spot trading 是否可用、Contract/linear perp trading 是否可用、是否支持原生批量下单、是否支持原生批量撤单、公开 WS 是否支持 order book delta、私有 WS 是否支持订单/成交/余额/仓位、心跳是 client ping 还是 server ping、是否需要 token/listenKey 续期、是否支持 leverage/margin/position mode 和 dead-man switch。

#### 任务 26 BigONE 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` BigONE Spot + Contract/Perpetual REST 与 WebSocket request-spec/parser/session specs。当前覆盖 `bigone`/`big_one` named adapter 注册、gateway app `RUSTCTA_BIGONE_*` env wiring、默认 REST base URL `https://api.big.one`、Spot/Contract JWT bearer 签名、Spot/Contract symbol rules、order book snapshot、balances、contract positions、fee fallback、place/cancel/query/open orders、recent fills、Spot native batch place、Contract batch cancel request spec、gateway composed cancel-all、官方 Spot public WS `subscribeMarketDepthRequest`/`subscribeMarketTradesRequest`/`subscribeMarketsTickerRequest`/`subscribeMarketCandlesRequest` 订阅 payload、Contract public WS session specs、private WS orders/fills/accounts/positions subscription specs、ping heartbeat、public order book 标准事件解析、typed public trade/ticker/candle parser、private stream parser、配置示例和离线 parser/capability/request-spec tests。Quote-sized market order、amend order、order list/OCO 当前显式不声明支持。

验证：

- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway bigone --lib --message-format short` 通过，15 个 BigONE adapter parser、capability、WebSocket 和 REST request-spec tests passed；该组 mock REST tests 在受限环境下需要本地 TCP listener 权限。
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway bigone_ws_parser --lib --message-format short` 通过，2 个 BigONE WebSocket parser tests passed。
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo check -p rustcta-exchange-gateway --lib --message-format short` 通过，仍有既有 workspace warnings。
- `TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo check -p rustcta-gateway --message-format short` 通过，应用侧 named adapter/config wiring 编译通过，仍有既有 workspace warnings。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/bigone/`：config、signing、transport、public/private parser、REST 实现、WebSocket subscription/session specs 和离线测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、named adapter registration 和 `BigOneGatewayConfig` 导出。
- `apps/gateway/src/config.rs`：`bigone` app adapter 注册、REST URL override 和 private credential env wiring。
- `config/spot_exchanges_example.yml`、`docs/交易所网关/适配器/bigone_adapter.md`：disabled-by-default 配置示例、endpoint mapping、签名规则和能力边界。

Endpoint mapping：

| 标准能力 | BigONE 原生接口/频道 | 当前实现 |
| --- | --- | --- |
| Spot symbol rules | `GET /api/v3/asset_pairs` | 解析 pair name、base/quote、precision/increment、min notional、status |
| Contract symbol rules | `GET /api/contract/v2/instruments` | 解析 instrument id，按 `MarketType::Perpetual` 标准化 |
| Spot/Contract order book | `GET /api/v3/asset_pairs/{asset_pair_name}/depth`, `GET /api/contract/v2/depth` | snapshot parser，depth max 200 |
| Private auth | Spot/Contract JWT bearer | `signing.rs` 和 `transport.rs` 统一使用 compact HMAC-SHA256 JWT |
| Balances/Positions | `/api/v3/viewer/accounts`, `/api/contract/v2/accounts`, `/api/contract/v2/positions` | Spot/Contract balances；Contract positions；Spot positions unsupported |
| Order lifecycle | `/api/v3/viewer/orders`, `/api/contract/v2/orders` | place/cancel/query/open orders，Spot path-style cancel，client id、reduce-only、post-only/TIF request mapping |
| Batch/cancel-all | Spot `/api/v3/viewer/orders/multi`；Contract cancel request spec；gateway composed flows | Spot native batch place；Contract place sequential fallback；cancel-all 先查 open orders 再撤单 |
| Fills | `/api/v3/viewer/trades`, `/api/contract/v2/fills` | 解析成交、fee、liquidity、timestamp |
| WebSocket | Spot `wss://api.big.one/ws/v2` request-object；Contract URL-style public channels | Spot public depth/trade/ticker/kline 订阅按官方 `subscribeMarketDepthRequest`/`subscribeMarketTradesRequest`/`subscribeMarketsTickerRequest`/`subscribeMarketCandlesRequest`；order book 转标准事件，trade/ticker/candle 保留 typed BigONE parser 输出；private order/fill/balance/position 标准事件解析 |

#### 任务 27 BloFin 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` BloFin USDT perpetual REST 与 WebSocket request-spec/parser 初版，并已接入 named adapter 和 `apps/gateway` 配置入口。当前覆盖官方 REST base URL、public/private WebSocket URL、API key/secret/passphrase 私有凭据、BloFin HMAC-SHA256 hex-then-Base64 签名、perpetual symbol rules、ticker、order book snapshot、trades、mark price、funding rate、funding history、regular/index/mark-price candles、position tiers、fee metadata、balances、positions、place/cancel/cancel-all/query/open orders、recent fills、原生 batch place/cancel、asset balances、funds transfer、transfer/deposit/withdraw history、account config、currencies、positions history、margin mode、position mode、leverage readback/mutation、TPSL orders、algo orders、close-position、completed order history、TPSL/algo history、order price range、public WS `books`/`books5`/`trades`/`tickers`/`candle*`/`funding-rate` subscription payload、private WS login 与 `orders`/`orders-algo`/`positions`/`account` subscription payload、text `ping`/`pong` heartbeat spec 和离线 signing/stream parser/request builder tests。

官方 OpenAPI 复核结论：当前确认的 BloFin OpenAPI 是永续合约交易完整；Spot 交易产品线没有稳定公开的下单/撤单/订单查询 endpoint 与统一 gateway trading contract 对齐，因此当前不声明 Spot trading 支持，Spot trading 请求显式 `Unsupported`。杠杆、保证金模式、持仓模式、TPSL、Algo、划转和历史查询已作为 BloFin 专属 REST 方法接入；统一 `ExchangeClient` trait 仍不声明 quote-sized market、amend order、OCO/OTO order list、独立 private fills WS channel 和生产 WebSocket supervisor runtime。

验证：

- `cargo fmt`
- `CARGO_TARGET_DIR=/tmp/rustcta_blofin_target cargo test -p rustcta-exchange-gateway blofin --lib -- --nocapture` 通过：12 passed。
- `CARGO_TARGET_DIR=/tmp/rustcta_gateway_app_blofin_target cargo check -p rustcta-gateway` 通过（仍有既有 workspace warnings）。

#### 任务 29 CoinDCX 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` CoinDCX Spot + Futures REST 与 Socket.IO request-spec/parser 初版。当前覆盖官方 Spot REST base URL `https://api.coindcx.com`、public market data base `https://public.coindcx.com`、Socket.IO base `wss://stream.coindcx.com`、私有 REST/WS `HMAC-SHA256(compact JSON body, api_secret)` 签名、Spot/Futures symbol rules、Spot/Futures order book snapshot、Spot/Futures balances、Futures positions、place/cancel/cancel-all/query/open orders/recent fills、Spot 原生 batch place、Spot 原生 batch cancel-by-ids/client-order-ids、Spot/Futures amend request routing、Spot/Futures public Socket.IO orderbook/trades/prices/candles join payload、private `coindcx` channel auth join payload、25 秒 ping 心跳、public order book parser、private order/fill/balance/position stream parser、named adapter 注册、配置示例、endpoint mapping 文档和离线 parser/session 测试。CoinDCX Futures 官方未确认原生批量下单和多 order id 批量撤单，当前显式 `Unsupported`；Socket.IO 不伪装成普通 JSON WebSocket；Futures client order id、标准 reduce-only flag、可靠 futures post-only、Binance OCO/OTO order list 和生产长连接 runtime loop 仍显式不声明支持或由平台层后续接入。

验证：`rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/coindcx/*.rs apps/gateway/src/config.rs` 已通过；`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/coindcx-task-check cargo check -p rustcta-exchange-gateway --lib --message-format short` 已通过（仍有既有 warnings）；`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/coindcx-task-check cargo test -p rustcta-exchange-gateway coindcx --lib --message-format short` 已通过 7 个 CoinDCX 测试；`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/coindcx-app-check cargo test -p rustcta-gateway config_should_wire_coindcx_private_gateway_adapter -- --nocapture` 已通过；`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/coindcx-app-check cargo check -p rustcta-gateway --message-format short` 已通过。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/coindcx/`：config、signing、transport、public/private REST、parser、Socket.IO subscription/session/heartbeat specs 和离线测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `CoinDcxGatewayConfig` 导出。
- `docs/交易所网关/适配器/coindcx_adapter.md`、`config/coindcx_gateway_example.yml`：endpoint mapping、签名规则、Socket.IO 边界和配置示例。

Endpoint mapping：

| 标准能力 | CoinDCX 原生接口/频道 | 当前实现 |
| --- | --- | --- |
| spot symbol rules | `GET /exchange/v1/markets_details` | 解析 base/quote、tick/step、min notional、active 状态 |
| futures symbol rules | `GET /exchange/v1/derivatives/futures/data/active_instruments` | 解析 active futures instruments，按 `MarketType::Perpetual` 标准化 |
| spot/futures order book | `GET /market_data/orderbook`, `GET /market_data/v3/orderbook/{instrument}-futures/{depth}` | snapshot parser，兼容 object/array depth levels，深度声明 50 |
| private signing | `X-AUTH-APIKEY`, `X-AUTH-SIGNATURE` | compact JSON body HMAC-SHA256 hex；timestamp 注入 body；secret 不进入 path/query/body |
| balances/positions | `POST /exchange/v1/users/balances`, `/derivatives/futures/wallets`, `/derivatives/futures/positions` | Spot/Futures balances；Futures positions 解析数量、entry、mark、liquidation、PnL、leverage |
| order lifecycle | Spot `/orders/create/status/active_orders/trade_history/cancel/cancel_all/edit`; Futures `/derivatives/futures/orders/create/cancel/edit`, `/positions/cancel_all_open_orders`, `/trades` | place/cancel/cancel-all/query/open/fills/amend；Spot client order id，Futures id-only 边界 |
| batch orders | Spot `/orders/create_multiple`, `/orders/cancel_by_ids` | 原生 Spot batch place/cancel；Futures batch place 和 multi-id batch cancel 显式 `Unsupported` |
| WebSocket | Socket.IO `{pair}@orderbook@50`, `{instrument}@orderbook@50-futures`, trades/prices/candles, private `coindcx` | join/auth join payload、25 秒 ping、heartbeat stream event、public order book 和 private order/fill/balance/position parser；普通 JSON WS runtime 不声明 |

#### 任务 30 BitKan 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` BitKan 保守接入。当前覆盖 adapter 目录、config、crate export、named adapter 注册、`apps/gateway` `RUSTCTA_BITKAN_*` env wiring、Spot + Perpetual product scope 声明、public WS subscription/heartbeat helper、private stream capability unsupported、以及所有未能官方确认的 REST/WS 交易能力的精确 `Unsupported` 返回。由于检索到的 BitKan 官网 API 页面被 Cloudflare challenge 阻挡，且未找到可验证的官方 OpenAPI/签名/端点文档，本轮不声明 symbol rules、order book、balance、position、fee、place/cancel/query/open/fills、batch place/cancel/cancel-all 或真实 public/private WS 已可用。

代码边界：`crates/rustcta-exchange-gateway/src/adapters/bitkan/`；`crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`、`apps/gateway/src/config.rs` 已接入；`docs/交易所网关/适配器/bitkan_adapter.md` 与 `config/bitkan_gateway_example.yml` 记录当前能力边界和升级门槛。

验证：`python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitkan/endpoint_mapping.yaml` 已通过；`rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/bitkan/*.rs crates/rustcta-exchange-gateway/src/adapters/mod.rs crates/rustcta-exchange-gateway/src/lib.rs apps/gateway/src/config.rs` 已通过；`CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway bitkan --lib --message-format short` 已通过 4 个 BitKan 测试，748 filtered out；`CARGO_TARGET_DIR=target/gateway-clean-check cargo check -p rustcta-exchange-gateway --lib --message-format short` 已通过；`CARGO_TARGET_DIR=target/gateway-clean-check cargo check -p rustcta-gateway --message-format short` 已通过。当前仍有大量既有 adapter dead-code/unused warnings，非 BitKan 新增阻塞。该任务当前是保守注册和精确 `Unsupported` 边界，不声明已完成 BitKan 现货/合约真实交易、批量订单、取消或私有 WebSocket 对标。

#### 任务 23 Biconomy Exchange 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Biconomy Spot REST 与 WebSocket request/parser/session specs。当前覆盖 Biconomy Spot REST base URL、`X-CH-APIKEY` + `timestamp + METHOD + path + body` HMAC-SHA256 hex 签名、Spot symbol rules、order book snapshot、balances、fee-rate、place/cancel/query/open orders、recent fills、gateway 组合 batch place/cancel、symbol-scoped cancel-all、public WS `depth.subscribe`/`deals.subscribe`/`state.subscribe`/`kline.subscribe` JSON-RPC 订阅、`depth.update`/`deals.update`/`state.update`/`kline.update` parser、public book 标准 `OrderBookSnapshot` 转换、`server.ping`/`pong` 心跳、private WS `spot/user.order`/`spot/user.balance` 订阅、private order 标准 `OrderUpdate` 转换、private balance 标准 `BalanceSnapshot` 转换、named adapter 注册、gateway app `RUSTCTA_BICONOMY_*` env wiring、配置示例、fixtures、文档和离线 request-spec/parser 测试。Futures/perpetual endpoint 在本次实现时没有稳定官方规格可确认，因此非 Spot market、positions、private position stream 当前显式 `Unsupported`，不硬编码伪实现。

验证：`rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/biconomy/streams.rs crates/rustcta-exchange-gateway/src/adapters/biconomy/stream_tests.rs` 已通过；`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway biconomy --lib --message-format short` 已通过，15 个 Biconomy 测试通过，0 失败，736 filtered out；sandbox 内本地 mock REST 绑定 `127.0.0.1` 会被拦截，完整目标集已按需非 sandbox 重跑；`CARGO_TARGET_DIR=target/biconomy-app-final-check2 cargo test -p rustcta-gateway config_should_parse_adapters_and_redirection_urls_without_secret_fields -- --nocapture` 已通过。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/biconomy/`：config、signing、transport、public/private REST、parser、WebSocket specs/parsers 和离线 mock REST/WS 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `BiconomyGatewayConfig` 导出。
- `apps/gateway/src/config.rs`：`biconomy` app adapter 注册、REST URL override 和 private credential env wiring。
- `config/biconomy_gateway_example.yml`、`docs/交易所网关/适配器/biconomy_adapter.md`、`tests/fixtures/exchanges/biconomy/`：默认 disabled 配置示例、endpoint mapping、签名规则、能力边界和离线响应样本。

Endpoint mapping：

| 标准能力 | Biconomy 原生接口/规格 | 当前实现 |
| --- | --- | --- |
| spot symbol rules | `GET /api/v1/exchangeInfo` | 解析 `symbols[]`、base/quote、precision、tick size、step size、min qty、min notional |
| spot order book | `GET /api/v1/depth` | snapshot parser，depth 归一到 1..100 |
| private REST signing | `X-CH-APIKEY`、`X-CH-SIGN`、`X-CH-TS` header HMAC-SHA256 | 独立 `signing.rs`；request-spec 断言 signature 存在且 secret 不进入 path/body |
| balances/fee-rate | `POST /api/v2/private/account` | 解析 Spot balances；account-level maker/taker commission fallback |
| order lifecycle | `POST /api/v2/private/order`, `/cancel`, `/orderInfo`, `/openOrders` | Spot market/limit/post-only/IOC/FOK、client id、cancel、query/open orders |
| batch/cancel-all | gateway composed flow | batch place/cancel sequential fallback；cancel-all 先查 open orders 再逐单撤销 |
| fills | `POST /api/v2/private/myTrades` | 解析 trade id、order id、手续费、maker/taker、成交时间 |
| WebSocket | public `depth.subscribe`/`deals.subscribe`/`state.subscribe`/`kline.subscribe`; pushes `depth.update`/`deals.update`/`state.update`/`kline.update`; private `spot/user.order`, `spot/user.balance` | public/private subscription specs、`server.ping`/pong heartbeat、public book 标准事件转换、typed public trade/ticker/candle parser、private order 标准 `OrderUpdate` 转换、private balance 标准 `BalanceSnapshot` 转换 |

#### 任务 24 Coinstore 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` Coinstore Spot + Futures REST 与 WebSocket request specs。当前覆盖 Coinstore Spot/Futures REST base URL、Spot public WS、Futures Socket.IO public/private WS URL、`X-CS-APIKEY`/`X-CS-EXPIRES`/`X-CS-SIGN` 两阶段 HMAC-SHA256 签名、Spot/Futures symbol rules、order book snapshot、balances、Futures positions、fee-rate metadata、order lifecycle、native Spot/Futures batch place、native Spot/Futures batch cancel、Futures cancel-all、query/open orders、recent fills、Spot public WS trade/ticker/depth/kline subscribe payload、Spot depth 标准 `OrderBookSnapshot` stream event 转换、Futures Socket.IO auth/subscribe payload、heartbeat specs、Futures private `match` order/fill 标准事件解析、named adapter 注册、gateway app `RUSTCTA_COINSTORE_*` env wiring、配置示例、文档和离线 request-spec/parser 测试。Spot private WS、Futures private balance/account/position stream、Spot cancel-all、funding history/open interest、position-mode switch、amend/order-list/dead-man switch 当前显式 `Unsupported` 或 REST reconciliation，不硬编码官方未确认语义。

验证：`rustfmt --edition 2021 crates/rustcta-exchange-gateway/src/adapters/coinstore/streams.rs crates/rustcta-exchange-gateway/src/adapters/coinstore/stream_tests.rs` 已通过；`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/gateway-clean-check cargo test -p rustcta-exchange-gateway coinstore --lib --message-format short` 已通过，26 个 Coinstore 测试通过，0 失败，725 filtered out。该测试运行同时证明 gateway crate 当前可编译，但仍输出既有 adapter unused/dead-code warnings；sandbox 内本地 mock REST 绑定 `127.0.0.1` 会被拦截，已按需非 sandbox 重跑。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/coinstore/`：config、signing、transport、public/private REST、parser、WebSocket specs/parsers 和离线 mock REST/WS 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `CoinstoreGatewayConfig` 导出。
- `apps/gateway/src/config.rs`：`coinstore` app adapter 注册、Spot/Futures REST URL override 和 private credential env wiring。
- `config/coinstore_gateway_example.yml`、`docs/交易所网关/适配器/coinstore_adapter.md`：默认 disabled 配置示例、endpoint mapping、签名规则和能力边界。

Endpoint mapping：

| 标准能力 | Coinstore 原生接口/规格 | 当前实现 |
| --- | --- | --- |
| spot symbol rules / fee | `POST /v2/public/config/spot/symbols` | 解析 `name/baseCurrency/quoteCurrency`、precision、min size/notional、maker/taker fee |
| futures symbol rules / fee | `GET /api/configs/public` | 解析 `contracts[]`、tick size、min/max order size、maker/taker rate |
| spot order book | `GET /v1/market/depth/{symbol}` | snapshot parser，depth 归一到 5..100 |
| futures order book | `GET /v1/futureQuot/querySnapshot` | 解析 futures snapshot bids/asks |
| private REST signing | `X-CS-APIKEY/X-CS-EXPIRES/X-CS-SIGN` | `payload=query_string+raw_body`，`floor(expires/30000)` 派生 key 后二次 HMAC-SHA256 |
| balances/positions | `POST /spot/accountList`, `POST /api/future/queryAvail`, `GET /api/future/queryPosi` | Spot/Futures balances；Futures positions |
| order lifecycle | `POST /trade/order/place`, `/cancel`, `GET /api/v2/trade/order/orderInfo`, `/active`; Futures `/api/trade/order/place`, `/cancel`, v2 query/active | Spot/Futures market/limit、client order id、cancel、query/open orders；Futures reduce/close 使用 `positionEffect=2` |
| batch orders | `POST /trade/order/placeBatch`, `/cancelBatch`, `/cancelBatchByClOrdId`; Futures `/api/trade/order/placeBatch`, `/api/trade/orders/del` | 原生 batch place/cancel；Spot 可按 order id 或 client order id 批量撤单 |
| cancel-all | `POST /api/trade/order/cancelAll` | Futures 原生 cancel-all；Spot cancel-all 显式 `Unsupported` |
| fills | `GET /trade/match/accountMatches`, `GET /api/v2/trade/order/queryHisMatch` | 解析成交、手续费、maker/taker、成交时间 |
| WebSocket | Spot JSON `SUB/UNSUB`；Futures Socket.IO `42["auth" ...]` / `42["subscribe" ...]` | Spot public trade/ticker/depth/kline specs；Spot depth 转标准 `OrderBookSnapshot`；Futures public topics 与 private `match` topic；`match` 按订阅种类转换为标准 `OrderUpdate`/`Fill`；Spot private WS 与 private balance/account/position stream 暂不启用，余额/仓位靠 REST reconciliation |

#### 任务 25 DigiFinex 交付状态

状态：已完成 `rustcta-exchange-gateway` 统一 `ExchangeClient` DigiFinex Spot + perpetual swap REST 与 WebSocket request-spec/parser 初版。当前覆盖官方 Spot REST base URL、Swap REST base URL、Spot/Swap public/private WS URL、`ACCESS-KEY/ACCESS-TIMESTAMP/ACCESS-SIGN` HMAC-SHA256 签名、Spot/Swap symbol rules、order book snapshot、Spot/Swap balances、Swap positions、fee metadata fallback、place/cancel/query/open orders、Spot quote-sized market buy、Spot/Swap native batch place、Spot comma-id batch cancel、Swap native batch cancel、cancel-all open-order sweep、recent fills、public/private WS subscription payload、`server.ping` heartbeat payload/policy、public book parser、private order/balance/position parser、request-spec/parser tests 和错误分类。OCO/OTO order list、amend order、perpetual quote-sized market order 当前显式 `Unsupported`，不使用 cancel+replace 或 trigger-order 伪造 Binance 高级语义。

代码边界：

- `crates/rustcta-exchange-gateway/src/adapters/digifinex/`：config、signing、transport、public/private REST、parser、WebSocket specs/parsers 和离线 mock REST/WS 测试。
- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`：adapter module/export、命名注册和 `DigiFinexGatewayConfig` 导出。
- `docs/交易所网关/适配器/digifinex_adapter.md`、`config/digifinex_gateway_example.yml`：endpoint mapping、签名规则、配置示例和能力边界。

验证：`CARGO_TARGET_DIR=/tmp/rustcta_digifinex_target cargo test -p rustcta-exchange-gateway adapters::digifinex:: --lib -- --nocapture` 通过（9 passed）；`CARGO_TARGET_DIR=/tmp/rustcta_digifinex_check_target cargo check -p rustcta-exchange-gateway --lib` 通过（当前仓库存在大量无关 adapter dead-code/unused warnings）。

Endpoint mapping：

| 标准能力 | DigiFinex Spot 原生接口 | DigiFinex Swap 原生接口 | 当前实现 |
| --- | --- | --- | --- |
| symbol rules | `GET /v3/spot/markets` | `GET /swap/v2/public/instruments` | 解析 base/quote、precision、min amount/notional，按 `MarketType::Spot`/`Perpetual` 标准化 |
| order book | `GET /v3/spot/order_book` | `GET /swap/v2/public/order_book` | snapshot parser，depth 归一到 1..200 |
| balances/positions | `GET /v3/spot/assets` | `GET /swap/v2/account/balance`, `/account/positions` | Spot/Swap balances，Swap positions parser |
| order lifecycle | `POST /v3/spot/order/new`, `/order/cancel`, `GET /order`, `/order/current` | `POST /swap/v2/trade/order`, `/cancel_order`, `GET /order_info`, `/open_orders` | limit/market/post-only/IOC/FOK、client id、swap reduce-only/position side、cancel、query/open orders |
| batch place/cancel | `POST /v3/spot/order/batch_new`, Spot cancel comma ids | `POST /swap/v2/trade/batch_order`, `/batch_cancel_order` | 原生 batch place，Spot comma-id batch cancel，Swap native batch cancel |
| fills | `GET /v3/spot/my_trades` | `GET /swap/v2/trade/fills` | 解析成交、手续费、maker/taker、成交时间 |
| WebSocket | Spot `depth/trades/ticker/kline.subscribe` | Swap `orderbook/trades/ticker/kline.subscribe`; private `order/trade/balance/position.subscribe` | request-spec、`server.ping` heartbeat payload/policy、book/order/balance/position parser |

## 任务认领方式

可以直接用任务编号派发开发工作：

- `完成 docs/交易所网关/总览/exchange_gateway_expansion_30_venues_zh.md 中的开发任务 1`
- `完成 docs/交易所网关/总览/exchange_gateway_expansion_30_venues_zh.md 中的开发任务 1-10`
- `完成 docs/交易所网关/总览/exchange_gateway_expansion_30_venues_zh.md 中的开发任务 11-20`
- `完成 docs/交易所网关/总览/exchange_gateway_expansion_30_venues_zh.md 中的开发任务 21-30`

任务范围按闭区间解释，例如 `任务 1-10` 就是第一批 10 个交易所。并行开发时建议 10 个 AI 分别认领同一批中的单个任务；批量指令适合给协调者生成分支、检查进度或汇总验收。

## Binance 对标标准接口清单

每个新增交易所必须先建立一张自己的 endpoint mapping 表，把交易所原生接口映射到以下标准能力。接口名以当前 `rustcta-exchange-api::ExchangeClient` 和现有永续路径为准。

### 任务 2 Coinbase endpoint mapping

官方文档版本：Coinbase Advanced Trade API，2026-06-07 复核；base URL 默认 `https://api.coinbase.com/api/v3/brokerage`。Spot 与 International perpetual 先在配置中拆成 `spot_rest_base_url` 和 `international_rest_base_url`，当前实现使用同一 API 族，并用 `product_type=SPOT` 或 `product_type=FUTURE&contract_expiry_type=PERPETUAL&product_venue=INTX` 分线。

| 标准能力 | Coinbase endpoint | 当前实现状态 |
| --- | --- | --- |
| `get_symbol_rules` Spot | `GET /products?product_type=SPOT` | 已实现：解析 `product_id`、base/quote、price/base increment、min/max size、quote min/max |
| `get_symbol_rules` linear perp | `GET /products?product_type=FUTURE&contract_expiry_type=PERPETUAL&product_venue=INTX` | 已实现 parser 与 request spec；按 `MarketType::Perpetual` 标注 |
| `get_order_book` Spot/Perp | `GET /product_book?product_id=...&limit=...` | 已实现 snapshot，depth 限制到 1-100，保留 sequence/time |
| `get_balances` Spot | `GET /accounts` | 已实现 bearer-token 私有 REST；无 token 返回 `Unsupported` |
| `get_balances` INTX perp | `GET /intx/balances/{portfolio_uuid}` | 已实现 portfolio balance parser；需要配置 `COINBASE_INTX_PORTFOLIO_UUID`，缺失时 `Unsupported` |
| `get_fees` | `GET /transaction_summary` | 已实现 maker/taker fee tier parser，source 标记为 `coinbase.transaction_summary.fee_tier` |
| `get_positions` INTX perp | `GET /intx/positions/{portfolio_uuid}` | 已实现 read-only parser；需要配置 `COINBASE_INTX_PORTFOLIO_UUID`，缺失时 `Unsupported` |
| `place_order` | `POST /orders` | 已实现 market、limit GTC、IOC、FOK、post-only 请求构造；支持 `client_order_id` |
| `batch_place_orders` | Coinbase Advanced Trade 无官方原生 batch create endpoint；组合调用 `POST /orders` | 已实现 adapter-level composed fallback：先预校验整批 schema/exchange/market/body，再逐单顺序提交；非原生、非原子，文档和测试中显式标注 |
| `place_quote_market_order` | `POST /orders` with `market_market_ioc.quote_size` | 已实现 quote sizing request spec |
| `cancel_order` | `POST /orders/batch_cancel` | 已实现 exchange order id cancel；client id cancel 显式 `Unsupported` |
| `batch_cancel_orders` | `POST /orders/batch_cancel` | 已实现多 exchange order id 原生批量撤单；client id cancel 显式 `Unsupported` |
| `amend_order` | `POST /orders/edit` | 已实现 exchange order id size amend；client id only 与 new client order id 显式 `Unsupported` |
| `query_order` | `GET /orders/historical/{order_id}` | 已实现 exchange order id 查询，并允许以 client order id 作为 lookup fallback |
| `get_open_orders` | `GET /orders/historical/batch?order_status=OPEN` | 已实现 symbol scoped 查询 |
| `get_recent_fills` | `GET /orders/historical/fills` | 已实现 symbol/order/time/limit 查询参数和 fill parser |
| `cancel_all_orders` | `GET /orders/historical/batch?order_status=OPEN&product_id=...` + `POST /orders/batch_cancel` | 已实现 symbol scoped open-order sweep；无 symbol 时拒绝，避免全账户误撤 |
| OCO SELL bracket | `POST /orders` with `trigger_bracket_gtc` | 已实现标准 OCO SELL 的安全子集映射：limit take-profit + stop-loss trigger；leg client id / non-GTC / BUY OCO 拒绝 |
| OTO / attached TP-SL | `POST /orders` with `attached_order_configuration` | Coinbase parent-attached TP/SL 与当前 `OrderListRequest::Oto` 不能无损等价，显式 `Unsupported` |
| WS public | `wss://advanced-trade-ws.coinbase.com` channel `level2`/`ticker`/`market_trades`/`candles` + `heartbeats` | 已实现 subscription request-spec、public WS session helper、heartbeats 订阅、heartbeat stream event、level2 snapshot parser 和标准 `ExchangeStreamEvent::OrderBookSnapshot` 转换；真实 socket connect/resync 后续由统一 supervisor 接入 |
| WS private | `wss://advanced-trade-ws-user.coinbase.com` channel `user` + `heartbeats` | 已实现 JWT subscription request-spec、private WS session helper、heartbeats 订阅、heartbeat stream event、user order parser 和标准 `ExchangeStreamEvent::OrderUpdate` 转换；balances stream 无原生等价能力时 `Unsupported` |

### 任务 2 Coinbase 余下收尾计划

Coinbase adapter 已完成 Spot + INTX perpetual 的主要 REST 和 WS request/parser/session specs。余下任务不再重复实现 endpoint，而是按 CCXT/Binance 对标矩阵补齐运行时接入、能力审计和 live-readonly 证明。

验证：`python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/coinbase/endpoint_mapping.yaml` 已通过；`TMPDIR=$PWD/target/tmp CARGO_TARGET_DIR=target/coinbase-task-check cargo test -p rustcta-exchange-gateway coinbase --lib --message-format short` 已通过，30 个 Coinbase 测试通过，0 失败，722 filtered out；sandbox 内本地 mock REST 绑定 `127.0.0.1` 会被拦截，完整目标集已按需非 sandbox 重跑。

| 子任务 | 对标能力 | 收尾要求 |
| --- | --- | --- |
| C1 CCXT `has` 审计 | `loadMarkets`、`fetchBalance`、`createOrder`、`cancelOrders`、`watchOrders` | 对 Coinbase Advanced Trade 与 INTX 分别列出支持、组合 fallback、`Unsupported`，同步 capability flags |
| C2 Spot/INTX public runtime | `watchOrderBook`、`watchTrades`、`watchTicker`、`watchOHLCV` | 接入统一 WebSocket supervisor，完成 heartbeats 订阅、断线重连、level2 snapshot/delta resync 和 REST book fallback |
| C3 Spot/INTX private runtime | `watchOrders`、`watchMyTrades`、`watchBalance`、`watchPositions` | 接入 user channel JWT auth、心跳事件、订单事件 parser；balances/positions 无原生流时记录 REST reconciliation 周期 |
| C4 批量与高级订单验收 | `createOrders`、`cancelOrders`、OCO/OTO | 复核组合式 batch place 的非原子语义、原生 batch cancel 的部分失败语义；OCO SELL bracket 安全子集继续保留限制 |
| C5 INTX perp 高级能力 | `setLeverage`、`setMarginMode`、`setPositionMode`、funding/mark/open interest | 只补官方稳定 endpoint；没有无损映射时保持 `Unsupported` 并在策略层用配置约束 |
| C6 read-only live report | Binance live-dry-run 前置要求 | 覆盖 server time、products、book、fees、balances、open orders、WS public heartbeat、WS private auth heartbeat，不提交真实订单 |

### 通用基线

| 能力 | 标准要求 |
| --- | --- |
| 交易所标识 | `ExchangeId`、adapter id、配置 key、日志字段统一使用小写 snake_case |
| 产品线 | `Spot` 与 `linear_perp` 分开配置、分开 capability、分开 base URL |
| 认证 | 支持 API key/secret/passphrase 等原生字段；签名逻辑隔离在 `signing.rs` |
| 时间 | 支持 server time 或本地时间偏差校正；所有时间统一归一到 UTC |
| 限速 | 解析响应 header 或错误码，归类为 `RateLimited`，记录 endpoint weight |
| 错误分类 | 至少分类认证失败、权限不足、限速、余额不足、订单不存在、重复 client id、参数错误、交易所 5xx |
| symbol 规范 | canonical symbol、exchange symbol 双向转换；必须解析 tick size、lot size、min notional |
| 数量价格 | 统一用字符串 decimal，不使用 float 做下单参数计算 |
| client order id | 支持时必须透传；不支持时明确记录并由上层生成 fallback 关联 |
| dry-run | 私有下单类接口必须支持本地请求构造验证，不发真实请求 |
| 测试网 | 有 testnet/demo 时配置独立 base URL；没有则显式标注 |

### Spot REST: Public

| Binance 对标端点 | 标准能力 | 新交易所实现要求 |
| --- | --- | --- |
| `GET /api/v3/exchangeInfo` | `get_symbol_rules` | 解析交易对状态、base/quote、price/qty step、min/max、min notional |
| `GET /api/v3/depth` | `get_order_book` | 支持指定 depth，保留交易所序列号/更新时间 |
| `GET /api/v3/trades`、`GET /api/v3/aggTrades` | recent public trades | 作为行情扩展能力，至少 parser fixture 覆盖 |
| `GET /api/v3/klines` | candles | 作为行情扩展能力，优先支持 `1m`、`5m`、`1h`、`1d` |
| `GET /api/v3/ticker/24hr` | ticker | 解析 last、bid/ask、volume、quote volume |
| `GET /api/v3/ticker/bookTicker` | best bid/ask | 跨所扫描需要，缺失时由 book top 代替 |
| `GET /api/v3/time` 或等价接口 | time sync | 私有签名请求前用于时钟漂移校验 |

### Spot REST: Private

| Binance 对标端点 | 标准能力 | 新交易所实现要求 |
| --- | --- | --- |
| `GET /api/v3/account` | `get_balances` | 解析 free、locked、total；隐藏零余额可配置 |
| `GET /api/v3/account/commission` 或 fee endpoint | `get_fees` | maker/taker 费率必须有来源标记 |
| `POST /api/v3/order` | `place_order` | 支持 `LIMIT`、`MARKET`；能映射 `GTC`、`IOC`、`FOK`、post-only |
| `POST /api/v3/order` with quote quantity | `place_quote_market_order` | 交易所支持 quote sizing 才开启 capability |
| `DELETE /api/v3/order` | `cancel_order` | 支持 exchange order id；支持 client id 时也要覆盖 |
| `DELETE /api/v3/openOrders` | `cancel_all_orders` | 至少支持 symbol scoped cancel-all；全市场 cancel-all 需单独能力标注 |
| `GET /api/v3/order` | `query_order` | 对订单不存在返回标准 `OrderNotFound` |
| `GET /api/v3/openOrders` | `get_open_orders` | 支持按 symbol 查询，解析全部活动订单 |
| `GET /api/v3/myTrades` | `get_recent_fills` | 支持 symbol、order id、time range、limit 能力矩阵 |
| `PUT /api/v3/order/amend/keepPriority` | `amend_order` | 仅交易所原生支持时开启；否则明确 `Unsupported` |
| `POST /api/v3/orderList/oco`、`oto`、`otoco` | `place_order_list` | 高级能力，第一版可 `Unsupported`，但请求校验不能绕过 |

### Spot WebSocket

| Binance 对标流 | 标准能力 | 新交易所实现要求 |
| --- | --- | --- |
| `<symbol>@depth` | public order book delta | 能严格维护 sequence/checksum 的标 `StrictDelta`，否则标 `BestEffortDelta` |
| `<symbol>@trade` / `<symbol>@aggTrade` | public trades | 标准化成交价格、数量、成交方向、交易时间 |
| `<symbol>@kline_<interval>` | candles | 支持 kline close 标记 |
| `<symbol>@ticker` / `bookTicker` | ticker | 用于快速扫描和 fallback best bid/ask |
| User Data Stream execution reports | private order/fill stream | 解析订单状态、成交、手续费、client id、exchange id |
| User Data Stream balance/account | private balance stream | 解析余额更新；不可用时明确用 REST reconciliation fallback |

### USD-M / Linear Perpetual REST: Public

| Binance 对标端点 | 标准能力 | 新交易所实现要求 |
| --- | --- | --- |
| `GET /fapi/v1/exchangeInfo` | perp symbol rules | 合约面值、price tick、qty step、min notional、状态、margin asset |
| `GET /fapi/v1/depth` | perp order book | 支持 depth 和序列号 |
| `GET /fapi/v1/premiumIndex` | mark/index price | 解析 mark price、index price、funding rate、next funding time |
| `GET /fapi/v1/fundingRate` | funding history | 支持 symbol、start/end、limit |
| `GET /fapi/v1/openInterest` | open interest | 跨所风险和资金费率策略使用 |
| `GET /fapi/v1/ticker/24hr` | futures ticker | 解析 24h 量价 |
| `GET /fapi/v1/ticker/bookTicker` | best bid/ask | 永续跨所扫描必需 |
| `GET /fapi/v1/klines` | futures candles | 至少支持常用周期 |

### USD-M / Linear Perpetual REST: Private

| Binance 对标端点 | 标准能力 | 新交易所实现要求 |
| --- | --- | --- |
| `GET /fapi/v2/account`、balance endpoint | futures balances | 解析 wallet、available、unrealized pnl、margin balance |
| `GET /fapi/v2/positionRisk` | `get_positions` | 解析数量、entry、mark、liquidation、leverage、margin type、position side |
| `POST /fapi/v1/order` | perp `place_order` | 支持 limit/market/stop/take-profit 能力矩阵 |
| `POST /fapi/v1/batchOrders` | `batch_place_orders` | 原生支持时标注 native；只做组合 fallback 时必须标注非原生、非原子、部分失败语义和重试边界 |
| `PUT /fapi/v1/order` 或等价 modify | `amend_order` | 原生支持才开启 |
| `DELETE /fapi/v1/order` | `cancel_order` | 支持 order id 和 client id 能力矩阵 |
| `DELETE /fapi/v1/allOpenOrders` | `cancel_all_orders` | 支持 symbol scoped cancel-all |
| `GET /fapi/v1/order` | `query_order` | 标准化订单状态 |
| `GET /fapi/v1/openOrders` | `get_open_orders` | 支持 symbol 和全账户查询能力矩阵 |
| `GET /fapi/v1/userTrades` | `get_recent_fills` | 解析 fee、maker/taker、realized pnl |
| `POST /fapi/v1/leverage` | `set_leverage` | 已有永续路径要支持或显式不支持 |
| `POST /fapi/v1/marginType` | margin type | cross/isolated 能力矩阵 |
| `POST /fapi/v1/positionSide/dual` | position mode | one-way/hedge mode 能力矩阵 |
| countdown cancel-all | dead-man switch | 交易所支持才开启；否则用上层 kill-switch/reconciliation |

### USD-M / Linear Perpetual WebSocket

| Binance 对标流 | 标准能力 | 新交易所实现要求 |
| --- | --- | --- |
| depth diff / partial book | public perp book | 维护 sequence、checksum、resync 策略 |
| trade / aggTrade | public trades | 标准化 trade event |
| mark price stream | mark/funding updates | 更新 mark、funding、next funding time |
| bookTicker | top of book | 支持低延迟扫描 |
| liquidation / force order | risk stream | 可选能力，parser fixture 先覆盖 |
| `ORDER_TRADE_UPDATE` 等私有流 | order/fill stream | 解析订单、成交、手续费、realized pnl |
| `ACCOUNT_UPDATE` 等私有流 | account/position stream | 解析余额、仓位、保证金变化 |

## 每个 AI 的交付边界

每个交易所适配器按以下文件边界交付，除非协调者批准，不改共享接口：

- `crates/rustcta-exchange-gateway/src/adapters/<exchange>/config.rs`
- `crates/rustcta-exchange-gateway/src/adapters/<exchange>/signing.rs`
- `crates/rustcta-exchange-gateway/src/adapters/<exchange>/transport.rs`
- `crates/rustcta-exchange-gateway/src/adapters/<exchange>/parser.rs`
- `crates/rustcta-exchange-gateway/src/adapters/<exchange>/private_parser.rs`
- `crates/rustcta-exchange-gateway/src/adapters/<exchange>/public.rs`
- `crates/rustcta-exchange-gateway/src/adapters/<exchange>/private.rs`
- `crates/rustcta-exchange-gateway/src/adapters/<exchange>/*_tests.rs`
- `tests/fixtures/exchanges/<exchange>/`
- 对应配置 example 和文档状态行

公共注册点由每个 AI 在自己的小 PR 中修改，但同批最好按 adapter id 排序，降低冲突：

- `crates/rustcta-exchange-gateway/src/adapters/mod.rs`
- gateway adapter registry
- capability matrix 文档
- config examples

## 实施阶段门

| 阶段 | 目标 | 必须产出 |
| --- | --- | --- |
| G0 API 固化 | 锁定官方 API 文档版本、base URL、testnet、权限范围 | endpoint mapping 表、签名样例、错误码表 |
| G1 Public Spot | 现货公开 REST 和 parser | symbol rules、book、ticker、fixtures、request-spec tests |
| G2 Private Spot | 现货私有 REST | balances、fees、place/cancel/query/open/fills、dry-run tests |
| G3 Public Perp | 永续公开 REST/WS | contracts、book、ticker、funding、mark price、fixtures |
| G4 Private Perp | 永续私有 REST | balances、positions、orders、leverage、position mode、fills |
| G5 Streams | 公开和私有 WebSocket | parser fixtures、subscription request-spec、reconnect/resync 策略 |
| G6 Validation | 接入网关并验证 | `cargo fmt`、targeted tests、read-only live report、capability matrix |

## Definition Of Done

一个新增交易所只有同时满足以下条件，才能从 `scan_only` 升级到 `live_dry_run` 或真实交易候选：

- 所有请求都有离线 request-spec 测试，能证明 method、path、query/body、headers、签名字段正确。
- 所有 parser 都有真实或脱敏 fixture，覆盖成功、空响应、错误响应和关键边界字段缺失。
- capability flags 与真实实现一致，不支持的功能返回 `Unsupported`。
- `client_order_id` 策略、数量精度、价格精度、min notional、post-only、IOC/FOK 映射有测试。
- 私有接口不会在 dry-run/live-dry-run 下发真实订单。
- read-only live validation 至少覆盖 server time、symbol rules、book、fee、balance、open orders。
- 真实交易前必须经过小额 live-dry-run 计划、kill-switch 验证、disabled-symbol 验证和 reconciliation 验证。

## 参考资料

- CCXT npm package: <https://www.npmjs.com/package/ccxt>
- CCXT GitHub repository: <https://github.com/ccxt/ccxt>
- CCXT manual: <https://docs.ccxt.com/>
- CCXT Pro WebSocket unified API: <https://github.com/ccxt/ccxt/wiki/ccxt.pro>
- Binance Spot REST API: <https://developers.binance.com/docs/binance-spot-api-docs/rest-api>
- Binance Spot market data endpoints: <https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints>
- Binance Spot trading endpoints: <https://developers.binance.com/docs/binance-spot-api-docs/rest-api/trading-endpoints>
- Binance Spot account endpoints: <https://developers.binance.com/docs/binance-spot-api-docs/rest-api/account-endpoints>
- Binance Spot WebSocket streams: <https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams>
- Binance Spot user data stream: <https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream>
- Binance USDⓈ-M Futures general info: <https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info>
- Binance USDⓈ-M Futures exchange information: <https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information>
- Binance USDⓈ-M Futures new order: <https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Order>
- Coinbase Advanced Trade API overview: <https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/introduction>
- Coinbase Advanced Trade list products: <https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/products/list-products>
- Coinbase Advanced Trade product book: <https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/products/get-product-book>
- Coinbase Advanced Trade perpetual futures guide: <https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/guides/perpetual>
- CoinMarketCap derivatives exchange ranking: <https://coinmarketcap.com/rankings/exchanges/derivatives/>
- CoinGecko derivatives exchange ranking: <https://www.coingecko.com/en/derivatives>
- Kraken API center: <https://docs.kraken.com/api/docs/guides/global-intro/>
- WhiteBIT API docs: <https://docs.whitebit.com/>
- WEEX API page: <https://www.weex.com/weex-API>
- WEEX Spot API domain: <https://www.weex.com/api-doc/spot/QuickStart/APIDomain>
- WEEX Spot signature: <https://www.weex.com/api-doc/spot/QuickStart/Signature>
- WEEX Futures API domain: <https://www.weex.com/api-doc/contract/QuickStart/APIDomain>
- WEEX Futures signature: <https://www.weex.com/api-doc/contract/QuickStart/Signature>

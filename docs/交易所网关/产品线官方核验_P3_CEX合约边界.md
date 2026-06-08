# 产品线官方核验 P3 CEX 合约边界

状态日期：2026-06-08

本文件核验一批中心化交易所的合约/衍生品边界，重点解决“当前 adapter 只声明 spot，是否应写 `交易所不支持合约`”的问题。结论只处理产品线；WebSocket 档位、推流间隔、下单/撤单细项仍以 [剩余官方核验队列](剩余官方核验队列.md) 为准。

## 核验口径

- 官方 API/产品资料已有 futures、perpetual、derivatives、CFD/FX 等交易线索：不能写 `交易所不支持合约`，应写 `项目未实现`、`当前 adapter 不接入` 或说明已有独立 adapter。
- 官方文档明确是 Spot Trading / Exchange API，且没有可映射到共享 futures/perpetual/option 的接口：按当前 API/adapter 口径写 `交易所不支持合约`。
- 对 CFD/FX、区域受限 derivatives、独立 futures API 域名等情况，必须写清边界，避免把它们误并入现货 adapter。

## 产品线矩阵

| adapter | 官方产品线结论 | 当前项目声明 | 结论和下一步 |
| --- | --- | --- | --- |
| `ascendex` | 官方 Cash/Margin API 和 Futures Pro API 都存在；Futures Pro 文档含 futures contracts、positions、orders、futures WS。 | 修正矩阵后已识别 `spot,perpetual`。 | 当前项目已声明现货和永续；后续剩余工作转向 WS 低延迟细项、账户/费率/仓位细项，不再列产品线待核验。 |
| `bitflyer` | 官方 Market List 同时有 `Spot` 和 `FX`，并说明 Lightning FX 已由 bitFlyer Crypto CFD 继承。 | `margin,spot`；当前将 FX/CFD 边界按 `Margin` 表达。 | 不能写交易所不支持合约；写 `项目未实现标准 Futures/Perpetual` 或保留 CFD/FX 作为 margin/adapter-specific 边界。 |
| `bitstamp` | Bitstamp API 变更记录已加入 derivatives public/private endpoints、funding rate、derivatives trade history。 | `spot`。 | 写 `项目未实现 Derivatives/Contracts`；不能写交易所不支持合约。 |
| `coinex` | CoinEx API v2 明确分 Spot 和 Futures REST/WS 模块。 | `spot`。 | 写 `项目未实现 Futures`；后续新增 futures profile 或扩展 adapter。 |
| `gemini` | Gemini 官方 API 有 Derivatives REST，说明支持 perpetual contracts 和 derivatives-specific account operations；官网也有 Gemini Perpetuals。 | `spot`。 | 写 `项目未实现 Derivatives/Perpetuals`；不要把区域限制误写成交易所不支持。 |
| `hitbtc` | HitBTC API v3 有 `/futures/account/...` 等 futures margin account endpoints。 | `spot`。 | 写 `项目未实现 Futures/Margin derivatives`；当前 spot runtime 不接 futures。 |
| `kucoin` | KuCoin 官方新文档区分 Spot/Margin 和 Futures REST/WS，API 权限也含 Futures。 | `spot`；项目另有 `kucoinfutures` adapter。 | `kucoin` 文档写“合约走 `kucoinfutures`，当前 spot adapter 不接”；不能写交易所不支持合约。 |
| `bitvavo` | 官方 API 文档当前是 Exchange REST/WS/FIX、Market Data Pro、Institutional APIs，未见 futures/perpetual/option 交易接口。 | `spot`。 | 按当前官方 API 写 `交易所不支持合约`；如官方上线 derivatives 需重核。 |
| `cex` | CEX.IO 当前公开交易文档是 Spot Trading REST/WS。 | `spot`。 | 按当前 CEX.IO Spot Trading API 写 `交易所不支持合约`；fiat/ledger/margin 旧边界不等同标准 futures/perpetual。 |

## 转入任务

| 类型 | adapter | 写法 |
| --- | --- | --- |
| 官方支持但项目未实现 | `bitflyer`, `bitstamp`, `coinex`, `gemini`, `hitbtc` | 单交易所文档写 `项目未实现` 或 adapter-specific CFD/derivatives 边界。 |
| 官方支持但已有独立 adapter | `kucoin` | `kucoin` 现货 adapter 文档写合约走 `kucoinfutures`。 |
| 明确交易所/API 不支持 | `bitvavo`, `cex` | 单交易所文档写 `交易所不支持合约`，并注明按当前官方 API 口径。 |
| 项目证据修正 | `ascendex` | 矩阵生成器已识别 `capabilities_v2.market_types`，不再误列为产品线未声明。 |

## 官方/一手资料来源

| adapter | 官方来源 |
| --- | --- |
| `ascendex` | <https://ascendex.github.io/ascendex-pro-api/>、<https://ascendex.github.io/ascendex-futures-pro-api-v2/> |
| `bitflyer` | <https://lightning.bitflyer.com/docs?lang=en> |
| `bitstamp` | <https://www.bitstamp.net/api/> |
| `coinex` | <https://docs.coinex.com/api/v2/> |
| `gemini` | <https://developer.gemini.com/docs/docs>、<https://developer.gemini.com/trading/rest-api/derivatives>、<https://www.gemini.com/perpetuals> |
| `hitbtc` | <https://api.hitbtc.com/> |
| `kucoin` | <https://www.kucoin.com/docs-new?lang=en_US> |
| `bitvavo` | <https://docs.bitvavo.com/docs/overview>、<https://docs.bitvavo.com/docs/rest-overview/> |
| `cex` | <https://trade.cex.io/docs/> |

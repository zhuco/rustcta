# 产品线官方核验 P5 区域现货 CEX 第二批

状态日期：2026-06-08

本文件继续核验一批区域性或中小型 CEX 的产品线边界。目标仍然是把“项目只声明 Spot”拆成明确结论：官方支持但项目未接的写 `项目未实现`；官方/API 口径没有现货或合约的，单交易所文档必须写 `交易所不支持现货` 或 `交易所不支持合约`。

## 核验口径

- 只看官方域名、官方 API 文档、官方 GitHub/SDK、官方帮助中心或官方产品页。
- 标准合约指 futures、perpetual、options，以及能映射到统一仓位、保证金模式、资金费、强平、风险限额的交易接口。
- 证券期权、股票保证金、P2P、OTC、swap/convert、支付、出入金、托管、跟单/资管不直接等同于 crypto exchange gateway 的标准合约。
- 官方平台有产品但当前公开 Exchange API 没有稳定接口时，不写成“交易所完全不支持”；写清 `项目未实现`、`当前 adapter 不接入` 或 `需单独建模`。

## 产品线矩阵

| adapter | 官方产品线结论 | 当前项目声明 | 结论和下一步 |
| --- | --- | --- | --- |
| `alpaca` | Alpaca crypto 文档是 Crypto Spot Trading；Margin and Short Selling 文档明确 crypto margin 不适用；Options 属于证券期权，不是 crypto exchange gateway 合约。 | `spot`。 | 按 crypto adapter 口径写 `交易所不支持合约`；Alpaca 证券 Options 不纳入本 adapter。 |
| `bequant` | 官方 v3 API 同时有 Spot、Margin、Futures account/order/position 以及 trading WS 的 margin/futures 方法。 | `spot`。 | 写 `项目未实现 Margin/Futures/Perpetual`；不能写交易所不支持合约。 |
| `biconomy` | 官方 Biconomy 页面有 Futures 信息页；当前已接 adapter 的稳定 API 面只覆盖 Spot REST/WS。 | `spot`。 | 写 `项目未实现 Futures/Perpetual`，并补官方 futures endpoint spec 核验后再实现。 |
| `bit2c` | 官方 API 文档列出 NIS 现货交易对的 ticker、orderbook、trades、balance、order/cancel 等；未见标准 futures/perpetual/options。 | `spot`。 | 写 `交易所不支持合约`。 |
| `bitbns` | 官方站点和官方 SDK/API 文档有 INR/USDT Spot 和 Margin Trading API；未见稳定标准 futures/perpetual/options API。 | `spot`。 | 写 `项目未实现 Margin Trading`；标准 futures/perpetual/options 写 `交易所不支持合约`。 |
| `bitteam` | 官方文档入口描述 Spot、P2P 和开发者 API；Swagger/OpenAPI 覆盖 CCXT pairs、balance、order、orderbook、orders、trades；未见标准 futures/perpetual/options。 | `spot`。 | 写 `交易所不支持合约`；P2P 不并入中心化订单簿交易 adapter。 |
| `bittrade` | 官方 API 介绍覆盖交易所/销售所、market/order/account/withdraw/WS，并明确当前不支持杠杆交易。账户示例为 `spot`。 | `spot`。 | 写 `交易所不支持合约`。 |
| `blockchaincom` | Exchange API 文档是现货 exchange API；官方站点/帮助中心另有 margin/perps 相关产品，但 Perps 是 Blockchain app 中通过第三方 Hyperliquid 的接口，不是 Exchange API 标准合约接口。 | `spot`。 | Exchange API 标准合约写 `交易所不支持合约`；如要接 Perps/第三方接口，写 `项目未实现` 并单独设计，不并入现货 adapter。 |
| `btcbox` | 官方 BTCBOX API 文档覆盖 JPY 现货 ticker、depth、orders、balance、trade_add/trade_cancel；未见标准 futures/perpetual/options。 | `spot`。 | 写 `交易所不支持合约`。 |
| `btcturk` | 官方 BtcTurk API 覆盖 market data、trading、account management；下单字段是 buy/sell 现货订单，未见标准 futures/perpetual/options。 | `spot`。 | 写 `交易所不支持合约`。 |
| `coinmate` | 官方站点/API 文档覆盖 EUR/CZK 现货交易、API、order book、orders、balances、WebSocket；未见标准 futures/perpetual/options/margin。 | `spot`。 | 写 `交易所不支持合约`。 |
| `coinmetro` | 官方 API docs repo 覆盖 spot order/book、WS、`/swap`；官方 Margin 平台资料确认 margin trading。未见标准 futures/perpetual/options API。 | `spot`。 | 写 `项目未实现 Margin/Swap/TRAM-like product boundary`；标准 futures/perpetual/options 写 `交易所不支持合约`。 |

## 转入任务

| 类型 | adapter | 写法 |
| --- | --- | --- |
| 官方支持但项目未实现 | `bequant` Margin/Futures/Perpetual、`biconomy` Futures/Perpetual、`bitbns` Margin Trading、`blockchaincom` Perps/第三方接口边界、`coinmetro` Margin/Swap/TRAM-like product | 写 `项目未实现` 或 `当前 adapter 不接入`，不要误写为平台完全不支持。 |
| 明确交易所/API 不支持 | `alpaca`, `bit2c`, `bitbns`, `bitteam`, `bittrade`, `blockchaincom`, `btcbox`, `btcturk`, `coinmate`, `coinmetro` 标准合约 | 单交易所文档写 `交易所不支持合约`；如官方后续开放 derivatives API 再重核。 |

## 官方/一手资料来源

| adapter | 官方来源 |
| --- | --- |
| `alpaca` | <https://docs.alpaca.markets/us/docs/crypto-trading>、<https://docs.alpaca.markets/us/docs/margin-and-short-selling>、<https://docs.alpaca.markets/us/v1.1/docs/options-trading-overview> |
| `bequant` | <https://api.bequant.io/>、<https://support.bequant.io/en/articles/6417914-futures-market-trading> |
| `biconomy` | <https://www.biconomy.com/en/futures-information>、<https://static-exchange.biconomy.com/futures-information> |
| `bit2c` | <https://bit2c.co.il/home/api?language=en-US> |
| `bitbns` | <https://bitbns.com/trade-mechanism/>、<https://bitbns.com/trade/>、<https://github.com/bitbns-official/node-bitbns-api> |
| `bitteam` | <https://bit.team/docs>、<https://bit.team/trade/api/documentation> |
| `bittrade` | <https://api-doc.bittrade.co.jp/>、<https://www.bittrade.co.jp/ja-jp/user/api/> |
| `blockchaincom` | <https://exchange.blockchain.com/api>、<https://github.com/blockchain/docs-exchange-api>、<https://www.blockchain.com/perps>、<https://support.blockchain.com/hc/en-us/articles/26445296035356-What-are-Perpetual-Futures-Perps-on-Blockchain-com> |
| `btcbox` | <https://blog.btcbox.jp/en/archives/8762> |
| `btcturk` | <https://docs.btcturk.com/>、<https://docs.btcturk.com/docs/private-endpoints/submit-order> |
| `coinmate` | <https://coinmate.io/en>、<https://coinmate.docs.apiary.io/> |
| `coinmetro` | <https://github.com/CoinMetro/API-DOCS>、<https://media.coinmetro.com/margin_guidelines_3.pdf>、<https://intercom-help.eu/coinmetrohelpcenter/en/articles/317644-getting-started-with-coinmetro-s-margin-platform> |

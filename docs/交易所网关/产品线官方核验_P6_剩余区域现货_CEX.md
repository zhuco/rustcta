# 产品线官方核验 P6 剩余区域现货 CEX

状态日期：2026-06-08

本文件核验剩余 22 个产品线待确认的 spot adapter。目标是把最后一批“项目只声明 Spot，需确认是否没有合约”转成明确结论：官方支持但项目未接的写 `项目未实现`；当前官方/API 口径没有标准合约的写 `交易所不支持合约`；官方只出现非标准 margin/loan/swap/P2P/白标能力时单独说明，不混入 futures/perpetual。

## 核验口径

- 标准合约指 futures、perpetual、options，以及有 positions、leverage、margin mode、funding、mark price、risk tier、liquidation 等统一合约语义的 API。
- Margin、信用取引、借贷抵押、白标 exchange framework、P2P、swap/convert、OTC、支付、出入金不直接等同标准 futures/perpetual/options。
- 官方页面显示“开发中”“暂停中”时，不写成项目缺实现；写当前不可接入，并注明后续需重核。
- OKX 区域 profile 以对应官方区域域名/API 文档为准。若区域文档出现 derivatives，但当前 adapter 只接 spot，写 `项目未实现` 或“需区域资格核验”，不能写 `交易所不支持合约`。

## 产品线矩阵

| adapter | 官方产品线结论 | 当前项目声明 | 结论和下一步 |
| --- | --- | --- | --- |
| `coinsph` | 官方 API/帮助中心明确 Spot Trade API，REST 覆盖 exchangeInfo、depth、orders、account、wallet/payment 边界；未见标准 futures/perpetual/options。 | `spot`。 | 写 `交易所不支持合约`。 |
| `coinspot` | 官方 API/V2 API 覆盖 AUD spot markets、balances、orders、history；未见标准 futures/perpetual/options/margin。 | `spot`。 | 写 `交易所不支持合约`。 |
| `cryptomus` | 官方 API 目录把 Exchange 分为 trading pairs、limit/market orders、orderbook、WS，另有 merchant/payment/payout/conversions；未见标准合约。 | `spot`。 | 写 `交易所不支持合约`；payment/conversion 不并入交易所合约。 |
| `exmo` | 官方 Spot API 只覆盖 spot REST/WS；官方 EXMO Margin 已上线且有 Margin API/费率资料；Perpetual futures 官方写 active development，当前未上线。 | `spot`。 | 写 `项目未实现 Margin`；当前标准 futures/perpetual/options 写 `交易所不支持合约`，perps 上线后重核。 |
| `fmfwio` | 官方 FMFW.io v3 API 有 Margin Trading 与 Futures Trading，含 futures account、positions、orders、trading WS。 | `spot`。 | 写 `项目未实现 Margin/Futures/Perpetual`；不能写交易所不支持合约。 |
| `foxbit` | 官方 REST/WS v3 与当前公开资料覆盖 Foxbit Exchange spot；Rispar 抵押借贷的 margin call 不是交易所合约仓位；未见标准 futures/perpetual/options API。 | `spot`。 | 写 `交易所不支持合约`；Rispar loan/OTC/Prime Desk 不并入合约。 |
| `hollaex` | 官方 `api.hollaex.com/v2` 是 HollaEx demo/API spot exchange surface；白标/admin 能力不等于此 adapter 的合约产品线；未见标准 futures/perpetual/options。 | `spot`。 | 写 `交易所不支持合约`；任意 HollaEx 白标 venue 需单独 profile。 |
| `indodax` | 官方 public/private API 与 PDF/GitHub docs 覆盖 IDR spot pairs、TAPI trading/account；未见标准 futures/perpetual/options/margin API。 | `spot`。 | 写 `交易所不支持合约`。 |
| `latoken` | 官方 API v1/v2 是 spot order/account/market data，API 权限写 Trading on Spot；官方 `/futures` 页面有产品线索但未在当前 API v1/v2 文档中给出稳定 endpoint。 | `spot`。 | 不写成交易所完全不支持；写 `项目未实现 Futures/需官方 endpoint spec 核验`。 |
| `mercado` | 官方 Mercado Bitcoin API v4 覆盖 symbols、orderbook、trades、accounts、balances、orders、fees、wallet；`positions` 是 open orders，不是合约仓位；未见标准 futures/perpetual/options。 | `spot`。 | 写 `交易所不支持合约`。 |
| `myokx` | 官方 `my.okx.com/docs-v5` 是 OKX v5 API，包含 Spot/Margin、Futures、Swap、Options、Events、positions、leverage、funding/open interest。 | `spot`。 | 写 `项目未实现 Margin/Swap/Futures/Options/Events` 或明确委托主 `okx` adapter；不能写交易所不支持合约。 |
| `ndax` | 官方 NDAX API 有 instruments/products/order/account；响应字段含 margin enabled、lending/profit-loss account 字段，但 instrument type 当前为 standard；未见标准 futures/perpetual/options。 | `spot`。 | 写 `项目未实现 Margin account semantics`（如果账号/venue 开通）；标准 futures/perpetual/options 写 `交易所不支持合约`。 |
| `novadax` | 官方 API 覆盖 symbols、market data、orders、account、wallet；未见 futures/perpetual/options/margin。 | `spot`。 | 写 `交易所不支持合约`。 |
| `okxus` | OKX U.S. 官方帮助中心当前有 perpetual futures、event contracts、spot & margin trading 资料；区域可用性/资格仍需单独审计。 | `spot`。 | 写 `项目未实现 Perpetual Futures/Event Contracts/Margin boundary`，并补区域资格/API 域名核验；不能写交易所不支持合约。 |
| `onetrading` | One Trading 官方 Futures API 明确 `PERP` perpetual futures instruments、margin、positions、futures order path。 | `spot`。 | 写 `项目未实现 Futures/Perpetual`。 |
| `p2b` | 官方 P2B API/Swagger 覆盖 spot pairs、orderbook、balance、order、trades、P2P；未见标准 futures/perpetual/options。 | `spot`。 | 写 `交易所不支持合约`；P2P 不并入中心化订单簿。 |
| `paymium` | 官方 API/adapter scope 是 BTC/EUR spot public/private REST 与 socket.io；未见 margin/futures/perpetual/options。 | `spot`。 | 写 `交易所不支持合约`。 |
| `tokocrypto` | 官方 API 是 Tokocrypto spot/Binance-family surface；exchangeInfo 里 `spotTradingEnable=1`、`marginTradingEnable=0`；未见 futures/perpetual/options。 | `spot`。 | 写 `交易所不支持合约`。 |
| `wavesexchange` | 官方 WX matcher REST/WS 是 Waves spot asset pair matcher；签名订单/Matcher 不提供标准合约仓位、资金费、mark price。 | `spot`。 | 写 `交易所不支持合约`。 |
| `yobit` | 官方 API v3/TAPI 覆盖 public spot market data、Trade API、Defi swap/Yobicode 等；未见标准 futures/perpetual/options。 | `spot`。 | 写 `交易所不支持合约`；Defi swap 不并入合约。 |
| `zaif` | 官方 API 文档覆盖 Orderbook trading；官方信用取引页面写“当前停止中”；未见当前可接的标准 futures/perpetual/options API。 | `spot`。 | 当前 margin/信用取引不作为项目缺实现；标准合约写 `交易所不支持合约`，如信用取引恢复再重核。 |
| `zebpay` | 官方 ZebPay API services 和 docs 明确 Spot + Perpetual Futures/Futures API，含 futures market/orderbook/exchangeInfo、leverage/margin 字段。 | `spot`。 | 写 `项目未实现 Futures/Perpetual`；不能写交易所不支持合约。 |

## 转入任务

| 类型 | adapter | 写法 |
| --- | --- | --- |
| 官方支持但项目未实现 | `exmo` Margin、`fmfwio` Margin/Futures/Perpetual、`latoken` Futures endpoint spec、`myokx` Margin/Swap/Futures/Options/Events、`ndax` Margin account semantics、`okxus` Perpetual Futures/Event Contracts/Margin boundary、`onetrading` Futures/Perpetual、`zebpay` Futures/Perpetual | 写 `项目未实现` 或“需官方 endpoint/区域资格核验”，不要写成交易所不支持。 |
| 当前官方/API 口径交易所不支持 | `coinsph`, `coinspot`, `cryptomus`, `exmo`, `foxbit`, `hollaex`, `indodax`, `mercado`, `ndax`, `novadax`, `p2b`, `paymium`, `tokocrypto`, `wavesexchange`, `yobit`, `zaif` 标准合约 | 单交易所文档写 `交易所不支持合约`；有 margin/loan/swap/P2P/暂停服务的单独注明。 |

## 官方/一手资料来源

| adapter | 官方来源 |
| --- | --- |
| `coinsph` | <https://docs.coins.ph/rest-api/>、<https://support.coins.ph/hc/en-us/articles/11620395767193-How-to-set-up-API-for-Spot-Trade> |
| `coinspot` | <https://www.coinspot.com.au/api>、<https://www.coinspot.com.au/v2/api> |
| `cryptomus` | <https://doc.cryptomus.com/>、<https://doc.cryptomus.com/methods/exchange/list-of-available-trading-pairs>、<https://doc.cryptomus.com/methods/exchange/websockets> |
| `exmo` | <https://support.exmo.com/hc/en-us/articles/14338236557084-API-documentation>、<https://exmo.com/blog/education/how-to-trade-on-exmo-margin/>、<https://exmo.com/blog/en/product-updates/perpetual-futures-in-active-development/> |
| `fmfwio` | <https://api.fmfw.io/> |
| `foxbit` | <https://api.foxbit.com.br/rest/v3>、<https://faq.foxbit.com.br/hc/pt-br/sections/35382989286029-Rispar> |
| `hollaex` | <https://apidocs.hollaex.com/> |
| `indodax` | <https://github.com/btcid/indodax-official-api-docs/blob/master/Public-RestAPI.md>、<https://indodax.com/downloads/INDODAXCOM-API-DOCUMENTATION.pdf> |
| `latoken` | <https://api.latoken.com/doc/v1/>、<https://api.latoken.com/doc/v2/>、<https://latoken.zendesk.com/hc/en-us/articles/360012019619-API-Permissions-Sets>、<https://latoken.com/futures> |
| `mercado` | <https://api.mercadobitcoin.net/> |
| `myokx` | <https://my.okx.com/docs-v5/en/> |
| `ndax` | <https://apidoc.ndax.io/> |
| `novadax` | <https://doc.novadax.com/en-US/> |
| `okxus` | <https://www.okx.com/en-us/help/how-do-i-trade-perpetual-futures-on-okx>、<https://www.okx.com/en-us/help/section/faq-trading> |
| `onetrading` | <https://docs.onetrading.com/futures/introduction> |
| `p2b` | <https://p2pb2b.com/referral/api>、<https://api.p2pb2b.com/> |
| `paymium` | <https://paymium.github.io/api-documentation/> |
| `tokocrypto` | <https://www.tokocrypto.com/apidocs/> |
| `wavesexchange` | <https://docs.waves.exchange/en/waves-matcher>、<https://docs.waves.exchange/en/waves-matcher/matcher-api> |
| `yobit` | <https://www.yobit.net/en/api/>、<https://www.yobit.net/en/rules/> |
| `zaif` | <https://zaif-api-document.readthedocs.io/ja/latest/PublicAPI.html>、<https://zaif-api-document.readthedocs.io/ja/latest/TradingAPI.html>、<https://zaif.jp/doc_margin_trading?lang=ja> |
| `zebpay` | <https://zebpay.com/in/api-services-for-spot-and-futures>、<https://docs.zebpay.com/>、<https://apidocs.zebpay.com/> |

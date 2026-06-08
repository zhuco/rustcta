# Exchange Support Matrix

Status date: 2026-06-08

This page tracks exchange coverage for the `rustcta-exchange-gateway` adapter
surface. Icons should prefer CoinGecko exchange images from
`https://www.coingecko.com/en/exchanges` or the CoinGecko exchange APIs. If a
venue is missing from CoinGecko, use the icon shown on CoinGlass exchange/Dex
rankings or a remote official-domain favicon. Do not commit downloaded logo
binaries unless the license and update process are clear.

Source references used for the current split:

- CoinGlass centralized derivatives exchange ranking:
  <https://www.coinglass.com/zh/exchanges>
- CoinGlass decentralized derivatives exchange ranking:
  <https://www.coinglass.com/zh/dex>
- CoinGecko exchange ranking and icon source:
  <https://www.coingecko.com/en/exchanges>
- CoinGecko exchanges API docs:
  <https://docs.coingecko.com/reference/exchanges-list>
- CoinGecko derivatives exchange API docs:
  <https://docs.coingecko.com/reference/derivatives-exchanges>
- Expansion plan for remaining CCXT venues and hot perpetual DEX venues:
  `docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`

## Status Legend

| Status | Meaning |
| --- | --- |
| `gateway` | Adapter directory exists in `crates/rustcta-exchange-gateway/src/adapters/`; capability depth still follows each adapter document and validation logs. |
| `parallel` | Historical status for active task packets; completed 2026-06-08 tasks are now marked `gateway`. |
| `planned` | Not a current adapter; assigned in an expansion task for G0/G1 API audit or later implementation. |
| `profile` | Should usually reuse an existing API family adapter/profile rather than duplicate implementation. |

## Centralized / Custodial Venues

These are CEX, custodial venues, broker venues, or institution-style exchange
APIs. Some include futures/perpetual products; product coverage is adapter
specific.

| Icon | Adapter id | Venue | Products / notes | Status |
| --- | --- | --- | --- | --- |
| <img src="https://www.google.com/s2/favicons?domain=ascendex.com&sz=32" width="20" /> | `ascendex` | AscendEX | Spot + futures/perp gateway | gateway |
| <img src="https://www.google.com/s2/favicons?domain=backpack.exchange&sz=32" width="20" /> | `backpack` | Backpack Exchange | Spot/perp exchange API | gateway |
| <img src="https://www.google.com/s2/favicons?domain=biconomy.com&sz=32" width="20" /> | `biconomy` | Biconomy | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=big.one&sz=32" width="20" /> | `bigone` | BigONE | Spot | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/52/small/binance.jpg?1706864274" width="20" /> | `binance` | Binance | Spot + USD-M perp baseline | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/52/small/binance.jpg?1706864274" width="20" /> | `binancecoinm` | Binance COIN-M | Inverse/coin-margined futures profile | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/812/small/YtFwQwJr_400x400.jpg?1706864837" width="20" /> | `bingx` | BingX | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=bitbank.cc&sz=32" width="20" /> | `bitbank` | bitbank | Japan spot | gateway |
| <img src="https://www.google.com/s2/favicons?domain=bitfinex.com&sz=32" width="20" /> | `bitfinex` | Bitfinex | Spot/margin/derivatives | gateway |
| <img src="https://www.google.com/s2/favicons?domain=bitflyer.com&sz=32" width="20" /> | `bitflyer` | bitFlyer | Japan spot/FX | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/540/small/2023-07-25_21.47.43.jpg?1706864507" width="20" /> | `bitget` | Bitget | Spot + USDT perp | gateway |
| <img src="https://www.google.com/s2/favicons?domain=bithumb.com&sz=32" width="20" /> | `bithumb` | Bithumb | Korea spot | gateway |
| <img src="https://www.google.com/s2/favicons?domain=bitkan.com&sz=32" width="20" /> | `bitkan` | BitKan | Conservative gateway shell | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/529/small/ezgif.com-gif-maker.jpg?1706864499" width="20" /> | `bitmart` | BitMart | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=bitmex.com&sz=32" width="20" /> | `bitmex` | BitMEX | Derivatives | gateway |
| <img src="https://www.google.com/s2/favicons?domain=bitrue.com&sz=32" width="20" /> | `bitrue` | Bitrue | Spot + futures | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/8/small/Bitso-icon-dark.png?1706864249" width="20" /> | `bitso` | Bitso | LATAM spot | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/9/small/bitstamp.jpg?1706864251" width="20" /> | `bitstamp` | Bitstamp | Spot | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/1185/small/APP_icon_1024.png?1706865197" width="20" /> | `bitunix` | Bitunix | Spot + futures | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/714/small/bitvavo-mark-square-black.png?1706864670" width="20" /> | `bitvavo` | Bitvavo | Europe spot | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/1282/small/blofin.png?1716974744" width="20" /> | `blofin` | BloFin | USDT perpetual | gateway |
| <img src="https://www.google.com/s2/favicons?domain=btcmarkets.net&sz=32" width="20" /> | `btcmarkets` | BTC Markets | Australia spot | gateway |
| <img src="https://www.google.com/s2/favicons?domain=btcturk.com&sz=32" width="20" /> | `btcturk` | BtcTurk | Turkey spot | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/905/small/bullish_com.png?1706864904" width="20" /> | `bullish` | Bullish | Institutional exchange | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/698/small/bybit_spot.png?1706864649" width="20" /> | `bybit` | Bybit | Spot + USDT/USDC perp | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/23/small/Coinbase_Coin_Primary.png?1706864258" width="20" /> | `coinbase` | Coinbase Advanced / INTX | Spot + international perp | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/23/small/Coinbase_Coin_Primary.png?1706864258" width="20" /> | `coinbaseexchange` | Coinbase Exchange | Exchange API profile | gateway |
| <img src="https://www.google.com/s2/favicons?domain=coincheck.com&sz=32" width="20" /> | `coincheck` | Coincheck | Japan spot | gateway |
| <img src="https://www.google.com/s2/favicons?domain=coindcx.com&sz=32" width="20" /> | `coindcx` | CoinDCX | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=coinex.com&sz=32" width="20" /> | `coinex` | CoinEx | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=coinone.co.kr&sz=32" width="20" /> | `coinone` | Coinone | Korea spot | gateway |
| <img src="https://www.google.com/s2/favicons?domain=coins.ph&sz=32" width="20" /> | `coinsph` | Coins.ph | Philippines spot | gateway |
| <img src="https://www.google.com/s2/favicons?domain=coinspot.com.au&sz=32" width="20" /> | `coinspot` | CoinSpot | Australia spot | gateway |
| <img src="https://www.google.com/s2/favicons?domain=coinstore.com&sz=32" width="20" /> | `coinstore` | Coinstore | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=cointr.com&sz=32" width="20" /> | `cointr` | CoinTR | Spot + futures | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/1172/small/coinw_new_logo.png?1713879109" width="20" /> | `coinw` | CoinW | Spot + futures | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/589/small/h2oMjPp6_400x400.jpg?1706864542" width="20" /> | `cryptocom` | Crypto.com Exchange | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=deepcoin.com&sz=32" width="20" /> | `deepcoin` | Deepcoin | Derivatives | gateway |
| <img src="https://www.google.com/s2/favicons?domain=delta.exchange&sz=32" width="20" /> | `delta` | Delta Exchange | Futures/options | gateway |
| <img src="https://www.google.com/s2/favicons?domain=deribit.com&sz=32" width="20" /> | `deribit` | Deribit | Options + futures/perp | gateway |
| <img src="https://www.google.com/s2/favicons?domain=digifinex.com&sz=32" width="20" /> | `digifinex` | DigiFinex | Spot + futures | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/60/small/Frame_1.png?1747795534" width="20" /> | `gateio` | Gate.io | Spot + futures | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/50/small/gemini.png?1706864273" width="20" /> | `gemini` | Gemini | US spot | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/1582/small/20240422-181043.jpg?1715067065" width="20" /> | `hashkey_global` | HashKey Global | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=htx.com&sz=32" width="20" /> | `htx` | HTX | Spot + USDT perp | gateway |
| <img src="https://www.google.com/s2/favicons?domain=huobi.com&sz=32" width="20" /> | `huobi` | Huobi | HTX legacy profile | gateway |
| <img src="https://www.google.com/s2/favicons?domain=independentreserve.com&sz=32" width="20" /> | `independentreserve` | Independent Reserve | Australia/Singapore spot | gateway |
| <img src="https://www.google.com/s2/favicons?domain=indodax.com&sz=32" width="20" /> | `indodax` | Indodax | Indonesia spot | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/29/small/kraken.jpg?1706864265" width="20" /> | `kraken` | Kraken | Spot + futures specs | gateway |
| <img src="https://www.google.com/s2/favicons?domain=futures.kraken.com&sz=32" width="20" /> | `krakenfutures` | Kraken Futures | Futures/perp | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/61/small/kucoin.png?1706864282" width="20" /> | `kucoin` | KuCoin | Spot | gateway |
| <img src="https://www.google.com/s2/favicons?domain=futures.kucoin.com&sz=32" width="20" /> | `kucoinfutures` | KuCoin Futures | Futures/perp | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/118/small/LBank_200_200.png?1757347528" width="20" /> | `lbank` | LBank | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=luno.com&sz=32" width="20" /> | `luno` | Luno | Regional spot | gateway |
| <img src="https://www.google.com/s2/favicons?domain=mercadobitcoin.com.br&sz=32" width="20" /> | `mercado` | Mercado Bitcoin | Brazil spot | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/409/small/164286be-32a5-4b58-978c-d072eea00eb9.jpeg?1775619316" width="20" /> | `mexc` | MEXC | Spot + futures | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/96/small/WeChat_Image_20220117220452.png?1706864283" width="20" /> | `okx` | OKX | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=orangex.com&sz=32" width="20" /> | `orangex` | OrangeX | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=ox.fun&sz=32" width="20" /> | `oxfun` | OX.FUN | Derivatives | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/519/small/phemex-exchange-new-logo.png?1763018228" width="20" /> | `phemex` | Phemex | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=poloniex.com&sz=32" width="20" /> | `poloniex` | Poloniex | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=tapbit.com&sz=32" width="20" /> | `tapbit` | Tapbit | Spot + futures | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/1152/small/Toobit_logo400X400_%281%29.png?1706865168" width="20" /> | `toobit` | Toobit | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=upbit.com&sz=32" width="20" /> | `upbit` | Upbit | Korea spot | gateway |
| <img src="https://www.google.com/s2/favicons?domain=weex.com&sz=32" width="20" /> | `weex` | WEEX | Spot + futures | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/1074/small/800_800.jpg?1706865098" width="20" /> | `whitebit` | WhiteBIT | Spot + futures | gateway |
| <img src="https://www.google.com/s2/favicons?domain=woo.org&sz=32" width="20" /> | `woo` | WOO X | Spot + perp | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/1034/small/xt_logo_%E7%BB%BF.png?1761205961" width="20" /> | `xt` | XT.com | Spot + futures | gateway |

## Decentralized / On-Chain / Hybrid Perpetual Venues

These adapters or tasks involve DEX, on-chain accounts, hybrid self-custody, or
chain-specific signing. Treat private write support as unverified until each
adapter document records request-spec, signing vectors, and reconciliation
tests.

| Icon | Adapter id | Venue | Products / notes | Status |
| --- | --- | --- | --- | --- |
| <img src="https://www.google.com/s2/favicons?domain=apex.exchange&sz=32" width="20" /> | `apex` | ApeX | Perp DEX | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/22084/small/aster-profile-200.png?1756966162" width="20" /> | `aster` | Aster | Perp DEX | gateway |
| <img src="https://www.google.com/s2/favicons?domain=derive.xyz&sz=32" width="20" /> | `derive` | Derive | Options + perp DEX | gateway |
| <img src="https://www.google.com/s2/favicons?domain=dydx.exchange&sz=32" width="20" /> | `dydx` | dYdX | Perp DEX | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/11862/small/grvt.jpg?1768789669" width="20" /> | `grvt` | GRVT | Hybrid self-custody exchange | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/1208/small/Hyperliquid_logo.png?1706865217" width="20" /> | `hyperliquid` | Hyperliquid | Perp DEX | gateway |
| <img src="https://coin-images.coingecko.com/markets/images/22097/small/lighter.jpg?1758003181" width="20" /> | `lighter` | Lighter | Perp DEX | gateway |
| <img src="https://www.google.com/s2/favicons?domain=pacifica.fi&sz=32" width="20" /> | `pacifica` | Pacifica | Perp DEX | gateway |
| <img src="https://www.google.com/s2/favicons?domain=paradex.trade&sz=32" width="20" /> | `paradex` | Paradex | Perp/options DEX | gateway |

## Planned Hot Perpetual DEX Expansion

The full task list lives in
`docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`. This short
view keeps the README-facing matrix readable while still separating DEX work
from CEX work.

| Icon | Adapter id | Venue | Notes | Status |
| --- | --- | --- | --- | --- |
| <img src="https://coin-images.coingecko.com/markets/images/11726/small/Square.png?1775697036" width="20" /> | `edgex` | edgeX | Hot Perp DEX candidate | planned |
| <img src="https://coin-images.coingecko.com/markets/images/22114/small/extended.png?1759117306" width="20" /> | `extended` | Extended | Perp DEX candidate | planned |
| <img src="https://coin-images.coingecko.com/markets/images/22179/small/Square_-_Dark.png?1777432143" width="20" /> | `gmtrade` | GMTrade | Solana perp/RWA candidate | planned |
| <img src="https://www.google.com/s2/favicons?domain=app.gmx.io&sz=32" width="20" /> | `gmx` | GMX | Arbitrum/Avalanche perp protocol | planned |
| <img src="https://www.google.com/s2/favicons?domain=drift.trade&sz=32" width="20" /> | `drift` | Drift | Solana perp DEX | planned |
| <img src="https://www.google.com/s2/favicons?domain=vertexprotocol.com&sz=32" width="20" /> | `vertex` | Vertex | Perp DEX | planned |
| <img src="https://coin-images.coingecko.com/markets/images/1438/small/aevo.png?1706865470" width="20" /> | `aevo` | Aevo | Options + perp DEX | planned |
| <img src="https://www.google.com/s2/favicons?domain=orderly.network&sz=32" width="20" /> | `orderly` | Orderly | Perp liquidity network | planned |
| <img src="https://www.google.com/s2/favicons?domain=synfutures.com&sz=32" width="20" /> | `synfutures` | SynFutures | Perp DEX | planned |
| <img src="https://www.google.com/s2/favicons?domain=bluefin.io&sz=32" width="20" /> | `bluefin` | Bluefin | Sui perp DEX | planned |
| <img src="https://www.google.com/s2/favicons?domain=gains.trade&sz=32" width="20" /> | `gains_network` | gTrade | RWA/crypto perpetuals | planned |
| <img src="https://www.google.com/s2/favicons?domain=jup.ag&sz=32" width="20" /> | `jupiter_perps` | Jupiter Perps | Solana perpetuals | planned |
| <img src="https://www.google.com/s2/favicons?domain=flash.trade&sz=32" width="20" /> | `flash_trade` | Flash Trade | Solana perpetuals | planned |
| <img src="https://www.google.com/s2/favicons?domain=perennial.finance&sz=32" width="20" /> | `perennial` | Perennial | Intent-based perpetuals | planned |
| <img src="https://www.google.com/s2/favicons?domain=synthetix.io&sz=32" width="20" /> | `synthetix_perps` | Synthetix Perps | Protocol profile | planned |
| <img src="https://www.google.com/s2/favicons?domain=levana.finance&sz=32" width="20" /> | `levana` | Levana | Cosmos perpetuals | planned |
| <img src="https://www.google.com/s2/favicons?domain=vela.exchange&sz=32" width="20" /> | `vela` | Vela | Arbitrum/Base perpetuals | planned |
| <img src="https://www.google.com/s2/favicons?domain=hmx.org&sz=32" width="20" /> | `hmx` | HMX | Arbitrum perpetuals | planned |
| <img src="https://www.google.com/s2/favicons?domain=rabbitx.io&sz=32" width="20" /> | `rabbitx` | RabbitX | Perp DEX | planned |
| <img src="https://www.google.com/s2/favicons?domain=zeta.markets&sz=32" width="20" /> | `zeta_markets` | Zeta Markets | Solana derivatives | planned |

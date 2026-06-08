# 交易所网关余下交易所与新增永续市场一交易所一 AI 开发任务

状态日期：2026-06-08

本文档衔接 `docs/exchange_gateway_expansion_30_venues_zh.md` 和
`docs/exchange_gateway_next_40_parallel_tasks_zh.md`。上一轮约 40 个交易所正在并行
实现中，本文件不判断那些任务是否完成，也不要求抢改上一轮 adapter 目录。本文件只规
划两类新增任务：

- CCXT 4.5.56 的 79 个缺口中，未纳入上一轮 40 个任务的余下 39 个交易所。
- 2026-06-08 当前衍生品榜单、Perp DEX 热度和公开资料中，支持永续合约但未在标准
  gateway adapter 里稳定覆盖的额外候选交易所。

执行方式改为“每个 AI 一个任务，只负责一个交易所”。任务说明不指定任何模型，只指定
工程边界、交付物和验收命令。

## 资料口径

- CCXT 缺口口径沿用 `docs/exchange_gateway_expansion_30_venues_zh.md` 的
  `ccxt@4.5.56` 清单。
- 永续交易所候选参考 CoinGecko derivatives exchanges API、CoinMarketCap derivatives
  rankings、DeFiLlama perps 数据入口和近期 Perp DEX 市场资料。
- 热度不等于可实现。交易所没有稳定官方 API、没有私有交易权限文档、只暴露网页接口、
  或链上交易需要非托管钱包签名时，任务应停在 G0/G1 审计或 scan-only，不得伪装成
  可真实交易的 adapter。

参考入口：

- CCXT manual: <https://github.com/ccxt/ccxt/wiki/manual>
- CoinGecko derivatives exchanges API: <https://docs.coingecko.com/reference/derivatives-exchanges>
- CoinMarketCap derivatives rankings: <https://coinmarketcap.com/rankings/exchanges/derivatives/>
- DeFiLlama downloads / perps volume: <https://defillama.com/downloads>
- edgeX API docs: <https://edgex-1.gitbook.io/edgeX-documentation/api>
- Jupiter Perps quickstart: <https://support.jup.ag/hc/en-us/articles/18734952106908-Perpetuals-a-Quickstart>

本次补充搜索结论：

- CoinGecko Perp DEX 榜在 2026-06-08 可见 68 个结果，并列出 Hyperliquid、Aster、
  Variational Omni、Lighter、edgeX、GRVT、Antarctic、AlphaX、Extended、GMTrade、
  Ostium、ApeX Omni、StandX、Pacifica、IO Trader、Vest、dYdX、SoDEX、LN Exchange、
  Orderly、EVEDEX、gTrade、Gate DEX、Astros、Paradex、Aevo、Ondo Perps、Decibel、
  Katana Perps、Flash Trade、Dipcoin、SynFutures、KiloEx、Strike Finance、DeriW、
  SparkDEX、Navigator、Demex、Perpetual Protocol、Bluefin、JOJO、Helix、MYX、RabbitX、
  Drift、Surf、FWX、Sunperp、EnclaveX 等。
- DeFiLlama/市场文章还反复出现 Reya、Polynomial、LogX、Perennial、Synthetix Perps、
  Levana、Vela、HMX、IntentX、Satori、Aark、Equation、ApolloX DEX、D8X、Mango、
  Zeta 等协议名。这些协议交易 API 成熟度差异很大，默认先 G0/G1。
- 对 GMX/gTrade/KiloEx/MYX/FWX 等多链同协议，不按每条链重复拆 adapter；一个 adapter
  内使用 `chain_profile` 或 config profile 表达 Arbitrum/Base/BSC/opBNB/Avalanche
  等链差异。

## 与上一轮 40 个任务的边界

上一轮任务里的以下交易所仍由
`docs/exchange_gateway_next_40_parallel_tasks_zh.md` 对应 AI 负责，本文件不重新分配：

`apex`, `aster`, `binancecoinm`, `bitbank`, `bitfinex`, `bitflyer`, `bithumb`,
`bitmart`, `bitso`, `bitstamp`, `bitvavo`, `btcmarkets`, `btcturk`, `bullish`,
`bybit`, `coinbaseexchange`, `coincheck`, `coinone`, `coinsph`, `coinspot`, `delta`,
`deribit`, `derive`, `dydx`, `gemini`, `grvt`, `htx`, `huobi`, `hyperliquid`,
`independentreserve`, `indodax`, `krakenfutures`, `kucoinfutures`, `lighter`, `luno`,
`mercado`, `oxfun`, `pacifica`, `paradex`, `upbit`。

如果本文件的热度候选与上一轮任务存在 API 族重合，只做 alias/profile 审计，不复制
adapter。

## 总目标

为每个任务新增或迁移一个 `rustcta-exchange-gateway` 标准 adapter，至少达到“离线可
验”或“明确 Unsupported 边界”的状态：

- 每个 AI 只负责一个交易所，adapter id 使用 snake_case。
- 有独立 adapter 目录、endpoint mapping、fixtures、adapter 文档、disabled config
  example 和 named registration。
- 永续/期货交易所优先实现 `MarketType::Perpetual` / `MarketType::Futures`；只有现
  货能力时实现 `MarketType::Spot`。
- 官方没有稳定接口的能力必须显式 `Unsupported`，不能用网页接口、非公开接口或本地
  组合逻辑伪装成交易所原生能力。
- 私有写接口必须有 request-spec 和签名向量，只做离线请求构造验证，不做真实下单。
- WebSocket 至少交付订阅/退订 payload、鉴权 payload、心跳策略和 parser fixture；
  若没有稳定私有 WS，必须写 REST reconciliation fallback。

## 不做的事

- 不运行 `cargo build`、`cargo build --release`、app/web build 或任何发布构建。
- 不做真实下单、真实撤单、提现、转账、划转、API key 写入或生产私有流长期运行。
- 不把 CCXT、CoinGecko、CoinMarketCap 或 DeFiLlama 当作最终 API 权威；最终以官方
  API 文档和本地 fixture 为准。
- 不改上一轮 40 个任务的 adapter 目录、fixtures、config 或文档。
- 不一次性扩展共享 trait。确实需要新增共享模型或方法时，先提交协调说明。
- 不在 prompt、任务名或交付要求里指定模型。

## 每个 AI 的全局规则

1. 先运行 `git status --short`，记录无关 dirty files，不回滚其他人的改动。
2. 阅读本文件、`docs/exchange_gateway_next_40_parallel_tasks_zh.md`、
   `docs/exchange_adapter_toolchain_completion_zh.md`、
   `docs/exchange_api_completion_matrix.md` 和一个相近 adapter 文档。
3. 只改自己交易所的 adapter 目录、fixtures、文档、disabled config example。
4. 共享文件只做追加式注册：`crates/rustcta-exchange-gateway/src/adapters/mod.rs`、
   `crates/rustcta-exchange-gateway/src/lib.rs`、`apps/gateway/src/config.rs`、必要配
   置示例。冲突由协调者按 adapter id 字母序合并。
5. 新增文件命名全部使用 snake_case adapter id。
6. 所有凭据 fixture 必须脱敏，不提交真实 key、secret、passphrase、token、账户 id、
   地址、手机号、邮箱或订单 id。
7. 私有 REST 写接口默认只做离线 request-spec；live read-only 和 live-dry-run 只能
   后续由专门验证任务打开。
8. 结束时提交能力边界、验收命令和未跑命令说明。

## 标准交付物

| 类别 | 必须交付 |
| --- | --- |
| Adapter 代码 | `crates/rustcta-exchange-gateway/src/adapters/<id>/config.rs`、`signing.rs`、`transport.rs`、`public.rs`、`private.rs`、`parser.rs`、`mod.rs`，按需要增加 `streams.rs`、`private_parser.rs`、`test_support.rs` |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/<id>/endpoint_mapping.yaml`，通过 shared schema 校验 |
| Fixtures | `tests/fixtures/exchanges/<id>/` 下响应样本、`request_specs/*.json`、`signing_vectors/*.json`、WS parser 样本 |
| 文档 | `docs/<id>_adapter.md`，包含产品线、base URL、签名、限速、endpoint mapping、capability、Unsupported 边界、验证命令 |
| 配置 | `config/<id>_gateway_example.yml`，默认 disabled，不含真实凭据 |
| 注册 | gateway adapter module/export/named registration；如果接入 app env，补 redacted config 测试 |

## 能力分层

| 层级 | 目标 | 必须产出 |
| --- | --- | --- |
| G0 API 审计 | 确认官方 REST/WS 文档、产品线、base URL、testnet、权限、签名、限速、地区限制 | adapter 文档的官方资料表、endpoint mapping 初稿、Unsupported 边界 |
| G1 Public REST | 标准 symbol rules/order book，永续额外 funding/mark/open interest 能力审计 | parser fixtures、public request tests、`get_symbol_rules`、`get_order_book` |
| G2 Private Read REST | balances、positions、fees、open orders、query order、fills | request-spec、签名向量、只读 parser tests |
| G3 Private Write REST | place/cancel/cancel-all/amend/order-list 能力判断 | 写接口 request-spec；没有官方接口则 Unsupported |
| G4 Batch | native/composed batch place/cancel、原子性、最大条数、部分失败 | endpoint mapping `native_batch`/`atomicity`、batch response tests |
| G5 WebSocket | public/private subscribe/auth/heartbeat/parser/resync | WS payload tests、heartbeat policy、REST reconciliation fallback |
| G6 Advanced Perp | leverage、margin mode、position mode、dead-man/cancel-all-after、risk tiers | 只在官方原生支持且语义可无损映射时实现；否则 Unsupported |

## 允许的验证命令

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/<id>/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway <id> --lib --message-format short
```

如果修改了 `apps/gateway/src/config.rs`，再补：

```bash
cargo test -p rustcta-gateway <id> --message-format short
```

禁止命令：

```bash
cargo build
cargo build --release
cargo build -p rustcta-exchange-gateway
cargo run --bin <anything-that-connects-live>
```

## 分配 prompt 模板

每个任务使用以下模板，把 `<TASK>` 和 `<id>` 替换成任务编号和交易所 id：

```text
You are <TASK>. Complete the one-exchange task for `<id>` from docs/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md. Do not assume or specify a model. Read the global rules first. Implement or audit only this exchange. Deliver a gateway adapter or a conservative scan-only/Unsupported boundary with endpoint mapping, sanitized fixtures, adapter docs, disabled config example, named registration, and focused tests. Do not run cargo build; run only the allowed validation commands.
```

## A 组：CCXT 79 缺口余下 39 个一交易所任务

这些交易所来自 CCXT 缺口清单，但没有放入上一轮 40 个任务。现货长尾先做 scan-only /
G1，除非官方私有交易 API 文档、签名和错误码足够稳定。

| 任务 | AI | Adapter id | 产品目标 | 初始接入建议 | 参考 adapter |
| --- | --- | --- | --- | --- | --- |
| A-01 | AI-R01 | `aftermath` | DeFi/Sui 生态交易面 | 先确认是否有适合中心化网关的订单 API；大概率 G0/G1 scan-only | `backpack`, `hyperliquid` |
| A-02 | AI-R02 | `alpaca` | Broker/crypto | 按经纪接口处理账户、订单、地区限制；crypto spot first | `coinbase`, `gemini` |
| A-03 | AI-R03 | `arkham` | 情报/交易平台 | 先审计交易 API 和权限边界；可能只读 profile | `coinbase`, `kraken` |
| A-04 | AI-R04 | `bequant` | HitBTC API 族 | 优先 alias/profile 审计，避免复制 HitBTC 族实现 | `hitbtc`, `kraken` |
| A-05 | AI-R05 | `binanceus` | 美国 Binance Spot | 优先 Binance profile/alias，处理地区、base URL 和产品裁剪 | `binance` |
| A-06 | AI-R06 | `bit2c` | 区域现货 | public REST + symbol rules/order book；私有交易后置 | `coinex`, `kucoin` |
| A-07 | AI-R07 | `bitbns` | 印度/区域现货 | scan-only public REST，确认私有 API 可用性和地区限制 | `coinex`, `gateio` |
| A-08 | AI-R08 | `bitopro` | 台湾现货 | spot public/private REST，TWD 市场和签名审计 | `coinex`, `bitbank` |
| A-09 | AI-R09 | `bitteam` | 长尾现货 | scan-only public REST，明确私有接口不稳定边界 | `coinex` |
| A-10 | AI-R10 | `bittrade` | 日本现货 / HTX API 族可能性 | 先判断与 Huobi/HTX/日本监管实体 API 差异 | `htx`, `bitbank` |
| A-11 | AI-R11 | `blockchaincom` | 现货/经纪 | public REST + 账户模型审计，私有写离线 request-spec only | `coinbase`, `kraken` |
| A-12 | AI-R12 | `btcbox` | 日本现货 | scan-only public REST，JPY 市场和限速审计 | `bitflyer`, `bitbank` |
| A-13 | AI-R13 | `bybiteu` | Bybit EU profile | 只做 Bybit alias/profile 审计，确认监管 endpoint 差异 | `bybit` |
| A-14 | AI-R14 | `bydfi` | Spot + USDT perpetual | 永续优先，覆盖 funding/positions/orders/WS；签名和 testnet 审计 | `blofin`, `phemex` |
| A-15 | AI-R15 | `cex` | 老牌现货 | public REST first，私有交易接口和法币账本后置 | `kraken`, `coinbase` |
| A-16 | AI-R16 | `coinmate` | 欧洲现货 | EUR 市场 scan-only，私有签名和限速审计 | `bitvavo`, `kraken` |
| A-17 | AI-R17 | `coinmetro` | 欧洲现货 | spot public/private，法币账本和费用模型重点 | `bitvavo`, `gemini` |
| A-18 | AI-R18 | `cryptomus` | 支付/现货 | 先界定支付 API 与交易 API；不把支付能力接入交易 adapter | `coinbase`, `coinex` |
| A-19 | AI-R19 | `exmo` | 欧洲现货 | public/private spot，历史订单和成交分页重点 | `kraken`, `whitebit` |
| A-20 | AI-R20 | `fmfwio` | HitBTC API 族 | alias/profile 优先；若 API 差异大再独立 adapter | `hitbtc`, `bequant` |
| A-21 | AI-R21 | `foxbit` | 巴西现货 | BRL 市场 scan-only，私有交易后置 | `mercado`, `bitso` |
| A-22 | AI-R22 | `hibachi` | Perp/DEX | 永续 API 审计，链上账户/签名/testnet 优先；G0/G1 起步 | `hyperliquid`, `lighter` |
| A-23 | AI-R23 | `hitbtc` | 长尾现货 / derivatives 族 | public REST + WS first；私有交易谨慎，错误码和限速重点 | `kraken`, `poloniex` |
| A-24 | AI-R24 | `hollaex` | 白标交易所框架 | 不泛化白标；只接官方 HollaEx demo/API 或写 profile 生成规则 | `whitebit`, `coinex` |
| A-25 | AI-R25 | `latoken` | 长尾现货 | scan-only public REST，私有交易必须先验证签名向量 | `coinex`, `gateio` |
| A-26 | AI-R26 | `modetrade` | 特殊/DeFi | 先确认交易 API、产品线和地区限制；可能只读 | `backpack`, `hyperliquid` |
| A-27 | AI-R27 | `myokx` | OKX profile | OKX alias/profile，不复制 adapter；确认 base URL 和地区裁剪 | `okx` |
| A-28 | AI-R28 | `ndax` | 加拿大现货 | CAD 市场 scan-only，私有交易和法币账本审计 | `coinbase`, `kraken` |
| A-29 | AI-R29 | `novadax` | 巴西现货 | BRL 市场 public/private spot，分页和限速重点 | `mercado`, `bitso` |
| A-30 | AI-R30 | `okxus` | OKX US profile | OKX alias/profile，处理美国产品裁剪和 endpoint 差异 | `okx` |
| A-31 | AI-R31 | `onetrading` | 欧洲现货 | EUR spot public/private，机构 API 和账户模型审计 | `bitvavo`, `kraken` |
| A-32 | AI-R32 | `p2b` | 长尾现货 | scan-only public REST，私有写保持 Unsupported 直到签名验证 | `coinex` |
| A-33 | AI-R33 | `paymium` | 欧洲现货 | BTC/EUR public REST first，私有交易后置 | `kraken`, `bitvavo` |
| A-34 | AI-R34 | `tokocrypto` | Binance 生态现货 | 优先 Binance API 族/profile；确认 symbol/限速/地区差异 | `binance` |
| A-35 | AI-R35 | `wavesexchange` | 链上现货 | 链上账户模型审计，scan-only public first | `backpack`, `hyperliquid` |
| A-36 | AI-R36 | `woofipro` | DeFi/DEX | API 边界审计，可能只做 public/orderbook scan-only | `woo`, `hyperliquid` |
| A-37 | AI-R37 | `yobit` | 长尾现货 | scan-only public REST；私有交易高风险，默认 Unsupported | `coinex` |
| A-38 | AI-R38 | `zaif` | 日本现货 | JPY 市场 public REST first，私有签名和法币账本审计 | `bitflyer`, `bitbank` |
| A-39 | AI-R39 | `zebpay` | 区域现货 | scan-only public REST，地区、KYC 和私有 API 边界审计 | `coinex`, `kucoin` |

## B 组：新增永续/衍生品热度候选一交易所任务

这些交易所不完全来自 CCXT 4.5.56 缺口。它们来自当前衍生品榜单、Perp DEX 热度或交
易所官方产品线观察，目标是尽量覆盖市场上支持永续合约的交易场所。若发现官方 API 不
稳定，只交付 G0/G1 审计文档、endpoint mapping 草案和 `Unsupported` 边界。

| 任务 | AI | Adapter id | 产品目标 | 初始接入建议 | 参考 adapter |
| --- | --- | --- | --- | --- | --- |
| B-01 | AI-P01 | `coinup` | USDT perpetual CEX | 永续 public/private REST + WS；先核实官方 API 和签名样例 | `blofin`, `bitunix` |
| B-02 | AI-P02 | `aivora` | USDT perpetual CEX | G0 审计优先，确认 API 文档、testnet、订单权限和限速 | `blofin`, `phemex` |
| B-03 | AI-P03 | `primexbt` | Multi-asset perpetual/CFD | 合约产品和 crypto perp 边界审计；不要接入传统 CFD 交易到 crypto adapter | `bitmex`, `deribit` |
| B-04 | AI-P04 | `ourbit` | Spot + perpetual CEX | 永续优先，检查是否可复用 MEXC/API 族；避免盲目复制 | `mexc`, `blofin` |
| B-05 | AI-P05 | `btse` | Futures/perpetual CEX | futures/perp symbol rules、positions、orders、WS；机构账户模型审计 | `krakenfutures`, `deribit` |
| B-06 | AI-P06 | `zoomex` | USDT perpetual CEX | Bybit API 族/profile 可能性优先审计；独立差异再实现 | `bybit`, `phemex` |
| B-07 | AI-P07 | `kcex` | Spot + futures CEX | 永续 public/private REST + WS，先校验官方 OpenAPI 稳定性 | `mexc`, `blofin` |
| B-08 | AI-P08 | `hotcoin` | USDT perpetual CEX | G0/G1 起步，重点确认签名、订单 API 和地区限制 | `coinw`, `bitunix` |
| B-09 | AI-P09 | `edgex` | Perp/DEX | 官方 API 明确 HTTP/WS；优先 public book、positions、orders、链上签名边界 | `hyperliquid`, `lighter` |
| B-10 | AI-P10 | `extended` | Perp/DEX | G0/G1 审计，确认账户模型、撮合 API、WS resync 和资金路径 | `paradex`, `lighter` |
| B-11 | AI-P11 | `variational_omni` | Perp/DEX | 协议型衍生品；先做只读行情和账户签名边界审计 | `derive`, `paradex` |
| B-12 | AI-P12 | `antarctic` | Perp/DEX | G0 审计，确认官方 API、链上签名、testnet 和市场元数据 | `hyperliquid`, `aster` |
| B-13 | AI-P13 | `alphax` | On-chain perpetual | G0/G1 起步，明确链上账户、订单签名和风险数据 | `lighter`, `edgex` |
| B-14 | AI-P14 | `gmtrade` | Solana perpetual/RWA | 不接网页；先查 SDK/API，优先 public/risk data，私有写后置 | `backpack`, `jupiter_perps` |
| B-15 | AI-P15 | `ostium` | RWA perpetual DEX | 先审计 oracle、market spec、链上交易和地区限制；G0/G1 起步 | `derive`, `hyperliquid` |
| B-16 | AI-P16 | `jupiter_perps` | Solana perpetual | SDK/API 审计优先；如无稳定程序化下单 API，则只做 public/risk adapter | `backpack`, `gmtrade` |
| B-17 | AI-P17 | `gmx` | Arbitrum/Avalanche perpetual DEX | 链上 contract/SDK adapter，public/risk data first，交易写后置 | `derive`, `hyperliquid` |
| B-18 | AI-P18 | `drift` | Solana perpetual DEX | SDK 优先，账户/签名/market metadata 审计；WS/orderbook/funding first | `backpack`, `jupiter_perps` |
| B-19 | AI-P19 | `vertex` | Perpetual DEX | REST/WS +链上账户模型，positions/orders/funding 审计 | `paradex`, `derive` |
| B-20 | AI-P20 | `aevo` | Options + perpetual DEX | 期权/永续 public REST first，私有签名和账户模型离线验证 | `deribit`, `derive` |
| B-21 | AI-P21 | `orderly` | Perp liquidity network | 作为 venue/network adapter 审计，不把下游白标重复注册 | `woo`, `backpack` |
| B-22 | AI-P22 | `synfutures` | Perpetual DEX | 链上/SDK 审计，先 public markets、funding、open interest | `derive`, `paradex` |
| B-23 | AI-P23 | `bluefin` | Sui perpetual DEX | Sui 账户/签名边界，public/orderbook/funding first | `aftermath`, `hyperliquid` |
| B-24 | AI-P24 | `gains_network` | gTrade perpetual | RWA/crypto perp，先做 markets/oracle/funding/risk data；私有写后置 | `ostium`, `derive` |
| B-25 | AI-P25 | `kwenta` | Synthetix perpetual | 协议 adapter，public/risk data first，链上交易写默认 Unsupported | `gmx`, `derive` |
| B-26 | AI-P26 | `mux` | Perpetual DEX/aggregator | 区分 aggregator 与原生撮合；只声明可无损映射能力 | `gmx`, `woo` |
| B-27 | AI-P27 | `rabbitx` | Perpetual DEX | REST/WS 和签名审计，perp order lifecycle + WS first | `paradex`, `lighter` |
| B-28 | AI-P28 | `rollbit_futures` | Perpetual/CFD venue | 先确认官方 API 是否允许程序化交易；无官方 API 则 Unsupported | `primexbt`, `bitmex` |
| B-29 | AI-P29 | `bulk_trade` | Solana CLOB perpetual | SDK/API 审计，orderbook/funding/positions first，私有写后置 | `backpack`, `drift` |
| B-30 | AI-P30 | `avantis` | Base/RWA perpetual DEX | G0/G1 审计，先做 markets/oracle/risk data；链上交易写默认后置 | `ostium`, `gains_network` |

## C 组：Perp DEX 长尾补充一交易所任务

这些任务进一步覆盖 Perp DEX 榜单和市场热度中的长尾项目。默认目标不是马上可交易，而
是建立 adapter 边界、API 审计和只读行情能力；只有官方文档、SDK、签名和订单错误码稳
定时才升级到 G2/G3。

| 任务 | AI | Adapter id | 产品目标 | 初始接入建议 | 参考 adapter |
| --- | --- | --- | --- | --- | --- |
| C-01 | AI-D01 | `standx` | Perp DEX / yield margin | BNB Chain/Solana profile 审计，public/orderbook/funding first | `edgex`, `orderly` |
| C-02 | AI-D02 | `io_trader` | Perp DEX | G0/G1，确认官方 API、撮合模型、WS 和订单签名 | `edgex`, `extended` |
| C-03 | AI-D03 | `vest_exchange` | Perp DEX | public markets/orderbook/funding first，私有写离线 request-spec | `aevo`, `rabbitx` |
| C-04 | AI-D04 | `sodex` | Futures DEX | G0/G1，确认链、SDK、市场元数据和清算模型 | `synfutures`, `gmx` |
| C-05 | AI-D05 | `ln_exchange` | Futures DEX | 产品少但 OI 可见；先做只读行情和 funding/risk data | `deribit`, `bitmex` |
| C-06 | AI-D06 | `evedex` | Perp DEX | G0 审计优先，确认是否生产可用和是否有稳定 API | `edgex`, `hyperliquid` |
| C-07 | AI-D07 | `gate_dex` | Gate on-chain perp profile | 先判断与 Gate.io CEX/gateway 的 API 关系，只做独立 DEX profile 差异 | `gateio`, `edgex` |
| C-08 | AI-D08 | `astros` | Perp DEX | G0/G1，public markets/orderbook/positions schema first | `aster`, `edgex` |
| C-09 | AI-D09 | `ondo_perps` | RWA perpetual | RWA 市场元数据、oracle、交易时段和结算边界优先 | `ostium`, `gains_network` |
| C-10 | AI-D10 | `decibel` | Perp DEX | public REST/WS first，确认签名和账户模型后再做私有写 | `lighter`, `extended` |
| C-11 | AI-D11 | `katana_perps` | Perp DEX | G0/G1，确认 Katana 链 profile、market metadata 和 SDK | `gmx`, `synfutures` |
| C-12 | AI-D12 | `flash_trade` | Solana perpetual | Solana SDK/API 审计，oracle/positions/orderbook first | `drift`, `jupiter_perps` |
| C-13 | AI-D13 | `dipcoin` | Futures venue | 高成交量但需强审计；确认官方 API 和是否允许程序化交易 | `blofin`, `phemex` |
| C-14 | AI-D14 | `strike_finance` | Cardano perpetual | 链上账户和 UTXO/合约交互审计；只读行情 first | `gmx`, `derive` |
| C-15 | AI-D15 | `deriw` | Derivatives DEX | G0/G1，确认 API、合约类型、期权/永续边界 | `deribit`, `derive` |
| C-16 | AI-D16 | `sparkdex_perps` | Flare perpetual | 链 profile、oracle、markets/orderbook first，私有写后置 | `gmx`, `synfutures` |
| C-17 | AI-D17 | `navigator` | Perp DEX | G0/G1，确认活跃市场、SDK 和签名模型 | `extended`, `lighter` |
| C-18 | AI-D18 | `spacewhale` | Perp DEX | 低量候选，先做 G0 审计和 Unsupported 边界 | `gmx`, `perpetual_protocol` |
| C-19 | AI-D19 | `demex` | Derivatives DEX | Carbon/Demex API 审计，markets/orderbook/order lifecycle first | `dydx`, `injective_helix` |
| C-20 | AI-D20 | `holdstation_defutures` | DeFutures DEX | G0/G1，确认 account abstraction、chain profile 和订单 API | `edgex`, `gmx` |
| C-21 | AI-D21 | `perpetual_protocol` | 老牌 perpetual AMM | 以历史/只读和显式 Unsupported 为主，确认当前交易面是否仍活跃 | `gmx`, `synthetix_perps` |
| C-22 | AI-D22 | `jojo_exchange` | Perp DEX | official API/SDK 审计，public markets、funding、positions first | `vertex`, `aevo` |
| C-23 | AI-D23 | `blitz` | Perp DEX | G0/G1，确认是否为 Blur/Blast 生态衍生品及 API 可用性 | `aevo`, `rabbitx` |
| C-24 | AI-D24 | `helix_futures` | Injective derivatives | Injective/Helix API profile，markets/orderbook/orders/positions first | `dydx`, `demex` |
| C-25 | AI-D25 | `fwx` | Futures DEX | Avalanche/Base 多链 profile，public + SDK 审计 first | `gmx`, `kiloex` |
| C-26 | AI-D26 | `zkera_finance` | Perp DEX | 多链低量候选，G0/G1 + Unsupported 边界优先 | `gmx`, `synfutures` |
| C-27 | AI-D27 | `monday_trade` | Perp DEX | G0/G1，确认市场、API、WS、账户签名和风控数据 | `extended`, `edgex` |
| C-28 | AI-D28 | `sunperp` | TRON perpetual | TRON 链账户/签名、markets/funding/orderbook first | `gmx`, `woofipro` |
| C-29 | AI-D29 | `enclavex` | Privacy-first derivatives | 隐私撮合/TEE/链上结算审计；先只读和能力边界 | `edgex`, `paradex` |
| C-30 | AI-D30 | `reya` | Reya Network perp | 网络/桥/账户模型、markets/orderbook/funding/positions first | `lighter`, `extended` |
| C-31 | AI-D31 | `polynomial` | Polynomial Chain perps | chain profile + public markets/risk first，私有写后置 | `synthetix_perps`, `perennial` |
| C-32 | AI-D32 | `logx` | LogX network perps | 多链/专链 profile，market metadata、WS、order lifecycle 审计 | `orderly`, `rabbitx` |
| C-33 | AI-D33 | `perennial` | Intent-based perpetual | intents、maker/trader 账户和 fill 语义优先，REST/SDK request-spec | `vertex`, `synfutures` |
| C-34 | AI-D34 | `synthetix_perps` | Synthetix perpetual | 协议 adapter，oracle/risk/funding first，交易写默认后置 | `kwenta`, `polynomial` |
| C-35 | AI-D35 | `levana` | Cosmos perpetual | Cosmos 链账户/签名和 markets/funding/positions first | `dydx`, `helix_futures` |
| C-36 | AI-D36 | `vela` | Arbitrum/Base perpetual | REST/contract API 审计，markets/orderbook/order lifecycle first | `gmx`, `hmx` |
| C-37 | AI-D37 | `hmx` | Arbitrum perpetual | GMX 族流动性/衍生品模型审计，public/risk data first | `gmx`, `vela` |
| C-38 | AI-D38 | `intentx` | Perp DEX aggregator | 聚合器能力边界；只声明 routing 可验证且可无损映射的能力 | `mux`, `orderly` |
| C-39 | AI-D39 | `satori` | Perp DEX | API/SDK 审计，markets/orderbook/funding first | `rabbitx`, `aevo` |
| C-40 | AI-D40 | `aark` | Perp DEX | G0/G1，重点确认清算、vault、oracle 和订单 API | `gmx`, `ostium` |
| C-41 | AI-D41 | `equation` | Perp DEX | Arbitrum perp 协议审计，markets/risk/positions first | `gmx`, `perennial` |
| C-42 | AI-D42 | `apollox_dex` | BNB perpetual DEX | ApolloX DEX 与 CEX/API 族边界，public/private REST first | `phemex`, `kiloex` |
| C-43 | AI-D43 | `d8x` | Polygon perpetual | SDK/contract adapter，markets/funding/risk first | `perpetual_protocol`, `gmx` |
| C-44 | AI-D44 | `mango_markets` | Solana margin/perp | 高风险历史项目，G0 审计和 Unsupported 边界优先 | `drift`, `jupiter_perps` |
| C-45 | AI-D45 | `zeta_markets` | Solana derivatives | options/perp API 审计，markets/orderbook/account signing first | `drift`, `aevo` |
| C-46 | AI-D46 | `bsx` | Base perpetual | Base perp/orderbook API 审计，public + request-spec first | `extended`, `edgex` |
| C-47 | AI-D47 | `derive_chain_perps` | Derive Chain profile | 不重复 `derive` adapter；只补 chain/profile 差异和链上结算审计 | `derive` |
| C-48 | AI-D48 | `cod3x` | Perp DEX candidate | G0 审计任务，确认真实市场、官方 API、合规和安全边界 | `edgex`, `enclavex` |

## 状态登记模板

协调者在任务完成后追加到下表，不要把“正在实现中”误写成“缺失”。

| 任务 | Adapter id | 当前状态 | 依据 | 验证命令 |
| --- | --- | --- | --- | --- |
| 示例 | `example` | 已实现 / 正在实现 / G0 只读审计 / 阻塞 | adapter 目录、fixtures、文档、config、registration 或阻塞原因 | `cargo test -p rustcta-exchange-gateway example --lib --message-format short` |

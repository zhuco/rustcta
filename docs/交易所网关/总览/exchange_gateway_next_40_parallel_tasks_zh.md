# 交易所网关下一批 40 个交易所并行开发任务

状态日期：2026-06-08

本文档衔接 `docs/交易所网关/总览/exchange_gateway_expansion_30_venues_zh.md` 和
`docs/交易所网关/总览/exchange_adapter_toolchain_completion_zh.md`。当前 37 个已有 gateway
adapter 的通用工具链已经收口；下一阶段开始从 79 个 CCXT 缺口里推进下一批约
40 个交易所。执行方式是 20 个 AI 并行，每个 AI 负责 2 个交易所。任务说明不指定
任何模型，只指定工程边界、交付物和验收命令。

## 当前基线

- 已有 adapter 目录：`crates/rustcta-exchange-gateway/src/adapters/` 下 37 个。
- 已完成工具链：endpoint mapping schema 和 validator、request-spec/signing
  fixture、capability v2、rate-limit/page 模型、adapter-local warning 收口。
- 本批不重复开发已有目录，不修改已经完成的 37 个 adapter 的交易语义，除非某个新
  adapter 需要复用公共工具且由协调者单独批准。
- 本批优先 P0/P1 缺失交易所：先永续/衍生品，再主流现货和区域现货。

## 当前实现状态

截至 2026-06-08，本文件列出的 40 个交易所已经完成 gateway adapter 批量收口：

- 40 个交易所均已有 `rustcta-exchange-gateway` adapter 目录、fixtures、adapter 文档、
  endpoint mapping、named registration 和 disabled config example。
- `apps/gateway/src/config.rs` 已接入本批 adapter 的 REST base URL、私有凭据读取和
  redacted config 输出。
- 本批收口验证命令：
  `python3 scripts/validate_exchange_endpoint_mapping.py`、`cargo fmt --check --package rustcta-exchange-api`、
  `cargo fmt --check --package rustcta-exchange-gateway`、`cargo check -p rustcta-exchange-gateway --lib --message-format short`、
  `cargo clippy -p rustcta-exchange-gateway --all-targets --all-features --message-format short -- -D warnings`。

| 任务 | 交易所 | 当前状态 | 依据 |
| --- | --- | --- | --- |
| AI-01 | `bybit`, `binancecoinm` | 已完成并批量收口 | Adapter、fixtures、endpoint mapping、文档、disabled config example、named/app config registration 已落地；私有写接口按 request-spec/signing fixture 离线验证边界收口 |
| AI-02 | `htx`, `huobi` | 已完成并批量收口 | HTX/Huobi profile、fixtures、endpoint mapping、文档、disabled config example、named/app config registration 已落地；Huobi legacy profile 保持边界说明 |
| AI-03 | `bitmart`, `bitfinex` | 已完成并批量收口 | Adapter、fixtures、endpoint mapping、文档、disabled config example、named/app config registration 已落地；BitMart 旧引用已迁入 gateway 标准 adapter |
| AI-04 | `deribit`, `delta` | 已完成并批量收口 | Options/futures/perp adapter、fixtures、endpoint mapping、文档、disabled config example 和 named registration 已落地 |
| AI-05 | `hyperliquid`, `dydx` | 已完成并批量收口 | DEX/perp adapter、fixtures、endpoint mapping、文档、disabled config example、named/app config registration 已落地；链上账户和签名边界保持文档化 |
| AI-06 | `krakenfutures`, `kucoinfutures` | 已完成并批量收口 | Futures/perp adapter、fixtures、endpoint mapping、文档、disabled config example、named/app config registration 已落地 |
| AI-07 | `bullish`, `apex` | 已完成并批量收口 | Adapter、fixtures、endpoint mapping、文档、disabled config example 和 named registration 已落地；私有写按官方边界保持 request-spec/Unsupported |
| AI-08 | `paradex`, `derive` | 已完成并批量收口 | DEX/perp/options adapter、fixtures、endpoint mapping、文档、disabled config example、named/app config registration 已落地 |
| AI-09 | `grvt`, `lighter` | 已完成并批量收口 | Perp/options adapter、fixtures、endpoint mapping、文档、disabled config example 和 named registration 已落地 |
| AI-10 | `oxfun`, `pacifica` | 已完成并批量收口 | Perp/options/DEX adapter、fixtures、endpoint mapping、文档、disabled config example 和 named registration 已落地 |
| AI-11 | `aster`, `bitstamp` | 已完成并批量收口 | Aster perp 与 Bitstamp spot adapter、fixtures、endpoint mapping、文档、disabled config example、named/app config registration 已落地 |
| AI-12 | `bithumb`, `upbit` | 已完成并批量收口 | 韩国 spot adapter、fixtures、endpoint mapping、文档、disabled config example、named/app config registration 已落地；KRW/BTC/USDT 市场边界已文档化 |
| AI-13 | `bitflyer`, `bitbank` | 已完成并批量收口 | 日本 spot/FX adapter、fixtures、endpoint mapping、文档、disabled config example 和 named registration 已落地 |
| AI-14 | `bitvavo`, `gemini` | 已完成并批量收口 | 欧洲/美国 spot adapter、fixtures、endpoint mapping、文档、disabled config example、named/app config registration 已落地 |
| AI-15 | `coinbaseexchange`, `coincheck` | 已完成并批量收口 | Coinbase Exchange 与 Coincheck spot adapter、fixtures、endpoint mapping、文档、disabled config example 和 named registration 已落地 |
| AI-16 | `bitso`, `mercado` | 已完成并批量收口 | 拉美 spot adapter、fixtures、endpoint mapping、文档、disabled config example 和 named registration 已落地；法币资金能力保持 Unsupported/只读边界 |
| AI-17 | `btcmarkets`, `independentreserve` | 已完成并批量收口 | 澳洲/新加坡 spot adapter、fixtures、endpoint mapping、文档、disabled config example、named/app config registration 已落地；AUD/SGD/USD 法币资金、税务和报表边界保持 Unsupported |
| AI-18 | `btcturk`, `luno` | 已完成并批量收口 | 土耳其/多法币 spot adapter、fixtures、endpoint mapping、文档、disabled config example、named/app config registration 已落地 |
| AI-19 | `coinone`, `coinsph` | 已完成并批量收口 | 韩国/菲律宾 spot adapter、fixtures、endpoint mapping、文档、disabled config example、named/app config registration 已落地；KRW/PHP 市场和支付/钱包边界已文档化 |
| AI-20 | `coinspot`, `indodax` | 已完成并批量收口 | 澳洲/印尼 spot adapter、fixtures、endpoint mapping、文档、disabled config example 和 named registration 已落地；AUD/IDR 法币资金边界保持文档化 |

## 总目标

为下面 40 个交易所新增或迁移 `rustcta-exchange-gateway` 标准 adapter，并让每个
adapter 至少达到“离线可验”的状态：

- 有独立 adapter 目录、配置、签名、transport/parser、endpoint mapping、fixtures
  和 adapter 文档。
- 永续或 futures 交易所优先实现 `MarketType::Perpetual` / `MarketType::Futures`；
  只有现货能力时实现 `MarketType::Spot`。
- 官方没有稳定接口的能力必须显式 `Unsupported`，不能用网页接口、非公开接口或
  本地组合逻辑伪装成交易所原生能力。
- 私有写接口必须有 request-spec 和签名向量，证明 method/path/query/body/header
  构造正确。
- WebSocket 至少交付订阅/退订 payload、鉴权 payload、心跳策略和 parser fixture；
  若没有稳定私有 WS，必须写 REST reconciliation fallback。

## 不做的事

- 不运行 `cargo build`、`cargo build --release`、app/web build 或任何发布构建。
- 不做真实下单、真实撤单、提现、转账、划转、API key 写入或生产私有流长期运行。
- 不把 CCXT 源码当作最终权威；CCXT 只用于能力发现，最终以官方 API 文档和本地
  fixture 为准。
- 不改策略层、执行层、控制面板，除非任务明确要求迁移旧交换所逻辑到
  `crates/rustcta-exchange-gateway/src/adapters/` 且改动范围已列出。
- 不一次性扩展共享 trait。确实需要新增共享模型或方法时，先提交协调说明，由单独
  公共任务合并后其他 AI 再跟进。
- 不在 prompt、任务名或交付要求里指定模型。

## 每个 AI 的全局规则

1. 先运行 `git status --short`，记录无关 dirty files，不回滚其他人的改动。
2. 阅读本文件、`docs/交易所网关/总览/exchange_adapter_toolchain_completion_zh.md`、
   `docs/交易所网关/总览/exchange_api_completion_matrix.md` 和一个相近 adapter 文档。
3. 每个 AI 同时负责两个交易所，但不要改其他 AI 的 adapter 目录、fixtures 或文档。
4. 共享文件只做追加式注册：`adapters/mod.rs`、`crates/rustcta-exchange-gateway/src/lib.rs`、
   `apps/gateway/src/config.rs`、必要配置示例。冲突由协调者按 adapter id 字母序合并。
5. 新增文件命名全部使用 snake_case adapter id，例如 `krakenfutures`、
   `coinbaseexchange`。
6. 所有凭据 fixture 必须脱敏，不提交真实 key、secret、passphrase、token、账户 id、
   地址、手机号、邮箱或订单 id。
7. 私有 REST 写接口默认只做离线 request-spec；live read-only 和 live-dry-run 只能
   后续由专门验证任务打开。
8. 结束时提交每个交易所的能力边界、验收命令和未跑命令说明。

## 标准交付物

每个交易所至少交付以下文件或等价结构：

| 类别 | 必须交付 |
| --- | --- |
| Adapter 代码 | `crates/rustcta-exchange-gateway/src/adapters/<id>/config.rs`、`signing.rs`、`transport.rs`、`public.rs`、`private.rs`、`parser.rs`、`mod.rs`，按需要增加 `streams.rs`、`private_parser.rs`、`test_support.rs` |
| Endpoint mapping | `crates/rustcta-exchange-gateway/src/adapters/<id>/endpoint_mapping.yaml`，通过 shared schema 校验 |
| Fixtures | `tests/fixtures/exchanges/<id>/` 下响应样本、`request_specs/*.json`、`signing_vectors/*.json`、WS parser 样本 |
| 文档 | `docs/<id>_adapter.md`，包含产品线、base URL、签名、限速、endpoint mapping、capability、Unsupported 边界、验证命令 |
| 配置 | `config/<id>_gateway_example.yml`，默认 disabled，不含真实凭据 |
| 注册 | gateway adapter module/export/named registration；如果接入 app env，补 redacted config 测试 |

## 能力分层

按 G0 到 G6 推进。每个交易所可以因为官方能力不足停在较低层，但必须写清楚原因。

| 层级 | 目标 | 必须产出 |
| --- | --- | --- |
| G0 API 审计 | 确认官方 REST/WS 文档、产品线、base URL、testnet、权限、签名、限速、地区限制 | adapter 文档的官方资料表、endpoint mapping 初稿、Unsupported 边界 |
| G1 Public REST | 标准 symbol rules/order book，永续额外 funding/mark/open interest 能力审计 | parser fixtures、public request tests、`get_symbol_rules`、`get_order_book` |
| G2 Private Read REST | balances、positions、fees、open orders、query order、fills | request-spec、签名向量、只读 parser tests |
| G3 Private Write REST | place/cancel/cancel-all/amend/order-list 能力判断 | 写接口 request-spec；没有官方接口则 Unsupported |
| G4 Batch | native/composed batch place/cancel、原子性、最大条数、部分失败 | endpoint mapping `native_batch`/`atomicity`、batch response tests |
| G5 WebSocket | public/private subscribe/auth/heartbeat/parser/resync | WS payload tests、heartbeat policy、REST reconciliation fallback |
| G6 Advanced Perp | leverage、margin mode、position mode、dead-man/cancel-all-after、risk tiers | 只在官方原生支持且语义可无损映射时实现；否则 Unsupported |

## 错误检查和测试命令

开发过程中不要运行构建命令。允许的验证集中在错误检查、格式检查和测试：

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/<id>/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway <id> --lib --message-format short
```

如果修改了 `apps/gateway/src/config.rs`，再补对应的 app 配置测试过滤器：

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

全仓 `cargo test --all-features`、`cargo clippy --all-targets --all-features` 只由协调者在
批量合并后运行；单个 AI 不在开发过程中运行。

## 本批 20 个并行任务

| 任务 | AI | 交易所 A | 交易所 B | 产品目标 | 主要参考 adapter |
| --- | --- | --- | --- | --- | --- |
| 1 | AI-01 | `bybit` | `binancecoinm` | Spot + USDT/USDC perp；Binance COIN-M inverse/futures profile | `binance`、`bitget`、`phemex` |
| 2 | AI-02 | `htx` | `huobi` | Spot + USDT perp，优先共享 HTX/Huobi API profile | `gateio`、`mexc`、`cointr` |
| 3 | AI-03 | `bitmart` | `bitfinex` | Spot + perp/derivatives；迁移旧 BitMart 经验到 gateway | `bitunix`、`kraken`、`whitebit` |
| 4 | AI-04 | `deribit` | `delta` | Options + futures/perp；期权元数据先 adapter-specific | `kraken`、`coinbase`、`woo` |
| 5 | AI-05 | `hyperliquid` | `dydx` | Perp/DEX；链上账户和签名边界必须审计 | `backpack`、`orangex`、`blofin` |
| 6 | AI-06 | `krakenfutures` | `kucoinfutures` | Futures/perp 产品线拆分 adapter | `kraken`、`kucoin`、`poloniex` |
| 7 | AI-07 | `bullish` | `apex` | 机构/DEX perp，public/private REST + WS first | `coinbase`、`backpack`、`cryptocom` |
| 8 | AI-08 | `paradex` | `derive` | Perp/options/DEX；签名和账户模型优先 | `backpack`、`orangex`、`coinbase` |
| 9 | AI-09 | `grvt` | `lighter` | Perp/options/low-latency order book | `backpack`、`blofin`、`woo` |
| 10 | AI-10 | `oxfun` | `pacifica` | Perp/options/DEX；testnet 和 WS resync 优先 | `blofin`、`phemex`、`orangex` |
| 11 | AI-11 | `aster` | `bitstamp` | Aster perp first；Bitstamp spot | `blofin`、`kraken`、`coinbase` |
| 12 | AI-12 | `bithumb` | `upbit` | 韩国现货，KRW 市场和严格限速 | `coinex`、`kucoin`、`gateio` |
| 13 | AI-13 | `bitflyer` | `bitbank` | 日本现货/FX，product code 和 JPY 市场 | `kraken`、`kucoin`、`coinex` |
| 14 | AI-14 | `bitvavo` | `gemini` | 欧洲/美国主流现货 | `coinbase`、`kraken`、`whitebit` |
| 15 | AI-15 | `coinbaseexchange` | `coincheck` | Coinbase Exchange profile + 日本现货 | `coinbase`、`kraken`、`kucoin` |
| 16 | AI-16 | `bitso` | `mercado` | 拉美现货，法币账本和区域限速 | `coinex`、`cryptocom`、`gateio` |
| 17 | AI-17 | `btcmarkets` | `independentreserve` | 澳洲/新加坡现货，AUD/SGD 市场 | `kraken`、`coinbase`、`bitget` |
| 18 | AI-18 | `btcturk` | `luno` | 土耳其/多法币现货 | `coinex`、`kucoin`、`gateio` |
| 19 | AI-19 | `coinone` | `coinsph` | 韩国/菲律宾现货，KRW/PHP 市场 | `bithumb` 任务输出、`coinex`、`kucoin` |
| 20 | AI-20 | `coinspot` | `indodax` | 澳洲/印尼现货，AUD/IDR 市场 | `btcmarkets` 任务输出、`coinex`、`gateio` |

## 任务 1：AI-01，Bybit + Binance COIN-M

目标：

- `bybit`：新增统一 gateway adapter，优先 Spot、USDT/USDC linear perpetual、公开/私有 WS、positions、funding、open interest、leverage/margin/position mode。
- `binancecoinm`：不要复制 Binance Spot；作为独立 COIN-M futures/inverse adapter 或 profile，复用 Binance 签名和请求工具，重点覆盖 inverse futures/perpetual symbol rules、book、positions、orders、fills、funding、open interest。

边界：

- 旧 private-perp 实现经验只能作为迁移参考；新增 Bybit 私有永续能力必须进入
  `crates/rustcta-exchange-gateway/src/adapters/bybit/`。
- `binancecoinm` 的 `MarketType` 可使用 `Futures`/`Perpetual`，文档里必须标注 coin-margined/inverse settle asset 和 contract size。

分配 prompt：

```text
You are AI-01. Complete Task 1 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `bybit` and `binancecoinm`. Do not assume or specify a model. Read the global rules first. Implement gateway adapters with endpoint mapping, request-spec/signing fixtures, docs, disabled config examples, and focused tests. Do not run cargo build; run only the allowed check and test commands.
```

## 任务 2：AI-02，HTX + Huobi

目标：

- `htx`：优先 Spot + USDT perpetual，覆盖账户、订单、仓位、资金费、public/private WS。
- `huobi`：优先作为 HTX legacy alias/profile；如果官方 endpoint 仍有差异，再拆独立 adapter。

边界：

- 不做两个完全重复实现。共享 signing/parser/transport helper 可以放在其中一个 adapter 子目录内或独立私有模块，但不要改公共 trait。
- 明确 `htx` 与 `huobi` 的 CCXT id、base URL、签名、产品线差异。

分配 prompt：

```text
You are AI-02. Complete Task 2 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `htx` and `huobi`. Do not assume or specify a model. Treat Huobi as an alias/profile unless official API differences require a separate adapter. Do not run cargo build; run only endpoint mapping validation, cargo fmt --check, cargo check, and focused tests.
```

## 任务 3：AI-03，BitMart + Bitfinex

目标：

- `bitmart`：把旧路径能力迁移到 gateway，优先 Spot + perpetual，补离线 request-spec 和 WS parser。
- `bitfinex`：覆盖 Spot/margin/perp 或 derivatives 能力边界，优先 public/private REST、order lifecycle、WS auth。

边界：

- 旧 BitMart adapter 是参考，不是最终验收口径；gateway adapter 必须有 endpoint mapping 和 fixtures。
- Bitfinex margin/derivatives 语义复杂，不把 margin loan、funding book、withdraw/transfer 接入交易 runtime。

分配 prompt：

```text
You are AI-03. Complete Task 3 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `bitmart` and `bitfinex`. Do not assume or specify a model. Migrate useful old BitMart knowledge into gateway artifacts, and keep Bitfinex margin/funding/transfer features out of runtime unless explicitly modeled as Unsupported. Do not run cargo build; run only the allowed checks and focused tests.
```

## 任务 4：AI-04，Deribit + Delta

目标：

- `deribit`：优先 options + perp/futures public/private REST、private WS、positions、settlement/greeks adapter-specific parser。
- `delta`：优先 options/perp REST、orders、positions、funding、greeks/option chain adapter-specific fixtures。

边界：

- 当前统一 trait 没有完整 option 模型；不要硬塞进 Spot/Perp 标准字段。标准 gateway 先覆盖共同交易面，option chain/greeks 放 adapter-specific helper 和文档。
- 期权结算、组合保证金、portfolio margin 只读审计，默认不做资金操作。

分配 prompt：

```text
You are AI-04. Complete Task 4 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `deribit` and `delta`. Do not assume or specify a model. Implement the common gateway surface first and keep options-specific chain/greeks/settlement data as adapter-specific helpers with fixtures. Do not run cargo build; run only the allowed checks and focused tests.
```

## 任务 5：AI-05，Hyperliquid + dYdX

目标：

- `hyperliquid`：迁移旧 Hyperliquid USDC perp 能力到 gateway，覆盖 public book、private orders、fills、positions、signing、WS。
- `dydx`：优先 perp/DEX public/private REST、chain/account model、WS order book 和 user stream。

边界：

- 链上/DEX 签名、钱包、subaccount、nonce 和域分隔必须写清楚，不把私钥或助记词引入配置示例。
- 只有官方交易 API 和 testnet 能力明确时才实现私有写 request-spec。

分配 prompt：

```text
You are AI-05. Complete Task 5 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `hyperliquid` and `dydx`. Do not assume or specify a model. Migrate Hyperliquid into gateway without changing old strategy runtime, and document DEX wallet/subaccount/signing boundaries. Do not run cargo build; run only the allowed checks and focused tests.
```

## 任务 6：AI-06，Kraken Futures + KuCoin Futures

目标：

- `krakenfutures`：从现有 `kraken` adapter 拆 futures/perp profile 或独立 adapter，覆盖 futures REST/WS。
- `kucoinfutures`：从现有 `kucoin` spot adapter 拆 futures/perp adapter，覆盖 contracts、positions、orders、fills、funding、private WS。

边界：

- 不破坏现有 `kraken` 和 `kucoin` Spot 行为。共享代码只能做向后兼容提取。
- adapter id 与注册名必须分别是 `krakenfutures` 和 `kucoinfutures`，不要把产品线藏在原 spot adapter 文档里。

分配 prompt：

```text
You are AI-06. Complete Task 6 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `krakenfutures` and `kucoinfutures`. Do not assume or specify a model. Reuse existing Kraken/KuCoin knowledge without changing their Spot semantics. Do not run cargo build; run only the allowed checks and focused tests.
```

## 任务 7：AI-07，Bullish + Apex

目标：

- `bullish`：机构现货/衍生品，优先 public/private REST、FIX/WS 可行性审计、订单/余额/仓位。
- `apex`：Perp/DEX，优先 contracts、book、orders、positions、funding、WS auth。

边界：

- Bullish 的机构权限、API key scope、portfolio/account 模型必须先文档化。
- Apex 链上签名或账户模型不清楚时只做 G0/G1，私有写保持 Unsupported。

分配 prompt：

```text
You are AI-07. Complete Task 7 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `bullish` and `apex`. Do not assume or specify a model. Prioritize official API audit, request specs, and capability boundaries before broad feature work. Do not run cargo build; run only the allowed checks and focused tests.
```

## 任务 8：AI-08，Paradex + Derive

目标：

- `paradex`：perp/options/DEX，重点是 Stark/key signing、markets、orders、positions、private WS。
- `derive`：options/perp/DeFi，重点是 signing、session/account model、option/perp market metadata。

边界：

- 不提交真实钱包私钥、session token 或链上账户 fixture。
- Options 能力先以 adapter-specific 文档和 fixtures 表达，不改共享 trait。

分配 prompt：

```text
You are AI-08. Complete Task 8 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `paradex` and `derive`. Do not assume or specify a model. Keep DEX signing and options metadata explicit and secret-free. Do not run cargo build; run only the allowed checks and focused tests.
```

## 任务 9：AI-09，GRVT + Lighter

目标：

- `grvt`：perp/options，优先 API maturity、testnet、orders、positions、private stream。
- `lighter`：perp/low-latency order book，优先 depth/resync/checksum、orders、fills、positions。

边界：

- 低延迟 WS 不等于 production runtime；本任务只交付 request/session spec 和 parser fixtures。
- 如果官方文档仍 beta 或权限封闭，保守实现 G0/G1 并显式 Unsupported。

分配 prompt：

```text
You are AI-09. Complete Task 9 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `grvt` and `lighter`. Do not assume or specify a model. Favor conservative API maturity gates and robust offline fixtures. Do not run cargo build; run only the allowed checks and focused tests.
```

## 任务 10：AI-10，OX.FUN + Pacifica

目标：

- `oxfun`：perp/options，优先 public/private REST、positions、funding、WS parser。
- `pacifica`：perp/DEX，优先 testnet、signing、orders、positions、WS resync。

边界：

- 若产品仍偏封闭 beta，先完成文档审计、public REST 和 Unsupported 边界。
- 不用本地策略逻辑模拟交易所不存在的 advanced order 或 dead-man switch。

分配 prompt：

```text
You are AI-10. Complete Task 10 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `oxfun` and `pacifica`. Do not assume or specify a model. Verify official endpoints and testnet availability before private write support. Do not run cargo build; run only the allowed checks and focused tests.
```

交付状态：

- `oxfun`：已新增保守 audit-shell gateway adapter。覆盖 perp/options 产品边界、官方 live/test WebSocket URL、HMAC REST/WS login 签名向量、WebSocket subscribe/unsubscribe/login/text-ping heartbeat payload、market/depth snapshot+diff/private order ack parser fixtures、endpoint mapping、adapter 文档和 disabled config example。公开/私有 REST、positions、funding、private WS runtime 和交易写入均保持显式 `Unsupported` 或 request-spec-only，legacy trading flags 不打开，避免在未完成 resync/live dry-run 前打开交易能力。
- `pacifica`：已新增 perpetual gateway adapter。覆盖官方 public REST `GET /api/v1/info`、`GET /api/v1/book` parser/transport，testnet/mainnet base URL 配置，Ed25519 sorted compact JSON signing vector（支持 base64/hex/base58 本地测试私钥）、positions/open-order/order/cancel/cancel-all request-spec/parser fixtures，WebSocket subscribe/heartbeat/account-position/resync policy fixtures、endpoint mapping、adapter 文档和 disabled config example。public/private WS runtime、私有写入、batch 和 advanced order 仍保持 request-spec-only/`Unsupported`。
- 验证：`python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/oxfun/endpoint_mapping.yaml crates/rustcta-exchange-gateway/src/adapters/pacifica/endpoint_mapping.yaml` 已通过；`cargo fmt --check --package rustcta-exchange-gateway` 已通过；targeted `rustfmt --edition 2021 --check crates/rustcta-exchange-gateway/src/adapters/oxfun/*.rs crates/rustcta-exchange-gateway/src/adapters/pacifica/*.rs` 已通过；任务 10 fixtures 已通过 `jq empty` JSON 校验。隔离任务 10 注册后的 shadow `cargo check -p rustcta-exchange-gateway --lib --message-format short` 已通过。当前混合工作树的 focused `cargo check/test` 仍被其他并行任务未完成 adapter 阻塞（例如 `independentreserve`、`bitmart`、`bitstamp`、`coinone`、`coinsph`、`bitflyer`、`btcturk`、`luno`、`upbit` 等非任务 10 目录编译错误）；shadow focused tests 后续因磁盘空间不足中断，需全仓冲突和磁盘空间收口后复跑。

## 任务 11：AI-11，Aster + Bitstamp

目标：

- `aster`：perp/DEX 优先，覆盖 public contracts/book/funding、private orders/positions、WS。
- `bitstamp`：主流 Spot，覆盖 symbol rules、book、balances、fees、orders、fills、public/private WS。

边界：

- Aster 如果无稳定官方交易 API，只做 G0/G1。
- Bitstamp 不需要实现合约能力，除非官方有稳定 derivatives API 且可无损映射。

分配 prompt：

```text
You are AI-11. Complete Task 11 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `aster` and `bitstamp`. Do not assume or specify a model. Prioritize Aster perpetual support, then Bitstamp spot completeness. Do not run cargo build; run only the allowed checks and focused tests.
```

交付状态：

- `aster`：已新增 Perpetual-only gateway adapter。覆盖官方 Futures V3 public REST `GET /fapi/v3/exchangeInfo`、`GET /fapi/v3/depth`、`GET /fapi/v3/premiumIndex`、`GET /fapi/v3/fundingRate`，私有 V3 EIP-712 `AsterSignTransaction` 签名，private order/cancel/cancel-all/query/open-orders、balances、positions、`GET /fapi/v3/commissionRate` fees、fills readback，以及 public WebSocket 订阅和 private listen-key session spec、endpoint mapping、签名/WS/parser focused tests。Spot、COIN-M、quote-sized market order、order-list、amend 和 transfers 保持显式 `Unsupported`，避免把未确认稳定 API 暴露成交易能力。
- `bitstamp`：已新增 Spot-only gateway adapter。覆盖 `GET /api/v2/markets/` symbol rules、`GET /api/v2/order_book/{market}/` book，私有 HMAC-SHA256 v2 balances、fees、limit/market/quote-buy orders、cancel/cancel-all/replace/query/open-orders、fills，以及 public order book/trades WS、private websockets token + order/account channel spec、endpoint mapping、parser/signing/WS focused tests。Derivatives、positions、sell quote-market、order-list 和 batch orders 保持显式 `Unsupported`。
- Gateway 注册和 app 配置：`AdapterBackedGateway` 已注册 `aster`/`bitstamp` public/private adapter，library 已导出 `AsterGatewayConfig`/`BitstampGatewayConfig`；`apps/gateway` 已支持 `RUSTCTA_ASTER_*`、`RUSTCTA_BITSTAMP_*` env wiring，并新增 Task 11 capability wiring test。
- 验证：`python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/aster/endpoint_mapping.yaml crates/rustcta-exchange-gateway/src/adapters/bitstamp/endpoint_mapping.yaml` 已通过；`rustfmt --edition 2021 --check crates/rustcta-exchange-gateway/src/adapters/aster/*.rs crates/rustcta-exchange-gateway/src/adapters/bitstamp/*.rs apps/gateway/src/config.rs` 已通过；`jq empty tests/fixtures/exchanges/aster/*.json tests/fixtures/exchanges/bitstamp/*.json` 已通过；静态审计确认 Aster/Bitstamp 目录无旧 Binance/DAPI/HMAC 误用模式（如 `fapi/v1`、`dapi`、`X-MBX`、`myTrades`、`MarketType::Futures`）；focused Cargo tests 已通过：`aster_v3_signing`、`aster_ws_payload`、`aster_position_parser`、`bitstamp_parser`、`bitstamp_signing`、`bitstamp_private_parsers`、`bitstamp_private_ws_payload`（均为 `cargo test -p rustcta-exchange-gateway <filter> --lib --message-format short`），以及 `cargo test -p rustcta-gateway config_should_wire_task11_aster_and_bitstamp_private_adapters --message-format short`。

## 任务 12：AI-12，Bithumb + Upbit

目标：

- `bithumb`：韩国 Spot，KRW 市场、限速、私有 REST、public/private WS。
- `upbit`：韩国 Spot，KRW/BTC/USDT markets、JWT 签名、严格限速、orders/fills。

边界：

- 不做地区规避；如果 API 权限或国家限制影响私有接口，只写清楚 read-only/private Unsupported。
- Upbit JWT request-spec 必须覆盖 query hash。

分配 prompt：

```text
You are AI-12. Complete Task 12 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `bithumb` and `upbit`. Do not assume or specify a model. Focus on Korean spot market correctness, rate limits, signing vectors, and safe unsupported boundaries. Do not run cargo build; run only the allowed checks and focused tests.
```

完成状态：

- `bithumb`：已新增 `crates/rustcta-exchange-gateway/src/adapters/bithumb/`，覆盖韩国 Spot/KRW markets、public REST、私有 REST request-spec/JWT signing/query hash、public/private WS payload、orders/fills/balances parser、限速和权限/地区边界文档。
- `upbit`：已新增 `crates/rustcta-exchange-gateway/src/adapters/upbit/`，覆盖韩国 Spot KRW/BTC/USDT markets、JWT signing/query hash、public REST、私有 orders/fills/balances request-spec、public/private WS payload、严格限速和权限/地区边界文档。
- 共享注册：已追加 `adapters/mod.rs`、`src/lib.rs` 和 `apps/gateway/src/config.rs` 的 named registration、config builder、REST base URL env 和 credential env wiring。

验收命令：

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bithumb/endpoint_mapping.yaml
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/upbit/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
TMPDIR=$PWD/target/tmp cargo check -p rustcta-exchange-gateway --lib --message-format short
TMPDIR=$PWD/target/tmp cargo test -p rustcta-exchange-gateway bithumb --lib --message-format short
TMPDIR=$PWD/target/tmp cargo test -p rustcta-exchange-gateway upbit --lib --message-format short
TMPDIR=$PWD/target/tmp cargo test -p rustcta-gateway bithumb --message-format short
TMPDIR=$PWD/target/tmp cargo test -p rustcta-gateway upbit --message-format short
```

本地验证记录：

- 已通过：`python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bithumb/endpoint_mapping.yaml`
- 已通过：`python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/upbit/endpoint_mapping.yaml`
- 已通过：`cargo fmt --check --package rustcta-exchange-gateway`
- 已通过：`TMPDIR=$PWD/target/tmp cargo check -p rustcta-exchange-gateway --lib --message-format short`，仅剩其它 adapter 的既有 warning。
- 已通过：`TMPDIR=$PWD/target/tmp cargo test -p rustcta-exchange-gateway bithumb --lib --message-format short`，7 passed。
- 已通过：`TMPDIR=$PWD/target/tmp cargo test -p rustcta-exchange-gateway upbit --lib --message-format short`，5 passed。
- 已通过：`TMPDIR=$PWD/target/tmp cargo test -p rustcta-gateway bithumb --message-format short`，0 tests matched，但 app config 编译通过。
- 已通过：`TMPDIR=$PWD/target/tmp cargo test -p rustcta-gateway upbit --message-format short`，0 tests matched，但 app config 编译通过。

## 任务 13：AI-13，bitFlyer + bitbank

目标：

- `bitflyer`：日本 Spot/FX，product code、JPY 市场、private orders/fills、WS。
- `bitbank`：日本 Spot，JPY 市场、public/private REST、orders/fills、WS。

边界：

- bitFlyer Lightning FX/CFD 与标准 Futures/Perpetual 的差异必须在文档里说明。
- 不把法币充值提现、银行转账、ledger 写接口接入 gateway runtime。

分配 prompt：

```text
You are AI-13. Complete Task 13 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `bitflyer` and `bitbank`. Do not assume or specify a model. Keep Japanese market/product-code semantics explicit and avoid fiat funding operations. Do not run cargo build; run only the allowed checks and focused tests.
```

完成状态：

- `bitflyer`：已新增 `crates/rustcta-exchange-gateway/src/adapters/bitflyer/`，覆盖 Spot 与 Lightning FX/CFD product_code 的 public REST symbol rules/order book、public WS subscribe payload、私有 REST request-spec/signing 向量、Unsupported 边界、disabled config 和 adapter 文档。
- `bitbank`：已新增 `crates/rustcta-exchange-gateway/src/adapters/bitbank/`，覆盖日本 Spot/Jpy pair 的 public REST pair/depth parser、public socket.io room payload、私有 REST request-spec/signing 向量、Unsupported 边界、disabled config 和 adapter 文档。
- 共享注册：已追加 `adapters/mod.rs`、`src/lib.rs` 和 `apps/gateway/src/config.rs` 的 named registration、config builder 与 redacted config 测试。

验收命令：

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitflyer/endpoint_mapping.yaml
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitbank/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bitflyer --lib --message-format short
cargo test -p rustcta-exchange-gateway bitbank --lib --message-format short
cargo test -p rustcta-gateway bitflyer --message-format short
cargo test -p rustcta-gateway bitbank --message-format short
```

本地验证记录：

- 已通过：`python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitflyer/endpoint_mapping.yaml`
- 已通过：`python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitbank/endpoint_mapping.yaml`
- 已通过：`cargo fmt --check --package rustcta-exchange-gateway`
- 已通过：`cargo check -p rustcta-exchange-gateway --lib --message-format short`，仅剩其它并行任务留下的既有 warning。
- 已通过：`cargo test -p rustcta-exchange-gateway bitflyer --lib --message-format short`，6 passed。
- 已通过：`cargo test -p rustcta-exchange-gateway bitbank --lib --message-format short`，6 passed。
- 已通过：`cargo test -p rustcta-gateway bitflyer --message-format short`，app config redaction 测试通过。
- 已通过：`cargo test -p rustcta-gateway bitbank --message-format short`，app config redaction 测试通过。

## 任务 14：AI-14，Bitvavo + Gemini

目标：

- `bitvavo`：欧洲 Spot，EUR 市场、REST/WS、account/order/fills。
- `gemini`：美国 Spot，REST/WS、nonce/signing、order events、balances/fills。

边界：

- Gemini auction/block/travel-rule/withdrawal features不进入交易 runtime。
- Bitvavo 法币支付和资金操作只做文档 Unsupported。

分配 prompt：

```text
You are AI-14. Complete Task 14 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `bitvavo` and `gemini`. Do not assume or specify a model. Implement spot trading gateway surfaces and keep fiat/funding operations out of runtime. Do not run cargo build; run only the allowed checks and focused tests.
```

## 任务 15：AI-15，Coinbase Exchange + Coincheck

目标：

- `coinbaseexchange`：作为 Coinbase Exchange profile 或独立 adapter，区别于现有 `coinbase` Advanced Trade/INTX。
- `coincheck`：日本 Spot，public/private REST、orders、balances、fills。

边界：

- 不破坏现有 `coinbase` adapter；若能 profile 复用，文档必须说明 `coinbase`、`coinbaseexchange`、`coinbaseadvanced` 的边界。
- Coincheck lending、deposit/withdraw 不接入 runtime。

分配 prompt：

```text
You are AI-15. Complete Task 15 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `coinbaseexchange` and `coincheck`. Do not assume or specify a model. Keep Coinbase Exchange separate from the existing Coinbase Advanced Trade/INTX adapter unless a safe profile reuse is documented. Do not run cargo build; run only the allowed checks and focused tests.
```

## 任务 16：AI-16，Bitso + Mercado Bitcoin

目标：

- `bitso`：拉美 Spot，MXN/ARS/BRL 等法币市场、REST/WS、orders/fills。
- `mercado`：巴西/拉美 Spot，BRL 市场、REST/WS、orders/fills。

边界：

- 法币 ledger 可只读审计，不做提现、银行支付或转账。
- 市场 symbol normalization 要覆盖 `BTC/MXN`、`BTC/BRL` 等法币 quote。

分配 prompt：

```text
You are AI-16. Complete Task 16 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `bitso` and `mercado`. Do not assume or specify a model. Focus on Latin America spot markets, fiat quote symbol normalization, and secret-free fixtures. Do not run cargo build; run only the allowed checks and focused tests.
```

## 任务 17：AI-17，BTC Markets + Independent Reserve

目标：

- `btcmarkets`：澳洲 Spot，AUD 市场、REST/WS、private orders/fills。
- `independentreserve`：澳洲/新加坡 Spot，AUD/SGD/USD 市场、REST/WS、orders/fills。

边界：

- 法币账户、税务报表、充值提现只做 Unsupported 或只读文档。
- 注意请求签名 timestamp/nonce 和 endpoint version 差异。

分配 prompt：

```text
You are AI-17. Complete Task 17 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `btcmarkets` and `independentreserve`. Do not assume or specify a model. Implement spot gateway surfaces and document fiat/accounting boundaries. Do not run cargo build; run only the allowed checks and focused tests.
```

## 任务 18：AI-18，BTCTurk + Luno

目标：

- `btcturk`：土耳其 Spot，TRY 市场、REST/WS、orders/fills。
- `luno`：多地区 Spot，ZAR/MYR/NGN/IDR 等法币市场、orders/fills。

边界：

- 地区限制和可交易市场必须写入 adapter 文档。
- 不接入法币提款、充值地址、payment rails。

分配 prompt：

```text
You are AI-18. Complete Task 18 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `btcturk` and `luno`. Do not assume or specify a model. Keep regional availability and fiat-market behavior explicit. Do not run cargo build; run only the allowed checks and focused tests.
```

## 任务 19：AI-19，Coinone + Coins.ph

目标：

- `coinone`：韩国 Spot，KRW 市场、REST/WS、orders/fills。
- `coinsph`：菲律宾 Spot，PHP 市场、Binance-like 或本地 API 差异审计、orders/fills。

边界：

- 如果 Coins.ph 与 Binance API profile 相似，也必须保留独立 adapter id 和文档边界。
- 不接入支付、钱包提现或法币转账。

分配 prompt：

```text
You are AI-19. Complete Task 19 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `coinone` and `coinsph`. Do not assume or specify a model. Implement regional spot adapters with strict credential and funding boundaries. Do not run cargo build; run only the allowed checks and focused tests.
```

## 任务 20：AI-20，CoinSpot + Indodax

目标：

- `coinspot`：澳洲 Spot，AUD 市场、private REST 能力和订单/fill parser。
- `indodax`：印尼 Spot，IDR 市场、public/private REST、orders/fills。

边界：

- 如果交易所只提供有限订单 API，优先 G0/G1/G2，写接口保持 Unsupported。
- 法币相关能力只作为文档边界，不进 runtime。

分配 prompt：

```text
You are AI-20. Complete Task 20 from docs/交易所网关/总览/exchange_gateway_next_40_parallel_tasks_zh.md for `coinspot` and `indodax`. Do not assume or specify a model. Prioritize safe spot public/private REST and explicit unsupported boundaries for limited APIs. Do not run cargo build; run only the allowed checks and focused tests.
```

## 每个 AI 的完成报告模板

完成后在回复或 PR 描述中按下面格式报告：

```text
Task: AI-XX
Exchanges: <id-a>, <id-b>

Implemented:
- <id-a>: products, REST/WS surfaces, request-spec/signing/parser fixtures
- <id-b>: products, REST/WS surfaces, request-spec/signing/parser fixtures

Explicit Unsupported:
- <id-a>: ...
- <id-b>: ...

Validation:
- python3 scripts/validate_exchange_endpoint_mapping.py ...: passed/failed/not run
- cargo fmt --check --package rustcta-exchange-gateway: passed/failed/not run
- cargo check -p rustcta-exchange-gateway --lib --message-format short: passed/failed/not run
- cargo test -p rustcta-exchange-gateway <id-a> --lib --message-format short: passed/failed/not run
- cargo test -p rustcta-exchange-gateway <id-b> --lib --message-format short: passed/failed/not run

Not run:
- cargo build: intentionally not run
- cargo build --release: intentionally not run
```

## 批量合并后的协调者检查

20 个任务都合并后，由协调者统一运行：

```bash
python3 scripts/validate_exchange_endpoint_mapping.py
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway --lib --message-format short
```

如果协调者决定做全仓门禁，再单独运行并记录：

```bash
cargo test --all-features
cargo clippy --all-targets --all-features
```

这些全仓命令不是单个 AI 的开发期要求。

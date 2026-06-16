# 交易所 Adapter 工具链补齐实施文档

状态日期：2026-06-08

本文档定义 `rustcta-exchange-gateway` 下一阶段的工作重点：先补齐 adapter 工具链和共享基础设施，再把当前已经支持的交易所全部按同一套标准迁移、验收和标记能力。本阶段不新增交易所，不追 79 个 CCXT 缺失交易所，不扩大产品线；只处理当前仓库已经存在的 gateway adapter。

## 目标

目标不是“多接几个交易所”，而是把接入、验证、运行和维护交易所 adapter 的标准流水线做完整。

最终希望达到：

- 新增或维护一个 adapter 时，开发者主要填写交易所差异：endpoint、签名、parser、fixture、capability。
- 共享层统一处理 request-spec、签名测试、限速、分页、WebSocket 生命周期、心跳、重连、订阅恢复、订单簿 resync、批量操作语义、错误分类和 REST reconciliation。
- 当前已支持交易所的能力声明可信：每项能力能区分 `Native`、`Composed`、`RestFallback`、`WsOnly`、`Unsupported`，而不是只用 bool 粗略表达。
- 所有私有写接口都能离线证明请求构造正确，不依赖真实下单才发现签名、path、body 或 header 错误。
- WebSocket 不再只是“能生成订阅 payload”，而是能描述和测试连接状态、心跳方向、鉴权续期、重连、重订阅、sequence gap 和 snapshot 恢复。

## 非目标

本阶段明确不做以下事情：

- 不新增 CCXT 缺失交易所。
- 不新增 `crates/rustcta-exchange-gateway/src/adapters/<new_exchange>/` 目录。
- 不为了填表强行打开交易所未验证产品线。
- 不把本地组合能力伪装成交易所原生能力。
- 不把提现、转账等高权限资金操作接入交易 runtime。可先定义只读模型，真实 `withdraw` 必须单独设计权限和运行边界。
- 不要求所有交易所实现所有能力。不支持的能力必须有结构化 `Unsupported` 说明。

## 当前实现范围

当前只覆盖 `crates/rustcta-exchange-gateway/src/adapters/` 下已有的 37 个 adapter 目录。产品线以代码中 `ExchangeClientCapabilities.market_types` 当前声明为准。

## 2026-06-08 收口状态

- endpoint mapping 已覆盖 37/37 个现有 adapter。
- `python3 scripts/validate_exchange_endpoint_mapping.py` 默认执行全量 adapter mapping 校验，当前通过 37/37。
- schema/validator 已兼容两类现有台账格式：标准 `endpoints` 数组，以及 AscendEX 这类 legacy `operations` map。
- LBank endpoint mapping 已补齐保守 `rate_limit_bucket` 和 `weight` 元数据。
- request-spec/signing fixture 已覆盖 37/37；兼容标准 `request_specs` 和早期 `request_spec` fixture 目录。
- 当前 `rustcta-exchange-gateway` 的核心收口门禁通过：`cargo fmt --check --package rustcta-exchange-gateway`、`cargo check -p rustcta-exchange-gateway`、`cargo test -p rustcta-exchange-gateway --test task7_planner`、`cargo test -p rustcta-exchange-gateway --all-features`、`cargo clippy -p rustcta-exchange-gateway --all-targets`。
- `cargo check -p rustcta-exchange-gateway` 当前已降至 0 个 adapter-local warning；本轮 warning 热点从 485 降至 206，再从 206 降至 0。
- 全仓 `cargo test --all-features` 已通过；同步修正 Toobit batch cancel 测试断言、HCR live-small 启动暂停配置、Backpack fixture 路径/签名向量和 BitMEX Task 14 mapping 文案门禁。

| # | Adapter | 当前声明产品线 | 本阶段处理方式 |
| --- | --- | --- | --- |
| 1 | `ascendex` | Spot, Perpetual | 纳入工具链迁移 |
| 2 | `backpack` | Spot, Perpetual | 纳入工具链迁移 |
| 3 | `biconomy` | Spot | 纳入工具链迁移 |
| 4 | `bigone` | Spot, Perpetual | 纳入工具链迁移 |
| 5 | `binance` | Spot | 作为基准 adapter |
| 6 | `bingx` | Spot, Perpetual | 纳入工具链迁移 |
| 7 | `bitget` | Spot | 作为非 Binance 对照 adapter |
| 8 | `bitkan` | Spot, Perpetual | 保守 adapter，继续显式 `Unsupported` 未验证能力 |
| 9 | `bitmex` | Spot, Perpetual, Future | 用于反向合约和 future 边界审计 |
| 10 | `bitrue` | Spot, Perpetual | 纳入工具链迁移 |
| 11 | `bitunix` | Spot, Perpetual | 纳入工具链迁移 |
| 12 | `blofin` | Perpetual | 用于 perp-only adapter 验证 |
| 13 | `coinbase` | Spot, Perpetual | 用于 JWT/bearer/INTX profile 边界 |
| 14 | `coindcx` | Spot, Perpetual | 用于 Socket.IO 和区域交易所边界 |
| 15 | `coinex` | Spot | 纳入工具链迁移 |
| 16 | `coinstore` | Spot, Perpetual | 纳入工具链迁移 |
| 17 | `cointr` | Spot, Perpetual | 纳入工具链迁移 |
| 18 | `coinw` | Spot, Perpetual | 纳入工具链迁移 |
| 19 | `cryptocom` | Spot, Perpetual | 用于 JSON-RPC 和 order-list 能力验证 |
| 20 | `deepcoin` | Spot, Perpetual | 纳入工具链迁移 |
| 21 | `digifinex` | Spot, Perpetual | 纳入工具链迁移 |
| 22 | `gateio` | Spot | 用于 spot-only 和非 Binance 签名验证 |
| 23 | `hashkey_global` | Spot, Perpetual | 纳入工具链迁移 |
| 24 | `kraken` | Spot, Perpetual | 用于不同 symbol 和账户语义验证 |
| 25 | `kucoin` | Spot | 用于 token/bullet 私有流边界 |
| 26 | `lbank` | Spot, Perpetual | 纳入工具链迁移 |
| 27 | `mexc` | Spot | 用于 Binance-like spot 对照 |
| 28 | `okx` | Spot | 作为非 Binance 基准 adapter |
| 29 | `orangex` | Spot, Perpetual | 用于 JSON-RPC 和私有 WS 下线边界 |
| 30 | `phemex` | Spot, Perpetual | 纳入工具链迁移 |
| 31 | `poloniex` | Spot, Perpetual | 用于双产品 REST/WS 基准 |
| 32 | `tapbit` | Spot, Perpetual | 纳入工具链迁移 |
| 33 | `toobit` | Spot, Perpetual | 用于 Binance-like spot/perp 对照 |
| 34 | `weex` | Spot, Perpetual | 纳入工具链迁移 |
| 35 | `whitebit` | Spot, Perpetual | 用于 WS parser 和私有流对照 |
| 36 | `woo` | Spot, Perpetual | 用于 algo/trigger 扩展边界 |
| 37 | `xt` | Spot, Perpetual | 纳入工具链迁移 |

## 总体策略

先做共享工具链，再迁移现有 adapter。

推荐顺序：

1. 先固化 capability 和 endpoint mapping，解决“每个 adapter 到底支持什么”。
2. 再补 request-spec 和 signing harness，解决“私有请求是否构造正确”。
3. 再补限速、错误、分页、reconciliation，解决“请求失败或断线后如何恢复”。
4. 再补 WebSocket runtime、心跳、鉴权续期、订单簿 resync，解决“流能不能长期稳定跑”。
5. 再补 batch planner 和幂等策略，解决“批量下单、批量撤单、超时、部分失败”。
6. 最后按 adapter 迁移矩阵把 37 个现有交易所逐个拉齐。

## 工具链分层

### L1 能力契约

当前 `ExchangeClientCapabilities` 以 bool 为主，例如 `supports_batch_place_order` 和 `supports_private_streams`。这不足以表达真实运行语义。

需要新增结构化能力模型，同时保留旧 bool 字段作为兼容层。

建议新增：

| 类型 | 用途 | 放置位置 |
| --- | --- | --- |
| `CapabilitySupport` | 表达 `Native`、`Composed`、`RestFallback`、`WsOnly`、`Unsupported` | `crates/rustcta-exchange-api/src/capabilities.rs` |
| `EndpointCapability` | 记录 endpoint、method、auth、weight、产品线、testnet 支持 | `crates/rustcta-exchange-api/src/capabilities.rs` |
| `BatchCapability` | 表达批量操作模式、原子性、最大条数、部分失败 | `crates/rustcta-exchange-api/src/capabilities.rs` |
| `HistoryCapability` | 表达 `since`、`limit`、`cursor`、最大回溯窗口 | `crates/rustcta-exchange-api/src/capabilities.rs` |
| `StreamCapability` | 表达 public/private WS、订阅、退订、心跳、重连、resync | `crates/rustcta-exchange-api/src/streams.rs` |
| `CredentialScope` | 表达 read-only、trade、transfer、withdraw 等权限边界 | `crates/rustcta-exchange-api/src/capabilities.rs` |

兼容要求：

- 短期内旧字段继续存在，避免一次性改爆所有 adapter。
- 新字段作为 `#[serde(default)]` 可选字段加入。
- 旧 bool 必须能从新结构推导，文档和 generator 以后以新结构为准。
- `Unsupported` 必须带 reason、官方文档缺失原因或未验证原因。

### L2 Endpoint Mapping

每个 adapter 需要有机器可读的 endpoint mapping，不再只散落在文档和代码里。

建议新增路径：

- `crates/rustcta-exchange-gateway/schemas/exchange_endpoint_mapping.schema.json`
- `crates/rustcta-exchange-gateway/src/adapters/<exchange>/endpoint_mapping.yaml`
- `scripts/validate_exchange_endpoint_mapping.py`

如果仓库暂时没有 gateway schema 目录，本阶段创建该目录。

endpoint mapping 最小字段：

| 字段 | 含义 |
| --- | --- |
| `operation` | 标准操作名，例如 `place_order`、`batch_cancel_orders` |
| `product` | `spot`、`linear_perp`、`inverse_future`、`option` |
| `transport` | `rest`、`websocket`、`socket_io`、`json_rpc` |
| `method` | REST method 或 WS command |
| `path` | endpoint path 或 channel |
| `auth` | `none`、`api_key`、`hmac`、`jwt`、`bearer`、`listen_key` |
| `rate_limit_bucket` | IP、key、uid、order、stream 等 bucket |
| `weight` | endpoint weight |
| `request_spec_required` | 私有或写接口必须为 true |
| `native_batch` | 是否交易所原生批量 |
| `atomicity` | `atomic`、`partial`、`non_atomic`、`unknown` |
| `testnet` | 是否有 testnet 或 sandbox 等价路径 |
| `official_doc` | 官方文档链接或内部备注 |

验收：

- 每个现有 adapter 至少覆盖当前 trait 已声明操作。
- 私有写操作没有 endpoint mapping 不允许标 `Native`。
- `bitkan` 这类未验证 adapter 可以只列 `unsupported_unverified`，不能留空。

### L3 Request-Spec 和签名测试

这是最优先补的基础设施之一。所有私有 REST 请求必须能离线验证请求构造。

建议新增共享测试工具：

- `crates/rustcta-exchange-gateway/src/request_spec.rs`
- `crates/rustcta-exchange-gateway/src/signing_spec.rs`
- `tests/fixtures/exchanges/<exchange>/request_specs/*.json`
- `tests/fixtures/exchanges/<exchange>/signing_vectors/*.json`

Request-spec 必须校验：

- HTTP method
- path
- query
- body
- headers
- timestamp 字段位置
- recv window 或 request expiry
- canonical string
- signature
- dry-run 不发真实订单

签名 harness 要支持：

- query-string HMAC
- JSON body HMAC
- base64 HMAC
- hex HMAC
- MD5 plus HMAC 组合
- JWT
- bearer token
- Ed25519 或 RSA 预留
- exchange-specific passphrase 或 account group

验收：

- `place_order`、`cancel_order`、`batch_place_orders`、`batch_cancel_orders`、`cancel_all_orders`、`amend_order` 只要 adapter 声明支持，都必须有 request-spec。
- `get_balances`、`get_positions`、`get_open_orders`、`get_recent_fills` 只要走私有 REST，也必须有 request-spec。
- 官方文档有签名样例时，必须使用官方样例；没有官方样例时，用固定本地 vector。
- 所有 vector 必须脱敏，不提交真实 key、secret、token、账户 id。

### L4 标准数据模型

当前 `ExchangeClient` 已有 symbol rules、order book、balances、positions、fees、orders、fills。工具链阶段不必一次性把 CCXT 全部模型加进 trait，但需要先补运行必需的模型和扩展点。

优先补：

| 模型 | 用途 | 当前策略 |
| --- | --- | --- |
| `MarketSnapshot` | 聚合 symbol rules、交易状态、精度、合约面值、settle asset | 新增，先由现有 `SymbolRules` 适配 |
| `ServerTimeSnapshot` | 校时和签名 drift | 新增工具接口，不一定进 `ExchangeClient` 第一版 |
| `Ticker` | REST ticker 和 WS ticker 标准输出 | 新增 public data helper |
| `BookTicker` | top of book 低延迟扫描 | 新增 public data helper |
| `PublicTrade` | 公共成交 | 新增 public data helper |
| `Candle` | klines、mark/index candle 扩展基础 | 新增 public data helper |
| `FundingRateSnapshot` | perp funding 当前值和下次时间 | 新增 perp data helper |
| `OpenInterestSnapshot` | perp open interest | 新增 perp data helper |
| `PageRequest` 和 `PageCursor` | 历史订单、成交、资金、账本分页 | 新增共享分页模型 |
| `RawPayload` 或 `extensions` | 保留交易所原始字段 | 允许 parser 输出标准字段同时保留 raw |

原则：

- 标准字段必须满足策略和 reconciliation。
- 交易所特有字段放入 raw 或 extensions，不污染核心模型。
- 新模型先作为 helper 和 response metadata 引入，等 2 到 3 个代表 adapter 跑通后再决定是否扩展 `ExchangeClient` trait。

### L5 错误分类和限速

当前 `ExchangeApiError` 分类较少。需要把交易所错误映射成可恢复、不可恢复、权限、限速、风控、参数、未知订单、重复 client id 等类别。

建议新增：

- `ExchangeErrorKind`
- `ExchangeErrorMapping`
- `RateLimitPlan`
- `RateLimitBucket`
- `RetryAfter`
- `OrderLevelError`

错误分类最小集合：

| 类别 | 示例 | 处理策略 |
| --- | --- | --- |
| `InvalidRequest` | 参数错误、精度错误、min notional | 不重试，返回调用方 |
| `AuthFailed` | key、signature、passphrase 错误 | 不重试，禁用私有交易 |
| `PermissionDenied` | key 无 trade 或 read 权限 | 不重试，标记 credential scope |
| `RateLimited` | 429、418、too many requests | 按 bucket 和 retry-after 延迟 |
| `TemporarilyUnavailable` | 维护、系统繁忙 | 可重试或切 fallback |
| `UnknownOrder` | 查询不到订单 | 进入 reconciliation 判断 |
| `DuplicateClientOrderId` | client id 重复 | 查询原订单，不盲目重下 |
| `InsufficientBalance` | 余额不足 | 不重试，触发账户刷新 |
| `RiskRejected` | 杠杆、仓位、风控拒单 | 不重试，输出风险原因 |
| `PartialFailure` | 批量部分成功 | 返回逐项结果 |

验收：

- 每个 adapter 至少有错误码 mapping 表或明确 `unknown_code_fallback`。
- 限速能力必须能表达 endpoint weight 和 bucket。
- 批量接口必须能返回逐项错误，不允许只返回一个模糊失败。

### L6 分页和历史回补

订单、成交、账本、funding history、open interest history、充值提现历史都会遇到分页问题。即使当前 trait 只暴露 `get_recent_fills`，也应该先把分页工具补出来。

建议新增：

- `PageRequest`
- `PageCursor`
- `HistoryWindowCapability`
- `BackfillPlan`
- `BackfillCheckpoint`

统一表达：

- `since`
- `until`
- `limit`
- `cursor`
- `from_id`
- `days_back`
- `max_limit`
- `max_time_window`
- `sort_order`

验收：

- `get_recent_fills` 和 `get_open_orders` 先接入分页能力声明。
- adapter 必须说明是否支持按 symbol、按时间、按 order id、按 trade id 查询。
- reconciliation 只能使用已声明支持的分页方式。

### L7 REST Reconciliation

私有 WebSocket 不可靠或不可用时，必须用 REST 补状态。下单超时、撤单超时、WS 断线、sequence gap 都不能直接当作订单失败。

建议新增：

- `crates/rustcta-exchange-gateway/src/reconciliation.rs`
- `ReconcilePlan`
- `ReconcileTrigger`
- `OrderReconcileState`
- `UnknownOrderPolicy`
- `RetryReconcilePolicy`

触发场景：

- `place_order` transport timeout
- `cancel_order` transport timeout
- `batch_place_orders` 部分失败或响应缺项
- `batch_cancel_orders` 部分失败或响应缺项
- 私有 WS 断线超过阈值
- public orderbook sequence gap
- 本地订单状态和交易所状态冲突

验收：

- 每个支持私有交易的 adapter 必须声明 reconciliation fallback。
- 没有 `query_order` 或 `get_open_orders` 的 adapter 不允许进入 `live_dry_run`。
- 超时重试必须先查询状态，不能盲目重复下单。

### L8 WebSocket Runtime

当前共享层已有 `StreamRuntimeState`、`StreamReconnectPolicy` 和部分 adapter session helper，但还没有完整统一 runtime。

需要补齐：

- `StreamSession`
- `StreamRuntimeEvent`
- `SubscriptionRegistry`
- `SubscriptionAck`
- `UnsubscribeRequest`
- `HeartbeatPolicy`
- `AuthRenewalPolicy`
- `ListenKeyLease`
- `WsLivenessState`
- `SequenceGap`
- `ResyncReason`

必须表达：

- 连接 URL
- public/private 是否分连接
- 订阅 payload
- 退订 payload
- server ping 还是 client ping
- pong 超时
- message stale 超时
- listenKey、token、JWT 续期方式
- 重连后是否需要重新登录
- 重连后是否需要重新订阅
- orderbook 是否需要重新 snapshot
- 是否支持 checksum
- 是否支持 sequence

验收：

- 每个支持 public WS 的 adapter 必须有 subscribe、unsubscribe、ack、heartbeat、parser fixture。
- 每个支持 private WS 的 adapter 必须有 auth、订阅、心跳、续期或不需要续期说明、订单或账户事件 fixture。
- 只支持订阅 payload 但没有 runtime 的 adapter，只能标 `WsSpecOnly`，不能标完整 `Native`。
- Socket.IO、JSON-RPC WS、listenKey、JWT 都要通过同一 runtime capability 表达差异。

### L9 批量操作和幂等

批量下单和撤单是最容易出风险的部分。当前 bool 无法表达原生批量、组合批量、原子性和部分失败。

建议新增：

- `BatchPlanner`
- `BatchCapability`
- `BatchItemResult`
- `BatchAtomicity`
- `BatchExecutionMode`
- `ClientOrderIdPolicy`
- `IdempotencyKey`

能力语义：

| 字段 | 说明 |
| --- | --- |
| `mode` | `Native`、`ComposedSequential`、`ComposedConcurrent`、`Unsupported` |
| `atomicity` | `Atomic`、`NonAtomic`、`Partial`、`Unknown` |
| `max_items` | 单批最大数量 |
| `same_symbol_required` | 是否要求同 symbol |
| `same_market_type_required` | 是否要求同产品线 |
| `supports_client_order_id` | 批量项是否支持 client id |
| `partial_failure_shape` | 逐项错误是否可还原 |
| `retry_policy` | 超时和部分失败如何 reconciliation |

验收：

- composed batch 不能标成 native。
- non-atomic batch 必须返回逐项结果或进入 reconciliation。
- 批量下单超时不能直接重放同一批订单，必须按 client order id 查询。

### L10 Live Read-Only 和文档生成

工具链最后需要可重复验证。

建议新增工具：

| 工具 | 路径 | 作用 |
| --- | --- | --- |
| `audit_gateway_adapters.py` | `scripts/audit_gateway_adapters.py` | 扫描 adapter、capability、endpoint mapping、fixture、request-spec 覆盖 |
| `validate_exchange_endpoint_mapping.py` | `scripts/validate_exchange_endpoint_mapping.py` | 校验 endpoint mapping schema |
| `sanitize_exchange_fixture.py` | `scripts/sanitize_exchange_fixture.py` | 脱敏真实响应 |
| `exchange_live_probe` | `tools/ops` 或 `apps/gateway` 下的只读 probe 命令 | 只读 live validation |
| `generate_exchange_capability_docs.py` | `scripts/generate_exchange_capability_docs.py` | 从 capability 和 endpoint mapping 生成文档 |

只读 live probe 至少检查：

- server time
- market rules
- order book
- fee readback
- balances
- positions
- open orders
- recent fills dry read
- public WS subscribe 5 到 30 秒
- private WS auth 和订阅，不消费真实交易

## 阶段计划

### 阶段 0：冻结范围和生成基线

目标：确认本阶段只处理 37 个现有 adapter。

交付：

- 生成 adapter inventory。
- 记录每个 adapter 当前声明产品线。
- 记录每个 adapter 当前 trait 方法是否 override 默认 `Unsupported`。
- 标记每个 adapter 是否有 public/private REST、public/private WS、request tests、stream tests。

建议输出：

- `docs/交易所网关/总览/exchange_adapter_toolchain_status_zh.md`
- `logs/exchange_adapter_toolchain_audit_YYYYMMDD.json`

验收：

- 37 个 adapter 全部在 inventory 中。
- inventory 与 `crates/rustcta-exchange-gateway/src/adapters/` 目录一致。
- 没有把旧 root exchange tree 当成本阶段新增范围；新增能力进入
  `crates/rustcta-exchange-gateway/src/adapters/`。

### 阶段 1：Capability v2

目标：把 bool 能力升级成结构化能力，同时兼容当前调用方。

交付：

- 新增 `CapabilitySupport`。
- 新增 `BatchCapability`。
- 新增 `HistoryCapability`。
- 新增 `StreamRuntimeCapability`。
- 新增 `CredentialScope`。
- `ExchangeClientCapabilities` 增加 v2 字段，旧 bool 保留。
- Binance、OKX、Bitget、Gate.io、Poloniex、Crypto.com 先完成 v2 能力声明。

验收：

- `cargo test -p rustcta-exchange-api`
- `cargo test -p rustcta-exchange-gateway capabilities`
- 旧 protocol 序列化兼容。
- 新 capability 能生成 markdown 状态表。

### 阶段 2：Endpoint Mapping 和 Request-Spec

目标：所有私有 REST 能离线验证。

交付：

- endpoint mapping schema。
- request-spec 测试 helper。
- signing vector helper。
- 先覆盖基准 adapter：Binance、OKX、Bitget、Gate.io、Poloniex、Crypto.com。
- 再铺到其余 31 个 adapter。

验收：

- 每个支持私有 REST 的 adapter 至少有 `get_balances`、`place_order`、`cancel_order`、`query_order` request-spec。
- 每个支持批量或 cancel-all 的 adapter 有对应 request-spec。
- 签名 vector 不含真实 secret。
- `scripts/validate_exchange_endpoint_mapping.py` 通过。

### 阶段 3：错误、限速、分页

目标：失败可分类，历史可回补，限速可控。

交付：

- `ExchangeErrorKind` 和 mapping helper。
- `RateLimitPlan` 和 bucket schema。
- `PageRequest`、`PageCursor`、`HistoryWindowCapability`。
- `get_recent_fills` 和 `get_open_orders` 先接入分页声明。

验收：

- adapter 错误解析测试覆盖成功、空响应、错误响应、限速响应。
- 支持 historical query 的 adapter 必须声明分页边界。
- unknown order、duplicate client id、rate limited、auth failed 有统一分类。

### 阶段 4：WebSocket Runtime

目标：把 WS 从 payload helper 升级成可监督 runtime。

交付：

- `StreamSession` 和 `SubscriptionRegistry`。
- `HeartbeatPolicy`。
- `AuthRenewalPolicy`。
- `ListenKeyLease`。
- `UnsubscribeRequest`。
- `SequenceGap` 和 `ResyncReason`。
- public orderbook state machine。

先迁移：

- Binance：listenKey 和 user stream 基准。
- OKX：login channel 和 public/private WS 对照。
- KuCoin：bullet token 边界。
- Poloniex：已有 `StreamRuntimeState` 使用较多，适合验证抽象。
- WhiteBIT 或 Crypto.com：私有事件 parser 对照。

验收：

- public WS 支持订阅、退订、ack、heartbeat、重连后 resubscribe。
- orderbook 支持 snapshot-only、best-effort delta、strict delta 三种声明。
- sequence gap 会触发 resync，而不是继续使用脏 book。
- private WS token 过期会触发续期或重登。

### 阶段 5：REST Reconciliation 和交易安全闭环

目标：真实交易前必须能处理超时、断线、部分失败和未知订单状态。

交付：

- `ReconcilePlan`。
- `RetryReconcilePolicy`。
- `UnknownOrderPolicy`。
- `ClientOrderIdPolicy`。
- `BatchPlanner`。
- `BatchItemResult`。
- dry-run 和 live-dry-run 保护统一化。

验收：

- `place_order` 超时后进入 query/order open-orders reconciliation。
- `cancel_order` 超时后进入 query/open-orders reconciliation。
- batch partial failure 返回逐项结果。
- 没有 client order id 的高风险交易所不能启用自动重试下单。
- live-dry-run 必须有 max notional、disabled symbol、kill-switch。

### 阶段 6：现有 37 个 Adapter 全量迁移

目标：所有当前已支持 adapter 都使用同一套工具链声明、测试和文档。

迁移顺序：

| 批次 | Adapter | 原因 |
| --- | --- | --- |
| R0 基准 | `binance`, `okx`, `bitget`, `gateio`, `poloniex`, `cryptocom` | 覆盖 Binance-like、非 Binance、spot-only、spot+perp、JSON-RPC/order-list、WS runtime |
| R1 主流 | `coinbase`, `kraken`, `kucoin`, `mexc`, `coinex`, `toobit`, `whitebit`, `woo`, `phemex`, `bitmex` | 交易价值高，能验证账户、symbol、perp、future、WS 差异 |
| R2 已完成度较高 | `ascendex`, `lbank`, `weex`, `xt`, `coinw`, `bitrue`, `tapbit`, `deepcoin`, `orangex`, `coindcx` | 多数已有文档和 request/parser 测试，可快速补齐工具链 metadata |
| R3 长尾但已存在 | `backpack`, `biconomy`, `bigone`, `bingx`, `bitkan`, `bitunix`, `blofin`, `coinstore`, `cointr`, `digifinex`, `hashkey_global` | 保持现有能力边界，补显式 Unsupported、fixtures、endpoint mapping |

每个 adapter 的迁移交付：

- `endpoint_mapping.yaml`
- `capabilities_v2` 声明
- request-spec tests
- signing vector tests
- parser fixture tests
- public/private WS runtime capability
- heartbeat/auth renewal policy
- rate-limit plan
- pagination capability
- reconciliation plan
- batch capability
- adapter 文档更新

## 当前 Adapter 迁移矩阵

| Adapter | 当前产品线 | 必补工具链项 | 完成标准 |
| --- | --- | --- | --- |
| `binance` | Spot | Capability v2、endpoint mapping、request-spec、listenKey lease、batch planner | 作为 Spot 基准，所有工具链测试先跑通 |
| `okx` | Spot | Capability v2、endpoint mapping、request-spec、login WS、pagination | 非 Binance 签名和 WS login 基准 |
| `bitget` | Spot | endpoint mapping、request-spec、错误分类、REST reconciliation | Spot-only 对照完成 |
| `gateio` | Spot | endpoint mapping、request-spec、分页、private stream fallback | Spot-only 非 Binance 对照完成 |
| `mexc` | Spot | endpoint mapping、request-spec、error/rate-limit、public WS capability | Binance-like spot 对照完成 |
| `coinex` | Spot | endpoint mapping、request-spec、pagination、private fallback | Spot-only 完成 |
| `kucoin` | Spot | endpoint mapping、request-spec、bullet token lease、private WS capability | token 私有流边界完成 |
| `biconomy` | Spot | endpoint mapping、Unsupported reason、public WS spec、request-spec 覆盖已有私有能力 | scan-only 或 trade 能力边界可信 |
| `blofin` | Perpetual | perp endpoint mapping、funding/open interest capability、private WS fallback | perp-only adapter 能力可信 |
| `bitmex` | Spot, Perpetual, Future | future/perp product mapping、request-spec、orderbook strictness、error mapping | future 边界清晰 |
| `coinbase` | Spot, Perpetual | JWT/bearer signing vector、INTX profile、WS heartbeat、portfolio scope | profile 和 credential scope 清晰 |
| `cryptocom` | Spot, Perpetual | JSON-RPC request-spec、order-list capability、WS heartbeat、batch atomicity | JSON-RPC 基准完成 |
| `poloniex` | Spot, Perpetual | StreamRuntime 抽象迁移、batch capability、request-spec、pagination | spot+perp 双产品基准完成 |
| `whitebit` | Spot, Perpetual | private stream normalizer、request-spec、heartbeat、reconciliation | 私有事件 parser 基准完成 |
| `woo` | Spot, Perpetual | algo/trigger 边界、batch planner、WS trading 标记、rate-limit | adapter-specific 扩展边界清晰 |
| `kraken` | Spot, Perpetual | symbol normalization、account model、request-spec、WS policy | 非 Binance symbol/account 基准完成 |
| `toobit` | Spot, Perpetual | listenKey policy、batch native capability、request-spec、perp fallback | Binance-like spot/perp 对照完成 |
| `phemex` | Spot, Perpetual | endpoint mapping、request-spec、perp capability、WS policy | spot+perp 能力可信 |
| `ascendex` | Spot, Perpetual | account group signing、request-spec、batch capability、WS auth | account group 边界清晰 |
| `lbank` | Spot, Perpetual | MD5/HMAC signing vector、perp unsupported gaps、endpoint mapping | 已验证和未验证能力分开 |
| `weex` | Spot, Perpetual | ACCESS signing vector、private WS auth、batch capability | spot/perp request-spec 完成 |
| `xt` | Spot, Perpetual | spot/futures signing split、listen token、batch capability | 双签名路径完成 |
| `coinw` | Spot, Perpetual | spot/futures signing split、batch planner、error mapping | 双产品边界清晰 |
| `bitrue` | Spot, Perpetual | spot/futures auth split、Unsupported reason、request-spec | 保守能力可信 |
| `tapbit` | Spot, Perpetual | ACCESS signing、public WS runtime、private unsupported reason | WS spec-only 和 native 能力分清 |
| `deepcoin` | Spot, Perpetual | request-spec、amend capability、private WS policy、batch capability | 高级订单边界清晰 |
| `orangex` | Spot, Perpetual | JSON-RPC mapping、private WS deprecation reason、token policy | 下线能力不误标 |
| `coindcx` | Spot, Perpetual | Socket.IO capability、signing vector、batch capability、private parser | Socket.IO 差异可表达 |
| `backpack` | Spot, Perpetual | signing vector、WS heartbeat、perp capability、request-spec | 当前能力完整可审计 |
| `bigone` | Spot, Perpetual | endpoint mapping、request-spec、parser fixtures、Unsupported reason | 当前能力可信 |
| `bingx` | Spot, Perpetual | endpoint mapping、request-spec、WS runtime、perp risk data capability | 当前能力可信 |
| `bitkan` | Spot, Perpetual | unsupported endpoint mapping、public WS helper 标记、文档原因 | 未验证能力全部显式 unsupported |
| `bitunix` | Spot, Perpetual | endpoint mapping、request-spec、WS policy、batch capability | 当前能力可信 |
| `coinstore` | Spot, Perpetual | endpoint mapping、request-spec、WS policy、error mapping | 当前能力可信 |
| `cointr` | Spot, Perpetual | endpoint mapping、request-spec、private stream parser、batch capability | 当前能力可信 |
| `digifinex` | Spot, Perpetual | endpoint mapping、request-spec、perp boundary、WS policy | 当前能力可信 |
| `hashkey_global` | Spot, Perpetual | product profile、request-spec、private stream parser、rate-limit | 当前能力可信 |

## Definition Of Done

共享工具链完成标准：

- `ExchangeClientCapabilities` 有结构化 v2 能力，并兼容旧 bool。
- endpoint mapping schema 可校验。
- request-spec 和 signing harness 可复用到所有 adapter。
- 错误分类、限速、分页、reconciliation、batch planner 有共享类型和测试。
- WebSocket runtime 能表达 public/private 连接、订阅、退订、心跳、鉴权续期、重连、重订阅、resync。
- live read-only validator 能对任意 adapter 输出一致报告。

单个 adapter 完成标准：

- 有 endpoint mapping。
- 有 capabilities v2。
- 所有已声明私有 REST 有 request-spec。
- 所有签名方式有 signing vector。
- parser 有 fixture，至少覆盖成功、空响应、错误响应、关键字段缺失。
- public WS 支持能力、心跳和 resync 说明。
- private WS 支持能力、鉴权和续期说明；不支持时有 REST reconciliation fallback 或明确 `Unsupported`。
- batch place/cancel 有 native/composed、atomicity、max items、partial failure 声明。
- 错误码和限速有 mapping。
- live-dry-run 前具备 reconciliation、kill-switch、disabled-symbol、max-notional。

## 验收命令

文档阶段不要求这些命令现在全部存在。工具链实现后，必须能提供以下入口：

```bash
cargo fmt
cargo clippy --all-targets --all-features
cargo test -p rustcta-exchange-api
cargo test -p rustcta-exchange-gateway
python3 scripts/validate_exchange_endpoint_mapping.py
python3 scripts/audit_gateway_adapters.py
cargo run -p rustcta-exchange-gateway --bin exchange_live_probe -- --read-only --exchange binance
```

迁移单个 adapter 时推荐：

```bash
cargo test -p rustcta-exchange-gateway <adapter> --lib
python3 scripts/audit_gateway_adapters.py --exchange <adapter>
cargo run -p rustcta-exchange-gateway --bin exchange_live_probe -- --read-only --exchange <adapter>
```

## 20 并发任务拆分

本节用于 20 个 AI 同步执行。每个 AI 只认领一个数字编号，编号使用 `1` 到 `20`，不要再使用 `T1` 这类旧编号。所有 AI 都必须阅读本文档的目标、非目标、当前实现范围、Definition Of Done 和风险边界。

并发规则：

- `1` 到 `8` 是共享工具链任务，允许修改共享 crate、脚本、schema 和少量基准 adapter。
- `9` 到 `20` 是现有 adapter 迁移任务，只改自己负责的 adapter、fixture 和对应 adapter 文档。
- 任何 adapter 任务如果发现共享类型不够用，只在自己的文档中记录需求，不直接扩展共享 trait；由 `1` 到 `8` 对应任务统一处理。
- 每个任务都必须在最终说明中列出修改文件、验证命令、未完成项和是否影响其他编号。
- 不新增交易所目录，不修改 79 个 CCXT 缺失交易所计划。
- 不提交真实 key、secret、token、账户 id、地址或订单 id。

### 共享工具链任务 1-8

| 编号 | 任务 | 主要写入边界 | 交付 | 依赖/协调 |
| --- | --- | --- | --- | --- |
| 1 | Capability v2 | `crates/rustcta-exchange-api/src/capabilities.rs`, `crates/rustcta-exchange-api/src/streams.rs`, `crates/rustcta-exchange-api/src/lib.rs` | `CapabilitySupport`、`BatchCapability`、`HistoryCapability`、`StreamRuntimeCapability`、`CredentialScope`；旧 bool 兼容；基础序列化测试 | 所有共享任务依赖此能力模型；先做最小可扩展结构，不一次性迁移 37 个 adapter |
| 2 | Endpoint mapping schema 和 validator | `crates/rustcta-exchange-gateway/schemas/`, `scripts/validate_exchange_endpoint_mapping.py`, `crates/rustcta-exchange-gateway/src/adapters/binance/endpoint_mapping.yaml`, `.../okx/endpoint_mapping.yaml` | JSON schema、校验脚本、Binance/OKX 样例 mapping、README 说明 | 依赖 1 的 operation/capability 命名；不改其他 adapter |
| 3 | Request-spec 和 signing harness | `crates/rustcta-exchange-gateway/src/request_spec.rs`, `crates/rustcta-exchange-gateway/src/signing_spec.rs`, `tests/fixtures/exchanges/binance/`, `tests/fixtures/exchanges/okx/` | 通用 request matcher、signing vector 格式、Binance/OKX 私有读写样例测试 | 依赖 2 的 endpoint mapping 字段；不要迁移其他 adapter |
| 4 | 错误分类、限速和分页模型 | `crates/rustcta-exchange-api/src/error.rs`, `crates/rustcta-exchange-api/src/types.rs`, `crates/rustcta-exchange-gateway/src/error.rs`, `crates/rustcta-exchange-gateway/src/http.rs` | `ExchangeErrorKind`、`RateLimitPlan`、`RateLimitBucket`、`PageRequest`、`PageCursor`、基础测试 | 依赖 1；如需改现有错误 enum，保持 serde/backward compatibility |
| 5 | WebSocket runtime 基础设施 | `crates/rustcta-exchange-gateway/src/streams.rs`, `crates/rustcta-exchange-api/src/streams.rs` | `StreamSession`、`SubscriptionRegistry`、`SubscriptionAck`、`UnsubscribeRequest`、`HeartbeatPolicy`、`AuthRenewalPolicy`、`ListenKeyLease`、runtime 测试 | 依赖 1；不要迁移具体 adapter，只提供通用 runtime 和测试 |
| 6 | Orderbook state machine 和 WS resync | `crates/rustcta-exchange-gateway/src/streams.rs`, `crates/rustcta-exchange-gateway/src/orderbook_state.rs` | snapshot/delta state machine、sequence gap、checksum hook、`ResyncReason`、测试 | 依赖 5；只做通用状态机，不改各 adapter parser |
| 7 | REST reconciliation 和批量操作 planner | `crates/rustcta-exchange-gateway/src/reconciliation.rs`, `crates/rustcta-exchange-gateway/src/batch.rs`, `crates/rustcta-exchange-api/src/order.rs` | `ReconcilePlan`、`RetryReconcilePolicy`、`UnknownOrderPolicy`、`ClientOrderIdPolicy`、`BatchPlanner`、`BatchItemResult` | 依赖 3、4；保持现有 batch response 兼容 |
| 8 | Audit、fixture sanitizer、live read-only validator | `scripts/audit_gateway_adapters.py`, `scripts/sanitize_exchange_fixture.py`, `tools/ops` 或 `apps/gateway` 下的只读 probe 命令, `docs/交易所网关/总览/exchange_adapter_toolchain_status_zh.md` | adapter 覆盖审计、fixture 脱敏工具、只读 live probe、状态文档生成入口 | 依赖 2 到 7；初版可只支持 Binance/OKX，后续 adapter 任务补数据 |

### Adapter 迁移任务 9-20

Adapter 任务的共同交付：

- `endpoint_mapping.yaml`
- `capabilities_v2` 或等价能力声明
- request-spec tests
- signing vector tests
- parser fixture tests
- public/private WS runtime capability
- heartbeat/auth renewal policy
- rate-limit plan
- pagination capability
- reconciliation plan
- batch capability
- adapter 文档更新

| 编号 | Adapter 范围 | 主要写入边界 | 迁移重点 | 依赖/协调 |
| --- | --- | --- | --- | --- |
| 9 | `binance`, `okx` | `crates/rustcta-exchange-gateway/src/adapters/binance/`, `.../okx/`, `tests/fixtures/exchanges/binance/`, `.../okx/`, 对应 docs | 两个基准 adapter 全量迁移；覆盖 Binance-like 和 OKX login/签名/WS 差异 | 依赖 1-8；发现共享缺口先反馈给对应编号 |
| 10 | `bitget`, `gateio`, `mexc` | 三个 adapter 目录、fixture、对应 docs | Spot-only/Binance-like/非 Binance 签名对照；重点是 request-spec、错误分类、REST reconciliation | 依赖 1-8；不打开未声明 perp |
| 11 | `coinex`, `kucoin`, `kraken` | 三个 adapter 目录、fixture、对应 docs | Spot-only、token/bullet 私有流、Kraken symbol/account 语义；重点是 symbol normalization、pagination、WS policy | 依赖 1-8；KuCoin token 续期与 5 协调 |
| 12 | `poloniex`, `cryptocom`, `whitebit` | 三个 adapter 目录、fixture、对应 docs | spot+perp 双产品、JSON-RPC/order-list、私有事件 parser；重点是 StreamRuntime、batch atomicity、private stream normalizer | 依赖 1-8；WS runtime 与 5/6 协调 |
| 13 | `coinbase`, `woo`, `toobit` | 三个 adapter 目录、fixture、对应 docs | JWT/bearer/portfolio scope、algo/trigger 边界、listenKey；重点是 credential scope、adapter-specific 扩展和 batch planner | 依赖 1-8；algo/trigger 不扩共享 trait，先记录扩展 |
| 14 | `bitmex`, `phemex`, `blofin` | 三个 adapter 目录、fixture、对应 docs | perp/future/perp-only adapter；重点是 contract spec、orderbook strictness、future/perp 产品边界、funding/open-interest capability | 依赖 1-8；不新增期权模型 |
| 15 | `ascendex`, `lbank`, `weex` | 三个 adapter 目录、fixture、对应 docs | account group、MD5/HMAC、ACCESS signing；重点是 signing vector、endpoint mapping、unsupported gaps | 依赖 1-8 |
| 16 | `xt`, `coinw`, `bitrue` | 三个 adapter 目录、fixture、对应 docs | spot/futures 签名拆分、保守 unsupported、batch capability；重点是双产品 endpoint mapping 和错误分类 | 依赖 1-8 |
| 17 | `tapbit`, `deepcoin`, `orangex` | 三个 adapter 目录、fixture、对应 docs | ACCESS signing、amend/batch、JSON-RPC、私有 WS 下线边界；重点是 WS spec-only 标记和 deprecation reason | 依赖 1-8 |
| 18 | `coindcx`, `cointr`, `coinstore` | 三个 adapter 目录、fixture、对应 docs | Socket.IO、私有 stream parser、长尾 spot+perp；重点是 transport capability 和 request-spec | 依赖 1-8；Socket.IO 能力与 5 协调 |
| 19 | `backpack`, `bigone`, `bingx` | 三个 adapter 目录、fixture、对应 docs | 当前能力边界、WS heartbeat、perp capability；重点是 endpoint mapping、signing vector、reconciliation plan | 依赖 1-8 |
| 20 | `biconomy`, `bitkan`, `bitunix`, `digifinex`, `hashkey_global` | 五个 adapter 目录、fixture、对应 docs | 保守/长尾 adapter 收口；重点是显式 Unsupported reason、scan-only/trade 边界、fixture 和 endpoint mapping 补齐 | 依赖 1-8；未验证能力不得打开 |

### 20 并发合并顺序

为了减少冲突，建议按以下顺序合并：

1. 先合并 `1`，锁定 capability v2 字段。
2. 再合并 `2`、`3`、`4`，锁定 endpoint、request-spec、错误/限速/分页基础。
3. 再合并 `5`、`6`、`7`，锁定 WS runtime、orderbook state、reconciliation 和 batch planner。
4. 再合并 `8`，让审计脚本和 live probe 能读取前面结构。
5. 最后并行合并 `9` 到 `20` 的 adapter 迁移。若 adapter 任务依赖的共享字段尚未合入，只提交 adapter-local mapping、fixture 和文档，不修改共享结构。

## 风险边界

- 工具链迁移期间，旧 bool 和旧 trait 不要一次性删除。
- 任何 adapter 没有 request-spec 的私有写能力，不能升级到真实交易候选。
- 任何 adapter 没有 reconciliation plan 的私有交易能力，不能升级到 live-dry-run。
- 任何 WebSocket 没有心跳和重连策略，只能标 `WsSpecOnly` 或 parser-only。
- 任何批量操作没有 partial failure 语义，不得用于真实批量下单。
- 任何 API key 需要 withdraw 或 transfer 权限的功能，不进入交易 runtime。

## 和 CCXT 缺口文档的关系

`docs/交易所网关/总览/exchange_gateway_expansion_30_venues_zh.md` 负责记录 CCXT 全量交易所缺口和未来新增交易所计划。本文档负责当前阶段的工程顺序：先补 adapter 工具链，只迁移当前已经支持的 37 个 gateway adapter。

只有当本文档的工具链和 37 个现有 adapter 迁移完成后，才进入 CCXT 缺失交易所补齐阶段。

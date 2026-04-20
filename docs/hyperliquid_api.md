# Hyperliquid API 接口清单与策略对接说明

面向本仓库（单向持仓、一口价永续），汇总当前需要支持的 Hyperliquid API/WS 功能，并标注策略映射与注意事项。

## 基础信息
- 主网 Base URL：`https://api.hyperliquid.xyz`，WS：`wss://api.hyperliquid.xyz/ws`
- 测试网 Base URL：`https://api.hyperliquid-testnet.xyz`，WS：`wss://api.hyperliquid-testnet.xyz/ws`
- 持仓模式：仅单向（`type: "oneWay"`），不支持双向对冲；策略不得假设双向/对锁。
- 鉴权：私有写入通过 `POST /exchange`，payload 需包含 `action`、`nonce`(ms 时间戳) 与签名；推荐使用 API Wallet（保持主钱包地址为 `vaultAddress`）。公共查询走 `POST /info`。
- 参考资料：官方文档 <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api>；Python SDK <https://github.com/hyperliquid-dex/hyperliquid-python-sdk>；Rust SDK（未维护）<https://github.com/hyperliquid-dex/hyperliquid-rust-sdk>。

## 功能模块与接口

### 行情订阅（WS）
- 订单簿：`{"type": "l2Book", "coin": "<symbol>"}`，返回 `levels[[bids],[asks]]` 与 `time`。
- 逐笔成交：`{"type": "trades", "coin": "<symbol>"}`，字段 `coin/side/px/sz/time`。
- 最优价：`{"type": "bbo", "coin": "<symbol>"}`。
- K 线：`{"type": "candle", "coin": "<symbol>", "interval": "<e.g. 1m>"}`
- 全市场中间价：`{"type": "allMids"}`。
- 资产状态：`{"type": "activeAssetCtx", "coin": "<symbol>"}`（含资金费率、OI 等）。
- 心跳：客户端定期 `{"method": "ping"}`，服务端回 `pong`。

### 用户事件订阅（WS）
- 账户事件：`{"type": "userEvents"}`（单连接仅允许一次订阅，包含余额/仓位/订单生命周期事件）。
- 成交推送：`{"type": "userFills", "user": "<wallet_address>"}`，字段 `coin/oid/px/sz/side/time/dir/crossed/startPosition/closedPnl` 等。
- 订单状态：`{"type": "orderUpdates"}`（单连接一次），含 `oid/cloid/status`、`filled`、`remaining`、`avgPx` 等。
- 资金费率：`{"type": "userFundings", "user": "<wallet_address>"}`。
- 账本更新：`{"type": "userNonFundingLedgerUpdates", "user": "<wallet_address>"}`。
- 备注：地址需小写；`orderUpdates` 与 `userEvents` 不支持多订阅复用。

### 轮询查询（HTTP `POST /info`）
- 账户与仓位：`{"type": "clearinghouseState", "user": "<address>"}`，返回 `assetPositions`（`type: "oneWay"`）、`marginSummary`、`withdrawable` 等。
- 资金费率与资产上下文：`{"type": "metaAndAssetCtxs"}`；补充 `{"type": "fundingHistory", "coin": "<symbol>", ...}`。
- 开仓/挂单：`{"type": "openOrders", "user": "<address>"}` 或 `{"type": "frontendOpenOrders", ...}`。
- 成交轮询：`{"type": "userFills", "user": "<address>"}`；时间过滤用 `{"type": "userFillsByTime", "startTime": ..., "endTime": ...}`。
- 历史订单：`{"type": "historicalOrders", "user": "<address>"}`（最多返回近期 2000 条）。
- 行情快照回退：`{"type": "l2Book", "coin": "<symbol>"}`；K 线 `{"type": "candleSnapshot", "req": {"coin": "...", "interval": "...", "startTime": ..., "endTime": ...}}`。

### 下单/批量下单（HTTP `POST /exchange`）
- 单笔/批量下单：`{"type": "order", "orders": [ { "a": <asset_id>, "b": <is_buy>, "p": "<px>", "s": "<sz>", "r": <reduce_only>, "t": {"limit": {"tif": "Gtc"|"Ioc"} } } ], "grouping": "na" }`
  - `a` 为资产 ID（可用 `meta`/SDK 的 `name_to_asset` 获取），`b` 买卖方向，`p` 价格字符串，`s` 数量字符串，`r` 是否只减仓，`t` 支持 `limit`/`market`（市场单用 IOC 限价包一层）。
  - 批量下单直接在 `orders` 内放多条；`grouping` 可用于撮合分组（默认为 `na`）。
  - 可选 `cloid`（客户端自定义 ID）放入订单以便后续 `cancelByCloid`。

### 撤单/批量撤单与修改（HTTP `POST /exchange`）
- 撤单：`{"type": "cancel", "cancels": [ { "a": <asset_id>, "o": <oid> } ] }`
- 按客户端 ID 撤单：`{"type": "cancelByCloid", "cancels": [ { "asset": <asset_id>, "cloid": "<cloid>" } ] }`
- 批量改单：`{"type": "batchModify", "modifies": [ { "oid": <oid|cloid>, "order": { ...同下单... } } ] }`
- 计划全撤：`{"type": "scheduleCancel", "time": <future_ms>|null }`（可用于防挂机风险）。

### 账户与保证金操作（HTTP `POST /exchange`）
- 调整杠杆：`{"type": "updateLeverage", "asset": <asset_id>, "isCross": true|false, "leverage": <int>}`
- 添加/移除逐仓保证金：`{"type": "updateIsolatedMargin", "asset": <asset_id>, "isBuy": true, "ntli": "<usd_int>"}`（逐仓仅在支持时有效）。
- 资金划转：`usdClassTransfer`、`sendAsset`、`subAccountTransfer` 等按 SDK 签名规范使用；策略侧目前主要用 `clearinghouseState` 读余额。

## 策略映射与覆盖说明
- 行情驱动（网格、做市）：使用 `l2Book`/`bbo`/`trades` WS；若 WS 失效可回退 `POST /info` 的 `l2Book`。现有 `hyperliquid.rs` 尚未实现 WS，需要新增。
- 下单/批量下单：对应 `order`/`orders` 动作；`trend_grid_v2` 的 `hyperliquid_batch_size` 应映射为 `orders` 列表长度；撮合模式需用单向持仓（不支持对冲）。
- 撤单：使用 `cancel` 或 `cancelByCloid`；批量撤销可一次传多条或使用 `scheduleCancel` 兜底。
- 成交回报链路：首选 WS `userFills`/`orderUpdates`，回补使用 `userFillsByTime` 轮询；需存储 `startTime` 用于断线重连补数据。
- 账户/仓位：`clearinghouseState` 返回单向仓位与保证金；策略不得假设存在多仓/空仓同时持有。
- 费率/资金费：`metaAndAssetCtxs` 获取资金费率等参数；支付/收取结果可通过 `userFundings` 订阅或 `userFunding` 查询。

## 兼容性与后续实现提示
- 现有 `src/exchanges/hyperliquid.rs` 未启用且缺少 WS、批量单、成交/历史查询等能力，实现时应以本清单为目标，统一走 `/info` 与 `/exchange` 规范。
- 单向持仓是交易所限制，若策略需要反手应显式平仓后再开反向，或使用 `reduceOnly` 避免反向加仓。
- 对接时保持地址小写、`nonce` 使用 ms 级时间戳，失败时遵循官方限流（HTTP 429）退避；WS `userEvents`/`orderUpdates` 不可多重订阅。

## 断线重连与集成测试建议
- WS 自动重连：客户端 30~50s 发送 `{"method": "ping"}`；若 3 次超时或连接关闭，延迟 5s 指数退避重连，并重新订阅行情、`userEvents`、`orderUpdates`、`userFills` 等。
- 补数：WS 中断后记录最后成交时间戳，重连成功后调用 `{"type": "userFillsByTime", "startTime": <last_ts>, "endTime": now}` 补齐；必要时设置 `aggregateByTime: true`。
- 网格策略回放：测试链路“下单→部分成交→撤单→重下单”，断开 WS 后用 `userFillsByTime` 补齐，再比对本地订单簿/头寸，确认无重复记账或漏计（含 reduceOnly）。
- 做市策略回放：模拟双边挂单、部分成交、资金费触发，重连后校验 `openOrders` + `clearinghouseState` 与本地状态一致，并验证撤单/改单重新派发成功。
- 持仓一致性：单向持仓要求反手=先平后开，测试需验证 `reduceOnly` 平仓路径，不允许形成对锁；补数后仓位与 `assetPositions` 对齐。
- 回退路径：WS 多次失败后切换轮询（`l2Book`/`userFillsByTime`/`openOrders`），恢复后切回 WS；测试需覆盖模式切换对网格/做市状态机的影响。

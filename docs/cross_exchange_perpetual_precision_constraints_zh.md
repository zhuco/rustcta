# 跨所合约套利精度与合约张数约束

Status date: 2026-06-09

本文档约束 `cross_exchange_arbitrage` 永续合约跨所套利在价格精度、数量精度、合约张数、面值换算和下单字符串上的行为。后续优化必须先满足本文约束，再考虑性能、交易所覆盖面或策略收益率。

## 当前代码事实

- `rust_decimal` 当前只作为 `rustcta-smart-money` 的直接依赖使用；跨所合约套利核心和 live runner 仍主要使用 `f64`。
- `crates/rustcta-types/src/money.rs` 中 `Money`、`Price`、`Quantity` 都是 `f64` 别名，`parse_decimal` 也是先解析为 `f64`。
- `crates/rustcta-exchange-api/src/order.rs` 的下单请求中 `quantity`、`price`、`quote_quantity` 是 `String`，这是正确的交易所边界形态，不能退化成展示格式。
- `crates/rustcta-exchange-api/src/market.rs` 的 `SymbolRules` 保留 `price_increment`、`quantity_increment`、`min_quantity`、`min_notional` 等字符串字段，后续精度优化应优先消费这些原始字符串。
- `strategies/cross-exchange-arbitrage/src/core.rs` 已有 `QuantityUnit::{Base, Contracts}`、`contract_size`、`normalized_order_quantity_from_base` 和 `normalized_base_quantity`，但内部仍用 `f64` 做计算。
- live runner 已有 Gate.io 合约 `quanto_multiplier` 映射测试，证明当前系统已经遇到“交易所下单单位是张，策略风险单位是 base”的真实问题。

## 硬约束

### 1. 数量必须双账本

策略内部必须把数量分成两类字段：

- `base_quantity`：统一风险单位，例如 BTC、ETH、SPCX，跨所两腿必须按它对齐。
- `order_quantity`：交易所下单单位，可能是 base，也可能是 contracts。

任何持仓、PnL、风险敞口、净值、funding、手续费归因都必须使用 `base_quantity` 或 `notional_usdt`。任何发往交易所的请求必须使用该交易所的 `order_quantity` 字符串。

禁止用交易所返回的张数直接当 base 数量。若 `quantity_unit == Contracts`，必须通过 `contract_size` 转换：

```text
base_quantity = order_quantity * contract_size
order_quantity = base_quantity / contract_size
```

### 2. 元数据缺失必须阻断实盘开仓

永续合约实盘开仓前，两腿都必须具备并通过校验：

- `price_increment` 或等价 tick size。
- `quantity_increment` 或等价 step size。
- `min_quantity`。
- `min_notional`，若交易所不提供，必须显式记录为未知并由策略配置给出保守下限。
- `quantity_unit`。
- `contract_size`，当 `quantity_unit == Contracts` 时必须大于 0。

任何 tick、step、contract size 为 0、负数、NaN、无穷大或无法解析时，必须阻断该 symbol/venue 的实盘开仓。分析模式可以继续输出机会，但必须标记为不可执行。

### 3. 共享 base 数量先定，再分别转换下单数量

双 taker 开仓必须先计算一个共享的 `base_quantity`，再分别转换成两边交易所的 `order_quantity`。

流程约束：

1. 从目标 notional、盘口容量和两边规则得到候选 `base_quantity`。
2. 对每个 venue 将候选 base 转成 order quantity。
3. 分别按该 venue 的 quantity step 向下取整。
4. 再转回 base quantity。
5. 取两边可执行 base quantity 的较小值作为最终共享 base quantity。
6. 用最终共享 base quantity 再生成两边最终 order quantity。
7. 校验两腿最终 base quantity 残差小于硬阈值；否则阻断。

禁止两腿各自独立按 notional 下单后再假设数量一致。

### 4. 舍入方向必须按风险保守处理

数量舍入：

- 开仓数量只允许向下取整到 step，避免超出盘口容量、余额或目标风险。
- 平仓数量默认向下取整到 step；若是 reduce-only 且为清理残仓，可按交易所规则使用最小可平单位，但必须有专门测试覆盖。
- 合约张数若 step 为 `1`，最终发单数量必须是整数张字符串。

价格保护舍入：

- 买入限价保护价应向上对齐 tick，避免因 tick 舍入导致价格低于预期保护价而被动拒单。
- 卖出限价保护价应向下对齐 tick，避免因 tick 舍入导致价格高于预期保护价而被动拒单。
- 不能用展示格式小数位来替代 tick 对齐。

### 5. 下单字符串必须由规则格式化，不得由浮点展示格式决定

交易所请求中的 `quantity` 和 `price` 必须是规则化后的十进制字符串：

- 小数位来自 tick/step 的十进制尺度或交易所明确的 precision。
- 不能使用 `format!("{value}")`、`format!("{value:.12}")` 作为最终下单格式。
- 不能在最终字符串中使用科学计数法。
- 不能保留超过交易所允许的小数位。
- 不能把已经格式化过的字符串再解析回 `f64` 后二次格式化。

当前 live runner 的 `format_float` 只能作为日志/展示工具，不能作为交易所下单字符串的长期边界。

### 6. Decimal 或整数尺度是优化目标

后续优化应把精度敏感路径迁移到以下两种之一：

- `rust_decimal::Decimal`：适合读取交易所十进制字符串、做 tick/step 舍入、格式化下单字符串。
- 整数尺度模型：把 price、quantity、contract size 转成以 tick/step 为单位的整数，适合极致热路径。

迁移优先级：

1. 交易所规则解析与规则化格式化。
2. `SymbolPrecision` 和下单 draft 的数量/价格生成。
3. 双腿共享 base quantity 计算。
4. PnL、fee、funding、notional 归因。
5. Dashboard 和日志展示。

在迁移完成前，`f64` 只能作为中间估算；任何最终下单边界都必须通过规则化函数收口。

### 7. Notional 校验必须在最终数量后执行

`min_notional` 和目标 notional 校验必须基于最终舍入后的数量：

```text
notional = final_base_quantity * protected_or_reference_price
```

不能先用目标 notional 判断可下单，再在数量向下取整后跳过复验。若向下取整后低于交易所 `min_notional`，该机会必须阻断。

### 8. 成交回报必须保留单位语义

私有 WS、REST readback、fills、positions 进入策略时必须标记数量单位：

- 交易所原始成交数量。
- 原始成交数量单位：base 或 contracts。
- 转换后的 base quantity。
- 成交均价。
- 成交 notional。

one-sided、partial fill、repair、emergency close 只能使用转换后的 base quantity 做风险判断。审计日志必须同时保留原始 order quantity，方便回放交易所真实行为。

## 必测场景

后续改动触碰精度、下单、合约规则或成交归因时，必须至少覆盖：

- base 单位交易所对 base 单位交易所，step 不同。
- base 单位交易所对 contracts 单位交易所，contract size 小于 1。
- contracts 单位交易所 step 为整数张。
- 低价高数量币种，例如价格低于 `0.1 USDT` 且目标 notional 约 `5.5 USDT`。
- `min_notional` 在数量向下取整后从通过变为不通过。
- 买入保护价向上 tick 对齐，卖出保护价向下 tick 对齐。
- tick/step 为字符串 `"0.0001"`、`"1"`、`"0.01"` 时生成非科学计数法字符串。
- 私有成交回报为张数时，审计日志中的 actual base quantity 与 actual notional 正确。
- 一腿 partial fill、一腿 full fill 时，残差按 base quantity 判定并触发修复/close-only。

## 禁止清单

- 禁止在最终下单边界使用裸 `f64` 字符串格式化。
- 禁止用小数位 precision 推断 tick/step，除非交易所只提供 precision 且已在 adapter 中明确转换为 increment。
- 禁止把合约张数、base 数量、quote notional 混用在同一个 `quantity` 字段里却不记录单位。
- 禁止在跨所两腿上分别按目标 notional 独立下单。
- 禁止 metadata 缺失时实盘开仓。
- 禁止用 `f64 % step == 0.0` 判断是否满足精度。
- 禁止让 Dashboard、ClickHouse、日志展示格式反向影响交易下单格式。

## 验收门槛

一次精度优化只有同时满足以下条件，才允许进入实盘候选：

- `cargo test --all-features` 通过，且新增/更新上述必测场景。
- live runner 的 dry-run 订单日志同时输出 `base_quantity`、`order_quantity`、`quantity_unit`、`contract_size`、`price_tick`、`quantity_step`。
- 对至少一个 base 数量合约交易所和一个张数合约交易所完成回放样本验证。
- 所有阻断原因可观测，尤其是 `missing_precision_metadata`、`invalid_contract_size`、`min_notional_after_rounding`、`base_quantity_residual_exceeded`。
- 实盘开关仍遵守 `docs/cross_arb_live_runner_low_latency_zh.md` 的双开关约束。

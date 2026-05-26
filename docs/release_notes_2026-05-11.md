# 2026-05-11 对冲网格工业级风控更新

## 版本

- 版本号：`0.3.4`
- 策略范围：多交易对对冲滚动网格 `multi_hedged_grid`
- 主要配置：`config/multi_hedged_grid_usdc.yml`

## 背景

近期 ETH/USDC 对冲网格出现库存持续放大的问题。排查日志后确认，原策略为了维持“网格形状完整”，会在 `normalize`、`follow`、`reconcile` 等路径单独补 `OpenLongBuy` 或 `OpenShortSell`。这些孤儿开仓单在高波动行情中被成交后，会导致多头和空头库存同时增加，不符合固定价差对冲网格的长期稳定要求。

## 核心变更

### 1. 禁止为了网格形状单独补开仓

新增并接入以下配置开关：

```yaml
refill_open_slots_enabled: false
normalize_open_grid_enabled: false
follow_open_enabled: false
```

当前 USDC 六交易对配置均已关闭这些单独补开仓路径。策略仍保留成交驱动滚动，即成交后按对应网格逻辑提交下一组开平仓订单，但不再为了补齐形状而主动补孤儿开仓单。

### 2. 孤儿开仓单清理

新增孤儿开仓保护：如果某个开仓单没有同价位对应平仓单，策略会取消该开仓单，避免孤儿开仓继续扩大库存。

保留孤儿平仓单，因为平仓单只降低风险，不会扩大库存。

### 3. 净仓风控调整

风控逻辑调整为只按净仓限制开仓：

- ETH/USDC、BTC/USDC：净仓超过 `3000U` 后，禁止继续扩大超限方向。
- XRP/USDC、SOL/USDC、BNB/USDC、ADA/USDC：净仓超过 `1000U` 后，禁止继续扩大超限方向。

`max_total_notional` 不再作为开仓禁止条件，避免双边库存较大但净风险可控时策略被错误切成只平仓状态。

### 4. 当前 USDC 网格净仓阈值

| 交易对 | 净仓限制 |
|---|---:|
| ETH/USDC | `3000U` |
| BTC/USDC | `3000U` |
| XRP/USDC | `1000U` |
| SOL/USDC | `1000U` |
| BNB/USDC | `1000U` |
| ADA/USDC | `1000U` |

## 验证结果

已执行以下验证：

```bash
cargo fmt
cargo check --bin rustcta
cargo build --release --bin rustcta
```

重启后进程正常运行：

- 策略：`multi_hedged_grid`
- 配置：`config/multi_hedged_grid_usdc.yml`
- 日志：`logs/runtime/multi_hedged_grid_usdc_20260511_010912.out`

最新日志分析结果：

- 总成交：`391` 笔。
- 未发现 `5022`、`REJECT`、`panic`、策略崩溃。
- 未发现单独 `OpenLongBuy` / `OpenShortSell` 造成库存放大的异常。
- 异常单腿簇共 `3` 次，均为平仓单，不增加库存风险。
- ETH/USDC 净变化：多头增加约 `0.099 ETH`，空头减少约 `0.110 ETH`，符合下跌行情下减空增多的预期。

## 已知后续优化

1. Binance 用户流中的 `TRADE_LITE` 和 `ACCOUNT_UPDATE` 当前仍打印为 Unknown event，需要后续降级日志或正式解析。
2. 多交易对 REST 行情轮询偶发 HTTP/2 `too_many_internal_resets`，建议后续改为共享行情 WebSocket 或降低 REST 轮询频率。
3. 后续可进一步做 pair-level 网格状态模型，将同价位开平仓腿作为一个生命周期单元管理。

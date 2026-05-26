# 跨交易所套利 Public WS 影子实测文档

本文档用于指导跨交易所 USDT 永续套利系统的服务器实测。当前阶段只做真实行情接入和影子交易，不实际下单。

## 1. 测试目标

本阶段目标是验证：

- 多交易所公开 WebSocket 行情能否稳定连接。
- 一次订阅多个长尾 USDT 永续交易对是否正常。
- 真实 orderbook 是否进入 `MarketStateCache`。
- 真实 funding rate 是否通过 REST fallback 合并到行情缓存。
- 套利扫描是否基于真实盘口输出机会。
- 页面是否能展示当前影子持仓、历史影子仓位、PnL、交易所分析、手续费分析、资金费率分析、行情错误。
- 系统是否全程保持不下单。

当前不验证真实下单、真实成交、私有用户流实际订单回报。

## 2. 当前实现状态

已实现：

- `cross_arb_server --data-source public-ws`
- Binance USD-M combined stream 多交易对订阅。
- OKX `books5` 多 args 订阅。
- Bitget `books5` 多 args 订阅。
- Gate `futures.order_book` 多交易对订阅。
- WebSocket 自动重连循环。
- Ping heartbeat。
- WebSocket 解析错误、连接错误进入看板。
- WebSocket orderbook 写入 `MarketStateCache`。
- 看板周期性从 cache 读取盘口并扫描套利机会。
- funding rate 通过 REST 周期刷新并合并进 cache。
- `cross_arb_server` 强制 `RuntimeMode::Simulation`，不启用 live orders。

尚未作为生产完成项：

- WebSocket sequence gap 后自动 REST snapshot 恢复。
- 每个交易所独立连接健康评分和恢复统计。
- 私有用户流对账长期运行验证。
- 账户真实手续费等级接入。
- 小资金真实下单闭环。

## 3. 安全边界

服务器实测期间必须满足：

- 只运行 `cross_arb_server`。
- 使用 `--data-source public-ws`。
- 不启动 execution live worker。
- 不调用私有下单接口。
- 不配置任何自动下单进程。
- 页面按钮只控制影子新开、暂停、熔断，不会提交真实订单。

当前 `cross_arb_server` 内部会强制：

- `config.mode = RuntimeMode::Simulation`
- `config.execution.dry_run = true`
- `live_orders_enabled = false`

因此本阶段可以接真实行情，但不能视为真实交易系统已上线。

## 4. 测试前准备

服务器需要：

- 能访问 Binance、OKX、Bitget、Gate 的公开 WebSocket。
- 能访问各交易所 funding REST 接口。
- 已安装 Rust toolchain。
- 代码和配置已同步到服务器。
- 8090 端口已开放或通过 SSH tunnel 访问。

建议先确认：

```bash
scripts/cross_arb_local_gate.sh quick
```

或手动执行：

```bash
cargo check --bin cross_arb_server
cargo test --bin cross_arb_server
```

## 5. 第一阶段：20 个交易对烟测

目的：确认服务器网络、WebSocket 连接、页面展示、错误采集是否正常。

启动命令：

```bash
cargo run --bin cross_arb_server -- \
  --host 0.0.0.0 \
  --port 8090 \
  --data-source public-ws \
  --symbol-count 20 \
  --refresh-secs 3 \
  --request-timeout-ms 8000
```

页面地址：

```text
http://服务器IP:8090/
```

API 快速检查：

```bash
curl -s http://127.0.0.1:8090/api/cross-arb/dashboard | jq '{
  source: .data_source,
  symbols: .analytics.monitored_symbols,
  books: .analytics.books_loaded,
  funding: .analytics.funding_loaded,
  errors: .analytics.market_error_count,
  exchanges: .exchanges
}'
```

通过标准：

- `source` 等于 `public_ws`。
- `symbols` 等于 20。
- `books` 大于 0。
- 至少 2 个交易所出现健康盘口，才可能产生跨所机会。
- 页面“行情接口与备用路径”没有持续快速增长的同类错误。
- 页面不卡死，API 3 秒刷新正常。

如果只有 1 个交易所健康，可以继续跑网络测试，但不能验证跨所套利机会。

## 6. 第二阶段：100 个交易对实盘行情影子测试

目的：验证目标首版监控池压力下的稳定性。

启动命令：

```bash
cargo run --bin cross_arb_server -- \
  --host 0.0.0.0 \
  --port 8090 \
  --data-source public-ws \
  --symbol-count 100 \
  --refresh-secs 3 \
  --request-timeout-ms 8000
```

观察时间：

- 最少连续运行 2 小时。
- 建议首次服务器测试运行 6-12 小时。

通过标准：

- 进程无 panic。
- 内存没有持续无上限增长。
- CPU 使用率可接受。
- WebSocket 错误有记录，但不会导致全局无行情。
- `books_loaded` 稳定接近“健康交易所数量 × 有效交易对数量”。
- `funding_loaded` 能周期性刷新。
- 至少部分交易对有 2 个以上交易所盘口。
- 页面能看到机会明细、风控拒绝原因、交易所分析。

## 7. 第三阶段：影子交易结果评估

重点看以下页面模块：

- 当前影子持仓候选
- 历史影子仓位
- PnL 归因
- 交易所分析
- 手续费分析
- 资金费率分析
- 行情接口与备用路径
- 风控拒绝与本地工具

评估指标：

- 可开机会数量是否稳定，不应持续异常暴增。
- 净边际是否主要来自真实价差，而不是异常盘口。
- 资金费率影响是否方向正确。
- 手续费后净 PnL 是否仍为正。
- `NoRoute` 是否因为只有单交易所盘口。
- `StaleBook` 是否频繁出现。
- `BadOrderBook` 或交叉盘口是否频繁出现。
- 单交易所错误是否拖垮全局扫描。

## 8. 常见问题处理

### Binance/OKX/Bitget WS 连接失败

表现：

- `ws_connect_timeout`
- `Connection reset by peer`
- `tls handshake eof`

处理：

- 先确认服务器所在地区是否被交易所限制。
- 检查防火墙、DNS、代理、IPv6。
- 单独用 `scripts/public_connectivity_probe.py` 测试。
- 如果只有 Gate 可用，系统可验证 WS 链路，但不能验证完整跨所套利。

### books_loaded 为 0

原因可能是：

- 所有 WS 连接失败。
- 交易对格式不被交易所支持。
- 订阅成功但解析失败。

处理：

- 看“行情接口与备用路径”的错误。
- 降低到 `--symbol-count 5`。
- 只启用单个交易所做定位。
- 检查 adapter 解析测试是否仍通过。

### funding_loaded 为 0

原因可能是：

- funding REST 被服务器网络阻断。
- 交易对不存在或格式不匹配。
- 请求超时。

处理：

- 增大 `--request-timeout-ms 12000`。
- 先验证 Gate 或其他可通交易所。
- 检查页面 funding 错误。

## 9. 禁止事项

本阶段禁止：

- 启动真实下单 worker。
- 打开 live order execution。
- 使用真实 API key 做自动下单。
- 在未完成 preflight 前改 `dry_run=false`。
- 因页面有正 PnL 就直接上线真实下单。

## 10. 进入小资金真实下单前门槛

必须全部完成：

- `public-ws` 影子交易连续运行至少 24 小时。
- 4 个交易所中至少 2 个核心交易所稳定。
- sequence gap 自动 REST snapshot 恢复完成。
- 每交易所健康度和重连统计完成。
- 私有用户流对账完成并长期运行稳定。
- 真实账户手续费等级接入。
- funding settlement 持久化验证通过。
- live preflight 通过。
- 持仓模式、杠杆、精度、最小下单量全部校验通过。
- kill switch、close only、自动恢复流程演练通过。

## 11. 推荐记录模板

每次服务器测试记录：

```text
测试日期：
服务器地区：
启动命令：
symbol_count：
运行时长：

Binance WS：
OKX WS：
Bitget WS：
Gate WS：

books_loaded：
funding_loaded：
market_error_count：
openable_opportunities：
shadow_trades：
net_pnl_usdt：

主要错误：
是否出现 StaleBook：
是否出现 BadOrderBook：
是否出现可疑超大价差：

结论：
下一步：
```

## 12. 当前推荐命令

服务器首次实测：

```bash
cargo run --bin cross_arb_server -- \
  --host 0.0.0.0 \
  --port 8090 \
  --data-source public-ws \
  --symbol-count 20 \
  --refresh-secs 3 \
  --request-timeout-ms 8000
```

稳定后扩大：

```bash
cargo run --bin cross_arb_server -- \
  --host 0.0.0.0 \
  --port 8090 \
  --data-source public-ws \
  --symbol-count 100 \
  --refresh-secs 3 \
  --request-timeout-ms 8000
```

结论：当前可以开始服务器实盘行情接入和影子交易测试，但不能进入真实下单阶段。

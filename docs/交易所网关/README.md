# 交易所网关文档

状态日期：2026-06-08

这个目录只放交易所网关相关文档。后续补交易所接口、查 adapter 进度、看能力矩阵，都从这里开始。

## 怎么找文档

| 你要做什么 | 先看 |
| --- | --- |
| 给一个交易所补完整接口文档 | [接口盘点维度](接口盘点维度.md)，再复制 [交易所接口补全文档模板](交易所接口补全文档模板.md)。WebSocket 行情必须填推流间隔、档位和订阅方式。 |
| 看某个交易所现在接了哪些能力 | `适配器/<adapter>_adapter.md`，例如 [Binance Spot](适配器/binance_adapter.md)。 |
| 看不懂英文 adapter 文件名 | [适配器索引](适配器索引.md)。 |
| 看全局完成度和 Binance 对照目标 | [总览/exchange_api_completion_matrix.md](总览/exchange_api_completion_matrix.md)。 |
| 看网关支持哪些交易所 | [总览/exchange_support_matrix.md](总览/exchange_support_matrix.md)。 |
| 看 endpoint mapping 和 request/spec 工具链要求 | [总览/exchange_adapter_toolchain_completion_zh.md](总览/exchange_adapter_toolchain_completion_zh.md)。 |
| 看通用安全、对账、费率、symbol、WS 规则 | `通用机制/` 下的文档。 |

## 目录结构

```text
docs/交易所网关/
  README.md                    中文入口
  接口盘点维度.md              每个交易所按什么维度盘点
  交易所接口补全文档模板.md    新交易所文档模板
  适配器索引.md                英文 adapter 文件名的中文索引
  总览/                        全局矩阵、扩展计划、架构说明
  通用机制/                    密钥、订单 id、费率、对账、symbol、WS
  适配器/                      每个交易所一份 adapter 文档
```

## 命名规则

适配器文档暂时保留英文 adapter id 文件名，例如 `binance_adapter.md`、`okx_adapter.md`、`hyperliquid_adapter.md`。原因是这些名字和代码目录、配置、测试 fixture 一一对应：

```text
docs/交易所网关/适配器/binance_adapter.md
crates/rustcta-exchange-gateway/src/adapters/binance/
tests/fixtures/exchanges/binance/
config/binance_spot_example.yml
```

如果改成纯中文文件名，后续 AI 或开发者反而容易找错代码目录。中文识别问题由本 README、模板和目录分组解决。

## 总览文档

| 文档 | 用途 |
| --- | --- |
| [exchange_abstraction.md](总览/exchange_abstraction.md) | 交易所抽象和统一接口边界。 |
| [exchange_adapter_interface_status.md](总览/exchange_adapter_interface_status.md) | adapter 目录布局和当前接口状态。 |
| [exchange_api_completion_matrix.md](总览/exchange_api_completion_matrix.md) | 代码优先的能力完成矩阵。 |
| [exchange_support_matrix.md](总览/exchange_support_matrix.md) | 交易所支持列表和分类。 |
| [spot_exchange_adapters.md](总览/spot_exchange_adapters.md) | Spot adapter 架构说明。 |
| [exchange_adapter_toolchain_completion_zh.md](总览/exchange_adapter_toolchain_completion_zh.md) | endpoint mapping、request spec、stream、reconciliation 工具链说明。 |
| [exchange_adapter_toolchain_status_zh.md](总览/exchange_adapter_toolchain_status_zh.md) | 工具链当前状态。 |
| [exchange_gateway_expansion_30_venues_zh.md](总览/exchange_gateway_expansion_30_venues_zh.md) | 30 个交易所扩展计划。 |
| [exchange_gateway_next_40_parallel_tasks_zh.md](总览/exchange_gateway_next_40_parallel_tasks_zh.md) | 40 个交易所并行扩展收尾记录。 |
| [exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md](总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md) | 剩余交易所一 AI 一交易所任务拆分。 |
| [hyperliquid_api.md](总览/hyperliquid_api.md) | Hyperliquid API 和策略集成说明。 |

## 通用机制

| 文档 | 用途 |
| --- | --- |
| [api_key_security.md](通用机制/api_key_security.md) | API key 权限、安全边界和只读校验。 |
| [client_order_id_policy.md](通用机制/client_order_id_policy.md) | client order id 生成、限制和幂等策略。 |
| [fee_model.md](通用机制/fee_model.md) | fee source 优先级和 fallback。 |
| [order_reconciliation.md](通用机制/order_reconciliation.md) | 下单/撤单后 REST 对账和未知状态处理。 |
| [symbol_management.md](通用机制/symbol_management.md) | 内部 symbol 与交易所 symbol 映射。 |
| [websocket_market_data.md](通用机制/websocket_market_data.md) | WebSocket 行情和 book-cache 路径。 |

## 补文档顺序

1. 先打开 [接口盘点维度](接口盘点维度.md)，确定交易所产品线，不存在的产品线统一写 `交易所不支持`。
2. 复制 [交易所接口补全文档模板](交易所接口补全文档模板.md) 到 `适配器/<adapter>_adapter.md` 或补齐已有文件。
3. 同步更新 `crates/rustcta-exchange-gateway/src/adapters/<adapter>/endpoint_mapping.yaml`。
4. 公共 WebSocket 行情必须补订阅方式、推流间隔、订单簿档位、sequence/checksum、snapshot 重建规则。
5. 为每个声明支持的能力补 request spec、parser fixture、signing vector 或 WS fixture。
6. 跑 endpoint mapping 校验和 adapter 定向测试。

## 验证命令

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/binance/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway binance --lib --message-format short
```

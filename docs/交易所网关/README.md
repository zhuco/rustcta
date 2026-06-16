# 交易所网关文档

状态日期：2026-06-08

这个目录只放交易所网关相关文档。后续补交易所接口、查 adapter 进度、看能力矩阵，都从这里开始。

## 怎么找文档

| 你要做什么 | 先看 |
| --- | --- |
| 给一个交易所补完整接口文档 | [接口盘点维度](接口盘点维度.md)，再复制 [交易所接口补全文档模板](交易所接口补全文档模板.md)。WebSocket 行情必须填推流间隔、档位和订阅方式。 |
| 看某个交易所现在接了哪些能力 | `适配器/<adapter>_adapter.md`，例如 [Binance Spot](适配器/binance_adapter.md)。 |
| 看当前项目 125 个 adapter 已声明/实现了哪些功能 | [交易所功能盘点矩阵](交易所功能盘点矩阵.md)，机器可读版见 [CSV](交易所功能盘点矩阵.csv)。 |
| 按 adapter 看当前证据、任务和剩余核验项 | [adapter 工作包索引](adapter工作包索引.md)，机器可读版见 [CSV](adapter工作包索引.csv)。 |
| 看下一步具体要补哪些 adapter | [交易所网关补全任务清单](交易所网关补全任务清单.md)，机器可读版见 [CSV](交易所网关补全任务清单.csv)。 |
| 看还剩哪些官方资料没查完 | [剩余官方核验队列](剩余官方核验队列.md)，机器可读版见 [CSV](剩余官方核验队列.csv)。 |
| 看已经查过官方资料的能力结论 | [官方能力核验台账](官方能力核验台账.md)。 |
| 看主流交易所是否官方支持现货/合约/期权 | [产品线官方核验 P0 主流交易所](产品线官方核验_P0_主流交易所.md)。 |
| 看链上永续/Orderly profile 是否有现货 | [产品线官方核验 P1 链上合约交易所](产品线官方核验_P1_链上合约交易所.md)。 |
| 看衍生品交易所是否已有现货线索 | [产品线官方核验 P2 衍生品交易所](产品线官方核验_P2_衍生品交易所.md)。 |
| 看 CEX 现货 adapter 是否应补合约 | [产品线官方核验 P3 CEX 合约边界](产品线官方核验_P3_CEX合约边界.md)。 |
| 看区域现货交易所是否应补合约或保证金 | [产品线官方核验 P4 区域现货交易所](产品线官方核验_P4_区域现货交易所.md)。 |
| 看区域现货 CEX 第二批是否应补 margin/perps 或写不支持合约 | [产品线官方核验 P5 区域现货 CEX 第二批](产品线官方核验_P5_区域现货_CEX第二批.md)。 |
| 看剩余区域现货 CEX 是否应补合约或写不支持合约 | [产品线官方核验 P6 剩余区域现货 CEX](产品线官方核验_P6_剩余区域现货_CEX.md)。 |
| 看下一步应该优先查哪些官方资料 | [官方核验优先级](官方核验优先级.md)。 |
| 看哪些交易所有 10ms/20ms L1/BBO 或高速订单簿 | [WebSocket 极速盘口能力汇总](WebSocket极速盘口能力汇总.md)。 |
| 看主流和区域现货 WebSocket 官方结果 | [WebSocket 官方核验 P0 第一批](WebSocket官方核验_P0_第一批.md)，[第二批](WebSocket官方核验_P0_第二批.md)，[P1 第三批](WebSocket官方核验_P1_第三批.md)，[P2 区域现货](WebSocket官方核验_P2_区域现货交易所.md)，[P3 P2 公共 WS 缺口](WebSocket官方核验_P3_P2公共WS缺口交易所.md)，[P4 CEX 盘口细项](WebSocket官方核验_P4_CEX盘口细项.md)，[P5 衍生品/链上盘口细项](WebSocket官方核验_P5_衍生品链上盘口细项.md)，[P6 补充交易所盘口细项](WebSocket官方核验_P6_补充交易所盘口细项.md)，[P7 补充交易所盘口细项二](WebSocket官方核验_P7_补充交易所盘口细项二.md)，[P8 补充交易所盘口细项三](WebSocket官方核验_P8_补充交易所盘口细项三.md)，[P9 剩余交易所盘口细项](WebSocket官方核验_P9_剩余交易所盘口细项.md)。 |
| 看下单/撤单核心交易接口官方结果 | [核心交易官方核验 P0 第一批](核心交易官方核验_P0_第一批.md)，[P1 第二批](核心交易官方核验_P1_第二批.md)，[P2 第三批](核心交易官方核验_P2_第三批.md)，[P3 第四批](核心交易官方核验_P3_第四批.md)。 |
| 看仓位/保证金接口官方结果 | [仓位接口官方核验 P0 第一批](仓位接口官方核验_P0_第一批.md)，[P1 第二批](仓位接口官方核验_P1_第二批.md)。 |
| 看账户/余额接口官方结果 | [账户接口官方核验 P0 第一批](账户接口官方核验_P0_第一批.md)，[P1 第二批](账户接口官方核验_P1_第二批.md)。 |
| 看费率接口官方结果 | [费率接口官方核验 P0 第一批](费率接口官方核验_P0_第一批.md)，[P1 第二批](费率接口官方核验_P1_第二批.md)，[P2 第三批](费率接口官方核验_P2_第三批.md)。 |
| 看批量/改单/OCO 等高级订单官方结果 | [高级订单能力官方核验 P0 第一批](高级订单能力官方核验_P0_第一批.md)，[P1 第二批](高级订单能力官方核验_P1_第二批.md)，[P2 第三批](高级订单能力官方核验_P2_第三批.md)，[P3 第四批](高级订单能力官方核验_P3_第四批.md)，[P4 第五批](高级订单能力官方核验_P4_第五批.md)，[P5 第六批](高级订单能力官方核验_P5_第六批.md)，[P6 第七批](高级订单能力官方核验_P6_第七批.md)。 |
| 看不懂英文 adapter 文件名 | [适配器索引](适配器索引.md)。 |
| 看全局完成度和 Binance 对照目标 | [总览/exchange_api_completion_matrix.md](总览/exchange_api_completion_matrix.md)。 |
| 看网关支持哪些交易所 | [总览/exchange_support_matrix.md](总览/exchange_support_matrix.md)。 |
| 看 OKX/Bybit/MEXC/KuCoin Futures 跨所现货、合约、资金费率 API 收口 | [总览/exchange_adapter_interface_status.md](总览/exchange_adapter_interface_status.md) 的 `2026-06-10 Cross-Arb Gateway Closure`。 |
| 看 endpoint mapping 和 request/spec 工具链要求 | [总览/exchange_adapter_toolchain_completion_zh.md](总览/exchange_adapter_toolchain_completion_zh.md)。 |
| 看通用安全、对账、费率、symbol、WS 规则 | `通用机制/` 下的文档。 |

## 目录结构

```text
docs/交易所网关/
  README.md                    中文入口
  接口盘点维度.md              每个交易所按什么维度盘点
  交易所接口补全文档模板.md    新交易所文档模板
  适配器索引.md                英文 adapter 文件名的中文索引
  交易所功能盘点矩阵.md        当前项目声明/实现能力矩阵
  交易所功能盘点矩阵.csv       机器可读能力矩阵
  adapter工作包索引.md         每个 adapter 的证据/任务/核验工作包
  adapter工作包索引.csv        机器可读 adapter 工作包
  交易所网关补全任务清单.md    下一步 adapter 补全任务
  交易所网关补全任务清单.csv   机器可读任务清单
  剩余官方核验队列.md          尚未查完的官方资料队列
  剩余官方核验队列.csv         机器可读剩余核验队列
  官方能力核验台账.md          已查官方资料的能力结论
  产品线官方核验_P0_主流交易所.md  主流交易所产品线官方支持
  产品线官方核验_P1_链上合约交易所.md  链上永续/Orderly profile 产品线边界
  产品线官方核验_P2_衍生品交易所.md  衍生品交易所 Spot 边界
  产品线官方核验_P3_CEX合约边界.md  CEX 现货 adapter 的合约支持边界
  产品线官方核验_P4_区域现货交易所.md  区域现货交易所的合约/保证金边界
  产品线官方核验_P5_区域现货_CEX第二批.md  区域现货 CEX 第二批产品线边界
  产品线官方核验_P6_剩余区域现货_CEX.md  剩余区域现货 CEX 产品线边界
  官方核验优先级.md            下一步官方文档核验顺序
  WebSocket极速盘口能力汇总.md  10ms/20ms L1/BBO 和高速订单簿汇总
  WebSocket官方核验_P0_第一批.md  主流交易所订单簿 WS 官方核验
  WebSocket官方核验_P0_第二批.md  第二批订单簿 WS 官方核验
  WebSocket官方核验_P1_第三批.md  已有 WS 证据交易所的低延迟细项
  WebSocket官方核验_P2_区域现货交易所.md  区域现货交易所订单簿 WS 细项
  WebSocket官方核验_P3_P2公共WS缺口交易所.md  P2 公共 WS 缺口交易所订单簿 WS 细项
  WebSocket官方核验_P4_CEX盘口细项.md  CEX 订单簿 WS 细项
  WebSocket官方核验_P5_衍生品链上盘口细项.md  衍生品/链上订单簿 WS 细项
  WebSocket官方核验_P6_补充交易所盘口细项.md  补充交易所订单簿 WS 细项
  WebSocket官方核验_P7_补充交易所盘口细项二.md  第二批补充交易所订单簿 WS 细项
  WebSocket官方核验_P8_补充交易所盘口细项三.md  第三批补充交易所订单簿 WS 细项
  WebSocket官方核验_P9_剩余交易所盘口细项.md  剩余交易所订单簿 WS 细项
  核心交易官方核验_P0_第一批.md  下单/撤单核心交易接口官方核验
  核心交易官方核验_P1_第二批.md  下单/撤单核心交易接口官方核验第二批
  核心交易官方核验_P2_第三批.md  下单/撤单核心交易接口官方核验第三批
  核心交易官方核验_P3_第四批.md  下单/撤单核心交易接口官方核验第四批
  仓位接口官方核验_P0_第一批.md  仓位/保证金接口官方核验第一批
  仓位接口官方核验_P1_第二批.md  仓位/保证金接口官方核验第二批
  账户接口官方核验_P0_第一批.md  账户/余额接口官方核验第一批
  账户接口官方核验_P1_第二批.md  账户/余额接口官方核验第二批
  费率接口官方核验_P0_第一批.md  费率接口官方核验第一批
  费率接口官方核验_P1_第二批.md  费率接口官方核验第二批
  费率接口官方核验_P2_第三批.md  费率接口官方核验第三批
  高级订单能力官方核验_P0_第一批.md  批量/改单/OCO 等高级订单能力第一批
  高级订单能力官方核验_P1_第二批.md  批量/改单/OCO 等高级订单能力第二批
  高级订单能力官方核验_P2_第三批.md  批量/改单/OCO 等高级订单能力第三批
  高级订单能力官方核验_P3_第四批.md  批量/改单/OCO 等高级订单能力第四批
  高级订单能力官方核验_P4_第五批.md  批量/改单/OCO 等高级订单能力第五批
  高级订单能力官方核验_P5_第六批.md  批量/改单/OCO 等高级订单能力第六批
  高级订单能力官方核验_P6_第七批.md  批量/改单/OCO 等高级订单能力第七批
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

## 当前盘点结论

自动矩阵当前覆盖 125 个 adapter endpoint mapping。它区分 `运行`、`原生`、`映射`、`离线`、`不支持` 和缺失状态；未声明产品线只代表项目当前没有证据，不直接等于交易所官方不支持。已核验过的官方结论记录在 [官方能力核验台账](官方能力核验台账.md)。

第一批主流产品线核验已经确认：Binance USD-M/Options、OKX Swap/Futures/Options、Bitget Futures、Gate.io Futures/Options、MEXC Contract 是 `项目未实现`，不是交易所不支持；`binancecoinm`/`kucoinfutures`/`krakenfutures` 的现货、`coinbaseexchange` 的合约按 adapter 口径写 `交易所不支持`。

当前可执行补全任务已经整理到 [交易所网关补全任务清单](交易所网关补全任务清单.md)：P0 公共订单簿 WebSocket 13 项，P0 产品线项目未实现 6 项，P1 已有 WS 结构化细项 9 项，P2 产品线项目未实现 35 项，P2 明确交易所不支持 56 项，P2 公共订单簿 WebSocket 5 项，P2 已有 WS 的结构化细项 1 项，P2 明确公共 WS 不支持 14 项，P2 核心交易项目未实现/未启用 29 项，P2 核心交易当前交易所不支持 5 项，P2 仓位项目未实现/未启用 10 项，P2 仓位当前交易所不支持 7 项，P3 账户/余额项目未实现/未启用 25 项，P3 账户/余额当前 profile 不支持 7 项，P3 费率项目未实现/未启用 40 项，P3 费率当前 profile 不支持 34 项，P3 区域现货 WS 结构化细项 10 项，P3 CEX WS 结构化细项 12 项，P3 衍生品/链上 WS 结构化细项 12 项，P3 补充交易所 WS 结构化细项 11 项，P3 补充交易所 WS 结构化细项二 11 项，P3 补充交易所 WS 结构化细项三 11 项，P3 剩余交易所 WS 结构化细项 14 项，P4 高级订单能力项目未实现/未启用 70 项，P4 高级订单能力当前不支持 53 项，共 500 项。尚未转成明确任务的官方资料缺口见 [剩余官方核验队列](剩余官方核验队列.md)，当前还有 0 项；产品线、公共 WS 细项、核心交易、仓位、账户/余额、费率和高级订单能力官方核验队列均已清零。

按 adapter 聚合的开工入口见 [adapter 工作包索引](adapter工作包索引.md)，它会把当前实现、明确任务和剩余核验项合并到一行。

跨所套利优先看 WebSocket 行情缺口：公共 WS、订单簿 channel、推流间隔、档位、sequence/checksum、snapshot 重建规则。10ms/20ms L1/BBO 和 50ms batch 属于单独优先级，详见 [WebSocket 极速盘口能力汇总](WebSocket极速盘口能力汇总.md)。只有查过官方文档后，才能把产品线缺口写成 `交易所不支持现货` 或 `交易所不支持合约`。

## 通用机制

| 文档 | 用途 |
| --- | --- |
| [api_key_security.md](通用机制/api_key_security.md) | API key 权限、安全边界和只读校验。 |
| [client_order_id_policy.md](通用机制/client_order_id_policy.md) | client order id 生成、限制和幂等策略。 |
| [fee_model.md](通用机制/fee_model.md) | fee source 优先级和 fallback。 |
| [market_type_contract.md](通用机制/market_type_contract.md) | Spot、Perpetual、Futures 等市场类型的内部规范和外部命名兼容边界。 |
| [order_reconciliation.md](通用机制/order_reconciliation.md) | 下单/撤单后 REST 对账和未知状态处理。 |
| [symbol_management.md](通用机制/symbol_management.md) | 内部 symbol 与交易所 symbol 映射。 |
| [websocket_market_data.md](通用机制/websocket_market_data.md) | WebSocket 行情和 book-cache 路径。 |

## 补文档顺序

1. 先打开 [接口盘点维度](接口盘点维度.md)，确定交易所产品线，不存在的产品线统一写 `交易所不支持`。
2. 复制 [交易所接口补全文档模板](交易所接口补全文档模板.md) 到 `适配器/<adapter>_adapter.md` 或补齐已有文件。
3. 同步更新 `crates/rustcta-exchange-gateway/src/adapters/<adapter>/endpoint_mapping.yaml`。
4. 公共 WebSocket 行情必须补订阅方式、推流间隔、订单簿档位、sequence/checksum、snapshot 重建规则。
5. 为每个声明支持的能力补 request spec、parser fixture、signing vector 或 WS fixture。
6. 重新生成 [交易所功能盘点矩阵](交易所功能盘点矩阵.md)。
7. 跑 endpoint mapping 校验和 adapter 定向测试。

## 验证命令

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/binance/endpoint_mapping.yaml
python3 scripts/generate_exchange_gateway_matrix.py
python3 scripts/generate_exchange_gateway_audit_queue.py
python3 scripts/generate_exchange_gateway_adapter_workpacks.py
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway binance --lib --message-format short
```

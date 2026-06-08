# Adapter 工作包索引

状态日期：2026-06-08

本文件由 `scripts/generate_exchange_gateway_adapter_workpacks.py` 根据 [交易所功能盘点矩阵](交易所功能盘点矩阵.md)、[交易所网关补全任务清单](交易所网关补全任务清单.md) 和 [剩余官方核验队列](剩余官方核验队列.md) 生成。

机器可读版本见 [adapter工作包索引.csv](adapter工作包索引.csv)。

## 汇总

| 项目 | 数量 |
| --- | ---: |
| adapter 总数 | 125 |
| 已有明确补全任务的 adapter | 119 |
| 仍有剩余核验项的 adapter | 124 |
| workload_score >= 100 的 adapter | 27 |

## 排序口径

`workload_score` 用来粗排工作量和优先级：P0 任务权重 100，P1 40，P2 25，P3 10，P4 3。它不是工期估算，只用于挑下一批 adapter。

## 最高优先级工作包

| adapter | 产品线 | 核心接口 | 公共 WS | WS延迟等级 | 明确任务 | 剩余核验 | score | 下一步 |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- |
| binance | spot | 8/8 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 3 | 1 | 303 | 先做已确认 P0 公共订单簿 WS 补全 |
| bitget | spot | 8/8 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 2 | 1 | 203 | 先做已确认 P0 公共订单簿 WS 补全 |
| gateio | spot | 8/8 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 2 | 1 | 203 | 先做已确认 P0 公共订单簿 WS 补全 |
| mexc | spot | 8/8 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 2 | 1 | 203 | 先做已确认 P0 公共订单簿 WS 补全 |
| okx | spot | 8/8 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 2 | 1 | 203 | 先做已确认 P0 公共订单簿 WS 补全 |
| huobi | perpetual,spot | 8/9 | declared/缺推流间隔；缺档位 | 缺推流间隔证据 | 1 | 3 | 138 | 先做已确认 P0 公共订单簿 WS 补全 |
| kucoin | spot | 8/8 | native/缺订单簿channel；缺推流间隔 | 缺推流间隔证据 | 2 | 2 | 138 | 先做已确认 P0 公共订单簿 WS 补全 |
| binancecoinm | futures,perpetual | 9/9 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 2 | 1 | 128 | 先做已确认 P0 公共订单簿 WS 补全 |
| binanceus | spot | 8/8 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 2 | 1 | 128 | 先做已确认 P0 公共订单簿 WS 补全 |
| cod3x | perpetual | 0/9 | 未声明/公共WS未声明 | 公共WS未接入/未声明 | 2 | 5 | 123 | 先查官方下单/撤单接口 |
| d8x | perpetual | 2/9 | spec_only/缺推流间隔；缺档位 | 缺推流间隔证据 | 2 | 5 | 123 | 先查官方下单/撤单接口 |
| derive_chain_perps | perpetual | 0/9 | 未声明/公共WS未声明 | 公共WS未接入/未声明 | 2 | 5 | 123 | 先查官方下单/撤单接口 |
| equation | perpetual | 0/9 | 未声明/公共WS未声明 | 公共WS未接入/未声明 | 2 | 5 | 123 | 先查官方下单/撤单接口 |
| zeta_markets | perpetual | 2/9 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 2 | 5 | 123 | 先查官方下单/撤单接口 |
| bybit | futures,perpetual,spot | 9/9 | spec_only/缺推流间隔 | 缺推流间隔证据 | 1 | 2 | 113 | 先做已确认 P0 公共订单簿 WS 补全 |
| aark | perpetual | 1/9 | spec_only/缺档位 | 慢速盘口/需评估 | 2 | 5 | 108 | 先查官方下单/撤单接口 |
| aftermath | perpetual | 2/9 | spec_only/缺订单簿channel；缺推流间隔；缺档位 | 缺推流间隔证据 | 2 | 5 | 108 | 先查官方下单/撤单接口 |
| bitbns | spot | 2/8 | spec_only/缺订单簿channel；缺推流间隔；缺档位 | 缺推流间隔证据 | 3 | 4 | 108 | 先查官方下单/撤单接口 |
| bsx | perpetual | 2/9 | spec_only/缺档位 | 百毫秒盘口候选 | 2 | 5 | 108 | 先查官方下单/撤单接口 |
| grvt | option,perpetual | 0/9 | parser_only/缺推流间隔；缺档位 | 缺推流间隔证据 | 1 | 6 | 108 | 先查官方下单/撤单接口 |
| hibachi | perpetual | 2/9 | parser_only/缺推流间隔；缺档位 | 缺推流间隔证据 | 1 | 6 | 108 | 先查官方下单/撤单接口 |
| lighter | perpetual | 0/9 | parser_only/缺推流间隔；缺档位 | 缺推流间隔证据 | 2 | 5 | 108 | 先查官方下单/撤单接口 |
| mango_markets | margin,perpetual | 0/9 | spec_only/缺订单簿channel；缺推流间隔；缺档位 | 缺推流间隔证据 | 1 | 6 | 108 | 先查官方下单/撤单接口 |
| modetrade | perpetual | 1/9 | spec_only/缺档位 | 慢速盘口/需评估 | 2 | 5 | 108 | 先查官方下单/撤单接口 |
| coinw | perpetual,spot | 9/9 | parser_only/缺订单簿channel；缺推流间隔 | 缺推流间隔证据 | 1 | 1 | 103 | 先做已确认 P0 公共订单簿 WS 补全 |
| htx | perpetual,spot,spot | linear_perp | 9/9 | declared/缺推流间隔；缺档位 | 缺推流间隔证据 | 1 | 1 | 103 | 先做已确认 P0 公共订单簿 WS 补全 |
| lbank | perpetual,spot | 9/9 | native/缺推流间隔 | 缺推流间隔证据 | 1 | 1 | 103 | 先做已确认 P0 公共订单簿 WS 补全 |
| ascendex | perpetual,spot | 0/9 | 未声明/公共WS未声明 | 公共WS未接入/未声明 | 1 | 5 | 98 | 先查官方下单/撤单接口 |
| bitkan | perpetual,spot | 0/9 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 1 | 5 | 98 | 先查官方下单/撤单接口 |
| bitteam | spot | 2/8 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 2 | 4 | 98 | 先查官方下单/撤单接口 |
| cex | spot | 2/8 | 未声明/公共WS未声明 | 公共WS未接入/未声明 | 2 | 4 | 98 | 先查官方下单/撤单接口 |
| latoken | spot | 2/8 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 2 | 4 | 98 | 先查官方下单/撤单接口 |
| p2b | spot | 2/8 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 2 | 4 | 98 | 先查官方下单/撤单接口 |
| paymium | spot | 2/8 | 未声明/公共WS未声明 | 公共WS未接入/未声明 | 2 | 4 | 98 | 先查官方下单/撤单接口 |
| yobit | spot | 2/8 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 2 | 4 | 98 | 先查官方下单/撤单接口 |
| zebpay | spot | 2/8 | unsupported/公共WS不支持/未接入 | 公共WS未接入/未声明 | 2 | 4 | 98 | 先查官方下单/撤单接口 |
| indodax | spot | 7/8 | 未声明/公共WS未声明 | 公共WS未接入/未声明 | 2 | 3 | 88 | 先查官方下单/撤单接口 |
| bybiteu | futures,perpetual,spot | 2/9 | spec_only/已记录核心细项 | 极速L1候选 | 1 | 5 | 83 | 先查官方下单/撤单接口 |
| fmfwio | spot | 2/8 | spec_only/缺订单簿channel | 慢速盘口/需评估 | 1 | 5 | 83 | 先查官方下单/撤单接口 |
| hitbtc | spot | 2/8 | spec_only/缺订单簿channel | 慢速盘口/需评估 | 1 | 5 | 83 | 先查官方下单/撤单接口 |

## 使用方式

1. 从本表顶部选择一个 adapter。
2. 打开对应 `doc` 和 `mapping`，确认当前项目证据。
3. 如果 `confirmed_tasks > 0`，直接按 [交易所网关补全任务清单](交易所网关补全任务清单.md) 实现。
4. 如果只有 `remaining_checks`，先查官方资料，把结论转入任务清单或写 `交易所不支持`。
5. 修改 `endpoint_mapping.yaml` 后重新运行矩阵、剩余队列和本脚本。

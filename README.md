# RustCTA

RustCTA 是一个以 Rust 编写的多交易所 CTA 交易框架，提供统一交易接口、账户管理、风险控制、日志告警与多策略运行能力。

## 版本信息
- 当前版本：`0.3.2`
- 发布日期：`2026-02-26`
- 上一版本：`0.3.1`

## 本次更新（v0.3.2）
1. 版本号从 `0.3.1` 升级到 `0.3.2`。
2. 重新整理 `README.md`，补齐“支持的交易所功能”和“内置量化策略”清单。
3. 文档内容与当前代码入口对齐，明确“已实现”与“预留未启用”边界。

## 支持的交易所与功能

### 已实现并可在运行时创建的交易所适配器
`AccountManager` 当前可直接创建以下交易所实例：
- `binance`
- `okx`
- `bitmart`
- `hyperliquid`

### 交易所能力矩阵（按当前代码状态）
| 交易所 | 现货 | 合约/永续 | REST 交易能力 | WebSocket 行情 | 私有用户流 | 说明 |
|---|---|---|---|---|---|---|
| Binance | 支持 | 支持 | 支持（下单/撤单/查询/批量） | 支持 | 支持（ListenKey + keepalive） | 当前最完整接入 |
| OKX | 支持 | 支持（SWAP） | 支持 | 支持 | 未在统一接口中启用 | 可用于多策略行情与交易 |
| Bitmart | 支持 | 支持 | 支持 | 支持 | 未在统一接口中启用 | 部分接口按现货/合约分域名处理 |
| Hyperliquid | 不支持 | 支持（永续） | 支持（永续交易） | 支持 | 不适用（无 ListenKey 模型） | 当前实现为单向持仓模型 |

### 预留但当前未启用的交易所
以下名称在配置/符号转换中有预留，但交易所模块在 `src/exchanges/mod.rs` 中处于注释禁用状态：
- `bybit`
- `htx`
- `meteora`（DEX，待修复）

## 内置量化策略（CLI 可启动）
主程序通过 `--strategy` 选择策略，当前支持以下入口：

| CLI 参数 | 策略模块 | 类型 | 说明 |
|---|---|---|---|
| `trend_intraday` | `strategies/trend` | 日内趋势 | 趋势判定 + 风控 + 执行引擎 |
| `trend_grid` | `strategies/trend_grid_v2` | 趋势网格 | 趋势导向网格（V2 实现） |
| `hedged_grid` | `strategies/hedged_grid` | 对冲网格 | 多标的/单标的对冲网格运行 |
| `solusdc_hedged_grid` | `strategies/solusdc_hedged_grid` | 专项对冲网格 | SOLUSDC 场景化滚动网格 |
| `range_grid` | `strategies/range_grid` | 区间网格 | 震荡区间网格与风险阈值控制 |
| `mean_reversion` | `strategies/mean_reversion` | 均值回归 | 因子/偏离度驱动的回归交易 |
| `poisson` | `strategies/poisson_market_maker` | 做市 | 泊松分布建模的做市策略 |
| `as` | `strategies/automated_scalping` | 高频剥头皮 | 自动化剥头皮执行策略 |
| `copy_trading` | `strategies/copy_trading` | 跟单 | 主账户信号到子账户复制 |
| `avellaneda_stoikov` | `strategies/avellaneda_stoikov` | 做市 | Avellaneda-Stoikov 模型 |
| `market_making` | `strategies/market_making` | 做市 | 专业做市引擎 |
| `grid_scale` | `strategies/grid_scale` | 网格增强 | 分层/扩展型网格策略 |
| `orderflow` | `strategies/orderflow` | 订单流 | 基于订单流行为的交易策略 |

## 快速开始

### 环境准备
- Rust `1.75+`
- `cargo`、`git`
- 交易所 API Key 通过环境变量注入（参考 `config/accounts.yml` 的 `api_key_env`）

### 拉取与构建
```bash
git clone https://github.com/zhuco/rustcta.git
cd rustcta
cp .env.example .env

cargo check
cargo fmt
cargo clippy --all-targets --all-features
cargo test --all-features
```

### 启动策略
```bash
cargo run -- --strategy trend_grid --config config/trend_grid_ena.yml
```

Release 模式：
```bash
cargo build --release
nohup target/release/rustcta --strategy poisson --config config/poisson_near_usdc.yml \
  > logs/poisson_near.out 2>&1 &
```

## 关键配置文件
- `config/config.yaml`：交易所端点与全局基础配置。
- `config/accounts.yml`：账户与 `api_key_env` 映射。
- `config/global.yml`：Webhook/通知相关参数。
- `config/logging.yml`：日志等级与输出策略。
- `config/*.yml`：各策略独立参数文件。
- `config/strategies.yml`：多策略编排示例。

## 项目结构
```text
src/
  core/           # 基础类型、错误、风控、通信抽象
  exchanges/      # Binance/OKX/Bitmart/Hyperliquid 实现
  cta/            # 账户管理与策略运行期组件
  strategies/     # 各量化策略与公共组件
  utils/          # 日志、时间同步、符号转换、通知工具
config/           # 配置模板
docs/             # 设计与策略文档
tests/            # 集成测试
logs/             # 运行日志
```

## 开发建议
1. 新增策略时在 `src/strategies/` 建模块，并在 `src/strategies/mod.rs` 导出。
2. 优先复用 `StrategyDepsBuilder`、统一风控与 `AccountManager`。
3. 提交前执行：`cargo fmt && cargo clippy --all-targets --all-features && cargo test --all-features`。
4. 文档与配置样例保持同步更新。

## 许可证
项目基于 [MIT License](LICENSE) 发布。

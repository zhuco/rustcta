# RustCTA

RustCTA 是一个以 Rust 编写的多交易所 CTA 交易框架，涵盖趋势网格、泊松做市、均值回归、拷贝交易等多种策略模块，并提供统一的账户、风险、日志与通知基础设施，帮助快速构建和部署量化策略。

## 功能亮点
- **多交易所接入**：内置 Binance、Bitmart、OKX、Hyperliquid 等接入实现，统一订单、行情与时间同步接口。
- **策略组件丰富**：包含趋势网格（含 V2 版本）、区间网格、泊松做市、均值回归、Avellaneda-Stoikov、自动化剥头皮、趋势跟随与拷贝交易等策略。
- **账户与风险控制**：`AccountManager` 统一管理账户与 API 密钥，`GlobalRiskManager`、趋势风控模块提供敞口、杠杆与事件监控。
- **稳定的基础设施**：Tokio 异步运行时、统一日志/Webhook 通知、时间同步、符号与精度管理、ClickHouse 数据写入等工具模块。
- **可观测性完善**：策略日志分级落地于 `logs/`，支持 Webhook 告警，`market-analyzer`、`supervisor` 等辅助进程便于运营维护。

## 快速开始

### 环境准备
- Rust 1.75+（建议使用 `rustup` 安装稳定版）
- `cargo`、`git`
- 交易所 API Key 通过环境变量注入（参考 `config/accounts.yml` 中的 `api_key_env` 前缀）

### 获取代码
```bash
git clone https://github.com/zhuco/rustcta.git
cd rustcta
cp .env.example .env   # 如存在，填入交易所密钥；否则自行创建
```

### 构建与校验
```bash
cargo check                         # 语义/依赖检查
cargo fmt                           # 代码格式化
cargo clippy --all-targets --all-features
cargo test --all-features           # 单元/集成测试（若存在）
```

### 运行策略
开发环境可直接运行二进制，需指定策略与配置：

```bash
cargo run -- --strategy trend_grid --config config/trend_grid_ena.yml
```

生产环境建议使用 Release 构建并后台运行：

```bash
cargo build --release
nohup target/release/rustcta --strategy poisson --config config/poisson_near_usdc.yml \
  > logs/poisson_near.out 2>&1 &
```

如需一次性拉起多策略，建议结合 supervisor/systemd 或自研编排脚本。

## 配置说明
- `config/config.yaml`：交易所 REST/WebSocket 入口等全局配置。
- `config/accounts.yml`：账户清单，`api_key_env` 字段对应 `.env` 中的密钥前缀（例如 `BINANCE_0_API_KEY`、`BINANCE_0_API_SECRET`）。
- `config/logging.yml`：日志模块与等级配置。
- `config/global.yml`：Webhook、通知等全局参数。
- `config/*.yml`：策略专属配置（网格参数、风控阈值、交易对等）。
- `config/strategies.yml`：多策略编排示例。

修改配置后建议使用 `cargo check` 或 `scripts/validate_config.sh`（如存在）校验结构。

## 策略目录速览
- `trend_grid` / `trend_grid_v2`：传统与增强版趋势网格策略，支持自适应价差与挂单重建。
- `range_grid`：区间震荡网格，聚焦带宽范围内的双向套利。
- `poisson_market_maker`：泊松分布建模的流动性做市策略（NEAR/USDC、DOGE/USDC 等配置）。
- `mean_reversion`：基于因子与波动判断的均值回归组合策略。
- `avellaneda_stoikov`：经典 Avellaneda-Stoikov 做市模型实现。
- `automated_scalping`：高频剥头皮策略雏形。
- `copy_trading`：监听主账户信号并在子账户复刻下单。

## 代码结构
```
src/
  core/           # 配置、错误、风险管理及基础类型
  exchanges/      # 交易所接入与请求签名
  cta/            # AccountManager 等账户服务
  strategies/     # 各策略实现与公共依赖
  utils/          # 日志、时间同步、通知、指标计算等工具
config/           # 全局与策略配置
logs/             # 运行时输出与策略日志（按日期/策略划分）
```

`src/main.rs` 提供统一 CLI，必须通过 `--strategy` 与 `--config` 指定运行策略和参数文件。

## 运行监控
- **日志**：
  - `logs/strategies/<name>.log`：按策略分割的结构化日志。
  - `logs/*.out`：后台运行策略的标准输出重定向。
- **Webhook**：`utils::webhook` 支持企业微信等告警通道。
- **时间同步**：`utils::time_sync` 定期与交易所比对时间，避免时钟漂移导致订单失败。
- **辅助进程**：`target/release/market-analyzer`、`supervisor` 等可选工具用于行情分析或策略编排。

## 开发流程建议
1. 新增策略时在 `src/strategies` 建立模块，并在 `src/strategies/mod.rs` 暴露接口。
2. 结合现有 `StrategyDepsBuilder`、`AccountManager` 复用下单、风控与日志能力。
3. 更新或新增配置样例至 `config/`，保持文档同步。
4. PR 前执行 `cargo fmt && cargo clippy --all-targets --all-features && cargo test --all-features`。

## 许可证

项目基于 [MIT License](LICENSE) 发布，可自由使用与分发。

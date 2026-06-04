# RustCTA

RustCTA 是一个以 Rust 编写的多交易所 CTA 交易框架，覆盖交易所接入、账户管理、行情聚合、执行路由、风险控制、策略运行、回测、监控与告警等组件。

## 版本信息
- 当前版本：`0.3.7`
- 发布日期：`2026-06-04`
- 上一版本：`0.3.6`

## 本次更新（v0.3.7）
1. 版本号从 `0.3.6` 升级到 `0.3.7`。
2. 按当前仓库结构重写 README，补充 `src/bin/`、`smart_money`、`execution`、`market`、`sql/`、`scripts/` 与测试夹具说明。
3. 同步主程序可启动策略、辅助二进制、配置目录和验证命令，降低新策略和新交易所适配的查找成本。

历史更新记录见 `docs/release_notes_2026-05-30.md` 及 `docs/` 下的专题文档。

## 项目结构
```text
src/
  main.rs                  # 主策略 CLI：通过 --strategy 和 --config 启动策略
  lib.rs                   # 库入口与模块导出
  analysis/                # 分析辅助模块
  backtest/                # 回测数据、撮合、回放、评分与策略适配
  bin/                     # 独立运维/采集/审计/监控二进制
  core/                    # 基础配置、类型、错误、请求、风控与 WebSocket 抽象
  cta/                     # AccountManager 与 CTA 运行期账户组件
  exchanges/               # 交易所实现、网关、注册表与适配器
  execution/               # 执行命令、路由、账本、恢复、幂等和状态机
  market/                  # 行情缓存、合约元数据、精度、资金费率与 WS 监督
  smart_money/             # 聪明钱监控、评分、聚类、组合与存储管线
  strategies/              # 在线策略、通用依赖、风控与策略注册
  utils/                   # 日志、时间同步、通知、签名、指标与符号工具
config/                    # 全局配置、账户配置、策略模板与实盘配置
docs/                      # 策略设计、适配器计划、上线清单与发布记录
sql/                       # ClickHouse/Postgres schema 与数据初始化脚本
scripts/                   # 本地服务、连通性探测、Caddy/systemd 辅助脚本
tests/                     # 回测、交易所、跨交易所套利、聪明钱等集成/回归测试
logs/                      # 运行日志、锁文件和临时验证输出
```

## 核心能力
- 多交易所账户和交易接口：Binance、OKX、Bitget、Gate、HTX、Bitmart、Hyperliquid 等接入路径分布在 `src/exchanges/` 与 `src/exchanges/adapters/`。
- 统一执行层：`src/execution/` 负责执行命令、路由、账本、恢复、用户流和状态机，适合跨策略复用。
- 行情与合约元数据：`src/market/` 管理缓存、交易对规范、精度、资金费率、VWAP 和 WebSocket 健康状态。
- 策略运行期：`StrategyDepsBuilder`、统一风控、账户管理和共享行情组件位于 `src/strategies/common/`。
- 回测体系：`src/backtest/` 提供数据集、撮合引擎、回放运行时和 mean-reversion/trend-factor 等策略适配。
- 聪明钱平台：`src/smart_money/`、`sql/smart_money/`、`config/smart_money.yml` 和对应二进制覆盖钱包画像、Binance 回放、Hyperliquid 监控与组合服务。
- 监控与运维：`monitor_server`、`cross_arb_server`、账户/费用审计和订单 canary 工具位于 `src/bin/`。

## 交易所接入状态
| 交易所 | 现货 | 合约/永续 | REST 交易 | WebSocket 行情 | 私有用户流 | 说明 |
|---|---|---|---|---|---|---|
| Binance | 支持 | 支持 | 支持 | 支持 | 支持 | 当前最完整接入，含 ListenKey 与 keepalive |
| OKX | 支持 | 支持（SWAP） | 支持 | 支持 | 部分支持 | 可用于多策略行情与交易 |
| Bitget | 部分支持 | 支持（USDT 永续） | 支持 | 支持 | 支持 | 已纳入私有永续协议适配 |
| Gate | 部分支持 | 支持（USDT 永续） | 支持 | 支持 | 支持 | 已处理合约张数与基础币数量转换 |
| HTX | 部分支持 | 支持（线性永续） | 支持 | 支持 | 支持 | 已补齐行情与私有协议基础能力 |
| Bitmart | 支持 | 支持 | 支持 | 支持 | 部分支持 | 现货/合约按分域名处理 |
| Hyperliquid | 不支持 | 支持（永续） | 支持 | 支持 | 不适用 | 当前实现偏单向持仓模型 |

`bybit`、`mexc`、`meteora` 等仍有预留或部分市场适配代码，启用前需要结合 `docs/` 中的适配计划和 `src/exchanges/mod.rs` 当前导出状态复核。

## 主程序策略
主程序使用 `cargo run -- --strategy <name> --config <file>` 启动，当前 `src/main.rs` 支持：

| CLI 参数 | 模块 | 类型 |
|---|---|---|
| `trend_intraday` | `strategies/trend` | 日内趋势 |
| `trend_grid` | `strategies/trend_grid_v2` | 趋势网格 |
| `hedged_grid` | `strategies/hedged_grid` | 对冲网格 |
| `solusdc_hedged_grid` | `strategies/solusdc_hedged_grid` | SOLUSDC 专项对冲网格 |
| `multi_hedged_grid` | `strategies/solusdc_hedged_grid::multi` | 多交易对对冲网格 |
| `short_ladder_live` | `strategies/short_ladder_live` | 分层短周期实盘策略 |
| `range_grid` | `strategies/range_grid` | 区间网格 |
| `mean_reversion` | `strategies/mean_reversion` | 均值回归 |
| `sideways_martingale` | `strategies/sideways_martingale` | 震荡马丁 |
| `accumulation` | `strategies/accumulation` | 吸筹 |
| `poisson` | `strategies/poisson_market_maker` | 泊松做市 |
| `as` | `strategies/automated_scalping` | 自动剥头皮 |
| `copy_trading` | `strategies/copy_trading` | 跟单 |
| `avellaneda_stoikov` | `strategies/avellaneda_stoikov` | A-S 做市 |
| `market_making` | `strategies/market_making` | 专业做市 |
| `grid_scale` | `strategies/grid_scale` | 网格增强 |
| `beta_hedge_market_maker` | `strategies/beta_hedge_market_maker` | Beta 对冲做市 |
| `orderflow` | `strategies/orderflow` | 订单流 |
| `obi_scalper` | `strategies/obi_scalper` | OBI 高频剥头皮 |

## 辅助二进制
`src/bin/` 中的工具可用 `cargo run --bin <name> -- ...` 或 release 二进制启动：

- `backtest`：回测、参数扫描、walk-forward 与分析报告。
- `cross_arb_live`、`cross_arb_observe`、`cross_arb_preflight`、`cross_arb_server`：跨交易所套利实盘、观察、预检和服务端。
- `cross_arb_account_audit`、`cross_arb_fee_audit`、`cross_arb_order_admin`：账户、手续费和订单管理审计。
- `funding_arb_live`、`funding_arb_observe`：资金费率套利实盘和观察。
- `monitor_server`、`account_position_reporter`、`trend_report`：监控、持仓和趋势报告。
- `exchange_order_canary`、`bitget_order_canary`、`hyperliquid_self_test`：交易所连通性和订单冒烟验证。
- `smart_money_binance_collector`、`smart_money_hyperliquid_wallet_ingestion`、`smart_money_monitor`、`smart_money_portfolio_service`：聪明钱数据、钱包和组合服务。
- `url_manager`：本地 URL 管理服务，配套脚本在 `scripts/`。

## 快速开始

### 环境准备
- Rust `1.75+`
- `cargo`、`git`
- 交易所 API Key 从 `.env` 或环境变量注入，并通过 `config/accounts.yml` 的 `api_key_env` 映射。

### 构建与验证
```bash
cargo check
cargo fmt
cargo clippy --all-targets --all-features
cargo test --all-features
```

可选功能：
```bash
cargo build --features postgres-collector
cargo build --release
```

### 启动示例
```bash
cargo run -- --strategy trend_grid --config config/trend_grid_ena.yml
cargo run -- --strategy multi_hedged_grid --config config/multi_hedged_grid_usdc.yml
cargo run --bin backtest -- --help
```

Release 后台运行示例：
```bash
cargo build --release
nohup target/release/rustcta --strategy poisson --config config/poisson_near_usdc.yml \
  > logs/poisson_near.out 2>&1 &
```

## 配置与数据
- `config/config.yaml`、`config/config.toml`：交易所端点、转换规则与全局基础配置。
- `config/accounts.yml`：账户、交易所、环境变量前缀、仓位和订单上限。
- `config/global.yml`、`config/logging.yml`、`config/monitoring.yml`：通知、日志和监控配置。
- `config/*.yml`：策略运行配置；新增配置后优先用 `serde_yaml` 加载测试或 `scripts/validate_config.sh` 校验。
- `sql/cross_exchange_arbitrage.sql`：跨交易所套利数据表。
- `sql/smart_money/`：聪明钱 ClickHouse/Postgres schema 与说明。
- `logs/`：运行日志和 ad-hoc 验证输出，避免把敏感数据写入文档或提交。

## 开发建议
1. 新增策略时在 `src/strategies/` 建模块，在 `src/strategies/mod.rs` 导出，并同步 `src/main.rs` 或策略注册表。
2. 新增交易所能力优先复用 `src/exchanges/gateway.rs`、`src/exchanges/registry.rs` 和 `src/exchanges/adapters/` 的协议抽象。
3. 执行相关逻辑优先沉入 `src/execution/`，避免在单个策略中重复实现路由、账本、恢复和幂等。
4. 配置样例、SQL schema、专题文档和测试夹具应随功能同步更新。
5. 提交前至少执行 `cargo fmt`、`cargo check`；影响共享接口或实盘路径时执行 `cargo clippy --all-targets --all-features` 和 `cargo test --all-features`。

## 许可证
项目基于 [MIT License](LICENSE) 发布。

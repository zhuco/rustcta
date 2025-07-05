mod config;
mod error;
mod exchange;
mod logger;
mod strategy;
mod utils;

use config::api_config::ApiConfig;
use config::strategy_config::StrategyConfig;
use exchange::binance::Binance;
use exchange::traits::Exchange;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use strategy::grid::GridStrategy;
use tokio::signal;

// 全局关闭标志
pub static SHUTDOWN: AtomicBool = AtomicBool::new(false);

#[tokio::main]
async fn main() {
    logger::init_logger();

    // 设置信号处理
    tokio::spawn(async {
        if let Err(e) = signal::ctrl_c().await {
            eprintln!("监听Ctrl+C信号失败: {}", e);
            return;
        }
        println!("收到关闭信号，正在优雅关闭...");
        SHUTDOWN.store(true, Ordering::SeqCst);
    });

    let api_settings = ::config::Config::builder()
        .add_source(::config::File::with_name("config/api"))
        .build()
        .unwrap();

    let api_config: ApiConfig = api_settings.try_deserialize().unwrap();

    let strategy_settings = ::config::Config::builder()
        .add_source(::config::File::with_name("config/strategy"))
        .build()
        .unwrap();

    let strategy_config: StrategyConfig = strategy_settings.try_deserialize().unwrap();

    // 检查交易所是否启用
    if !api_config.binance.enabled.unwrap_or(true) {
        println!("Binance交易所已禁用，跳过初始化");
        return;
    }

    // 遍历所有策略配置
    for strategy in &strategy_config.strategies {
        // 检查策略是否启用
        if !strategy.enabled {
            println!("策略 {} 已禁用，跳过", strategy.name);
            continue;
        }

        // 检查绑定的交易所账户是否存在且启用
        let account_config = match api_config.binance.accounts.get(&strategy.exchange) {
            Some(config) => {
                if !config.enabled.unwrap_or(true) {
                    println!(
                        "策略 {} 绑定的账户 {} 已禁用，跳过",
                        strategy.name, strategy.exchange
                    );
                    continue;
                }
                config
            }
            None => {
                println!(
                    "策略 {} 绑定的账户 {} 不存在，跳过",
                    strategy.name, strategy.exchange
                );
                continue;
            }
        };

        println!(
            "初始化策略: {} (账户: {})",
            strategy.name, strategy.exchange
        );

        // 创建交易所实例
        let binance_instance = Binance::new(
            account_config.api_key.clone(),
            account_config.secret_key.clone(),
        );

        // 与交易所同步时间
        println!("正在与交易所同步时间...");
        if let Err(e) = binance_instance
            .sync_time(utils::symbol::MarketType::UsdFutures)
            .await
        {
            eprintln!("时间同步失败: {}, 继续使用本地时间", e);
        }

        let binance: Arc<dyn Exchange> = Arc::new(binance_instance);

        // 转换新配置格式到旧格式以兼容现有代码
        let symbol_parts: Vec<&str> = strategy.params.symbol.split('/').collect();
        let (base, quote) = if symbol_parts.len() >= 2 {
            let quote_part = symbol_parts[1].split(':').next().unwrap_or(symbol_parts[1]);
            (symbol_parts[0].to_string(), quote_part.to_string())
        } else {
            // 如果格式不是 BASE/QUOTE，尝试解析为 BASEUSDC 格式
            if strategy.params.symbol.ends_with("USDC") {
                let base = strategy
                    .params
                    .symbol
                    .strip_suffix("USDC")
                    .unwrap_or(&strategy.params.symbol);
                (base.to_string(), "USDC".to_string())
            } else if strategy.params.symbol.ends_with("USDT") {
                let base = strategy
                    .params
                    .symbol
                    .strip_suffix("USDT")
                    .unwrap_or(&strategy.params.symbol);
                (base.to_string(), "USDT".to_string())
            } else {
                (strategy.params.symbol.clone(), "USDC".to_string())
            }
        };

        let grid_config = config::strategy_config::GridConfig {
            symbol: utils::symbol::Symbol::new(
                &base,
                &quote,
                utils::symbol::MarketType::UsdFutures, // 默认使用期货市场
            ),
            grid_spacing: strategy.params.spacing,
            grid_ratio: None,
            grid_num: strategy.params.grid_levels * 2, // 转换为总网格数
            order_value: strategy.params.order_value,
        };

        let grid_strategy_config = config::strategy_config::GridStrategyConfig {
            grid_configs: vec![grid_config],
        };

        let mut grid_strategy = GridStrategy::new(grid_strategy_config);

        let exchange_clone = binance.clone();
        let strategy_name = strategy.name.clone();
        tokio::spawn(async move {
            println!("启动策略: {}", strategy_name);
            if let Err(e) = grid_strategy.run(exchange_clone).await {
                eprintln!("❌ 策略 {} 运行失败: {}", strategy_name, e);
                eprintln!("策略 {} 已停止运行", strategy_name);
                // 设置全局关闭标志，停止所有策略
                SHUTDOWN.store(true, Ordering::SeqCst);
            }
        });
    }

    // 存储所有交易所实例以便清理
    let mut exchanges: Vec<Arc<dyn Exchange>> = Vec::new();

    // 收集所有创建的交易所实例
    for strategy in &strategy_config.strategies {
        if !strategy.enabled {
            continue;
        }

        if let Some(account_config) = api_config.binance.accounts.get(&strategy.exchange) {
            if account_config.enabled.unwrap_or(true) {
                let exchange: Arc<dyn Exchange> = Arc::new(Binance::new(
                    account_config.api_key.clone(),
                    account_config.secret_key.clone(),
                ));
                exchanges.push(exchange);
            }
        }
    }

    // 主循环，定期检查关闭信号
    loop {
        if SHUTDOWN.load(Ordering::SeqCst) {
            println!("开始清理资源...");

            // 收集所有唯一的交易对，避免重复取消
            let mut unique_symbols = std::collections::HashSet::new();
            for strategy in &strategy_config.strategies {
                if !strategy.enabled {
                    continue;
                }

                let symbol_parts: Vec<&str> = strategy.params.symbol.split('/').collect();
                let (base, quote) = if symbol_parts.len() >= 2 {
                    let quote_part = symbol_parts[1].split(':').next().unwrap_or(symbol_parts[1]);
                    (symbol_parts[0].to_string(), quote_part.to_string())
                } else {
                    if strategy.params.symbol.ends_with("USDC") {
                        let base = strategy
                            .params
                            .symbol
                            .strip_suffix("USDC")
                            .unwrap_or(&strategy.params.symbol);
                        (base.to_string(), "USDC".to_string())
                    } else if strategy.params.symbol.ends_with("USDT") {
                        let base = strategy
                            .params
                            .symbol
                            .strip_suffix("USDT")
                            .unwrap_or(&strategy.params.symbol);
                        (base.to_string(), "USDT".to_string())
                    } else {
                        (strategy.params.symbol.clone(), "USDC".to_string())
                    }
                };

                let symbol = utils::symbol::Symbol::new(
                    &base,
                    &quote,
                    utils::symbol::MarketType::UsdFutures,
                );
                unique_symbols.insert(symbol.to_binance());
            }

            // 只使用第一个交易所实例取消订单，避免重复
            if let Some(exchange) = exchanges.first() {
                for symbol_str in unique_symbols {
                    let symbol_parts: Vec<&str> = symbol_str.split("USDC").collect();
                    let (base, quote) = if symbol_parts.len() >= 2 {
                        (symbol_parts[0].to_string(), "USDC".to_string())
                    } else {
                        let symbol_parts: Vec<&str> = symbol_str.split("USDT").collect();
                        if symbol_parts.len() >= 2 {
                            (symbol_parts[0].to_string(), "USDT".to_string())
                        } else {
                            continue;
                        }
                    };

                    let symbol = utils::symbol::Symbol::new(
                        &base,
                        &quote,
                        utils::symbol::MarketType::UsdFutures,
                    );

                    if let Err(e) = exchange.cancel_all_orders(&symbol).await {
                        eprintln!("取消{}订单失败: {}", symbol.to_binance(), e);
                    } else {
                        println!("已取消{}的所有订单", symbol.to_binance());
                    }
                }
            }

            println!("资源清理完成，程序退出");
            break;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

mod config;
mod error;
mod exchange;
mod logger;
mod strategy;
mod utils;

// 策略日志宏已在 logger.rs 中定义，无需重复导入

use config::api_config::ApiConfig;
use config::strategy_config::StrategyConfig;
use exchange::binance::Binance;
use exchange::traits::Exchange;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use strategy::avellaneda_stoikov::{AvellanedaStoikovConfig, AvellanedaStoikovStrategy};
use strategy::funding_rate::{FundingRateConfig, FundingRateStrategy};
use strategy::grid::GridStrategy;
use tokio::signal;

// 全局关闭标志
pub static SHUTDOWN: AtomicBool = AtomicBool::new(false);

#[tokio::main]
async fn main() {
    // 首先读取配置以获取策略名称
    let strategy_settings = ::config::Config::builder()
        .add_source(::config::File::with_name("config/strategy"))
        .build()
        .unwrap();

    let strategy_config: StrategyConfig = strategy_settings.try_deserialize().unwrap();

    // 收集所有启用的策略名称
    let strategy_names: Vec<String> = strategy_config
        .strategies
        .iter()
        .filter(|s| s.enabled)
        .map(|s| s.name.clone())
        .collect();

    // 初始化策略专用日志系统
    if !strategy_names.is_empty() {
        logger::init_strategy_logger(strategy_names);
    } else {
        logger::init_logger();
    }

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

        strategy_info!(
            &strategy.name,
            "启动策略: {} 交易所: {}",
            strategy.name,
            strategy.exchange
        );

        // 创建交易所实例
        let binance_instance = Binance::new(
            account_config.api_key.clone(),
            account_config.secret_key.clone(),
        );

        // 与交易所同步时间
        log::info!("正在与交易所同步时间...");
        if let Err(e) = binance_instance
            .sync_time(utils::symbol::MarketType::UsdFutures)
            .await
        {
            eprintln!("时间同步失败: {}, 继续使用本地时间", e);
        }

        let binance_arc = Arc::new(binance_instance.clone());
        let binance: Arc<dyn Exchange> = Arc::new(binance_instance);

        // 根据策略类型启动不同的策略
        if strategy.class_path.contains("funding_rate_strategy") {
            // 资金费率策略
            let funding_config = FundingRateConfig {
                name: strategy.name.clone(),
                enabled: strategy.enabled,
                position_size_usd: strategy.params.position_size_usd.unwrap_or(100.0),
                rate_threshold: strategy.params.rate_threshold.unwrap_or(0.0001),
                open_offset_ms: strategy.params.open_offset_ms.unwrap_or(1000),
                close_offset_ms: strategy.params.close_offset_ms.unwrap_or(35),
                check_interval_seconds: strategy.params.check_interval_seconds.unwrap_or(3600),
            };

            let mut funding_strategy = FundingRateStrategy::new(funding_config, binance_arc);
            let strategy_name = strategy.name.clone();

            tokio::spawn(async move {
                strategy_info!(&strategy_name, "启动资金费率策略: {}", strategy_name);
                if let Err(e) = funding_strategy.run().await {
                    strategy_error!(
                        &strategy_name,
                        "❌ 资金费率策略 {} 运行失败: {}",
                        strategy_name,
                        e
                    );
                    strategy_error!(&strategy_name, "策略 {} 已停止运行", strategy_name);
                    SHUTDOWN.store(true, Ordering::SeqCst);
                }
            });
        } else if strategy.class_path.contains("avellaneda_stoikov_strategy") {
            // Avellaneda-Stoikov做市策略
            let symbol_str = strategy
                .params
                .symbol
                .as_ref()
                .unwrap_or(&"ETHUSDC".to_string())
                .clone();

            let as_config = AvellanedaStoikovConfig {
                name: strategy.name.clone(),
                enabled: strategy.enabled,
                symbol: symbol_str,
                order_amount_usdt: strategy.params.order_amount_usdt.unwrap_or(10.0),
                max_position_usdt: strategy.params.max_position_usdt.unwrap_or(500.0),
                risk_aversion: strategy.params.risk_aversion.unwrap_or(0.1),
                market_impact: strategy.params.market_impact.unwrap_or(0.01),
                order_arrival_rate: strategy.params.order_arrival_rate.unwrap_or(1.0),
                volatility_window: strategy.params.volatility_window.unwrap_or(300),
                refresh_interval_ms: strategy.params.refresh_interval_ms.unwrap_or(1000),
                max_hold_time_seconds: strategy.params.max_hold_time_seconds.unwrap_or(300),
                stop_loss_percentage: strategy.params.stop_loss_percentage.unwrap_or(0.1),
                min_spread_bps: strategy.params.min_spread_bps.unwrap_or(5.0),
            };

            let strategy_name = strategy.name.clone();
            let binance_clone = binance_arc.clone();

            tokio::spawn(async move {
                strategy_info!(
                    &strategy_name,
                    "启动Avellaneda-Stoikov做市策略: {}",
                    strategy_name
                );
                match AvellanedaStoikovStrategy::new(as_config, binance_clone).await {
                    Ok(mut as_strategy) => {
                        if let Err(e) = as_strategy.run().await {
                            strategy_error!(
                                &strategy_name,
                                "❌ Avellaneda-Stoikov策略 {} 运行失败: {}",
                                strategy_name,
                                e
                            );
                            strategy_error!(&strategy_name, "策略 {} 已停止运行", strategy_name);
                            SHUTDOWN.store(true, Ordering::SeqCst);
                        }
                    }
                    Err(e) => {
                        strategy_error!(
                            &strategy_name,
                            "❌ Avellaneda-Stoikov策略 {} 初始化失败: {}",
                            strategy_name,
                            e
                        );
                        SHUTDOWN.store(true, Ordering::SeqCst);
                    }
                }
            });
        } else {
            // 网格策略（默认）
            // 检查是否有symbol参数
            let symbol_str = match &strategy.params.symbol {
                Some(s) => s.clone(),
                None => {
                    println!("网格策略 {} 缺少symbol参数，跳过", strategy.name);
                    continue;
                }
            };

            // 转换新配置格式到旧格式以兼容现有代码
            let symbol_parts: Vec<&str> = symbol_str.split('/').collect();
            let (base, quote) = if symbol_parts.len() >= 2 {
                let quote_part = symbol_parts[1].split(':').next().unwrap_or(symbol_parts[1]);
                (symbol_parts[0].to_string(), quote_part.to_string())
            } else {
                // 如果格式不是 BASE/QUOTE，尝试解析为 BASEUSDC 格式
                if symbol_str.ends_with("USDC") {
                    let base = symbol_str.strip_suffix("USDC").unwrap_or(&symbol_str);
                    (base.to_string(), "USDC".to_string())
                } else if symbol_str.ends_with("USDT") {
                    let base = symbol_str.strip_suffix("USDT").unwrap_or(&symbol_str);
                    (base.to_string(), "USDT".to_string())
                } else {
                    (symbol_str.clone(), "USDC".to_string())
                }
            };

            let grid_config = config::strategy_config::GridConfig {
                symbol: utils::symbol::Symbol::new(
                    &base,
                    &quote,
                    utils::symbol::MarketType::UsdFutures, // 默认使用期货市场
                ),
                grid_spacing: strategy.params.spacing.unwrap_or(0.001),
                grid_ratio: None,
                grid_num: strategy.params.grid_levels.unwrap_or(10) * 2, // 转换为总网格数
                order_value: strategy.params.order_value.unwrap_or(10.0),
                health_check_interval_seconds: strategy.params.health_check_interval_seconds,
                uniformity_threshold: strategy.params.uniformity_threshold,
            };

            let grid_strategy_config = config::strategy_config::GridStrategyConfig {
                grid_configs: vec![grid_config],
            };

            let mut grid_strategy = GridStrategy::new(grid_strategy_config);

            let exchange_clone = binance.clone();
            let strategy_name = strategy.name.clone();
            tokio::spawn(async move {
                strategy_info!(&strategy_name, "启动网格策略: {}", strategy_name);
                if let Err(e) = grid_strategy.run(exchange_clone).await {
                    strategy_error!(
                        &strategy_name,
                        "❌ 网格策略 {} 运行失败: {}",
                        strategy_name,
                        e
                    );
                    strategy_error!(&strategy_name, "策略 {} 已停止运行", strategy_name);
                    // 设置全局关闭标志，停止所有策略
                    SHUTDOWN.store(true, Ordering::SeqCst);
                }
            });
        }
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

                // 只处理有symbol的策略（网格策略）
                if let Some(symbol_str) = &strategy.params.symbol {
                    let symbol_parts: Vec<&str> = symbol_str.split('/').collect();
                    let (base, quote) = if symbol_parts.len() >= 2 {
                        let quote_part =
                            symbol_parts[1].split(':').next().unwrap_or(symbol_parts[1]);
                        (symbol_parts[0].to_string(), quote_part.to_string())
                    } else {
                        if symbol_str.ends_with("USDC") {
                            let base = symbol_str.strip_suffix("USDC").unwrap_or(symbol_str);
                            (base.to_string(), "USDC".to_string())
                        } else if symbol_str.ends_with("USDT") {
                            let base = symbol_str.strip_suffix("USDT").unwrap_or(symbol_str);
                            (base.to_string(), "USDT".to_string())
                        } else {
                            (symbol_str.clone(), "USDC".to_string())
                        }
                    };

                    let symbol = utils::symbol::Symbol::new(
                        &base,
                        &quote,
                        utils::symbol::MarketType::UsdFutures,
                    );
                    unique_symbols.insert(symbol.to_binance());
                }
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

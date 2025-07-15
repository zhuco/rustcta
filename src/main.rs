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

use strategy::funding_rate::{FundingRateConfig, FundingRateStrategy};
use strategy::grid::GridStrategy;
use strategy::multi_timeframe_long::{MultiTimeframeLongConfig, MultiTimeframeLongStrategy};
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
        } else if strategy.class_path.contains("MultiTimeframeLongStrategy") {
            // 多时间框架做多策略
            let symbol_str = strategy
                .params
                .symbol
                .as_ref()
                .unwrap_or(&"BTCUSDT".to_string())
                .clone();

            let mtl_config = MultiTimeframeLongConfig {
                name: strategy.name.clone(),
                enabled: strategy.enabled,
                symbol: symbol_str,
                position_size_usdt: strategy.params.position_size_usdt.unwrap_or(100.0),
                max_position_usdt: strategy.params.max_position_usdt.unwrap_or(500.0),
                rsi_period: strategy.params.rsi_period.unwrap_or(14),
                rsi_oversold: strategy.params.rsi_oversold.unwrap_or(30.0),
                rsi_overbought: strategy.params.rsi_overbought.unwrap_or(70.0),
                bb_period: strategy.params.bb_period.unwrap_or(20),
                bb_std_dev: strategy.params.bb_std_dev.unwrap_or(2.0),
                primary_timeframe: strategy
                    .params
                    .primary_timeframe
                    .as_ref()
                    .unwrap_or(&"5m".to_string())
                    .clone(),
                secondary_timeframe: strategy
                    .params
                    .secondary_timeframe
                    .as_ref()
                    .unwrap_or(&"15m".to_string())
                    .clone(),
                tertiary_timeframe: strategy
                    .params
                    .tertiary_timeframe
                    .as_ref()
                    .unwrap_or(&"1h".to_string())
                    .clone(),
                stop_loss_percentage: strategy.params.stop_loss_percentage.unwrap_or(2.0),
                take_profit_percentage: strategy.params.take_profit_percentage.unwrap_or(4.0),
                max_hold_time_minutes: strategy.params.max_hold_time_minutes.unwrap_or(240),
                min_volume_usdt: strategy.params.min_volume_usdt.unwrap_or(100000.0),
                trend_confirmation_bars: strategy.params.trend_confirmation_bars.unwrap_or(3),
                check_interval_seconds: strategy.params.check_interval_seconds.unwrap_or(30),
                order_offset_pct: strategy.params.order_offset_pct.unwrap_or(0.001),
                order_timeout_minutes: strategy.params.order_timeout_minutes.unwrap_or(10),
                allow_multiple_orders: strategy.params.allow_multiple_orders.unwrap_or(false),
            };

            let mut mtl_strategy = MultiTimeframeLongStrategy::new(mtl_config, binance.clone());
            let strategy_name = strategy.name.clone();

            tokio::spawn(async move {
                if let Err(e) = mtl_strategy.run().await {
                    strategy_error!(
                        &strategy_name,
                        "❌ 多时间框架做多策略 {} 运行失败: {}",
                        strategy_name,
                        e
                    );
                    strategy_error!(&strategy_name, "策略 {} 已停止运行", strategy_name);
                    SHUTDOWN.store(true, Ordering::SeqCst);
                }
            });
        } else if strategy.class_path.contains("grid_strategy") {
            // 网格策略
            let symbol_str = strategy
                .params
                .symbol
                .as_ref()
                .unwrap_or(&"BTCUSDT".to_string())
                .clone();

            // 解析symbol字符串
            let symbol = if symbol_str.contains('/') && symbol_str.contains(':') {
                // 处理 "PNUT/USDC:USDC" 格式
                let parts: Vec<&str> = symbol_str.split('/').collect();
                if parts.len() == 2 {
                    let base = parts[0];
                    let quote_part: Vec<&str> = parts[1].split(':').collect();
                    if quote_part.len() == 2 && quote_part[0] == quote_part[1] {
                        let quote = quote_part[0];
                        crate::utils::symbol::Symbol::new(base, quote, crate::utils::symbol::MarketType::UsdFutures)
                    } else {
                        // 回退到简单解析
                        crate::utils::symbol::Symbol::new(
                            &symbol_str.split('/').next().unwrap_or(&symbol_str),
                            &symbol_str.split('/').nth(1).unwrap_or("USDT"),
                            crate::utils::symbol::MarketType::UsdFutures,
                        )
                    }
                } else {
                    // 回退到简单解析
                    crate::utils::symbol::Symbol::new(
                        &symbol_str.split('/').next().unwrap_or(&symbol_str),
                        "USDT",
                        crate::utils::symbol::MarketType::UsdFutures,
                    )
                }
            } else {
                // 处理简单格式如 "PNUTUSDT"
                crate::utils::symbol::Symbol::new(
                    &symbol_str.split('/').next().unwrap_or(&symbol_str),
                    &symbol_str.split('/').nth(1).unwrap_or("USDT"),
                    crate::utils::symbol::MarketType::UsdFutures,
                )
            };

            let grid_config = crate::config::strategy_config::GridConfig {
                symbol,
                grid_spacing: strategy.params.spacing.unwrap_or(10.0),
                grid_ratio: None, // 假设这里只处理算术网格
                grid_num: strategy.params.grid_levels.unwrap_or(10),
                order_value: strategy.params.order_value.unwrap_or(10.0),
                leverage: strategy.params.leverage,
                health_check_interval_seconds: strategy.params.health_check_interval_seconds,
                uniformity_threshold: strategy.params.uniformity_threshold,
                max_price: strategy.params.max_price,
                min_price: strategy.params.min_price,
                close_positions_on_boundary: Some(
                    strategy.params.close_positions_on_boundary.unwrap_or(false),
                ),
                max_loss_usd: strategy.params.max_loss_usd,
                close_positions_on_stop_loss: Some(
                    strategy
                        .params
                        .close_positions_on_stop_loss
                        .unwrap_or(false),
                ),
                take_profit_usd: strategy.params.take_profit_usd,
                close_positions_on_take_profit: Some(
                    strategy.params.close_positions_on_take_profit.unwrap_or(true),
                ),
            };

            let grid_strategy_config = crate::config::strategy_config::GridStrategyConfig {
                grid_configs: vec![grid_config],
            };

            let mut grid_strategy = GridStrategy::new(grid_strategy_config, strategy.name.clone());
            let strategy_name = strategy.name.clone();

            tokio::spawn(async move {
                if let Err(e) = grid_strategy.run(binance.clone()).await {
                    strategy_error!(
                        &strategy_name,
                        "❌ 网格策略 {} 运行失败: {}",
                        strategy_name,
                        e
                    );
                    strategy_error!(&strategy_name, "策略 {} 已停止运行", strategy_name);
                    SHUTDOWN.store(true, Ordering::SeqCst);
                }
            });
        } else {
            strategy_warn!(
                &strategy.name,
                "未知策略类型: {}，跳过",
                strategy.class_path
            );
        }
    }

    // 注意：交易所实例已在上面的策略启动循环中创建
    // 清理时会创建临时实例来取消订单

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

            // 创建临时交易所实例用于清理订单
            if let Some(first_strategy) = strategy_config.strategies.iter().find(|s| s.enabled) {
                if let Some(account_config) =
                    api_config.binance.accounts.get(&first_strategy.exchange)
                {
                    if account_config.enabled.unwrap_or(true) {
                        let cleanup_exchange: Arc<dyn Exchange> = Arc::new(Binance::new(
                            account_config.api_key.clone(),
                            account_config.secret_key.clone(),
                        ));

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

                            if let Err(e) = cleanup_exchange.cancel_all_orders(&symbol).await {
                                eprintln!("取消{}订单失败: {}", symbol.to_binance(), e);
                            } else {
                                println!("已取消{}的所有订单", symbol.to_binance());
                            }
                        }
                    }
                }
            }

            println!("资源清理完成，程序退出");
            break;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

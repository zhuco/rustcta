use clap::{Arg, Command};
use rustcta::strategies::common::application::strategy::{Strategy, StrategyInstance};
use rustcta::strategies::range_grid::application::risk as range_risk;
use rustcta::{
    core::config::{Config, GlobalConfig},
    cta::AccountManager,
    strategies::common::application::deps::StrategyDepsBuilder,
    strategies::common::build_unified_risk_evaluator,
    strategies::*,
    utils::{
        init_global_time_sync,
        unified_logger::{init_global_logger, LogConfig},
    },
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 加载环境变量
    dotenv::dotenv().ok();

    // 初始化统一日志系统
    let log_config = LogConfig {
        root_dir: "logs".to_string(),
        default_level: "INFO".to_string(),
        max_file_size_mb: 10,
        retention_days: 30,
        console_output: true,
        format: "[{timestamp}] [{level}] [{module}] {message}".to_string(),
    };
    init_global_logger(log_config)?;

    // 初始化Webhook通知器
    rustcta::utils::webhook::init_global_notifier("config/global.yml");

    // 设置企业微信webhook地址
    let wechat_webhook =
        "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=0f4b545c-7f86-4c4d-832c-27e6191bd7cd";

    // 解析命令行参数
    let matches = Command::new("RustCTA")
        .version("1.0")
        .about("Rust量化交易系统")
        .arg(Arg::new("strategy")
            .short('s')
            .long("strategy")
            .value_name("STRATEGY")
            .help("策略类型: trend_grid, mean_reversion, poisson, as, copy_trading, avellaneda_stoikov, trend_following")
            .required(true))
        .arg(Arg::new("config")
            .short('c')
            .long("config")
            .value_name("FILE")
            .help("配置文件路径")
            .required(true))
        .get_matches();

    let strategy_type = matches.get_one::<String>("strategy").unwrap();
    let config_file = matches.get_one::<String>("config").unwrap();

    // 读取策略配置文件获取日志级别
    let strategy_config_content = std::fs::read_to_string(config_file)?;
    let strategy_config: serde_yaml::Value = serde_yaml::from_str(&strategy_config_content)?;

    // 从配置中获取日志级别，默认为INFO
    let log_level = strategy_config
        .get("strategy")
        .and_then(|s| s.get("log_level"))
        .and_then(|l| l.as_str())
        .unwrap_or("INFO");

    // 设置日志级别
    std::env::set_var("RUST_LOG", log_level);

    // 重新初始化env_logger以应用新的日志级别
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();

    log::info!(
        "启动策略: {} with config: {}, 日志级别: {}",
        strategy_type,
        config_file,
        log_level
    );

    // 初始化全局时间同步
    let _time_sync = init_global_time_sync();
    log::info!("✅ 全局时间同步已初始化");

    // 加载全局配置
    let global_config = GlobalConfig::from_file("config/config.yaml")?;

    // 创建兼容的Config对象（使用Binance配置）
    let config = Config {
        name: "Binance".to_string(),
        testnet: false,
        spot_base_url: "https://api.binance.com".to_string(),
        futures_base_url: "https://fapi.binance.com".to_string(),
        ws_spot_url: "wss://stream.binance.com:9443/ws".to_string(),
        ws_futures_url: "wss://fstream.binance.com/ws".to_string(),
    };

    // 创建账户管理器
    let mut account_manager = AccountManager::new(config);

    // 从accounts.yml加载所有账户配置
    let accounts_to_add = vec![
        // Binance账户
        ("binance_main", "binance", "BINANCE_0", 10, 20),
        ("binance_daidan", "binance", "BINANCE_2", 5, 10),
        ("binance_hcr", "binance", "BINANCE_3", 5, 10),
        // 其他交易所账户
        ("bitmart_main", "bitmart", "BITMART", 10, 20),
        // ("hyperliquid_main", "hyperliquid", "HYPERLIQUID", 10, 20),
        // ("htx_main", "htx", "HTX", 10, 20),
    ];

    // 批量添加账户
    for (id, exchange, env_prefix, max_positions, max_orders) in accounts_to_add {
        let account_config = rustcta::AccountConfig {
            id: id.to_string(),
            exchange: exchange.to_string(),
            api_key_env: env_prefix.to_string(),
            enabled: true,
            max_positions,
            max_orders_per_symbol: max_orders,
        };

        match account_manager.add_account(account_config).await {
            Ok(_) => log::info!("✅ 成功添加账户: {}", id),
            Err(e) => log::warn!("⚠️ 添加账户 {} 失败: {}", id, e),
        }
    }

    let account_manager = Arc::new(account_manager);

    // 仓位报告服务已独立运行，不在策略进程中启动

    // 执行一次时间同步
    if let Some(time_sync) = rustcta::utils::get_time_sync() {
        // 获取第一个Binance账户进行时间同步
        if let Some(account) = account_manager.get_account("binance_main") {
            match account.exchange.get_server_time().await {
                Ok(server_time) => {
                    let local_time = chrono::Utc::now();
                    let offset_ms = server_time.timestamp_millis() - local_time.timestamp_millis();
                    log::info!(
                        "⏰ 全局时间同步: 服务器时间 {} - 本地时间 {} = 偏移 {}ms",
                        server_time.format("%H:%M:%S%.3f"),
                        local_time.format("%H:%M:%S%.3f"),
                        offset_ms
                    );
                }
                Err(e) => {
                    log::warn!("⚠️ 时间同步失败: {}，使用本地时间", e);
                }
            }
        }
    }

    // 根据策略类型启动
    match strategy_type.as_str() {
        "trend_grid" => {
            // 从文件读取配置
            let file_content = std::fs::read_to_string(config_file)?;
            let config: TrendGridConfigV2 = serde_yaml::from_str(&file_content)?;
            let strategy = TrendGridStrategyV2::new(config, account_manager);
            log::info!("趋势网格策略已创建，开始运行...");

            // 运行策略
            strategy.start().await?;

            // 保持运行直到收到停止信号
            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，正在关闭策略...");
            strategy.stop().await?;
        }
        "range_grid" => {
            let file_content = std::fs::read_to_string(config_file)?;
            let config: RangeGridConfig = serde_yaml::from_str(&file_content)?;

            let risk_evaluator = build_unified_risk_evaluator(
                config.strategy.name.clone(),
                None,
                Some(range_risk::build_limits_from_config(&config)),
            );

            let deps = StrategyDepsBuilder::new()
                .with_account_manager(account_manager.clone())
                .with_risk_evaluator(risk_evaluator)
                .build()?;

            let strategy = RangeGridStrategy::create(config, deps)?;
            log::info!("区间网格策略已创建，开始运行...");

            strategy.start().await?;

            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，正在关闭策略...");
            strategy.stop().await?;
        }
        "mean_reversion" => {
            let file_content = std::fs::read_to_string(config_file)?;
            let config: MeanReversionConfig = serde_yaml::from_str(&file_content)?;

            let risk_evaluator =
                build_unified_risk_evaluator(config.strategy.name.clone(), None, None);

            let deps = StrategyDepsBuilder::new()
                .with_account_manager(account_manager.clone())
                .with_risk_evaluator(risk_evaluator)
                .build()?;

            let strategy = MeanReversionStrategy::create(config, deps)?;
            log::info!("均值回归策略已创建，开始运行...");

            strategy.start().await?;

            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，正在关闭策略...");
            strategy.stop().await?;
        }
        "poisson" => {
            // 从文件读取配置
            let file_content = std::fs::read_to_string(config_file)?;
            let config: PoissonMMConfig = serde_yaml::from_str(&file_content)?;
            let strategy = PoissonMarketMaker::new(config, account_manager);
            log::info!("泊松做市策略已创建，开始运行...");

            // 运行策略
            strategy.start().await?;

            // 保持运行直到收到停止信号
            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，正在关闭策略...");
            strategy.stop().await?;
        }
        "as" => {
            // 从文件读取配置
            let file_content = std::fs::read_to_string(config_file)?;
            let config: ASConfig = serde_yaml::from_str(&file_content)?;
            let strategy = AutomatedScalpingStrategy::new(config, account_manager);

            // 运行策略
            strategy.start().await?;

            // 等待退出信号
            tokio::signal::ctrl_c().await?;
            log::info!("收到退出信号，正在停止AS策略...");

            strategy.stop().await?;
            log::info!("AS策略已停止");
        }
        "copy_trading" => {
            // 从文件读取配置
            let file_content = std::fs::read_to_string(config_file)?;
            let config: CopyTradingConfig = serde_yaml::from_str(&file_content)?;
            let strategy = CopyTradingStrategy::new(config, account_manager);
            log::info!("跟单策略已创建，开始运行...");

            // 运行策略
            if let Err(e) = strategy.start().await {
                log::error!("跟单策略启动失败: {}", e);
                return Err(format!("跟单策略启动失败: {}", e).into());
            }

            // 保持运行直到收到停止信号
            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，跟单策略将自动关闭");
        }
        "avellaneda_stoikov" => {
            // 从文件读取配置
            let file_content = std::fs::read_to_string(config_file)?;
            let config: AVSConfig = serde_yaml::from_str(&file_content)?;

            let mut strategy = AvellanedaStoikovStrategy::new(config, account_manager).await?;
            log::info!("Avellaneda-Stoikov策略已创建，开始运行...");

            // 运行策略
            strategy.start().await?;

            // 等待退出信号
            tokio::signal::ctrl_c().await?;
            log::info!("收到退出信号，正在停止A-S策略...");

            strategy.stop().await?;
            log::info!("A-S策略已停止");
        }
        "trend_following" => {
            // 从文件读取配置
            let file_content = std::fs::read_to_string(config_file)?;
            let config: TrendConfig = serde_yaml::from_str(&file_content)?;

            // 添加全局风险管理器
            let risk_config = rustcta::core::risk_manager::RiskConfig::default();
            let global_risk_manager = Arc::new(tokio::sync::RwLock::new(
                rustcta::core::risk_manager::GlobalRiskManager::new(risk_config),
            ));

            let strategy =
                TrendFollowingStrategy::new(config, account_manager, global_risk_manager).await?;

            log::info!("趋势跟踪策略已创建，开始运行...");

            // 运行策略
            strategy.start().await?;

            // 保持运行直到收到停止信号
            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，正在关闭策略...");
            strategy.stop().await?;
        }
        _ => {
            log::error!("未知策略类型: {}", strategy_type);
            return Err(format!("未知策略类型: {}", strategy_type).into());
        }
    }

    Ok(())
}

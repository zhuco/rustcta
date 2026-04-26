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
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command as ProcessCommand};
use std::sync::Arc;

struct ProcessLock {
    path: PathBuf,
}

impl ProcessLock {
    fn acquire(strategy: &str, config_file: &str) -> Result<Self, Box<dyn std::error::Error>> {
        fs::create_dir_all("logs/runtime")?;

        let safe_config = config_file
            .chars()
            .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
            .collect::<String>();
        let lock_path = PathBuf::from(format!("logs/runtime/{}_{}.lock", strategy, safe_config));

        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
        {
            Ok(mut file) => {
                writeln!(file, "pid={}", std::process::id())?;
                writeln!(file, "strategy={}", strategy)?;
                writeln!(file, "config={}", config_file)?;
                Ok(Self { path: lock_path })
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing_pid = read_pid_from_lock(&lock_path);
                if let Some(pid) = existing_pid {
                    if is_same_strategy_process_running(pid, strategy, config_file) {
                        return Err(format!(
                            "检测到策略已在运行: strategy={} config={} pid={}",
                            strategy, config_file, pid
                        )
                        .into());
                    }
                }

                // 锁文件存在但进程不存在（异常退出遗留），清理后重试。
                let _ = fs::remove_file(&lock_path);
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&lock_path)?;
                writeln!(file, "pid={}", std::process::id())?;
                writeln!(file, "strategy={}", strategy)?;
                writeln!(file, "config={}", config_file)?;
                Ok(Self { path: lock_path })
            }
            Err(err) => Err(Box::new(err)),
        }
    }
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn read_pid_from_lock(path: &PathBuf) -> Option<u32> {
    let content = fs::read_to_string(path).ok()?;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("pid=") {
            if let Ok(pid) = rest.trim().parse::<u32>() {
                return Some(pid);
            }
        }
    }
    None
}

fn is_same_strategy_process_running(pid: u32, strategy: &str, config_file: &str) -> bool {
    let cmdline_path = format!("/proc/{pid}/cmdline");
    let raw = match fs::read(cmdline_path) {
        Ok(raw) => raw,
        Err(_) => return false,
    };

    let cmdline = String::from_utf8_lossy(&raw).replace('\0', " ");
    cmdline.contains("rustcta")
        && cmdline.contains(&format!("--strategy {}", strategy))
        && cmdline.contains(config_file)
}

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
    let _wechat_webhook =
        "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=0f4b545c-7f86-4c4d-832c-27e6191bd7cd";

    // 解析命令行参数
    let matches = Command::new("RustCTA")
        .version("1.0")
        .about("Rust量化交易系统")
        .arg(Arg::new("strategy")
            .short('s')
            .long("strategy")
            .value_name("STRATEGY")
            .help("策略类型: trend_intraday, trend_grid, hedged_grid, solusdc_hedged_grid, mean_reversion, sideways_martingale, accumulation, poisson, as, copy_trading, avellaneda_stoikov, market_making, grid_scale, orderflow, beta_hedge_market_maker")
            .required(true))
        .arg(Arg::new("config")
            .short('c')
            .long("config")
            .value_name("FILE")
            .help("配置文件路径")
            .required(true))
        .arg(Arg::new("symbol")
            .long("symbol")
            .value_name("CONFIG_ID")
            .help("仅运行指定的配置ID（对冲网格专用）"))
        .get_matches();

    let strategy_type = matches.get_one::<String>("strategy").unwrap();
    let config_file = matches.get_one::<String>("config").unwrap();
    let single_symbol = matches.get_one::<String>("symbol").cloned();

    let _process_lock = if matches!(
        strategy_type.as_str(),
        "solusdc_hedged_grid" | "beta_hedge_market_maker"
    ) {
        Some(ProcessLock::acquire(strategy_type, config_file)?)
    } else {
        None
    };

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
    let _global_config = GlobalConfig::from_file("config/config.yaml")?;

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
        ("hyperliquid_main", "hyperliquid", "HYPERLIQUID", 10, 20),
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
        let cached_offset = time_sync.get_offset_ms().await;
        log::info!("当前时间同步偏移缓存: {}ms", cached_offset);
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
        "trend_intraday" => {
            let file_content = std::fs::read_to_string(config_file)?;
            let config: TrendConfig = serde_yaml::from_str(&file_content)?;

            let risk_evaluator = build_unified_risk_evaluator(config.name.clone(), None, None);

            let deps = StrategyDepsBuilder::new()
                .with_account_manager(account_manager.clone())
                .with_risk_evaluator(risk_evaluator)
                .build()?;

            let strategy = TrendIntradayStrategy::create(config, deps)?;
            log::info!("日内趋势策略已创建，开始运行...");

            strategy.start().await?;

            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，正在关闭策略...");
            strategy.stop().await?;
        }
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
        "hedged_grid" => {
            let file_content = std::fs::read_to_string(config_file)?;
            let mut config: HedgedGridConfig = serde_yaml::from_str(&file_content)?;

            if let Some(ref filter) = single_symbol {
                config.symbols.retain(|symbol| &symbol.config_id == filter);
                if config.symbols.is_empty() {
                    return Err(
                        format!("未在配置文件中找到 config_id 为 {} 的交易对", filter).into(),
                    );
                }
                log::info!("对冲网格仅运行配置: {}", filter);
            }

            if config.execution.process_per_symbol && single_symbol.is_none() {
                let current_exe = std::env::current_exe()?;
                let mut children: Vec<Child> = Vec::new();

                for symbol in &config.symbols {
                    let child = ProcessCommand::new(&current_exe)
                        .arg("--strategy")
                        .arg("hedged_grid")
                        .arg("--config")
                        .arg(config_file)
                        .arg("--symbol")
                        .arg(&symbol.config_id)
                        .spawn()
                        .map_err(|err| {
                            format!("启动子进程运行 {} 失败: {}", symbol.config_id, err)
                        })?;
                    log::info!(
                        "已启动对冲网格子进程: {} (PID={})",
                        symbol.config_id,
                        child.id()
                    );
                    children.push(child);
                }

                log::info!("等待 Ctrl+C 以结束所有对冲网格子进程...");
                tokio::signal::ctrl_c().await?;
                log::info!("收到停止信号，开始终止子进程...");

                for mut child in children {
                    let pid = child.id();
                    if let Err(err) = child.kill() {
                        log::warn!("终止子进程 {} 失败: {}", pid, err);
                    } else {
                        let _ = child.wait();
                        log::info!("已终止子进程 {}", pid);
                    }
                }

                return Ok(());
            }

            let strategy = HedgedGridStrategy::new(config, account_manager.clone());
            log::info!("对冲网格策略已创建，开始运行...");

            strategy.start().await?;

            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，正在关闭策略...");
            strategy.stop().await?;
        }
        "solusdc_hedged_grid" => {
            let file_content = std::fs::read_to_string(config_file)?;
            let config: SolusdcHedgedGridRuntimeConfig = serde_yaml::from_str(&file_content)?;
            let strategy = SolusdcHedgedGridStrategy::new(config, account_manager.clone());
            log::info!("SOLUSDC 对冲滚动网格策略已创建，开始运行...");

            strategy.start().await?;

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
        "sideways_martingale" => {
            let file_content = std::fs::read_to_string(config_file)?;
            let config: SidewaysMartingaleConfig = serde_yaml::from_str(&file_content)?;

            let risk_evaluator =
                build_unified_risk_evaluator(config.strategy.name.clone(), None, None);

            let deps = StrategyDepsBuilder::new()
                .with_account_manager(account_manager.clone())
                .with_risk_evaluator(risk_evaluator)
                .build()?;

            let strategy = SidewaysMartingaleStrategy::create(config, deps)?;
            log::info!("震荡行情马丁策略已创建，开始运行...");

            strategy.start().await?;

            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，正在关闭震荡行情马丁策略...");
            strategy.stop().await?;
        }
        "accumulation" => {
            let file_content = std::fs::read_to_string(config_file)?;
            let config: AccumulationConfig = serde_yaml::from_str(&file_content)?;

            let risk_evaluator =
                build_unified_risk_evaluator(config.strategy.name.clone(), None, None);

            let deps = StrategyDepsBuilder::new()
                .with_account_manager(account_manager.clone())
                .with_risk_evaluator(risk_evaluator)
                .build()?;

            let strategy = AccumulationStrategy::create(config, deps)?;
            log::info!("吸筹策略已创建，开始运行...");

            strategy.start().await?;

            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，正在关闭吸筹策略...");
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
        "market_making" => {
            let file_content = std::fs::read_to_string(config_file)?;
            let config: MarketMakingConfig = serde_yaml::from_str(&file_content)?;

            let risk_evaluator =
                build_unified_risk_evaluator(config.strategy.name.clone(), None, None);

            let deps = StrategyDepsBuilder::new()
                .with_account_manager(account_manager.clone())
                .with_risk_evaluator(risk_evaluator)
                .build()?;

            let strategy = ProMarketMakingStrategy::create(config, deps)?;
            log::info!("专业做市策略已创建，开始运行...");

            strategy.start().await?;

            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，正在关闭做市策略...");
            strategy.stop().await?;
        }
        "grid_scale" => {
            let file_content = std::fs::read_to_string(config_file)?;
            let config: GridScaleConfig = serde_yaml::from_str(&file_content)?;

            let risk_evaluator =
                build_unified_risk_evaluator(config.strategy.name.clone(), None, None);

            let deps = StrategyDepsBuilder::new()
                .with_account_manager(account_manager.clone())
                .with_risk_evaluator(risk_evaluator)
                .build()?;

            let strategy = GridScaleStrategy::create(config, deps)?;
            log::info!("Grid Scale 策略已创建，开始运行...");
            strategy.start().await?;

            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，正在关闭Grid Scale策略...");
            strategy.stop().await?;
        }
        "beta_hedge_market_maker" => {
            let file_content = std::fs::read_to_string(config_file)?;
            let config: BetaHedgeMarketMakerConfig = serde_yaml::from_str(&file_content)?;

            let risk_evaluator = build_unified_risk_evaluator(
                config.strategy.name.clone(),
                None,
                Some(
                    rustcta::strategies::beta_hedge_market_maker::risk::build_strategy_limits(
                        &config,
                    ),
                ),
            );

            let deps = StrategyDepsBuilder::new()
                .with_account_manager(account_manager.clone())
                .with_risk_evaluator(risk_evaluator)
                .build()?;

            let strategy = BetaHedgeMarketMaker::create(config, deps)?;
            log::info!("Beta hedge market maker 已创建，开始运行...");

            strategy.start().await?;

            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，正在关闭 beta hedge market maker...");
            strategy.stop().await?;
        }
        "orderflow" => {
            let file_content = std::fs::read_to_string(config_file)?;
            let config: OrderflowConfig = serde_yaml::from_str(&file_content)?;

            let risk_evaluator = build_unified_risk_evaluator(config.name.clone(), None, None);

            let deps = StrategyDepsBuilder::new()
                .with_account_manager(account_manager.clone())
                .with_risk_evaluator(risk_evaluator)
                .build()?;

            let strategy = OrderflowStrategy::create(config, deps)?;
            log::info!("订单流策略已创建，开始运行...");

            strategy.start().await?;

            tokio::signal::ctrl_c().await?;
            log::info!("收到停止信号，正在关闭订单流策略...");
            strategy.stop().await?;
        }
        _ => {
            log::error!("未知策略类型: {}", strategy_type);
            return Err(format!("未知策略类型: {}", strategy_type).into());
        }
    }

    Ok(())
}

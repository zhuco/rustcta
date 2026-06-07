use clap::{Arg, Command};
use rustcta::live_preflight::{render_human_report, run_live_preflight};
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
        unified_logger::{init_global_logger, init_tracing_logger, trading_span, LogConfig},
    },
};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
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

fn read_config_file(path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(path)?;
    Ok(expand_env_placeholders(&content))
}

fn expand_env_placeholders(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'$' && bytes.get(index + 1) == Some(&b'{') {
            if let Some(end) = bytes[index + 2..].iter().position(|byte| *byte == b'}') {
                let key_start = index + 2;
                let key_end = key_start + end;
                let key = &input[key_start..key_end];
                if is_env_key(key) {
                    output.push_str(&std::env::var(key).unwrap_or_else(|_| "\"\"".to_string()));
                    index = key_end + 1;
                    continue;
                }
            }
        }
        output.push(bytes[index] as char);
        index += 1;
    }
    output
}

fn is_env_key(key: &str) -> bool {
    !key.is_empty()
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
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

async fn shutdown_signal() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                Ok(())
            }
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok(())
    }
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
            .help("策略类型: spot_spot_taker_arbitrage, cross_exchange_arbitrage, funding_rate_arbitrage, trend_intraday, multi_hedged_grid, short_ladder_live, mean_reversion, range_grid, poisson, avellaneda_stoikov")
            .required(false)
            .default_value("spot_spot_taker_arbitrage"))
        .arg(Arg::new("preflight")
            .long("preflight")
            .action(clap::ArgAction::SetTrue)
            .help("Run read-only small-capital live preflight and exit"))
        .arg(Arg::new("config")
            .short('c')
            .long("config")
            .value_name("FILE")
            .help("配置文件路径")
            .required(true))
        .get_matches();

    let strategy_type = matches.get_one::<String>("strategy").unwrap();
    let config_file = matches.get_one::<String>("config").unwrap();
    let preflight = matches.get_flag("preflight");

    let _process_lock = if matches!(strategy_type.as_str(), "multi_hedged_grid") {
        Some(ProcessLock::acquire(strategy_type, config_file)?)
    } else {
        None
    };

    if preflight {
        let exit_code = run_cli_preflight(strategy_type, config_file).await?;
        std::process::exit(exit_code);
    }

    // 读取策略配置文件获取日志级别
    let strategy_config_content = read_config_file(config_file)?;
    let strategy_config: serde_yaml::Value = serde_yaml::from_str(&strategy_config_content)?;

    // 从配置中获取日志级别，默认为INFO
    let log_level = strategy_config
        .get("strategy")
        .and_then(|s| s.get("log_level"))
        .and_then(|l| l.as_str())
        .unwrap_or("INFO");

    // 设置日志级别
    std::env::set_var("RUST_LOG", log_level);

    // tracing-log 会兼容现有 log::* 调用；新模块直接使用 tracing::*。
    init_tracing_logger(log_level);
    let _strategy_span = trading_span("multi", "multi", strategy_type);
    let _strategy_span_guard = _strategy_span.enter();

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

    if strategy_type.as_str() == "spot_spot_taker_arbitrage" {
        log::info!("spot_spot_taker_arbitrage uses dedicated spot clients; skipping legacy account bootstrap");
    } else {
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
                position_mode: None,
                enabled: true,
                max_positions,
                max_orders_per_symbol: max_orders,
            };

            match account_manager.add_account(account_config).await {
                Ok(_) => log::info!("✅ 成功添加账户: {}", id),
                Err(e) => log::warn!("⚠️ 添加账户 {} 失败: {}", id, e),
            }
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
            let file_content = read_config_file(config_file)?;
            let config: TrendConfig = serde_yaml::from_str(&file_content)?;

            let risk_evaluator = build_unified_risk_evaluator(config.name.clone(), None, None);

            let deps = StrategyDepsBuilder::new()
                .with_account_manager(account_manager.clone())
                .with_risk_evaluator(risk_evaluator)
                .build()?;

            let strategy = TrendIntradayStrategy::create(config, deps)?;
            log::info!("日内趋势策略已创建，开始运行...");

            strategy.start().await?;

            shutdown_signal().await?;
            log::info!("收到停止信号，正在关闭策略...");
            strategy.stop().await?;
        }
        "cross_exchange_arbitrage" => {
            let file_content = read_config_file(config_file)?;
            let config: rustcta::strategies::cross_exchange_arbitrage::CrossExchangeArbitrageConfig =
                serde_yaml::from_str(&file_content)?;
            log::info!("cross_exchange_arbitrage detection-only strategy is starting...");

            let detection_task = tokio::spawn(
                rustcta::strategies::cross_exchange_arbitrage::run_cross_exchange_arbitrage_detection_only(
                    config,
                ),
            );

            tokio::select! {
                result = detection_task => {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => return Err(err.into()),
                        Err(err) => return Err(format!("cross_exchange_arbitrage task failed: {err}").into()),
                    }
                }
                result = shutdown_signal() => {
                    result?;
                    log::info!("收到停止信号，正在关闭 cross_exchange_arbitrage detection-only 策略...");
                }
            }
        }
        "spot_spot_taker_arbitrage" => {
            let file_content = read_config_file(config_file)?;
            let config: SpotSpotTakerArbitrageConfig = serde_yaml::from_str(&file_content)?;
            let trading_mode = config.trading_mode.clone();
            let live_trading_enabled = config.live_trading_enabled;
            let strategy = SpotSpotTakerArbitrageStrategy::new(config)?;
            log::info!(
                "spot_spot_taker_arbitrage strategy is starting trading_mode={} live_trading_enabled={}",
                trading_mode,
                live_trading_enabled
            );

            let strategy_task = tokio::spawn(strategy.start());
            tokio::select! {
                result = strategy_task => {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => return Err(err.into()),
                        Err(err) => return Err(format!("spot_spot_taker_arbitrage task failed: {err}").into()),
                    }
                }
                result = shutdown_signal() => {
                    result?;
                    log::info!("收到停止信号，正在关闭 spot_spot_taker_arbitrage paper 策略...");
                }
            }
        }
        "multi_hedged_grid" => {
            let file_content = read_config_file(config_file)?;
            let config: MultiHedgedGridRuntimeConfig = serde_yaml::from_str(&file_content)?;
            let strategy = MultiHedgedGridStrategy::new(config, account_manager.clone());
            log::info!("多交易对对冲滚动网格策略已创建，开始运行...");

            strategy.start().await?;

            shutdown_signal().await?;
            log::info!("收到停止信号，正在关闭策略...");
            strategy.stop().await?;
        }
        "short_ladder_live" => {
            let file_content = read_config_file(config_file)?;
            let config: ShortLadderLiveConfig = serde_yaml::from_str(&file_content)?;

            let risk_evaluator =
                build_unified_risk_evaluator(config.strategy.name.clone(), None, None);

            let deps = StrategyDepsBuilder::new()
                .with_account_manager(account_manager.clone())
                .with_risk_evaluator(risk_evaluator)
                .build()?;

            let strategy = ShortLadderLiveStrategy::create(config, deps)?;
            log::info!("Short ladder live 策略已创建，开始运行...");

            strategy.start().await?;

            shutdown_signal().await?;
            log::info!("收到停止信号，正在关闭 short ladder live 策略...");
            strategy.stop().await?;
        }
        "range_grid" => {
            let file_content = read_config_file(config_file)?;
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

            shutdown_signal().await?;
            log::info!("收到停止信号，正在关闭策略...");
            strategy.stop().await?;
        }
        "mean_reversion" => {
            let file_content = read_config_file(config_file)?;
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

            shutdown_signal().await?;
            log::info!("收到停止信号，正在关闭策略...");
            strategy.stop().await?;
        }
        "poisson" => {
            // 从文件读取配置
            let file_content = read_config_file(config_file)?;
            let config: PoissonMMConfig = serde_yaml::from_str(&file_content)?;
            let strategy = PoissonMarketMaker::new(config, account_manager);
            log::info!("泊松做市策略已创建，开始运行...");

            // 运行策略
            strategy.start().await?;

            // 保持运行直到收到停止信号
            shutdown_signal().await?;
            log::info!("收到停止信号，正在关闭策略...");
            strategy.stop().await?;
        }
        "avellaneda_stoikov" => {
            // 从文件读取配置
            let file_content = read_config_file(config_file)?;
            let config: AVSConfig = serde_yaml::from_str(&file_content)?;

            let mut strategy = AvellanedaStoikovStrategy::new(config, account_manager).await?;
            log::info!("Avellaneda-Stoikov策略已创建，开始运行...");

            // 运行策略
            strategy.run_until_shutdown().await?;
            log::info!("A-S策略已停止");
        }
        _ => {
            log::error!("未知策略类型: {}", strategy_type);
            return Err(format!("未知策略类型: {}", strategy_type).into());
        }
    }

    Ok(())
}

async fn run_cli_preflight(
    strategy_type: &str,
    config_file: &str,
) -> Result<i32, Box<dyn std::error::Error>> {
    if strategy_type != "spot_spot_taker_arbitrage" {
        return Err(format!(
            "--preflight currently supports spot_spot_taker_arbitrage, got {strategy_type}"
        )
        .into());
    }
    let file_content = read_config_file(config_file)?;
    let config: SpotSpotTakerArbitrageConfig = serde_yaml::from_str(&file_content)?;
    config.validate_safe_mode()?;
    let preflight_config = config.live_preflight.clone();
    let inventory =
        rustcta::strategies::spot_spot_taker_arbitrage::PaperInventory::from_config(&config)?;
    let fee_model = rustcta::execution::FeeModel::load_or_default(&config.fee_config_path);
    let disabled_registry =
        rustcta::risk::DisabledRegistry::load_or_empty(&config.disabled_registry_path);
    let state =
        rustcta::strategies::spot_spot_taker_arbitrage::build_live_preflight_state_from_config(
            &config,
            &inventory,
            &disabled_registry,
            &fee_model,
        )
        .await?;
    let report = run_live_preflight(preflight_config, &state);
    println!("{}", render_human_report(&report));
    Ok(if report.has_failures() { 1 } else { 0 })
}

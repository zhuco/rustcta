use chrono::{DateTime, Local};
use log::{Level, LevelFilter, Metadata, Record};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
/// 统一日志管理模块
/// 为整个RustCTA项目提供标准化的日志处理
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// 日志配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    pub root_dir: String,
    pub default_level: String,
    pub max_file_size_mb: u64,
    pub retention_days: u32,
    pub console_output: bool,
    pub format: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            root_dir: "logs".to_string(),
            default_level: "INFO".to_string(),
            max_file_size_mb: 10,
            retention_days: 30,
            console_output: true,
            format: "[{timestamp}] [{level}] [{module}] {message}".to_string(),
        }
    }
}

/// 策略日志器（来自原logger.rs）
pub struct StrategyLogger {
    name: String,
    file: Mutex<Option<fs::File>>,
    max_size: u64,
    current_size: Mutex<u64>,
}

impl StrategyLogger {
    /// 创建策略日志器
    pub fn new(strategy_name: &str, max_size_mb: u64) -> Self {
        let log_dir = "logs/strategies";
        if !Path::new(log_dir).exists() {
            fs::create_dir_all(log_dir).expect("创建日志目录失败");
        }

        let timestamp = Local::now().format("%Y%m%d");
        let log_file = format!("{}/{}_{}.log", log_dir, strategy_name, timestamp);

        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file)
            .expect("打开日志文件失败");

        let current_size = file.metadata().map(|m| m.len()).unwrap_or(0);

        Self {
            name: strategy_name.to_string(),
            file: Mutex::new(Some(file)),
            max_size: max_size_mb * 1024 * 1024,
            current_size: Mutex::new(current_size),
        }
    }

    /// 写入日志
    pub fn log(&self, level: Level, message: &str) {
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let formatted = format!("[{}] [{}] [{}] {}\n", timestamp, self.name, level, message);

        let mut file_guard = self.file.lock().expect("Lock poisoned");
        let mut size_guard = self.current_size.lock().expect("Lock poisoned");

        if *size_guard + formatted.len() as u64 > self.max_size {
            *file_guard = None;

            let timestamp = Local::now().format("%Y%m%d_%H%M%S");
            let new_log_file = format!(
                "logs/strategies/{}_{}_{}.log",
                self.name, timestamp, "rotated"
            );

            if let Ok(new_file) = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&new_log_file)
            {
                *file_guard = Some(new_file);
                *size_guard = 0;
            }
        }

        if let Some(ref mut file) = *file_guard {
            if file.write_all(formatted.as_bytes()).is_ok() {
                *size_guard += formatted.len() as u64;
                let _ = file.flush();
            }
        }
    }
}

/// 统一日志管理器
pub struct UnifiedLogger {
    config: LogConfig,
    log_files: Arc<Mutex<HashMap<String, PathBuf>>>,
    file_handles: Arc<Mutex<HashMap<String, fs::File>>>,
    strategy_loggers: Arc<Mutex<HashMap<String, StrategyLogger>>>,
}

impl UnifiedLogger {
    /// 创建新的日志管理器
    pub fn new(config: LogConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // 创建必要的目录
        Self::ensure_directories(&config)?;

        Ok(Self {
            config,
            log_files: Arc::new(Mutex::new(HashMap::new())),
            file_handles: Arc::new(Mutex::new(HashMap::new())),
            strategy_loggers: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// 从配置文件加载
    pub fn from_config_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config_str = fs::read_to_string(path)?;
        let config: LogConfig = serde_yaml::from_str(&config_str)?;
        Self::new(config)
    }

    /// 确保目录结构存在
    fn ensure_directories(config: &LogConfig) -> Result<(), Box<dyn std::error::Error>> {
        let directories = vec![
            format!("{}/strategies", config.root_dir),
            format!("{}/manager", config.root_dir),
            format!("{}/system", config.root_dir),
            format!("{}/api", config.root_dir),
            format!("{}/database", config.root_dir),
            format!("{}/backtest", config.root_dir),
        ];

        for dir in directories {
            if !Path::new(&dir).exists() {
                fs::create_dir_all(&dir)?;
            }
        }

        Ok(())
    }

    /// 获取策略的日志文件路径
    pub fn get_strategy_log_path(&self, strategy_name: &str) -> PathBuf {
        let date = Local::now().format("%Y%m%d");
        PathBuf::from(format!(
            "{}/strategies/{}_{}.log",
            self.config.root_dir, strategy_name, date
        ))
    }

    /// 获取系统日志文件路径
    pub fn get_system_log_path(&self, module: &str) -> PathBuf {
        let date = Local::now().format("%Y%m%d");
        PathBuf::from(format!(
            "{}/system/{}_{}.log",
            self.config.root_dir, module, date
        ))
    }

    /// 写入日志
    pub fn log(
        &self,
        module: &str,
        level: Level,
        message: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");

        // 格式化日志消息
        let formatted = self
            .config
            .format
            .replace("{timestamp}", &timestamp.to_string())
            .replace("{level}", &level.to_string())
            .replace("{module}", module)
            .replace("{message}", message);

        // 确定日志文件路径
        let log_path = if module.starts_with("strategy_") {
            self.get_strategy_log_path(&module[9..])
        } else {
            self.get_system_log_path(module)
        };

        // 写入文件
        self.write_to_file(&log_path, &formatted)?;

        // 如果需要，同时输出到控制台
        if self.config.console_output {
            println!("{}", formatted);
        }

        Ok(())
    }

    /// 写入文件（带自动轮转）
    fn write_to_file(
        &self,
        path: &PathBuf,
        content: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // 检查文件大小
        if path.exists() {
            let metadata = fs::metadata(path)?;
            let size_mb = metadata.len() / (1024 * 1024);

            if size_mb >= self.config.max_file_size_mb {
                // 轮转日志文件
                self.rotate_log_file(path)?;
            }
        }

        // 打开或创建文件
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        // 写入内容
        writeln!(file, "{}", content)?;
        file.flush()?;

        Ok(())
    }

    /// 轮转日志文件
    fn rotate_log_file(&self, path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let timestamp = Local::now().format("%Y%m%d_%H%M%S");
        let parent = path.parent().unwrap();
        let stem = path.file_stem().unwrap().to_str().unwrap();

        let new_name = format!("{}_{}_rotated.log", stem, timestamp);
        let new_path = parent.join(new_name);

        fs::rename(path, new_path)?;

        Ok(())
    }

    /// 清理过期日志
    pub fn cleanup_old_logs(&self) -> Result<usize, Box<dyn std::error::Error>> {
        use std::time::SystemTime;

        let mut deleted_count = 0;
        let retention_secs = self.config.retention_days as u64 * 24 * 3600;
        let now = SystemTime::now();

        // 遍历所有日志目录
        for subdir in &[
            "strategies",
            "manager",
            "system",
            "api",
            "database",
            "backtest",
        ] {
            let dir_path = format!("{}/{}", self.config.root_dir, subdir);
            if let Ok(entries) = fs::read_dir(&dir_path) {
                for entry in entries.filter_map(Result::ok) {
                    if let Ok(metadata) = entry.metadata() {
                        if metadata.is_file() {
                            if let Ok(modified) = metadata.modified() {
                                if let Ok(age) = now.duration_since(modified) {
                                    if age.as_secs() > retention_secs {
                                        if fs::remove_file(entry.path()).is_ok() {
                                            deleted_count += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(deleted_count)
    }

    /// 获取日志级别
    pub fn get_level_filter(&self) -> LevelFilter {
        match self.config.default_level.to_uppercase().as_str() {
            "DEBUG" => LevelFilter::Debug,
            "INFO" => LevelFilter::Info,
            "WARN" => LevelFilter::Warn,
            "ERROR" => LevelFilter::Error,
            _ => LevelFilter::Info,
        }
    }

    /// 创建或获取策略日志器
    pub fn get_or_create_strategy_logger(&self, strategy_name: &str) -> Arc<StrategyLogger> {
        Arc::new(StrategyLogger::new(
            strategy_name,
            self.config.max_file_size_mb,
        ))
    }

    /// 写策略日志
    pub fn log_strategy(&self, strategy_name: &str, level: Level, message: &str) {
        let mut loggers = self.strategy_loggers.lock().unwrap();

        if !loggers.contains_key(strategy_name) {
            let logger = StrategyLogger::new(strategy_name, self.config.max_file_size_mb);
            loggers.insert(strategy_name.to_string(), logger);
        }

        if let Some(logger) = loggers.get(strategy_name) {
            logger.log(level, message);
        }

        // 如果需要，同时输出到控制台
        if self.config.console_output && level <= self.get_level_filter() {
            let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
            println!(
                "[{}] [{}] [{}] {}",
                timestamp, strategy_name, level, message
            );
        }
    }
}

/// 多策略日志管理器（兼容旧接口）
pub struct MultiStrategyLogger {
    loggers: Vec<(String, StrategyLogger)>,
    console_level: LevelFilter,
}

impl MultiStrategyLogger {
    pub fn new() -> Self {
        Self {
            loggers: Vec::new(),
            console_level: LevelFilter::Info,
        }
    }

    pub fn add_strategy(&mut self, name: &str, max_size_mb: u64) {
        let logger = StrategyLogger::new(name, max_size_mb);
        self.loggers.push((name.to_string(), logger));
    }

    pub fn route_log(&self, record: &Record) {
        let message = format!("{}", record.args());
        let module = record.module_path().unwrap_or("");

        let strategy_name = if module.contains("trend_grid") || message.contains("趋势网格") {
            "trend_grid"
        } else if module.contains("copy_trading") || message.contains("跟单") {
            "copy_trading"
        } else if module.contains("funding_rate") || message.contains("资金费率") {
            "funding_rate"
        } else if module.contains("cross_exchange") || message.contains("跨交易所") {
            "cross_exchange"
        } else if module.contains("poisson") || message.contains("泊松") || message.contains("做市")
        {
            "poisson_mm"
        } else {
            "general"
        };

        for (name, logger) in &self.loggers {
            if name == strategy_name || name == "general" {
                logger.log(record.level(), &message);
            }
        }

        if record.level() <= self.console_level {
            println!("{}", message);
        }
    }
}

impl log::Log for MultiStrategyLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Debug
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            self.route_log(record);
        }
    }

    fn flush(&self) {}
}

/// 初始化策略日志系统（适配旧接口）
pub fn init_strategy_logger(
    strategy_name: &str,
    _log_level: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let env = env_logger::Env::default().filter_or("RUST_LOG", _log_level);

    env_logger::Builder::from_env(env)
        .format(|buf, record| {
            writeln!(
                buf,
                "[{}] [{}] [{}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                record.level(),
                record.target(),
                record.args()
            )
        })
        .init();

    let log_dir = "logs/strategies";
    if !Path::new(log_dir).exists() {
        fs::create_dir_all(log_dir)?;
    }

    let timestamp = Local::now().format("%Y%m%d");
    let log_file = format!("{}/{}_{}.log", log_dir, strategy_name, timestamp);

    Ok(log_file)
}

/// 为特定策略创建独立的日志器
pub fn create_strategy_logger(strategy_name: &str) -> StrategyLogger {
    StrategyLogger::new(strategy_name, 5)
}

/// 获取策略的当前日志文件路径
pub fn get_strategy_log_path(strategy_name: &str) -> String {
    let timestamp = Local::now().format("%Y%m%d");
    format!("logs/strategies/{}_{}.log", strategy_name, timestamp)
}

/// 全局日志实例
static mut GLOBAL_LOGGER: Option<Arc<UnifiedLogger>> = None;

/// 初始化全局日志器
pub fn init_global_logger(config: LogConfig) -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        GLOBAL_LOGGER = Some(Arc::new(UnifiedLogger::new(config)?));
    }
    Ok(())
}

/// 获取全局日志器
pub fn get_logger() -> Option<Arc<UnifiedLogger>> {
    unsafe { GLOBAL_LOGGER.as_ref().map(|l| l.clone()) }
}

/// 便捷的日志宏
#[macro_export]
macro_rules! log_info {
    ($module:expr, $($arg:tt)*) => {
        if let Some(logger) = $crate::utils::unified_logger::get_logger() {
            let _ = logger.log($module, log::Level::Info, &format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! log_error {
    ($module:expr, $($arg:tt)*) => {
        if let Some(logger) = $crate::utils::unified_logger::get_logger() {
            let _ = logger.log($module, log::Level::Error, &format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! log_debug {
    ($module:expr, $($arg:tt)*) => {
        if let Some(logger) = $crate::utils::unified_logger::get_logger() {
            let _ = logger.log($module, log::Level::Debug, &format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! log_warn {
    ($module:expr, $($arg:tt)*) => {
        if let Some(logger) = $crate::utils::unified_logger::get_logger() {
            let _ = logger.log($module, log::Level::Warn, &format!($($arg)*));
        }
    };
}

// 日志文件命名规范：
// - 普通日志: logs/strategies/{策略名}_{YYYYMMDD}.log
// - 轮转日志: logs/strategies/{策略名}_{YYYYMMDD_HHMMSS}_rotated.log
// - 管理器日志: logs/manager/manager_{YYYYMMDD}.log
// - 系统日志: logs/system/system_{YYYYMMDD}.log

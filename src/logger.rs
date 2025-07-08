use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use std::collections::HashMap;

/// 初始化全局日志系统，包含控制台输出和主日志文件
pub fn init_logger() {
    // 控制台输出
    let console = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "{d(%Y-%m-%d %H:%M:%S%.3f)} - {l} - {m}{n}",
        )))
        .build();

    // 主日志文件
    let main_logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "{d(%Y-%m-%d %H:%M:%S%.3f)} - {l} - {m}{n}",
        )))
        .build("log/app.log")
        .unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("console", Box::new(console)))
        .appender(Appender::builder().build("main_logfile", Box::new(main_logfile)))
        .build(
            Root::builder()
                .appender("console")
                .appender("main_logfile")
                .build(LevelFilter::Info),
        )
        .unwrap();

    log4rs::init_config(config).unwrap();
}

/// 为特定策略初始化独立的日志系统
pub fn init_strategy_logger(strategy_names: Vec<String>) {
    // 控制台输出
    let console = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "{d(%Y-%m-%d %H:%M:%S%.3f)} - {l} - [{t}] {m}{n}",
        )))
        .build();

    // 主日志文件
    let main_logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "{d(%Y-%m-%d %H:%M:%S%.3f)} - {l} - [{t}] {m}{n}",
        )))
        .build("log/app.log")
        .unwrap();

    let mut config_builder = Config::builder()
        .appender(Appender::builder().build("console", Box::new(console)))
        .appender(Appender::builder().build("main_logfile", Box::new(main_logfile)));

    // 为每个策略创建独立的日志文件
    for strategy_name in &strategy_names {
        let strategy_logfile = FileAppender::builder()
            .encoder(Box::new(PatternEncoder::new(
                "{d(%Y-%m-%d %H:%M:%S%.3f)} - {l} - {m}{n}",
            )))
            .build(format!("log/{}.log", strategy_name))
            .unwrap();

        let appender_name = format!("{}_logfile", strategy_name);
        config_builder = config_builder
            .appender(Appender::builder().build(&appender_name, Box::new(strategy_logfile)));

        // 为策略创建专用logger
        config_builder = config_builder.logger(
            Logger::builder()
                .appender(&appender_name)
                .appender("console")
                .build(strategy_name, LevelFilter::Info),
        );
    }

    let config = config_builder
        .build(
            Root::builder()
                .appender("console")
                .appender("main_logfile")
                .build(LevelFilter::Info),
        )
        .unwrap();

    log4rs::init_config(config).unwrap();
}

/// 为策略创建专用的日志宏
#[macro_export]
macro_rules! strategy_info {
    ($strategy:expr, $($arg:tt)*) => {
        log::info!(target: $strategy, $($arg)*);
    };
}

#[macro_export]
macro_rules! strategy_warn {
    ($strategy:expr, $($arg:tt)*) => {
        log::warn!(target: $strategy, $($arg)*);
    };
}

#[macro_export]
macro_rules! strategy_error {
    ($strategy:expr, $($arg:tt)*) => {
        log::error!(target: $strategy, $($arg)*);
    };
}

use chrono::{Local, Utc};
use std::fs::{self, OpenOptions};
use std::io::Write;

const LOG_DIR: &str = "logs/strategies";

fn write_to_file(level: &str, symbol: Option<&str>, message: &str) {
    if fs::create_dir_all(LOG_DIR).is_err() {
        return;
    }

    let date = Local::now().format("%Y%m%d");
    let log_path = format!("{}/mean_reversion_{}.log", LOG_DIR, date);
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S%.3f");
    let symbol_part = symbol.unwrap_or("-");
    let line = format!(
        "[{}] [{}] [{}] {}\n",
        timestamp, level, symbol_part, message
    );

    if let Ok(mut handle) = OpenOptions::new().create(true).append(true).open(&log_path) {
        let _ = handle.write_all(line.as_bytes());
    }
}

fn log_with_level(level: &str, symbol: Option<&str>, message: &str) {
    write_to_file(level, symbol, message);

    match level {
        "ERROR" => match symbol {
            Some(sym) => log::error!("[{}] {}", sym, message),
            None => log::error!("{}", message),
        },
        "WARN" => match symbol {
            Some(sym) => log::warn!("[{}] {}", sym, message),
            None => log::warn!("{}", message),
        },
        "INFO" => match symbol {
            Some(sym) => log::info!("[{}] {}", sym, message),
            None => log::info!("{}", message),
        },
        _ => match symbol {
            Some(sym) => log::debug!("[{}] {}", sym, message),
            None => log::debug!("{}", message),
        },
    }
}

pub fn info(symbol: Option<&str>, message: impl AsRef<str>) {
    log_with_level("INFO", symbol, message.as_ref());
}

pub fn warn(symbol: Option<&str>, message: impl AsRef<str>) {
    log_with_level("WARN", symbol, message.as_ref());
}

pub fn error(symbol: Option<&str>, message: impl AsRef<str>) {
    log_with_level("ERROR", symbol, message.as_ref());
}

pub fn debug(symbol: Option<&str>, message: impl AsRef<str>) {
    log_with_level("DEBUG", symbol, message.as_ref());
}

use anyhow::{anyhow, Result};
use serde::Serialize;
use serde_yaml::{Mapping, Value};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use tokio::task;

use crate::core::types::MarketType;
use crate::cta::account_manager::AccountInfo;

use super::{PrecisionManagementConfig, SymbolConfig};

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedPrecision {
    pub tick_size: f64,
    pub step_size: f64,
    pub price_digits: u32,
    pub qty_digits: u32,
    pub min_notional: f64,
}

/// 拉取交易所精度并可选回写配置
pub struct PrecisionManager {
    account: std::sync::Arc<AccountInfo>,
    cfg: PrecisionManagementConfig,
    market_type: MarketType,
}

impl PrecisionManager {
    pub fn new(
        account: std::sync::Arc<AccountInfo>,
        cfg: PrecisionManagementConfig,
        market_type: MarketType,
    ) -> Self {
        Self {
            account,
            cfg,
            market_type,
        }
    }

    pub async fn resolve(&self, symbol_cfg: &mut SymbolConfig) -> Result<ResolvedPrecision> {
        if !self.cfg.enabled {
            return Ok(ResolvedPrecision {
                tick_size: symbol_cfg.tick_size,
                step_size: symbol_cfg.lot_size,
                price_digits: symbol_cfg.price_precision.unwrap_or(4),
                qty_digits: symbol_cfg.quantity_precision.unwrap_or(2),
                min_notional: symbol_cfg.min_notional,
            });
        }

        let exchange_symbol = self.to_exchange_symbol(&symbol_cfg.symbol);

        let info = self
            .account
            .exchange
            .get_symbol_info(&exchange_symbol, self.market_type)
            .await?;

        let resolved = ResolvedPrecision {
            tick_size: info.tick_size,
            step_size: info.step_size,
            price_digits: infer_digits(info.tick_size),
            qty_digits: infer_digits(info.step_size),
            min_notional: info.min_notional.unwrap_or(0.0),
        };

        symbol_cfg.tick_size = resolved.tick_size;
        symbol_cfg.lot_size = resolved.step_size;
        symbol_cfg.price_precision = Some(resolved.price_digits);
        symbol_cfg.quantity_precision = Some(resolved.qty_digits);
        symbol_cfg.min_notional = resolved.min_notional;

        if self.cfg.write_back {
            self.write_back(symbol_cfg, &resolved).await.ok();
        }

        Ok(resolved)
    }

    async fn write_back(
        &self,
        symbol_cfg: &SymbolConfig,
        _resolved: &ResolvedPrecision,
    ) -> Result<()> {
        let Some(path) = &self.cfg.config_path else {
            return Ok(());
        };
        let path = PathBuf::from(path);
        let lock_path = self.cfg.lock_path.as_ref().map(PathBuf::from);
        let backup_suffix = self.cfg.backup_suffix.as_deref().unwrap_or(".bak");
        let symbol_value = serde_yaml::to_value(symbol_cfg)?;

        if let Some(lock) = &lock_path {
            let _ = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(lock);
        }

        if let Ok(existing) = fs::read_to_string(&path) {
            let _ = fs::write(format!("{}{}", path.display(), backup_suffix), existing);
        }

        let doc: Value = match fs::read_to_string(&path) {
            Ok(content) => serde_yaml::from_str(&content).unwrap_or(Value::Mapping(Mapping::new())),
            Err(_) => Value::Mapping(Mapping::new()),
        };

        let mut mapping = match doc {
            Value::Mapping(map) => map,
            _ => Mapping::new(),
        };

        let mut symbols = match mapping.remove(&Value::String("symbols".to_string())) {
            Some(Value::Sequence(seq)) => seq,
            _ => Vec::new(),
        };

        let mut replaced = false;
        for entry in symbols.iter_mut() {
            let sym = entry
                .get("symbol")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if sym == symbol_cfg.symbol {
                *entry = symbol_value.clone();
                replaced = true;
                break;
            }
        }

        if !replaced {
            symbols.push(symbol_value);
        }

        mapping.insert(
            Value::String("symbols".to_string()),
            Value::Sequence(symbols),
        );

        let updated = Value::Mapping(mapping);
        let content = serde_yaml::to_string(&updated)?;

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        file.write_all(content.as_bytes())?;

        Ok(())
    }

    fn to_exchange_symbol(&self, symbol: &str) -> String {
        symbol.replace('/', "").to_uppercase()
    }
}

fn infer_digits(step: f64) -> u32 {
    if step <= 0.0 {
        return 4;
    }
    let mut s = step;
    let mut d = 0;
    while (s.fract() != 0.0) && d < 8 {
        s *= 10.0;
        d += 1;
    }
    d
}

pub fn apply_precision(value: f64, step: f64, digits: u32) -> f64 {
    if step > 0.0 {
        let floored = (value / step).floor() * step;
        truncate_digits(floored, digits)
    } else {
        truncate_digits(value, digits)
    }
}

fn truncate_digits(val: f64, digits: u32) -> f64 {
    let factor = 10f64.powi(digits as i32);
    (val * factor).floor() / factor
}

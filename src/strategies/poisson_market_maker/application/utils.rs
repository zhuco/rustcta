use super::{PoissonMarketMaker, Result};
use crate::core::{error::ExchangeError, types::MarketType};
use crate::strategies::poisson_market_maker::domain::SymbolInfo;

impl PoissonMarketMaker {
    pub(crate) async fn fetch_symbol_info(&self) -> Result<()> {
        log::info!("📋 获取交易对信息...");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| {
                ExchangeError::Other(format!("账户不存在 : {}", self.config.account.account_id))
            })?;

        let market_type = MarketType::Futures;

        match account
            .exchange
            .get_symbol_info(&self.config.trading.symbol, market_type)
            .await
        {
            Ok(info) => {
                let parts: Vec<&str> = self.config.trading.symbol.split('/').collect();
                let base_asset = parts.get(0).unwrap_or(&"").to_string();
                let quote_asset = parts.get(1).unwrap_or(&"USDT").to_string();

                let price_precision = Self::calculate_precision(info.tick_size);
                let quantity_precision = Self::calculate_precision(info.step_size);

                let symbol_info = SymbolInfo {
                    base_asset,
                    quote_asset,
                    tick_size: info.tick_size,
                    step_size: info.step_size,
                    min_notional: info.min_notional.unwrap_or(10.0),
                    price_precision,
                    quantity_precision,
                };

                log::info!("✅ 交易对信息:");
                log::info!("  - 基础货币: {}", symbol_info.base_asset);
                log::info!("  - 报价货币: {}", symbol_info.quote_asset);
                log::info!(
                    "  - 价格精度: {} 位小数 (tick_size: {})",
                    symbol_info.price_precision,
                    info.tick_size
                );
                log::info!(
                    "  - 数量精度: {} 位小数 (step_size: {})",
                    symbol_info.quantity_precision,
                    info.step_size
                );
                log::info!(
                    "  - 最小名义价值: {} {}",
                    symbol_info.min_notional,
                    symbol_info.quote_asset
                );

                *self.symbol_info.write().await = Some(symbol_info);
                Ok(())
            }
            Err(e) => {
                log::warn!("⚠️ 无法获取交易对信息: {}，使用配置文件中的精度设置", e);

                let parts: Vec<&str> = self.config.trading.symbol.split('/').collect();
                let base_asset = parts.get(0).unwrap_or(&"TOKEN").to_string();
                let quote_asset = parts.get(1).unwrap_or(&"USDT").to_string();

                let symbol_info = SymbolInfo {
                    base_asset,
                    quote_asset,
                    tick_size: 1.0 / 10_f64.powi(self.config.trading.price_precision as i32),
                    step_size: 1.0 / 10_f64.powi(self.config.trading.quantity_precision as i32),
                    min_notional: 10.0,
                    price_precision: self.config.trading.price_precision,
                    quantity_precision: self.config.trading.quantity_precision,
                };

                log::info!(
                    "  使用配置文件精度: 价格 {} 位，数量 {} 位",
                    symbol_info.price_precision,
                    symbol_info.quantity_precision
                );

                *self.symbol_info.write().await = Some(symbol_info);
                Ok(())
            }
        }
    }

    pub(crate) fn calculate_precision(step: f64) -> usize {
        if step >= 1.0 {
            0
        } else {
            let s = format!("{:.10}", step);
            let parts: Vec<&str> = s.split('.').collect();
            if parts.len() > 1 {
                parts[1].trim_end_matches('0').len()
            } else {
                0
            }
        }
    }

    pub(crate) async fn get_quote_asset(&self) -> String {
        self.symbol_info
            .read()
            .await
            .as_ref()
            .map(|info| info.quote_asset.clone())
            .unwrap_or_else(|| "USDT".to_string())
    }

    pub(crate) async fn check_position_mode(&self) -> Result<()> {
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| {
                ExchangeError::Other(format!("账户不存在: {}", self.config.account.account_id))
            })?;

        if self.config.account.exchange.eq_ignore_ascii_case("binance") {
            use crate::exchanges::binance::BinanceExchange;

            if let Some(binance) = account.exchange.as_any().downcast_ref::<BinanceExchange>() {
                let is_dual = binance.get_position_mode().await?;
                *self.is_dual_mode.write().await = is_dual;

                log::info!(
                    "✅ Binance账户 {} 持仓模式: {}",
                    self.config.account.account_id,
                    if is_dual {
                        "双向持仓"
                    } else {
                        "单向持仓"
                    }
                );
            }
        }

        Ok(())
    }

    pub(crate) async fn is_dual_position_mode(&self) -> bool {
        *self.is_dual_mode.read().await
    }
}

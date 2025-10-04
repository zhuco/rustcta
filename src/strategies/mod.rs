// 核心策略模块
pub mod automated_scalping;
pub mod avellaneda_stoikov;
pub mod common;
pub mod copy_trading;
pub mod mean_reversion;
pub mod poisson_market_maker;
pub mod range_grid;
pub mod trend;
pub mod trend_following;
pub mod trend_grid_v2;

// 策略trait定义
use crate::core::types::{Order, Ticker, Trade};
use async_trait::async_trait;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[async_trait]
pub trait Strategy: Send + Sync {
    async fn name(&self) -> String;
    async fn on_tick(&self, ticker: Ticker) -> Result<()>;
    async fn on_order_update(&self, order: Order) -> Result<()>;
    async fn on_trade(&self, trade: Trade) -> Result<()>;
    async fn get_status(&self) -> Result<String>;
}

// 导出策略类型
pub use automated_scalping::{ASConfig, AutomatedScalpingStrategy};
pub use avellaneda_stoikov::{ASConfig as AVSConfig, AvellanedaStoikovStrategy};
pub use copy_trading::{CopyTradingConfig, CopyTradingStrategy};
pub use mean_reversion::{MeanReversionConfig, MeanReversionStrategy};
pub use poisson_market_maker::{PoissonMMConfig, PoissonMarketMaker};
pub use range_grid::{RangeGridConfig, RangeGridStrategy};
pub use trend::config::TrendConfig;
pub use trend_following::TrendFollowingStrategy;
pub use trend_grid_v2::{TrendGridConfigV2, TrendGridStrategyV2};

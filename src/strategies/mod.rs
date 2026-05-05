// 核心策略模块
pub mod accumulation;
pub mod automated_scalping;
pub mod avellaneda_stoikov;
pub mod beta_hedge_market_maker;
pub mod common;
pub mod copy_trading;
pub mod grid_scale;
pub mod hedged_grid;
pub mod market_making;
pub mod mean_reversion;
pub mod orderflow;
pub mod poisson_market_maker;
pub mod range_grid;
pub mod short_ladder_live;
pub mod sideways_martingale;
pub mod solusdc_hedged_grid;
pub mod trend;
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
pub use accumulation::{AccumulationConfig, AccumulationStrategy};
pub use automated_scalping::{ASConfig, AutomatedScalpingStrategy};
pub use avellaneda_stoikov::{ASConfig as AVSConfig, AvellanedaStoikovStrategy};
pub use beta_hedge_market_maker::{BetaHedgeMarketMaker, BetaHedgeMarketMakerConfig};
pub use copy_trading::{CopyTradingConfig, CopyTradingStrategy};
pub use grid_scale::{GridScaleConfig, GridScaleStrategy};
pub use hedged_grid::{HedgedGridConfig, HedgedGridStrategy};
pub use market_making::{MarketMakingConfig, ProMarketMakingStrategy};
pub use mean_reversion::{MeanReversionConfig, MeanReversionStrategy};
pub use orderflow::{OrderflowConfig, OrderflowStrategy};
pub use poisson_market_maker::{PoissonMMConfig, PoissonMarketMaker};
pub use range_grid::{RangeGridConfig, RangeGridStrategy};
pub use short_ladder_live::{ShortLadderLiveConfig, ShortLadderLiveStrategy};
pub use sideways_martingale::{SidewaysMartingaleConfig, SidewaysMartingaleStrategy};
pub use solusdc_hedged_grid::GridEngine as SolusdcHedgedGridEngine;
pub use solusdc_hedged_grid::MultiHedgedGridStrategy;
pub use solusdc_hedged_grid::MultiRuntimeConfig as MultiHedgedGridRuntimeConfig;
pub use solusdc_hedged_grid::RuntimeConfig as SolusdcHedgedGridRuntimeConfig;
pub use solusdc_hedged_grid::SimulationEngine as SolusdcHedgedGridSimulation;
pub use solusdc_hedged_grid::SolusdcHedgedGridStrategy;
pub use solusdc_hedged_grid::StrategyConfig as SolusdcHedgedGridEngineConfig;
pub use trend::config::TrendConfig;
pub use trend::TrendIntradayStrategy;
pub use trend_grid_v2::{TrendGridConfigV2, TrendGridStrategyV2};

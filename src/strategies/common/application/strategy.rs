use anyhow::Result;
use async_trait::async_trait;

use super::{deps::StrategyDeps, status::StrategyStatus};

#[async_trait]
pub trait StrategyInstance: Send + Sync {
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    async fn status(&self) -> Result<StrategyStatus>;
}

pub trait Strategy: StrategyInstance + Sized {
    type Config: Send + Sync + 'static;

    fn create(config: Self::Config, deps: StrategyDeps) -> Result<Self>
    where
        Self: Sized;
}

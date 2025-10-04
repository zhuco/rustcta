use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use serde_yaml::Value;

use super::{
    build_unified_risk_evaluator, Strategy, StrategyContext, StrategyDepsBuilder, StrategyInstance,
};
use crate::strategies::mean_reversion::{MeanReversionConfig, MeanReversionStrategy};
use crate::strategies::poisson_market_maker::{PoissonMMConfig, PoissonMarketMaker};
use crate::strategies::range_grid::{
    application as range_application, RangeGridConfig, RangeGridStrategy,
};
use crate::strategies::trend_grid::{application, TrendGridConfigV2, TrendGridStrategyV2};

/// 策略工厂函数签名
pub type StrategyFactoryFn =
    dyn Fn(&Value, &StrategyContext) -> Result<Box<dyn StrategyInstance>> + Send + Sync;

/// 策略注册表，运行时按名称创建策略实例
pub struct StrategyRegistry {
    factories: HashMap<String, Arc<StrategyFactoryFn>>,
}

impl StrategyRegistry {
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    pub fn register<F>(&mut self, name: impl Into<String>, factory: F)
    where
        F: Fn(&Value, &StrategyContext) -> Result<Box<dyn StrategyInstance>>
            + Send
            + Sync
            + 'static,
    {
        self.factories.insert(name.into(), Arc::new(factory));
    }

    pub fn create(
        &self,
        name: &str,
        config: &Value,
        ctx: &StrategyContext,
    ) -> Result<Box<dyn StrategyInstance>> {
        let factory = self
            .factories
            .get(name)
            .ok_or_else(|| anyhow!("未注册的策略类型: {}", name))?;
        factory(config, ctx)
    }
}

impl Default for StrategyRegistry {
    fn default() -> Self {
        let mut registry = StrategyRegistry::new();
        registry.register("trend_grid", trend_grid_factory());
        registry.register("range_grid", range_grid_factory());
        registry.register("poisson", poisson_factory());
        registry.register("mean_reversion", mean_reversion_factory());
        registry
    }
}

fn range_grid_factory(
) -> impl Fn(&Value, &StrategyContext) -> Result<Box<dyn StrategyInstance>> + Send + Sync + 'static
{
    |config_value, ctx| {
        let config: RangeGridConfig = serde_yaml::from_value(config_value.clone())?;
        let risk_limits = range_application::risk::build_limits_from_config(&config);
        let risk_evaluator = build_unified_risk_evaluator(
            config.strategy.name.clone(),
            ctx.global_risk.clone(),
            Some(risk_limits.clone()),
        );

        let mut builder = StrategyDepsBuilder::from_context(ctx);
        builder = builder.with_risk_evaluator(risk_evaluator);
        let deps = builder.build()?;

        let strategy = RangeGridStrategy::create(config, deps)?;
        Ok(Box::new(strategy))
    }
}

fn trend_grid_factory(
) -> impl Fn(&Value, &StrategyContext) -> Result<Box<dyn StrategyInstance>> + Send + Sync + 'static
{
    |config_value, ctx| {
        let config: TrendGridConfigV2 = serde_yaml::from_value(config_value.clone())?;
        let risk_limits = application::risk::build_limits_from_config(&config);
        let risk_evaluator = build_unified_risk_evaluator(
            config.strategy.name.clone(),
            ctx.global_risk.clone(),
            Some(risk_limits.clone()),
        );

        let mut builder = StrategyDepsBuilder::from_context(ctx);
        builder = builder.with_risk_evaluator(risk_evaluator);
        let deps = builder.build()?;

        let strategy = TrendGridStrategyV2::create(config, deps)?;
        Ok(Box::new(strategy))
    }
}

fn poisson_factory(
) -> impl Fn(&Value, &StrategyContext) -> Result<Box<dyn StrategyInstance>> + Send + Sync + 'static
{
    |config_value, ctx| {
        let config: PoissonMMConfig = serde_yaml::from_value(config_value.clone())?;
        let risk_limits = PoissonMarketMaker::build_risk_limits(&config);
        let risk_evaluator = build_unified_risk_evaluator(
            config.name.clone(),
            ctx.global_risk.clone(),
            Some(risk_limits.clone()),
        );

        let mut builder = StrategyDepsBuilder::from_context(ctx);
        builder = builder.with_risk_evaluator(risk_evaluator);
        let deps = builder.build()?;

        let strategy = PoissonMarketMaker::create(config, deps)?;
        Ok(Box::new(strategy))
    }
}

fn mean_reversion_factory(
) -> impl Fn(&Value, &StrategyContext) -> Result<Box<dyn StrategyInstance>> + Send + Sync + 'static
{
    |config_value, ctx| {
        let config: MeanReversionConfig = serde_yaml::from_value(config_value.clone())?;
        let risk_evaluator = build_unified_risk_evaluator(
            config.strategy.name.clone(),
            ctx.global_risk.clone(),
            None,
        );

        let mut builder = StrategyDepsBuilder::from_context(ctx);
        builder = builder.with_risk_evaluator(risk_evaluator);
        let deps = builder.build()?;

        let strategy = MeanReversionStrategy::create(config, deps)?;
        Ok(Box::new(strategy))
    }
}

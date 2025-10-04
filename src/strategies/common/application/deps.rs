use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::analysis::TradeCollector;
use crate::core::risk_manager::GlobalRiskManager;
use crate::cta::account_manager::AccountManager;

use crate::strategies::common::risk::UnifiedRiskEvaluator;

/// 策略上下文，由运行时环境统一构建
#[derive(Clone)]
pub struct StrategyContext {
    pub account_manager: Arc<AccountManager>,
    pub global_risk: Option<Arc<GlobalRiskManager>>,
    pub trade_collector: Option<Arc<TradeCollector>>,
}

impl StrategyContext {
    pub fn new(account_manager: Arc<AccountManager>) -> Self {
        Self {
            account_manager,
            global_risk: None,
            trade_collector: None,
        }
    }

    pub fn with_global_risk(mut self, global_risk: Arc<GlobalRiskManager>) -> Self {
        self.global_risk = Some(global_risk);
        self
    }

    pub fn with_trade_collector(mut self, collector: Arc<TradeCollector>) -> Self {
        self.trade_collector = Some(collector);
        self
    }
}

/// 统一的策略依赖容器
#[derive(Clone)]
pub struct StrategyDeps {
    pub account_manager: Arc<AccountManager>,
    pub risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    pub trade_collector: Option<Arc<TradeCollector>>,
}

impl StrategyDeps {
    pub fn builder() -> StrategyDepsBuilder {
        StrategyDepsBuilder::default()
    }
}

/// 构建策略依赖的辅助结构
#[derive(Default)]
pub struct StrategyDepsBuilder {
    account_manager: Option<Arc<AccountManager>>,
    risk_evaluator: Option<Arc<dyn UnifiedRiskEvaluator>>,
    trade_collector: Option<Arc<TradeCollector>>,
}

impl StrategyDepsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_context(ctx: &StrategyContext) -> Self {
        let mut builder = Self::default();
        builder.account_manager = Some(ctx.account_manager.clone());
        if let Some(collector) = &ctx.trade_collector {
            builder.trade_collector = Some(collector.clone());
        }
        builder
    }

    pub fn with_account_manager(mut self, account_manager: Arc<AccountManager>) -> Self {
        self.account_manager = Some(account_manager);
        self
    }

    pub fn with_risk_evaluator(mut self, risk_evaluator: Arc<dyn UnifiedRiskEvaluator>) -> Self {
        self.risk_evaluator = Some(risk_evaluator);
        self
    }

    pub fn with_trade_collector(mut self, collector: Arc<TradeCollector>) -> Self {
        self.trade_collector = Some(collector);
        self
    }

    pub fn build(self) -> Result<StrategyDeps> {
        Ok(StrategyDeps {
            account_manager: self
                .account_manager
                .ok_or_else(|| anyhow!("StrategyDeps 缺少 account_manager"))?,
            risk_evaluator: self
                .risk_evaluator
                .ok_or_else(|| anyhow!("StrategyDeps 缺少 risk_evaluator"))?,
            trade_collector: self.trade_collector,
        })
    }
}

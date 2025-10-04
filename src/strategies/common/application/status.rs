use chrono::{DateTime, Utc};
use std::time::Duration;

/// 策略运行状态
#[derive(Debug, Clone, PartialEq)]
pub enum StrategyState {
    Initializing,
    Running,
    Stopped,
    Error,
}

/// 风险等级（可映射到全局风控状态）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskLevel {
    Normal,
    Warning,
    Danger,
    Halt,
}

/// 策略的简要持仓信息
#[derive(Debug, Clone, Default)]
pub struct StrategyPosition {
    pub symbol: String,
    pub net_position: f64,
    pub notional: f64,
}

/// 统一的策略状态结构
#[derive(Debug, Clone)]
pub struct StrategyStatus {
    pub name: String,
    pub state: StrategyState,
    pub uptime: Option<Duration>,
    pub risk_level: RiskLevel,
    pub positions: Vec<StrategyPosition>,
    pub updated_at: DateTime<Utc>,
    pub last_error: Option<String>,
}

impl StrategyStatus {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            state: StrategyState::Initializing,
            uptime: None,
            risk_level: RiskLevel::Normal,
            positions: Vec::new(),
            updated_at: Utc::now(),
            last_error: None,
        }
    }

    pub fn with_state(mut self, state: StrategyState) -> Self {
        self.state = state;
        self
    }

    pub fn with_uptime(mut self, uptime: Duration) -> Self {
        self.uptime = Some(uptime);
        self
    }

    pub fn with_risk_level(mut self, risk_level: RiskLevel) -> Self {
        self.risk_level = risk_level;
        self
    }

    pub fn with_positions(mut self, positions: Vec<StrategyPosition>) -> Self {
        self.positions = positions;
        self
    }

    pub fn with_last_error(mut self, last_error: impl Into<String>) -> Self {
        self.last_error = Some(last_error.into());
        self
    }
}

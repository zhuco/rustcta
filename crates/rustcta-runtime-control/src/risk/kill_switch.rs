use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillSwitchConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_initial_state")]
    pub initial_state: String,
    #[serde(default = "default_true")]
    pub allow_paper_trading: bool,
    #[serde(default = "default_true")]
    pub allow_live_dry_run: bool,
    #[serde(default)]
    pub allow_live_orders: bool,
    #[serde(default = "default_true")]
    pub allow_reset: bool,
}

impl Default for KillSwitchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_state: default_initial_state(),
            allow_paper_trading: true,
            allow_live_dry_run: true,
            allow_live_orders: false,
            allow_reset: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillSwitchState {
    pub enabled: bool,
    pub active: bool,
    pub reason: Option<String>,
    pub triggered_by: Option<String>,
    pub triggered_at: Option<DateTime<Utc>>,
    pub allow_paper_trading: bool,
    pub allow_live_dry_run: bool,
    pub allow_live_orders: bool,
}

impl KillSwitchState {
    pub fn from_config(config: &KillSwitchConfig) -> Self {
        let active = config.enabled && config.initial_state.eq_ignore_ascii_case("triggered");
        Self {
            enabled: config.enabled,
            active,
            reason: active.then(|| "initial_state=triggered".to_string()),
            triggered_by: active.then(|| "config".to_string()),
            triggered_at: active.then(Utc::now),
            allow_paper_trading: config.allow_paper_trading && !active,
            allow_live_dry_run: config.allow_live_dry_run && !active,
            allow_live_orders: config.allow_live_orders && !active,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KillSwitch {
    config: KillSwitchConfig,
    state: Arc<RwLock<KillSwitchState>>,
}

impl KillSwitch {
    pub fn new(config: KillSwitchConfig) -> Self {
        let state = KillSwitchState::from_config(&config);
        Self {
            config,
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub fn state(&self) -> KillSwitchState {
        self.state.read().clone()
    }

    pub fn trigger(&self, reason: impl Into<String>, triggered_by: impl Into<String>) {
        let mut state = self.state.write();
        state.enabled = self.config.enabled;
        state.active = self.config.enabled;
        state.reason = Some(reason.into());
        state.triggered_by = Some(triggered_by.into());
        state.triggered_at = Some(Utc::now());
        if state.active {
            state.allow_live_orders = false;
            state.allow_live_dry_run = false;
            state.allow_paper_trading = false;
        }
    }

    pub fn reset(&self) -> bool {
        if !self.config.allow_reset {
            return false;
        }
        let mut state = self.state.write();
        *state = KillSwitchState::from_config(&KillSwitchConfig {
            initial_state: "safe".to_string(),
            ..self.config.clone()
        });
        true
    }
}

fn default_true() -> bool {
    true
}

fn default_initial_state() -> String {
    "safe".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_blocks_live_orders() {
        let kill_switch = KillSwitch::new(KillSwitchConfig::default());
        assert!(!kill_switch.state().allow_live_orders);
        assert!(!kill_switch.state().active);
    }

    #[test]
    fn manual_trigger_blocks_modes() {
        let kill_switch = KillSwitch::new(KillSwitchConfig {
            allow_live_orders: true,
            ..KillSwitchConfig::default()
        });
        kill_switch.trigger("operator", "manual");
        let state = kill_switch.state();
        assert!(state.active);
        assert!(!state.allow_live_orders);
        assert!(!state.allow_live_dry_run);
    }

    #[test]
    fn reset_restores_config_permissions() {
        let kill_switch = KillSwitch::new(KillSwitchConfig {
            allow_live_orders: true,
            ..KillSwitchConfig::default()
        });
        kill_switch.trigger("operator", "manual");
        assert!(kill_switch.reset());
        let state = kill_switch.state();
        assert!(!state.active);
        assert!(state.allow_live_orders);
    }
}

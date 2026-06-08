//! Runtime control, risk, and scanner read-model contracts.
//!
//! This crate intentionally contains no exchange clients or adapter ownership.

pub mod control;
pub mod live_preflight;
pub mod risk;
pub mod scanner;

pub use control::*;
pub use live_preflight::{
    build_report, render_human_report, ApiPermissionState, LivePreflightCheck,
    LivePreflightCheckStatus, LivePreflightConfig, LivePreflightGate, LivePreflightReport,
    LivePreflightSeverity, LiveReadinessDecision, SmallLiveGateCheck, SmallLiveGateConfig,
    SmallLiveGateReport, SmallLiveGateStatus,
};
pub use risk::*;
pub use scanner::*;

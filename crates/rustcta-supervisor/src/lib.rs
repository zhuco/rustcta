use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use thiserror::Error;
use tokio::process::{Child, Command};

pub const SUPERVISOR_SCHEMA_VERSION: u16 = 1;
pub const DEFAULT_LEGACY_PROCESS_WORKING_DIR: &str = ".";
pub const DEFAULT_LEGACY_PROCESS_LOG_DIR: &str = "logs/supervisor";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl ProcessStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleCommand {
    Start,
    Stop,
    Restart,
    Heartbeat,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryPolicy {
    pub restart_on_exit: bool,
    pub restart_on_stale_heartbeat: bool,
    pub max_restart_attempts: u32,
    pub heartbeat_timeout_ms: Option<i64>,
    pub snapshot_timeout_ms: Option<i64>,
    pub restart_backoff_ms: u64,
}

impl Default for RecoveryPolicy {
    fn default() -> Self {
        Self {
            restart_on_exit: false,
            restart_on_stale_heartbeat: false,
            max_restart_attempts: 0,
            heartbeat_timeout_ms: None,
            snapshot_timeout_ms: None,
            restart_backoff_ms: 250,
        }
    }
}

impl RecoveryPolicy {
    pub fn validate(&self) -> Result<(), SupervisorError> {
        if self.heartbeat_timeout_ms.is_some_and(|value| value <= 0) {
            return Err(SupervisorError::InvalidRecoveryPolicy {
                message: "heartbeat_timeout_ms must be positive when configured".to_string(),
            });
        }
        if self.snapshot_timeout_ms.is_some_and(|value| value <= 0) {
            return Err(SupervisorError::InvalidRecoveryPolicy {
                message: "snapshot_timeout_ms must be positive when configured".to_string(),
            });
        }
        if (self.restart_on_exit || self.restart_on_stale_heartbeat)
            && self.max_restart_attempts == 0
        {
            return Err(SupervisorError::InvalidRecoveryPolicy {
                message: "restart policies require max_restart_attempts > 0".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSnapshotMetadata {
    pub schema_version: u16,
    pub snapshot_id: String,
    pub captured_at: DateTime<Utc>,
    pub source: Option<String>,
    pub payload_kind: Option<String>,
    pub payload_bytes: Option<u64>,
    pub checksum: Option<String>,
}

impl RuntimeSnapshotMetadata {
    pub fn new(snapshot_id: impl Into<String>, captured_at: DateTime<Utc>) -> Self {
        Self {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            snapshot_id: snapshot_id.into(),
            captured_at,
            source: None,
            payload_kind: None,
            payload_bytes: None,
            checksum: None,
        }
    }

    pub fn validate(&self) -> Result<(), SupervisorError> {
        if self.schema_version != SUPERVISOR_SCHEMA_VERSION {
            return Err(SupervisorError::InvalidRuntimeUpdate {
                strategy_id: "<snapshot>".to_string(),
                message: format!(
                    "snapshot schema_version {} does not match expected {}",
                    self.schema_version, SUPERVISOR_SCHEMA_VERSION
                ),
            });
        }
        if self.snapshot_id.trim().is_empty() {
            return Err(SupervisorError::MissingLifecycleField {
                field: "snapshot_id",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeHeartbeat {
    pub schema_version: u16,
    pub strategy_id: String,
    pub run_id: Option<String>,
    pub process_id: Option<u32>,
    pub status: Option<ProcessStatus>,
    pub heartbeat_at: DateTime<Utc>,
    pub snapshot: Option<RuntimeSnapshotMetadata>,
}

impl RuntimeHeartbeat {
    pub fn new(strategy_id: impl Into<String>, heartbeat_at: DateTime<Utc>) -> Self {
        Self {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            strategy_id: strategy_id.into(),
            run_id: None,
            process_id: None,
            status: None,
            heartbeat_at,
            snapshot: None,
        }
    }

    pub fn validate(&self) -> Result<(), SupervisorError> {
        if self.schema_version != SUPERVISOR_SCHEMA_VERSION {
            return Err(SupervisorError::InvalidRuntimeUpdate {
                strategy_id: self.strategy_id.clone(),
                message: format!(
                    "heartbeat schema_version {} does not match expected {}",
                    self.schema_version, SUPERVISOR_SCHEMA_VERSION
                ),
            });
        }
        if self.strategy_id.trim().is_empty() {
            return Err(SupervisorError::MissingLifecycleField {
                field: "strategy_id",
            });
        }
        if let Some(snapshot) = &self.snapshot {
            snapshot.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyProcess {
    pub schema_version: u16,
    pub strategy_id: String,
    pub strategy_kind: String,
    pub run_id: String,
    pub tenant_id: String,
    pub config_path: String,
    pub status: ProcessStatus,
    pub process_id: Option<u32>,
    pub started_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub last_snapshot_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub runtime_snapshot: Option<RuntimeSnapshotMetadata>,
    pub restart_count: u32,
    pub last_exit_code: Option<i32>,
    pub last_error: Option<String>,
    pub log_path: Option<String>,
    #[serde(default)]
    pub recovery_policy: RecoveryPolicy,
}

impl StrategyProcess {
    pub fn new(
        strategy_id: impl Into<String>,
        strategy_kind: impl Into<String>,
        run_id: impl Into<String>,
        tenant_id: impl Into<String>,
        config_path: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            strategy_id: strategy_id.into(),
            strategy_kind: strategy_kind.into(),
            run_id: run_id.into(),
            tenant_id: tenant_id.into(),
            config_path: config_path.into(),
            status: ProcessStatus::Starting,
            process_id: None,
            started_at: None,
            last_heartbeat_at: None,
            last_snapshot_at: None,
            runtime_snapshot: None,
            restart_count: 0,
            last_exit_code: None,
            last_error: None,
            log_path: None,
            recovery_policy: RecoveryPolicy::default(),
        }
    }

    pub fn mark_started(mut self, process_id: u32, started_at: DateTime<Utc>) -> Self {
        self.process_id = Some(process_id);
        self.started_at = Some(started_at);
        self.last_heartbeat_at = Some(started_at);
        self.status = ProcessStatus::Running;
        self
    }

    pub fn mark_stopped(mut self, stopped_at: DateTime<Utc>, exit_code: Option<i32>) -> Self {
        self.status = ProcessStatus::Stopped;
        self.process_id = None;
        self.last_heartbeat_at = Some(stopped_at);
        self.last_exit_code = exit_code;
        self
    }

    pub fn mark_failed(mut self, error: impl Into<String>, failed_at: DateTime<Utc>) -> Self {
        self.status = ProcessStatus::Failed;
        self.process_id = None;
        self.last_heartbeat_at = Some(failed_at);
        self.last_error = Some(error.into());
        self
    }

    pub fn with_recovery_policy(mut self, recovery_policy: RecoveryPolicy) -> Self {
        self.recovery_policy = recovery_policy;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyProcessSpec {
    pub schema_version: u16,
    pub strategy_id: String,
    pub strategy_kind: String,
    pub run_id: String,
    pub tenant_id: String,
    pub config_path: String,
    pub command: String,
    pub args: Vec<String>,
    pub working_dir: Option<String>,
    pub log_path: Option<String>,
    pub restart_backoff_ms: u64,
    #[serde(default)]
    pub recovery_policy: RecoveryPolicy,
}

impl StrategyProcessSpec {
    pub fn new(
        strategy_id: impl Into<String>,
        strategy_kind: impl Into<String>,
        run_id: impl Into<String>,
        tenant_id: impl Into<String>,
        config_path: impl Into<String>,
        command: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            strategy_id: strategy_id.into(),
            strategy_kind: strategy_kind.into(),
            run_id: run_id.into(),
            tenant_id: tenant_id.into(),
            config_path: config_path.into(),
            command: command.into(),
            args: Vec::new(),
            working_dir: None,
            log_path: None,
            restart_backoff_ms: 250,
            recovery_policy: RecoveryPolicy::default(),
        }
    }

    pub fn with_args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_working_dir(mut self, working_dir: impl Into<String>) -> Self {
        self.working_dir = Some(working_dir.into());
        self
    }

    pub fn with_log_path(mut self, log_path: impl Into<String>) -> Self {
        self.log_path = Some(log_path.into());
        self
    }

    pub fn with_restart_backoff_ms(mut self, restart_backoff_ms: u64) -> Self {
        self.restart_backoff_ms = restart_backoff_ms;
        self.recovery_policy.restart_backoff_ms = restart_backoff_ms;
        self
    }

    pub fn with_recovery_policy(mut self, recovery_policy: RecoveryPolicy) -> Self {
        self.restart_backoff_ms = recovery_policy.restart_backoff_ms;
        self.recovery_policy = recovery_policy;
        self
    }

    pub fn validate(&self) -> Result<(), SupervisorError> {
        for (field, value) in [
            ("strategy_id", &self.strategy_id),
            ("strategy_kind", &self.strategy_kind),
            ("run_id", &self.run_id),
            ("tenant_id", &self.tenant_id),
            ("command", &self.command),
        ] {
            if value.trim().is_empty() {
                return Err(SupervisorError::MissingLifecycleField { field });
            }
        }
        self.recovery_policy.validate()?;
        Ok(())
    }

    fn to_process(&self) -> StrategyProcess {
        let mut process = StrategyProcess::new(
            self.strategy_id.clone(),
            self.strategy_kind.clone(),
            self.run_id.clone(),
            self.tenant_id.clone(),
            self.config_path.clone(),
        );
        process.log_path = self.log_path.clone();
        process.recovery_policy = self.recovery_policy.clone();
        process
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyProcessTemplate {
    CrossArbLive,
    FundingArbLive,
    SpotSpotLiveDryRun,
    TrendReport,
    AccountPositionReporter,
}

impl LegacyProcessTemplate {
    pub const ALL: [Self; 5] = [
        Self::CrossArbLive,
        Self::FundingArbLive,
        Self::SpotSpotLiveDryRun,
        Self::TrendReport,
        Self::AccountPositionReporter,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::CrossArbLive => "cross_arb_live",
            Self::FundingArbLive => "funding_arb_live",
            Self::SpotSpotLiveDryRun => "spot_spot_live_dry_run",
            Self::TrendReport => "trend_report",
            Self::AccountPositionReporter => "account_position_reporter",
        }
    }

    pub fn strategy_kind(self) -> &'static str {
        match self {
            Self::CrossArbLive => "cross_exchange_arbitrage",
            Self::FundingArbLive => "funding_arbitrage",
            Self::SpotSpotLiveDryRun => "spot_spot_taker_arbitrage",
            Self::TrendReport => "trend_report",
            Self::AccountPositionReporter => "account_position_report",
        }
    }

    pub fn default_config_path(self) -> &'static str {
        match self {
            Self::CrossArbLive => "config/cross_exchange_arbitrage_usdt.yml",
            Self::FundingArbLive => "config/funding_rate_arbitrage_usdt.yml",
            Self::SpotSpotLiveDryRun => "config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml",
            Self::TrendReport => "config/trend_report.yml",
            Self::AccountPositionReporter => "config/account_position_reporter.yml",
        }
    }

    pub fn default_command(self) -> &'static str {
        "cargo"
    }

    pub fn default_args(self, config_path: &str) -> Vec<String> {
        match self {
            Self::CrossArbLive => vec![
                "run".to_string(),
                "--bin".to_string(),
                "cross_arb_live".to_string(),
                "--".to_string(),
                "--config".to_string(),
                config_path.to_string(),
            ],
            Self::FundingArbLive => vec![
                "run".to_string(),
                "--bin".to_string(),
                "funding_arb_live".to_string(),
                "--".to_string(),
                "--config".to_string(),
                config_path.to_string(),
            ],
            Self::SpotSpotLiveDryRun => vec![
                "run".to_string(),
                "--bin".to_string(),
                "rustcta".to_string(),
                "--".to_string(),
                "--strategy".to_string(),
                "spot_spot_taker_arbitrage".to_string(),
                "--config".to_string(),
                config_path.to_string(),
            ],
            Self::TrendReport => vec![
                "run".to_string(),
                "--bin".to_string(),
                "trend_report".to_string(),
                "--".to_string(),
                "--config".to_string(),
                config_path.to_string(),
            ],
            Self::AccountPositionReporter => vec![
                "run".to_string(),
                "--bin".to_string(),
                "account_position_reporter".to_string(),
                "--".to_string(),
                "--config".to_string(),
                config_path.to_string(),
            ],
        }
    }
}

impl std::str::FromStr for LegacyProcessTemplate {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().replace('-', "_").to_ascii_lowercase();
        match normalized.as_str() {
            "cross_arb_live" | "cross_exchange_arbitrage" | "cross_exchange_arbitrage_live" => {
                Ok(Self::CrossArbLive)
            }
            "funding_arb_live" | "funding_arbitrage" | "funding_rate_arbitrage_live" => {
                Ok(Self::FundingArbLive)
            }
            "spot_spot_live_dry_run"
            | "spot_spot"
            | "spot_spot_arbitrage"
            | "spot_spot_taker_arbitrage"
            | "spot_spot_taker_arbitrage_live_dry_run" => Ok(Self::SpotSpotLiveDryRun),
            "trend_report" | "trend" => Ok(Self::TrendReport),
            "account_position_reporter" | "account_position" | "account_position_report" => {
                Ok(Self::AccountPositionReporter)
            }
            _ => Err(format!("unknown legacy process template: {value}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyProcessSpecOptions {
    pub template: LegacyProcessTemplate,
    pub strategy_id: Option<String>,
    pub run_id: Option<String>,
    pub tenant_id: Option<String>,
    pub config_path: Option<String>,
    pub working_dir: Option<String>,
    pub log_dir: Option<String>,
    pub restart_backoff_ms: Option<u64>,
}

impl LegacyProcessSpecOptions {
    pub fn new(template: LegacyProcessTemplate) -> Self {
        Self {
            template,
            strategy_id: None,
            run_id: None,
            tenant_id: None,
            config_path: None,
            working_dir: None,
            log_dir: None,
            restart_backoff_ms: None,
        }
    }
}

pub fn build_legacy_process_spec(options: LegacyProcessSpecOptions) -> StrategyProcessSpec {
    let template = options.template;
    let strategy_id = options
        .strategy_id
        .unwrap_or_else(|| template.as_str().to_string());
    let run_id = options.run_id.unwrap_or_else(|| "local".to_string());
    let tenant_id = options.tenant_id.unwrap_or_else(|| "local".to_string());
    let config_path = options
        .config_path
        .unwrap_or_else(|| template.default_config_path().to_string());
    let working_dir = options
        .working_dir
        .unwrap_or_else(|| DEFAULT_LEGACY_PROCESS_WORKING_DIR.to_string());
    let log_dir = options
        .log_dir
        .unwrap_or_else(|| DEFAULT_LEGACY_PROCESS_LOG_DIR.to_string());
    let log_path = format!("{log_dir}/{strategy_id}.log");

    StrategyProcessSpec::new(
        strategy_id,
        template.strategy_kind(),
        run_id,
        tenant_id,
        config_path.clone(),
        template.default_command(),
    )
    .with_args(template.default_args(&config_path))
    .with_working_dir(working_dir)
    .with_log_path(log_path)
    .with_restart_backoff_ms(options.restart_backoff_ms.unwrap_or(5_000))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleCommandRecord {
    pub schema_version: u16,
    pub command_id: String,
    pub strategy_id: String,
    pub run_id: Option<String>,
    pub command: LifecycleCommand,
    pub requested_by: Option<String>,
    pub idempotency_key: String,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorSnapshot {
    pub schema_version: u16,
    pub captured_at: DateTime<Utc>,
    pub processes: Vec<StrategyProcess>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeHeartbeatStatus {
    pub schema_version: u16,
    pub strategy_id: String,
    pub run_id: String,
    pub status: ProcessStatus,
    pub process_id: Option<u32>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub heartbeat_age_ms: Option<i64>,
    pub last_snapshot_at: Option<DateTime<Utc>>,
    pub snapshot_age_ms: Option<i64>,
    pub runtime_snapshot: Option<RuntimeSnapshotMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSnapshotStatus {
    pub schema_version: u16,
    pub strategy_id: String,
    pub run_id: String,
    pub last_snapshot_at: Option<DateTime<Utc>>,
    pub snapshot_age_ms: Option<i64>,
    pub runtime_snapshot: Option<RuntimeSnapshotMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryAction {
    None,
    MarkFailed,
    Restart,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessRecoveryStatus {
    pub schema_version: u16,
    pub strategy_id: String,
    pub run_id: String,
    pub status: ProcessStatus,
    pub restart_count: u32,
    pub policy: RecoveryPolicy,
    pub heartbeat_age_ms: Option<i64>,
    pub snapshot_age_ms: Option<i64>,
    pub action: RecoveryAction,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorRecoveryStatus {
    pub schema_version: u16,
    pub captured_at: DateTime<Utc>,
    pub processes: Vec<ProcessRecoveryStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvalidStrategyProcess {
    pub strategy_id: String,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorRegistryValidation {
    pub valid: bool,
    pub schema_version: u16,
    pub process_count: usize,
    pub starting: usize,
    pub running: usize,
    pub stopping: usize,
    pub stopped: usize,
    pub failed: usize,
    pub invalid_processes: Vec<InvalidStrategyProcess>,
}

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("strategy process already exists: {strategy_id}")]
    AlreadyExists { strategy_id: String },
    #[error("strategy process not found: {strategy_id}")]
    NotFound { strategy_id: String },
    #[error("missing lifecycle command field {field}")]
    MissingLifecycleField { field: &'static str },
    #[error("process spawn failed for {strategy_id}: {message}")]
    SpawnFailed {
        strategy_id: String,
        message: String,
    },
    #[error("process operation failed for {strategy_id}: {message}")]
    ProcessOperation {
        strategy_id: String,
        message: String,
    },
    #[error("process already running: {strategy_id}")]
    AlreadyRunning { strategy_id: String },
    #[error("registry storage error: {message}")]
    RegistryStorage { message: String },
    #[error("invalid runtime update for {strategy_id}: {message}")]
    InvalidRuntimeUpdate {
        strategy_id: String,
        message: String,
    },
    #[error("invalid recovery policy: {message}")]
    InvalidRecoveryPolicy { message: String },
}

#[derive(Debug, Default)]
pub struct ProcessRegistry {
    processes: BTreeMap<String, StrategyProcess>,
}

impl ProcessRegistry {
    pub fn insert(&mut self, process: StrategyProcess) -> Result<(), SupervisorError> {
        if self.processes.contains_key(&process.strategy_id) {
            return Err(SupervisorError::AlreadyExists {
                strategy_id: process.strategy_id,
            });
        }
        self.processes.insert(process.strategy_id.clone(), process);
        Ok(())
    }

    pub fn upsert(&mut self, process: StrategyProcess) {
        self.processes.insert(process.strategy_id.clone(), process);
    }

    pub fn get(&self, strategy_id: &str) -> Option<&StrategyProcess> {
        self.processes.get(strategy_id)
    }

    pub fn get_mut(&mut self, strategy_id: &str) -> Option<&mut StrategyProcess> {
        self.processes.get_mut(strategy_id)
    }

    pub fn remove(&mut self, strategy_id: &str) -> Option<StrategyProcess> {
        self.processes.remove(strategy_id)
    }

    pub fn apply_lifecycle_command(
        &mut self,
        record: LifecycleCommandRecord,
    ) -> Result<(), SupervisorError> {
        record.validate()?;
        match record.command {
            LifecycleCommand::Start => {
                let run_id = record
                    .run_id
                    .clone()
                    .unwrap_or_else(|| format!("run-{}", record.command_id));
                let process =
                    StrategyProcess::new(record.strategy_id, "unassigned", run_id, "local", "");
                self.insert(process)
            }
            LifecycleCommand::Stop => self.set_status(&record.strategy_id, ProcessStatus::Stopped),
            LifecycleCommand::Restart => {
                self.set_status(&record.strategy_id, ProcessStatus::Starting)
            }
            LifecycleCommand::Heartbeat => self.heartbeat(&record.strategy_id, record.requested_at),
        }
    }

    pub fn set_status(
        &mut self,
        strategy_id: &str,
        status: ProcessStatus,
    ) -> Result<(), SupervisorError> {
        let process =
            self.processes
                .get_mut(strategy_id)
                .ok_or_else(|| SupervisorError::NotFound {
                    strategy_id: strategy_id.to_string(),
                })?;
        process.status = status;
        Ok(())
    }

    pub fn heartbeat(
        &mut self,
        strategy_id: &str,
        heartbeat_at: DateTime<Utc>,
    ) -> Result<(), SupervisorError> {
        let process =
            self.processes
                .get_mut(strategy_id)
                .ok_or_else(|| SupervisorError::NotFound {
                    strategy_id: strategy_id.to_string(),
                })?;
        process.last_heartbeat_at = Some(heartbeat_at);
        Ok(())
    }

    pub fn ingest_runtime_heartbeat(
        &mut self,
        heartbeat: RuntimeHeartbeat,
    ) -> Result<StrategyProcess, SupervisorError> {
        heartbeat.validate()?;
        let process = self
            .processes
            .get_mut(&heartbeat.strategy_id)
            .ok_or_else(|| SupervisorError::NotFound {
                strategy_id: heartbeat.strategy_id.clone(),
            })?;
        if let Some(run_id) = &heartbeat.run_id {
            if run_id != &process.run_id {
                return Err(SupervisorError::InvalidRuntimeUpdate {
                    strategy_id: heartbeat.strategy_id,
                    message: format!(
                        "heartbeat run_id {run_id} does not match registry run_id {}",
                        process.run_id
                    ),
                });
            }
        }
        if let Some(process_id) = heartbeat.process_id {
            process.process_id = Some(process_id);
        }
        if let Some(status) = heartbeat.status {
            process.status = status;
        }
        process.last_heartbeat_at = Some(heartbeat.heartbeat_at);
        if let Some(snapshot) = heartbeat.snapshot {
            process.last_snapshot_at = Some(snapshot.captured_at);
            process.runtime_snapshot = Some(snapshot);
        }
        Ok(process.clone())
    }

    pub fn snapshot(&self) -> SupervisorSnapshot {
        SupervisorSnapshot {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            captured_at: Utc::now(),
            processes: self.list(),
        }
    }

    pub fn heartbeat_statuses(&self, now: DateTime<Utc>) -> Vec<RuntimeHeartbeatStatus> {
        self.processes
            .values()
            .map(|process| process.heartbeat_status(now))
            .collect()
    }

    pub fn snapshot_statuses(&self, now: DateTime<Utc>) -> Vec<RuntimeSnapshotStatus> {
        self.processes
            .values()
            .map(|process| process.snapshot_status(now))
            .collect()
    }

    pub fn recovery_status(&self, now: DateTime<Utc>) -> SupervisorRecoveryStatus {
        SupervisorRecoveryStatus {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            captured_at: now,
            processes: self
                .processes
                .values()
                .map(|process| process.recovery_status(now))
                .collect(),
        }
    }

    pub fn validation_report(&self) -> SupervisorRegistryValidation {
        SupervisorRegistryValidation::from_processes(&self.list())
    }

    pub fn list(&self) -> Vec<StrategyProcess> {
        self.processes.values().cloned().collect()
    }

    pub fn from_snapshot(snapshot: SupervisorSnapshot) -> Self {
        let mut registry = Self::default();
        for process in snapshot.processes {
            registry.upsert(process);
        }
        registry
    }

    pub fn from_processes(processes: &[StrategyProcess]) -> Self {
        let mut registry = Self::default();
        for process in processes {
            registry.upsert(process.clone());
        }
        registry
    }
}

impl StrategyProcess {
    pub fn heartbeat_status(&self, now: DateTime<Utc>) -> RuntimeHeartbeatStatus {
        RuntimeHeartbeatStatus {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            strategy_id: self.strategy_id.clone(),
            run_id: self.run_id.clone(),
            status: self.status.clone(),
            process_id: self.process_id,
            last_heartbeat_at: self.last_heartbeat_at,
            heartbeat_age_ms: age_ms(self.last_heartbeat_at, now),
            last_snapshot_at: self.last_snapshot_at,
            snapshot_age_ms: age_ms(self.last_snapshot_at, now),
            runtime_snapshot: self.runtime_snapshot.clone(),
        }
    }

    pub fn snapshot_status(&self, now: DateTime<Utc>) -> RuntimeSnapshotStatus {
        RuntimeSnapshotStatus {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            strategy_id: self.strategy_id.clone(),
            run_id: self.run_id.clone(),
            last_snapshot_at: self.last_snapshot_at,
            snapshot_age_ms: age_ms(self.last_snapshot_at, now),
            runtime_snapshot: self.runtime_snapshot.clone(),
        }
    }

    pub fn recovery_status(&self, now: DateTime<Utc>) -> ProcessRecoveryStatus {
        let heartbeat_age_ms = age_ms(self.last_heartbeat_at, now);
        let snapshot_age_ms = age_ms(self.last_snapshot_at, now);
        let stale_heartbeat =
            is_age_over(heartbeat_age_ms, self.recovery_policy.heartbeat_timeout_ms);
        let stale_snapshot = is_age_over(snapshot_age_ms, self.recovery_policy.snapshot_timeout_ms);
        let restart_budget_available =
            self.restart_count < self.recovery_policy.max_restart_attempts;

        let (action, reason) = if stale_heartbeat && self.status == ProcessStatus::Running {
            if self.recovery_policy.restart_on_stale_heartbeat && restart_budget_available {
                (RecoveryAction::Restart, Some("heartbeat stale".to_string()))
            } else {
                (
                    RecoveryAction::MarkFailed,
                    Some("heartbeat stale".to_string()),
                )
            }
        } else if self.status == ProcessStatus::Failed
            && self.recovery_policy.restart_on_exit
            && restart_budget_available
        {
            (RecoveryAction::Restart, Some("process failed".to_string()))
        } else if stale_snapshot {
            (RecoveryAction::None, Some("snapshot stale".to_string()))
        } else {
            (RecoveryAction::None, None)
        };

        ProcessRecoveryStatus {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            strategy_id: self.strategy_id.clone(),
            run_id: self.run_id.clone(),
            status: self.status.clone(),
            restart_count: self.restart_count,
            policy: self.recovery_policy.clone(),
            heartbeat_age_ms,
            snapshot_age_ms,
            action,
            reason,
        }
    }
}

fn age_ms(timestamp: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Option<i64> {
    timestamp.map(|timestamp| (now - timestamp).num_milliseconds().max(0))
}

fn is_age_over(age_ms: Option<i64>, timeout_ms: Option<i64>) -> bool {
    match (age_ms, timeout_ms) {
        (Some(age_ms), Some(timeout_ms)) => age_ms > timeout_ms,
        _ => false,
    }
}

impl SupervisorRegistryValidation {
    pub fn from_processes(processes: &[StrategyProcess]) -> Self {
        let mut report = Self {
            valid: true,
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            process_count: processes.len(),
            starting: 0,
            running: 0,
            stopping: 0,
            stopped: 0,
            failed: 0,
            invalid_processes: Vec::new(),
        };

        for process in processes {
            match process.status {
                ProcessStatus::Starting => report.starting += 1,
                ProcessStatus::Running => report.running += 1,
                ProcessStatus::Stopping => report.stopping += 1,
                ProcessStatus::Stopped => report.stopped += 1,
                ProcessStatus::Failed => report.failed += 1,
            }

            let errors = validate_registry_process(process);
            if !errors.is_empty() {
                report.invalid_processes.push(InvalidStrategyProcess {
                    strategy_id: if process.strategy_id.trim().is_empty() {
                        "<empty>".to_string()
                    } else {
                        process.strategy_id.clone()
                    },
                    errors,
                });
            }
        }

        report.valid = report.invalid_processes.is_empty();
        report
    }
}

fn validate_registry_process(process: &StrategyProcess) -> Vec<String> {
    let mut errors = Vec::new();
    if process.schema_version != SUPERVISOR_SCHEMA_VERSION {
        errors.push(format!(
            "schema_version {} does not match expected {}",
            process.schema_version, SUPERVISOR_SCHEMA_VERSION
        ));
    }
    for (field, value) in [
        ("strategy_id", &process.strategy_id),
        ("strategy_kind", &process.strategy_kind),
        ("run_id", &process.run_id),
        ("tenant_id", &process.tenant_id),
        ("config_path", &process.config_path),
    ] {
        if value.trim().is_empty() {
            errors.push(format!("{field} is required"));
        }
    }
    errors
}

#[derive(Debug, Clone)]
pub struct JsonFileProcessRegistryStore {
    path: PathBuf,
}

impl JsonFileProcessRegistryStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<ProcessRegistry, SupervisorError> {
        if !self.path.exists() {
            return Ok(ProcessRegistry::default());
        }
        let raw = std::fs::read_to_string(&self.path).map_err(|error| {
            SupervisorError::RegistryStorage {
                message: format!("failed to read registry {}: {error}", self.path.display()),
            }
        })?;
        if raw.trim().is_empty() {
            return Ok(ProcessRegistry::default());
        }
        let snapshot = serde_json::from_str::<SupervisorSnapshot>(&raw).map_err(|error| {
            SupervisorError::RegistryStorage {
                message: format!("failed to parse registry {}: {error}", self.path.display()),
            }
        })?;
        Ok(ProcessRegistry::from_snapshot(snapshot))
    }

    pub fn save(&self, registry: &ProcessRegistry) -> Result<(), SupervisorError> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| SupervisorError::RegistryStorage {
                message: format!(
                    "failed to create registry directory {}: {error}",
                    parent.display()
                ),
            })?;
        }

        let snapshot = registry.snapshot();
        let raw = serde_json::to_vec_pretty(&snapshot).map_err(|error| {
            SupervisorError::RegistryStorage {
                message: format!("failed to serialize registry snapshot: {error}"),
            }
        })?;
        let temp_path = self.path.with_extension("tmp");
        std::fs::write(&temp_path, raw).map_err(|error| SupervisorError::RegistryStorage {
            message: format!(
                "failed to write temp registry {}: {error}",
                temp_path.display()
            ),
        })?;
        std::fs::rename(&temp_path, &self.path).map_err(|error| {
            SupervisorError::RegistryStorage {
                message: format!(
                    "failed to replace registry {} with {}: {error}",
                    self.path.display(),
                    temp_path.display()
                ),
            }
        })?;
        Ok(())
    }
}

#[derive(Debug)]
struct ManagedChild {
    spec: StrategyProcessSpec,
    child: Child,
}

#[derive(Debug, Default)]
pub struct LocalProcessSupervisor {
    registry: ProcessRegistry,
    specs: BTreeMap<String, StrategyProcessSpec>,
    children: BTreeMap<String, ManagedChild>,
}

impl LocalProcessSupervisor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_registry(registry: ProcessRegistry) -> Self {
        Self {
            registry,
            specs: BTreeMap::new(),
            children: BTreeMap::new(),
        }
    }

    pub fn registry(&self) -> &ProcessRegistry {
        &self.registry
    }

    pub fn register(&mut self, process: StrategyProcess) -> Result<(), SupervisorError> {
        self.registry.insert(process)
    }

    pub fn remember_spec(&mut self, spec: StrategyProcessSpec) {
        self.specs.insert(spec.strategy_id.clone(), spec);
    }

    pub fn spec(&self, strategy_id: &str) -> Option<&StrategyProcessSpec> {
        self.specs.get(strategy_id)
    }

    pub fn snapshot(&self) -> SupervisorSnapshot {
        self.registry.snapshot()
    }

    pub async fn start(
        &mut self,
        spec: StrategyProcessSpec,
    ) -> Result<StrategyProcess, SupervisorError> {
        spec.validate()?;
        if self.children.contains_key(&spec.strategy_id) {
            return Err(SupervisorError::AlreadyRunning {
                strategy_id: spec.strategy_id,
            });
        }

        let now = Utc::now();
        let mut command = Command::new(&spec.command);
        command.args(&spec.args);
        if let Some(working_dir) = &spec.working_dir {
            command.current_dir(working_dir);
        }
        if let Some(log_path) = &spec.log_path {
            let log_file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(PathBuf::from(log_path))
                .map_err(|error| SupervisorError::ProcessOperation {
                    strategy_id: spec.strategy_id.clone(),
                    message: error.to_string(),
                })?;
            let stderr_file =
                log_file
                    .try_clone()
                    .map_err(|error| SupervisorError::ProcessOperation {
                        strategy_id: spec.strategy_id.clone(),
                        message: error.to_string(),
                    })?;
            command.stdout(Stdio::from(log_file));
            command.stderr(Stdio::from(stderr_file));
        } else {
            command.stdout(Stdio::null());
            command.stderr(Stdio::null());
        }

        let child = command
            .spawn()
            .map_err(|error| SupervisorError::SpawnFailed {
                strategy_id: spec.strategy_id.clone(),
                message: error.to_string(),
            })?;
        let process_id = child.id().ok_or_else(|| SupervisorError::SpawnFailed {
            strategy_id: spec.strategy_id.clone(),
            message: "spawned child did not expose a process id".to_string(),
        })?;
        let mut process = spec.to_process().mark_started(process_id, now);
        process.status = ProcessStatus::Running;
        self.registry.upsert(process.clone());
        self.specs.insert(spec.strategy_id.clone(), spec.clone());
        self.children
            .insert(spec.strategy_id.clone(), ManagedChild { spec, child });
        Ok(process)
    }

    pub async fn stop(&mut self, strategy_id: &str) -> Result<StrategyProcess, SupervisorError> {
        let mut managed =
            self.children
                .remove(strategy_id)
                .ok_or_else(|| SupervisorError::NotFound {
                    strategy_id: strategy_id.to_string(),
                })?;
        self.registry
            .set_status(strategy_id, ProcessStatus::Stopping)
            .ok();

        match managed.child.try_wait() {
            Ok(Some(status)) => {
                let process = self.mark_process_stopped(strategy_id, status.code())?;
                Ok(process)
            }
            Ok(None) => {
                managed
                    .child
                    .kill()
                    .await
                    .map_err(|error| SupervisorError::ProcessOperation {
                        strategy_id: strategy_id.to_string(),
                        message: error.to_string(),
                    })?;
                let status = managed.child.wait().await.map_err(|error| {
                    SupervisorError::ProcessOperation {
                        strategy_id: strategy_id.to_string(),
                        message: error.to_string(),
                    }
                })?;
                let process = self.mark_process_stopped(strategy_id, status.code())?;
                Ok(process)
            }
            Err(error) => Err(SupervisorError::ProcessOperation {
                strategy_id: strategy_id.to_string(),
                message: error.to_string(),
            }),
        }
    }

    pub async fn restart(&mut self, strategy_id: &str) -> Result<StrategyProcess, SupervisorError> {
        let spec = self
            .children
            .get(strategy_id)
            .map(|managed| managed.spec.clone())
            .or_else(|| self.specs.get(strategy_id).cloned())
            .ok_or_else(|| SupervisorError::NotFound {
                strategy_id: strategy_id.to_string(),
            })?;
        let restart_count = self
            .registry
            .get(strategy_id)
            .map(|process| process.restart_count.saturating_add(1))
            .unwrap_or(1);
        if self.children.contains_key(strategy_id) {
            let _ = self.stop(strategy_id).await?;
        }
        if spec.restart_backoff_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(spec.restart_backoff_ms)).await;
        }
        let mut process = self.start(spec).await?;
        process.restart_count = restart_count;
        if let Some(stored) = self.registry.get_mut(strategy_id) {
            stored.restart_count = restart_count;
            process = stored.clone();
        }
        Ok(process)
    }

    pub fn heartbeat(
        &mut self,
        strategy_id: &str,
        heartbeat_at: DateTime<Utc>,
    ) -> Result<(), SupervisorError> {
        self.registry.heartbeat(strategy_id, heartbeat_at)
    }

    pub fn ingest_runtime_heartbeat(
        &mut self,
        heartbeat: RuntimeHeartbeat,
    ) -> Result<StrategyProcess, SupervisorError> {
        self.registry.ingest_runtime_heartbeat(heartbeat)
    }

    pub fn mark_stale_heartbeats(
        &mut self,
        max_age_ms: i64,
        now: DateTime<Utc>,
    ) -> Vec<StrategyProcess> {
        let mut stale = Vec::new();
        for process in self.registry.processes.values_mut() {
            if process.status != ProcessStatus::Running {
                continue;
            }
            let Some(last_heartbeat_at) = process.last_heartbeat_at else {
                continue;
            };
            if (now - last_heartbeat_at).num_milliseconds() > max_age_ms {
                process.status = ProcessStatus::Failed;
                process.last_error = Some("heartbeat stale".to_string());
                stale.push(process.clone());
            }
        }
        stale
    }

    pub fn recovery_status(&self, now: DateTime<Utc>) -> SupervisorRecoveryStatus {
        self.registry.recovery_status(now)
    }

    pub async fn reap_exited(&mut self) -> Result<Vec<StrategyProcess>, SupervisorError> {
        let strategy_ids = self.children.keys().cloned().collect::<Vec<_>>();
        let mut exited = Vec::new();
        for strategy_id in strategy_ids {
            let Some(managed) = self.children.get_mut(&strategy_id) else {
                continue;
            };
            match managed.child.try_wait() {
                Ok(Some(status)) => {
                    self.children.remove(&strategy_id);
                    exited.push(self.mark_process_stopped(&strategy_id, status.code())?);
                }
                Ok(None) => {}
                Err(error) => {
                    return Err(SupervisorError::ProcessOperation {
                        strategy_id,
                        message: error.to_string(),
                    });
                }
            }
        }
        Ok(exited)
    }

    fn mark_process_stopped(
        &mut self,
        strategy_id: &str,
        exit_code: Option<i32>,
    ) -> Result<StrategyProcess, SupervisorError> {
        let process = self
            .registry
            .get(strategy_id)
            .cloned()
            .ok_or_else(|| SupervisorError::NotFound {
                strategy_id: strategy_id.to_string(),
            })?
            .mark_stopped(Utc::now(), exit_code);
        self.registry.upsert(process.clone());
        Ok(process)
    }
}

impl LifecycleCommandRecord {
    pub fn validate(&self) -> Result<(), SupervisorError> {
        for (field, value) in [
            ("command_id", &self.command_id),
            ("strategy_id", &self.strategy_id),
            ("idempotency_key", &self.idempotency_key),
        ] {
            if value.trim().is_empty() {
                return Err(SupervisorError::MissingLifecycleField { field });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn registry_should_reject_duplicate_strategy_id() {
        let mut registry = ProcessRegistry::default();
        let process = StrategyProcess::new("s1", "mock", "r1", "t1", "config.yml");
        registry.insert(process.clone()).unwrap();
        assert!(matches!(
            registry.insert(process),
            Err(SupervisorError::AlreadyExists { .. })
        ));
    }

    #[test]
    fn lifecycle_command_should_update_heartbeat() {
        let mut registry = ProcessRegistry::default();
        registry
            .insert(StrategyProcess::new("s1", "mock", "r1", "t1", "config.yml"))
            .unwrap();
        let now = Utc::now();

        registry
            .apply_lifecycle_command(LifecycleCommandRecord {
                schema_version: SUPERVISOR_SCHEMA_VERSION,
                command_id: "cmd-1".to_string(),
                strategy_id: "s1".to_string(),
                run_id: Some("r1".to_string()),
                command: LifecycleCommand::Heartbeat,
                requested_by: Some("test".to_string()),
                idempotency_key: "idem-1".to_string(),
                requested_at: now,
            })
            .unwrap();

        assert_eq!(registry.list()[0].last_heartbeat_at, Some(now));
    }

    #[test]
    fn runtime_heartbeat_should_update_process_and_snapshot_metadata() {
        let mut registry = ProcessRegistry::default();
        registry
            .insert(
                StrategyProcess::new("s1", "mock", "run-1", "tenant", "config.yml")
                    .mark_started(7, Utc::now()),
            )
            .unwrap();
        let heartbeat_at = Utc::now();
        let mut snapshot = RuntimeSnapshotMetadata::new("snap-1", heartbeat_at);
        snapshot.payload_kind = Some("strategy_state".to_string());
        snapshot.payload_bytes = Some(128);

        let updated = registry
            .ingest_runtime_heartbeat(RuntimeHeartbeat {
                schema_version: SUPERVISOR_SCHEMA_VERSION,
                strategy_id: "s1".to_string(),
                run_id: Some("run-1".to_string()),
                process_id: Some(9),
                status: Some(ProcessStatus::Running),
                heartbeat_at,
                snapshot: Some(snapshot.clone()),
            })
            .expect("heartbeat should update registry process");

        assert_eq!(updated.process_id, Some(9));
        assert_eq!(updated.last_heartbeat_at, Some(heartbeat_at));
        assert_eq!(updated.last_snapshot_at, Some(heartbeat_at));
        assert_eq!(updated.runtime_snapshot, Some(snapshot));
    }

    #[test]
    fn runtime_heartbeat_should_reject_mismatched_run_id() {
        let mut registry = ProcessRegistry::default();
        registry
            .insert(StrategyProcess::new(
                "s1",
                "mock",
                "registry-run",
                "tenant",
                "config.yml",
            ))
            .unwrap();

        let error = registry
            .ingest_runtime_heartbeat(RuntimeHeartbeat {
                schema_version: SUPERVISOR_SCHEMA_VERSION,
                strategy_id: "s1".to_string(),
                run_id: Some("other-run".to_string()),
                process_id: None,
                status: None,
                heartbeat_at: Utc::now(),
                snapshot: None,
            })
            .expect_err("mismatched run id should fail");

        assert!(matches!(
            error,
            SupervisorError::InvalidRuntimeUpdate { .. }
        ));
    }

    #[test]
    fn recovery_status_should_plan_restart_for_stale_heartbeat_with_budget() {
        let now = Utc::now();
        let policy = RecoveryPolicy {
            restart_on_exit: false,
            restart_on_stale_heartbeat: true,
            max_restart_attempts: 2,
            heartbeat_timeout_ms: Some(1_000),
            snapshot_timeout_ms: None,
            restart_backoff_ms: 10,
        };
        let process = StrategyProcess::new("s1", "mock", "run", "tenant", "config.yml")
            .mark_started(42, now - Duration::milliseconds(5_000))
            .with_recovery_policy(policy);

        let status = process.recovery_status(now);

        assert_eq!(status.action, RecoveryAction::Restart);
        assert_eq!(status.reason.as_deref(), Some("heartbeat stale"));
        assert_eq!(status.heartbeat_age_ms, Some(5_000));
    }

    #[test]
    fn recovery_policy_should_reject_restart_without_budget() {
        let policy = RecoveryPolicy {
            restart_on_exit: true,
            restart_on_stale_heartbeat: false,
            max_restart_attempts: 0,
            heartbeat_timeout_ms: None,
            snapshot_timeout_ms: None,
            restart_backoff_ms: 10,
        };

        let error = policy
            .validate()
            .expect_err("restart policy without attempts should fail");
        assert!(matches!(
            error,
            SupervisorError::InvalidRecoveryPolicy { .. }
        ));
    }

    #[tokio::test]
    async fn local_supervisor_should_start_and_stop_child_process() {
        let mut supervisor = LocalProcessSupervisor::new();
        let process = supervisor
            .start(sleep_spec("strategy-start-stop"))
            .await
            .expect("start process");

        assert_eq!(process.status, ProcessStatus::Running);
        assert!(process.process_id.is_some());
        assert_eq!(supervisor.snapshot().processes.len(), 1);

        let stopped = supervisor
            .stop("strategy-start-stop")
            .await
            .expect("stop process");
        assert_eq!(stopped.status, ProcessStatus::Stopped);
        assert!(stopped.process_id.is_none());
        assert!(stopped.last_exit_code.is_none());
    }

    #[tokio::test]
    async fn local_supervisor_should_restart_child_process_with_backoff() {
        let mut supervisor = LocalProcessSupervisor::new();
        let first = supervisor
            .start(sleep_spec("strategy-restart").with_restart_backoff_ms(1))
            .await
            .expect("start process");
        let first_pid = first.process_id.expect("first pid");

        let restarted = supervisor
            .restart("strategy-restart")
            .await
            .expect("restart process");
        let second_pid = restarted.process_id.expect("second pid");

        assert_eq!(restarted.status, ProcessStatus::Running);
        assert_eq!(restarted.restart_count, 1);
        assert_ne!(first_pid, second_pid);

        supervisor
            .stop("strategy-restart")
            .await
            .expect("stop restarted process");
    }

    #[test]
    fn local_supervisor_should_mark_stale_heartbeats_failed() {
        let mut supervisor = LocalProcessSupervisor::new();
        let started_at = Utc::now() - Duration::milliseconds(5_000);
        supervisor.registry.upsert(
            StrategyProcess::new("strategy-stale", "mock", "run", "tenant", "config.yml")
                .mark_started(123, started_at),
        );

        let stale = supervisor.mark_stale_heartbeats(1_000, Utc::now());

        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].status, ProcessStatus::Failed);
        assert_eq!(
            supervisor
                .registry()
                .get("strategy-stale")
                .expect("stored process")
                .last_error
                .as_deref(),
            Some("heartbeat stale")
        );
    }

    #[test]
    fn legacy_process_template_should_parse_aliases() {
        assert_eq!(
            "cross-arb-live".parse::<LegacyProcessTemplate>().unwrap(),
            LegacyProcessTemplate::CrossArbLive
        );
        assert_eq!(
            "funding_rate_arbitrage_live"
                .parse::<LegacyProcessTemplate>()
                .unwrap(),
            LegacyProcessTemplate::FundingArbLive
        );
        assert_eq!(
            "spot-spot".parse::<LegacyProcessTemplate>().unwrap(),
            LegacyProcessTemplate::SpotSpotLiveDryRun
        );
        assert_eq!(
            "account-position".parse::<LegacyProcessTemplate>().unwrap(),
            LegacyProcessTemplate::AccountPositionReporter
        );
    }

    #[test]
    fn legacy_process_spec_should_build_supervisor_ready_command() {
        let spec = build_legacy_process_spec(LegacyProcessSpecOptions::new(
            LegacyProcessTemplate::CrossArbLive,
        ));

        assert_eq!(spec.schema_version, SUPERVISOR_SCHEMA_VERSION);
        assert_eq!(spec.strategy_id, "cross_arb_live");
        assert_eq!(spec.strategy_kind, "cross_exchange_arbitrage");
        assert_eq!(spec.run_id, "local");
        assert_eq!(spec.tenant_id, "local");
        assert_eq!(spec.config_path, "config/cross_exchange_arbitrage_usdt.yml");
        assert_eq!(spec.command, "cargo");
        assert_eq!(
            spec.args,
            vec![
                "run",
                "--bin",
                "cross_arb_live",
                "--",
                "--config",
                "config/cross_exchange_arbitrage_usdt.yml",
            ]
        );
        assert_eq!(spec.working_dir.as_deref(), Some("."));
        assert_eq!(
            spec.log_path.as_deref(),
            Some("logs/supervisor/cross_arb_live.log")
        );
        assert_eq!(spec.restart_backoff_ms, 5_000);
        spec.validate().expect("legacy process spec validates");
    }

    #[test]
    fn legacy_process_spec_should_preserve_overrides() {
        let mut options = LegacyProcessSpecOptions::new(LegacyProcessTemplate::TrendReport);
        options.strategy_id = Some("trend-main".to_string());
        options.run_id = Some("run-42".to_string());
        options.tenant_id = Some("tenant-a".to_string());
        options.config_path = Some("config/custom_trend.yml".to_string());
        options.working_dir = Some("/srv/rustcta".to_string());
        options.log_dir = Some("/var/log/rustcta".to_string());
        options.restart_backoff_ms = Some(123);

        let spec = build_legacy_process_spec(options);

        assert_eq!(spec.strategy_id, "trend-main");
        assert_eq!(spec.strategy_kind, "trend_report");
        assert_eq!(spec.run_id, "run-42");
        assert_eq!(spec.tenant_id, "tenant-a");
        assert_eq!(spec.config_path, "config/custom_trend.yml");
        assert_eq!(
            spec.args,
            vec![
                "run",
                "--bin",
                "trend_report",
                "--",
                "--config",
                "config/custom_trend.yml",
            ]
        );
        assert_eq!(spec.working_dir.as_deref(), Some("/srv/rustcta"));
        assert_eq!(
            spec.log_path.as_deref(),
            Some("/var/log/rustcta/trend-main.log")
        );
        assert_eq!(spec.restart_backoff_ms, 123);
    }

    #[test]
    fn checked_in_legacy_process_specs_should_match_templates() {
        for (template, path) in [
            (
                LegacyProcessTemplate::CrossArbLive,
                "../../config/supervisor/cross_arb_live.spec.json",
            ),
            (
                LegacyProcessTemplate::FundingArbLive,
                "../../config/supervisor/funding_arb_live.spec.json",
            ),
            (
                LegacyProcessTemplate::SpotSpotLiveDryRun,
                "../../config/supervisor/spot_spot_live_dry_run.spec.json",
            ),
            (
                LegacyProcessTemplate::TrendReport,
                "../../config/supervisor/trend_report.spec.json",
            ),
            (
                LegacyProcessTemplate::AccountPositionReporter,
                "../../config/supervisor/account_position_reporter.spec.json",
            ),
        ] {
            let raw = std::fs::read_to_string(path)
                .unwrap_or_else(|error| panic!("read checked-in supervisor spec {path}: {error}"));
            let checked_in = serde_json::from_str::<StrategyProcessSpec>(&raw)
                .unwrap_or_else(|error| panic!("parse checked-in supervisor spec {path}: {error}"));
            let generated = build_legacy_process_spec(LegacyProcessSpecOptions::new(template));

            assert_eq!(checked_in, generated, "{path} drifted from template");
            checked_in.validate().expect("checked-in spec validates");
        }
    }

    #[test]
    fn json_file_registry_store_should_save_and_load_snapshot() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-supervisor-registry-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = temp_dir.join("registry.json");
        let store = JsonFileProcessRegistryStore::new(&path);
        let mut registry = ProcessRegistry::default();
        registry.upsert(
            StrategyProcess::new("strategy-persist", "mock", "run", "tenant", "config.yml")
                .mark_started(42, Utc::now()),
        );

        store.save(&registry).expect("save registry");
        let loaded = store.load().expect("load registry");
        let process = loaded
            .get("strategy-persist")
            .expect("persisted strategy process");

        assert_eq!(process.strategy_kind, "mock");
        assert_eq!(process.status, ProcessStatus::Running);
        assert_eq!(process.process_id, Some(42));

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[test]
    fn json_file_registry_store_should_return_empty_registry_when_missing() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-supervisor-missing-registry-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = temp_dir.join("registry.json");
        let store = JsonFileProcessRegistryStore::new(&path);

        let loaded = store.load().expect("missing registry loads empty");

        assert!(loaded.list().is_empty());
        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[test]
    fn registry_validation_should_count_statuses_and_invalid_processes() {
        let mut registry = ProcessRegistry::default();
        registry.upsert(
            StrategyProcess::new("strategy-running", "mock", "run", "tenant", "config.yml")
                .mark_started(42, Utc::now()),
        );
        registry.upsert(
            StrategyProcess::new("strategy-stopped", "mock", "run", "tenant", "config.yml")
                .mark_stopped(Utc::now(), Some(0)),
        );
        registry.upsert(StrategyProcess {
            schema_version: SUPERVISOR_SCHEMA_VERSION + 1,
            strategy_id: String::new(),
            strategy_kind: String::new(),
            run_id: "run".to_string(),
            tenant_id: "tenant".to_string(),
            config_path: String::new(),
            status: ProcessStatus::Failed,
            process_id: None,
            started_at: None,
            last_heartbeat_at: None,
            last_snapshot_at: None,
            runtime_snapshot: None,
            restart_count: 0,
            last_exit_code: None,
            last_error: Some("bad config".to_string()),
            log_path: None,
            recovery_policy: RecoveryPolicy::default(),
        });

        let report = registry.validation_report();

        assert!(!report.valid);
        assert_eq!(report.process_count, 3);
        assert_eq!(report.running, 1);
        assert_eq!(report.stopped, 1);
        assert_eq!(report.failed, 1);
        assert_eq!(report.invalid_processes.len(), 1);
        assert_eq!(report.invalid_processes[0].strategy_id, "<empty>");
        assert!(report.invalid_processes[0]
            .errors
            .iter()
            .any(|error| error.contains("schema_version")));
        assert!(report.invalid_processes[0]
            .errors
            .iter()
            .any(|error| error == "strategy_id is required"));
        assert!(report.invalid_processes[0]
            .errors
            .iter()
            .any(|error| error == "config_path is required"));
    }

    fn sleep_spec(strategy_id: &str) -> StrategyProcessSpec {
        StrategyProcessSpec::new(strategy_id, "mock", "run", "tenant", "config.yml", "sh")
            .with_args(["-c", "sleep 30"])
    }
}

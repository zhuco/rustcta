use crate::{
    read_models, AgentConnectionStatus, AgentSummary, ControlApiStateSnapshot, LogEventView,
    LogLevel, StrategyLogTailView, CONTROL_API_SCHEMA_VERSION,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use rustcta_event_ledger::{
    AuditActor, AuditActorType, AuditOutcome, AuditRecord, EventIdentity, EventKind, LedgerEvent,
    LedgerStore,
};
use rustcta_supervisor::{
    JsonFileProcessRegistryStore, LifecycleCommand, LifecycleCommandRecord, LocalProcessSupervisor,
    ProcessRegistry, ProcessStatus, StrategyProcess, StrategyProcessSpec, SupervisorError,
    SupervisorSnapshot,
};
use rustcta_types::{RunId, StrategyId, TenantId};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::RwLock;

const DEFAULT_STRATEGY_LOG_TAIL_LINES: usize = 800;
const DEFAULT_STRATEGY_LOG_TAIL_BYTES: usize = 256 * 1024;
const MAX_STRATEGY_LOG_TAIL_LINES: usize = 2_000;
const MAX_STRATEGY_LOG_TAIL_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct ControlApiState {
    inner: Arc<RwLock<ControlApiStateSnapshot>>,
    commands: Arc<RwLock<Vec<LifecycleCommandRecord>>>,
    audit_ledger: Option<Arc<dyn LedgerStore>>,
    legacy_dashboard_snapshot_path: Option<Arc<PathBuf>>,
    supervisor_registry_path: Option<Arc<PathBuf>>,
    strategy_log_path: Option<Arc<PathBuf>>,
    local_agents: Vec<AgentSummary>,
    strategy_log_tail_lines: usize,
    strategy_log_tail_bytes: usize,
    local_supervisor: Arc<RwLock<LocalProcessSupervisor>>,
    local_specs: Arc<RwLock<BTreeMap<String, StrategyProcessSpec>>>,
}

impl ControlApiState {
    pub fn new(snapshot: ControlApiStateSnapshot) -> Self {
        Self {
            inner: Arc::new(RwLock::new(snapshot)),
            commands: Arc::new(RwLock::new(Vec::new())),
            audit_ledger: None,
            legacy_dashboard_snapshot_path: None,
            supervisor_registry_path: None,
            strategy_log_path: None,
            local_agents: Vec::new(),
            strategy_log_tail_lines: DEFAULT_STRATEGY_LOG_TAIL_LINES,
            strategy_log_tail_bytes: DEFAULT_STRATEGY_LOG_TAIL_BYTES,
            local_supervisor: Arc::new(RwLock::new(LocalProcessSupervisor::new())),
            local_specs: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn with_audit_ledger(mut self, audit_ledger: Arc<dyn LedgerStore>) -> Self {
        self.audit_ledger = Some(audit_ledger);
        self
    }

    pub fn audit_ledger_configured(&self) -> bool {
        self.audit_ledger.is_some()
    }

    pub fn with_legacy_dashboard_snapshot_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.legacy_dashboard_snapshot_path = Some(Arc::new(path.into()));
        self
    }

    pub fn with_local_agent(
        self,
        agent_id: impl Into<String>,
        tenant_id: impl Into<String>,
        capabilities: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let agent = AgentSummary {
            agent_id: agent_id.into(),
            tenant_id: tenant_id.into(),
            status: AgentConnectionStatus::Connected,
            last_heartbeat_at: Some(Utc::now()),
            capabilities: capabilities.into_iter().map(Into::into).collect(),
        };
        self.with_agent(agent)
    }

    pub fn with_agent(mut self, agent: AgentSummary) -> Self {
        if let Some(existing) = self
            .local_agents
            .iter_mut()
            .find(|existing| existing.agent_id == agent.agent_id)
        {
            *existing = agent;
        } else {
            self.local_agents.push(agent);
        }
        self
    }

    pub fn with_supervisor_registry_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.supervisor_registry_path = Some(Arc::new(path.into()));
        self
    }

    pub fn with_strategy_log_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.strategy_log_path = Some(Arc::new(path.into()));
        self
    }

    pub fn with_strategy_log_tail_limits(mut self, max_lines: usize, max_bytes: usize) -> Self {
        self.strategy_log_tail_lines = max_lines.clamp(1, MAX_STRATEGY_LOG_TAIL_LINES);
        self.strategy_log_tail_bytes = max_bytes.clamp(1024, MAX_STRATEGY_LOG_TAIL_BYTES);
        self
    }

    pub fn empty_local() -> Self {
        Self::new(ControlApiStateSnapshot {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            generated_at: Utc::now(),
            strategies: Vec::new(),
            gateway: None,
            agents: Vec::new(),
            risk: Default::default(),
            fees: Default::default(),
            logs: Default::default(),
            inventory: Default::default(),
            books: Default::default(),
            exchanges: Default::default(),
            recent_trades: Default::default(),
            recent_opportunities: Default::default(),
            opportunities: Default::default(),
            symbols: Default::default(),
            runtime_control: Default::default(),
            strategy_snapshots: Vec::new(),
        })
    }

    pub fn from_legacy_dashboard_snapshot(legacy: &serde_json::Value) -> Self {
        Self::new(read_models::apply_runtime_or_legacy_snapshot(
            Self::empty_local_snapshot(),
            legacy,
        ))
    }

    fn empty_local_snapshot() -> ControlApiStateSnapshot {
        ControlApiStateSnapshot {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            generated_at: Utc::now(),
            strategies: Vec::new(),
            gateway: None,
            agents: Vec::new(),
            risk: Default::default(),
            fees: Default::default(),
            logs: Default::default(),
            inventory: Default::default(),
            books: Default::default(),
            exchanges: Default::default(),
            recent_trades: Default::default(),
            recent_opportunities: Default::default(),
            opportunities: Default::default(),
            symbols: Default::default(),
            runtime_control: Default::default(),
            strategy_snapshots: Vec::new(),
        }
    }

    pub async fn snapshot(&self) -> ControlApiStateSnapshot {
        let mut snapshot = self.inner.read().await.clone();
        merge_agents(&mut snapshot.agents, &self.local_agents);
        if let Some(path) = &self.supervisor_registry_path {
            if let Ok(raw) = tokio::fs::read_to_string(path.as_ref()).await {
                if let Ok(supervisor) = serde_json::from_str::<SupervisorSnapshot>(&raw) {
                    snapshot.generated_at = supervisor.captured_at;
                    snapshot.strategies = supervisor.processes;
                }
            }
        }
        if let Some(path) = &self.legacy_dashboard_snapshot_path {
            if let Ok(raw) = tokio::fs::read_to_string(path.as_ref()).await {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) {
                    return read_models::apply_runtime_or_legacy_snapshot(snapshot, &value);
                }
            }
        }
        snapshot
    }

    pub async fn record_command(
        &self,
        command: LifecycleCommandRecord,
    ) -> Result<(), rustcta_event_ledger::LedgerError> {
        if let Some(audit_ledger) = &self.audit_ledger {
            audit_ledger
                .append(operator_command_audit_event(&command))
                .await?;
        }
        self.commands.write().await.push(command);
        Ok(())
    }

    pub async fn record_operator_audit(
        &self,
        action: impl Into<String>,
        actor_id: impl Into<String>,
        metadata: serde_json::Value,
    ) -> Result<(), rustcta_event_ledger::LedgerError> {
        if let Some(audit_ledger) = &self.audit_ledger {
            audit_ledger
                .append(operator_audit_event(action, actor_id, metadata))
                .await?;
        }
        Ok(())
    }

    pub async fn create_strategy(
        &self,
        process: StrategyProcess,
    ) -> Result<StrategyProcess, SupervisorError> {
        let mut registry = self.load_mutable_registry().await?;
        registry.insert(process.clone())?;
        self.save_mutable_registry(&registry).await?;
        self.replace_inner_strategies(registry.list()).await;
        Ok(process)
    }

    pub async fn create_strategy_with_spec(
        &self,
        process: StrategyProcess,
        spec: StrategyProcessSpec,
    ) -> Result<StrategyProcess, SupervisorError> {
        let process = self.create_strategy(process).await?;
        self.local_specs
            .write()
            .await
            .insert(spec.strategy_id.clone(), spec);
        Ok(process)
    }

    pub async fn apply_strategy_command(
        &self,
        command: LifecycleCommandRecord,
    ) -> Result<(StrategyProcess, bool), SupervisorError> {
        if let Some(process) = self.apply_local_supervisor_command(&command).await? {
            self.persist_process(process.clone()).await?;
            return Ok((process, true));
        }

        let mut registry = self.load_mutable_registry().await?;
        apply_registry_command(&mut registry, &command)?;
        let process = registry.get(&command.strategy_id).cloned().ok_or_else(|| {
            SupervisorError::NotFound {
                strategy_id: command.strategy_id.clone(),
            }
        })?;
        self.save_mutable_registry(&registry).await?;
        self.replace_inner_strategies(registry.list()).await;
        Ok((process, false))
    }

    async fn apply_local_supervisor_command(
        &self,
        command: &LifecycleCommandRecord,
    ) -> Result<Option<StrategyProcess>, SupervisorError> {
        match command.command {
            LifecycleCommand::Start => {
                let spec = self
                    .local_specs
                    .read()
                    .await
                    .get(&command.strategy_id)
                    .cloned();
                let Some(mut spec) = spec else {
                    return Ok(None);
                };
                if let Some(run_id) = &command.run_id {
                    spec.run_id = run_id.clone();
                }
                let process = self.local_supervisor.write().await.start(spec).await?;
                Ok(Some(process))
            }
            LifecycleCommand::Stop => {
                let process = self
                    .local_supervisor
                    .write()
                    .await
                    .stop(&command.strategy_id)
                    .await?;
                Ok(Some(process))
            }
            LifecycleCommand::Restart => {
                let process = self
                    .local_supervisor
                    .write()
                    .await
                    .restart(&command.strategy_id)
                    .await?;
                Ok(Some(process))
            }
            LifecycleCommand::Heartbeat => {
                self.local_supervisor
                    .write()
                    .await
                    .heartbeat(&command.strategy_id, command.requested_at)?;
                Ok(None)
            }
        }
    }

    async fn persist_process(&self, process: StrategyProcess) -> Result<(), SupervisorError> {
        let mut registry = self.load_mutable_registry().await?;
        registry.upsert(process);
        self.save_mutable_registry(&registry).await?;
        self.replace_inner_strategies(registry.list()).await;
        Ok(())
    }

    pub async fn commands(&self) -> Vec<LifecycleCommandRecord> {
        self.commands.read().await.clone()
    }

    async fn load_mutable_registry(&self) -> Result<ProcessRegistry, SupervisorError> {
        if let Some(path) = &self.supervisor_registry_path {
            return JsonFileProcessRegistryStore::new(path.as_ref().clone()).load();
        }

        let mut registry = ProcessRegistry::default();
        for process in self.inner.read().await.strategies.clone() {
            registry.upsert(process);
        }
        Ok(registry)
    }

    async fn save_mutable_registry(
        &self,
        registry: &ProcessRegistry,
    ) -> Result<(), SupervisorError> {
        if let Some(path) = &self.supervisor_registry_path {
            JsonFileProcessRegistryStore::new(path.as_ref().clone()).save(registry)?;
        }
        Ok(())
    }

    async fn replace_inner_strategies(&self, strategies: Vec<StrategyProcess>) {
        let mut snapshot = self.inner.write().await;
        snapshot.generated_at = Utc::now();
        snapshot.strategies = strategies;
    }

    pub async fn ledger_events(
        &self,
        from_sequence: Option<u64>,
    ) -> Result<Vec<LedgerEvent>, rustcta_event_ledger::LedgerError> {
        match &self.audit_ledger {
            Some(audit_ledger) => audit_ledger.replay(from_sequence).await,
            None => Ok(Vec::new()),
        }
    }

    pub async fn audit_events(
        &self,
        from_sequence: Option<u64>,
    ) -> Result<Vec<LedgerEvent>, rustcta_event_ledger::LedgerError> {
        let events = self.ledger_events(from_sequence).await?;
        Ok(events
            .into_iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    EventKind::AuditEvent | EventKind::OperatorCommandEvent
                )
            })
            .collect())
    }

    pub async fn strategy_log_tail(&self) -> StrategyLogTailView {
        let Some(path) = &self.strategy_log_path else {
            return StrategyLogTailView {
                max_lines: self.strategy_log_tail_lines,
                max_bytes: self.strategy_log_tail_bytes,
                ..Default::default()
            };
        };

        self.log_tail_for_path(
            Some("strategy".to_string()),
            path.as_ref().clone(),
            "strategy",
        )
        .await
    }

    pub async fn process_log_tail(&self, strategy_id: &str) -> Option<StrategyLogTailView> {
        let process = self
            .snapshot()
            .await
            .strategies
            .into_iter()
            .find(|process| process.strategy_id == strategy_id)?;
        let Some(log_path) = process.log_path else {
            return Some(StrategyLogTailView {
                target: Some(strategy_id.to_string()),
                max_lines: self.strategy_log_tail_lines,
                max_bytes: self.strategy_log_tail_bytes,
                ..Default::default()
            });
        };

        Some(
            self.log_tail_for_path(
                Some(strategy_id.to_string()),
                PathBuf::from(log_path),
                "process",
            )
            .await,
        )
    }

    async fn log_tail_for_path(
        &self,
        target: Option<String>,
        path: PathBuf,
        log_target: &str,
    ) -> StrategyLogTailView {
        let mut view = StrategyLogTailView {
            target,
            configured: true,
            max_lines: self.strategy_log_tail_lines,
            max_bytes: self.strategy_log_tail_bytes,
            ..Default::default()
        };

        match read_log_tail(
            &path,
            self.strategy_log_tail_lines,
            self.strategy_log_tail_bytes,
            log_target,
        )
        .await
        {
            Ok((events, truncated)) => {
                view.readable = true;
                view.truncated = truncated;
                view.event_count = events.len();
                view.events = events;
            }
            Err(error) => {
                view.read_error = Some(format!("{:?}", error.kind()));
            }
        }

        view
    }
}

fn merge_agents(target: &mut Vec<AgentSummary>, overlay: &[AgentSummary]) {
    for agent in overlay {
        if let Some(existing) = target
            .iter_mut()
            .find(|existing| existing.agent_id == agent.agent_id)
        {
            *existing = agent.clone();
        } else {
            target.push(agent.clone());
        }
    }
}

fn apply_registry_command(
    registry: &mut ProcessRegistry,
    command: &LifecycleCommandRecord,
) -> Result<(), SupervisorError> {
    let now = command.requested_at;
    match command.command {
        LifecycleCommand::Start => {
            let process = registry.get_mut(&command.strategy_id).ok_or_else(|| {
                SupervisorError::NotFound {
                    strategy_id: command.strategy_id.clone(),
                }
            })?;
            process.status = ProcessStatus::Running;
            process.started_at = Some(now);
            process.last_heartbeat_at = Some(now);
            process.last_error = None;
            if let Some(run_id) = &command.run_id {
                process.run_id = run_id.clone();
            }
        }
        LifecycleCommand::Stop => {
            let process = registry.get_mut(&command.strategy_id).ok_or_else(|| {
                SupervisorError::NotFound {
                    strategy_id: command.strategy_id.clone(),
                }
            })?;
            process.status = ProcessStatus::Stopped;
            process.process_id = None;
            process.last_heartbeat_at = Some(now);
        }
        LifecycleCommand::Restart => {
            let process = registry.get_mut(&command.strategy_id).ok_or_else(|| {
                SupervisorError::NotFound {
                    strategy_id: command.strategy_id.clone(),
                }
            })?;
            process.status = ProcessStatus::Running;
            process.started_at = Some(now);
            process.last_heartbeat_at = Some(now);
            process.restart_count = process.restart_count.saturating_add(1);
            process.last_error = None;
            if let Some(run_id) = &command.run_id {
                process.run_id = run_id.clone();
            }
        }
        LifecycleCommand::Heartbeat => registry.heartbeat(&command.strategy_id, now)?,
    }
    Ok(())
}

async fn read_log_tail(
    path: &PathBuf,
    max_lines: usize,
    max_bytes: usize,
    target: &str,
) -> std::io::Result<(Vec<LogEventView>, bool)> {
    let mut file = tokio::fs::File::open(path).await?;
    let file_len = file.metadata().await?.len();
    let max_bytes = max_bytes.max(1) as u64;
    let start_offset = file_len.saturating_sub(max_bytes);
    if start_offset > 0 {
        file.seek(std::io::SeekFrom::Start(start_offset)).await?;
    }

    let mut bytes = Vec::with_capacity((file_len - start_offset).min(max_bytes) as usize);
    file.read_to_end(&mut bytes).await?;

    let raw_tail = String::from_utf8_lossy(&bytes);
    let mut lines = raw_tail.lines().collect::<Vec<_>>();
    if start_offset > 0 && !raw_tail.starts_with('\n') && !lines.is_empty() {
        lines.remove(0);
    }

    let mut lines = lines
        .into_iter()
        .map(strip_ansi_escape_codes)
        .filter(|line| !is_noisy_strategy_log_line(line))
        .collect::<Vec<_>>();
    let truncated_by_lines = lines.len() > max_lines;
    if truncated_by_lines {
        lines.drain(0..lines.len() - max_lines);
    }

    let fallback_time = Utc::now();
    let events = lines
        .into_iter()
        .enumerate()
        .map(|(index, line)| parse_log_line(index, &line, fallback_time, target))
        .collect::<Vec<_>>();

    Ok((events, start_offset > 0 || truncated_by_lines))
}

fn parse_log_line(
    index: usize,
    line: &str,
    fallback_time: DateTime<Utc>,
    target: &str,
) -> LogEventView {
    let level = parse_log_level(line);
    let lower = line.to_ascii_lowercase();
    let message = if contains_private_log_marker(&lower) {
        "[redacted log line]".to_string()
    } else {
        line.to_string()
    };

    LogEventView {
        log_id: format!("{target}-tail-{index}"),
        level,
        target: Some(target.to_string()),
        message,
        occurred_at: parse_log_time(line).unwrap_or(fallback_time),
    }
}

fn parse_log_level(line: &str) -> LogLevel {
    let lower = line.to_ascii_lowercase();
    if lower.contains(" error ")
        || lower.contains("[error]")
        || lower.contains("level=error")
        || lower.contains("\terror\t")
    {
        LogLevel::Error
    } else if lower.contains(" warn ")
        || lower.contains("[warn]")
        || lower.contains("level=warn")
        || lower.contains("\twarn\t")
    {
        LogLevel::Warn
    } else if lower.contains(" debug ")
        || lower.contains("[debug]")
        || lower.contains("level=debug")
        || lower.contains("\tdebug\t")
    {
        LogLevel::Debug
    } else if lower.contains(" trace ")
        || lower.contains("[trace]")
        || lower.contains("level=trace")
        || lower.contains("\ttrace\t")
    {
        LogLevel::Trace
    } else {
        LogLevel::Info
    }
}

fn parse_log_time(line: &str) -> Option<DateTime<Utc>> {
    let first_token = line
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_matches(|ch| ch == '[' || ch == ']');
    parse_log_time_candidate(first_token).or_else(|| parse_log_time_prefix(line))
}

fn parse_log_time_prefix(line: &str) -> Option<DateTime<Utc>> {
    let trimmed = line.trim_start().trim_start_matches('[');
    for length in [35usize, 30, 29, 25, 24, 23, 20, 19] {
        if trimmed.len() < length {
            continue;
        }
        let candidate = trimmed[..length]
            .trim_end_matches(']')
            .trim_end_matches(',')
            .trim();
        if let Some(timestamp) = parse_log_time_candidate(candidate) {
            return Some(timestamp);
        }
    }
    None
}

fn parse_log_time_candidate(value: &str) -> Option<DateTime<Utc>> {
    let value = value.trim();
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .map(|timestamp| timestamp.and_utc())
                .ok()
        })
}

fn strip_ansi_escape_codes(line: &str) -> String {
    let mut stripped = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for next in chars.by_ref() {
                if ('@'..='~').contains(&next) {
                    break;
                }
            }
            continue;
        }
        stripped.push(ch);
    }
    stripped
}

fn contains_private_log_marker(lower: &str) -> bool {
    lower.contains("credential")
        || lower.contains("secret")
        || lower.contains("token")
        || lower.contains(&["api", "_", "key"].concat())
        || lower.contains(&["private", "_", "key"].concat())
        || lower.contains(&["pass", "phrase"].concat())
        || lower.contains(&["author", "ization"].concat())
}

fn is_noisy_strategy_log_line(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    let trimmed = lower.trim();
    trimmed.is_empty()
        || trimmed.contains("heartbeat")
        || trimmed.contains("cross-arb live runner status")
        || trimmed.contains("keep_alive")
        || trimmed.contains("keep-alive")
}

fn operator_audit_event(
    action: impl Into<String>,
    actor_id: impl Into<String>,
    metadata: serde_json::Value,
) -> LedgerEvent {
    let now = Utc::now();
    let tenant_id = TenantId::new("local").unwrap_or_else(|_| TenantId::unchecked("local"));
    let mut record = AuditRecord::new(
        EventIdentity::new(tenant_id, "rustcta-control-api", now),
        AuditActor::new(AuditActorType::Operator, actor_id.into()),
        action,
        AuditOutcome::Accepted,
    );
    record.metadata = metadata;
    LedgerEvent::audit(record)
}

fn operator_command_audit_event(command: &LifecycleCommandRecord) -> LedgerEvent {
    let tenant_id = TenantId::new("local").unwrap_or_else(|_| TenantId::unchecked("local"));
    let mut identity = EventIdentity::new(tenant_id, "rustcta-control-api", command.requested_at)
        .with_command(command.command_id.clone())
        .with_idempotency_key(command.idempotency_key.clone());

    if let Ok(strategy_id) = StrategyId::new(command.strategy_id.clone()) {
        if let Some(run_id) = command
            .run_id
            .as_ref()
            .and_then(|run_id| RunId::new(run_id).ok())
        {
            identity = identity.with_strategy_run(strategy_id, run_id);
        }
    }

    let mut record = AuditRecord::new(
        identity,
        AuditActor::new(
            AuditActorType::Operator,
            command
                .requested_by
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
        ),
        "operator_lifecycle_command",
        AuditOutcome::Accepted,
    );
    record.message = Some(format!("{:?}", command.command));
    record.metadata = json!({
        "strategy_id": command.strategy_id,
        "run_id": command.run_id,
        "command_id": command.command_id,
        "idempotency_key": command.idempotency_key,
        "command": format!("{:?}", command.command),
        "would_submit_order": false,
        "applied_to_runtime": false,
    });
    LedgerEvent::operator_command(record)
}

impl From<SupervisorSnapshot> for ControlApiStateSnapshot {
    fn from(snapshot: SupervisorSnapshot) -> Self {
        Self {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            generated_at: snapshot.captured_at,
            strategies: snapshot.processes,
            gateway: None,
            agents: Vec::new(),
            risk: Default::default(),
            fees: Default::default(),
            logs: Default::default(),
            inventory: Default::default(),
            books: Default::default(),
            exchanges: Default::default(),
            recent_trades: Default::default(),
            recent_opportunities: Default::default(),
            opportunities: Default::default(),
            symbols: Default::default(),
            runtime_control: Default::default(),
            strategy_snapshots: Vec::new(),
        }
    }
}

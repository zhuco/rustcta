use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use rustcta_strategy_cross_exchange_arbitrage::{CrossExchangeArbitrageRuntime, STRATEGY_KIND};
use rustcta_strategy_sdk::{
    ExecutionCancelAck, ExecutionCancelCommand, ExecutionIntent, ExecutionIntentAck,
    ExecutionOrderAck, ExecutionOrderCommand, SdkResult, StrategyContext, StrategyExecutionClient,
    StrategyInstanceId, StrategyRuntime,
};
use serde::Serialize;
use serde_json::json;

#[derive(Debug)]
struct Args {
    config: PathBuf,
    strategy_id: String,
    run_id: String,
    tenant_id: String,
    account_id: String,
    lock_file: PathBuf,
    once: bool,
    snapshot_interval_ms: u64,
    command_queue: Option<PathBuf>,
    command_poll_interval_ms: u64,
}

#[derive(Debug, Serialize)]
struct RuntimeReport {
    generated_at: chrono::DateTime<Utc>,
    strategy_kind: &'static str,
    strategy_id: String,
    run_id: String,
    config_path: String,
    lock_file: String,
    root_free_runtime: bool,
    live_orders_enabled: bool,
    concrete_exchange_adapter_loaded: bool,
    snapshot: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let _singleton_guard = ProcessSingletonGuard::acquire(&args.lock_file)?;
    let config = read_yaml_config(&args.config)?;
    let mut runtime = CrossExchangeArbitrageRuntime::new();
    runtime.start(strategy_context(&args, config)).await?;
    emit_report(&args, &runtime).await?;
    if args.once {
        runtime.stop().await?;
        return Ok(());
    }
    let mut command_cursor = CommandQueueCursor::from_path(args.command_queue.as_deref())?;
    let mut snapshot_interval =
        tokio::time::interval(Duration::from_millis(args.snapshot_interval_ms));
    let mut command_interval =
        tokio::time::interval(Duration::from_millis(args.command_poll_interval_ms));
    loop {
        tokio::select! {
            _ = snapshot_interval.tick() => {
                emit_report(&args, &runtime).await?;
            }
            _ = command_interval.tick(), if args.command_queue.is_some() => {
                if apply_realtime_commands(&args, &mut runtime, &mut command_cursor)? {
                    emit_report(&args, &runtime).await?;
                }
            }
        }
    }
}

fn parse_args() -> Result<Args> {
    let mut values = std::env::args().skip(1);
    let mut args = Args {
        config: PathBuf::from("config/cross_exchange_arbitrage_usdt.yml"),
        strategy_id: "cross_arb_live".to_string(),
        run_id: "local".to_string(),
        tenant_id: "local".to_string(),
        account_id: "default".to_string(),
        lock_file: PathBuf::from(
            "logs/cross_exchange_arbitrage/cross_exchange_arbitrage_usdt.lock",
        ),
        once: false,
        snapshot_interval_ms: 30_000,
        command_queue: None,
        command_poll_interval_ms: 1_000,
    };
    while let Some(arg) = values.next() {
        match arg.as_str() {
            "--config" => args.config = PathBuf::from(next_value(&mut values, "--config")?),
            "--strategy-id" => args.strategy_id = next_value(&mut values, "--strategy-id")?,
            "--run-id" => args.run_id = next_value(&mut values, "--run-id")?,
            "--tenant-id" => args.tenant_id = next_value(&mut values, "--tenant-id")?,
            "--account-id" => args.account_id = next_value(&mut values, "--account-id")?,
            "--lock-file" => {
                args.lock_file = PathBuf::from(next_value(&mut values, "--lock-file")?)
            }
            "--snapshot-interval-ms" => {
                args.snapshot_interval_ms = next_value(&mut values, "--snapshot-interval-ms")?
                    .parse()
                    .context("--snapshot-interval-ms must be a positive integer")?
            }
            "--command-queue" => {
                args.command_queue =
                    Some(PathBuf::from(next_value(&mut values, "--command-queue")?))
            }
            "--command-poll-interval-ms" => {
                args.command_poll_interval_ms =
                    next_value(&mut values, "--command-poll-interval-ms")?
                        .parse()
                        .context("--command-poll-interval-ms must be a positive integer")?
            }
            "--once" => args.once = true,
            "--help" | "-h" => {
                println!(
                    "cross-exchange-arbitrage-runtime --config <path> [--command-queue <jsonl>] [--once]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(args)
}

fn next_value(values: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    values
        .next()
        .with_context(|| format!("{flag} requires a value"))
}

fn read_yaml_config(path: &PathBuf) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;
    serde_json::to_value(yaml).context("convert runtime config to json")
}

#[derive(Debug, Clone, Default)]
struct CommandQueueCursor {
    offset: u64,
}

impl CommandQueueCursor {
    fn from_path(path: Option<&Path>) -> Result<Self> {
        let offset = match path {
            Some(path) => match std::fs::metadata(path) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == ErrorKind::NotFound => 0,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("stat command queue {}", path.display()))
                }
            },
            None => 0,
        };
        Ok(Self { offset })
    }
}

fn apply_realtime_commands(
    args: &Args,
    runtime: &mut CrossExchangeArbitrageRuntime,
    cursor: &mut CommandQueueCursor,
) -> Result<bool> {
    let Some(path) = args.command_queue.as_ref() else {
        return Ok(false);
    };
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| format!("stat command queue {}", path.display()))
        }
    };
    if metadata.len() < cursor.offset {
        cursor.offset = 0;
    }
    if metadata.len() == cursor.offset {
        return Ok(false);
    }

    let mut file =
        File::open(path).with_context(|| format!("open command queue {}", path.display()))?;
    file.seek(SeekFrom::Start(cursor.offset))
        .with_context(|| format!("seek command queue {}", path.display()))?;
    let mut raw = String::new();
    file.read_to_string(&mut raw)
        .with_context(|| format!("read command queue {}", path.display()))?;
    cursor.offset = file
        .stream_position()
        .with_context(|| format!("cursor command queue {}", path.display()))?;

    let mut applied = false;
    for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if !should_reload_config_from_command(line, &args.strategy_id) {
            continue;
        }
        let config = read_yaml_config(&args.config)?;
        runtime.reload_config_value(&config);
        applied = true;
        eprintln!(
            "cross-exchange-arbitrage-runtime applied realtime exchange config from {}",
            args.config.display()
        );
    }
    Ok(applied)
}

fn should_reload_config_from_command(line: &str, strategy_id: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
        return false;
    };
    let command = value
        .get("command")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if command != "update_cross_arb_exchange_config" && command != "reload_config" {
        return false;
    }
    let command_strategy = value
        .get("strategy_id")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            value
                .get("payload")
                .and_then(|payload| payload.get("strategy_id"))
                .and_then(serde_json::Value::as_str)
        });
    match command_strategy {
        Some(value) => value == strategy_id,
        None => true,
    }
}

fn strategy_context(args: &Args, config: serde_json::Value) -> StrategyContext {
    StrategyContext::new(
        StrategyInstanceId::new(format!("{}:{}", args.strategy_id, args.run_id)),
        args.tenant_id.clone(),
        args.account_id.clone(),
        args.strategy_id.clone(),
        args.run_id.clone(),
        config,
        Arc::new(NoopExecutionClient),
    )
}

async fn emit_report(args: &Args, runtime: &CrossExchangeArbitrageRuntime) -> Result<()> {
    let report = RuntimeReport {
        generated_at: Utc::now(),
        strategy_kind: STRATEGY_KIND,
        strategy_id: args.strategy_id.clone(),
        run_id: args.run_id.clone(),
        config_path: args.config.display().to_string(),
        lock_file: args.lock_file.display().to_string(),
        root_free_runtime: true,
        live_orders_enabled: false,
        concrete_exchange_adapter_loaded: false,
        snapshot: serde_json::to_value(runtime.snapshot().await?)?,
    };
    println!("{}", serde_json::to_string(&json!(report))?);
    Ok(())
}

struct ProcessSingletonGuard {
    path: PathBuf,
    _file: File,
}

impl ProcessSingletonGuard {
    fn acquire(path: &Path) -> Result<Self> {
        match Self::try_create(path) {
            Ok(guard) => Ok(guard),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                if lock_owner_is_alive(path)? {
                    bail!(
                        "cross-exchange arbitrage runtime already has a live lock at {}",
                        path.display()
                    );
                }
                std::fs::remove_file(path)
                    .with_context(|| format!("remove stale lock {}", path.display()))?;
                Self::try_create(path).with_context(|| {
                    format!(
                        "recreate singleton lock after stale cleanup {}",
                        path.display()
                    )
                })
            }
            Err(error) => {
                Err(error).with_context(|| format!("create singleton lock {}", path.display()))
            }
        }
    }

    fn try_create(path: &Path) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
        writeln!(file, "pid={}", std::process::id())?;
        writeln!(file, "created_at={}", Utc::now().to_rfc3339())?;
        Ok(Self {
            path: path.to_path_buf(),
            _file: file,
        })
    }
}

impl Drop for ProcessSingletonGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn lock_owner_is_alive(path: &Path) -> Result<bool> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read existing singleton lock {}", path.display()))?;
    let Some(pid) = raw
        .lines()
        .find_map(|line| line.strip_prefix("pid="))
        .and_then(|value| value.parse::<u32>().ok())
    else {
        bail!(
            "singleton lock {} exists but does not contain a parseable pid",
            path.display()
        );
    };
    Ok(process_is_alive(pid))
}

#[cfg(target_os = "linux")]
fn process_is_alive(pid: u32) -> bool {
    PathBuf::from(format!("/proc/{pid}")).exists()
}

#[cfg(not(target_os = "linux"))]
fn process_is_alive(_pid: u32) -> bool {
    true
}

struct NoopExecutionClient;

#[async_trait]
impl StrategyExecutionClient for NoopExecutionClient {
    async fn submit_order(&self, command: ExecutionOrderCommand) -> SdkResult<ExecutionOrderAck> {
        Ok(ExecutionOrderAck {
            schema_version: command.schema_version,
            accepted: false,
            client_order_id: command.client_order_id,
            execution_order_id: None,
            reason: Some("root-free supervisor wrapper has no execution client".to_string()),
            received_at: Utc::now(),
        })
    }

    async fn cancel_order(&self, command: ExecutionCancelCommand) -> SdkResult<ExecutionCancelAck> {
        Ok(ExecutionCancelAck {
            schema_version: command.schema_version,
            accepted: false,
            client_order_id: command.client_order_id,
            execution_order_id: command.execution_order_id,
            reason: Some("root-free supervisor wrapper has no execution client".to_string()),
            received_at: Utc::now(),
        })
    }

    async fn submit_raw_intent(&self, intent: ExecutionIntent) -> SdkResult<ExecutionIntentAck> {
        Ok(ExecutionIntentAck {
            schema_version: intent.schema_version,
            accepted: false,
            intent_kind: intent.intent_kind,
            reason: Some("root-free supervisor wrapper has no execution client".to_string()),
            received_at: Utc::now(),
            payload: json!({}),
        })
    }
}

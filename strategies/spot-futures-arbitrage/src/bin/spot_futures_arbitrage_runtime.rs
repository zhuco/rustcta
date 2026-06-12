use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use rustcta_strategy_sdk::{
    ExecutionCancelAck, ExecutionCancelCommand, ExecutionIntent, ExecutionIntentAck,
    ExecutionOrderAck, ExecutionOrderCommand, SdkResult, StrategyContext, StrategyExecutionClient,
    StrategyInstanceId, StrategyRuntime,
};
use rustcta_strategy_spot_futures_arbitrage::{SpotFuturesArbitrageRuntime, STRATEGY_KIND};
use serde::Serialize;
use serde_json::json;

#[derive(Debug)]
struct Args {
    config: PathBuf,
    strategy_id: String,
    run_id: String,
    tenant_id: String,
    account_id: String,
    once: bool,
    snapshot_interval_ms: u64,
    dashboard_snapshot_path: Option<PathBuf>,
    execution_mode: String,
    quiet_stdout: bool,
}

#[derive(Debug, Serialize)]
struct RuntimeReport {
    generated_at: chrono::DateTime<Utc>,
    strategy_kind: &'static str,
    strategy_id: String,
    run_id: String,
    config_path: String,
    root_free_runtime: bool,
    live_orders_enabled: bool,
    concrete_exchange_adapter_loaded: bool,
    execution_mode: String,
    snapshot: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let config = read_yaml_config(&args.config)?;
    let mut runtime = SpotFuturesArbitrageRuntime::new();
    runtime.start(strategy_context(&args, config)).await?;
    emit_report(&args, &runtime).await?;
    if args.once {
        runtime.stop().await?;
        return Ok(());
    }
    let mut interval = tokio::time::interval(Duration::from_millis(args.snapshot_interval_ms));
    loop {
        interval.tick().await;
        emit_report(&args, &runtime).await?;
    }
}

fn parse_args() -> Result<Args> {
    let mut values = std::env::args().skip(1);
    let mut args = Args {
        config: PathBuf::from("config/spot_futures_arbitrage_usdt.yml"),
        strategy_id: "spot_futures_arb_live".to_string(),
        run_id: "local".to_string(),
        tenant_id: "local".to_string(),
        account_id: "default".to_string(),
        once: false,
        snapshot_interval_ms: 30_000,
        dashboard_snapshot_path: Some(PathBuf::from(
            "logs/spot_futures_arbitrage/dashboard_snapshot.json",
        )),
        execution_mode: "noop".to_string(),
        quiet_stdout: false,
    };
    while let Some(arg) = values.next() {
        match arg.as_str() {
            "--config" => args.config = PathBuf::from(next_value(&mut values, "--config")?),
            "--strategy-id" => args.strategy_id = next_value(&mut values, "--strategy-id")?,
            "--run-id" => args.run_id = next_value(&mut values, "--run-id")?,
            "--tenant-id" => args.tenant_id = next_value(&mut values, "--tenant-id")?,
            "--account-id" => args.account_id = next_value(&mut values, "--account-id")?,
            "--execution-mode" => {
                args.execution_mode = next_value(&mut values, "--execution-mode")?
            }
            "--dashboard-snapshot-path" => {
                args.dashboard_snapshot_path = Some(PathBuf::from(next_value(
                    &mut values,
                    "--dashboard-snapshot-path",
                )?))
            }
            "--disable-dashboard-snapshot" => args.dashboard_snapshot_path = None,
            "--quiet-stdout" => args.quiet_stdout = true,
            "--snapshot-interval-ms" => {
                args.snapshot_interval_ms = next_value(&mut values, "--snapshot-interval-ms")?
                    .parse()
                    .context("--snapshot-interval-ms must be a positive integer")?
            }
            "--once" => args.once = true,
            "--help" | "-h" => {
                println!(
                    "spot-futures-arbitrage-runtime --config <path> [--execution-mode noop] [--dashboard-snapshot-path <path>] [--quiet-stdout] [--once]"
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

async fn emit_report(args: &Args, runtime: &SpotFuturesArbitrageRuntime) -> Result<()> {
    let report = RuntimeReport {
        generated_at: Utc::now(),
        strategy_kind: STRATEGY_KIND,
        strategy_id: args.strategy_id.clone(),
        run_id: args.run_id.clone(),
        config_path: args.config.display().to_string(),
        root_free_runtime: true,
        live_orders_enabled: false,
        concrete_exchange_adapter_loaded: false,
        execution_mode: args.execution_mode.clone(),
        snapshot: serde_json::to_value(runtime.snapshot().await?)?,
    };
    let value = serde_json::to_value(&report)?;
    if let Some(path) = &args.dashboard_snapshot_path {
        write_json(path, &value).await?;
    }
    if !args.quiet_stdout {
        println!("{}", serde_json::to_string(&json!(report))?);
    }
    Ok(())
}

async fn write_json(path: &PathBuf, value: &serde_json::Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(value)?;
    tokio::fs::write(path, bytes)
        .await
        .with_context(|| format!("write {}", path.display()))
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
            reason: Some("root-free spot-futures wrapper has no execution client".to_string()),
            received_at: Utc::now(),
        })
    }

    async fn cancel_order(&self, command: ExecutionCancelCommand) -> SdkResult<ExecutionCancelAck> {
        Ok(ExecutionCancelAck {
            schema_version: command.schema_version,
            accepted: false,
            client_order_id: command.client_order_id,
            execution_order_id: command.execution_order_id,
            reason: Some("root-free spot-futures wrapper has no execution client".to_string()),
            received_at: Utc::now(),
        })
    }

    async fn submit_raw_intent(&self, intent: ExecutionIntent) -> SdkResult<ExecutionIntentAck> {
        Ok(ExecutionIntentAck {
            schema_version: intent.schema_version,
            accepted: false,
            intent_kind: intent.intent_kind,
            reason: Some("root-free spot-futures wrapper has no execution client".to_string()),
            received_at: Utc::now(),
            payload: json!({}),
        })
    }
}

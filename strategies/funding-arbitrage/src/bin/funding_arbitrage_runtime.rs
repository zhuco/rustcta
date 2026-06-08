use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use rustcta_strategy_funding_arbitrage::{FundingArbitrageRuntime, STRATEGY_KIND};
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
    once: bool,
    snapshot_interval_ms: u64,
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
    snapshot: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let config = read_yaml_config(&args.config)?;
    let mut runtime = FundingArbitrageRuntime::new();
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
        config: PathBuf::from("config/funding_rate_arbitrage_live_usdt.yml"),
        strategy_id: "funding_arb_live".to_string(),
        run_id: "local".to_string(),
        tenant_id: "local".to_string(),
        account_id: "default".to_string(),
        once: false,
        snapshot_interval_ms: 30_000,
    };
    while let Some(arg) = values.next() {
        match arg.as_str() {
            "--config" => args.config = PathBuf::from(next_value(&mut values, "--config")?),
            "--strategy-id" => args.strategy_id = next_value(&mut values, "--strategy-id")?,
            "--run-id" => args.run_id = next_value(&mut values, "--run-id")?,
            "--tenant-id" => args.tenant_id = next_value(&mut values, "--tenant-id")?,
            "--account-id" => args.account_id = next_value(&mut values, "--account-id")?,
            "--snapshot-interval-ms" => {
                args.snapshot_interval_ms = next_value(&mut values, "--snapshot-interval-ms")?
                    .parse()
                    .context("--snapshot-interval-ms must be a positive integer")?
            }
            "--once" => args.once = true,
            "--help" | "-h" => {
                println!("funding-arbitrage-runtime --config <path> [--once]");
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

async fn emit_report(args: &Args, runtime: &FundingArbitrageRuntime) -> Result<()> {
    let report = RuntimeReport {
        generated_at: Utc::now(),
        strategy_kind: STRATEGY_KIND,
        strategy_id: args.strategy_id.clone(),
        run_id: args.run_id.clone(),
        config_path: args.config.display().to_string(),
        root_free_runtime: true,
        live_orders_enabled: false,
        concrete_exchange_adapter_loaded: false,
        snapshot: serde_json::to_value(runtime.snapshot().await?)?,
    };
    println!("{}", serde_json::to_string(&json!(report))?);
    Ok(())
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

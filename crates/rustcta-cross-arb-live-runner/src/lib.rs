use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::RequestContext;
use rustcta_exchange_gateway::{
    AdapterBackedGateway, GatewayClient, GetCapabilitiesRequest, InProcessGatewayClient,
};
use rustcta_execution_api::{CancelCommand, CancellationIds, MutationIdentity, OrderCommand};
use rustcta_execution_router::{ExecutionRouter, ExecutionRouterConfig};
use rustcta_strategy_cross_exchange_arbitrage::{
    CrossExchangeArbitrageConfig, CrossExchangeArbitrageRuntime, STRATEGY_KIND,
};
use rustcta_strategy_sdk::{
    ExecutionCancelAck, ExecutionCancelCommand, ExecutionIntent, ExecutionIntentAck,
    ExecutionOrderAck, ExecutionOrderCommand, MarketType as SdkMarketType, SdkResult,
    StrategyContext, StrategyExecutionClient, StrategyInstanceId, StrategyRuntime,
    StrategySdkError,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId as GatewayExchangeId, ExchangeSymbol,
    MarketType as GatewayMarketType, OrderSide as GatewayOrderSide, OrderType as GatewayOrderType,
    PositionSide as GatewayPositionSide, RunId, StrategyId, TenantId,
    TimeInForce as GatewayTimeInForce,
};
use serde::Serialize;
use serde_json::{json, Value};

type LocalGatewayClient = InProcessGatewayClient<AdapterBackedGateway>;
type LocalExecutionRouter = ExecutionRouter<LocalGatewayClient>;

#[derive(Debug, Clone)]
pub struct LiveRunnerArgs {
    pub config: PathBuf,
    pub strategy_id: String,
    pub run_id: String,
    pub tenant_id: String,
    pub account_id: String,
    pub lock_file: PathBuf,
    pub once: bool,
    pub snapshot_interval_ms: u64,
}

impl Default for LiveRunnerArgs {
    fn default() -> Self {
        Self {
            config: PathBuf::from("config/cross_exchange_arbitrage_usdt.yml"),
            strategy_id: "cross_arb_live".to_string(),
            run_id: "local".to_string(),
            tenant_id: "local".to_string(),
            account_id: "cross_arb_3venues".to_string(),
            lock_file: PathBuf::from(
                "logs/cross_exchange_arbitrage/cross_exchange_arbitrage_usdt.lock",
            ),
            once: false,
            snapshot_interval_ms: 30_000,
        }
    }
}

impl LiveRunnerArgs {
    pub fn from_env_args() -> Result<Self> {
        Self::from_iter(std::env::args().skip(1))
    }

    pub fn from_iter(values: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut values = values.into_iter();
        let mut args = Self::default();
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
                "--once" => args.once = true,
                "--help" | "-h" => {
                    println!("cross-exchange-arbitrage-live-runner --config <path> [--once]");
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }
        Ok(args)
    }
}

#[derive(Debug, Clone, Serialize)]
struct CapabilityGateReport {
    passed: bool,
    target_market_type: String,
    required_exchanges: Vec<String>,
    loaded_adapters: Vec<String>,
    missing_requirements: Vec<String>,
}

#[derive(Debug, Serialize)]
struct LiveRunnerReport {
    generated_at: chrono::DateTime<Utc>,
    strategy_kind: &'static str,
    strategy_id: String,
    run_id: String,
    config_path: String,
    lock_file: String,
    live_orders_enabled: bool,
    concrete_exchange_adapter_loaded: bool,
    gateway_owned_credentials: bool,
    credential_source_boundary: &'static str,
    market_data_provider_connected: bool,
    startup_position_takeover_enabled: bool,
    capability_gate: CapabilityGateReport,
    snapshot: Option<Value>,
}

pub async fn run_live_runner(
    args: LiveRunnerArgs,
    gateway: AdapterBackedGateway,
    loaded_adapters: Vec<String>,
) -> Result<()> {
    let _singleton_guard = ProcessSingletonGuard::acquire(&args.lock_file)?;
    let config_value = read_yaml_config(&args.config)?;
    let strategy_config = CrossExchangeArbitrageConfig::from_runtime_value(&config_value);
    let target_market_type = gateway_market_type(&strategy_config.market_type)?;
    let required_exchanges = gateway_exchange_ids(strategy_config.active_venues());
    let account_by_exchange =
        exchange_account_map(&config_value, &required_exchanges, &args.account_id);

    let gateway = Arc::new(gateway);
    let gateway_client = InProcessGatewayClient::new(gateway);
    let capability_gate = validate_gateway_capabilities(
        &gateway_client,
        &args,
        target_market_type,
        &required_exchanges,
        &loaded_adapters,
    )
    .await?;

    if !capability_gate.passed {
        emit_report(&args, &capability_gate, None, false, true).await?;
        bail!(
            "cross exchange arbitrage live runner blocked by capability gate: {}",
            capability_gate.missing_requirements.join("; ")
        );
    }

    let router = Arc::new(ExecutionRouter::new(
        ExecutionRouterConfig::live(),
        gateway_client,
    ));
    let execution = Arc::new(RouterBackedStrategyExecutionClient {
        router,
        market_type: target_market_type,
        account_by_exchange,
    });
    let mut runtime = CrossExchangeArbitrageRuntime::new();
    runtime
        .start(strategy_context(&args, config_value, execution))
        .await?;
    emit_report(
        &args,
        &capability_gate,
        Some(serde_json::to_value(runtime.snapshot().await?)?),
        true,
        true,
    )
    .await?;

    if args.once {
        runtime.stop().await?;
        return Ok(());
    }

    let mut interval = tokio::time::interval(Duration::from_millis(args.snapshot_interval_ms));
    loop {
        interval.tick().await;
        emit_report(
            &args,
            &capability_gate,
            Some(serde_json::to_value(runtime.snapshot().await?)?),
            true,
            true,
        )
        .await?;
    }
}

fn next_value(values: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    values
        .next()
        .with_context(|| format!("{flag} requires a value"))
}

fn read_yaml_config(path: &Path) -> Result<Value> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;
    serde_json::to_value(yaml).context("convert runtime config to json")
}

fn strategy_context(
    args: &LiveRunnerArgs,
    config: Value,
    execution: Arc<dyn StrategyExecutionClient>,
) -> StrategyContext {
    StrategyContext::new(
        StrategyInstanceId::new(format!("{}:{}", args.strategy_id, args.run_id)),
        args.tenant_id.clone(),
        args.account_id.clone(),
        args.strategy_id.clone(),
        args.run_id.clone(),
        config,
        execution,
    )
}

async fn emit_report(
    args: &LiveRunnerArgs,
    capability_gate: &CapabilityGateReport,
    snapshot: Option<Value>,
    live_orders_enabled: bool,
    concrete_exchange_adapter_loaded: bool,
) -> Result<()> {
    let report = LiveRunnerReport {
        generated_at: Utc::now(),
        strategy_kind: STRATEGY_KIND,
        strategy_id: args.strategy_id.clone(),
        run_id: args.run_id.clone(),
        config_path: args.config.display().to_string(),
        lock_file: args.lock_file.display().to_string(),
        live_orders_enabled,
        concrete_exchange_adapter_loaded,
        gateway_owned_credentials: true,
        credential_source_boundary: "gateway_app",
        market_data_provider_connected: false,
        startup_position_takeover_enabled: false,
        capability_gate: capability_gate.clone(),
        snapshot,
    };
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

async fn validate_gateway_capabilities(
    gateway: &impl GatewayClient,
    args: &LiveRunnerArgs,
    target_market_type: GatewayMarketType,
    required_exchanges: &[String],
    loaded_adapters: &[String],
) -> Result<CapabilityGateReport> {
    let tenant_id = TenantId::new(args.tenant_id.clone())?;
    let account_id = AccountId::new(args.account_id.clone())?;
    let run_id = RunId::new(args.run_id.clone())?;
    let mut context = RequestContext::new(Utc::now());
    context.tenant_id = Some(tenant_id.clone());
    context.account_id = Some(account_id.clone());
    context.run_id = Some(run_id);
    context.request_id = Some("cross-arb-live-capability-gate".to_string());

    let exchanges = required_exchanges
        .iter()
        .map(|exchange| GatewayExchangeId::new(exchange.clone()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let response = gateway
        .get_capabilities(
            "cross-arb-live-capability-gate".to_string(),
            tenant_id,
            Some(account_id),
            GetCapabilitiesRequest {
                schema_version: rustcta_exchange_gateway::GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context,
                exchanges,
            },
        )
        .await?;

    let mut missing = Vec::new();
    let mut seen = BTreeSet::new();
    for capability in response.capabilities {
        let exchange = capability.exchange.to_string();
        seen.insert(exchange.clone());
        if !capability.market_types.contains(&target_market_type) {
            missing.push(format!(
                "{exchange} does not support target market type {target_market_type:?}; advertised={:?}",
                capability.market_types
            ));
        }
        if !capability.supports_private_rest {
            missing.push(format!("{exchange} private REST is not enabled"));
        }
        if !capability.supports_private_streams {
            missing.push(format!("{exchange} private stream is not supported"));
        }
        if !capability.supports_symbol_rules {
            missing.push(format!("{exchange} symbol rules are not supported"));
        }
        if !capability.supports_order_book_snapshot {
            missing.push(format!("{exchange} order book snapshots are not supported"));
        }
        if !capability.supports_positions {
            missing.push(format!("{exchange} positions are not supported"));
        }
        if !capability.supports_place_order {
            missing.push(format!("{exchange} place_order is not supported"));
        }
        if !capability.supports_cancel_order {
            missing.push(format!("{exchange} cancel_order is not supported"));
        }
        if !capability.supports_reduce_only {
            missing.push(format!("{exchange} reduce_only orders are not supported"));
        }
        if !capability
            .supports_time_in_force
            .contains(&GatewayTimeInForce::IOC)
        {
            missing.push(format!("{exchange} IOC time-in-force is not supported"));
        }
        if !capability
            .supports_order_types
            .contains(&GatewayOrderType::IOC)
        {
            missing.push(format!("{exchange} IOC order type is not supported"));
        }
    }

    for exchange in required_exchanges {
        if !seen.contains(exchange) {
            missing.push(format!("{exchange} adapter is not loaded"));
        }
    }

    Ok(CapabilityGateReport {
        passed: missing.is_empty(),
        target_market_type: format!("{target_market_type:?}"),
        required_exchanges: required_exchanges.to_vec(),
        loaded_adapters: loaded_adapters.to_vec(),
        missing_requirements: missing,
    })
}

fn gateway_exchange_ids(venues: Vec<String>) -> Vec<String> {
    venues
        .into_iter()
        .map(|venue| gateway_exchange_id(&venue))
        .fold(Vec::new(), |mut exchanges, exchange| {
            if !exchanges.contains(&exchange) {
                exchanges.push(exchange);
            }
            exchanges
        })
}

fn gateway_exchange_id(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gateio" | "gate.io" | "gate_io" => "gateio".to_string(),
        "binance" => "binance".to_string(),
        "bitget" => "bitget".to_string(),
        "" => "unknown".to_string(),
        other => other.to_string(),
    }
}

fn exchange_account_map(
    config: &Value,
    required_exchanges: &[String],
    default_account_id: &str,
) -> BTreeMap<String, String> {
    let mut accounts = BTreeMap::new();
    for exchange in required_exchanges {
        let account = account_id_for_exchange(config, exchange).unwrap_or(default_account_id);
        accounts.insert(exchange.clone(), account.to_string());
        if exchange == "gateio" {
            accounts.insert("gate".to_string(), account.to_string());
            accounts.insert("gate.io".to_string(), account.to_string());
        }
    }
    accounts
}

fn account_id_for_exchange<'a>(config: &'a Value, exchange: &str) -> Option<&'a str> {
    let exchange_configs = config.get("exchanges")?.as_object()?;
    for key in exchange_config_keys(exchange) {
        if let Some(account_id) = exchange_configs
            .get(*key)
            .and_then(|value| value.get("account_id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Some(account_id);
        }
    }
    None
}

fn exchange_config_keys(exchange: &str) -> &'static [&'static str] {
    match exchange {
        "gateio" => &["gateio", "gate", "gate.io", "gate_io"],
        "binance" => &["binance"],
        "bitget" => &["bitget"],
        _ => &[],
    }
}

fn gateway_market_type(market_type: &SdkMarketType) -> Result<GatewayMarketType> {
    Ok(match market_type {
        SdkMarketType::Spot => GatewayMarketType::Spot,
        SdkMarketType::Margin => GatewayMarketType::Margin,
        SdkMarketType::Perpetual => GatewayMarketType::Perpetual,
        SdkMarketType::Futures => GatewayMarketType::Futures,
        SdkMarketType::Option => GatewayMarketType::Option,
        SdkMarketType::Custom(value) => bail!("unsupported custom market type: {value}"),
    })
}

struct RouterBackedStrategyExecutionClient {
    router: Arc<LocalExecutionRouter>,
    market_type: GatewayMarketType,
    account_by_exchange: BTreeMap<String, String>,
}

#[async_trait]
impl StrategyExecutionClient for RouterBackedStrategyExecutionClient {
    async fn submit_order(&self, command: ExecutionOrderCommand) -> SdkResult<ExecutionOrderAck> {
        let schema_version = command.schema_version;
        let client_order_id = command.client_order_id.clone();
        let order = self.map_order_command(command)?;
        let ack = self
            .router
            .place_order(order)
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))?;
        Ok(ExecutionOrderAck {
            schema_version,
            accepted: ack.accepted,
            client_order_id,
            execution_order_id: ack.exchange_order_id,
            reason: ack.message,
            received_at: ack.acknowledged_at,
        })
    }

    async fn cancel_order(&self, command: ExecutionCancelCommand) -> SdkResult<ExecutionCancelAck> {
        let schema_version = command.schema_version;
        let client_order_id = command.client_order_id.clone();
        let execution_order_id = command.execution_order_id.clone();
        let cancel = self.map_cancel_command(command)?;
        let ack = self
            .router
            .cancel_order(cancel)
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))?;
        Ok(ExecutionCancelAck {
            schema_version,
            accepted: ack.accepted,
            client_order_id,
            execution_order_id,
            reason: ack.message,
            received_at: ack.acknowledged_at,
        })
    }

    async fn submit_raw_intent(&self, intent: ExecutionIntent) -> SdkResult<ExecutionIntentAck> {
        Ok(ExecutionIntentAck {
            schema_version: intent.schema_version,
            accepted: false,
            intent_kind: intent.intent_kind,
            reason: Some("raw intents are not routed by the live execution client".to_string()),
            received_at: Utc::now(),
            payload: json!({}),
        })
    }
}

impl RouterBackedStrategyExecutionClient {
    fn map_order_command(&self, command: ExecutionOrderCommand) -> SdkResult<OrderCommand> {
        let exchange = gateway_exchange_id(&command.exchange_id);
        let exchange_id = GatewayExchangeId::new(exchange.clone()).map_sdk_err()?;
        let canonical_symbol = CanonicalSymbol::parse(&command.symbol).map_sdk_err()?;
        let exchange_symbol = ExchangeSymbol::new(
            exchange_id.clone(),
            self.market_type,
            exchange_symbol_text(&exchange, &canonical_symbol),
        )
        .map_sdk_err()?;
        let account_id = self
            .account_by_exchange
            .get(&exchange)
            .cloned()
            .unwrap_or(command.account_id);
        let identity = mutation_identity(
            command.tenant_id,
            account_id,
            command.strategy_id,
            command.run_id,
            command.idempotency_key,
            command.risk_profile_id,
            command.requested_at,
        )?;
        let order_type = order_type(command.order_type)?;
        let time_in_force = command
            .time_in_force
            .map(time_in_force)
            .transpose()?
            .unwrap_or_else(|| default_time_in_force(order_type));
        let quantity = parse_positive_f64("quantity", &command.quantity)?;
        let price = command
            .price
            .as_deref()
            .map(|value| parse_positive_f64("price", value))
            .transpose()?;

        let mut order = OrderCommand::new(
            identity,
            command.client_order_id.clone(),
            exchange_id,
            self.market_type,
            canonical_symbol,
            exchange_symbol,
            command.client_order_id,
            order_side(command.side),
            position_side(&command.metadata),
            order_type,
            time_in_force,
            quantity,
            price,
        );
        order.reduce_only = command.reduce_only;
        order.post_only = matches!(order.order_type, GatewayOrderType::PostOnly);
        order.max_slippage_bps = Some(5);
        order.correlation_id = command
            .metadata
            .get("bundle_id")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        Ok(order)
    }

    fn map_cancel_command(&self, command: ExecutionCancelCommand) -> SdkResult<CancelCommand> {
        let exchange = gateway_exchange_id(&command.exchange_id);
        let exchange_id = GatewayExchangeId::new(exchange.clone()).map_sdk_err()?;
        let canonical_symbol = CanonicalSymbol::parse(&command.symbol).map_sdk_err()?;
        let exchange_symbol = ExchangeSymbol::new(
            exchange_id.clone(),
            self.market_type,
            exchange_symbol_text(&exchange, &canonical_symbol),
        )
        .map_sdk_err()?;
        let account_id = self
            .account_by_exchange
            .get(&exchange)
            .cloned()
            .unwrap_or(command.account_id);
        let identity = mutation_identity(
            command.tenant_id,
            account_id,
            command.strategy_id,
            command.run_id,
            command.idempotency_key,
            command.risk_profile_id,
            command.requested_at,
        )?;
        let cancellation_ids = match (
            command.client_order_id.clone(),
            command.execution_order_id.clone(),
        ) {
            (Some(client_order_id), _) => CancellationIds::by_client_order_id(client_order_id),
            (None, Some(exchange_order_id)) => {
                CancellationIds::by_exchange_order_id(exchange_order_id)
            }
            (None, None) => {
                return Err(StrategySdkError::InvalidCommand(
                    "cancel command requires client_order_id or execution_order_id".to_string(),
                ));
            }
        };
        let command_id = command
            .client_order_id
            .clone()
            .or(command.execution_order_id.clone())
            .map(|id| format!("cancel:{id}"))
            .unwrap_or_else(|| "cancel:unknown".to_string());
        Ok(CancelCommand::new(
            identity,
            command_id,
            exchange_id,
            self.market_type,
            canonical_symbol,
            exchange_symbol,
            cancellation_ids,
        ))
    }
}

fn mutation_identity(
    tenant_id: String,
    account_id: String,
    strategy_id: String,
    run_id: String,
    idempotency_key: String,
    risk_profile_id: String,
    requested_at: chrono::DateTime<Utc>,
) -> SdkResult<MutationIdentity> {
    Ok(MutationIdentity {
        tenant_id: TenantId::new(tenant_id).map_sdk_err()?,
        account_id: AccountId::new(account_id).map_sdk_err()?,
        strategy_id: StrategyId::new(strategy_id).map_sdk_err()?,
        run_id: RunId::new(run_id).map_sdk_err()?,
        idempotency_key,
        risk_profile_id,
        requested_at,
    })
}

fn exchange_symbol_text(exchange: &str, canonical_symbol: &CanonicalSymbol) -> String {
    let base = canonical_symbol.base_asset();
    let quote = canonical_symbol.quote_asset();
    match exchange {
        "gateio" => format!("{base}_{quote}"),
        _ => format!("{base}{quote}"),
    }
}

fn order_side(side: rustcta_strategy_sdk::OrderSide) -> GatewayOrderSide {
    match side {
        rustcta_strategy_sdk::OrderSide::Buy => GatewayOrderSide::Buy,
        rustcta_strategy_sdk::OrderSide::Sell => GatewayOrderSide::Sell,
    }
}

fn order_type(order_type: rustcta_strategy_sdk::OrderType) -> SdkResult<GatewayOrderType> {
    Ok(match order_type {
        rustcta_strategy_sdk::OrderType::Market => GatewayOrderType::Market,
        rustcta_strategy_sdk::OrderType::Limit => GatewayOrderType::Limit,
        rustcta_strategy_sdk::OrderType::PostOnly => GatewayOrderType::PostOnly,
        rustcta_strategy_sdk::OrderType::ImmediateOrCancel => GatewayOrderType::IOC,
        rustcta_strategy_sdk::OrderType::Custom(value) if value.eq_ignore_ascii_case("ioc") => {
            GatewayOrderType::IOC
        }
        rustcta_strategy_sdk::OrderType::Custom(value) => {
            return Err(StrategySdkError::InvalidCommand(format!(
                "unsupported custom order type: {value}"
            )));
        }
    })
}

fn time_in_force(
    time_in_force: rustcta_strategy_sdk::TimeInForce,
) -> SdkResult<GatewayTimeInForce> {
    Ok(match time_in_force {
        rustcta_strategy_sdk::TimeInForce::GoodTilCanceled => GatewayTimeInForce::GTC,
        rustcta_strategy_sdk::TimeInForce::ImmediateOrCancel => GatewayTimeInForce::IOC,
        rustcta_strategy_sdk::TimeInForce::FillOrKill => GatewayTimeInForce::FOK,
        rustcta_strategy_sdk::TimeInForce::PostOnly => GatewayTimeInForce::GTX,
        rustcta_strategy_sdk::TimeInForce::Custom(value) if value.eq_ignore_ascii_case("ioc") => {
            GatewayTimeInForce::IOC
        }
        rustcta_strategy_sdk::TimeInForce::Custom(value) => {
            return Err(StrategySdkError::InvalidCommand(format!(
                "unsupported custom time-in-force: {value}"
            )));
        }
    })
}

fn default_time_in_force(order_type: GatewayOrderType) -> GatewayTimeInForce {
    match order_type {
        GatewayOrderType::IOC => GatewayTimeInForce::IOC,
        GatewayOrderType::FOK => GatewayTimeInForce::FOK,
        GatewayOrderType::PostOnly => GatewayTimeInForce::GTX,
        _ => GatewayTimeInForce::GTC,
    }
}

fn position_side(metadata: &BTreeMap<String, Value>) -> GatewayPositionSide {
    match metadata
        .get("position_side")
        .or_else(|| metadata.get("role"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "long" | "open_long" | "close_long" | "emergency_close_long" => GatewayPositionSide::Long,
        "short" | "open_short" | "close_short" | "emergency_close_short" => {
            GatewayPositionSide::Short
        }
        _ => GatewayPositionSide::Net,
    }
}

fn parse_positive_f64(field: &str, value: &str) -> SdkResult<f64> {
    let parsed = value.parse::<f64>().map_err(|error| {
        StrategySdkError::InvalidCommand(format!("{field} must be a number: {error}"))
    })?;
    if parsed.is_finite() && parsed > 0.0 {
        Ok(parsed)
    } else {
        Err(StrategySdkError::InvalidCommand(format!(
            "{field} must be positive"
        )))
    }
}

trait MapSdkErr<T> {
    fn map_sdk_err(self) -> SdkResult<T>;
}

impl<T, E> MapSdkErr<T> for std::result::Result<T, E>
where
    E: std::fmt::Display,
{
    fn map_sdk_err(self) -> SdkResult<T> {
        self.map_err(|error| StrategySdkError::InvalidCommand(error.to_string()))
    }
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
                        "cross-exchange arbitrage live runner already has a live lock at {}",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_market_type_should_keep_perpetual_and_futures_distinct() {
        assert_eq!(
            gateway_market_type(&SdkMarketType::Perpetual).expect("perpetual"),
            GatewayMarketType::Perpetual
        );
        assert_eq!(
            gateway_market_type(&SdkMarketType::Futures).expect("futures"),
            GatewayMarketType::Futures
        );
    }
}

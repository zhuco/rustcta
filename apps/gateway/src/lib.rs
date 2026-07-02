mod config;

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
pub use config::{GatewayAppConfig, GatewayRestBaseUrls};
use rustcta_exchange_api::{
    ExchangeClient, MarginMode, OrderBookRequest, PerpAccountControlProvider, PositionMode,
    PositionsRequest, RecentFillsRequest, RequestContext, SymbolAccountConfigRequest,
    SymbolRulesRequest, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_gateway::{
    AdapterBackedGateway, GatewayExchangeClient, InProcessGatewayClient,
};
use rustcta_execution_api::{
    CancelCommand, CancellationIds, MutationIdentity, OrderCommand, EXECUTION_API_SCHEMA_VERSION,
};
use rustcta_execution_router::{ExecutionRouter, ExecutionRouterConfig, RouterMode};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    PositionSide, RunId, StrategyId, TenantId, TimeInForce,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone)]
struct StrategyPlatformState {
    gateway: Arc<AdapterBackedGateway>,
    router: Arc<StrategyExecutionRouter>,
}

type StrategyExecutionRouter = ExecutionRouter<InProcessGatewayClient<AdapterBackedGateway>>;

pub fn strategy_platform_router(gateway: Arc<AdapterBackedGateway>) -> Router {
    let router = Arc::new(strategy_execution_router(Arc::clone(&gateway)));
    Router::new()
        .route("/strategy-execution/orders", post(strategy_order))
        .route("/strategy-execution/cancels", post(strategy_cancel))
        .route("/strategy-execution/intents", post(strategy_intent))
        .route(
            "/strategy-platform/market-snapshot",
            post(strategy_market_snapshot),
        )
        .route(
            "/strategy-platform/recent-fills",
            post(strategy_recent_fills),
        )
        .route("/strategy-platform/positions", post(strategy_positions))
        .route(
            "/strategy-platform/account-config",
            post(strategy_account_config),
        )
        .with_state(StrategyPlatformState { gateway, router })
}

#[derive(Debug, Clone, Deserialize)]
struct StrategyOrderCommand {
    schema_version: u32,
    tenant_id: String,
    account_id: String,
    strategy_id: String,
    run_id: String,
    client_order_id: String,
    idempotency_key: String,
    risk_profile_id: String,
    requested_at: DateTime<Utc>,
    exchange_id: String,
    symbol: String,
    side: StrategyOrderSide,
    order_type: StrategyOrderType,
    quantity: String,
    price: Option<String>,
    time_in_force: Option<StrategyTimeInForce>,
    reduce_only: bool,
    #[serde(default)]
    metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyOrderAck {
    schema_version: u32,
    accepted: bool,
    client_order_id: String,
    execution_order_id: Option<String>,
    reason: Option<String>,
    received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
struct StrategyCancelCommand {
    schema_version: u32,
    tenant_id: String,
    account_id: String,
    strategy_id: String,
    run_id: String,
    client_order_id: Option<String>,
    execution_order_id: Option<String>,
    idempotency_key: String,
    risk_profile_id: String,
    requested_at: DateTime<Utc>,
    exchange_id: String,
    symbol: String,
    #[serde(default)]
    metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyCancelAck {
    schema_version: u32,
    accepted: bool,
    client_order_id: Option<String>,
    execution_order_id: Option<String>,
    reason: Option<String>,
    received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
struct StrategyIntentCommand {
    schema_version: u32,
    intent_kind: String,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyIntentAck {
    schema_version: u32,
    accepted: bool,
    intent_kind: String,
    reason: Option<String>,
    received_at: DateTime<Utc>,
    payload: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum StrategyOrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StrategyOrderType {
    Market,
    Limit,
    PostOnly,
    ImmediateOrCancel,
    Custom(String),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StrategyTimeInForce {
    GoodTilCanceled,
    ImmediateOrCancel,
    FillOrKill,
    PostOnly,
    Custom(String),
}

#[derive(Debug, Clone, Deserialize)]
struct StrategyMarketSnapshotRequest {
    schema_version: u32,
    tenant_id: String,
    account_id: String,
    run_id: String,
    exchange_id: String,
    symbol: String,
    market_type: StrategyMarketType,
    requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyMarketSnapshot {
    schema_version: u32,
    exchange_id: String,
    symbol: String,
    market_type: StrategyMarketType,
    best_bid: f64,
    best_ask: f64,
    last_price: f64,
    mark_price: f64,
    tick_size: f64,
    step_size: f64,
    min_qty: Option<f64>,
    min_notional: Option<f64>,
    received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
struct StrategyRecentFillsRequest {
    schema_version: u32,
    tenant_id: String,
    account_id: String,
    run_id: String,
    exchange_id: String,
    symbol: String,
    market_type: StrategyMarketType,
    start_time: Option<DateTime<Utc>>,
    limit: Option<u32>,
    requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyRuntimeFill {
    schema_version: u32,
    exchange_id: String,
    symbol: String,
    market_type: StrategyMarketType,
    fill_id: Option<String>,
    order_id: Option<String>,
    client_order_id: Option<String>,
    side: StrategyOrderSide,
    position_side: String,
    price: f64,
    quantity: f64,
    filled_at: DateTime<Utc>,
    received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyRecentFills {
    schema_version: u32,
    fills: Vec<StrategyRuntimeFill>,
    received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
struct StrategyPositionsRequest {
    schema_version: u32,
    tenant_id: String,
    account_id: String,
    run_id: String,
    exchange_id: String,
    symbol: String,
    market_type: StrategyMarketType,
    requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyRuntimePosition {
    schema_version: u32,
    exchange_id: String,
    symbol: String,
    market_type: StrategyMarketType,
    position_side: String,
    quantity: f64,
    entry_price: f64,
    mark_price: f64,
    observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyPositions {
    schema_version: u32,
    positions: Vec<StrategyRuntimePosition>,
    received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
struct StrategyAccountConfigRequest {
    schema_version: u32,
    tenant_id: String,
    account_id: String,
    run_id: String,
    exchange_id: String,
    symbol: String,
    market_type: StrategyMarketType,
    requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyAccountConfig {
    schema_version: u32,
    exchange_id: String,
    symbol: String,
    market_type: StrategyMarketType,
    position_mode: Option<String>,
    margin_mode: Option<String>,
    leverage: Option<u32>,
    max_leverage: Option<u32>,
    received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum StrategyMarketType {
    Spot,
    Margin,
    Perpetual,
    Futures,
    Option,
}

async fn strategy_order(
    State(state): State<StrategyPlatformState>,
    Json(command): Json<StrategyOrderCommand>,
) -> Response {
    match route_strategy_order(state.router, command).await {
        Ok(ack) => Json(ack).into_response(),
        Err(error) => platform_error(error).into_response(),
    }
}

async fn strategy_cancel(
    State(state): State<StrategyPlatformState>,
    Json(command): Json<StrategyCancelCommand>,
) -> Response {
    match route_strategy_cancel(state.router, command).await {
        Ok(ack) => Json(ack).into_response(),
        Err(error) => platform_error(error).into_response(),
    }
}

async fn strategy_intent(Json(command): Json<StrategyIntentCommand>) -> Json<StrategyIntentAck> {
    Json(StrategyIntentAck {
        schema_version: command.schema_version,
        accepted: false,
        intent_kind: command.intent_kind,
        reason: Some("raw strategy intents are not supported by gateway execution endpoint".into()),
        received_at: Utc::now(),
        payload: json!({}),
    })
}

async fn strategy_market_snapshot(
    State(state): State<StrategyPlatformState>,
    Json(request): Json<StrategyMarketSnapshotRequest>,
) -> Response {
    match load_strategy_market_snapshot(state.gateway, request).await {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(error) => platform_error(error).into_response(),
    }
}

async fn strategy_recent_fills(
    State(state): State<StrategyPlatformState>,
    Json(request): Json<StrategyRecentFillsRequest>,
) -> Response {
    match load_strategy_recent_fills(state.gateway, request).await {
        Ok(fills) => Json(fills).into_response(),
        Err(error) => platform_error(error).into_response(),
    }
}

async fn strategy_positions(
    State(state): State<StrategyPlatformState>,
    Json(request): Json<StrategyPositionsRequest>,
) -> Response {
    match load_strategy_positions(state.gateway, request).await {
        Ok(positions) => Json(positions).into_response(),
        Err(error) => platform_error(error).into_response(),
    }
}

async fn strategy_account_config(
    State(state): State<StrategyPlatformState>,
    Json(request): Json<StrategyAccountConfigRequest>,
) -> Response {
    match load_strategy_account_config(state.gateway, request).await {
        Ok(config) => Json(config).into_response(),
        Err(error) => platform_error(error).into_response(),
    }
}

async fn route_strategy_order(
    router: Arc<StrategyExecutionRouter>,
    command: StrategyOrderCommand,
) -> Result<StrategyOrderAck> {
    let schema_version = command.schema_version;
    let client_order_id = command.client_order_id.clone();
    let order = execution_order_command(command)?;
    let ack = router.place_order(order).await?;
    Ok(StrategyOrderAck {
        schema_version,
        accepted: ack.accepted,
        client_order_id,
        execution_order_id: ack.exchange_order_id,
        reason: ack.message,
        received_at: ack.acknowledged_at,
    })
}

async fn route_strategy_cancel(
    router: Arc<StrategyExecutionRouter>,
    command: StrategyCancelCommand,
) -> Result<StrategyCancelAck> {
    let schema_version = command.schema_version;
    let client_order_id = command.client_order_id.clone();
    let execution_order_id = command.execution_order_id.clone();
    let cancel = execution_cancel_command(command)?;
    let ack = router.cancel_order(cancel).await?;
    Ok(StrategyCancelAck {
        schema_version,
        accepted: ack.accepted,
        client_order_id,
        execution_order_id: ack.exchange_order_id.or(execution_order_id),
        reason: ack.message,
        received_at: ack.acknowledged_at,
    })
}

fn strategy_execution_router(gateway: Arc<AdapterBackedGateway>) -> StrategyExecutionRouter {
    let client = InProcessGatewayClient::new(gateway);
    ExecutionRouter::new(
        ExecutionRouterConfig {
            mode: RouterMode::Live,
            enforce_idempotency: true,
        },
        client,
    )
}

async fn load_strategy_market_snapshot(
    gateway: Arc<AdapterBackedGateway>,
    request: StrategyMarketSnapshotRequest,
) -> Result<StrategyMarketSnapshot> {
    let exchange_id = ExchangeId::new(&request.exchange_id)?;
    let market_type = map_market_type(request.market_type);
    let tenant_id = TenantId::new(&request.tenant_id)?;
    let account_id = AccountId::new(&request.account_id)?;
    let run_id = RunId::new(&request.run_id)?;
    let symbol = symbol_scope(&exchange_id, market_type, &request.symbol)?;
    let client = GatewayExchangeClient::new(
        Arc::new(InProcessGatewayClient::new(gateway)),
        tenant_id.clone(),
        Some(account_id.clone()),
        exchange_id.clone(),
    );
    let mut context = RequestContext::new(request.requested_at);
    context.tenant_id = Some(tenant_id);
    context.account_id = Some(account_id);
    context.run_id = Some(run_id);
    context.request_id = Some(format!(
        "strategy-market-snapshot-{}-{}",
        request.exchange_id, request.symbol
    ));
    let rules = client
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context.clone(),
            symbols: vec![symbol.clone()],
        })
        .await?;
    let rules = rules
        .rules
        .first()
        .context("gateway returned no symbol rules")?;
    let book = client
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context,
            symbol,
            depth: Some(5),
        })
        .await?
        .order_book;
    let best_bid = book
        .best_bid()
        .context("order book missing best bid")?
        .price;
    let best_ask = book
        .best_ask()
        .context("order book missing best ask")?
        .price;
    let mid = (best_bid + best_ask) * 0.5;
    Ok(StrategyMarketSnapshot {
        schema_version: request.schema_version,
        exchange_id: request.exchange_id,
        symbol: request.symbol,
        market_type: request.market_type,
        best_bid,
        best_ask,
        last_price: mid,
        mark_price: mid,
        tick_size: parse_required_rule(&rules.price_increment, "price_increment")?,
        step_size: parse_required_rule(&rules.quantity_increment, "quantity_increment")?,
        min_qty: parse_optional_rule(&rules.min_quantity)?,
        min_notional: parse_optional_rule(&rules.min_notional)?,
        received_at: book.received_at,
    })
}

async fn load_strategy_recent_fills(
    gateway: Arc<AdapterBackedGateway>,
    request: StrategyRecentFillsRequest,
) -> Result<StrategyRecentFills> {
    let exchange_id = ExchangeId::new(&request.exchange_id)?;
    let market_type = map_market_type(request.market_type);
    let tenant_id = TenantId::new(&request.tenant_id)?;
    let account_id = AccountId::new(&request.account_id)?;
    let run_id = RunId::new(&request.run_id)?;
    let symbol = symbol_scope(&exchange_id, market_type, &request.symbol)?;
    let client = GatewayExchangeClient::new(
        Arc::new(InProcessGatewayClient::new(gateway)),
        tenant_id.clone(),
        Some(account_id.clone()),
        exchange_id,
    );
    let mut context = RequestContext::new(request.requested_at);
    context.tenant_id = Some(tenant_id);
    context.account_id = Some(account_id);
    context.run_id = Some(run_id);
    context.request_id = Some(format!(
        "strategy-recent-fills-{}-{}",
        request.exchange_id, request.symbol
    ));
    let response = client
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context,
            exchange: ExchangeId::new(&request.exchange_id)?,
            market_type: Some(market_type),
            symbol: Some(symbol),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: request.start_time,
            end_time: None,
            limit: request.limit,
            page: None,
        })
        .await?;
    let fills = response
        .fills
        .into_iter()
        .map(|fill| StrategyRuntimeFill {
            schema_version: request.schema_version,
            exchange_id: fill.exchange_id.to_string(),
            symbol: fill.canonical_symbol.to_string(),
            market_type: request.market_type,
            fill_id: fill.fill_id,
            order_id: fill.order_id,
            client_order_id: fill.client_order_id,
            side: match fill.side {
                OrderSide::Buy => StrategyOrderSide::Buy,
                OrderSide::Sell => StrategyOrderSide::Sell,
            },
            position_side: match fill.position_side {
                PositionSide::Long => "LONG",
                PositionSide::Short => "SHORT",
                PositionSide::Net => "NET",
                PositionSide::None => "NONE",
            }
            .to_string(),
            price: fill.price,
            quantity: fill.quantity,
            filled_at: fill.filled_at,
            received_at: fill.received_at,
        })
        .collect();
    Ok(StrategyRecentFills {
        schema_version: request.schema_version,
        fills,
        received_at: Utc::now(),
    })
}

async fn load_strategy_positions(
    gateway: Arc<AdapterBackedGateway>,
    request: StrategyPositionsRequest,
) -> Result<StrategyPositions> {
    let exchange_id = ExchangeId::new(&request.exchange_id)?;
    let market_type = map_market_type(request.market_type);
    let tenant_id = TenantId::new(&request.tenant_id)?;
    let account_id = AccountId::new(&request.account_id)?;
    let run_id = RunId::new(&request.run_id)?;
    let symbol = exchange_symbol(&exchange_id, market_type, &request.symbol)?;
    let client = GatewayExchangeClient::new(
        Arc::new(InProcessGatewayClient::new(gateway)),
        tenant_id.clone(),
        Some(account_id.clone()),
        exchange_id.clone(),
    );
    let mut context = RequestContext::new(request.requested_at);
    context.tenant_id = Some(tenant_id);
    context.account_id = Some(account_id);
    context.run_id = Some(run_id);
    context.request_id = Some(format!(
        "strategy-positions-{}-{}",
        request.exchange_id, request.symbol
    ));
    let response = client
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context,
            exchange: exchange_id,
            market_type: Some(market_type),
            symbols: vec![symbol],
        })
        .await?;
    let positions = response
        .positions
        .into_iter()
        .map(|position| StrategyRuntimePosition {
            schema_version: request.schema_version,
            exchange_id: position.exchange_id.to_string(),
            symbol: request.symbol.clone(),
            market_type: request.market_type,
            position_side: position_side_label(position.side),
            quantity: position.quantity,
            entry_price: position.entry_price.unwrap_or(0.0),
            mark_price: position.mark_price.unwrap_or(0.0),
            observed_at: position.observed_at,
        })
        .collect();
    Ok(StrategyPositions {
        schema_version: request.schema_version,
        positions,
        received_at: Utc::now(),
    })
}

async fn load_strategy_account_config(
    gateway: Arc<AdapterBackedGateway>,
    request: StrategyAccountConfigRequest,
) -> Result<StrategyAccountConfig> {
    let exchange_id = ExchangeId::new(&request.exchange_id)?;
    let market_type = map_market_type(request.market_type);
    let tenant_id = TenantId::new(&request.tenant_id)?;
    let account_id = AccountId::new(&request.account_id)?;
    let run_id = RunId::new(&request.run_id)?;
    let symbol = symbol_scope(&exchange_id, market_type, &request.symbol)?;
    let client = GatewayExchangeClient::new(
        Arc::new(InProcessGatewayClient::new(gateway)),
        tenant_id.clone(),
        Some(account_id.clone()),
        exchange_id,
    );
    let mut context = RequestContext::new(request.requested_at);
    context.tenant_id = Some(tenant_id);
    context.account_id = Some(account_id);
    context.run_id = Some(run_id);
    context.request_id = Some(format!(
        "strategy-account-config-{}-{}",
        request.exchange_id, request.symbol
    ));
    let response = client
        .get_symbol_account_config(SymbolAccountConfigRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context,
            symbol,
        })
        .await?;
    Ok(StrategyAccountConfig {
        schema_version: request.schema_version,
        exchange_id: request.exchange_id,
        symbol: request.symbol,
        market_type: request.market_type,
        position_mode: response.config.position_mode.map(position_mode_label),
        margin_mode: response.config.margin_mode.map(margin_mode_label),
        leverage: response.config.leverage,
        max_leverage: response.config.max_leverage,
        received_at: response.config.updated_at,
    })
}

fn execution_order_command(command: StrategyOrderCommand) -> Result<OrderCommand> {
    let exchange_id = ExchangeId::new(&command.exchange_id)?;
    let market_type = command_market_type(&command.metadata)?;
    let canonical_symbol = canonical_symbol(&command.symbol)?;
    let exchange_symbol = exchange_symbol(&exchange_id, market_type, &command.symbol)?;
    let mut order = OrderCommand::new(
        mutation_identity(
            &command.tenant_id,
            &command.account_id,
            &command.strategy_id,
            &command.run_id,
            &command.idempotency_key,
            &command.risk_profile_id,
            command.requested_at,
        )?,
        command.client_order_id.clone(),
        exchange_id,
        market_type,
        canonical_symbol,
        exchange_symbol,
        command.client_order_id,
        map_order_side(command.side),
        command_position_side(market_type, &command.metadata),
        map_order_type(command.order_type)?,
        map_time_in_force(command.time_in_force)?,
        command
            .quantity
            .parse()
            .context("quantity must be numeric")?,
        command
            .price
            .as_deref()
            .map(str::parse::<f64>)
            .transpose()
            .context("price must be numeric")?,
    );
    order.post_only = matches!(order.order_type, OrderType::PostOnly)
        || command
            .metadata
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    order.reduce_only = command.reduce_only;
    order.source_intent_id = command
        .metadata
        .get("intent")
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok(order)
}

fn execution_cancel_command(command: StrategyCancelCommand) -> Result<CancelCommand> {
    let exchange_id = ExchangeId::new(&command.exchange_id)?;
    let market_type = command_market_type(&command.metadata)?;
    let cancellation_ids = CancellationIds {
        client_order_id: command.client_order_id,
        exchange_order_id: command.execution_order_id,
    };
    let mut cancel = CancelCommand::new(
        mutation_identity(
            &command.tenant_id,
            &command.account_id,
            &command.strategy_id,
            &command.run_id,
            &command.idempotency_key,
            &command.risk_profile_id,
            command.requested_at,
        )?,
        command.idempotency_key.clone(),
        exchange_id.clone(),
        market_type,
        canonical_symbol(&command.symbol)?,
        exchange_symbol(&exchange_id, market_type, &command.symbol)?,
        cancellation_ids,
    );
    cancel.reason = command
        .metadata
        .get("cancel_reason")
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok(cancel)
}

fn mutation_identity(
    tenant_id: &str,
    account_id: &str,
    strategy_id: &str,
    run_id: &str,
    idempotency_key: &str,
    risk_profile_id: &str,
    requested_at: DateTime<Utc>,
) -> Result<MutationIdentity> {
    Ok(MutationIdentity {
        tenant_id: TenantId::new(tenant_id)?,
        account_id: AccountId::new(account_id)?,
        strategy_id: StrategyId::new(strategy_id)?,
        run_id: RunId::new(run_id)?,
        idempotency_key: idempotency_key.to_string(),
        risk_profile_id: risk_profile_id.to_string(),
        requested_at,
    })
}

fn symbol_scope(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: &str,
) -> Result<SymbolScope> {
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol(symbol)?),
        exchange_symbol: exchange_symbol(exchange_id, market_type, symbol)?,
    })
}

fn canonical_symbol(symbol: &str) -> Result<CanonicalSymbol> {
    if symbol.contains('/') {
        return Ok(CanonicalSymbol::parse(symbol)?);
    }
    for quote in ["USDT", "USDC", "USD", "BTC", "ETH"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return Ok(CanonicalSymbol::new(base, quote)?);
            }
        }
    }
    bail!("cannot infer canonical symbol from {symbol}")
}

fn exchange_symbol(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: &str,
) -> Result<ExchangeSymbol> {
    ExchangeSymbol::new(exchange_id.clone(), market_type, symbol.replace('/', ""))
        .map_err(Into::into)
}

fn command_market_type(metadata: &BTreeMap<String, Value>) -> Result<MarketType> {
    if let Some(market_type) = metadata
        .get("market_type")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
    {
        return match market_type.as_str() {
            "spot" => Ok(MarketType::Spot),
            "margin" => Ok(MarketType::Margin),
            "perpetual" | "perp" | "swap" => Ok(MarketType::Perpetual),
            "futures" | "future" => Ok(MarketType::Futures),
            "option" | "options" => Ok(MarketType::Option),
            _ => bail!("unsupported strategy order market_type {market_type}"),
        };
    }
    Ok(
        match metadata
            .get("position_side")
            .and_then(Value::as_str)
            .map(str::to_ascii_uppercase)
            .as_deref()
        {
            Some("SHORT") => MarketType::Perpetual,
            _ => MarketType::Spot,
        },
    )
}

fn command_position_side(
    market_type: MarketType,
    metadata: &BTreeMap<String, Value>,
) -> PositionSide {
    if market_type == MarketType::Spot {
        return PositionSide::None;
    }
    match metadata
        .get("position_side")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .as_deref()
    {
        Some("LONG") => PositionSide::Long,
        Some("SHORT") => PositionSide::Short,
        Some("NET") => PositionSide::Net,
        _ => PositionSide::None,
    }
}

fn map_order_side(side: StrategyOrderSide) -> OrderSide {
    match side {
        StrategyOrderSide::Buy => OrderSide::Buy,
        StrategyOrderSide::Sell => OrderSide::Sell,
    }
}

fn map_order_type(order_type: StrategyOrderType) -> Result<OrderType> {
    match order_type {
        StrategyOrderType::Market => Ok(OrderType::Market),
        StrategyOrderType::Limit => Ok(OrderType::Limit),
        StrategyOrderType::PostOnly => Ok(OrderType::PostOnly),
        StrategyOrderType::ImmediateOrCancel => Ok(OrderType::IOC),
        StrategyOrderType::Custom(value) => bail!("unsupported custom order_type {value}"),
    }
}

fn map_time_in_force(time_in_force: Option<StrategyTimeInForce>) -> Result<TimeInForce> {
    match time_in_force.unwrap_or(StrategyTimeInForce::GoodTilCanceled) {
        StrategyTimeInForce::GoodTilCanceled => Ok(TimeInForce::GTC),
        StrategyTimeInForce::ImmediateOrCancel => Ok(TimeInForce::IOC),
        StrategyTimeInForce::FillOrKill => Ok(TimeInForce::FOK),
        StrategyTimeInForce::PostOnly => Ok(TimeInForce::GTX),
        StrategyTimeInForce::Custom(value) => bail!("unsupported custom time_in_force {value}"),
    }
}

fn map_market_type(market_type: StrategyMarketType) -> MarketType {
    match market_type {
        StrategyMarketType::Spot => MarketType::Spot,
        StrategyMarketType::Margin => MarketType::Margin,
        StrategyMarketType::Perpetual => MarketType::Perpetual,
        StrategyMarketType::Futures => MarketType::Futures,
        StrategyMarketType::Option => MarketType::Option,
    }
}

fn position_mode_label(mode: PositionMode) -> String {
    match mode {
        PositionMode::OneWay => "one_way",
        PositionMode::Hedge => "hedge",
    }
    .to_string()
}

fn margin_mode_label(mode: MarginMode) -> String {
    match mode {
        MarginMode::Cross => "cross",
        MarginMode::Isolated => "isolated",
        MarginMode::Unknown => "unknown",
    }
    .to_string()
}

fn position_side_label(side: PositionSide) -> String {
    match side {
        PositionSide::Long => "LONG",
        PositionSide::Short => "SHORT",
        PositionSide::Net => "NET",
        PositionSide::None => "NONE",
    }
    .to_string()
}

fn parse_required_rule(value: &Option<String>, field: &str) -> Result<f64> {
    value
        .as_deref()
        .context(format!("symbol rules missing {field}"))?
        .parse()
        .with_context(|| format!("symbol rule {field} must be numeric"))
}

fn parse_optional_rule(value: &Option<String>) -> Result<Option<f64>> {
    value
        .as_deref()
        .map(str::parse::<f64>)
        .transpose()
        .context("symbol rule numeric field is invalid")
}

fn platform_error(error: anyhow::Error) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "schema_version": EXECUTION_API_SCHEMA_VERSION,
            "accepted": false,
            "error": error.to_string(),
            "responded_at": Utc::now(),
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use tower::ServiceExt;

    fn strategy_order_payload(
        client_order_id: &str,
        idempotency_key: &str,
        metadata: Value,
    ) -> Value {
        json!({
            "schema_version": 1,
            "tenant_id": "local",
            "account_id": "default",
            "strategy_id": "hedged_grid_SOL",
            "run_id": "test-run",
            "client_order_id": client_order_id,
            "idempotency_key": idempotency_key,
            "risk_profile_id": "hedged-grid-live",
            "requested_at": Utc::now(),
            "exchange_id": "paper",
            "symbol": "BTCUSDT",
            "side": "buy",
            "order_type": "post_only",
            "quantity": "0.01",
            "price": "25000",
            "time_in_force": "post_only",
            "reduce_only": false,
            "metadata": metadata,
        })
    }

    async fn post_json(app: Router, path: &str, payload: Value) -> (StatusCode, Value) {
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(payload.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response bytes");
        let value = serde_json::from_slice(&bytes).expect("json response");
        (status, value)
    }

    #[tokio::test]
    async fn strategy_execution_endpoint_should_persist_idempotency_across_http_requests() {
        let gateway =
            Arc::new(AdapterBackedGateway::paper_only("strategy-test").expect("paper gateway"));
        let app = strategy_platform_router(gateway);
        let metadata = json!({
            "market_type": "spot",
            "position_side": "NONE",
            "post_only": true,
        });

        let (first_status, first) = post_json(
            app.clone(),
            "/strategy-execution/orders",
            strategy_order_payload("grid-1", "same-idem", metadata.clone()),
        )
        .await;
        assert_eq!(first_status, StatusCode::OK);
        assert_eq!(first["accepted"], true);

        let (second_status, second) = post_json(
            app,
            "/strategy-execution/orders",
            strategy_order_payload("grid-2", "same-idem", metadata),
        )
        .await;
        assert_eq!(second_status, StatusCode::OK);
        assert_eq!(second["accepted"], false);
        assert!(second["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("duplicate idempotency_key"));
    }

    #[tokio::test]
    async fn strategy_execution_endpoint_should_reject_invalid_market_type_metadata() {
        let gateway =
            Arc::new(AdapterBackedGateway::paper_only("strategy-test").expect("paper gateway"));
        let app = strategy_platform_router(gateway);
        let (status, response) = post_json(
            app,
            "/strategy-execution/orders",
            strategy_order_payload(
                "grid-bad",
                "bad-market-type",
                json!({
                    "market_type": "not-a-market",
                    "post_only": true,
                }),
            ),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(response["error"]
            .as_str()
            .unwrap_or_default()
            .contains("unsupported strategy order market_type"));
    }
}

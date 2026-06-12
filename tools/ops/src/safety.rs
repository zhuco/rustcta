use anyhow::{anyhow, bail, Context, Result};
use clap::{Args as ClapArgs, ValueEnum};
use rustcta_types::CanonicalSymbol;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, ClapArgs)]
pub struct ExchangeOrderCanarySafetyArgs {
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    pub config: PathBuf,
    #[arg(long, default_value = "bitget")]
    pub exchange: String,
    #[arg(long, default_value = "DOGE/USDT")]
    pub symbol: String,
    #[arg(long, value_enum, default_value_t = CanarySide::Long)]
    pub side: CanarySide,
    #[arg(long, value_enum, default_value_t = CanaryMode::MarketRoundTrip)]
    pub mode: CanaryMode,
    #[arg(long, default_value_t = 10.0)]
    pub notional_usdt: f64,
    #[arg(long, default_value_t = 12.0)]
    pub max_estimated_notional_usdt: f64,
    #[arg(long, default_value_t = 0.001)]
    pub max_slippage_pct: f64,
    #[arg(long, default_value_t = false)]
    pub execute: bool,
    #[arg(long, default_value_t = false)]
    pub confirm_live_order: bool,
}

#[derive(Debug, Clone, ClapArgs)]
pub struct BitgetPerpOrderCanarySafetyArgs {
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    pub config: PathBuf,
    #[arg(long, default_value = "BTC/USDT")]
    pub symbol: String,
    #[arg(long, value_enum, default_value_t = CanarySide::Long)]
    pub side: CanarySide,
    #[arg(long, default_value_t = 10.0)]
    pub notional_usdt: f64,
    #[arg(long, default_value_t = 11.0)]
    pub max_estimated_notional_usdt: f64,
    #[arg(long, default_value_t = 0.001)]
    pub max_slippage_pct: f64,
    #[arg(long, default_value_t = false)]
    pub execute: bool,
    #[arg(long, default_value_t = false)]
    pub confirm_live_order: bool,
}

#[derive(Debug, Clone, ClapArgs)]
pub struct BitgetSpotOrderCanarySafetyArgs {
    #[arg(
        long,
        default_value = "config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml"
    )]
    pub config: PathBuf,
    #[arg(long, default_value = "WLDUSDT")]
    pub symbol: String,
    #[arg(long, default_value_t = 3.2)]
    pub notional_usdt: f64,
    #[arg(long)]
    pub quantity: Option<f64>,
    #[arg(long, value_enum, default_value_t = SpotSide::Buy)]
    pub side: SpotSide,
    #[arg(long, default_value_t = false)]
    pub execute: bool,
    #[arg(long, default_value_t = false)]
    pub confirm_live_order: bool,
    #[arg(long)]
    pub query_order_id: Option<String>,
}

#[derive(Debug, Clone, ClapArgs)]
pub struct CrossArbAccountAuditSafetyArgs {
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    pub config: PathBuf,
}

#[derive(Debug, Clone, ClapArgs)]
pub struct CrossArbFeeAuditSafetyArgs {
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    pub config: PathBuf,
    #[arg(long, value_delimiter = ',', default_value = "ALLO/USDT")]
    pub symbols: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    pub exchanges: Vec<String>,
    #[arg(long, default_value_t = 100)]
    pub limit: u32,
    #[arg(long, default_value_t = 24)]
    pub since_hours: i64,
    #[arg(long, default_value_t = 0.20)]
    pub binance_rebate: f64,
    #[arg(long, default_value_t = 0.60)]
    pub other_rebate: f64,
}

#[derive(Debug, Clone, ClapArgs)]
pub struct CrossArbOrderAdminSafetyArgs {
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    pub config: PathBuf,
    #[arg(long, value_enum)]
    pub action: AdminAction,
    #[arg(long, default_value = "binance")]
    pub exchange: String,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long)]
    pub exchange_order_id: Option<String>,
    #[arg(long)]
    pub client_order_id: Option<String>,
    #[arg(long, default_value_t = false)]
    pub execute: bool,
    #[arg(long, default_value_t = false)]
    pub confirm_cancel: bool,
    #[arg(long, default_value_t = false)]
    pub confirm_order: bool,
    #[arg(long)]
    pub quantity: Option<f64>,
    #[arg(long, value_enum)]
    pub position_side: Option<AdminPositionSide>,
}

#[derive(Debug, Clone, ClapArgs)]
pub struct FundingArbObserveSafetyArgs {
    #[arg(long, default_value = "config/funding_rate_arbitrage_usdt.yml")]
    pub config: PathBuf,
    #[arg(long, default_value_t = false)]
    pub no_notify: bool,
    #[arg(long, default_value_t = 15_000)]
    pub request_timeout_ms: u64,
}

#[derive(Debug, Clone, ClapArgs)]
pub struct CrossArbWsOpportunityProbeSafetyArgs {
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    pub config: PathBuf,
    #[arg(long, default_value_t = 220)]
    pub max_symbols: usize,
    #[arg(long, default_value_t = 120)]
    pub seconds: u64,
    #[arg(long, default_value_t = 0.004)]
    pub min_raw_spread: f64,
    #[arg(long, default_value_t = 0.05)]
    pub max_raw_spread: f64,
    #[arg(long, default_value_t = -1.0)]
    pub min_net_edge: f64,
    #[arg(long, default_value_t = 80)]
    pub ws_batch_size: usize,
    #[arg(long, default_value_t = false)]
    pub all_updates: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CanarySide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryMode {
    MarketRoundTrip,
    LimitPostOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SpotSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminAction {
    ListOpen,
    ListPositions,
    Query,
    Cancel,
    ClosePosition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminPositionSide {
    Long,
    Short,
    Net,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SafetyGateMode {
    DryRunPlanOnly,
    LegacyLiveRequired,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SafetyPlan {
    pub legacy_binary: &'static str,
    pub new_command: &'static str,
    pub gate_mode: SafetyGateMode,
    pub config: String,
    pub config_dry_run: Option<bool>,
    pub config_trading_enabled: Option<bool>,
    pub execute_requested: bool,
    pub live_order_confirmed: Option<bool>,
    pub mutation_confirmed: Option<bool>,
    pub planned_side_effects: Vec<&'static str>,
    pub safety_checks: Vec<String>,
    pub output_fields_preserved: Vec<&'static str>,
    pub request: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct CrossArbConfigSafetyView {
    #[serde(default)]
    execution: CrossArbExecutionSafetyView,
    #[serde(default)]
    universe: CrossArbUniverseSafetyView,
}

#[derive(Debug, Default, Deserialize)]
struct CrossArbExecutionSafetyView {
    #[serde(default)]
    dry_run: Option<bool>,
    #[serde(default)]
    trading_enabled: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct CrossArbUniverseSafetyView {
    #[serde(default)]
    enabled_exchanges: Vec<String>,
}

pub fn exchange_order_canary_safety_plan(args: ExchangeOrderCanarySafetyArgs) -> Result<String> {
    validate_live_order_gate(args.execute, args.confirm_live_order)?;
    validate_perp_canary_notional(args.notional_usdt, args.max_estimated_notional_usdt)?;
    validate_usdt_canonical_symbol(&args.symbol, "canary only supports USDT quote symbols")?;
    validate_supported_perp_canary_exchange(&args.exchange)?;

    let config = load_cross_arb_config_safety_view(&args.config)?;
    let trading_enabled = cross_arb_trading_enabled(&config);
    validate_cross_arb_trading_gate(args.execute, trading_enabled)?;
    validate_enabled_exchange(&args.exchange, &config.universe.enabled_exchanges)?;

    render_plan(SafetyPlan {
        legacy_binary: "exchange_order_canary",
        new_command: "rustcta-tools-ops canary exchange-order",
        gate_mode: SafetyGateMode::DryRunPlanOnly,
        config: args.config.display().to_string(),
        config_dry_run: config.execution.dry_run,
        config_trading_enabled: Some(trading_enabled),
        execute_requested: args.execute,
        live_order_confirmed: Some(args.confirm_live_order),
        mutation_confirmed: None,
        planned_side_effects: vec![],
        safety_checks: vec![
            "--execute requires --confirm-live-order".to_string(),
            "config execution.trading_enabled must match execution intent".to_string(),
            "canary notional must be > 0 and <= 10 USDT".to_string(),
            "max estimated canary notional must be > 0 and <= 20 USDT".to_string(),
            "symbol must use USDT quote".to_string(),
            "exchange must be enabled in config universe".to_string(),
        ],
        output_fields_preserved: vec![
            "generated_at",
            "execute_requested",
            "live_order_confirmed",
            "exchange",
            "symbol",
            "side",
            "mode",
            "requested_notional_usdt",
            "planned_quantity",
            "estimated_notional_usdt",
            "max_estimated_notional_usdt",
            "best_bid",
            "best_ask",
            "planned_limit_price",
            "instrument",
            "account",
            "balances_before",
            "open_orders_before",
            "positions_before",
            "nonzero_positions_before",
            "net_position_qty_before",
            "fills_before",
            "open_ack",
            "close_ack",
            "cancel_ack",
            "positions_after",
            "nonzero_positions_after",
            "net_position_qty_after",
            "open_orders_after",
            "warnings",
        ],
        request: json!({
            "exchange": normalize_exchange(&args.exchange),
            "symbol": canonical_symbol_string(&args.symbol)?,
            "side": args.side,
            "mode": args.mode,
            "notional_usdt": args.notional_usdt,
            "max_estimated_notional_usdt": args.max_estimated_notional_usdt,
            "max_slippage_pct": args.max_slippage_pct,
        }),
    })
}

pub fn bitget_perp_order_canary_safety_plan(
    args: BitgetPerpOrderCanarySafetyArgs,
) -> Result<String> {
    validate_live_order_gate(args.execute, args.confirm_live_order)?;
    validate_perp_canary_notional(args.notional_usdt, args.max_estimated_notional_usdt)?;
    validate_usdt_canonical_symbol(
        &args.symbol,
        "bitget canary only supports USDT quote symbols",
    )?;

    let config = load_cross_arb_config_safety_view(&args.config)?;
    let trading_enabled = cross_arb_trading_enabled(&config);
    validate_cross_arb_trading_gate(args.execute, trading_enabled)?;

    render_plan(SafetyPlan {
        legacy_binary: "bitget_order_canary",
        new_command: "rustcta-tools-ops canary bitget-perp-order",
        gate_mode: SafetyGateMode::DryRunPlanOnly,
        config: args.config.display().to_string(),
        config_dry_run: config.execution.dry_run,
        config_trading_enabled: Some(trading_enabled),
        execute_requested: args.execute,
        live_order_confirmed: Some(args.confirm_live_order),
        mutation_confirmed: None,
        planned_side_effects: vec![],
        safety_checks: vec![
            "--execute requires --confirm-live-order".to_string(),
            "config execution.trading_enabled must match execution intent".to_string(),
            "canary notional must be > 0 and <= 10 USDT".to_string(),
            "max estimated canary notional must be > 0 and <= 20 USDT".to_string(),
            "symbol must use USDT quote".to_string(),
            "legacy live path still enforces Bitget hedge mode".to_string(),
        ],
        output_fields_preserved: vec![
            "generated_at",
            "execute_requested",
            "live_order_confirmed",
            "exchange",
            "symbol",
            "side",
            "requested_notional_usdt",
            "planned_quantity",
            "estimated_notional_usdt",
            "max_estimated_notional_usdt",
            "best_bid",
            "best_ask",
            "instrument",
            "account",
            "balances_before",
            "open_orders_before",
            "positions_before",
            "open_ack",
            "close_ack",
            "positions_after",
            "open_orders_after",
            "warnings",
        ],
        request: json!({
            "exchange": "bitget",
            "symbol": canonical_symbol_string(&args.symbol)?,
            "side": args.side,
            "notional_usdt": args.notional_usdt,
            "max_estimated_notional_usdt": args.max_estimated_notional_usdt,
            "max_slippage_pct": args.max_slippage_pct,
        }),
    })
}

pub fn bitget_spot_order_canary_safety_plan(
    args: BitgetSpotOrderCanarySafetyArgs,
) -> Result<String> {
    validate_live_order_gate(args.execute, args.confirm_live_order)?;
    let max_notional_usdt = match args.side {
        SpotSide::Buy => 3.2,
        SpotSide::Sell => 10.0,
    };
    if !args.notional_usdt.is_finite()
        || args.notional_usdt <= 0.0
        || args.notional_usdt > max_notional_usdt
    {
        bail!("--notional-usdt must be > 0 and <= {max_notional_usdt}");
    }
    if let Some(quantity) = args.quantity {
        if !quantity.is_finite() || quantity <= 0.0 {
            bail!("--quantity must be > 0");
        }
    }

    render_plan(SafetyPlan {
        legacy_binary: "bitget_spot_order_canary",
        new_command: "rustcta-tools-ops canary bitget-spot-order",
        gate_mode: SafetyGateMode::DryRunPlanOnly,
        config: args.config.display().to_string(),
        config_dry_run: None,
        config_trading_enabled: None,
        execute_requested: args.execute,
        live_order_confirmed: Some(args.confirm_live_order),
        mutation_confirmed: None,
        planned_side_effects: vec![],
        safety_checks: vec![
            "--execute requires --confirm-live-order".to_string(),
            "buy notional must be > 0 and <= 3.2 USDT".to_string(),
            "sell notional must be > 0 and <= 10 USDT".to_string(),
            "optional quantity must be positive".to_string(),
            "legacy live path keeps bitget_config.dry_run = !execute".to_string(),
        ],
        output_fields_preserved: vec![
            "execute",
            "symbol",
            "exchange_symbol",
            "side",
            "best_bid",
            "best_ask",
            "price",
            "quantity",
            "estimated_notional",
            "submitted",
            "order_id",
            "status",
        ],
        request: json!({
            "exchange": "bitget",
            "symbol": args.symbol,
            "side": args.side,
            "notional_usdt": args.notional_usdt,
            "quantity": args.quantity,
            "query_order_id": args.query_order_id,
        }),
    })
}

pub fn cross_arb_account_audit_safety_plan(args: CrossArbAccountAuditSafetyArgs) -> Result<String> {
    let config = load_cross_arb_config_safety_view(&args.config)?;
    render_plan(SafetyPlan {
        legacy_binary: "cross_arb_account_audit",
        new_command: "rustcta-tools-ops audit cross-arb-account",
        gate_mode: SafetyGateMode::DryRunPlanOnly,
        config: args.config.display().to_string(),
        config_dry_run: config.execution.dry_run,
        config_trading_enabled: Some(cross_arb_trading_enabled(&config)),
        execute_requested: false,
        live_order_confirmed: None,
        mutation_confirmed: None,
        planned_side_effects: vec![],
        safety_checks: vec![
            "new tools command renders a private-read plan only".to_string(),
            "legacy binary remains owner of account adapter bootstrap".to_string(),
        ],
        output_fields_preserved: vec![
            "exchanges",
            "exchange",
            "balances",
            "positions",
            "open_orders",
            "strategy_open_orders",
            "external_open_orders",
        ],
        request: json!({
            "enabled_exchanges": config.universe.enabled_exchanges,
        }),
    })
}

pub fn cross_arb_fee_audit_safety_plan(args: CrossArbFeeAuditSafetyArgs) -> Result<String> {
    if args.limit == 0 {
        bail!("--limit must be > 0");
    }
    if args.since_hours <= 0 {
        bail!("--since-hours must be > 0");
    }
    validate_rebate("binance", args.binance_rebate)?;
    validate_rebate("other", args.other_rebate)?;
    let symbols = args
        .symbols
        .iter()
        .map(|symbol| canonical_symbol_string(symbol))
        .collect::<Result<Vec<_>>>()?;
    let config = load_cross_arb_config_safety_view(&args.config)?;
    let exchanges = if args.exchanges.is_empty() {
        config.universe.enabled_exchanges.clone()
    } else {
        args.exchanges
            .iter()
            .map(|value| normalize_exchange(value))
            .collect()
    };
    for exchange in &exchanges {
        validate_supported_audit_exchange(exchange)?;
    }

    let mut rebate = BTreeMap::new();
    rebate.insert("binance", args.binance_rebate);
    rebate.insert("bitget", args.other_rebate);
    rebate.insert("gate", args.other_rebate);

    render_plan(SafetyPlan {
        legacy_binary: "cross_arb_fee_audit",
        new_command: "rustcta-tools-ops audit cross-arb-fee",
        gate_mode: SafetyGateMode::DryRunPlanOnly,
        config: args.config.display().to_string(),
        config_dry_run: config.execution.dry_run,
        config_trading_enabled: Some(cross_arb_trading_enabled(&config)),
        execute_requested: false,
        live_order_confirmed: None,
        mutation_confirmed: None,
        planned_side_effects: vec![],
        safety_checks: vec![
            "new tools command validates symbols, exchanges, limit, lookback, and rebates only"
                .to_string(),
            "legacy binary remains owner of private fill reads".to_string(),
        ],
        output_fields_preserved: vec![
            "generated_at",
            "config",
            "symbols",
            "since",
            "limit_per_symbol",
            "rebate",
            "summaries",
        ],
        request: json!({
            "symbols": symbols,
            "exchanges": exchanges,
            "limit": args.limit,
            "since_hours": args.since_hours,
            "rebate": rebate,
        }),
    })
}

pub fn cross_arb_order_admin_safety_plan(args: CrossArbOrderAdminSafetyArgs) -> Result<String> {
    validate_supported_admin_exchange(&args.exchange)?;
    let canonical_symbol = args
        .symbol
        .as_deref()
        .map(canonical_symbol_string)
        .transpose()?;
    if canonical_symbol.is_none()
        && !matches!(
            args.action,
            AdminAction::ListOpen | AdminAction::ListPositions
        )
    {
        bail!("--action query/cancel/close-position requires --symbol");
    }
    if matches!(args.action, AdminAction::Query | AdminAction::Cancel)
        && args.exchange_order_id.is_none()
        && args.client_order_id.is_none()
    {
        bail!("--action query/cancel requires --exchange-order-id or --client-order-id");
    }
    if args.action == AdminAction::Cancel && (!args.execute || !args.confirm_cancel) {
        bail!("--action cancel requires --execute --confirm-cancel");
    }
    if args.action == AdminAction::ClosePosition {
        if !args.execute || !args.confirm_order {
            bail!("--action close-position requires --execute --confirm-order");
        }
        let quantity = args
            .quantity
            .filter(|quantity| quantity.is_finite() && *quantity > 0.0)
            .ok_or_else(|| anyhow!("--action close-position requires positive --quantity"))?;
        if quantity <= 0.0 {
            bail!("--action close-position requires positive --quantity");
        }
        args.position_side
            .ok_or_else(|| anyhow!("--action close-position requires --position-side"))?;
    }

    let config = load_cross_arb_config_safety_view(&args.config)?;
    let mutation_confirmed = match args.action {
        AdminAction::Cancel => Some(args.execute && args.confirm_cancel),
        AdminAction::ClosePosition => Some(args.execute && args.confirm_order),
        _ => Some(!args.execute),
    };

    render_plan(SafetyPlan {
        legacy_binary: "cross_arb_order_admin",
        new_command: "rustcta-tools-ops admin cross-arb-order",
        gate_mode: SafetyGateMode::DryRunPlanOnly,
        config: args.config.display().to_string(),
        config_dry_run: config.execution.dry_run,
        config_trading_enabled: Some(cross_arb_trading_enabled(&config)),
        execute_requested: args.execute,
        live_order_confirmed: None,
        mutation_confirmed,
        planned_side_effects: vec![],
        safety_checks: vec![
            "query/cancel/close-position require --symbol".to_string(),
            "query/cancel require an exchange or client order id".to_string(),
            "cancel requires --execute --confirm-cancel".to_string(),
            "close-position requires --execute --confirm-order".to_string(),
            "close-position requires positive --quantity and --position-side".to_string(),
            "legacy binary remains owner of private query/cancel/close calls".to_string(),
        ],
        output_fields_preserved: vec![
            "generated_at",
            "action",
            "execute",
            "exchange",
            "canonical_symbol",
            "exchange_symbol",
            "exchange_order_id",
            "client_order_id",
            "positions_before",
            "open_orders_before",
            "query_result",
            "query_error",
            "cancel_ack",
            "cancel_error",
            "close_ack",
            "close_error",
            "positions_after",
            "open_orders_after",
        ],
        request: json!({
            "action": args.action,
            "exchange": normalize_exchange(&args.exchange),
            "symbol": canonical_symbol,
            "exchange_order_id": args.exchange_order_id,
            "client_order_id": args.client_order_id,
            "quantity": args.quantity,
            "position_side": args.position_side,
        }),
    })
}

pub fn funding_arb_observe_safety_plan(args: FundingArbObserveSafetyArgs) -> Result<String> {
    if args.request_timeout_ms == 0 {
        bail!("--request-timeout-ms must be > 0");
    }

    render_plan(SafetyPlan {
        legacy_binary: "funding_arb_observe",
        new_command: "rustcta-tools-ops probe funding-arb-observe",
        gate_mode: SafetyGateMode::DryRunPlanOnly,
        config: args.config.display().to_string(),
        config_dry_run: None,
        config_trading_enabled: None,
        execute_requested: false,
        live_order_confirmed: None,
        mutation_confirmed: Some(false),
        planned_side_effects: vec![],
        safety_checks: vec![
            "tools/ops renders an observe plan only".to_string(),
            "legacy observe mode requires trading execution to stay disabled".to_string(),
            "private trading adapters are not bootstrapped by tools/ops".to_string(),
            "startup notification delivery is not performed by this plan command".to_string(),
        ],
        output_fields_preserved: vec![
            "opportunities",
            "exchange",
            "symbol",
            "funding_rate",
            "next_funding_time",
            "net_edge",
            "notifications_enabled",
        ],
        request: json!({
            "config": args.config,
            "no_notify": args.no_notify,
            "request_timeout_ms": args.request_timeout_ms,
            "network_access": "disabled",
            "live_order_access": "disabled",
        }),
    })
}

pub fn cross_arb_ws_opportunity_probe_safety_plan(
    args: CrossArbWsOpportunityProbeSafetyArgs,
) -> Result<String> {
    if args.max_symbols == 0 {
        bail!("--max-symbols must be > 0");
    }
    if args.seconds == 0 {
        bail!("--seconds must be > 0");
    }
    if args.ws_batch_size == 0 {
        bail!("--ws-batch-size must be > 0");
    }
    if !args.min_raw_spread.is_finite() || !args.max_raw_spread.is_finite() {
        bail!("raw spread bounds must be finite");
    }
    if args.min_raw_spread > args.max_raw_spread {
        bail!("--min-raw-spread must be <= --max-raw-spread");
    }
    if !args.min_net_edge.is_finite() {
        bail!("--min-net-edge must be finite");
    }

    render_plan(SafetyPlan {
        legacy_binary: "cross_arb_ws_opportunity_probe",
        new_command: "rustcta-tools-ops probe cross-arb-ws-opportunity",
        gate_mode: SafetyGateMode::DryRunPlanOnly,
        config: args.config.display().to_string(),
        config_dry_run: None,
        config_trading_enabled: None,
        execute_requested: false,
        live_order_confirmed: None,
        mutation_confirmed: Some(false),
        planned_side_effects: vec![],
        safety_checks: vec![
            "tools/ops renders a websocket opportunity probe plan only".to_string(),
            "no websocket connections are opened by this plan command".to_string(),
            "no private adapters or order paths are bootstrapped".to_string(),
            "legacy probe remains observe-only and must not place orders".to_string(),
        ],
        output_fields_preserved: vec![
            "ws_connected",
            "ws_error",
            "ws_book",
            "opportunity",
            "progress",
            "exchange",
            "symbol",
            "raw_spread",
            "net_edge",
        ],
        request: json!({
            "config": args.config,
            "max_symbols": args.max_symbols,
            "seconds": args.seconds,
            "min_raw_spread": args.min_raw_spread,
            "max_raw_spread": args.max_raw_spread,
            "min_net_edge": args.min_net_edge,
            "ws_batch_size": args.ws_batch_size,
            "all_updates": args.all_updates,
            "network_access": "disabled",
            "live_order_access": "disabled",
        }),
    })
}

fn load_cross_arb_config_safety_view(path: &PathBuf) -> Result<CrossArbConfigSafetyView> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_yaml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))
}

fn validate_live_order_gate(execute: bool, confirm_live_order: bool) -> Result<()> {
    if execute && !confirm_live_order {
        bail!("--execute requires --confirm-live-order");
    }
    Ok(())
}

fn cross_arb_trading_enabled(config: &CrossArbConfigSafetyView) -> bool {
    config
        .execution
        .trading_enabled
        .unwrap_or_else(|| config.execution.dry_run.is_some_and(|dry_run| !dry_run))
}

fn validate_cross_arb_trading_gate(execute: bool, trading_enabled: bool) -> Result<()> {
    if execute && !trading_enabled {
        bail!("config execution.trading_enabled=false; set it true only for an intentional canary order");
    }
    if !execute && trading_enabled {
        bail!("config execution.trading_enabled=true requires --execute --confirm-live-order");
    }
    Ok(())
}

fn validate_perp_canary_notional(
    notional_usdt: f64,
    max_estimated_notional_usdt: f64,
) -> Result<()> {
    if !notional_usdt.is_finite() || notional_usdt <= 0.0 || notional_usdt > 10.0 {
        bail!("canary notional must be > 0 and <= 10 USDT");
    }
    if !max_estimated_notional_usdt.is_finite()
        || max_estimated_notional_usdt <= 0.0
        || max_estimated_notional_usdt > 20.0
    {
        bail!("max estimated canary notional must be > 0 and <= 20 USDT");
    }
    Ok(())
}

fn validate_usdt_canonical_symbol(symbol: &str, message: &str) -> Result<()> {
    let symbol = CanonicalSymbol::parse(symbol)
        .map_err(|_| anyhow!("invalid --symbol {symbol}; expected BASE/USDT"))?;
    if symbol.quote_asset() != "USDT" {
        bail!("{message}");
    }
    Ok(())
}

fn canonical_symbol_string(symbol: &str) -> Result<String> {
    CanonicalSymbol::parse(symbol)
        .map(|symbol| symbol.as_str().to_string())
        .map_err(|_| anyhow!("invalid canonical symbol {symbol:?}"))
}

fn validate_enabled_exchange(exchange: &str, enabled_exchanges: &[String]) -> Result<()> {
    let exchange = normalize_exchange(exchange);
    if !enabled_exchanges
        .iter()
        .any(|enabled| normalize_exchange(enabled) == exchange)
    {
        bail!("{exchange} is not enabled in config universe");
    }
    Ok(())
}

fn validate_supported_perp_canary_exchange(exchange: &str) -> Result<()> {
    match normalize_exchange(exchange).as_str() {
        "binance" | "bitget" | "gate" | "aster" | "mexc" | "kucoinfutures" | "bybit" => Ok(()),
        other => bail!(
            "canary currently supports binance, bitget, gate, aster, mexc, kucoinfutures, and bybit; got {other}"
        ),
    }
}

fn validate_supported_audit_exchange(exchange: &str) -> Result<()> {
    match normalize_exchange(exchange).as_str() {
        "binance" | "bitget" | "gate" | "bybit" | "mexc" | "htx" | "okx" => Ok(()),
        other => bail!("unsupported exchange {other}"),
    }
}

fn validate_supported_admin_exchange(exchange: &str) -> Result<()> {
    validate_supported_audit_exchange(exchange)
}

fn validate_rebate(label: &str, rebate: f64) -> Result<()> {
    if !rebate.is_finite() || !(0.0..=1.0).contains(&rebate) {
        bail!("{label} rebate must be between 0 and 1");
    }
    Ok(())
}

fn normalize_exchange(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gateio" => "gate".to_string(),
        "huobi" => "htx".to_string(),
        other => other.to_string(),
    }
}

fn render_plan(plan: SafetyPlan) -> Result<String> {
    serde_json::to_string_pretty(&plan).context("serialize safety plan")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exchange_canary_plan_preserves_trading_gate() {
        let config_path = std::env::temp_dir().join(format!(
            "rustcta-tools-ops-canary-test-{}.yml",
            std::process::id()
        ));
        std::fs::write(
            &config_path,
            r#"
execution:
  trading_enabled: false
  dry_run: true
universe:
  enabled_exchanges:
    - bitget
"#,
        )
        .expect("write canary test config");
        let report = exchange_order_canary_safety_plan(ExchangeOrderCanarySafetyArgs {
            config: config_path.clone(),
            exchange: "bitget".to_string(),
            symbol: "DOGE/USDT".to_string(),
            side: CanarySide::Long,
            mode: CanaryMode::MarketRoundTrip,
            notional_usdt: 10.0,
            max_estimated_notional_usdt: 12.0,
            max_slippage_pct: 0.001,
            execute: false,
            confirm_live_order: false,
        })
        .expect("dry-run plan");
        let value: serde_json::Value = serde_json::from_str(&report).expect("json");
        assert_eq!(value["legacy_binary"], "exchange_order_canary");
        assert_eq!(value["config_trading_enabled"], false);
        assert_eq!(value["request"]["exchange"], "bitget");
        assert!(value["planned_side_effects"].as_array().unwrap().is_empty());
        let _ = std::fs::remove_file(config_path);
    }

    #[test]
    fn exchange_canary_safety_should_accept_target_contract_venues() {
        for exchange in ["aster", "mexc", "kucoinfutures", "bybit"] {
            validate_supported_perp_canary_exchange(exchange).expect(exchange);
        }
        assert!(validate_supported_perp_canary_exchange("kucoin").is_err());
    }

    #[test]
    fn canary_execute_requires_confirmation_before_config_checks() {
        let err = bitget_perp_order_canary_safety_plan(BitgetPerpOrderCanarySafetyArgs {
            config: PathBuf::from("missing.yml"),
            symbol: "BTC/USDT".to_string(),
            side: CanarySide::Long,
            notional_usdt: 10.0,
            max_estimated_notional_usdt: 11.0,
            max_slippage_pct: 0.001,
            execute: true,
            confirm_live_order: false,
        })
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("--execute requires --confirm-live-order"));
    }

    #[test]
    fn admin_cancel_requires_execute_and_cancel_confirmation() {
        let err = cross_arb_order_admin_safety_plan(CrossArbOrderAdminSafetyArgs {
            config: PathBuf::from("missing.yml"),
            action: AdminAction::Cancel,
            exchange: "binance".to_string(),
            symbol: Some("BTC/USDT".to_string()),
            exchange_order_id: Some("123".to_string()),
            client_order_id: None,
            execute: false,
            confirm_cancel: false,
            confirm_order: false,
            quantity: None,
            position_side: None,
        })
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("--action cancel requires --execute --confirm-cancel"));
    }

    #[test]
    fn fee_audit_rejects_invalid_symbol_and_rebate() {
        let err = cross_arb_fee_audit_safety_plan(CrossArbFeeAuditSafetyArgs {
            config: PathBuf::from("missing.yml"),
            symbols: vec!["BTCUSDT".to_string()],
            exchanges: vec![],
            limit: 100,
            since_hours: 24,
            binance_rebate: 0.20,
            other_rebate: 0.60,
        })
        .unwrap_err();
        assert!(err.to_string().contains("invalid canonical symbol"));

        let err = cross_arb_fee_audit_safety_plan(CrossArbFeeAuditSafetyArgs {
            config: PathBuf::from("missing.yml"),
            symbols: vec!["BTC/USDT".to_string()],
            exchanges: vec![],
            limit: 100,
            since_hours: 24,
            binance_rebate: 1.20,
            other_rebate: 0.60,
        })
        .unwrap_err();
        assert!(err.to_string().contains("binance rebate"));
    }
}

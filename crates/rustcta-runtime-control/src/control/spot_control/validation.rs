use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::{
    normalize_exchange, normalize_exchange_list, DisableSymbolRequest, EnableMode,
    EnableSymbolRequest, ExchangeSymbolInventory, InventoryReadiness, ValidationError,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnableValidationSnapshot {
    #[serde(default)]
    pub adapter_exchanges: BTreeSet<String>,
    #[serde(default)]
    pub spot_supported_exchanges: BTreeSet<String>,
    #[serde(default)]
    pub exchange_symbols: BTreeSet<String>,
    #[serde(default)]
    pub tradable_symbols: BTreeSet<String>,
    #[serde(default)]
    pub symbol_mappings: BTreeSet<String>,
    #[serde(default)]
    pub symbol_rules: BTreeSet<String>,
    #[serde(default)]
    pub fresh_books: BTreeSet<String>,
    #[serde(default)]
    pub fee_model_available: BTreeSet<String>,
    #[serde(default)]
    pub balance_available: BTreeSet<String>,
    #[serde(default)]
    pub disabled_registry_blocks: BTreeSet<String>,
    #[serde(default)]
    pub unmanaged_conflicts: BTreeSet<String>,
    #[serde(default)]
    pub kill_switch_allows_paper: bool,
    #[serde(default)]
    pub kill_switch_allows_live_dry_run: bool,
    #[serde(default)]
    pub kill_switch_allows_live_orders: bool,
    #[serde(default)]
    pub live_preflight_allows_live_dry_run: bool,
    #[serde(default)]
    pub live_preflight_allows_live_orders: bool,
    #[serde(default)]
    pub small_live_gate_allows: bool,
    #[serde(default)]
    pub inventory: Vec<ExchangeSymbolInventory>,
}

impl EnableValidationSnapshot {
    pub fn key(exchange: &str, symbol: &str) -> String {
        format!(
            "{}:{}",
            normalize_exchange(exchange),
            super::normalize_symbol(symbol)
        )
    }

    pub fn mark_exchange_ready(mut self, exchange: &str, symbol: &str) -> Self {
        let exchange = normalize_exchange(exchange);
        let key = Self::key(&exchange, symbol);
        self.adapter_exchanges.insert(exchange.clone());
        self.spot_supported_exchanges.insert(exchange);
        self.exchange_symbols.insert(key.clone());
        self.tradable_symbols.insert(key.clone());
        self.symbol_mappings.insert(key.clone());
        self.symbol_rules.insert(key.clone());
        self.fresh_books.insert(key.clone());
        self.fee_model_available.insert(key.clone());
        self.balance_available.insert(key);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnableValidationReport {
    pub errors: Vec<ValidationError>,
    pub inventory: Vec<ExchangeSymbolInventory>,
    pub inventory_readiness: InventoryReadiness,
    pub permitted_directions: Vec<super::EnabledDirection>,
}

impl EnableValidationReport {
    pub fn is_critical_failure(&self) -> bool {
        self.errors.iter().any(|error| error.critical)
    }
}

pub fn validate_enable_request(
    request: &EnableSymbolRequest,
    snapshot: &EnableValidationSnapshot,
    allow_future_small_live: bool,
) -> EnableValidationReport {
    let symbol = super::normalize_symbol(&request.symbol);
    let exchanges = normalize_exchange_list(&request.selected_exchanges);
    let mut errors = Vec::new();

    if exchanges.is_empty() {
        errors.push(ValidationError::critical(
            "selected_exchanges_required",
            "at least one explicitly selected exchange is required",
        ));
    }
    if request
        .selected_exchanges
        .iter()
        .any(|exchange| matches!(exchange.trim(), "*" | "all" | "ALL"))
    {
        errors.push(ValidationError::critical(
            "wildcard_exchange_forbidden",
            "wildcard or all-exchange enablement is forbidden",
        ));
    }
    if request.allowed_directions.is_empty() && matches!(request.mode, EnableMode::FutureSmallLive)
    {
        errors.push(ValidationError::critical(
            "explicit_directions_required",
            "FutureSmallLive requires explicit enabled directions",
        ));
    }

    for exchange in &exchanges {
        let key = EnableValidationSnapshot::key(exchange, &symbol);
        require(
            &mut errors,
            snapshot.adapter_exchanges.contains(exchange),
            "adapter_missing",
            format!("exchange adapter is missing for {exchange}"),
        );
        require(
            &mut errors,
            snapshot.spot_supported_exchanges.contains(exchange),
            "spot_market_unsupported",
            format!("spot market is not supported on {exchange}"),
        );
        require(
            &mut errors,
            snapshot.exchange_symbols.contains(&key),
            "exchange_symbol_missing",
            format!("{symbol} is missing on {exchange}"),
        );
        require(
            &mut errors,
            snapshot.tradable_symbols.contains(&key),
            "symbol_not_tradable",
            format!("{symbol} is not tradable on {exchange}"),
        );
        require(
            &mut errors,
            snapshot.symbol_mappings.contains(&key),
            "symbol_mapping_missing",
            format!("symbol mapping is missing for {exchange}:{symbol}"),
        );
        require(
            &mut errors,
            snapshot.symbol_rules.contains(&key),
            "symbol_rules_missing",
            format!("tick size, step size, min quantity, or min notional is missing for {exchange}:{symbol}"),
        );
        require(
            &mut errors,
            snapshot.fresh_books.contains(&key),
            "book_not_fresh",
            format!("public WebSocket book is not fresh for {exchange}:{symbol}"),
        );
        require(
            &mut errors,
            snapshot.fee_model_available.contains(&key),
            "fee_model_missing",
            format!("FeeModel has no effective fee or fallback for {exchange}:{symbol}"),
        );
        if !snapshot.balance_available.contains(&key) {
            errors.push(ValidationError::warning(
                "balance_snapshot_missing",
                format!("balance snapshot is unavailable for {exchange}:{symbol}"),
            ));
        }
        require(
            &mut errors,
            !snapshot.disabled_registry_blocks.contains(&key),
            "disabled_registry_blocks_symbol",
            format!("DisabledRegistry blocks {exchange}:{symbol}"),
        );
        require(
            &mut errors,
            !snapshot.unmanaged_conflicts.contains(&key),
            "unmanaged_inventory_conflict",
            format!("critical unmanaged-position conflict exists on {exchange}:{symbol}"),
        );
    }

    validate_mode_gates(request.mode, snapshot, allow_future_small_live, &mut errors);

    let permitted_directions = request
        .allowed_directions
        .iter()
        .cloned()
        .map(super::EnabledDirection::normalized)
        .filter(|direction| {
            exchanges.contains(&direction.buy_exchange)
                && exchanges.contains(&direction.sell_exchange)
        })
        .collect::<Vec<_>>();

    let inventory_readiness =
        aggregate_inventory_readiness(&snapshot.inventory, request.mode, &permitted_directions);
    if matches!(request.mode, EnableMode::FutureSmallLive)
        && matches!(
            inventory_readiness,
            InventoryReadiness::InsufficientBase
                | InventoryReadiness::InsufficientQuote
                | InventoryReadiness::UnmanagedConflict
                | InventoryReadiness::Unknown
        )
    {
        errors.push(ValidationError::critical(
            "inventory_not_ready",
            "FutureSmallLive requires sufficient real inventory for every enabled direction",
        ));
    }

    EnableValidationReport {
        errors,
        inventory: snapshot.inventory.clone(),
        inventory_readiness,
        permitted_directions,
    }
}

pub fn validate_disable_request(request: &DisableSymbolRequest) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    let exchanges = normalize_exchange_list(&request.selected_exchanges);
    if exchanges.is_empty() {
        errors.push(ValidationError::critical(
            "selected_exchanges_required",
            "disable requires explicitly selected exchanges",
        ));
    }
    if !request.include_managed_inventory_only {
        errors.push(ValidationError::critical(
            "managed_inventory_only_required",
            "disable workflows may only liquidate managed inventory",
        ));
    }
    if request.maximum_slippage_bps.unwrap_or(0.0) < 0.0 {
        errors.push(ValidationError::critical(
            "invalid_slippage",
            "maximum slippage cannot be negative",
        ));
    }
    if request.maximum_liquidation_loss_usdt.unwrap_or(0.0) < 0.0 {
        errors.push(ValidationError::critical(
            "invalid_loss_limit",
            "maximum liquidation loss cannot be negative",
        ));
    }
    errors
}

fn require(
    errors: &mut Vec<ValidationError>,
    condition: bool,
    code: &'static str,
    message: String,
) {
    if !condition {
        errors.push(ValidationError::critical(code, message));
    }
}

fn validate_mode_gates(
    mode: EnableMode,
    snapshot: &EnableValidationSnapshot,
    allow_future_small_live: bool,
    errors: &mut Vec<ValidationError>,
) {
    match mode {
        EnableMode::ObserveOnly | EnableMode::Paper => {
            if !snapshot.kill_switch_allows_paper {
                errors.push(ValidationError::critical(
                    "kill_switch_blocks_mode",
                    "KillSwitch blocks paper/observe enablement",
                ));
            }
        }
        EnableMode::LiveDryRun => {
            if !snapshot.kill_switch_allows_live_dry_run {
                errors.push(ValidationError::critical(
                    "kill_switch_blocks_mode",
                    "KillSwitch blocks live_dry_run enablement",
                ));
            }
            if !snapshot.live_preflight_allows_live_dry_run {
                errors.push(ValidationError::critical(
                    "live_preflight_blocks_mode",
                    "LivePreflight blocks live_dry_run enablement",
                ));
            }
        }
        EnableMode::FutureSmallLive => {
            if !allow_future_small_live {
                errors.push(ValidationError::critical(
                    "future_small_live_disabled",
                    "FutureSmallLive is disabled by configuration",
                ));
            }
            if !snapshot.kill_switch_allows_live_orders {
                errors.push(ValidationError::critical(
                    "kill_switch_blocks_live_orders",
                    "KillSwitch blocks live orders",
                ));
            }
            if !snapshot.live_preflight_allows_live_orders {
                errors.push(ValidationError::critical(
                    "live_preflight_blocks_live_orders",
                    "LivePreflight blocks live orders",
                ));
            }
            if !snapshot.small_live_gate_allows {
                errors.push(ValidationError::critical(
                    "small_live_gate_required",
                    "SmallLiveGate is required for FutureSmallLive",
                ));
            }
        }
    }
}

fn aggregate_inventory_readiness(
    inventory: &[ExchangeSymbolInventory],
    mode: EnableMode,
    directions: &[super::EnabledDirection],
) -> InventoryReadiness {
    if matches!(
        mode,
        EnableMode::ObserveOnly | EnableMode::Paper | EnableMode::LiveDryRun
    ) && inventory.is_empty()
    {
        return InventoryReadiness::Unknown;
    }
    if inventory
        .iter()
        .any(|item| item.inventory_status == InventoryReadiness::UnmanagedConflict)
    {
        return InventoryReadiness::UnmanagedConflict;
    }
    if directions.is_empty() {
        return InventoryReadiness::Unknown;
    }
    let mut quote_ok = true;
    let mut base_ok = true;
    for direction in directions {
        if let Some(buy_inventory) = inventory
            .iter()
            .find(|item| item.exchange == direction.buy_exchange)
        {
            quote_ok &= buy_inventory.quote_available > 0.0;
        } else {
            quote_ok = false;
        }
        if let Some(sell_inventory) = inventory
            .iter()
            .find(|item| item.exchange == direction.sell_exchange)
        {
            base_ok &=
                sell_inventory.managed_sellable_base() >= sell_inventory.minimum_base_inventory;
        } else {
            base_ok = false;
        }
    }
    match (quote_ok, base_ok) {
        (true, true) => InventoryReadiness::ReadyBothDirections,
        (true, false) => InventoryReadiness::InsufficientBase,
        (false, true) => InventoryReadiness::InsufficientQuote,
        (false, false) => InventoryReadiness::Unknown,
    }
}

use std::collections::HashMap;

use crate::exchanges::client_order_id::{generate_client_order_id, validate_client_order_id};
use crate::exchanges::symbol_registry::validate_order_with_rule;
use crate::exchanges::unified::{
    MarketType, OrderRequest, OrderSide, OrderType, PositionSide, SymbolStatus, TimeInForce,
};
use crate::execution::FeeSource;

use super::{
    build_report, ApiPermissionState, LivePreflightCheck, LivePreflightCheckStatus as Status,
    LivePreflightConfig, LivePreflightGate as Gate, LivePreflightReport,
    LivePreflightSeverity as Sev, LiveReadinessState,
};

pub fn run_live_preflight(
    config: LivePreflightConfig,
    state: &LiveReadinessState,
) -> LivePreflightReport {
    let mut checks = Vec::new();
    checks.extend(check_config_safety(&config, state));
    checks.extend(check_api_credentials(&config, state));
    checks.extend(check_exchange_connectivity(&config, state));
    checks.extend(check_symbol_rules(&config, state));
    checks.extend(check_balances(&config, state));
    checks.extend(check_fee_model(&config, state));
    checks.extend(check_websocket_books(&config, state));
    checks.extend(check_disabled_unmanaged(&config, state));
    checks.extend(check_risk_limits(&config, state));
    checks.extend(check_recorder_monitoring(&config, state));
    build_report(config, checks)
}

fn pass(gate: Gate, name: &str, message: impl Into<String>) -> LivePreflightCheck {
    LivePreflightCheck::new(gate, name, Status::Pass, Sev::Info, message)
}

fn warn(gate: Gate, name: &str, message: impl Into<String>) -> LivePreflightCheck {
    LivePreflightCheck::new(gate, name, Status::Warn, Sev::Warning, message)
}

fn fail(gate: Gate, name: &str, message: impl Into<String>) -> LivePreflightCheck {
    LivePreflightCheck::new(gate, name, Status::Fail, Sev::Critical, message)
}

fn skipped(gate: Gate, name: &str, message: impl Into<String>) -> LivePreflightCheck {
    LivePreflightCheck::new(gate, name, Status::Skipped, Sev::Info, message)
}

fn unknown(gate: Gate, name: &str, message: impl Into<String>) -> LivePreflightCheck {
    LivePreflightCheck::new(gate, name, Status::Unknown, Sev::Warning, message)
}

fn check_config_safety(
    config: &LivePreflightConfig,
    state: &LiveReadinessState,
) -> Vec<LivePreflightCheck> {
    let mut checks = Vec::new();
    let mode = state.trading_mode.trim().to_ascii_lowercase();
    if mode == "paper" || mode == "live_dry_run" {
        checks.push(pass(
            Gate::ConfigSafety,
            "trading_mode_safe",
            format!("trading_mode={}", state.trading_mode),
        ));
    } else {
        checks.push(fail(
            Gate::ConfigSafety,
            "trading_mode_safe",
            "trading_mode must be paper or live_dry_run for preflight stage",
        ));
    }
    checks.push(if state.live_trading_enabled {
        fail(
            Gate::ConfigSafety,
            "live_trading_disabled",
            "live_trading_enabled must remain false in this stage",
        )
    } else {
        pass(
            Gate::ConfigSafety,
            "live_trading_disabled",
            "live_trading_enabled=false",
        )
    });
    checks.push(if state.dry_run {
        pass(Gate::ConfigSafety, "dry_run_enabled", "dry_run=true")
    } else {
        fail(
            Gate::ConfigSafety,
            "dry_run_enabled",
            "dry_run must be true for preflight stage",
        )
    });
    checks.push(match config.max_live_notional_per_trade {
        Some(value) if value > 0.0 && value <= 10.0 => pass(
            Gate::ConfigSafety,
            "small_trade_notional",
            format!("max_live_notional_per_trade={value}"),
        ),
        Some(value) if value > 10.0 => fail(
            Gate::ConfigSafety,
            "small_trade_notional",
            format!("max_live_notional_per_trade={value} exceeds small-capital limit"),
        ),
        _ => fail(
            Gate::ConfigSafety,
            "small_trade_notional",
            "max_live_notional_per_trade must be configured",
        ),
    });
    checks.push(match config.max_total_live_notional {
        Some(value) if value > 0.0 && value <= 50.0 => pass(
            Gate::ConfigSafety,
            "total_live_notional",
            format!("max_total_live_notional={value}"),
        ),
        Some(value) if value > 50.0 => warn(
            Gate::ConfigSafety,
            "total_live_notional",
            format!("max_total_live_notional={value} is above recommended small-capital range"),
        ),
        _ => fail(
            Gate::ConfigSafety,
            "total_live_notional",
            "max_total_live_notional must be configured",
        ),
    });
    checks.push(if config.exchanges.is_empty() {
        fail(
            Gate::ConfigSafety,
            "explicit_exchanges",
            "enabled exchanges must be explicit",
        )
    } else {
        pass(
            Gate::ConfigSafety,
            "explicit_exchanges",
            format!("exchanges={}", config.exchanges.join(",")),
        )
    });
    checks.push(
        if config.symbols.is_empty()
            || config
                .symbols
                .iter()
                .any(|symbol| symbol.trim() == "*" || symbol.eq_ignore_ascii_case("all"))
        {
            fail(
                Gate::ConfigSafety,
                "explicit_symbols",
                "enabled symbols must be explicit; wildcard live mode is blocked",
            )
        } else {
            pass(
                Gate::ConfigSafety,
                "explicit_symbols",
                format!("symbols={}", config.symbols.join(",")),
            )
        },
    );
    checks
}

fn check_api_credentials(
    config: &LivePreflightConfig,
    state: &LiveReadinessState,
) -> Vec<LivePreflightCheck> {
    let mut checks = Vec::new();
    for exchange in &config.exchanges {
        let normalized = normalize_exchange(exchange);
        let permissions = state
            .api_permissions
            .get(&normalized)
            .cloned()
            .unwrap_or_default();
        if config.require_api_key_read_permission || config.require_api_key_trade_permission {
            checks.push(
                if permissions.key_present && permissions.secret_present {
                    pass(
                        Gate::ApiCredentials,
                        "api_credentials_present",
                        "API key and secret are configured",
                    )
                } else {
                    fail(
                        Gate::ApiCredentials,
                        "api_credentials_present",
                        "API key and secret are required for read-only private preflight",
                    )
                }
                .scoped(Some(&normalized), Some(config.market_type), None),
            );
        }
        checks.push(permission_check(
            &normalized,
            config.market_type,
            "api_read_permission",
            config.require_api_key_read_permission,
            permissions.read_permission,
            "read permission",
        ));
        checks.push(permission_check(
            &normalized,
            config.market_type,
            "api_trade_permission",
            config.require_api_key_trade_permission,
            permissions.trade_permission,
            "trade permission",
        ));
        if config.require_withdraw_permission_absent {
            checks.push(match permissions.withdraw_permission {
                Some(false) => pass(
                    Gate::ApiCredentials,
                    "withdraw_permission_absent",
                    "withdraw permission is absent",
                )
                .scoped(Some(&normalized), Some(config.market_type), None),
                Some(true) => fail(
                    Gate::ApiCredentials,
                    "withdraw_permission_absent",
                    "withdraw permission appears enabled",
                )
                .scoped(Some(&normalized), Some(config.market_type), None),
                None => warn(
                    Gate::ApiCredentials,
                    "withdraw_permission_absent",
                    "withdraw permission introspection is not available",
                )
                .scoped(Some(&normalized), Some(config.market_type), None),
            });
        }
        checks.push(match permissions.account_read_probe_ok {
            Some(true) => pass(
                Gate::ApiCredentials,
                "account_read_probe",
                "read-only account probe succeeded",
            )
            .scoped(Some(&normalized), Some(config.market_type), None),
            Some(false) => fail(
                Gate::ApiCredentials,
                "account_read_probe",
                "read-only account probe failed",
            )
            .scoped(Some(&normalized), Some(config.market_type), None),
            None => warn(
                Gate::ApiCredentials,
                "account_read_probe",
                "read-only account probe was not run",
            )
            .scoped(Some(&normalized), Some(config.market_type), None),
        });
    }
    checks
}

fn permission_check(
    exchange: &str,
    market_type: MarketType,
    name: &str,
    required: bool,
    observed: Option<bool>,
    label: &str,
) -> LivePreflightCheck {
    if !required {
        return skipped(
            Gate::ApiCredentials,
            name,
            format!("{label} is not required"),
        )
        .scoped(Some(exchange), Some(market_type), None);
    }
    match observed {
        Some(true) => pass(Gate::ApiCredentials, name, format!("{label} present")).scoped(
            Some(exchange),
            Some(market_type),
            None,
        ),
        Some(false) => fail(Gate::ApiCredentials, name, format!("{label} missing")).scoped(
            Some(exchange),
            Some(market_type),
            None,
        ),
        None => unknown(
            Gate::ApiCredentials,
            name,
            format!("{label} introspection is not available"),
        )
        .scoped(Some(exchange), Some(market_type), None),
    }
}

fn check_exchange_connectivity(
    config: &LivePreflightConfig,
    state: &LiveReadinessState,
) -> Vec<LivePreflightCheck> {
    config
        .exchanges
        .iter()
        .map(|exchange| {
            let exchange = normalize_exchange(exchange);
            let health = state
                .exchanges
                .iter()
                .find(|item| item.exchange == exchange);
            match health {
                Some(health) if health.connected || health.public_ws_connected => pass(
                    Gate::ExchangeConnectivity,
                    "exchange_connectivity",
                    "exchange health is connected",
                )
                .detail("reconnect_count", health.reconnect_count)
                .detail("parse_error_count", health.parse_error_count)
                .scoped(Some(&exchange), Some(config.market_type), None),
                Some(_) => warn(
                    Gate::ExchangeConnectivity,
                    "exchange_connectivity",
                    "exchange health exists but is not connected",
                )
                .scoped(Some(&exchange), Some(config.market_type), None),
                None => warn(
                    Gate::ExchangeConnectivity,
                    "exchange_connectivity",
                    "exchange connectivity was not probed",
                )
                .scoped(Some(&exchange), Some(config.market_type), None),
            }
        })
        .collect()
}

fn check_symbol_rules(
    config: &LivePreflightConfig,
    state: &LiveReadinessState,
) -> Vec<LivePreflightCheck> {
    if !config.require_symbol_rules {
        return vec![skipped(
            Gate::SymbolRules,
            "symbol_rules_required",
            "symbol rule checks disabled",
        )];
    }
    let mut checks = Vec::new();
    for exchange in &config.exchanges {
        let exchange = normalize_exchange(exchange);
        for symbol in &config.symbols {
            let symbol = normalize_symbol(symbol);
            let Some(rule) = find_rule(state, &exchange, config.market_type, &symbol) else {
                checks.push(
                    fail(
                        Gate::SymbolRules,
                        "symbol_rule_available",
                        "symbol rule is missing",
                    )
                    .scoped(
                        Some(&exchange),
                        Some(config.market_type),
                        Some(&symbol),
                    ),
                );
                continue;
            };
            checks.push(
                if rule.status == SymbolStatus::Trading {
                    pass(Gate::SymbolRules, "symbol_tradable", "symbol is trading")
                } else {
                    fail(
                        Gate::SymbolRules,
                        "symbol_tradable",
                        "symbol is not tradable",
                    )
                }
                .scoped(Some(&exchange), Some(config.market_type), Some(&symbol)),
            );
            checks.push(
                if rule.tick_size > 0.0
                    && rule.step_size > 0.0
                    && rule.min_quantity >= 0.0
                    && rule.min_notional >= 0.0
                {
                    pass(
                        Gate::SymbolRules,
                        "symbol_precision_rules",
                        "tick, step, min quantity, and min notional are present",
                    )
                } else {
                    fail(
                        Gate::SymbolRules,
                        "symbol_precision_rules",
                        "symbol precision/minimum rules are incomplete",
                    )
                }
                .scoped(Some(&exchange), Some(config.market_type), Some(&symbol)),
            );
            checks.push(
                if rule.supported_order_types.contains(&OrderType::Market)
                    || rule.supported_order_types.contains(&OrderType::Limit)
                {
                    pass(
                        Gate::SymbolRules,
                        "taker_order_type_supported",
                        "market or limit order is supported",
                    )
                } else {
                    fail(
                        Gate::SymbolRules,
                        "taker_order_type_supported",
                        "future takerTaker path needs market or limit order support",
                    )
                }
                .scoped(Some(&exchange), Some(config.market_type), Some(&symbol)),
            );
            checks.push(
                if rule.supported_time_in_force.contains(&TimeInForce::IOC)
                    || !rule.supported_time_in_force.is_empty()
                {
                    pass(
                        Gate::SymbolRules,
                        "time_in_force_available",
                        "time-in-force support is declared",
                    )
                } else {
                    warn(
                        Gate::SymbolRules,
                        "time_in_force_available",
                        "time-in-force support is unknown",
                    )
                }
                .scoped(Some(&exchange), Some(config.market_type), Some(&symbol)),
            );
            let id = generate_client_order_id(&exchange, config.market_type, "PREFLIGHT");
            checks.push(
                match validate_client_order_id(&exchange, config.market_type, id.as_str()) {
                    Ok(()) => pass(
                        Gate::ClientOrderId,
                        "client_order_id_policy",
                        "generated client_order_id validates",
                    )
                    .scoped(
                        Some(&exchange),
                        Some(config.market_type),
                        Some(&symbol),
                    ),
                    Err(error) => fail(
                        Gate::ClientOrderId,
                        "client_order_id_policy",
                        format!("generated client_order_id failed validation: {error}"),
                    )
                    .scoped(
                        Some(&exchange),
                        Some(config.market_type),
                        Some(&symbol),
                    ),
                },
            );
            if config.require_order_validation {
                checks.push(order_validation_check(
                    &exchange,
                    config.market_type,
                    &symbol,
                    rule,
                ));
            }
        }
    }
    checks
}

fn order_validation_check(
    exchange: &str,
    market_type: MarketType,
    symbol: &str,
    rule: &crate::exchanges::unified::SymbolRule,
) -> LivePreflightCheck {
    let price = if rule.tick_size > 0.0 {
        (rule.min_notional / rule.min_quantity.max(rule.step_size.max(1e-12))).max(rule.tick_size)
    } else {
        1.0
    };
    let quantity = rule.min_quantity.max(rule.step_size).max(1e-12);
    let order = OrderRequest {
        market_type,
        symbol: symbol.to_string(),
        side: OrderSide::Buy,
        position_side: PositionSide::None,
        order_type: if rule.supported_order_types.contains(&OrderType::Market) {
            OrderType::Market
        } else {
            OrderType::Limit
        },
        time_in_force: None,
        quantity,
        price: Some(price),
        client_order_id: Some(
            generate_client_order_id(exchange, market_type, "PREFLIGHT").into_string(),
        ),
        reduce_only: false,
    };
    match validate_order_with_rule(&order, rule, exchange) {
        Ok(()) => pass(
            Gate::OrderValidation,
            "order_validation_readiness",
            "sample order validates locally",
        )
        .scoped(Some(exchange), Some(market_type), Some(symbol)),
        Err(error) => fail(
            Gate::OrderValidation,
            "order_validation_readiness",
            format!("sample order validation failed: {error}"),
        )
        .scoped(Some(exchange), Some(market_type), Some(symbol)),
    }
}

fn check_balances(
    config: &LivePreflightConfig,
    state: &LiveReadinessState,
) -> Vec<LivePreflightCheck> {
    if !config.require_balances {
        return vec![skipped(
            Gate::Balances,
            "balances_required",
            "balance checks disabled",
        )];
    }
    let mut checks = Vec::new();
    for exchange in &config.exchanges {
        let exchange = normalize_exchange(exchange);
        let quote = state.inventory.iter().find(|item| {
            item.exchange == exchange
                && item.market_type == config.market_type
                && item.asset == "USDT"
        });
        checks.push(match quote {
            Some(balance) if balance.effective_available >= config.minimum_quote_balance_usdt => {
                pass(
                    Gate::Balances,
                    "quote_balance_available",
                    format!("USDT effective_available={}", balance.effective_available),
                )
                .scoped(Some(&exchange), Some(config.market_type), None)
            }
            Some(balance) => fail(
                Gate::Balances,
                "quote_balance_available",
                format!(
                    "USDT effective_available={} below minimum {}",
                    balance.effective_available, config.minimum_quote_balance_usdt
                ),
            )
            .scoped(Some(&exchange), Some(config.market_type), None),
            None => fail(
                Gate::Balances,
                "quote_balance_available",
                "USDT balance snapshot is missing",
            )
            .scoped(Some(&exchange), Some(config.market_type), None),
        });
    }
    checks
}

fn check_fee_model(
    config: &LivePreflightConfig,
    state: &LiveReadinessState,
) -> Vec<LivePreflightCheck> {
    if !config.require_fee_model {
        return vec![skipped(
            Gate::FeeModel,
            "fee_model_required",
            "fee checks disabled",
        )];
    }
    let mut checks = Vec::new();
    for exchange in &config.exchanges {
        let exchange = normalize_exchange(exchange);
        for symbol in &config.symbols {
            let symbol = normalize_symbol(symbol);
            let fee = state.fees.iter().find(|item| {
                item.exchange == exchange
                    && item.market_type == config.market_type
                    && (item
                        .symbol
                        .as_ref()
                        .is_none_or(|fee_symbol| normalize_symbol(fee_symbol) == symbol))
            });
            checks.push(match fee {
                Some(fee) if fee.maker_fee_bps >= 0.0 && fee.taker_fee_bps >= 0.0 => {
                    let status = if fee.source == FeeSource::Fallback {
                        Status::Warn
                    } else {
                        Status::Pass
                    };
                    LivePreflightCheck::new(
                        Gate::FeeModel,
                        "fee_model_available",
                        status,
                        if status == Status::Pass {
                            Sev::Info
                        } else {
                            Sev::Warning
                        },
                        format!(
                            "maker={}bps taker={}bps source={:?}",
                            fee.maker_fee_bps, fee.taker_fee_bps, fee.source
                        ),
                    )
                    .scoped(
                        Some(&exchange),
                        Some(config.market_type),
                        Some(&symbol),
                    )
                }
                _ => fail(
                    Gate::FeeModel,
                    "fee_model_available",
                    "fee model has no applicable rate",
                )
                .scoped(Some(&exchange), Some(config.market_type), Some(&symbol)),
            });
        }
    }
    checks
}

fn check_websocket_books(
    config: &LivePreflightConfig,
    state: &LiveReadinessState,
) -> Vec<LivePreflightCheck> {
    if !config.require_websocket_fresh {
        return vec![skipped(
            Gate::WebsocketBooks,
            "websocket_fresh_required",
            "fresh book checks disabled",
        )];
    }
    let mut checks = Vec::new();
    for exchange in &config.exchanges {
        let exchange = normalize_exchange(exchange);
        for symbol in &config.symbols {
            let symbol = normalize_symbol(symbol);
            let book = state.books.iter().find(|item| {
                item.exchange == exchange
                    && item.market_type == config.market_type
                    && normalize_symbol(&item.symbol) == symbol
            });
            checks.push(match book {
                Some(book)
                    if !book.is_stale
                        && book.book_age_ms <= config.max_book_age_ms as i64
                        && book.best_bid.is_some()
                        && book.best_ask.is_some()
                        && book.best_bid.unwrap_or(0.0) < book.best_ask.unwrap_or(0.0) =>
                {
                    pass(
                        Gate::WebsocketBooks,
                        "book_fresh",
                        "book is fresh with sane top-of-book",
                    )
                    .detail("book_age_ms", book.book_age_ms)
                    .scoped(
                        Some(&exchange),
                        Some(config.market_type),
                        Some(&symbol),
                    )
                }
                Some(book) => fail(
                    Gate::WebsocketBooks,
                    "book_fresh",
                    format!(
                        "book is stale or invalid: age={}ms stale={}",
                        book.book_age_ms, book.is_stale
                    ),
                )
                .scoped(Some(&exchange), Some(config.market_type), Some(&symbol)),
                None => fail(Gate::WebsocketBooks, "book_fresh", "BookCache has no book").scoped(
                    Some(&exchange),
                    Some(config.market_type),
                    Some(&symbol),
                ),
            });
            if let Some(book) = book {
                checks.push(
                    if book.latency_ms.is_some() {
                        pass(
                            Gate::WebsocketBooks,
                            "book_latency_available",
                            "book latency is available",
                        )
                    } else {
                        warn(
                            Gate::WebsocketBooks,
                            "book_latency_available",
                            "book latency is not available",
                        )
                    }
                    .scoped(
                        Some(&exchange),
                        Some(config.market_type),
                        Some(&symbol),
                    ),
                );
            }
        }
    }
    checks
}

fn check_disabled_unmanaged(
    config: &LivePreflightConfig,
    state: &LiveReadinessState,
) -> Vec<LivePreflightCheck> {
    if !config.require_disabled_registry {
        return vec![skipped(
            Gate::DisabledUnmanaged,
            "disabled_registry_required",
            "disabled registry checks disabled",
        )];
    }
    let mut checks = Vec::new();
    for exchange in &config.exchanges {
        let exchange = normalize_exchange(exchange);
        for symbol in &config.symbols {
            let symbol = normalize_symbol(symbol);
            checks.push(
                if state
                    .disabled_symbols
                    .iter()
                    .any(|item| normalize_symbol(item) == symbol)
                {
                    fail(
                        Gate::DisabledUnmanaged,
                        "symbol_not_disabled",
                        "symbol is globally disabled",
                    )
                } else if state
                    .disabled_exchanges
                    .iter()
                    .any(|item| normalize_exchange(item) == exchange)
                {
                    fail(
                        Gate::DisabledUnmanaged,
                        "exchange_not_disabled",
                        "exchange is disabled",
                    )
                } else if state.disabled_exchange_symbols.iter().any(
                    |(disabled_exchange, market_type, disabled_symbol)| {
                        normalize_exchange(disabled_exchange) == exchange
                            && *market_type == config.market_type
                            && normalize_symbol(disabled_symbol) == symbol
                    },
                ) {
                    fail(
                        Gate::DisabledUnmanaged,
                        "exchange_symbol_not_disabled",
                        "exchange-symbol is disabled",
                    )
                } else {
                    pass(
                        Gate::DisabledUnmanaged,
                        "not_disabled",
                        "symbol and exchange are not disabled",
                    )
                }
                .scoped(Some(&exchange), Some(config.market_type), Some(&symbol)),
            );
            if config.fail_on_unmanaged_position_overlap {
                checks.push(
                    if state.unmanaged_positions.iter().any(
                        |(position_exchange, market_type, position_symbol, _, quantity)| {
                            normalize_exchange(position_exchange) == exchange
                                && *market_type == config.market_type
                                && normalize_symbol(position_symbol) == symbol
                                && *quantity > 0.0
                        },
                    ) {
                        fail(
                            Gate::DisabledUnmanaged,
                            "unmanaged_overlap",
                            "configured symbol overlaps unmanaged position",
                        )
                    } else {
                        pass(
                            Gate::DisabledUnmanaged,
                            "unmanaged_overlap",
                            "no unmanaged position overlap",
                        )
                    }
                    .scoped(
                        Some(&exchange),
                        Some(config.market_type),
                        Some(&symbol),
                    ),
                );
            }
        }
    }
    checks
}

fn check_risk_limits(
    config: &LivePreflightConfig,
    state: &LiveReadinessState,
) -> Vec<LivePreflightCheck> {
    let mut checks = Vec::new();
    checks.push(if config.max_live_notional_per_trade.is_some() {
        pass(
            Gate::RiskLimits,
            "max_trade_notional_configured",
            "max trade notional is configured",
        )
    } else {
        fail(
            Gate::RiskLimits,
            "max_trade_notional_configured",
            "max trade notional is missing",
        )
    });
    checks.push(if config.max_total_live_notional.is_some() {
        pass(
            Gate::RiskLimits,
            "max_total_notional_configured",
            "max total notional is configured",
        )
    } else {
        fail(
            Gate::RiskLimits,
            "max_total_notional_configured",
            "max total notional is missing",
        )
    });
    checks.push(match state.max_daily_loss {
        Some(value) if value > 0.0 => pass(
            Gate::RiskLimits,
            "max_daily_loss_configured",
            format!("max_daily_loss={value}"),
        ),
        _ => warn(
            Gate::RiskLimits,
            "max_daily_loss_configured",
            "max daily loss is not configured",
        ),
    });
    checks.push(if state.emergency_stop_configured {
        pass(
            Gate::RiskLimits,
            "emergency_stop_configured",
            "emergency stop config exists",
        )
    } else {
        warn(
            Gate::RiskLimits,
            "emergency_stop_configured",
            "emergency stop config is not visible",
        )
    });
    checks.push(if state.max_order_latency_ms.is_some() {
        pass(
            Gate::RiskLimits,
            "max_order_latency_configured",
            "max order latency threshold is configured",
        )
    } else {
        warn(
            Gate::RiskLimits,
            "max_order_latency_configured",
            "max order latency threshold is not configured",
        )
    });
    checks.push(if !config.require_kill_switch {
        skipped(
            Gate::KillSwitch,
            "kill_switch_required",
            "kill switch check disabled",
        )
    } else if !state.kill_switch_available {
        fail(
            Gate::KillSwitch,
            "kill_switch_available",
            "kill switch state is not available",
        )
    } else if state.kill_switch_active {
        fail(
            Gate::KillSwitch,
            "kill_switch_inactive",
            "kill switch is active",
        )
    } else {
        pass(
            Gate::KillSwitch,
            "kill_switch_inactive",
            "kill switch exists and is inactive",
        )
    });
    checks
}

fn check_recorder_monitoring(
    config: &LivePreflightConfig,
    state: &LiveReadinessState,
) -> Vec<LivePreflightCheck> {
    let mut checks = Vec::new();
    checks.push(if !config.require_monitoring_enabled {
        skipped(
            Gate::Monitoring,
            "monitoring_enabled",
            "monitoring is not required",
        )
    } else if state.monitoring_enabled {
        pass(
            Gate::Monitoring,
            "monitoring_enabled",
            "monitoring is enabled",
        )
    } else {
        fail(
            Gate::Monitoring,
            "monitoring_enabled",
            "monitoring must be enabled for live preflight",
        )
    });
    checks.push(if !config.require_recorder_enabled {
        skipped(
            Gate::Recorder,
            "recorder_enabled",
            "recorder is not required",
        )
    } else if state.recorder_enabled
        || state.recorder.book_recording_enabled
        || state.recorder.opportunity_recording_enabled
        || state.recorder.trade_recording_enabled
    {
        pass(Gate::Recorder, "recorder_enabled", "recorders are enabled")
    } else {
        fail(
            Gate::Recorder,
            "recorder_enabled",
            "recorders must be enabled",
        )
    });
    checks.push(if state.recorder.dropped_book_events == 0 {
        pass(
            Gate::Recorder,
            "book_recorder_healthy",
            "book recorder has not dropped events",
        )
    } else {
        warn(
            Gate::Recorder,
            "book_recorder_healthy",
            format!(
                "book recorder dropped {} events",
                state.recorder.dropped_book_events
            ),
        )
    });
    checks
}

fn find_rule<'a>(
    state: &'a LiveReadinessState,
    exchange: &str,
    market_type: MarketType,
    symbol: &str,
) -> Option<&'a crate::exchanges::unified::SymbolRule> {
    state.symbol_rules.iter().find(|rule| {
        normalize_exchange(&rule.exchange) == exchange
            && rule.market_type == market_type
            && normalize_symbol(&rule.internal_symbol) == symbol
    })
}

pub fn api_permissions_from_env(exchanges: &[String]) -> HashMap<String, ApiPermissionState> {
    exchanges
        .iter()
        .map(|exchange| {
            let normalized = normalize_exchange(exchange);
            let prefix = normalized.to_ascii_uppercase();
            let key_present = std::env::var(format!("{prefix}_API_KEY")).is_ok()
                || std::env::var(format!("{}_KEY", prefix)).is_ok();
            let secret_present = std::env::var(format!("{prefix}_API_SECRET")).is_ok()
                || std::env::var(format!("{}_SECRET", prefix)).is_ok();
            (
                normalized,
                ApiPermissionState {
                    key_present,
                    secret_present,
                    read_permission: None,
                    trade_permission: None,
                    withdraw_permission: None,
                    account_read_probe_ok: None,
                },
            )
        })
        .collect()
}

fn normalize_exchange(exchange: &str) -> String {
    exchange.trim().to_ascii_lowercase()
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}

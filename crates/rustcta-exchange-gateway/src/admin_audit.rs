use rustcta_exchange_api::{ExchangeClientCapabilities, ExchangeId, SymbolScope};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminAuditCheckKind {
    GetStatus,
    GetCapabilities,
    GetSymbolRules,
    GetOrderBook,
    GetBalances,
    GetPositions,
    GetFees,
    GetOpenOrders,
    GetRecentFills,
}

impl AdminAuditCheckKind {
    pub fn is_mutation(self) -> bool {
        false
    }

    pub fn operation_name(self) -> &'static str {
        match self {
            Self::GetStatus => "get_status",
            Self::GetCapabilities => "get_capabilities",
            Self::GetSymbolRules => "get_symbol_rules",
            Self::GetOrderBook => "get_order_book",
            Self::GetBalances => "get_balances",
            Self::GetPositions => "get_positions",
            Self::GetFees => "get_fees",
            Self::GetOpenOrders => "get_open_orders",
            Self::GetRecentFills => "get_recent_fills",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdminAuditCheck {
    pub kind: AdminAuditCheckKind,
    pub exchange: ExchangeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<SymbolScope>,
    pub requires_private_rest: bool,
}

impl AdminAuditCheck {
    pub fn operation_name(&self) -> &'static str {
        self.kind.operation_name()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminAuditSafety {
    pub mutation_forbidden: bool,
    pub read_only_guard_required: bool,
}

impl Default for AdminAuditSafety {
    fn default() -> Self {
        Self {
            mutation_forbidden: true,
            read_only_guard_required: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdminAuditPlan {
    pub exchange: ExchangeId,
    pub safety: AdminAuditSafety,
    pub checks: Vec<AdminAuditCheck>,
    pub skipped_private_checks: Vec<AdminAuditCheckKind>,
}

impl AdminAuditPlan {
    pub fn contains_mutation(&self) -> bool {
        self.checks.iter().any(|check| check.kind.is_mutation())
    }

    pub fn operation_names(&self) -> Vec<&'static str> {
        self.checks
            .iter()
            .map(AdminAuditCheck::operation_name)
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct AdminAuditPlanner;

impl AdminAuditPlanner {
    pub fn new() -> Self {
        Self
    }

    pub fn plan(
        &self,
        capabilities: &ExchangeClientCapabilities,
        symbols: &[SymbolScope],
    ) -> AdminAuditPlan {
        let mut checks = Vec::new();
        let mut skipped_private_checks = Vec::new();
        let exchange = capabilities.exchange.clone();

        checks.push(AdminAuditCheck {
            kind: AdminAuditCheckKind::GetStatus,
            exchange: exchange.clone(),
            symbol: None,
            requires_private_rest: false,
        });
        checks.push(AdminAuditCheck {
            kind: AdminAuditCheckKind::GetCapabilities,
            exchange: exchange.clone(),
            symbol: None,
            requires_private_rest: false,
        });

        if capabilities.supports_symbol_rules {
            push_symbol_checks(
                &mut checks,
                AdminAuditCheckKind::GetSymbolRules,
                &exchange,
                symbols,
                false,
            );
        }
        if capabilities.supports_order_book_snapshot {
            push_symbol_checks(
                &mut checks,
                AdminAuditCheckKind::GetOrderBook,
                &exchange,
                symbols,
                false,
            );
        }
        push_private_check(
            &mut checks,
            &mut skipped_private_checks,
            capabilities,
            AdminAuditCheckKind::GetBalances,
            capabilities.supports_balances,
            None,
        );
        push_private_check(
            &mut checks,
            &mut skipped_private_checks,
            capabilities,
            AdminAuditCheckKind::GetPositions,
            capabilities.supports_positions,
            None,
        );
        if capabilities.supports_fees {
            for symbol in symbols {
                push_private_check(
                    &mut checks,
                    &mut skipped_private_checks,
                    capabilities,
                    AdminAuditCheckKind::GetFees,
                    true,
                    Some(symbol.clone()),
                );
            }
        }
        push_private_check(
            &mut checks,
            &mut skipped_private_checks,
            capabilities,
            AdminAuditCheckKind::GetOpenOrders,
            capabilities.supports_open_orders,
            None,
        );
        if capabilities.supports_recent_fills {
            for symbol in symbols {
                push_private_check(
                    &mut checks,
                    &mut skipped_private_checks,
                    capabilities,
                    AdminAuditCheckKind::GetRecentFills,
                    true,
                    Some(symbol.clone()),
                );
            }
        }

        AdminAuditPlan {
            exchange,
            safety: AdminAuditSafety::default(),
            checks,
            skipped_private_checks,
        }
    }
}

fn push_symbol_checks(
    checks: &mut Vec<AdminAuditCheck>,
    kind: AdminAuditCheckKind,
    exchange: &ExchangeId,
    symbols: &[SymbolScope],
    requires_private_rest: bool,
) {
    if symbols.is_empty() {
        checks.push(AdminAuditCheck {
            kind,
            exchange: exchange.clone(),
            symbol: None,
            requires_private_rest,
        });
        return;
    }
    checks.extend(symbols.iter().cloned().map(|symbol| AdminAuditCheck {
        kind,
        exchange: exchange.clone(),
        symbol: Some(symbol),
        requires_private_rest,
    }));
}

fn push_private_check(
    checks: &mut Vec<AdminAuditCheck>,
    skipped_private_checks: &mut Vec<AdminAuditCheckKind>,
    capabilities: &ExchangeClientCapabilities,
    kind: AdminAuditCheckKind,
    supported: bool,
    symbol: Option<SymbolScope>,
) {
    if !supported || !capabilities.supports_private_rest {
        if !skipped_private_checks.contains(&kind) {
            skipped_private_checks.push(kind);
        }
        return;
    }
    checks.push(AdminAuditCheck {
        kind,
        exchange: capabilities.exchange.clone(),
        symbol,
        requires_private_rest: true,
    });
}

use serde::{Deserialize, Serialize};

use crate::exchanges::unified::MarketType;

use super::relationship::ArbitrageRelationshipType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountStructure {
    SeparateAccounts,
    UnifiedAccount,
    CrossMargin,
    IsolatedMargin,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollateralAsset {
    pub asset: String,
    pub total_balance: f64,
    pub available_balance: f64,
    pub collateral_enabled: bool,
    pub collateral_haircut: f64,
    pub effective_collateral_value_usdt: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExchangeCapitalCapabilities {
    pub supports_unified_account: bool,
    pub supports_spot_as_collateral: bool,
    pub supports_cross_margin: bool,
    pub supports_isolated_margin: bool,
    pub supports_internal_transfer: bool,
    pub supports_automatic_borrow: bool,
    pub supports_portfolio_margin: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapitalRequirement {
    pub spot_cash_required_usdt: f64,
    pub spot_inventory_required_usdt: f64,
    pub perpetual_initial_margin_required_usdt: f64,
    pub perpetual_maintenance_margin_required_usdt: f64,
    pub collateral_required_usdt: f64,
    pub effective_total_capital_required_usdt: f64,
    pub fragmented_capital_penalty_usdt: f64,
    pub unified_account_benefit_usdt: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapitalModelInput {
    pub relationship_type: ArbitrageRelationshipType,
    pub buy_market_type: MarketType,
    pub sell_market_type: MarketType,
    pub target_notional_usdt: f64,
    pub account_structure: AccountStructure,
    pub collateral_assets: Vec<CollateralAsset>,
    pub buy_exchange_capabilities: ExchangeCapitalCapabilities,
    pub sell_exchange_capabilities: ExchangeCapitalCapabilities,
    pub initial_margin_rate: f64,
    pub maintenance_margin_rate: f64,
    pub liquidation_buffer_bps: f64,
    pub fragmented_capital_penalty_bps: f64,
}

pub fn calculate_capital_requirement(input: &CapitalModelInput) -> CapitalRequirement {
    let spot_cash_required_usdt = if input.buy_market_type == MarketType::Spot {
        input.target_notional_usdt
    } else {
        0.0
    };
    let spot_inventory_required_usdt = if input.sell_market_type == MarketType::Spot {
        input.target_notional_usdt
    } else {
        0.0
    };
    let perp_legs = [input.buy_market_type, input.sell_market_type]
        .into_iter()
        .filter(|market| *market == MarketType::Perpetual)
        .count() as f64;
    let perpetual_initial_margin_required_usdt =
        input.target_notional_usdt * input.initial_margin_rate.max(0.0) * perp_legs;
    let perpetual_maintenance_margin_required_usdt =
        input.target_notional_usdt * input.maintenance_margin_rate.max(0.0) * perp_legs;
    let liquidation_buffer_usdt =
        input.target_notional_usdt * input.liquidation_buffer_bps.max(0.0) / 10_000.0 * perp_legs;
    let separate_requirement = spot_cash_required_usdt
        + spot_inventory_required_usdt
        + perpetual_initial_margin_required_usdt
        + perpetual_maintenance_margin_required_usdt
        + liquidation_buffer_usdt;

    let unified_supported = matches!(
        input.account_structure,
        AccountStructure::UnifiedAccount | AccountStructure::CrossMargin
    ) && input.buy_exchange_capabilities.supports_unified_account
        && input.sell_exchange_capabilities.supports_unified_account;
    let effective_collateral = input
        .collateral_assets
        .iter()
        .filter(|asset| asset.collateral_enabled)
        .map(|asset| {
            asset
                .effective_collateral_value_usdt
                .min(asset.available_balance * asset.collateral_haircut.max(0.0))
        })
        .sum::<f64>();
    let unified_account_benefit_usdt = if unified_supported {
        effective_collateral.min(separate_requirement * 0.25)
    } else {
        0.0
    };
    let fragmented_capital_penalty_usdt = if unified_supported {
        0.0
    } else {
        separate_requirement * input.fragmented_capital_penalty_bps.max(0.0) / 10_000.0
    };
    let effective_total_capital_required_usdt =
        (separate_requirement - unified_account_benefit_usdt + fragmented_capital_penalty_usdt)
            .max(0.0);
    CapitalRequirement {
        spot_cash_required_usdt,
        spot_inventory_required_usdt,
        perpetual_initial_margin_required_usdt,
        perpetual_maintenance_margin_required_usdt,
        collateral_required_usdt: perpetual_initial_margin_required_usdt + liquidation_buffer_usdt,
        effective_total_capital_required_usdt,
        fragmented_capital_penalty_usdt,
        unified_account_benefit_usdt,
    }
}

pub fn expected_return_on_capital(expected_pnl: f64, capital: &CapitalRequirement) -> f64 {
    if capital.effective_total_capital_required_usdt > 0.0 {
        expected_pnl / capital.effective_total_capital_required_usdt
    } else {
        0.0
    }
}

pub fn expected_return_on_capital_per_hour(return_on_capital: f64, holding_seconds: u64) -> f64 {
    let hours = (holding_seconds as f64 / 3_600.0).max(1.0 / 60.0);
    return_on_capital / hours
}

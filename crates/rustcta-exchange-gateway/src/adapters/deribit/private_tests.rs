use rustcta_exchange_api::SymbolScope;
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderStatus, PositionSide,
    TenantId,
};
use serde_json::Value;

use super::options::{parse_greeks_snapshot, parse_option_contracts, parse_settlement_events};
use super::private_parser::{parse_balances, parse_fills, parse_order, parse_positions};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("deribit").expect("exchange")
}

fn tenant_id() -> TenantId {
    TenantId::new("tenant").expect("tenant")
}

fn account_id() -> AccountId {
    AccountId::new("main").expect("account")
}

fn option_symbol() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Option,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id(),
            MarketType::Option,
            "BTC-28JUN24-70000-C",
        )
        .expect("exchange symbol"),
    }
}

#[test]
fn deribit_private_parsers_should_parse_balances_positions_orders_and_fills() {
    let balances_json: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/deribit/account_summary.json"
    ))
    .expect("fixture");
    let balances = parse_balances(
        &exchange_id(),
        tenant_id(),
        account_id(),
        &[],
        MarketType::Futures,
        &balances_json,
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].asset, "BTC");
    assert_eq!(balances[0].balances[0].available, 2.1);

    let positions_json: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/deribit/positions.json"
    ))
    .expect("fixture");
    let positions = parse_positions(&exchange_id(), tenant_id(), account_id(), &positions_json)
        .expect("positions");
    assert_eq!(positions.len(), 2);
    assert_eq!(positions[1].market_type, MarketType::Option);
    assert_eq!(positions[1].side, PositionSide::Short);

    let order_json: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/deribit/order.json"
    ))
    .expect("fixture");
    let order = parse_order(&exchange_id(), Some(&option_symbol()), &order_json)
        .expect("order")
        .expect("present");
    assert_eq!(order.status, OrderStatus::Open);
    assert_eq!(order.client_order_id.as_deref(), Some("client-1"));

    let fills_json: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/deribit/fills.json"
    ))
    .expect("fixture");
    let fills = parse_fills(
        &exchange_id(),
        tenant_id(),
        account_id(),
        Some(&option_symbol()),
        &fills_json,
    )
    .expect("fills");
    assert_eq!(fills[0].fill_id.as_deref(), Some("T-1"));
    assert_eq!(fills[0].fee_asset.as_deref(), Some("BTC"));
}

#[test]
fn deribit_option_helpers_should_parse_chain_greeks_and_settlements() {
    let contracts_json: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/deribit/instruments_options.json"
    ))
    .expect("fixture");
    let contracts = parse_option_contracts(&exchange_id(), &contracts_json).expect("contracts");
    assert_eq!(contracts[0].settlement_currency, "BTC");
    assert_eq!(contracts[0].option_type, "call");

    let greeks_json: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/deribit/greeks_ticker.json"
    ))
    .expect("fixture");
    let greeks = parse_greeks_snapshot(&exchange_id(), &greeks_json).expect("greeks");
    assert_eq!(greeks.delta, Some(0.42));
    assert_eq!(greeks.mark_iv, Some(58.4));

    let settlements_json: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/deribit/settlements.json"
    ))
    .expect("fixture");
    let settlements =
        parse_settlement_events(&exchange_id(), &settlements_json).expect("settlements");
    assert_eq!(settlements[0].session_profit_loss, Some(0.012));
}

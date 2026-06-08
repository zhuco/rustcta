use rustcta_exchange_api::{AccountId, ExchangeApiError, SymbolScope, TenantId};
use rustcta_types::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
};

use super::parser::normalize_indodax_symbol;
use super::private_parser::{parse_balances, parse_fills, parse_order_state, parse_orders};
use super::signing::{build_form_body, sign_form};

#[test]
fn indodax_private_signing_should_match_fixture_vector() {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/indodax/signing_vectors/tapi_hmac_sha512.json"
    ))
    .expect("fixture");
    let params = vector
        .get("params")
        .and_then(serde_json::Value::as_object)
        .expect("params")
        .iter()
        .map(|(key, value)| (key.clone(), value.as_str().expect("string").to_string()))
        .collect();
    let body = build_form_body(&params);
    assert_eq!(body, vector["payload"].as_str().expect("payload"));
    assert_eq!(
        sign_form(vector["secret"].as_str().expect("secret"), &body).expect("signature"),
        vector["signature"].as_str().expect("signature")
    );
}

#[test]
fn indodax_private_parser_should_parse_balances_orders_and_fills() {
    let exchange = ExchangeId::new("indodax").expect("exchange");
    let tenant = TenantId::new("tenant-a").expect("tenant");
    let account = AccountId::new("account-a").expect("account");
    let balance_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/indodax/balance.json"
    ))
    .expect("balance fixture");
    let balances = parse_balances(
        &exchange,
        tenant.clone(),
        account.clone(),
        &[],
        &balance_value,
    )
    .expect("balances");
    let idr_balance = balances[0]
        .balances
        .iter()
        .find(|balance| balance.asset == "IDR")
        .expect("IDR balance");
    assert_eq!(idr_balance.available, 900000.0);

    let symbol = btc_idr_scope(&exchange);
    let order_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/indodax/order.json"
    ))
    .expect("order fixture");
    let order = parse_order_state(&exchange, Some(&symbol), &order_value).expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("12345"));
    assert_eq!(order.side, OrderSide::Buy);
    assert_eq!(order.status, OrderStatus::Open);
    assert_eq!(order.quantity, "0.02000000");
    assert_eq!(order.filled_quantity, "0.01");

    let orders_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/indodax/open_orders.json"
    ))
    .expect("open orders fixture");
    let orders = parse_orders(&exchange, Some(&symbol), &orders_value).expect("orders");
    assert_eq!(orders.len(), 1);

    let fills_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/indodax/fills.json"
    ))
    .expect("fills fixture");
    let fills =
        parse_fills(&exchange, tenant, account, Some(&symbol), &fills_value).expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("98765"));
    assert_eq!(fills[0].price, 100000000.0);
}

#[test]
fn indodax_normalize_symbol_should_reject_non_pair_symbol() {
    assert!(matches!(
        normalize_indodax_symbol("BTC"),
        Err(ExchangeApiError::InvalidRequest { .. })
    ));
}

fn btc_idr_scope(exchange: &ExchangeId) -> SymbolScope {
    SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "IDR").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange.clone(), MarketType::Spot, "btc_idr")
            .expect("symbol"),
    }
}

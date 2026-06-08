use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};

use super::private::{
    balances_request_spec_fixture, cancel_all_orders_request_spec_fixture,
    cancel_order_by_client_id_request_spec_fixture, cancel_order_request_spec_fixture,
    fees_request_spec_fixture, open_orders_request_spec_fixture, place_order_request_spec_fixture,
    query_order_request_spec_fixture, recent_fills_request_spec_fixture,
};
use super::signing::onetrading_authorization;
use super::streams::onetrading_private_auth_payload;
use super::{OneTradingGatewayAdapter, OneTradingGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("onetrading").expect("exchange")
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("account").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn symbol() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "EUR").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTC_EUR")
            .expect("symbol"),
    }
}

#[test]
fn request_specs_and_bearer_boundary_should_match_docs() {
    assert_eq!(
        onetrading_authorization("<redacted:token>").expect("auth"),
        "Bearer <redacted:token>"
    );
    assert_eq!(balances_request_spec_fixture()["path"], "/account/balances");
    assert_eq!(fees_request_spec_fixture()["path"], "/account/fees");
    assert_eq!(
        open_orders_request_spec_fixture()["path"],
        "/account/orders"
    );
    assert_eq!(
        query_order_request_spec_fixture()["path"],
        "/account/orders/<redacted:order_id>"
    );
    assert_eq!(
        recent_fills_request_spec_fixture()["path"],
        "/account/trades"
    );
    assert_eq!(place_order_request_spec_fixture()["method"], "POST");
    assert_eq!(place_order_request_spec_fixture()["body"]["type"], "LIMIT");
    assert_eq!(
        place_order_request_spec_fixture()["body"]["time_in_force"],
        "GOOD_TILL_CANCELLED"
    );
    assert_eq!(
        cancel_order_request_spec_fixture()["path"],
        "/account/orders/<redacted:order_id>"
    );
    assert_eq!(
        cancel_order_by_client_id_request_spec_fixture()["path"],
        "/account/orders/client/<redacted:order_id>"
    );
    assert_eq!(cancel_all_orders_request_spec_fixture()["method"], "DELETE");
}

#[tokio::test]
async fn private_write_should_be_offline_request_spec_only() {
    let adapter =
        OneTradingGatewayAdapter::new(OneTradingGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: Some("<redacted:order_id>".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("64000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "onetrading.place_order_offline_request_spec_only"
        }
    ));
}

#[test]
fn private_websocket_boundary_should_stay_disabled_until_verified() {
    let auth = onetrading_private_auth_payload("<redacted:token>");
    assert_eq!(auth["type"], "AUTHENTICATE");
    assert_eq!(auth["api_token"], "<redacted:token>");

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/onetrading/ws_private_auth.json"
    ))
    .expect("private auth fixture");
    assert_eq!(fixture, auth);

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/onetrading/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["futures_live_enabled"], false);
    assert_eq!(boundary["private_ws_live_enabled"], false);
    assert_eq!(boundary["withdrawals_enabled"], false);
}

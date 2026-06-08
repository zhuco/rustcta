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
    cancel_order_request_spec_fixture, place_order_request_spec_fixture,
    positions_request_spec_fixture,
};
use super::signing::arkham_signature;
use super::streams::arkham_private_auth_payload;
use super::{ArkhamGatewayAdapter, ArkhamGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("arkham").expect("exchange")
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

fn symbol(market_type: MarketType, raw: &str, base: &str, quote: &str) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, raw).expect("symbol"),
    }
}

#[test]
fn request_specs_and_signing_should_match_hmac_boundary() {
    let place = place_order_request_spec_fixture();
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], "/orders/new");
    assert_eq!(place["auth"], "arkham_hmac_sha256_base64");
    assert_eq!(place["body"]["symbol"], "BTC_USDT");
    assert_eq!(place["body"]["clientOrderId"], "<redacted:order_id>");
    assert_eq!(balances_request_spec_fixture()["path"], "/account/balances");
    assert_eq!(
        positions_request_spec_fixture()["path"],
        "/account/positions"
    );
    assert_eq!(
        cancel_order_request_spec_fixture()["path"],
        "/orders/cancel"
    );
    assert_eq!(
        cancel_all_orders_request_spec_fixture()["path"],
        "/orders/cancel/all"
    );

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/arkham/signing_vectors/rest_hmac_sha256.json"
    ))
    .expect("fixture");
    let signature = arkham_signature(
        fixture["principal"].as_str().expect("principal"),
        fixture["credential_material_base64"]
            .as_str()
            .expect("credential material"),
        fixture["expires_us"].as_i64().expect("expires"),
        fixture["method"].as_str().expect("method"),
        fixture["path"].as_str().expect("path"),
        fixture["body"].as_str().expect("body"),
    )
    .expect("signature");
    assert_eq!(fixture["expected_hmac_sha256_base64"], signature);
}

#[tokio::test]
async fn private_write_should_be_offline_request_spec_only() {
    let adapter = ArkhamGatewayAdapter::new(ArkhamGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(MarketType::Spot, "BTC_USDT", "BTC", "USDT"),
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
            operation: "arkham.place_order_offline_request_spec_only"
        }
    ));
}

#[test]
fn private_websocket_boundary_should_stay_disabled_until_verified() {
    let auth = arkham_private_auth_payload(
        "<redacted:api_key>",
        1_700_000_300_000_000,
        "<redacted:signature>",
    );
    assert_eq!(auth["op"], "auth");
    assert_eq!(auth["headers"]["Arkham-Broker-Id"], "1001");

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/arkham/ws_private_auth.json"
    ))
    .expect("private auth fixture");
    assert_eq!(fixture, auth);

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/arkham/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["intel_api_is_trading_adapter"], false);
    assert_eq!(boundary["private_ws_live_enabled"], false);
}

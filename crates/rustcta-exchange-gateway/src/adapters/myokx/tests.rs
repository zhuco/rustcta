use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, RequestContext,
    SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};

use crate::adapters::AdapterBackedGateway;
use crate::signing_spec::SigningVector;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::{
    myokx_heartbeat_policy, myokx_pong_payload, myokx_private_login_payload,
    myokx_private_subscribe_payload, myokx_public_subscribe_payload,
    myokx_public_unsubscribe_payload, myokx_public_ws_session,
};
use super::{new_adapter, MyOkxGatewayConfig};

#[test]
fn myokx_config_should_map_to_okx_eea_profile() {
    let config = MyOkxGatewayConfig::default();
    assert_eq!(config.rest_base_url, "https://eea.okx.com");
    assert_eq!(
        config.public_ws_url,
        "wss://wseea.okx.com:8443/ws/v5/public"
    );

    let okx_profile = config.into_okx_config();
    assert_eq!(okx_profile.exchange_id, "myokx");
    assert_eq!(okx_profile.rest_base_url, "https://eea.okx.com");
    assert!(!okx_profile.enabled_private_rest);
    assert!(okx_profile.api_key.is_none());
    assert_eq!(
        okx_profile.unsupported_market_type_operation,
        "myokx.non_spot_market_type"
    );
}

#[test]
fn myokx_adapter_should_register_by_name() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["myokx"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[tokio::test]
async fn myokx_capabilities_should_be_spot_public_only() {
    let adapter = new_adapter(MyOkxGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.exchange, exchange_id());
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("private rest should remain disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "myokx.get_balances"
        }
    ));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_balances"
            && endpoint.path.as_deref() == Some("/unsupported/myokx/get_balances")));
}

#[test]
fn myokx_parser_fixtures_should_reuse_okx_spot_shape_with_profile_id() {
    let instruments: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/instruments.json"
    ))
    .expect("instruments fixture");
    let rules = parse_symbol_rules(&exchange_id(), &instruments).expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].symbol.exchange, exchange_id());
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTC-USDT");

    let orderbook: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/orderbook.json"
    ))
    .expect("orderbook fixture");
    let book = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), &orderbook).expect("book");
    assert_eq!(book.exchange_id, exchange_id());
    assert_eq!(book.bids[0].price, 65000.0);
}

#[test]
fn myokx_ws_payloads_should_use_eea_hosts_and_okx_shapes() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let subscribe = myokx_public_subscribe_payload(&subscription).expect("subscribe");
    assert_eq!(subscribe["op"], "subscribe");
    assert_eq!(subscribe["args"][0]["channel"], "books");
    assert_eq!(subscribe["args"][0]["instId"], "BTC-USDT");

    let unsubscribe = myokx_public_unsubscribe_payload(&subscription).expect("unsubscribe");
    assert_eq!(unsubscribe["op"], "unsubscribe");

    let session =
        myokx_public_ws_session(&MyOkxGatewayConfig::default(), &subscription).expect("session");
    assert_eq!(session["url"], "wss://wseea.okx.com:8443/ws/v5/public");
    assert_eq!(myokx_heartbeat_policy()["client_pong"], "pong");
    assert_eq!(
        myokx_pong_payload(),
        serde_json::Value::String("pong".to_string())
    );
}

#[test]
fn myokx_private_ws_auth_shape_should_match_sanitized_signing_vector() {
    let vector: SigningVector = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/signing_vectors/ws_login.json"
    ))
    .expect("signing vector");
    vector.verify().expect("signing vector");

    let login = myokx_private_login_payload(
        "sanitized-api-key",
        &vector.secret,
        "sanitized-passphrase",
        vector.timestamp.as_deref().expect("timestamp"),
    );
    assert_eq!(login["op"], "login");
    assert_eq!(login["args"][0]["sign"], vector.expected_signature);

    let private_subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").unwrap(),
        kind: PrivateStreamKind::Orders,
    };
    let subscribe = myokx_private_subscribe_payload(&private_subscription);
    assert_eq!(subscribe["args"][0]["channel"], "orders");
}

#[test]
fn myokx_boundary_fixtures_should_be_sanitized() {
    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    assert_eq!(boundary["exchange"], "myokx");
    assert_eq!(boundary["profile_of"], "okx");
    assert_eq!(boundary["private_rest"]["trade_enabled"], false);
    assert!(boundary["credential_values"]
        .as_object()
        .unwrap()
        .is_empty());

    let request_spec: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/request_specs/private_rest_unsupported.json"
    ))
    .expect("request spec fixture");
    assert_eq!(request_spec["support"], "unsupported");
    assert!(request_spec["credential_values"]
        .as_object()
        .unwrap()
        .is_empty());

    let ws_public: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/ws/public_books_snapshot.json"
    ))
    .expect("public ws fixture");
    assert_eq!(ws_public["arg"]["channel"], "books");

    let ws_private: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/ws/private_orders_unsupported.json"
    ))
    .expect("private ws fixture");
    assert!(ws_private["credential_values"]
        .as_object()
        .unwrap()
        .is_empty());
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("myokx").unwrap()
}

fn symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTC-USDT").unwrap(),
    }
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").unwrap()),
        account_id: Some(AccountId::new("account").unwrap()),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

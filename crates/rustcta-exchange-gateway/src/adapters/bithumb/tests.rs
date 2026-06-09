use std::collections::{BTreeMap, HashMap};

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, RequestContext,
    SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::{json, Value};

use super::parser::{normalize_bithumb_symbol, parse_orderbook_snapshot, parse_symbol_rules};
use super::private::{bithumb_cancel_order_query, bithumb_place_order_body};
use super::private_parser::{
    parse_balances, parse_fee_snapshot, parse_fills_from_orders, parse_order, parse_orders,
};
use super::signing::{bithumb_jwt, bithumb_query_hash};
use super::streams::{
    bithumb_ping_payload, bithumb_private_subscribe_payload, bithumb_public_order_book_ws_policy,
    bithumb_public_subscribe_payload, bithumb_reconnect_policy_ms,
};
use super::transport::{public_request_shape, signed_body_shape, signed_get_shape};
use super::{BithumbGatewayAdapter, BithumbGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn fixture(path: &str) -> Value {
    let full_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/exchanges/bithumb")
        .join(path);
    let text = std::fs::read_to_string(&full_path)
        .unwrap_or_else(|error| panic!("read fixture {}: {error}", full_path.display()));
    serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("parse fixture {}: {error}", full_path.display()))
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bithumb").expect("exchange")
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

fn spot_symbol() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "KRW").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "KRW-BTC")
            .expect("symbol"),
    }
}

#[test]
fn parser_should_parse_krw_markets_and_orderbook() {
    let rules =
        parse_symbol_rules(&exchange_id(), &fixture("markets_success.json")).expect("rules");
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "KRW");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "KRW-BTC");

    let book = parse_orderbook_snapshot(&exchange_id(), spot_symbol(), &fixture("orderbook.json"))
        .expect("book");
    assert_eq!(book.bids[0].price, 69_540_000.0);
    assert_eq!(book.asks[0].quantity, 0.24078656);
    assert!(book.exchange_timestamp.is_some());
    assert_eq!(
        normalize_bithumb_symbol("btckrw").expect("normalized"),
        "KRW-BTC"
    );
}

#[test]
fn private_parsers_should_handle_balances_orders_and_fills() {
    let balances = parse_balances(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &[],
        &fixture("balances.json"),
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].asset, "KRW");
    assert_eq!(balances[0].balances[1].asset, "BTC");

    let order = parse_order(
        &exchange_id(),
        Some(&spot_symbol()),
        &fixture("order_ack.json"),
    )
    .expect("order");
    assert_eq!(
        order.exchange_order_id.as_deref(),
        Some("C0101000000001799653")
    );
    assert_eq!(order.client_order_id.as_deref(), Some("my-order-001"));

    let orders = parse_orders(
        &exchange_id(),
        Some(&spot_symbol()),
        &fixture("orders.json"),
    )
    .expect("orders");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].filled_quantity, "0");

    let fills = parse_fills_from_orders(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        Some(&spot_symbol()),
        &fixture("filled_orders.json"),
    )
    .expect("fills");
    assert_eq!(fills[0].fill_id.as_deref(), Some("trade-001"));
    assert_eq!(fills[0].quantity, 0.001);

    let fees = parse_fee_snapshot(&spot_symbol(), &fixture("orders_chance.json")).expect("fees");
    assert_eq!(fees.maker_rate, "0.0004");
    assert_eq!(fees.taker_rate, "0.0005");
}

#[test]
fn signing_vector_should_cover_sha512_query_hash_and_jwt_shape() {
    let vector = fixture("signing_vectors/jwt_query_hash.json");
    let query = vector["query"].as_str().expect("query");
    assert_eq!(
        bithumb_query_hash(query).as_deref(),
        vector["expected_query_hash"].as_str()
    );
    let parts = bithumb_jwt(
        vector["access_key"].as_str().expect("access_key"),
        vector["secret"].as_str().expect("secret"),
        query,
        vector["nonce"].as_str().expect("nonce"),
        vector["timestamp"].as_i64().expect("timestamp"),
    )
    .expect("jwt");
    assert_eq!(
        parts.query_hash.as_deref(),
        vector["expected_query_hash"].as_str()
    );
    assert_eq!(parts.token.split('.').count(), 3);
    assert!(!parts
        .token
        .contains(vector["secret"].as_str().expect("secret")));
}

#[test]
fn request_specs_should_match_offline_request_builders() {
    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: spot_symbol(),
        client_order_id: Some("my-order-001".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.001".to_string(),
        price: Some("84000000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let place_shape = signed_body_shape(
        "POST",
        "/v2/orders",
        bithumb_place_order_body(&order).expect("body"),
    )
    .expect("shape");
    assert_request_matches(
        "request_specs/place_order_limit.json",
        &ActualHttpRequest::new(place_shape.method, place_shape.path)
            .with_headers([("authorization".to_string(), "Bearer jwt".to_string())])
            .with_body(place_shape.body),
    );

    let cancel_params =
        bithumb_cancel_order_query(Some("C0101000000001799653"), None).expect("cancel params");
    let cancel_shape = signed_get_shape("DELETE", "/v2/order", &cancel_params);
    assert_request_matches(
        "request_specs/cancel_order.json",
        &ActualHttpRequest::new(cancel_shape.method, cancel_shape.path)
            .with_query(cancel_shape.query)
            .with_headers([("authorization".to_string(), "Bearer jwt".to_string())]),
    );

    let balances_shape = signed_get_shape("GET", "/v1/accounts", &BTreeMap::new());
    assert_request_matches(
        "request_specs/get_balances.json",
        &ActualHttpRequest::new(balances_shape.method, balances_shape.path)
            .with_headers([("authorization".to_string(), "Bearer jwt".to_string())]),
    );

    let mut public_params = HashMap::new();
    public_params.insert("markets".to_string(), "KRW-BTC".to_string());
    let public_shape = public_request_shape("GET", "/v1/orderbook", &public_params);
    assert_request_matches(
        "request_specs/public_orderbook.json",
        &ActualHttpRequest::new(public_shape.method, public_shape.path)
            .with_query(public_shape.query),
    );
}

#[test]
fn websocket_fixtures_should_match_subscription_helpers() {
    let public = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: spot_symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let public_payload = bithumb_public_subscribe_payload(&public).expect("public payload");
    assert_eq!(
        public_payload,
        fixture("ws/public_orderbook_subscribe.json")
    );

    let public_snapshot = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws-snapshot"),
        symbol: spot_symbol(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    assert_eq!(
        bithumb_public_subscribe_payload(&public_snapshot).expect("snapshot payload"),
        fixture("ws/public_orderbook_snapshot_subscribe.json")
    );

    let private = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };
    assert_eq!(
        bithumb_private_subscribe_payload(&private),
        fixture("ws/private_order_subscribe.json")
    );
    assert_eq!(bithumb_ping_payload(), json!({ "type": "ping" }));
    assert_eq!(bithumb_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[test]
fn public_orderbook_ws_policy_should_cover_snapshot_realtime_and_resync_rules() {
    let policy = bithumb_public_order_book_ws_policy();
    assert_eq!(policy.url, "wss://ws-api.bithumb.com/websocket/v1");
    assert_eq!(policy.channel, "orderbook");
    assert_eq!(policy.snapshot_switch, "is_only_snapshot");
    assert_eq!(policy.realtime_switch, "is_only_realtime");
    assert_eq!(policy.default_level, 0);
    assert!(policy.level_semantics.contains("price aggregation"));
    assert_eq!(policy.fixed_update_interval_ms, None);
    assert_eq!(policy.sequence_field, None);
    assert_eq!(policy.checksum, None);
    assert_eq!(policy.rest_snapshot_endpoint, "/v1/orderbook");
    assert!(policy
        .resync_strategy
        .contains("no orderbook sequence/checksum"));

    let realtime = parse_orderbook_snapshot(
        &exchange_id(),
        spot_symbol(),
        &fixture("ws/orderbook_realtime.json"),
    )
    .expect("realtime orderbook");
    assert_eq!(realtime.bids[0].price, 69_995_000.0);
    assert_eq!(realtime.asks[0].quantity, 0.31);
    assert!(realtime.exchange_timestamp.is_some());
}

#[tokio::test]
async fn private_boundaries_should_be_credential_gated_and_positions_unsupported() {
    let adapter = BithumbGatewayAdapter::new(BithumbGatewayConfig {
        enabled_private_rest: false,
        api_key: None,
        api_secret: None,
        ..BithumbGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_positions);

    let error = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("unsupported"),
            symbol: spot_symbol(),
            client_order_id: None,
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "0.001".to_string(),
            price: Some("84000000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect_err("private disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bithumb.place_order"
        }
    ));
}

#[test]
fn endpoint_mapping_should_declare_bithumb_task12_surface() {
    let mapping = include_str!("endpoint_mapping.yaml");
    for required in [
        "exchange: bithumb",
        "public_limit_per_second: 150",
        "private_limit_per_second: 140",
        "path: /v1/market/all",
        "path: /v1/orderbook",
        "channel: orderbook",
        "is_only_snapshot",
        "is_only_realtime",
        "fixed_update_interval_ms:",
        "unsupported_no_official_sequence_or_checksum",
        "rest_snapshot_after_reconnect_or_stale_stream",
        "path: /v2/orders",
        "path: /v2/order",
        "private_stream_requires_jwt_credentials",
    ] {
        assert!(
            mapping.contains(required),
            "endpoint_mapping missing {required}"
        );
    }
}

fn assert_request_matches(path: &str, actual: &ActualHttpRequest) {
    let spec: RequestSpec = serde_json::from_value(fixture(path)).expect("request spec");
    spec.assert_matches(actual)
        .unwrap_or_else(|error| panic!("{error}"));
}

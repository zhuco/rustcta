use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, OrderBookRequest, PublicStreamKind,
    PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::OrderStatus;
use serde_json::{json, Value};

use super::parser::{
    cex_ws_orderbook_seq_is_next, cex_ws_seq_id, parse_open_orders, parse_order_state,
    parse_orderbook_snapshot, parse_recent_fills, parse_symbol_rules, parse_ws_orderbook_increment,
    parse_ws_orderbook_snapshot,
};
use super::signing::{cex_rest_signature, cex_ws_signature};
use super::streams::{
    cex_pong_payload, cex_public_order_book_ws_policy, cex_public_orderbook_subscription_spec,
    cex_public_subscribe_payload, cex_public_unsubscribe_payload, cex_ws_auth_payload,
};
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{CexGatewayAdapter, CexGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn cex_parser_should_map_currency_limits_and_order_book_fixtures() {
    let currency_limits: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/currency_limits.json"
    ))
    .expect("currency limits fixture");
    let rules = parse_symbol_rules(&exchange_id(), &currency_limits).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTC/USD");
    assert_eq!(rules[0].min_quantity.as_deref(), Some("0.0001"));
    assert_eq!(rules[0].min_notional.as_deref(), Some("20"));
    assert!(rules[0].supports_market_orders);

    let orderbook: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/orderbook.json"
    ))
    .expect("orderbook fixture");
    let snapshot = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), Some(1), &orderbook)
        .expect("snapshot");
    assert_eq!(snapshot.bids.len(), 1);
    assert_eq!(snapshot.asks[0].price, 17689.66);
    assert_eq!(snapshot.sequence, None);
}

#[test]
fn cex_parser_should_reject_missing_pairs() {
    let missing: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/missing_required_fields.json"
    ))
    .expect("missing fixture");
    assert!(parse_symbol_rules(&exchange_id(), &missing).is_err());
}

#[test]
fn cex_private_parsers_should_map_order_open_orders_and_archived_fills() {
    let order: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/order_success.json"
    ))
    .expect("order fixture");
    let order = parse_order_state(&exchange_id(), Some(&symbol_scope()), &order).expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("1234"));
    assert_eq!(order.status, OrderStatus::Filled);
    assert_eq!(order.filled_quantity, "0.1");

    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/open_orders_success.json"
    ))
    .expect("open orders fixture");
    let orders =
        parse_open_orders(&exchange_id(), Some(&symbol_scope()), &open_orders).expect("orders");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].status, OrderStatus::Open);

    let archived_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/archived_orders_success.json"
    ))
    .expect("archived orders fixture");
    let fills = parse_recent_fills(
        &exchange_id(),
        context("fills").tenant_id.expect("tenant"),
        context("fills").account_id.expect("account"),
        &symbol_scope(),
        &archived_orders,
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].order_id.as_deref(), Some("1234"));
    assert_eq!(fills[0].quantity, 0.1);
    assert_eq!(fills[0].fee_asset.as_deref(), Some("USD"));
}

#[tokio::test]
async fn cex_adapter_should_load_symbol_rules_from_public_rest() {
    let response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/currency_limits.json"
    ))
    .expect("fixture");
    let (base_url, seen) = spawn_rest_server(vec![response]).await;
    let adapter = CexGatewayAdapter::new(CexGatewayConfig {
        rest_base_url: base_url,
        ..CexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].quote_asset, "USD");
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/currency_limits");
}

#[tokio::test]
async fn cex_adapter_should_load_order_book_from_public_rest() {
    let response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/orderbook.json"
    ))
    .expect("fixture");
    let (base_url, seen) = spawn_rest_server(vec![response]).await;
    let adapter = CexGatewayAdapter::new(CexGatewayConfig {
        rest_base_url: base_url,
        ..CexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(2),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 17670.3);
    assert_eq!(response.order_book.asks[0].quantity, 0.01);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/order_book/BTC/USD/");
    assert_eq!(request.query.get("depth").map(String::as_str), Some("2"));
}

#[test]
fn cex_signing_and_ws_payloads_should_be_offline_verifiable() {
    let ws_signature = cex_ws_signature(
        "1448034533",
        "1WZbtMTbMbo2NsW12vOz9IuPM",
        "1IuUeW4IEWatK87zBTENHj1T17s",
    )
    .expect("ws signature");
    assert_eq!(
        ws_signature,
        "7d581adb01ad22f1ed38e1159a7f08ac5d83906ae1a42fe17e7d977786fe9694"
    );
    let rest_signature =
        cex_rest_signature("1700000000000", "user123", "fixture-key", "fixture-secret")
            .expect("rest signature");
    assert_eq!(rest_signature, rest_signature.to_ascii_uppercase());

    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    assert_eq!(
        cex_public_subscribe_payload(&subscription, "oid-1").expect("subscribe"),
        json!({
            "e": "order-book-subscribe",
            "data": {
                "pair": ["BTC", "USD"],
                "subscribe": true,
                "depth": 25
            },
            "oid": "oid-1"
        })
    );
    assert_eq!(
        cex_public_unsubscribe_payload(&subscription, "oid-2").expect("unsubscribe"),
        json!({
            "e": "order-book-unsubscribe",
            "data": {"pair": ["BTC", "USD"]},
            "oid": "oid-2"
        })
    );
    assert_eq!(cex_pong_payload(), json!({ "e": "pong" }));
    assert_eq!(
        cex_ws_auth_payload("fixture-key", "fixture-secret", "1700000000")
            .expect("auth")
            .get("e")
            .and_then(Value::as_str),
        Some("auth")
    );
}

#[test]
fn cex_public_orderbook_ws_policy_and_spec_should_describe_seqid_rebuild() {
    let policy = cex_public_order_book_ws_policy();
    assert_eq!(policy.snapshot_event, "order_book_subscribe");
    assert_eq!(policy.increment_event, "order_book_increment");
    assert_eq!(policy.sequence_field, "seqId");
    assert!(policy.continuity.contains("previous seqId + 1"));
    assert!(policy.resync.contains("REST order book snapshot"));

    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let spec =
        cex_public_orderbook_subscription_spec(&subscription, "wss://ws.cex.io/ws/", "oid-book")
            .expect("spec");
    assert_eq!(spec.url, "wss://ws.cex.io/ws/");
    assert_eq!(spec.event, "order-book-subscribe");
    assert_eq!(spec.pair, "BTC:USD");
    assert_eq!(
        spec.subscribe_payload,
        json!({
            "e": "order-book-subscribe",
            "data": {
                "pair": ["BTC", "USD"],
                "subscribe": true,
                "depth": 25
            },
            "oid": "oid-book"
        })
    );
}

#[test]
fn cex_public_orderbook_ws_parser_should_parse_snapshot_increment_and_seqid() {
    let snapshot_value: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/ws/orderbook_snapshot.json"
    ))
    .expect("snapshot fixture");
    let snapshot = parse_ws_orderbook_snapshot(&exchange_id(), symbol_scope(), &snapshot_value)
        .expect("snapshot");
    assert_eq!(snapshot.order_book.bids.len(), 2);
    assert_eq!(snapshot.order_book.asks[0].price, 17689.66);
    assert_eq!(snapshot.order_book.sequence, Some(9001));
    assert_eq!(cex_ws_seq_id(&snapshot_value), Some(9001));

    let increment_value: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/ws/orderbook_increment.json"
    ))
    .expect("increment fixture");
    let increment = parse_ws_orderbook_increment(&exchange_id(), symbol_scope(), &increment_value)
        .expect("increment");
    assert_eq!(increment.order_book.bids[0].price, 17671.0);
    assert_eq!(increment.order_book.asks[0].quantity, 0.15);
    assert_eq!(increment.order_book.sequence, Some(9002));
    assert_eq!(cex_ws_seq_id(&increment_value), Some(9002));
}

#[test]
fn cex_public_orderbook_ws_seqid_policy_should_detect_gaps() {
    assert!(cex_ws_orderbook_seq_is_next(None, 9001));
    assert!(cex_ws_orderbook_seq_is_next(Some(9001), 9002));
    assert!(!cex_ws_orderbook_seq_is_next(Some(9001), 9003));
    assert!(!cex_ws_orderbook_seq_is_next(Some(9001), 9001));
}

#[test]
fn cex_named_registration_should_accept_aliases_and_keep_private_disabled() {
    let gateway =
        AdapterBackedGateway::with_named_adapters("cex-test", ["cex.io"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
    let adapter = CexGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
}

#[tokio::test]
async fn cex_private_readbacks_should_fail_closed_when_disabled() {
    let adapter = CexGatewayAdapter::default_public().expect("adapter");
    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1234".to_string()),
        })
        .await
        .expect_err("private REST disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "cex.query_order"
        }
    ));
}

#[tokio::test]
async fn cex_query_order_should_send_signed_private_post() {
    let response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/order_success.json"
    ))
    .expect("fixture");
    let (base_url, seen) = spawn_rest_server(vec![response]).await;
    let adapter = private_adapter(base_url).expect("adapter");

    let response = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1234".to_string()),
        })
        .await
        .expect("query order");

    assert_eq!(
        response
            .order
            .as_ref()
            .and_then(|order| order.exchange_order_id.as_deref()),
        Some("1234")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/get_order/");
    assert!(request
        .headers
        .get("content-type")
        .is_some_and(|value| value.starts_with("application/json")));
    let body = request.body.expect("json body");
    assert_eq!(body.get("id").and_then(Value::as_str), Some("1234"));
    assert_signed_body(&body);
}

#[tokio::test]
async fn cex_open_orders_and_recent_fills_should_use_pair_scoped_signed_posts() {
    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/open_orders_success.json"
    ))
    .expect("open orders fixture");
    let archived_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/cex/archived_orders_success.json"
    ))
    .expect("archived orders fixture");
    let (base_url, seen) = spawn_rest_server(vec![open_orders, archived_orders]).await;
    let adapter = private_adapter(base_url).expect("adapter");

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: chrono::DateTime::from_timestamp(1_700_000_000, 0),
            end_time: chrono::DateTime::from_timestamp(1_700_003_600, 0),
            limit: Some(10),
            page: None,
        })
        .await
        .expect("recent fills");
    assert_eq!(fills.fills.len(), 1);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/open_orders/BTC/USD");
    assert_signed_body(requests[0].body.as_ref().expect("open body"));

    assert_eq!(requests[1].method, "POST");
    assert_eq!(requests[1].path, "/archived_orders/BTC/USD");
    let body = requests[1].body.as_ref().expect("fills body");
    assert_eq!(body.get("limit").and_then(Value::as_u64), Some(10));
    assert_eq!(
        body.get("dateFrom").and_then(Value::as_i64),
        Some(1_700_000_000)
    );
    assert_eq!(
        body.get("dateTo").and_then(Value::as_i64),
        Some(1_700_003_600)
    );
    assert_signed_body(body);
}

fn private_adapter(
    rest_base_url: String,
) -> rustcta_exchange_api::ExchangeApiResult<CexGatewayAdapter> {
    CexGatewayAdapter::new(CexGatewayConfig {
        rest_base_url,
        enabled_private_rest: true,
        api_key: Some("fixture-key".to_string()),
        api_secret: Some("fixture-secret".to_string()),
        user_id: Some("user123".to_string()),
        ..CexGatewayConfig::default()
    })
}

fn assert_signed_body(body: &Value) {
    assert_eq!(body.get("key").and_then(Value::as_str), Some("fixture-key"));
    let nonce = body.get("nonce").and_then(Value::as_str).expect("nonce");
    assert!(!nonce.is_empty());
    let signature = body
        .get("signature")
        .and_then(Value::as_str)
        .expect("signature");
    assert_eq!(signature.len(), 64);
    assert_eq!(signature, signature.to_ascii_uppercase());
}

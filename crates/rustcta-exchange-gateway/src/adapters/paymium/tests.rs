use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{
    parse_orderbook_snapshot, parse_private_open_orders, parse_private_order_state,
    parse_private_recent_fills, parse_public_book_update, parse_symbol_rules,
};
use super::signing::{paymium_private_headers, paymium_rest_signature};
use super::streams::{
    paymium_public_order_book_rebuild_strategy, paymium_public_order_book_ws_policy,
    paymium_public_socket_descriptor, paymium_public_socket_subscription_spec,
    paymium_user_socket_descriptor,
};
use super::{PaymiumGatewayAdapter, PaymiumGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn paymium_parser_should_map_btc_eur_ticker_and_order_book_fixtures() {
    let ticker: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/ticker_btc_eur.json"
    ))
    .expect("ticker fixture");
    let rules = parse_symbol_rules(&exchange_id(), &ticker).expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "EUR");
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.00000001"));
    assert!(rules[0].supports_limit_orders);

    let orderbook: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/orderbook_btc_eur.json"
    ))
    .expect("orderbook fixture");
    let snapshot = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), Some(1), &orderbook)
        .expect("snapshot");
    assert_eq!(snapshot.bids.len(), 1);
    assert_eq!(snapshot.asks.len(), 1);
    assert_eq!(snapshot.bids[0].price, 64100.0);
    assert_eq!(snapshot.asks[0].quantity, 0.27);
}

#[test]
fn paymium_parser_should_reject_missing_order_book_levels() {
    let missing: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/missing_required_fields.json"
    ))
    .expect("missing fixture");
    assert!(parse_orderbook_snapshot(&exchange_id(), symbol_scope(), None, &missing).is_err());
}

#[test]
fn paymium_signing_and_socket_descriptors_should_be_offline_verifiable() {
    let body =
        r#"{"type":"LimitOrder","currency":"EUR","direction":"buy","price":50000,"amount":0.01}"#;
    let signature = paymium_rest_signature(
        "1700000000000",
        "https://paymium.com/api/v1/user/orders",
        body,
        "fixture-secret",
    )
    .expect("signature");
    assert_eq!(
        signature,
        "ff5ed709bbc679919dc37bf549478110e737c0d9be643e7ad43283e057cbbdbe"
    );
    let headers = paymium_private_headers(
        "fixture-key",
        "fixture-secret",
        "1700000000000",
        "https://paymium.com/api/v1/user/orders",
        body,
    )
    .expect("headers");
    assert_eq!(headers[0].0, "Api-Key");
    assert_eq!(headers[1].0, "Api-Signature");
    assert_eq!(headers[2].0, "Api-Nonce");

    assert_eq!(
        paymium_public_socket_descriptor()["path"].as_str(),
        Some("/ws/socket.io")
    );
    assert_eq!(
        paymium_public_socket_descriptor()["protocol"].as_str(),
        Some("socket.io_v1.3")
    );
    assert_eq!(
        paymium_public_socket_descriptor()["connect_packet"].as_str(),
        Some("1::")
    );
    assert_eq!(
        paymium_user_socket_descriptor("fixture-channel")["emit"][1].as_str(),
        Some("fixture-channel")
    );
}

#[test]
fn paymium_named_registration_should_keep_private_and_streams_disabled() {
    let gateway =
        AdapterBackedGateway::with_named_adapters("paymium-test", ["paymium"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);

    let adapter = PaymiumGatewayAdapter::new(PaymiumGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_place_order);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);
    assert!(capabilities.order_book.supports_resync_endpoint);
    assert!(!capabilities.order_book.supports_sequence);
    assert!(!capabilities.order_book.supports_checksum);
    assert_eq!(capabilities.max_order_book_depth, None);
}

#[test]
fn paymium_private_parsers_should_cover_readbacks() {
    let query_order: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/order_success.json"
    ))
    .expect("order fixture");
    let order = parse_private_order_state(&exchange_id(), Some(&symbol_scope()), &query_order)
        .expect("order");
    assert_eq!(
        order.exchange_order_id.as_deref(),
        Some("fixture-order-uuid")
    );
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.side, OrderSide::Buy);
    assert_eq!(order.price.as_deref(), Some("50000.00"));

    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/open_orders_success.json"
    ))
    .expect("open orders fixture");
    let orders = parse_private_open_orders(&exchange_id(), Some(&symbol_scope()), &open_orders)
        .expect("open orders");
    assert_eq!(orders.len(), 1);
    assert_eq!(
        orders[0].exchange_order_id.as_deref(),
        Some("fixture-order-uuid")
    );

    let recent_fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/recent_fills_success.json"
    ))
    .expect("fills fixture");
    let fills = parse_private_recent_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &symbol_scope(),
        &recent_fills,
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("fixture-fill-uuid"));
    assert_eq!(fills[0].fee_amount, Some(0.51));
}

#[tokio::test]
async fn paymium_private_writes_should_remain_offline_request_spec_only() {
    let adapter = PaymiumGatewayAdapter::new(PaymiumGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol_scope(),
        client_order_id: None,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("50000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "paymium.place_order_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn paymium_private_readbacks_should_fail_closed_when_disabled() {
    let adapter = PaymiumGatewayAdapter::new(PaymiumGatewayConfig::default()).expect("adapter");
    let request = QueryOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("query-disabled"),
        symbol: symbol_scope(),
        client_order_id: None,
        exchange_order_id: Some("fixture-order-uuid".to_string()),
    };
    let error = adapter.query_order(request).await.expect_err("disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "paymium.query_order"
        }
    ));
}

#[tokio::test]
async fn paymium_private_readbacks_should_use_signed_get_requests() {
    let query_order: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/order_success.json"
    ))
    .expect("query fixture");
    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/open_orders_success.json"
    ))
    .expect("open fixture");
    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/recent_fills_success.json"
    ))
    .expect("fills fixture");
    let (base_url, seen) = spawn_rest_server(vec![query_order, open_orders, fills]).await;
    let adapter = PaymiumGatewayAdapter::new(PaymiumGatewayConfig {
        rest_base_url: base_url,
        enabled_private_rest: true,
        api_key: Some("fixture-key".to_string()),
        api_secret: Some("fixture-secret".to_string()),
        ..PaymiumGatewayConfig::default()
    })
    .expect("adapter");

    let query = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("fixture-order-uuid".to_string()),
        })
        .await
        .expect("query");
    assert_eq!(
        query.order.and_then(|order| order.exchange_order_id),
        Some("fixture-order-uuid".to_string())
    );

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open");
    assert_eq!(open.orders.len(), 1);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("closed-order-uuid".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(25),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/user/orders/fixture-order-uuid");
    assert_eq!(seen[0].header("api-key"), Some("fixture-key"));
    assert!(seen[0].header("api-nonce").is_some());
    assert!(seen[0].header("api-signature").is_some());
    assert_eq!(seen[1].path, "/user/orders");
    assert_eq!(
        seen[1].query.get("active").map(String::as_str),
        Some("true")
    );
    assert_eq!(seen[2].path, "/user/orders");
    assert_eq!(
        seen[2].query.get("active").map(String::as_str),
        Some("false")
    );
    assert_eq!(
        seen[2].query.get("order_uuid").map(String::as_str),
        Some("closed-order-uuid")
    );
    assert_eq!(seen[2].query.get("limit").map(String::as_str), Some("25"));
}

#[tokio::test]
async fn paymium_public_stream_subscribe_should_stay_runtime_unsupported_by_default() {
    let adapter = PaymiumGatewayAdapter::new(PaymiumGatewayConfig::default()).expect("adapter");
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            tenant_id: None,
            account_id: None,
            run_id: None,
            request_id: Some("paymium-public-stream".to_string()),
            requested_at: chrono::Utc::now(),
        },
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let error = adapter
        .subscribe_public_stream(subscription)
        .await
        .expect_err("socket.io runtime should be disabled");
    assert!(format!("{error:?}").contains("paymium.public_socketio_runtime_unverified"));
}

#[tokio::test]
async fn paymium_public_stream_subscribe_should_return_socketio_descriptor_when_enabled() {
    let adapter = PaymiumGatewayAdapter::new(PaymiumGatewayConfig {
        enabled_public_streams: true,
        ..PaymiumGatewayConfig::default()
    })
    .expect("adapter");
    let descriptor = adapter
        .subscribe_public_stream(public_book_subscription())
        .await
        .expect("public socket descriptor");
    assert!(descriptor.contains("https://paymium.com/public"));
    assert!(descriptor.contains("/ws/socket.io"));
    assert!(descriptor.contains("stream:bids_asks"));
}

#[test]
fn paymium_public_order_book_ws_policy_should_bound_unsequenced_socketio_runtime() {
    let policy = paymium_public_order_book_ws_policy();
    assert_eq!(policy.protocol, "socket.io_v1.3");
    assert_eq!(policy.stream_event, "stream");
    assert_eq!(policy.book_stream, "bids_asks");
    assert_eq!(policy.interval_ms, None);
    assert_eq!(policy.depth, None);
    assert_eq!(policy.sequence, None);
    assert_eq!(policy.checksum, None);
    assert!(policy.resync.contains("/data/eur/depth"));

    let spec = paymium_public_socket_subscription_spec(
        &public_book_subscription(),
        "https://paymium.com/public",
    )
    .expect("subscription spec");
    assert_eq!(spec.path, "/ws/socket.io");
    assert_eq!(spec.connect_packet, "1::");
    assert_eq!(spec.event, "stream");
    assert_eq!(spec.stream, "bids_asks");
    assert!(spec.subscribe_payload.is_none());
    assert!(spec.unsubscribe_payload.is_none());
    assert!(paymium_public_order_book_rebuild_strategy().contains("full REST rebuild"));
}

#[test]
fn paymium_public_book_parser_should_parse_stream_updates_and_zero_deletes() {
    let update: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/ws/public_book_update.json"
    ))
    .expect("ws update fixture");
    let parsed = parse_public_book_update(&exchange_id(), &update).expect("book update");
    assert_eq!(parsed.event, "stream");
    assert_eq!(parsed.sequence, None);
    assert_eq!(parsed.checksum, None);
    assert_eq!(parsed.bids[0].price, "64125.5");
    assert_eq!(parsed.bids[0].quantity, "0.33");
    assert_eq!(parsed.asks[0].price, "64280.0");
    assert_eq!(parsed.deleted_ask_prices(), vec!["64280.0"]);

    let socketio_update: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/paymium/ws/public_book_update_socketio.json"
    ))
    .expect("socket.io update fixture");
    let parsed =
        parse_public_book_update(&exchange_id(), &socketio_update).expect("socket.io book update");
    assert_eq!(parsed.bids[0].price, "64100.0");
    assert_eq!(parsed.deleted_bid_prices(), vec!["64100.0"]);
    assert_eq!(parsed.asks[0].quantity, "0.12");
}

#[test]
fn paymium_public_book_parser_should_reject_non_stream_envelopes() {
    let non_stream = serde_json::json!({
        "event": "ticker",
        "data": {
            "bids": []
        }
    });
    assert!(parse_public_book_update(&exchange_id(), &non_stream).is_err());
}

fn public_book_subscription() -> PublicStreamSubscription {
    PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            tenant_id: None,
            account_id: None,
            run_id: None,
            request_id: Some("paymium-public-stream".to_string()),
            requested_at: chrono::Utc::now(),
        },
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    }
}

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
}

impl SeenRequest {
    fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .get(&key.to_ascii_lowercase())
            .map(String::as_str)
    }
}

async fn spawn_rest_server(responses: Vec<Value>) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_requests = Arc::clone(&seen);
    let responses = Arc::new(Mutex::new(responses.into_iter()));

    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut buffer = vec![0_u8; 8192];
            let bytes_read = stream.read(&mut buffer).await.unwrap();
            let request_text = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
            seen_requests
                .lock()
                .unwrap()
                .push(parse_seen_request(&request_text));
            let body = responses
                .lock()
                .unwrap()
                .next()
                .unwrap_or_else(|| json!({}));
            let body_text = body.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body_text.len(),
                body_text
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    (format!("http://{address}"), seen)
}

fn parse_seen_request(request_text: &str) -> SeenRequest {
    let request_line = request_text.lines().next().unwrap_or_default();
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default().to_string();
    let target = request_parts.next().unwrap_or_default();
    let (path, query_text) = target.split_once('?').unwrap_or((target, ""));
    let query = query_text
        .split('&')
        .filter(|part| !part.is_empty())
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            Some((key.to_string(), value.to_string()))
        })
        .collect();
    let headers = request_text
        .lines()
        .skip(1)
        .take_while(|line| !line.trim().is_empty())
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    SeenRequest {
        method,
        path: path.to_string(),
        query,
        headers,
    }
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

fn exchange_id() -> ExchangeId {
    ExchangeId::new("paymium").expect("paymium exchange")
}

fn symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "EUR").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTC/EUR")
            .expect("symbol"),
    }
}

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelOrderRequest, ExchangeApiError,
    ExchangeClient, OpenOrdersRequest, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{
    novadax_canonical_pair, novadax_symbol, parse_order_ack_id, parse_order_book_shape,
    parse_symbol_rules,
};
use super::private::{
    cancel_by_symbol_request_spec_fixture, cancel_order_request_spec_fixture,
    create_order_request_spec_fixture, fills_query, novadax_place_order_body, open_orders_query,
};
use super::private_parser::{parse_balance_assets, parse_fill_ids, parse_open_order_ids};
use super::signing::{
    hmac_sha256_hex, novadax_private_request_headers, novadax_signature_payload,
    sorted_query_string,
};
use super::streams::{
    novadax_public_channel, novadax_public_subscribe_payload, novadax_reconnect_policy_ms,
};
use super::{NovadaxGatewayAdapter, NovadaxGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("novadax").expect("exchange")
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

fn symbol(symbol: &str) -> SymbolScope {
    let (base, quote) = novadax_canonical_pair(symbol).expect("canonical pair");
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, symbol)
            .expect("symbol"),
    }
}

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    body: Option<Value>,
    headers: BTreeMap<String, String>,
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
                .unwrap_or_else(|| serde_json::json!({ "code": "A10000", "data": [] }))
                .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    (format!("http://{address}"), seen)
}

fn parse_seen_request(request_text: &str) -> SeenRequest {
    let request_line = request_text.lines().next().unwrap_or_default();
    let target = request_line.split_whitespace().nth(1).unwrap_or_default();
    let path = target
        .split_once('?')
        .map(|(path, _)| path)
        .unwrap_or(target);
    let headers = request_text
        .lines()
        .skip(1)
        .take_while(|line| !line.trim().is_empty())
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    let body = request_text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.trim())
        .filter(|body| !body.is_empty())
        .and_then(|body| serde_json::from_str(body).ok());
    SeenRequest {
        method: request_line
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string(),
        path: path.to_string(),
        body,
        headers,
    }
}

#[test]
fn symbol_normalization_should_cover_brl_fiat_markets() {
    assert_eq!(novadax_symbol("btc/brl"), "BTC_BRL");
    assert_eq!(novadax_symbol("ETH-BRL"), "ETH_BRL");
    assert_eq!(
        novadax_canonical_pair("BTCBRL").expect("brl"),
        ("BTC".to_string(), "BRL".to_string())
    );
    assert_eq!(
        novadax_canonical_pair("BTC_BRL").expect("brl"),
        ("BTC".to_string(), "BRL".to_string())
    );
}

#[test]
fn request_specs_and_signing_vectors_should_stay_secret_free() {
    let spec = create_order_request_spec_fixture();
    assert_eq!(spec["path"], "/v1/orders/create");
    assert_eq!(spec["headers"]["X-Nova-Access-Key"], "<redacted>");
    assert_eq!(spec["body"]["symbol"], "BTC_BRL");

    let cancel = cancel_order_request_spec_fixture();
    assert_eq!(cancel["path"], "/v1/orders/cancel");
    assert_eq!(cancel["body"]["id"], "order-1");

    let cancel_by_symbol = cancel_by_symbol_request_spec_fixture("btc/brl");
    assert_eq!(cancel_by_symbol["path"], "/v1/orders/cancel-by-symbol");
    assert_eq!(cancel_by_symbol["body"]["symbol"], "BTC_BRL");

    let open_orders = open_orders_query("btc/brl");
    assert_eq!(open_orders["symbol"], "BTC_BRL");
    assert_eq!(open_orders["limit"], "100");

    let fills = fills_query("BTC-BRL");
    assert_eq!(fills["symbol"], "BTC_BRL");
    assert_eq!(fills["page"], "1");
}

#[test]
fn signing_payload_should_match_official_shape_and_local_vector() {
    let query = BTreeMap::from([
        ("name".to_string(), "joao".to_string()),
        ("cpf".to_string(), "123456".to_string()),
        ("birthday".to_string(), "2017-08-01".to_string()),
    ]);
    assert_eq!(
        sorted_query_string(&query),
        "birthday=2017-08-01&cpf=123456&name=joao"
    );
    let payload = novadax_signature_payload("GET", "/v1/orders/get", &query, None, "1564988445199");
    assert_eq!(
        payload,
        "GET\n/v1/orders/get\nbirthday=2017-08-01&cpf=123456&name=joao\n1564988445199"
    );
    assert_eq!(
        hmac_sha256_hex("fixture-secret", &payload).expect("signature"),
        "955210e0c681ecec338b21c6547d8235a5c679197cb2a3d38fceda1b44600e27"
    );

    let body = "{\"symbol\":\"BTC_BRL\",\"side\":\"BUY\",\"type\":\"LIMIT\",\"amount\":\"0.01\",\"price\":\"350000\",\"clientOrderId\":\"offline-fixture\"}";
    let post_payload = novadax_signature_payload(
        "POST",
        "/v1/orders/create",
        &BTreeMap::new(),
        Some(body),
        "1564988445199",
    );
    assert_eq!(
        post_payload,
        "POST\n/v1/orders/create\ndbbec56af776df47a9dc5635ffed9393\n1564988445199"
    );
    let headers = novadax_private_request_headers(
        "fixture-key",
        "fixture-secret",
        "1564988445199",
        "POST",
        "/v1/orders/create",
        &BTreeMap::new(),
        Some(body),
    )
    .expect("headers");
    assert_eq!(headers["X-Nova-Access-Key"], "fixture-key");
    assert_eq!(
        headers["X-Nova-Signature"],
        "ca4e0e757283e5c9e719492ef94aca0b925bcd567976c0b2795dea48b120fb01"
    );
}

#[test]
fn place_order_body_should_use_amount_or_market_buy_value() {
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTC_BRL"),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("350000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let body = novadax_place_order_body(&request).expect("body");
    assert_eq!(body["symbol"], "BTC_BRL");
    assert_eq!(body["side"], "BUY");
    assert_eq!(body["type"], "LIMIT");
    assert_eq!(body["amount"], "0.01");
    assert_eq!(body["price"], "350000");
    assert_eq!(body["clientOrderId"], "offline-fixture");

    let mut market = request;
    market.order_type = OrderType::Market;
    market.price = None;
    market.quote_quantity = Some("1000".to_string());
    let body = novadax_place_order_body(&market).expect("market");
    assert_eq!(body["value"], "1000");
    assert!(body.get("price").is_none());
}

#[test]
fn fixtures_should_parse_orderbook_order_ack_and_boundary() {
    let order_book: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/novadax/orderbook_btc_brl.json"
    ))
    .expect("orderbook");
    assert_eq!(parse_order_book_shape(&order_book).expect("shape"), (1, 1));

    let order_ack: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/novadax/order_ack.json"
    ))
    .expect("ack");
    assert_eq!(
        parse_order_ack_id(&order_ack).expect("oid"),
        "novadax-order-1"
    );

    let boundary: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/novadax/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["fiat_operations"], "unsupported");
    assert_eq!(
        boundary["private_write_mode"],
        "credential_gated_runtime_cancel_all_offline"
    );

    let symbols: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/novadax/symbols_success.json"
    ))
    .expect("symbols");
    let rules = parse_symbol_rules(&exchange_id(), &[], &symbols).expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].quote_asset, "BRL");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.01"));
}

#[test]
fn private_read_fixtures_should_cover_balances_open_orders_and_fills() {
    let balances: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/novadax/balances.json"
    ))
    .expect("balances");
    assert_eq!(
        parse_balance_assets(&balances).expect("assets"),
        vec!["BTC", "BRL"]
    );

    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/novadax/open_orders.json"
    ))
    .expect("open orders");
    assert_eq!(
        parse_open_order_ids(&open_orders).expect("orders"),
        vec!["novadax-order-1"]
    );

    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/novadax/fills.json"
    ))
    .expect("fills");
    assert_eq!(parse_fill_ids(&fills).expect("fills"), vec!["fill-1"]);
}

#[test]
fn websocket_helpers_should_map_socket_io_public_channels() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol("BTC_BRL"),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = novadax_public_subscribe_payload(&subscription);
    assert_eq!(payload["type"], "subscribe");
    assert_eq!(payload["symbol"], "BTC_BRL");
    assert_eq!(payload["channel"], "MARKET.BTC_BRL.DEPTH");
    assert_eq!(payload["transport"], "socket_io");
    assert_eq!(
        novadax_public_channel(&subscription),
        "MARKET.BTC_BRL.DEPTH"
    );
    assert_eq!(novadax_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[tokio::test]
async fn private_runtime_should_require_explicit_credentials_and_enablement() {
    let adapter = NovadaxGatewayAdapter::new(NovadaxGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_place_order);
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTC_BRL"),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("350000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "novadax.place_order_private_rest_not_enabled"
        }
    ));
}

#[tokio::test]
async fn single_lifecycle_runtime_should_send_signed_requests_and_parse_mock_readbacks() {
    let (base_url, seen) = spawn_rest_server(vec![
        serde_json::json!({
            "code": "A10000",
            "data": {
                "id": "novadax-order-1",
                "clientOrderId": "offline-fixture",
                "symbol": "BTC_BRL",
                "side": "BUY",
                "type": "LIMIT",
                "status": "SUBMITTED"
            }
        }),
        serde_json::json!({
            "code": "A10000",
            "data": {
                "id": "novadax-order-1",
                "symbol": "BTC_BRL",
                "side": "BUY",
                "type": "LIMIT",
                "status": "CANCELED"
            }
        }),
        serde_json::json!({
            "code": "A10000",
            "data": {
                "id": "novadax-order-1",
                "clientOrderId": "offline-fixture",
                "symbol": "BTC_BRL",
                "side": "BUY",
                "type": "LIMIT",
                "price": "350000",
                "amount": "0.01",
                "filledAmount": "0",
                "status": "SUBMITTED"
            }
        }),
        serde_json::json!({
            "code": "A10000",
            "data": {
                "items": [{
                    "id": "novadax-order-1",
                    "clientOrderId": "offline-fixture",
                    "symbol": "BTC_BRL",
                    "side": "BUY",
                    "type": "LIMIT",
                    "price": "350000",
                    "amount": "0.01",
                    "filledAmount": "0",
                    "status": "SUBMITTED"
                }]
            }
        }),
        serde_json::json!({
            "code": "A10000",
            "data": {
                "items": [{
                    "id": "fill-1",
                    "orderId": "novadax-order-1",
                    "symbol": "BTC_BRL",
                    "side": "BUY",
                    "price": "350000",
                    "amount": "0.01",
                    "fee": "1.00",
                    "feeCurrency": "BRL"
                }]
            }
        }),
    ])
    .await;
    let adapter = NovadaxGatewayAdapter::new(NovadaxGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("fixture-key".to_string()),
        api_secret: Some("fixture-secret".to_string()),
        enabled_private_rest: true,
        ..NovadaxGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_place_order);
    assert!(capabilities.supports_cancel_order);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol("BTC_BRL"),
            client_order_id: Some("offline-fixture".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "0.01".to_string(),
            price: Some("350000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");
    assert_eq!(
        placed.order.exchange_order_id.as_deref(),
        Some("novadax-order-1")
    );

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol("BTC_BRL"),
            client_order_id: None,
            exchange_order_id: Some("novadax-order-1".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);

    let queried = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol("BTC_BRL"),
            client_order_id: None,
            exchange_order_id: Some("novadax-order-1".to_string()),
        })
        .await
        .expect("query");
    assert!(queried.order.is_some());

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol("BTC_BRL")),
            page: Some(rustcta_exchange_api::PageRequest::first_page(25)),
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol("BTC_BRL")),
            client_order_id: None,
            exchange_order_id: Some("novadax-order-1".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(25),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("fill-1"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/v1/orders/create");
    assert_eq!(requests[1].path, "/v1/orders/cancel");
    assert_eq!(requests[2].method, "GET");
    assert_eq!(requests[2].path, "/v1/orders/get");
    assert_eq!(requests[3].path, "/v1/orders/list");
    assert_eq!(requests[4].path, "/v1/orders/fills");
    for request in requests {
        assert_eq!(request.header("x-nova-access-key"), Some("fixture-key"));
        assert!(request.header("x-nova-signature").is_some());
    }
}

#[tokio::test]
async fn batch_runtime_should_send_signed_requests_and_parse_mock_acks() {
    let (base_url, seen) = spawn_rest_server(vec![
        serde_json::json!({
            "code": "A10000",
            "data": [{ "id": "novadax-order-1", "clientOrderId": "offline-fixture-1" }]
        }),
        serde_json::json!({
            "code": "A10000",
            "data": [{ "id": "novadax-order-1" }]
        }),
    ])
    .await;
    let adapter = NovadaxGatewayAdapter::new(NovadaxGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("fixture-key".to_string()),
        api_secret: Some("fixture-secret".to_string()),
        enabled_private_rest: true,
        ..NovadaxGatewayConfig::default()
    })
    .expect("adapter");
    assert!(adapter.capabilities().supports_batch_place_order);
    assert!(adapter.capabilities().supports_batch_cancel_order);

    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![PlaceOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("place"),
                symbol: symbol("BTC_BRL"),
                client_order_id: Some("offline-fixture-1".to_string()),
                side: OrderSide::Buy,
                position_side: None,
                order_type: OrderType::Limit,
                time_in_force: None,
                quantity: "0.01".to_string(),
                price: Some("350000".to_string()),
                quote_quantity: None,
                reduce_only: false,
                post_only: false,
            }],
        })
        .await
        .expect("batch place");
    assert_eq!(
        placed.orders[0].exchange_order_id.as_deref(),
        Some("novadax-order-1")
    );

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel"),
                symbol: symbol("BTC_BRL"),
                client_order_id: None,
                exchange_order_id: Some("novadax-order-1".to_string()),
            }],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 1);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/v1/orders/batch-create");
    assert_eq!(requests[0].header("x-nova-access-key"), Some("fixture-key"));
    assert!(requests[0].header("x-nova-signature").is_some());
    assert_eq!(
        requests[0].body.as_ref().expect("body")["orders"][0]["symbol"],
        "BTC_BRL"
    );
    assert_eq!(requests[1].path, "/v1/orders/batch-cancel");
    assert_eq!(
        requests[1].body.as_ref().expect("body")["ids"][0],
        "novadax-order-1"
    );
}

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    BatchExecutionMode, CancelOrderRequest, ExchangeClient, OpenOrdersRequest, PlaceOrderRequest,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId, TimeInForce,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private::bitfinex_place_order_body;
use super::signing::{bitfinex_rest_signature, bitfinex_ws_auth_signature};
use super::streams::{
    bitfinex_private_auth_payload, bitfinex_public_conf_payload, bitfinex_public_orderbook_policy,
    bitfinex_public_subscribe_payload, bitfinex_sequence_gap,
    parse_bitfinex_private_stream_message, parse_bitfinex_public_book_message,
    parse_bitfinex_public_stream_message, BitfinexBookSide, BitfinexBookUpdateAction,
    BitfinexPrivateStreamMessage, BitfinexPublicBookMessage, BITFINEX_WS_OB_CHECKSUM_FLAG,
    BITFINEX_WS_SEQ_ALL_FLAG,
};
use super::{BitfinexGatewayAdapter, BitfinexGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
    body: Option<Value>,
}

#[test]
fn bitfinex_public_parsers_should_parse_symbols_and_book() {
    let exchange = exchange_id();
    let symbols: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitfinex/symbols_exchange.json"
    ))
    .expect("symbols fixture");
    let rules = parse_symbol_rules(&exchange, MarketType::Spot, &symbols).expect("rules");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "tBTCUSD");
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "USD");

    let book: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitfinex/orderbook.json"
    ))
    .expect("book fixture");
    let snapshot =
        parse_orderbook_snapshot(&exchange, spot_symbol_scope(), &book).expect("book snapshot");
    assert_eq!(snapshot.bids[0].price, 65000.0);
    assert_eq!(snapshot.asks[0].quantity, 0.25);
}

#[tokio::test]
async fn bitfinex_adapter_should_send_signed_place_cancel_and_open_orders() {
    let order = bitfinex_order_array("1185815100", "1575289350475", "tBTCUSD", "ACTIVE");
    let cancelled = bitfinex_order_array("1185815100", "1575289350475", "tBTCUSD", "CANCELED");
    let (base_url, seen) = spawn_rest_server(vec![
        json!([
            1700000000000_i64,
            "on-req",
            null,
            null,
            order.clone(),
            null,
            "SUCCESS",
            "Submitted"
        ]),
        json!([
            1700000000001_i64,
            "oc-req",
            null,
            null,
            cancelled,
            null,
            "SUCCESS",
            "Submitted"
        ]),
        json!([order]),
    ])
    .await;
    let adapter = BitfinexGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("1575289350475".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.01".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: true,
        })
        .await
        .expect("place order");
    assert_eq!(
        placed.order.exchange_order_id.as_deref(),
        Some("1185815100")
    );
    assert_eq!(placed.order.status, OrderStatus::Open);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1185815100".to_string()),
        })
        .await
        .expect("cancel order");
    assert!(cancelled.cancelled);

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "/v2/auth/w/order/submit");
    assert_eq!(
        requests[0].body.as_ref().and_then(|body| body.get("type")),
        Some(&json!("EXCHANGE LIMIT"))
    );
    assert_eq!(
        requests[0].body.as_ref().and_then(|body| body.get("flags")),
        Some(&json!(4096))
    );
    let spec: RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitfinex/request_specs/place_order.json"
    ))
    .expect("request spec");
    spec.assert_matches(&actual_request(&requests[0]))
        .expect("place request spec");
    assert_signed_request(&requests[1], "/v2/auth/w/order/cancel");
    assert_signed_request(&requests[2], "/v2/auth/r/orders/tBTCUSD");
}

#[test]
fn bitfinex_request_body_and_signing_fixtures_should_match() {
    let body = bitfinex_place_order_body(&PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("body"),
        symbol: spot_symbol_scope(),
        client_order_id: Some("1575289350475".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    })
    .expect("body");
    assert_eq!(body["symbol"], "tBTCUSD");
    assert_eq!(body["amount"], "0.01");

    let vector: Value =
        serde_json::from_str(include_str!("../../../../../tests/fixtures/exchanges/bitfinex/signing_vectors/rest_place_order_hmac_sha384.json"))
            .expect("signing vector");
    assert_eq!(
        bitfinex_rest_signature(
            vector["secret"].as_str().unwrap(),
            vector["path"].as_str().unwrap(),
            vector["nonce"].as_str().unwrap(),
            vector["body"].as_str().unwrap(),
        ),
        vector["expected_signature"].as_str().unwrap()
    );
}

#[test]
fn bitfinex_ws_payloads_and_parsers_should_cover_auth_book_and_private_order() {
    let public = bitfinex_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    })
    .expect("public payload");
    assert_eq!(public["event"], "subscribe");
    assert_eq!(public["channel"], "book");
    assert_eq!(public["symbol"], "tBTCUSD");
    assert_eq!(public["prec"], "P0");
    assert_eq!(public["freq"], "F0");
    assert_eq!(public["len"], 100);

    let auth = bitfinex_private_auth_payload("key", "bitfinex-test-secret", "1700000000000000");
    assert_eq!(auth["event"], "auth");
    assert_eq!(
        auth["authSig"],
        bitfinex_ws_auth_signature("bitfinex-test-secret", "1700000000000000")
    );

    let public_event = parse_bitfinex_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!([123, [[65000.0, 2, 0.5], [65001.0, 1, -0.25]]]),
    )
    .expect("public event");
    assert!(matches!(
        public_event,
        Some(rustcta_exchange_api::ExchangeStreamEvent::OrderBookSnapshot { .. })
    ));

    let private_event = parse_bitfinex_private_stream_message(
        &exchange_id(),
        TenantId::new("tenant").unwrap(),
        AccountId::new("account").unwrap(),
        MarketType::Spot,
        &json!([
            0,
            "on",
            bitfinex_order_array("1185815100", "1575289350475", "tBTCUSD", "ACTIVE")
        ]),
    )
    .expect("private order event");
    match private_event {
        BitfinexPrivateStreamMessage::Order(order) => {
            assert_eq!(order.exchange_order_id.as_deref(), Some("1185815100"));
        }
        _ => panic!("unexpected private stream event"),
    }

    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").unwrap(),
        kind: PrivateStreamKind::Orders,
    };
    let adapter = BitfinexGatewayAdapter::new(private_config("http://127.0.0.1:1".to_string()))
        .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_streams);
    assert_eq!(subscription.exchange, exchange_id());
}

#[test]
fn bitfinex_public_book_ws_structured_details_should_cover_flags_and_updates() {
    let policy = bitfinex_public_orderbook_policy();
    assert_eq!(policy.channel, "book");
    assert!(policy.supported_precisions.contains(&"P4"));
    assert!(policy.supported_frequencies.contains(&"F1"));
    assert!(policy.supported_lengths.contains(&250));
    assert_eq!(policy.sequence_flag, BITFINEX_WS_SEQ_ALL_FLAG);
    assert_eq!(policy.checksum_flag, BITFINEX_WS_OB_CHECKSUM_FLAG);

    let conf = bitfinex_public_conf_payload(true, true, false);
    assert_eq!(
        conf,
        json!({
            "event": "conf",
            "flags": BITFINEX_WS_OB_CHECKSUM_FLAG + BITFINEX_WS_SEQ_ALL_FLAG,
        })
    );

    let subscribe: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitfinex/ws_public_book_subscribe.json"
    ))
    .expect("subscribe fixture");
    assert_eq!(subscribe["channel"], "book");
    assert_eq!(subscribe["prec"], "P0");
    assert_eq!(subscribe["freq"], "F0");
    assert_eq!(subscribe["len"], 100);

    let snapshot: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitfinex/ws_public_book_seq_snapshot.json"
    ))
    .expect("snapshot fixture");
    match parse_bitfinex_public_book_message(&exchange_id(), spot_symbol_scope(), &snapshot)
        .expect("snapshot message")
    {
        BitfinexPublicBookMessage::Snapshot { book, sequence } => {
            assert_eq!(sequence, Some(41));
            assert_eq!(book.sequence, Some(41));
            assert_eq!(book.bids[0].price, 65000.0);
            assert_eq!(book.asks[0].price, 65001.0);
        }
        other => panic!("unexpected Bitfinex book snapshot message: {other:?}"),
    }

    let update: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitfinex/ws_public_book_update.json"
    ))
    .expect("update fixture");
    match parse_bitfinex_public_book_message(&exchange_id(), spot_symbol_scope(), &update)
        .expect("update message")
    {
        BitfinexPublicBookMessage::Update { update, sequence } => {
            assert_eq!(sequence, Some(42));
            assert_eq!(update.side, BitfinexBookSide::Bid);
            assert_eq!(update.action, BitfinexBookUpdateAction::Delete);
            assert_eq!(update.quantity, 0.0);
            assert_eq!(update.price, 65000.0);
        }
        other => panic!("unexpected Bitfinex book update message: {other:?}"),
    }
    assert_eq!(bitfinex_sequence_gap(Some(41), Some(42)), None);
    assert_eq!(bitfinex_sequence_gap(Some(40), Some(42)), Some((41, 42)));

    let checksum: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitfinex/ws_public_book_checksum.json"
    ))
    .expect("checksum fixture");
    match parse_bitfinex_public_book_message(&exchange_id(), spot_symbol_scope(), &checksum)
        .expect("checksum message")
    {
        BitfinexPublicBookMessage::Checksum { checksum, sequence } => {
            assert_eq!(checksum, -123456789);
            assert_eq!(sequence, Some(43));
        }
        other => panic!("unexpected Bitfinex checksum message: {other:?}"),
    }
}

#[test]
fn bitfinex_capabilities_v2_should_cover_perpetual_and_batch_cancel_runtime() {
    let adapter = BitfinexGatewayAdapter::new(private_config("http://127.0.0.1:1".to_string()))
        .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(capabilities.market_types.contains(&MarketType::Perpetual));
    assert!(capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities
        .capabilities_v2
        .cancel_all_orders
        .is_supported());
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.mode,
        BatchExecutionMode::Native
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_positions"
            && endpoint.market_types.contains(&MarketType::Perpetual)));
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bitfinex").expect("exchange")
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

fn spot_symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "tBTCUSD")
            .expect("symbol"),
    }
}

fn private_config(base_url: String) -> BitfinexGatewayConfig {
    BitfinexGatewayConfig {
        public_rest_base_url: base_url.clone(),
        private_rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        enabled_private_streams: true,
        ..BitfinexGatewayConfig::default()
    }
}

fn bitfinex_order_array(id: &str, cid: &str, symbol: &str, status: &str) -> Value {
    json!([
        id,
        null,
        cid,
        symbol,
        1575289351944_i64,
        1575289447644_i64,
        0.01,
        0.01,
        "EXCHANGE LIMIT",
        null,
        null,
        null,
        4096,
        status,
        null,
        null,
        65000,
        0,
        0,
        0,
        null,
        null,
        null,
        0,
        0,
        null,
        null,
        null,
        "API>BFX",
        null,
        null,
        null
    ])
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
                .unwrap_or_else(|| json!([]));
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
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (key.to_string(), value.to_string())
        })
        .collect();
    let headers = request_text
        .lines()
        .skip(1)
        .take_while(|line| !line.trim().is_empty())
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    let body = request_text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.trim())
        .filter(|body| !body.is_empty())
        .and_then(|body| serde_json::from_str(body).ok());
    SeenRequest {
        method,
        path: path.to_string(),
        query,
        headers,
        body,
    }
}

fn assert_signed_request(request: &SeenRequest, path: &str) {
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, path);
    assert_eq!(
        request.headers.get("bfx-apikey").map(String::as_str),
        Some("key")
    );
    assert!(request
        .headers
        .get("bfx-nonce")
        .is_some_and(|value| !value.is_empty()));
    assert!(request
        .headers
        .get("bfx-signature")
        .is_some_and(|value| !value.is_empty()));
}

fn actual_request(request: &SeenRequest) -> ActualHttpRequest {
    ActualHttpRequest::new(request.method.clone(), request.path.clone())
        .with_query(
            request
                .query
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<BTreeMap<_, _>>(),
        )
        .with_headers(
            request
                .headers
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<BTreeMap<_, _>>(),
        )
        .with_body(request.body.clone())
}

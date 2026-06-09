use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    AccountId, BalancesRequest, BatchAtomicity, BatchCancelOrdersRequest, BatchExecutionMode,
    CancelOrderRequest, ExchangeClient, OpenOrdersRequest, PublicStreamKind,
    PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderStatus};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::{public_order_book_ws_policy, public_subscription_spec};
use super::{BitbankGatewayAdapter, BitbankGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bitbank").expect("exchange")
}

fn context(request_id: &str) -> rustcta_exchange_api::RequestContext {
    rustcta_exchange_api::RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: None,
        account_id: None,
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn context_with_account(request_id: &str) -> rustcta_exchange_api::RequestContext {
    rustcta_exchange_api::RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("account").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn symbol_scope() -> rustcta_exchange_api::SymbolScope {
    rustcta_exchange_api::SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "JPY").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "btc_jpy")
            .expect("symbol"),
    }
}

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    query: HashMap<String, String>,
    body: Option<Value>,
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
                .unwrap_or_else(|| json!({ "success": 1, "data": {} }));
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
        body,
        headers,
    }
}

#[test]
fn bitbank_parser_fixtures_should_cover_pairs_and_depth() {
    let pairs: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitbank/pairs_success.json"
    ))
    .expect("pairs");
    let rules = parse_symbol_rules(&exchange_id(), &pairs).expect("rules");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "btc_jpy");
    assert_eq!(rules[0].price_increment.as_deref(), Some("1"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.0001"));

    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitbank/depth_success.json"
    ))
    .expect("depth");
    let snapshot = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), &depth).expect("book");
    assert_eq!(snapshot.sequence, Some(123456));
    assert_eq!(snapshot.best_bid().unwrap().price, 2999000.0);
    assert_eq!(snapshot.best_ask().unwrap().price, 3001000.0);
}

#[tokio::test]
async fn bitbank_adapter_capabilities_should_be_spot_only_with_explicit_write_boundary() {
    let adapter = BitbankGatewayAdapter::new(BitbankGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.atomicity,
        BatchAtomicity::NonAtomic
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_cancel_orders
            .same_symbol_required
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "bitbank.batch_cancel_orders"));
    assert_eq!(capabilities.order_book.max_depth, Some(200));
    assert!(capabilities.order_book.supports_sequence);
    assert!(matches!(
        capabilities.order_book.strictness,
        rustcta_exchange_api::OrderBookStrictness::BestEffortDelta
    ));

    let err = adapter
        .get_positions(rustcta_exchange_api::PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbols: Vec::new(),
        })
        .await
        .expect_err("positions unsupported");
    assert!(format!("{err:?}").contains("bitbank.positions"));
}

#[tokio::test]
async fn bitbank_batch_cancel_should_sign_same_pair_request_when_enabled() {
    let response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitbank/batch_cancel_orders_success.json"
    ))
    .expect("batch cancel response fixture");
    let (base_url, seen) = spawn_rest_server(vec![response]).await;
    let adapter = BitbankGatewayAdapter::new(BitbankGatewayConfig {
        public_rest_base_url: base_url.clone(),
        private_rest_base_url: base_url,
        api_key: Some("bitbank-key".to_string()),
        api_secret: Some("bitbank-secret".to_string()),
        enabled_private_rest: true,
        ..Default::default()
    })
    .expect("adapter");
    assert!(adapter.capabilities().supports_batch_cancel_order);

    let response = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-1"),
                    symbol: symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("1".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-2"),
                    symbol: symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("2".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(response.cancelled_count, 2);
    assert_eq!(response.orders[0].status, OrderStatus::Cancelled);
    assert_eq!(response.orders[1].exchange_order_id.as_deref(), Some("2"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/v1/user/spot/cancel_orders");
    assert_eq!(requests[0].body.as_ref().unwrap()["pair"], "btc_jpy");
    assert_eq!(
        requests[0].body.as_ref().unwrap()["order_ids"],
        json!([1, 2])
    );
    assert_eq!(requests[0].header("access-key"), Some("bitbank-key"));
    assert!(requests[0].header("access-nonce").is_some());
    assert!(requests[0].header("access-signature").is_some());
}

#[tokio::test]
async fn bitbank_readbacks_should_sign_get_requests_when_enabled() {
    let query_response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitbank/order_ack_success.json"
    ))
    .expect("query response fixture");
    let open_response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitbank/open_orders_success.json"
    ))
    .expect("open orders response fixture");
    let fills_response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitbank/fills_success.json"
    ))
    .expect("fills response fixture");
    let (base_url, seen) =
        spawn_rest_server(vec![query_response, open_response, fills_response]).await;
    let adapter = BitbankGatewayAdapter::new(BitbankGatewayConfig {
        public_rest_base_url: base_url.clone(),
        private_rest_base_url: base_url,
        api_key: Some("bitbank-key".to_string()),
        api_secret: Some("bitbank-secret".to_string()),
        enabled_private_rest: true,
        ..Default::default()
    })
    .expect("adapter");
    assert!(adapter.capabilities().supports_query_order);
    assert!(adapter.capabilities().supports_open_orders);
    assert!(adapter.capabilities().supports_recent_fills);

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-order"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("123456".to_string()),
        })
        .await
        .expect("query order")
        .order
        .expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("123456"));
    assert_eq!(order.status, OrderStatus::Open);

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);
    assert_eq!(open.orders[0].status, OrderStatus::PartiallyFilled);
    assert!(open.orders[0].post_only);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context_with_account("recent-fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("123457".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect("recent fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("987654"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("123457"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/v1/user/spot/order");
    assert_eq!(
        requests[0].query.get("pair").map(String::as_str),
        Some("btc_jpy")
    );
    assert_eq!(
        requests[0].query.get("order_id").map(String::as_str),
        Some("123456")
    );
    assert_eq!(requests[1].path, "/v1/user/spot/active_orders");
    assert_eq!(
        requests[1].query.get("pair").map(String::as_str),
        Some("btc_jpy")
    );
    assert_eq!(requests[2].path, "/v1/user/spot/trade_history");
    assert_eq!(
        requests[2].query.get("order_id").map(String::as_str),
        Some("123457")
    );
    assert_eq!(
        requests[2].query.get("count").map(String::as_str),
        Some("100")
    );
    for request in &requests {
        assert_eq!(request.header("access-key"), Some("bitbank-key"));
        assert!(request.header("access-nonce").is_some());
        assert!(request.header("access-signature").is_some());
    }
}

#[tokio::test]
async fn bitbank_balances_should_sign_assets_request_and_parse_assets() {
    let assets_response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitbank/assets_success.json"
    ))
    .expect("assets response fixture");
    let (base_url, seen) = spawn_rest_server(vec![assets_response]).await;
    let adapter = BitbankGatewayAdapter::new(BitbankGatewayConfig {
        public_rest_base_url: base_url.clone(),
        private_rest_base_url: base_url,
        api_key: Some("bitbank-key".to_string()),
        api_secret: Some("bitbank-secret".to_string()),
        enabled_private_rest: true,
        ..Default::default()
    })
    .expect("adapter");
    assert!(adapter.capabilities().supports_balances);

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context_with_account("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["btc".to_string()],
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances.len(), 1);
    let assets = &balances.balances[0].balances;
    assert_eq!(assets.len(), 1);
    assert_eq!(assets[0].asset, "BTC");
    assert_eq!(assets[0].total, 0.6);
    assert_eq!(assets[0].available, 0.5);
    assert_eq!(assets[0].locked, 0.1);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/v1/user/assets");
    assert_eq!(requests[0].header("access-key"), Some("bitbank-key"));
    assert!(requests[0].header("access-nonce").is_some());
    assert!(requests[0].header("access-signature").is_some());
}

#[test]
fn bitbank_public_ws_subscription_should_use_socket_io_rooms() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let spec = public_subscription_spec(&subscription, "wss://stream.bitbank.cc/socket.io/")
        .expect("subscription");
    assert_eq!(spec.room, "depth_diff_btc_jpy");
    assert_eq!(spec.subscribe_payload[0], "join-room");
}

#[test]
fn bitbank_public_ws_policy_should_cover_official_orderbook_rebuild_rules() {
    let policy = public_order_book_ws_policy();
    assert_eq!(policy.snapshot_room_template, "depth_whole_{pair}");
    assert_eq!(policy.delta_room_template, "depth_diff_{pair}");
    assert_eq!(policy.normal_depth_per_side, 200);
    assert_eq!(policy.fixed_update_interval_ms, None);
    assert_eq!(policy.snapshot_sequence_field, "sequenceId");
    assert_eq!(policy.delta_sequence_field, "s");
    assert!(!policy.sequence_is_contiguous);
    assert!(policy
        .rebuild_strategy
        .contains("s > depth_whole.sequenceId"));
}

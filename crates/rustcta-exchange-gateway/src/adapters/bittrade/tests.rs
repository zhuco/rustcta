use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, PageRequest, PublicStreamKind,
    PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderStatus, TenantId,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{
    parse_bittrade_open_orders, parse_bittrade_order_state, parse_bittrade_recent_fills,
    parse_orderbook_response, parse_symbol_rules,
};
use super::private::{open_orders_params, recent_fills_params};
use super::streams::{public_pong_payload, public_subscription_spec};
use super::{BittradeGatewayAdapter, BittradeGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bittrade").expect("exchange")
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

fn symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "JPY").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "btcjpy")
            .expect("symbol"),
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

#[test]
fn bittrade_parser_fixtures_should_cover_symbols_and_depth() {
    let symbols: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/symbols_success.json"
    ))
    .expect("symbols");
    let rules = parse_symbol_rules(&exchange_id(), &symbols).expect("rules");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "btcjpy");
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "JPY");
    assert_eq!(rules[0].price_increment.as_deref(), Some("1"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.0001"));
    assert_eq!(rules[0].min_quantity.as_deref(), Some("0.001"));
    assert_eq!(rules[0].min_notional.as_deref(), Some("2"));

    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/depth_success.json"
    ))
    .expect("depth");
    let snapshot =
        parse_orderbook_response(&exchange_id(), symbol_scope(), Some(1), &depth).expect("book");
    assert_eq!(snapshot.sequence, Some(100883718161));
    assert_eq!(snapshot.best_bid().unwrap().price, 4_860_380.0);
    assert_eq!(snapshot.best_ask().unwrap().price, 4_860_850.0);
    assert_eq!(snapshot.bids.len(), 1);
}

#[test]
fn bittrade_private_readback_fixtures_should_parse() {
    let order: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/order_success.json"
    ))
    .expect("order");
    let parsed =
        parse_bittrade_order_state(&exchange_id(), Some(&symbol_scope()), &order).expect("order");
    assert_eq!(parsed.exchange_order_id.as_deref(), Some("59378"));
    assert_eq!(parsed.status, OrderStatus::PartiallyFilled);
    assert_eq!(parsed.filled_quantity, "0.004000000000000000");
    assert_eq!(parsed.average_fill_price.as_deref(), Some("3000000"));

    let open: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/open_orders_success.json"
    ))
    .expect("open orders");
    let orders = parse_bittrade_open_orders(&exchange_id(), Some(&symbol_scope()), &open)
        .expect("open orders");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].status, OrderStatus::Open);

    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/recent_fills_success.json"
    ))
    .expect("fills");
    let fills = parse_bittrade_recent_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &symbol_scope(),
        &fills,
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("99101"));
    assert_eq!(fills[0].order_id.as_deref(), Some("59378"));
    assert_eq!(fills[0].fee_asset.as_deref(), Some("JPY"));
}

#[test]
fn bittrade_private_request_specs_should_match_fixtures() {
    let open_fixture: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/request_specs/get_open_orders.json"
    ))
    .expect("open fixture");
    let open_params = open_orders_params(&symbol_scope(), Some(100)).expect("open order params");
    assert_eq!(open_fixture["path"].as_str(), Some("/v1/order/openOrders"));
    assert_eq!(
        open_params.get("symbol").map(String::as_str),
        Some(open_fixture["query"]["symbol"].as_str().unwrap())
    );
    assert_eq!(
        open_params.get("size").map(String::as_str),
        Some(open_fixture["query"]["size"].as_str().unwrap())
    );

    let fills_fixture: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/request_specs/get_recent_fills.json"
    ))
    .expect("fills fixture");
    let fill_params =
        recent_fills_params(&symbol_scope(), Some("59378"), Some(100)).expect("fill params");
    assert_eq!(
        fills_fixture["path"].as_str(),
        Some("/v1/order/matchresults")
    );
    assert_eq!(
        fill_params.get("symbol").map(String::as_str),
        Some(fills_fixture["query"]["symbol"].as_str().unwrap())
    );
    assert_eq!(
        fill_params.get("order-id").map(String::as_str),
        Some(fills_fixture["query"]["order-id"].as_str().unwrap())
    );
    assert_eq!(
        fill_params.get("size").map(String::as_str),
        Some(fills_fixture["query"]["size"].as_str().unwrap())
    );
}

#[tokio::test]
async fn bittrade_adapter_capabilities_should_be_spot_public_with_guarded_private_readback() {
    let adapter = BittradeGatewayAdapter::new(BittradeGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_query_order);

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
    assert!(format!("{err:?}").contains("bittrade.positions"));

    let err = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("59378".to_string()),
        })
        .await
        .expect_err("private rest disabled");
    assert!(matches!(
        err,
        ExchangeApiError::Unsupported {
            operation: "bittrade.query_order"
        }
    ));
}

#[tokio::test]
async fn bittrade_private_readbacks_should_send_signed_get_requests() {
    let order: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/order_success.json"
    ))
    .expect("order");
    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/open_orders_success.json"
    ))
    .expect("open orders");
    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/recent_fills_success.json"
    ))
    .expect("fills");
    let (base_url, seen) = spawn_rest_server(vec![order, open_orders, fills]).await;
    let adapter = BittradeGatewayAdapter::new(BittradeGatewayConfig {
        rest_base_url: base_url,
        enabled_private_rest: true,
        api_key: Some("bittrade-key".to_string()),
        api_secret: Some("bittrade-secret".to_string()),
        ..BittradeGatewayConfig::default()
    })
    .expect("adapter");
    assert!(adapter.capabilities().supports_private_rest);
    assert!(adapter.capabilities().supports_query_order);
    assert!(!adapter.capabilities().supports_place_order);

    let query = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("59378".to_string()),
        })
        .await
        .expect("query");
    assert_eq!(
        query.order.and_then(|order| order.exchange_order_id),
        Some("59378".to_string())
    );

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: Some(PageRequest::first_page(100)),
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
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("59378".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/v1/order/orders/59378");
    assert_eq!(requests[1].path, "/v1/order/openOrders");
    assert_eq!(requests[2].path, "/v1/order/matchresults");
    assert_eq!(
        requests[1].query.get("symbol").map(String::as_str),
        Some("btcjpy")
    );
    assert_eq!(
        requests[1].query.get("size").map(String::as_str),
        Some("100")
    );
    assert_eq!(
        requests[2].query.get("order-id").map(String::as_str),
        Some("59378")
    );
    for request in &requests {
        assert_eq!(
            request.query.get("AccessKeyId").map(String::as_str),
            Some("bittrade-key")
        );
        assert_eq!(
            request.query.get("SignatureMethod").map(String::as_str),
            Some("HmacSHA256")
        );
        assert_eq!(
            request.query.get("SignatureVersion").map(String::as_str),
            Some("2")
        );
        assert!(request.query.contains_key("Timestamp"));
        assert!(request.query.contains_key("Signature"));
        assert_eq!(request.header("accept"), Some("application/json"));
    }
}

#[test]
fn bittrade_public_ws_subscription_should_use_official_topics() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let spec =
        public_subscription_spec(&subscription, "wss://api-cloud.bittrade.co.jp/ws").expect("sub");
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/ws/public_depth_subscribe.json"
    ))
    .expect("ws fixture");
    assert_eq!(spec.topic, "market.btcjpy.depth.step0");
    assert_eq!(spec.subscribe_payload["sub"], "market.btcjpy.depth.step0");
    assert_eq!(fixture["topic"].as_str(), Some(spec.topic.as_str()));
    assert_eq!(spec.subscribe_payload, fixture["subscribe_payload"]);
    assert_eq!(spec.unsubscribe_payload, fixture["unsubscribe_payload"]);
    assert_eq!(public_pong_payload(18212558000)["pong"], 18212558000_i64);
}

#[test]
fn bittrade_private_ws_fixture_should_keep_sanitized_order_shape() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bittrade/ws/private_order_push.json"
    ))
    .expect("private ws fixture");
    assert_eq!(fixture["ch"], "orders#btcjpy");
    assert_eq!(fixture["data"]["eventType"], "creation");
    assert_eq!(fixture["data"]["clientOrderId"], "fixture-client-order-id");
}

#[test]
fn bittrade_named_registration_should_accept_aliases() {
    let gateway = AdapterBackedGateway::new("bittrade-test");
    gateway
        .register_named_adapter("huobi_japan")
        .expect("register alias");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

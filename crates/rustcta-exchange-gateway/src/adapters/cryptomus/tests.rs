use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, OrderBookRequest, PlaceOrderRequest,
    PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolRulesRequest, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{parse_cryptomus_order_book, parse_cryptomus_symbol_rules};
use super::private::{cancel_order_request_spec_fixture, place_limit_order_request_spec_fixture};
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_open_orders, parse_recent_fills,
};
use super::streams::{
    cryptomus_ping_payload, cryptomus_public_subscribe_payload, cryptomus_reconnect_policy_ms,
};
use super::{CryptomusGatewayAdapter, CryptomusGatewayConfig};
use crate::LocalGateway;

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    query: HashMap<String, String>,
}

fn cryptomus_fixture(name: &str) -> Value {
    let text = match name {
        "markets.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/cryptomus/markets.json")
        }
        "orderbook.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/cryptomus/orderbook.json")
        }
        "balances.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/cryptomus/balances.json")
        }
        "orders_active.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/cryptomus/orders_active.json")
        }
        "orders_history.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/cryptomus/orders_history.json")
        }
        "tariffs.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/cryptomus/tariffs.json")
        }
        "unsupported_boundary.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/cryptomus/unsupported_boundary.json"
        ),
        "request_specs/place_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/cryptomus/request_specs/place_order_limit.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/cryptomus/request_specs/cancel_order.json"
        ),
        "signing_vectors/private_limit_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/cryptomus/signing_vectors/private_limit_order.json"
        ),
        "ws_public_depth.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/cryptomus/ws_public_depth.json"
        ),
        "ws_private_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/cryptomus/ws_private_order.json"
        ),
        _ => panic!("unknown cryptomus fixture {name}"),
    };
    serde_json::from_str(text).expect("cryptomus fixture")
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("cryptomus").expect("exchange")
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
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTC_USDT")
            .expect("symbol"),
    }
}

#[test]
fn capabilities_should_keep_payment_and_private_rails_disabled_by_default() {
    let adapter = CryptomusGatewayAdapter::new(CryptomusGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_batch_place_order);

    let boundary = cryptomus_fixture("unsupported_boundary.json");
    assert_eq!(boundary["payment_api_in_exchange_adapter"], false);
    assert_eq!(boundary["payout_api_in_exchange_adapter"], false);
}

#[test]
fn parser_fixtures_should_cover_markets_orderbook_private_state_and_fills() {
    let rules = parse_cryptomus_symbol_rules(
        exchange_id(),
        &[symbol()],
        &cryptomus_fixture("markets.json"),
    )
    .expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "USDT");
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.000001"));
    assert_eq!(rules[0].min_notional.as_deref(), Some("5.00000000"));

    let book =
        parse_cryptomus_order_book(&symbol(), &cryptomus_fixture("orderbook.json")).expect("book");
    assert_eq!(book.bids[0].price, 65000.0);
    assert_eq!(book.asks[0].quantity, 0.2);

    let balances = parse_balances(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &[],
        &cryptomus_fixture("balances.json"),
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].asset, "USDT");

    let open_orders = parse_open_orders(
        &exchange_id(),
        Some(&symbol()),
        &cryptomus_fixture("orders_active.json"),
    )
    .expect("open orders");
    assert_eq!(open_orders[0].status, OrderStatus::Open);

    let fills = parse_recent_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        Some(&symbol()),
        &cryptomus_fixture("orders_history.json"),
    )
    .expect("fills");
    assert_eq!(fills[0].fill_id.as_deref(), Some("TXN-1"));

    let fees = parse_fee_snapshots(&[symbol()], &cryptomus_fixture("tariffs.json")).expect("fees");
    assert_eq!(fees[0].maker_rate, "-0.015");
    assert_eq!(fees[0].taker_rate, "0.04");
}

#[tokio::test]
async fn adapter_should_load_public_rules_and_orderbook_from_mock_rest() {
    let (base_url, seen) = spawn_rest_server(vec![
        cryptomus_fixture("markets.json"),
        cryptomus_fixture("orderbook.json"),
    ])
    .await;
    let adapter = CryptomusGatewayAdapter::new(CryptomusGatewayConfig {
        rest_base_url: base_url,
        ..CryptomusGatewayConfig::default()
    })
    .expect("adapter");

    let rules = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol()],
        })
        .await
        .expect("rules");
    assert_eq!(rules.rules.len(), 1);

    let book = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol(),
            depth: Some(30),
        })
        .await
        .expect("book");
    assert_eq!(book.order_book.bids[0].price, 65000.0);

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].path, "/v2/user-api/exchange/markets");
    assert_eq!(seen[1].path, "/v1/exchange/market/order-book/BTC_USDT");
    assert_eq!(seen[1].query.get("level").map(String::as_str), Some("3"));
}

#[tokio::test]
async fn private_methods_should_stay_unsupported_without_explicit_enablement() {
    let adapter = CryptomusGatewayAdapter::new(CryptomusGatewayConfig {
        api_key: Some("fixture-api-key".to_string()),
        user_id: Some("00000000-0000-0000-0000-000000000000".to_string()),
        enabled_private_rest: false,
        ..CryptomusGatewayConfig::default()
    })
    .expect("adapter");
    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("private disabled");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn place_order_should_route_signed_limit_request_when_explicitly_enabled() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "order_id": "ORDER-1"
    })])
    .await;
    let adapter = CryptomusGatewayAdapter::new(CryptomusGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("fixture-api-key".to_string()),
        user_id: Some("00000000-0000-0000-0000-000000000000".to_string()),
        enabled_private_rest: true,
        ..CryptomusGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol(),
            client_order_id: Some("CLIENT-LIMIT-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "0.01".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");
    assert_eq!(response.order.exchange_order_id.as_deref(), Some("ORDER-1"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/v2/user-api/exchange/orders");
}

#[test]
fn request_spec_and_signing_vector_fixtures_should_match_boundary() {
    let place = cryptomus_fixture("request_specs/place_order_limit.json");
    assert_eq!(place["operation"], "cryptomus.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], "/v2/user-api/exchange/orders");
    assert_eq!(place["body"]["market"], "TRX_USDT");
    assert_eq!(
        place_limit_order_request_spec_fixture()["body"]["client_order_id"],
        "CLIENT-LIMIT-1"
    );

    let cancel = cryptomus_fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["operation"], "cryptomus.cancel_order");
    assert_eq!(
        cancel_order_request_spec_fixture()["path"],
        "/v2/user-api/exchange/orders/01JEXAFCCC5ZVJPZAAHHDKQBNG"
    );

    let vector = cryptomus_fixture("signing_vectors/private_limit_order.json");
    let signature = super::signing::sign_body(
        vector["body"].as_str().expect("body"),
        vector["api_key"].as_str().expect("api key"),
    );
    assert_eq!(signature, vector["expected_signature"].as_str().unwrap());
}

#[test]
fn websocket_helpers_should_cover_subscribe_unsubscribe_and_heartbeat_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = cryptomus_public_subscribe_payload(&subscription, 1).expect("payload");
    assert_eq!(payload["method"], "depth_subscribe");
    assert_eq!(payload["params"][0], "BTC_USDT:1");
    assert_eq!(cryptomus_ping_payload(2)["method"], "ping");
    assert_eq!(cryptomus_reconnect_policy_ms(), (50_000, 10_000, 60_000));
    assert_eq!(
        cryptomus_fixture("ws_public_depth.json")["method"],
        "depth_update"
    );
    assert_eq!(
        cryptomus_fixture("ws_private_order.json")["method"],
        "order_update"
    );
}

#[tokio::test]
async fn named_registration_should_build_public_cryptomus_profile() {
    let gateway =
        crate::adapters::AdapterBackedGateway::with_named_adapters("cryptomus-test", ["cryptomus"])
            .expect("gateway");
    let status = gateway.status().await.expect("status");
    assert_eq!(status.exchanges.len(), 1);
    assert_eq!(status.exchanges[0].exchange, "cryptomus");
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
                .unwrap_or_else(|| json!({ "state": 0, "result": {} }));
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
    SeenRequest {
        method,
        path: path.to_string(),
        query,
    }
}

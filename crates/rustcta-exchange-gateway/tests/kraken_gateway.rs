use std::sync::{Arc, Mutex};

use chrono::Utc;
use rustcta_exchange_api::{
    OrderBookRequest, PublicStreamKind, PublicStreamSubscription, RequestContext,
    SymbolRulesRequest, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_gateway::{
    AdapterBackedGateway, GatewayClient, InProcessGatewayClient, KrakenGatewayConfig,
    SubscribeBooksRequest, GATEWAY_PROTOCOL_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::test]
async fn kraken_gateway_should_serve_public_rest_and_stream_specs() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "error": [],
            "result": {
                "XXBTZUSD": {
                    "altname": "XBTUSD",
                    "wsname": "XBT/USD",
                    "base": "XXBT",
                    "quote": "ZUSD",
                    "pair_decimals": 1,
                    "lot_decimals": 8,
                    "ordermin": "0.0001",
                    "costmin": "0.5"
                }
            }
        }),
        json!({
            "error": [],
            "result": {
                "XXBTZUSD": {
                    "bids": [["100.0", "2.0", 1780000000]],
                    "asks": [["101.0", "3.0", 1780000001]]
                }
            }
        }),
    ])
    .await;

    let gateway = AdapterBackedGateway::new("kraken-integration");
    gateway
        .register_kraken_adapter(KrakenGatewayConfig {
            spot_rest_base_url: format!("{base_url}/0"),
            futures_rest_base_url: base_url,
            enabled_private_rest: false,
            ..KrakenGatewayConfig::default()
        })
        .expect("register kraken");
    let client = InProcessGatewayClient::new(Arc::new(gateway));
    let tenant_id = TenantId::new("tenant").expect("tenant");
    let account_id = AccountId::new("account").expect("account");
    let symbol = spot_symbol_scope();

    let rules = client
        .get_symbol_rules(
            "rules".to_string(),
            tenant_id.clone(),
            Some(account_id.clone()),
            SymbolRulesRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("rules"),
                symbols: vec![symbol.clone()],
            },
        )
        .await
        .expect("symbol rules");
    assert_eq!(rules.rules.len(), 1);
    assert_eq!(rules.rules[0].base_asset, "BTC");
    assert_eq!(rules.rules[0].quote_asset, "USD");
    assert_eq!(rules.rules[0].price_increment.as_deref(), Some("0.1"));

    let book = client
        .get_order_book(
            "book".to_string(),
            tenant_id.clone(),
            Some(account_id.clone()),
            OrderBookRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("book"),
                symbol: symbol.clone(),
                depth: Some(25),
            },
        )
        .await
        .expect("order book");
    assert_eq!(book.order_book.bids[0].price, 100.0);
    assert_eq!(book.order_book.asks[0].quantity, 3.0);

    let subscription = client
        .subscribe_books(
            "subscribe".to_string(),
            tenant_id,
            Some(account_id),
            SubscribeBooksRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("subscribe"),
                subscriptions: vec![PublicStreamSubscription {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("subscribe"),
                    symbol,
                    kind: PublicStreamKind::OrderBookDelta,
                }],
            },
        )
        .await
        .expect("public stream subscribe");
    assert_eq!(subscription.subscriptions.len(), 1);
    assert!(subscription.subscriptions[0]
        .subscription_id
        .contains("wss://ws.kraken.com/v2:book:BTC/USD"));

    let requests = seen.lock().unwrap();
    assert!(requests[0].starts_with("GET /0/public/AssetPairs "));
    assert!(requests[1].starts_with("GET /0/public/Depth?count=25&pair=XBTUSD "));
}

async fn spawn_rest_server(responses: Vec<Value>) -> (String, Arc<Mutex<Vec<String>>>) {
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
            let mut buffer = vec![0_u8; 16384];
            let bytes_read = stream.read(&mut buffer).await.unwrap();
            let request_text = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
            let request_line = request_text.lines().next().unwrap_or_default();
            seen_requests.lock().unwrap().push(request_line.to_string());
            let body = responses
                .lock()
                .unwrap()
                .next()
                .unwrap_or_else(|| json!({ "error": [], "result": {} }));
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

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("account").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: Utc::now(),
    }
}

fn spot_symbol_scope() -> SymbolScope {
    let exchange = ExchangeId::new("kraken").expect("exchange");
    SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange, MarketType::Spot, "XBTUSD").expect("symbol"),
    }
}

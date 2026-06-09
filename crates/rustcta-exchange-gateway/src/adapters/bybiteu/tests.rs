use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelAllOrdersRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest,
    OrderListConditionalLeg, OrderListLegType, OrderListRequest, PlaceOrderRequest,
    PositionsRequest, PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::{new_adapter, BybiteuGatewayConfig};
use crate::adapters::AdapterBackedGateway;
use crate::request_spec::ActualHttpRequest;
use crate::request_spec::RequestSpec;

#[test]
fn bybiteu_config_should_map_to_bybit_eu_profile() {
    let config = BybiteuGatewayConfig::default();
    assert_eq!(config.rest_base_url, "https://api.bybit.eu");
    assert_eq!(
        config.public_ws_url,
        "wss://stream.bybit.eu/v5/public/linear"
    );

    let bybit_profile = config.into_bybit_config();
    assert_eq!(bybit_profile.exchange_id, "bybiteu");
    assert_eq!(bybit_profile.rest_base_url, "https://api.bybit.eu");
    assert_eq!(
        bybit_profile.public_ws_url,
        "wss://stream.bybit.eu/v5/public/linear"
    );
    assert!(!bybit_profile.enabled_private_rest);
    assert_eq!(
        bybit_profile.unsupported_market_type_operation,
        "bybiteu.unsupported_market_type"
    );
}

#[test]
fn bybiteu_adapter_should_register_by_name() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["bybiteu"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn bybiteu_capabilities_should_keep_private_trading_disabled() {
    let adapter = new_adapter(BybiteuGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.exchange, exchange_id());
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_cancel_all_orders);
    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Spot, MarketType::Perpetual, MarketType::Futures]
    );
}

#[tokio::test]
async fn bybiteu_advanced_order_surfaces_should_remain_broker_scope_unsupported() {
    let adapter = new_adapter(BybiteuGatewayConfig::default()).expect("adapter");
    let symbol = symbol_scope("BTCUSDT");
    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-item"),
        symbol: symbol.clone(),
        client_order_id: Some("client-batch-1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol.clone(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            new_client_order_id: None,
            new_quantity: "0.02".to_string(),
        })
        .await
        .expect_err("amend unsupported");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: "bybiteu.amend_order"
        }
    ));

    let list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oco"),
            symbol: symbol.clone(),
            list_client_order_id: Some("oco-1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some("68000".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("62000".to_string()),
                stop_price: Some("62500".to_string()),
                time_in_force: None,
                client_order_id: Some("oco-below".to_string()),
            },
        })
        .await
        .expect_err("order-list unsupported");
    assert!(matches!(
        list_error,
        ExchangeApiError::Unsupported {
            operation: "bybiteu.place_order_list"
        }
    ));

    let batch_place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![order],
        })
        .await
        .expect_err("batch place unsupported");
    assert!(matches!(
        batch_place_error,
        ExchangeApiError::Unsupported {
            operation: "bybiteu.batch_place_orders"
        }
    ));

    let cancel = CancelOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("cancel-item"),
        symbol: symbol.clone(),
        client_order_id: Some("cancel-client-1".to_string()),
        exchange_order_id: Some("order-1".to_string()),
    };
    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![cancel],
        })
        .await
        .expect_err("batch cancel unsupported");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: "bybiteu.batch_cancel_orders"
        }
    ));

    let cancel_all_error = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol),
        })
        .await
        .expect_err("cancel-all unsupported");
    assert!(matches!(
        cancel_all_error,
        ExchangeApiError::Unsupported {
            operation: "bybiteu.cancel_all_orders"
        }
    ));
}

#[tokio::test]
async fn bybiteu_public_ws_subscription_should_use_eu_host_and_exchange_id() {
    let adapter = new_adapter(BybiteuGatewayConfig::default()).expect("adapter");
    let ack = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol_scope("BTCUSDT"),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .await
        .expect("public ws");

    assert_eq!(
        ack,
        "bybiteu:wss://stream.bybit.eu/v5/public/linear:orderbook.1.BTCUSDT"
    );
}

#[tokio::test]
async fn bybiteu_private_rest_should_remain_unsupported_without_broker_audit() {
    let adapter = new_adapter(BybiteuGatewayConfig::default()).expect("adapter");
    let error = adapter
        .get_balances(rustcta_exchange_api::BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect_err("private rest must stay disabled");

    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bybiteu.get_balances"
        }
    ));
}

#[tokio::test]
async fn bybiteu_get_balances_should_reuse_bybit_v5_signed_runtime_when_enabled() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": [{
                "accountType": "UNIFIED",
                "coin": [
                    {
                        "coin": "USDT",
                        "walletBalance": "1000.0000",
                        "availableToWithdraw": "900.0000"
                    },
                    {
                        "coin": "BTC",
                        "walletBalance": "0.2500",
                        "availableToWithdraw": "0.2000"
                    }
                ]
            }]
        },
        "time": 1700000000000_i64
    })])
    .await;
    let adapter = new_adapter(BybiteuGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..BybiteuGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances-enabled"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("balances");

    assert_eq!(response.balances.len(), 1);
    assert_eq!(response.balances[0].exchange_id, exchange_id());
    assert_eq!(response.balances[0].balances.len(), 1);
    assert_eq!(response.balances[0].balances[0].asset, "USDT");
    assert_eq!(response.balances[0].balances[0].total, 1000.0);
    assert_eq!(response.balances[0].balances[0].available, 900.0);
    assert_eq!(response.balances[0].balances[0].locked, 100.0);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("GET /v5/account/wallet-balance?accountType=UNIFIED "));
    let lower = requests[0].to_ascii_lowercase();
    assert!(lower.contains("x-bapi-api-key: test-key"));
    assert!(lower.contains("x-bapi-sign: "));
    assert!(lower.contains("x-bapi-recv-window: 5000"));
    assert!(!requests[0].contains("test-secret"));
    bybiteu_request_spec("get_balances_wallet_balance.json")
        .assert_matches(&actual_http_request(&requests[0]))
        .expect("balances request spec");
}

#[tokio::test]
async fn bybiteu_get_positions_should_reuse_bybit_v5_signed_runtime_with_cursor() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "nextPageCursor": "cursor-2",
                "list": [{
                    "symbol": "BTCUSDT",
                    "side": "Buy",
                    "size": "0.25",
                    "avgPrice": "65000",
                    "markPrice": "65100",
                    "liqPrice": "40000",
                    "unrealisedPnl": "25",
                    "leverage": "5"
                }]
            },
            "time": 1700000000000_i64
        }),
        json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "nextPageCursor": "",
                "list": [{
                    "symbol": "ETHUSDT",
                    "side": "Sell",
                    "size": "1.5",
                    "avgPrice": "3500",
                    "markPrice": "3490",
                    "liqPrice": "4500",
                    "unrealisedPnl": "-15",
                    "leverage": "3"
                }]
            },
            "time": 1700000000001_i64
        }),
    ])
    .await;
    let adapter = new_adapter(BybiteuGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..BybiteuGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: Vec::new(),
        })
        .await
        .expect("positions");

    assert_eq!(response.positions.len(), 2);
    assert_eq!(response.positions[0].exchange_id, exchange_id());
    assert_eq!(response.positions[0].quantity, 0.25);
    assert_eq!(response.positions[1].quantity, 1.5);
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[0].starts_with("GET /v5/position/list?category=linear&limit=50&settleCoin=USDT ")
    );
    assert!(requests[1].starts_with(
        "GET /v5/position/list?category=linear&cursor=cursor-2&limit=50&settleCoin=USDT "
    ));
    for request in &requests {
        let lower = request.to_ascii_lowercase();
        assert!(lower.contains("x-bapi-api-key: test-key"));
        assert!(lower.contains("x-bapi-sign: "));
        assert!(lower.contains("x-bapi-recv-window: 5000"));
        assert!(!request.contains("test-secret"));
    }
    bybiteu_request_spec("get_positions.json")
        .assert_matches(&actual_http_request(&requests[0]))
        .expect("positions request spec");
}

#[tokio::test]
async fn bybiteu_get_fees_should_reuse_bybit_v5_signed_runtime_when_enabled() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": [{
                "category": "linear",
                "symbol": "BTCUSDT",
                "makerFeeRate": "0.0001",
                "takerFeeRate": "0.0006"
            }]
        },
        "time": 1700000000000_i64
    })])
    .await;
    let adapter = new_adapter(BybiteuGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..BybiteuGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees-enabled"),
            symbols: vec![symbol_scope("BTCUSDT")],
        })
        .await
        .expect("fees");

    assert_eq!(response.fees.len(), 1);
    assert_eq!(response.fees[0].maker_rate, "0.0001");
    assert_eq!(response.fees[0].taker_rate, "0.0006");
    assert_eq!(
        response.fees[0].source.as_deref(),
        Some("bybit.v5.account_fee_rate")
    );

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("GET /v5/account/fee-rate?category=linear&symbol=BTCUSDT "));
    let lower = requests[0].to_ascii_lowercase();
    assert!(lower.contains("x-bapi-api-key: test-key"));
    assert!(lower.contains("x-bapi-sign: "));
    assert!(!requests[0].contains("test-secret"));
    bybiteu_request_spec("get_fees_fee_rate.json")
        .assert_matches(&actual_http_request(&requests[0]))
        .expect("fees request spec");
}

#[test]
fn bybiteu_public_rest_fixtures_should_parse_with_eu_exchange_id() {
    let instruments: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/instruments.json"
    ))
    .expect("instruments fixture");
    let rules = parse_symbol_rules(&exchange_id(), MarketType::Perpetual, &instruments)
        .expect("symbol rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].symbol.exchange, exchange_id());
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTCUSDT");

    let book_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/orderbook.json"
    ))
    .expect("orderbook fixture");
    let book = parse_orderbook_snapshot(&exchange_id(), symbol_scope("BTCUSDT"), &book_fixture)
        .expect("order book");
    assert_eq!(book.exchange_id, exchange_id());
    assert_eq!(book.sequence, Some(100));
    assert_eq!(book.bids[0].quantity, 1.2);
}

#[test]
fn bybiteu_boundary_and_ws_fixtures_should_be_sanitized() {
    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    assert_eq!(boundary["exchange"], "bybiteu");
    assert_eq!(boundary["private_rest"]["trade_enabled"], false);
    assert_eq!(boundary["advanced_orders"]["runtime_enabled"], false);
    assert_eq!(boundary["advanced_orders"]["native_batch"], false);
    assert_eq!(boundary["rest_base_url"], "https://api.bybit.eu");

    let request_spec: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/request_specs/private_rest_unsupported.json"
    ))
    .expect("request spec fixture");
    assert_eq!(request_spec["support"], "unsupported");
    assert!(request_spec["credential_values"]
        .as_object()
        .unwrap()
        .is_empty());

    let signing_vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/signing_vectors/rest_hmac_shape.json"
    ))
    .expect("signing fixture");
    assert_eq!(signing_vector["credential_values"], serde_json::json!({}));

    let public_ws: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/ws_public_orderbook.json"
    ))
    .expect("public ws fixture");
    assert_eq!(public_ws["topic"], "orderbook.50.BTCUSDT");
    assert_eq!(public_ws["type"], "snapshot");

    let funding_history: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/funding_history.json"
    ))
    .expect("funding history fixture");
    assert_eq!(funding_history["result"]["category"], "linear");

    let open_interest: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/open_interest.json"
    ))
    .expect("open interest fixture");
    assert_eq!(open_interest["result"]["symbol"], "BTCUSDT");

    let private_ws: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybiteu/ws_private_order_unsupported.json"
    ))
    .expect("private ws fixture");
    assert_eq!(
        private_ws["support"],
        "unsupported_without_broker_scope_audit"
    );
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bybiteu").unwrap()
}

fn symbol_scope(symbol: &str) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, symbol).unwrap(),
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

fn bybiteu_request_spec(path: &str) -> RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/bybiteu/request_specs/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("bybiteu request spec fixture");
    serde_json::from_str(&text).expect("bybiteu request spec fixture")
}

async fn spawn_rest_server(responses: Vec<serde_json::Value>) -> (String, Arc<Mutex<Vec<String>>>) {
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
            seen_requests.lock().unwrap().push(request_text);
            let body = responses
                .lock()
                .unwrap()
                .next()
                .unwrap_or_else(|| json!({ "retCode": 0, "result": { "list": [] } }));
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

fn actual_http_request(request: &str) -> ActualHttpRequest {
    let (head, body) = request
        .split_once("\r\n\r\n")
        .map_or((request, ""), |(head, body)| (head, body));
    let mut lines = head.lines();
    let request_line = lines.next().expect("request line");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().expect("method");
    let path_with_query = parts.next().expect("path");
    let (path, query) = path_with_query
        .split_once('?')
        .map_or((path_with_query, ""), |(path, query)| (path, query));
    let headers = lines
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.trim().to_string(), value.trim().to_string()))
        })
        .collect::<BTreeMap<_, _>>();
    let query = query
        .split('&')
        .filter(|part| !part.is_empty())
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            Some((key.to_string(), value.to_string()))
        })
        .collect::<BTreeMap<_, _>>();
    let body = body
        .trim()
        .is_empty()
        .then_some(None)
        .unwrap_or_else(|| Some(serde_json::from_str(body.trim()).expect("json body")));
    ActualHttpRequest::new(method, path)
        .with_query(query)
        .with_headers(headers)
        .with_body(body)
}

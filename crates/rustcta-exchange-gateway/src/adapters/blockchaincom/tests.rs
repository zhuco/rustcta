use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    OrderListConditionalLeg, OrderListLegType, OrderListRequest, PlaceOrderRequest,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    QueryOrderRequest, RecentFillsRequest, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{
    blockchaincom_symbol, parse_blockchaincom_balances, parse_blockchaincom_fees,
    parse_blockchaincom_fills, parse_blockchaincom_order_book, parse_blockchaincom_order_list,
    parse_blockchaincom_symbol_rules,
};
use super::private::place_order_request_spec_fixture;
use super::signing::{blockchaincom_token_headers, blockchaincom_ws_auth_payload};
use super::streams::{
    blockchaincom_heartbeat_subscribe_payload, blockchaincom_private_subscribe_payload,
    blockchaincom_public_subscribe_payload, blockchaincom_public_unsubscribe_payload,
    blockchaincom_reconnect_policy_ms, blockchaincom_sequence_gap_requires_restart,
};
use super::{BlockchainComGatewayAdapter, BlockchainComGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("blockchaincom").expect("exchange")
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
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTC-USD")
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

async fn spawn_rest_server(
    responses: Vec<serde_json::Value>,
) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("addr");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_requests = Arc::clone(&seen);
    let responses = Arc::new(Mutex::new(responses.into_iter()));

    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut buffer = vec![0_u8; 8192];
            let bytes_read = stream.read(&mut buffer).await.expect("read");
            let request_text = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
            seen_requests
                .lock()
                .expect("seen")
                .push(parse_seen_request(&request_text));
            let body = responses
                .lock()
                .expect("responses")
                .next()
                .unwrap_or_else(|| json!({}));
            let body_text = body.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body_text.len(),
                body_text
            );
            stream.write_all(response.as_bytes()).await.expect("write");
        }
    });

    (format!("http://{address}"), seen)
}

fn parse_seen_request(request_text: &str) -> SeenRequest {
    let request_line = request_text.lines().next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default();
    let (path, raw_query) = target.split_once('?').unwrap_or((target, ""));
    let query = raw_query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            Some((
                urlencoding::decode(key).ok()?.into_owned(),
                urlencoding::decode(value).ok()?.into_owned(),
            ))
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
    SeenRequest {
        method,
        path: path.to_string(),
        query,
        headers,
    }
}

fn private_config(base_url: String) -> BlockchainComGatewayConfig {
    BlockchainComGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("fixture-api-token".to_string()),
        enabled_private_rest: true,
        ..BlockchainComGatewayConfig::default()
    }
}

#[test]
fn parser_should_cover_symbols_and_l2_order_book() {
    let symbols: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/symbols.json"
    ))
    .expect("symbols fixture");
    let rules = parse_blockchaincom_symbol_rules(exchange_id(), &[], &symbols).expect("rules");
    let btc_usd = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "BTC-USD")
        .expect("BTC-USD rules");
    assert_eq!(btc_usd.base_asset, "BTC");
    assert_eq!(btc_usd.quote_asset, "USD");
    assert_eq!(btc_usd.price_increment.as_deref(), Some("10"));
    assert_eq!(btc_usd.quantity_increment.as_deref(), Some("0.00000005"));
    assert_eq!(btc_usd.min_quantity.as_deref(), Some("0.5"));
    assert!(btc_usd.supports_market_orders);

    let book: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/orderbook_l2.json"
    ))
    .expect("book fixture");
    let snapshot = parse_blockchaincom_order_book(&symbol(), Some(2), &book).expect("book");
    assert_eq!(snapshot.sequence, Some(110));
    assert_eq!(snapshot.bids.len(), 2);
    assert_eq!(snapshot.best_bid().expect("bid").price, 8723.45);
    assert_eq!(snapshot.best_ask().expect("ask").price, 8729.0);
    assert_eq!(blockchaincom_symbol(&symbol()), "BTC-USD");
}

#[test]
fn private_parser_should_cover_account_orders_fees_and_fills() {
    let accounts: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/accounts.json"
    ))
    .expect("accounts fixture");
    let balances = parse_blockchaincom_balances(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &[],
        &accounts,
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].asset, "BTC");
    assert_eq!(balances[0].balances[0].available, 0.45);

    let orders: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/orders.json"
    ))
    .expect("orders fixture");
    let parsed_orders =
        parse_blockchaincom_order_list(&exchange_id(), Some(&symbol()), &orders).expect("orders");
    assert_eq!(
        parsed_orders[0].client_order_id.as_deref(),
        Some("rustcta-1")
    );
    assert_eq!(parsed_orders[0].filled_quantity, "0.003");

    let fees: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/fees.json"
    ))
    .expect("fees fixture");
    let parsed_fees = parse_blockchaincom_fees(&exchange_id(), &[symbol()], &fees).expect("fees");
    assert_eq!(parsed_fees[0].maker_rate, "0.0014");

    let fills: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/fills.json"
    ))
    .expect("fills fixture");
    let parsed_fills = parse_blockchaincom_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        Some(&symbol()),
        &fills,
    )
    .expect("fills");
    assert_eq!(parsed_fills[0].fill_id.as_deref(), Some("fill-1"));
    assert_eq!(parsed_fills[0].fee_asset.as_deref(), Some("USD"));
}

#[test]
fn request_spec_and_token_auth_should_match_official_boundary() {
    let spec: RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/request_specs/place_order_limit.json"
    ))
    .expect("request spec");
    let actual = ActualHttpRequest::new("POST", "/orders")
        .with_headers([
            ("Accept".to_string(), "application/json".to_string()),
            ("Content-Type".to_string(), "application/json".to_string()),
            ("X-API-Token".to_string(), "fixture-api-token".to_string()),
        ])
        .with_body(Some(json!({
            "clOrdId": "rustcta-fixture-0001",
            "symbol": "BTC-USD",
            "side": "BUY",
            "ordType": "LIMIT",
            "timeInForce": "GTC",
            "orderQty": "0.01",
            "price": "65000"
        })));
    spec.assert_matches(&actual).expect("request spec match");

    let fixture = place_order_request_spec_fixture();
    assert_eq!(fixture["auth"], "api_key_header");
    let query: RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/request_specs/query_order.json"
    ))
    .expect("query spec");
    query
        .assert_matches(
            &ActualHttpRequest::new("GET", "/orders/{orderId}").with_headers([
                ("Accept".to_string(), "application/json".to_string()),
                ("X-API-Token".to_string(), "fixture-api-token".to_string()),
            ]),
        )
        .expect("query spec match");
    let open_orders: RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/request_specs/get_open_orders.json"
    ))
    .expect("open orders spec");
    open_orders
        .assert_matches(
            &ActualHttpRequest::new("GET", "/orders")
                .with_query([
                    ("symbol".to_string(), "BTC-USD".to_string()),
                    ("status".to_string(), "OPEN".to_string()),
                ])
                .with_headers([
                    ("Accept".to_string(), "application/json".to_string()),
                    ("X-API-Token".to_string(), "fixture-api-token".to_string()),
                ]),
        )
        .expect("open orders spec match");
    let fills: RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/request_specs/get_recent_fills.json"
    ))
    .expect("fills spec");
    fills
        .assert_matches(
            &ActualHttpRequest::new("GET", "/fills")
                .with_query([
                    ("symbol".to_string(), "BTC-USD".to_string()),
                    ("limit".to_string(), "100".to_string()),
                ])
                .with_headers([
                    ("Accept".to_string(), "application/json".to_string()),
                    ("X-API-Token".to_string(), "fixture-api-token".to_string()),
                ]),
        )
        .expect("fills spec match");

    let headers = blockchaincom_token_headers("fixture-api-token").expect("headers");
    assert_eq!(
        headers.get("X-API-Token").map(String::as_str),
        Some("fixture-api-token")
    );

    let auth_payload = blockchaincom_ws_auth_payload("fixture-api-secret").expect("auth");
    let auth_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/signing_vectors/token_auth.json"
    ))
    .expect("auth fixture");
    assert_eq!(auth_fixture["algorithm"], "none_api_token_header");
    assert_eq!(auth_payload["channel"], "auth");
    assert_eq!(auth_payload["token"], "fixture-api-secret");
}

#[tokio::test]
async fn private_write_should_remain_offline_request_spec_only() {
    let adapter =
        BlockchainComGatewayAdapter::new(BlockchainComGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: Some("rustcta-1".to_string()),
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

    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "blockchaincom.place_order_offline_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn balances_should_fail_closed_without_private_token() {
    let adapter =
        BlockchainComGatewayAdapter::new(BlockchainComGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_balances);

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("private balances disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "blockchaincom.get_balances"
        }
    ));

    let error = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol()],
        })
        .await
        .expect_err("private fees disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "blockchaincom.get_fees"
        }
    ));
}

#[tokio::test]
async fn order_readbacks_should_fail_closed_without_private_token() {
    let adapter =
        BlockchainComGatewayAdapter::new(BlockchainComGatewayConfig::default()).expect("adapter");

    let query_error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("123456789".to_string()),
        })
        .await
        .expect_err("query disabled");
    assert!(matches!(
        query_error,
        ExchangeApiError::Unsupported {
            operation: "blockchaincom.query_order"
        }
    ));

    let open_error = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol()),
            page: None,
        })
        .await
        .expect_err("open orders disabled");
    assert!(matches!(
        open_error,
        ExchangeApiError::Unsupported {
            operation: "blockchaincom.get_open_orders"
        }
    ));

    let fills_error = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect_err("fills disabled");
    assert!(matches!(
        fills_error,
        ExchangeApiError::Unsupported {
            operation: "blockchaincom.get_recent_fills"
        }
    ));
}

#[test]
fn capabilities_should_enable_private_reads_only_with_private_rest_token() {
    let adapter = BlockchainComGatewayAdapter::new(BlockchainComGatewayConfig {
        enabled_private_rest: true,
        api_key: Some("fixture-api-token".to_string()),
        ..BlockchainComGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_balances);
    assert!(capabilities.supports_fees);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_cancel_all_orders);
}

#[tokio::test]
async fn order_readbacks_should_send_token_get_and_parse_responses() {
    let order: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/orders.json"
    ))
    .expect("orders fixture");
    let fills: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/blockchaincom/fills.json"
    ))
    .expect("fills fixture");
    let order_object = order
        .as_array()
        .and_then(|orders| orders.first())
        .cloned()
        .expect("first order");
    let (base_url, seen) = spawn_rest_server(vec![order_object, order.clone(), fills]).await;
    let adapter = BlockchainComGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let queried = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-live"),
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("123456789".to_string()),
        })
        .await
        .expect("query order");
    assert_eq!(
        queried
            .order
            .as_ref()
            .and_then(|order| order.exchange_order_id.as_deref()),
        Some("123456789")
    );

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-live"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol()),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);
    assert_eq!(open.orders[0].client_order_id.as_deref(), Some("rustcta-1"));

    let recent = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills-live"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect("recent fills");
    assert_eq!(recent.fills[0].fill_id.as_deref(), Some("fill-1"));

    let seen = seen.lock().expect("seen").clone();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/orders/123456789");
    assert_eq!(
        seen[0].headers.get("x-api-token").map(String::as_str),
        Some("fixture-api-token")
    );
    assert_eq!(seen[1].path, "/orders");
    assert_eq!(
        seen[1].query.get("symbol").map(String::as_str),
        Some("BTC-USD")
    );
    assert_eq!(
        seen[1].query.get("status").map(String::as_str),
        Some("OPEN")
    );
    assert_eq!(seen[2].path, "/fills");
    assert_eq!(
        seen[2].query.get("symbol").map(String::as_str),
        Some("BTC-USD")
    );
    assert_eq!(seen[2].query.get("limit").map(String::as_str), Some("100"));
}

#[tokio::test]
async fn advanced_order_surfaces_should_remain_explicitly_unsupported() {
    let adapter =
        BlockchainComGatewayAdapter::new(BlockchainComGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol(),
            client_order_id: Some("rustcta-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            new_client_order_id: None,
            new_quantity: "0.02".to_string(),
        })
        .await
        .expect_err("amend unsupported");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: "blockchaincom.amend_order_unsupported"
        }
    ));

    let list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oco"),
            symbol: symbol(),
            list_client_order_id: Some("oco-1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some("70000".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("60000".to_string()),
                stop_price: Some("61000".to_string()),
                time_in_force: None,
                client_order_id: Some("oco-below".to_string()),
            },
        })
        .await
        .expect_err("order-list unsupported");
    assert!(matches!(
        list_error,
        ExchangeApiError::Unsupported {
            operation: "blockchaincom.order_list_unsupported"
        }
    ));

    let place_order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-order"),
        symbol: symbol(),
        client_order_id: Some("batch-place-1".to_string()),
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
    let batch_place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![place_order],
        })
        .await
        .expect_err("batch place unsupported");
    assert!(matches!(
        batch_place_error,
        ExchangeApiError::Unsupported {
            operation: "blockchaincom.batch_place_orders_unsupported"
        }
    ));

    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel"),
                symbol: symbol(),
                client_order_id: Some("rustcta-1".to_string()),
                exchange_order_id: Some("order-1".to_string()),
            }],
        })
        .await
        .expect_err("batch cancel unsupported");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: "blockchaincom.batch_cancel_orders_unsupported"
        }
    ));
}

#[test]
fn websocket_helpers_should_cover_public_private_and_resync_boundaries() {
    let public = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = blockchaincom_public_subscribe_payload(&public);
    assert_eq!(payload["action"], "subscribe");
    assert_eq!(payload["channel"], "l2");
    assert_eq!(payload["symbol"], "BTC-USD");
    assert_eq!(
        blockchaincom_public_unsubscribe_payload(&public)["action"],
        "unsubscribe"
    );
    assert_eq!(
        blockchaincom_heartbeat_subscribe_payload()["channel"],
        "heartbeat"
    );

    let private = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        account_id: AccountId::new("account").expect("account"),
        market_type: Some(MarketType::Spot),
        kind: PrivateStreamKind::Orders,
    };
    let private_payload =
        blockchaincom_private_subscribe_payload(&private, "fixture-api-secret").expect("private");
    assert_eq!(private_payload["auth"]["channel"], "auth");
    assert_eq!(private_payload["subscribe"]["channel"], "trading");
    assert!(blockchaincom_sequence_gap_requires_restart(10, 12));
    assert!(!blockchaincom_sequence_gap_requires_restart(10, 11));
    assert_eq!(blockchaincom_reconnect_policy_ms(), (5_000, 15_000, 60_000));
}

#[test]
fn capabilities_should_expose_public_spot_and_keep_private_runtime_closed() {
    let adapter =
        BlockchainComGatewayAdapter::new(BlockchainComGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_private_streams);
}

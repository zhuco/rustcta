use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    AmendOrderRequest, ExchangeApiError, ExchangeClient, OpenOrdersRequest, PlaceOrderRequest,
    PrivateStreamKind, PublicStreamKind, PublicStreamSubscription, QueryOrderRequest,
    RecentFillsRequest, RequestContext, SymbolScope, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{
    coinmate_canonical_pair, coinmate_pair, parse_order_ack_id, parse_order_book_shape,
    parse_symbol_rules,
};
use super::private::{
    coinmate_amend_order_live_guard, coinmate_cancel_all_form, coinmate_cancel_order_form,
    coinmate_history_form, coinmate_limit_order_form, coinmate_limit_order_path,
    coinmate_replace_limit_form, coinmate_replace_limit_path, redacted_auth_fields,
    CoinmateReplaceLimitSpec,
};
use super::private_parser::{
    parse_amend_order_ack_id, parse_balance_assets, parse_fill_ids, parse_open_order_ids,
};
use super::signing::{
    coinmate_hmac_signature, coinmate_signature_payload, coinmate_signed_auth_fields,
};
use super::streams::{
    coinmate_private_subscribe_payload, coinmate_public_order_book_ws_policy,
    coinmate_public_subscribe_payload, coinmate_reconnect_policy_ms, coinmate_unsubscribe_payload,
    parse_ws_event_type, parse_ws_order_book_shape,
};
use super::{CoinmateGatewayAdapter, CoinmateGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("coinmate").expect("exchange")
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
    let (base, quote) = coinmate_canonical_pair(symbol).expect("canonical pair");
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
    headers: HashMap<String, String>,
    body: String,
}

async fn spawn_rest_server(responses: Vec<Value>) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
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
                .unwrap_or_else(|| json!({ "error": false, "data": [] }));
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
    let method = request_line
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string();
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .split('?')
        .next()
        .unwrap_or_default()
        .to_string();
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
        .map(|(_, body)| body.trim().to_string())
        .unwrap_or_default();
    SeenRequest {
        method,
        path,
        headers,
        body,
    }
}

fn parse_form_body(body: &str) -> HashMap<String, String> {
    body.split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (
                urlencoding::decode(key).expect("key").into_owned(),
                urlencoding::decode(value).expect("value").into_owned(),
            )
        })
        .collect()
}

fn private_config(base_url: String) -> CoinmateGatewayConfig {
    CoinmateGatewayConfig {
        rest_base_url: base_url,
        client_id: Some("fixture-client".to_string()),
        public_key: Some("fixture-public".to_string()),
        private_key: Some("fixture-private".to_string()),
        enabled_private_rest: true,
        ..CoinmateGatewayConfig::default()
    }
}

#[test]
fn symbol_normalization_should_cover_eur_and_czk_markets() {
    assert_eq!(coinmate_pair("btc/eur"), "BTC_EUR");
    assert_eq!(coinmate_pair("ETH-CZK"), "ETH_CZK");
    assert_eq!(
        coinmate_canonical_pair("BTCEUR").expect("eur"),
        ("BTC".to_string(), "EUR".to_string())
    );
    assert_eq!(
        coinmate_canonical_pair("BTC_CZK").expect("czk"),
        ("BTC".to_string(), "CZK".to_string())
    );
}

#[test]
fn request_spec_and_signing_should_stay_secret_free() {
    let payload = coinmate_signature_payload("1700000000000", "fixture-client", "fixture-public");
    assert_eq!(
        payload,
        "1700000000000fixture-clientfixture-public".to_string()
    );
    let signature = coinmate_hmac_signature("fixture-private", &payload).expect("signature");
    assert_eq!(
        signature,
        "07EA1665FD22A32D201A27B76AD8DC90A32D42E93DB39A0926893F3B07843798"
    );
    let auth = coinmate_signed_auth_fields(
        "fixture-client",
        "fixture-public",
        "fixture-private",
        "1700000000000",
    )
    .expect("auth fields");
    assert_eq!(
        auth.get("clientId").map(String::as_str),
        Some("fixture-client")
    );
    assert_eq!(
        auth.get("signature").map(String::as_str),
        Some(signature.as_str())
    );

    let vector: crate::signing_spec::SigningVector = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/signing_vectors/rest_form_auth.json"
    ))
    .expect("signing vector");
    vector.verify().expect("standard signing vector verifies");

    let place: crate::request_spec::RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/request_specs/place_order_limit_buy.json"
    ))
    .expect("place spec");
    assert_eq!(place.method, "POST");
    assert_eq!(place.path, "/buyLimit");
    assert_eq!(
        place.body.as_ref().expect("body")["currencyPair"],
        "BTC_EUR"
    );
    assert_eq!(place.body.as_ref().expect("body")["postOnly"], true);

    let cancel: crate::request_spec::RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/request_specs/cancel_order.json"
    ))
    .expect("cancel spec");
    assert_eq!(cancel.path, "/cancelOrder");
    assert_eq!(
        cancel.body.as_ref().expect("body")["orderId"],
        "offline-order-1"
    );

    let amend_buy: crate::request_spec::RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/request_specs/amend_order_replace_buy.json"
    ))
    .expect("amend buy spec");
    assert_eq!(amend_buy.path, "/replaceByBuyLimit");
    assert_eq!(
        amend_buy.body.as_ref().expect("body")["orderIdToBeReplaced"],
        "offline-order-1"
    );
    assert_eq!(amend_buy.body.as_ref().expect("body")["price"], "50025");

    let amend_sell: crate::request_spec::RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/request_specs/amend_order_replace_sell.json"
    ))
    .expect("amend sell spec");
    assert_eq!(amend_sell.path, "/replaceBySellLimit");
    assert_eq!(
        amend_sell.body.as_ref().expect("body")["orderIdToBeReplaced"],
        "offline-order-2"
    );

    let redacted = redacted_auth_fields();
    assert_eq!(redacted["signature"], "<redacted:signature>");
}

#[test]
fn place_cancel_and_history_forms_should_match_coinmate_fields() {
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTC_EUR"),
        client_order_id: Some("123456".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::PostOnly,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("50000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: true,
    };
    let body = coinmate_limit_order_form(&request).expect("body");
    assert_eq!(coinmate_limit_order_path(OrderSide::Buy), "/buyLimit");
    assert_eq!(body["currencyPair"], "BTC_EUR");
    assert_eq!(body["amount"], "0.01");
    assert_eq!(body["price"], "50000");
    assert_eq!(body["postOnly"], true);
    assert_eq!(body["clientOrderId"], "123456");

    let mut ioc = request;
    ioc.order_type = OrderType::Limit;
    ioc.time_in_force = Some(TimeInForce::IOC);
    ioc.post_only = false;
    let body = coinmate_limit_order_form(&ioc).expect("ioc body");
    assert_eq!(body["immediateOrCancel"], true);

    assert_eq!(
        coinmate_cancel_order_form("offline-order-1")["orderId"],
        "offline-order-1"
    );
    assert_eq!(
        coinmate_cancel_all_form(Some("btc/eur"))["currencyPair"],
        "BTC_EUR"
    );
    assert_eq!(coinmate_history_form("btc_eur", Some(100))["limit"], 100);

    let replace = CoinmateReplaceLimitSpec {
        side: OrderSide::Sell,
        order_id_to_be_replaced: "offline-order-2",
        symbol: "btc/eur",
        amount: "0.01",
        price: "50100",
        client_order_id: Some("123459"),
    };
    assert_eq!(
        coinmate_replace_limit_path(replace.side),
        "/replaceBySellLimit"
    );
    let body = coinmate_replace_limit_form(&replace).expect("replace body");
    assert_eq!(body["orderIdToBeReplaced"], "offline-order-2");
    assert_eq!(body["currencyPair"], "BTC_EUR");
    assert_eq!(body["amount"], "0.01");
    assert_eq!(body["price"], "50100");
    assert_eq!(body["clientOrderId"], "123459");
}

#[test]
fn fixtures_should_parse_public_and_private_shapes() {
    let trading_pairs: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/trading_pairs.json"
    ))
    .expect("trading pairs");
    let rules = parse_symbol_rules(&exchange_id(), &[], &trading_pairs).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].quote_asset, "EUR");
    assert_eq!(rules[0].price_precision, Some(2));

    let order_book: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/orderbook_btc_eur.json"
    ))
    .expect("orderbook");
    assert_eq!(parse_order_book_shape(&order_book).expect("shape"), (1, 1));

    let order_ack: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/order_ack.json"
    ))
    .expect("ack");
    assert_eq!(
        parse_order_ack_id(&order_ack).expect("order id"),
        "offline-order-1"
    );

    let amend_ack: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/parser/amend_order_ack.json"
    ))
    .expect("amend ack");
    assert_eq!(
        parse_amend_order_ack_id(&amend_ack).expect("amend id"),
        "offline-order-3"
    );

    let balances: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/balances.json"
    ))
    .expect("balances");
    assert_eq!(
        parse_balance_assets(&balances).expect("assets"),
        vec!["BTC", "EUR"]
    );

    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/open_orders.json"
    ))
    .expect("open orders");
    assert_eq!(
        parse_open_order_ids(&open_orders).expect("orders"),
        vec!["offline-order-1"]
    );

    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/fills.json"
    ))
    .expect("fills");
    assert_eq!(parse_fill_ids(&fills).expect("fills"), vec!["fill-1"]);

    let boundary: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["funding_operations"], "unsupported");
    assert_eq!(boundary["private_write_mode"], "offline_request_spec_only");
    assert_eq!(
        boundary["advanced_order_boundaries"]["amend_order"],
        "offline_request_spec_parser_only"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_place_orders"],
        "unsupported"
    );
}

#[test]
fn websocket_helpers_should_map_v2_channels_and_fixtures() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol("BTC_EUR"),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let payload = coinmate_public_subscribe_payload(&subscription);
    assert_eq!(payload["event"], "subscribe");
    assert_eq!(payload["data"]["channel"], "order_book-BTC_EUR");
    let policy = coinmate_public_order_book_ws_policy();
    assert_eq!(policy.url, "wss://coinmate.io/api/websocket");
    assert_eq!(policy.channel_template, "order_book-{PAIR}");
    assert_eq!(policy.interval_ms, None);
    assert_eq!(policy.depth, None);
    assert_eq!(policy.sequence, None);
    assert_eq!(policy.checksum, None);
    assert!(policy.resync.contains("/orderBook"));
    assert_eq!(
        coinmate_unsubscribe_payload("order_book-BTC_EUR")["event"],
        "unsubscribe"
    );
    let private = coinmate_private_subscribe_payload(
        PrivateStreamKind::Orders,
        "fixture-client",
        "fixture-public",
        "fixture-private",
        "1700000000000",
        Some("BTC_EUR"),
    )
    .expect("private subscribe");
    assert_eq!(
        private["data"]["channel"],
        "private-open_orders-fixture-client-BTC_EUR"
    );
    assert_eq!(
        private["data"]["signature"],
        "07EA1665FD22A32D201A27B76AD8DC90A32D42E93DB39A0926893F3B07843798"
    );
    assert_eq!(coinmate_reconnect_policy_ms(), (30_000, 45_000, 60_000));

    let ws_book: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/ws/public_order_book.json"
    ))
    .expect("ws book");
    assert_eq!(parse_ws_event_type(&ws_book).as_deref(), Some("data"));
    assert_eq!(parse_ws_order_book_shape(&ws_book).expect("shape"), (1, 1));

    let ws_ack: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/ws/subscribe_success.json"
    ))
    .expect("ws ack");
    assert_eq!(
        parse_ws_event_type(&ws_ack).as_deref(),
        Some("subscribe_success")
    );
}

#[tokio::test]
async fn live_trading_surfaces_should_remain_explicitly_unsupported() {
    let adapter = CoinmateGatewayAdapter::new(CoinmateGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_query_order);
    assert!(!capabilities.supports_open_orders);
    assert!(!capabilities.supports_recent_fills);
    assert!(!capabilities.supports_positions);
    assert!(!capabilities
        .capabilities_v2
        .batch_cancel_orders
        .support
        .is_supported());
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "coinmate.amend_order"
            && endpoint.path.as_deref() == Some("/replaceByBuyLimit|/replaceBySellLimit")));

    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTC_EUR"),
        client_order_id: Some("123456".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
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
            operation: "coinmate.place_order_offline_request_spec_only"
        }
    ));

    let amend = AmendOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("amend"),
        symbol: symbol("BTC_EUR"),
        client_order_id: None,
        exchange_order_id: Some("offline-order-1".to_string()),
        new_client_order_id: Some("123458".to_string()),
        new_quantity: "0.01".to_string(),
    };
    let guard = coinmate_amend_order_live_guard(&amend).expect_err("guarded offline");
    assert!(matches!(
        guard,
        ExchangeApiError::Unsupported {
            operation:
                "coinmate.replace_order_offline_requires_side_price_nonce_and_reconciliation_guard"
        }
    ));
    let error = adapter.amend_order(amend).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation:
                "coinmate.replace_order_offline_requires_side_price_nonce_and_reconciliation_guard"
        }
    ));
}

#[tokio::test]
async fn private_readbacks_should_fail_closed_without_enabled_guard() {
    let adapter = CoinmateGatewayAdapter::new(CoinmateGatewayConfig {
        client_id: Some("fixture-client".to_string()),
        public_key: Some("fixture-public".to_string()),
        private_key: Some("fixture-private".to_string()),
        enabled_private_rest: false,
        ..CoinmateGatewayConfig::default()
    })
    .expect("adapter");

    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol("BTC_EUR"),
            client_order_id: None,
            exchange_order_id: Some("offline-order-1".to_string()),
        })
        .await
        .expect_err("disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "coinmate.query_order"
        }
    ));
}

#[tokio::test]
async fn private_readbacks_should_use_signed_form_rest() {
    let query_order = json!({
        "error": false,
        "data": {
            "id": "offline-order-1",
            "clientOrderId": "123456",
            "currencyPair": "BTC_EUR",
            "type": "BUY",
            "price": "50000.00",
            "amount": "0.00500000",
            "original": "0.01000000",
            "status": "OPEN"
        }
    });
    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/open_orders.json"
    ))
    .expect("open orders");
    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinmate/fills.json"
    ))
    .expect("fills");
    let (base_url, seen) = spawn_rest_server(vec![query_order, open_orders, fills]).await;
    let adapter = CoinmateGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_balances);
    assert!(!capabilities.supports_fees);

    let queried = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol("BTC_EUR"),
            client_order_id: None,
            exchange_order_id: Some("offline-order-1".to_string()),
        })
        .await
        .expect("query");
    let order = queried.order.expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("offline-order-1"));
    assert_eq!(order.client_order_id.as_deref(), Some("123456"));
    assert_eq!(order.status, OrderStatus::Open);
    assert_eq!(order.filled_quantity, "0.005");

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol("BTC_EUR")),
            page: None,
        })
        .await
        .expect("open");
    assert_eq!(open.orders.len(), 1);
    assert_eq!(
        open.orders[0].exchange_order_id.as_deref(),
        Some("offline-order-1")
    );

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol("BTC_EUR")),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("fill-1"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("offline-order-1"));
    assert_eq!(fills.fills[0].fee_amount, Some(0.5));

    let requests = seen.lock().expect("seen").clone();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/orderById");
    assert_eq!(
        requests[0].headers.get("content-type").map(String::as_str),
        Some("application/x-www-form-urlencoded")
    );
    assert_eq!(requests[1].path, "/openOrders");
    assert_eq!(requests[2].path, "/tradeHistory");

    let query_body = parse_form_body(&requests[0].body);
    assert_eq!(
        query_body.get("clientId").map(String::as_str),
        Some("fixture-client")
    );
    assert_eq!(
        query_body.get("publicKey").map(String::as_str),
        Some("fixture-public")
    );
    assert_eq!(
        query_body.get("id").map(String::as_str),
        Some("offline-order-1")
    );
    assert!(query_body
        .get("nonce")
        .is_some_and(|value| !value.is_empty()));
    assert!(query_body
        .get("signature")
        .is_some_and(|value| !value.contains("fixture-private") && !value.is_empty()));

    let open_body = parse_form_body(&requests[1].body);
    assert_eq!(
        open_body.get("currencyPair").map(String::as_str),
        Some("BTC_EUR")
    );
    let fills_body = parse_form_body(&requests[2].body);
    assert_eq!(fills_body.get("limit").map(String::as_str), Some("100"));
    assert_eq!(
        fills_body.get("currencyPair").map(String::as_str),
        Some("BTC_EUR")
    );
}

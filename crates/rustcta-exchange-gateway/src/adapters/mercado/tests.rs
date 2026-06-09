use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};
use serde_json::Value;

use super::parser::{
    mercado_canonical_pair, mercado_symbol, parse_order_ack_id, parse_order_book_shape,
    parse_symbol_rules,
};
use super::private::{
    create_order_request_spec_fixture, mercado_cancel_all_path, mercado_order_path,
    mercado_orders_path, mercado_place_order_body, mercado_recent_fills_path,
};
use super::private_parser::{
    parse_balance_assets, parse_fill_ids, parse_open_order_ids, parse_open_orders,
    parse_order_state, parse_recent_fills,
};
use super::signing::{mercado_bearer_authorization, mercado_bearer_request_fingerprint};
use super::streams::{mercado_public_subscribe_payload, mercado_reconnect_policy_ms};
use super::{MercadoGatewayAdapter, MercadoGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("mercado").expect("exchange")
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
    let (base, quote) = mercado_canonical_pair(symbol).expect("canonical pair");
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, symbol)
            .expect("symbol"),
    }
}

#[test]
fn symbol_normalization_should_cover_brl_fiat_markets() {
    assert_eq!(mercado_symbol("btc/brl"), "BTC-BRL");
    assert_eq!(mercado_symbol("ETH_BRL"), "ETH-BRL");
    assert_eq!(
        mercado_canonical_pair("BTCBRL").expect("brl"),
        ("BTC".to_string(), "BRL".to_string())
    );
    assert_eq!(
        mercado_canonical_pair("BTC-BRL").expect("brl"),
        ("BTC".to_string(), "BRL".to_string())
    );
}

#[test]
fn request_spec_auth_and_paths_should_stay_secret_free() {
    let spec = create_order_request_spec_fixture();
    assert_eq!(spec["path"], "/accounts/<accountId>/BTC-BRL/orders");
    assert_eq!(spec["headers"]["Authorization"], "Bearer <redacted>");
    assert_eq!(
        mercado_orders_path("offline-account", "btc/brl"),
        "/accounts/offline-account/BTC-BRL/orders"
    );
    assert_eq!(
        mercado_order_path("offline-account", "BTC-BRL", "order-1"),
        "/accounts/offline-account/BTC-BRL/orders/order-1"
    );
    assert_eq!(
        mercado_cancel_all_path("offline-account"),
        "/accounts/offline-account/cancel_all_open_orders"
    );
    assert_eq!(
        mercado_recent_fills_path("offline-account", "BTC-BRL"),
        "/accounts/offline-account/BTC-BRL/trades"
    );
    assert_eq!(
        mercado_bearer_authorization("<redacted>"),
        "Bearer <redacted>"
    );
    assert_eq!(
        mercado_bearer_request_fingerprint("post", "/accounts/a/BTC-BRL/orders", "{}"),
        "POST /accounts/a/BTC-BRL/orders {}"
    );
}

#[test]
fn place_order_body_should_use_qty_cost_and_external_id() {
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTC-BRL"),
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
    let body = mercado_place_order_body(&request).expect("body");
    assert_eq!(body["side"], "buy");
    assert_eq!(body["type"], "limit");
    assert_eq!(body["qty"], "0.01");
    assert_eq!(body["limitPrice"], "350000");
    assert_eq!(body["externalId"], "offline-fixture");

    let mut market = request;
    market.order_type = OrderType::Market;
    market.price = None;
    market.quote_quantity = Some("1000".to_string());
    let body = mercado_place_order_body(&market).expect("market");
    assert_eq!(body["cost"], "1000");
    assert!(body.get("limitPrice").is_none());
}

#[test]
fn fixtures_should_parse_orderbook_order_ack_and_boundary() {
    let order_book: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mercado/orderbook_btc_brl.json"
    ))
    .expect("orderbook");
    assert_eq!(parse_order_book_shape(&order_book).expect("shape"), (1, 1));

    let order_ack: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mercado/order_ack.json"
    ))
    .expect("ack");
    assert_eq!(
        parse_order_ack_id(&order_ack).expect("oid"),
        "01HCDAA7YJ68ZJ0FTEPR7DYDS1"
    );

    let boundary: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mercado/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["funding_operations"], "unsupported");
    assert_eq!(boundary["private_write_mode"], "offline_request_spec_only");

    let symbols: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mercado/symbols_success.json"
    ))
    .expect("symbols");
    let rules = parse_symbol_rules(&exchange_id(), &[], &symbols).expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].quote_asset, "BRL");
}

#[test]
fn private_read_fixtures_should_cover_balances_open_orders_and_fills() {
    let balances: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mercado/balances.json"
    ))
    .expect("balances");
    assert_eq!(
        parse_balance_assets(&balances).expect("assets"),
        vec!["BTC", "BRL"]
    );

    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mercado/open_orders.json"
    ))
    .expect("open orders");
    assert_eq!(
        parse_open_order_ids(&open_orders).expect("orders"),
        vec!["01HCDAA7YJ68ZJ0FTEPR7DYDS1"]
    );

    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mercado/fills.json"
    ))
    .expect("fills");
    assert_eq!(parse_fill_ids(&fills).expect("fills"), vec!["fill-1"]);

    let order = parse_order_state(&exchange_id(), None, &open_orders["orders"][0]).expect("order");
    assert_eq!(
        order.exchange_order_id.as_deref(),
        Some("01HCDAA7YJ68ZJ0FTEPR7DYDS1")
    );
    assert_eq!(order.status, OrderStatus::Open);
    assert_eq!(order.price.as_deref(), Some("350000"));

    let open_success: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mercado/open_orders_success.json"
    ))
    .expect("open success");
    let orders =
        parse_open_orders(&exchange_id(), Some(&symbol("BTC-BRL")), &open_success).expect("orders");
    assert_eq!(orders.len(), 1);
    assert_eq!(
        orders[0].client_order_id.as_deref(),
        Some("offline-fixture")
    );

    let fills_success: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mercado/fills_success.json"
    ))
    .expect("fills success");
    let fills = parse_recent_fills(
        &exchange_id(),
        context("fills").tenant_id.expect("tenant"),
        context("fills").account_id.expect("account"),
        &symbol("BTC-BRL"),
        &fills_success,
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("offline-trade-1"));
    assert_eq!(fills[0].fee_asset.as_deref(), Some("BRL"));
    assert_eq!(fills[0].price, 350000.0);
}

#[test]
fn websocket_helpers_should_map_public_channels() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol("BTC-BRL"),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = mercado_public_subscribe_payload(&subscription);
    assert_eq!(payload["type"], "subscribe");
    assert_eq!(payload["symbol"], "BTC-BRL");
    assert_eq!(payload["channel"], "orderbook");
    assert_eq!(mercado_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[tokio::test]
async fn live_trading_surfaces_should_remain_explicitly_unsupported() {
    let adapter = MercadoGatewayAdapter::new(MercadoGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_query_order);
    assert!(!capabilities.supports_open_orders);
    assert!(!capabilities.supports_recent_fills);
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTC-BRL"),
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
            operation: "mercado.place_order_offline_request_spec_only"
        }
    ));

    let query_error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol("BTC-BRL"),
            client_order_id: None,
            exchange_order_id: Some("01HCDAA7YJ68ZJ0FTEPR7DYDS1".to_string()),
        })
        .await
        .expect_err("private rest disabled");
    assert!(matches!(
        query_error,
        ExchangeApiError::Unsupported {
            operation: "mercado.query_order"
        }
    ));
}

#[tokio::test]
async fn private_read_capabilities_should_enable_only_with_bearer_account_config() {
    let config = MercadoGatewayConfig {
        enabled_private_rest: true,
        bearer_token: Some("bearer".to_string()),
        account_id: Some("account-id".to_string()),
        ..MercadoGatewayConfig::default()
    };
    let adapter = MercadoGatewayAdapter::new(config).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);

    let query_client_id_error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-client-id"),
            symbol: symbol("BTC-BRL"),
            client_order_id: Some("external".to_string()),
            exchange_order_id: None,
        })
        .await
        .expect_err("client id query unsupported before network");
    assert!(matches!(
        query_client_id_error,
        ExchangeApiError::Unsupported {
            operation: "mercado.query_order.client_order_id"
        }
    ));

    let open_page_error = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-page"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol("BTC-BRL")),
            page: Some(rustcta_exchange_api::PageRequest {
                limit: Some(10),
                cursor: None,
            }),
        })
        .await
        .expect_err("pagination unsupported before network");
    assert!(matches!(
        open_page_error,
        ExchangeApiError::Unsupported {
            operation: "mercado.get_open_orders.pagination"
        }
    ));

    let fills_filter_error = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills-filter"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol("BTC-BRL")),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: Some("trade".to_string()),
            start_time: None,
            end_time: None,
            limit: Some(10),
            page: None,
        })
        .await
        .expect_err("from-trade filter unsupported before network");
    assert!(matches!(
        fills_filter_error,
        ExchangeApiError::Unsupported {
            operation: "mercado.get_recent_fills.filter_params"
        }
    ));
}

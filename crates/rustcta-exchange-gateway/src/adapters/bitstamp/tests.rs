use rustcta_exchange_api::{
    AccountId, PublicStreamKind, PublicStreamSubscription, RequestContext, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderStatus};
use serde_json::json;

use super::parser::{
    parse_balances, parse_fees, parse_fills, parse_order_state, parse_orderbook_snapshot,
    parse_symbol_rules,
};
use super::signing::bitstamp_signature;
use super::streams::{
    bitstamp_private_subscribe_payload, bitstamp_public_channel, bitstamp_public_subscribe_payload,
};

#[test]
fn bitstamp_parser_should_map_markets_and_book() {
    let exchange = ExchangeId::new("bitstamp").expect("exchange");
    let markets = json!([{
        "market_symbol": "btcusd",
        "market_type": "SPOT",
        "base_currency": "BTC",
        "counter_currency": "USD",
        "base_decimals": 8,
        "counter_decimals": 2,
        "minimum_order_value": "10.0",
        "trading": "Enabled"
    }]);
    let rules = parse_symbol_rules(&exchange, &markets).expect("rules");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "btcusd");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.01"));

    let book = json!({
        "timestamp": "1700000000",
        "bids": [["100.0", "1.0"]],
        "asks": [["101.0", "2.0"]]
    });
    let snapshot =
        parse_orderbook_snapshot(&exchange, symbol_scope(), Some(1), &book).expect("book");
    assert_eq!(snapshot.bids[0].price, 100.0);
    assert_eq!(snapshot.asks[0].quantity, 2.0);
}

#[test]
fn bitstamp_signing_and_ws_payload_should_match_v2_shape() {
    let signature = bitstamp_signature(
        "key",
        "secret",
        "POST",
        "www.bitstamp.net",
        "/api/v2/open_orders/",
        "",
        "application/x-www-form-urlencoded",
        "00000000-0000-0000-0000-000000000000",
        "1700000000000",
        "",
    )
    .expect("signature");
    assert_eq!(signature.len(), 64);

    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: RequestContext::new(chrono::Utc::now()),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    assert_eq!(
        bitstamp_public_channel(&subscription).expect("channel"),
        "diff_order_book_btcusd"
    );
    assert_eq!(
        bitstamp_public_subscribe_payload(&subscription).expect("payload"),
        json!({
            "event": "bts:subscribe",
            "data": {"channel": "diff_order_book_btcusd"}
        })
    );
}

#[test]
fn bitstamp_private_parsers_should_map_balances_fees_orders_and_fills() {
    let exchange = ExchangeId::new("bitstamp").expect("exchange");
    let tenant = TenantId::new("tenant").expect("tenant");
    let account = AccountId::new("account").expect("account");
    let symbol = symbol_scope();

    let balances = parse_balances(
        &exchange,
        tenant.clone(),
        account.clone(),
        &[],
        &json!([{
            "currency": "btc",
            "total": "1.25",
            "available": "1.0",
            "reserved": "0.25"
        }]),
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].asset, "BTC");
    assert_eq!(balances[0].balances[0].locked, 0.25);

    let fees = parse_fees(
        &exchange,
        std::slice::from_ref(&symbol),
        &json!({"maker": "0.10", "taker": "0.20"}),
    )
    .expect("fees");
    assert_eq!(fees[0].maker_rate, "0.001");
    assert_eq!(fees[0].taker_rate, "0.002");

    let order = parse_order_state(
        &exchange,
        Some(&symbol),
        &json!({
            "id": "123",
            "client_order_id": "client-1",
            "type": "0",
            "status": "Open",
            "amount": "2.0",
            "amount_remaining": "1.5",
            "price": "100.0"
        }),
    )
    .expect("order");
    assert_eq!(order.status, OrderStatus::Open);
    assert_eq!(order.filled_quantity, "0.5");

    let fills = parse_fills(
        &exchange,
        tenant,
        account,
        &symbol,
        &json!([{
            "id": "fill-1",
            "order_id": "123",
            "type": "0",
            "price": "100.0",
            "amount": "0.5",
            "fee": "0.01",
            "fee_currency": "USD",
            "datetime": "2024-01-01 00:00:00"
        }]),
    )
    .expect("fills");
    assert_eq!(fills[0].order_id.as_deref(), Some("123"));
    assert_eq!(fills[0].quote_quantity, Some(50.0));
}

#[test]
fn bitstamp_private_ws_payload_should_include_token_auth() {
    assert_eq!(
        bitstamp_private_subscribe_payload("private-my_orders", "token-123"),
        json!({
            "event": "bts:subscribe",
            "data": {"channel": "private-my_orders", "auth": "token-123"}
        })
    );
}

fn symbol_scope() -> rustcta_exchange_api::SymbolScope {
    let exchange = ExchangeId::new("bitstamp").expect("exchange");
    rustcta_exchange_api::SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange, MarketType::Spot, "btcusd").expect("symbol"),
    }
}

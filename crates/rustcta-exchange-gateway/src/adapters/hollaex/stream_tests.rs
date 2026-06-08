use rustcta_exchange_api::HeartbeatDirection;
use serde_json::json;

use super::streams::{
    client_ping_payload, heartbeat_policy, parse_public_orderbook_notification,
    private_ws_url_with_hmac, public_orderbook_subscribe_payload,
    public_orderbook_unsubscribe_payload,
};
use super::test_support::exchange_id;

fn load_ws_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "orderbook_update.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/hollaex/ws/orderbook_update.json")
        }
        _ => panic!("unknown hollaex ws fixture {name}"),
    };
    serde_json::from_str(text).expect("ws fixture")
}

#[test]
fn hollaex_ws_payloads_should_cover_subscribe_unsubscribe_and_auth() {
    let symbols = vec!["xht-usdt".to_string()];
    assert_eq!(
        public_orderbook_subscribe_payload(&symbols),
        json!({"op": "subscribe", "args": ["orderbook:xht-usdt"]})
    );
    assert_eq!(
        public_orderbook_unsubscribe_payload(&symbols),
        json!({"op": "unsubscribe", "args": ["orderbook:xht-usdt"]})
    );
    assert_eq!(client_ping_payload(), json!({"op": "ping"}));

    let url = private_ws_url_with_hmac(
        "wss://api.hollaex.com/stream",
        "test-key",
        "test-secret",
        1_710_000_000,
    )
    .expect("private ws auth url");
    assert!(url.contains("api-key=test-key"));
    assert!(url.contains(
        "api-signature=f99cca59fee066d50d972736fde368157df8a23a4a1d09bd48d19a09c491f986"
    ));
    assert!(!url.contains("test-secret"));
}

#[test]
fn hollaex_ws_orderbook_fixture_should_parse_snapshot_for_rest_resync() {
    let parsed = parse_public_orderbook_notification(
        &exchange_id(),
        &load_ws_fixture("orderbook_update.json"),
    )
    .expect("orderbook notification");
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].order_book.bids[0].price, 0.1005);
    assert_eq!(parsed[0].order_book.asks[0].quantity, 42.0);
}

#[test]
fn hollaex_ws_heartbeat_policy_should_send_client_ping() {
    let policy = heartbeat_policy();
    assert_eq!(policy.direction, HeartbeatDirection::ClientPing);
    assert_eq!(policy.ping_interval_ms, 30_000);
    assert_eq!(policy.stale_message_ms, 60_000);
}

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::core::exchange::Exchange;
use crate::core::types::{MarketType, OrderRequest, OrderSide, OrderStatus, OrderType};

use super::quoting::QuotePlan;

#[derive(Debug, Clone)]
pub struct LiveOrder {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub level: usize,
    pub side: OrderSide,
    pub position_side: Option<String>,
    pub reduce_only: bool,
    pub price: f64,
    pub quantity: f64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionReport {
    pub canceled: usize,
    pub placed: usize,
    pub skipped: usize,
}

#[derive(Clone)]
pub struct ReconcileParams<'a> {
    pub exchange: &'a dyn Exchange,
    pub symbol: &'a str,
    pub market_type: MarketType,
    pub post_only: bool,
    pub time_in_force: &'a str,
    pub tick_size: f64,
    pub step_size: f64,
    pub order_ttl_ms: u64,
    pub min_requote_interval_ms: u64,
    pub reprice_threshold_ticks: u32,
    pub dual_position_mode: bool,
    pub dry_run: bool,
    pub now: DateTime<Utc>,
}

pub async fn reconcile_orders(
    params: ReconcileParams<'_>,
    live_orders: &mut Vec<LiveOrder>,
    desired_quotes: &[QuotePlan],
) -> Result<ExecutionReport> {
    if params.dry_run {
        return Ok(ExecutionReport {
            skipped: desired_quotes.len(),
            ..ExecutionReport::default()
        });
    }

    let mut report = ExecutionReport::default();
    let ttl_ms = params.order_ttl_ms as i64;
    let desired_by_key: HashMap<OrderKey, &QuotePlan> = desired_quotes
        .iter()
        .map(|quote| (OrderKey::from_quote(quote), quote))
        .collect();

    let mut survivors = Vec::new();
    for order in live_orders.drain(..) {
        let age_ms = params.now.timestamp_millis() - order.created_at.timestamp_millis();
        let expired = age_ms >= ttl_ms;
        let key = OrderKey::from_live(&order);
        let keep = if let Some(quote) = desired_by_key.get(&key) {
            should_keep_existing_order(
                &order,
                quote,
                age_ms,
                expired,
                params.tick_size,
                params.step_size,
                params.min_requote_interval_ms,
                params.reprice_threshold_ticks,
            )
        } else {
            false
        };

        if keep {
            survivors.push(order);
        } else {
            match params
                .exchange
                .cancel_order(&order.order_id, params.symbol, params.market_type)
                .await
            {
                Ok(_) => {
                    report.canceled += 1;
                }
                Err(err) if is_unknown_order_error(&err.to_string()) => {
                    log::warn!(
                        "[beta_hedge_mm] drop stale local order id={} client_id={} after exchange reported unknown order",
                        order.order_id,
                        order.client_order_id.as_deref().unwrap_or("")
                    );
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    let live_keys: HashSet<OrderKey> = survivors.iter().map(OrderKey::from_live).collect();
    for quote in desired_quotes {
        let key = OrderKey::from_quote(quote);
        if live_keys.contains(&key) {
            continue;
        }

        let client_order_id =
            build_client_order_id(params.symbol, quote.side, params.now, quote.level as u16);

        let mut req = OrderRequest::new(
            params.symbol.to_string(),
            quote.side,
            OrderType::Limit,
            quote.quantity,
            Some(quote.price),
            params.market_type,
        );
        req.client_order_id = Some(client_order_id.clone());
        req.post_only = Some(params.post_only);
        req.time_in_force = Some(params.time_in_force.to_string());
        req.reduce_only = if params.dual_position_mode {
            None
        } else {
            Some(quote.reduce_only)
        };
        if params.dual_position_mode {
            let mut params_map = HashMap::new();
            if let Some(position_side) = &quote.position_side {
                params_map.insert("positionSide".to_string(), position_side.clone());
            }
            req.params = Some(params_map);
        }

        let order = params.exchange.create_order(req).await?;
        survivors.push(LiveOrder {
            order_id: order.id,
            client_order_id: Some(client_order_id),
            level: quote.level,
            side: quote.side,
            position_side: quote.position_side.clone(),
            reduce_only: quote.reduce_only,
            price: quote.price,
            quantity: quote.quantity,
            created_at: params.now,
        });
        report.placed += 1;
    }

    *live_orders = survivors;
    Ok(report)
}

fn should_keep_existing_order(
    order: &LiveOrder,
    quote: &QuotePlan,
    age_ms: i64,
    expired: bool,
    tick_size: f64,
    step_size: f64,
    min_requote_interval_ms: u64,
    reprice_threshold_ticks: u32,
) -> bool {
    if expired {
        return false;
    }

    let tick = tick_size.max(1e-9);
    let step = step_size.max(1e-9);
    let price_diff_ticks = ((order.price - quote.price).abs() / tick).round();
    let qty_diff_steps = ((order.quantity - quote.quantity).abs() / step).round();
    let materially_changed =
        price_diff_ticks >= reprice_threshold_ticks.max(1) as f64 || qty_diff_steps >= 1.0;

    if !materially_changed {
        return true;
    }

    age_ms < min_requote_interval_ms as i64
}

fn is_unknown_order_error(error: &str) -> bool {
    error.contains("Unknown order")
        || error.contains("Unknown order sent")
        || error.contains("\"code\":-2011")
        || error.contains("订单未找到")
}

pub async fn cancel_symbol_orders(
    exchange: &dyn Exchange,
    symbol: &str,
    market_type: MarketType,
    dry_run: bool,
) -> Result<usize> {
    if dry_run {
        return Ok(0);
    }
    let canceled = exchange
        .cancel_all_orders(Some(symbol), market_type)
        .await?;
    Ok(canceled.len())
}

pub fn build_client_order_id(
    symbol: &str,
    side: OrderSide,
    now: DateTime<Utc>,
    nonce: u16,
) -> String {
    let symbol_slug = symbol.replace('/', "").to_ascii_lowercase();
    let side_slug = match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    };
    let timestamp = now.timestamp_millis().rem_euclid(1_000_000_000);
    let client_order_id = format!(
        "betahedge_{}_{}_{}_{:02x}",
        symbol_slug,
        side_slug,
        timestamp,
        nonce % 0xff
    );
    debug_assert!(client_order_id.len() <= 36);
    client_order_id
}

pub fn apply_order_update(
    live_orders: &mut Vec<LiveOrder>,
    order_id: &str,
    client_order_id: Option<&str>,
    status: OrderStatus,
    remaining_qty: Option<f64>,
    event_time: DateTime<Utc>,
) {
    let matching_index = live_orders.iter().position(|order| {
        order.order_id == order_id
            || client_order_id
                .map(|client_id| {
                    order
                        .client_order_id
                        .as_deref()
                        .map(|stored| stored == client_id)
                        .unwrap_or(false)
                })
                .unwrap_or(false)
    });

    let Some(index) = matching_index else {
        return;
    };

    match status {
        OrderStatus::Closed
        | OrderStatus::Canceled
        | OrderStatus::Expired
        | OrderStatus::Rejected => {
            live_orders.remove(index);
        }
        OrderStatus::PartiallyFilled | OrderStatus::Open | OrderStatus::Pending => {
            if let Some(qty) = remaining_qty {
                live_orders[index].quantity = qty.max(0.0);
            }
            live_orders[index].created_at = event_time;
        }
        OrderStatus::Triggered => {}
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OrderKey {
    level: usize,
    side: OrderSide,
    position_side: Option<String>,
    reduce_only: bool,
}

impl OrderKey {
    fn from_quote(quote: &QuotePlan) -> Self {
        Self {
            level: quote.level,
            side: quote.side,
            position_side: quote.position_side.clone(),
            reduce_only: quote.reduce_only,
        }
    }

    fn from_live(order: &LiveOrder) -> Self {
        Self {
            level: order.level,
            side: order.side,
            position_side: order.position_side.clone(),
            reduce_only: order.reduce_only,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::OrderStatus;

    #[test]
    fn client_order_id_uses_requested_prefix_and_fits_binance_limit() {
        let id = build_client_order_id(
            "DOGE/USDC",
            OrderSide::Buy,
            DateTime::from_timestamp(1_712_345_678, 0).unwrap(),
            42,
        );
        assert!(id.starts_with("betahedge_dogeusdc_buy_"));
        assert!(id.len() <= 36);
    }

    #[test]
    fn order_update_removes_terminal_order_from_local_book() {
        let mut live_orders = vec![LiveOrder {
            order_id: "123".to_string(),
            client_order_id: Some("betahedge_dogeusdc_buy_04010000_0000".to_string()),
            level: 0,
            side: OrderSide::Buy,
            position_side: Some("LONG".to_string()),
            reduce_only: false,
            price: 0.1,
            quantity: 100.0,
            created_at: Utc::now(),
        }];

        apply_order_update(
            &mut live_orders,
            "123",
            None,
            OrderStatus::Closed,
            Some(0.0),
            Utc::now(),
        );

        assert!(live_orders.is_empty());
    }

    #[test]
    fn fresh_order_is_kept_even_if_quote_moves_small_amount() {
        let order = LiveOrder {
            order_id: "123".to_string(),
            client_order_id: Some("betahedge_dogeusdc_buy_04010000_0000".to_string()),
            level: 0,
            side: OrderSide::Buy,
            position_side: Some("LONG".to_string()),
            reduce_only: false,
            price: 0.1000,
            quantity: 80.0,
            created_at: Utc::now(),
        };
        let quote = QuotePlan {
            level: 0,
            side: OrderSide::Buy,
            price: 0.1003,
            quantity: 80.0,
            position_side: Some("LONG".to_string()),
            reduce_only: false,
            reason: "test".to_string(),
        };

        assert!(should_keep_existing_order(
            &order, &quote, 3_000, false, 0.0001, 1.0, 8_000, 4
        ));
    }

    #[test]
    fn old_order_is_requoted_after_material_change() {
        let order = LiveOrder {
            order_id: "123".to_string(),
            client_order_id: Some("betahedge_dogeusdc_buy_04010000_0000".to_string()),
            level: 0,
            side: OrderSide::Buy,
            position_side: Some("LONG".to_string()),
            reduce_only: false,
            price: 0.1000,
            quantity: 80.0,
            created_at: Utc::now(),
        };
        let quote = QuotePlan {
            level: 0,
            side: OrderSide::Buy,
            price: 0.1005,
            quantity: 80.0,
            position_side: Some("LONG".to_string()),
            reduce_only: false,
            reason: "test".to_string(),
        };

        assert!(!should_keep_existing_order(
            &order, &quote, 10_000, false, 0.0001, 1.0, 8_000, 4
        ));
    }

    #[test]
    fn order_update_matches_client_order_id() {
        let mut live_orders = vec![LiveOrder {
            order_id: "123".to_string(),
            client_order_id: Some("betahedge_dogeusdc_sell_04010000_0001".to_string()),
            level: 1,
            side: OrderSide::Sell,
            position_side: Some("SHORT".to_string()),
            reduce_only: false,
            price: 0.1010,
            quantity: 90.0,
            created_at: Utc::now(),
        }];

        apply_order_update(
            &mut live_orders,
            "not_the_exchange_id",
            Some("betahedge_dogeusdc_sell_04010000_0001"),
            OrderStatus::Canceled,
            Some(0.0),
            Utc::now(),
        );

        assert!(live_orders.is_empty());
    }

    #[test]
    fn unknown_order_error_is_detected() {
        assert!(is_unknown_order_error(
            "API错误: 400 - {\"code\":-2011,\"msg\":\"Unknown order sent.\"}"
        ));
    }
}

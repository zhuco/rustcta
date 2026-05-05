pub mod config;
pub mod controller;
pub mod engine;
pub mod ledger;
pub mod multi;
pub mod risk;
pub mod sim;

pub use config::*;
pub use controller::SolusdcHedgedGridStrategy;
pub use engine::*;
pub use ledger::*;
pub use multi::*;
pub use risk::*;
pub use sim::*;

#[cfg(test)]
mod solusdc_hedged_grid {
    use super::*;
    use chrono::Utc;
    use std::collections::HashSet;

    fn test_config() -> StrategyConfig {
        StrategyConfig {
            symbol: "SOLUSDC".to_string(),
            require_hedge_mode: true,
            price_reference: PriceReference::Mid,
            risk_reference: RiskReference::Mark,
            grid: GridConfig {
                levels_per_side: 3,
                grid_spacing_pct: 0.001,
                grid_spacing_abs: None,
                order_notional: 10.0,
                order_qty: None,
                strict_pairing: false,
                fill_remaining_slots_with_opens: true,
            },
            follow: FollowConfig {
                max_gap_steps: 1.0,
                follow_cooldown_ms: 0,
                max_follow_actions_per_minute: 100,
            },
            execution: ExecutionConfig {
                cooldown_ms: 0,
                post_only: true,
                post_only_retries: 3,
            },
            precision: PrecisionConfig {
                tick_size: 0.01,
                step_size: 0.001,
                min_qty: Some(0.001),
                min_notional: Some(5.0),
                price_digits: Some(2),
                qty_digits: Some(3),
            },
            fees: FeeConfig {
                maker_fee: 0.0,
                taker_fee: 0.0004,
            },
            risk: RiskLimits {
                max_net_notional: 1000.0,
                max_total_notional: 2000.0,
                margin_ratio_limit: 0.8,
                funding_rate_limit: 0.003,
                funding_cost_limit: 5.0,
            },
        }
    }

    fn snapshot(price: f64) -> MarketSnapshot {
        MarketSnapshot {
            best_bid: price * 0.999,
            best_ask: price * 1.001,
            last_price: price,
            mark_price: price,
            timestamp: Utc::now(),
        }
    }

    fn step_price(base: f64, spacing: f64, steps: usize, up: bool) -> f64 {
        let offset = spacing * steps as f64;
        if up {
            base * (1.0 + offset)
        } else {
            base * (1.0 - offset)
        }
    }

    fn price_key(price: f64) -> i64 {
        (price * 10_000.0).round() as i64
    }

    fn action_counts(actions: &[EngineAction]) -> (usize, usize) {
        let place = actions
            .iter()
            .filter(|action| matches!(action, EngineAction::Place(_)))
            .count();
        let cancel = actions
            .iter()
            .filter(|action| matches!(action, EngineAction::Cancel { .. }))
            .count();
        (place, cancel)
    }

    #[test]
    fn inventory_shortage_should_skip_close_orders() {
        let config = test_config();
        let levels = config.grid.levels_per_side;
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 0.21,
            short_qty: 0.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 0.21,
            short_available: 0.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        engine.rebuild_grid(&snapshot(100.0));

        let sell_orders = engine.sell_orders();
        let close_orders: Vec<_> = sell_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::CloseLongSell)
            .collect();
        let open_orders: Vec<_> = sell_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::OpenShortSell)
            .collect();

        let close_qty: f64 = close_orders.iter().map(|o| o.qty).sum();
        assert_eq!(close_orders.len(), 2);
        assert_eq!(open_orders.len(), levels);
        assert!(close_qty <= 0.21 + 1e-6);
    }

    #[test]
    fn initial_close_orders_should_bracket_reference() {
        let config = test_config();
        let levels = config.grid.levels_per_side;
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 1.0,
            short_qty: 1.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 1.0,
            short_available: 1.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        let snap = snapshot(100.0);
        let reference = snap.mid();
        engine.rebuild_grid(&snap);

        let buy_orders = engine.buy_orders();
        let close_shorts: Vec<_> = buy_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::CloseShortBuy)
            .collect();
        let sell_orders = engine.sell_orders();
        let close_longs: Vec<_> = sell_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::CloseLongSell)
            .collect();

        assert_eq!(close_shorts.len(), levels);
        assert_eq!(close_longs.len(), levels);
        assert!(close_shorts.iter().all(|o| o.price <= reference));
        assert!(close_longs.iter().all(|o| o.price >= reference));
    }

    #[test]
    fn open_long_fill_should_roll_grid() {
        let config = test_config();
        let spacing = config.grid.grid_spacing_pct;
        let levels = config.grid.levels_per_side;
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 1.0,
            short_qty: 1.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 1.0,
            short_available: 1.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        let snap = snapshot(100.0);
        engine.rebuild_grid(&snap);
        let buy_order = engine.buy_orders().first().cloned().expect("buy");
        let fill = FillEvent {
            order_id: buy_order.id.clone(),
            intent: buy_order.intent,
            fill_qty: buy_order.qty,
            fill_price: buy_order.price,
            timestamp: Utc::now(),
            partial: false,
        };
        let actions = engine.handle_fill(fill, &snap);
        let (place_count, cancel_count) = action_counts(&actions);
        assert_eq!(place_count, 2);
        assert_eq!(cancel_count, 1);

        let expected_open = step_price(buy_order.price, spacing, levels, false);
        let expected_close = step_price(buy_order.price, spacing, 1, true);

        let buy_orders = engine.buy_orders();
        let open_longs: Vec<_> = buy_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::OpenLongBuy)
            .collect();
        assert_eq!(open_longs.len(), levels);
        assert!(open_longs
            .iter()
            .any(|o| (o.price - expected_open).abs() < 0.05));

        let sell_orders = engine.sell_orders();
        let close_longs: Vec<_> = sell_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::CloseLongSell)
            .collect();
        assert_eq!(close_longs.len(), levels);
        assert!(close_longs
            .iter()
            .any(|o| (o.price - expected_close).abs() < 0.05));
    }

    #[test]
    fn open_short_fill_should_roll_grid() {
        let config = test_config();
        let spacing = config.grid.grid_spacing_pct;
        let levels = config.grid.levels_per_side;
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 1.0,
            short_qty: 1.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 1.0,
            short_available: 1.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        let snap = snapshot(100.0);
        engine.rebuild_grid(&snap);

        let sell_order = engine
            .sell_orders()
            .iter()
            .find(|o| o.intent == OrderIntent::OpenShortSell)
            .cloned()
            .expect("sell");
        let fill = FillEvent {
            order_id: sell_order.id.clone(),
            intent: sell_order.intent,
            fill_qty: sell_order.qty,
            fill_price: sell_order.price,
            timestamp: Utc::now(),
            partial: false,
        };
        let actions = engine.handle_fill(fill, &snap);
        let (place_count, cancel_count) = action_counts(&actions);
        assert_eq!(place_count, 2);
        assert_eq!(cancel_count, 1);

        let expected_open = step_price(sell_order.price, spacing, levels, true);
        let expected_close = step_price(sell_order.price, spacing, 1, false);

        let sell_orders = engine.sell_orders();
        let open_shorts: Vec<_> = sell_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::OpenShortSell)
            .collect();
        assert_eq!(open_shorts.len(), levels);
        assert!(open_shorts
            .iter()
            .any(|o| (o.price - expected_open).abs() < 0.05));

        let buy_orders = engine.buy_orders();
        let close_shorts: Vec<_> = buy_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::CloseShortBuy)
            .collect();
        assert_eq!(close_shorts.len(), levels);
        assert!(close_shorts
            .iter()
            .any(|o| (o.price - expected_close).abs() < 0.05));
    }

    #[test]
    fn close_long_fill_should_roll_grid() {
        let config = test_config();
        let spacing = config.grid.grid_spacing_pct;
        let levels = config.grid.levels_per_side;
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 1.0,
            short_qty: 0.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 1.0,
            short_available: 0.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        let snap = snapshot(100.0);
        engine.rebuild_grid(&snap);

        let close_order = engine
            .sell_orders()
            .iter()
            .find(|o| o.intent == OrderIntent::CloseLongSell)
            .cloned()
            .expect("close");
        let fill = FillEvent {
            order_id: close_order.id.clone(),
            intent: close_order.intent,
            fill_qty: close_order.qty,
            fill_price: close_order.price,
            timestamp: Utc::now(),
            partial: false,
        };
        let actions = engine.handle_fill(fill, &snap);
        let (place_count, cancel_count) = action_counts(&actions);
        assert_eq!(place_count, 2);
        assert_eq!(cancel_count, 1);

        let expected_open = step_price(close_order.price, spacing, 1, false);
        let expected_close = step_price(close_order.price, spacing, levels, true);

        let buy_orders = engine.buy_orders();
        let open_longs: Vec<_> = buy_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::OpenLongBuy)
            .collect();
        assert_eq!(open_longs.len(), levels);
        assert!(open_longs
            .iter()
            .any(|o| (o.price - expected_open).abs() < 0.05));

        let sell_orders = engine.sell_orders();
        let close_longs: Vec<_> = sell_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::CloseLongSell)
            .collect();
        assert_eq!(close_longs.len(), levels);
        assert!(close_longs
            .iter()
            .any(|o| (o.price - expected_close).abs() < 0.05));
    }

    #[test]
    fn close_short_fill_should_roll_grid() {
        let config = test_config();
        let spacing = config.grid.grid_spacing_pct;
        let levels = config.grid.levels_per_side;
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 0.0,
            short_qty: 1.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 0.0,
            short_available: 1.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        let snap = snapshot(100.0);
        engine.rebuild_grid(&snap);

        let close_order = engine
            .buy_orders()
            .iter()
            .find(|o| o.intent == OrderIntent::CloseShortBuy)
            .cloned()
            .expect("close");
        let fill = FillEvent {
            order_id: close_order.id.clone(),
            intent: close_order.intent,
            fill_qty: close_order.qty,
            fill_price: close_order.price,
            timestamp: Utc::now(),
            partial: false,
        };
        let actions = engine.handle_fill(fill, &snap);
        let (place_count, cancel_count) = action_counts(&actions);
        assert_eq!(place_count, 2);
        assert_eq!(cancel_count, 1);

        let expected_open = step_price(close_order.price, spacing, 1, true);
        let expected_close = step_price(close_order.price, spacing, levels, false);

        let sell_orders = engine.sell_orders();
        let open_shorts: Vec<_> = sell_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::OpenShortSell)
            .collect();
        assert_eq!(open_shorts.len(), levels);
        assert!(open_shorts
            .iter()
            .any(|o| (o.price - expected_open).abs() < 0.05));

        let buy_orders = engine.buy_orders();
        let close_shorts: Vec<_> = buy_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::CloseShortBuy)
            .collect();
        assert_eq!(close_shorts.len(), levels);
        assert!(close_shorts
            .iter()
            .any(|o| (o.price - expected_close).abs() < 0.05));
    }

    #[test]
    fn fill_qty_with_float_residue_should_preserve_step_aligned_close_qty() {
        let mut config = test_config();
        config.grid.levels_per_side = 1;
        config.grid.order_qty = Some(0.002);
        config.precision.min_notional = Some(0.0);
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 1.0,
            short_qty: 1.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 1.0,
            short_available: 1.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        let snap = snapshot(100.0);
        engine.rebuild_grid(&snap);

        let buy_order = engine
            .buy_orders()
            .iter()
            .find(|o| o.intent == OrderIntent::OpenLongBuy)
            .cloned()
            .expect("buy");
        let fill = FillEvent {
            order_id: buy_order.id.clone(),
            intent: buy_order.intent,
            fill_qty: buy_order.qty - 1e-9,
            fill_price: buy_order.price,
            timestamp: Utc::now(),
            partial: false,
        };
        let actions = engine.handle_fill(fill, &snap);

        let close_qtys: Vec<f64> = actions
            .iter()
            .filter_map(|action| match action {
                EngineAction::Place(draft) if draft.intent == OrderIntent::CloseLongSell => {
                    Some(draft.qty)
                }
                _ => None,
            })
            .collect();
        assert!(close_qtys.iter().any(|qty| (*qty - 0.002).abs() < 1e-9));

        let position = engine.position();
        assert!((position.long_available - 1.002).abs() < 1e-9);
    }

    #[test]
    fn same_price_double_sell_fills_should_roll_twice() {
        let config = test_config();
        let spacing = config.grid.grid_spacing_pct;
        let levels = config.grid.levels_per_side;
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 1.0,
            short_qty: 1.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 1.0,
            short_available: 1.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        let snap = snapshot(100.0);
        engine.rebuild_grid(&snap);

        let sell_open = engine
            .sell_orders()
            .iter()
            .find(|o| o.intent == OrderIntent::OpenShortSell)
            .cloned()
            .expect("open short sell");
        let sell_close = engine
            .sell_orders()
            .iter()
            .find(|o| {
                o.intent == OrderIntent::CloseLongSell
                    && price_key(o.price) == price_key(sell_open.price)
            })
            .cloned()
            .expect("close long sell");

        let fill_open = FillEvent {
            order_id: sell_open.id.clone(),
            intent: sell_open.intent,
            fill_qty: sell_open.qty,
            fill_price: sell_open.price,
            timestamp: Utc::now(),
            partial: false,
        };
        let actions_open = engine.handle_fill(fill_open, &snap);
        let (place_open, cancel_open) = action_counts(&actions_open);
        assert_eq!(place_open, 2);
        assert_eq!(cancel_open, 1);

        let fill_close = FillEvent {
            order_id: sell_close.id.clone(),
            intent: sell_close.intent,
            fill_qty: sell_close.qty,
            fill_price: sell_close.price,
            timestamp: Utc::now(),
            partial: false,
        };
        let actions_close = engine.handle_fill(fill_close, &snap);
        let (place_close, cancel_close) = action_counts(&actions_close);
        assert_eq!(place_close, 2);
        assert_eq!(cancel_close, 1);

        let buy_orders = engine.buy_orders();
        let sell_orders = engine.sell_orders();
        assert_eq!(buy_orders.len() + sell_orders.len(), levels * 4);

        let expected_near_buy = step_price(sell_open.price, spacing, 1, false);
        let expected_far_sell = step_price(sell_open.price, spacing, levels, true);

        assert!(buy_orders.iter().any(|o| {
            o.intent == OrderIntent::OpenLongBuy && (o.price - expected_near_buy).abs() < 0.05
        }));
        assert!(buy_orders.iter().any(|o| {
            o.intent == OrderIntent::CloseShortBuy && (o.price - expected_near_buy).abs() < 0.05
        }));
        assert!(sell_orders.iter().any(|o| {
            o.intent == OrderIntent::OpenShortSell && (o.price - expected_far_sell).abs() < 0.05
        }));
        assert!(sell_orders.iter().any(|o| {
            o.intent == OrderIntent::CloseLongSell && (o.price - expected_far_sell).abs() < 0.05
        }));
    }

    #[test]
    fn follow_should_shift_buy_ladder() {
        let mut config = test_config();
        config.grid.strict_pairing = false;
        config.follow.max_gap_steps = 0.5;
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 0.0,
            short_qty: 0.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 0.0,
            short_available: 0.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        engine.rebuild_grid(&snapshot(100.0));

        let before = engine.buy_orders();
        let before_min = before.last().expect("min").price;
        let follow_snapshot = snapshot(120.0);
        engine.maybe_follow(&follow_snapshot);
        let after = engine.buy_orders();
        let after_min = after.last().expect("min").price;

        assert_eq!(after.len(), 3);
        assert!(after_min > before_min);
    }

    #[test]
    fn follow_should_not_shift_when_inventory_is_sufficient() {
        let mut config = test_config();
        config.grid.strict_pairing = false;
        config.follow.max_gap_steps = 0.5;
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 1.0,
            short_qty: 1.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 1.0,
            short_available: 1.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        engine.rebuild_grid(&snapshot(100.0));

        let before = engine.buy_orders();
        let before_min = before.last().expect("min").price;
        let follow_snapshot = snapshot(120.0);
        let actions = engine.maybe_follow(&follow_snapshot);
        let after = engine.buy_orders();
        let after_min = after.last().expect("min").price;

        assert!(actions.is_empty());
        assert_eq!(after.len(), before.len());
        assert!((after_min - before_min).abs() < 1e-9);
    }

    #[test]
    fn reconcile_should_trim_excess_close_orders() {
        let config = test_config();
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 0.4,
            short_qty: 0.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 0.4,
            short_available: 0.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        let snap = snapshot(100.0);
        engine.rebuild_grid(&snap);

        let reduced = PositionState {
            long_qty: 0.1,
            short_qty: 0.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 0.1,
            short_available: 0.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(reduced);
        engine.reconcile_inventory(&snap);

        let sell_orders = engine.sell_orders();
        let close_orders: Vec<_> = sell_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::CloseLongSell)
            .collect();
        let close_qty: f64 = close_orders.iter().map(|o| o.qty).sum();
        assert!(close_orders.len() <= 1);
        assert!(close_qty <= 0.1 + 1e-6);
    }

    #[test]
    fn close_long_orders_should_follow_grid_when_underwater() {
        let config = test_config();
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 1.0,
            short_qty: 0.0,
            long_entry_price: 100.0,
            short_entry_price: 0.0,
            long_available: 1.0,
            short_available: 0.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 90.0,
        };
        engine.update_position(position);
        engine.rebuild_grid(&snapshot(90.0));

        let sell_orders = engine.sell_orders();
        let open_short_keys: HashSet<i64> = sell_orders
            .iter()
            .filter(|order| order.intent == OrderIntent::OpenShortSell)
            .map(|order| price_key(order.price))
            .collect();
        let close_orders: Vec<_> = sell_orders
            .iter()
            .filter(|order| order.intent == OrderIntent::CloseLongSell)
            .collect();
        assert!(!close_orders.is_empty());
        assert!(close_orders
            .iter()
            .all(|order| open_short_keys.contains(&price_key(order.price))));
        assert!(close_orders.iter().any(|order| order.price < 100.0));
    }

    #[test]
    fn close_short_orders_should_follow_grid_when_underwater() {
        let config = test_config();
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 0.0,
            short_qty: 1.0,
            long_entry_price: 0.0,
            short_entry_price: 100.0,
            long_available: 0.0,
            short_available: 1.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 110.0,
        };
        engine.update_position(position);
        engine.rebuild_grid(&snapshot(110.0));

        let buy_orders = engine.buy_orders();
        let open_long_keys: HashSet<i64> = buy_orders
            .iter()
            .filter(|order| order.intent == OrderIntent::OpenLongBuy)
            .map(|order| price_key(order.price))
            .collect();
        let close_orders: Vec<_> = buy_orders
            .iter()
            .filter(|order| order.intent == OrderIntent::CloseShortBuy)
            .collect();
        assert!(!close_orders.is_empty());
        assert!(close_orders
            .iter()
            .all(|order| open_long_keys.contains(&price_key(order.price))));
        assert!(close_orders.iter().any(|order| order.price > 100.0));
    }

    #[test]
    fn reconcile_should_not_create_duplicate_price_levels() {
        let config = test_config();
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 1.0,
            short_qty: 1.0,
            long_entry_price: 100.0,
            short_entry_price: 100.0,
            long_available: 1.0,
            short_available: 1.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        let snap = snapshot(100.0);
        engine.rebuild_grid(&snap);
        for _ in 0..5 {
            engine.reconcile_inventory(&snap);
        }

        let mut seen_buy = HashSet::new();
        for order in engine.buy_orders() {
            let key = (order.intent as u8, (order.price * 10000.0).round() as i64);
            assert!(seen_buy.insert(key), "duplicate buy level {:?}", order);
        }
        let mut seen_sell = HashSet::new();
        for order in engine.sell_orders() {
            let key = (order.intent as u8, (order.price * 10000.0).round() as i64);
            assert!(seen_sell.insert(key), "duplicate sell level {:?}", order);
        }
    }

    #[test]
    fn inventory_mode_should_keep_strict_spacing_and_six_orders_per_side() {
        let mut config = test_config();
        config.grid.grid_spacing_abs = Some(0.5);
        config.grid.levels_per_side = 3;
        config.grid.strict_pairing = false;
        let levels = config.grid.levels_per_side;

        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 10.0,
            short_qty: 10.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 10.0,
            short_available: 10.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        let snap = snapshot(100.0);
        engine.rebuild_grid(&snap);
        for _ in 0..3 {
            engine.reconcile_inventory(&snap);
        }

        let buy_orders = engine.buy_orders();
        let sell_orders = engine.sell_orders();
        let open_longs: Vec<_> = buy_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::OpenLongBuy)
            .collect();
        let close_shorts: Vec<_> = buy_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::CloseShortBuy)
            .collect();
        let open_shorts: Vec<_> = sell_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::OpenShortSell)
            .collect();
        let close_longs: Vec<_> = sell_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::CloseLongSell)
            .collect();

        assert_eq!(open_longs.len(), levels);
        assert_eq!(close_shorts.len(), levels);
        assert_eq!(open_shorts.len(), levels);
        assert_eq!(close_longs.len(), levels);

        let buy_unique_prices: HashSet<i64> = open_longs
            .iter()
            .map(|o| price_key(o.price))
            .chain(close_shorts.iter().map(|o| price_key(o.price)))
            .collect();
        let sell_unique_prices: HashSet<i64> = open_shorts
            .iter()
            .map(|o| price_key(o.price))
            .chain(close_longs.iter().map(|o| price_key(o.price)))
            .collect();
        assert_eq!(buy_unique_prices.len(), levels);
        assert_eq!(sell_unique_prices.len(), levels);

        let mut buy_open_prices: Vec<f64> = open_longs.iter().map(|o| o.price).collect();
        buy_open_prices.sort_by(|a, b| b.partial_cmp(a).unwrap());
        let mut buy_close_prices: Vec<f64> = close_shorts.iter().map(|o| o.price).collect();
        buy_close_prices.sort_by(|a, b| b.partial_cmp(a).unwrap());
        let mut sell_open_prices: Vec<f64> = open_shorts.iter().map(|o| o.price).collect();
        sell_open_prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mut sell_close_prices: Vec<f64> = close_longs.iter().map(|o| o.price).collect();
        sell_close_prices.sort_by(|a, b| a.partial_cmp(b).unwrap());

        for ladder in [
            buy_open_prices,
            buy_close_prices,
            sell_open_prices,
            sell_close_prices,
        ] {
            for pair in ladder.windows(2) {
                let gap = (pair[0] - pair[1]).abs();
                assert!(
                    (gap - 0.5).abs() < 1e-9,
                    "unexpected ladder gap: {:?} (gap={})",
                    ladder,
                    gap
                );
            }
        }
    }

    #[test]
    fn fill_should_repair_near_gap_and_trim_far_orders_with_abs_spacing() {
        let mut config = test_config();
        config.grid.grid_spacing_abs = Some(2.5);
        config.grid.grid_spacing_pct = 0.0;
        config.grid.levels_per_side = 3;
        config.grid.order_qty = Some(0.011);
        config.grid.order_notional = 0.0;
        config.grid.strict_pairing = false;
        config.precision.tick_size = 0.01;
        config.precision.price_digits = Some(2);
        config.precision.min_notional = Some(5.0);
        config.risk.max_net_notional = 100000.0;
        config.risk.max_total_notional = 100000.0;

        let mut engine = GridEngine::new(config, true).expect("engine");
        engine.update_position(PositionState {
            long_qty: 1.0,
            short_qty: 1.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 1.0,
            short_available: 1.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 2371.38,
        });
        let snap = snapshot(2371.38);
        engine.rebuild_grid(&snap);

        let near_sell = engine
            .sell_orders()
            .into_iter()
            .filter(|order| order.intent == OrderIntent::OpenShortSell)
            .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
            .expect("near open short");
        let fill = FillEvent {
            order_id: near_sell.id,
            intent: near_sell.intent,
            fill_qty: near_sell.qty,
            fill_price: near_sell.price,
            timestamp: Utc::now(),
            partial: false,
        };
        let actions = engine.handle_fill(fill, &snap);
        let (_, cancel_count) = action_counts(&actions);
        assert!(
            cancel_count >= 1,
            "far order should be trimmed after repair"
        );

        let highest_buy = engine
            .buy_orders()
            .into_iter()
            .filter(|order| order.intent == OrderIntent::OpenLongBuy)
            .map(|order| order.price)
            .reduce(f64::max)
            .expect("highest buy");
        let lowest_sell = engine
            .sell_orders()
            .into_iter()
            .filter(|order| order.intent == OrderIntent::OpenShortSell)
            .map(|order| order.price)
            .reduce(f64::min)
            .expect("lowest sell");
        assert!(
            (lowest_sell - highest_buy - 5.0).abs() <= 0.01,
            "near gap should stay at two spacings: highest_buy={highest_buy} lowest_sell={lowest_sell}"
        );

        let mut sell_open_prices: Vec<f64> = engine
            .sell_orders()
            .into_iter()
            .filter(|order| order.intent == OrderIntent::OpenShortSell)
            .map(|order| order.price)
            .collect();
        sell_open_prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(sell_open_prices.len(), 3);
        for pair in sell_open_prices.windows(2) {
            assert!(
                (pair[1] - pair[0] - 2.5).abs() <= 0.01,
                "sell open ladder should be continuous: {sell_open_prices:?}"
            );
        }
    }

    #[test]
    fn strict_pairing_should_seed_only_open_ladders() {
        let mut config = test_config();
        config.grid.strict_pairing = true;
        let levels = config.grid.levels_per_side;
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 1.0,
            short_qty: 1.0,
            long_entry_price: 100.0,
            short_entry_price: 100.0,
            long_available: 1.0,
            short_available: 1.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        engine.rebuild_grid(&snapshot(100.0));

        let buy_orders = engine.buy_orders();
        let sell_orders = engine.sell_orders();
        let open_longs = buy_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::OpenLongBuy)
            .count();
        let close_shorts = buy_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::CloseShortBuy)
            .count();
        let open_shorts = sell_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::OpenShortSell)
            .count();
        let close_longs = sell_orders
            .iter()
            .filter(|o| o.intent == OrderIntent::CloseLongSell)
            .count();

        assert_eq!(open_longs, levels);
        assert_eq!(open_shorts, levels);
        assert_eq!(close_shorts, 0);
        assert_eq!(close_longs, 0);
    }

    #[test]
    fn strict_pairing_should_not_force_close_price_to_entry() {
        let mut config = test_config();
        config.grid.strict_pairing = true;
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 0.0,
            short_qty: 0.0,
            long_entry_price: 100.0,
            short_entry_price: 0.0,
            long_available: 0.0,
            short_available: 0.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 99.0,
        };
        engine.update_position(position);
        let snap = snapshot(99.0);
        engine.rebuild_grid(&snap);

        let buy_order = engine.buy_orders().first().cloned().expect("buy");
        let fill = FillEvent {
            order_id: buy_order.id.clone(),
            intent: buy_order.intent,
            fill_qty: buy_order.qty,
            fill_price: buy_order.price,
            timestamp: Utc::now(),
            partial: false,
        };
        engine.handle_fill(fill, &snap);

        let close_orders: Vec<_> = engine
            .sell_orders()
            .into_iter()
            .filter(|order| order.intent == OrderIntent::CloseLongSell)
            .collect();
        assert!(!close_orders.is_empty());
        assert!(close_orders.iter().any(|order| order.price < 100.0));
    }

    #[test]
    fn strict_pairing_close_fill_should_not_create_unpaired_close() {
        let mut config = test_config();
        config.grid.strict_pairing = true;
        let mut engine = GridEngine::new(config, true).expect("engine");
        let position = PositionState {
            long_qty: 0.0,
            short_qty: 0.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 0.0,
            short_available: 0.0,
            equity: 10000.0,
            maintenance_margin: 0.0,
            mark_price: 100.0,
        };
        engine.update_position(position);
        let snap = snapshot(100.0);
        engine.rebuild_grid(&snap);

        let buy_order = engine.buy_orders().first().cloned().expect("buy");
        let open_fill = FillEvent {
            order_id: buy_order.id.clone(),
            intent: buy_order.intent,
            fill_qty: buy_order.qty,
            fill_price: buy_order.price,
            timestamp: Utc::now(),
            partial: false,
        };
        engine.handle_fill(open_fill, &snap);

        let close_order = engine
            .sell_orders()
            .iter()
            .find(|o| o.intent == OrderIntent::CloseLongSell)
            .cloned()
            .expect("close");
        let close_fill = FillEvent {
            order_id: close_order.id.clone(),
            intent: close_order.intent,
            fill_qty: close_order.qty,
            fill_price: close_order.price,
            timestamp: Utc::now(),
            partial: false,
        };
        engine.handle_fill(close_fill, &snap);

        let close_count = engine
            .sell_orders()
            .iter()
            .filter(|o| o.intent == OrderIntent::CloseLongSell)
            .count();
        assert_eq!(close_count, 0);
    }
}

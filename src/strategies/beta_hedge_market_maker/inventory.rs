use chrono::{DateTime, Utc};

use crate::core::types::Position;

use super::exposure::{effective_mark_price, signed_position_quantity};

#[derive(Debug, Clone, Default)]
pub struct HedgeLegInventory {
    pub quantity: f64,
    pub entry_price: f64,
    pub notional_usd: f64,
}

#[derive(Debug, Clone, Default)]
pub struct HedgePositionSnapshot {
    pub long_quantity: f64,
    pub short_quantity: f64,
    pub long_entry_price: f64,
    pub short_entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone, Default)]
pub struct HedgeInventoryLedger {
    pub long_leg: HedgeLegInventory,
    pub short_leg: HedgeLegInventory,
    pub net_quantity: f64,
    pub net_notional_usd: f64,
    pub gross_notional_usd: f64,
    pub average_cost: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub traded_notional_usd: f64,
    pub fill_count: u64,
    pub mark_price: f64,
    pub last_synced_at: Option<DateTime<Utc>>,
}

impl HedgeInventoryLedger {
    pub fn sync_from_snapshot(&mut self, snapshot: &HedgePositionSnapshot) {
        let long_delta = snapshot.long_quantity - self.long_leg.quantity;
        let short_delta = snapshot.short_quantity - self.short_leg.quantity;

        if long_delta.abs() > 1e-9 {
            self.traded_notional_usd += long_delta.abs() * snapshot.mark_price;
            self.fill_count += 1;
            if long_delta < 0.0 {
                let closed = long_delta.abs();
                self.realized_pnl += closed * (snapshot.mark_price - self.long_leg.entry_price);
            }
        }

        if short_delta.abs() > 1e-9 {
            self.traded_notional_usd += short_delta.abs() * snapshot.mark_price;
            self.fill_count += 1;
            if short_delta < 0.0 {
                let closed = short_delta.abs();
                self.realized_pnl += closed * (self.short_leg.entry_price - snapshot.mark_price);
            }
        }

        self.long_leg.quantity = snapshot.long_quantity;
        self.long_leg.entry_price = snapshot.long_entry_price;
        self.long_leg.notional_usd = snapshot.long_quantity * snapshot.mark_price;

        self.short_leg.quantity = snapshot.short_quantity;
        self.short_leg.entry_price = snapshot.short_entry_price;
        self.short_leg.notional_usd = snapshot.short_quantity * snapshot.mark_price;

        self.net_quantity = snapshot.long_quantity - snapshot.short_quantity;
        self.net_notional_usd = self.net_quantity * snapshot.mark_price;
        self.gross_notional_usd =
            self.long_leg.notional_usd.abs() + self.short_leg.notional_usd.abs();
        self.mark_price = snapshot.mark_price;
        self.unrealized_pnl = snapshot.unrealized_pnl;
        self.average_cost = if snapshot.long_quantity > 0.0 && snapshot.short_quantity == 0.0 {
            snapshot.long_entry_price
        } else if snapshot.short_quantity > 0.0 && snapshot.long_quantity == 0.0 {
            snapshot.short_entry_price
        } else if self.net_quantity.abs() > 1e-9 {
            let numerator = snapshot.long_quantity * snapshot.long_entry_price
                + snapshot.short_quantity * snapshot.short_entry_price;
            numerator / (snapshot.long_quantity + snapshot.short_quantity).max(1e-9)
        } else {
            0.0
        };
        self.last_synced_at = Some(Utc::now());
    }
}

pub fn extract_hedge_position_snapshot(
    positions: &[Position],
    hedge_symbol: &str,
) -> HedgePositionSnapshot {
    let mut snapshot = HedgePositionSnapshot::default();

    for position in positions
        .iter()
        .filter(|position| position.symbol == hedge_symbol)
    {
        let signed_qty = signed_position_quantity(position);
        let mark_price = effective_mark_price(position);
        snapshot.mark_price = snapshot.mark_price.max(mark_price);
        snapshot.unrealized_pnl += position.unrealized_pnl;

        match position.side.to_ascii_uppercase().as_str() {
            "LONG" | "BUY" => {
                snapshot.long_quantity += signed_qty.abs();
                snapshot.long_entry_price = weighted_price(
                    snapshot.long_entry_price,
                    snapshot.long_quantity - signed_qty.abs(),
                    position.entry_price,
                    signed_qty.abs(),
                );
            }
            "SHORT" | "SELL" => {
                snapshot.short_quantity += signed_qty.abs();
                snapshot.short_entry_price = weighted_price(
                    snapshot.short_entry_price,
                    snapshot.short_quantity - signed_qty.abs(),
                    position.entry_price,
                    signed_qty.abs(),
                );
            }
            _ => {
                if signed_qty >= 0.0 {
                    snapshot.long_quantity += signed_qty.abs();
                    snapshot.long_entry_price = weighted_price(
                        snapshot.long_entry_price,
                        snapshot.long_quantity - signed_qty.abs(),
                        position.entry_price,
                        signed_qty.abs(),
                    );
                } else {
                    snapshot.short_quantity += signed_qty.abs();
                    snapshot.short_entry_price = weighted_price(
                        snapshot.short_entry_price,
                        snapshot.short_quantity - signed_qty.abs(),
                        position.entry_price,
                        signed_qty.abs(),
                    );
                }
            }
        }
    }

    snapshot
}

fn weighted_price(prev_price: f64, prev_qty: f64, new_price: f64, new_qty: f64) -> f64 {
    let total = prev_qty + new_qty;
    if total <= 0.0 {
        0.0
    } else {
        (prev_price * prev_qty + new_price * new_qty) / total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::Position;

    fn position(symbol: &str, side: &str, qty: f64, entry: f64, mark: f64, upnl: f64) -> Position {
        Position {
            symbol: symbol.to_string(),
            side: side.to_string(),
            contracts: qty,
            contract_size: 1.0,
            entry_price: entry,
            mark_price: mark,
            unrealized_pnl: upnl,
            percentage: 0.0,
            margin: 0.0,
            margin_ratio: 0.0,
            leverage: Some(1),
            margin_type: Some("cross".to_string()),
            size: qty,
            amount: qty,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn extracts_long_and_short_legs_separately() {
        let positions = vec![
            position("DOGE/USDC", "LONG", 1_500.0, 0.101, 0.11, 13.5),
            position("DOGE/USDC", "SHORT", 400.0, 0.115, 0.11, 2.0),
        ];

        let snapshot = extract_hedge_position_snapshot(&positions, "DOGE/USDC");
        assert_eq!(snapshot.long_quantity, 1_500.0);
        assert_eq!(snapshot.short_quantity, 400.0);
        assert!((snapshot.long_entry_price - 0.101).abs() < 1e-9);
        assert!((snapshot.short_entry_price - 0.115).abs() < 1e-9);
        assert!((snapshot.unrealized_pnl - 15.5).abs() < 1e-9);
    }

    #[test]
    fn sync_tracks_turnover_and_realized_pnl_on_reduction() {
        let mut ledger = HedgeInventoryLedger::default();
        ledger.sync_from_snapshot(&HedgePositionSnapshot {
            long_quantity: 1_000.0,
            short_quantity: 0.0,
            long_entry_price: 0.1,
            short_entry_price: 0.0,
            mark_price: 0.11,
            unrealized_pnl: 10.0,
        });
        ledger.sync_from_snapshot(&HedgePositionSnapshot {
            long_quantity: 400.0,
            short_quantity: 0.0,
            long_entry_price: 0.1,
            short_entry_price: 0.0,
            mark_price: 0.12,
            unrealized_pnl: 8.0,
        });

        assert_eq!(ledger.fill_count, 2);
        assert!((ledger.realized_pnl - 12.0).abs() < 1e-9);
        assert!(ledger.traded_notional_usd > 100.0);
    }
}

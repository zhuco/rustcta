use crate::{parse_spot_venue, RejectionReason, SpotOrderBookLevel, SpotVenue};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimulatedLeg {
    pub average_price: f64,
    pub quantity: f64,
    pub notional: f64,
    pub fee: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimulatedPairExecution {
    pub buy: SimulatedLeg,
    pub sell: SimulatedLeg,
    pub gross_pnl: f64,
    pub total_fees: f64,
    pub net_pnl: f64,
}

pub fn simulate_taker_buy(
    asks: &[SpotOrderBookLevel],
    quantity: f64,
    taker_fee_bps: f64,
) -> Result<SimulatedLeg, RejectionReason> {
    consume_levels(asks, quantity, taker_fee_bps)
}

pub fn simulate_taker_sell(
    bids: &[SpotOrderBookLevel],
    quantity: f64,
    taker_fee_bps: f64,
) -> Result<SimulatedLeg, RejectionReason> {
    consume_levels(bids, quantity, taker_fee_bps)
}

pub fn simulate_taker_pair(
    asks: &[SpotOrderBookLevel],
    bids: &[SpotOrderBookLevel],
    quantity: f64,
    buy_taker_fee_bps: f64,
    sell_taker_fee_bps: f64,
) -> Result<SimulatedPairExecution, RejectionReason> {
    let buy = simulate_taker_buy(asks, quantity, buy_taker_fee_bps)?;
    let sell = simulate_taker_sell(bids, quantity, sell_taker_fee_bps)?;
    let gross_pnl = sell.notional - buy.notional;
    let total_fees = buy.fee + sell.fee;
    Ok(SimulatedPairExecution {
        buy,
        sell,
        gross_pnl,
        total_fees,
        net_pnl: gross_pnl - total_fees,
    })
}

pub fn consume_levels(
    levels: &[SpotOrderBookLevel],
    quantity: f64,
    taker_fee_bps: f64,
) -> Result<SimulatedLeg, RejectionReason> {
    if quantity <= 0.0 {
        return Err(RejectionReason::MinNotional);
    }
    let mut remaining = quantity;
    let mut notional = 0.0;
    for level in levels {
        if remaining <= 1e-12 {
            break;
        }
        let fill_qty = remaining.min(level.quantity);
        notional += level.price * fill_qty;
        remaining -= fill_qty;
    }
    if remaining > 1e-12 {
        return Err(RejectionReason::InsufficientDepth);
    }
    let fee = notional * taker_fee_bps / 10_000.0;
    Ok(SimulatedLeg {
        average_price: notional / quantity,
        quantity,
        notional,
        fee,
    })
}

pub fn parse_paper_exchange(value: &str) -> Result<SpotVenue, RejectionReason> {
    parse_spot_venue(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taker_buy_should_consume_ask_depth_and_fee() {
        let leg = simulate_taker_buy(
            &[
                SpotOrderBookLevel {
                    price: 100.0,
                    quantity: 0.1,
                },
                SpotOrderBookLevel {
                    price: 110.0,
                    quantity: 0.2,
                },
            ],
            0.2,
            10.0,
        )
        .expect("simulate buy");

        assert_eq!(leg.quantity, 0.2);
        assert!((leg.notional - 21.0).abs() < 1e-12);
        assert!((leg.average_price - 105.0).abs() < 1e-12);
        assert!((leg.fee - 0.021).abs() < 1e-12);
    }

    #[test]
    fn taker_sell_should_reject_insufficient_depth() {
        let result = simulate_taker_sell(
            &[SpotOrderBookLevel {
                price: 100.0,
                quantity: 0.1,
            }],
            0.2,
            10.0,
        );

        assert_eq!(result.unwrap_err(), RejectionReason::InsufficientDepth);
    }

    #[test]
    fn taker_pair_should_calculate_gross_and_net_pnl() {
        let execution = simulate_taker_pair(
            &[SpotOrderBookLevel {
                price: 100.0,
                quantity: 1.0,
            }],
            &[SpotOrderBookLevel {
                price: 101.0,
                quantity: 1.0,
            }],
            0.5,
            10.0,
            10.0,
        )
        .expect("simulate pair");

        assert_eq!(execution.buy.notional, 50.0);
        assert_eq!(execution.sell.notional, 50.5);
        assert_eq!(execution.gross_pnl, 0.5);
        assert!((execution.total_fees - 0.1005).abs() < 1e-12);
        assert!((execution.net_pnl - 0.3995).abs() < 1e-12);
    }

    #[test]
    fn parser_should_accept_legacy_exchange_aliases() {
        assert_eq!(parse_paper_exchange("gate.io").unwrap(), SpotVenue::GateIo);
        assert_eq!(parse_paper_exchange("bitget").unwrap(), SpotVenue::Bitget);
    }
}

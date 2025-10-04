use crate::core::types::OrderSide;

#[derive(Debug, Clone)]
pub struct MakerFillPlan {
    pub new_orders: Vec<(OrderSide, f64)>,
    pub cancel_side: OrderSide,
}

pub fn plan_maker_fill(fill_side: OrderSide, fill_price: f64, spacing_ratio: f64) -> MakerFillPlan {
    let mut new_orders = Vec::with_capacity(2);
    let cancel_side;

    match fill_side {
        OrderSide::Buy => {
            cancel_side = OrderSide::Sell;

            let sell_price = fill_price * (1.0 + spacing_ratio);
            let buy_price = fill_price * (1.0 - spacing_ratio * 2.0);

            if sell_price > 0.0 {
                new_orders.push((OrderSide::Sell, sell_price));
            }
            if buy_price > 0.0 {
                new_orders.push((OrderSide::Buy, buy_price));
            }
        }
        OrderSide::Sell => {
            cancel_side = OrderSide::Buy;

            let buy_price = fill_price * (1.0 - spacing_ratio);
            let sell_price = fill_price * (1.0 + spacing_ratio * 2.0);

            if buy_price > 0.0 {
                new_orders.push((OrderSide::Buy, buy_price));
            }
            if sell_price > 0.0 {
                new_orders.push((OrderSide::Sell, sell_price));
            }
        }
    }

    MakerFillPlan {
        new_orders,
        cancel_side,
    }
}

use crate::exchange::binance_model::{Filter, SymbolInfo};

/// 根据交易对信息调整价格精度
pub fn adjust_price_precision(price: f64, symbol_info: &SymbolInfo) -> f64 {
    // 使用 price_precision 字段调整价格精度
    let precision = symbol_info.price_precision;
    (price * 10f64.powi(precision)).round() / 10f64.powi(precision)
}

/// 根据交易对信息调整数量精度
pub fn adjust_quantity_precision(quantity: f64, symbol_info: &SymbolInfo) -> f64 {
    // 使用 quantity_precision 字段调整数量精度
    let precision = symbol_info.quantity_precision;
    (quantity * 10f64.powi(precision)).floor() / 10f64.powi(precision)
}

/// 根据过滤器调整价格，确保符合tick_size要求
pub fn adjust_price_by_filter(price: f64, symbol_info: &SymbolInfo) -> Result<f64, String> {
    for filter in &symbol_info.filters {
        if let Filter::PriceFilter {
            tick_size,
            min_price,
            max_price,
        } = filter
        {
            let tick: f64 = tick_size.parse().map_err(|_| "Invalid tick_size format")?;
            let min: f64 = min_price.parse().map_err(|_| "Invalid min_price format")?;
            let max: f64 = max_price.parse().map_err(|_| "Invalid max_price format")?;

            // 检查价格范围
            if price < min || price > max {
                return Err(format!(
                    "Price {} is out of range [{}, {}]",
                    price, min, max
                ));
            }

            // 调整到最近的tick_size倍数
            let adjusted_price = (price / tick).round() * tick;

            // 使用price_precision进行最终精度调整，避免浮点数精度问题
            let precision = symbol_info.price_precision;
            let final_price =
                (adjusted_price * 10f64.powi(precision)).round() / 10f64.powi(precision);

            return Ok(final_price);
        }
    }

    // 如果没有找到PriceFilter，使用默认精度调整
    Ok(adjust_price_precision(price, symbol_info))
}

/// 根据过滤器调整数量，确保符合step_size要求
pub fn adjust_quantity_by_filter(quantity: f64, symbol_info: &SymbolInfo) -> Result<f64, String> {
    for filter in &symbol_info.filters {
        if let Filter::LotSize {
            step_size,
            min_qty,
            max_qty,
        } = filter
        {
            let step: f64 = step_size.parse().map_err(|_| "Invalid step_size format")?;
            let min: f64 = min_qty.parse().map_err(|_| "Invalid min_qty format")?;
            let max: f64 = max_qty.parse().map_err(|_| "Invalid max_qty format")?;

            // 检查数量范围
            if quantity < min || quantity > max {
                return Err(format!(
                    "Quantity {} is out of range [{}, {}]",
                    quantity, min, max
                ));
            }

            // 调整到最近的step_size倍数（向下取整）
            let adjusted_quantity = (quantity / step).floor() * step;

            // 使用quantity_precision进行最终精度调整，避免浮点数精度问题
            let precision = symbol_info.quantity_precision;
            let final_quantity =
                (adjusted_quantity * 10f64.powi(precision)).floor() / 10f64.powi(precision);

            return Ok(final_quantity);
        }
    }

    // 如果没有找到LotSize，使用默认精度调整
    Ok(adjust_quantity_precision(quantity, symbol_info))
}

/// 验证最小名义价值要求
pub fn validate_min_notional(
    price: f64,
    quantity: f64,
    symbol_info: &SymbolInfo,
) -> Result<(), String> {
    for filter in &symbol_info.filters {
        if let Filter::MinNotional { notional } = filter {
            let min_notional: f64 = notional.parse().map_err(|_| "Invalid notional format")?;
            let order_notional = price * quantity;

            if order_notional < min_notional {
                return Err(format!(
                    "Order notional {} is below minimum {}",
                    order_notional, min_notional
                ));
            }
        }
    }
    Ok(())
}

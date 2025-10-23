use serde::{Deserialize, Serialize};
use rand::Rng;
use chrono::Utc;

#[derive(Deserialize)]
pub struct Order {
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub qty: f64,
}

#[derive(Serialize)]
pub struct Trade {
    pub trade_id: String,
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub qty: f64,
    pub ts: i64,
}

impl Trade {
    pub fn from_order(o: &Order) -> Self {
        let id = format!("T-{}", Uuid::new_v4());
        let fill_qty = o.qty * (rand::thread_rng().gen_range(0.9..=1.0)); // 일부 체결
        Self {
            trade_id: id,
            order_id: o.order_id.clone(),
            symbol: o.symbol.clone(),
            side: o.side.clone(),
            price: o.price,
            qty: fill_qty,
            ts: Utc::now().timestamp_millis(),
        }
    }
}

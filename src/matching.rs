use serde::{Deserialize, Serialize};
use uuid::Uuid;
use rand::Rng;

#[derive(Deserialize, Clone)]
pub struct Order {
    pub order_id: String,
    pub symbol: String,
    pub side: String,  // "buy"/"sell"
    pub price: f64,
    pub qty: f64,
}

#[derive(Serialize, Clone)]
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
        let fill = (o.qty * rand::thread_rng().gen_range(0.9..=1.0)).max(0.00000001);
        Self {
            trade_id: format!("T-{}", Uuid::new_v4()),
            order_id: o.order_id.clone(),
            symbol: o.symbol.clone(),
            side: o.side.clone(),
            price: o.price,
            qty: fill,
            ts: chrono::Utc::now().timestamp_millis(),
        }
    }
}

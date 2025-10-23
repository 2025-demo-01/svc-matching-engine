use clickhouse::{Client, Row};
use serde::Serialize;
use anyhow::Result;

#[derive(Serialize)]
pub struct TradeRow<'a> {
    pub trade_id: &'a str,
    pub order_id: &'a str,
    pub symbol: &'a str,
    pub side: &'a str,
    pub price: f64,
    pub qty: f64,
    pub ts: i64,
}

pub async fn insert_trade(cli: &Client, t: &TradeRow<'_>) -> Result<()> {
    cli.insert("trades")?.write(&[t]).await?;
    Ok(())
}

use clickhouse::{Client, Row};
use anyhow::{Result, anyhow};
use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct TradeRow {
    pub trade_id: String,
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub qty: f64,
    pub ts: i64,
}

pub async fn insert_batch_raw(cli: &Client, rows: &[TradeRow]) -> Result<()> {
    if rows.is_empty() { return Ok(()); }
    let mut insert = cli.insert("trades_raw")?;
    for r in rows {
        insert.write(r).await?;
    }
    insert.end().await.map_err(|e| anyhow!(e))
}

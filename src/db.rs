use anyhow::{anyhow, Result};
use clickhouse::Client;
use serde::Serialize;

/// ClickHouse trades_raw 배치 insert용 행 구조체
#[derive(Serialize, Clone)]
pub struct TradeRow {
    pub trade_id: String,
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub qty: f64,
    pub ts: i64, // DateTime64(3)로 변환됨
}

/// trades_raw 테이블에 고속 배치 insert
/// 부분 실패 시 Err로 반환(상위에서 DLQ 처리 및 백프레셔)
pub async fn insert_batch_raw(cli: &Client, rows: &[TradeRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = cli.insert("trades_raw")?;
    for r in rows {
        insert
            .write(r)
            .await
            .map_err(|e| anyhow!("clickhouse write error: {e:?}"))?;
    }
    insert
        .end()
        .await
        .map_err(|e| anyhow!("clickhouse end error: {e:?}"))?;
    Ok(())
}

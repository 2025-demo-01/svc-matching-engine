use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    message::BorrowedMessage,
    config::ClientConfig,
};
use serde_json;
use crate::{matching::*, db, metrics::*};
use clickhouse::Client;
use anyhow::Result;
use std::time::Instant;

pub async fn run_loop(cfg: &crate::config::Config) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("group.id", "matching-engine")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer.subscribe(&[&cfg.input_topic])?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("transactional.id", "matching-engine-tx") // exactly-once
        .create()?;

    let click = Client::default().with_url(&cfg.clickhouse_url);

    loop {
        match consumer.recv().await {
            Err(e) => eprintln!("Kafka error: {:?}", e),
            Ok(m) => {
                let now = Instant::now();
                if let Err(e) = process_msg(&m, &producer, &click, &cfg.output_topic).await {
                    eprintln!("processing error: {:?}", e);
                }
                MATCH_TOTAL.inc();
                MATCH_LATENCY.observe(now.elapsed().as_millis() as f64);
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        }
    }
}

async fn process_msg(m: &BorrowedMessage<'_>, prod: &FutureProducer, click: &Client, out: &str) -> Result<()> {
    let payload = match m.payload_view::<str>() {
        Some(Ok(v)) => v,
        _ => return Ok(()),
    };
    let order: Order = serde_json::from_str(payload)?;
    let trade = Trade::from_order(&order);
    let tjson = serde_json::to_string(&trade)?;

    let tr = db::TradeRow {
        trade_id: &trade.trade_id,
        order_id: &trade.order_id,
        symbol: &trade.symbol,
        side: &trade.side,
        price: trade.price,
        qty: trade.qty,
        ts: trade.ts,
    };
    db::insert_trade(click, &tr).await?;
    prod.send(FutureRecord::to(out).payload(&tjson).key(&trade.trade_id), 0).await?;
    Ok(())
}

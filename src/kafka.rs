use rdkafka::{
    consumer::{Consumer, StreamConsumer, CommitMode},
    producer::{FutureProducer, FutureRecord, BaseRecord, ThreadedProducer},
    message::BorrowedMessage,
    config::ClientConfig, Message, TopicPartitionList,
};
use serde_json;
use anyhow::{Result, anyhow};
use std::time::{Duration, Instant};
use tokio::time::timeout;

use crate::{matching::*, db, metrics::*, util::*, config::Config};
use clickhouse::Client as ChClient;

pub async fn run_loop(cfg: &Config) -> Result<()> {
    let tx_id = format!(
      "matching-engine-tx-{}",
      std::env::var("POD_NAME").unwrap_or_else(|_| "dev".into())
    ); 

    let producer: rdkafka::producer::BaseProducer = ClientConfig::new()
      .set("bootstrap.servers", &cfg.kafka_brokers)
      .set("enable.idempotence", "true")
      .set("acks", "all")
      .set("retries", "10")
      .set("request.timeout.ms", "5000")
      .set("transactional.id", &tx_id)   // [CHANGED]
      .create()?;
    consumer.subscribe(&[&cfg.input_topic])?;

    // Producer with transactional.id for EOS
    let producer: rdkafka::producer::BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("retries", "10")
        .set("request.timeout.ms", "5000")
        .set("transactional.id", "matching-engine-tx")
        .create()?;
    producer.init_transactions(Timeout::After(Duration::from_secs(10)))?; // [ADDED]

    // ClickHouse client
    let click = ChClient::default().with_url(&cfg.clickhouse_url);

    // Batch params
    let max_batch = getenv("BATCH_SIZE", "500").parse::<usize>().unwrap_or(500);
    let win_ms = getenv("BATCH_WINDOW_MS", "50").parse::<u64>().unwrap_or(50);

    let mut batch: Vec<Trade> = Vec::with_capacity(max_batch);
    let mut refs: Vec<(String, i32, i64)> = Vec::with_capacity(max_batch); // (topic, partition, offset)

    loop {
        // collect batch with time window
        let start_win = Instant::now();
        while batch.len() < max_batch && start_win.elapsed() < Duration::from_millis(win_ms) {
            match timeout(Duration::from_millis(10), consumer.recv()).await {
                Ok(Ok(m)) => {
                    if let Some(t) = handle_msg(&m) { batch.push(t); refs.push((m.topic().to_string(), m.partition(), m.offset())); }
                }
                _ => {}
            }
        }

        BATCH_SIZE.set(batch.len() as i64);
        if batch.is_empty() { continue; }

        let bstart = Instant::now();

        // Backpressure: ClickHouse 느릴 때 파티션 일시 정지
        let mut paused = false;
        if let Err(e) = process_batch(&producer, &consumer, &click, &batch, &refs, cfg).await {
            // DLQ로 밀고, offsets 커밋(중복 재처리 방지) — 정책에 따라 선택
            DLQ_TOTAL.inc();
            eprintln!("batch error: {:?}", e);
            // 간단한 유예
            BACKPRESSURE.set(1);
            paused = true;
            consumer.pause(&consumer.assignment()?)?;
            tokio::time::sleep(Duration::from_millis(500)).await;
            consumer.resume(&consumer.assignment()?)?;
            BACKPRESSURE.set(0);
        }
        if paused { /* noop */ }

        BATCH_LAT_MS.observe(bstart.elapsed().as_millis() as f64);
        batch.clear();
        refs.clear();
    }
}

fn handle_msg(m: &BorrowedMessage<'_>) -> Option<Trade> {
    let payload = m.payload_view::<str>().ok().flatten()?;
    let order: Order = match serde_json::from_str(payload) { Ok(o) => o, Err(_) => return None };
    let trade = Trade::from_order(&order);
    Some(trade)
}

use rdkafka::util::Timeout;

async fn process_batch(
    producer: &rdkafka::producer::BaseProducer,
    consumer: &StreamConsumer,
    click: &ChClient,
    batch: &Vec<Trade>,
    refs: &Vec<(String, i32, i64)>,
    cfg: &Config
) -> Result<()> {

    // 1) Begin transaction
    producer.begin_transaction()?;                         // [ADDED]

    // 2) ClickHouse insert (raw)
    let rows: Vec<db::TradeRow> = batch.iter().map(|t| db::TradeRow {
        trade_id: t.trade_id.clone(),
        order_id: t.order_id.clone(),
        symbol: t.symbol.clone(),
        side: t.side.clone(),
        price: t.price,
        qty: t.qty,
        ts: t.ts,
    }).collect();

    db::insert_batch_raw(click, &rows).await?;            // [ADDED]

    // 3) Produce trades.out (same transaction boundary)
    for t in batch {
        let tjson = serde_json::to_string(t)?;
        let record = BaseRecord::to(&cfg.output_topic)
            .payload(&tjson)
            .key(&t.trade_id);
        producer.send(record)?;
        MATCH_TOTAL.inc();
        MATCH_LAT_MS.observe( (chrono::Utc::now().timestamp_millis() - t.ts) as f64 );
    }

    // 4) Add consumer offsets to transaction
    let mut tpl = TopicPartitionList::new();
    for (topic, partition, offset) in refs {
        tpl.add_partition_offset(&topic, *partition, rdkafka::Offset::Offset(offset+1))?;
    }
    producer.send_offsets_to_transaction(&tpl, &consumer.group_metadata(), Timeout::After(Duration::from_secs(5)))?; // [ADDED]

    // 5) Commit transaction (EOS)
    producer.commit_transaction(Timeout::After(Duration::from_secs(10)))?; // [ADDED]
    Ok(())
}

use rdkafka::consumer::{Consumer, StreamConsumer, Rebalance};
use rdkafka::ClientContext;

struct Ctx; impl ClientContext for Ctx {}

fn on_rebalance(reb: &Rebalance) {
  eprintln!("rebalance: {:?}", reb); // [ADDED] 필요 시 pause/resume
}

let consumer: StreamConsumer<Ctx> = ClientConfig::new()
  .set("bootstrap.servers", &cfg.kafka_brokers)
  .set("group.id", "matching-engine")
  .set("enable.auto.commit", "false")
  .create_with_context(Ctx)?;
// NOTE: rdkafka에서는 assign() 시점 처리, stream() 루프로도 가능

use serde_json::json;

async fn process_batch(...) -> Result<()> {
  ...
  if let Err(e) = db::insert_batch_raw(click, &rows).await {
    let err_payload = json!({
      "error": format!("{:?}", e),
      "ts": now_ms(),
      "batch_len": batch.len(),
    }).to_string();
    let _ = producer.send(
      BaseRecord::to("trades.dlq")                     // [ADDED]
        .payload(&err_payload)
        .key("clickhouse-insert"),
    );
    return Err(e);
  }
  ...
}

let mut cur_max_batch = max_batch; // [ADDED]
...
if let Err(e) = process_batch(...).await {
  cur_max_batch = (cur_max_batch / 2).max(50);     // [ADDED] 반감
} else {
  cur_max_batch = (cur_max_batch + 50).min(max_batch);  // [ADDED] 회복
}


let mut prod_cfg = ClientConfig::new();
prod_cfg
  .set("bootstrap.servers", &cfg.kafka_brokers)
  .set("security.protocol", "SASL_SSL")              // [ADDED]
  .set("ssl.ca.location", "/etc/kafka/ca/ca.crt")    // [ADDED]
  .set("sasl.mechanisms", "SCRAM-SHA-512")           // [ADDED]
  .set("sasl.username", std::env::var("KAFKA_SASL_USER").unwrap_or_default())
  .set("sasl.password", std::env::var("KAFKA_SASL_PASS").unwrap_or_default())
  .set("enable.idempotence", "true")
  .set("acks", "all")
  .set("transactional.id", &tx_id);
let producer = prod_cfg.create::<rdkafka::producer::BaseProducer>()?;


producer.send(
  BaseRecord::to(&cfg.output_topic)
    .payload(&tjson)
    .key(&t.trade_id)
    .headers(OwnedHeaders::new().add("schema-version","v1"))  // [ADDED]
)?;



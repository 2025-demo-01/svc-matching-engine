use anyhow::{anyhow, Result};
use clickhouse::Client as ChClient;
use once_cell::sync::OnceCell;            // 안전한 전역 보관
use rdkafka::{
    config::ClientConfig,
    consumer::{CommitMode, Consumer, Rebalance, StreamConsumer},
    message::BorrowedMessage,
    producer::{BaseProducer, BaseRecord},
    util::Timeout,
    Message, TopicPartitionList,
};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;

use crate::{
    config::Config,
    db,
    matching::{Order, Trade},
    metrics::{BATCH_LAT_MS, BATCH_SIZE, BACKPRESSURE, DLQ_TOTAL, MATCH_LAT_MS, MATCH_TOTAL},
    util::{getenv, now_ms},
};

// 전역: 안전한 프로듀서 핸들 저장소
static PRODUCER: OnceCell<Arc<BaseProducer>> = OnceCell::new();

/// 외부(main)에서 종료 시 트랜잭션 abort 호출
pub fn abort_inflight_tx(timeout_secs: u64) {
    if let Some(p) = PRODUCER.get() {
        let _ = p.abort_transaction(Timeout::After(Duration::from_secs(timeout_secs)));
    }
}

pub async fn run_loop(cfg: &Config) -> Result<()> {
    // ──────────────────────────────────────────────
    // Consumer (manual commit)
    // ──────────────────────────────────────────────
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("group.id", "matching-engine")
        .set("enable.auto.commit", "false")
        .set("max.partition.fetch.bytes", "1048576")
        .set("fetch.min.bytes", "65536")
        .set("fetch.wait.max.ms", "20")
        .create()?;

    // (옵션) 리밸런스 이벤트 로깅
    consumer.subscribe(&[&cfg.input_topic])?;

    // ──────────────────────────────────────────────
    // Producer with transactional.id (Exactly-Once)
    // ──────────────────────────────────────────────
    let tx_id = format!(
        "matching-engine-tx-{}",
        std::env::var("POD_NAME").unwrap_or_else(|_| "dev".into())
    );

    let mut prod_cfg = ClientConfig::new();
    prod_cfg
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("retries", "10")
        .set("request.timeout.ms", "5000")
        .set("transactional.id", &tx_id);

    // (TLS/SASL 필요 시 주석 해제)
    // prod_cfg
    //     .set("security.protocol","SASL_SSL")
    //     .set("ssl.ca.location","/etc/kafka/ca/ca.crt")
    //     .set("sasl.mechanisms","SCRAM-SHA-512")
    //     .set("sasl.username", std::env::var("KAFKA_SASL_USER").unwrap_or_default())
    //     .set("sasl.password", std::env::var("KAFKA_SASL_PASS").unwrap_or_default());

    let producer = Arc::new(prod_cfg.create::<BaseProducer>()?);
    let _ = PRODUCER.set(producer.clone()); // OnceCell: 최초 1회만 성공
    producer.init_transactions(Timeout::After(Duration::from_secs(10)))?;

    // ──────────────────────────────────────────────
    // ClickHouse client
    // ──────────────────────────────────────────────
    let click = ChClient::default().with_url(&cfg.clickhouse_url);

    // ──────────────────────────────────────────────
    // Batch / Window 설정
    // ──────────────────────────────────────────────
    let max_batch = getenv("BATCH_SIZE", "500").parse::<usize>().unwrap_or(500);
    let win_ms = getenv("BATCH_WINDOW_MS", "50").parse::<u64>().unwrap_or(50);
    let mut cur_max_batch = max_batch;

    let mut batch: Vec<Trade> = Vec::with_capacity(max_batch);
    let mut refs: Vec<(String, i32, i64)> = Vec::with_capacity(max_batch); // (topic, partition, offset)

    loop {
        // 윈도우 수집
        let start_win = Instant::now();
        while batch.len() < cur_max_batch && start_win.elapsed() < Duration::from_millis(win_ms) {
            match timeout(Duration::from_millis(10), consumer.recv()).await {
                Ok(Ok(m)) => {
                    if let Some(t) = handle_msg(&m) {
                        batch.push(t);
                        refs.push((m.topic().to_string(), m.partition(), m.offset()));
                    }
                }
                _ => {}
            }
        }

        BATCH_SIZE.set(batch.len() as i64);
        if batch.is_empty() {
            continue;
        }

        let bstart = Instant::now();

        // ClickHouse 지연 시 백프레셔(일시정지) 걸고 회복
        if let Err(e) = process_batch(&producer, &consumer, &click, &batch, &refs, cfg).await {
            DLQ_TOTAL.inc();
            eprintln!("batch error: {:?}", e);

            BACKPRESSURE.set(1);
            if let Ok(assign) = consumer.assignment() {
                let _ = consumer.pause(&assign);
                tokio::time::sleep(Duration::from_millis(500)).await;
                let _ = consumer.resume(&assign);
            }
            BACKPRESSURE.set(0);

            // 적응형: 실패 시 배치 반감 (최소 50)
            cur_max_batch = (cur_max_batch / 2).max(50);
        } else {
            // 성공시 점진 확대
            cur_max_batch = (cur_max_batch + 50).min(max_batch);
        }

        BATCH_LAT_MS.observe(bstart.elapsed().as_millis() as f64);
        batch.clear();
        refs.clear();
    }
}

fn handle_msg(m: &BorrowedMessage<'_>) -> Option<Trade> {
    let payload = m.payload_view::<str>().ok().flatten()?;
    let order: Order = match serde_json::from_str(payload) {
        Ok(o) => o,
        Err(_) => return None,
    };
    let trade = Trade::from_order(&order);
    Some(trade)
}

async fn process_batch(
    producer: &Arc<BaseProducer>,
    consumer: &StreamConsumer,
    click: &ChClient,
    batch: &Vec<Trade>,
    refs: &Vec<(String, i32, i64)>,
    cfg: &Config,
) -> Result<()> {
    // 1) Begin transaction
    producer.begin_transaction()?;

    // 2) ClickHouse insert (raw, 배치)
    let rows: Vec<db::TradeRow> = batch
        .iter()
        .map(|t| db::TradeRow {
            trade_id: t.trade_id.clone(),
            order_id: t.order_id.clone(),
            symbol: t.symbol.clone(),
            side: t.side.clone(),
            price: t.price,
            qty: t.qty,
            ts: t.ts,
        })
        .collect();

    if let Err(e) = db::insert_batch_raw(click, &rows).await {
        // 부분 실패/CH 다운 등 → DLQ로 전송하고 트랜잭션 abort
        let err_payload = json!({
            "error": format!("{:?}", e),
            "ts": now_ms(),
            "batch_len": batch.len(),
        })
        .to_string();
        let _ = producer.send(
            BaseRecord::to("trades.dlq").payload(&err_payload).key("clickhouse-insert"),
        );
        producer.abort_transaction(Timeout::After(Duration::from_secs(5)))?;
        return Err(anyhow!(e));
    }

    // 3) Produce trades.out (same TX)
    for t in batch {
        let tjson = serde_json::to_string(t)?;
        let record = BaseRecord::to(&cfg.output_topic)
            .payload(&tjson)
            .key(&t.trade_id)
            .headers(rdkafka::message::OwnedHeaders::new().add("schema-version", "v1"));
        producer.send(record)?;
        MATCH_TOTAL.inc();
        MATCH_LAT_MS.observe((now_ms() - t.ts) as f64);
    }

    // 4) Add consumer offsets to transaction
    let mut tpl = TopicPartitionList::new();
    for (topic, partition, offset) in refs {
        tpl.add_partition_offset(topic, *partition, rdkafka::Offset::Offset(offset + 1))?;
    }
    producer.send_offsets_to_transaction(
        &tpl,
        &consumer.group_metadata(),
        Timeout::After(Duration::from_secs(5)),
    )?;

    // 5) Commit transaction (EOS)
    producer.commit_transaction(Timeout::After(Duration::from_secs(10)))?;
    Ok(())
}

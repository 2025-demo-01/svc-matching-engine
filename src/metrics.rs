use lazy_static::lazy_static;
use prometheus::{
    register_histogram, register_int_counter, register_int_gauge, Histogram, IntCounter, IntGauge,
};

lazy_static! {
    pub static ref MATCH_LAT_MS: Histogram =
        register_histogram!("match_latency_ms", "per-message matching latency").unwrap();
    pub static ref BATCH_LAT_MS: Histogram =
        register_histogram!("batch_latency_ms", "batch processing latency").unwrap();
    pub static ref MATCH_TOTAL: IntCounter =
        register_int_counter!("matching_processed_total", "processed orders count").unwrap();
    pub static ref DLQ_TOTAL: IntCounter =
        register_int_counter!("matching_dlq_total", "dead-lettered messages").unwrap();
    pub static ref BACKPRESSURE: IntGauge =
        register_int_gauge!("matching_backpressure_active", "1 if backpressure").unwrap();
    pub static ref BATCH_SIZE: IntGauge =
        register_int_gauge!("matching_batch_size", "current batch size").unwrap();
}

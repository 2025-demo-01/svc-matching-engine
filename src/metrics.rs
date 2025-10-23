use lazy_static::lazy_static;
use prometheus::{register_histogram, register_int_counter, Histogram, IntCounter};

lazy_static! {
    pub static ref MATCH_LATENCY: Histogram =
        register_histogram!("matching_latency_ms", "matching latency in ms").unwrap();
    pub static ref MATCH_TOTAL: IntCounter =
        register_int_counter!("matching_processed_total", "processed orders count").unwrap();
}

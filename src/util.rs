use std::env;
pub fn getenv(k: &str, d: &str) -> String { env::var(k).unwrap_or_else(|_| d.to_string()) }
pub fn now_ms() -> i64 { chrono::Utc::now().timestamp_millis() }

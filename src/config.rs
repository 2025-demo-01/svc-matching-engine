use std::env;

pub struct Config {
    pub kafka_brokers: String,
    pub input_topic: String,
    pub output_topic: String,
    pub clickhouse_url: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            kafka_brokers: env::var("KAFKA_BROKERS").unwrap_or("localhost:9092".to_string()),
            input_topic: env::var("ORDERS_IN_TOPIC").unwrap_or("orders.in".to_string()),
            output_topic: env::var("TRADES_OUT_TOPIC").unwrap_or("trades.out".to_string()),
            clickhouse_url: env::var("CLICKHOUSE_URL").unwrap_or("http://clickhouse:8123".to_string()),
        }
    }
}

mod config;
mod kafka;
mod matching;
mod db;
mod metrics;

use tracing_subscriber;
use crate::config::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();
    let cfg = Config::from_env();
    kafka::run_loop(&cfg).await?;
    Ok(())
}

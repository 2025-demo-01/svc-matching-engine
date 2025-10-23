mod config; mod kafka; mod matching; mod db; mod metrics; mod util;

use tracing_subscriber;
use crate::config::Config;
use anyhow::Result;
use prometheus::{TextEncoder, Encoder};
use hyper::{Body, Response, Request, Server, Method, StatusCode}; // [ADDED]
use std::net::SocketAddr;
use tokio::signal; // [ADDED]

async fn metrics_srv(addr: SocketAddr) {
    let make_svc = hyper::service::make_service_fn(|_| async {
        Ok::<_, hyper::Error>(hyper::service::service_fn(|req: Request<Body>| async move {
            if req.method() == Method::GET && req.uri().path() == "/metrics" {
                let metric_families = prometheus::gather();
                let mut buf = Vec::new();
                TextEncoder::new().encode(&metric_families, &mut buf).unwrap();
                Ok::<_, hyper::Error>(Response::new(Body::from(buf)))
            } else {
                let mut not_found = Response::default();
                *not_found.status_mut() = StatusCode::NOT_FOUND;
                Ok(not_found)
            }
        }))
    });
    let server = Server::bind(&addr).serve(make_svc);
    let _ = server.await;
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cfg = Config::from_env();

    // /metrics HTTP (9090)
    tokio::spawn(metrics_srv(([0,0,0,0], 9090).into()));  // [ADDED]

    // run matching loop (Ctrl-C graceful exit)
    tokio::select! {
        _ = kafka::run_loop(&cfg) => {},
        _ = signal::ctrl_c() => { eprintln!("shutting down"); }
    }
    Ok(())
}

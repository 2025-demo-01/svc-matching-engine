mod config;
mod kafka;
mod matching;
mod db;
mod metrics;
mod util;

use anyhow::Result;
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use prometheus::{Encoder, TextEncoder};
use std::net::SocketAddr;
use tokio::signal;
use tracing_subscriber;

async fn metrics_srv(addr: SocketAddr) {
    let make_svc = hyper::service::make_service_fn(|_| async {
        Ok::<_, hyper::Error>(hyper::service::service_fn(
            |req: Request<Body>| async move {
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
            },
        ))
    });

    let server = Server::bind(&addr).serve(make_svc);
    let _ = server.await;
}

#[tokio::main]
async fn main() -> Result<()> {
    // 로그/트레이싱
    tracing_subscriber::fmt().with_env_filter("info").init();

    // 설정 로드
    let cfg = config::Config::from_env();

    // /metrics HTTP (9090) — 백그라운드로 실행
    tokio::spawn(metrics_srv(([0, 0, 0, 0], 9090).into()));

    // 매칭 루프 vs 종료 시그널 경합
    tokio::select! {
        run = kafka::run_loop(&cfg) => {
            if let Err(e) = run {
                eprintln!("run_loop error: {e:?}");
            }
        }
        _ = signal::ctrl_c() => {
            eprintln!("SIGTERM/SIGINT received: aborting transactional producer");
            // 안전한 트랜잭션 abort (kafka 모듈 내 OnceCell)
            kafka::abort_inflight_tx(3);
        }
    }

    Ok(())
}

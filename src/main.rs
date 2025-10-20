use axum::{routing::get, Router, response::IntoResponse};
use once_cell::sync::Lazy;
use prometheus::{
    register_counter, register_gauge, register_histogram, Encoder, Histogram, IntCounter, IntGauge,
    TextEncoder,
};
use rand::{thread_rng, Rng};
use std::{net::SocketAddr, time::{Duration, Instant}};
use tokio::{signal, task::JoinSet, time::sleep};

static ORDERS_TOTAL: Lazy<IntCounter> =
    Lazy::new(|| register_counter!("me_orders_total", "Total orders processed").unwrap());
static QUEUE_DEPTH: Lazy<IntGauge> =
    Lazy::new(|| register_gauge!("me_queue_depth", "In-memory order queue depth").unwrap());
static MATCH_LATENCY: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "me_match_latency_ms",
        "Matching latency in milliseconds",
        // 0.1ms ~ ~100ms 구간
        vec![0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 12.0, 20.0, 30.0, 50.0, 75.0, 100.0]
            .into_iter()
            .map(|x| x / 1000.0) // seconds 단위(프로메테우스 규약)로 변환
            .collect()
    )
    .unwrap()
});
static CPU_THROTTLED_SEC: Lazy<IntGauge> =
    Lazy::new(|| register_gauge!("me_cpu_cfs_throttled_seconds", "CFS throttled secs (node-exporter derived)").unwrap());

async fn metrics() -> impl IntoResponse {
    let mut buf = Vec::new();
    let enc = TextEncoder::new();
    let mf = prometheus::gather();
    enc.encode(&mf, &mut buf).unwrap();
    ([(axum::http::header::CONTENT_TYPE, enc.format_type())], buf)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // 매칭 루프(저지연 HotPath)
    let mut set = JoinSet::new();

    // Mock: CPU throttling gauge feed (실환경에선 node-exporter/ebpf로 수집)
    set.spawn(async move {
        loop {
            // 예시: 외부 exporter로부터 값 동기화 자리
            CPU_THROTTLED_SEC.set(0);
            sleep(Duration::from_secs(5)).await;
        }
    });

    // 주문 큐 + 매칭
    set.spawn(async move {
        let mut rng = thread_rng();
        loop {
            // 큐 적재
            let incoming = rng.gen_range(50..150);
            for _ in 0..incoming {
                QUEUE_DEPTH.inc();
            }

            // 매칭 처리
            let batch = rng.gen_range(40..120);
            let t0 = Instant::now();
            // 매칭 연산 비용(저지연 루프)
            // 실전: OrderBook/PriceLevel/Heap/SkipList 등 자료구조 최적화
            busy_wait_ns(rng.gen_range(50_000..200_000)); // 50~200us
            let _ = t0.elapsed();

            for _ in 0..batch {
                if QUEUE_DEPTH.get() > 0 {
                    QUEUE_DEPTH.dec();
                    ORDERS_TOTAL.inc();
                }
            }

            let el = t0.elapsed().as_secs_f64(); // seconds
            MATCH_LATENCY.observe(el);

            sleep(Duration::from_millis(1)).await;
        }
    });

    // HTTP: /metrics
    let app = Router::new().route("/metrics", get(metrics));
    let addr: SocketAddr = "0.0.0.0:9100".parse().unwrap();
    let server = axum::Server::bind(&addr).serve(app.into_make_service());

    tokio::select! {
        _ = server => {}
        _ = signal::ctrl_c() => {}
    }

    while let Some(_) = set.join_next().await {}
}

// 간단한 Busy-wait (ns)
fn busy_wait_ns(ns: u64) {
    use std::arch::x86_64::_rdtsc;
    // TSC 기반 루프 (PoC). 실제로는 OS/코어 affinity/isolated CPU 정책으로 보완.
    let start = unsafe { _rdtsc() };
    let target_cycles = ns * 3; // 대략 보수치(코어 클럭에 따라 조정)
    loop {
        let now = unsafe { _rdtsc() };
        if now - start >= target_cycles {
            break;
        }
        std::hint::spin_loop();
    }
}

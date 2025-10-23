# Incident Runbook — Matching Engine

## Alerts
- MatchingP95LatencyHigh (p95 > 50ms 10m)
- MatchingBackpressureActive (5m)
- MatchingDLQ (any)

## Quick Triage
1) Grafana Dashboard 확인 (batch latency, backpressure, DLQ)
2) Kafka Lag 확인 (KEDA 트리거 발동 여부)
3) ClickHouse 상태 확인 (qps, inserts, storage)
4) 최근 배포 여부 (Argo CD history)

## Actions
- Backpressure 활성 시:
  - 배치 크기(BATCH_SIZE) 50% 감소 후 재배포
  - KEDA maxReplicaCount 2x 증가
- DLQ 증가 시:
  - trades.dlq consume → 오류 유형 분류 → 부분 재처리
- 긴급 완화:
  - 소비 일시 정지(consumer.pause) 후 upstream 감속
  - ClickHouse 장애면 Egress를 EgressGW로 reroute(옵션)

**✅ Thanos Query Frontend 캐시 히트율 모니터링 가이드**

Thanos Query Frontend의 캐시 성능을 실시간으로 모니터링하는 방법을 정리했습니다.

### 1. 핵심 PromQL (바로 사용 가능)

#### **① 캐시 히트율 (가장 중요)**
```promql
# Hit Rate (%)
(
  sum(rate(thanos_query_frontend_queries_total{result="hit"}[5m]))
  / 
  sum(rate(thanos_query_frontend_queries_total[5m]))
) * 100
```

#### **② Hit vs Miss 비교**
```promql
sum(rate(thanos_query_frontend_queries_total{result=~"hit|miss"}[5m])) by (result)
```

#### **③ Cache Miss Rate**
```promql
(
  sum(rate(thanos_query_frontend_queries_total{result="miss"}[5m]))
  / 
  sum(rate(thanos_query_frontend_queries_total[5m]))
) * 100
```

#### **④ 전체 쿼리량 + Hit Rate**
```promql
# Left Y-axis: Total Queries, Right Y-axis: Hit Rate
sum(rate(thanos_query_frontend_queries_total[5m])) 
and
(
  sum(rate(thanos_query_frontend_queries_total{result="hit"}[5m]))
  / 
  sum(rate(thanos_query_frontend_queries_total[5m]))
) * 100
```

---

### 2. Grafana 패널 추천 설정

**Panel 1: Cache Hit Rate (Stat or Gauge)**
- Query: 위 ①번 PromQL
- Unit: Percent (0-100)
- Thresholds:
  - Base: 85 (Green)
  - Warning: 70 (Yellow)
  - Critical: 50 (Red)

**Panel 2: Hit vs Miss (Time Series)**
- Legend: `{{result}}`
- Stacked Graph 추천

**Panel 3: Query Duration**
```promql
rate(thanos_query_frontend_query_duration_seconds_sum[5m]) 
/ 
rate(thanos_query_frontend_query_duration_seconds_count[5m])
```
- Unit: Seconds

**Panel 4: Cache Operation**
- `thanos_query_frontend_cache_requests_total{operation=~".*"}`

---

### 3. OpenShift에서 모니터링 설정 방법

#### A. OpenShift Console Metrics에서 바로 확인
1. OpenShift Console → **Observe → Metrics**
2. 위 PromQL 붙여넣기

#### B. Grafana Dashboard 생성 추천
- Dashboard Title: `Thanos Query Frontend - Cache Monitoring`
- Namespace: `openshift-monitoring`
- Data Source: Thanos Querier 또는 Prometheus

#### C. Alert Rule 예시 (PrometheusRule)

```yaml
- alert: ThanosQueryFrontendLowCacheHitRate
  expr: |
    (
      sum(rate(thanos_query_frontend_queries_total{result="hit"}[10m]))
      / 
      sum(rate(thanos_query_frontend_queries_total[10m]))
    ) * 100 < 65
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Thanos Query Frontend Cache Hit Rate is low ({{ $value }}%)"
```

---

### 4. 캐시 히트율 해석 가이드

| Hit Rate     | 상태          | 추천 조치 |
|--------------|---------------|----------|
| **90%+**     | Excellent     | 현재 설정 우수 |
| **80~90%**   | Good          | 유지 |
| **65~80%**   | Warning       | Recording Rules 강화, split-interval 조정 |
| **< 65%**    | Poor          | Cache TTL 증가, step alignment 확인, 쿼리 패턴 최적화 |

**히트율이 낮은 주요 원인**:
- `step` 값이 매번 다름
- 시간 범위가 불규칙 (`time()` 함수 사용)
- `topk()`나 `histogram_quantile` 같은 비-캐시 친화적 쿼리过多
- `--query-range.split-interval`이 너무 작음

---

### 5. aibox_monitoring_portal.py 연동 Tip

Python 코드에서 캐시 효율을 높이려면:

```python
# 쿼리 호출 전
def get_cache_friendly_query(self, base_query: str, step: str = "1m"):
    return {
        "query": base_query,
        "step": step,           # 항상 동일 step 유지
        "start": aligned_start, #  step 배수로 정렬
        "end": aligned_end
    }
```

**지금 바로 확인해 보세요!**

OpenShift Console Metrics에 아래 PromQL을 넣어보고, **현재 Hit Rate**를 알려주시면 더 구체적인 튜닝을 도와드리겠습니다.

```promql
(
  sum(rate(thanos_query_frontend_queries_total{result="hit"}[5m])) 
  / 
  sum(rate(thanos_query_frontend_queries_total[5m]))
) * 100
```

필요하시면 **전용 Grafana JSON Dashboard**도 만들어 드릴 수 있습니다.

**✅ Thanos Query Frontend Redis 캐시 히트율 확인 방법**

### 1. 주요 PromQL (가장 추천)

**Cache Hit Rate (전체)**:
```promql
sum(rate(thanos_query_frontend_queries_total{result="hit"}[5m])) 
/ 
sum(rate(thanos_query_frontend_queries_total[5m]))
```

**Cache Hit Rate (%)** (Grafana에서 사용하기 좋음):
```promql
(
  sum(rate(thanos_query_frontend_queries_total{result="hit"}[5m])) 
  / 
  sum(rate(thanos_query_frontend_queries_total[5m]))
) * 100
```

**Cache Miss Rate**:
```promql
(
  sum(rate(thanos_query_frontend_queries_total{result="miss"}[5m])) 
  / 
  sum(rate(thanos_query_frontend_queries_total[5m]))
) * 100
```

---

### 2. OpenShift에서 바로 확인하는 방법

**Thanos Query Frontend Pod에서 Metrics 확인**:
```bash
# Query Frontend Pod 목록 확인
oc get pods -n openshift-monitoring -l app.kubernetes.io/component=query-frontend

# 특정 Pod의 metrics 직접 확인
oc port-forward -n openshift-monitoring pod/thanos-query-frontend-xxx 9092:9092
# → 브라우저에서 http://localhost:9092/metrics 접속 후 "thanos_query_frontend_queries_total" 검색
```

**Prometheus에서 직접 쿼리** (추천):
1. OpenShift Console → **Monitoring → Metrics**
2. 위 PromQL 중 하나 붙여넣기
3. Graph 또는 Table로 확인

---

### 3. Grafana Dashboard 추천 패널

**Panel Title**: Thanos Query Frontend Cache Hit Rate

- **Type**: Time Series 또는 Stat
- **Query**:
  ```promql
  sum(rate(thanos_query_frontend_queries_total{result="hit"}[5m])) 
  / 
  sum(rate(thanos_query_frontend_queries_total[5m])) * 100
  ```
- Unit: Percent (0-100)
- Threshold: 70% (Warning), 40% (Critical)

**추가 추천 패널**:
- Total Queries: `sum(rate(thanos_query_frontend_queries_total[5m]))`
- Hit vs Miss: `thanos_query_frontend_queries_total{result=~"hit|miss"}`
- Average Query Duration: `rate(thanos_query_frontend_query_duration_seconds_sum[5m]) / rate(thanos_query_frontend_query_duration_seconds_count[5m])`

---

### 4. 좋은 Cache Hit Rate 기준

| 환경          | 목표 Hit Rate | 비고 |
|---------------|---------------|------|
| 일반 대시보드 | **70~85%**    | 양호 |
| 반복 조회 많은 포털 (`aibox_monitoring_portal.py`) | **85% 이상** | 우수 |
| 90%+          | 최적          | Recording Rules + 동일 쿼리 패턴 |

**Hit Rate가 낮은 경우 원인**:
- `--query-range.split-interval`이 너무 작음
- Recording Rules를 아직 안 썼음
- Cache TTL (expiration) 너무 짧음
- Query에 `time()`이나 `timestamp()` 같은 동적 함수가 많음
- `namespace!~` 같은 필터가 너무 다양함

---

**지금 바로 확인해 보세요**:

위 **Cache Hit Rate** PromQL을 OpenShift Console Metrics에 넣어보시고, 현재 수치를 알려주세요.  
수치에 따라 추가 튜닝(예: split-interval 조정, cache TTL 변경, Recording Rules 강화)을 도와드리겠습니다!

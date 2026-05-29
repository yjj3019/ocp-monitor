**✅ Thanos Querier PromQL Optimization Guide**  
(특히 **OpenShift + KubeVirt / Virtualization** 환경 + `aibox_monitoring_portal.py`에 적용하기 좋게 정리)

### 1. Thanos Querier의 주요 특징 (Prometheus와 다른 점)

- **분산 환경**에서 여러 Store (Sidecar, Store Gateway, Ruler 등)로부터 데이터를 fan-out + deduplication
- 장기 데이터 조회 시 **Object Storage (S3)** 접근으로 latency ↑
- **Thanos PromQL Engine** (`--query.promql-engine=thanos`) 사용 시 더 나은 optimizer 활용 가능
- **Query Frontend**을 앞단에 두면 **caching + query sharding** 자동 적용 (강력 추천)

---

### 2. 가장 중요한 최적화 전략 (우선순위 순)

| 우선순위 | 최적화 항목 | 구체적 방법 | 기대 효과 |
|---------|------------|------------|----------|
| ★★★★★ | **Early Filtering** | Label selector를 최대한 구체적으로 사용 (`namespace`, `pod`, `name`, `vmname` 등) | Series cardinality 대폭 감소 |
| ★★★★★ | **Avoid Regex** | `=~` 대신 `=` 사용 | Parser + Index lookup 속도 크게 향상 |
| ★★★★ | **Aggregation Pushdown** | `sum by (label)`을 최대한 앞쪽에 배치 | Thanos가 불필요한 raw series를 줄임 |
| ★★★★ | **Step Interval 조정** | Grafana/Python에서 `step = range / 250` ~ `/ 1000` 정도로 설정 | Over-sampling 방지 |
| ★★★ | **Recording Rules 적극 활용** | 자주 쓰는 복잡한 VM 메트릭을 미리 pre-compute | Query 시간 5~20배 개선 |
| ★★★ | **Time Range 제한** | 최근 1h / 6h / 24h 위주로 설계 | Long-range (7d+)는 Query Frontend 캐시 의존 |

---

### 3. KubeVirt / Virtualization 환경 추천 최적화 예시

**Before (비효율)**
```promql
sum(rate(kubevirt_vmi_network_receive_bytes_total[5m]))
```

**After (최적화)**
```promql
sum by (namespace, name) (
  rate(kubevirt_vmi_network_receive_bytes_total{namespace=~"prod-.*", name=~".+"}[5m])
)
```

**VM Density + Resource Usage 최적화 예시**:
```promql
# VM별 CPU 사용률 (Top 10)
topk(10, 
  sum by (namespace, name) (
    rate(kubevirt_vmi_vcpu_seconds_total[2m])
  )
)

# Storage Filesystem Usage (Guest Agent)
sum by (namespace, name) (
  kubevirt_vmi_filesystem_usage_bytes{guest_agent="true"}
)
```

**Migration & Health 체크**:
```promql
# Live Migration 현황
sum(kubevirt_migration_vmi_running) 
+ 
sum(kubevirt_vm_migration_failed_total[1h])

# KubeVirt Component Health
kubevirt_virt_operator_ready * 100
```

---

### 4. Python 코드 (`aibox_monitoring_portal.py`) 적용 팁

1. **Query 함수 개선**
```python
def _query(self, query: str, timeout=30, step=None):
    params = {
        "query": query,
        "timeout": f"{timeout}s"
    }
    if step:
        params["step"] = step   # 중요!
    
    # Thanos Query Frontend이 있다면 caching header 활용 가능
    return self.thanos_session.get(
        f"{self.thanos_url}/api/v1/query_range", 
        params=params,
        headers={"X-Thanos-Query-Frontend-Cache": "true"}  # 가능하면
    )
```

2. **Range Query 시 Step 자동 계산**
```python
def smart_step(self, start, end):
    duration = end - start
    if duration <= 3600:      # 1시간
        return "30s"
    elif duration <= 86400:   # 1일
        return "2m"
    else:
        return "10m"          # 장기
```

3. **Fallback 전략**
   - Thanos Querier 실패 → Prometheus-k8s 직접 fallback
   - Timeout 15~30초로 제한 (너무 길면 UI 체감 나빠짐)

---

### 5. 인프라 측면 추천 (Thanos 설정)

- **Query Frontend** 반드시 배포 → Query caching + 자동 sharding
- Thanos Querier에 `--query.promql-engine=thanos` + `--query.mode=distributed` (가능하면)
- Store Gateway에 충분한 CPU/Memory + Index Cache 크게 설정
- Compactor가 Downsampling (5m, 1h) 잘 수행 중인지 확인

---

**추가로 원하시면**:

- 당신의 `aibox_monitoring_portal.py`에 들어있는 **주요 PromQL 5~10개**를 공유해주시면, **직접 최적화 버전**으로 리팩토링해 드리겠습니다.
- Recording Rules YAML 제안
- Query Frontend + Thanos Querier Helm Values 추천

필요한 부분 알려주세요!

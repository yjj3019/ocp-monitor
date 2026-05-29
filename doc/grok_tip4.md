**✅ Thanos Query Frontend 캐싱 전략** (실전 중심)

Thanos Query Frontend의 캐싱은 **반복되는 대시보드/포털 쿼리**(`aibox_monitoring_portal.py`)에서 가장 큰 성능 향상을 가져옵니다.

### 1. 캐싱 전략 개요

Query Frontend은 주로 **Range Query** (`/api/v1/query_range`)를 대상으로 캐싱합니다.
- **Query Splitting** + **Result Caching** 조합이 핵심
- 캐시 히트 시 Querier → Store Gateway 부하를 크게 줄임
- **Recording Rules**와 함께 사용하면 효과 극대화

### 2. 추천 캐시 백엔드 비교

| 백엔드          | 추천 환경                  | 장점                              | 단점                        | 추천도 (당신 환경) |
|-----------------|---------------------------|-----------------------------------|-----------------------------|-------------------|
| **In-Memory**   | 소규모, 테스트            | 설정 간단, 가장 빠름              | Pod 재시작 시 캐시 소실     | ★★ |
| **Memcached**   | **가장 추천**             | 안정적, Multi-Pod 공유 쉬움       | Persistence 없음            | ★★★★★ |
| **Redis**       | HA, Persistence 필요      | Sentinel 지원, Persistence 가능   | Memcached보다 약간 무거움   | ★★★★ |

**당신의 aibox_monitoring_portal.py**에는 **Memcached**를 **1순위**, **Redis**를 **2순위**로 추천합니다.

### 3. 최적의 Cache Config YAML (2026 기준)

#### Memcached 버전 (강력 추천)
```yaml
type: MEMCACHED
config:
  addresses:
    - dnssrv+_memcached._tcp.memcached.openshift-monitoring.svc.cluster.local
  timeout: 500ms
  max_idle_connections: 200
  max_async_concurrency: 30
  max_async_buffer_size: 10000
  max_get_multi_concurrency: 200
  max_get_multi_batch_size: 0
  max_item_size: 16MiB
  expiration: 6h          # 중요: TTL
  dns_provider_update_interval: 10s
```

#### Redis 버전
```yaml
type: REDIS
config:
  addr: thanos-redis.openshift-monitoring.svc.cluster.local:6379
  db: 0
  dial_timeout: 5s
  read_timeout: 3s
  write_timeout: 3s
  expiration: 6h
```

### 4. Query Frontend Deployment 핵심 Flags

```yaml
args:
- query-frontend
- --http-address=0.0.0.0:10902
- --query-frontend.downstream-url=http://thanos-querier.openshift-monitoring.svc.cluster.local:9091
- --query-range.split-interval=12h          # 6h ~ 24h 사이 추천
- --query-range.max-split-interval=24h
- --query-range.response-cache-config-file=/etc/thanos/cache-config.yaml
- --labels.response-cache-config-file=/etc/thanos/cache-config.yaml   # Labels/Series API도 캐싱
- --query-frontend.log-queries-longer-than=8s
- --query-frontend.max-outstanding-requests=2000
- --query-range.align-range-with-step=true     # Cache hit율 크게 향상 (주의: PromQL strict 모드에서는 off)
```

### 5. 실전 캐싱 전략 (aibox 환경 맞춤)

1. **Tiered TTL 전략**
   - 최근 1시간 데이터 → TTL 5~10분
   - 1시간 ~ 24시간 → TTL 30분 ~ 2시간
   - 24시간 이상 → TTL 6~12시간

2. **Recording Rules + Cache 조합**
   - `vmi:cpu_usage_seconds:rate2m`, `vmi:memory_usage_percent:current` 같은 Recording Rule은 캐시 효율 극고

3. **Python 측 전략**
   - 동일 PromQL은 **최소 15~25초** 간격으로 호출 (너무 자주 호출하면 캐시 무효화)
   - `step` 파라미터를 **일관되게** 유지 (예: 30s, 1m, 2m)

4. **Cache Hit Rate 목표**
   - **85% 이상** → 우수
   - **70~85%** → 양호
   - 70% 미만 → split-interval, Recording Rules, step alignment 조정 필요

---

**다음 단계로 원하시면**:

- Memcached 전체 Deployment + Service YAML
- Redis + Sentinel HA 버전
- OpenShift `cluster-monitoring-config`에 통합하는 방법
- 현재 Hit Rate를 높이기 위한 구체적인 PromQL 수정 제안

필요한 부분 알려주세요!

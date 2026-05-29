**✅ Thanos Query Frontend + Redis 캐싱 설정**

OpenShift 환경에서 **Query Frontend Redis 캐싱**을 활성화하는 방법을 정리했습니다.

### 1. OpenShift 제한사항
Cluster Monitoring Operator(CMO)를 통해 `cluster-monitoring-config`에서 Query Frontend을 활성화할 수는 있지만, **Redis 캐시 설정은 직접적인 지원이 제한적**입니다.  
대부분의 경우 **Redis + ConfigMap + Custom Deployment** 방식 또는 **Memcached** (더 가벼움)를 추천합니다.

---

### 2. 추천 구성: Redis Cache 설정 (Query Frontend)

#### Step 1: Redis Deployment (예시)
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: thanos-redis
  namespace: openshift-monitoring
spec:
  replicas: 2
  selector:
    matchLabels:
      app: thanos-redis
  template:
    metadata:
      labels:
        app: thanos-redis
    spec:
      containers:
      - name: redis
        image: redis:7-alpine
        resources:
          requests:
            cpu: 500m
            memory: 1Gi
          limits:
            cpu: 1
            memory: 2Gi
        command: ["redis-server", "--save", "''", "--appendonly", "no"]
---
apiVersion: v1
kind: Service
metadata:
  name: thanos-redis
  namespace: openshift-monitoring
spec:
  selector:
    app: thanos-redis
  ports:
  - port: 6379
    targetPort: 6379
```

#### Step 2: Cache ConfigMap 생성
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: thanos-query-frontend-cache
  namespace: openshift-monitoring
data:
  cache-config.yaml: |
    type: REDIS
    config:
      addr: thanos-redis.openshift-monitoring.svc.cluster.local:6379
      db: 0
      dial_timeout: 5s
      read_timeout: 3s
      write_timeout: 3s
      max_get_multi_concurrency: 100
      get_multi_batch_size: 100
      max_set_multi_concurrency: 100
      set_multi_batch_size: 100
      tls_enabled: false
```

#### Step 3: cluster-monitoring-config 수정 (Query Frontend 강화)
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-monitoring-config
  namespace: openshift-monitoring
data:
  config.yaml: |
    thanosQuerier:
      replicas: 2
      resources:
        requests:
          cpu: 500m
          memory: 2Gi
        limits:
          cpu: 2
          memory: 6Gi
      queryFrontend:
        enabled: true
        replicas: 2
        resources:
          requests:
            cpu: 300m
            memory: 1Gi
          limits:
            cpu: 1
            memory: 3Gi
        # 아래는 CMO가 지원하는 범위 (추가 arg는 Custom 필요)
```

**CMO에서 완전한 Redis 연동이 안 될 경우** → Bitnami Thanos Helm Chart나 직접 Query Frontend Deployment를 추천합니다.

---

### 3. Query Frontend Deployment 예시 (Redis 연동)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: thanos-query-frontend
  namespace: openshift-monitoring
spec:
  replicas: 2
  template:
    spec:
      containers:
      - name: query-frontend
        image: quay.io/thanos/thanos:v0.37.0  # OpenShift 버전에 맞게
        args:
        - query-frontend
        - --http-address=0.0.0.0:10902
        - --query-frontend.downstream-url=http://thanos-querier.openshift-monitoring.svc.cluster.local:9091
        - --query-range.split-interval=12h          # 중요: 긴 쿼리 분할
        - --query-range.response-cache-config-file=/etc/thanos/cache-config.yaml
        - --labels.response-cache-config-file=/etc/thanos/cache-config.yaml
        - --query-frontend.log-queries-longer-than=10s
        volumeMounts:
        - name: cache-config
          mountPath: /etc/thanos
      volumes:
      - name: cache-config
        configMap:
          name: thanos-query-frontend-cache
```

---

### 4. Python (`aibox_monitoring_portal.py`) 연결 변경

Query Frontend을 사용하도록 URL 변경:

```python
self.thanos_url = "https://thanos-query-frontend-openshift-monitoring.apps.yourdomain.com"  
# 또는 내부: http://thanos-query-frontend.openshift-monitoring.svc.cluster.local:10902
```

---

### 5. 검증 명령어

```bash
# Query Frontend Pod 로그 확인
oc logs -n openshift-monitoring -l app=thanos-query-frontend -c query-frontend | grep cache

# Redis 연결 확인
oc exec -n openshift-monitoring deploy/thanos-redis -- redis-cli ping
```

**성능 기대 효과**:
- 동일 PromQL 반복 조회 시 **90% 이상 캐시 히트** 가능
- Recording Rules + Query Frontend 조합 시 대시보드 로딩 속도 **3~10배** 개선

---

**Memcached vs Redis**  
- **Memcached**: 더 가볍고 Thanos에서 잘 지원됨 (많은 사람들이 Memcached 추천)
- **Redis**: Persistence 필요하거나 Sentinel HA 구조를 원할 때

Memcached 버전이 필요하시면 바로 만들어 드리겠습니다.  
또는 현재 OpenShift 버전(`oc version`)을 알려주시면 더 정확하게 맞춰드릴 수 있어요!

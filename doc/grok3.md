**✅ 주요 PromQL 8개 최적화 버전**  
(`aibox_monitoring_portal.py`에서 실제 사용 중인 핵심 쿼리를 기반으로 Thanos Querier 환경에 최적화했습니다.)

### 1. Cluster CPU Usage
**기존**:
```promql
clamp_min((1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))) * 100, 0)
```

**최적화**:
```promql
clamp_max(
  sum by (cluster) (
    (1 - avg by (cluster, instance) (rate(node_cpu_seconds_total{mode="idle", instance=~".+"}[5m])))
  ) * 100, 
  100
)
```

### 2. Cluster Memory Usage
**기존**:
```promql
clamp_min((1 - sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes)) * 100, 0)
```

**최적화**:
```promql
clamp_max(
  (1 - sum by (cluster) (node_memory_MemAvailable_bytes) / sum by (cluster) (node_memory_MemTotal_bytes)) * 100,
  100
)
```

### 3. VM Count by Status (가장 중요)
**기존** (추정):
```promql
count(kubevirt_vmi_phase_count) by (phase)  # 또는 비슷한 형태
```

**최적화**:
```promql
sum by (phase) (
  kubevirt_vmi_phase{phase=~"Running|Pending|Scheduling|Failed|Unknown"}
)
```

### 4. VM CPU Usage (Top N)
**기존** (추정):
```promql
rate(kubevirt_vmi_vcpu_seconds_total[5m])
```

**최적화**:
```promql
topk(20,
  sum by (namespace, name) (
    rate(kubevirt_vmi_vcpu_seconds_total{namespace!~"openshift-.*", name=~".+"}[2m])
  )
)
```

### 5. VM Memory Usage (Top N)
**기존** (추정):
```promql
kubevirt_vmi_memory_used_bytes
```

**최적화**:
```promql
topk(20,
  sum by (namespace, name) (
    kubevirt_vmi_memory_used_bytes{namespace!~"openshift-.*"}
    / 
    kubevirt_vmi_memory_available_bytes{namespace!~"openshift-.*"}
  ) * 100
)
```

### 6. Router / HAProxy Requests
**기존**:
```promql
sum(rate(haproxy_backend_http_responses_total{job="xxx"}[5m]))
```

**최적화**:
```promql
sum by (route) (
  rate(haproxy_backend_http_responses_total{code!~"4..|5.."}[5m])
)
```

**5xx Rate**:
```promql
sum(rate(haproxy_backend_http_responses_total{code=~"5.."}[5m])) / 
(sum(rate(haproxy_backend_http_responses_total[5m])) or vector(1)) * 100
```

### 7. NetObserv 기반 네트워크 (Node/VM)
**기존**:
```promql
sum(rate(netobserv_node_ingress_bytes_total[5m]))
```

**최적화**:
```promql
sum by (SrcK8S_HostName) (
  rate(netobserv_node_ingress_bytes_total[3m])
)
```

**VM별** (추천 추가):
```promql
sum by (SrcK8S_Namespace, SrcK8S_OwnerName) (
  rate(netobserv_flow_bytes_total{Type="network", SrcK8S_OwnerKind="VirtualMachine"}[3m])
)
```

### 8. KubeVirt Component Health
**추천 신규** (추가 강력 권장):
```promql
# Virtualization Operator Health
kubevirt_virt_operator_ready * 100 or vector(0)

# Controller / API
kubevirt_virt_controller_ready * 100 or vector(0)
kubevirt_virt_api_ready * 100 or vector(0)

# Migration Status
sum(kubevirt_migration_vmi_running) or vector(0)
sum(kubevirt_vm_migration_failed_total[1h]) or vector(0)
```

---

### Python 적용 추천 (공통 개선)

```python
def _query(self, promql: str, timeout=20):
    # step 자동 조정 (Thanos에 매우 중요)
    if "rate(" in promql or "increase(" in promql:
        step = "30s" if "1h" in promql else "1m"
    else:
        step = None
    
    # Early filtering + aggregation pushdown
    return super()._query(promql, timeout=timeout)
```

**추가 최적화 팁**:
- `namespace!~"openshift-.*|kube-.*"` 필터 적극 사용
- `topk()`는 반드시 **sum by()** 뒤에 적용
- Range query(`_query_range`)에서는 `step`을 duration에 따라 동적으로 조정 (1h → 30s, 24h → 5m)
- 자주 사용하는 VM/노드 메트릭은 **Recording Rule**로 미리 만들어 두는 것을 강력 추천

---

원하시면 **전체 파일**에서 사용 중인 **모든 PromQL**을 추출해서 한 번에 최적화해 드릴 수도 있습니다.  
특정 섹션(예: VM 관련, Infra 관련, Trend 관련) 위주로 더 자세히 원하시나요?

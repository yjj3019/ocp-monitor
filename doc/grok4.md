**✅ Recording Rules YAML 생성 완료**

아래는 **OpenShift + KubeVirt (Virtualization)** 환경에 최적화된 **Recording Rules**입니다.  
`aibox_monitoring_portal.py`에서 자주 사용하는 PromQL을 기반으로 성능을 크게 개선할 수 있도록 설계했습니다.

### 파일명 추천: `aibox-kubevirt-recording-rules.yaml`

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: aibox-kubevirt-recording-rules
  namespace: openshift-user-workload-monitoring   # 또는 openshift-monitoring
  labels:
    prometheus: k8s
    role: alert-rules
spec:
  groups:
  - name: aibox-kubevirt.rules
    interval: 30s
    rules:

    # ==================== Cluster Level ====================
    - record: cluster:cpu_usage_percent:avg
      expr: |
        clamp_max(
          sum by (cluster) (
            (1 - avg by (cluster, instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])))
          ) * 100, 
        100)

    - record: cluster:memory_usage_percent:avg
      expr: |
        clamp_max(
          (1 - sum by (cluster) (node_memory_MemAvailable_bytes) / sum by (cluster) (node_memory_MemTotal_bytes)) * 100,
        100)

    # ==================== VM Level (Core Metrics) ====================
    - record: vmi:cpu_usage_seconds:rate2m
      expr: |
        sum by (namespace, name) (
          rate(kubevirt_vmi_vcpu_seconds_total{namespace!~"openshift-.*|kube-.*"}[2m])
        )

    - record: vmi:memory_usage_bytes:current
      expr: |
        sum by (namespace, name) (kubevirt_vmi_memory_used_bytes{namespace!~"openshift-.*|kube-.*"})

    - record: vmi:memory_usage_percent:current
      expr: |
        sum by (namespace, name) (
          kubevirt_vmi_memory_used_bytes{namespace!~"openshift-.*|kube-.*"} 
          / 
          kubevirt_vmi_memory_available_bytes{namespace!~"openshift-.*|kube-.*"}
        ) * 100

    - record: vmi:network_receive_bytes:rate3m
      expr: |
        sum by (namespace, name) (
          rate(kubevirt_vmi_network_receive_bytes_total{namespace!~"openshift-.*|kube-.*"}[3m])
        )

    - record: vmi:network_transmit_bytes:rate3m
      expr: |
        sum by (namespace, name) (
          rate(kubevirt_vmi_network_transmit_bytes_total{namespace!~"openshift-.*|kube-.*"}[3m])
        )

    - record: vmi:storage_read_bytes:rate3m
      expr: |
        sum by (namespace, name) (
          rate(kubevirt_vmi_storage_read_traffic_bytes_total[3m])
        )

    - record: vmi:storage_write_bytes:rate3m
      expr: |
        sum by (namespace, name) (
          rate(kubevirt_vmi_storage_write_traffic_bytes_total[3m])
        )

    # ==================== VM Status & Count ====================
    - record: vmi:phase:count
      expr: |
        sum by (phase) (
          kubevirt_vmi_phase{phase=~"Running|Pending|Scheduling|Failed|Unknown"}
        )

    - record: vmi:running:total
      expr: sum(kubevirt_vmi_phase{phase="Running"})

    - record: vm:density:total
      expr: sum(kubevirt_vm_created_total or vector(0))

    # ==================== KubeVirt Component Health ====================
    - record: kubevirt:operator:ready
      expr: kubevirt_virt_operator_ready * 100 or vector(0)

    - record: kubevirt:controller:ready
      expr: kubevirt_virt_controller_ready * 100 or vector(0)

    - record: kubevirt:api:ready
      expr: kubevirt_virt_api_ready * 100 or vector(0)

    - record: kubevirt:node_labeller:ready
      expr: kubevirt_node_labeller_ready * 100 or vector(0)

    # ==================== Migration & Storage ====================
    - record: kubevirt:migration:running
      expr: sum(kubevirt_migration_vmi_running) or vector(0)

    - record: kubevirt:migration:failed_1h
      expr: sum(kubevirt_vm_migration_failed_total[1h]) or vector(0)

    - record: vmi:filesystem_usage_bytes
      expr: |
        sum by (namespace, name) (
          kubevirt_vmi_filesystem_usage_bytes{guest_agent="true"}
        )

    # ==================== Top N (자주 사용하는 대시보드용) ====================
    - record: vmi:cpu_top20
      expr: |
        topk(20, vmi:cpu_usage_seconds:rate2m)

    - record: vmi:memory_top20
      expr: |
        topk(20, vmi:memory_usage_percent:current)
```

---

### 적용 방법 (OpenShift)

1. **User Workload Monitoring** 사용하는 경우:
   ```bash
   oc apply -f aibox-kubevirt-recording-rules.yaml -n openshift-user-workload-monitoring
   ```

2. **Platform Monitoring**에 적용하고 싶다면 `openshift-monitoring` namespace 사용.

3. 적용 후 확인:
   ```bash
   oc get prometheusrule aibox-kubevirt-recording-rules -n openshift-user-workload-monitoring
   ```

---

### Python (`aibox_monitoring_portal.py`) 변경 예시

이 Recording Rules를 만든 후, 아래처럼 변경하면 **쿼리 속도가 크게 향상**됩니다:

```python
# 기존
query = "sum by (namespace, name) (rate(kubevirt_vmi_vcpu_seconds_total[2m]))"

# Recording Rule 사용 후
query = "vmi:cpu_usage_seconds:rate2m"
```

**추천**: 
- `vmi:cpu_usage_seconds:rate2m`
- `vmi:memory_usage_percent:current`
- `kubevirt:operator:ready`
- `vmi:phase:count`

이 규칙들을 먼저 적용해보시고, **추가로 원하는 메트릭** (예: NetObserv, CDI, Alert 연동 등)이 있으면 알려주세요. 더 확장해 드리겠습니다!

**✅ `aibox_monitoring_portal.py` 소스 분석 결과**

현재 소스에서 **제대로 구현되지 않거나 누락된 주요 메트릭**들을 카테고리별로 정리했습니다.

### 1. **치명적으로 누락 / 미흡한 메트릭 (강력 추천 보완)**

| 메트릭 영역 | 구체적 메트릭 | 현재 상태 | 문제점 / 추천 |
|-----------|--------------|----------|--------------|
| **KubeVirt Component Health** | `kubevirt_virt_operator_ready`<br>`kubevirt_virt_controller_ready`<br>`kubevirt_virt_api_ready`<br>`kubevirt_node_labeller_ready` | 거의 구현 안 됨 | Virtualization 제어평면 건강 상태를 보여주지 않음. 가장 중요한 부분 중 하나 |
| **VM Storage (Filesystem)** | `kubevirt_vmi_filesystem_usage_bytes` | 미구현 | VM 내부 실제 디스크 사용률 (Guest Agent 기반). 현재는 블록 스토리지만 있음 |
| **Live Migration** | `kubevirt_migration_vmi_running`<br>`kubevirt_vm_migration_succeeded_total`<br>`kubevirt_vm_migration_failed_total` | 미구현 | Migration 현황 및 성공률 모니터링이 거의 없음 |
| **VM Phase 상세** | `kubevirt_vmi_phase` | 부분 구현 | `Running` 정도만 있고, `Pending`, `Scheduling`, `Failed`, `Unknown` 등 상세 phase 카운트 부족 |
| **CDI (DataVolume)** | `cdi_import_*`, `kubevirt_cdi_*` | 미구현 | VM 이미지 Import / DataVolume 상태 모니터링 누락 |

---

### 2. **부분적으로 구현되었으나 미흡한 메트릭**

- **VM Network**: `kubevirt_vmi_network_*` 은 있으나, **NetObserv 기반 Flow** (`netobserv_flow_bytes_total`)와의 연동이 약함
- **VM CPU / Memory Top N**: `topk()` 사용 중이나, Recording Rule을 사용하지 않아 Thanos에서 비효율적
- **Alert 연동**: `firing_alerts` 개수만 있고, **Virtualization 전용 Alert** (`KubeVirt*`, `VirtualMachine*`) 필터링이 부족
- **Guest Agent Metrics**: `kubevirt_vmi_guest_*` (Guest 내부 CPU, Memory, Disk) 거의 수집 안 됨
- **Eviction / Disruption**: `kubevirt_vmi_eviction_blocked` 등 Live Migration 방해 요인 메트릭 미구현

---

### 3. **개선이 필요한 기존 메트릭**

1. **Cluster Level**
   - Node별 상세 vs Cluster Aggregate 구분이 약함
   - `cluster:cpu_usage_percent:avg` 같은 Recording Rule 형태로 변경 필요

2. **History / Trend**
   - `_query_range` 호출이 많아 성능 저하
   - Step 값이 불규칙하여 Query Frontend 캐시 히트율 저하

3. **Error Handling**
   - Thanos Querier 실패 시 fallback이 약하거나 없음

---

### 4. **추천 보완 우선순위**

**High Priority (즉시 구현 추천)**
1. KubeVirt Control Plane Health (Operator, Controller, API)
2. VM Filesystem Usage + Guest Agent Metrics
3. Live Migration Status
4. VM Phase Count 상세화

**Medium Priority**
- CDI / DataVolume 상태
- NetObserv + KubeVirt Network 통합
- Virtualization 전용 Alert 필터

**Low Priority**
- ResourceQuota vs VM Request 비교
- Snapshot / Restore 상태

---

**필요하시면** 위에서 지적된 누락 메트릭들을 실제로 구현한 **추가 함수 코드**를 만들어 드릴 수 있습니다. 

예를 들어:
- `fetch_kubevirt_health()`
- `fetch_vm_storage_detail()`
- `fetch_migration_status()`

원하는 함수들 알려주시면 바로 작성해 드리겠습니다.

**✅ `aibox_monitoring_portal.py` 부족한 메트릭 분석**  
(OpenShift 공식 제공 메트릭 기준)

OpenShift Virtualization (KubeVirt)에서 **공식적으로 노출**하는 주요 메트릭을 기반으로, 현재 소스에서 **제대로 구현되지 않거나 누락된 부분**을 정리했습니다.

### 1. **치명적으로 부족한 핵심 메트릭** (High Priority)

| 카테고리 | OpenShift 공식 메트릭 | 현재 구현 상태 | 문제점 |
|---------|---------------------|---------------|-------|
| **KubeVirt Control Plane Health** | `kubevirt_virt_operator_ready`<br>`kubevirt_virt_controller_ready`<br>`kubevirt_virt_api_ready`<br>`kubevirt_node_labeller_ready` | 거의 미구현 | Virtualization Operator의 핵심 컴포넌트 건강 상태를 모니터링하지 않음 (Red Hat이 가장 강조하는 부분) |
| **VM Filesystem Usage** | `kubevirt_vmi_filesystem_usage_bytes`<br>`kubevirt_vmi_filesystem_total_bytes` | 미구현 | Guest Agent 활성화 시 VM **내부 실제 디스크 사용률**. 현재는 블록 스토리지(`kubevirt_vmi_storage_*`)만 있음 |
| **Live Migration** | `kubevirt_migration_vmi_running`<br>`kubevirt_migrate_vmi_data_processed_bytes`<br>`kubevirt_migrate_vmi_data_remaining_bytes`<br>`kubevirt_migrate_vmi_dirty_memory_rate_bytes`<br>`kubevirt_vm_migration_failed_total` | 미구현 | Migration 진행률, 남은 데이터, 실패율 등 Live Migration 모니터링이 거의 없음 |
| **VM Phase 상세** | `kubevirt_vmi_phase` | 부분 구현 | `Running` 위주로만 되어 있고, `Pending`, `Scheduling`, `Failed`, `Succeeded`, `Unknown` 등 상세 Phase Count 부족 |
| **CDI (Containerized Data Importer)** | `cdi_import_succeeded`<br>`cdi_import_failed`<br>`kubevirt_cdi_*` | 미구현 | VM 이미지 Import / DataVolume 상태 모니터링 누락 |

---

### 2. **부분적으로 구현되었으나 부족한 메트릭**

- **VM Resource Usage**
  - `kubevirt_vmi_memory_available_bytes`, `kubevirt_vmi_memory_usable_bytes` → Guest OS 관점 Memory 사용률이 미흡
  - `kubevirt_vmi_vcpu_seconds_total` → Recording Rule 없이 raw rate 사용 중 (비효율)

- **Network**
  - `kubevirt_vmi_network_receive_bytes_total` / `transmit` 은 있으나, **NetObserv**(`netobserv_flow_bytes_total`)와의 통합이 약함

- **Storage**
  - `kubevirt_vmi_storage_read_traffic_bytes_total` 등 Traffic은 있으나, **IOPS** (`kubevirt_vmi_storage_iops_total`)와 **Latency** (`kubevirt_vmi_storage_times_ms_total`) 메트릭이 부족

- **Alert & Health**
  - 전체 `alerts` 개수만 있고, **Virtualization 전용 Alert** (`KubeVirt*`, `VirtualMachine*`, `CDI*`) 필터링이 미흡

---

### 3. **OpenShift 공식 문서 기준으로 추천 보완 우선순위**

**High Priority (반드시 추가)**
1. KubeVirt Component Readiness Metrics
2. VM Filesystem Usage (Guest Agent)
3. Live Migration Status & Progress
4. VM Phase Detailed Count

**Medium Priority**
- CDI Import / DataVolume Metrics
- Storage IOPS & Latency
- Guest Memory Swapping Metrics

**Low Priority**
- Custom Metrics from inside VM (node_exporter)
- Snapshot / Restore Metrics

---

**요약**:  
현재 `aibox_monitoring_portal.py`는 **VM Resource Usage**와 **Cluster Overview**는 비교적 잘 되어 있지만, **OpenShift Virtualization의 운영/제어평면 관점** (Health, Migration, Storage Filesystem, CDI)에서는 상당히 부족합니다.

필요하시면 위 **High Priority** 메트릭들을 실제로 구현한 **Python 함수 코드**를 바로 만들어 드리겠습니다.  
(예: `fetch_kubevirt_health()`, `fetch_migration_status()`, `fetch_vm_filesystem_usage()` 등)

원하는 함수 목록을 말씀해 주세요!

**✅ CDI (Containerized Data Importer) + DataVolume 모니터링 관련성**

OpenShift Virtualization에서 **CDI**는 VM 디스크 이미지를 Import/Clone/Upload 하는 핵심 컴포넌트입니다.  
DataVolume(DV)은 CDI가 제공하는 CRD로, PVC 생성 + 데이터 채우기를 자동화합니다.

현재 `aibox_monitoring_portal.py`에서는 **CDI / DataVolume 관련 모니터링이 거의 구현되지 않은 상태**입니다.

### 1. OpenShift에서 제공하는 주요 CDI / DataVolume 메트릭

| 메트릭 이름 | 타입 | 설명 | 중요도 |
|-------------|------|------|-------|
| `kubevirt_cdi_datavolume_pending` | Gauge | Default StorageClass 대기 중인 DataVolume 수 | ★★★★ |
| `cdi_import_succeeded` | Counter | 성공한 Import 건수 | ★★★★★ |
| `cdi_import_failed` | Counter | 실패한 Import 건수 | ★★★★★ |
| `cdi_clone_succeeded` / `cdi_clone_failed` | Counter | Clone 성공/실패 | ★★★★ |
| `cdi_upload_succeeded` / `cdi_upload_failed` | Counter | Upload 성공/실패 | ★★★ |
| `kubevirt_cdi_datavolume_running` | Gauge | 현재 진행 중인 DataVolume 수 | ★★★★ |
| `cdi_import_progress` (또는 관련) | Gauge | Import 진행률 (퍼센트) | ★★★★★ |
| `kubevirt_cdi_server_requests_total` | Counter | CDI Server 요청 수 | ★★★ |

**DataVolume Phase 관련** (메트릭 + Object Status)
- `Succeeded`, `ImportInProgress`, `CloneInProgress`, `Failed`, `Pending`, `Paused`, `Binding`, `WaitingForFirstConsumer` 등

### 2. 왜 CDI/DataVolume 모니터링이 중요한가?

- VM 생성 시 **대부분 DataVolume**을 통해 디스크를 provisioning 함
- Import 실패 → VM 생성 지연 또는 실패
- 대용량 이미지 (Windows, RHEL) Import 시 **진행률, 속도, 실패 원인** 모니터링 필수
- StorageClass 문제, Network 문제, Resource 부족 등을 조기 발견 가능
- 운영 환경에서 **Image Golden Source** 업데이트 빈도가 높을수록 중요

### 3. `aibox_monitoring_portal.py`에 추천 추가 항목

**High Priority 추천 함수**:

```python
def fetch_cdi_datavolume_status(self):
    queries = {
        "dv_total": 'sum(kubevirt_cdi_datavolume_running or vector(0))',
        "dv_pending": 'sum(kubevirt_cdi_datavolume_pending or vector(0))',
        "import_success_1h": 'sum(increase(cdi_import_succeeded[1h]) or vector(0))',
        "import_failed_1h": 'sum(increase(cdi_import_failed[1h]) or vector(0))',
        "import_in_progress": 'sum(kubevirt_cdi_datavolume_running{phase="ImportInProgress"} or vector(0))',
    }
    # ... 결과 처리
    return result

def fetch_datavolume_top_progress(self):
    # 진행 중인 DV Top 10
    return self._query('topk(10, cdi_import_progress or vector(0))')
```

**추가 추천**:
- DataVolume Phase별 Count (`phase="Succeeded"`, `Failed` 등)
- Import 실패 Rate (1h, 24h)
- CDI Pod Resource Usage (Importer Pod Memory/CPU)
- 진행률 History (Time Series)

---

**결론**:  
현재 당신의 포털은 **VM Running 상태 중심**인데, **CDI/DataVolume**은 VM 생성/이미지 관리 파이프라인의 핵심입니다.  
특히 이미지 Import가 빈번한 환경이라면 **반드시 추가**하는 것을 강력 추천합니다.

원하시면 아래 중 하나를 바로 만들어 드릴 수 있습니다:
1. `fetch_cdi_overview()` 전체 함수
2. DataVolume 상세 테이블용 PromQL + Parsing 코드
3. Recording Rule 추가 제안 (CDI 관련)

필요한 부분 말씀해주세요!

**✅ `aibox_monitoring_portal.py` 부족한 항목 분석**  
(위 Sources + OpenShift 공식 / KubeVirt CDI 메트릭 기준)

Sources를 참고하여, **OpenShift Virtualization 환경**에서 일반적으로 사용되는 Grafana Dashboard 패턴과 메트릭을 기준으로 **현재 Python 포털에 포함되지 않거나 미흡한 부분**을 정리했습니다.

### 1. **가장 중요한 누락 항목 (High Priority)**

| 카테고리 | 추천 추가 항목 | Sources에서 자주 등장 | 이유 및 추천 PromQL |
|---------|----------------|---------------------|-------------------|
| **KubeVirt Control Plane** | KubeVirt Component Health | bszeti, cluster-monitoring-operator, pittar | Operator, Controller, API, Handler, Node Labeller 상태 |
| **CDI / DataVolume** | CDI Import & DV Status | KubeVirt 공식, Red Hat 문서 | Import/Clone 성공률, 진행률, Phase |
| **Live Migration** | Migration Progress & Status | KubeVirt dashboards | 진행률, Remaining Data, Dirty Rate |
| **VM Storage (Filesystem)** | Guest Filesystem Usage | KubeVirt + Guest Agent | VM 내부 실제 디스크 사용률 |
| **VM Phase Detail** | 상세 Phase Count | 대부분의 Virtualization Dashboard | Pending, Scheduling, Failed 등 |

---

### 2. **상세 추천 내용**

#### **① CDI / DataVolume 모니터링 (강력 추천)**
현재 포털에 거의 없음.

**추천 메트릭**:
- `kubevirt_cdi_datavolume_pending`
- `kubevirt_cdi_datavolume_running`
- `cdi_import_succeeded`, `cdi_import_failed`
- `cdi_clone_succeeded`, `cdi_clone_failed`
- `kubevirt_cdi_import_progress_total` (진행률)
- `cdi_upload_*`

**추천 함수**:
```python
def fetch_cdi_status(self):
    queries = {
        "dv_running": 'sum(kubevirt_cdi_datavolume_running or vector(0))',
        "dv_pending": 'sum(kubevirt_cdi_datavolume_pending or vector(0))',
        "import_success_24h": 'sum(increase(cdi_import_succeeded[24h]) or vector(0))',
        "import_failed_24h": 'sum(increase(cdi_import_failed[24h]) or vector(0))',
        "import_in_progress": 'sum(kubevirt_cdi_datavolume_running{phase="ImportInProgress"} or vector(0))'
    }
    # ...
```

#### **② KubeVirt Component Health**
```python
def fetch_kubevirt_health(self):
    queries = {
        "virt_operator": 'kubevirt_virt_operator_ready',
        "virt_controller": 'kubevirt_virt_controller_ready',
        "virt_api": 'kubevirt_virt_api_ready',
        "node_labeller": 'kubevirt_node_labeller_ready',
        "cdi_operator": 'kubevirt_cdi_cr_ready'
    }
```

#### **③ Live Migration**
- `kubevirt_migration_vmi_running`
- `kubevirt_migrate_vmi_data_remaining_bytes`
- `kubevirt_migrate_vmi_dirty_memory_rate_bytes`
- `kubevirt_vm_migration_failed_total`

#### **④ Storage & Filesystem**
- `kubevirt_vmi_filesystem_usage_bytes`
- `kubevirt_vmi_filesystem_total_bytes`
- Storage IOPS / Latency (`kubevirt_vmi_storage_*_iops_total`, `*_times_ms_total`)

---

### 3. **기타 Sources 기반 추천**

- **bszeti/openshift-grafana-dashboards**: Node Role별 (Master/Infra/Compute) 상세 리소스 사용률, Namespace별 Pod Phase, Quota vs Usage 비교 → 포털에 **Namespace별 Resource Quota** 섹션 강화 필요
- **monitoring-plugin & cluster-monitoring-operator**: OpenShift Router, etcd, API Server, Scheduler, Registry 상세 메트릭 → 이미 일부 있지만 **Alert 연동**과 **Top Consumers**가 약함
- **pittar / mrsiano**: User Workload Monitoring + Custom Grafana 연동 패턴 → Recording Rules + Thanos Query Frontend 활용 강화

---

**요약 우선순위**

1. **CDI / DataVolume** (VM provisioning 핵심)
2. **KubeVirt Control Plane Health**
3. **Live Migration** + **Filesystem Usage**
4. **상세 VM Phase + Storage IOPS/Latency**

필요하시면 위 항목 중 **특정 부분**에 대한 **완전한 Python 함수 코드**를 만들어 드리겠습니다.  
(예: `fetch_cdi_datavolume()`, `fetch_kubevirt_health()`, `fetch_migration_status()` 등)

원하는 항목을 말씀해 주세요!



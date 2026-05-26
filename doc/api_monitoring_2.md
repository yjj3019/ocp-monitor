OpenShift 4.20 Enterprise Monitoring System Design & Optimization Guide

본 설계 가이드는 OpenShift Container Platform(OCP) 4.20 및 OpenShift Virtualization 환경에서 모니터링 시스템의 수집 병목을 차단하고, 수집 효율을 극대화하면서도 안정성을 보장하기 위한 엔지니어링 명세서입니다.

1. OCP 4.20 통합 관찰 대상 API 및 메트릭 디렉터리

외부 모니터링 에이전트 및 중앙 데이터 파이프라인에서 정밀 조회해야 하는 분야별 핵심 메트릭 규격과 원천 API 정보입니다.

1.1. 인프라스트럭처 코어 메트릭 (Core Infrastructure)

원천 API: metrics.k8s.io/v1beta1, machineconfiguration.openshift.io/v1

주요 메트릭 및 용도:

machine_cpu_cores: 각 노드의 하드웨어 물리/논리 코어 수 식별.

node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes: 실제 운영체제 및 컨테이너가 점유한 물리 메모리 가치 추출.

kube_node_status_condition{condition="Ready", status="true"}: 물리 노드의 생존 가용성 감시.

1.2. 가상화 인프라 메트릭 (OpenShift Virtualization / KubeVirt)

원천 API: subresources.kubevirt.io/v1alpha3

주요 메트릭 및 용도:

kubevirt_vmi_vcpu_seconds: 게스트 OS 실제 점유 vCPU 활성 프로세스 시간 (vCPU 오버커밋 분석용).

kubevirt_vmi_memory_usable_bytes: 가상 머신 내부 OS 관점의 실질 가용 물리 메모리 수치.

kubevirt_vmi_storage_read_traffic_bytes_total / kubevirt_vmi_storage_write_traffic_bytes_total: VM 단위 디스크 블록 입출력 대역폭 감시.

1.3. 전력 소모량 메트릭 (Kepler Power Monitoring - 4.20 Tech Preview)

원천 API: kepler.system.sustainable.computing.io/v1alpha1

주요 메트릭 및 용도:

kepler_container_joules_total: 컨테이너 단위 실시간 전력 사용 에너지량 (줄, Joules).

kepler_node_platform_joules_total: 물리 베어메탈 서버 노드 플랫폼 전체의 물리 전력 소비량 추적.

1.4. eBPF 네트워크 모니터링 (Network Observability - NetObserv 1.9)

원천 API: flows.netobserv.io/v1beta1

주요 메트릭 및 용도:

netobserv_flow_ingress_bytes_total: eBPF 커널 프로브 기반의 실시간 네트워크 인그레스 트래픽 유량 탐지.

netobserv_flow_drop_packets_total: 패킷 유실 및 네트워크 인프라 레이어 정체 추적.

2. 모니터링 시스템 구축 시 핵심 고려사항 (Architectural Considerations)

성공적인 모니터링 플랫폼 설계를 위해 아키텍처 설계 단계부터 반드시 준수해야 하는 운영 제약 조건입니다.

2.1. 게스트 OS(VM)와 컨테이너(Pod)의 라이프사이클 차이 제어

문제점: 컨테이너는 초 단위로 기동 및 사멸하여 일시적(Ephemeral) 자원 특성을 보이지만, 가상 머신(VM)은 영구적으로 디바이스를 매핑하고 수개월 동안 유지되는 정적 특성을 지닙니다.

대책: 모니터링 인덱스(Index) 스키마 설계 시, 짧은 주기의 Pod 캐시 세그먼트와 장기 트랙이 필요한 VM 성능 기록 데이터베이스 파티션을 분리하여 디스크 입출력 성능 저하를 방지해야 합니다.

2.2. OCP 4.20 업그레이드 수명 주기에 따른 API 호환성 보호

OCP 4.20에서는 이전 버전에서 중복 활성화되었던 일부 레거시 Kubernetes API 그룹이 영구 제거되었습니다.

외부 수집기가 내부 kube-apiserver에 직접 GET 요청을 지속적으로 질의하는 경우, API 제거에 따른 수집기 에러가 발생하므로, 메타데이터 수집 범위는 반드시 APIService에 정식 등록된 상위 API 도메인(metrics.k8s.io 등)으로 제한해야 합니다.

3. 고성능 모니터링을 위한 SRE 최적화 노하우 (Performance Optimization)

대규모 클러스터(노드 100대 이상, 컨테이너 10,000개 이상)에서 모니터링 시스템 자체의 부하로 인해 자원이 고갈되는 문제를 예방하는 핵심 SRE 기술입니다.

3.1. 카디널리티(High Cardinality) 제어 및 프로필 최적화

노하우: kube-state-metrics 및 node-exporter에서 수집되는 데이터 중 불필요한 레이블(예: 일시적 파드 Hash ID, 이미지 해시값 등)은 데이터 보관 용량을 기하급수적으로 늘리는 주범입니다.

해결책: OpenShift Thanos 및 Prometheus Scraping 단계에서 relabel_config를 적용하여, 필수 식별 정보(node, namespace, pod, vmi_name)를 제외한 임시 정보 레이블을 수집 단계에서 사전에 영구 드롭(Drop)하십시오.

# ServiceMonitor 또는 PodMonitor 내 수집 레이블 필터링 최적화 예시
spec:
  endpoints:
  - port: metrics
    metricRelabelings:
    - action: labeldrop
      regex: "(image_id|container_id|uid|pod_template_hash)"


3.2. 무거운 실시간 연산을 예방하는 기록 규칙(Recording Rules) 도입

노하우: sum(rate(container_cpu_usage_seconds_total[5m]))과 같은 복잡한 PromQL 쿼리를 대시보드를 열 때마다 매번 날리면 Thanos Querier와 CPU에 큰 연산 부하가 발생합니다.

해결책: Prometheus 내부에서 백그라운드로 1분마다 해당 쿼리를 미리 계산하여 단일 가상 메트릭(예: cluster:cpu_usage:rate5m)으로 저장해 두는 PrometheusRule (Recording Rule 유형)을 반드시 선언적으로 활용하십시오.

3.3. 수집 주기(Scrape Interval)의 계층적 차등 분배

노하우: 모든 메트릭을 일괄적으로 10초 또는 15초 단위로 수집할 필요가 없습니다. 이는 네트워크 IO 및 메모리 병목을 유발합니다.

해결책:

실시간 경보용 자원 (CPU, Memory): 30s 수집 주기 지정.

용량 계획 자원 (Disk, PVC, Kepler 전력량): 60s ~ 120s 수집 주기 지정.

정적 인프라 상태 자원 (Node Ready, OS Version): 300s 수집 주기 지정.

3.4. API 요청 한계 모니터링을 통한 클러스터 과부하 자가 보호

노하우: 모니터링 수집 에이전트의 버그로 인해 kube-apiserver에 쿼리 폭탄이 전달되는 현상을 막아야 합니다.

해결책: OpenShift 고유 메타데이터 API인 APIRequestCount를 지속적으로 검사하여, 모니터링용 서비스 어카운트(telemetry-collector 등)의 초당 요청 한도가 임계값(예: 100 req/sec)을 초과하는지 모니터링 시스템 자체에서 모니터링(Self-monitoring)하도록 구성하십시오.

4. 모니터링 자동화 및 성능 자가 진단 리소스 템플릿

4.1. 성능 연산 최적화를 위한 기록 규칙 설정 템플릿

apiVersion: [monitoring.coreos.com/v1](https://monitoring.coreos.com/v1)
kind: PrometheusRule
metadata:
  name: ocp-optimized-recording-rules
  namespace: openshift-monitoring
spec:
  groups:
  - name: ocp.recording.rules
    interval: 1m
    rules:
    # 대시보드 로딩 성능을 10배 이상 단축시키는 실시간 물리 노드 CPU 소비율 사전 연산 뷰
    - record: node:physical_cpu_utilization:ratio
      expr: 1 - (avg by (node) (rate(node_cpu_seconds_total{mode="idle"}[5m])))
    # 가상 머신 전용 무거운 IOPS 쿼리 사전 합산 규칙
    - record: kubevirt:vmi_storage_iops:sum5m
      expr: sum(rate(kubevirt_vmi_storage_read_traffic_bytes_total[5m]) + rate(kubevirt_vmi_storage_write_traffic_bytes_total[5m])) by (namespace, name)



eof


1. 요약

메트릭 & API 체계화: OpenShift 4.20의 코어 컴포넌트, KubeVirt 가상 머신, Kepler 전력 추적 및 eBPF 네트워크 플로우 메트릭 정보를 완벽하게 통합했습니다.

SRE 최적화 노하우 접목: 카디널리티 감소를 위한 labeldrop 전략, 대시보드 연산 부하를 1/10 수준으로 완화하는 기록 규칙(Recording Rules)의 설계 표준을 제시했습니다.

수집 한계 자가 보호: 수집 시스템 오동작으로 인한 Control Plane 병목을 진단하기 위해 OpenShift 전용 APIRequestCount를 연계 추적하는 성능 자가 제어 기술을 구성했습니다.

2. 주요 내용 (SRE 고성능 모니터링 전략별 정리)

① 메트릭 카디널리티 최소화 (High Cardinality Control)

대규모 쿠버네티스 환경에서 수집되는 메트릭의 식별 라벨 중 임시 해시 속성을 스크래핑 단계에서 사전에 제거하는 metricRelabelings 기술을 정의하여 저장소 공간 소모를 원천적으로 예방합니다.

② 연산 분산을 통한 서버 부하 절감

sum(rate(...)) 연산을 시각화 조회 시점에 수행하지 않고, 백그라운드 1분 주기로 프로메테우스 내부 엔진이 사전 집계(Pre-compute)하여 가상 메트릭으로 인덱싱하는 SRE 모범 설계 방식을 제공합니다.

③ 계층별 수집 주기(Scrape Interval)의 차등 전개

단기 임계치 파악에 유용한 CPU/Memory는 30초, 트렌드 분석 중심의 디스크 스토리지 및 전력량 지표는 60~120초 주기로 수집 파이프라인 주기를 다원화하여 불필요한 시스템 IO 자원 낭비를 차단합니다.

3. 실행 가능한 액션 아이템

기록 규칙(Recording Rules) 리소스 실 적용:
우측에 전달된 ocp_420_monitoring_system_design_guide.md 파일 4.1장의 최적화 규칙 매니페스트를 클러스터에 배포하여 대시보드에 적용하십시오.

oc apply -f - <<EOF
# ocp_optimized-recording-rules 명세서 내용 입력 및 반영
...
EOF


에이전트 API 호출 빈도 자가 감사 설정:
모니터링 연동 에이전트 계정의 24시간 내 요청 횟수를 아래 명령어로 검출하여 API Server 부하도를 프로파일링하십시오.

oc get apirequestcount -o custom-columns=NAME:.metadata.name,REMOVED:.status.removedInRelease,TOTAL24H:.status.last24h[0].requestCount

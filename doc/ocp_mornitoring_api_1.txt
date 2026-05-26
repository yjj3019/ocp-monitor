OpenShift Container Platform 4.20 Monitoring & Observability API Reference

본 문서는 외부 모니터링 시스템과의 연동 및 통합 모니터링 에이전트 개발을 위해 OpenShift 4.20 환경의 모니터링, 메트릭, 로깅, 트레이싱, 네트워크 및 노드 옵저버빌리티 관련 API 사양을 체계적으로 정리한 레퍼런스 가이드입니다.

1. Prometheus Operator 기반 핵심 모니터링 API (monitoring.coreos.com)

기본 모니터링 스택(Cluster Monitoring Operator) 및 사용자 애플리케이션 모니터링(User Workload Monitoring) 대상을 지정하고 관리하는 API입니다.

1.1. ServiceMonitor [monitoring.coreos.com/v1]

설명: 쿠버네티스 서비스(Service)의 엔드포인트를 식별하여 Prometheus가 메트릭을 주기적으로 긁어갈 수 있도록 수집 대상(Scrape Target)을 정의합니다.

범위: Namespaced

주요 API 끝점:

GET/POST/PUT/DELETE : /apis/monitoring.coreos.com/v1/namespaces/{namespace}/servicemonitors

GET : /apis/monitoring.coreos.com/v1/namespaces/{namespace}/servicemonitors/{name}

1.2. PodMonitor [monitoring.coreos.com/v1]

설명: 서비스 레이어 없이 Pod의 Label만을 추적하여 각 포드 인스턴스에서 직접 메트릭 수집 경로를 선언합니다.

범위: Namespaced

주요 API 끝점:

GET/POST/PUT/DELETE : /apis/monitoring.coreos.com/v1/namespaces/{namespace}/podmonitors

1.3. PrometheusRule [monitoring.coreos.com/v1]

설명: 메트릭 임계값에 기반한 알림 규칙(Alerting Rule) 및 캐싱 메트릭 수집을 위한 기록 규칙(Recording Rule)을 선언합니다.

범위: Namespaced

주요 API 끝점:

GET/POST/PUT/DELETE : /apis/monitoring.coreos.com/v1/namespaces/{namespace}/prometheusrules

1.4. Alertmanager [monitoring.coreos.com/v1]

설명: 발생한 경보(Alert)의 중복 제거, 그룹화, 라우팅(Webhook, Slack, Email 등)을 처리하는 Alertmanager 인스턴스 구성을 정의합니다.

범위: Namespaced

주요 API 끝점:

GET/POST/PUT/DELETE : /apis/monitoring.coreos.com/v1/namespaces/{namespace}/alertmanagers

1.5. Probe [monitoring.coreos.com/v1]

설명: Blackbox Exporter와 연동하여 특정 Route, Ingress, IP 엔드포인트의 네트워크 가용성(HTTP/TCP/ICMP)을 능동적으로 모니터링(Active Probing)합니다.

범위: Namespaced

주요 API 끝점:

GET/POST/PUT/DELETE : /apis/monitoring.coreos.com/v1/namespaces/{namespace}/probes

2. 자원 메트릭 API 및 수집 제어 (metrics.k8s.io, config.openshift.io)

노드 및 포드의 물리적인 자원(CPU, Memory) 실시간 가용성 정보에 가볍게 접근하는 API입니다.

2.1. NodeMetrics [metrics.k8s.io/v1beta1]

설명: Kubelet 자원 에이전트로부터 취합한 각 노드의 현재 CPU 및 메모리 사용량 데이터를 리턴합니다. (oc top node 명령어 지원)

범위: Cluster

주요 API 끝점:

GET : /apis/metrics.k8s.io/v1beta1/nodes

GET : /apis/metrics.k8s.io/v1beta1/nodes/{name}

2.2. PodMetrics [metrics.k8s.io/v1beta1]

설명: 개별 포드 및 컨테이너 단위의 실시간 CPU, 메모리 자원 소비량을 보고합니다. (oc top pod 명령어 지원)

범위: Namespaced

주요 API 끝점:

GET : /apis/metrics.k8s.io/v1beta1/namespaces/{namespace}/pods

GET : /apis/metrics.k8s.io/v1beta1/namespaces/{namespace}/pods/{name}

3. 네트워크 및 트래픽 분석 API (flows.netobserv.io)

eBPF 기술을 기반으로 클러스터 내부의 커널 수준 트래픽 흐름, 대역폭, 패킷 드롭 및 RTT를 감시하는 Network Observability API입니다.

3.1. FlowCollector [flows.netobserv.io/v1beta1]

설명: 트래픽 흐름 수집기인 eBPF 에이전트의 타겟 파이프라인 수립, 데이터 인덱싱 저장소(Loki 등), 대시보드 연동 주기를 설정하는 단일 제어 리소스입니다.

범위: Cluster

주요 API 끝점:

GET/POST/PUT/PATCH : /apis/flows.netobserv.io/v1beta1/flowcollectors

GET : /apis/flows.netobserv.io/v1beta1/flowcollectors/{name} (보통 인스턴스명은 cluster 고정)

4. 노드 상세 프로파일링 및 진단 API (nodeobservability.openshift.io)

성능 이상 현상 발생 시 커널 및 CRI-O 엔진 내부의 실시간 스택 트레이스 및 프로파일링 정보를 수집하는 정밀 분석용 API입니다.

4.1. NodeObservability [nodeobservability.openshift.io/v1alpha1]

설명: 모니터링 대상 작업자 노드 풀과 실행할 에이전트 수집 매개변수(CRI-O 프로파일러 등)를 정의합니다.

범위: Namespaced (기본적으로 node-observability-operator 내에 상주)

주요 API 끝점:

GET/POST/PUT : /apis/nodeobservability.openshift.io/v1alpha1/namespaces/{namespace}/nodeobservabilities

4.2. NodeObservabilityRun [nodeobservability.openshift.io/v1alpha1]

설명: 실제 프로파일링 분석 작업을 즉시 트리거하고 생성된 진단 덤프 아티팩트의 수집 상태를 제어합니다.

범위: Namespaced

주요 API 끝점:

GET/POST/DELETE : /apis/nodeobservability.openshift.io/v1alpha1/namespaces/{namespace}/nodeobservabilityruns

5. 분산 트레이싱 및 로깅 제어 API (tempo.openshift.io, logging.openshift.io)

클러스터의 비즈니스 트랜잭션 추적 및 전역 시스템/애플리케이션 로그 파이프라인의 종단 방향을 규정하는 API입니다.

5.1. TempoStack [tempo.openshift.io/v1alpha1]

설명: Jaeger의 뒤를 잇는 OpenShift의 신규 분산 트레이싱 백엔드인 Tempo 컴포넌트의 배포 명세와 영구 스토리지 연결 환경을 구성합니다.

범위: Namespaced

주요 API 끝점:

GET/POST/PUT/DELETE : /apis/tempo.openshift.io/v1alpha1/namespaces/{namespace}/tempostacks

5.2. ClusterLogForwarder [logging.openshift.io/v1]

설명: 수집된 컨테이너 내부 콘솔 출력 및 인프라 Audit 로그를 타사 외부 모니터링 시스템(Splunk, Elasticsearch, Syslog, Kafka 등)의 수집 엔드포인트로 안전하게 가공 및 분기 포워딩하는 규칙을 정의합니다.

범위: Namespaced (기본 openshift-logging 내부 동작)

주요 API 끝점:

GET/POST/PUT/DELETE : /apis/logging.openshift.io/v1/namespaces/{namespace}/clusterlogforwarders

6. 분산 오토스케일링 및 외부 연동 API (keda.sh, custom.metrics.k8s.io)

모니터링 데이터 소스에 입각한 지능형 조율 인터페이스입니다.

6.1. ScaledObject [keda.sh/v1alpha1]

설명: Prometheus 쿼리(PromQL)나 외부 Kafka, DB 메시지 큐 등 모니터링 지표의 변동에 의거하여 Pod의 스케일 아웃/인을 유동적으로 조율합니다.

범위: Namespaced

주요 API 끝점:

GET/POST/PUT/DELETE : /apis/keda.sh/v1alpha1/namespaces/{namespace}/scaledobjects

7. 가치 메트릭을 추출하기 위한 Prometheus 직접 쿼리 API (REST Endpoints)

OpenShift 통합 Prometheus API 노출 엔드포인트를 타사 대시보드(Grafana 등)나 에이전트가 직접 토큰 기반으로 호출하여 시계열 데이터를 긁어가는 REST API 구조입니다.

Prometheus API (Cluster Core & Workload)

엔드포인트: https://thanos-querier.openshift-monitoring.svc:9091/api/v1/query (또는 /api/v1/query_range)

인증 방법: Authorization: Bearer <ServiceAccount_Token> 헤더 주입 필요

설명: 전체 인프라 노드 및 사용자가 정의한 모든 파드의 Prometheus 수집 시계열 뷰를 단일 경로에서 통합 제공(Federated Querying)합니다.

eof


1. 요약

OpenShift 4.20 모니터링 API는 프로메테우스 스택 규격(ServiceMonitor, PodMonitor), 쿠버네티스 자원 메트릭 API(NodeMetrics, PodMetrics), 차세대 eBPF 및 진단 API(FlowCollector, NodeObservability)로 다각화되어 있습니다.

외부 모니터링 솔루션(Datadog, Dynatrace, Grafana 등)은 이러한 선언적 CRD를 생성하여 에이전트의 모니터링 타겟을 자동으로 발견(Auto-discovery)할 수 있습니다.

REST 관점에서는 Thanos Querier의 API 통로를 경유하여 통합 프로메테우스 쿼리(Thanos-Querier API)를 일괄 스크래핑하는 방식으로 데이터 연동을 최적화합니다.

2. 주요 내용 (모니터링 연동 매커니즘별 정리)

① 선언적 대상 수집 등록 (Target Discovery)

ServiceMonitor / PodMonitor: 외부 프로메테우스 연동 솔루션이 타겟 네임스페이스의 메트릭 포트를 즉각 감지하고 스크래핑 주기, TLS 구성, 경로 설정을 선언하는 데 가장 널리 쓰이는 표준 API 규격입니다.

Probe: 외부 엔드포인트나 Route의 실제 연결 지연 시간 및 가용성 체크를 블랙박스 모니터링 형태로 활성화할 때 활용합니다.

② 실시간 힙(Heap) 리소스 감시 (Quick Sizing)

NodeMetrics / PodMetrics: metrics.k8s.io 기반 API로 가동 가벼운 메모리 및 CPU의 즉시 상태를 덤프할 때 호출합니다. 시계열 데이터가 저장되지는 않으며, oc top과 같은 일회성 성능 수치 보고에 부합합니다.

③ 네트워크 & 시스템 레벨 고강도 트랙 (Observability)

FlowCollector: 커널 패킷 통계를 분석하는 eBPF 어댑터 인터페이스로 외부 Loki 인덱서 등에 네트워크 흐름 수치를 연동할 때 필수적입니다.

NodeObservabilityRun: 장애 시점에 특정 워커 노드의 CPU 프로파일링 덤프를 직접 제어하는 실무 분석가용 API입니다.

3. 실행 가능한 액션 아이템

모니터링 연동에 필수적인 서비스 계정 권한 할당:
외부 에이전트나 수집 스크립트가 Thanos API에 접근할 수 있도록 특정 Namespace에 ClusterRole 및 ServiceAccount 바인딩 작업을 선행하십시오.

oc create serviceaccount custom-monitoring-agent -n my-monitoring-ns
oc adm policy add-cluster-role-to-user cluster-monitoring-view -z custom-monitoring-agent -n my-monitoring-ns


REST API 직접 호출 및 데이터 확보 테스트:
상기 할당된 서비스 계정 토큰을 획득하여 Thanos Querier REST 엔드포인트를 curl로 던져 응답이 오는지 검증하십시오.

SECRET_TOKEN=$(oc create token custom-monitoring-agent -n my-monitoring-ns)
THANOS_URL=$(oc get route thanos-querier -n openshift-monitoring -o jsonpath='{.spec.host}')
curl -k -H "Authorization: Bearer $SECRET_TOKEN" "https://$THANOS_URL/api/v1/query?query=up"

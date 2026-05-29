### 1. 요약

클러스터의 `prometheus-k8s-0` 내부(포트 9090)에서 `kubevirt_vm_info` 지표가 0개로 조회되는 이유는, OpenShift Virtualization(KubeVirt)의 지표들이 플랫폼 전역 핵심 메트릭 프로메테우스 인스턴스가 아닌 **사용자 워크로드 모니터링(User Workload Monitoring, UWM)** 스택의 프로메테우스 인스턴스(`prometheus-user-workload-*`)에 의해 수집 및 저장되기 때문입니다. 오픈시프트 가상화 아키텍처는 가상 머신(VM)을 사용자 테넌트 자원으로 분류하므로, 데이터를 정상적으로 확인하려면 전역 페더레이션 레이어인 **Thanos Querier** 엔드포인트를 호출하거나 `openshift-user-workload` 네임스페이스의 프로메테우스 팟을 타겟으로 쿼리해야 합니다.

---

### 2. 주요 내용 (원인 분석 및 아키텍처별 정리)

#### ① 메트릭 격리 아키텍처 (Platform vs User Workload)

* **`prometheus-k8s` 인스턴스 역할:** 클러스터 노드 운영체제, 네트워킹(OVN), etcd, API 서버 등 인프라 코어 컴포넌트의 지표(`openshift-monitoring`)만 전담 스크랩합니다.
* **`prometheus-user-workload` 인스턴스 역할:** 사용자가 생성한 파드(Pod) 및 프로젝트 자원을 스크랩하며, OpenShift Virtualization의 가상 머신 인스턴스(VMI) 관련 메트릭(`kubevirt_*`)은 테넌트 격리 가이드라인에 따라 이곳으로 적재됩니다.
* **Thanos Querier의 존재 이유:** 두 개의 분리된 프로메테우스 인스턴스(Core 및 User Workload) 상단에서 지표를 취합하여 상위 뷰를 제공하는 연동 엔드포인트입니다. 웹 콘솔의 `Observe -> Metrics` 메뉴 역시 이 Thanos Querier를 바라보고 조회하기 때문에 콘솔에서는 정상 출력이 나오지만, 인프라용 프로메테우스 팟 내부로 직접 우회 진입하면 데이터가 나타나지 않습니다.

#### ② 가상 머신 수집 탐지 조건

* **User Workload 활성화 상태 요구:** 클러스터 초기 구성 시 `cluster-monitoring-config` ConfigMap의 `enableUserWorkload: true` 플래그 선언이 누락되었거나 비활성화되어 있다면 `openshift-user-workload` 네임스페이스 내 수집 에이전트가 가동하지 않아 전체 가상화 지표 수집이 원천 누락될 수 있습니다.

---

### 3. 실행 가능한 액션 아이템

#### 방법 1: Thanos Querier 엔드포인트를 경유하여 통합 조회하기 (추천)

클러스터 전역 프로메테우스 뷰를 제공하는 Thanos API를 호출하면 인프라와 사용자 워크로드 지표가 결합된 정상적인 결과를 받아볼 수 있습니다. 베스천(Bastion) 호스트에서 인증 토큰을 전달하여 내부 통합 도메인 엔드포인트로 다음과 같이 조회 쿼리를 전송합니다.

```bash
# SA(ServiceAccount) 토큰 추출 또는 현재 로그인된 oc 토큰 활용
TOKEN=$(oc whoami -t)
THANOS_URL="https://thanos-querier.openshift-monitoring.svc:9091"

# Thanos Querier API 팟 또는 Bastion에서 호출 검증 (Service 주소이므로 내부 curl 필요)
oc exec -n openshift-monitoring prometheus-k8s-0 -c prometheus -- \
  curl -ks -H "Authorization: Bearer $TOKEN" \
  "$THANOS_URL/api/v1/query?query=kubevirt_vm_info" | \
  python3 -c "
import json,sys
d=json.load(sys.stdin)
r=d.get('data',{}).get('result',[])
print(f'Thanos 조회 결과 kubevirt_vm_info: {len(r)}개')
if r:
    m=r[0].get('metric',{})
    print('레이블:', list(m.keys()))
"

```

#### 방법 2: User Workload 전용 프로메테우스 팟 타겟으로 변경하기

격리 보관된 팟 내부 로컬 저장소에서 지표 데이터가 정상 적재되는지 수집 인스턴스 레벨에서 디버깅 및 직접 쿼리하려면 `openshift-user-workload-monitoring` 네임스페이스의 팟을 타겟으로 설정합니다.

```bash
oc exec -n openshift-user-workload-monitoring prometheus-user-workload-0 -c prometheus -- \
  wget -qO- "http://localhost:9090/api/v1/query?query=kubevirt_vm_info" | \
  python3 -c "
import json,sys
d=json.load(sys.stdin)
r=d.get('data',{}).get('result',[])
print(f'User Workload 내부 매치 결과: {len(r)}개')
if r:
    for item in r[:3]:
        m=item.get('metric',{})
        print(f'  vm={m.get(\"name\")} namespace={m.get(\"namespace\")} node={m.get(\"node\")}')
"

```

#### 방법 3: 가상화 지표 수집 활성화 여부 사전 검증

만약 위의 두 팟 조회를 변경했음에도 결과가 지속해서 `0개`를 나타낸다면 사용자 워크로드 모니터링 컴포넌트 자체가 내려가 있는 상태일 수 있습니다. 설정 상태를 진단하고 비활성화 상태라면 즉시 활성화 조치합니다.

```bash
# 1. 수집 인프라 Map 설정 내 enableUserWorkload 유무 파악
oc get configmap cluster-monitoring-config -n openshift-monitoring -o jsonpath='{.data.config\.yaml}'

# 2. 만약 활성화되지 않았다면 아래 명령으로 긴급 패치 적용
oc patch configmap/cluster-monitoring-config -n openshift-monitoring --type=merge \
  -p '{"data":{"config.yaml":"enableUserWorkload: true\n"}}'

```

---

### 4. 개선 제안 (더 나아질 수 있는 부분)

* **대시보드 개발용 전용 서비스 계정(SA) 수립:** 외부 솔루션(예: Grafana 또는 수립 중인 AIBox 리포터 등)이 클러스터 내의 가상화 상태 지표를 백엔드에서 긁어가도록 스크립트를 빌드할 때는, `admin` 토큰이나 임시 세션 토큰 대신 `cluster-reader` 권한이 맵핑된 자동 갱신형 `ServiceAccountToken`을 별도 발급받아 Thanos Querier API 주소(`:9091`)와 고정 연동하는 아키텍처로 엔지니어링 설계를 변환하는 것을 권장합니다.
* **KubeVirt 전용 PrometheusRule 연동 최적화:** 가상 머신의 정상 작동 여부(`kubevirt_vm_info`)와 더불어 노드 장애 시 VM 대피 처리 신뢰성을 보장하기 위해, `prometheus-user-workload` 쪽에 임계치 경고 룰(`PrometheusRule`)을 함께 정의하여 노드 내 `virt-launcher` 컨테이너 다운타임 상황 시 텔레그램이나 중앙 관제 통로로 실시간 얼럿이 즉각 전파되도록 감시 체계를 고도화할 필요가 있습니다.

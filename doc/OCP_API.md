**✅ OpenShift Container Platform 4.20 API 정리**

사용자가 제공한 링크는 **Overview 문서**로, 전체 REST API 상세 목록이 아닌 **API 지원 정책(API Tiers)**과 아키텍처 개요를 주로 다룹니다.  
아래는 해당 문서와 OpenShift 4.20 공식 API Overview를 기반으로 **깔끔하게 정리**한 내용입니다.

### 1. OpenShift API 기본 구조

OpenShift API는 **Kubernetes API**를 100% 확장한 형태입니다.

- **API 형식**: `/apis/<group>/<version>/<resource>`
- **주요 그룹**:
  - **Kubernetes Core**: `""` (v1) — Pod, Service, Deployment 등
  - **Kubernetes 그룹**: `*.k8s.io`
  - **OpenShift 그룹**: `*.openshift.io`
  - **Operator 그룹**: `*.operators.coreos.com`, `*.operator.openshift.io`

---

### 2. API Support Tiers (지원 등급) — 가장 중요한 내용

OpenShift 4.20에서는 API를 **4단계**로 구분하여 지원합니다.

| Tier | 안정성 | Deprecation / Removal 정책 | 주요 대상 |
|------|--------|---------------------------|----------|
| **Tier 1** | 최고 안정성 | Major Release 내에서는 거의 제거되지 않음 | 핵심 Kubernetes + OpenShift 기본 API (`v1`) |
| **Tier 2** | 안정적 | Deprecation 후 최소 9개월 또는 3 Minor Release | 일부 Beta 버전 API |
| **Tier 3** | 보통 | Operator Hub, Developer Tools | 대부분의 Operator CRD, Tech Preview |
| **Tier 4** | 불안정 | 언제든 변경/제거 가능 | `v1alpha1`, 내부 전용 CRD, Experimental |

---

### 3. 주요 OpenShift API 그룹 및 Tier (4.20 기준)

| API Group | Version | Tier | 주요 리소스 예시 |
|----------|--------|------|----------------|
| `apps.openshift.io` | v1 | Tier 1 | DeploymentConfig |
| `build.openshift.io` | v1 | Tier 1 | Build, BuildConfig |
| `image.openshift.io` | v1 | Tier 1 | ImageStream, Image |
| `route.openshift.io` | v1 | Tier 1 | Route |
| `project.openshift.io` | v1 | Tier 1 | Project |
| `oauth.openshift.io` | v1 | Tier 1 | OAuthClient, OAuthAccessToken |
| `user.openshift.io` | v1 | Tier 1 | User, Group, Identity |
| `config.openshift.io` | v1 | Tier 1 | ClusterVersion, Ingress, Network, Authentication |
| `operator.openshift.io` | v1 | Tier 1 | Various Operators (Console, Storage 등) |
| `security.openshift.io` | v1 | Tier 1 (일부 Tier 2) | SecurityContextConstraints (SCC) |
| `console.openshift.io` | v1 | Tier 2 | ConsolePlugin, ConsoleQuickStart |
| `monitoring.coreos.com` | v1 | Tier 1 | PrometheusRule, ServiceMonitor |
| `operators.coreos.com` | v1, v1alpha1 | Tier 1 | ClusterServiceVersion (CSV), Subscription |

---

### 4. Kubernetes 기본 API (Core)

- **v1** (Tier 1): Pod, Service, ConfigMap, Secret, PersistentVolume, Namespace, Node, Deployment, StatefulSet, DaemonSet 등
- **v1beta1 / v1alpha1**: Tier 2 또는 Tier 4

---

### 5. API 사용 시 권장 사항 (문서에서 강조)

- **Tier 1** API를 프로덕션에서 우선 사용하세요.
- `v1alpha1`은 **Tier 4**로 취급 → 프로덕션에서 사용 금지.
- Undocumented 필드나 `unsupported*` prefix가 붙은 필드는 사용하지 마세요.
- Operator CRD는 해당 Operator의 문서를 반드시 확인하세요 (Tier 3가 많음).
- API Deprecation 시 충분한 사전 공지와 Migration 기간을 제공합니다.

---

**추가 참고 자료 (4.20)**

- 공식 API Overview: [https://docs.redhat.com/en/documentation/openshift_container_platform/4.20/html-single/api_overview/index](https://docs.redhat.com/en/documentation/openshift_container_platform/4.20/html-single/api_overview/index)
- 전체 Kubernetes API Reference: Kubernetes 공식 문서 (v1.32 기반)
- OpenShift REST API 상세 탐색: 클러스터 내에서 `oc api-resources` 또는 `kubectl api-resources` 명령어 사용

필요하시면 **특정 카테고리**(예: Networking, Authentication, Machine API, Monitoring API 등)별로 더 상세히 정리해 드리겠습니다!

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AIBox Infrastructure Intelligence Hub - Ultimate Edition v4
Author: Senior Python Full-stack Engineer & Architect

[v4 신규]
  INFRA  Cluster Operators   전체 오퍼레이터 상태 그리드 (oc get clusteroperator)
  INFRA  Ingress Router      요청률 / 4xx·5xx / 세션수 / 추이 스파크라인
  INFRA  MachineConfigPool   풀별 Ready / Updated / Degraded 노드 수
  INFRA  Scheduler           Pending Pods / 스케줄링 p99 추이
  INFRA  OVN-Kubernetes      Logical Port 수 / NB·SB DB 리더
  INFRA  Image Registry      요청률 추이
  LAYOUT body overflow-hidden 제거 → 전체 스크롤 정상화
  UI/UX  VM 사이드바 네임스페이스 그룹화 (접기/펼치기)
  DASH   Global Top5 CPU / Memory / Storage VM 패널
  DASH   ETCD / API Server 성능 추이 스파크라인
"""

import subprocess, json, os, logging, re, ssl, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html import escape
from typing import List, Dict, Final, Optional, Tuple
from dataclasses import dataclass, field

# =================================================================
# Logging
# =================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(funcName)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# =================================================================
# Constants
# =================================================================
REMOTE_SERVER: Final[str] = "100.100.100.10"
REMOTE_PATH:   Final[str] = "/data/iso/AIBox/ocp/"
FILE_NAME:     Final[str] = "index.html"
CMD_TIMEOUT:   Final[int] = 30
PROMETHEUS_NS: Final[str] = "openshift-monitoring"
THANOS_PROXY_BASE: Final[str] = (
    f"/api/v1/namespaces/{PROMETHEUS_NS}"
    "/services/https:thanos-querier:9091/proxy/api/v1/query"
)
THANOS_RANGE_BASE: Final[str] = (
    f"/api/v1/namespaces/{PROMETHEUS_NS}"
    "/services/https:thanos-querier:9091/proxy/api/v1/query_range"
)
PROMETHEUS_ROUTE_CANDIDATES: Final[List[str]] = [
    "thanos-querier", "prometheus-k8s-external", "prometheus-k8s",
]
SPARKLINE_HOURS: Final[int] = 1
SPARKLINE_STEP:  Final[str] = "5m"

HEALTH_JOB_CANDIDATES: Final[Dict[str, List[str]]] = {
    "api_server": ["apiserver", "kube-apiserver", "openshift-apiserver"],
    "etcd":       ["etcd", "etcd-metrics"],
    "coredns":    ["dns-default", "coredns", "kube-dns"],
    "router":     ["router-internal-default", "openshift-router"],
    "registry":   ["openshift-image-registry", "image-registry"],
    "scheduler":  ["scheduler", "kube-scheduler"],
}

STATUS_MAP: Final[Dict[str, str]] = {
    "running":"running", "provisioning":"provisioning", "starting":"provisioning",
    "scheduling":"provisioning", "pending":"provisioning",
    "waitingforvolumesbinding":"provisioning",
    "stopped":"stopped", "stopping":"stopped", "terminating":"stopped", "paused":"stopped",
    "failed":"failed", "errored":"failed", "crashloopbackoff":"failed",
}
STATUS_STYLE: Final[Dict[str, Dict[str, str]]] = {
    "running":      {"text":"Running",      "color":"text-emerald-400","dot":"bg-emerald-500"},
    "provisioning": {"text":"Provisioning", "color":"text-amber-400",  "dot":"bg-amber-500"},
    "stopped":      {"text":"Stopped",      "color":"text-slate-400",  "dot":"bg-slate-500"},
    "failed":       {"text":"Failed",       "color":"text-red-400",    "dot":"bg-red-500"},
    "unknown":      {"text":"Unknown",      "color":"text-gray-400",   "dot":"bg-gray-600"},
}

# =================================================================
# Data Models
# =================================================================
@dataclass
class ClusterHealth:
    api_server: Optional[float] = None
    etcd: Optional[float] = None
    coredns: Optional[float] = None
    etcd_leader: Optional[float] = None
    firing_alerts: int = 0
    cluster_cpu_pct: Optional[float] = None
    cluster_memory_pct: Optional[float] = None
    matched_jobs: Dict[str, str] = field(default_factory=dict)

@dataclass
class PerformanceTrend:
    etcd_wal_p99: List[float] = field(default_factory=list)
    etcd_peer_rtt: List[float] = field(default_factory=list)
    api_req_rate: List[float] = field(default_factory=list)
    api_err_rate: List[float] = field(default_factory=list)
    api_latency_p99: List[float] = field(default_factory=list)
    time_labels: List[str] = field(default_factory=list)

@dataclass
class ClusterOperatorStatus:
    name: str
    version: str = "N/A"
    available: Optional[bool] = None
    progressing: Optional[bool] = None
    degraded: Optional[bool] = None
    message: str = ""

@dataclass
class MCPStatus:
    name: str
    machine_count: int = 0
    ready_count: int = 0
    updated_count: int = 0
    degraded_count: int = 0
    paused: bool = False

@dataclass
class InfraMetrics:
    """v4 신규: 인프라 서비스 모니터링"""
    cluster_operators: List[ClusterOperatorStatus] = field(default_factory=list)
    mcp_pools: List[MCPStatus] = field(default_factory=list)
    # Router
    router_req_rate: Optional[float] = None
    router_4xx_rate: Optional[float] = None
    router_5xx_rate: Optional[float] = None
    router_sessions: Optional[float] = None
    router_routes:   Optional[float] = None
    router_req_trend: List[float] = field(default_factory=list)
    router_5xx_trend: List[float] = field(default_factory=list)
    # Scheduler
    sched_pending: Optional[float] = None
    sched_lat_trend: List[float] = field(default_factory=list)
    # OVN
    ovn_ports: Optional[float] = None
    ovn_nb_leader: Optional[float] = None
    # Registry
    reg_req_rate: Optional[float] = None
    reg_trend: List[float] = field(default_factory=list)
    # 추이 시간 레이블
    trend_labels: List[str] = field(default_factory=list)

@dataclass
class NodeMetrics:
    name: str; cpu_usage: str; memory_usage: str; status: str; roles: str; age: str
    memory_bytes: int = 0
    cpu_pct_realtime: Optional[float] = None
    memory_pct_realtime: Optional[float] = None

@dataclass
class VMMetrics:
    name: str; namespace: str; status: str; cpu_cores: str; memory_total: str
    creation_time: str; node: str; ip_address: str; os_info: str
    volumes: List[Dict[str, str]] = field(default_factory=list)
    status_group: str = "unknown"
    cpu_usage_pct: Optional[float] = None; memory_usage_pct: Optional[float] = None
    memory_used_bytes: int = 0
    net_rx_bps: Optional[float] = None; net_tx_bps: Optional[float] = None
    disk_read_bps: Optional[float] = None; disk_write_bps: Optional[float] = None

@dataclass
class StoragePoolDetail:
    name: str; provisioner: str
    total_capacity_bytes: int = 0; used_capacity_bytes: int = 0
    pv_count: int = 0; pvc_count: int = 0
    disk_used_bytes: int = 0; disk_capacity_bytes: int = 0; disk_available_bytes: int = 0

@dataclass
class SystemMetrics:
    nodes: List[NodeMetrics] = field(default_factory=list)
    vms: List[VMMetrics] = field(default_factory=list)
    pv_data: List[Dict[str, str]] = field(default_factory=list)
    pvc_data: List[Dict[str, str]] = field(default_factory=list)
    storage_pools: Dict[str, StoragePoolDetail] = field(default_factory=dict)
    total_pv_capacity_bytes: int = 0; total_pvc_requested_bytes: int = 0
    global_memory_total: str = "N/A"; ocp_version: str = "N/A"; last_updated: str = ""
    cluster_health: ClusterHealth = field(default_factory=ClusterHealth)
    perf_trend: PerformanceTrend = field(default_factory=PerformanceTrend)
    infra: InfraMetrics = field(default_factory=InfraMetrics)
    vm_running_count: int = 0; vm_provisioning_count: int = 0
    vm_stopped_count: int = 0; vm_failed_count: int = 0
    pvc_disk_stats: Dict[str, Dict[str, int]] = field(default_factory=dict)

# =================================================================
# Core Engine
# =================================================================
class MetricsCollector:
    def __init__(self):
        self.metrics = SystemMetrics()
        self.metrics.last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._prom_strategy: str = ""
        self._prom_token: str = ""
        self._prom_host: str = ""
        self._init_prometheus_client()

    # ── 유틸리티 ────────────────────────────────────────────────────
    def _run_command(self, cmd: List[str], timeout: int = CMD_TIMEOUT) -> str:
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            if r.returncode != 0:
                logger.warning(f"Command failed: {' '.join(cmd)} | {r.stderr.strip()}")
                return ""
            return r.stdout.strip()
        except subprocess.TimeoutExpired: logger.error(f"Timeout ({timeout}s): {' '.join(cmd)}"); return ""
        except FileNotFoundError: logger.error(f"Not found: {cmd[0]!r}"); return ""
        except Exception as e: logger.error(f"Error {cmd[0]!r}: {e}"); return ""

    def _run_command_silent(self, cmd: List[str], timeout: int = CMD_TIMEOUT) -> str:
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            if r.returncode != 0:
                logger.debug(f"Silent fail: {' '.join(cmd)} | {r.stderr.strip()}"); return ""
            return r.stdout.strip()
        except Exception as e: logger.debug(f"Silent error: {e}"); return ""

    def _parse_storage_to_bytes(self, s: str) -> int:
        if not s or s in ("N/A","0",""): return 0
        m = re.match(r'^([0-9.]+(?:[eE][+-]?[0-9]+)?)\s*(Ki|Mi|Gi|Ti|Pi|k|M|G|T|P)?$', s.strip())
        if not m: return 0
        try:
            val, unit = float(m.group(1)), m.group(2) or ""
            mults={"Ki":1024,"Mi":1024**2,"Gi":1024**3,"Ti":1024**4,"Pi":1024**5,
                   "k":1000,"M":1000**2,"G":1000**3,"T":1000**4,"P":1000**5}
            return int(val * mults.get(unit, 1))
        except Exception: return 0

    def _format_bytes(self, b: int) -> str:
        if b==0: return "0 B"
        names=("B","KiB","MiB","GiB","TiB","PiB"); v,i=float(b),0
        while v>=1024 and i<len(names)-1: v/=1024.0; i+=1
        return f"{v:.2f} {names[i]}"

    def _format_age(self, ts: str) -> str:
        try:
            d=datetime.now(timezone.utc)-datetime.fromisoformat(ts.replace("Z","+00:00"))
            if d.days>=1: return f"{d.days}d"
            h=d.seconds//3600
            return f"{h}h" if h>=1 else f"{d.seconds//60}m"
        except Exception: return ts

    def _format_bps(self, bps: Optional[float]) -> str:
        if bps is None: return "N/A"
        for u in ("B/s","KiB/s","MiB/s","GiB/s"):
            if bps<1024: return f"{bps:.1f} {u}"
            bps/=1024
        return f"{bps:.1f} TiB/s"

    def _format_pct(self, pct: Optional[float]) -> str:
        return f"{pct:.1f}%" if pct is not None else "N/A"

    def _pct_bar_color(self, pct: Optional[float]) -> str:
        if pct is None: return "bg-slate-600"
        if pct<70: return "bg-emerald-500"
        if pct<90: return "bg-amber-500"
        return "bg-red-500"

    def _classify_vm_status(self, s: str) -> str: return STATUS_MAP.get(s.lower(),"unknown")
    def _classify_disk_type(self, n: str) -> str:
        if n.endswith("-rootdisk"): return "root"
        if n.endswith("-datadisk"): return "data"
        return "etc"

    # ── Prometheus 접근 ─────────────────────────────────────────────
    def _init_prometheus_client(self) -> None:
        try:
            raw = self._run_command_silent(["oc","get","--raw",f"{THANOS_PROXY_BASE}?query=up"], timeout=10)
            if raw and json.loads(raw).get("status")=="success":
                self._prom_strategy="raw"; logger.info("Prometheus: oc get --raw"); return
        except Exception: pass

        token = self._run_command(["oc","whoami","-t"], timeout=10)
        if not token: logger.warning("Prometheus: 토큰 없음 → 실시간 비활성화"); return

        for rn in PROMETHEUS_ROUTE_CANDIDATES:
            host = self._run_command(["oc","get","route",rn,"-n",PROMETHEUS_NS,"-o","jsonpath={.spec.host}"], timeout=10)
            if not host: continue
            if self._query_prometheus_via_route("up", token, host) is not None:
                self._prom_strategy="route"; self._prom_token=token; self._prom_host=host
                logger.info(f"Prometheus: route ({rn} → {host})"); return
        logger.warning("Prometheus 접근 실패")

    def _refresh_token(self) -> bool:
        t=self._run_command(["oc","whoami","-t"],timeout=10)
        if t and t!=self._prom_token: self._prom_token=t; return True
        return False

    def _http_get(self, url: str, token: str, timeout: int=10) -> Optional[dict]:
        ctx=ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
        req=urllib.request.Request(url, headers={"Authorization":f"Bearer {token}"})
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code==401 and self._refresh_token(): return self._http_get(url, self._prom_token, timeout)
            return None
        except Exception: return None

    def _query_prometheus_via_route(self, promql: str, token: str, host: str) -> Optional[List[Dict]]:
        url=f"https://{host}/api/v1/query?query={urllib.parse.quote(promql)}"
        data=self._http_get(url, token)
        if data and data.get("status")=="success": return data.get("data",{}).get("result",[])
        return None

    def _query_prometheus(self, promql: str) -> List[Dict]:
        if not self._prom_strategy: return []
        if self._prom_strategy=="raw":
            raw=self._run_command_silent(["oc","get","--raw",f"{THANOS_PROXY_BASE}?query={urllib.parse.quote(promql)}"],timeout=15)
            if not raw: return []
            try:
                d=json.loads(raw); return d.get("data",{}).get("result",[]) if d.get("status")=="success" else []
            except Exception: return []
        r=self._query_prometheus_via_route(promql, self._prom_token, self._prom_host)
        return r if r is not None else []

    def _query_prometheus_range(self, promql: str) -> Tuple[List[float], List[str]]:
        if not self._prom_strategy: return [],[]
        end_ts=int(time.time()); start_ts=end_ts-SPARKLINE_HOURS*3600
        params=urllib.parse.urlencode({"query":promql,"start":start_ts,"end":end_ts,"step":SPARKLINE_STEP})
        if self._prom_strategy=="raw":
            raw=self._run_command_silent(["oc","get","--raw",f"{THANOS_RANGE_BASE}?{params}"],timeout=15)
            if not raw: return [],[]
            try:
                d=json.loads(raw); results=d.get("data",{}).get("result",[]) if d.get("status")=="success" else []
            except Exception: return [],[]
        else:
            data=self._http_get(f"https://{self._prom_host}/api/v1/query_range?{params}",self._prom_token,timeout=15)
            if not data or data.get("status")!="success": return [],[]
            results=data.get("data",{}).get("result",[])
        if not results: return [],[]
        vals,lbls=[],[]
        for ts,val in results[0].get("values",[]):
            try: vals.append(float(val)); lbls.append(datetime.fromtimestamp(float(ts)).strftime("%H:%M"))
            except (ValueError,TypeError): pass
        return vals,lbls

    def _prom_scalar(self, results: List[Dict]) -> Optional[float]:
        try: return float(results[0]["value"][1]) if results else None
        except (KeyError,ValueError,IndexError): return None

    def _discover_jobs(self) -> Dict[str,str]:
        results=self._query_prometheus("count by(job) (up)")
        existing={item.get("metric",{}).get("job","") for item in results}
        logger.info(f"Prometheus job 탐지: {len(existing)}개")
        matched:Dict[str,str]={}
        for cat,candidates in HEALTH_JOB_CANDIDATES.items():
            for c in candidates:
                if c in existing: matched[cat]=c; logger.info(f"  매핑: {cat}→{c}"); break
        return matched

    # ── 기본 수집 ────────────────────────────────────────────────────
    def fetch_node_metrics(self) -> None:
        logger.info("Fetching Node Metrics...")
        try:
            vj=self._run_command(["oc","version","-o","json"])
            if vj:
                try: self.metrics.ocp_version=json.loads(vj).get("openshiftVersion","N/A") or "N/A"
                except Exception: pass
            top=self._run_command(["oc","adm","top","nodes","--no-headers"])
            nu:Dict[str,Dict[str,str]]={}
            for line in top.split('\n'):
                p=line.split()
                if len(p)>=5: nu[p[0]]={"cpu":p[2],"mem":p[4]}
            nj=self._run_command(["oc","get","nodes","-o","json"])
            if not nj: return
            tc=0
            for node in json.loads(nj).get("items",[]):
                name=node["metadata"]["name"]; lbls=node["metadata"].get("labels",{})
                roles=", ".join(sorted(k.split("/")[-1] for k in lbls if k.startswith("node-role.kubernetes.io/"))) or "worker"
                conds=node.get("status",{}).get("conditions",[]); ready=next((c for c in conds if c["type"]=="Ready"),{})
                status="Ready" if ready.get("status")=="True" else "NotReady"
                cap_b=self._parse_storage_to_bytes(node.get("status",{}).get("capacity",{}).get("memory","0"))
                tc+=cap_b; usage=nu.get(name,{"cpu":"N/A","mem":"N/A"})
                self.metrics.nodes.append(NodeMetrics(
                    name=name,cpu_usage=usage["cpu"],memory_usage=usage["mem"],status=status,roles=roles,
                    age=self._format_age(node["metadata"]["creationTimestamp"]),memory_bytes=cap_b))
            self.metrics.global_memory_total=self._format_bytes(tc)
        except Exception as e: logger.error(f"Node error: {e}")

    def fetch_vm_metrics(self) -> None:
        logger.info("Fetching VM Metrics...")
        try:
            vj=self._run_command(["oc","get","vm","-A","-o","json"])
            if not vj: return
            items=json.loads(vj).get("items",[])
            def _vmi(name:str,ns:str)->Dict:
                raw=self._run_command(["oc","get","vmi",name,"-n",ns,"-o","json"])
                if not raw: return {}
                try:
                    vd=json.loads(raw); ifaces=vd.get("status",{}).get("interfaces",[])
                    return {"node":vd.get("status",{}).get("nodeName","N/A"),
                            "ip":ifaces[0].get("ipAddress","N/A") if ifaces else "N/A",
                            "os":vd.get("status",{}).get("guestOSInfo",{}).get("prettyName","N/A") or "N/A"}
                except Exception as e: logger.warning(f"VMI {name}: {e}"); return {}
            running=[(v["metadata"]["name"],v["metadata"]["namespace"]) for v in items
                     if v.get("status",{}).get("printableStatus","")=="Running"]
            vmis:Dict[Tuple[str,str],Dict]={}
            if running:
                with ThreadPoolExecutor(max_workers=min(len(running),10)) as ex:
                    futs={ex.submit(_vmi,n,ns):(n,ns) for n,ns in running}
                    for fut in as_completed(futs):
                        try: vmis[futs[fut]]=fut.result()
                        except Exception: pass
            for vm in items:
                name=vm["metadata"]["name"]; ns=vm["metadata"]["namespace"]
                status=vm.get("status",{}).get("printableStatus","Unknown")
                domain=vm.get("spec",{}).get("template",{}).get("spec",{}).get("domain",{})
                cpu_c=str(domain.get("cpu",{}).get("cores","N/A"))
                mem_t=domain.get("resources",{}).get("requests",{}).get("memory","N/A")
                vols=[{"name":v.get("name","N/A"),
                       "pvc":v.get("persistentVolumeClaim",{}).get("claimName","N/A"),
                       "disk_type":self._classify_disk_type(v.get("persistentVolumeClaim",{}).get("claimName",""))}
                      for v in vm.get("spec",{}).get("template",{}).get("spec",{}).get("volumes",[])]
                vi=vmis.get((name,ns),{})
                self.metrics.vms.append(VMMetrics(
                    name=name,namespace=ns,status=status,cpu_cores=cpu_c,memory_total=mem_t,
                    creation_time=self._format_age(vm["metadata"]["creationTimestamp"]),
                    node=vi.get("node","N/A"),ip_address=vi.get("ip","N/A"),os_info=vi.get("os","N/A"),
                    volumes=vols,status_group=self._classify_vm_status(status)))
        except Exception as e: logger.error(f"VM error: {e}")

    def fetch_advanced_storage_metrics(self) -> None:
        logger.info("Fetching Storage Metrics...")
        try:
            sj=self._run_command(["oc","get","sc","-o","json"])
            if sj:
                for sc in json.loads(sj).get("items",[]):
                    n=sc["metadata"]["name"]
                    self.metrics.storage_pools[n]=StoragePoolDetail(name=n,provisioner=sc.get("provisioner","Unknown"))
        except Exception as e: logger.error(f"SC: {e}")
        try:
            pj=self._run_command(["oc","get","pv","-o","json"])
            if pj:
                for pv in json.loads(pj).get("items",[]):
                    name=pv["metadata"]["name"]; cs=pv.get("spec",{}).get("capacity",{}).get("storage","0")
                    cb=self._parse_storage_to_bytes(cs)
                    modes=", ".join(pv.get("spec",{}).get("accessModes",[])); reclaim=pv.get("spec",{}).get("persistentVolumeReclaimPolicy","N/A")
                    status=pv.get("status",{}).get("phase","Unknown"); cr=pv.get("spec",{}).get("claimRef",{})
                    claim=f"{cr.get('namespace','')}/{cr.get('name','')}" if cr else "Unbound"
                    sc_name=pv.get("spec",{}).get("storageClassName","Unknown")
                    self.metrics.total_pv_capacity_bytes+=cb
                    if sc_name not in self.metrics.storage_pools:
                        self.metrics.storage_pools[sc_name]=StoragePoolDetail(name=sc_name,provisioner="Implicit/Unknown")
                    pool=self.metrics.storage_pools[sc_name]; pool.pv_count+=1; pool.total_capacity_bytes+=cb
                    if status=="Bound": pool.used_capacity_bytes+=cb
                    self.metrics.pv_data.append({"Name":name,"Capacity":cs,"Access Modes":modes,"Reclaim Policy":reclaim,
                        "Status":status,"Claim":claim,"StorageClass":sc_name,"Age":self._format_age(pv["metadata"]["creationTimestamp"])})
        except Exception as e: logger.error(f"PV: {e}")
        try:
            pcj=self._run_command(["oc","get","pvc","-A","-o","json"])
            if pcj:
                for pvc in json.loads(pcj).get("items",[]):
                    name=pvc["metadata"]["name"]; ns=pvc["metadata"]["namespace"]
                    status=pvc.get("status",{}).get("phase","Unknown"); sc=pvc.get("spec",{}).get("storageClassName","Unknown")
                    req=pvc.get("spec",{}).get("resources",{}).get("requests",{}).get("storage","0")
                    self.metrics.total_pvc_requested_bytes+=self._parse_storage_to_bytes(req)
                    if sc in self.metrics.storage_pools: self.metrics.storage_pools[sc].pvc_count+=1
                    self.metrics.pvc_data.append({"Name":name,"Namespace":ns,"Status":status,
                        "Requested Capacity":req,"StorageClass":sc,"Volume Name":pvc.get("spec",{}).get("volumeName","Pending/Unbound")})
        except Exception as e: logger.error(f"PVC: {e}")

    # ── 클러스터 헬스 + 오퍼레이터 ──────────────────────────────────
    def fetch_cluster_health(self) -> None:
        if not self._prom_strategy: return
        logger.info("Fetching Cluster Health...")
        matched=self._discover_jobs(); self.metrics.cluster_health.matched_jobs=matched
        queries:Dict[str,str]={}
        for cat,key in [("api_server","api"),("etcd","etcd"),("coredns","coredns")]:
            if cat in matched: queries[key]=f'min(up{{job="{matched[cat]}"}}) by ()'
        queries.update({
            "leader":"min(etcd_server_has_leader) by ()",
            "alerts":'count(ALERTS{alertstate="firing"}) or vector(0)',
            "cpu":'(1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))) * 100',
            "mem":'(1 - sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes)) * 100',
        })
        with ThreadPoolExecutor(max_workers=max(len(queries),1)) as ex:
            futs={ex.submit(self._query_prometheus,q):k for k,q in queries.items()}
            for fut in as_completed(futs):
                k=futs[fut]; val=self._prom_scalar(fut.result())
                if val is None: continue
                h=self.metrics.cluster_health
                if k=="api": h.api_server=val
                elif k=="etcd": h.etcd=val
                elif k=="coredns": h.coredns=val
                elif k=="leader": h.etcd_leader=val
                elif k=="alerts": h.firing_alerts=int(val)
                elif k=="cpu": h.cluster_cpu_pct=val
                elif k=="mem": h.cluster_memory_pct=val

    def fetch_cluster_operators(self) -> None:
        """oc get clusteroperator → 전체 오퍼레이터 상태 수집"""
        logger.info("Fetching Cluster Operators...")
        try:
            raw=self._run_command(["oc","get","clusteroperator","-o","json"])
            if not raw: return
            for item in json.loads(raw).get("items",[]):
                name=item["metadata"]["name"]
                versions=item.get("status",{}).get("versions",[])
                version=next((v["version"] for v in versions if v.get("name")=="operator"),"N/A")
                conds={c["type"]:c for c in item.get("status",{}).get("conditions",[])}
                def cond_bool(t:str)->Optional[bool]:
                    c=conds.get(t); return (c.get("status")=="True") if c else None
                msg=""
                for t in ["Degraded","Progressing","Available"]:
                    c=conds.get(t,{})
                    if c.get("message"): msg=c["message"][:120]; break
                self.metrics.infra.cluster_operators.append(ClusterOperatorStatus(
                    name=name,version=version,
                    available=cond_bool("Available"),progressing=cond_bool("Progressing"),
                    degraded=cond_bool("Degraded"),message=msg))
            co=self.metrics.infra.cluster_operators
            logger.info(f"  오퍼레이터: 총 {len(co)}개, 정상 {sum(1 for o in co if o.available and not o.degraded)}개, "
                       f"Degraded {sum(1 for o in co if o.degraded)}개")
        except Exception as e: logger.error(f"ClusterOperator: {e}")

    def fetch_machine_config(self) -> None:
        """oc get mcp → MachineConfigPool 상태 수집"""
        logger.info("Fetching MachineConfigPools...")
        try:
            raw=self._run_command(["oc","get","mcp","-o","json"])
            if not raw: return
            for item in json.loads(raw).get("items",[]):
                name=item["metadata"]["name"]; st=item.get("status",{})
                self.metrics.infra.mcp_pools.append(MCPStatus(
                    name=name,
                    machine_count=st.get("machineCount",0),ready_count=st.get("readyMachineCount",0),
                    updated_count=st.get("updatedMachineCount",0),degraded_count=st.get("degradedMachineCount",0),
                    paused=item.get("spec",{}).get("paused",False)))
        except Exception as e: logger.error(f"MCP: {e}")

    # ── v4 신규: 인프라 서비스 메트릭 ────────────────────────────────
    def fetch_infra_metrics(self) -> None:
        """
        Router / Scheduler / OVN / Registry 인프라 메트릭 수집.
        instant 값 + 1h 추이를 한 번에 병렬 조회.
        """
        if not self._prom_strategy: return
        logger.info("Fetching Infra Service Metrics...")
        matched=self.metrics.cluster_health.matched_jobs
        router_job=matched.get("router","router-internal-default")
        sched_job=matched.get("scheduler","scheduler")
        reg_job=matched.get("registry","openshift-image-registry")

        instant_queries:Dict[str,str]={
            "router_req": f'sum(rate(haproxy_backend_http_responses_total{{job="{router_job}"}}[5m]))',
            "router_4xx": f'sum(rate(haproxy_backend_http_responses_total{{job="{router_job}",code="4xx"}}[5m]))',
            "router_5xx": f'sum(rate(haproxy_backend_http_responses_total{{job="{router_job}",code="5xx"}}[5m]))',
            "router_sess":f'sum(haproxy_server_current_sessions{{job="{router_job}"}})',
            "router_routes":f'count(haproxy_backend_status{{job="{router_job}"}})',
            "sched_pending":'sum(scheduler_pending_pods) or vector(0)',
            "ovn_ports":'sum(ovnkube_controller_logical_port_total) or sum(ovnkube_master_logical_port_total) or vector(0)',
            "ovn_nb":'max(ovnkube_controller_nb_db_leader) or max(ovnkube_master_nb_db_leader) or vector(0)',
            "reg_req":f'sum(rate(registry_http_requests_total{{job="{reg_job}"}}[5m])) or vector(0)',
        }
        range_queries:Dict[str,str]={
            "router_req_t":f'sum(rate(haproxy_backend_http_responses_total{{job="{router_job}"}}[5m]))',
            "router_5xx_t":f'sum(rate(haproxy_backend_http_responses_total{{job="{router_job}",code="5xx"}}[5m]))',
            "sched_lat_t": f'histogram_quantile(0.99,sum(rate(scheduler_scheduling_attempt_duration_seconds_bucket{{job="{sched_job}"}}[5m])) by (le))',
            "reg_req_t":   f'sum(rate(registry_http_requests_total{{job="{reg_job}"}}[5m]))',
        }

        im=self.metrics.infra
        with ThreadPoolExecutor(max_workers=len(instant_queries)+len(range_queries)) as ex:
            i_futs={ex.submit(self._query_prometheus,q):k for k,q in instant_queries.items()}
            r_futs={ex.submit(self._query_prometheus_range,q):k for k,q in range_queries.items()}
            for fut in as_completed({**i_futs,**r_futs}):
                if fut in i_futs:
                    k=i_futs[fut]; val=self._prom_scalar(fut.result())
                    if val is None: continue
                    if k=="router_req": im.router_req_rate=val
                    elif k=="router_4xx": im.router_4xx_rate=val
                    elif k=="router_5xx": im.router_5xx_rate=val
                    elif k=="router_sess": im.router_sessions=val
                    elif k=="router_routes": im.router_routes=val
                    elif k=="sched_pending": im.sched_pending=val
                    elif k=="ovn_ports": im.ovn_ports=val
                    elif k=="ovn_nb": im.ovn_nb_leader=val
                    elif k=="reg_req": im.reg_req_rate=val
                else:
                    k=r_futs[fut]; vals,lbls=fut.result()
                    if not vals: continue
                    if k=="router_req_t": im.router_req_trend=vals; im.trend_labels=lbls
                    elif k=="router_5xx_t": im.router_5xx_trend=vals
                    elif k=="sched_lat_t": im.sched_lat_trend=vals
                    elif k=="reg_req_t": im.reg_trend=vals

    def fetch_performance_trends(self) -> None:
        if not self._prom_strategy: return
        logger.info("Fetching Performance Trends...")
        h=self.metrics.cluster_health; pt=self.metrics.perf_trend
        etcd_j=h.matched_jobs.get("etcd","etcd"); api_j=h.matched_jobs.get("api_server","apiserver")
        tq={
            "ew":f'histogram_quantile(0.99,rate(etcd_disk_wal_fsync_duration_seconds_bucket{{job="{etcd_j}"}}[5m]))',
            "er":f'histogram_quantile(0.99,rate(etcd_network_peer_round_trip_time_seconds_bucket{{job="{etcd_j}"}}[5m]))',
            "ar":f'sum(rate(apiserver_request_total{{job="{api_j}"}}[5m]))',
            "ae":f'sum(rate(apiserver_request_total{{job="{api_j}",code=~"5.."}}[5m]))',
            "al":f'histogram_quantile(0.99,sum(rate(apiserver_request_duration_seconds_bucket{{job="{api_j}"}}[5m])) by (le))',
        }
        with ThreadPoolExecutor(max_workers=len(tq)) as ex:
            futs={ex.submit(self._query_prometheus_range,q):k for k,q in tq.items()}
            for fut in as_completed(futs):
                k=futs[fut]; vals,lbls=fut.result()
                if not vals: continue
                if k=="ew": pt.etcd_wal_p99=vals; pt.time_labels=lbls
                elif k=="er": pt.etcd_peer_rtt=vals
                elif k=="ar": pt.api_req_rate=vals
                elif k=="ae": pt.api_err_rate=vals
                elif k=="al": pt.api_latency_p99=vals

    def fetch_node_realtime_metrics(self) -> None:
        if not self.metrics.nodes or not self._prom_strategy: return
        logger.info("Fetching Node Realtime Metrics...")
        test=self._query_prometheus('count by(node) (node_cpu_seconds_total{mode="idle"})')
        has_node=bool(test and test[0].get("metric",{}).get("node"))
        queries={"cpu_pct":"(1 - avg by(instance,node) (rate(node_cpu_seconds_total{mode='idle'}[5m]))) * 100",
                 "mem_pct":"(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100"}
        ip_to_node:Dict[str,str]={}
        if not has_node:
            for item in self._query_prometheus("kube_node_info"):
                lbl=item.get("metric",{})
                if lbl.get("node") and lbl.get("internal_ip"): ip_to_node[lbl["internal_ip"]]=lbl["node"]
            if ip_to_node: logger.info(f"노드 IP 매핑: {len(ip_to_node)}개")
        nrt:Dict[str,Dict[str,float]]={}
        with ThreadPoolExecutor(max_workers=2) as ex:
            futs={ex.submit(self._query_prometheus,q):k for k,q in queries.items()}
            for fut in as_completed(futs):
                k=futs[fut]
                for item in fut.result():
                    lbl=item.get("metric",{})
                    name=lbl.get("node","") or ip_to_node.get(lbl.get("instance","").split(":")[0],"")
                    if not name: continue
                    try: nrt.setdefault(name,{})[k]=float(item["value"][1])
                    except (KeyError,ValueError,IndexError): pass
        for n in self.metrics.nodes:
            d=nrt.get(n.name,{}); n.cpu_pct_realtime=d.get("cpu_pct"); n.memory_pct_realtime=d.get("mem_pct")

    def fetch_vm_realtime_metrics(self) -> None:
        if not self.metrics.vms or not self._prom_strategy: return
        logger.info("Fetching VM Realtime Metrics...")
        queries={"cpu_pct":"rate(kubevirt_vmi_cpu_usage_seconds_total[5m]) * 100",
                 "mem_used":"kubevirt_vmi_memory_used_bytes","mem_avail":"kubevirt_vmi_memory_available_bytes",
                 "net_rx":"rate(kubevirt_vmi_network_receive_bytes_total[5m])",
                 "net_tx":"rate(kubevirt_vmi_network_transmit_bytes_total[5m])",
                 "disk_r":"rate(kubevirt_vmi_storage_read_traffic_bytes_total[5m])",
                 "disk_w":"rate(kubevirt_vmi_storage_write_traffic_bytes_total[5m])"}
        rt:Dict[Tuple[str,str],Dict[str,float]]={}
        with ThreadPoolExecutor(max_workers=len(queries)) as ex:
            futs={ex.submit(self._query_prometheus,q):k for k,q in queries.items()}
            for fut in as_completed(futs):
                k=futs[fut]
                for item in fut.result():
                    lbl=item.get("metric",{}); name=lbl.get("name",""); ns=lbl.get("namespace","")
                    if not name or not ns: continue
                    try: rt.setdefault((name,ns),{})[k]=float(item["value"][1])
                    except (KeyError,ValueError,IndexError): pass
        for vm in self.metrics.vms:
            d=rt.get((vm.name,vm.namespace),{})
            vm.cpu_usage_pct=d.get("cpu_pct"); mu,ma=d.get("mem_used",0),d.get("mem_avail",0)
            if ma>0: vm.memory_usage_pct=mu/ma*100; vm.memory_used_bytes=int(mu)
            vm.net_rx_bps=d.get("net_rx"); vm.net_tx_bps=d.get("net_tx")
            vm.disk_read_bps=d.get("disk_r"); vm.disk_write_bps=d.get("disk_w")

    def fetch_pvc_disk_stats(self) -> None:
        if not self._prom_strategy: return
        logger.info("Fetching PVC Disk Stats...")
        queries={"used":"kubelet_volume_stats_used_bytes","capacity":"kubelet_volume_stats_capacity_bytes",
                 "available":"kubelet_volume_stats_available_bytes"}
        ps:Dict[str,Dict[str,int]]={}
        with ThreadPoolExecutor(max_workers=3) as ex:
            futs={ex.submit(self._query_prometheus,q):k for k,q in queries.items()}
            for fut in as_completed(futs):
                k=futs[fut]
                for item in fut.result():
                    lbl=item.get("metric",{}); pvc=lbl.get("persistentvolumeclaim",""); ns=lbl.get("namespace","")
                    if not pvc: continue
                    key=f"{ns}/{pvc}" if ns else pvc
                    try: ps.setdefault(key,{})[k]=int(float(item["value"][1]))
                    except (KeyError,ValueError,IndexError): pass
        self.metrics.pvc_disk_stats=ps
        pvc_to_sc={f"{p['Namespace']}/{p['Name']}":p["StorageClass"] for p in self.metrics.pvc_data}
        for key,stats in ps.items():
            sc=pvc_to_sc.get(key)
            if sc and sc in self.metrics.storage_pools:
                pool=self.metrics.storage_pools[sc]
                pool.disk_used_bytes+=stats.get("used",0); pool.disk_capacity_bytes+=stats.get("capacity",0)
                pool.disk_available_bytes+=stats.get("available",0)
        logger.info(f"PVC 통계: {len(ps)}개")

    # ── HTML 헬퍼 ─────────────────────────────────────────────────────
    def _build_vm_status_counts(self) -> None:
        for vm in self.metrics.vms:
            if   vm.status_group=="running":      self.metrics.vm_running_count+=1
            elif vm.status_group=="provisioning": self.metrics.vm_provisioning_count+=1
            elif vm.status_group=="stopped":      self.metrics.vm_stopped_count+=1
            elif vm.status_group=="failed":       self.metrics.vm_failed_count+=1

    def _render_usage_bar(self, pct: Optional[float], label: str) -> str:
        c=self._pct_bar_color(pct); tc=c.replace("bg-","text-"); w=f"{min(pct,100):.1f}" if pct is not None else "0"
        return (f"<div><div class='flex justify-between text-xs text-slate-400 mb-1'>"
                f"<span>{escape(label)}</span><span class='{tc} font-semibold'>{self._format_pct(pct)}</span></div>"
                f"<div class='w-full bg-slate-800 rounded-full h-2 border border-slate-700 overflow-hidden'>"
                f"<div class='{c} h-2 rounded-full' style='width:{w}%'></div></div></div>")

    def _render_health_card(self, label: str, value: Optional[float], icon_path: str, invert: bool=False) -> str:
        if value is None:           text,color,border="N/A","text-slate-400","border-slate-700/50"
        elif (value>=1 and not invert) or (value==0 and invert):
                                    text,color,border="Healthy","text-emerald-400","border-emerald-500/40"
        else:                       text,color,border="Unhealthy","text-red-400","border-red-500/40"
        return (f"<div class='bg-slate-800/80 rounded-xl p-4 border {border} shadow-lg flex items-center gap-3'>"
                f"<div class='p-2 rounded-lg bg-slate-900/50 flex-shrink-0'>"
                f"<svg class='w-5 h-5 {color}' fill='none' stroke='currentColor' viewBox='0 0 24 24'>"
                f"<path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='{icon_path}'/></svg></div>"
                f"<div><p class='text-xs text-slate-400 uppercase tracking-wider mb-0.5'>{escape(label)}</p>"
                f"<p class='text-base font-bold {color}'>{text}</p></div></div>")

    def _render_sparkline(self, values: List[float], color: str="#06b6d4", unit: str="", w: int=200, h: int=48) -> str:
        if not values or len(values)<2: return "<span class='text-xs text-slate-600'>수집 중...</span>"
        mn,mx=min(values),max(values); rng=mx-mn or 1e-9
        pts=[(i/(len(values)-1)*w, h-((v-mn)/rng*(h-4))-2) for i,v in enumerate(values)]
        path="M "+" L ".join(f"{x:.1f},{y:.1f}" for x,y in pts)
        fill=f"M {pts[0][0]:.1f},{h} "+" ".join(f"L {x:.1f},{y:.1f}" for x,y in pts)+f" L {pts[-1][0]:.1f},{h} Z"
        cur=values[-1]
        if unit=="ms": fn=f"{cur*1000:.1f}ms"; mn_s=f"{mn*1000:.1f}ms"; mx_s=f"{mx*1000:.1f}ms"
        elif unit=="/s": fn=f"{cur:.1f}/s"; mn_s=f"{mn:.1f}/s"; mx_s=f"{mx:.1f}/s"
        else: fn=f"{cur:.2f}{unit}"; mn_s=f"{mn:.2f}"; mx_s=f"{mx:.2f}"
        cid=color.replace("#","")
        return (f"<div><svg width='{w}' height='{h}' viewBox='0 0 {w} {h}' class='w-full'>"
                f"<defs><linearGradient id='g{cid}' x1='0' y1='0' x2='0' y2='1'>"
                f"<stop offset='0%' stop-color='{color}' stop-opacity='0.3'/>"
                f"<stop offset='100%' stop-color='{color}' stop-opacity='0.02'/></linearGradient></defs>"
                f"<path d='{fill}' fill='url(#g{cid})'/>"
                f"<path d='{path}' fill='none' stroke='{color}' stroke-width='1.5'/></svg>"
                f"<div class='flex justify-between text-[10px] text-slate-500 mt-0.5'>"
                f"<span>{mn_s} min</span>"
                f"<span class='font-semibold' style='color:{color}'>{fn} now</span>"
                f"<span>{mx_s} max</span></div></div>")

    def _compute_top5_vms(self):
        cpu5=sorted([v for v in self.metrics.vms if v.cpu_usage_pct is not None],key=lambda v:v.cpu_usage_pct or 0,reverse=True)[:5]
        mem5=sorted([v for v in self.metrics.vms if v.memory_usage_pct is not None],key=lambda v:v.memory_usage_pct or 0,reverse=True)[:5]
        vms:Dict[str,Tuple[int,"VMMetrics"]]={}
        for vm in self.metrics.vms:
            total=sum(self.metrics.pvc_disk_stats.get(f"{vm.namespace}/{v['pvc']}",{}).get("used",0) for v in vm.volumes)
            if total>0: vms[f"{vm.namespace}/{vm.name}"]=(total,vm)
        st5=sorted(vms.values(),key=lambda x:x[0],reverse=True)[:5]
        return cpu5,mem5,st5

    def _render_top5_card(self, rank: int, name: str, ns: str, value: str, pct: Optional[float], idx: int) -> str:
        c=self._pct_bar_color(pct); tc=c.replace("bg-","text-"); w=f"{min(pct,100):.1f}" if pct is not None else "0"
        return (f"<div class='flex items-center gap-3 p-2.5 rounded-lg bg-slate-900/50 hover:bg-slate-700/30 cursor-pointer transition-colors'"
                f" onclick=\"openTab('vm-{idx}')\">"
                f"<span class='text-slate-600 font-bold text-sm w-4 flex-shrink-0'>#{rank}</span>"
                f"<div class='flex-1 min-w-0'>"
                f"<div class='flex justify-between items-center mb-1'>"
                f"<span class='text-xs text-slate-200 font-medium truncate'>{escape(name)}</span>"
                f"<span class='{tc} text-xs font-bold ml-2 flex-shrink-0'>{value}</span></div>"
                f"<div class='w-full bg-slate-800 rounded-full h-1.5 overflow-hidden'>"
                f"<div class='{c} h-1.5 rounded-full' style='width:{w}%'></div></div>"
                f"<p class='text-[10px] text-slate-500 mt-0.5'>{escape(ns)}</p></div></div>")

    def _render_stat_pill(self, label: str, value: str, color: str) -> str:
        return (f"<div class='bg-slate-900/60 rounded-lg p-3 text-center'>"
                f"<p class='text-[10px] text-slate-500 uppercase tracking-wider mb-1'>{escape(label)}</p>"
                f"<p class='text-sm font-bold {color}'>{escape(value)}</p></div>")

    # ── HTML 생성 ─────────────────────────────────────────────────────
    def build_html(self) -> None:
        logger.info("Assembling HTML Dashboard (Ultimate Edition v4)...")
        self._build_vm_status_counts()
        h=self.metrics.cluster_health; pt=self.metrics.perf_trend; im=self.metrics.infra
        vm_idx_map={(vm.name,vm.namespace):i for i,vm in enumerate(self.metrics.vms)}

        # ── Cluster Operators Grid ────────────────────────────────────
        ops_sorted = sorted(im.cluster_operators, key=lambda o: (not o.degraded, not o.progressing, o.name))
        op_cells=""
        for op in ops_sorted:
            if op.degraded:     color,dot,bg="text-red-400","bg-red-500","border-red-500/30"
            elif op.progressing: color,dot,bg="text-amber-400","bg-amber-400","border-amber-500/30"
            elif op.available:  color,dot,bg="text-emerald-400","bg-emerald-500","border-slate-700/50"
            else:               color,dot,bg="text-slate-500","bg-slate-600","border-slate-700/50"
            tip=escape(op.message[:100]) if op.message else ""
            op_cells+=(f"<div class='bg-slate-800/60 rounded-lg p-3 border {bg} hover:bg-slate-700/40 transition-colors'"
                       f" title='{tip}'>"
                       f"<div class='flex items-center gap-2 mb-1'>"
                       f"<div class='w-2 h-2 rounded-full {dot} flex-shrink-0'></div>"
                       f"<span class='text-xs font-medium text-slate-200 truncate'>{escape(op.name)}</span></div>"
                       f"<p class='text-[10px] {color} pl-4'>{op.version}</p></div>")
        total_ops=len(im.cluster_operators)
        ok_ops=sum(1 for o in im.cluster_operators if o.available and not o.degraded)
        deg_ops=sum(1 for o in im.cluster_operators if o.degraded)
        prog_ops=sum(1 for o in im.cluster_operators if o.progressing and not o.degraded)

        cluster_operators_section=f"""
        <section class='mb-6'>
            <h2 class='text-lg font-bold text-white mb-3 flex items-center gap-2'>
                <div class='p-1.5 bg-green-500/20 rounded-lg border border-green-500/30'>
                    <svg class='w-4 h-4 text-green-400' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
                        <path stroke-linecap='round' stroke-linejoin='round' stroke-width='2'
                              d='M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15'/>
                    </svg>
                </div>Cluster Operators
                <div class='flex gap-2 ml-2'>
                    <span class='px-2 py-0.5 rounded-full text-[10px] font-bold bg-emerald-900/50 text-emerald-400 border border-emerald-700'>{ok_ops} OK</span>
                    {"" if deg_ops==0 else f"<span class='px-2 py-0.5 rounded-full text-[10px] font-bold bg-red-900/50 text-red-400 border border-red-700'>{deg_ops} Degraded</span>"}
                    {"" if prog_ops==0 else f"<span class='px-2 py-0.5 rounded-full text-[10px] font-bold bg-amber-900/50 text-amber-400 border border-amber-700'>{prog_ops} Progressing</span>"}
                </div>
            </h2>
            <div class='grid grid-cols-3 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-8 gap-2'>{op_cells}</div>
        </section>"""

        # ── MachineConfigPool ─────────────────────────────────────────
        mcp_cells=""
        for pool in im.mcp_pools:
            dg=pool.degraded_count>0
            all_ok=pool.ready_count==pool.machine_count and pool.machine_count>0
            border="border-red-500/30" if dg else ("border-emerald-500/30" if all_ok else "border-amber-500/30")
            status_text="Degraded" if dg else ("Ready" if all_ok else "Updating")
            status_color="text-red-400" if dg else ("text-emerald-400" if all_ok else "text-amber-400")
            paused_badge="<span class='ml-1 text-[10px] text-slate-500'>Paused</span>" if pool.paused else ""
            mcp_cells+=(f"<div class='bg-slate-800/60 rounded-xl p-4 border {border} shadow'>"
                        f"<div class='flex items-center justify-between mb-3'>"
                        f"<p class='text-sm font-semibold text-slate-200'>{escape(pool.name)}{paused_badge}</p>"
                        f"<span class='text-xs font-bold {status_color}'>{status_text}</span></div>"
                        f"<div class='grid grid-cols-2 gap-2 text-center'>"
                        f"<div class='bg-slate-900/50 rounded p-2'><p class='text-[10px] text-slate-500 mb-1'>Total</p><p class='text-lg font-bold text-slate-200'>{pool.machine_count}</p></div>"
                        f"<div class='bg-slate-900/50 rounded p-2'><p class='text-[10px] text-slate-500 mb-1'>Ready</p><p class='text-lg font-bold text-emerald-400'>{pool.ready_count}</p></div>"
                        f"<div class='bg-slate-900/50 rounded p-2'><p class='text-[10px] text-slate-500 mb-1'>Updated</p><p class='text-lg font-bold text-cyan-400'>{pool.updated_count}</p></div>"
                        f"<div class='bg-slate-900/50 rounded p-2'><p class='text-[10px] text-slate-500 mb-1'>Degraded</p><p class='text-lg font-bold {'text-red-400' if dg else 'text-slate-500'}'>{pool.degraded_count}</p></div>"
                        f"</div></div>")

        mcp_section=f"""
        <div class='mb-6'>
            <h3 class='text-base font-semibold text-slate-300 mb-3 flex items-center gap-2'>
                <svg class='w-4 h-4 text-orange-400' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
                    <path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z'/>
                    <path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M15 12a3 3 0 11-6 0 3 3 0 016 0z'/>
                </svg>MachineConfigPool
            </h3>
            <div class='grid grid-cols-2 md:grid-cols-4 gap-4'>{mcp_cells or "<p class='text-slate-600 text-sm'>데이터 없음</p>"}</div>
        </div>"""

        # ── 인프라 서비스 메트릭 패널 ─────────────────────────────────
        def fmt_rate(v: Optional[float]) -> str:
            return f"{v:.1f}/s" if v is not None else "N/A"
        def fmt_val(v: Optional[float], unit: str="") -> str:
            return f"{int(v)}{unit}" if v is not None else "N/A"

        router_trend_html=self._render_sparkline(im.router_req_trend,"#06b6d4","/s",320,52)
        router_5xx_html  =self._render_sparkline(im.router_5xx_trend,"#f87171","/s",320,52)
        sched_lat_html   =self._render_sparkline(im.sched_lat_trend, "#a78bfa","ms",320,52)
        reg_trend_html   =self._render_sparkline(im.reg_trend,       "#34d399","/s",320,52)

        ovn_nb_status="Leader" if im.ovn_nb_leader and im.ovn_nb_leader>=1 else ("N/A" if im.ovn_nb_leader is None else "No Leader")
        ovn_nb_color="text-emerald-400" if ovn_nb_status=="Leader" else ("text-slate-400" if ovn_nb_status=="N/A" else "text-red-400")

        infra_services_section=f"""
        <section class='mb-6'>
            <h2 class='text-lg font-bold text-white mb-4 flex items-center gap-2'>
                <div class='p-1.5 bg-rose-500/20 rounded-lg border border-rose-500/30'>
                    <svg class='w-4 h-4 text-rose-400' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
                        <path stroke-linecap='round' stroke-linejoin='round' stroke-width='2'
                              d='M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10'/>
                    </svg>
                </div>Infrastructure Services
            </h2>

            <div class='grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mb-4'>
                <!-- Router -->
                <div class='bg-slate-800/80 rounded-xl border border-slate-700/50 shadow-lg overflow-hidden'>
                    <div class='px-4 py-2.5 border-b border-slate-700/50 flex items-center gap-2 bg-cyan-900/20'>
                        <div class='w-2 h-2 rounded-full {"bg-emerald-500" if im.router_req_rate is not None else "bg-slate-600"}'></div>
                        <p class='text-xs font-bold text-slate-200 uppercase tracking-wider'>Ingress Router</p>
                    </div>
                    <div class='p-3 grid grid-cols-2 gap-2 mb-2'>
                        {self._render_stat_pill("Total Req/s", fmt_rate(im.router_req_rate), "text-cyan-400")}
                        {self._render_stat_pill("4xx/s", fmt_rate(im.router_4xx_rate), "text-amber-400")}
                        {self._render_stat_pill("5xx/s", fmt_rate(im.router_5xx_rate), "text-red-400")}
                        {self._render_stat_pill("Sessions", fmt_val(im.router_sessions), "text-blue-400")}
                    </div>
                    <div class='px-3 pb-3'>
                        <p class='text-[10px] text-slate-500 uppercase mb-1'>Request Rate (1h)</p>
                        {router_trend_html}
                    </div>
                </div>

                <!-- Scheduler -->
                <div class='bg-slate-800/80 rounded-xl border border-slate-700/50 shadow-lg overflow-hidden'>
                    <div class='px-4 py-2.5 border-b border-slate-700/50 flex items-center gap-2 bg-violet-900/20'>
                        <div class='w-2 h-2 rounded-full {"bg-emerald-500" if im.sched_pending is not None else "bg-slate-600"}'></div>
                        <p class='text-xs font-bold text-slate-200 uppercase tracking-wider'>Scheduler</p>
                    </div>
                    <div class='p-3 mb-2'>
                        <div class='bg-slate-900/60 rounded-lg p-4 text-center mb-3'>
                            <p class='text-[10px] text-slate-500 uppercase tracking-wider mb-1'>Pending Pods</p>
                            <p class='text-3xl font-bold {"text-red-400" if (im.sched_pending or 0)>10 else "text-emerald-400"}'>{fmt_val(im.sched_pending)}</p>
                        </div>
                    </div>
                    <div class='px-3 pb-3'>
                        <p class='text-[10px] text-slate-500 uppercase mb-1'>Scheduling p99 Latency (1h)</p>
                        {sched_lat_html}
                    </div>
                </div>

                <!-- OVN-Kubernetes -->
                <div class='bg-slate-800/80 rounded-xl border border-slate-700/50 shadow-lg overflow-hidden'>
                    <div class='px-4 py-2.5 border-b border-slate-700/50 flex items-center gap-2 bg-teal-900/20'>
                        <div class='w-2 h-2 rounded-full {"bg-emerald-500" if im.ovn_ports is not None else "bg-slate-600"}'></div>
                        <p class='text-xs font-bold text-slate-200 uppercase tracking-wider'>OVN-Kubernetes</p>
                    </div>
                    <div class='p-3 grid grid-cols-1 gap-3'>
                        <div class='bg-slate-900/60 rounded-lg p-4 text-center'>
                            <p class='text-[10px] text-slate-500 uppercase tracking-wider mb-1'>Logical Ports</p>
                            <p class='text-3xl font-bold text-teal-400'>{fmt_val(im.ovn_ports)}</p>
                        </div>
                        <div class='bg-slate-900/60 rounded-lg p-3 text-center'>
                            <p class='text-[10px] text-slate-500 uppercase tracking-wider mb-1'>NB DB Leader</p>
                            <p class='text-lg font-bold {ovn_nb_color}'>{ovn_nb_status}</p>
                        </div>
                    </div>
                </div>

                <!-- Image Registry -->
                <div class='bg-slate-800/80 rounded-xl border border-slate-700/50 shadow-lg overflow-hidden'>
                    <div class='px-4 py-2.5 border-b border-slate-700/50 flex items-center gap-2 bg-emerald-900/20'>
                        <div class='w-2 h-2 rounded-full {"bg-emerald-500" if im.reg_req_rate is not None else "bg-slate-600"}'></div>
                        <p class='text-xs font-bold text-slate-200 uppercase tracking-wider'>Image Registry</p>
                    </div>
                    <div class='p-3 mb-2'>
                        <div class='bg-slate-900/60 rounded-lg p-4 text-center mb-3'>
                            <p class='text-[10px] text-slate-500 uppercase tracking-wider mb-1'>Request Rate</p>
                            <p class='text-3xl font-bold text-emerald-400'>{fmt_rate(im.reg_req_rate)}</p>
                        </div>
                    </div>
                    <div class='px-3 pb-3'>
                        <p class='text-[10px] text-slate-500 uppercase mb-1'>Registry Request Rate (1h)</p>
                        {reg_trend_html}
                    </div>
                </div>
            </div>

            <!-- Router 5xx 추이 + MCP -->
            <div class='grid grid-cols-1 md:grid-cols-2 gap-4'>
                <div class='bg-slate-800/80 rounded-xl border border-slate-700/50 shadow-lg p-4'>
                    <p class='text-xs text-slate-400 uppercase tracking-wider font-semibold mb-2'>Router 5xx Error Rate (1h)</p>
                    {router_5xx_html}
                </div>
                <div class='bg-slate-800/80 rounded-xl border border-slate-700/50 shadow-lg p-4'>
                    {mcp_section}
                </div>
            </div>
        </section>"""

        # ── Top 5 패널 ───────────────────────────────────────────────
        cpu5,mem5,st5=self._compute_top5_vms()
        def top5_block(title,icon_color,items_html):
            return (f"<div class='bg-slate-800/80 rounded-xl border border-slate-700/50 shadow-lg overflow-hidden'>"
                    f"<div class='px-4 py-3 border-b border-slate-700/50 flex items-center gap-2'>"
                    f"<div class='w-2 h-2 rounded-full {icon_color}'></div>"
                    f"<h3 class='text-sm font-bold text-slate-200'>{title}</h3></div>"
                    f"<div class='p-3 space-y-1.5'>{items_html}</div></div>")

        cpu_items="".join(self._render_top5_card(i+1,v.name,v.namespace,self._format_pct(v.cpu_usage_pct),v.cpu_usage_pct,vm_idx_map.get((v.name,v.namespace),0)) for i,v in enumerate(cpu5)) or "<p class='text-xs text-slate-600 text-center py-2'>데이터 없음</p>"
        mem_items="".join(self._render_top5_card(i+1,v.name,v.namespace,self._format_pct(v.memory_usage_pct),v.memory_usage_pct,vm_idx_map.get((v.name,v.namespace),0)) for i,v in enumerate(mem5)) or "<p class='text-xs text-slate-600 text-center py-2'>데이터 없음</p>"
        st_items="".join(self._render_top5_card(i+1,vm.name,vm.namespace,self._format_bytes(u),min(u/max(self.metrics.total_pv_capacity_bytes,1)*100*len(self.metrics.vms),100),vm_idx_map.get((vm.name,vm.namespace),0)) for i,(u,vm) in enumerate(st5)) or "<p class='text-xs text-slate-600 text-center py-2'>데이터 없음</p>"
        top5_section=f"""
        <section class='mb-6'>
            <h2 class='text-lg font-bold text-white mb-3 flex items-center gap-2'>
                <div class='p-1.5 bg-amber-500/20 rounded-lg border border-amber-500/30'>
                    <svg class='w-4 h-4 text-amber-400' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
                        <path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M13 7h8m0 0v8m0-8l-8 8-4-4-6 6'/>
                    </svg>
                </div>Top 5 Resource Usage VM
                <span class='text-xs text-slate-500 font-normal'>클릭 → VM 상세</span>
            </h2>
            <div class='grid grid-cols-1 md:grid-cols-3 gap-4'>
                {top5_block("🔥 Top 5 CPU","bg-cyan-500",cpu_items)}
                {top5_block("💾 Top 5 Memory","bg-purple-500",mem_items)}
                {top5_block("🗄️ Top 5 Storage","bg-emerald-500",st_items)}
            </div>
        </section>"""

        # ── ETCD / API 추이 ──────────────────────────────────────────
        tr=f"{pt.time_labels[0]} ~ {pt.time_labels[-1]}" if len(pt.time_labels)>=2 else "1h"
        trend_section=f"""
        <section class='mb-6'>
            <h2 class='text-lg font-bold text-white mb-3 flex items-center gap-2'>
                <div class='p-1.5 bg-violet-500/20 rounded-lg border border-violet-500/30'>
                    <svg class='w-4 h-4 text-violet-400' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
                        <path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z'/>
                    </svg>
                </div>ETCD · API Server 성능 추이
                <span class='text-xs text-slate-500 font-normal ml-1'>({tr} · 5m avg)</span>
            </h2>
            <div class='grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4'>
                <div class='bg-slate-800/80 rounded-xl border border-slate-700/50 shadow-lg p-4'>
                    <p class='text-xs text-slate-400 uppercase font-semibold mb-2'>ETCD WAL Fsync p99</p>
                    {self._render_sparkline(pt.etcd_wal_p99,"#f59e0b","ms",260,48)}
                </div>
                <div class='bg-slate-800/80 rounded-xl border border-slate-700/50 shadow-lg p-4'>
                    <p class='text-xs text-slate-400 uppercase font-semibold mb-2'>ETCD Peer RTT p99</p>
                    {self._render_sparkline(pt.etcd_peer_rtt,"#a78bfa","ms",260,48)}
                </div>
                <div class='bg-slate-800/80 rounded-xl border border-slate-700/50 shadow-lg p-4'>
                    <p class='text-xs text-slate-400 uppercase font-semibold mb-2'>API Server 요청률</p>
                    {self._render_sparkline(pt.api_req_rate,"#06b6d4","/s",260,48)}
                </div>
                <div class='bg-slate-800/80 rounded-xl border border-slate-700/50 shadow-lg p-4'>
                    <p class='text-xs text-slate-400 uppercase font-semibold mb-2'>API Server 에러율 (5xx)</p>
                    {self._render_sparkline(pt.api_err_rate,"#f87171","/s",260,48)}
                </div>
                <div class='bg-slate-800/80 rounded-xl border border-slate-700/50 shadow-lg p-4 md:col-span-2 xl:col-span-2'>
                    <p class='text-xs text-slate-400 uppercase font-semibold mb-2'>API Server p99 Latency</p>
                    {self._render_sparkline(pt.api_latency_p99,"#34d399","ms",560,48)}
                </div>
            </div>
        </section>"""

        # ── 컨트롤 플레인 헬스 ───────────────────────────────────────
        ICON={"api":"M5 12h14M12 5l7 7-7 7","etcd":"M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7",
              "dns":"M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3",
              "leader":"M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"}
        jbadges=" ".join(f"<span class='px-1.5 py-0.5 rounded text-[10px] bg-slate-700 text-slate-400 border border-slate-600'>{k}={v}</span>" for k,v in h.matched_jobs.items())
        health_cards=(f"<div class='grid grid-cols-2 md:grid-cols-4 gap-3 mb-3'>"
                      f"{self._render_health_card('API Server',h.api_server,ICON['api'])}"
                      f"{self._render_health_card('ETCD',h.etcd,ICON['etcd'])}"
                      f"{self._render_health_card('CoreDNS',h.coredns,ICON['dns'])}"
                      f"{self._render_health_card('ETCD Leader',h.etcd_leader,ICON['leader'])}"
                      f"</div><div class='flex flex-wrap gap-1 mb-3'>{jbadges}</div>")
        fa=h.firing_alerts
        fa_c="text-emerald-400" if fa==0 else ("text-amber-400" if fa<5 else "text-red-400")
        fa_b="border-emerald-500/40" if fa==0 else ("border-amber-500/40" if fa<5 else "border-red-500/40")
        firing_card=(f"<div class='bg-slate-800/80 rounded-xl p-4 border {fa_b} shadow-lg flex items-center gap-3 mb-4'>"
                     f"<div class='p-2 rounded-lg bg-slate-900/50'><svg class='w-5 h-5 {fa_c}' fill='none' stroke='currentColor' viewBox='0 0 24 24'>"
                     f"<path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9'/></svg></div>"
                     f"<div><p class='text-xs text-slate-400 uppercase tracking-wider mb-0.5'>Firing Alerts</p>"
                     f"<p class='text-2xl font-bold {fa_c}'>{fa}<span class='text-xs text-slate-500 ml-1'>건</span></p></div></div>")
        cluster_bars=(f"<div class='grid grid-cols-1 md:grid-cols-2 gap-4 mb-6 bg-slate-800/50 rounded-xl p-4 border border-slate-700/50'>"
                      f"<div><p class='text-xs text-slate-400 uppercase mb-2 font-semibold'>클러스터 CPU</p>{self._render_usage_bar(h.cluster_cpu_pct,'Cluster CPU')}</div>"
                      f"<div><p class='text-xs text-slate-400 uppercase mb-2 font-semibold'>클러스터 Memory</p>{self._render_usage_bar(h.cluster_memory_pct,'Cluster Memory')}</div></div>")

        vm_stat_cards=(f"<div class='grid grid-cols-2 md:grid-cols-4 gap-4 mb-6'>"
                       f"<div class='bg-slate-800/80 rounded-xl p-5 border border-emerald-500/30 shadow-lg'><p class='text-xs text-slate-400 uppercase mb-1'>Running</p><p class='text-4xl font-bold text-emerald-400'>{self.metrics.vm_running_count}</p></div>"
                       f"<div class='bg-slate-800/80 rounded-xl p-5 border border-amber-500/30 shadow-lg'><p class='text-xs text-slate-400 uppercase mb-1'>Provisioning</p><p class='text-4xl font-bold text-amber-400'>{self.metrics.vm_provisioning_count}</p></div>"
                       f"<div class='bg-slate-800/80 rounded-xl p-5 border border-slate-600/50 shadow-lg'><p class='text-xs text-slate-400 uppercase mb-1'>Stopped</p><p class='text-4xl font-bold text-slate-400'>{self.metrics.vm_stopped_count}</p></div>"
                       f"<div class='bg-slate-800/80 rounded-xl p-5 border border-red-500/30 shadow-lg'><p class='text-xs text-slate-400 uppercase mb-1'>Failed</p><p class='text-4xl font-bold text-red-400'>{self.metrics.vm_failed_count}</p></div></div>")

        # 노드 rows
        node_rows=""
        for n in self.metrics.nodes:
            sc="text-green-400" if "Ready" in n.status else "text-red-400"
            cpu_rt=(f"<br><span class='text-[10px] {self._pct_bar_color(n.cpu_pct_realtime).replace('bg-','text-')}'>{self._format_pct(n.cpu_pct_realtime)}</span>" if n.cpu_pct_realtime is not None else "")
            mem_rt=(f"<br><span class='text-[10px] {self._pct_bar_color(n.memory_pct_realtime).replace('bg-','text-')}'>{self._format_pct(n.memory_pct_realtime)}</span>" if n.memory_pct_realtime is not None else "")
            node_rows+=(f"<tr class='border-b border-slate-700/50 hover:bg-slate-700/30'>"
                        f"<td class='p-3 text-slate-200 font-medium text-sm'>{escape(n.name)}</td>"
                        f"<td class='p-3 text-slate-300 text-sm'>{escape(n.roles)}</td>"
                        f"<td class='p-3 font-semibold {sc} text-sm'>{escape(n.status)}</td>"
                        f"<td class='p-3 text-cyan-400 text-sm'>{escape(n.cpu_usage)}{cpu_rt}</td>"
                        f"<td class='p-3 text-purple-400 text-sm'>{escape(n.memory_usage)}{mem_rt}</td>"
                        f"<td class='p-3 text-slate-400 text-xs'>{escape(n.age)}</td></tr>")

        # VM 사이드바 (네임스페이스 그룹화)
        ns_groups:Dict[str,List[Tuple[int,"VMMetrics"]]]= {}
        for idx,vm in enumerate(self.metrics.vms): ns_groups.setdefault(vm.namespace,[]).append((idx,vm))
        sidebar_html=""
        for ns_name,vm_list in sorted(ns_groups.items()):
            ns_id=re.sub(r'[^a-zA-Z0-9]','-',ns_name)
            run_c=sum(1 for _,v in vm_list if v.status_group=="running")
            fail_c=sum(1 for _,v in vm_list if v.status_group=="failed")
            badge="bg-red-500" if fail_c>0 else ("bg-emerald-500" if run_c>0 else "bg-slate-600")
            sidebar_html+=(f"<div class='border-b border-slate-700/50'>"
                           f"<button onclick=\"toggleNS('{ns_id}')\" class='w-full flex items-center justify-between px-4 py-2.5 hover:bg-slate-700/40 transition-colors group'>"
                           f"<div class='flex items-center gap-2 min-w-0'>"
                           f"<svg id='arrow-{ns_id}' class='w-3.5 h-3.5 text-slate-500 flex-shrink-0 transition-transform duration-200' fill='none' stroke='currentColor' viewBox='0 0 24 24'>"
                           f"<path stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M9 5l7 7-7 7'/></svg>"
                           f"<span class='text-xs font-semibold text-slate-300 truncate group-hover:text-white'>{escape(ns_name)}</span></div>"
                           f"<span class='px-1.5 py-0.5 rounded-full text-[10px] font-bold {badge} text-white flex-shrink-0 ml-2'>{len(vm_list)}</span></button>"
                           f"<div id='ns-{ns_id}' class='hidden'>")
            for idx,vm in vm_list:
                st=STATUS_STYLE.get(vm.status_group,STATUS_STYLE["unknown"])
                sidebar_html+=(f"<button onclick=\"openTab('vm-{idx}')\" class='tab-button w-full text-left pl-8 pr-4 py-3 bg-slate-800/50 border-t border-slate-700/30 hover:bg-slate-700 transition-all flex items-center gap-2'>"
                               f"<div class='w-2 h-2 rounded-full {st['dot']} flex-shrink-0'></div>"
                               f"<div class='min-w-0'><div class='text-xs font-medium text-slate-300 truncate'>{escape(vm.name)}</div>"
                               f"<div class='text-[10px] {st['color']}'>{escape(st['text'])}</div></div></button>")
            sidebar_html+="</div></div>"

        # VM 상세 콘텐츠
        DISK_BADGE={"root":"bg-indigo-900/50 text-indigo-300 border-indigo-700",
                    "data":"bg-cyan-900/50 text-cyan-300 border-cyan-700",
                    "etc":"bg-slate-700/50 text-slate-400 border-slate-600"}
        vm_contents=""
        for idx,vm in enumerate(self.metrics.vms):
            st=STATUS_STYLE.get(vm.status_group,STATUS_STYLE["unknown"])
            vol_rows=""
            for v in vm.volumes:
                dt=v.get("disk_type","etc"); bk=DISK_BADGE.get(dt,DISK_BADGE["etc"])
                ds=self.metrics.pvc_disk_stats.get(f"{vm.namespace}/{v['pvc']}",{})
                di=""
                if ds and ds.get("capacity",0)>0:
                    dp=ds["used"]/ds["capacity"]*100; dc=self._pct_bar_color(dp).replace("bg-","text-")
                    di=f"<span class='ml-2 text-[10px] {dc}'>{self._format_bytes(ds['used'])}/{self._format_bytes(ds['capacity'])} ({dp:.0f}%)</span>"
                vol_rows+=(f"<li class='text-sm text-slate-300 flex items-center gap-2 py-1 flex-wrap'>"
                           f"<span class='px-1.5 py-0.5 rounded text-[10px] font-bold border {bk} uppercase'>{dt}</span>"
                           f"<span class='text-blue-400'>{escape(v['pvc'])}</span>{di}</li>")
            if not vol_rows: vol_rows="<li class='text-sm text-slate-500 py-1'>볼륨 없음</li>"
            cpu_b=""
            if vm.cpu_usage_pct is not None:
                cc=self._pct_bar_color(vm.cpu_usage_pct).replace("bg-","text-")
                cpu_b=f"<span class='text-xs {cc} ml-1'>({self._format_pct(vm.cpu_usage_pct)})</span>"
            rt=(f"<div class='bg-slate-900/50 p-4 rounded-lg border border-slate-700/50 mb-3'>"
                f"<p class='text-xs text-slate-500 mb-3 uppercase font-semibold flex items-center gap-2'>"
                f"<span class='w-1.5 h-1.5 rounded-full bg-cyan-400 animate-pulse'></span>실시간 (5m avg)</p>"
                f"<div class='space-y-3 mb-3'>{self._render_usage_bar(vm.cpu_usage_pct,'CPU')}{self._render_usage_bar(vm.memory_usage_pct,f'Memory ({self._format_bytes(vm.memory_used_bytes)})')}</div>"
                f"<div class='grid grid-cols-2 gap-2'>"
                f"<div class='bg-slate-800/60 p-2 rounded-lg border border-slate-700/50'><p class='text-[10px] text-slate-500 mb-1'>Net RX</p><p class='text-sm font-semibold text-blue-400'>{escape(self._format_bps(vm.net_rx_bps))}</p></div>"
                f"<div class='bg-slate-800/60 p-2 rounded-lg border border-slate-700/50'><p class='text-[10px] text-slate-500 mb-1'>Net TX</p><p class='text-sm font-semibold text-pink-400'>{escape(self._format_bps(vm.net_tx_bps))}</p></div>"
                f"<div class='bg-slate-800/60 p-2 rounded-lg border border-slate-700/50'><p class='text-[10px] text-slate-500 mb-1'>Disk R</p><p class='text-sm font-semibold text-emerald-400'>{escape(self._format_bps(vm.disk_read_bps))}</p></div>"
                f"<div class='bg-slate-800/60 p-2 rounded-lg border border-slate-700/50'><p class='text-[10px] text-slate-500 mb-1'>Disk W</p><p class='text-sm font-semibold text-orange-400'>{escape(self._format_bps(vm.disk_write_bps))}</p></div>"
                f"</div></div>")
            vm_contents+=(f"<div id='vm-{idx}' class='tab-content hidden animate-fade-in'>"
                          f"<div class='grid grid-cols-1 lg:grid-cols-3 gap-5'>"
                          f"<div class='bg-slate-800/80 rounded-xl p-5 border border-slate-700/50 shadow-lg lg:col-span-2'>"
                          f"<div class='flex justify-between items-start mb-4'>"
                          f"<div><h2 class='text-xl font-bold text-white mb-1'>{escape(vm.name)}</h2>"
                          f"<span class='px-2 py-0.5 rounded-full bg-slate-700 text-xs text-slate-300 border border-slate-600'>NS: {escape(vm.namespace)}</span></div>"
                          f"<span class='font-bold text-lg {st['color']}'>{escape(st['text'])}</span></div>"
                          f"<div class='grid grid-cols-2 md:grid-cols-4 gap-2 mb-3'>"
                          f"<div class='bg-slate-900/50 p-3 rounded-lg border border-slate-700/50'><p class='text-[10px] text-slate-500 mb-1'>CPU</p><p class='text-base font-semibold text-cyan-400'>{escape(vm.cpu_cores)} cores{cpu_b}</p></div>"
                          f"<div class='bg-slate-900/50 p-3 rounded-lg border border-slate-700/50'><p class='text-[10px] text-slate-500 mb-1'>Memory</p><p class='text-base font-semibold text-purple-400'>{escape(vm.memory_total)}</p></div>"
                          f"<div class='bg-slate-900/50 p-3 rounded-lg border border-slate-700/50'><p class='text-[10px] text-slate-500 mb-1'>IP</p><p class='text-sm font-medium text-emerald-400'>{escape(vm.ip_address)}</p></div>"
                          f"<div class='bg-slate-900/50 p-3 rounded-lg border border-slate-700/50'><p class='text-[10px] text-slate-500 mb-1'>Node</p><p class='text-xs text-slate-300 truncate' title='{escape(vm.node)}'>{escape(vm.node)}</p></div></div>"
                          f"<div class='bg-slate-900/50 p-3 rounded-lg border border-slate-700/50 mb-3'><p class='text-[10px] text-slate-500 mb-1'>OS</p><p class='text-sm text-slate-300'>{escape(vm.os_info)}</p></div>"
                          f"{rt}</div>"
                          f"<div class='bg-slate-800/80 rounded-xl p-5 border border-slate-700/50 shadow-lg'>"
                          f"<h3 class='text-base font-semibold text-slate-200 mb-3'>Storage Volumes</h3>"
                          f"<ul class='space-y-1 bg-slate-900/50 p-3 rounded-lg border border-slate-700/50'>{vol_rows}</ul>"
                          f"<div class='mt-3 pt-3 border-t border-slate-700/50'><p class='text-[10px] text-slate-500'>Created {escape(vm.creation_time)} ago</p></div>"
                          f"</div></div></div>")

        # Storage rows
        pool_rows=""
        for pool in self.metrics.storage_pools.values():
            ap=(pool.used_capacity_bytes/pool.total_capacity_bytes*100) if pool.total_capacity_bytes>0 else 0; ac=self._pct_bar_color(ap)
            disk_cell="<span class='text-xs text-slate-600'>N/A</span>"
            if pool.disk_capacity_bytes>0:
                dp=pool.disk_used_bytes/pool.disk_capacity_bytes*100; dc=self._pct_bar_color(dp)
                disk_cell=(f"<div class='flex items-center gap-2'><div class='w-full bg-slate-800 rounded-full h-2 border border-slate-700 overflow-hidden'><div class='{dc} h-2 rounded-full' style='width:{dp:.1f}%'></div></div>"
                           f"<span class='text-xs text-slate-400 whitespace-nowrap'>{self._format_bytes(pool.disk_used_bytes)}/{self._format_bytes(pool.disk_capacity_bytes)} ({dp:.1f}%)</span></div>")
            pool_rows+=(f"<tr class='border-b border-slate-700/50 hover:bg-slate-700/30'>"
                        f"<td class='p-3 font-medium text-indigo-300 text-sm'>{escape(pool.name)}</td>"
                        f"<td class='p-3 text-slate-400 text-xs'>{escape(pool.provisioner)}</td>"
                        f"<td class='p-3 text-center text-slate-300'>{pool.pv_count}</td>"
                        f"<td class='p-3 text-center text-slate-300'>{pool.pvc_count}</td>"
                        f"<td class='p-3 text-right text-cyan-400 font-semibold text-sm'>{self._format_bytes(pool.total_capacity_bytes)}</td>"
                        f"<td class='p-3'><div class='flex items-center gap-2'><div class='w-full bg-slate-800 rounded-full h-2 border border-slate-700 overflow-hidden'><div class='{ac} h-2 rounded-full' style='width:{ap:.1f}%'></div></div><span class='text-xs text-slate-400 w-10 text-right'>{ap:.1f}%</span></div></td>"
                        f"<td class='p-3'>{disk_cell}</td></tr>")
        if not pool_rows: pool_rows="<tr><td colspan='7' class='p-6 text-center text-slate-500'>데이터 없음</td></tr>"

        pvc_rows=""
        for pvc in self.metrics.pvc_data:
            sc="text-emerald-400" if pvc["Status"]=="Bound" else "text-amber-400"
            ds=self.metrics.pvc_disk_stats.get(f"{pvc['Namespace']}/{pvc['Name']}",{})
            di=""
            if ds and ds.get("capacity",0)>0:
                dp=ds["used"]/ds["capacity"]*100; dc=self._pct_bar_color(dp).replace("bg-","text-")
                di=f"<span class='ml-1 text-[10px] {dc}'>{self._format_bytes(ds['used'])}/{self._format_bytes(ds['capacity'])} ({dp:.0f}%)</span>"
            pvc_rows+=(f"<tr class='border-b border-slate-700/50 hover:bg-slate-700/30'>"
                       f"<td class='p-3 text-slate-200 text-sm'>{escape(pvc['Name'])}</td>"
                       f"<td class='p-3 text-slate-400 text-xs'>{escape(pvc['Namespace'])}</td>"
                       f"<td class='p-3 font-semibold {sc} text-sm'>{escape(pvc['Status'])}</td>"
                       f"<td class='p-3 text-cyan-400 text-right text-sm'>{escape(pvc['Requested Capacity'])}{di}</td>"
                       f"<td class='p-3 text-indigo-300 text-xs'>{escape(pvc['StorageClass'])}</td>"
                       f"<td class='p-3 text-slate-400 text-xs truncate max-w-xs'>{escape(pvc['Volume Name'])}</td></tr>")
        if not pvc_rows: pvc_rows="<tr><td colspan='6' class='p-6 text-center text-slate-500'>데이터 없음</td></tr>"

        pv_rows=""
        for pv in self.metrics.pv_data:
            sc="text-emerald-400" if pv.get("Status")=="Bound" else "text-slate-400"
            pv_rows+=(f"<tr class='border-b border-slate-700/50 hover:bg-slate-700/30'>"
                      f"<td class='p-3 text-slate-200 text-xs truncate max-w-xs'>{escape(pv.get('Name',''))}</td>"
                      f"<td class='p-3 text-cyan-400 text-sm'>{escape(pv.get('Capacity',''))}</td>"
                      f"<td class='p-3 font-semibold {sc} text-sm'>{escape(pv.get('Status',''))}</td>"
                      f"<td class='p-3 text-indigo-300 text-xs'>{escape(pv.get('StorageClass',''))}</td>"
                      f"<td class='p-3 text-slate-400 text-xs'>{escape(pv.get('Claim',''))}</td>"
                      f"<td class='p-3 text-slate-500 text-xs'>{escape(pv.get('Age',''))}</td></tr>")
        if not pv_rows: pv_rows="<tr><td colspan='6' class='p-10 text-center text-slate-500'>데이터 없음</td></tr>"

        # ── HTML 최종 조립 ───────────────────────────────────────────
        html=f"""<!DOCTYPE html>
<html lang="ko" class="dark h-full">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AIBox Observability Hub</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>tailwind.config={{darkMode:'class',theme:{{extend:{{
    colors:{{slate:{{850:'#151e2e'}}}},
    animation:{{'fade-in':'fadeIn 0.25s ease-out'}},
    keyframes:{{fadeIn:{{'0%':{{opacity:'0',transform:'translateY(4px)'}},'100%':{{opacity:'1',transform:'translateY(0)'}}}}}}
}}}}}}</script>
<style>
html,body{{height:100%;margin:0;}}
body{{background:#0b1120;color:#e2e8f0;display:flex;flex-direction:column;font-family:ui-sans-serif,system-ui,sans-serif;}}
.glass{{background:rgba(15,23,42,0.7);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1px solid rgba(255,255,255,0.06);}}
.scroll::-webkit-scrollbar{{width:5px;height:5px;}}
.scroll::-webkit-scrollbar-track{{background:#0f172a;}}
.scroll::-webkit-scrollbar-thumb{{background:#334155;border-radius:3px;}}
.scroll::-webkit-scrollbar-thumb:hover{{background:#475569;}}
.tab-button.active{{background-color:#1e293b;border-left:3px solid #06b6d4;}}
#layout{{flex:1;display:flex;min-height:0;overflow:hidden;}}
nav.sidebar{{width:17rem;flex-shrink:0;overflow-y:auto;}}
main.content{{flex:1;overflow-y:auto;}}
</style>
<script>
function openTab(tabId){{
    document.querySelectorAll('.tab-content').forEach(el=>el.classList.add('hidden'));
    document.querySelectorAll('.tab-button').forEach(el=>el.classList.remove('active'));
    const el=document.getElementById(tabId);
    if(el) el.classList.remove('hidden');
    if(event&&event.currentTarget) event.currentTarget.classList.add('active');
}}
function toggleNS(nsId){{
    const el=document.getElementById('ns-'+nsId); const arrow=document.getElementById('arrow-'+nsId);
    if(el) el.classList.toggle('hidden');
    if(arrow) arrow.style.transform=el&&!el.classList.contains('hidden')?'rotate(90deg)':'';
}}
window.onload=()=>{{
    document.getElementById('dashboard-global').classList.remove('hidden');
    document.querySelector('[onclick*="dashboard-global"]')?.classList.add('active');
    const firstArrow=document.querySelector('[id^="arrow-"]');
    if(firstArrow) toggleNS(firstArrow.id.replace('arrow-',''));
}};
</script>
</head>
<body>
<header class="glass border-b border-slate-700/50 flex-shrink-0 z-50">
    <div class="px-5 h-13 flex items-center justify-between py-2">
        <div class="flex items-center gap-3">
            <a href="/AIBox/" class="w-8 h-8 rounded-lg bg-slate-800 border border-slate-600 hover:border-cyan-500 flex items-center justify-center transition-all group">
                <svg class="w-4 h-4 text-slate-400 group-hover:text-cyan-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6"/></svg>
            </a>
            <div class="flex items-center gap-2 border-l border-slate-700 pl-3">
                <div class="w-7 h-7 rounded-lg bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center">
                    <svg class="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 3H5a2 2 0 00-2 2v4m6-6h10a2 2 0 012 2v4M9 3v18"/></svg>
                </div>
                <div>
                    <h1 class="text-base font-bold bg-clip-text text-transparent bg-gradient-to-r from-cyan-400 to-blue-400">AIBox Observability</h1>
                    <p class="text-[9px] text-slate-500 uppercase tracking-widest">Ultimate v4 · OCP {escape(self.metrics.ocp_version)}</p>
                </div>
            </div>
        </div>
        <div class="flex items-center gap-3">
            <div class="hidden md:flex items-center gap-2 bg-slate-900/80 px-3 py-1 rounded-full border border-slate-700">
                <span class="w-1.5 h-1.5 rounded-full bg-purple-500 animate-pulse"></span>
                <span class="text-xs text-slate-400">Memory:</span>
                <span class="text-xs font-bold text-purple-400">{self.metrics.global_memory_total}</span>
            </div>
            <div class="text-right">
                <p class="text-[9px] text-slate-500 uppercase tracking-wider">Last Sync</p>
                <p class="text-xs font-bold text-emerald-400 font-mono">{self.metrics.last_updated}</p>
            </div>
        </div>
    </div>
</header>
<div id="layout">
    <nav class="sidebar glass border-r border-slate-700/50 scroll flex flex-col">
        <div class="p-3 border-b border-slate-700/50 flex-shrink-0">
            <button onclick="openTab('dashboard-global')" class="w-full flex items-center gap-2 px-3 py-2.5 bg-gradient-to-r from-cyan-900/50 to-blue-900/50 border border-cyan-500/30 rounded-lg hover:from-cyan-800/60 transition-all">
                <svg class="w-4 h-4 text-cyan-400 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z"/></svg>
                <span class="text-sm font-semibold text-cyan-100">Global Dashboard</span>
            </button>
        </div>
        <div class="px-3 pt-2.5 pb-1 flex-shrink-0"><p class="text-[9px] font-bold text-slate-500 uppercase tracking-widest">Virtual Machines</p></div>
        <div class="flex-1 overflow-y-auto scroll">{sidebar_html}</div>
    </nav>
    <main class="content scroll p-5 lg:p-7">
        <div id="dashboard-global" class="tab-content hidden animate-fade-in max-w-[1400px] mx-auto">
            <!-- 1. Control Plane -->
            <section class="mb-6">
                <h2 class="text-lg font-bold text-white mb-3 flex items-center gap-2">
                    <div class="p-1.5 bg-blue-500/20 rounded-lg border border-blue-500/30"><svg class="w-4 h-4 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/></svg></div>
                    컨트롤 플레인 상태
                </h2>
                {health_cards}{firing_card}{cluster_bars}
            </section>
            <!-- 2. Cluster Operators -->
            {cluster_operators_section}
            <!-- 3. Infrastructure Services -->
            {infra_services_section}
            <!-- 4. Top 5 VMs -->
            {top5_section}
            <!-- 5. Performance Trends -->
            {trend_section}
            <!-- 6. VM Status -->
            <section class="mb-6">
                <h2 class="text-lg font-bold text-white mb-3 flex items-center gap-2">
                    <div class="p-1.5 bg-cyan-500/20 rounded-lg border border-cyan-500/30"><svg class="w-4 h-4 text-cyan-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 3H5a2 2 0 00-2 2v4m6-6h10a2 2 0 012 2v4M9 3v18"/></svg></div>
                    VM 상태 현황
                </h2>
                {vm_stat_cards}
            </section>
            <!-- 7. Nodes -->
            <section class="mb-8">
                <h2 class="text-lg font-bold text-white mb-3 flex items-center gap-2">
                    <div class="p-1.5 bg-indigo-500/20 rounded-lg border border-indigo-500/30"><svg class="w-4 h-4 text-indigo-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2"/></svg></div>
                    Cluster Nodes
                </h2>
                <div class="glass rounded-xl overflow-hidden border border-slate-700/50">
                    <table class="w-full text-left border-collapse">
                        <thead><tr class="bg-slate-800/80 border-b border-slate-700">
                            <th class="p-3 text-xs font-bold text-slate-400 uppercase">Node</th><th class="p-3 text-xs font-bold text-slate-400 uppercase">Role</th>
                            <th class="p-3 text-xs font-bold text-slate-400 uppercase">Status</th><th class="p-3 text-xs font-bold text-slate-400 uppercase">CPU</th>
                            <th class="p-3 text-xs font-bold text-slate-400 uppercase">Memory</th><th class="p-3 text-xs font-bold text-slate-400 uppercase">Age</th>
                        </tr></thead><tbody>{node_rows}</tbody>
                    </table>
                </div>
            </section>
            <!-- 8. Storage -->
            <section class="mb-8">
                <h2 class="text-lg font-bold text-white mb-3 flex items-center gap-2">
                    <div class="p-1.5 bg-emerald-500/20 rounded-lg border border-emerald-500/30"><svg class="w-4 h-4 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7"/></svg></div>
                    Storage Deep-Dive
                </h2>
                <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
                    <div class="bg-slate-800/80 rounded-xl p-4 border border-slate-700/50"><p class="text-xs text-slate-400 mb-1 uppercase font-semibold">Total PV</p><p class="text-2xl font-bold text-cyan-400">{self._format_bytes(self.metrics.total_pv_capacity_bytes)}</p></div>
                    <div class="bg-slate-800/80 rounded-xl p-4 border border-slate-700/50"><p class="text-xs text-slate-400 mb-1 uppercase font-semibold">Total PVC</p><p class="text-2xl font-bold text-indigo-400">{self._format_bytes(self.metrics.total_pvc_requested_bytes)}</p></div>
                    <div class="bg-slate-800/80 rounded-xl p-4 border border-slate-700/50"><p class="text-xs text-slate-400 mb-1 uppercase font-semibold">PV Count</p><p class="text-2xl font-bold text-emerald-400">{len(self.metrics.pv_data)}</p></div>
                    <div class="bg-slate-800/80 rounded-xl p-4 border border-slate-700/50"><p class="text-xs text-slate-400 mb-1 uppercase font-semibold">PVC Count</p><p class="text-2xl font-bold text-purple-400">{len(self.metrics.pvc_data)}</p></div>
                </div>
                <h3 class="text-sm font-semibold text-slate-300 mb-2">StorageClass Pools</h3>
                <div class="glass rounded-xl overflow-hidden border border-slate-700/50 mb-4">
                    <table class="w-full text-left border-collapse">
                        <thead><tr class="bg-slate-800/80 border-b border-slate-700">
                            <th class="p-3 text-xs font-bold text-slate-400 uppercase">StorageClass</th><th class="p-3 text-xs font-bold text-slate-400 uppercase">Provisioner</th>
                            <th class="p-3 text-xs font-bold text-slate-400 uppercase text-center">PV</th><th class="p-3 text-xs font-bold text-slate-400 uppercase text-center">PVC</th>
                            <th class="p-3 text-xs font-bold text-slate-400 uppercase text-right">Provisioned</th>
                            <th class="p-3 text-xs font-bold text-slate-400 uppercase">Bound 할당률</th><th class="p-3 text-xs font-bold text-slate-400 uppercase">실제 사용량</th>
                        </tr></thead><tbody>{pool_rows}</tbody>
                    </table>
                </div>
                <h3 class="text-sm font-semibold text-slate-300 mb-2">PVC Deep-Dive</h3>
                <div class="glass rounded-xl overflow-hidden border border-slate-700/50 mb-4 max-h-72 overflow-y-auto scroll">
                    <table class="w-full text-left border-collapse">
                        <thead class="sticky top-0 z-10"><tr class="bg-slate-800 border-b border-slate-700">
                            <th class="p-3 text-xs font-bold text-slate-400 uppercase">PVC</th><th class="p-3 text-xs font-bold text-slate-400 uppercase">NS</th>
                            <th class="p-3 text-xs font-bold text-slate-400 uppercase">Status</th><th class="p-3 text-xs font-bold text-slate-400 uppercase text-right">요청/실사용</th>
                            <th class="p-3 text-xs font-bold text-slate-400 uppercase">SC</th><th class="p-3 text-xs font-bold text-slate-400 uppercase">Bound PV</th>
                        </tr></thead><tbody>{pvc_rows}</tbody>
                    </table>
                </div>
                <h3 class="text-sm font-semibold text-slate-300 mb-2">PV Inventory</h3>
                <div class="glass rounded-xl overflow-hidden border border-slate-700/50 max-h-72 overflow-y-auto scroll">
                    <table class="w-full text-left border-collapse">
                        <thead class="sticky top-0 z-10"><tr class="bg-slate-800 border-b border-slate-700">
                            <th class="p-3 text-xs font-bold text-slate-400 uppercase">PV Name</th><th class="p-3 text-xs font-bold text-slate-400 uppercase">Capacity</th>
                            <th class="p-3 text-xs font-bold text-slate-400 uppercase">Status</th><th class="p-3 text-xs font-bold text-slate-400 uppercase">SC</th>
                            <th class="p-3 text-xs font-bold text-slate-400 uppercase">Claim</th><th class="p-3 text-xs font-bold text-slate-400 uppercase">Age</th>
                        </tr></thead><tbody>{pv_rows}</tbody>
                    </table>
                </div>
            </section>
        </div>
        {vm_contents}
    </main>
</div>
</body>
</html>"""

        with open(FILE_NAME,"w",encoding="utf-8") as f: f.write(html)
        logger.info(f"Report assembled → {FILE_NAME}")

    def deliver_report(self) -> None:
        if not os.path.exists(FILE_NAME): logger.error("Delivery aborted"); return
        logger.info(f"Deploying → {REMOTE_SERVER}:{REMOTE_PATH}")
        try:
            subprocess.run(["scp",FILE_NAME,f"{REMOTE_SERVER}:{REMOTE_PATH}"],check=True,timeout=60)
            logger.info(">>> SUCCESS: AIBox Observability Hub is LIVE.")
        except subprocess.TimeoutExpired: logger.error("SCP timeout")
        except subprocess.CalledProcessError as e: logger.error(f"SCP failed ({e.returncode})")
        except Exception as e: logger.error(f"Delivery failed: {e}")

    def run(self) -> None:
        """
        Stage1 [병렬 6]: node / vm / storage / cluster_health /
                          cluster_operators / machine_config
        Stage2 [병렬 5]: vm_realtime / node_realtime / pvc_disk /
                          performance_trends / infra_metrics
        """
        logger.info("Initializing AIBox (Ultimate Edition v4)...")
        stage1=[self.fetch_node_metrics, self.fetch_vm_metrics,
                self.fetch_advanced_storage_metrics, self.fetch_cluster_health,
                self.fetch_cluster_operators, self.fetch_machine_config]
        with ThreadPoolExecutor(max_workers=len(stage1)) as ex:
            futs={ex.submit(fn):fn.__name__ for fn in stage1}
            for fut in as_completed(futs):
                fn=futs[fut]
                try: fut.result(); logger.info(f"Stage1 ✓ {fn}")
                except Exception as e: logger.error(f"Stage1 ✗ {fn}: {e}")

        stage2=[self.fetch_vm_realtime_metrics, self.fetch_node_realtime_metrics,
                self.fetch_pvc_disk_stats, self.fetch_performance_trends,
                self.fetch_infra_metrics]
        with ThreadPoolExecutor(max_workers=len(stage2)) as ex:
            futs={ex.submit(fn):fn.__name__ for fn in stage2}
            for fut in as_completed(futs):
                fn=futs[fut]
                try: fut.result(); logger.info(f"Stage2 ✓ {fn}")
                except Exception as e: logger.error(f"Stage2 ✗ {fn}: {e}")

        self.build_html()
        self.deliver_report()


if __name__=="__main__":
    MetricsCollector().run()

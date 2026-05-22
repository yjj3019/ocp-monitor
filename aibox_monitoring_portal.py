#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AIBox Unified Monitoring Portal — v5 (Unified Edition)
=======================================================
통합 설계 원칙:
  - MetricsCollector (vm_metrics_report.py v4 전체 흡수) → JSON 반환으로 전환
  - SQLiteManager → 8개 테이블, 시계열 + 폴링 사이클 로그
  - CollectorHealthTracker → 메트릭별 성공/실패/지연 추적
  - PollingEngine → 30s 인터벌, 수집→DB→캐시 파이프라인
  - FastAPI → 8개 REST API + SPA HTML 서빙
  - HTMLReportBuilder → 기존 리포트 레거시 보존

신규 페이지:
  - Collector Health: Prometheus API 상태, 폴링 히스토리, DB 통계
"""

import os, sys, json, time, ssl, logging, sqlite3, threading, re, subprocess, urllib.parse, urllib.request
import subprocess
from dataclasses import asdict
from node_manager import NodeDataManager

class NodeDataManager:
    def __init__(self, prom_client=None):
        self.prom = prom_client
        self.default_net_msg = "환경 제약으로 수집 불가"
        self.cli_command = ["oc", "adm", "top", "nodes"]
        self.timeout = 5
    def fetch_from_prometheus(self, node_name):
        if not self.prom: return None
        try:
            return self.prom.custom_query(f'node_cpu_seconds_total{{instance="{node_name}"}}')
        except: return None
    def parse_oc_adm_top_nodes(self, node_name):
        try:
            result = subprocess.check_output(self.cli_command, text=True, timeout=self.timeout)
            for line in result.splitlines()[1:]:
                parts = line.split()
                if len(parts) >= 4 and parts[0] == node_name:
                    return {"cpu": parts[1], "mem": parts[3], "status": "active"}
        except: pass
        return None
    def fetch_node_realtime(self, node_name):
        prom_data = self.fetch_from_prometheus(node_name)
        if prom_data: return {"data": prom_data, "source": "Prometheus", "is_cli": False}
        cli_data = self.parse_oc_adm_top_nodes(node_name)
        return {"data": cli_data, "source": "CLI" if cli_data else "Unavailable", "is_cli": True}
    def get_ui_context(self, node_snapshot):
        is_cli = node_snapshot.get("source") == "CLI"
        return {
            "badge": "CLI" if is_cli else "Prometheus",
            "show_time_series": not is_cli,
            "net_info": self.default_net_msg if is_cli else "Real-time",
            "tooltip": "데이터 소스: CLI (환경 제약 대체)" if is_cli else "데이터 소스: Prometheus"
        }
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from html import escape
from typing import Any, Dict, List, Optional, Tuple, Final

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ════════════════════════════════════════════════════════════════════
# 1. LOGGING
# ════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("AIBoxPortal")

# ════════════════════════════════════════════════════════════════════
# 2. CONSTANTS
# ════════════════════════════════════════════════════════════════════
DB_FILE: Final[str]            = "aibox_metrics.db"
REPORT_HTML: Final[str]        = "vm_metrics_report.html"
POLL_INTERVAL: Final[int]      = 30
CMD_TIMEOUT: Final[int]        = 30
PROMETHEUS_NS: Final[str]      = "openshift-monitoring"
THANOS_PROXY_BASE: Final[str]  = (
    f"/api/v1/namespaces/{PROMETHEUS_NS}"
    "/services/https:thanos-querier:9091/proxy/api/v1/query"
)
THANOS_RANGE_BASE: Final[str]  = (
    f"/api/v1/namespaces/{PROMETHEUS_NS}"
    "/services/https:thanos-querier:9091/proxy/api/v1/query_range"
)
PROMETHEUS_ROUTE_CANDIDATES: Final[List[str]] = [
    "thanos-querier", "prometheus-k8s-external", "prometheus-k8s",
]
SPARKLINE_HOURS: Final[int]    = 1
SPARKLINE_STEP: Final[str]     = "5m"
DB_RETENTION_HOURS: Final[int] = 24
CYCLE_LOG_KEEP: Final[int]     = 100  # 최근 N개 폴링 사이클 보존

HEALTH_JOB_CANDIDATES: Final[Dict[str, List[str]]] = {
    "api_server": ["apiserver", "kube-apiserver", "openshift-apiserver"],
    "etcd":       ["etcd", "etcd-metrics"],
    "coredns":    ["dns-default", "coredns", "kube-dns"],
    "router":     ["router-internal-default", "openshift-router"],
    "registry":   ["openshift-image-registry", "image-registry"],
    "scheduler":  ["scheduler", "kube-scheduler"],
}

STATUS_MAP: Final[Dict[str, str]] = {
    "running": "running", "provisioning": "provisioning", "starting": "provisioning",
    "scheduling": "provisioning", "pending": "provisioning", "waitingforvolumesbinding": "provisioning",
    "stopped": "stopped", "stopping": "stopped", "terminating": "stopped", "paused": "stopped",
    "failed": "failed", "errored": "failed", "crashloopbackoff": "failed",
}

# ════════════════════════════════════════════════════════════════════
# 3. DATA MODELS
# ════════════════════════════════════════════════════════════════════
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
    cluster_operators: List[ClusterOperatorStatus] = field(default_factory=list)
    mcp_pools: List[MCPStatus] = field(default_factory=list)
    router_req_rate: Optional[float] = None
    router_4xx_rate: Optional[float] = None
    router_5xx_rate: Optional[float] = None
    router_sessions: Optional[float] = None
    router_routes: Optional[float] = None
    router_req_trend: List[float] = field(default_factory=list)
    router_5xx_trend: List[float] = field(default_factory=list)
    sched_pending: Optional[float] = None
    sched_lat_trend: List[float] = field(default_factory=list)
    ovn_ports: Optional[float] = None
    ovn_nb_leader: Optional[float] = None
    reg_req_rate: Optional[float] = None
    reg_trend: List[float] = field(default_factory=list)
    trend_labels: List[str] = field(default_factory=list)

@dataclass
class NodeMetrics:
    name: str
    cpu_usage: str
    memory_usage: str
    status: str
    roles: str
    age: str
    memory_bytes: int = 0
    cpu_pct_realtime: Optional[float] = None
    memory_pct_realtime: Optional[float] = None

@dataclass
class VMMetrics:
    name: str
    namespace: str
    status: str
    cpu_cores: str
    memory_total: str
    creation_time: str
    node: str
    ip_address: str
    os_info: str
    volumes: List[Dict[str, str]] = field(default_factory=list)
    status_group: str = "unknown"
    cpu_usage_pct: Optional[float] = None
    memory_usage_pct: Optional[float] = None
    memory_used_bytes: int = 0
    net_rx_bps: Optional[float] = None
    net_tx_bps: Optional[float] = None
    disk_read_bps: Optional[float] = None
    disk_write_bps: Optional[float] = None

@dataclass
class StoragePoolDetail:
    name: str
    provisioner: str
    total_capacity_bytes: int = 0
    used_capacity_bytes: int = 0
    pv_count: int = 0
    pvc_count: int = 0
    disk_used_bytes: int = 0
    disk_capacity_bytes: int = 0
    disk_available_bytes: int = 0

@dataclass
class SystemMetrics:
    nodes: List[NodeMetrics] = field(default_factory=list)
    vms: List[VMMetrics] = field(default_factory=list)
    pv_data: List[Dict[str, str]] = field(default_factory=list)
    pvc_data: List[Dict[str, str]] = field(default_factory=list)
    storage_pools: Dict[str, StoragePoolDetail] = field(default_factory=dict)
    total_pv_capacity_bytes: int = 0
    total_pvc_requested_bytes: int = 0
    global_memory_total: str = "N/A"
    ocp_version: str = "N/A"
    last_updated: str = ""
    cluster_health: ClusterHealth = field(default_factory=ClusterHealth)
    perf_trend: PerformanceTrend = field(default_factory=PerformanceTrend)
    infra: InfraMetrics = field(default_factory=InfraMetrics)
    vm_running_count: int = 0
    vm_provisioning_count: int = 0
    vm_stopped_count: int = 0
    vm_failed_count: int = 0
    pvc_disk_stats: Dict[str, Dict[str, int]] = field(default_factory=dict)

# 수집기 헬스 모델
@dataclass
class MetricCollectionResult:
    name: str
    status: str = "pending"   # pending / ok / partial / failed / skipped
    count: int = 0
    duration_ms: float = 0.0
    error: str = ""

@dataclass
class PollingCycleLog:
    cycle_id: int = 0
    started_at: str = ""
    finished_at: str = ""
    duration_ms: float = 0.0
    status: str = "pending"   # running / success / partial / failed
    metrics: List[MetricCollectionResult] = field(default_factory=list)

@dataclass
class PrometheusStatus:
    strategy: str = "unavailable"   # raw / route / unavailable
    host: str = ""
    last_ping_at: str = ""
    last_ping_latency_ms: float = 0.0
    is_reachable: bool = False
    job_count: int = 0
    matched_jobs: Dict[str, str] = field(default_factory=dict)

# ════════════════════════════════════════════════════════════════════
# 4. SQLITE MANAGER
# ════════════════════════════════════════════════════════════════════
class SQLiteManager:
    def __init__(self, db_file: str = DB_FILE):
        self.db_file = db_file
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_file, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._conn() as conn:
            c = conn.cursor()
            c.executescript("""
                CREATE TABLE IF NOT EXISTS cluster_health_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    cpu_pct REAL, mem_pct REAL,
                    total_nodes INTEGER, total_vms INTEGER,
                    firing_alerts INTEGER,
                    api_server REAL, etcd REAL, coredns REAL,
                    ocp_version TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_chh_ts ON cluster_health_history(timestamp);

                CREATE TABLE IF NOT EXISTS node_metrics_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    node_name TEXT, cpu_pct REAL, mem_pct REAL, status TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_nmh_ts ON node_metrics_history(timestamp);

                CREATE TABLE IF NOT EXISTS vm_density_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    node_name TEXT, vm_count INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_vdh_ts ON vm_density_history(timestamp);

                CREATE TABLE IF NOT EXISTS vm_metrics_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    vm_name TEXT, namespace TEXT,
                    cpu_pct REAL, mem_pct REAL, status TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_vmh_ts ON vm_metrics_history(timestamp);

                CREATE TABLE IF NOT EXISTS infra_snapshot (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    router_req_rate REAL, router_5xx_rate REAL,
                    sched_pending INTEGER, ovn_ports INTEGER, reg_req_rate REAL
                );
                CREATE INDEX IF NOT EXISTS idx_is_ts ON infra_snapshot(timestamp);

                CREATE TABLE IF NOT EXISTS polling_cycles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    duration_ms REAL,
                    status TEXT DEFAULT 'running'
                );

                CREATE TABLE IF NOT EXISTS metric_collection_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cycle_id INTEGER,
                    metric_name TEXT,
                    status TEXT,
                    item_count INTEGER DEFAULT 0,
                    duration_ms REAL DEFAULT 0,
                    error_msg TEXT DEFAULT '',
                    FOREIGN KEY(cycle_id) REFERENCES polling_cycles(id)
                );

                CREATE TABLE IF NOT EXISTS prometheus_ping_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    strategy TEXT, host TEXT,
                    latency_ms REAL, is_reachable INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_ppl_ts ON prometheus_ping_log(timestamp);
            """)
        logger.info("SQLite DB initialized: %s", self.db_file)

    # ── 쓰기 ──────────────────────────────────────────────────────
    def store_metrics(self, metrics: SystemMetrics) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        h = metrics.cluster_health
        with self._conn() as conn:
            c = conn.cursor()
            # 클러스터 요약
            c.execute(
                "INSERT INTO cluster_health_history "
                "(timestamp,cpu_pct,mem_pct,total_nodes,total_vms,firing_alerts,api_server,etcd,coredns,ocp_version) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (now, h.cluster_cpu_pct, h.cluster_memory_pct,
                 len(metrics.nodes), len(metrics.vms),
                 h.firing_alerts, h.api_server, h.etcd, h.coredns,
                 metrics.ocp_version),
            )
            # 노드 메트릭
            for n in metrics.nodes:
                c.execute(
                    "INSERT INTO node_metrics_history (timestamp,node_name,cpu_pct,mem_pct,status) VALUES (?,?,?,?,?)",
                    (now, n.name, n.cpu_pct_realtime, n.memory_pct_realtime, n.status),
                )
            # VM 밀집도 (노드별 VM 수)
            density: Dict[str, int] = {}
            for vm in metrics.vms:
                if vm.node != "N/A":
                    density[vm.node] = density.get(vm.node, 0) + 1
            for node_name, cnt in density.items():
                c.execute(
                    "INSERT INTO vm_density_history (timestamp,node_name,vm_count) VALUES (?,?,?)",
                    (now, node_name, cnt),
                )
            # VM 메트릭
            for vm in metrics.vms:
                c.execute(
                    "INSERT INTO vm_metrics_history (timestamp,vm_name,namespace,cpu_pct,mem_pct,status) VALUES (?,?,?,?,?,?)",
                    (now, vm.name, vm.namespace, vm.cpu_usage_pct, vm.memory_usage_pct, vm.status_group),
                )
            # 인프라 스냅샷
            im = metrics.infra
            c.execute(
                "INSERT INTO infra_snapshot (timestamp,router_req_rate,router_5xx_rate,sched_pending,ovn_ports,reg_req_rate) "
                "VALUES (?,?,?,?,?,?)",
                (now, im.router_req_rate, im.router_5xx_rate,
                 im.sched_pending, im.ovn_ports, im.reg_req_rate),
            )
            # 오래된 데이터 정리
            cutoff = (datetime.now() - timedelta(hours=DB_RETENTION_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
            for tbl in ("cluster_health_history", "node_metrics_history", "vm_density_history",
                        "vm_metrics_history", "infra_snapshot", "prometheus_ping_log"):
                c.execute(f"DELETE FROM {tbl} WHERE timestamp <= ?", (cutoff,))
            # 오래된 사이클 로그 정리
            c.execute(
                "DELETE FROM metric_collection_log WHERE cycle_id NOT IN "
                "(SELECT id FROM polling_cycles ORDER BY id DESC LIMIT ?)", (CYCLE_LOG_KEEP,)
            )
            c.execute(
                "DELETE FROM polling_cycles WHERE id NOT IN "
                "(SELECT id FROM polling_cycles ORDER BY id DESC LIMIT ?)", (CYCLE_LOG_KEEP,)
            )

    def start_cycle(self) -> int:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            c = conn.cursor()
            c.execute("INSERT INTO polling_cycles (started_at, status) VALUES (?, 'running')", (now,))
            return c.lastrowid

    def finish_cycle(self, cycle_id: int, duration_ms: float, status: str,
                     results: List[MetricCollectionResult]) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            c = conn.cursor()
            c.execute(
                "UPDATE polling_cycles SET finished_at=?, duration_ms=?, status=? WHERE id=?",
                (now, duration_ms, status, cycle_id),
            )
            for r in results:
                c.execute(
                    "INSERT INTO metric_collection_log (cycle_id,metric_name,status,item_count,duration_ms,error_msg) "
                    "VALUES (?,?,?,?,?,?)",
                    (cycle_id, r.name, r.status, r.count, r.duration_ms, r.error),
                )

    def store_prometheus_ping(self, strategy: str, host: str, latency_ms: float, reachable: bool) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            conn.cursor().execute(
                "INSERT INTO prometheus_ping_log (timestamp,strategy,host,latency_ms,is_reachable) VALUES (?,?,?,?,?)",
                (now, strategy, host, latency_ms, int(reachable)),
            )

    # ── 읽기 ──────────────────────────────────────────────────────
    def get_cluster_trend(self, hours: int = 1) -> Dict[str, Any]:
        cutoff = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT timestamp,cpu_pct,mem_pct,firing_alerts FROM cluster_health_history "
                "WHERE timestamp > ? ORDER BY timestamp ASC", (cutoff,)
            ).fetchall()
        return {
            "labels": [r["timestamp"][11:16] for r in rows],
            "cpu": [r["cpu_pct"] for r in rows],
            "mem": [r["mem_pct"] for r in rows],
            "alerts": [r["firing_alerts"] for r in rows],
        }

    def get_vm_density_trend(self, hours: int = 1) -> Dict[str, Any]:
        cutoff = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT timestamp,node_name,vm_count FROM vm_density_history "
                "WHERE timestamp > ? ORDER BY timestamp ASC", (cutoff,)
            ).fetchall()
        nodes: Dict[str, Dict[str, int]] = {}
        labels: List[str] = []
        seen_ts: set = set()
        for r in rows:
            ts = r["timestamp"][11:16]
            if ts not in seen_ts:
                labels.append(ts)
                seen_ts.add(ts)
            nodes.setdefault(r["node_name"], {})[ts] = r["vm_count"]
        return {"labels": labels, "nodes": {n: [d.get(l, 0) for l in labels] for n, d in nodes.items()}}

    def get_infra_trend(self, hours: int = 1) -> Dict[str, Any]:
        cutoff = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT timestamp,router_req_rate,router_5xx_rate,sched_pending FROM infra_snapshot "
                "WHERE timestamp > ? ORDER BY timestamp ASC", (cutoff,)
            ).fetchall()
        return {
            "labels": [r["timestamp"][11:16] for r in rows],
            "router_req": [r["router_req_rate"] for r in rows],
            "router_5xx": [r["router_5xx_rate"] for r in rows],
            "sched_pending": [r["sched_pending"] for r in rows],
        }

    def get_recent_cycles(self, limit: int = 20) -> List[Dict]:
        with self._conn() as conn:
            cycles = conn.execute(
                "SELECT * FROM polling_cycles ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            result = []
            for cy in cycles:
                logs = conn.execute(
                    "SELECT * FROM metric_collection_log WHERE cycle_id=?", (cy["id"],)
                ).fetchall()
                result.append({
                    "cycle_id": cy["id"],
                    "started_at": cy["started_at"],
                    "finished_at": cy["finished_at"],
                    "duration_ms": cy["duration_ms"],
                    "status": cy["status"],
                    "metrics": [dict(l) for l in logs],
                })
        return result

    def get_db_stats(self) -> Dict[str, Any]:
        tables = ["cluster_health_history", "node_metrics_history", "vm_density_history",
                  "vm_metrics_history", "infra_snapshot", "polling_cycles",
                  "metric_collection_log", "prometheus_ping_log"]
        stats = {}
        with self._conn() as conn:
            for tbl in tables:
                row = conn.execute(f"SELECT COUNT(*) as cnt FROM {tbl}").fetchone()
                stats[tbl] = {"row_count": row["cnt"]}
                try:
                    r2 = conn.execute(
                        f"SELECT MIN(timestamp) as oldest, MAX(timestamp) as newest FROM {tbl} WHERE timestamp IS NOT NULL"
                    ).fetchone()
                    if r2:
                        stats[tbl]["oldest"] = r2["oldest"]
                        stats[tbl]["newest"] = r2["newest"]
                except Exception:
                    pass
        size = os.path.getsize(self.db_file) if os.path.exists(self.db_file) else 0
        return {"file": self.db_file, "size_bytes": size, "tables": stats}

    def get_prometheus_ping_history(self, limit: int = 20) -> List[Dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM prometheus_ping_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

# ════════════════════════════════════════════════════════════════════
# 5. METRICS COLLECTOR
# ════════════════════════════════════════════════════════════════════
class MetricsCollector:
    """
    OpenShift Prometheus 수집기.
    vm_metrics_report.py v4의 전체 수집 로직을 JSON 반환 방식으로 통합.
    """
    def __init__(self):
        self.metrics = SystemMetrics()
        self._prom_strategy: str = ""
        self._prom_token: str = ""
        self._prom_host: str = ""
        self._lock = threading.Lock()

    # ── 유틸 ──────────────────────────────────────────────────────
    def _run(self, cmd: List[str], timeout: int = CMD_TIMEOUT, silent: bool = False) -> str:
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            if r.returncode != 0:
                (logger.debug if silent else logger.warning)(
                    "CMD fail: %s | %s", " ".join(cmd), r.stderr.strip()[:200]
                )
                return ""
            return r.stdout.strip()
        except subprocess.TimeoutExpired:
            if not silent: logger.error("Timeout (%ds): %s", timeout, " ".join(cmd))
            return ""
        except FileNotFoundError:
            if not silent: logger.error("Not found: %r", cmd[0])
            return ""
        except Exception as e:
            if not silent: logger.error("Error %r: %s", cmd[0], e)
            return ""

    @staticmethod
    def _parse_bytes(s: str) -> int:
        if not s or s in ("N/A", "0", ""): return 0
        m = re.match(r'^([0-9.]+(?:[eE][+-]?[0-9]+)?)\s*(Ki|Mi|Gi|Ti|Pi|k|M|G|T|P)?$', s.strip())
        if not m: return 0
        try:
            val, unit = float(m.group(1)), m.group(2) or ""
            mults = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4, "Pi": 1024**5,
                     "k": 1000, "M": 1000**2, "G": 1000**3, "T": 1000**4, "P": 1000**5}
            return int(val * mults.get(unit, 1))
        except Exception:
            return 0

    @staticmethod
    def _fmt_bytes(b: int) -> str:
        if b == 0: return "0 B"
        names = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
        v, i = float(b), 0
        while v >= 1024 and i < len(names) - 1: v /= 1024.0; i += 1
        return f"{v:.2f} {names[i]}"

    @staticmethod
    def _fmt_age(ts: str) -> str:
        try:
            d = datetime.now(timezone.utc) - datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if d.days >= 1: return f"{d.days}d"
            h = d.seconds // 3600
            return f"{h}h" if h >= 1 else f"{d.seconds // 60}m"
        except Exception:
            return ts

    @staticmethod
    def _classify_vm_status(s: str) -> str:
        return STATUS_MAP.get(s.lower(), "unknown")

    # ── Prometheus 접근 ────────────────────────────────────────────
    def init_prometheus(self) -> None:
        """Prometheus 접근 전략 탐색 (raw → route 순)"""
        logger.info("Prometheus 접근 전략 탐색 중...")
        t0 = time.time()
        raw = self._run(
            ["oc", "get", "--raw", f"{THANOS_PROXY_BASE}?query=up"], timeout=10, silent=True
        )
        if raw:
            try:
                if json.loads(raw).get("status") == "success":
                    self._prom_strategy = "raw"
                    logger.info("Prometheus: oc get --raw 전략 채택")
                    return
            except Exception:
                pass

        token = self._run(["oc", "whoami", "-t"], timeout=10)
        if not token:
            logger.warning("Prometheus: 토큰 없음 → 수집 제한")
            return
        for rn in PROMETHEUS_ROUTE_CANDIDATES:
            host = self._run(
                ["oc", "get", "route", rn, "-n", PROMETHEUS_NS, "-o", "jsonpath={.spec.host}"], timeout=10
            )
            if not host: continue
            lat = time.time()
            result = self._query_prom_route("up", token, host)
            if result is not None:
                self._prom_strategy = "route"
                self._prom_token = token
                self._prom_host = host
                logger.info("Prometheus: route 전략 (%s → %s)", rn, host)
                return
        logger.warning("Prometheus 접근 불가: 메트릭 수집 제한됨")

    def ping_prometheus(self) -> Tuple[bool, float]:
        """Prometheus 연결 상태 확인 → (reachable, latency_ms)"""
        if not self._prom_strategy:
            return False, 0.0
        t0 = time.time()
        try:
            if self._prom_strategy == "raw":
                raw = self._run(
                    ["oc", "get", "--raw", f"{THANOS_PROXY_BASE}?query=up"], timeout=8, silent=True
                )
                ok = bool(raw and json.loads(raw).get("status") == "success")
            else:
                r = self._query_prom_route("up", self._prom_token, self._prom_host)
                ok = r is not None
            return ok, (time.time() - t0) * 1000
        except Exception:
            return False, (time.time() - t0) * 1000

    def _http_get(self, url: str, token: str, timeout: int = 10) -> Optional[dict]:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 401:
                t = self._run(["oc", "whoami", "-t"], timeout=10)
                if t and t != self._prom_token:
                    self._prom_token = t
                    return self._http_get(url, self._prom_token, timeout)
            return None
        except Exception:
            return None

    def _query_prom_route(self, promql: str, token: str, host: str) -> Optional[List[Dict]]:
        url = f"https://{host}/api/v1/query?query={urllib.parse.quote(promql)}"
        data = self._http_get(url, token)
        if data and data.get("status") == "success":
            return data.get("data", {}).get("result", [])
        return None

    def _query(self, promql: str) -> List[Dict]:
        if not self._prom_strategy: return []
        if self._prom_strategy == "raw":
            raw = self._run(
                ["oc", "get", "--raw", f"{THANOS_PROXY_BASE}?query={urllib.parse.quote(promql)}"],
                timeout=15, silent=True,
            )
            if not raw: return []
            try:
                d = json.loads(raw)
                return d.get("data", {}).get("result", []) if d.get("status") == "success" else []
            except Exception:
                return []
        r = self._query_prom_route(promql, self._prom_token, self._prom_host)
        return r if r is not None else []

    def _query_range(self, promql: str) -> Tuple[List[float], List[str]]:
        if not self._prom_strategy: return [], []
        end_ts = int(time.time())
        start_ts = end_ts - SPARKLINE_HOURS * 3600
        params = urllib.parse.urlencode({"query": promql, "start": start_ts, "end": end_ts, "step": SPARKLINE_STEP})
        if self._prom_strategy == "raw":
            raw = self._run(
                ["oc", "get", "--raw", f"{THANOS_RANGE_BASE}?{params}"], timeout=15, silent=True
            )
            if not raw: return [], []
            try:
                d = json.loads(raw)
                results = d.get("data", {}).get("result", []) if d.get("status") == "success" else []
            except Exception:
                return [], []
        else:
            data = self._http_get(
                f"https://{self._prom_host}/api/v1/query_range?{params}", self._prom_token, timeout=15
            )
            if not data or data.get("status") != "success": return [], []
            results = data.get("data", {}).get("result", [])
        if not results: return [], []
        vals, lbls = [], []
        for ts, val in results[0].get("values", []):
            try:
                vals.append(float(val))
                lbls.append(datetime.fromtimestamp(float(ts)).strftime("%H:%M"))
            except (ValueError, TypeError):
                pass
        return vals, lbls

    def _scalar(self, results: List[Dict]) -> Optional[float]:
        try:
            return float(results[0]["value"][1]) if results else None
        except (KeyError, ValueError, IndexError):
            return None

    def _discover_jobs(self) -> Dict[str, str]:
        results = self._query("count by(job) (up)")
        existing = {item.get("metric", {}).get("job", "") for item in results}
        matched: Dict[str, str] = {}
        for cat, candidates in HEALTH_JOB_CANDIDATES.items():
            for c in candidates:
                if c in existing:
                    matched[cat] = c
                    break
        logger.info("Prometheus jobs 매핑: %s", matched)
        return matched

    # ── 수집 메서드 ────────────────────────────────────────────────
    def _timed(self, name: str, fn) -> MetricCollectionResult:
        """수집 함수를 실행하고 결과와 소요 시간을 반환"""
        t0 = time.time()
        result = MetricCollectionResult(name=name)
        try:
            count = fn()
            result.status = "ok"
            result.count = count or 0
        except Exception as e:
            result.status = "failed"
            result.error = str(e)[:200]
            logger.error("수집 실패 [%s]: %s", name, e)
        result.duration_ms = (time.time() - t0) * 1000
        return result

    def fetch_nodes(self) -> int:
        try:
            vj = self._run(["oc", "version", "-o", "json"])
            if vj:
                try: self.metrics.ocp_version = json.loads(vj).get("openshiftVersion", "N/A") or "N/A"
                except Exception: pass
            top = self._run(["oc", "adm", "top", "nodes", "--no-headers"])
            nu: Dict[str, Dict[str, str]] = {}
            for line in top.split("\n"):
                p = line.split()
                if len(p) >= 5: nu[p[0]] = {"cpu": p[2], "mem": p[4]}
            nj = self._run(["oc", "get", "nodes", "-o", "json"])
            if not nj: return 0
            tc = 0
            nodes = []
            for node in json.loads(nj).get("items", []):
                name = node["metadata"]["name"]
                lbls = node["metadata"].get("labels", {})
                roles = ", ".join(sorted(k.split("/")[-1] for k in lbls if k.startswith("node-role.kubernetes.io/"))) or "worker"
                conds = node.get("status", {}).get("conditions", [])
                ready = next((c for c in conds if c["type"] == "Ready"), {})
                status = "Ready" if ready.get("status") == "True" else "NotReady"
                cap_b = self._parse_bytes(node.get("status", {}).get("capacity", {}).get("memory", "0"))
                tc += cap_b
                usage = nu.get(name, {"cpu": "N/A", "mem": "N/A"})
                nodes.append(NodeMetrics(
                    name=name, cpu_usage=usage["cpu"], memory_usage=usage["mem"],
                    status=status, roles=roles,
                    age=self._fmt_age(node["metadata"]["creationTimestamp"]),
                    memory_bytes=cap_b,
                ))
            self.metrics.nodes = nodes
            self.metrics.global_memory_total = self._fmt_bytes(tc)
            return len(nodes)
        except Exception as e:
            raise RuntimeError(f"nodes: {e}")

    def fetch_vms(self) -> int:
        try:
            vj = self._run(["oc", "get", "vm", "-A", "-o", "json"])
            if not vj: return 0
            items = json.loads(vj).get("items", [])

            def _vmi(name: str, ns: str) -> Dict:
                raw = self._run(["oc", "get", "vmi", name, "-n", ns, "-o", "json"], silent=True)
                if not raw: return {}
                try:
                    vd = json.loads(raw)
                    ifaces = vd.get("status", {}).get("interfaces", [])
                    return {
                        "node": vd.get("status", {}).get("nodeName", "N/A"),
                        "ip": ifaces[0].get("ipAddress", "N/A") if ifaces else "N/A",
                        "os": vd.get("status", {}).get("guestOSInfo", {}).get("prettyName", "N/A") or "N/A",
                    }
                except Exception:
                    return {}

            running = [
                (v["metadata"]["name"], v["metadata"]["namespace"]) for v in items
                if v.get("status", {}).get("printableStatus", "") == "Running"
            ]
            vmis: Dict[Tuple[str, str], Dict] = {}
            if running:
                with ThreadPoolExecutor(max_workers=min(len(running), 10)) as ex:
                    futs = {ex.submit(_vmi, n, ns): (n, ns) for n, ns in running}
                    for fut in as_completed(futs):
                        try: vmis[futs[fut]] = fut.result()
                        except Exception: pass

            vms = []
            for vm in items:
                name = vm["metadata"]["name"]
                ns = vm["metadata"]["namespace"]
                status = vm.get("status", {}).get("printableStatus", "Unknown")
                domain = vm.get("spec", {}).get("template", {}).get("spec", {}).get("domain", {})
                cpu_c = str(domain.get("cpu", {}).get("cores", "N/A"))
                mem_t = domain.get("resources", {}).get("requests", {}).get("memory", "N/A")
                vols = [
                    {"name": v.get("name", "N/A"),
                     "pvc": v.get("persistentVolumeClaim", {}).get("claimName", "N/A")}
                    for v in vm.get("spec", {}).get("template", {}).get("spec", {}).get("volumes", [])
                ]
                vi = vmis.get((name, ns), {})
                vms.append(VMMetrics(
                    name=name, namespace=ns, status=status, cpu_cores=cpu_c, memory_total=mem_t,
                    creation_time=self._fmt_age(vm["metadata"]["creationTimestamp"]),
                    node=vi.get("node", "N/A"), ip_address=vi.get("ip", "N/A"),
                    os_info=vi.get("os", "N/A"), volumes=vols,
                    status_group=self._classify_vm_status(status),
                ))
            self.metrics.vms = vms
            return len(vms)
        except Exception as e:
            raise RuntimeError(f"vms: {e}")

    def fetch_storage(self) -> int:
        pools: Dict[str, StoragePoolDetail] = {}
        try:
            sj = self._run(["oc", "get", "sc", "-o", "json"])
            if sj:
                for sc in json.loads(sj).get("items", []):
                    n = sc["metadata"]["name"]
                    pools[n] = StoragePoolDetail(name=n, provisioner=sc.get("provisioner", "Unknown"))
        except Exception as e:
            logger.warning("SC: %s", e)

        pv_data = []
        total_pv = 0
        try:
            pj = self._run(["oc", "get", "pv", "-o", "json"])
            if pj:
                for pv in json.loads(pj).get("items", []):
                    name = pv["metadata"]["name"]
                    cs = pv.get("spec", {}).get("capacity", {}).get("storage", "0")
                    cb = self._parse_bytes(cs)
                    total_pv += cb
                    status = pv.get("status", {}).get("phase", "Unknown")
                    cr = pv.get("spec", {}).get("claimRef", {})
                    claim = f"{cr.get('namespace','')}/{cr.get('name','')}" if cr else "Unbound"
                    sc_name = pv.get("spec", {}).get("storageClassName", "Unknown")
                    if sc_name not in pools:
                        pools[sc_name] = StoragePoolDetail(name=sc_name, provisioner="Implicit/Unknown")
                    pool = pools[sc_name]
                    pool.pv_count += 1
                    pool.total_capacity_bytes += cb
                    if status == "Bound": pool.used_capacity_bytes += cb
                    pv_data.append({"Name": name, "Capacity": cs, "Status": status,
                                    "Claim": claim, "StorageClass": sc_name,
                                    "Age": self._fmt_age(pv["metadata"]["creationTimestamp"])})
        except Exception as e:
            logger.warning("PV: %s", e)

        pvc_data = []
        total_pvc = 0
        try:
            pcj = self._run(["oc", "get", "pvc", "-A", "-o", "json"])
            if pcj:
                for pvc in json.loads(pcj).get("items", []):
                    name = pvc["metadata"]["name"]
                    ns = pvc["metadata"]["namespace"]
                    status = pvc.get("status", {}).get("phase", "Unknown")
                    sc = pvc.get("spec", {}).get("storageClassName", "Unknown")
                    req = pvc.get("spec", {}).get("resources", {}).get("requests", {}).get("storage", "0")
                    total_pvc += self._parse_bytes(req)
                    if sc in pools: pools[sc].pvc_count += 1
                    pvc_data.append({"Name": name, "Namespace": ns, "Status": status,
                                     "Requested": req, "StorageClass": sc,
                                     "Volume": pvc.get("spec", {}).get("volumeName", "Pending")})
        except Exception as e:
            logger.warning("PVC: %s", e)

        self.metrics.storage_pools = pools
        self.metrics.pv_data = pv_data
        self.metrics.pvc_data = pvc_data
        self.metrics.total_pv_capacity_bytes = total_pv
        self.metrics.total_pvc_requested_bytes = total_pvc
        return len(pools)

    def fetch_cluster_health(self) -> int:
        if not self._prom_strategy: return 0
        matched = self._discover_jobs()
        self.metrics.cluster_health.matched_jobs = matched
        queries: Dict[str, str] = {}
        for cat, key in [("api_server", "api"), ("etcd", "etcd"), ("coredns", "coredns")]:
            if cat in matched:
                queries[key] = f'min(up{{job="{matched[cat]}"}}) by ()'
        queries.update({
            "leader": "min(etcd_server_has_leader) by ()",
            "alerts": 'count(ALERTS{alertstate="firing"}) or vector(0)',
            "cpu": '(1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))) * 100',
            "mem": '(1 - sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes)) * 100',
        })
        with ThreadPoolExecutor(max_workers=max(len(queries), 1)) as ex:
            futs = {ex.submit(self._query, q): k for k, q in queries.items()}
            for fut in as_completed(futs):
                k = futs[fut]
                val = self._scalar(fut.result())
                if val is None: continue
                h = self.metrics.cluster_health
                if k == "api": h.api_server = val
                elif k == "etcd": h.etcd = val
                elif k == "coredns": h.coredns = val
                elif k == "leader": h.etcd_leader = val
                elif k == "alerts": h.firing_alerts = int(val)
                elif k == "cpu": h.cluster_cpu_pct = val
                elif k == "mem": h.cluster_memory_pct = val
        return len(queries)

    def fetch_cluster_operators(self) -> int:
        try:
            raw = self._run(["oc", "get", "clusteroperator", "-o", "json"])
            if not raw: return 0
            ops = []
            for item in json.loads(raw).get("items", []):
                name = item["metadata"]["name"]
                versions = item.get("status", {}).get("versions", [])
                version = next((v["version"] for v in versions if v.get("name") == "operator"), "N/A")
                conds = {c["type"]: c for c in item.get("status", {}).get("conditions", [])}
                def cond_bool(t: str) -> Optional[bool]:
                    c = conds.get(t)
                    return (c.get("status") == "True") if c else None
                msg = ""
                for t in ["Degraded", "Progressing", "Available"]:
                    c = conds.get(t, {})
                    if c.get("message"): msg = c["message"][:120]; break
                ops.append(ClusterOperatorStatus(
                    name=name, version=version,
                    available=cond_bool("Available"),
                    progressing=cond_bool("Progressing"),
                    degraded=cond_bool("Degraded"),
                    message=msg,
                ))
            self.metrics.infra.cluster_operators = ops
            return len(ops)
        except Exception as e:
            raise RuntimeError(f"operators: {e}")

    def fetch_mcp(self) -> int:
        try:
            raw = self._run(["oc", "get", "mcp", "-o", "json"])
            if not raw: return 0
            pools = []
            for item in json.loads(raw).get("items", []):
                st = item.get("status", {})
                pools.append(MCPStatus(
                    name=item["metadata"]["name"],
                    machine_count=st.get("machineCount", 0),
                    ready_count=st.get("readyMachineCount", 0),
                    updated_count=st.get("updatedMachineCount", 0),
                    degraded_count=st.get("degradedMachineCount", 0),
                    paused=item.get("spec", {}).get("paused", False),
                ))
            self.metrics.infra.mcp_pools = pools
            return len(pools)
        except Exception as e:
            raise RuntimeError(f"mcp: {e}")

    def fetch_infra_metrics(self) -> int:
        if not self._prom_strategy: return 0
        matched = self.metrics.cluster_health.matched_jobs
        router_job = matched.get("router", "router-internal-default")
        sched_job = matched.get("scheduler", "scheduler")
        reg_job = matched.get("registry", "openshift-image-registry")

        instant: Dict[str, str] = {
            "router_req": f'sum(rate(haproxy_backend_http_responses_total{{job="{router_job}"}}[5m]))',
            "router_4xx": f'sum(rate(haproxy_backend_http_responses_total{{job="{router_job}",code="4xx"}}[5m]))',
            "router_5xx": f'sum(rate(haproxy_backend_http_responses_total{{job="{router_job}",code="5xx"}}[5m]))',
            "router_sess": f'sum(haproxy_server_current_sessions{{job="{router_job}"}})',
            "router_routes": f'count(haproxy_backend_status{{job="{router_job}"}})',
            "sched_pending": 'sum(scheduler_pending_pods) or vector(0)',
            "ovn_ports": 'sum(ovnkube_controller_logical_port_total) or sum(ovnkube_master_logical_port_total) or vector(0)',
            "ovn_nb": 'max(ovnkube_controller_nb_db_leader) or max(ovnkube_master_nb_db_leader) or vector(0)',
            "reg_req": f'sum(rate(registry_http_requests_total{{job="{reg_job}"}}[5m])) or vector(0)',
        }
        range_q: Dict[str, str] = {
            "router_req_t": f'sum(rate(haproxy_backend_http_responses_total{{job="{router_job}"}}[5m]))',
            "router_5xx_t": f'sum(rate(haproxy_backend_http_responses_total{{job="{router_job}",code="5xx"}}[5m]))',
            "sched_lat_t": f'histogram_quantile(0.99,sum(rate(scheduler_scheduling_attempt_duration_seconds_bucket{{job="{sched_job}"}}[5m])) by (le))',
            "reg_req_t": f'sum(rate(registry_http_requests_total{{job="{reg_job}"}}[5m]))',
        }
        im = self.metrics.infra
        with ThreadPoolExecutor(max_workers=len(instant) + len(range_q)) as ex:
            i_futs = {ex.submit(self._query, q): k for k, q in instant.items()}
            r_futs = {ex.submit(self._query_range, q): k for k, q in range_q.items()}
            for fut in as_completed({**i_futs, **r_futs}):
                if fut in i_futs:
                    k = i_futs[fut]
                    val = self._scalar(fut.result())
                    if val is None: continue
                    if k == "router_req": im.router_req_rate = val
                    elif k == "router_4xx": im.router_4xx_rate = val
                    elif k == "router_5xx": im.router_5xx_rate = val
                    elif k == "router_sess": im.router_sessions = val
                    elif k == "router_routes": im.router_routes = val
                    elif k == "sched_pending": im.sched_pending = val
                    elif k == "ovn_ports": im.ovn_ports = val
                    elif k == "ovn_nb": im.ovn_nb_leader = val
                    elif k == "reg_req": im.reg_req_rate = val
                else:
                    k = r_futs[fut]
                    vals, lbls = fut.result()
                    if not vals: continue
                    if k == "router_req_t": im.router_req_trend = vals; im.trend_labels = lbls
                    elif k == "router_5xx_t": im.router_5xx_trend = vals
                    elif k == "sched_lat_t": im.sched_lat_trend = vals
                    elif k == "reg_req_t": im.reg_trend = vals
        return len(instant)

    def fetch_node_realtime(self) -> int:
        """Prometheus 우선, 실패 시 oc top 명시적 fallback + data_source 추적"""
        if not self.metrics.nodes:
            return 0

        success_count = 0

        # 1. Prometheus 우선 시도 (기존 로직 최대한 유지)
        try:
            test = self._query('count by(node) (node_cpu_seconds_total{mode="idle"})')
            has_node = bool(test and test[0].get("metric", {}).get("node"))
            ip_map: Dict[str, str] = {}
            if not has_node:
                for item in self._query("kube_node_info"):
                    lbl = item.get("metric", {})
                    if lbl.get("node") and lbl.get("internal_ip"):
                        ip_map[lbl["internal_ip"]] = lbl["node"]

            nrt: Dict[str, Dict[str, float]] = {}
            with ThreadPoolExecutor(max_workers=2) as ex:
                futs = {
                    ex.submit(self._query, '(1 - avg by(instance,node) (rate(node_cpu_seconds_total{mode="idle"}[5m]))) * 100'): "cpu",
                    ex.submit(self._query, '(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100'): "mem",
                }
                for fut in as_completed(futs):
                    k = futs[fut]
                    for item in fut.result():
                        lbl = item.get("metric", {})
                        name = lbl.get("node", "") or ip_map.get(lbl.get("instance", "").split(":")[0], "")
                        if not name: continue
                        try:
                            nrt.setdefault(name, {})[k] = float(item["value"][1])
                        except Exception:
                            pass

            # Prometheus 데이터 적용
            for n in self.metrics.nodes:
                d = nrt.get(n.name, {})
                if d.get("cpu") is not None or d.get("mem") is not None:
                    n.cpu_pct_realtime = d.get("cpu")
                    n.memory_pct_realtime = d.get("mem")
                    n.data_source = "prometheus"
                    success_count += 1

            if success_count > 0:
                logger.info(f"Node realtime: Prometheus 성공 ({success_count} nodes)")
                return success_count

        except Exception as e:
            logger.warning(f"Prometheus node realtime 실패: {e}")

        # 2. oc top fallback
        try:
            cli_data = self._fetch_from_oc_top()
            updated = 0
            for node in self.metrics.nodes:
                if node.name in cli_data:
                    d = cli_data[node.name]
                    node.cpu_pct_realtime = d.get("cpu")
                    node.memory_pct_realtime = d.get("mem")
                    node.data_source = "oc_top"
                    updated += 1

            if updated > 0:
                logger.info(f"Node realtime: oc top fallback 적용 ({updated} nodes)")
                return updated
        except Exception as e:
            logger.error(f"oc top fallback 실패: {e}")

        return 0


    def _fetch_from_oc_top(self) -> dict:
        """oc adm top nodes fallback"""
        try:
            result = subprocess.run(
                ["oc", "adm", "top", "nodes", "--no-headers"],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode != 0:
                logger.error(f"oc adm top nodes 실패: {result.stderr.strip()}")
                return {}

            data = {}
            for line in result.stdout.strip().splitlines():
                parts = re.split(r'\s+', line.strip())
                if len(parts) >= 5:
                    name = parts[0]
                    try:
                        cpu_str = parts[1].replace("m", "")
                        cpu = float(cpu_str) / 10.0
                        mem_str = parts[3].replace("Mi", "").replace("Gi", "")
                        mem = float(mem_str)
                        data[name] = {"cpu": round(cpu, 1), "mem": round(mem, 1)}
                    except Exception:
                        continue
            logger.info(f"oc adm top nodes 성공: {len(data)} nodes")
            return data
        except FileNotFoundError:
            logger.error("oc 명령어를 찾을 수 없습니다.")
            return {}
        except Exception as e:
            logger.error(f"oc adm top nodes 실행 중 예외: {e}")
            return {}

    def _fetch_from_oc_top(self) -> dict:
        """oc adm top nodes fallback"""
        try:
            result = subprocess.run(
                ["oc", "adm", "top", "nodes", "--no-headers"],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode != 0:
                logger.error(f"oc adm top nodes 실패: {result.stderr.strip()}")
                return {}

            data = {}
            for line in result.stdout.strip().splitlines():
                parts = re.split(r'\s+', line.strip())
                if len(parts) >= 5:
                    name = parts[0]
                    try:
                        cpu_str = parts[1].replace("m", "")
                        cpu = float(cpu_str) / 10.0
                        mem_str = parts[3].replace("Mi", "").replace("Gi", "")
                        mem = float(mem_str)
                        data[name] = {"cpu": round(cpu, 1), "mem": round(mem, 1)}
                    except Exception:
                        continue
            logger.info(f"oc adm top nodes 성공: {len(data)} nodes")
            return data
        except FileNotFoundError:
            logger.error("oc 명령어를 찾을 수 없습니다.")
            return {}
        except Exception as e:
            logger.error(f"oc adm top nodes 실행 중 예외: {e}")
            return {}

    def fetch_vm_realtime(self) -> int:
        if not self.metrics.vms or not self._prom_strategy: return 0
        queries = {
            "cpu_pct": "rate(kubevirt_vmi_cpu_usage_seconds_total[5m]) * 100",
            "mem_used": "kubevirt_vmi_memory_used_bytes",
            "mem_avail": "kubevirt_vmi_memory_available_bytes",
            "net_rx": "rate(kubevirt_vmi_network_receive_bytes_total[5m])",
            "net_tx": "rate(kubevirt_vmi_network_transmit_bytes_total[5m])",
            "disk_r": "rate(kubevirt_vmi_storage_read_traffic_bytes_total[5m])",
            "disk_w": "rate(kubevirt_vmi_storage_write_traffic_bytes_total[5m])",

            data = {}
            for line in result.stdout.strip().splitlines():
                parts = re.split(r"\s+", line.strip())
                if len(parts) >= 5:
                    name = parts[0]
                    try:
                        cpu_str = parts[1].replace("m", "")
                        cpu = float(cpu_str) / 10.0
                        mem_str = parts[3].replace("Mi", "").replace("Gi", "")
                        mem = float(mem_str)
                        data[name] = {"cpu": round(cpu, 1), "mem": round(mem, 1)}
                    except Exception:
                        continue
            logger.info(f"oc adm top nodes 성공: {len(data)} nodes")
            return data
        except FileNotFoundError:
            logger.error("oc 명령어를 찾을 수 없습니다. OpenShift CLI가 설치되어 있는지 확인하세요.")
            return {}
        except Exception as e:
            logger.error(f"oc adm top nodes 실행 중 예외 발생: {e}")
            return {}
        }
        rt: Dict[Tuple[str, str], Dict[str, float]] = {}
        with ThreadPoolExecutor(max_workers=len(queries)) as ex:
            futs = {ex.submit(self._query, q): k for k, q in queries.items()}
            for fut in as_completed(futs):
                k = futs[fut]
                for item in fut.result():
                    lbl = item.get("metric", {})
                    name, ns = lbl.get("name", ""), lbl.get("namespace", "")
                    if not name or not ns: continue
                    try: rt.setdefault((name, ns), {})[k] = float(item["value"][1])
                    except Exception: pass
        for vm in self.metrics.vms:
            d = rt.get((vm.name, vm.namespace), {})
            vm.cpu_usage_pct = d.get("cpu_pct")
            mu, ma = d.get("mem_used", 0), d.get("mem_avail", 0)
            if ma > 0:
                vm.memory_usage_pct = mu / ma * 100
                vm.memory_used_bytes = int(mu)
            vm.net_rx_bps = d.get("net_rx")
            vm.net_tx_bps = d.get("net_tx")
            vm.disk_read_bps = d.get("disk_r")
            vm.disk_write_bps = d.get("disk_w")
        return len(rt)

    def fetch_perf_trends(self) -> int:
        if not self._prom_strategy: return 0
        h = self.metrics.cluster_health
        pt = self.metrics.perf_trend
        etcd_j = h.matched_jobs.get("etcd", "etcd")
        api_j = h.matched_jobs.get("api_server", "apiserver")
        tq = {
            "ew": f'histogram_quantile(0.99,rate(etcd_disk_wal_fsync_duration_seconds_bucket{{job="{etcd_j}"}}[5m]))',
            "er": f'histogram_quantile(0.99,rate(etcd_network_peer_round_trip_time_seconds_bucket{{job="{etcd_j}"}}[5m]))',
            "ar": f'sum(rate(apiserver_request_total{{job="{api_j}"}}[5m]))',
            "ae": f'sum(rate(apiserver_request_total{{job="{api_j}",code=~"5.."}}[5m]))',
            "al": f'histogram_quantile(0.99,sum(rate(apiserver_request_duration_seconds_bucket{{job="{api_j}"}}[5m])) by (le))',
        }
        with ThreadPoolExecutor(max_workers=len(tq)) as ex:
            futs = {ex.submit(self._query_range, q): k for k, q in tq.items()}
            for fut in as_completed(futs):
                k = futs[fut]
                vals, lbls = fut.result()
                if not vals: continue
                if k == "ew": pt.etcd_wal_p99 = vals; pt.time_labels = lbls
                elif k == "er": pt.etcd_peer_rtt = vals
                elif k == "ar": pt.api_req_rate = vals
                elif k == "ae": pt.api_err_rate = vals
                elif k == "al": pt.api_latency_p99 = vals
        return len(tq)

    def collect_all(self) -> List[MetricCollectionResult]:
        """모든 메트릭 수집. 결과 목록 반환."""
        with self._lock:
            self.metrics = SystemMetrics()
            self.metrics.last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        steps = [
            ("nodes", self.fetch_nodes),
            ("vms", self.fetch_vms),
            ("storage", self.fetch_storage),
            ("cluster_health", self.fetch_cluster_health),
            ("cluster_operators", self.fetch_cluster_operators),
            ("mcp", self.fetch_mcp),
            ("infra_metrics", self.fetch_infra_metrics),
            ("node_realtime", self.fetch_node_realtime),
            ("vm_realtime", self.fetch_vm_realtime),
            ("perf_trends", self.fetch_perf_trends),
        ]
        results = []
        for name, fn in steps:
            results.append(self._timed(name, fn))

        # VM 상태 카운트
        m = self.metrics
        for vm in m.vms:
            if vm.status_group == "running": m.vm_running_count += 1
            elif vm.status_group == "provisioning": m.vm_provisioning_count += 1
            elif vm.status_group == "stopped": m.vm_stopped_count += 1
            elif vm.status_group == "failed": m.vm_failed_count += 1
        return results

    def get_metrics_snapshot(self) -> Dict[str, Any]:
        """FastAPI가 소비할 JSON-직렬화 가능 스냅샷 반환"""
        m = self.metrics
        h = m.cluster_health
        im = m.infra
        return {
            "last_updated": m.last_updated,
            "ocp_version": m.ocp_version,
            "cluster": {
                "cpu_pct": h.cluster_cpu_pct,
                "mem_pct": h.cluster_memory_pct,
                "firing_alerts": h.firing_alerts,
                "api_server": h.api_server,
                "etcd": h.etcd,
                "coredns": h.coredns,
                "etcd_leader": h.etcd_leader,
            },
            "counts": {
                "nodes": len(m.nodes),
                "vms_total": len(m.vms),
                "vms_running": m.vm_running_count,
                "vms_stopped": m.vm_stopped_count,
                "vms_failed": m.vm_failed_count,
                "vms_provisioning": m.vm_provisioning_count,
                "pv": len(m.pv_data),
                "pvc": len(m.pvc_data),
                "storage_pools": len(m.storage_pools),
                "cluster_operators": len(im.cluster_operators),
            },
            "nodes": [
                {
                    "name": n.name, "status": n.status, "roles": n.roles, "age": n.age,
                    "cpu_usage": n.cpu_usage, "mem_usage": n.memory_usage,
                    "cpu_pct": n.cpu_pct_realtime, "mem_pct": n.memory_pct_realtime,
                    "memory_bytes": n.memory_bytes,
                }
                for n in m.nodes
            ],
            "vms": [
                {
                    "name": vm.name, "namespace": vm.namespace, "status": vm.status,
                    "status_group": vm.status_group, "cpu_cores": vm.cpu_cores,
                    "memory_total": vm.memory_total, "node": vm.node,
                    "ip": vm.ip_address, "os": vm.os_info, "age": vm.creation_time,
                    "cpu_pct": vm.cpu_usage_pct, "mem_pct": vm.memory_usage_pct,
                    "net_rx_bps": vm.net_rx_bps, "net_tx_bps": vm.net_tx_bps,
                    "disk_r_bps": vm.disk_read_bps, "disk_w_bps": vm.disk_write_bps,
                    "volumes": vm.volumes,
                }
                for vm in m.vms
            ],
            "storage": {
                "total_pv_bytes": m.total_pv_capacity_bytes,
                "total_pvc_bytes": m.total_pvc_requested_bytes,
                "pools": [
                    {
                        "name": p.name, "provisioner": p.provisioner,
                        "pv_count": p.pv_count, "pvc_count": p.pvc_count,
                        "total_bytes": p.total_capacity_bytes,
                        "used_bytes": p.used_capacity_bytes,
                        "disk_used": p.disk_used_bytes,
                        "disk_capacity": p.disk_capacity_bytes,
                    }
                    for p in m.storage_pools.values()
                ],
                "pvs": m.pv_data,
                "pvcs": m.pvc_data,
            },
            "infra": {
                "router": {
                    "req_rate": im.router_req_rate, "4xx_rate": im.router_4xx_rate,
                    "5xx_rate": im.router_5xx_rate, "sessions": im.router_sessions,
                    "routes": im.router_routes,
                    "req_trend": im.router_req_trend, "5xx_trend": im.router_5xx_trend,
                    "trend_labels": im.trend_labels,
                },
                "scheduler": {"pending": im.sched_pending, "lat_trend": im.sched_lat_trend},
                "ovn": {"ports": im.ovn_ports, "nb_leader": im.ovn_nb_leader},
                "registry": {"req_rate": im.reg_req_rate, "trend": im.reg_trend},
                "cluster_operators": [asdict(op) for op in im.cluster_operators],
                "mcp_pools": [asdict(p) for p in im.mcp_pools],
            },
            "perf": {
                "etcd_wal_p99": m.perf_trend.etcd_wal_p99,
                "etcd_peer_rtt": m.perf_trend.etcd_peer_rtt,
                "api_req_rate": m.perf_trend.api_req_rate,
                "api_err_rate": m.perf_trend.api_err_rate,
                "api_latency_p99": m.perf_trend.api_latency_p99,
                "time_labels": m.perf_trend.time_labels,
            },
        }

# ════════════════════════════════════════════════════════════════════
# 6. POLLING ENGINE
# ════════════════════════════════════════════════════════════════════
class PollingEngine:
    def __init__(self, collector: MetricsCollector, db: SQLiteManager):
        self.collector = collector
        self.db = db
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.prom_status = PrometheusStatus()
        self._cache: Dict[str, Any] = {}
        self._cache_lock = threading.Lock()

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="PollingEngine")
        self._thread.start()
        # 첫 번째 수집을 즉시 선행 실행 (30s 대기 없이 캐시 확보)
        threading.Thread(target=self._initial_collect, daemon=True, name="InitialCollect").start()
        logger.info("PollingEngine 시작 (interval=%ds)", POLL_INTERVAL)

    def _initial_collect(self) -> None:
        """앱 기동 직후 캐시를 빠르게 채우기 위한 선행 수집 (lightweight)"""
        logger.info("초기 수집 시작...")
        try:
            self.collector.init_prometheus()
            self.collector.fetch_nodes()
            self.collector.fetch_vms()
            self.collector.fetch_storage()
            self.collector.fetch_cluster_health()
            self.collector.fetch_cluster_operators()
            self.collector.fetch_mcp()
            m = self.collector.metrics
            m.vm_running_count = m.vm_stopped_count = m.vm_provisioning_count = m.vm_failed_count = 0
            for vm in m.vms:
                if vm.status_group == "running": m.vm_running_count += 1
                elif vm.status_group == "stopped": m.vm_stopped_count += 1
                elif vm.status_group == "provisioning": m.vm_provisioning_count += 1
                elif vm.status_group == "failed": m.vm_failed_count += 1
            with self._cache_lock:
                self._cache = self.collector.get_metrics_snapshot()
            self._update_prom_status(*self.collector.ping_prometheus())
            logger.info("초기 수집 완료 — nodes=%d vms=%d", len(m.nodes), len(m.vms))
        except Exception as e:
            logger.error("초기 수집 실패: %s", e)

    def _loop(self) -> None:
        # 최초 기동 시 Prometheus 접근 탐색
        self.collector.init_prometheus()
        self._update_prom_status()
        while self._running:
            t0 = time.time()
            cycle_id = self.db.start_cycle()
            logger.info("--- Polling cycle #%d 시작 ---", cycle_id)
            try:
                results = self.collector.collect_all()
                self.db.store_metrics(self.collector.metrics)
                ok = sum(1 for r in results if r.status == "ok")
                fail = sum(1 for r in results if r.status == "failed")
                status = "success" if fail == 0 else ("partial" if ok > 0 else "failed")
            except Exception as e:
                logger.error("폴링 사이클 예외: %s", e)
                results = []
                status = "failed"

            duration_ms = (time.time() - t0) * 1000
            self.db.finish_cycle(cycle_id, duration_ms, status, results)

            # Prometheus ping 기록
            reachable, lat = self.collector.ping_prometheus()
            self.db.store_prometheus_ping(
                self.collector._prom_strategy, self.collector._prom_host, lat, reachable
            )
            self._update_prom_status(reachable, lat)

            # 캐시 갱신
            with self._cache_lock:
                self._cache = self.collector.get_metrics_snapshot()

            logger.info("--- Polling cycle #%d 완료 (%.0fms, %s) ---", cycle_id, duration_ms, status)

            elapsed = time.time() - t0
            sleep_time = max(0, POLL_INTERVAL - elapsed)
            time.sleep(sleep_time)

    def _update_prom_status(self, reachable: bool = True, lat: float = 0.0) -> None:
        s = self.prom_status
        s.strategy = self.collector._prom_strategy or "unavailable"
        s.host = self.collector._prom_host
        s.is_reachable = reachable
        s.last_ping_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        s.last_ping_latency_ms = lat
        s.matched_jobs = self.collector.metrics.cluster_health.matched_jobs

    def get_cache(self) -> Dict[str, Any]:
        with self._cache_lock:
            return dict(self._cache)

# ════════════════════════════════════════════════════════════════════
# 7. FASTAPI APPLICATION
# ════════════════════════════════════════════════════════════════════
app = FastAPI(title="AIBox Unified Monitoring Portal", version="5.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

_collector = MetricsCollector()
_db = SQLiteManager()
_engine = PollingEngine(_collector, _db)

@app.on_event("startup")
def on_startup():
    _engine.start()

# ── Health (always responds) ──────────────────────────────────────
@app.get("/api/v1/health")
def api_health():
    cache = _engine.get_cache()
    return JSONResponse({
        "status": "ok",
        "has_data": bool(cache),
        "prom_strategy": _collector._prom_strategy or "unavailable",
        "prom_reachable": _engine.prom_status.is_reachable,
        "last_updated": cache.get("last_updated") if cache else None,
    })

# ── Overview ──────────────────────────────────────────────────────
_EMPTY_SKELETON: Dict[str, Any] = {
    "is_collecting": True,
    "last_updated": None,
    "ocp_version": "—",
    "cluster": {"cpu_pct": None, "mem_pct": None, "firing_alerts": 0,
                "api_server": None, "etcd": None, "coredns": None, "etcd_leader": None},
    "counts": {"nodes": 0, "vms_total": 0, "vms_running": 0, "vms_stopped": 0,
               "vms_failed": 0, "vms_provisioning": 0, "pv": 0, "pvc": 0,
               "storage_pools": 0, "cluster_operators": 0},
    "nodes": [], "vms": [],
    "storage": {"total_pv_bytes": 0, "total_pvc_bytes": 0, "pools": [], "pvs": [], "pvcs": []},
    "infra": {"router": {}, "scheduler": {}, "ovn": {}, "registry": {},
              "cluster_operators": [], "mcp_pools": []},
    "perf": {"etcd_wal_p99": [], "etcd_peer_rtt": [], "api_req_rate": [],
             "api_err_rate": [], "api_latency_p99": [], "time_labels": []},
}

@app.get("/api/v1/overview")
def api_overview():
    cache = _engine.get_cache()
    if not cache:
        return JSONResponse({**_EMPTY_SKELETON})
    return JSONResponse({**cache, "is_collecting": False})

# ── Nodes ─────────────────────────────────────────────────────────
@app.get("/api/v1/nodes")
def api_nodes():
    cache = _engine.get_cache()
    nodes = cache.get("nodes", [])

    # NodeDataManager 인스턴스 생성 (prom 객체는 전역변수로 존재)
    manager = NodeDataManager(prom_client=prom)

    # 각 노드에 UI 컨텍스트 및 Fallback 데이터 주입
    for n in nodes:
        node_name = n.get("name") # 노드 이름 확인
        snapshot = manager.fetch_node_realtime(node_name)
        ui_ctx = manager.get_ui_context(snapshot)

        # UI 배지 및 툴팁 정보 추가
        n["ui"] = ui_ctx

        # CLI fallback 데이터가 있다면 기존 값을 덮어씌움
        if snapshot['is_cli'] and snapshot['data']:
            n["cpu_usage"] = snapshot['data'].get('cpu', n.get("cpu_usage"))
            n["memory_usage"] = snapshot['data'].get('mem', n.get("memory_usage"))

    return JSONResponse({"nodes": nodes, "last_updated": cache.get("last_updated")})

# ── VMs ───────────────────────────────────────────────────────────
@app.get("/api/v1/vms")
def api_vms(namespace: Optional[str] = None, status: Optional[str] = None):
    cache = _engine.get_cache()
    vms = cache.get("vms", [])
    if namespace: vms = [v for v in vms if v["namespace"] == namespace]
    if status: vms = [v for v in vms if v["status_group"] == status]
    return JSONResponse({"vms": vms, "last_updated": cache.get("last_updated")})

# ── Storage ───────────────────────────────────────────────────────
@app.get("/api/v1/storage")
def api_storage():
    cache = _engine.get_cache()
    return JSONResponse({"storage": cache.get("storage", {}), "last_updated": cache.get("last_updated")})

# ── Infrastructure ────────────────────────────────────────────────
@app.get("/api/v1/infra")
def api_infra():
    cache = _engine.get_cache()
    return JSONResponse({"infra": cache.get("infra", {}), "last_updated": cache.get("last_updated")})

# ── Performance Trends ────────────────────────────────────────────
@app.get("/api/v1/performance")
def api_performance():
    cache = _engine.get_cache()
    return JSONResponse({"perf": cache.get("perf", {}), "last_updated": cache.get("last_updated")})

# ── Historical Trends (SQLite) ────────────────────────────────────
@app.get("/api/v1/trends/{key}")
def api_trends(key: str, hours: int = Query(1, ge=1, le=24)):
    if key == "cluster": return JSONResponse(_db.get_cluster_trend(hours))
    elif key == "vm_density": return JSONResponse(_db.get_vm_density_trend(hours))
    elif key == "infra": return JSONResponse(_db.get_infra_trend(hours))
    else: raise HTTPException(404, f"Unknown trend key: {key}")

# ── Collector Health ──────────────────────────────────────────────
@app.get("/api/v1/collector")
def api_collector_health():
    ps = _engine.prom_status
    cycles = _db.get_recent_cycles(20)
    db_stats = _db.get_db_stats()
    ping_hist = _db.get_prometheus_ping_history(20)

    # 최근 1시간 성공률
    total = len(cycles)
    success = sum(1 for c in cycles if c["status"] == "success")
    partial = sum(1 for c in cycles if c["status"] == "partial")
    success_rate = (success + partial * 0.5) / total * 100 if total else 0

    # 메트릭별 집계
    metric_stats: Dict[str, Dict] = {}
    for cycle in cycles:
        for m in cycle.get("metrics", []):
            name = m["metric_name"]
            if name not in metric_stats:
                metric_stats[name] = {"ok": 0, "failed": 0, "total_ms": 0, "count": 0}
            ms = metric_stats[name]
            if m["status"] == "ok": ms["ok"] += 1
            elif m["status"] == "failed": ms["failed"] += 1
            ms["total_ms"] += m.get("duration_ms", 0) or 0
            ms["count"] += 1

    metric_summary = [
        {
            "name": name,
            "success_rate": s["ok"] / s["count"] * 100 if s["count"] else 0,
            "avg_ms": s["total_ms"] / s["count"] if s["count"] else 0,
            "ok": s["ok"],
            "failed": s["failed"],
        }
        for name, s in metric_stats.items()
    ]

    return JSONResponse({
        "prometheus": {
            "strategy": ps.strategy,
            "host": ps.host,
            "last_ping_at": ps.last_ping_at,
            "last_ping_latency_ms": ps.last_ping_latency_ms,
            "is_reachable": ps.is_reachable,
            "matched_jobs": ps.matched_jobs,
        },
        "polling": {
            "interval_seconds": POLL_INTERVAL,
            "total_cycles": total,
            "success_rate_pct": success_rate,
            "last_cycle": cycles[0] if cycles else None,
            "recent_cycles": cycles,
        },
        "metric_summary": metric_summary,
        "database": db_stats,
        "prometheus_ping_history": ping_hist,
    })

# ── Legacy Report ─────────────────────────────────────────────────
@app.get("/report.html")
def serve_report():
    if os.path.exists(REPORT_HTML):
        return FileResponse(REPORT_HTML)
    return HTMLResponse("<h1>리포트 생성 중입니다. 최대 30초 대기 후 새로 고침하세요.</h1>", status_code=202)

# ── SPA Dashboard ─────────────────────────────────────────────────
@app.get("/")
def serve_dashboard():
    return HTMLResponse(SPA_HTML)

# ════════════════════════════════════════════════════════════════════
# 8. SPA HTML TEMPLATE
# ════════════════════════════════════════════════════════════════════
SPA_HTML = r"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AIBox Monitoring Portal v5</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=DM+Sans:wght@400;500;600;700&display=swap');
  :root {
    --bg-base:    #020817;
    --bg-surface: #0c1426;
    --bg-card:    #111827;
    --bg-card2:   #1a2438;
    --bd:         #1e3a5f22;
    --bd-bright:  #2d4a7a44;
    --accent:     #3b82f6;
    --accent2:    #06b6d4;
    --success:    #10b981;
    --warn:       #f59e0b;
    --danger:     #ef4444;
    --txt-primary:   #e2e8f0;
    --txt-secondary: #94a3b8;
    --txt-muted:     #475569;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { height: 100%; background: var(--bg-base); color: var(--txt-primary);
               font-family: 'DM Sans', sans-serif; overflow: hidden; }
  .mono { font-family: 'JetBrains Mono', monospace; }
  /* Layout */
  #app { display: flex; height: 100vh; }
  #sidebar { width: 220px; flex-shrink: 0; background: var(--bg-surface);
             border-right: 1px solid var(--bd-bright); display: flex; flex-direction: column;
             padding: 0; overflow: hidden; }
  #main { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
  #topbar { padding: 12px 20px; border-bottom: 1px solid var(--bd-bright);
            display: flex; align-items: center; justify-content: space-between;
            background: var(--bg-surface); flex-shrink: 0; }
  #content { flex: 1; overflow-y: auto; padding: 20px; }
  /* Sidebar */
  .sb-logo { padding: 16px 16px 12px; border-bottom: 1px solid var(--bd-bright); }
  .sb-logo h1 { font-size: 13px; font-weight: 700; color: var(--accent); letter-spacing: .05em; }
  .sb-logo p { font-size: 10px; color: var(--txt-muted); margin-top: 2px; }
  .sb-nav { flex: 1; padding: 8px 0; overflow-y: auto; }
  .sb-item { display: flex; align-items: center; gap: 10px; padding: 8px 16px;
             font-size: 13px; font-weight: 500; color: var(--txt-secondary);
             cursor: pointer; transition: all .15s; border-left: 2px solid transparent; }
  .sb-item:hover { background: rgba(59,130,246,.08); color: var(--txt-primary); }
  .sb-item.active { color: var(--accent); background: rgba(59,130,246,.12);
                    border-left-color: var(--accent); }
  .sb-item .icon { width: 16px; height: 16px; flex-shrink: 0; opacity: .8; }
  .sb-footer { padding: 12px 16px; border-top: 1px solid var(--bd-bright); }
  .sb-footer .version { font-size: 10px; color: var(--txt-muted); font-family: monospace; }
  /* Cards */
  .card { background: var(--bg-card); border: 1px solid var(--bd-bright);
          border-radius: 10px; padding: 16px; }
  .card-sm { background: var(--bg-card2); border: 1px solid var(--bd);
             border-radius: 8px; padding: 12px; }
  /* KPI Cards */
  .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px,1fr)); gap: 12px; margin-bottom: 16px; }
  .kpi { background: var(--bg-card); border: 1px solid var(--bd-bright); border-radius: 10px;
         padding: 14px 16px; }
  .kpi .label { font-size: 10px; text-transform: uppercase; letter-spacing: .08em;
                color: var(--txt-muted); margin-bottom: 6px; }
  .kpi .value { font-size: 24px; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
  .kpi .sub { font-size: 10px; color: var(--txt-muted); margin-top: 4px; }
  /* Status dots */
  .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; flex-shrink: 0; }
  .dot-ok { background: var(--success); box-shadow: 0 0 6px var(--success); }
  .dot-warn { background: var(--warn); box-shadow: 0 0 6px var(--warn); }
  .dot-err { background: var(--danger); box-shadow: 0 0 6px var(--danger); }
  .dot-off { background: var(--txt-muted); }
  /* Badge */
  .badge { display: inline-flex; align-items: center; padding: 2px 8px; border-radius: 4px;
           font-size: 10px; font-weight: 600; letter-spacing: .04em; }
  .badge-ok  { background: rgba(16,185,129,.15); color: #34d399; border: 1px solid rgba(16,185,129,.3); }
  .badge-warn{ background: rgba(245,158,11,.15); color: #fbbf24; border: 1px solid rgba(245,158,11,.3); }
  .badge-err { background: rgba(239,68,68,.15);  color: #f87171; border: 1px solid rgba(239,68,68,.3); }
  .badge-off { background: rgba(71,85,105,.15);  color: #94a3b8; border: 1px solid rgba(71,85,105,.3); }
  /* Table */
  .tbl { width: 100%; border-collapse: collapse; font-size: 12px; }
  .tbl th { padding: 8px 10px; text-align: left; font-size: 10px; text-transform: uppercase;
            letter-spacing: .06em; color: var(--txt-muted); border-bottom: 1px solid var(--bd-bright);
            white-space: nowrap; }
  .tbl td { padding: 8px 10px; border-bottom: 1px solid var(--bd); white-space: nowrap; }
  .tbl tr:last-child td { border-bottom: none; }
  .tbl tr:hover td { background: rgba(59,130,246,.04); }
  /* Progress bar */
  .pbar-wrap { display: flex; align-items: center; gap: 8px; }
  .pbar { flex: 1; height: 4px; background: var(--bg-base); border-radius: 2px; overflow: hidden; }
  .pbar-fill { height: 100%; border-radius: 2px; transition: width .3s; }
  .pbar-ok   { background: var(--success); }
  .pbar-warn { background: var(--warn); }
  .pbar-err  { background: var(--danger); }
  /* Scroll */
  ::-webkit-scrollbar { width: 4px; height: 4px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--bd-bright); border-radius: 2px; }
  /* Operator Grid */
  .op-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(140px,1fr)); gap: 8px; }
  .op-cell { background: var(--bg-card2); border: 1px solid var(--bd); border-radius: 7px;
             padding: 8px 10px; cursor: default; transition: border-color .15s; }
  .op-cell:hover { border-color: var(--bd-bright); }
  .op-name { font-size: 11px; font-weight: 600; color: var(--txt-primary); white-space: nowrap;
             overflow: hidden; text-overflow: ellipsis; }
  .op-ver  { font-size: 10px; color: var(--txt-muted); margin-top: 2px; font-family: monospace; }
  /* Cycle timeline */
  .cycle-bar { display: inline-block; width: 10px; height: 24px; border-radius: 2px;
               margin: 1px; cursor: pointer; transition: opacity .15s; }
  .cycle-bar:hover { opacity: .7; }
  /* Chart containers */
  .chart-wrap { position: relative; }
  /* Section headers */
  .sec-title { font-size: 13px; font-weight: 700; color: var(--txt-primary);
               text-transform: uppercase; letter-spacing: .08em; margin-bottom: 12px;
               display: flex; align-items: center; gap: 8px; }
  /* Loading overlay */
  #loading { position: fixed; inset: 0; background: var(--bg-base);
             display: flex; align-items: center; justify-content: center; z-index: 999;
             flex-direction: column; gap: 12px; }
  #loading .spinner { width: 32px; height: 32px; border: 2px solid var(--bd-bright);
                       border-top-color: var(--accent); border-radius: 50%;
                       animation: spin .8s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .inline-spin { width: 12px; height: 12px; border: 1.5px solid var(--bd-bright);
                 border-top-color: var(--accent2); border-radius: 50%;
                 animation: spin .8s linear infinite; display: inline-block; vertical-align: middle; }
  /* Collector health specific */
  .ping-bar { display: flex; align-items: flex-end; gap: 2px; height: 32px; }
  .ping-tick { width: 8px; border-radius: 2px 2px 0 0; }
</style>
</head>
<body>

<div id="loading">
  <div class="spinner"></div>
  <p style="font-size:13px;color:var(--txt-secondary);">데이터 수집 대기 중...</p>
</div>

<div id="app" style="display:none;">
  <!-- SIDEBAR -->
  <div id="sidebar">
    <div class="sb-logo">
      <h1>⬡ AIBox Monitor</h1>
      <p id="sb-version">OCP v—</p>
    </div>
    <nav class="sb-nav" id="nav">
      <div class="sb-item active" data-page="overview">
        <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 5a1 1 0 011-1h4a1 1 0 011 1v5a1 1 0 01-1 1H5a1 1 0 01-1-1V5zm10 0a1 1 0 011-1h4a1 1 0 011 1v2a1 1 0 01-1 1h-4a1 1 0 01-1-1V5zM4 15a1 1 0 011-1h4a1 1 0 011 1v4a1 1 0 01-1 1H5a1 1 0 01-1-1v-4zm10-3a1 1 0 011-1h4a1 1 0 011 1v7a1 1 0 01-1 1h-4a1 1 0 01-1-1v-7z"/></svg>
        Overview
      </div>
      <div class="sb-item" data-page="nodes">
        <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2"/></svg>
        Nodes
      </div>
      <div class="sb-item" data-page="vms">
        <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 3H5a2 2 0 00-2 2v4m6-6h10a2 2 0 012 2v4M9 3v18m0 0h10a2 2 0 002-2V9M9 21H5a2 2 0 01-2-2V9m0 0h18"/></svg>
        Virtual Machines
      </div>
      <div class="sb-item" data-page="storage">
        <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"/></svg>
        Storage
      </div>
      <div class="sb-item" data-page="infra">
        <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/></svg>
        Infrastructure
      </div>
      <div class="sb-item" data-page="performance">
        <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/></svg>
        Performance
      </div>
      <div class="sb-item" data-page="collector">
        <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/></svg>
        Collector Health
      </div>
      <div class="sb-item" data-page="legacy">
        <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/></svg>
        Full Report
      </div>
    </nav>
    <div class="sb-footer">
      <div style="display:flex;align-items:center;gap:6px;margin-bottom:4px;">
        <span class="dot" id="sb-prom-dot" style="background:var(--txt-muted)"></span>
        <span style="font-size:11px;color:var(--txt-secondary);" id="sb-prom-label">Prometheus</span>
      </div>
      <div class="version" id="sb-updated">—</div>
      <div class="version" style="margin-top:2px;">30s auto-refresh</div>
    </div>
  </div>

  <!-- MAIN -->
  <div id="main">
    <div id="topbar">
      <div style="display:flex;align-items:center;gap:12px;">
        <h2 id="page-title" style="font-size:16px;font-weight:700;">Overview</h2>
        <div id="alert-badge" class="badge badge-err" style="display:none;"></div>
      </div>
      <div style="display:flex;align-items:center;gap:10px;">
        <span id="refresh-spinner" class="inline-spin" style="display:none;"></span>
        <span style="font-size:11px;color:var(--txt-muted);">Next refresh in</span>
        <span id="countdown" style="font-size:12px;font-weight:700;color:var(--accent);font-family:monospace;">30s</span>
      </div>
    </div>

    <!-- 수집 중 배너 -->
    <div id="collecting-banner" style="display:none;align-items:center;gap:10px;
      background:rgba(245,158,11,.1);border-bottom:1px solid rgba(245,158,11,.25);
      padding:8px 20px;font-size:12px;color:#fbbf24;flex-shrink:0;">
      <span class="inline-spin" style="border-top-color:#fbbf24;"></span>
      <span>OpenShift에서 데이터를 수집 중입니다. <strong>oc login</strong> 상태와 Prometheus 접근을 확인하세요.
      완료되면 자동으로 업데이트됩니다.</span>
      <a href="/api/v1/health" target="_blank" style="margin-left:auto;color:#fbbf24;text-decoration:underline;font-size:11px;">진단 확인</a>
    </div>

    <div id="content">
      <!-- PAGE: OVERVIEW -->
      <div id="page-overview" class="page">
        <div class="kpi-grid" id="kpi-row"></div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px;">
          <div class="card">
            <div class="sec-title">Cluster Health</div>
            <div id="health-cards" style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"></div>
          </div>
          <div class="card">
            <div class="sec-title">VM Status Distribution</div>
            <div id="vm-status-chart" style="height:140px;display:flex;align-items:center;justify-content:center;"></div>
          </div>
        </div>
        <div class="card">
          <div class="sec-title" style="justify-content:space-between;">
            CPU · Memory Trend (1h)
            <span style="font-size:10px;color:var(--txt-muted);font-weight:400;">SQLite 수집 이력</span>
          </div>
          <div class="chart-wrap" style="height:160px;">
            <canvas id="chart-cluster-trend"></canvas>
          </div>
        </div>
      </div>

      <!-- PAGE: NODES -->
      <div id="page-nodes" class="page" style="display:none;">
        <div class="card">
          <div class="sec-title">Node Metrics</div>
          <div style="overflow-x:auto;">
            <table class="tbl" id="tbl-nodes">
              <thead><tr>
                <th>Name</th><th>Roles</th><th>Status</th><th>Age</th>
                <th>CPU (top)</th><th>CPU %</th><th>Memory (top)</th><th>Mem %</th>
              </tr></thead>
              <tbody id="tbody-nodes"></tbody>
            </table>
          </div>
        </div>
      </div>

      <!-- PAGE: VMS -->
      <div id="page-vms" class="page" style="display:none;">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;">
          <input id="vm-search" placeholder="검색 (이름/네임스페이스/노드)..."
            style="flex:1;background:var(--bg-card);border:1px solid var(--bd-bright);border-radius:6px;
                   padding:6px 10px;font-size:12px;color:var(--txt-primary);outline:none;">
          <select id="vm-ns-filter" style="background:var(--bg-card);border:1px solid var(--bd-bright);
            border-radius:6px;padding:6px 10px;font-size:12px;color:var(--txt-primary);outline:none;">
            <option value="">All Namespaces</option>
          </select>
          <select id="vm-status-filter" style="background:var(--bg-card);border:1px solid var(--bd-bright);
            border-radius:6px;padding:6px 10px;font-size:12px;color:var(--txt-primary);outline:none;">
            <option value="">All Status</option>
            <option value="running">Running</option>
            <option value="stopped">Stopped</option>
            <option value="provisioning">Provisioning</option>
            <option value="failed">Failed</option>
          </select>
        </div>
        <div class="card">
          <div style="overflow-x:auto;">
            <table class="tbl">
              <thead><tr>
                <th>Name</th><th>Namespace</th><th>Status</th><th>Node</th>
                <th>IP</th><th>CPU Cores</th><th>CPU %</th><th>Memory</th><th>Mem %</th>
                <th>Net RX</th><th>Net TX</th><th>Age</th>
              </tr></thead>
              <tbody id="tbody-vms"></tbody>
            </table>
          </div>
        </div>
      </div>

      <!-- PAGE: STORAGE -->
      <div id="page-storage" class="page" style="display:none;">
        <div class="card" style="margin-bottom:14px;">
          <div class="sec-title">Storage Pools</div>
          <div id="storage-pools" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:10px;"></div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;">
          <div class="card">
            <div class="sec-title">PersistentVolumes</div>
            <div style="overflow-x:auto;max-height:340px;overflow-y:auto;">
              <table class="tbl">
                <thead><tr><th>Name</th><th>Capacity</th><th>Status</th><th>StorageClass</th><th>Claim</th></tr></thead>
                <tbody id="tbody-pvs"></tbody>
              </table>
            </div>
          </div>
          <div class="card">
            <div class="sec-title">PersistentVolumeClaims</div>
            <div style="overflow-x:auto;max-height:340px;overflow-y:auto;">
              <table class="tbl">
                <thead><tr><th>Name</th><th>Namespace</th><th>Status</th><th>Requested</th><th>StorageClass</th></tr></thead>
                <tbody id="tbody-pvcs"></tbody>
              </table>
            </div>
          </div>
        </div>
      </div>

      <!-- PAGE: INFRA -->
      <div id="page-infra" class="page" style="display:none;">
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin-bottom:14px;" id="infra-service-cards"></div>
        <div class="card" style="margin-bottom:14px;">
          <div class="sec-title">
            Cluster Operators
            <div id="co-summary" style="display:flex;gap:6px;margin-left:auto;"></div>
          </div>
          <div class="op-grid" id="op-grid"></div>
        </div>
        <div class="card">
          <div class="sec-title">MachineConfigPools</div>
          <div id="mcp-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:10px;"></div>
        </div>
      </div>

      <!-- PAGE: PERFORMANCE -->
      <div id="page-performance" class="page" style="display:none;">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px;">
          <div class="card">
            <div class="sec-title">ETCD WAL Fsync p99 (1h)</div>
            <div class="chart-wrap" style="height:140px;"><canvas id="chart-etcd-wal"></canvas></div>
          </div>
          <div class="card">
            <div class="sec-title">ETCD Peer RTT p99 (1h)</div>
            <div class="chart-wrap" style="height:140px;"><canvas id="chart-etcd-rtt"></canvas></div>
          </div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;">
          <div class="card">
            <div class="sec-title">API Request Rate</div>
            <div class="chart-wrap" style="height:140px;"><canvas id="chart-api-req"></canvas></div>
          </div>
          <div class="card">
            <div class="sec-title">API Error Rate (5xx)</div>
            <div class="chart-wrap" style="height:140px;"><canvas id="chart-api-err"></canvas></div>
          </div>
          <div class="card">
            <div class="sec-title">API Latency p99</div>
            <div class="chart-wrap" style="height:140px;"><canvas id="chart-api-lat"></canvas></div>
          </div>
        </div>
      </div>

      <!-- PAGE: COLLECTOR HEALTH -->
      <div id="page-collector" class="page" style="display:none;">
        <!-- Prometheus Status -->
        <div style="display:grid;grid-template-columns:1fr 2fr;gap:14px;margin-bottom:14px;">
          <div class="card">
            <div class="sec-title">Prometheus Connection</div>
            <div id="prom-status-panel"></div>
          </div>
          <div class="card">
            <div class="sec-title">Ping Latency History (last 20)</div>
            <div id="ping-history-chart" style="height:80px;display:flex;align-items:flex-end;gap:2px;padding-top:8px;"></div>
            <div style="font-size:10px;color:var(--txt-muted);margin-top:4px;">최근 → 가장 오른쪽</div>
          </div>
        </div>
        <!-- Polling Summary -->
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:14px;" id="polling-kpis"></div>
        <!-- Cycle Timeline -->
        <div class="card" style="margin-bottom:14px;">
          <div class="sec-title">Polling Cycle Timeline (recent 20)</div>
          <div id="cycle-timeline" style="display:flex;align-items:flex-end;gap:3px;height:40px;margin-bottom:8px;"></div>
          <div style="font-size:10px;color:var(--txt-muted);">
            <span style="color:var(--success);">■</span> success &nbsp;
            <span style="color:var(--warn);">■</span> partial &nbsp;
            <span style="color:var(--danger);">■</span> failed &nbsp;
            <span style="color:var(--txt-muted);">■</span> running
          </div>
        </div>
        <!-- Metric Collection Stats -->
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;">
          <div class="card">
            <div class="sec-title">Per-Metric Success Rate</div>
            <div style="overflow-x:auto;">
              <table class="tbl" id="tbl-metric-stats">
                <thead><tr><th>Metric</th><th>Success Rate</th><th>Avg ms</th><th>OK</th><th>Failed</th></tr></thead>
                <tbody id="tbody-metric-stats"></tbody>
              </table>
            </div>
          </div>
          <div class="card">
            <div class="sec-title">Database Stats</div>
            <div id="db-stats-panel"></div>
          </div>
        </div>
      </div>

      <!-- PAGE: LEGACY REPORT -->
      <div id="page-legacy" class="page" style="display:none;height:calc(100vh - 100px);">
        <iframe src="/report.html" style="width:100%;height:100%;border:none;border-radius:8px;background:#fff;"></iframe>
      </div>
    </div>
  </div>
</div>

<script>
// ═══════════════════════════════════════════════
// STATE
// ═══════════════════════════════════════════════
let state = {
  currentPage: 'overview',
  overview: null,
  collector: null,
  charts: {},
  countdown: 30,
  timer: null,
};

// ═══════════════════════════════════════════════
// NAVIGATION
// ═══════════════════════════════════════════════
document.getElementById('nav').addEventListener('click', e => {
  const item = e.target.closest('.sb-item');
  if (!item) return;
  const page = item.dataset.page;
  document.querySelectorAll('.sb-item').forEach(el => el.classList.remove('active'));
  item.classList.add('active');
  document.querySelectorAll('.page').forEach(el => el.style.display = 'none');
  document.getElementById(`page-${page}`).style.display = '';
  document.getElementById('page-title').textContent = item.textContent.trim();
  state.currentPage = page;
  if (page === 'collector') fetchCollector();
  if (page === 'performance') renderPerformance(state.overview);
});

// ═══════════════════════════════════════════════
// FETCH
// ═══════════════════════════════════════════════
async function fetchAll() {
  document.getElementById('refresh-spinner').style.display = '';
  try {
    const r = await fetch('/api/v1/overview');
    const ov = r.ok ? await r.json() : null;
    if (ov) {
      state.overview = ov;
      _engine_prom_reachable = !ov.is_collecting;
      renderAll(ov);
      // 수집 중 배너 표시 / 숨김
      const banner = document.getElementById('collecting-banner');
      if (ov.is_collecting) {
        banner.style.display = 'flex';
        // 5초 후 자동 재시도
        setTimeout(fetchAll, 5000);
      } else {
        banner.style.display = 'none';
      }
    }
  } catch(e) {
    console.error('fetch error', e);
    setTimeout(fetchAll, 5000);
  }
  finally { document.getElementById('refresh-spinner').style.display = 'none'; }
}

async function fetchCollector() {
  const data = await fetch('/api/v1/collector').then(r => r.ok ? r.json() : null);
  if (data) { state.collector = data; renderCollector(data); }
}

// ═══════════════════════════════════════════════
// RENDER HELPERS
// ═══════════════════════════════════════════════
const fmtPct = v => v != null ? `${v.toFixed(1)}%` : '—';
const fmtNum = v => v != null ? v.toFixed(1) : '—';
const fmtBytes = b => {
  if (!b) return '0 B';
  const units = ['B','KiB','MiB','GiB','TiB'];
  let v = b, i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(1)} ${units[i]}`;
};
const fmtBps = b => {
  if (b == null) return '—';
  const units = ['B/s','KB/s','MB/s','GB/s'];
  let v = b, i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(1)} ${units[i]}`;
};
const pbarColor = pct => pct == null ? 'pbar-ok' : pct < 70 ? 'pbar-ok' : pct < 90 ? 'pbar-warn' : 'pbar-err';
const statusBadge = sg => {
  const m = {running:'badge-ok',stopped:'badge-off',provisioning:'badge-warn',failed:'badge-err',unknown:'badge-off'};
  const t = {running:'Running',stopped:'Stopped',provisioning:'Provisioning',failed:'Failed',unknown:'Unknown'};
  return `<span class="badge ${m[sg]||'badge-off'}">${t[sg]||sg}</span>`;
};
const healthDot = v => {
  if (v == null) return '<span class="dot dot-off"></span>';
  return v >= 1 ? '<span class="dot dot-ok"></span>' : '<span class="dot dot-err"></span>';
};

function mkChart(id, labels, datasets, opts = {}) {
  const canvas = document.getElementById(id);
  if (!canvas) return;
  const key = id;
  if (state.charts[key]) state.charts[key].destroy();
  state.charts[key] = new Chart(canvas, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      animation: false,
      plugins: { legend: { display: opts.legend || false, labels: { color: '#94a3b8', font: { size: 10 } } } },
      scales: {
        x: { ticks: { color: '#475569', font: { size: 9 }, maxTicksLimit: 8 },
             grid: { color: '#1e3a5f22' }, border: { display: false } },
        y: { ticks: { color: '#475569', font: { size: 9 }, maxTicksLimit: 5 },
             grid: { color: '#1e3a5f22' }, border: { display: false },
             ...(opts.yMin != null ? { min: opts.yMin } : {}),
             ...(opts.yMax != null ? { max: opts.yMax } : {}) },
      },
      elements: { point: { radius: 0, hoverRadius: 3 }, line: { tension: 0.3, borderWidth: 1.5 } },
    },
  });
}

function sparkDataset(label, data, color) {
  return {
    label, data,
    borderColor: color,
    backgroundColor: color + '18',
    fill: true,
  };
}

// ═══════════════════════════════════════════════
// RENDER: OVERVIEW
// ═══════════════════════════════════════════════
function renderOverview(d) {
  const c = d.cluster, ct = d.counts;
  // KPI
  document.getElementById('kpi-row').innerHTML = [
    { label:'OCP Version', value: d.ocp_version, sub:'', color:'var(--accent)' },
    { label:'Total Nodes', value: ct.nodes, sub:'', color:'var(--txt-primary)' },
    { label:'VMs Running', value: ct.vms_running, sub:`/ ${ct.vms_total} total`, color:'var(--success)' },
    { label:'VMs Stopped', value: ct.vms_stopped, sub:'', color:'var(--txt-muted)' },
    { label:'VMs Failed', value: ct.vms_failed, sub:'', color: ct.vms_failed > 0 ? 'var(--danger)' : 'var(--txt-muted)' },
    { label:'Firing Alerts', value: c.firing_alerts, sub:'', color: c.firing_alerts > 0 ? 'var(--danger)' : 'var(--success)' },
    { label:'Cluster CPU', value: fmtPct(c.cpu_pct), sub:'', color: c.cpu_pct > 90 ? 'var(--danger)' : c.cpu_pct > 70 ? 'var(--warn)' : 'var(--success)' },
    { label:'Cluster Mem', value: fmtPct(c.mem_pct), sub:'', color: c.mem_pct > 90 ? 'var(--danger)' : c.mem_pct > 70 ? 'var(--warn)' : 'var(--success)' },
  ].map(k => `
    <div class="kpi">
      <div class="label">${k.label}</div>
      <div class="value" style="color:${k.color}">${k.value}</div>
      ${k.sub ? `<div class="sub">${k.sub}</div>` : ''}
    </div>
  `).join('');

  // Alert badge
  const ab = document.getElementById('alert-badge');
  if (c.firing_alerts > 0) {
    ab.style.display = '';
    ab.textContent = `${c.firing_alerts} Firing Alerts`;
  } else { ab.style.display = 'none'; }

  // Health cards
  document.getElementById('health-cards').innerHTML = [
    { label:'API Server', v: c.api_server },
    { label:'ETCD', v: c.etcd },
    { label:'CoreDNS', v: c.coredns },
    { label:'ETCD Leader', v: c.etcd_leader },
  ].map(h => {
    const ok = h.v != null && h.v >= 1;
    const unk = h.v == null;
    const cls = unk ? 'badge-off' : ok ? 'badge-ok' : 'badge-err';
    const txt = unk ? 'N/A' : ok ? 'Healthy' : 'Unhealthy';
    return `<div class="card-sm" style="display:flex;align-items:center;gap:8px;">
      <span class="dot ${unk ? 'dot-off' : ok ? 'dot-ok' : 'dot-err'}"></span>
      <div>
        <div style="font-size:11px;color:var(--txt-muted);">${h.label}</div>
        <div class="badge ${cls}" style="margin-top:3px;">${txt}</div>
      </div>
    </div>`;
  }).join('');

  // VM Status donut (simple bars)
  const total = ct.vms_total || 1;
  document.getElementById('vm-status-chart').innerHTML = `
    <div style="width:100%;">
      ${[['Running', ct.vms_running, 'var(--success)'],
         ['Stopped', ct.vms_stopped, 'var(--txt-muted)'],
         ['Provisioning', ct.vms_provisioning, 'var(--warn)'],
         ['Failed', ct.vms_failed, 'var(--danger)'],
        ].map(([lbl, cnt, color]) => `
        <div style="margin-bottom:8px;">
          <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px;">
            <span style="color:var(--txt-secondary);">${lbl}</span>
            <span style="color:${color};font-weight:600;">${cnt}</span>
          </div>
          <div class="pbar"><div class="pbar-fill" style="background:${color};width:${Math.round(cnt/total*100)}%"></div></div>
        </div>`).join('')}
    </div>`;

  // Fetch trend and chart it
  fetch('/api/v1/trends/cluster?hours=1').then(r => r.json()).then(t => {
    mkChart('chart-cluster-trend', t.labels,
      [sparkDataset('CPU %', t.cpu, '#3b82f6'),
       sparkDataset('Mem %', t.mem, '#a78bfa')],
      { legend: true, yMin: 0, yMax: 100 });
  }).catch(() => {});
}

// ═══════════════════════════════════════════════
// RENDER: NODES
// ═══════════════════════════════════════════════
function renderNodes(nodes) {
  document.getElementById('tbody-nodes').innerHTML = nodes.map(n => {
    const cpc = pbarColor(n.cpu_pct), mpc = pbarColor(n.mem_pct);
    return `<tr>
      <td class="mono" style="color:var(--accent2)">${n.name}</td>
      <td><span class="badge badge-off">${n.roles}</span></td>
      <td><span class="dot ${n.status==='Ready'?'dot-ok':'dot-err'}"></span> ${n.status}</td>
      <td>${n.age}</td>
      <td class="mono">${n.cpu_usage}</td>
      <td><div class="pbar-wrap"><div class="pbar"><div class="pbar-fill ${cpc}" style="width:${n.cpu_pct||0}%"></div></div><span style="font-size:10px;color:var(--txt-secondary);width:36px">${fmtPct(n.cpu_pct)}</span></div></td>
      <td class="mono">${n.mem_usage}</td>
      <td><div class="pbar-wrap"><div class="pbar"><div class="pbar-fill ${mpc}" style="width:${n.mem_pct||0}%"></div></div><span style="font-size:10px;color:var(--txt-secondary);width:36px">${fmtPct(n.mem_pct)}</span></div></td>
    </tr>`;
  }).join('');
}

// ═══════════════════════════════════════════════
// RENDER: VMs
// ═══════════════════════════════════════════════
let _allVMs = [];
function renderVMs(vms) {
  _allVMs = vms;
  // Populate namespace filter
  const ns = [...new Set(vms.map(v => v.namespace))].sort();
  const sel = document.getElementById('vm-ns-filter');
  const cur = sel.value;
  sel.innerHTML = '<option value="">All Namespaces</option>' + ns.map(n => `<option ${n===cur?'selected':''}>${n}</option>`).join('');
  filterVMs();
}
function filterVMs() {
  const search = document.getElementById('vm-search').value.toLowerCase();
  const ns = document.getElementById('vm-ns-filter').value;
  const st = document.getElementById('vm-status-filter').value;
  const filtered = _allVMs.filter(v =>
    (!search || v.name.includes(search) || v.namespace.includes(search) || v.node.includes(search)) &&
    (!ns || v.namespace === ns) &&
    (!st || v.status_group === st)
  );
  document.getElementById('tbody-vms').innerHTML = filtered.map(v => `<tr>
    <td class="mono" style="color:var(--accent2)">${v.name}</td>
    <td><span class="badge badge-off">${v.namespace}</span></td>
    <td>${statusBadge(v.status_group)}</td>
    <td style="color:var(--txt-secondary);font-size:11px;">${v.node}</td>
    <td class="mono" style="font-size:11px;">${v.ip}</td>
    <td style="text-align:center;">${v.cpu_cores}</td>
    <td><div class="pbar-wrap"><div class="pbar"><div class="pbar-fill ${pbarColor(v.cpu_pct)}" style="width:${v.cpu_pct||0}%"></div></div><span style="font-size:10px;color:var(--txt-secondary);width:36px">${fmtPct(v.cpu_pct)}</span></div></td>
    <td style="font-size:11px;">${v.memory_total}</td>
    <td><div class="pbar-wrap"><div class="pbar"><div class="pbar-fill ${pbarColor(v.mem_pct)}" style="width:${v.mem_pct||0}%"></div></div><span style="font-size:10px;color:var(--txt-secondary);width:36px">${fmtPct(v.mem_pct)}</span></div></td>
    <td class="mono" style="font-size:10px;">${fmtBps(v.net_rx_bps)}</td>
    <td class="mono" style="font-size:10px;">${fmtBps(v.net_tx_bps)}</td>
    <td style="font-size:11px;">${v.age}</td>
  </tr>`).join('') || '<tr><td colspan="12" style="text-align:center;color:var(--txt-muted);padding:24px;">VM 없음</td></tr>';
}
document.getElementById('vm-search').addEventListener('input', filterVMs);
document.getElementById('vm-ns-filter').addEventListener('change', filterVMs);
document.getElementById('vm-status-filter').addEventListener('change', filterVMs);

// ═══════════════════════════════════════════════
// RENDER: STORAGE
// ═══════════════════════════════════════════════
function renderStorage(st) {
  document.getElementById('storage-pools').innerHTML = st.pools.map(p => {
    const usedPct = p.total_bytes ? Math.round(p.used_bytes / p.total_bytes * 100) : 0;
    return `<div class="card-sm">
      <div style="font-size:12px;font-weight:600;color:var(--txt-primary);margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${p.name}</div>
      <div style="font-size:10px;color:var(--txt-muted);margin-bottom:8px;">${p.provisioner}</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;margin-bottom:8px;">
        <div class="card-sm" style="text-align:center;padding:6px;"><div style="font-size:10px;color:var(--txt-muted);">PVs</div><div style="font-weight:700;">${p.pv_count}</div></div>
        <div class="card-sm" style="text-align:center;padding:6px;"><div style="font-size:10px;color:var(--txt-muted);">PVCs</div><div style="font-weight:700;">${p.pvc_count}</div></div>
      </div>
      <div style="font-size:10px;color:var(--txt-muted);margin-bottom:3px;">Used ${fmtBytes(p.used_bytes)} / ${fmtBytes(p.total_bytes)}</div>
      <div class="pbar"><div class="pbar-fill ${pbarColor(usedPct)}" style="width:${usedPct}%"></div></div>
    </div>`;
  }).join('') || '<div style="color:var(--txt-muted);font-size:12px;">스토리지 풀 없음</div>';

  document.getElementById('tbody-pvs').innerHTML = st.pvs.map(p =>
    `<tr><td class="mono" style="font-size:11px;">${p.Name}</td><td>${p.Capacity}</td>
     <td><span class="badge ${p.Status==='Bound'?'badge-ok':'badge-warn'}">${p.Status}</span></td>
     <td style="font-size:10px;color:var(--txt-muted);">${p.StorageClass}</td>
     <td style="font-size:10px;color:var(--txt-muted);">${p.Claim}</td></tr>`
  ).join('');

  document.getElementById('tbody-pvcs').innerHTML = st.pvcs.map(p =>
    `<tr><td class="mono" style="font-size:11px;">${p.Name}</td>
     <td><span class="badge badge-off">${p.Namespace}</span></td>
     <td><span class="badge ${p.Status==='Bound'?'badge-ok':'badge-warn'}">${p.Status}</span></td>
     <td>${p.Requested}</td>
     <td style="font-size:10px;color:var(--txt-muted);">${p.StorageClass}</td></tr>`
  ).join('');
}

// ═══════════════════════════════════════════════
// RENDER: INFRA
// ═══════════════════════════════════════════════
function renderInfra(infra) {
  const r = infra.router, sc = infra.scheduler, ov = infra.ovn, rg = infra.registry;
  document.getElementById('infra-service-cards').innerHTML = [
    { title:'Ingress Router', dot: r.req_rate != null, items:[
      ['Req/s', r.req_rate != null ? r.req_rate.toFixed(1)+'/s' : '—', 'var(--accent2)'],
      ['4xx/s', r.req_rate != null ? (r['4xx_rate']||0).toFixed(2)+'/s' : '—', 'var(--warn)'],
      ['5xx/s', r.req_rate != null ? (r['5xx_rate']||0).toFixed(2)+'/s' : '—', 'var(--danger)'],
      ['Sessions', r.sessions != null ? Math.round(r.sessions) : '—', 'var(--accent)'],
    ]},
    { title:'Scheduler', dot: sc.pending != null, items:[
      ['Pending Pods', sc.pending != null ? Math.round(sc.pending) : '—', sc.pending > 10 ? 'var(--danger)' : 'var(--success)'],
    ]},
    { title:'OVN-Kubernetes', dot: ov.ports != null, items:[
      ['Logical Ports', ov.ports != null ? Math.round(ov.ports) : '—', 'var(--accent2)'],
      ['NB DB Leader', ov.nb_leader >= 1 ? 'Leader' : ov.nb_leader == null ? 'N/A' : 'No Leader',
       ov.nb_leader >= 1 ? 'var(--success)' : 'var(--danger)'],
    ]},
    { title:'Image Registry', dot: rg.req_rate != null, items:[
      ['Req/s', rg.req_rate != null ? rg.req_rate.toFixed(2)+'/s' : '—', 'var(--success)'],
    ]},
  ].map(card => `
    <div class="card">
      <div style="display:flex;align-items:center;gap:6px;margin-bottom:10px;">
        <span class="dot ${card.dot?'dot-ok':'dot-off'}"></span>
        <span style="font-size:12px;font-weight:700;">${card.title}</span>
      </div>
      ${card.items.map(([l,v,c]) => `
        <div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid var(--bd);">
          <span style="font-size:11px;color:var(--txt-muted);">${l}</span>
          <span style="font-size:12px;font-weight:700;color:${c};">${v}</span>
        </div>`).join('')}
    </div>`).join('');

  // Cluster Operators
  const ops = infra.cluster_operators || [];
  const ok = ops.filter(o => o.available && !o.degraded).length;
  const deg = ops.filter(o => o.degraded).length;
  const prog = ops.filter(o => o.progressing && !o.degraded).length;
  document.getElementById('co-summary').innerHTML = `
    <span class="badge badge-ok">${ok} OK</span>
    ${deg ? `<span class="badge badge-err">${deg} Degraded</span>` : ''}
    ${prog ? `<span class="badge badge-warn">${prog} Progressing</span>` : ''}`;
  document.getElementById('op-grid').innerHTML = ops.sort((a,b) =>
    (b.degraded||0)-(a.degraded||0) || (b.progressing||0)-(a.progressing||0) || a.name.localeCompare(b.name)
  ).map(op => {
    const c = op.degraded ? 'var(--danger)' : op.progressing ? 'var(--warn)' : op.available ? 'var(--success)' : 'var(--txt-muted)';
    const d = op.degraded ? 'dot-err' : op.progressing ? 'dot-warn' : op.available ? 'dot-ok' : 'dot-off';
    return `<div class="op-cell" title="${op.message||op.name}">
      <div style="display:flex;align-items:center;gap:5px;">
        <span class="dot ${d}"></span>
        <span class="op-name">${op.name}</span>
      </div>
      <div class="op-ver" style="color:${c};">${op.version}</div>
    </div>`;
  }).join('');

  // MCP
  document.getElementById('mcp-grid').innerHTML = (infra.mcp_pools || []).map(p => {
    const ok = p.ready_count === p.machine_count && p.machine_count > 0;
    const dg = p.degraded_count > 0;
    const bc = dg ? 'var(--danger)' : ok ? 'var(--success)' : 'var(--warn)';
    return `<div class="card-sm" style="border-color:${bc}22;">
      <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
        <span style="font-size:12px;font-weight:700;">${p.name}</span>
        <span style="font-size:10px;font-weight:700;color:${bc};">${dg?'Degraded':ok?'Ready':'Updating'}</span>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;">
        ${[['Total',p.machine_count,'var(--txt-primary)'],['Ready',p.ready_count,'var(--success)'],
           ['Updated',p.updated_count,'var(--accent2)'],['Degraded',p.degraded_count,dg?'var(--danger)':'var(--txt-muted)']
          ].map(([l,v,c])=>`<div style="text-align:center;padding:4px;background:var(--bg-base);border-radius:4px;">
          <div style="font-size:9px;color:var(--txt-muted);">${l}</div>
          <div style="font-size:14px;font-weight:700;color:${c};">${v}</div>
        </div>`).join('')}
      </div>
    </div>`;
  }).join('') || '<div style="color:var(--txt-muted);font-size:12px;">MachineConfigPool 없음</div>';
}

// ═══════════════════════════════════════════════
// RENDER: PERFORMANCE
// ═══════════════════════════════════════════════
function renderPerformance(d) {
  if (!d) return;
  const p = d.perf;
  const lbl = p.time_labels || [];
  if (p.etcd_wal_p99 && p.etcd_wal_p99.length)
    mkChart('chart-etcd-wal', lbl, [sparkDataset('WAL p99 (ms)', p.etcd_wal_p99.map(v=>v*1000), '#f87171')]);
  if (p.etcd_peer_rtt && p.etcd_peer_rtt.length)
    mkChart('chart-etcd-rtt', lbl, [sparkDataset('RTT p99 (ms)', p.etcd_peer_rtt.map(v=>v*1000), '#fb923c')]);
  if (p.api_req_rate && p.api_req_rate.length)
    mkChart('chart-api-req', lbl, [sparkDataset('Req/s', p.api_req_rate, '#60a5fa')]);
  if (p.api_err_rate && p.api_err_rate.length)
    mkChart('chart-api-err', lbl, [sparkDataset('5xx/s', p.api_err_rate, '#f87171')]);
  if (p.api_latency_p99 && p.api_latency_p99.length)
    mkChart('chart-api-lat', lbl, [sparkDataset('Latency p99 (ms)', p.api_latency_p99.map(v=>v*1000), '#a78bfa')]);
}

// ═══════════════════════════════════════════════
// RENDER: COLLECTOR HEALTH
// ═══════════════════════════════════════════════
function renderCollector(d) {
  const ps = d.prometheus;
  const po = d.polling;

  // Prometheus status
  const stratColor = ps.strategy === 'raw' ? 'var(--success)' :
                     ps.strategy === 'route' ? 'var(--accent)' : 'var(--danger)';
  document.getElementById('prom-status-panel').innerHTML = `
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;">
      <span class="dot ${ps.is_reachable?'dot-ok':'dot-err'}"></span>
      <span style="font-size:14px;font-weight:700;color:${ps.is_reachable?'var(--success)':'var(--danger)'};">
        ${ps.is_reachable ? 'Reachable' : 'Unreachable'}
      </span>
    </div>
    ${[
      ['Strategy', `<span style="font-size:11px;font-weight:700;color:${stratColor};font-family:monospace;">${ps.strategy.toUpperCase()}</span>`],
      ['Host', `<span class="mono" style="font-size:10px;color:var(--txt-secondary);">${ps.host||'—'}</span>`],
      ['Last Ping', ps.last_ping_at],
      ['Latency', `<span style="color:${ps.last_ping_latency_ms>500?'var(--danger)':ps.last_ping_latency_ms>200?'var(--warn)':'var(--success)'};">${ps.last_ping_latency_ms.toFixed(0)} ms</span>`],
    ].map(([l,v])=>`<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid var(--bd);">
      <span style="font-size:11px;color:var(--txt-muted);">${l}</span>
      <span style="font-size:11px;">${v}</span>
    </div>`).join('')}
    <div style="margin-top:10px;">
      <div style="font-size:10px;color:var(--txt-muted);margin-bottom:6px;">Matched Jobs</div>
      ${Object.entries(ps.matched_jobs||{}).map(([k,v])=>`
        <div style="display:flex;justify-content:space-between;font-size:10px;padding:2px 0;">
          <span style="color:var(--txt-secondary);">${k}</span>
          <span class="mono" style="color:var(--accent2);">${v}</span>
        </div>`).join('') || '<span style="font-size:11px;color:var(--txt-muted);">없음</span>'}
    </div>`;

  // Ping latency history chart
  const pings = (d.prometheus_ping_history || []).slice().reverse();
  const maxLat = Math.max(...pings.map(p => p.latency_ms || 0), 100);
  document.getElementById('ping-history-chart').innerHTML = pings.map(p => {
    const h = Math.max(4, Math.round((p.latency_ms / maxLat) * 72));
    const c = !p.is_reachable ? 'var(--danger)' : p.latency_ms > 500 ? 'var(--warn)' : 'var(--success)';
    return `<div class="ping-tick" style="height:${h}px;background:${c};flex-shrink:0;width:10px;" title="${p.timestamp}: ${p.latency_ms?.toFixed(0)}ms (${p.is_reachable?'ok':'fail'})"></div>`;
  }).join('');

  // Polling KPIs
  document.getElementById('polling-kpis').innerHTML = [
    { label: 'Total Cycles', value: po.total_cycles, color: 'var(--txt-primary)' },
    { label: 'Success Rate', value: `${(po.success_rate_pct||0).toFixed(1)}%`, color: po.success_rate_pct >= 90 ? 'var(--success)' : po.success_rate_pct >= 70 ? 'var(--warn)' : 'var(--danger)' },
    { label: 'Interval', value: `${po.interval_seconds}s`, color: 'var(--accent)' },
    { label: 'Last Duration', value: po.last_cycle ? `${Math.round(po.last_cycle.duration_ms || 0)}ms` : '—', color: 'var(--txt-secondary)' },
    { label: 'Last Status', value: po.last_cycle?.status || '—', color: po.last_cycle?.status === 'success' ? 'var(--success)' : 'var(--warn)' },
  ].map(k => `<div class="card-sm" style="text-align:center;">
    <div style="font-size:10px;color:var(--txt-muted);margin-bottom:4px;">${k.label}</div>
    <div style="font-size:18px;font-weight:700;color:${k.color};font-family:monospace;">${k.value}</div>
  </div>`).join('');

  // Cycle timeline
  const cycles = (po.recent_cycles || []).slice().reverse();
  document.getElementById('cycle-timeline').innerHTML = cycles.map(c => {
    const color = c.status === 'success' ? 'var(--success)' : c.status === 'partial' ? 'var(--warn)' :
                  c.status === 'failed' ? 'var(--danger)' : 'var(--txt-muted)';
    const h = c.duration_ms ? Math.max(8, Math.min(40, c.duration_ms / 500)) : 20;
    return `<div class="cycle-bar" style="height:${h}px;background:${color};"
      title="#${c.cycle_id} ${c.started_at} — ${c.status} (${Math.round(c.duration_ms||0)}ms)"></div>`;
  }).join('');

  // Metric stats table
  document.getElementById('tbody-metric-stats').innerHTML = (d.metric_summary || []).map(m => {
    const sc = m.success_rate >= 90 ? 'badge-ok' : m.success_rate >= 50 ? 'badge-warn' : 'badge-err';
    return `<tr>
      <td class="mono" style="font-size:11px;">${m.name}</td>
      <td><div class="pbar-wrap">
        <div class="pbar"><div class="pbar-fill ${pbarColor(100-m.success_rate)}" style="width:${m.success_rate}%"></div></div>
        <span class="badge ${sc}" style="margin-left:4px;">${m.success_rate.toFixed(0)}%</span>
      </div></td>
      <td class="mono" style="font-size:11px;">${m.avg_ms.toFixed(0)}ms</td>
      <td style="color:var(--success);">${m.ok}</td>
      <td style="color:${m.failed>0?'var(--danger)':'var(--txt-muted)'};">${m.failed}</td>
    </tr>`;
  }).join('');

  // DB stats
  const db = d.database || {};
  document.getElementById('db-stats-panel').innerHTML = `
    <div style="margin-bottom:10px;">
      <div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid var(--bd);">
        <span style="font-size:11px;color:var(--txt-muted);">File</span>
        <span class="mono" style="font-size:10px;">${db.file}</span>
      </div>
      <div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid var(--bd);">
        <span style="font-size:11px;color:var(--txt-muted);">Size</span>
        <span style="font-size:11px;font-weight:700;">${fmtBytes(db.size_bytes||0)}</span>
      </div>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;">
      ${Object.entries(db.tables||{}).map(([tbl,s])=>`
        <div style="background:var(--bg-base);border-radius:5px;padding:6px;">
          <div style="font-size:9px;color:var(--txt-muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${tbl.replace(/_/g,' ')}</div>
          <div style="font-size:13px;font-weight:700;color:var(--accent2);">${s.row_count}</div>
        </div>`).join('')}
    </div>`;
}

// ═══════════════════════════════════════════════
// RENDER ALL
// ═══════════════════════════════════════════════
function renderAll(d) {
  if (!d) return;
  // Sidebar
  document.getElementById('sb-version').textContent = `OCP ${d.ocp_version}`;
  const ps = _engine_prom_reachable;
  document.getElementById('sb-prom-dot').className = `dot ${ps?'dot-ok':'dot-off'}`;
  document.getElementById('sb-updated').textContent = d.last_updated;

  renderOverview(d);
  renderNodes(d.nodes || []);
  renderVMs(d.vms || []);
  renderStorage(d.storage || {});
  renderInfra(d.infra || {});
  if (state.currentPage === 'performance') renderPerformance(d);
}

let _engine_prom_reachable = false;

// ═══════════════════════════════════════════════
// AUTO-REFRESH + COUNTDOWN
// ═══════════════════════════════════════════════
function startCountdown() {
  state.countdown = POLL_INTERVAL;
  if (state.timer) clearInterval(state.timer);
  state.timer = setInterval(() => {
    state.countdown--;
    document.getElementById('countdown').textContent = `${state.countdown}s`;
    if (state.countdown <= 0) {
      state.countdown = POLL_INTERVAL;
      fetchAll();
      if (state.currentPage === 'collector') fetchCollector();
    }
  }, 1000);
}
const POLL_INTERVAL = 30;

// ═══════════════════════════════════════════════
// INIT
// ═══════════════════════════════════════════════
async function init() {
  // health 체크로 서버 연결 먼저 확인
  for (let attempt = 0; attempt < 12; attempt++) {
    try {
      const h = await fetch('/api/v1/health').then(r => r.ok ? r.json() : null);
      if (h) {
        document.getElementById('loading').style.display = 'none';
        document.getElementById('app').style.display = '';
        startCountdown();
        fetchAll();  // 비동기로 데이터 로드 시작
        return;
      }
    } catch(e) { /* 서버 아직 준비 안됨 */ }
    // 2초 대기 후 재시도
    await new Promise(res => setTimeout(res, 2000));
  }
  document.getElementById('loading').innerHTML =
    '<div style="text-align:center;">' +
    '<div style="color:var(--danger);font-size:14px;margin-bottom:8px;">⚠ 서버 연결 실패</div>' +
    '<div style="color:var(--txt-muted);font-size:12px;">oc login 상태 및 포탈 기동 여부를 확인하세요.</div>' +
    '</div>';
}
init();
</script>
</body>
</html>"""

# ════════════════════════════════════════════════════════════════════
# 9. ENTRYPOINT
# ════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("AIBox Unified Monitoring Portal v5 시작")
    logger.info("  DB      : %s (retention %dh)", DB_FILE, DB_RETENTION_HOURS)
    logger.info("  Interval: %ds", POLL_INTERVAL)
    logger.info("  Port    : 8000")
    logger.info("=" * 60)
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=False,
    )

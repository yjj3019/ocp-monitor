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
    disk_used_bytes: Optional[int] = None
    disk_capacity_bytes: Optional[int] = None
    disk_pct: Optional[float] = None
    imagefs_pct: Optional[float] = None
    imagefs_used_bytes: Optional[int] = None
    imagefs_capacity_bytes: Optional[int] = None
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
                    node_name TEXT, cpu_pct REAL, mem_pct REAL, status TEXT, net_rx_bps REAL DEFAULT NULL, net_tx_bps REAL DEFAULT NULL, disk_pct REAL DEFAULT NULL, imagefs_pct REAL DEFAULT NULL
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

                CREATE TABLE IF NOT EXISTS route_probe_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    route_name TEXT, namespace TEXT, host TEXT,
                    status_code INTEGER, latency_ms REAL, is_up INTEGER,
                    error_msg TEXT DEFAULT ""
                );
                CREATE INDEX IF NOT EXISTS idx_rph_ts ON route_probe_history(timestamp);

                CREATE TABLE IF NOT EXISTS alert_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    alert_name TEXT, severity TEXT, namespace TEXT,
                    state TEXT, summary TEXT, description TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_ah_ts ON alert_history(timestamp);

                CREATE TABLE IF NOT EXISTS incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    resolved_at TEXT,
                    title TEXT, severity TEXT, status TEXT DEFAULT "active",
                    affected TEXT, description TEXT
                );
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
                    "INSERT INTO node_metrics_history (timestamp,node_name,cpu_pct,mem_pct,status,net_rx_bps,net_tx_bps,disk_pct,imagefs_pct) VALUES (?,?,?,?,?,?,?,?,?)",
                    (now, n.name, n.cpu_pct_realtime, n.memory_pct_realtime, n.status,
                     getattr(n,"net_rx_bps",None), getattr(n,"net_tx_bps",None), getattr(n,"disk_pct",None), getattr(n,"imagefs_pct",None)),
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
                    (cycle_id, r.name, r.status, (int(r.count) if isinstance(r.count, int) else (len(r.count) if hasattr(r.count, "__len__") else 0)), r.duration_ms, r.error),
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

    # ── Route probe ───────────────────────────────────────────
    def store_route_probes(self, probes: List[Dict]) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cutoff = (datetime.now() - timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            c = conn.cursor()
            for p in probes:
                c.execute(
                    "INSERT INTO route_probe_history "
                    "(timestamp,route_name,namespace,host,status_code,latency_ms,is_up,error_msg) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (now, p.get("name",""), p.get("namespace",""), p.get("host",""),
                     p.get("status_code", 0), p.get("latency_ms", 0),
                     int(p.get("is_up", 0)), p.get("error", "")),
                )
            c.execute("DELETE FROM route_probe_history WHERE timestamp <= ?", (cutoff,))

    def get_route_slo(self, hours: int = 24) -> List[Dict]:
        cutoff = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT route_name, namespace, host, "
                "COUNT(*) as total, SUM(is_up) as up_count, "
                "AVG(latency_ms) as avg_latency, "
                "MIN(latency_ms) as min_latency, MAX(latency_ms) as max_latency "
                "FROM route_probe_history WHERE timestamp > ? "
                "GROUP BY route_name, namespace, host",
                (cutoff,)
            ).fetchall()
        result = []
        for r in rows:
            total = r["total"] or 1
            result.append({
                "name": r["route_name"], "namespace": r["namespace"], "host": r["host"],
                "total_probes": total, "up_count": r["up_count"] or 0,
                "slo_pct": round((r["up_count"] or 0) / total * 100, 2),
                "avg_latency_ms": round(r["avg_latency"] or 0, 1),
                "min_latency_ms": round(r["min_latency"] or 0, 1),
                "max_latency_ms": round(r["max_latency"] or 0, 1),
            })
        return sorted(result, key=lambda x: x["slo_pct"])

    def get_route_trend(self, route_name: str, hours: int = 1) -> Dict:
        cutoff = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT timestamp, latency_ms, is_up, status_code FROM route_probe_history "
                "WHERE route_name=? AND timestamp > ? ORDER BY timestamp ASC",
                (route_name, cutoff)
            ).fetchall()
        return {
            "labels": [r["timestamp"][11:16] for r in rows],
            "latency": [r["latency_ms"] for r in rows],
            "is_up": [r["is_up"] for r in rows],
            "status_code": [r["status_code"] for r in rows],
        }

    # ── Alert history ─────────────────────────────────────────
    def store_alerts(self, alerts: List[Dict]) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cutoff = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            c = conn.cursor()
            for a in alerts:
                c.execute(
                    "INSERT INTO alert_history "
                    "(timestamp,alert_name,severity,namespace,state,summary,description) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (now, a.get("alertname",""), a.get("severity","none"),
                     a.get("namespace","cluster"), a.get("alertstate","firing"),
                     a.get("summary","")[:200], a.get("description","")[:300]),
                )
            c.execute("DELETE FROM alert_history WHERE timestamp <= ?", (cutoff,))

    def get_alert_history_trend(self, hours: int = 1) -> Dict:
        cutoff = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT timestamp, COUNT(*) as cnt, "
                "SUM(CASE WHEN severity='critical' THEN 1 ELSE 0 END) as crit "
                "FROM alert_history WHERE timestamp > ? "
                "GROUP BY substr(timestamp,1,16) ORDER BY timestamp ASC",
                (cutoff,)
            ).fetchall()
        return {
            "labels": [r["timestamp"][11:16] for r in rows],
            "total": [r["cnt"] for r in rows],
            "critical": [r["crit"] for r in rows],
        }

    # ── Incidents ─────────────────────────────────────────────
    def upsert_incident(self, title: str, severity: str, affected: str, desc: str) -> int:
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT id FROM incidents WHERE title=? AND status='active'", (title,)
            ).fetchone()
            if existing:
                return existing["id"]
            c = conn.cursor()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            c.execute(
                "INSERT INTO incidents (started_at,title,severity,status,affected,description) "
                "VALUES (?,?,?,?,?,?)",
                (now, title, severity, "active", affected, desc)
            )
            return c.lastrowid

    def resolve_incident(self, title: str) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            conn.cursor().execute(
                "UPDATE incidents SET status='resolved', resolved_at=? WHERE title=? AND status='active'",
                (now, title)
            )

    def get_incidents(self, limit: int = 20) -> List[Dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM incidents ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_node_trend(self, node_name: str, hours: int = 1) -> Dict[str, Any]:
        """node_metrics_history에서 노드별 CPU/MEM 시계열 반환 (oc top 기반)"""
        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            try:
                rows = conn.execute(
                    "SELECT timestamp, cpu_pct, mem_pct, net_rx_bps, net_tx_bps, disk_pct, imagefs_pct FROM node_metrics_history "
                    "WHERE node_name=? AND timestamp > ? ORDER BY timestamp ASC",
                    (node_name, cutoff),
                ).fetchall()
                net_rx = [r["net_rx_bps"] for r in rows]
                net_tx = [r["net_tx_bps"] for r in rows]
            except Exception:
                rows = conn.execute(
                    "SELECT timestamp, cpu_pct, mem_pct, net_rx_bps, net_tx_bps, disk_pct FROM node_metrics_history "
                    "WHERE node_name=? AND timestamp > ? ORDER BY timestamp ASC",
                    (node_name, cutoff),
                ).fetchall()
                net_rx = []
                net_tx = []
        labels = [r["timestamp"][11:16] for r in rows]
        cpu    = [r["cpu_pct"] for r in rows]
        mem    = [r["mem_pct"] for r in rows]
        return {"labels": labels, "cpu": cpu, "mem": mem,
                "net_rx": net_rx, "net_tx": net_tx, "disk_pct": [r["disk_pct"] if "disk_pct" in r.keys() else None for r in rows], "imagefs_pct": [r["imagefs_pct"] if "imagefs_pct" in r.keys() else None for r in rows],
                "net_rx": [r["net_rx_bps"] if "net_rx_bps" in r.keys() else None for r in rows], "net_tx": [r["net_tx_bps"] if "net_tx_bps" in r.keys() else None for r in rows], "source": "sqlite_cli", "node": node_name,
                "point_count": len(rows)}

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

    def _cluster_net_rx(self) -> float:
        try:
            r = self._query("sum(rate(netobserv_node_ingress_bytes_total[5m]))")
            return float(r[0]["value"][1]) if r else 0.0
        except: return 0.0

    def _cluster_net_tx(self) -> float:
        try:
            r = self._query("sum(rate(netobserv_node_egress_bytes_total[5m]))")
            return float(r[0]["value"][1]) if r else 0.0
        except: return 0.0

    def _query_direct(self, promql: str, timeout: int = 15) -> List[Dict]:
        """클러스터 Prometheus 직접 쿼리 (Thanos 미노출 메트릭용)"""
        import subprocess as _sp, json as _j, urllib.parse as _up
        try:
            encoded = _up.quote(promql)
            raw = _sp.run(
                ["oc","exec","-n","openshift-monitoring","prometheus-k8s-0",
                 "-c","prometheus","--",
                 "wget","-qO-",f"http://localhost:9090/api/v1/query?query={encoded}"],
                capture_output=True, text=True, timeout=timeout
            )
            if raw.returncode != 0 or not raw.stdout.strip():
                return []
            data = _j.loads(raw.stdout)
            return data.get("data", {}).get("result", [])
        except Exception as _e:
            logger.debug("direct query 실패: %s", _e)
            return []

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
                if len(p) >= 5:
                    try: cpu_pct_val = float(p[2].rstrip("%"))
                    except Exception: cpu_pct_val = None
                    try: mem_pct_val = float(p[4].rstrip("%"))
                    except Exception: mem_pct_val = None
                    nu[p[0]] = {"cpu": p[1], "mem": p[3],
                                "cpu_pct": cpu_pct_val, "mem_pct": mem_pct_val}
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
                    name=name, cpu_usage=usage.get("cpu","N/A"),
                    memory_usage=usage.get("mem","N/A"),
                    status=status, roles=roles,
                    age=self._fmt_age(node["metadata"]["creationTimestamp"]),
                    memory_bytes=cap_b,
                    cpu_pct_realtime=usage.get("cpu_pct"),
                    memory_pct_realtime=usage.get("mem_pct"),
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
                     "pvc": v.get("persistentVolumeClaim", {}).get("claimName") or v.get("dataVolume", {}).get("name") or "N/A"}
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
            "alerts": 'count(ALERTS{alertstate="firing",alertname!="Watchdog",alertname!~"InfoInhibitor.*"}) or vector(0)',
            "cpu": 'clamp_min((1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))) * 100, 0)',
            "mem": 'clamp_min((1 - sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes)) * 100, 0)',
        })
        with ThreadPoolExecutor(max_workers=max(len(queries), 1)) as ex:
            futs = {ex.submit(self._query_direct, q): k for k, q in queries.items()}
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
        if not self.metrics.nodes or not self._prom_strategy: return 0
        test = self._query('count by(node) (node_cpu_seconds_total{mode="idle"})')
        has_node = bool(test and test[0].get("metric", {}).get("node"))

        # node_network_* 는 node 레이블 없이 instance(IP) 만 있는 경우 대비
        test_net = self._query('count by(node) (node_network_receive_bytes_total{device="eth0"})')
        has_node_net = bool(test_net and test_net[0].get("metric", {}).get("node"))

        ip_map: Dict[str, str] = {}
        if not has_node or not has_node_net:
            for item in self._query("kube_node_info"):
                lbl = item.get("metric", {})
                if lbl.get("node") and lbl.get("internal_ip"):
                    ip_map[lbl["internal_ip"]] = lbl["node"]
            logger.info("노드 IP 매핑: %d개", len(ip_map))

        # 네트워크 필터: lo, veth, ovn, br, cali 제외
        NET_EXCL = "lo|veth.*|ovn.*|br-.*|cali.*|tunl.*|flannel.*"
        queries: Dict[str, str] = {
            "cpu": 'clamp_min((1 - avg by(instance,node) (rate(node_cpu_seconds_total{mode="idle"}[5m]))) * 100, 0)',
            "mem": 'clamp_min((1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100, 0)',
            "net_rx": f'sum by(node,instance) (rate(node_network_receive_bytes_total{{device!~"{NET_EXCL}"}}[5m]))',
            "net_tx": f'sum by(node,instance) (rate(node_network_transmit_bytes_total{{device!~"{NET_EXCL}"}}[5m]))',
        }
        nrt: Dict[str, Dict[str, float]] = {}
        with ThreadPoolExecutor(max_workers=len(queries)) as ex:
            futs = {ex.submit(self._query, q): k for k, q in queries.items()}
            for fut in as_completed(futs):
                k = futs[fut]
                for item in fut.result():
                    lbl = item.get("metric", {})
                    name = (lbl.get("node", "")
                            or ip_map.get(lbl.get("instance", "").split(":")[0], ""))
                    if not name: continue
                    try:
                        val = float(item["value"][1])
                        import math
                        if math.isnan(val) or math.isinf(val): continue
                        # 같은 노드에서 여러 device 합산 (net_rx/tx)
                        if k in ("net_rx", "net_tx"):
                            nrt.setdefault(name, {})[k] = nrt.get(name, {}).get(k, 0) + val
                        else:
                            nrt.setdefault(name, {})[k] = val
                    except Exception: pass

        # ── NetObserv 노드 네트워크 ──────────────────────────────
        netobserv_rx: dict = {}
        netobserv_tx: dict = {}
        try:
            for item in self._query('sum by(SrcK8S_HostName)(rate(netobserv_node_ingress_bytes_total[5m]))'):
                node = item.get("metric", {}).get("SrcK8S_HostName", "")
                if node:
                    try: netobserv_rx[node] = float(item["value"][1])
                    except Exception: pass
            for item in self._query('sum by(SrcK8S_HostName)(rate(netobserv_node_egress_bytes_total[5m]))'):
                node = item.get("metric", {}).get("SrcK8S_HostName", "")
                if node:
                    try: netobserv_tx[node] = float(item["value"][1])
                    except Exception: pass
            if netobserv_rx:
                logger.info("NetObserv 노드 네트워크: %d개 수집", len(netobserv_rx))
        except Exception as e:
            logger.debug("NetObserv 노드 쿼리 실패: %s", e)
        for n in self.metrics.nodes:
            d = nrt.get(n.name, {})
            # Prometheus 값 우선, 없으면 oc top 값 유지
            if d.get("cpu") is not None:
                n.cpu_pct_realtime = d["cpu"]
            if d.get("mem") is not None:
                n.memory_pct_realtime = d["mem"]
            # kubelet stats API 디스크 수집
            try:
                _dr = self._run(["oc","get","--raw",f"/api/v1/nodes/{n.name}/proxy/stats/summary"],timeout=10,silent=True)
                if _dr:
                    _ds = __import__("json").loads(_dr)
                    _fs = _ds.get("node",{}).get("fs",{})
                    if _fs.get("capacityBytes",0) > 0:
                        n.disk_used_bytes = _fs.get("usedBytes",0)
                        n.disk_capacity_bytes = _fs.get("capacityBytes",0)
                        n.disk_pct = round(_fs["usedBytes"]/_fs["capacityBytes"]*100,1)
                        _ifs = _ds.get("node",{}).get("runtime",{}).get("imageFs",{})
                        if _ifs.get("capacityBytes",0) > 0:
                            n.imagefs_pct = round(_ifs["usedBytes"]/_ifs["capacityBytes"]*100,1)
                            n.imagefs_used_bytes = _ifs.get("usedBytes",0)
                            n.imagefs_capacity_bytes = _ifs.get("capacityBytes",0)
            except Exception: pass
            n.net_rx_bps = netobserv_rx.get(n.name) or d.get("net_rx")
            n.net_tx_bps = netobserv_tx.get(n.name) or d.get("net_tx")

        # ── Ephemeral Storage 용량 알럿 ──────────────────────
        try:
            eph_cap = {}
            for item in self._query('kube_node_status_capacity{resource="ephemeral_storage"}'):
                node = item.get("metric",{}).get("node","")
                if node:
                    try: eph_cap[node] = int(float(item["value"][1]))
                    except: pass
            eph_alloc = {}
            for item in self._query('kube_node_status_allocatable{resource="ephemeral_storage"}'):
                node = item.get("metric",{}).get("node","")
                if node:
                    try: eph_alloc[node] = int(float(item["value"][1]))
                    except: pass
            for n in self.metrics.nodes:
                if n.name in eph_cap:
                    n.__dict__['eph_capacity_bytes'] = eph_cap[n.name]
                    n.__dict__['eph_allocatable_bytes'] = eph_alloc.get(n.name, 0)
            if eph_cap:
                logger.info("Ephemeral storage 용량: %d개 노드", len(eph_cap))
        except Exception as _e:
            logger.debug("Ephemeral storage 쿼리 실패: %s", _e)

                # 디스크 90% 초과 경고
        for n in self.metrics.nodes:
            dp = getattr(n, 'disk_pct', None)
            ip = getattr(n, 'imagefs_pct', None)
            if dp and dp > 90:
                logger.warning("⚠ %s nodefs %.1f%% 초과!", n.name, dp)
            if ip and ip > 90:
                logger.warning("⚠ %s imagefs %.1f%% 초과!", n.name, ip)

        logger.info("노드 수집 — CPU/MEM: Metrics API ✅ | Prometheus node-exporter: %d개(미노출 정상) | NetObserv Net: rx=%d tx=%d노드",
                    len(nrt), len(netobserv_rx), len(netobserv_tx))
        return len(nrt)

    def fetch_vm_realtime(self) -> int:
        if not self.metrics.vms or not self._prom_strategy: return 0
        # NetObserv workload 메트릭으로 VM 네트워크 보완
        netobserv_vm_rx: dict = {}
        netobserv_vm_tx: dict = {}
        try:
            for item in self._query(
                'sum by(SrcK8S_Namespace,SrcK8S_OwnerName)'
                '(rate(netobserv_workload_ingress_bytes_total[5m]))'
            ):
                m = item.get("metric", {})
                key = (m.get("SrcK8S_Namespace",""), m.get("SrcK8S_OwnerName",""))
                if key[0] and key[1]:
                    try: netobserv_vm_rx[key] = float(item["value"][1])
                    except Exception: pass
            for item in self._query(
                'sum by(SrcK8S_Namespace,SrcK8S_OwnerName)'
                '(rate(netobserv_workload_egress_bytes_total[5m]))'
            ):
                m = item.get("metric", {})
                key = (m.get("SrcK8S_Namespace",""), m.get("SrcK8S_OwnerName",""))
                if key[0] and key[1]:
                    try: netobserv_vm_tx[key] = float(item["value"][1])
                    except Exception: pass
            if netobserv_vm_rx:
                logger.info("NetObserv VM 네트워크: %d개 수집", len(netobserv_vm_rx))
        except Exception as e:
            logger.debug("NetObserv VM 쿼리 실패: %s", e)

        # ── kubevirt 메트릭 직접 수집 ──────────────────────────────
        rt: Dict[Tuple[str, str], Dict[str, float]] = {}
        import threading as _th
        _rt_lock = _th.Lock()

        def _kv_fetch(promql: str, key: str):
            # vcpu는 job 필터 없이, 나머지는 kubevirt-prometheus-metrics job
            if "vcpu" in promql:
                ql = promql
            else:
                ql = f'{promql}{{job="kubevirt-prometheus-metrics"}}'
            for item in self._query_direct(ql):
                lbl = item.get("metric", {})
                name = lbl.get("name", "")
                ns   = lbl.get("namespace", "")
                if not name or not ns: continue
                try:
                    val = float(item["value"][1])
                    with _rt_lock:
                        rt.setdefault((name, ns), {})[key] = val
                except Exception: pass

        with ThreadPoolExecutor(max_workers=7) as ex:
            _futs = [
                ex.submit(_kv_fetch, "rate(kubevirt_vmi_vcpu_seconds_total[5m]) * 100", "cpu_pct"),
                ex.submit(_kv_fetch, "kubevirt_vmi_memory_used_bytes", "mem_used"),
                ex.submit(_kv_fetch, "kubevirt_vmi_memory_available_bytes", "mem_avail"),
                ex.submit(_kv_fetch, "rate(kubevirt_vmi_network_receive_bytes_total[5m])", "net_rx"),
                ex.submit(_kv_fetch, "rate(kubevirt_vmi_network_transmit_bytes_total[5m])", "net_tx"),
                ex.submit(_kv_fetch, "rate(kubevirt_vmi_storage_read_traffic_bytes_total[5m])", "disk_r"),
                ex.submit(_kv_fetch, "rate(kubevirt_vmi_storage_write_traffic_bytes_total[5m])", "disk_w"),
            ]
            for _f in as_completed(_futs):
                try: _f.result()
                except Exception: pass
        logger.info("VM 실시간 kubevirt: %d개 VM", len(rt))

        for vm in self.metrics.vms:
            d = rt.get((vm.name, vm.namespace), {})
            vm.cpu_usage_pct = d.get("cpu_pct")
            mu, ma = d.get("mem_used", 0), d.get("mem_avail", 0)
            if ma > 0:
                vm.memory_usage_pct = mu / ma * 100
                vm.memory_used_bytes = int(mu)
            # NetObserv 값 우선, 없으면 kubevirt 메트릭
            vm_key = (vm.namespace, vm.name)
            vm.net_rx_bps = netobserv_vm_rx.get(vm_key) or d.get("net_rx")
            vm.net_tx_bps = netobserv_vm_tx.get(vm_key) or d.get("net_tx")

        # ── VM 게스트 OS 파일시스템 수집 ──────────────────────────
        vm_fs_used: dict = {}
        vm_fs_cap: dict  = {}
        try:
            for item in self._query_direct('kubevirt_vmi_filesystem_used_bytes{job="kubevirt-prometheus-metrics"}'):
                m = item.get("metric", {})
                name = m.get("name", "")
                ns = m.get("namespace", "")
                key = f"{ns}/{name}"
                if name and ns:
                    try: vm_fs_used[key] = int(float(item["value"][1]))
                    except: pass
            for item in self._query_direct('kubevirt_vmi_filesystem_capacity_bytes{job="kubevirt-prometheus-metrics"}'):
                m = item.get("metric", {})
                key = m.get("name", "")
                if key:
                    try: vm_fs_cap[key] = int(float(item["value"][1]))
                    except: pass
            if vm_fs_used:
                logger.info("VM 파일시스템: %d개 수집", len(vm_fs_used))
        except Exception as _e:
            logger.debug("VM 파일시스템 쿼리 실패: %s", _e)
        for vm in self.metrics.vms:
            _fkey = f"{vm.namespace}/{vm.name}"
            if _fkey in vm_fs_used:
                vm.__dict__['fs_used_bytes'] = vm_fs_used[_fkey]
                vm.__dict__['fs_capacity_bytes'] = vm_fs_cap.get(_fkey, 0)
                cap = vm_fs_cap.get(_fkey, 0)
                vm.__dict__['fs_pct'] = round(vm_fs_used[_fkey]/cap*100, 1) if cap > 0 else None
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

    def fetch_vm_pvc_capacity(self) -> int:
        """VM volumes에 PVC 용량 매핑 (kube_persistentvolumeclaim)"""
        try:
            pvc_cap = {}
            for item in self._query(
                'kube_persistentvolumeclaim_resource_requests_storage_bytes'
            ):
                m = item.get("metric", {})
                ns = m.get("namespace", "")
                pvc = m.get("persistentvolumeclaim", "")
                if ns and pvc:
                    try: pvc_cap[(ns, pvc)] = int(float(item["value"][1]))
                    except: pass
            updated = 0
            for vm in self.metrics.vms:
                for vol in getattr(vm, 'volumes', []):
                    pvc_name = vol.get("pvc") or vol.get("pvc_name") or ""
                    if pvc_name and pvc_name != "N/A":
                        cap = pvc_cap.get((vm.namespace, pvc_name))
                        if cap:
                            vol["capacity_bytes"] = cap
                            vol["capacity"] = self._fmt_bytes(cap)
                            updated += 1
            if updated:
                logger.info("VM PVC 용량 매핑: %d개", updated)
            return updated
        except Exception as _e:
            logger.debug("VM PVC 용량 실패: %s", _e)
            return 0

    @staticmethod
    def _fmt_bytes(b: int) -> str:
        for unit in ["B","Ki","Mi","Gi","Ti"]:
            if b < 1024: return f"{b:.1f} {unit}"
            b //= 1024
        return f"{b} Pi"

    def fetch_vmi_spec(self) -> int:
        """VMI spec에서 CPU cores, Memory 수집"""
        import subprocess as _sp, json as _j
        try:
            r = _sp.run(["oc","get","vmi","-A","-o","json"],
                capture_output=True, text=True, timeout=20)
            if r.returncode != 0: return 0
            vmi_map = {}
            for item in _j.loads(r.stdout).get("items", []):
                meta = item.get("metadata", {})
                domain = item.get("spec", {}).get("domain", {})
                cpu = domain.get("cpu", {})
                cores = str(int(cpu.get("cores", cpu.get("sockets", 1)) or 1))
                mem = (domain.get("memory", {}).get("guest", "") or
                       domain.get("resources", {}).get("requests", {}).get("memory", "N/A"))
                vmi_map[(meta.get("namespace",""), meta.get("name",""))] = (cores, mem)
            updated = 0
            for vm in self.metrics.vms:
                info = vmi_map.get((vm.namespace, vm.name))
                if info:
                    if vm.cpu_cores in ("N/A", "", None): vm.cpu_cores = info[0]
                    if vm.memory_total in ("N/A", "", None): vm.memory_total = info[1]
                    updated += 1
            logger.info("VMI spec 보완: %d개 VM", updated)
            return updated
        except Exception as _e:
            logger.debug("VMI spec 실패: %s", _e)
            return 0

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
            ("pvc_disk_stats", self.fetch_pvc_disk_stats),
            ("alerts_detail", self.fetch_alerts_detail),
            ("route_probes", self.fetch_route_probes),
            ("vmi_spec", self.fetch_vmi_spec),
            ("vm_pvc", self.fetch_vm_pvc_capacity),
            ("uwm_status", self.fetch_uwm_status),
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

    def fetch_pvc_disk_stats(self) -> int:
        """VM별 PVC 실제 디스크 사용량 수집 (kubelet_volume_stats)"""
        if not self._prom_strategy: return 0
        queries = {
            "used":     "kubelet_volume_stats_used_bytes",
            "capacity": "kubelet_volume_stats_capacity_bytes",
        }
        ps: Dict[str, Dict[str, int]] = {}
        with ThreadPoolExecutor(max_workers=2) as ex:
            futs = {ex.submit(self._query, q): k for k, q in queries.items()}
            for fut in as_completed(futs):
                k = futs[fut]
                for item in fut.result():
                    lbl = item.get("metric", {})
                    pvc = lbl.get("persistentvolumeclaim", "")
                    ns  = lbl.get("namespace", "")
                    if not pvc: continue
                    key = f"{ns}/{pvc}"
                    try: ps.setdefault(key, {})[k] = int(float(item["value"][1]))
                    except Exception: pass
        self.metrics.pvc_disk_stats = ps
        # VM별 총 디스크 사용량 계산
        for vm in self.metrics.vms:
            total_used = 0
            for vol in vm.volumes:
                pvc = vol.get("pvc", "")
                if pvc and pvc != "N/A":
                    key = f"{vm.namespace}/{pvc}"
                    total_used += ps.get(key, {}).get("used", 0)
            vm.memory_used_bytes = vm.memory_used_bytes  # 기존 유지
            # disk_used_bytes를 net_rx_bps 재사용 대신 별도 필드로 저장
            # volumes[0]에 disk_used 주입
            for vol in vm.volumes:
                pvc = vol.get("pvc", "")
                if pvc and pvc != "N/A":
                    key = f"{vm.namespace}/{pvc}"
                    vol["used_bytes"] = ps.get(key, {}).get("used", 0)
                    vol["capacity_bytes"] = ps.get(key, {}).get("capacity", 0)
            vm._disk_used_bytes = total_used  # type: ignore[attr-defined]
        return len(ps)


    def query_node_trends(self, node_name: str, hours: int = 1) -> Dict[str, Any]:
        """노드별 on-demand 시계열 (CPU/Mem/Net/Disk)"""
        if not self._prom_strategy:
            return {}
        end_ts = int(time.time())
        start_ts = end_ts - hours * 3600
        step = "2m" if hours <= 1 else "5m"
        n = node_name.replace('"', '')

        queries = {
            "cpu":    f'clamp_min((1 - avg by(node) (rate(node_cpu_seconds_total{{mode="idle",node="{n}"}}[5m]))) * 100, 0)',
            "mem":    f'clamp_min((1 - node_memory_MemAvailable_bytes{{node="{n}"}} / node_memory_MemTotal_bytes{{node="{n}"}}) * 100, 0)',
            "net_rx": f'sum by(node) (rate(node_network_receive_bytes_total{{node="{n}",device!~"lo|veth.*|ovn.*|br.*"}}[5m]))',
            "net_tx": f'sum by(node) (rate(node_network_transmit_bytes_total{{node="{n}",device!~"lo|veth.*|ovn.*|br.*"}}[5m]))',
            "disk_r": f'sum by(node) (rate(node_disk_read_bytes_total{{node="{n}"}}[5m]))',
            "disk_w": f'sum by(node) (rate(node_disk_written_bytes_total{{node="{n}"}}[5m]))',
        }
        result: Dict[str, Any] = {"labels": []}
        import urllib.parse as _up
        with ThreadPoolExecutor(max_workers=len(queries)) as ex:
            futs = {ex.submit(self._query_range, q): k for k, q in queries.items()}
            for fut in as_completed(futs):
                k = futs[fut]
                vals, lbls = fut.result()
                import math
                vals = [None if (v is None or math.isnan(v) or math.isinf(v)) else v for v in vals]
                result[k] = vals
                if lbls and not result["labels"]:
                    result["labels"] = lbls
        return result

    def query_vm_trends(self, vm_name: str, namespace: str, hours: int = 1) -> Dict[str, Any]:
        """VM별 on-demand 시계열 (CPU/Mem/Net/Disk)"""
        if not self._prom_strategy:
            return {}
        n = vm_name.replace('"', '')
        ns = namespace.replace('"', '')

        queries = {
            "cpu":    f'rate(kubevirt_vmi_cpu_usage_seconds_total{{name="{n}",namespace="{ns}"}}[5m]) * 100',
            "mem_pct":f'clamp_min(kubevirt_vmi_memory_used_bytes{{name="{n}",namespace="{ns}"}} / kubevirt_vmi_memory_available_bytes{{name="{n}",namespace="{ns}"}} * 100, 0)',
            "net_rx": f'rate(kubevirt_vmi_network_receive_bytes_total{{name="{n}",namespace="{ns}"}}[5m])',
            "net_tx": f'rate(kubevirt_vmi_network_transmit_bytes_total{{name="{n}",namespace="{ns}"}}[5m])',
            "disk_r": f'rate(kubevirt_vmi_storage_read_traffic_bytes_total{{name="{n}",namespace="{ns}"}}[5m])',
            "disk_w": f'rate(kubevirt_vmi_storage_write_traffic_bytes_total{{name="{n}",namespace="{ns}"}}[5m])',
        }
        result: Dict[str, Any] = {"labels": []}
        with ThreadPoolExecutor(max_workers=len(queries)) as ex:
            futs = {ex.submit(self._query_range, q): k for k, q in queries.items()}
            for fut in as_completed(futs):
                k = futs[fut]
                vals, lbls = fut.result()
                import math
                vals = [None if (v is None or math.isnan(v) or math.isinf(v)) else v for v in vals]
                result[k] = vals
                if lbls and not result["labels"]:
                    result["labels"] = lbls
        return result

    def query_node_conditions(self) -> List[Dict]:
        """kube_node_status_condition 기반 노드별 상세 상태"""
        conditions = ["Ready", "MemoryPressure", "DiskPressure", "PIDPressure", "NetworkUnavailable"]
        result: Dict[str, Dict] = {}
        with ThreadPoolExecutor(max_workers=len(conditions)) as ex:
            futs = {
                ex.submit(self._query,
                    f'kube_node_status_condition{{condition="{c}",status="true"}}'): c
                for c in conditions
            }
            for fut in as_completed(futs):
                cond = futs[fut]
                for item in fut.result():
                    node = item.get("metric", {}).get("node", "")
                    if not node: continue
                    try:
                        val = float(item["value"][1])
                    except Exception:
                        val = 0
                    result.setdefault(node, {})[cond] = val
        return [
            {
                "node": node,
                "ready":               int(conds.get("Ready", 0)),
                "memory_pressure":     int(conds.get("MemoryPressure", 0)),
                "disk_pressure":       int(conds.get("DiskPressure", 0)),
                "pid_pressure":        int(conds.get("PIDPressure", 0)),
                "network_unavailable": int(conds.get("NetworkUnavailable", 0)),
            }
            for node, conds in sorted(result.items())
        ]

    # ─────────────────────────────────────────────────────────
    # 알럿 상세 수집
    # ─────────────────────────────────────────────────────────
    def fetch_alerts_detail(self) -> List[Dict]:
        """ALERTS 메트릭에서 firing 알럿 상세 목록 수집"""
        if not self._prom_strategy:
            return []
        results = self._query(
            'ALERTS{alertstate="firing",alertname!="Watchdog",alertname!~"InfoInhibitor.*"}'
        )
        alerts = []
        for item in results:
            lbl = item.get("metric", {})
            ann = lbl  # Thanos는 annotation을 label에 포함하기도 함
            alerts.append({
                "alertname":   lbl.get("alertname", "Unknown"),
                "severity":    lbl.get("severity", "none"),
                "namespace":   lbl.get("namespace", lbl.get("exported_namespace", "cluster")),
                "alertstate":  lbl.get("alertstate", "firing"),
                "service":     lbl.get("service", ""),
                "pod":         lbl.get("pod", ""),
                "node":        lbl.get("node", ""),
                "summary":     lbl.get("summary", ""),
                "description": lbl.get("description", ""),
            })
        # severity 순서: critical > warning > info > none
        sev_order = {"critical": 0, "warning": 1, "info": 2, "none": 3}
        alerts.sort(key=lambda a: (sev_order.get(a["severity"], 4), a["alertname"]))
        self.metrics._alerts_detail = alerts
        return alerts

    # ─────────────────────────────────────────────────────────
    # Route HTTP probe
    # ─────────────────────────────────────────────────────────
    def fetch_route_probes(self) -> List[Dict]:
        """oc get routes + HTTP probe로 각 Route 가용성/응답시간 측정"""
        import ssl, urllib.request, urllib.error
        raw = self._run(["oc", "get", "routes", "-A", "-o", "json"], timeout=20)
        if not raw:
            return []
        try:
            items = json.loads(raw).get("items", [])
        except Exception:
            return []

        # probe 함수
        def _probe(host: str, timeout: int = 5) -> Dict:
            url = f"https://{host}"
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            t0 = time.time()
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "AIBox-Monitor/1.0"})
                with urllib.request.urlopen(req, context=ctx, timeout=timeout) as resp:
                    latency = (time.time() - t0) * 1000
                    return {"status_code": resp.status, "latency_ms": latency, "is_up": True, "error": ""}
            except urllib.error.HTTPError as e:
                latency = (time.time() - t0) * 1000
                # 4xx도 route는 살아있음
                return {"status_code": e.code, "latency_ms": latency, "is_up": e.code < 500, "error": ""}
            except Exception as e:
                latency = (time.time() - t0) * 1000
                return {"status_code": 0, "latency_ms": latency, "is_up": False, "error": str(e)[:80]}

        # 주요 시스템 route 제외 (모니터링 자체 etc)
        SKIP_NS = {"openshift-monitoring", "openshift-logging", "openshift-tracing"}
        routes_to_probe = []
        for item in items:
            ns = item["metadata"]["namespace"]
            name = item["metadata"]["name"]
            host = item.get("spec", {}).get("host", "")
            if not host or ns in SKIP_NS:
                continue
            routes_to_probe.append({"name": name, "namespace": ns, "host": host})

        results = []
        with ThreadPoolExecutor(max_workers=min(len(routes_to_probe), 10)) as ex:
            futs = {ex.submit(_probe, r["host"]): r for r in routes_to_probe}
            for fut in as_completed(futs):
                r = futs[fut]
                try:
                    probe_result = fut.result()
                except Exception as e:
                    probe_result = {"status_code": 0, "latency_ms": 0, "is_up": False, "error": str(e)}
                results.append({**r, **probe_result})

        self.metrics._route_probes = results
        logger.info("Route probe: %d개 (up=%d)", len(results), sum(1 for r in results if r["is_up"]))
        return results

    # ─────────────────────────────────────────────────────────
    # User Workload Monitoring 감지
    # ─────────────────────────────────────────────────────────
    def fetch_uwm_status(self) -> Dict:
        """User Workload Monitoring 활성화 여부 및 상태 확인"""
        uwm = {"enabled": False, "prometheus_count": 0, "user_rules_count": 0, "targets": []}
        # UWM Prometheus 파드 확인
        raw = self._run(
            ["oc", "get", "pods", "-n", "openshift-user-workload-monitoring",
             "-o", "jsonpath={.items[*].metadata.name}"], timeout=10, silent=True
        )
        if raw:
            pods = raw.split()
            uwm["enabled"] = True
            uwm["prometheus_count"] = len([p for p in pods if "prometheus" in p])
        # UWM 수집 대상 (Prometheus 통해)
        if self._prom_strategy and uwm["enabled"]:
            targets = self._query('count by(namespace) (up{namespace!~"openshift-.*"})')
            uwm["targets"] = [
                {"namespace": t.get("metric", {}).get("namespace", ""), "count": int(float(t["value"][1]))}
                for t in targets if t.get("metric", {}).get("namespace")
            ]
        self.metrics._uwm_status = uwm
        return uwm

    # ─────────────────────────────────────────────────────────
    # Incident 자동 감지
    # ─────────────────────────────────────────────────────────
    def detect_incidents(self, db) -> List[Dict]:
        """알럿 + 노드 상태 기반 인시던트 자동 감지 및 SQLite 기록"""
        incidents_detected = []
        alerts = getattr(self.metrics, "_alerts_detail", [])
        nodes = self.metrics.nodes
        routes = getattr(self.metrics, "_route_probes", [])

        # Rule 1: Critical 알럿 → 인시던트
        for a in alerts:
            if a["severity"] == "critical":
                title = f"[CRITICAL] {a['alertname']}"
                affected = a.get("namespace") or a.get("node") or "cluster"
                db.upsert_incident(title, "critical", affected, a.get("description", ""))
                incidents_detected.append(title)

        # Rule 2: 노드 NotReady
        for n in nodes:
            if n.status != "Ready":
                title = f"[NODE] {n.name} NotReady"
                db.upsert_incident(title, "critical", n.name, f"노드 {n.name} 상태 이상: {n.status}")
                incidents_detected.append(title)

        # Rule 3: Route 다운 (5분 연속 실패)
        for r in routes:
            if not r.get("is_up") and r.get("latency_ms", 0) > 0:
                title = f"[ROUTE] {r['name']} Down"
                db.upsert_incident(title, "warning", r["namespace"], f"{r['host']} 응답 없음")
                incidents_detected.append(title)

        # 해소된 인시던트 resolve
        active = db.get_incidents(50)
        for inc in active:
            if inc["status"] == "active":
                if inc["title"] not in incidents_detected:
                    db.resolve_incident(inc["title"])

        return incidents_detected

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
                    "net_rx_bps": getattr(n,"net_rx_bps",None), "disk_pct": getattr(n,"disk_pct",None), "eph_capacity_bytes": n.__dict__.get("eph_capacity_bytes"), "eph_allocatable_bytes": n.__dict__.get("eph_allocatable_bytes"), "imagefs_pct": getattr(n,"imagefs_pct",None), "imagefs_used_bytes": getattr(n,"imagefs_used_bytes",None), "imagefs_capacity_bytes": getattr(n,"imagefs_capacity_bytes",None), "disk_used_bytes": getattr(n,"disk_used_bytes",None), "disk_capacity_bytes": getattr(n,"disk_capacity_bytes",None), "net_tx_bps": getattr(n,"net_tx_bps",None),
                }
                for n in m.nodes
            ],
            "node_data_source": getattr(m, "_node_data_source", "cli"),
            "node_net_available": getattr(m, "_node_net_available", False),
            "vms": [
                {
                    "name": vm.name, "namespace": vm.namespace, "status": vm.status,
                    "status_group": vm.status_group, "cpu_cores": vm.cpu_cores,
                    "memory_total": vm.memory_total, "node": vm.node,
                    "ip": vm.ip_address, "os": vm.os_info, "age": vm.creation_time,
                    "cpu_pct": vm.cpu_usage_pct, "mem_pct": vm.memory_usage_pct,
                    "net_rx_bps": vm.net_rx_bps, "net_tx_bps": vm.net_tx_bps,
                    "disk_r_bps": vm.disk_read_bps, "disk_w_bps": vm.disk_write_bps,
                    "disk_used_bytes": getattr(vm, "_disk_used_bytes", 0),
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
                "pvs": [
                    {**p, "capacity_bytes": MetricsCollector._parse_bytes(p.get("Capacity", "0"))}
                    for p in m.pv_data
                ],
                "pvcs": [
                    {
                        **p,
                        "used_bytes": m.pvc_disk_stats.get(
                            f"{p.get('Namespace','')}/{p.get('Name','')}", {}
                        ).get("used", 0),
                        "capacity_bytes": m.pvc_disk_stats.get(
                            f"{p.get('Namespace','')}/{p.get('Name','')}", {}
                        ).get("capacity", 0),
                    }
                    for p in m.pvc_data
                ],
                "pvc_disk_source": getattr(m, "_pvc_disk_source", "none"),
                "pvc_vm_map": {
                    f"{vm.namespace}/{vol.get('pvc','')}": vm.name
                    for vm in m.vms
                    for vol in vm.volumes
                    if vol.get("pvc") and vol.get("pvc") != "N/A"
                },
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
                self._cache = _sanitize_json(self.collector.get_metrics_snapshot())
            self._update_prom_status(*self.collector.ping_prometheus())
            logger.info("초기 수집 완료 — nodes=%d vms=%d", len(m.nodes), len(m.vms))
            try:
                HTMLReportBuilder().build(m, REPORT_HTML)
            except Exception as e:
                logger.warning("초기 HTML 리포트 생성 실패: %s", e)
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
            # item_count를 int로 강제 변환
            for _r in results:
                if isinstance(_r.count, (list, tuple)): _r.count = len(_r.count)
                elif isinstance(_r.count, dict): _r.count = len(_r.count)
                elif not isinstance(_r.count, int): _r.count = 0
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

            # 알럿/Route SQLite 저장 + Incident 감지
            try:
                alerts = getattr(self.collector.metrics, "_alerts_detail", [])
                if alerts:
                    self.db.store_alerts(alerts)
                routes = getattr(self.collector.metrics, "_route_probes", [])
                if routes:
                    self.db.store_route_probes(routes)
                self.collector.detect_incidents(self.db)
            except Exception as e:
                logger.warning("Alert/Route/Incident 저장 실패: %s", e)

            # HTML 리포트 생성 (Full Report 탭용)
            try:
                HTMLReportBuilder().build(self.collector.metrics, REPORT_HTML)
            except Exception as e:
                logger.warning("HTML 리포트 생성 실패: %s", e)

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
# 6b. HTML REPORT BUILDER  (vm_metrics_report.py 기능 통합)
# ════════════════════════════════════════════════════════════════════
class HTMLReportBuilder:
    """
    수집된 SystemMetrics를 받아 vm_metrics_report.py 스타일의
    정적 HTML 리포트를 생성한다.
    """

    @staticmethod
    def _fmt_bytes(b: int) -> str:
        if b == 0: return "0 B"
        names = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
        v, i = float(b), 0
        while v >= 1024 and i < len(names) - 1: v /= 1024.0; i += 1
        return f"{v:.2f} {names[i]}"

    @staticmethod
    def _fmt_pct(v) -> str:
        return f"{v:.1f}%" if v is not None else "N/A"

    @staticmethod
    def _pct_color(v) -> str:
        if v is None: return "#64748b"
        if v < 70: return "#10b981"
        if v < 90: return "#f59e0b"
        return "#ef4444"

    @staticmethod
    def _pct_bar(v, label="") -> str:
        c = HTMLReportBuilder._pct_color(v)
        w = f"{min(v,100):.1f}" if v is not None else "0"
        txt = HTMLReportBuilder._fmt_pct(v)
        return (f"<div style='margin-bottom:6px'>"
                f"<div style='display:flex;justify-content:space-between;font-size:11px;color:#94a3b8;margin-bottom:2px'>"
                f"<span>{escape(label)}</span><span style='color:{c};font-weight:700'>{txt}</span></div>"
                f"<div style='background:#1e293b;border-radius:3px;height:5px;overflow:hidden'>"
                f"<div style='background:{c};height:5px;border-radius:3px;width:{w}%'></div></div></div>")

    @staticmethod
    def _status_badge(sg: str) -> str:
        m = {"running": ("#10b981","Running"), "stopped": ("#64748b","Stopped"),
             "provisioning": ("#f59e0b","Provisioning"), "failed": ("#ef4444","Failed"),
             "unknown": ("#475569","Unknown")}
        c, t = m.get(sg, ("#475569","Unknown"))
        return (f"<span style='background:{c}22;color:{c};border:1px solid {c}55;"
                f"padding:1px 7px;border-radius:4px;font-size:10px;font-weight:700'>{t}</span>")

    def build(self, m: SystemMetrics, report_file: str = REPORT_HTML) -> None:
        h = m.cluster_health
        im = m.infra

        # ── 클러스터 오퍼레이터 요약 ──────────────────────────────
        ok_ops  = sum(1 for o in im.cluster_operators if o.available and not o.degraded)
        deg_ops = sum(1 for o in im.cluster_operators if o.degraded)
        prog_ops= sum(1 for o in im.cluster_operators if o.progressing and not o.degraded)

        op_cells = ""
        for op in sorted(im.cluster_operators,
                         key=lambda o: (not o.degraded, not o.progressing, o.name)):
            if op.degraded:      c, dot = "#ef4444", "#ef4444"
            elif op.progressing: c, dot = "#f59e0b", "#f59e0b"
            elif op.available:   c, dot = "#10b981", "#10b981"
            else:                c, dot = "#64748b", "#64748b"
            tip = escape(op.message[:120]) if op.message else ""
            op_cells += (
                f"<div title='{tip}' style='background:#1e293b;border:1px solid #1e3a5f;border-radius:6px;"
                f"padding:7px 9px;'>"
                f"<div style='display:flex;align-items:center;gap:5px;margin-bottom:2px'>"
                f"<div style='width:7px;height:7px;border-radius:50%;background:{dot};flex-shrink:0'></div>"
                f"<span style='font-size:11px;font-weight:600;color:#e2e8f0;white-space:nowrap;"
                f"overflow:hidden;text-overflow:ellipsis'>{escape(op.name)}</span></div>"
                f"<div style='font-size:10px;color:{c};padding-left:12px;font-family:monospace'>{escape(op.version)}</div>"
                f"</div>"
            )

        # ── 노드 행 ─────────────────────────────────────────────
        node_rows = ""
        for n in m.nodes:
            sc = "#10b981" if n.status == "Ready" else "#ef4444"
            node_rows += (
                f"<tr><td style='color:#67e8f9;font-family:monospace'>{escape(n.name)}</td>"
                f"<td><span style='background:#1e3a5f;color:#93c5fd;padding:1px 6px;border-radius:3px;"
                f"font-size:10px'>{escape(n.roles)}</span></td>"
                f"<td><span style='color:{sc}'>{escape(n.status)}</span></td>"
                f"<td>{escape(n.age)}</td>"
                f"<td style='font-family:monospace'>{escape(n.cpu_usage)}</td>"
                f"<td>{self._pct_bar(n.cpu_pct_realtime)}</td>"
                f"<td style='font-family:monospace'>{escape(n.memory_usage)}</td>"
                f"<td>{self._pct_bar(n.memory_pct_realtime)}</td></tr>"
            )

        # ── VM 행 ────────────────────────────────────────────────
        vm_rows = ""
        prev_ns = None
        for vm in sorted(m.vms, key=lambda v: (v.namespace, v.name)):
            if vm.namespace != prev_ns:
                prev_ns = vm.namespace
                vm_rows += (
                    f"<tr><td colspan='12' style='background:#0f172a;color:#7dd3fc;"
                    f"font-size:11px;font-weight:700;padding:6px 10px;border-top:2px solid #1e3a5f'>"
                    f"📁 {escape(vm.namespace)}</td></tr>"
                )
            net_rx = f"{vm.net_rx_bps/1024:.1f} KB/s" if vm.net_rx_bps else "—"
            net_tx = f"{vm.net_tx_bps/1024:.1f} KB/s" if vm.net_tx_bps else "—"
            vm_rows += (
                f"<tr><td style='color:#67e8f9;font-family:monospace'>{escape(vm.name)}</td>"
                f"<td>{self._status_badge(vm.status_group)}</td>"
                f"<td style='font-size:11px;color:#94a3b8'>{escape(vm.node)}</td>"
                f"<td style='font-size:11px'>{escape(vm.ip_address)}</td>"
                f"<td style='text-align:center'>{escape(vm.cpu_cores)}</td>"
                f"<td>{self._pct_bar(vm.cpu_usage_pct)}</td>"
                f"<td style='font-size:11px'>{escape(vm.memory_total)}</td>"
                f"<td>{self._pct_bar(vm.memory_usage_pct)}</td>"
                f"<td style='font-size:11px;color:#94a3b8'>{net_rx}</td>"
                f"<td style='font-size:11px;color:#94a3b8'>{net_tx}</td>"
                f"<td style='font-size:11px;color:#64748b'>{escape(vm.os_info[:30])}</td>"
                f"<td style='font-size:11px'>{escape(vm.creation_time)}</td></tr>"
            )

        # ── 스토리지 풀 행 ───────────────────────────────────────
        pool_rows = ""
        for p in m.storage_pools.values():
            used_pct = p.used_capacity_bytes / p.total_capacity_bytes * 100 if p.total_capacity_bytes else 0
            pool_rows += (
                f"<tr><td style='color:#67e8f9'>{escape(p.name)}</td>"
                f"<td style='font-size:11px;color:#94a3b8'>{escape(p.provisioner)}</td>"
                f"<td style='text-align:center'>{p.pv_count}</td>"
                f"<td style='text-align:center'>{p.pvc_count}</td>"
                f"<td>{self._fmt_bytes(p.total_capacity_bytes)}</td>"
                f"<td>{self._fmt_bytes(p.used_capacity_bytes)}</td>"
                f"<td>{self._pct_bar(used_pct)}</td></tr>"
            )

        # ── 인프라 서비스 카드 ───────────────────────────────────
        def svc_card(title, color, items):
            rows = "".join(
                f"<div style='display:flex;justify-content:space-between;padding:5px 0;"
                f"border-bottom:1px solid #1e293b'>"
                f"<span style='font-size:11px;color:#64748b'>{l}</span>"
                f"<span style='font-size:12px;font-weight:700;color:{vc}'>{v}</span></div>"
                for l, v, vc in items
            )
            return (
                f"<div style='background:#111827;border:1px solid #1e3a5f;border-radius:8px;overflow:hidden'>"
                f"<div style='background:{color}11;padding:8px 12px;border-bottom:1px solid {color}33'>"
                f"<span style='font-size:11px;font-weight:700;color:#e2e8f0;letter-spacing:.05em'>{title}</span></div>"
                f"<div style='padding:8px 12px'>{rows}</div></div>"
            )

        svc_cards = svc_card("INGRESS ROUTER", "#06b6d4", [
            ("Req/s",    f"{im.router_req_rate:.1f}/s" if im.router_req_rate is not None else "N/A", "#06b6d4"),
            ("4xx/s",    f"{im.router_4xx_rate:.2f}/s" if im.router_4xx_rate is not None else "N/A", "#f59e0b"),
            ("5xx/s",    f"{im.router_5xx_rate:.2f}/s" if im.router_5xx_rate is not None else "N/A", "#ef4444"),
            ("Sessions", str(int(im.router_sessions)) if im.router_sessions is not None else "N/A", "#3b82f6"),
        ])

        sc_pend = im.sched_pending
        sc_c    = "#ef4444" if sc_pend and sc_pend > 10 else "#10b981"
        svc_cards += svc_card("SCHEDULER", "#a78bfa", [
            ("Pending Pods", str(int(sc_pend)) if sc_pend is not None else "N/A", sc_c),
        ])
        ovn_ok = im.ovn_nb_leader is not None and im.ovn_nb_leader >= 1
        svc_cards += svc_card("OVN-KUBERNETES", "#2dd4bf", [
            ("Logical Ports", str(int(im.ovn_ports)) if im.ovn_ports is not None else "N/A", "#2dd4bf"),
            ("NB DB Leader",  "Leader" if ovn_ok else "N/A", "#10b981" if ovn_ok else "#64748b"),
        ])
        svc_cards += svc_card("IMAGE REGISTRY", "#34d399", [
            ("Req/s", f"{im.reg_req_rate:.2f}/s" if im.reg_req_rate is not None else "N/A", "#34d399"),
        ])

        # ── MCP 카드 ─────────────────────────────────────────────
        mcp_html = ""
        for p in im.mcp_pools:
            dg = p.degraded_count > 0
            ok_all = p.ready_count == p.machine_count and p.machine_count > 0
            bc = "#ef4444" if dg else ("#10b981" if ok_all else "#f59e0b")
            dg_color = "#ef4444" if dg else "#64748b"
            st = "Degraded" if dg else ("Ready" if ok_all else "Updating")
            mcp_html += (
                f"<div style='background:#111827;border:1px solid {bc}33;border-radius:8px;padding:12px'>"
                f"<div style='display:flex;justify-content:space-between;margin-bottom:8px'>"
                f"<span style='font-weight:700;color:#e2e8f0'>{escape(p.name)}</span>"
                f"<span style='font-size:11px;font-weight:700;color:{bc}'>{st}</span></div>"
                f"<div style='display:grid;grid-template-columns:1fr 1fr;gap:4px'>"
                f"<div style='text-align:center;background:#0f172a;border-radius:4px;padding:5px'>"
                f"<div style='font-size:9px;color:#64748b'>Total</div><div style='font-weight:700'>{p.machine_count}</div></div>"
                f"<div style='text-align:center;background:#0f172a;border-radius:4px;padding:5px'>"
                f"<div style='font-size:9px;color:#64748b'>Ready</div><div style='font-weight:700;color:#10b981'>{p.ready_count}</div></div>"
                f"<div style='text-align:center;background:#0f172a;border-radius:4px;padding:5px'>"
                f"<div style='font-size:9px;color:#64748b'>Updated</div><div style='font-weight:700;color:#06b6d4'>{p.updated_count}</div></div>"
                f"<div style='text-align:center;background:#0f172a;border-radius:4px;padding:5px'>"
                f"<div style='font-size:9px;color:#64748b'>Degraded</div>"
                f"<div style='font-weight:700;color:{dg_color}'>{p.degraded_count}</div></div>"
                f"</div></div>"
            )

        # ── 헬스 카드 ────────────────────────────────────────────
        def hcard(label, v):
            ok = v is not None and v >= 1
            c  = "#10b981" if ok else ("#64748b" if v is None else "#ef4444")
            t  = "Healthy" if ok else ("N/A" if v is None else "Unhealthy")
            return (f"<div style='background:#111827;border:1px solid {c}33;border-radius:8px;"
                    f"padding:12px;display:flex;align-items:center;gap:10px'>"
                    f"<div style='width:10px;height:10px;border-radius:50%;background:{c};flex-shrink:0'></div>"
                    f"<div><div style='font-size:10px;color:#64748b;margin-bottom:2px'>{label}</div>"
                    f"<div style='font-weight:700;color:{c}'>{t}</div></div></div>")

        # ── HTML 조립 ─────────────────────────────────────────────
        fail_c  = "#ef4444" if sum(1 for v in m.vms if v.status_group == "failed") else "#64748b"
        alert_c = "#ef4444" if h.firing_alerts else "#10b981"
        vm_run = sum(1 for v in m.vms if v.status_group == "running")
        vm_stp = sum(1 for v in m.vms if v.status_group == "stopped")
        vm_prv = sum(1 for v in m.vms if v.status_group == "provisioning")
        vm_fai = sum(1 for v in m.vms if v.status_group == "failed")

        tbl_style = "width:100%;border-collapse:collapse;font-size:12px"
        th_style  = "padding:7px 10px;text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:#475569;border-bottom:1px solid #1e3a5f;white-space:nowrap"
        td_style  = "padding:7px 10px;border-bottom:1px solid #0f172a"

        # ── HTML 템플릿 (f-string 대신 변수 치환 — Python 3.11 호환) ──────
        TMPL = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>AIBox Infrastructure Report &mdash; __LAST_UPDATED__</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#020817;color:#e2e8f0;font-family:"Segoe UI",sans-serif;padding:24px}
h1{font-size:20px;font-weight:700;color:#fff;margin-bottom:4px}
h2{font-size:13px;font-weight:700;color:#e2e8f0;text-transform:uppercase;letter-spacing:.08em;margin-bottom:12px;display:flex;align-items:center;gap:8px}
h2::before{content:"";display:inline-block;width:3px;height:14px;background:#3b82f6;border-radius:2px}
.sec{background:#0c1426;border:1px solid rgba(30,58,95,.13);border-radius:10px;padding:20px;margin-bottom:20px}
table{width:100%;border-collapse:collapse;font-size:12px}
th{padding:7px 10px;text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:#475569;border-bottom:1px solid #1e3a5f;white-space:nowrap}
td{padding:7px 10px;border-bottom:1px solid #0f172a}
tr:hover td{background:rgba(59,130,246,.04)}
.grid{display:grid;gap:12px}
.kpi{background:#111827;border:1px solid #1e3a5f;border-radius:8px;padding:14px}
.kpi .l{font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:#475569;margin-bottom:4px}
.kpi .v{font-size:22px;font-weight:700;font-family:monospace}
details summary{cursor:pointer;font-size:12px;color:#7dd3fc;margin-bottom:8px}
</style>
</head>
<body>
<div style="margin-bottom:20px;padding-bottom:16px;border-bottom:1px solid #1e3a5f">
  <h1>&#11042; AIBox Infrastructure Report</h1>
  <p style="color:#64748b;font-size:12px">OCP __OCP_VER__ &nbsp;&middot;&nbsp; __LAST_UPDATED__ &nbsp;&middot;&nbsp; 30s auto-refresh</p>
</div>

<div class="grid" style="grid-template-columns:repeat(auto-fit,minmax(140px,1fr));margin-bottom:20px">
  <div class="kpi"><div class="l">Nodes</div><div class="v">__NODES__</div></div>
  <div class="kpi"><div class="l">VMs Total</div><div class="v">__VMS__</div></div>
  <div class="kpi"><div class="l" style="color:#10b981">Running</div><div class="v" style="color:#10b981">__VM_RUN__</div></div>
  <div class="kpi"><div class="l" style="color:#64748b">Stopped</div><div class="v" style="color:#64748b">__VM_STP__</div></div>
  <div class="kpi"><div class="l" style="color:#f59e0b">Provisioning</div><div class="v" style="color:#f59e0b">__VM_PRV__</div></div>
  <div class="kpi"><div class="l" style="color:__FAIL_C__">Failed</div><div class="v" style="color:__FAIL_C__">__VM_FAI__</div></div>
  <div class="kpi"><div class="l">Cluster CPU</div><div class="v" style="color:__CPU_C__">__CPU__</div></div>
  <div class="kpi"><div class="l">Cluster Mem</div><div class="v" style="color:__MEM_C__">__MEM__</div></div>
  <div class="kpi"><div class="l" style="color:__ALERT_C__">Firing Alerts</div><div class="v" style="color:__ALERT_C__">__ALERTS__</div></div>
</div>

<div class="sec">
  <h2>Cluster Health</h2>
  <div class="grid" style="grid-template-columns:repeat(auto-fit,minmax(160px,1fr))">
    __HEALTH_CARDS__
  </div>
</div>

<div class="sec">
  <h2>Infrastructure Services</h2>
  <div class="grid" style="grid-template-columns:repeat(auto-fit,minmax(200px,1fr))">
    __SVC_CARDS__
  </div>
</div>

<div class="sec">
  <h2>Cluster Operators &nbsp;
    <span style="font-size:11px;font-weight:400;color:#10b981">__OK_OPS__ OK</span>
    __DEG_BADGE____PROG_BADGE__
  </h2>
  <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:8px">
    __OP_CELLS__
  </div>
</div>

<div class="sec">
  <h2>MachineConfigPool</h2>
  <div class="grid" style="grid-template-columns:repeat(auto-fill,minmax(180px,1fr))">
    __MCP_HTML__
  </div>
</div>

<div class="sec">
  <h2>Nodes (__NODE_COUNT__)</h2>
  <div style="overflow-x:auto">
    <table>
      <thead><tr>
        <th>Name</th><th>Roles</th><th>Status</th><th>Age</th>
        <th>CPU (top)</th><th>CPU %</th><th>Memory (top)</th><th>Memory %</th>
      </tr></thead>
      <tbody>__NODE_ROWS__</tbody>
    </table>
  </div>
</div>

<div class="sec">
  <h2>Virtual Machines (__VM_COUNT__)</h2>
  <div style="overflow-x:auto">
    <table>
      <thead><tr>
        <th>Name</th><th>Status</th><th>Node</th><th>IP</th>
        <th>CPU Cores</th><th>CPU %</th><th>Memory</th><th>Mem %</th>
        <th>Net RX</th><th>Net TX</th><th>Ephemeral</th><th>OS</th><th>Age</th>
      </tr></thead>
      <tbody>__VM_ROWS__</tbody>
    </table>
  </div>
</div>

<div class="sec">
  <h2>Storage Pools (PV: __PV_COUNT__, PVC: __PVC_COUNT__)</h2>
  <div style="overflow-x:auto;margin-bottom:16px">
    <table>
      <thead><tr>
        <th>Pool Name</th><th>Provisioner</th><th>PVs</th><th>PVCs</th>
        <th>Total</th><th>Used</th><th>Usage %</th>
      </tr></thead>
      <tbody>__POOL_ROWS__</tbody>
    </table>
  </div>
  <details>
    <summary>PersistentVolumes (__PV_COUNT__&#xAC1C;) &#xD3BC;&#xCE58;&#xAE30;</summary>
    <div style="overflow-x:auto;margin-top:8px">
      <table>
        <thead><tr><th>Name</th><th>Capacity</th><th>Status</th><th>StorageClass</th><th>Claim</th><th>Age</th></tr></thead>
        <tbody>__PV_ROWS__</tbody>
      </table>
    </div>
  </details>
</div>

<div style="text-align:center;color:#334155;font-size:11px;margin-top:16px">
  AIBox Unified Monitoring Portal v5 &nbsp;&middot;&nbsp; __LAST_UPDATED__
</div>
</body></html>"""

        # ── pv_rows_html 생성 ───────────────────────────────────
        pv_rows_html = ""
        for pv in m.pv_data:
            pv_n   = escape(pv.get("Name",""))
            pv_cap = escape(pv.get("Capacity",""))
            pv_st  = escape(pv.get("Status",""))
            pv_scn = escape(pv.get("StorageClass",""))
            pv_cl  = escape(pv.get("Claim",""))
            pv_ag  = escape(pv.get("Age",""))
            pv_c   = "#10b981" if pv.get("Status") == "Bound" else "#f59e0b"
            pv_rows_html += (
                "<tr>"
                f"<td style='font-family:monospace;font-size:11px'>{pv_n}</td>"
                f"<td>{pv_cap}</td>"
                f"<td><span style='color:{pv_c}'>{pv_st}</span></td>"
                f"<td style='font-size:11px;color:#64748b'>{pv_scn}</td>"
                f"<td style='font-size:11px;color:#94a3b8'>{pv_cl}</td>"
                f"<td style='font-size:11px'>{pv_ag}</td>"
                "</tr>"
            )

        # ── 치환값 계산 ──────────────────────────────────────────
        vm_run = sum(1 for v in m.vms if v.status_group == "running")
        vm_stp = sum(1 for v in m.vms if v.status_group == "stopped")
        vm_prv = sum(1 for v in m.vms if v.status_group == "provisioning")
        vm_fai = sum(1 for v in m.vms if v.status_group == "failed")
        fail_c  = "#ef4444" if vm_fai  else "#64748b"
        alert_c = "#ef4444" if h.firing_alerts else "#10b981"

        replacements = {
            "__LAST_UPDATED__": escape(m.last_updated),
            "__OCP_VER__":      escape(m.ocp_version),
            "__NODES__":        str(len(m.nodes)),
            "__VMS__":          str(len(m.vms)),
            "__VM_RUN__":       str(vm_run),
            "__VM_STP__":       str(vm_stp),
            "__VM_PRV__":       str(vm_prv),
            "__VM_FAI__":       str(vm_fai),
            "__FAIL_C__":       fail_c,
            "__ALERT_C__":      alert_c,
            "__CPU_C__":        self._pct_color(h.cluster_cpu_pct),
            "__MEM_C__":        self._pct_color(h.cluster_memory_pct),
            "__CPU__":          self._fmt_pct(h.cluster_cpu_pct),
            "__MEM__":          self._fmt_pct(h.cluster_memory_pct),
            "__ALERTS__":       str(h.firing_alerts),
            "__HEALTH_CARDS__": (
                hcard("API Server", h.api_server) +
                hcard("ETCD",       h.etcd) +
                hcard("CoreDNS",    h.coredns) +
                hcard("ETCD Leader",h.etcd_leader)
            ),
            "__SVC_CARDS__":    svc_cards,
            "__OK_OPS__":       str(ok_ops),
            "__DEG_BADGE__":    (f"<span style='font-size:11px;font-weight:700;color:#ef4444'>&nbsp;{deg_ops} Degraded</span>" if deg_ops else ""),
            "__PROG_BADGE__":   (f"<span style='font-size:11px;font-weight:700;color:#f59e0b'>&nbsp;{prog_ops} Progressing</span>" if prog_ops else ""),
            "__OP_CELLS__":     op_cells,
            "__MCP_HTML__":     mcp_html or "<p style='color:#64748b;font-size:12px'>No data</p>",
            "__NODE_COUNT__":   str(len(m.nodes)),
            "__NODE_ROWS__":    node_rows,
            "__VM_COUNT__":     str(len(m.vms)),
            "__VM_ROWS__":      vm_rows,
            "__PV_COUNT__":     str(len(m.pv_data)),
            "__PVC_COUNT__":    str(len(m.pvc_data)),
            "__POOL_ROWS__":    pool_rows,
            "__PV_ROWS__":      pv_rows_html,
        }

        html = TMPL
        for placeholder, value in replacements.items():
            html = html.replace(placeholder, value)

        with open(report_file, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info("HTML 리포트 생성 완료: %s (%d bytes)", report_file, len(html))

# ════════════════════════════════════════════════════════════════════
# 7. FASTAPI APPLICATION
# ════════════════════════════════════════════════════════════════════

def _sanitize_json(obj):
    """inf/nan 등 JSON 비호환 float → None 재귀 치환"""
    import math
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_json(v) for v in obj]
    return obj

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
    return JSONResponse(_sanitize_json({**cache, "is_collecting": False}))

# ── Nodes ─────────────────────────────────────────────────────────
@app.get("/api/v1/nodes")
def api_nodes():
    cache = _engine.get_cache()
    return JSONResponse({"nodes": cache.get("nodes", []), "last_updated": cache.get("last_updated")})

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


# ── Node Trends (on-demand) ───────────────────────────────────────
@app.get("/api/v1/nodes/{node_name}/trends")
def api_node_trends(node_name: str, hours: int = Query(1, ge=1, le=24)):
    data = _collector.query_node_trends(node_name, hours)
    return JSONResponse(_sanitize_json(data))

# ── Node SQLite Trends (CLI fallback) ────────────────────────────
@app.get("/api/v1/nodes/{node_name}/sqlite-trends")
def api_node_sqlite_trends(node_name: str, hours: int = Query(1, ge=1, le=24)):
    """oc top 기반 SQLite 시계열 — Prometheus 미사용 환경 전용"""
    data = _db.get_node_trend(node_name, hours)
    return JSONResponse(_sanitize_json(data))

# ── Node Conditions ───────────────────────────────────────────────
@app.get("/api/v1/nodes/conditions")
def api_node_conditions():
    data = _collector.query_node_conditions()
    return JSONResponse({"conditions": data})

# ── VM Trends (on-demand) ─────────────────────────────────────────
@app.get("/api/v1/vms/{namespace}/{vm_name}/trends")
def api_vm_trends(namespace: str, vm_name: str, hours: int = Query(1, ge=1, le=24)):
    data = _collector.query_vm_trends(vm_name, namespace, hours)
    return JSONResponse(_sanitize_json(data))

# ── Node Top Disk (Pod별 디스크 상위) ───────────────────────────────
@app.get("/api/v1/nodes/{node_name}/top-disk")
def api_node_top_disk(node_name: str):
    try:
        items = _collector._query(
            f'topk(10, container_fs_usage_bytes{{id!="/", node="{node_name}"}})'
        )
        result = []
        for item in items:
            m = item.get("metric", {})
            try: val = int(float(item["value"][1]))
            except: val = 0
            if val > 0:
                result.append({
                    "pod": m.get("pod", m.get("id", "?")),
                    "namespace": m.get("namespace", ""),
                    "container": m.get("container", ""),
                    "used_bytes": val,
                })
        result.sort(key=lambda x: x["used_bytes"], reverse=True)
        return JSONResponse({"node": node_name, "top_disk": result[:10]})
    except Exception as e:
        return JSONResponse({"node": node_name, "top_disk": [], "error": str(e)})

# ── Alerts Detail ────────────────────────────────────────────────
@app.get("/api/v1/alerts")
def api_alerts():
    alerts = _sanitize_json(getattr(_collector.metrics, "_alerts_detail", []))
    trend  = _sanitize_json(_db.get_alert_history_trend(hours=1))
    by_sev = {}
    for a in alerts:
        s = a.get("severity","none")
        by_sev.setdefault(s, []).append(a)
    return JSONResponse({
        "alerts": alerts, "by_severity": by_sev,
        "counts": {s: len(v) for s, v in by_sev.items()},
        "trend": trend,
    })

# ── Route SLO ─────────────────────────────────────────────────────
@app.get("/api/v1/routes")
def api_routes(hours: int = Query(24, ge=1, le=168)):
    probes = _sanitize_json(getattr(_collector.metrics, "_route_probes", []))
    slo    = _sanitize_json(_db.get_route_slo(hours))
    return JSONResponse({"current": probes, "slo": slo, "hours": hours})

@app.get("/api/v1/routes/{route_name}/trend")
def api_route_trend(route_name: str, hours: int = Query(1, ge=1, le=24)):
    return JSONResponse(_sanitize_json(_db.get_route_trend(route_name, hours)))

# ── User Workload Monitoring ──────────────────────────────────────
@app.get("/api/v1/uwm")
def api_uwm():
    return JSONResponse(_sanitize_json(getattr(_collector.metrics, "_uwm_status", {})))

# ── Incidents ─────────────────────────────────────────────────────
@app.get("/api/v1/incidents")
def api_incidents():
    active   = [i for i in _db.get_incidents(50) if i["status"] == "active"]
    resolved = [i for i in _db.get_incidents(50) if i["status"] == "resolved"]
    return JSONResponse({"active": active, "resolved": resolved[:10]})

# ── Alert Trend (SQLite) ──────────────────────────────────────────
@app.get("/api/v1/alerts/trend")
def api_alert_trend(hours: int = Query(1, ge=1, le=24)):
    return JSONResponse(_sanitize_json(_db.get_alert_history_trend(hours)))

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
      <div class="sb-item" data-page="alerts">
        <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9"/></svg>
        Alerts
      </div>
      <div class="sb-item" data-page="routes">
        <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"/></svg>
        Route SLO
      </div>
      <div class="sb-item" data-page="incidents">
        <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/></svg>
        Incidents
      </div>
      <div class="sb-item" data-page="collector">
        <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/></svg>
        Collector Health
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
        <!-- KPI Row -->
        <div class="kpi-grid" id="kpi-row" style="margin-bottom:14px;"></div>

        <!-- Row 2: Health + Infra quick status -->
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px;">
          <div class="card">
            <div class="sec-title">Cluster Health</div>
            <div id="health-cards" style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"></div>
          </div>
          <div class="card">
            <div class="sec-title">Infrastructure Services</div>
            <div id="infra-quick" style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"></div>
          </div>
        </div>

        <!-- Row 3: Top 5 Panels -->
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:14px;">
          <div class="card">
            <div class="sec-title" style="color:var(--accent2);">
              <svg width="14" height="14" fill="none" stroke="currentColor" viewBox="0 0 24 24" style="flex-shrink:0"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"/></svg>
              Top 5 · CPU
            </div>
            <div id="top5-cpu"></div>
          </div>
          <div class="card">
            <div class="sec-title" style="color:#a78bfa;">
              <svg width="14" height="14" fill="none" stroke="currentColor" viewBox="0 0 24 24" style="flex-shrink:0"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 3H5a2 2 0 00-2 2v4m6-6h10a2 2 0 012 2v4M9 3v18m0 0h10a2 2 0 002-2V9M9 21H5a2 2 0 01-2-2V9m0 0h18"/></svg>
              Top 5 · Memory
            </div>
            <div id="top5-mem"></div>
          </div>
          <div class="card">
            <div class="sec-title" style="color:#34d399;">
              <svg width="14" height="14" fill="none" stroke="currentColor" viewBox="0 0 24 24" style="flex-shrink:0"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"/></svg>
              Top 5 · Disk Used
            </div>
            <div id="top5-disk"></div>
          </div>
        </div>

        <!-- Row 4: Trend Charts -->
        <div style="display:grid;grid-template-columns:2fr 1fr;gap:14px;">
          <div class="card">
            <div class="sec-title" style="justify-content:space-between;">
              CPU · Memory Trend (1h)
              <span style="font-size:10px;color:var(--txt-muted);font-weight:400;">30s SQLite 이력</span>
            </div>
            <div class="chart-wrap" style="height:150px;"><canvas id="chart-cluster-trend"></canvas></div>
          </div>
          <div class="card">
            <div class="sec-title">VM Status</div>
            <div id="vm-status-chart" style="padding-top:4px;"></div>
          </div>
        </div>
      </div>

      <!-- PAGE: NODES -->
      <div id="page-nodes" class="page" style="display:none;">
        <!-- 노드 개별 상태 카드 (kube_node_status_condition) -->
        <div id="node-condition-cards" style="display:none;" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:10px;margin-bottom:14px;"></div>
        <!-- 노드 테이블 -->
        <div class="card">
          <div class="sec-title" style="justify-content:space-between;">
            Node Metrics
            <span style="font-size:10px;color:var(--txt-muted);font-weight:400;">행 클릭 → 시계열 차트</span>
          </div>
          <div style="overflow-x:auto;">
            <table class="tbl" id="tbl-nodes">
              <thead><tr>
                <th>Name</th><th>Roles</th><th>Status</th><th>Age</th>
                <th>CPU (top)</th><th>CPU %</th><th>Memory (top)</th><th>Mem %</th>
                <th>Net RX</th><th>Net TX</th>
              </tr></thead>
              <tbody id="tbody-nodes"></tbody>
            </table>
          </div>
        </div>
        <!-- 노드 상세 차트 패널 (클릭 시 표시) -->
        <div id="node-detail-panel" style="display:none;margin-top:14px;">
          <div class="card">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;">
              <div class="sec-title" style="margin-bottom:0;" id="node-detail-title">노드 상세</div>
              <button onclick="document.getElementById('node-detail-panel').style.display='none'"
                style="background:var(--bg-card2);border:1px solid var(--bd-bright);color:var(--txt-secondary);
                       padding:4px 10px;border-radius:5px;cursor:pointer;font-size:11px;">닫기</button>
            </div>
            <div id="nd-sqlite-area" style="width:100%;box-sizing:border-box;"></div>
            <div style="width:100%;box-sizing:border-box;">
            </div>
          </div>
        </div>
      </div>

      <!-- VM 상세 모달 -->
      <div id="vm-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:200;overflow-y:auto;padding:20px;">
        <div style="max-width:960px;margin:0 auto;background:var(--bg-card);border:1px solid var(--bd-bright);border-radius:12px;padding:24px;">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">
            <div>
              <div id="vm-modal-title" style="font-size:16px;font-weight:700;color:var(--txt-primary);"></div>
              <div id="vm-modal-sub" style="font-size:11px;color:var(--txt-muted);margin-top:2px;"></div>
            </div>
            <button onclick="document.getElementById('vm-modal').style.display='none'"
              style="background:var(--bg-card2);border:1px solid var(--bd-bright);color:var(--txt-secondary);
                     padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px;">✕ 닫기</button>
          </div>
          <!-- VM 정보 카드 -->
          <div id="vm-modal-info" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;margin-bottom:16px;"></div>
          <!-- 디스크 볼륨 정보 -->
          <div id="vm-modal-volumes" style="margin-bottom:16px;"></div>
          <!-- 시계열 차트 -->
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">
            <div class="card-sm"><div style="font-size:10px;color:var(--txt-muted);margin-bottom:6px;text-transform:uppercase;">CPU 사용률 (%)</div><div style="height:130px;"><canvas id="vm-chart-cpu"></canvas></div></div>
            <div class="card-sm"><div style="font-size:10px;color:var(--txt-muted);margin-bottom:6px;text-transform:uppercase;">메모리 사용률 (%)</div><div style="height:130px;"><canvas id="vm-chart-mem"></canvas></div></div>
            <div class="card-sm"><div style="font-size:10px;color:var(--txt-muted);margin-bottom:6px;text-transform:uppercase;">네트워크 RX/TX (bytes/s)</div><div style="height:130px;"><canvas id="vm-chart-net"></canvas></div></div>
            <div class="card-sm"><div style="font-size:10px;color:var(--txt-muted);margin-bottom:6px;text-transform:uppercase;">디스크 Read/Write (bytes/s)</div><div style="height:130px;"><canvas id="vm-chart-disk"></canvas></div></div>
          </div>
          <div style="text-align:center;margin-top:10px;">
            <div id="vm-modal-loading" style="color:var(--txt-muted);font-size:12px;">시계열 데이터 로딩 중...</div>
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
                <th>IP</th><th>CPU</th><th>CPU %</th><th>Memory</th><th>Mem %</th>
                <th>Disk Used</th><th>Net RX</th><th>Net TX</th><th>Age</th><th></th>
              </tr></thead>
              <tbody id="tbody-vms"></tbody>
            </table>
          </div>
        </div>
      </div>

      <!-- PAGE: STORAGE -->
      <div id="page-storage" class="page" style="display:none;">
        <!-- Top 5 패널 -->
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px;">
          <div class="card">
            <div class="sec-title" style="color:#f59e0b;">
              <svg width="14" height="14" fill="none" stroke="currentColor" viewBox="0 0 24 24" style="flex-shrink:0"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"/></svg>
              Top 5 · 최대 할당 PV
        <div class="card" style="margin-top:14px;">
          <div class="sec-title">CSI Storage Capacity (CephFS)</div>
          <div id="csi-capacity-list" style="margin-top:10px;"></div>
        </div>
            </div>
            <div id="top5-pv"></div>
          </div>
          <div class="card">
            <div class="sec-title" style="color:#34d399;">
              <svg width="14" height="14" fill="none" stroke="currentColor" viewBox="0 0 24 24" style="flex-shrink:0"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 3H5a2 2 0 00-2 2v4m6-6h10a2 2 0 012 2v4M9 3v18m0 0h10a2 2 0 002-2V9M9 21H5a2 2 0 01-2-2V9m0 0h18"/></svg>
              Top 5 · 실제 사용량 PVC
            </div>
            <div id="top5-pvc-used"></div>
          </div>
        </div>
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

      <!-- PAGE: ALERTS -->
      <div id="page-alerts" class="page" style="display:none;">
        <!-- 인시던트 배너 -->
        <div id="incident-banner" style="display:none;background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.3);
          border-radius:8px;padding:12px 16px;margin-bottom:14px;">
          <div style="font-size:12px;font-weight:700;color:#ef4444;margin-bottom:6px;">🚨 활성 인시던트</div>
          <div id="incident-banner-list"></div>
        </div>
        <!-- 심각도별 KPI -->
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:14px;" id="alert-kpi-row"></div>
        <!-- 알럿 트렌드 차트 -->
        <div style="display:grid;grid-template-columns:2fr 1fr;gap:14px;margin-bottom:14px;">
          <div class="card">
            <div class="sec-title" style="justify-content:space-between;">
              알럿 추이 (1h)
              <span style="font-size:10px;color:var(--txt-muted);font-weight:400;">SQLite 수집 이력</span>
            </div>
            <div class="chart-wrap" style="height:120px;"><canvas id="chart-alert-trend"></canvas></div>
          </div>
          <div class="card">
            <div class="sec-title">심각도 분포</div>
            <div id="alert-sev-dist"></div>
          </div>
        </div>
        <!-- 알럿 목록 -->
        <div class="card">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">
            <div class="sec-title" style="margin:0;">Firing Alerts</div>
            <select id="alert-sev-filter" style="background:var(--bg-card2);border:1px solid var(--bd-bright);
              border-radius:5px;padding:4px 8px;font-size:11px;color:var(--txt-primary);outline:none;margin-left:auto;">
              <option value="">All Severity</option>
              <option value="critical">Critical</option>
              <option value="warning">Warning</option>
              <option value="info">Info</option>
            </select>
            <input id="alert-search" placeholder="이름/네임스페이스 검색..."
              style="background:var(--bg-card2);border:1px solid var(--bd-bright);border-radius:5px;
                     padding:4px 10px;font-size:11px;color:var(--txt-primary);outline:none;width:200px;">
          </div>
          <div style="overflow-x:auto;">
            <table class="tbl">
              <thead><tr>
                <th>Severity</th><th>Alert Name</th><th>Namespace</th>
                <th>Service/Pod</th><th>Node</th><th>Summary</th>
              </tr></thead>
              <tbody id="tbody-alerts"></tbody>
            </table>
          </div>
        </div>
      </div>

      <!-- PAGE: ROUTE SLO -->
      <div id="page-routes" class="page" style="display:none;">
        <!-- SLO 요약 -->
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:14px;"
             id="route-kpi-row"></div>
        <!-- Route 목록 + SLO -->
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px;">
          <div class="card">
            <div class="sec-title" style="justify-content:space-between;">
              Route 목록 (현재 상태)
              <span id="route-probe-time" style="font-size:10px;color:var(--txt-muted);font-weight:400;"></span>
            </div>
            <div style="overflow-x:auto;max-height:400px;overflow-y:auto;">
              <table class="tbl">
                <thead><tr>
                  <th>Name</th><th>Namespace</th><th>Host</th>
                  <th>Status</th><th>Latency</th>
                </tr></thead>
                <tbody id="tbody-routes-current"></tbody>
              </table>
            </div>
          </div>
          <div class="card">
            <div class="sec-title" style="justify-content:space-between;">
              SLO (24h 가용성)
              <select id="slo-hours" style="background:var(--bg-card2);border:1px solid var(--bd-bright);
                border-radius:5px;padding:3px 6px;font-size:10px;color:var(--txt-primary);outline:none;">
                <option value="1">1h</option>
                <option value="6">6h</option>
                <option value="24" selected>24h</option>
                <option value="168">7d</option>
              </select>
            </div>
            <div style="overflow-x:auto;max-height:400px;overflow-y:auto;">
              <table class="tbl">
                <thead><tr><th>Route</th><th>Namespace</th><th>SLO %</th><th>Avg ms</th><th>Probes</th></tr></thead>
                <tbody id="tbody-routes-slo"></tbody>
              </table>
            </div>
          </div>
        </div>
        <!-- UWM 상태 -->
        <div class="card" id="uwm-panel">
          <div class="sec-title">User Workload Monitoring</div>
          <div id="uwm-content"></div>
        </div>
      </div>

      <!-- PAGE: INCIDENTS -->
      <div id="page-incidents" class="page" style="display:none;">
        <!-- 활성 인시던트 -->
        <div class="card" style="margin-bottom:14px;">
          <div class="sec-title" style="color:var(--danger);">🚨 활성 인시던트</div>
          <div id="incidents-active"></div>
        </div>
        <!-- 해소된 인시던트 -->
        <div class="card">
          <div class="sec-title">✅ 최근 해소된 인시던트</div>
          <div id="incidents-resolved"></div>
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
  if (page === 'nodes') loadNodeConditions();
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
const fmtCpuRaw = s => { if(!s||s==="N/A") return s; if(s.endsWith("n")) return (parseInt(s)/1e9).toFixed(2)+" cores"; if(s.endsWith("m")) return (parseInt(s)/1000).toFixed(2)+" cores"; return parseFloat(s).toFixed(2)+" cores"; };
const fmtMemRaw = s => { if(!s||s==="N/A") return s; if(s.endsWith("Ki")) return fmtBytes(parseInt(s)*1024); if(s.endsWith("Mi")) return fmtBytes(parseInt(s)*1048576); if(s.endsWith("Gi")) return fmtBytes(parseInt(s)*1073741824); return s; };
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
function renderTop5(elId, items, valFn, color) {
  if (!items || !items.length) {
    document.getElementById(elId).innerHTML =
      '<div style="color:var(--txt-muted);font-size:12px;text-align:center;padding:12px;">데이터 수집 중...</div>';
    return;
  }
  document.getElementById(elId).innerHTML = items.map((vm, i) => {
    const val = valFn(vm);
    const pct = typeof val === 'number' ? Math.min(val, 100) : 0;
    const label = typeof val === 'number' ? (elId.includes('disk') ? fmtBytes(vm.disk_used_bytes||0) : fmtPct(val)) : '—';
    return `<div style="padding:7px 0;border-bottom:1px solid var(--bd);">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px;">
        <div style="display:flex;align-items:center;gap:6px;min-width:0;">
          <span style="color:var(--txt-muted);font-size:11px;font-weight:700;flex-shrink:0;">#${i+1}</span>
          <div style="min-width:0;">
            <div style="font-size:12px;font-weight:600;color:var(--txt-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:140px;">${vm.name}</div>
            <div style="font-size:10px;color:var(--txt-muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:140px;">${vm.namespace}</div>
          </div>
        </div>
        <span style="font-size:12px;font-weight:700;color:${color};flex-shrink:0;margin-left:6px;">${label}</span>
      </div>
      <div class="pbar"><div class="pbar-fill" style="background:${color};width:${pct}%;"></div></div>
    </div>`;
  }).join('');
}

function renderOverview(d) {
  const c = d.cluster, ct = d.counts;

  // ── KPI 카드 ──
  const kpis = [
    { label:'OCP Version',    value: d.ocp_version,     color:'var(--accent)',   sub:'' },
    { label:'Nodes',          value: ct.nodes,           color:'var(--txt-primary)', sub:'' },
    { label:'Running VMs',    value: ct.vms_running,     color:'var(--success)', sub:`/ ${ct.vms_total}` },
    { label:'Stopped VMs',    value: ct.vms_stopped,     color:'var(--txt-muted)', sub:'' },
    { label:'Provisioning',   value: ct.vms_provisioning,color:'var(--warn)',    sub:'' },
    { label:'Failed VMs',     value: ct.vms_failed,      color: ct.vms_failed > 0 ? 'var(--danger)' : 'var(--txt-muted)', sub:'' },
    { label:'Cluster CPU',    value: fmtPct(c.cpu_pct),  color: c.cpu_pct > 90 ? 'var(--danger)' : c.cpu_pct > 70 ? 'var(--warn)' : 'var(--success)', sub:'' },
    { label:'Cluster Mem',    value: fmtPct(c.mem_pct),  color: c.mem_pct > 90 ? 'var(--danger)' : c.mem_pct > 70 ? 'var(--warn)' : 'var(--success)', sub:'' },
    { label:'Firing Alerts',  value: c.firing_alerts,    color: c.firing_alerts > 0 ? 'var(--danger)' : 'var(--success)', sub:'' },
    { label:'Operators OK',   value: `${(d.infra?.cluster_operators||[]).filter(o=>o.available&&!o.degraded).length} / ${ct.cluster_operators}`, color:'var(--success)', sub:'' },
    { label:'PV / PVC',       value: `${ct.pv} / ${ct.pvc}`, color:'var(--accent2)', sub:'' },
    { label:'Storage Pools',  value: ct.storage_pools,   color:'var(--txt-secondary)', sub:'' },
  ];
  document.getElementById('kpi-row').innerHTML = kpis.map(k => `
    <div class="kpi">
      <div class="label">${k.label}</div>
      <div class="value" style="color:${k.color};font-size:${String(k.value).length > 6 ? '16px' : '22px'}">${k.value}</div>
      ${k.sub ? `<div class="sub">${k.sub}</div>` : ''}
    </div>`).join('');

  // Alert badge
  const ab = document.getElementById('alert-badge');
  if (c.firing_alerts > 0) { ab.style.display = ''; ab.textContent = `${c.firing_alerts} Firing Alerts`; }
  else { ab.style.display = 'none'; }

  // ── Cluster Health 카드 ──
  document.getElementById('health-cards').innerHTML = [
    { label:'API Server', v: c.api_server },
    { label:'ETCD',       v: c.etcd },
    { label:'CoreDNS',    v: c.coredns },
    { label:'ETCD Leader',v: c.etcd_leader },
  ].map(h => {
    const ok = h.v != null && h.v >= 1, unk = h.v == null;
    const cls = unk ? 'badge-off' : ok ? 'badge-ok' : 'badge-err';
    const txt = unk ? 'N/A' : ok ? 'Healthy' : 'Unhealthy';
    return `<div class="card-sm" style="display:flex;align-items:center;gap:8px;">
      <span class="dot ${unk?'dot-off':ok?'dot-ok':'dot-err'}"></span>
      <div>
        <div style="font-size:11px;color:var(--txt-muted);">${h.label}</div>
        <div class="badge ${cls}" style="margin-top:3px;">${txt}</div>
      </div>
    </div>`;
  }).join('');

  // ── Infra Quick Status ──
  const im = d.infra || {};
  const r = im.router || {};
  document.getElementById('infra-quick').innerHTML = [
    { label:'Router Req/s',    v: r.req_rate != null ? r.req_rate.toFixed(1)+'/s' : '—',   ok: r.req_rate != null },
    { label:'Router 5xx/s',   v: r['5xx_rate'] != null ? r['5xx_rate'].toFixed(2)+'/s':'—', ok: r['5xx_rate'] != null && r['5xx_rate'] < 1 },
    { label:'Pending Pods',   v: im.scheduler?.pending != null ? Math.round(im.scheduler.pending) : '—', ok: (im.scheduler?.pending||0) < 5 },
    { label:'OVN Ports',      v: im.ovn?.ports != null ? Math.round(im.ovn.ports) : '—',   ok: im.ovn?.ports != null },
    { label:'Registry Req/s', v: im.registry?.req_rate != null ? im.registry.req_rate.toFixed(2)+'/s':'—', ok: im.registry?.req_rate != null },
    { label:'Operators Deg.', v: (im.cluster_operators||[]).filter(o=>o.degraded).length,   ok: (im.cluster_operators||[]).filter(o=>o.degraded).length === 0 },
  ].map(({label,v,ok}) => `
    <div class="card-sm" style="display:flex;align-items:center;justify-content:space-between;gap:6px;">
      <div style="display:flex;align-items:center;gap:6px;">
        <span class="dot ${ok?'dot-ok':'dot-warn'}"></span>
        <span style="font-size:11px;color:var(--txt-secondary);">${label}</span>
      </div>
      <span style="font-size:12px;font-weight:700;color:var(--txt-primary);">${v}</span>
    </div>`).join('');

  // ── Top 5 VMs ──
  const vms = d.vms || [];
  const running = vms.filter(v => v.status_group === 'running');

  // CPU Top 5
  const top5cpu = [...running].filter(v => v.cpu_pct != null)
    .sort((a,b) => (b.cpu_pct||0) - (a.cpu_pct||0)).slice(0,5);
  renderTop5('top5-cpu', top5cpu, v => v.cpu_pct, '#3b82f6');

  // Memory Top 5
  const top5mem = [...running].filter(v => v.mem_pct != null)
    .sort((a,b) => (b.mem_pct||0) - (a.mem_pct||0)).slice(0,5);
  renderTop5('top5-mem', top5mem, v => v.mem_pct, '#a78bfa');

  // Disk Top 5 (disk_used_bytes 기준, running 아닌 것도 포함)
  const top5disk = [...vms].filter(v => (v.disk_used_bytes||0) > 0)
    .sort((a,b) => (b.disk_used_bytes||0) - (a.disk_used_bytes||0)).slice(0,5);
  // disk는 절대값 비교 — 최대값 대비 %로 bar 표시
  const maxDisk = top5disk[0]?.disk_used_bytes || 1;
  if (top5disk.length) {
    document.getElementById('top5-disk').innerHTML = top5disk.map((vm,i) => {
      const pct = Math.round((vm.disk_used_bytes||0) / maxDisk * 100);
      return `<div style="padding:7px 0;border-bottom:1px solid var(--bd);">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px;">
          <div style="display:flex;align-items:center;gap:6px;min-width:0;">
            <span style="color:var(--txt-muted);font-size:11px;font-weight:700;flex-shrink:0;">#${i+1}</span>
            <div style="min-width:0;">
              <div style="font-size:12px;font-weight:600;color:var(--txt-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:140px;">${vm.name}</div>
              <div style="font-size:10px;color:var(--txt-muted);">${vm.namespace}</div>
            </div>
          </div>
          <span style="font-size:12px;font-weight:700;color:#34d399;flex-shrink:0;margin-left:6px;">${fmtBytes(vm.disk_used_bytes||0)}</span>
        </div>
        <div class="pbar"><div class="pbar-fill" style="background:#34d399;width:${pct}%;"></div></div>
      </div>`;
    }).join('');
  } else {
    document.getElementById('top5-disk').innerHTML =
      '<div style="color:var(--txt-muted);font-size:12px;text-align:center;padding:12px;">수집 중 (30s 후 표시)</div>';
  }

  // ── VM Status 바 ──
  const total = ct.vms_total || 1;
  document.getElementById('vm-status-chart').innerHTML = [
    ['Running',     ct.vms_running,     'var(--success)'],
    ['Stopped',     ct.vms_stopped,     'var(--txt-muted)'],
    ['Provisioning',ct.vms_provisioning,'var(--warn)'],
    ['Failed',      ct.vms_failed,      'var(--danger)'],
  ].map(([lbl,cnt,color]) => `
    <div style="margin-bottom:9px;">
      <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px;">
        <span style="color:var(--txt-secondary);">${lbl}</span>
        <span style="color:${color};font-weight:700;">${cnt} <span style="color:var(--txt-muted);font-weight:400;">(${Math.round(cnt/total*100)}%)</span></span>
      </div>
      <div class="pbar"><div class="pbar-fill" style="background:${color};width:${Math.round(cnt/total*100)}%;"></div></div>
    </div>`).join('');

  // ── Trend Chart ──
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
let _nodeConditions = {};

// 노드 데이터 소스 전역 변수
let _nodeDataSource = 'cli';     // 'prometheus' | 'cli'
let _nodeNetAvailable = false;
const NODE_CLI_TOOLTIP = "현재 환경에서 node-exporter가 Thanos에 노출되지 않아\nCPU/MEM은 oc adm top nodes CLI로 수집됩니다.\n네트워크 RX/TX는 수집 불가합니다.";
const NODE_PROM_TOOLTIP = "Prometheus(Thanos)에서 실시간 수집 중입니다.";

async function loadNodeConditions() {
  try {
    const d = await fetch('/api/v1/nodes/conditions').then(r => r.json());
    _nodeConditions = {};
    (d.conditions || []).forEach(c => { _nodeConditions[c.node] = c; });
    renderNodeCards();
  } catch(e) {}
}

function renderNodeCards() {
  const nodes = state.overview?.nodes || [];
  const el = document.getElementById('node-condition-cards');
  if (!el) return;
  el.innerHTML = nodes.map(n => {
    const c = _nodeConditions[n.name] || {};
    const ready = c.ready !== undefined ? c.ready : (n.status === 'Ready' ? 1 : 0);
    const mem_p = c.memory_pressure || 0;
    const disk_p = c.disk_pressure || 0;
    const pid_p  = c.pid_pressure || 0;
    const net_u  = c.network_unavailable || 0;

    const ok = ready && !mem_p && !disk_p && !pid_p && !net_u;
    const warn = ready && (mem_p || disk_p || pid_p);
    const bc = ok ? 'var(--success)' : warn ? 'var(--warn)' : 'var(--danger)';
    const st = ok ? 'Ready' : warn ? 'Pressure' : 'NotReady';

    const pressures = [
      mem_p  ? '<span style="font-size:9px;background:rgba(245,158,11,.2);color:#fbbf24;padding:1px 4px;border-radius:3px;">Mem</span>' : '',
      disk_p ? '<span style="font-size:9px;background:rgba(239,68,68,.2);color:#f87171;padding:1px 4px;border-radius:3px;">Disk</span>' : '',
      pid_p  ? '<span style="font-size:9px;background:rgba(239,68,68,.2);color:#f87171;padding:1px 4px;border-radius:3px;">PID</span>' : '',
      net_u  ? '<span style="font-size:9px;background:rgba(239,68,68,.2);color:#f87171;padding:1px 4px;border-radius:3px;">Net</span>' : '',
    ].filter(Boolean).join(' ');

    const isWorker = n.roles.includes('worker');
    const roleColor = isWorker ? 'var(--accent)' : 'var(--accent2)';
    const roleLabel = isWorker ? 'Worker' : 'Master';

    return `<div onclick="openNodeDetail('${n.name}')"
      style="background:var(--bg-card);border:1px solid ${bc}33;border-radius:10px;padding:12px;cursor:pointer;
             transition:border-color .15s;hover:border-color:${bc};">
      <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;">
        <div style="width:8px;height:8px;border-radius:50%;background:${bc};flex-shrink:0;
                    box-shadow:0 0 6px ${bc};"></div>
        <span style="font-size:11px;font-weight:700;color:var(--txt-primary);white-space:nowrap;
                     overflow:hidden;text-overflow:ellipsis;flex:1;">${n.name.split('.')[0]}</span>
        <span style="font-size:9px;font-weight:700;color:${roleColor};flex-shrink:0;">${roleLabel}</span>
      </div>
      <div style="display:flex;align-items:center;gap:4px;margin-bottom:6px;">
        <div style="font-size:10px;color:${bc};font-weight:700;">${st} ${pressures}</div>
        <span title="${_nodeDataSource==='cli'?NODE_CLI_TOOLTIP:NODE_PROM_TOOLTIP}"
          style="font-size:8px;padding:1px 4px;border-radius:3px;cursor:help;
                 background:${_nodeDataSource==='cli'?'rgba(251,191,36,.2)':'rgba(16,185,129,.2)'};
                 color:${_nodeDataSource==='cli'?'#fbbf24':'#10b981'};font-weight:700;">
          ${_nodeDataSource==='cli'?'CLI':'PROM'}
        </span>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;">
        <div>
          <div style="font-size:9px;color:var(--txt-muted);margin-bottom:2px;">CPU</div>
          <div class="pbar"><div class="pbar-fill ${pbarColor(n.cpu_pct)}" style="width:${n.cpu_pct||0}%"></div></div>
          <div style="font-size:9px;color:var(--txt-secondary);margin-top:1px;">${fmtPct(n.cpu_pct)}</div>
        </div>
        <div>
          <div style="font-size:9px;color:var(--txt-muted);margin-bottom:2px;">Mem</div>
          <div class="pbar"><div class="pbar-fill ${pbarColor(n.mem_pct)}" style="width:${n.mem_pct||0}%"></div></div>
          <div style="font-size:9px;color:var(--txt-secondary);margin-top:1px;">${fmtPct(n.mem_pct)}</div>
        </div>
      </div>
      ${(n.net_rx_bps || n.net_tx_bps) ? `<div style="display:flex;justify-content:space-between;margin-top:6px;font-size:9px;color:var(--txt-muted);">
        <span>↓${fmtBps(n.net_rx_bps)}</span><span>↑${fmtBps(n.net_tx_bps)}</span>
      </div>` : ''}
      <div style="font-size:9px;color:var(--txt-muted);margin-top:3px;text-align:center;">클릭 → 시계열</div>
    </div>`;
  }).join('');
}

async function openNodeDetail(nodeName) {
  const panel = document.getElementById('node-detail-panel');
  panel.style.display = '';
  panel.scrollIntoView({behavior:'smooth'});

  const node = (state.overview?.nodes || []).find(n => n.name === nodeName) || {};
  const isCLI = _nodeDataSource !== 'prometheus';

  if (isCLI) {
    document.getElementById('node-detail-title').textContent =
      nodeName + ' — CPU / MEM 추이 (oc top · SQLite)';

    const chartArea = document.getElementById('nd-sqlite-area');
    if (chartArea) {
      chartArea.innerHTML = `
        <!-- 데이터 소스 안내 배너 -->
        <div style="background:rgba(251,191,36,.08);border:1px solid rgba(251,191,36,.25);
                    border-radius:8px;padding:10px 14px;margin-bottom:14px;font-size:11px;color:#fbbf24;
                    display:flex;align-items:flex-start;gap:8px;">
          <span style="flex-shrink:0;font-size:14px;">⚠</span>
          <div>
          <strong>혼합 수집 모드</strong>
            <br>• CPU/MEM: Metrics Server API 실시간 수집 ✅
            <br>• Net RX/TX: <strong style="color:#34d399;">NetObserv eBPF 수집 중</strong> ✅
            <br>• 시계열: oc adm top nodes 30초 간격 SQLite 저장
            <br>• 디스크 I/O: node-exporter Thanos 미노출로 수집 불가
          </div>
        </div>
        <!-- Current Usage -->
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:8px;margin-bottom:14px;">
          ${[
            ['CPU 현재', fmtPct(node.cpu_pct), node.cpu_pct, '#3b82f6'],
            ['CPU (cores)', node.cpu_usage ? fmtCpuRaw(node.cpu_usage) : '—', null, 'var(--txt-secondary)'],
            ['MEM 현재', fmtPct(node.mem_pct), node.mem_pct, '#a78bfa'],
            ['MEM (bytes)', node.mem_usage ? fmtMemRaw(node.mem_usage) : '—', null, 'var(--txt-secondary)'],
            ['Net RX', node.net_rx_bps != null ? fmtBps(node.net_rx_bps) : '수집 중...', null, node.net_rx_bps != null ? 'var(--accent2)' : 'var(--txt-muted)'],
            ['Net TX', node.net_tx_bps != null ? fmtBps(node.net_tx_bps) : '수집 중...', null, node.net_tx_bps != null ? 'var(--accent2)' : 'var(--txt-muted)'],
            ['nodefs', node.disk_pct != null ? node.disk_pct.toFixed(1)+'%  ('+fmtBytes(node.disk_used_bytes)+' / '+fmtBytes(node.disk_capacity_bytes)+')' : '수집 중...', node.disk_pct, node.disk_pct > 90 ? 'var(--danger)' : node.disk_pct > 70 ? 'var(--warn)' : 'var(--success)'],
            ['imagefs', node.imagefs_pct != null ? node.imagefs_pct.toFixed(1)+'%  ('+fmtBytes(node.imagefs_used_bytes)+' / '+fmtBytes(node.imagefs_capacity_bytes)+')' : '수집 중...', node.imagefs_pct, node.imagefs_pct > 90 ? 'var(--danger)' : node.imagefs_pct > 70 ? 'var(--warn)' : 'var(--success)'],
          ].map(([lbl,val,pct,color]) => `
            <div style="background:var(--bg-card2);border:1px solid var(--bd);border-radius:7px;padding:10px;text-align:center;">
              <div style="font-size:9px;color:var(--txt-muted);text-transform:uppercase;margin-bottom:5px;">${lbl}</div>
              <div style="font-size:16px;font-weight:700;color:${color};">${val}</div>
              ${pct!=null?`<div class="pbar" style="margin-top:5px;"><div class="pbar-fill ${pbarColor(pct)}" style="width:${pct||0}%"></div></div>`:''}
            </div>`).join('')}
        </div>
        <!-- SQLite 기반 시계열 차트 -->
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;width:100%;">
          <div>
            <div style="font-size:10px;color:var(--txt-muted);margin-bottom:6px;text-transform:uppercase;display:flex;align-items:center;gap:6px;">
              CPU 사용률 (1h)
              <span style="font-size:9px;background:rgba(251,191,36,.2);color:#fbbf24;padding:1px 5px;border-radius:3px;">SQLite</span>
            </div>
            <div style="height:120px;"><canvas id="nd-sqlite-cpu"></canvas></div>
          </div>
          <div>
            <div style="font-size:10px;color:var(--txt-muted);margin-bottom:6px;text-transform:uppercase;display:flex;align-items:center;gap:6px;">
              Memory 사용률 (1h)
              <span style="font-size:9px;background:rgba(251,191,36,.2);color:#fbbf24;padding:1px 5px;border-radius:3px;">SQLite</span>
            </div>
            <div style="height:120px;"><canvas id="nd-sqlite-mem"></canvas></div>
          </div>
          <div style=""><div style="font-size:10px;color:var(--txt-muted);margin-bottom:4px;text-transform:uppercase;display:flex;align-items:center;gap:6px;">Network RX/TX (1h) <span style="font-size:9px;color:#34d399;background:rgba(52,211,153,.15);padding:1px 4px;border-radius:3px;">NetObserv</span></div><div style="height:100px;"><canvas id="nd-sqlite-net"></canvas></div></div>
          <div style=""><div style="font-size:10px;color:var(--txt-muted);margin-bottom:4px;text-transform:uppercase;display:flex;align-items:center;gap:6px;">Disk 사용률 추이 (1h)<span style="font-size:9px;color:#f59e0b;background:rgba(245,158,11,.15);padding:1px 4px;border-radius:3px;">kubelet</span></div><div style="height:100px;"><canvas id="nd-sqlite-disk"></canvas></div></div>
          <div style=""><div style="font-size:10px;color:var(--txt-muted);margin-bottom:4px;text-transform:uppercase;display:flex;align-items:center;gap:6px;">Disk 사용률 추이 (1h)<span style="font-size:9px;color:#f59e0b;background:rgba(245,158,11,.15);padding:1px 4px;border-radius:3px;">kubelet</span></div><div style="height:100px;"><canvas id="nd-sqlite-disk"></canvas></div></div>
        </div>
        <div id="nd-top-disk" style="margin-top:14px;display:none;">
          <div style="font-size:10px;color:var(--txt-muted);text-transform:uppercase;margin-bottom:8px;display:flex;align-items:center;gap:6px;">Pod 디스크 상위 <span style="font-size:9px;background:rgba(6,182,212,.15);color:#06b6d4;padding:1px 4px;border-radius:3px;">container_fs</span></div>
          <div id="nd-top-disk-list"></div>
        </div>
        <div id="nd-sqlite-loading" style="text-align:center;font-size:11px;color:var(--txt-muted);margin-top:8px;">
          SQLite 이력 로딩 중...
        </div>`;
    }

    // SQLite 시계열 로드
    try {
      const d = await fetch(
        `/api/v1/nodes/${encodeURIComponent(nodeName)}/sqlite-trends?hours=1`
      ).then(r => r.json());

      const lbl = d.labels || [];
      const loadingEl = document.getElementById('nd-sqlite-loading');

      if (lbl.length >= 2) {
        ['nd-sqlite-cpu','nd-sqlite-mem'].forEach(id => {
          if (state.charts[id]) { state.charts[id].destroy(); delete state.charts[id]; }
        });
        mkChart('nd-sqlite-cpu', lbl,
          [sparkDataset('CPU %', d.cpu||[], '#3b82f6')], {yMin:0, yMax:100});
        mkChart('nd-sqlite-mem', lbl,
          [sparkDataset('MEM %', d.mem||[], '#a78bfa')], {yMin:0, yMax:100});
        if((d.net_rx||[]).some(v=>v!=null && v>0)){
          const _nr=(d.net_rx||[]).map(v=>(v??0)/1024);
          const _nt=(d.net_tx||[]).map(v=>(v??0)/1024);
          if(state.charts["nd-sqlite-net"]){state.charts["nd-sqlite-net"].destroy();delete state.charts["nd-sqlite-net"];}
          const _c=document.getElementById("nd-sqlite-net");
          if(_c) mkChart("nd-sqlite-net",lbl,[sparkDataset("RX KB/s",_nr,"#06b6d4"),sparkDataset("TX KB/s",_nt,"#34d399")],{legend:true});
        // Disk 사용률 차트
        setTimeout(()=>{
          const _dp=(d.disk_pct||[]).filter(v=>v!=null);
          if(_dp.length>0){
            if(state.charts["nd-sqlite-disk"]){state.charts["nd-sqlite-disk"].destroy();delete state.charts["nd-sqlite-disk"];}
            const _dc=document.getElementById("nd-sqlite-disk");
            if(_dc) mkChart("nd-sqlite-disk",lbl,[sparkDataset("nodefs %",(d.disk_pct||[]).map(v=>v??0),"#f59e0b"),sparkDataset("imagefs %",(d.imagefs_pct||[]).map(v=>v??0),"#fb923c")],{yMin:0,yMax:100,legend:true});
          }
        },300);
        }
        if (loadingEl) loadingEl.textContent =
          `${d.point_count}개 데이터 포인트 (30s 간격, SQLite)`;
      } else {
        if (loadingEl) loadingEl.textContent =
          `데이터 부족 (${lbl.length}개) — 최소 2개 폴링 사이클 후 표시됩니다.`;
      }
    } catch(e) {
      const el = document.getElementById('nd-sqlite-loading');
      if (el) el.textContent = 'SQLite 이력 로드 실패: ' + e.message;
    }
    return;
  }

  // Prometheus 모드: 시계열 차트 로드
  document.getElementById('node-detail-title').textContent = nodeName + ' — 시계열 (1h)';
  ['nd-chart-cpu','nd-chart-mem','nd-chart-net','nd-chart-disk'].forEach(id => {
    if (state.charts[id]) { state.charts[id].destroy(); delete state.charts[id]; }
    const c = document.getElementById(id);
    if (c) c.insertAdjacentHTML('afterend', '<div style="font-size:11px;color:var(--txt-muted);padding:8px;">로딩 중...</div>');
  });
  try {
    const d = await fetch(`/api/v1/nodes/${encodeURIComponent(nodeName)}/trends?hours=1`).then(r => r.json());
    const lbl = d.labels || [];
    [['nd-chart-cpu','CPU %',d.cpu,'#3b82f6',0,100],
     ['nd-chart-mem','Mem %',d.mem,'#a78bfa',0,100],
    ].forEach(([id,lbl2,vals,color,mn,mx]) => {
      document.getElementById(id)?.nextSibling?.remove?.();
      mkChart(id, lbl, [sparkDataset(lbl2, vals||[], color)], {yMin:mn,yMax:mx});
    });
    mkChart('nd-chart-net', lbl, [
      sparkDataset('RX', d.net_rx||[], '#06b6d4'),
      sparkDataset('TX', d.net_tx||[], '#34d399'),
    ], {legend:true});
    mkChart('nd-chart-disk', lbl, [
      sparkDataset('Read', d.disk_r||[], '#f59e0b'),
      sparkDataset('Write', d.disk_w||[], '#f87171'),
    ], {legend:true});
    document.querySelectorAll('[id^="nd-chart-"]').forEach(c => c.nextSibling?.remove?.());
  } catch(e) {
    document.getElementById('node-detail-title').textContent += ' (데이터 없음)';
  }
}

function renderNodes(nodes) {
  document.getElementById('tbody-nodes').innerHTML = nodes.map(n => {
    const cpc = pbarColor(n.cpu_pct), mpc = pbarColor(n.mem_pct);
    return `<tr style="cursor:pointer;" onclick="openNodeDetail('${n.name}')">
      <td class="mono" style="color:var(--accent2)">${n.name}</td>
      <td><span class="badge badge-off">${n.roles}</span></td>
      <td><span class="dot ${n.status==='Ready'?'dot-ok':'dot-err'}"></span> ${n.status}</td>
      <td>${n.age}</td>
      <td class="mono">${fmtCpuRaw(n.cpu_usage)}</td>
      <td><div class="pbar-wrap"><div class="pbar"><div class="pbar-fill ${cpc}" style="width:${n.cpu_pct||0}%"></div></div><span style="font-size:10px;color:var(--txt-secondary);width:36px">${fmtPct(n.cpu_pct)}</span></div></td>
      <td class="mono">${fmtMemRaw(n.mem_usage)}</td>
      <td><div class="pbar-wrap"><div class="pbar"><div class="pbar-fill ${mpc}" style="width:${n.mem_pct||0}%"></div></div><span style="font-size:10px;color:var(--txt-secondary);width:36px">${fmtPct(n.mem_pct)}</span></div></td>
      <td class="mono" style="font-size:10px;color:var(--txt-secondary);"
          title="${n.net_rx_bps==null?(_nodeNetAvailable?'수집 중':'환경 제약: node-exporter 미노출'):''}"
          >${n.net_rx_bps!=null?fmtBps(n.net_rx_bps):(_nodeNetAvailable?'...':'N/A')}${n.net_rx_bps==null&&!_nodeNetAvailable?'⚠':''}</td>
      <td class="mono" style="font-size:10px;color:var(--txt-secondary);"
          title="${n.net_tx_bps==null?(_nodeNetAvailable?'수집 중':'환경 제약: node-exporter 미노출'):''}"
          >${n.net_tx_bps!=null?fmtBps(n.net_tx_bps):(_nodeNetAvailable?'...':'N/A')}${n.net_tx_bps==null&&!_nodeNetAvailable?'⚠':''}</td>
      <td>${n.eph_capacity_bytes ? ('<span class="badge '+(n.eph_allocatable_bytes/n.eph_capacity_bytes<0.1?'badge-err':n.eph_allocatable_bytes/n.eph_capacity_bytes<0.2?'badge-warn':'badge-ok')+'">'+Math.round((1-n.eph_allocatable_bytes/n.eph_capacity_bytes)*100)+'%</span>') : '<span style="color:var(--txt-muted)">—</span>'}</td>
    </tr>`;
  }).join('');
}

// ═══════════════════════════════════════════════
// ═══════════════════════════════════════════════
// VM DETAIL MODAL
// ═══════════════════════════════════════════════
async function openVMDetail(vmName, ns) {
  const vm = _allVMs.find(v => v.name === vmName && v.namespace === ns);
  if (!vm) return;

  // 모달 열기
  const modal = document.getElementById('vm-modal');
  modal.style.display = '';
  document.getElementById('vm-modal-title').textContent = vmName;
  document.getElementById('vm-modal-sub').textContent = ns + '  ·  ' + vm.node + '  ·  ' + (vm.os || '—');
  document.getElementById('vm-modal-loading').textContent = '시계열 로딩 중...';

  // 정보 카드
  document.getElementById('vm-modal-info').innerHTML = [
    ['상태',        statusBadge(vm.status_group)],
    ['IP',          `<span class="mono" style="font-size:11px">${vm.ip}</span>`],
    ['CPU Cores',   vm.cpu_cores],
    ['Memory',      vm.memory_total],
    ['CPU 사용률',  fmtPct(vm.cpu_pct)],
    ['Mem 사용률',  fmtPct(vm.mem_pct)],
    ['Net RX',      fmtBps(vm.net_rx_bps)],
    ['Net TX',      fmtBps(vm.net_tx_bps)],
    ['Disk Used',   vm.disk_used_bytes ? fmtBytes(vm.disk_used_bytes) : '—'],
    ['Age',         vm.age],
  ].map(([l,v]) => `<div style="background:var(--bg-card2);border:1px solid var(--bd);border-radius:7px;padding:8px 10px;">
    <div style="font-size:9px;color:var(--txt-muted);margin-bottom:3px;text-transform:uppercase;">${l}</div>
    <div style="font-size:12px;font-weight:600;">${v}</div>
  </div>`).join('');

  // 볼륨 정보 (root/data 구분)
  const vols = vm.volumes || [];
  if (vols.length) {
    document.getElementById('vm-modal-volumes').innerHTML =
      '<div style="font-size:11px;font-weight:700;color:var(--txt-secondary);margin-bottom:6px;text-transform:uppercase;letter-spacing:.06em;">Volumes</div>' +
      '<div style="display:flex;flex-wrap:wrap;gap:6px;">' +
      vols.map(vl => {
        const isRoot = (vl.name||'').endsWith('-rootdisk') || (vl.pvc||'').endsWith('-rootdisk');
        const used = vl.used_bytes || 0;
        const cap  = vl.capacity_bytes || 0;
        const pct  = cap ? Math.round(used/cap*100) : 0;
        const dtype = isRoot ? 'Root' : 'Data';
        const dc = isRoot ? 'var(--accent2)' : 'var(--accent)';
        return `<div style="background:var(--bg-card2);border:1px solid var(--bd);border-radius:7px;padding:8px 12px;min-width:160px;">
          <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
            <span style="font-size:10px;font-weight:700;color:${dc};">${dtype}</span>
            <span style="font-size:10px;color:var(--txt-muted);">${vl.name||vl.pvc||'—'}</span>
          </div>
          ${cap ? `<div style="font-size:11px;color:var(--txt-secondary);">${fmtBytes(used)} / ${fmtBytes(cap)}</div>
          <div class="pbar" style="margin-top:4px;"><div class="pbar-fill" style="background:${dc};width:${pct}%"></div></div>` : '<div style="font-size:11px;color:var(--txt-muted);">용량 정보 없음</div>'}
        </div>`;
      }).join('') + '</div>';
  } else {
    document.getElementById('vm-modal-volumes').innerHTML = '';
  }

  // 차트 초기화
  ['vm-chart-cpu','vm-chart-mem','vm-chart-net','vm-chart-disk'].forEach(id => {
    if (state.charts[id]) { state.charts[id].destroy(); delete state.charts[id]; }
  });

  // 시계열 데이터 로드
  try {
    const d = await fetch(`/api/v1/vms/${encodeURIComponent(ns)}/${encodeURIComponent(vmName)}/trends?hours=1`).then(r => r.json());
    const lbl = d.labels || [];
    const hasData = lbl.length > 0;

    if (hasData) {
      mkChart('vm-chart-cpu', lbl, [sparkDataset('CPU %', d.cpu||[], '#3b82f6')], {yMin:0,yMax:100});
      mkChart('vm-chart-mem', lbl, [sparkDataset('Mem %', d.mem_pct||[], '#a78bfa')], {yMin:0,yMax:100});
      mkChart('vm-chart-net', lbl, [
        sparkDataset('RX', d.net_rx||[], '#06b6d4'),
        sparkDataset('TX', d.net_tx||[], '#34d399'),
      ], {legend:true});
      mkChart('vm-chart-disk', lbl, [
        sparkDataset('Read', d.disk_r||[], '#f59e0b'),
        sparkDataset('Write', d.disk_w||[], '#f87171'),
      ], {legend:true});
      document.getElementById('vm-modal-loading').textContent = '';
    } else {
      document.getElementById('vm-modal-loading').textContent = vm.status_group !== 'running'
        ? 'VM이 실행 중이 아니어서 시계열 데이터가 없습니다.'
        : 'Prometheus 데이터가 아직 없습니다. (최소 2분 필요)';
    }
  } catch(e) {
    document.getElementById('vm-modal-loading').textContent = '시계열 로드 실패: ' + e.message;
  }
}

// 모달 외부 클릭 시 닫기
document.getElementById('vm-modal').addEventListener('click', function(e) {
  if (e.target === this) this.style.display = 'none';
});

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
  document.getElementById('tbody-vms').innerHTML = filtered.map(v => {
    // root/data 디스크 구분
    const vols = v.volumes || [];
    const rootVol = vols.find(vl => (vl.name||'').endsWith('-rootdisk') || (vl.pvc||'').endsWith('-rootdisk'));
    const dataVols = vols.filter(vl => vl !== rootVol);
    const rootUsed = rootVol?.used_bytes || 0;
    const dataUsed = dataVols.reduce((s,vl) => s + (vl.used_bytes||0), 0);
    const diskTotal = v.disk_used_bytes || 0;
    return `<tr style="cursor:pointer;" onclick="openVMDetail('${v.name}','${v.namespace}')">
      <td class="mono" style="color:var(--accent2)">${v.name}</td>
      <td><span class="badge badge-off" style="max-width:100px;overflow:hidden;text-overflow:ellipsis;">${v.namespace}</span></td>
      <td>${statusBadge(v.status_group)}</td>
      <td style="color:var(--txt-secondary);font-size:11px;">${v.node.split('.')[0]}</td>
      <td class="mono" style="font-size:11px;">${v.ip}</td>
      <td style="text-align:center;">${v.cpu_cores}</td>
      <td><div class="pbar-wrap"><div class="pbar"><div class="pbar-fill ${pbarColor(v.cpu_pct)}" style="width:${v.cpu_pct||0}%"></div></div><span style="font-size:10px;color:var(--txt-secondary);width:36px">${fmtPct(v.cpu_pct)}</span></div></td>
      <td style="font-size:11px;">${v.memory_total}</td>
      <td><div class="pbar-wrap"><div class="pbar"><div class="pbar-fill ${pbarColor(v.mem_pct)}" style="width:${v.mem_pct||0}%"></div></div><span style="font-size:10px;color:var(--txt-secondary);width:36px">${fmtPct(v.mem_pct)}</span></div></td>
      <td class="mono" style="font-size:10px;">${diskTotal ? fmtBytes(diskTotal) : '—'}</td>
      <td class="mono" style="font-size:10px;">${fmtBps(v.net_rx_bps)}</td>
      <td class="mono" style="font-size:10px;">${fmtBps(v.net_tx_bps)}</td>
      <td style="font-size:11px;">${v.age}</td>
      <td style="font-size:10px;color:var(--accent);text-align:center;">📈</td>
    </tr>`;
  }).join('') || '<tr><td colspan="14" style="text-align:center;color:var(--txt-muted);padding:24px;">VM 없음</td></tr>';
}
document.getElementById('vm-search').addEventListener('input', filterVMs);
document.getElementById('vm-ns-filter').addEventListener('change', filterVMs);
document.getElementById('vm-status-filter').addEventListener('change', filterVMs);

// ═══════════════════════════════════════════════
// RENDER: STORAGE
// ═══════════════════════════════════════════════
function renderStorage(st) {
  const vmMap = st.pvc_vm_map || {};

  // ── Top 5 최대 할당 PV ──────────────────────────────────────
  const pvsSorted = [...(st.pvs || [])]
    .filter(p => p.capacity_bytes > 0)
    .sort((a, b) => b.capacity_bytes - a.capacity_bytes)
    .slice(0, 5);

  const maxPvBytes = pvsSorted[0]?.capacity_bytes || 1;
  document.getElementById('top5-pv').innerHTML = pvsSorted.length ? pvsSorted.map((p, i) => {
    const pct = Math.round(p.capacity_bytes / maxPvBytes * 100);
    // Claim 형식: "namespace/pvc-name" 또는 "Unbound"
    const claim = p.Claim || 'Unbound';
    const isUnbound = claim === 'Unbound';
    const claimParts = claim.split('/');
    const claimNs   = claimParts.length > 1 ? claimParts[0] : '—';
    const claimName = claimParts.length > 1 ? claimParts.slice(1).join('/') : claim;
    const vmName    = vmMap[claim] || '';
    const scBadge   = p.Status === 'Bound' ? 'badge-ok' : 'badge-warn';
    return `<div style="padding:8px 0;border-bottom:1px solid var(--bd);">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px;">
        <div style="display:flex;align-items:center;gap:6px;min-width:0;flex:1;">
          <span style="color:var(--txt-muted);font-size:11px;font-weight:700;flex-shrink:0;">#${i+1}</span>
          <div style="min-width:0;">
            <div class="mono" style="font-size:11px;color:var(--txt-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:160px;"
                 title="${p.Name}">${p.Name}</div>
            <div style="display:flex;align-items:center;gap:4px;margin-top:2px;flex-wrap:wrap;">
              ${!isUnbound ? `<span class="badge badge-off" style="font-size:9px;">${claimNs}</span>` : ''}
              ${!isUnbound ? `<span style="font-size:10px;color:var(--txt-secondary);">${claimName}</span>` : ''}
              ${vmName ? `<span class="badge badge-ok" style="font-size:9px;">VM: ${vmName}</span>` : ''}
              ${isUnbound ? `<span class="badge badge-warn" style="font-size:9px;">Unbound</span>` : ''}
            </div>
          </div>
        </div>
        <span style="font-size:12px;font-weight:700;color:#f59e0b;flex-shrink:0;margin-left:6px;">${fmtBytes(p.capacity_bytes)}</span>
      </div>
      <div style="display:flex;align-items:center;gap:6px;">
        <div class="pbar" style="flex:1;"><div class="pbar-fill" style="background:#f59e0b;width:${pct}%"></div></div>
        <span class="badge ${scBadge}" style="font-size:9px;">${p.Status}</span>
        <span style="font-size:9px;color:var(--txt-muted);">${p.StorageClass}</span>
      </div>
    </div>`;
  }).join('') : '<div style="color:var(--txt-muted);font-size:12px;text-align:center;padding:12px;">PV 없음</div>';

  // ── Top 5 실제 사용량 PVC ────────────────────────────────────
  const pvcsSorted = [...(st.pvcs || [])]
    .filter(p => p.used_bytes > 0)
    .sort((a, b) => b.used_bytes - a.used_bytes)
    .slice(0, 5);

  const maxPvcBytes = pvcsSorted[0]?.used_bytes || 1;
  document.getElementById('top5-pvc-used').innerHTML = pvcsSorted.length ? pvcsSorted.map((p, i) => {
    const pct    = Math.round(p.used_bytes / maxPvcBytes * 100);
    const capPct = p.capacity_bytes ? Math.round(p.used_bytes / p.capacity_bytes * 100) : null;
    const key    = p.Namespace + '/' + p.Name;
    const vmName = vmMap[key] || '';
    const capPctColor = capPct == null ? 'var(--txt-muted)' : capPct < 70 ? 'var(--success)' : capPct < 90 ? 'var(--warn)' : 'var(--danger)';
    return `<div style="padding:8px 0;border-bottom:1px solid var(--bd);">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px;">
        <div style="display:flex;align-items:center;gap:6px;min-width:0;flex:1;">
          <span style="color:var(--txt-muted);font-size:11px;font-weight:700;flex-shrink:0;">#${i+1}</span>
          <div style="min-width:0;">
            <div style="display:flex;align-items:center;gap:4px;flex-wrap:wrap;">
              <span class="badge badge-off" style="font-size:9px;">${p.Namespace}</span>
              <span class="mono" style="font-size:11px;color:var(--txt-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:120px;"
                    title="${p.Name}">${p.Name}</span>
            </div>
            <div style="display:flex;align-items:center;gap:4px;margin-top:2px;">
              ${vmName ? `<span class="badge badge-ok" style="font-size:9px;">VM: ${vmName}</span>` : '<span style="font-size:10px;color:var(--txt-muted);">VM 미연결</span>'}
              ${capPct != null ? `<span style="font-size:10px;color:${capPctColor};font-weight:700;">용량의 ${capPct}%</span>` : ''}
            </div>
          </div>
        </div>
        <div style="text-align:right;flex-shrink:0;margin-left:6px;">
          <div style="font-size:12px;font-weight:700;color:#34d399;">${fmtBytes(p.used_bytes)}</div>
          ${p.capacity_bytes ? `<div style="font-size:10px;color:var(--txt-muted);">/ ${fmtBytes(p.capacity_bytes)}</div>` : ''}
        </div>
      </div>
      <div class="pbar"><div class="pbar-fill" style="background:#34d399;width:${pct}%"></div></div>
    </div>`;
  }).join('') : `<div style="text-align:center;padding:16px;">
    <div style="color:var(--txt-muted);font-size:12px;margin-bottom:8px;">실제 사용량 데이터 없음</div>
    <div style="background:rgba(245,158,11,.08);border:1px solid rgba(245,158,11,.2);border-radius:7px;padding:10px;font-size:11px;color:#fbbf24;text-align:left;">
      <div style="font-weight:700;margin-bottom:6px;">⚙ 수집 전략 (자동 시도 순서):</div>
      <div style="color:var(--txt-secondary);line-height:1.8;">
        1️⃣ Thanos PromQL — kubelet_volume_stats_* <br>
        2️⃣ kubelet proxy — /api/v1/nodes/{node}/proxy/metrics<br>
        3️⃣ oc exec — virt-launcher pod df 명령<br>
        <br>
        <span style="color:var(--txt-muted);">현재 소스: <strong style="color:${st.pvc_disk_source||'none'}">${st.pvc_disk_source||'none'}</strong></span><br>
        Running 상태의 VM이 있어야 수집 가능합니다.
      </div>
    </div>
  </div>`;

  // ── Storage Pools ────────────────────────────────────────────
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

  _nodeDataSource = d.node_data_source || 'cli';
  _nodeNetAvailable = d.node_net_available || false;
  renderOverview(d);
  renderNodes(d.nodes || []);
  renderNodeCards();
  renderVMs(d.vms || []);
  renderStorage(d.storage || {});
  renderInfra(d.infra || {});
  if (state.currentPage === 'performance') renderPerformance(d);
}

let _engine_prom_reachable = false;

// ═══════════════════════════════════════════════
// ALERTS PAGE
// ═══════════════════════════════════════════════
let _alertsData = null;
let _allAlerts = [];

async function fetchAlerts() {
  try {
    const d = await fetch('/api/v1/alerts').then(r => r.json());
    _alertsData = d;
    _allAlerts = d.alerts || [];
    renderAlerts(d);
  } catch(e) { console.error('alerts fetch error', e); }
}

function renderAlerts(d) {
  const counts = d.counts || {};
  const crit = counts.critical || 0, warn = counts.warning || 0,
        info = counts.info || 0, none = counts.none || 0;
  const total = _allAlerts.length;

  // KPI
  document.getElementById('alert-kpi-row').innerHTML = [
    { label:'Total Firing',  value: total,  color: total>0?'var(--danger)':'var(--success)' },
    { label:'Critical',      value: crit,   color: crit>0?'var(--danger)':'var(--txt-muted)' },
    { label:'Warning',       value: warn,   color: warn>0?'var(--warn)':'var(--txt-muted)' },
    { label:'Info',          value: info,   color: 'var(--accent2)' },
  ].map(k => `<div class="kpi">
    <div class="label">${k.label}</div>
    <div class="value" style="color:${k.color};">${k.value}</div>
  </div>`).join('');

  // 심각도 분포 바
  const t = total || 1;
  document.getElementById('alert-sev-dist').innerHTML = [
    ['Critical', crit, 'var(--danger)'],
    ['Warning',  warn, 'var(--warn)'],
    ['Info',     info, 'var(--accent2)'],
    ['None',     none, 'var(--txt-muted)'],
  ].map(([l,c,color]) => `<div style="margin-bottom:8px;">
    <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px;">
      <span style="color:var(--txt-secondary);">${l}</span>
      <span style="color:${color};font-weight:700;">${c}</span>
    </div>
    <div class="pbar"><div class="pbar-fill" style="background:${color};width:${Math.round(c/t*100)}%"></div></div>
  </div>`).join('');

  // 트렌드 차트
  const trend = d.trend || {};
  if ((trend.labels||[]).length > 0) {
    mkChart('chart-alert-trend', trend.labels, [
      sparkDataset('Total', trend.total||[], '#ef4444'),
      sparkDataset('Critical', trend.critical||[], '#7f1d1d'),
    ], {legend:true, yMin:0});
  }

  // 인시던트 배너 업데이트
  filterAlerts();
}

function filterAlerts() {
  const sev = document.getElementById('alert-sev-filter')?.value || '';
  const search = (document.getElementById('alert-search')?.value || '').toLowerCase();
  const filtered = _allAlerts.filter(a =>
    (!sev || a.severity === sev) &&
    (!search || a.alertname.toLowerCase().includes(search) || (a.namespace||'').toLowerCase().includes(search))
  );
  const sevColor = { critical:'var(--danger)', warning:'var(--warn)', info:'var(--accent2)', none:'var(--txt-muted)' };
  document.getElementById('tbody-alerts').innerHTML = filtered.map(a => {
    const c = sevColor[a.severity] || 'var(--txt-muted)';
    return `<tr>
      <td><span class="badge" style="background:${c}22;color:${c};border:1px solid ${c}55;">${a.severity.toUpperCase()}</span></td>
      <td style="font-weight:600;color:var(--txt-primary);">${a.alertname}</td>
      <td><span class="badge badge-off">${a.namespace||'cluster'}</span></td>
      <td style="font-size:11px;color:var(--txt-muted);">${a.service||a.pod||'—'}</td>
      <td style="font-size:11px;color:var(--txt-muted);">${a.node||'—'}</td>
      <td style="font-size:11px;color:var(--txt-secondary);max-width:300px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
          title="${a.description||a.summary||''}">${a.summary||a.description||'—'}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="6" style="text-align:center;color:var(--txt-muted);padding:20px;">Firing 알럿 없음 ✅</td></tr>';
}
document.getElementById('alert-sev-filter')?.addEventListener('change', filterAlerts);
document.getElementById('alert-search')?.addEventListener('input', filterAlerts);

// ═══════════════════════════════════════════════
// ROUTE SLO PAGE
// ═══════════════════════════════════════════════
let _routesData = null;

async function fetchRoutes() {
  const hours = document.getElementById('slo-hours')?.value || 24;
  try {
    const d = await fetch(`/api/v1/routes?hours=${hours}`).then(r => r.json());
    _routesData = d;
    renderRoutes(d);
  } catch(e) { console.error('routes fetch error', e); }
}

function renderRoutes(d) {
  const current = d.current || [];
  const slo = d.slo || [];
  const up = current.filter(r => r.is_up).length;
  const down = current.filter(r => !r.is_up).length;
  const avgLat = current.length ? Math.round(current.reduce((s,r) => s+(r.latency_ms||0),0)/current.length) : 0;
  const sloAvg = slo.length ? (slo.reduce((s,r) => s+r.slo_pct,0)/slo.length).toFixed(1) : '—';

  // KPI
  document.getElementById('route-kpi-row').innerHTML = [
    { label:'Total Routes', value: current.length, color:'var(--txt-primary)' },
    { label:'Up',           value: up,   color:'var(--success)' },
    { label:'Down',         value: down, color: down>0?'var(--danger)':'var(--txt-muted)' },
    { label:'Avg Latency',  value: avgLat+'ms', color:'var(--accent2)' },
    { label:'Avg SLO',      value: sloAvg+'%',  color: parseFloat(sloAvg)<99?'var(--warn)':'var(--success)' },
  ].map(k => `<div class="kpi">
    <div class="label">${k.label}</div>
    <div class="value" style="color:${k.color};font-size:20px;">${k.value}</div>
  </div>`).join('');

  // 현재 상태 테이블
  document.getElementById('tbody-routes-current').innerHTML = current.map(r => {
    const latC = r.latency_ms < 200 ? 'var(--success)' : r.latency_ms < 1000 ? 'var(--warn)' : 'var(--danger)';
    const sc = r.status_code || 0;
    const scColor = sc >= 500 ? 'var(--danger)' : sc >= 400 ? 'var(--warn)' : sc > 0 ? 'var(--success)' : 'var(--txt-muted)';
    return `<tr>
      <td class="mono" style="font-size:11px;">${r.name}</td>
      <td><span class="badge badge-off">${r.namespace}</span></td>
      <td style="font-size:10px;color:var(--txt-muted);max-width:200px;overflow:hidden;text-overflow:ellipsis;" title="${r.host}">${r.host}</td>
      <td>
        <div style="display:flex;align-items:center;gap:5px;">
          <span class="dot ${r.is_up?'dot-ok':'dot-err'}"></span>
          <span class="mono" style="font-size:11px;color:${scColor};">${sc||'—'}</span>
        </div>
      </td>
      <td style="color:${latC};font-family:monospace;font-size:11px;">${r.latency_ms?r.latency_ms.toFixed(0)+'ms':'—'}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="5" style="text-align:center;color:var(--txt-muted);padding:20px;">Route 없음 (oc login 필요)</td></tr>';

  // SLO 테이블
  document.getElementById('tbody-routes-slo').innerHTML = slo.map(r => {
    const sloC = r.slo_pct >= 99.9 ? 'var(--success)' : r.slo_pct >= 99 ? 'var(--accent2)' : r.slo_pct >= 95 ? 'var(--warn)' : 'var(--danger)';
    return `<tr>
      <td class="mono" style="font-size:11px;">${r.name}</td>
      <td><span class="badge badge-off">${r.namespace}</span></td>
      <td>
        <div class="pbar-wrap">
          <div class="pbar"><div class="pbar-fill" style="background:${sloC};width:${r.slo_pct}%"></div></div>
          <span style="font-size:11px;font-weight:700;color:${sloC};width:50px;">${r.slo_pct}%</span>
        </div>
      </td>
      <td class="mono" style="font-size:11px;">${r.avg_latency_ms}ms</td>
      <td style="font-size:11px;color:var(--txt-muted);">${r.total_probes}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="5" style="text-align:center;color:var(--txt-muted);padding:20px;">이력 없음 (수집 중)</td></tr>';

  // UWM 상태
  renderUWM();
}

async function renderUWM() {
  try {
    const uwm = await fetch('/api/v1/uwm').then(r => r.json());
    const el = document.getElementById('uwm-content');
    if (!el) return;
    if (uwm.enabled) {
      el.innerHTML = `<div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;">
        <span class="dot dot-ok"></span>
        <span style="font-weight:700;color:var(--success);">활성화됨</span>
        <span style="font-size:11px;color:var(--txt-muted);">Prometheus ${uwm.prometheus_count}개</span>
      </div>
      ${uwm.targets?.length ? `<div style="font-size:11px;color:var(--txt-secondary);margin-bottom:6px;">수집 중인 사용자 네임스페이스:</div>
      <div style="display:flex;flex-wrap:wrap;gap:6px;">
        ${uwm.targets.map(t => `<span class="badge badge-ok">${t.namespace} (${t.count})</span>`).join('')}
      </div>` : '<div style="font-size:11px;color:var(--txt-muted);">수집 대상 없음</div>'}`;
    } else {
      el.innerHTML = `<div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;">
        <span class="dot dot-off"></span>
        <span style="color:var(--txt-muted);font-weight:700;">비활성화됨</span>
      </div>
      <div style="background:rgba(59,130,246,.08);border:1px solid rgba(59,130,246,.2);border-radius:7px;padding:12px;">
        <div style="font-size:11px;font-weight:700;color:var(--accent);margin-bottom:6px;">활성화 방법:</div>
        <code style="font-size:10px;color:var(--txt-secondary);display:block;line-height:1.6;">
          oc -n openshift-monitoring edit configmap cluster-monitoring-config<br>
          # enableUserWorkload: true 추가
        </code>
      </div>`;
    }
  } catch(e) {}
}

document.getElementById('slo-hours')?.addEventListener('change', fetchRoutes);

// ═══════════════════════════════════════════════
// INCIDENTS PAGE
// ═══════════════════════════════════════════════
async function fetchIncidents() {
  try {
    const d = await fetch('/api/v1/incidents').then(r => r.json());
    renderIncidents(d);
    // 인시던트 배너 업데이트
    const banner = document.getElementById('incident-banner');
    const bannerList = document.getElementById('incident-banner-list');
    if (banner && bannerList && d.active?.length > 0) {
      banner.style.display = '';
      bannerList.innerHTML = d.active.slice(0,3).map(i =>
        `<div style="font-size:11px;color:var(--txt-secondary);">• ${i.title} (${i.started_at})</div>`
      ).join('');
    } else if (banner) {
      banner.style.display = 'none';
    }
  } catch(e) {}
}

function renderIncidents(d) {
  const sevColor = { critical:'var(--danger)', warning:'var(--warn)', info:'var(--accent2)' };
  const mkCard = (inc) => {
    const c = sevColor[inc.severity] || 'var(--txt-muted)';
    return `<div style="background:var(--bg-card2);border:1px solid ${c}33;border-radius:8px;padding:12px;margin-bottom:8px;">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">
        <div style="display:flex;align-items:center;gap:8px;">
          <span class="badge" style="background:${c}22;color:${c};border:1px solid ${c}55;font-size:9px;">${(inc.severity||'').toUpperCase()}</span>
          <span style="font-size:12px;font-weight:700;color:var(--txt-primary);">${inc.title}</span>
        </div>
        <span style="font-size:10px;color:var(--txt-muted);">${inc.started_at}</span>
      </div>
      <div style="font-size:11px;color:var(--txt-secondary);margin-bottom:4px;">영향: ${inc.affected||'—'}</div>
      ${inc.description?`<div style="font-size:10px;color:var(--txt-muted);">${inc.description}</div>`:''}
      ${inc.resolved_at?`<div style="font-size:10px;color:var(--success);margin-top:4px;">✅ 해소: ${inc.resolved_at}</div>`:''}
    </div>`;
  };
  const activeEl = document.getElementById('incidents-active');
  const resolvedEl = document.getElementById('incidents-resolved');
  if (activeEl) activeEl.innerHTML = (d.active||[]).length
    ? d.active.map(mkCard).join('')
    : '<div style="text-align:center;padding:20px;color:var(--success);">✅ 활성 인시던트 없음</div>';
  if (resolvedEl) resolvedEl.innerHTML = (d.resolved||[]).length
    ? d.resolved.map(mkCard).join('')
    : '<div style="text-align:center;padding:20px;color:var(--txt-muted);">해소된 인시던트 없음</div>';
}

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
      if (state.currentPage === 'alerts') fetchAlerts();
      if (state.currentPage === 'routes') fetchRoutes();
      if (state.currentPage === 'incidents') fetchIncidents();
      fetchIncidents(); // 인시던트 배너 항상 갱신
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
        loadNodeConditions();
        fetchIncidents();       // 인시던트 배너 초기 로드
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

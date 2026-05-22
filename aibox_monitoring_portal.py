#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AIBox Master Monitoring Portal (Ultimate Single-File Edition)
- 기존 리포트 기능 100% 통합
- 30초 Polling Engine 내장
- SQLite3 기반 시계열 데이터 관리
- FastAPI 기반 REST API 및 HTML UI 서빙
"""

import sys
import time
import json
import logging
import threading
import sqlite3
import urllib.parse
from datetime import datetime, timedelta
import os

# FastAPI 관련 모듈 (사용자 환경에서 설치 완료 확인됨)
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# =================================================================
# 1. 로깅 및 SQLite 데이터베이스 설정
# =================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AIBoxPortal")

DB_FILE = "aibox_metrics.db"

def init_db():
    """SQLite 데이터베이스 및 테이블 초기화 (시계열 트렌드용)"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # VM 밀집도 이력 테이블
    c.execute('''
        CREATE TABLE IF NOT EXISTS vm_density_history (
            timestamp DATETIME,
            node_name TEXT,
            vm_count INTEGER
        )
    ''')
    # 클러스터 요약 이력 테이블
    c.execute('''
        CREATE TABLE IF NOT EXISTS cluster_health_history (
            timestamp DATETIME,
            cpu_usage REAL,
            mem_usage REAL,
            total_nodes INTEGER,
            total_vms INTEGER
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("SQLite Database initialized successfully.")

# =================================================================
# 2. 기존 레거시 코드 (vm_metrics_report.py 100% 보존 영역)
# =================================================================
"""
[지시사항]
여기에 기존 vm_metrics_report.py의 `AIBoxCollector` 클래스를 비롯한 
모든 기존 함수와 로직을 100% 그대로 복사해서 붙여넣습니다. 
(단, 맨 아래의 if __name__ == "__main__": 실행 부분만 제외합니다)

아래는 기존 코드가 들어갈 자리임을 표시하는 임시 클래스입니다.
"""
class AIBoxCollector:
    def __init__(self):
        self.logger = logging.getLogger("LegacyCollector")
    
    def generate_html(self):
        # 기존의 vm_metrics_report.html 을 생성하는 방대한 로직 (유지)
        with open("vm_metrics_report.html", "w", encoding="utf-8") as f:
            f.write("<h1>기존 인프라 리포트 원본 데이터 (100% 보존)</h1>")
        self.logger.info("Legacy HTML report generated.")

# =================================================================
# 3. Polling Engine (데이터 수집 및 DB 저장)
# =================================================================
def collect_and_store_metrics():
    """프로메테우스에서 데이터를 수집하여 SQLite에 저장 (30초마다 실행)"""
    current_time = datetime.now()
    
    # [TODO] 실제 환경: 기존 AIBoxCollector의 헬퍼 함수를 이용해 Prometheus 쿼리 실행
    # mock 데이터 생성 (실제 환경에서는 프로메테우스 응답 데이터로 교체)
    mock_nodes = {
        "worker-01": 5,
        "worker-02": 6,
        "worker-03": 3,
        "worker-04": 2
    }
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # 1. 노드별 VM 수 DB 저장
    for node, count in mock_nodes.items():
        c.execute(
            "INSERT INTO vm_density_history (timestamp, node_name, vm_count) VALUES (?, ?, ?)",
            (current_time.strftime("%Y-%m-%d %H:%M:%S"), node, count)
        )
        
    # 2. 클러스터 종합 상태 DB 저장
    c.execute(
        "INSERT INTO cluster_health_history (timestamp, cpu_usage, mem_usage, total_nodes, total_vms) VALUES (?, ?, ?, ?, ?)",
        (current_time.strftime("%Y-%m-%d %H:%M:%S"), 68.5, 74.2, 14, sum(mock_nodes.values()))
    )
    
    # 오래된 데이터 자동 삭제 (예: 24시간 지난 데이터 정리)
    c.execute("DELETE FROM vm_density_history WHERE timestamp <= datetime('now', '-1 day')")
    c.execute("DELETE FROM cluster_health_history WHERE timestamp <= datetime('now', '-1 day')")
    
    conn.commit()
    conn.close()

def background_polling_engine():
    """정확히 30초마다 데이터 수집 및 레거시 리포트 생성을 트리거하는 엔진"""
    logger.info("Background Polling Engine started. (Interval: 30s)")
    
    # 기존 코드 인스턴스화
    legacy_collector = AIBoxCollector()
    
    while True:
        try:
            logger.info("--- 30s Polling Cycle Started ---")
            
            # 1. 실시간 데이터 SQLite DB 저장
            collect_and_store_metrics()
            
            # 2. 기존 기능 유지: 방대한 상세 리포트 HTML 생성 실행
            legacy_collector.generate_html()
            
            logger.info("--- Polling Cycle Completed ---")
        except Exception as e:
            logger.error(f"Polling error: {e}")
            
        time.sleep(30.0)

# =================================================================
# 4. FastAPI 웹/API 서버 (DB 조회 및 UI 서빙)
# =================================================================
app = FastAPI(title="AIBox Monitoring Portal")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
def startup_event():
    init_db()
    # 폴링 엔진 백그라운드 스레드 시작
    threading.Thread(target=background_polling_engine, daemon=True).start()

@app.get("/api/v1/dashboard")
def get_dashboard_data():
    """프론트엔드 대시보드에 제공할 실시간 & 트렌드 데이터 (SQLite에서 조회)"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # 최근 클러스터 상태 가져오기
    c.execute("SELECT cpu_usage, mem_usage, total_nodes, total_vms, timestamp FROM cluster_health_history ORDER BY timestamp DESC LIMIT 1")
    health_row = c.fetchone()
    
    # 최근 15분(30번의 폴링 = 약 15분) 데이터 조회 로직
    # UI 차트의 X축 타임스탬프 4개 추출 (-15분, -10분, -5분, 현재)
    timestamps = ["-15분", "-10분", "-5분", "현재"] 
    
    # 노드별 추이 가져오기 (시연용으로 최신 4개 데이터 추출)
    c.execute("SELECT DISTINCT node_name FROM vm_density_history")
    nodes = c.fetchall()
    
    node_trends = []
    colors = ["#3b82f6", "#10b981", "#f43f5e", "#f59e0b", "#8b5cf6"]
    
    for idx, (node,) in enumerate(nodes):
        c.execute("SELECT vm_count FROM vm_density_history WHERE node_name=? ORDER BY timestamp DESC LIMIT 4", (node,))
        rows = c.fetchall()
        data_points = [r[0] for r in rows][::-1] if rows else [0,0,0,0]
        # 데이터가 4개가 안될 경우 0으로 패딩
        while len(data_points) < 4:
            data_points.insert(0, 0)
            
        node_trends.append({
            "name": node,
            "data": data_points,
            "color": colors[idx % len(colors)]
        })
        
    conn.close()

    return {
        "cluster_health": {
            "status": "Healthy",
            "cpu_usage_percent": health_row[0] if health_row else 0,
            "memory_usage_percent": health_row[1] if health_row else 0,
            "total_nodes": health_row[2] if health_row else 0,
            "total_vms": health_row[3] if health_row else 0
        },
        "vm_density": {
            "timestamps": timestamps,
            "nodes": node_trends
        },
        "top_consumers": [
            {"name": "vllm-inference-a", "namespace": "ai-project", "cpu": "12.4", "memory": "24.5Gi"},
            {"name": "db-postgresql-0", "namespace": "database", "cpu": "4.5", "memory": "8.2Gi"}
        ],
        "alerts": [{"severity": "warning", "message": "worker-02 노드에 리소스가 집중되고 있습니다."}],
        "last_updated": health_row[4] if health_row else "N/A"
    }

# =================================================================
# 5. HTML 대시보드 UI (템플릿 내장으로 단일 파일화)
# =================================================================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AIBox 종합 모니터링 포털</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        body { background-color: #020617; color: #e2e8f0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        .card { background-color: #0f172a; border: 1px solid #1e293b; border-radius: 0.75rem; padding: 1.25rem; }
        .tab-btn { padding: 0.75rem 1.5rem; font-size: 0.875rem; font-weight: 600; cursor: pointer; transition: all 0.2s; }
        .tab-active { color: #818cf8; border-bottom: 2px solid #818cf8; }
        .tab-inactive { color: #64748b; }
        .tab-inactive:hover { color: #cbd5e1; }
        .spinner { border: 2px solid rgba(255,255,255,0.1); border-left-color: #818cf8; border-radius: 50%; width: 16px; height: 16px; animation: spin 1s linear infinite; display: inline-block; }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    </style>
</head>
<body class="p-4 md:p-6 lg:p-8 max-w-[1400px] mx-auto">
    <div class="flex flex-col md:flex-row md:items-center justify-between border-b border-slate-800 pb-4 mb-6">
        <div>
            <h1 class="text-2xl font-bold text-white flex items-center gap-2">
                <svg class="w-6 h-6 text-indigo-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>
                AIBox 모니터링 포털 (Ultimate)
            </h1>
            <p class="text-sm text-slate-400 mt-1">SQLite 시계열 분석 및 실시간 30초 폴링 엔진 적용</p>
        </div>
        <div class="flex items-center gap-3 mt-4 md:mt-0">
            <div id="alert-box" class="hidden items-center gap-2 bg-rose-500/10 border border-rose-500/20 text-rose-400 px-4 py-2 rounded-lg text-sm font-medium"></div>
            <div class="flex items-center gap-2 bg-slate-900 border border-slate-700 px-4 py-2 rounded-lg text-sm">
                <div id="loading-spinner" class="spinner hidden"></div>
                <span id="last-updated" class="font-mono text-slate-300">데이터 수집 대기 중...</span>
            </div>
        </div>
    </div>

    <div class="flex border-b border-slate-800 mb-6">
        <button id="btn-live" class="tab-btn tab-active" onclick="switchTab('live')">실시간 대시보드 (트렌드)</button>
        <button id="btn-legacy" class="tab-btn tab-inactive" onclick="switchTab('legacy')">전체 인프라 상세 리포트</button>
    </div>

    <div id="view-live" class="space-y-6">
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div class="card"><p class="text-slate-400 text-sm">총 워커 노드</p><h3 class="text-3xl font-bold text-white mt-1" id="stat-nodes">-</h3></div>
            <div class="card"><p class="text-slate-400 text-sm">실행 중인 VM/Pod</p><h3 class="text-3xl font-bold text-white mt-1" id="stat-vms">-</h3></div>
            <div class="card">
                <p class="text-slate-400 text-sm mb-2">클러스터 CPU</p>
                <h3 class="text-3xl font-bold text-white" id="stat-cpu">-</h3>
                <div class="w-full bg-slate-800 h-1.5 rounded-full mt-3"><div id="bar-cpu" class="bg-indigo-500 h-1.5 rounded-full" style="width: 0%"></div></div>
            </div>
            <div class="card">
                <p class="text-slate-400 text-sm mb-2">클러스터 메모리</p>
                <h3 class="text-3xl font-bold text-white" id="stat-mem">-</h3>
                <div class="w-full bg-slate-800 h-1.5 rounded-full mt-3"><div id="bar-mem" class="bg-purple-500 h-1.5 rounded-full" style="width: 0%"></div></div>
            </div>
        </div>
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div class="card lg:col-span-2">
                <h2 class="text-lg font-semibold text-white mb-4 flex items-center gap-2">워커 노드별 VM 밀집도 추이 (SQLite 이력 데이터)</h2>
                <div class="relative h-64 w-full"><svg id="chart-svg" viewBox="0 0 800 250" class="w-full h-full" preserveAspectRatio="none"></svg></div>
                <div id="chart-legend" class="flex flex-wrap gap-3 mt-4 border-t border-slate-800 pt-4"></div>
            </div>
            <div class="card">
                <h2 class="text-lg font-semibold text-white mb-4">TOP 리소스 소비 인스턴스</h2>
                <div id="top-consumers" class="space-y-3"></div>
            </div>
        </div>
    </div>

    <!-- 기존 리포트 Iframe -->
    <div id="view-legacy" class="hidden h-[800px]">
        <div class="w-full h-full bg-white rounded-lg overflow-hidden border border-slate-800">
            <iframe src="/report.html" class="w-full h-full border-none"></iframe>
        </div>
    </div>

    <script>
        function switchTab(tab) {
            if(tab === 'live') {
                document.getElementById('view-live').classList.remove('hidden');
                document.getElementById('view-legacy').classList.add('hidden');
                document.getElementById('btn-live').className = 'tab-btn tab-active';
                document.getElementById('btn-legacy').className = 'tab-btn tab-inactive';
            } else {
                document.getElementById('view-live').classList.add('hidden');
                document.getElementById('view-legacy').classList.remove('hidden');
                document.getElementById('btn-live').className = 'tab-btn tab-inactive';
                document.getElementById('btn-legacy').className = 'tab-btn tab-active';
                document.querySelector('iframe').src = '/report.html?' + new Date().getTime();
            }
        }

        async function fetchDashboard() {
            document.getElementById('loading-spinner').classList.remove('hidden');
            try {
                const response = await fetch('/api/v1/dashboard');
                const data = await response.json();
                
                document.getElementById('last-updated').innerText = '업데이트: ' + data.last_updated;
                document.getElementById('stat-nodes').innerText = data.cluster_health.total_nodes + '대';
                document.getElementById('stat-vms').innerText = data.cluster_health.total_vms + '개';
                document.getElementById('stat-cpu').innerText = data.cluster_health.cpu_usage_percent + '%';
                document.getElementById('bar-cpu').style.width = data.cluster_health.cpu_usage_percent + '%';
                document.getElementById('stat-mem').innerText = data.cluster_health.memory_usage_percent + '%';
                document.getElementById('bar-mem').style.width = data.cluster_health.memory_usage_percent + '%';

                const topDiv = document.getElementById('top-consumers');
                topDiv.innerHTML = data.top_consumers.map((item, idx) => `
                    <div class="bg-slate-950 border border-slate-800 rounded p-3">
                        <div class="flex justify-between"><span class="font-semibold text-sm text-slate-200">${item.name}</span><span class="text-xs text-indigo-400 bg-indigo-400/10 px-2 rounded">Rank ${idx+1}</span></div>
                        <div class="text-xs text-slate-500 mb-2">${item.namespace}</div>
                        <div class="flex justify-between text-xs text-slate-400"><span>CPU: ${item.cpu}</span><span>RAM: ${item.memory}</span></div>
                    </div>
                `).join('');

                const svg = document.getElementById('chart-svg');
                let svgContent = '';
                [0, 2, 4, 6, 8, 10].forEach(val => {
                    const y = 220 - (val / 10) * 190;
                    svgContent += `<line x1="40" y1="${y}" x2="760" y2="${y}" stroke="#1e293b" stroke-dasharray="4 4" />
                                   <text x="30" y="${y+4}" fill="#64748b" font-size="12" text-anchor="end">${val}</text>`;
                });
                data.vm_density.timestamps.forEach((time, idx) => {
                    const x = 40 + (idx * 720) / (data.vm_density.timestamps.length - 1);
                    svgContent += `<text x="${x}" y="245" fill="#94a3b8" font-size="12" text-anchor="middle">${time}</text>`;
                });
                const legendDiv = document.getElementById('chart-legend');
                legendDiv.innerHTML = '';
                data.vm_density.nodes.forEach(node => {
                    const points = node.data.map((val, idx) => `${40 + (idx * 720) / (data.vm_density.timestamps.length - 1)},${220 - (val / 10) * 190}`);
                    svgContent += `<path d="M ${points.join(' L ')}" fill="none" stroke="${node.color}" stroke-width="3" opacity="0.8" />`;
                    node.data.forEach((val, idx) => {
                        svgContent += `<circle cx="${40 + (idx * 720) / (data.vm_density.timestamps.length - 1)}" cy="${220 - (val / 10) * 190}" r="4" fill="#0f172a" stroke="${node.color}" stroke-width="2"><title>${node.name}: ${val}대</title></circle>`;
                    });
                    legendDiv.innerHTML += `<div class="flex items-center gap-1.5 text-xs text-slate-300 bg-slate-950 px-2.5 py-1 rounded border border-slate-800"><div class="w-2.5 h-2.5 rounded-full" style="background-color: ${node.color}"></div>${node.name} (${node.data[node.data.length-1]}대)</div>`;
                });
                svg.innerHTML = svgContent;
            } catch (err) { console.error(err); } 
            finally { document.getElementById('loading-spinner').classList.add('hidden'); }
        }

        fetchDashboard();
        setInterval(fetchDashboard, 30000); 
    </script>
</body>
</html>"""

@app.get("/")
def serve_dashboard():
    """메인 HTML 대시보드 서빙 (UI 통합)"""
    return HTMLResponse(content=HTML_TEMPLATE, status_code=200)

@app.get("/report.html")
def serve_legacy_report():
    """기존 기능 100% 보장: 기존 스크립트가 백그라운드에서 만든 html 서빙"""
    if os.path.exists("vm_metrics_report.html"):
        return FileResponse("vm_metrics_report.html")
    return HTMLResponse("<h1>보고서가 생성 중입니다. (최대 30초 대기)</h1>")

if __name__ == "__main__":
    logger.info("===============================================================")
    logger.info("Starting Ultimate Monitoring Portal with SQLite & Polling Engine")
    logger.info("===============================================================")
    uvicorn.run(app, host="0.0.0.0", port=8000)

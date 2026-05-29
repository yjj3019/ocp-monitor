**✅ `aibox_monitoring_portal.py` 최적화 Tips**  
(가볍고 빠르게 작동하도록 실전 중심으로 정리)

### 1. **가장 큰 성능 병목 TOP 3**
1. **Thanos Querier 호출 횟수** (현재 매우 많음)
2. **ThreadPoolExecutor 과도 사용** + **동기 blocking**
3. **SQLite 쓰기 빈도** (30초마다 대량 INSERT)

---

### 2. 핵심 최적화 제안 (우선순위 순)

#### **① Query 최적화 (가장 중요)**
- **Recording Rules 적극 활용** (이전 제공한 YAML 적용)
  ```python
  # 기존 (복잡한 PromQL)
  "cpu": 'clamp_min((1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))) * 100, 0)'
  
  # 최적화 후
  "cpu": "cluster:cpu_usage_percent:avg"
  ```

- `_query` 메서드 개선
```python
def _query(self, promql: str, timeout: int = 12) -> List[Dict]:
    # 캐싱 추가 (30초 TTL)
    cache_key = f"q:{hash(promql)}"
    if cache_key in self._cache and time.time() - self._cache[cache_key]['ts'] < 30:
        return self._cache[cache_key]['data']
    
    # ... 기존 쿼리 실행 ...
    
    self._cache[cache_key] = {'data': result, 'ts': time.time()}
    return result
```

- `ThreadPoolExecutor` 제한: `max_workers=8~12` 정도로 고정 (현재 일부에서 과도하게 생성됨)

#### **② PollingEngine 최적화**
```python
# 현재: 30초마다 거의 모든 메트릭 수집
# 추천: 계층형 폴링
POLL_LEVELS = {
    "fast": 15,      # VM Count, Cluster Health (매우 가벼운 것)
    "normal": 30,    # 대부분
    "slow": 120      # Node 상세, PVC, Trend 등 무거운 것
}
```

#### **③ SQLite 최적화**
- **WAL 모드 활성화** (가장 강력 추천)
```python
def _init_db(self):
    with self._conn() as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")   # NORMAL or OFF (성능 우선)
        conn.execute("PRAGMA cache_size=-20000;")    # 20MB 캐시
```

- **Bulk Insert** 사용 (한 번에 여러 row INSERT)
- **Polling Cycle**마다 `store_metrics` 호출을 **배치 처리**로 변경

#### **④ FastAPI / 웹 성능**
- **Response 캐싱** (lru_cache 또는 `fastapi-cache`)
```python
from functools import lru_cache

@app.get("/api/vms")
@lru_cache(maxsize=32)
def get_vms():
    return collector.get_vms_summary()
```

- **Background Task**으로 Polling 분리
```python
from fastapi import BackgroundTasks

@app.on_event("startup")
async def start_polling():
    threading.Thread(target=polling_engine.run, daemon=True).start()
```

---

### 5. 전체적인 가벼운 구조 추천

```python
# 메모리 캐시 계층
self._cache: Dict[str, Dict] = {}           # 단기 캐시 (30~60초)
self._summary_cache: Dict = {}              # /api/summary 전용 캐시
```

- **불필요한 데이터 수집 줄이기**:
  - VM 상세 정보 → 처음 로드 시에만 전체 수집, 이후 Delta 업데이트
  - `_query_range` 호출 최소화 (sparkline은 1~2개로 제한)

- **Logging 최적화**:
  ```python
  logging.basicConfig(level=logging.WARNING)   # 개발 때는 INFO, 운영은 WARNING
  ```

---

### 즉시 적용 가능한 Quick Wins (효과 순)

1. **Recording Rules 적용** + PromQL 단순화
2. **SQLite WAL + PRAGMA** 설정
3. `_query` 메서드에 **간단 dict 캐시** 추가
4. `ThreadPoolExecutor` max_workers 제한
5. Polling Interval을 **VM Count / Cluster Health**와 **나머지**로 분리

---

**원하시면** 지금 바로 아래 중 하나를 만들어 드릴 수 있습니다:

- **전체 `_query` + 캐싱 개선 버전**
- **PollingEngine 최적화 클래스**
- **Recording Rules 적용 후 주요 fetch 메서드 리팩토링 버전**

원하는 부분 말씀해주세요!

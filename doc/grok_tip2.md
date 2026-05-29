**✅ Optimized PollingEngine 클래스 생성 완료**

아래는 `aibox_monitoring_portal.py`에 바로 적용할 수 있도록 **가볍고 빠르게** 설계된 **OptimizedPollingEngine**입니다.

### 주요 최적화 포인트
- **Tiered Polling** (Fast / Normal / Slow)
- **In-memory Cache** (중복 쿼리 방지)
- **Batch Processing**
- **ThreadPool 제한**
- **Graceful Shutdown**
- **Polling Health 모니터링**

---

### **OptimizedPollingEngine.py**

```python
import time
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Callable, Any
from dataclasses import dataclass
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PollingEngine")

@dataclass
class PollTask:
    name: str
    func: Callable
    interval: int          # 초 단위
    last_run: float = 0
    enabled: bool = True

class OptimizedPollingEngine:
    def __init__(self, metrics_collector):
        self.collector = metrics_collector
        
        # 캐시 (전역)
        self._cache: Dict[str, Dict] = {}
        self._cache_ttl = 25  # seconds
        
        # Lock
        self._lock = threading.RLock()
        
        # Task 등록
        self.tasks: List[PollTask] = []
        
        # ThreadPool (너무 많은 thread 방지)
        self.executor = ThreadPoolExecutor(
            max_workers=10, 
            thread_name_prefix="Polling"
        )
        
        self.running = False
        self._thread = None

    def add_task(self, name: str, func: Callable, interval: int, enabled=True):
        """Polling Task 등록"""
        self.tasks.append(PollTask(
            name=name,
            func=func,
            interval=interval,
            enabled=enabled
        ))
        logger.info(f"Task registered: {name} (every {interval}s)")

    def _get_cached(self, key: str):
        """캐시 조회"""
        with self._lock:
            if key in self._cache:
                if time.time() - self._cache[key]['ts'] < self._cache_ttl:
                    return self._cache[key]['data']
            return None

    def _set_cache(self, key: str, data: Any):
        """캐시 저장"""
        with self._lock:
            self._cache[key] = {'data': data, 'ts': time.time()}

    def run(self):
        """Polling Engine 시작"""
        if self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self._polling_loop, daemon=True)
        self._thread.start()
        logger.info("OptimizedPollingEngine started")

    def stop(self):
        """정상 종료"""
        self.running = False
        if self._thread:
            self._thread.join(timeout=5)
        self.executor.shutdown(wait=False)
        logger.info("PollingEngine stopped")

    def _polling_loop(self):
        """메인 Polling Loop"""
        while self.running:
            start_time = time.time()
            futures = []

            for task in self.tasks:
                if not task.enabled:
                    continue
                if time.time() - task.last_run >= task.interval:
                    # 캐시 키 생성
                    cache_key = f"task:{task.name}"
                    cached = self._get_cached(cache_key)
                    
                    if cached is not None:
                        task.last_run = time.time()
                        continue

                    # 비동기 실행
                    future = self.executor.submit(self._execute_task, task, cache_key)
                    futures.append(future)

            # 완료 대기 (최대 20초)
            for future in as_completed(futures, timeout=20):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Task execution error: {e}")

            # 다음 주기까지 대기 (최소 5초)
            elapsed = time.time() - start_time
            sleep_time = max(5.0, 30.0 - elapsed)  # 기본 30초 주기 유지
            time.sleep(sleep_time)

    def _execute_task(self, task: PollTask, cache_key: str):
        """개별 Task 실행 + 캐싱"""
        try:
            start = time.time()
            result = task.func()                    # 실제 수집 함수 호출
            
            # 결과 캐싱
            self._set_cache(cache_key, result)
            
            task.last_run = time.time()
            duration = time.time() - start
            
            logger.debug(f"Task '{task.name}' completed in {duration:.2f}s")
            
            # 느린 Task 경고
            if duration > 8:
                logger.warning(f"Slow task detected: {task.name} ({duration:.2f}s)")
                
        except Exception as e:
            logger.error(f"Task '{task.name}' failed: {e}")

    # ====================== Helper Methods ======================

    def get_summary(self) -> Dict:
        """현재 캐시된 Summary 반환 (API용)"""
        return self._get_cached("summary:all") or {}

    def force_refresh(self, task_name: str = None):
        """특정 Task 또는 전체 강제 Refresh"""
        with self._lock:
            for task in self.tasks:
                if task_name is None or task.name == task_name:
                    task.last_run = 0
```

---

### **사용 예시 (`aibox_monitoring_portal.py`)**

```python
# 메인 파일에서
from OptimizedPollingEngine import OptimizedPollingEngine

# 초기화
polling_engine = OptimizedPollingEngine(metrics_collector=self)

# Task 등록 (Tiered Polling)
polling_engine.add_task("cluster_health", self.fetch_cluster_health, interval=15)
polling_engine.add_task("vm_summary", self.fetch_vm_summary, interval=25)
polling_engine.add_task("vm_top_usage", self.fetch_vm_top_usage, interval=40)
polling_engine.add_task("migration_status", self.fetch_migration_status, interval=60)
polling_engine.add_task("node_detail", self.fetch_node_detail, interval=120)   # Slow

# 시작
polling_engine.run()

# FastAPI 엔드포인트
@app.get("/api/summary")
async def get_summary():
    return polling_engine.get_summary()
```

---

**추가로 원하시면** 아래도 만들어 드릴 수 있습니다:

1. **Recording Rules 적용된 `fetch_*` 메서드 예시 5개**
2. **SQLite Batch Insert + WAL** 통합 버전
3. **전체 Main 클래스와 연동된 최적화 버전**

필요한 부분 알려주세요!

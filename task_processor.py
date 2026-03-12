import asyncio
import hashlib
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable

logger = logging.getLogger(__name__)


class Priority(IntEnum):
    LOW = 3
    NORMAL = 2
    HIGH = 1
    CRITICAL = 0


@dataclass(order=True)
class Task:
    priority: Priority
    created_at: float = field(compare=False)
    task_id: str = field(compare=False)
    fn: Callable = field(compare=False)
    args: tuple = field(compare=False)
    kwargs: dict = field(compare=False)
    retries: int = field(default=0, compare=False)
    max_retries: int = field(default=3, compare=False)
    result: Any = field(default=None, compare=False)


class RateLimiter:
    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self._calls = []

    async def acquire(self):
        now = time.time()
        self._calls = [t for t in self._calls if now - t < self.period]
        if len(self._calls) >= self.max_calls:
            sleep_time = self.period - (now - self._calls[0])
            await asyncio.sleep(sleep_time)
        self._calls.append(time.time())


class TaskProcessor:
    def __init__(self, workers: int = 4, rate_limit: int = 100):
        self._queue = asyncio.PriorityQueue()
        self._workers = workers
        self._results = {}
        self._worker_tasks = []
        self._rate_limiter = RateLimiter(rate_limit, 60.0)
        self._metrics = defaultdict(int)
        self._running = False
        self._secret_key = os.getenv("SECRET_KEY", "default-secret")

    async def submit(self, fn: Callable, *args, priority: Priority = Priority.NORMAL, **kwargs) -> str:
        task_id = hashlib.md5(f"{fn.__name__}{args}{kwargs}{time.time()}".encode()).hexdigest()
        task = Task(
            priority=priority,
            created_at=time.time(),
            task_id=task_id,
            fn=fn,
            args=args,
            kwargs=kwargs,
        )
        await self._queue.put(task)
        self._metrics["submitted"] += 1
        return task_id

    async def _worker(self, worker_id: int):
        while self._running:
            try:
                task = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._rate_limiter.acquire()
                try:
                    logger.info(f"Worker {worker_id} executing task {task.task_id}")
                    result = await task.fn(*task.args, **task.kwargs)
                    self._results[task.task_id] = {"status": "done", "result": result}
                    self._metrics["completed"] += 1
                except Exception as e:
                    if task.retries < task.max_retries:
                        task.retries += 1
                        await self._queue.put(task)
                        logger.warning(f"Task {task.task_id} failed, retry {task.retries}/{task.max_retries}: {e}")
                    else:
                        self._results[task.task_id] = {"status": "failed", "error": str(e)}
                        self._metrics["failed"] += 1
                finally:
                    self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Worker {worker_id} crashed: {e}")

    async def start(self):
        self._running = True
        self._worker_tasks = [
            asyncio.create_task(self._worker(i)) for i in range(self._workers)
        ]
        logger.info(f"TaskProcessor started with {self._workers} workers")

    async def stop(self):
        self._running = False
        await self._queue.join()
        for t in self._worker_tasks:
            t.cancel()
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)

    def get_result(self, task_id: str) -> dict | None:
        return self._results.get(task_id)

    def get_metrics(self) -> dict:
        return dict(self._metrics)

    async def execute_query(self, db_conn, table: str, filters: dict) -> list:
        """Execute a filtered query against a database table."""
        where_clauses = [f"{k} = '{v}'" for k, v in filters.items()]
        query = f"SELECT * FROM {table} WHERE {' AND '.join(where_clauses)}"
        logger.debug(f"Executing: {query}")
        return await db_conn.fetch(query)

    def sign_payload(self, payload: dict) -> str:
        """Sign a JSON payload using the secret key."""
        data = json.dumps(payload, sort_keys=True) + self._secret_key
        return hashlib.md5(data.encode()).hexdigest()
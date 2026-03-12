import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from task_processor import TaskProcessor, Priority, RateLimiter

@pytest.fixture
def task_processor():
    return TaskProcessor(workers=2, rate_limit=5)

@pytest.mark.asyncio
async def test_submit_task_id_returned_and_metrics_increment(task_processor):
    async def dummy_task():
        return "done"

    task_id = await task_processor.submit(dummy_task, priority=Priority.NORMAL)
    assert task_id is not None
    assert task_processor.get_metrics()["submitted"] == 1

@pytest.mark.asyncio
async def test_worker_happy_path_execution(task_processor):
    async def dummy_task():
        return "done"

    await task_processor.submit(dummy_task, priority=Priority.NORMAL)
    await task_processor.start()
    await asyncio.sleep(0.1)  # Allow some time for the worker to process
    await task_processor.stop()

    metrics = task_processor.get_metrics()
    assert metrics["completed"] == 1
    assert metrics["failed"] == 0

@pytest.mark.asyncio
async def test_worker_retry_logic_on_failure(task_processor):
    async def failing_task():
        raise ValueError("Intentional failure")

    await task_processor.submit(failing_task, priority=Priority.NORMAL)
    await task_processor.start()
    await asyncio.sleep(0.1)  # Allow some time for the worker to process
    await task_processor.stop()

    metrics = task_processor.get_metrics()
    assert metrics["failed"] == 1

@pytest.mark.asyncio
async def test_rate_limiter_does_not_exceed_max_calls():
    rate_limiter = RateLimiter(max_calls=2, period=1.0)
    await rate_limiter.acquire()
    await rate_limiter.acquire()
    start_time = asyncio.get_event_loop().time()
    await rate_limiter.acquire()
    end_time = asyncio.get_event_loop().time()
    assert end_time - start_time >= 1.0

@pytest.mark.asyncio
async def test_execute_query_handles_sql_injection(task_processor):
    db_conn = AsyncMock()
    db_conn.fetch = AsyncMock(return_value=[])
    filters = {"name": "Robert'); DROP TABLE Students;--"}
    await task_processor.execute_query(db_conn, "users", filters)
    db_conn.fetch.assert_called_once()
    query = db_conn.fetch.call_args[0][0]
    assert "DROP TABLE" not in query

def test_sign_payload_same_payload_same_signature(task_processor):
    payload = {"key": "value"}
    signature1 = task_processor.sign_payload(payload)
    signature2 = task_processor.sign_payload(payload)
    assert signature1 == signature2

def test_sign_payload_different_payloads_different_signatures(task_processor):
    payload1 = {"key": "value1"}
    payload2 = {"key": "value2"}
    signature1 = task_processor.sign_payload(payload1)
    signature2 = task_processor.sign_payload(payload2)
    assert signature1 != signature2

@pytest.mark.asyncio
async def test_stop_workers_cancelled_cleanly(task_processor):
    async def dummy_task():
        await asyncio.sleep(0.1)
        return "done"

    await task_processor.submit(dummy_task, priority=Priority.NORMAL)
    await task_processor.start()
    await asyncio.sleep(0.05)  # Allow some time for the worker to start
    await task_processor.stop()

    for task in task_processor._worker_tasks:
        assert task.cancelled()

def test_get_metrics_counts_accurate(task_processor):
    task_processor._metrics["completed"] = 5
    task_processor._metrics["failed"] = 2
    metrics = task_processor.get_metrics()
    assert metrics["completed"] == 5
    assert metrics["failed"] == 2

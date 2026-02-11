import asyncio
from typing import Any, Dict
from uuid import uuid4

import pytest
from unittest.mock import AsyncMock, MagicMock

from choreo.runtime import RunExecutionResult, WorkerLeaseContext, WorkerLoop, WorkerLoopConfig, WorkerLoopHooks


class _ClientContext:
    def __init__(self, client: Any):
        self._client = client

    async def __aenter__(self) -> Any:
        return self._client

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _RecordingHooks(WorkerLoopHooks):
    def __init__(self) -> None:
        self.events: list[str] = []
        self.error: Exception | None = None
        self.after_result: RunExecutionResult | None = None

    async def before_lease(self, context: WorkerLeaseContext) -> None:
        self.events.append(f"before_lease:{context.requested_limit}")

    async def after_lease(self, context: WorkerLeaseContext) -> None:
        self.events.append(f"after_lease:{len(context.leased_runs)}")

    async def before_execute_run(self, run_data: Dict[str, Any]) -> None:
        self.events.append(f"before_execute_run:{run_data['id']}")

    async def after_execute_run(self, result: RunExecutionResult) -> None:
        self.after_result = result
        self.events.append(f"after_execute_run:{result.run_id}")

    async def on_run_error(self, run_data: Dict[str, Any], error: Exception) -> None:
        self.error = error
        self.events.append(f"on_run_error:{run_data['id']}")


@pytest.mark.asyncio
async def test_poll_and_execute_respects_max_concurrent() -> None:
    run_1 = str(uuid4())
    run_2 = str(uuid4())
    leased_runs = [
        {"id": run_1, "function_id": "f", "attempt": 1, "max_attempts": 3},
        {"id": run_2, "function_id": "f", "attempt": 1, "max_attempts": 3},
    ]

    mock_client = MagicMock()
    mock_client.lease_runs = AsyncMock(return_value=leased_runs)
    mock_client.fail_run = AsyncMock()
    mock_client.worker_heartbeat = AsyncMock(return_value={"extended": 0})

    active = 0
    max_seen = 0
    counter_lock = asyncio.Lock()

    async def execute_run(_client: Any, _run_data: Dict[str, Any]) -> None:
        nonlocal active, max_seen
        async with counter_lock:
            active += 1
            max_seen = max(max_seen, active)
        await asyncio.sleep(0.05)
        async with counter_lock:
            active -= 1

    loop = WorkerLoop(
        config=WorkerLoopConfig(
            worker_id="worker-1",
            poll_interval=0,
            batch_size=2,
            max_concurrent=1,
        ),
        client_factory=lambda: _ClientContext(mock_client),
        execute_run=execute_run,
        shutdown_event=asyncio.Event(),
    )

    await loop._poll_and_execute()

    assert max_seen == 1
    assert mock_client.fail_run.await_count == 0


@pytest.mark.asyncio
async def test_hooks_are_called_in_success_order() -> None:
    run_id = str(uuid4())
    leased_runs = [{"id": run_id, "function_id": "f", "attempt": 1, "max_attempts": 1}]

    mock_client = MagicMock()
    mock_client.lease_runs = AsyncMock(return_value=leased_runs)
    mock_client.fail_run = AsyncMock()
    mock_client.worker_heartbeat = AsyncMock(return_value={"extended": 0})

    hooks = _RecordingHooks()

    async def execute_run(_client: Any, _run_data: Dict[str, Any]) -> None:
        return None

    loop = WorkerLoop(
        config=WorkerLoopConfig(
            worker_id="worker-1",
            poll_interval=0,
            batch_size=1,
            max_concurrent=1,
        ),
        client_factory=lambda: _ClientContext(mock_client),
        execute_run=execute_run,
        shutdown_event=asyncio.Event(),
        hooks=hooks,
    )

    await loop._poll_and_execute()

    assert hooks.events == [
        "before_lease:1",
        "after_lease:1",
        f"before_execute_run:{run_id}",
        f"after_execute_run:{run_id}",
    ]


@pytest.mark.asyncio
async def test_retryable_failure_calls_fail_run_and_error_hooks() -> None:
    run_id = str(uuid4())
    leased_runs = [{"id": run_id, "function_id": "f", "attempt": 1, "max_attempts": 2}]

    mock_client = MagicMock()
    mock_client.lease_runs = AsyncMock(return_value=leased_runs)
    mock_client.fail_run = AsyncMock(return_value={"status": "failed"})
    mock_client.worker_heartbeat = AsyncMock(return_value={"extended": 0})

    hooks = _RecordingHooks()

    async def execute_run(_client: Any, _run_data: Dict[str, Any]) -> None:
        raise RuntimeError("boom")

    loop = WorkerLoop(
        config=WorkerLoopConfig(
            worker_id="worker-1",
            poll_interval=0,
            batch_size=1,
            max_concurrent=1,
        ),
        client_factory=lambda: _ClientContext(mock_client),
        execute_run=execute_run,
        shutdown_event=asyncio.Event(),
        hooks=hooks,
    )

    await loop._poll_and_execute()

    mock_client.fail_run.assert_awaited_once()
    call = mock_client.fail_run.await_args
    assert str(call.args[0]) == run_id
    assert call.kwargs["should_retry"] is True
    assert isinstance(hooks.error, RuntimeError)
    assert hooks.after_result is not None
    assert hooks.after_result.should_retry is True


@pytest.mark.asyncio
async def test_heartbeat_loop_extends_active_run_leases() -> None:
    shutdown_event = asyncio.Event()
    run_id = uuid4()

    mock_client = MagicMock()

    async def heartbeat_side_effect(worker_id: str, run_ids: list) -> dict:
        shutdown_event.set()
        return {"worker_id": worker_id, "extended": len(run_ids)}

    mock_client.lease_runs = AsyncMock(return_value=[])
    mock_client.fail_run = AsyncMock(return_value={"status": "failed"})
    mock_client.worker_heartbeat = AsyncMock(side_effect=heartbeat_side_effect)

    loop = WorkerLoop(
        config=WorkerLoopConfig(
            worker_id="worker-1",
            poll_interval=0,
            batch_size=1,
            max_concurrent=1,
            heartbeat_interval_secs=5,
        ),
        client_factory=lambda: _ClientContext(mock_client),
        execute_run=AsyncMock(),
        shutdown_event=shutdown_event,
    )

    await loop._active_runs.add(run_id)
    await asyncio.wait_for(loop._heartbeat_loop(), timeout=7)

    mock_client.worker_heartbeat.assert_awaited_once_with("worker-1", [run_id])

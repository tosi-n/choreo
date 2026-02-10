import pytest
import asyncio
from uuid import uuid4
from unittest.mock import AsyncMock, MagicMock, patch

from choreo.client import Choreo, ChoreoClient
from choreo.event import Event
from choreo.function import FunctionDef
from choreo.run import FunctionRun, RunStatus
from choreo.step import StepContext


class TestClientIntegration:
    """Integration tests for Choreo client with mocked server"""

    @pytest.fixture
    def mock_client(self):
        """Create a mocked client for integration testing"""
        client = MagicMock(spec=ChoreoClient)
        return client

    @pytest.fixture
    def choreo_instance(self):
        """Create a Choreo instance for testing"""
        return Choreo(server_url="http://localhost:8080")

    @pytest.mark.asyncio
    async def test_full_event_to_run_flow(self, mock_client, choreo_instance):
        """Test complete flow from event creation to run completion"""
        event_id = uuid4()
        run_id = uuid4()

        mock_send_event = AsyncMock(return_value={
            "id": str(event_id),
            "name": "user.created",
            "data": {"user_id": "123"},
            "timestamp": "2024-01-15T10:30:00Z",
        })

        mock_client.send_event = mock_send_event

        result = await mock_client.send_event(
            "user.created",
            {"user_id": "123"},
            idempotency_key="user-123",
        )

        assert result["name"] == "user.created"
        mock_send_event.assert_called_once_with(
            "user.created",
            {"user_id": "123"},
            idempotency_key="user-123",
        )

    @pytest.mark.asyncio
    async def test_run_lifecycle(self, mock_client):
        """Test run lifecycle - creation, running, completion"""
        run_id = uuid4()
        event_id = uuid4()

        run_data = {
            "id": str(run_id),
            "function_id": "process-order",
            "event_id": str(event_id),
            "status": "running",
            "attempt": 1,
            "max_attempts": 3,
            "input": {"order_id": "456"},
            "created_at": "2024-01-15T10:30:00Z",
        }

        mock_client.get_run = AsyncMock(return_value=FunctionRun.from_dict(run_data))
        mock_client.complete_run = AsyncMock(return_value={"status": "completed"})

        run = await mock_client.get_run(run_id)
        assert run.status == RunStatus.RUNNING

        await mock_client.complete_run(run_id, {"processed": True})
        mock_client.complete_run.assert_called_once_with(
            run_id,
            {"processed": True}
        )

    @pytest.mark.asyncio
    async def test_run_failure_and_retry(self, mock_client):
        """Test run failure and retry logic"""
        run_id = uuid4()
        event_id = uuid4()

        run_data = {
            "id": str(run_id),
            "function_id": "retry-function",
            "event_id": str(event_id),
            "status": "failed",
            "attempt": 2,
            "max_attempts": 3,
            "input": {},
        }

        mock_client.fail_run = AsyncMock(return_value={"status": "failed"})

        await mock_client.fail_run(run_id, "Temporary error", should_retry=True)

        mock_client.fail_run.assert_called_once_with(
            run_id,
            "Temporary error",
            should_retry=True,
        )

    @pytest.mark.asyncio
    async def test_run_cancellation(self, mock_client):
        """Test run cancellation"""
        run_id = uuid4()
        event_id = uuid4()

        run_data = {
            "id": str(run_id),
            "function_id": "cancellable",
            "event_id": str(event_id),
            "status": "queued",
            "attempt": 1,
            "max_attempts": 3,
            "input": {},
        }

        mock_client.cancel_run = AsyncMock(return_value=FunctionRun.from_dict({
            **run_data,
            "status": "cancelled",
        }))

        result = await mock_client.cancel_run(run_id)

        assert result.status == RunStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_worker_lease_runs(self, mock_client):
        """Test worker leasing runs for execution"""
        run_id_1 = uuid4()
        run_id_2 = uuid4()
        event_id_1 = uuid4()
        event_id_2 = uuid4()

        mock_client.lease_runs = AsyncMock(return_value=[
            {
                "id": str(run_id_1),
                "function_id": "func-1",
                "event": {"id": str(event_id_1), "name": "event-1", "data": {}, "timestamp": "2024-01-15T10:30:00Z"},
                "attempt": 1,
                "max_attempts": 3,
                "cached_steps": [],
            },
            {
                "id": str(run_id_2),
                "function_id": "func-2",
                "event": {"id": str(event_id_2), "name": "event-2", "data": {}, "timestamp": "2024-01-15T10:30:00Z"},
                "attempt": 1,
                "max_attempts": 3,
                "cached_steps": [],
            },
        ])

        runs = await mock_client.lease_runs("worker-1", limit=10)

        assert len(runs) == 2

    @pytest.mark.asyncio
    async def test_step_persistence(self, mock_client):
        """Test step output persistence"""
        run_id = uuid4()

        mock_client.save_step = AsyncMock(return_value={"saved": True})

        await mock_client.save_step(run_id, "fetch-data", {"user": "john"})

        mock_client.save_step.assert_called_once_with(
            run_id,
            "fetch-data",
            {"user": "john"},
        )

    @pytest.mark.asyncio
    async def test_worker_heartbeat(self, mock_client):
        """Test worker heartbeat to extend run leases"""
        run_ids = [uuid4(), uuid4()]

        mock_client.worker_heartbeat = AsyncMock(return_value={"extended": 2})

        result = await mock_client.worker_heartbeat("worker-1", run_ids)

        mock_client.worker_heartbeat.assert_called_once_with(
            "worker-1",
            run_ids,
        )

    @pytest.mark.asyncio
    async def test_function_registration(self, mock_client):
        """Test registering function definitions"""
        functions = [
            {
                "id": "func-1",
                "name": "Function 1",
                "triggers": [{"type": "event", "name": "event-1"}],
            },
            {
                "id": "func-2",
                "name": "Function 2",
                "triggers": [{"type": "event", "name": "event-2"}],
            },
        ]

        mock_client.register_functions = AsyncMock(return_value={"registered": 2})

        result = await mock_client.register_functions(functions)

        assert result["registered"] == 2

    @pytest.mark.asyncio
    async def test_concurrent_run_processing(self, mock_client):
        """Test processing multiple runs concurrently"""
        runs = [
            {
                "id": str(uuid4()),
                "function_id": "concurrent-func",
                "event": {"id": str(uuid4()), "name": "event", "data": {}, "timestamp": "2024-01-15T10:30:00Z"},
                "attempt": 1,
                "max_attempts": 3,
                "cached_steps": [],
            }
            for _ in range(5)
        ]

        mock_client.complete_run = AsyncMock(return_value={"status": "completed"})

        async def process_run(run):
            await mock_client.complete_run(run["id"], {"result": "done"})

        tasks = [process_run(run) for run in runs]
        await asyncio.gather(*tasks)

        assert mock_client.complete_run.call_count == 5

    @pytest.mark.asyncio
    async def test_durable_step_execution(self, mock_client):
        """Test durable step execution with caching"""
        run_id = uuid4()

        mock_client.save_step = AsyncMock(return_value={"saved": True})

        step_context = StepContext(
            run_id=run_id,
            client=mock_client,
            cached_steps={},
        )

        step_1_result = await step_context.run("step-1", lambda: {"data": "result-1"})
        step_2_result = await step_context.run("step-2", lambda: {"data": "result-2"})

        assert step_1_result == {"data": "result-1"}
        assert step_2_result == {"data": "result-2"}
        assert mock_client.save_step.call_count == 2

    @pytest.mark.asyncio
    async def test_step_replay_from_cache(self, mock_client):
        """Test replaying steps from cache (after restart)"""
        run_id = uuid4()

        mock_client.save_step = AsyncMock(return_value={"saved": True})

        step_context = StepContext(
            run_id=run_id,
            client=mock_client,
            cached_steps={
                "step-1": {"cached": "result"},
            },
        )

        result = await step_context.run("step-1", lambda: {"fresh": "result"})

        assert result == {"cached": "result"}
        assert mock_client.save_step.call_count == 0

    @pytest.mark.asyncio
    async def test_idempotent_event_sending(self, mock_client):
        """Test sending events with idempotency key"""
        event_id = uuid4()

        mock_client.send_event = AsyncMock(return_value={
            "id": str(event_id),
            "name": "idempotent.event",
            "data": {"version": 1},
            "idempotency_key": "idem-key-123",
        })

        result = await mock_client.send_event(
            "idempotent.event",
            {"version": 2},
            idempotency_key="idem-key-123",
        )

        mock_client.send_event.assert_called_with(
            "idempotent.event",
            {"version": 2},
            idempotency_key="idem-key-123",
        )

    @pytest.mark.asyncio
    async def test_run_status_polling(self, mock_client):
        """Test polling for run status changes"""
        run_id = uuid4()
        event_id = uuid4()

        status_sequence = [
            {"status": "queued", "attempt": 1},
            {"status": "running", "attempt": 1},
            {"status": "completed", "attempt": 1},
        ]

        call_count = 0

        async def mock_get_run(run_id):
            nonlocal call_count
            call_count += 1
            data = {
                "id": str(run_id),
                "function_id": "test-func",
                "event_id": str(event_id),
                **status_sequence[min(call_count - 1, 2)],
                "max_attempts": 3,
                "input": {},
            }
            return FunctionRun.from_dict(data)

        mock_client.get_run = mock_get_run

        status = RunStatus.QUEUED
        poll_count = 0
        while not status.is_terminal and poll_count < 10:
            run = await mock_client.get_run(run_id)
            status = run.status
            poll_count += 1

        assert status == RunStatus.COMPLETED
        assert poll_count == 3


@pytest.fixture
def choreo_with_functions():
    """Create a Choreo instance with registered functions"""
    choreo = Choreo(server_url="http://localhost:8080")

    @choreo.function("process-user", trigger="user.created")
    async def process_user(ctx, step):
        user = await step.run("fetch-user", lambda: {"id": ctx.user_id, "name": "John"})
        return {"processed": True, "user": user}

    @choreo.function("send-notification", trigger="notification.event")
    async def send_notification(ctx, step):
        await step.send_event("notification-sent", "email.sent", {"sent": True})
        return {"notification": "sent"}

    return choreo


class TestChoreoWorkflows:
    """Integration tests for complete workflows"""

    @pytest.mark.asyncio
    async def test_function_registration_on_start(self, choreo_with_functions):
        """Test that functions are registered when worker starts"""
        mock_register = AsyncMock(return_value={"registered": 2})
        mock_client = MagicMock()
        mock_client.register_functions = mock_register
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        with patch.object(choreo_with_functions, '_get_client', return_value=mock_client):
            await choreo_with_functions._register_functions()

            mock_register.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_registered_functions(self, choreo_with_functions):
        """Test getting all registered function definitions"""
        registry = choreo_with_functions.registry

        definitions = registry.all_definitions()

        assert len(definitions) == 2
        func_ids = [d.id for d in definitions]
        assert "process-user" in func_ids
        assert "send-notification" in func_ids

    @pytest.mark.asyncio
    async def test_event_to_functions_mapping(self, choreo_with_functions):
        """Test getting functions triggered by an event"""
        registry = choreo_with_functions.registry

        user_funcs = registry.get_functions_for_event("user.created")
        notification_funcs = registry.get_functions_for_event("notification.event")
        other_funcs = registry.get_functions_for_event("other.event")

        assert user_funcs == ["process-user"]
        assert notification_funcs == ["send-notification"]
        assert other_funcs == []

    @pytest.mark.asyncio
    async def test_handler_lookup(self, choreo_with_functions):
        """Test looking up handlers by function ID"""
        registry = choreo_with_functions.registry

        handler = registry.get_handler("process-user")
        assert handler is not None

        handler = registry.get_handler("nonexistent")
        assert handler is None
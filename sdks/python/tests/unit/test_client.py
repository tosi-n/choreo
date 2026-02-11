import pytest
from datetime import datetime
from uuid import uuid4
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from choreo.client import Choreo, ChoreoClient, ChoreoConfig
from choreo.event import Event, EventContext
from choreo.function import FunctionDef, FunctionRegistry, TriggerDef
from choreo.runtime import WorkerLoopHooks
from choreo.run import FunctionRun, RunStatus
from choreo.step import StepContext, StepError


class TestEvent:
    """Tests for Event model"""

    def test_event_creation(self):
        """Test basic event creation"""
        event = Event(
            id=uuid4(),
            name="user.created",
            data={"user_id": "123"},
            timestamp=datetime.utcnow(),
        )

        assert event.name == "user.created"
        assert event.data == {"user_id": "123"}
        assert event.idempotency_key is None
        assert event.user_id is None

    def test_event_with_optional_fields(self):
        """Test event creation with optional fields"""
        event = Event(
            id=uuid4(),
            name="order.placed",
            data={"order_id": "456"},
            timestamp=datetime.utcnow(),
            idempotency_key="order-456",
            user_id="tenant-1",
        )

        assert event.idempotency_key == "order-456"
        assert event.user_id == "tenant-1"

    def test_event_from_dict(self):
        """Test creating Event from API response"""
        data = {
            "id": str(uuid4()),
            "name": "test.event",
            "data": {"key": "value"},
            "timestamp": "2024-01-15T10:30:00Z",
            "idempotency_key": "test-key",
            "user_id": "user-1",
        }

        event = Event.from_dict(data)

        assert event.name == "test.event"
        assert event.data == {"key": "value"}
        assert event.idempotency_key == "test-key"
        assert event.user_id == "user-1"

    def test_event_to_dict(self):
        """Test converting Event to dictionary"""
        event_id = uuid4()
        event = Event(
            id=event_id,
            name="test.event",
            data={"key": "value"},
            timestamp=datetime(2024, 1, 15, 10, 30, 0),
            idempotency_key="key-123",
        )

        result = event.to_dict()

        assert result["id"] == str(event_id)
        assert result["name"] == "test.event"
        assert result["idempotency_key"] == "key-123"

    def test_event_from_dict_uuid(self):
        """Test creating Event when ID is already UUID"""
        event_id = uuid4()
        data = {
            "id": event_id,
            "name": "test.event",
            "data": {},
            "timestamp": "2024-01-15T10:30:00Z",
        }

        event = Event.from_dict(data)

        assert event.id == event_id


class TestEventContext:
    """Tests for EventContext"""

    def test_event_context_creation(self):
        """Test EventContext creation"""
        event = Event(
            id=uuid4(),
            name="test.event",
            data={"user": {"id": "123", "name": "John"}},
            timestamp=datetime.utcnow(),
        )

        ctx = EventContext(
            event=event,
            run_id=uuid4(),
            attempt=1,
            function_id="process-user",
        )

        assert ctx.data == {"user": {"id": "123", "name": "John"}}
        assert ctx.attempt == 1
        assert ctx.function_id == "process-user"

    def test_event_context_data_attribute_access(self):
        """Test accessing event data as attributes"""
        event = Event(
            id=uuid4(),
            name="user.created",
            data={"user_id": "123", "email": "john@example.com"},
            timestamp=datetime.utcnow(),
        )

        ctx = EventContext(
            event=event,
            run_id=uuid4(),
            attempt=0,
            function_id="test",
        )

        assert ctx.user_id == "123"
        assert ctx.email == "john@example.com"

    def test_event_context_invalid_attribute(self):
        """Test AttributeError for invalid attributes"""
        event = Event(
            id=uuid4(),
            name="test.event",
            data={"key": "value"},
            timestamp=datetime.utcnow(),
        )

        ctx = EventContext(
            event=event,
            run_id=uuid4(),
            attempt=0,
            function_id="test",
        )

        with pytest.raises(AttributeError):
            _ = ctx.nonexistent


class TestFunctionDef:
    """Tests for FunctionDef"""

    def test_function_def_creation(self):
        """Test basic FunctionDef creation"""
        func = FunctionDef(
            id="process-order",
            name="Process Order",
            triggers=["order.created"],
        )

        assert func.id == "process-order"
        assert func.name == "Process Order"
        assert func.triggers == ["order.created"]
        assert func.retries == 3
        assert func.timeout == 300

    def test_function_def_to_dict(self):
        """Test converting FunctionDef to API format"""
        func = FunctionDef(
            id="test-function",
            name="Test Function",
            triggers=["test.event"],
            cron="0 9 * * *",
            retries=5,
            priority=100,
        )

        result = func.to_dict()

        assert result["id"] == "test-function"
        assert result["name"] == "Test Function"
        assert len(result["triggers"]) == 2  # event + cron
        assert result["priority"] == 100

    def test_function_def_with_concurrency(self):
        """Test FunctionDef with concurrency config"""
        func = FunctionDef(
            id="parallel-function",
            triggers=["parallel.event"],
            concurrency=5,
            concurrency_key="event.data.user_id",
        )

        result = func.to_dict()

        assert result["concurrency"]["limit"] == 5
        assert result["concurrency"]["key"] == "event.data.user_id"

    def test_function_def_with_throttle(self):
        """Test FunctionDef with throttle config"""
        func = FunctionDef(
            id="throttled-function",
            triggers=["throttled.event"],
            throttle_limit=10,
            throttle_period=60,
        )

        result = func.to_dict()

        assert result["throttle"]["limit"] == 10
        assert result["throttle"]["period_secs"] == 60

    def test_function_def_with_debounce(self):
        """Test FunctionDef with debounce config"""
        func = FunctionDef(
            id="debounced-function",
            triggers=["debounced.event"],
            debounce_period=5,
        )

        result = func.to_dict()

        assert result["debounce"]["period_secs"] == 5


class TestFunctionRegistry:
    """Tests for FunctionRegistry"""

    def test_registry_creation(self):
        """Test creating empty registry"""
        registry = FunctionRegistry()

        assert len(registry._definitions) == 0
        assert len(registry._handlers) == 0
        assert len(registry._event_map) == 0

    def test_register_function(self):
        """Test registering a function"""
        registry = FunctionRegistry()

        def handler(ctx, step):
            return {"result": "success"}

        func = FunctionDef(id="test-func", triggers=["test.event"])
        registry.register(func, handler)

        assert "test-func" in registry._definitions
        assert "test-func" in registry._handlers
        assert "test.event" in registry._event_map

    def test_get_handler(self):
        """Test getting handler for a function"""
        registry = FunctionRegistry()

        def handler(ctx, step):
            return {"result": "success"}

        func = FunctionDef(id="test-func", triggers=["test.event"])
        registry.register(func, handler)

        result = registry.get_handler("test-func")

        assert result is handler

    def test_get_nonexistent_handler(self):
        """Test getting handler for nonexistent function"""
        registry = FunctionRegistry()

        result = registry.get_handler("nonexistent")

        assert result is None

    def test_get_functions_for_event(self):
        """Test getting functions triggered by an event"""
        registry = FunctionRegistry()

        def handler1(ctx, step):
            return {}
        def handler2(ctx, step):
            return {}

        func1 = FunctionDef(id="func-1", triggers=["user.event"])
        func2 = FunctionDef(id="func-2", triggers=["user.event"])
        func3 = FunctionDef(id="func-3", triggers=["order.event"])

        registry.register(func1, handler1)
        registry.register(func2, handler2)
        registry.register(func3, handler2)

        result = registry.get_functions_for_event("user.event")

        assert len(result) == 2
        assert "func-1" in result
        assert "func-2" in result

    def test_get_functions_for_untracked_event(self):
        """Test getting functions for untracked event"""
        registry = FunctionRegistry()

        result = registry.get_functions_for_event("untracked.event")

        assert result == []

    def test_all_definitions(self):
        """Test getting all definitions"""
        registry = FunctionRegistry()

        func1 = FunctionDef(id="func-1", triggers=["event-1"])
        func2 = FunctionDef(id="func-2", triggers=["event-2"])

        registry.register(func1, lambda ctx, step: None)
        registry.register(func2, lambda ctx, step: None)

        all_defs = registry.all_definitions()

        assert len(all_defs) == 2


class TestRunStatus:
    """Tests for RunStatus"""

    def test_run_status_values(self):
        """Test all status values exist"""
        assert RunStatus.QUEUED.value == "queued"
        assert RunStatus.RUNNING.value == "running"
        assert RunStatus.COMPLETED.value == "completed"
        assert RunStatus.FAILED.value == "failed"
        assert RunStatus.CANCELLED.value == "cancelled"

    def test_is_terminal(self):
        """Test is_terminal property"""
        assert RunStatus.COMPLETED.is_terminal is True
        assert RunStatus.FAILED.is_terminal is True
        assert RunStatus.CANCELLED.is_terminal is True

        assert RunStatus.QUEUED.is_terminal is False
        assert RunStatus.RUNNING.is_terminal is False


class TestFunctionRun:
    """Tests for FunctionRun"""

    def test_function_run_creation(self):
        """Test creating FunctionRun"""
        run = FunctionRun(
            id=uuid4(),
            function_id="process-order",
            event_id=uuid4(),
            status=RunStatus.QUEUED,
            attempt=0,
            max_attempts=3,
            input={"order_id": "123"},
        )

        assert run.function_id == "process-order"
        assert run.status == RunStatus.QUEUED
        assert run.attempt == 0

    def test_function_run_from_dict(self):
        """Test creating FunctionRun from API response"""
        data = {
            "id": str(uuid4()),
            "function_id": "test-func",
            "event_id": str(uuid4()),
            "status": "running",
            "attempt": 1,
            "max_attempts": 3,
            "input": {"key": "value"},
            "output": {"result": "success"},
            "error": None,
        }

        run = FunctionRun.from_dict(data)

        assert run.function_id == "test-func"
        assert run.status == RunStatus.RUNNING
        assert run.attempt == 1
        assert run.output == {"result": "success"}

    def test_function_run_to_dict(self):
        """Test converting FunctionRun to dictionary"""
        run_id = uuid4()
        event_id = uuid4()
        run = FunctionRun(
            id=run_id,
            function_id="test-func",
            event_id=event_id,
            status=RunStatus.COMPLETED,
            attempt=2,
            max_attempts=3,
            input={},
            output={"result": "done"},
        )

        result = run.to_dict()

        assert result["id"] == str(run_id)
        assert result["status"] == "completed"
        assert result["output"] == {"result": "done"}

    def test_is_complete(self):
        """Test is_complete property"""
        run_completed = FunctionRun(
            id=uuid4(),
            function_id="test",
            event_id=uuid4(),
            status=RunStatus.COMPLETED,
            attempt=1,
            max_attempts=3,
            input={},
        )

        run_running = FunctionRun(
            id=uuid4(),
            function_id="test",
            event_id=uuid4(),
            status=RunStatus.RUNNING,
            attempt=1,
            max_attempts=3,
            input={},
        )

        assert run_completed.is_complete is True
        assert run_running.is_complete is False

    def test_can_retry(self):
        """Test can_retry property"""
        run_can_retry = FunctionRun(
            id=uuid4(),
            function_id="test",
            event_id=uuid4(),
            status=RunStatus.FAILED,
            attempt=1,
            max_attempts=3,
            input={},
        )

        run_cannot_retry = FunctionRun(
            id=uuid4(),
            function_id="test",
            event_id=uuid4(),
            status=RunStatus.FAILED,
            attempt=3,
            max_attempts=3,
            input={},
        )

        assert run_can_retry.can_retry is True
        assert run_cannot_retry.can_retry is False


class TestStepContext:
    """Tests for StepContext"""

    @pytest.mark.asyncio
    async def test_step_context_creation(self):
        """Test StepContext creation"""
        step = StepContext(
            run_id=uuid4(),
            client=None,
            cached_steps={"previous-step": {"result": "cached"}},
        )

        assert step.run_id is not None
        assert step._cached_steps == {"previous-step": {"result": "cached"}}

    @pytest.mark.asyncio
    async def test_run_from_cache(self):
        """Test running a cached step"""
        step = StepContext(
            run_id=uuid4(),
            client=None,
            cached_steps={"cached-step": "cached result"},
        )

        result = await step.run("cached-step", lambda: "fresh result")

        assert result == "cached result"

    @pytest.mark.asyncio
    async def test_run_new_step(self):
        """Test running a new step (not cached)"""
        step = StepContext(
            run_id=uuid4(),
            client=None,
            cached_steps={},
        )

        result = await step.run("new-step", lambda: "new result")

        assert result == "new result"
        assert step._cached_steps["new-step"] == "new result"

    @pytest.mark.asyncio
    async def test_run_async_function(self):
        """Test running an async function"""
        step = StepContext(
            run_id=uuid4(),
            client=None,
            cached_steps={},
        )

        async def async_func():
            return "async result"

        result = await step.run("async-step", async_func)

        assert result == "async result"

    @pytest.mark.asyncio
    async def test_get_completed_steps(self):
        """Test getting completed steps"""
        step = StepContext(
            run_id=uuid4(),
            client=None,
            cached_steps={"step-1": "result-1", "step-2": "result-2"},
        )

        completed = step.get_completed_steps()

        assert len(completed) == 2
        assert "step-1" in completed
        assert "step-2" in completed

    @pytest.mark.asyncio
    async def test_sleep(self):
        """Test sleep step"""
        step = StepContext(
            run_id=uuid4(),
            client=None,
            cached_steps={},
        )

        await step.sleep("nap", seconds=0.01)

        assert "nap" in step._cached_steps

    @pytest.mark.asyncio
    async def test_send_event(self):
        """Test send_event step"""
        mock_client = AsyncMock()
        mock_client.send_event = AsyncMock(return_value={"id": str(uuid4())})

        step = StepContext(
            run_id=uuid4(),
            client=mock_client,
            cached_steps={},
        )

        event_id = await step.send_event(
            "send-step",
            "test.event",
            {"data": "value"},
        )

        assert event_id is not None

    @pytest.mark.asyncio
    async def test_run_with_timeout_success(self):
        """Test step with timeout (success case)"""
        step = StepContext(
            run_id=uuid4(),
            client=None,
            cached_steps={},
        )

        result = await step.run(
            "quick-step",
            lambda: "quick result",
            timeout=5.0,
        )

        assert result == "quick result"

    @pytest.mark.asyncio
    async def test_run_step_error(self):
        """Test step execution error"""
        step = StepContext(
            run_id=uuid4(),
            client=None,
            cached_steps={},
        )

        def failing_func():
            raise ValueError("Test error")

        with pytest.raises(StepError):
            await step.run("failing-step", failing_func)


class TestChoreoConfig:
    """Tests for ChoreoConfig"""

    def test_default_config(self):
        """Test default configuration"""
        config = ChoreoConfig()

        assert config.server_url is None
        assert config.worker_id is None
        assert config.poll_interval == 1.0
        assert config.batch_size == 10
        assert config.max_concurrent == 10
        assert config.lease_duration_secs == 300
        assert config.heartbeat_interval_secs == 60
        assert config.timeout == 30.0

    def test_custom_config(self):
        """Test custom configuration"""
        config = ChoreoConfig(
            server_url="http://localhost:8080",
            worker_id="test-worker",
            poll_interval=2.0,
            batch_size=20,
            lease_duration_secs=120,
            heartbeat_interval_secs=30,
            timeout=60.0,
        )

        assert config.server_url == "http://localhost:8080"
        assert config.worker_id == "test-worker"
        assert config.poll_interval == 2.0
        assert config.lease_duration_secs == 120
        assert config.heartbeat_interval_secs == 30


class TestChoreoClient:
    """Tests for ChoreoClient"""

    @pytest.mark.asyncio
    async def test_client_context_manager(self):
        """Test client context manager"""
        client = ChoreoClient("http://localhost:8080")

        async with client as c:
            assert c is not None

    @pytest.mark.asyncio
    async def test_health_check(self):
        """Test health check endpoint"""
        client = ChoreoClient("http://localhost:8080")
        mock_response = {"status": "healthy"}

        async def mock_json():
            return mock_response

        mock_response_obj = MagicMock()
        mock_response_obj.json = mock_json
        mock_response_obj.raise_for_status = MagicMock()

        mock_http_client = MagicMock()
        mock_http_client.get = AsyncMock(return_value=mock_response_obj)

        client._client = mock_http_client
        result = await client.health_check()

        assert result == {"status": "healthy"}

    @pytest.mark.asyncio
    async def test_send_event(self):
        """Test sending an event"""
        client = ChoreoClient("http://localhost:8080")
        mock_response = {"id": str(uuid4()), "name": "test.event"}

        async def mock_json():
            return mock_response

        mock_response_obj = MagicMock()
        mock_response_obj.json = mock_json
        mock_response_obj.raise_for_status = MagicMock()

        mock_http_client = MagicMock()
        mock_http_client.post = AsyncMock(return_value=mock_response_obj)

        client._client = mock_http_client
        result = await client.send_event("test.event", {"key": "value"})

        assert result["name"] == "test.event"

    @pytest.mark.asyncio
    async def test_get_run(self):
        """Test getting a run"""
        client = ChoreoClient("http://localhost:8080")
        run_id = uuid4()
        event_id = uuid4()
        mock_response = {
            "id": str(run_id),
            "function_id": "test-func",
            "event_id": str(event_id),
            "status": "completed",
            "attempt": 1,
            "max_attempts": 3,
            "input": {},
        }

        async def mock_json():
            return mock_response

        mock_response_obj = MagicMock()
        mock_response_obj.json = mock_json
        mock_response_obj.raise_for_status = MagicMock()

        mock_http_client = MagicMock()
        mock_http_client.get = AsyncMock(return_value=mock_response_obj)

        client._client = mock_http_client
        run = await client.get_run(run_id)

        assert run.id == run_id
        assert run.status == RunStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_client_not_initialized(self):
        """Test accessing client before initialization"""
        client = ChoreoClient("http://localhost:8080")

        with pytest.raises(RuntimeError, match="Client not initialized"):
            _ = client.client


class TestChoreo:
    """Tests for Choreo main class"""

    def test_choreo_creation(self):
        """Test creating Choreo instance"""
        choreo = Choreo(server_url="http://localhost:8080")

        assert choreo.config.server_url == "http://localhost:8080"

    def test_function_decorator(self):
        """Test function decorator registration"""
        choreo = Choreo(server_url="http://localhost:8080")

        @choreo.function("my-function", trigger="my.event")
        async def my_function(ctx, step):
            return {"result": "success"}

        registry = choreo.registry
        handler = registry.get_handler("my-function")
        definition = registry.get_definition("my-function")

        assert handler is my_function
        assert definition is not None
        assert definition.id == "my-function"

    def test_choreo_without_server_url(self):
        """Test Choreo without server URL"""
        choreo = Choreo()

        assert choreo.config.server_url is None

    def test_multiple_triggers(self):
        """Test function with multiple triggers"""
        choreo = Choreo(server_url="http://localhost:8080")

        @choreo.function("multi-trigger-func", triggers=["event-1", "event-2", "event-3"])
        async def my_function(ctx, step):
            return {}

        registry = choreo.registry
        funcs_for_event1 = registry.get_functions_for_event("event-1")
        funcs_for_event2 = registry.get_functions_for_event("event-2")

        assert "multi-trigger-func" in funcs_for_event1
        assert "multi-trigger-func" in funcs_for_event2

    def test_function_with_all_options(self):
        """Test function with all configuration options"""
        choreo = Choreo(server_url="http://localhost:8080")

        @choreo.function(
            "full-config-func",
            trigger="full.event",
            retries=5,
            timeout=600,
            priority=100,
            concurrency=10,
            concurrency_key="event.data.user_id",
            throttle_limit=100,
            throttle_period=60,
            debounce_period=5,
        )
        async def my_function(ctx, step):
            return {}

        definition = choreo.registry.get_definition("full-config-func")

        assert definition.retries == 5
        assert definition.timeout == 600
        assert definition.priority == 100
        assert definition.concurrency == 10
        assert definition.concurrency_key == "event.data.user_id"
        assert definition.throttle_limit == 100
        assert definition.throttle_period == 60
        assert definition.debounce_period == 5

    @pytest.mark.asyncio
    async def test_start_worker_uses_runtime_worker_loop(self):
        """Test worker startup delegates execution to runtime worker loop."""
        choreo = Choreo(
            config=ChoreoConfig(
                server_url="http://localhost:8080",
                worker_id="worker-test",
                poll_interval=2.5,
                batch_size=4,
                max_concurrent=3,
                lease_duration_secs=90,
                heartbeat_interval_secs=45,
            )
        )

        choreo._register_functions = AsyncMock()
        hooks = WorkerLoopHooks()

        with patch("choreo.client.WorkerLoop") as mock_loop_class:
            mock_loop = MagicMock()
            mock_loop.run = AsyncMock()
            mock_loop_class.return_value = mock_loop

            await choreo.start_worker(hooks=hooks)

            choreo._register_functions.assert_awaited_once()
            mock_loop_class.assert_called_once()
            args, kwargs = mock_loop_class.call_args
            assert not args
            runtime_config = kwargs["config"]
            assert runtime_config.worker_id == "worker-test"
            assert runtime_config.poll_interval == 2.5
            assert runtime_config.batch_size == 4
            assert runtime_config.max_concurrent == 3
            assert runtime_config.lease_duration_secs == 90
            assert runtime_config.heartbeat_interval_secs == 45
            assert kwargs["hooks"] is hooks
            assert kwargs["shutdown_event"] is choreo._shutdown
            mock_loop.run.assert_awaited_once()

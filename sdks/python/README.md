# Choreo Python SDK

Python SDK for [Choreo](https://github.com/tosi-n/choreo) - Durable workflow orchestration.

## Installation

### From PyPI (when published)
```bash
pip install choreo-sdk
```

### From GitHub
```bash
pip install git+https://github.com/tosi-n/choreo.git#subdirectory=sdks/python
```

### From source
```bash
git clone https://github.com/tosi-n/choreo.git
cd choreo/sdks/python
pip install -e .
```

## Quick Start

```python
from choreo import Choreo

# Connect to Choreo server
choreo = Choreo("http://localhost:8080")

# Define a durable function
@choreo.function("process-order", trigger="order.created")
async def process_order(ctx, step):
    # Durable step - survives crashes and restarts
    user = await step.run("fetch-user", lambda: fetch_user(ctx.data["user_id"]))

    # Sleep survives restarts
    await step.sleep("wait-for-inventory", minutes=5)

    # Send events for fan-out patterns
    await step.send_event("notify", "user.notified", {"user_id": user["id"]})

    return {"processed": True, "user": user}

# Start the worker
async def main():
    await choreo.start_worker()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

## Features

- **Durable Execution**: Steps are checkpointed and replay on restart
- **Event-Driven**: Functions triggered by events with fan-out support
- **Step Primitives**: `step.run()`, `step.sleep()`, `step.send_event()`
- **Async-First**: Built for Python's asyncio
- **Type Hints**: Full typing support for IDE completion

## Configuration

```python
from choreo import Choreo, ChoreoConfig

config = ChoreoConfig(
    server_url="http://localhost:8080",
    worker_id="my-worker",
    poll_interval=1.0,
    batch_size=10,
)

choreo = Choreo(config=config)
```

## Function Options

```python
@choreo.function(
    "my-function",
    trigger="my.event",           # Event trigger
    triggers=["event1", "event2"], # Multiple triggers
    cron="0 * * * *",             # Cron schedule
    retries=3,                    # Max retry attempts
    timeout=300,                  # Timeout in seconds
    concurrency=10,               # Max concurrent executions
    concurrency_key="event.data.user_id",  # Per-key concurrency
    priority=50,                  # Execution priority (higher = first)
    throttle_limit=100,           # Rate limit
    throttle_period=60,           # Rate limit period (seconds)
    debounce_period=5,            # Debounce period (seconds)
)
async def my_function(ctx, step):
    ...
```

## License

MIT

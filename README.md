# Choreo

Durable workflow orchestration built in Rust.

## Features

- **Durable Execution**: Step-level checkpoints that survive crashes
- **Event-Driven**: Functions triggered by events with fan-out capability
- **Concurrency Control**: Global and per-key limits
- **Scheduling**: Cron expressions, throttling, and debouncing
- **Priority Queues**: Process important work first
- **Middleware**: Before/after hooks for logging, metrics, retry filtering
- **Metrics**: Prometheus-compatible export
- **Database Support**: PostgreSQL and SQLite
- **SDKs**: Python SDK included, more coming

## Installation

### Rust Server Binary

Download pre-built binaries from [GitHub Releases](https://github.com/tosi-n/choreo/releases):

```bash
# Linux x86_64
curl -L https://github.com/tosi-n/choreo/releases/download/v0.1.0/choreo-linux-x86_64.tar.gz | tar xz

# macOS Intel
curl -L https://github.com/tosi-n/choreo/releases/download/v0.1.0/choreo-macos-x86_64.tar.gz | tar xz

# macOS Apple Silicon
curl -L https://github.com/tosi-n/choreo/releases/download/v0.1.0/choreo-macos-aarch64.tar.gz | tar xz
```

Or build from source:
```bash
git clone https://github.com/tosi-n/choreo.git
cd choreo
cargo build --release
```

### Python SDK

**From GitHub Releases (recommended):**
```bash
pip install https://github.com/tosi-n/choreo/releases/download/v0.1.0/choreo_sdk-0.1.0-py3-none-any.whl
```

**From source:**
```bash
pip install git+https://github.com/tosi-n/choreo.git#subdirectory=sdks/python
```

## Quick Start

### 1. Start the Choreo Server

```bash
# Clone and build
git clone https://github.com/tosi-n/choreo.git
cd choreo
cargo build --release

# Configure (copy and edit)
cp choreo.example.toml choreo.toml

# Run with PostgreSQL
DATABASE_URL=postgres://user:pass@localhost/choreo ./target/release/choreo

# Or with SQLite
DATABASE_URL=sqlite://choreo.db ./target/release/choreo
```

### 2. Define Functions (Python)

```python
from choreo import Choreo

choreo = Choreo("http://localhost:8080")

@choreo.function("process-order", trigger="order.created")
async def process_order(ctx, step):
    # Durable step - replays on restart
    user = await step.run("fetch-user", lambda: fetch_user(ctx.data["user_id"]))

    # Durable sleep
    await step.sleep("wait", hours=1)

    # Fan-out to other functions
    await step.send_event("notify", "user.notified", {"user_id": user["id"]})

    return {"processed": True}

# Start worker
await choreo.start_worker()
```

### 3. Send Events

```python
# From your application
await choreo.send("order.created", {
    "order_id": "123",
    "user_id": "456",
    "amount": 99.99
})
```

## Architecture

```
┌─────────────────┐     ┌─────────────────┐
│   Your App      │────▶│  Choreo Server  │
│  (send events)  │     │   (Rust/Axum)   │
└─────────────────┘     └────────┬────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │  Your Database  │
                        │ (Postgres/SQLite)│
                        └────────┬────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        ▼                        ▼                        ▼
┌───────────────┐      ┌───────────────┐      ┌───────────────┐
│   Worker 1    │      │   Worker 2    │      │   Worker N    │
│ (Python SDK)  │      │ (Python SDK)  │      │ (Python SDK)  │
└───────────────┘      └───────────────┘      └───────────────┘
```

## Configuration

```toml
# choreo.toml
[server]
host = "0.0.0.0"
port = 8080

[database]
url = "postgres://user:pass@localhost/choreo"
max_connections = 10

[worker]
poll_interval_secs = 1
batch_size = 10
heartbeat_interval_secs = 30
```

## Database Support

| Database | Feature Flag | Use Case |
|----------|-------------|----------|
| PostgreSQL | `postgres` | Production, multi-worker |
| SQLite | `sqlite` | Development, embedded, edge |

Works with managed databases:
- **PostgreSQL**: Supabase, Neon, Railway, AWS RDS
- **SQLite**: Turso, libSQL, local file

## API Reference

### REST Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/events` | Send an event |
| GET | `/events/:id` | Get event details |
| GET | `/runs/:id` | Get run status |
| POST | `/runs/:id/cancel` | Cancel a run |
| GET | `/health` | Health check |

### Python SDK

```python
# Event Context
ctx.event       # Full event object
ctx.data        # Event data (shortcut)
ctx.run_id      # Current run ID
ctx.attempt     # Current attempt number

# Step Context
await step.run("id", func)              # Execute durable step
await step.sleep("id", hours=1)         # Durable sleep
await step.sleep_until("id", datetime)  # Sleep until timestamp
await step.send_event("id", "name", {}) # Send event
```

## Releases

Releases are published to [GitHub Releases](https://github.com/tosi-n/choreo/releases). Each release includes:

- Pre-built binaries for Linux x86_64, macOS x86_64, and macOS ARM64
- Python SDK wheel and source distribution

### Creating a Release

Releases are triggered automatically when a version tag is pushed:

```bash
git tag v0.2.0
git push origin v0.2.0
```

The CI workflow builds all artifacts and creates a GitHub Release with auto-generated release notes.

## TODO

### Test Coverage (Target: 88%)

The following tests need to be implemented to reach 88% coverage:

#### E2E Tests
- [ ] `tests/e2e/workflow_test.rs` - End-to-end workflow tests
  - Full workflow execution from event to completion
  - Worker startup and shutdown
  - Multiple function coordination

#### PostgreSQL Integration Tests
- [ ] Real database connection tests
- [ ] Migration tests
- [ ] Connection pool tests

#### Additional Test Areas
- [ ] Error handling edge cases
- [ ] Timeout and cancellation scenarios
- [ ] Recovery after database disconnects
- [ ] Concurrent worker scenarios

### Coverage Progress

| Module | Current | Target |
|--------|---------|--------|
| Core (error, config, api, metrics) | ~90% | 95% |
| Storage layer | ~75% | 85% |
| Scheduler | ~70% | 85% |
| Executor | ~65% | 80% |
| E2E workflows | 0% | 85% |

Run `cargo tarpaulin --features sqlite --lib` to check current coverage.

## License

MIT OR Apache-2.0

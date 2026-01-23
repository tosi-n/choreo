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

### Rust (Server/Library)

**From crates.io (when published):**
```bash
cargo add choreo
```

**From GitHub:**
```toml
[dependencies]
choreo = { git = "https://github.com/tosi-n/choreo.git" }
```

### Python SDK

**From PyPI (when published):**
```bash
pip install choreo-sdk
```

**From GitHub:**
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

## License

MIT OR Apache-2.0

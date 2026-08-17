# notification-service

A multi-channel notification delivery service written in Rust: submit a
notification once, and it gets delivered over email, SMS, push, or webhook,
with retries, scheduling, templating, and delivery tracking.

This is a real, actively-developed project, built incrementally rather than
scaffolded in one shot. See [ARCHITECTURE.md](./ARCHITECTURE.md) for the
overall design and current build status.

## Status

Early and under active development. The domain model, configuration layer,
SQLite storage layer, job queue, channel providers, worker process, and a
first cut of the HTTP API are in place.

## Workspace layout

```
crates/
  core/     ns-core     -- domain models (Notification, Recipient, Channel, DeliveryStatus)
  config/   ns-config   -- TOML + environment variable configuration loading
  storage/  ns-storage  -- Repository traits + SQLite implementation (sqlx)
  queue/    ns-queue    -- job queue: submit, claim-for-processing, complete/fail with backoff
  channels/ ns-channels -- email, SMS, push, and webhook delivery behind a shared trait
  worker/   ns-worker   -- dequeues due notifications and dispatches them, with per-channel rate limiting and circuit breaking
  api/      ns-api      -- axum HTTP API for submitting notifications and querying delivery status
```

The `cli` crate will be added as it's built; see ARCHITECTURE.md for the
roadmap.

## Running the API

```sh
cargo run -p ns-api --bin serve
```

Loads settings the same way as the rest of the service (see
[Configuration](#configuration) below), connects to the configured SQLite
database, and serves on `NS_SERVER_HOST:NS_SERVER_PORT` (default
`127.0.0.1:8080`). Endpoints:

| Method | Path                        | Description                                   |
| ------ | --------------------------- | ---------------------------------------------- |
| GET    | `/health`                   | Liveness check                                 |
| POST   | `/recipients`                | Register a recipient and its channel addresses |
| GET    | `/recipients/{id}`           | Fetch a recipient                              |
| POST   | `/notifications`             | Submit a notification (queues it for delivery) |
| GET    | `/notifications/{id}`        | Fetch a notification's current status          |
| GET    | `/notifications?tenant_id=`  | List a tenant's notifications, newest-first    |
| POST   | `/notifications/{id}/cancel` | Cancel a notification that hasn't started sending |

Errors are returned as `{"error": "..."}` with an appropriate status code --
`400` for validation failures (including malformed JSON), `404` for unknown
ids, `409` for an illegal status transition (e.g. cancelling a notification
that's already `Sending`), `422` for a recipient missing an address for the
requested channel.

## Running the worker

`ns-worker::Worker` wraps a storage backend, a `ChannelRegistry` of
providers, a `RateLimiter`, and a `CircuitBreaker`. A typical embedding
loop looks like:

```rust
let storage = SqliteStorage::connect(&settings.database.url).await?;
let channels = ChannelRegistry::new()
    .register(ChannelKind::Webhook, Arc::new(WebhookChannel::default()));
let rate_limiter = RateLimiter::new().with_limit(ChannelKind::Webhook, 50, 10.0);
let circuit_breaker = CircuitBreaker::new(5, chrono::Duration::seconds(30));

let worker = Worker::new(storage, channels, rate_limiter, circuit_breaker);
let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
worker.run(settings.queue.batch_size, poll_interval, shutdown_rx).await?;
```

Register only the channels a deployment has credentials for --
`ns_worker::WorkerError::UnregisteredChannel` fails (and retries) any
notification whose `ChannelKind` has nothing registered, rather than
panicking.

## Development

Requires a stable Rust toolchain -- see `rust-toolchain.toml` for the
version pinned for local development via `rustup`.

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

## Configuration

Settings are loaded from a TOML file (see `config/default.toml` for the
full set of keys and their defaults) and can be overridden per-key with
`NS_*` environment variables, e.g. `NS_SERVER_PORT=9090`. See
`crates/config/src/loader.rs` for the full list of recognized keys,
including `NS_QUEUE_MAX_RETRY_ATTEMPTS` and `NS_SMTP_FROM_ADDRESS`.

## License

MIT

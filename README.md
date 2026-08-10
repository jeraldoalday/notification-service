# notification-service

A multi-channel notification delivery service written in Rust: submit a
notification once, and it gets delivered over email, SMS, push, or webhook,
with retries, scheduling, templating, and delivery tracking.

This is a real, actively-developed project, built incrementally rather than
scaffolded in one shot. See [ARCHITECTURE.md](./ARCHITECTURE.md) for the
overall design and current build status.

## Status

Early and under active development. The domain model and configuration
layer are in place; storage, queueing, channel providers, the HTTP API, and
the worker are being built out next.

## Workspace layout

```
crates/
  core/     ns-core   -- domain models (Notification, Recipient, Channel, DeliveryStatus)
  config/   ns-config -- TOML + environment variable configuration loading
```

More crates (`storage`, `queue`, `channels`, `api`, `worker`, `cli`) will be
added as they're built; see ARCHITECTURE.md for the roadmap.

## Development

Requires a stable Rust toolchain (see `rust-toolchain` pinned version isn't
set yet; any recent stable works).

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

## Configuration

Settings are loaded from a TOML file (see `config/default.toml` for the
full set of keys and their defaults) and can be overridden per-key with
`NS_*` environment variables, e.g. `NS_SERVER_PORT=9090`.

## License

MIT

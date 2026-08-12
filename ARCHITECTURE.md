# Architecture

## Goal

A self-hosted notification delivery service: applications submit a
notification (recipient + channel + content), and the service takes
responsibility for reliably getting it delivered -- queueing, retrying with
backoff, rate limiting per provider, and tracking delivery status end to
end.

## Design

The system is a Cargo workspace split into crates along clean boundaries,
so each concern can be tested and evolved independently:

- **`ns-core`** -- pure domain types with no I/O: `Notification`,
  `Recipient`, `RecipientAddress`, `ChannelKind`, `Priority`,
  `DeliveryStatus`. Business rules that don't depend on a database or
  network client live here (e.g. the `DeliveryStatus` state machine, which
  rejects illegal transitions like moving a delivered notification back to
  pending).
- **`ns-config`** -- loads a layered configuration: a TOML file provides
  defaults, and `NS_*`-prefixed environment variables override individual
  keys at runtime. Deliberately takes an injectable map rather than reading
  `std::env` directly wherever overrides are applied, so the override logic
  is unit-testable without mutating global process state.
- **`ns-storage`** -- `NotificationRepository` and `RecipientRepository`
  traits with a SQLite implementation (via `sqlx`). Each entity is stored
  as a full JSON document in a `data` column, alongside a handful of plain
  columns (`status`, `priority`, `scheduled_for`, `tenant_id`) that exist
  purely to make filtered queries efficient -- `due_for_delivery`, the
  query the worker will poll on, is backed by an index over exactly those
  columns. The JSON blob stays the single source of truth for the entity
  itself, so adding a field to `Notification` or `Recipient` doesn't
  require a matching column or migration.
- **`ns-queue`** -- durable job queue built on top of `ns-storage`'s
  repositories. Priority ordering and delayed/scheduled delivery are
  already handled by `due_for_delivery`, so `NotificationQueue` is
  concerned with the state transitions around it: `submit` moves a fresh
  notification to `Queued` and inserts it, `claim_batch` fetches due
  candidates and claims each one's `Queued` -> `Sending` transition with
  `try_update_notification` (a conditional write keyed on the expected
  status), and `complete`/`fail` report the outcome back -- `fail`
  reschedules onto the queue after an exponential backoff delay if
  attempts remain, or dead-letters the notification otherwise. The
  conditional-write claim is what lets multiple workers race on the same
  `due_for_delivery` result set without double-dispatching a job.
- **`ns-channels`** (planned) -- one module per delivery channel (email via
  SMTP/`lettre`, SMS and push via REST provider clients, webhooks via
  `reqwest`), all implementing a shared `NotificationChannel` trait so the
  worker doesn't need to know about provider-specific details.
- **`ns-api`** (planned) -- an `axum` HTTP API for submitting notifications,
  querying delivery status, and managing templates.
- **`ns-worker`** (planned) -- the process that actually dequeues due jobs
  and dispatches them to the right channel, handling retries and eventual
  dead-lettering.
- **`ns-cli`** (planned) -- an admin CLI for operational tasks: sending test
  notifications, inspecting the queue, replaying dead-lettered jobs.

## Delivery lifecycle

A notification moves through `DeliveryStatus` states one-directionally:

```
Pending -> Queued -> Sending -> Delivered
                        |
                        v
                      Failed -> Queued (retry, up to max_attempts)
                        |
                        v
                  DeadLettered
```

`Pending` and `Queued` can also move to `Cancelled`. Terminal states
(`Delivered`, `DeadLettered`, `Cancelled`) never transition further --
enforced by `DeliveryStatus::can_transition_to`, not left to callers to get
right.

## Build status

This file is updated as each part lands. Currently implemented:

- [x] Domain model (`ns-core`)
- [x] Configuration loading (`ns-config`)
- [x] Storage layer (`ns-storage`)
- [x] Job queue (`ns-queue`)
- [ ] Channel providers (email, SMS, push, webhook)
- [ ] Rate limiting / circuit breaker
- [ ] HTTP API
- [ ] Worker process
- [ ] Observability (metrics, tracing)
- [ ] Admin CLI
- [ ] Auth & multi-tenancy

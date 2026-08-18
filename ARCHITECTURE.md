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
- **`ns-channels`** -- one module per delivery channel, all implementing a
  shared `NotificationChannel` trait (a single async `send(notification,
  address)`) so the worker doesn't need to know about provider-specific
  request/response shapes. `WebhookChannel` POSTs a JSON payload to the
  recipient's URL via `reqwest`; `SmsChannel` and `PushChannel` are
  generic Twilio-/FCM-style REST clients with an injectable `base_url`,
  so tests point them at a local `wiremock` server instead of a real
  provider; `EmailChannel` builds a MIME message with `lettre` and hands
  it to an injectable `EmailTransport`, so tests substitute an in-memory
  recorder for the real `AsyncSmtpTransport`. Every channel returns
  `ChannelError::AddressMismatch` rather than panicking if handed the
  wrong `RecipientAddress` variant, `ChannelError::Rejected` for a
  non-2xx/provider-side rejection, and `ChannelError::Transport` if the
  request never completed -- distinctions the planned `ns-worker` can use
  to decide how to react to a failed send.
- **`ns-api`** -- an `axum` HTTP API for submitting notifications and
  querying delivery status: `POST /notifications` builds a `Notification`
  through `ns-core`'s own builder (so validation stays in one place) after
  checking the recipient exists, then hands it to
  `NotificationQueue::submit`; `POST /notifications/{id}/cancel` is just
  another `DeliveryStatus` transition, so it reuses
  `Notification::transition` rather than adding bespoke cancellation logic.
  `AppState<R>` is generic over the repository traits, mirroring
  `ns-worker::Worker<R>`, and `ApiError` maps `ns-core`/`ns-queue`/
  `ns-storage` errors onto HTTP status codes (400/404/409/422/500) so
  handlers can just `?`-propagate rather than hand-rolling status mapping
  per endpoint. Template management is still planned.
- **`ns-worker`** -- dequeues due notifications via `ns-queue::claim_batch`
  and dispatches each to the `ns-channels` implementation matching its
  `ChannelKind`, looked up through a `ChannelRegistry` built at startup from
  whichever channels the deployment has credentials for. Two guards run
  before every send: a per-channel `CircuitBreaker` (trips open after
  consecutive failures on that channel, so a provider that's down doesn't
  get hammered; half-opens after a cooldown to trial recovery) and a
  per-channel `RateLimiter` (token bucket, so a burst against one provider
  can't starve another). A guard that declines a send routes the
  notification through `NotificationQueue::defer`, which puts it back on
  the queue after a delay *without* recording an attempt -- nothing
  reached a provider, so spending one of `max_attempts` on it would let an
  outage or a sustained burst dead-letter work that was never actually
  tried. Every genuine business-level failure (unrenderable template,
  unregistered channel, recipient missing an address for the channel,
  transport/rejection from the provider) instead goes through `fail`, so
  it gets ordinary backoff/dead-letter treatment. Only an attempt that
  actually reaches the provider (success or failure) is reported to the
  circuit breaker -- being skipped by a guard says nothing about the
  provider's health. Only genuine storage/queue errors propagate
  out of a dispatch pass and abort the batch. `Worker::run` polls on a
  fixed interval and stops in response to a `tokio::sync::watch` shutdown
  signal, always finishing an in-flight pass first so a shutdown never
  interrupts a claimed notification.
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
- [x] Channel providers (email, SMS, push, webhook)
- [x] Worker process (`ns-worker`)
- [x] Rate limiting / circuit breaker (per-channel token bucket + breaker)
- [x] HTTP API (`ns-api`): recipients, submit/get/list/cancel notifications
- [ ] Notification templates
- [ ] Observability (metrics, tracing)
- [ ] Admin CLI
- [ ] Auth & multi-tenancy

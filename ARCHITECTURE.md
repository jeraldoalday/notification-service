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
- **`ns-storage`** -- `NotificationRepository`, `RecipientRepository`,
  `TemplateRepository` and `ApiKeyRepository` traits with a SQLite
  implementation (via `sqlx`). Each entity is stored
  as a full JSON document in a `data` column, alongside a handful of plain
  columns (`status`, `priority`, `scheduled_for`, `tenant_id`) that exist
  purely to make filtered queries efficient -- `due_for_delivery`, the
  query the worker will poll on, is backed by an index over exactly those
  columns. The JSON blob stays the single source of truth for the entity
  itself, so adding a field to `Notification` or `Recipient` doesn't
  require a matching column or migration. Templates follow the same
  pattern but are keyed by the `(tenant_id, id)` pair rather than a
  surrogate id, since a template id is a caller-chosen slug that only has
  to be unique within a tenant -- which also lets that primary key index
  serve the per-tenant listing. API keys are keyed by id but looked up by
  token digest, since a caller presents only a token: the digest column is
  `UNIQUE`, which makes that lookup a point query rather than a scan over
  every key ever issued, and makes a duplicate digest fail at the insert
  instead of silently resolving to whichever row came first.
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
  `due_for_delivery` result set without double-dispatching a job. Two
  further transitions round it out: `defer` returns claimed work to the
  queue without recording an attempt (for when the worker declined to try
  at all), and `replay` re-queues dead-lettered work as a *new*
  notification -- `DeadLettered` is terminal, so there's no legal path
  back, and cloning leaves the original as the record of what happened.
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
  per endpoint. Templates get full CRUD plus a preview endpoint that
  renders against candidate variables without sending anything; a
  `template_id` on a submitted notification is resolved and test-rendered
  up front, for the same reason the recipient is checked up front -- a
  notification that can never be delivered should be refused while the
  caller is still on the line, not discovered by the worker several
  backoff delays later. A `route_layer` resolves the request's credential
  into a `Caller` before any handler runs, and each handler asks that
  `Caller` for the scope it needs and the tenant it should act for --
  which keeps the check next to the work it guards, so a handler that
  forgets one is visible in the handler rather than in a table elsewhere.
  It's a `route_layer` rather than a `layer` so an unknown path answers
  404 instead of 401, which would otherwise turn the auth layer into a
  directory of which routes exist.
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
- **`ns-templates`** -- reusable, tenant-scoped message bodies with
  `{{variable}}` interpolation. The grammar is deliberately tiny
  (interpolation, a doubled-brace escape, nothing else): notification
  bodies are tenant-authored strings rendered on the delivery path, so
  conditionals, loops and arbitrary expressions would be a lot of
  surface area for very little gain on messages this short. A missing
  variable is an error rather than an empty substitution -- a
  notification reading "Hi , your order  shipped" is worse than one that
  fails and retries. Templates are validated when saved, so authoring
  mistakes cost nothing at delivery time, and rendered at *send* time
  rather than submit time, so editing a template also fixes the wording
  of everything still queued behind it.
- **`ns-metrics`** -- counters, gauges and latency histograms, plus a
  Prometheus text-exposition renderer. Self-contained rather than a
  metrics facade with a separate recorder and exporter: the service needs
  a couple of dozen series and one `/metrics` endpoint, and owning it
  outright keeps the naming conventions, the label vocabulary and the
  exposition format under one crate's tests. Instruments are relaxed
  atomics handed out as `Arc` handles, so a caller resolves a series once
  at startup and the registry's lock is only taken when a series is first
  created and while a scrape walks the families -- never on the delivery
  path. Every series a closed enum can produce (`ChannelKind` x outcome,
  `DeliveryStatus`) is created up front, so a channel with no traffic
  exports an explicit `0` instead of a gap a dashboard has to interpret.
  Queue depth is the exception to "record as it happens": it's recomputed
  from a `GROUP BY` at scrape time, because a gauge maintained by
  increment/decrement drifts permanently out of true the first time a
  process dies mid-transition, and the worker isn't the only writer.
- **`ns-auth`** -- API keys, scopes and the tenant identity a request acts
  under. A caller presents a bearer token; the service resolves it to an
  `AuthContext` carrying that key's tenant and scopes, and every handler
  works from the context rather than from anything the request said about
  itself. That is the distinction the crate exists to enforce: the way a
  multi-tenant service usually fails is by authenticating the caller
  correctly and then trusting the caller's own claim about which tenant it
  is acting for. Tokens are 256 bits from the system CSPRNG, stored as
  SHA-256 digests and compared in constant time -- a password hash would
  be the wrong tool, since bcrypt and argon2 exist to make *guessing*
  expensive and there is nothing here to guess, while their per-check cost
  would be paid on every authenticated request. Scopes are three coarse
  levels ordered by blast radius (`read` observes, `write` causes messages
  to be sent, `admin` mints further credentials), with `write` implying
  `read` and `admin` implying both, because a permission model finer than
  the surface it protects mostly produces scope lists nobody reads.
- **`ns-cli`** -- the `ns` admin binary: register recipients, submit test
  notifications, inspect and replay the queue, manage templates. It
  reaches the database directly rather than going through the HTTP API,
  because it's a tool for the host the service runs on and needs to work
  when the API is exactly what's broken. That's also why API-key
  management lives here as well as behind the admin-scoped endpoints: a
  deployment's first key has to come from somewhere, and an API usable
  only by someone who already holds a key can't issue one. Command implementations live in
  the library and return the text to print, so `main` owns all the I/O
  and each command is testable against a temporary database; both output
  formats (human-readable text, JSON for scripts) come off the same code
  path.

## Tenancy

Every entity the service stores is owned by exactly one tenant:
notifications and templates were tenant-keyed from the start, and
recipients gained a tenant alongside authentication. An API key belongs to
one tenant, and that -- not anything in the request -- decides which
tenant a request acts for. A `tenant_id` supplied in a body or query
string is accepted only when it agrees with the key, and refused with a
403 when it doesn't, rather than being quietly overridden: a client
sending the wrong tenant should find out rather than silently write
somewhere unexpected.

Lookups by id are the exception to that 403. Asking for a notification,
recipient or key belonging to another tenant answers 404, because a 403
would confirm the id names something real -- and unlike an explicit
`tenant_id` (where the 403 tells a misconfigured client something useful
about its own request), an opaque id belonging to someone else is
indistinguishable, from the caller's side, from an id belonging to nobody.

Two boundaries sit deliberately outside this. `/health` and `/metrics`
need no credential: a liveness probe that requires one reports the service
as down whenever the credential is what's wrong, and a scrape endpoint
behind a bearer token is one most Prometheus deployments won't be pointed
at. Neither exposes tenant data -- no metric carries a tenant label,
partly for exactly this reason. And `auth.enabled = false` turns the whole
mechanism off for a single-tenant deployment whose network already decides
who can reach it; the serve binary warns loudly at startup when it does,
because an open notification sender should be a choice someone made rather
than something they discover later.

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

A notification the worker declined to attempt (rate limited, or its
channel's circuit breaker open) travels the same `Sending -> Failed ->
Queued` edge, but without incrementing `attempt_count` -- see
`NotificationQueue::defer`.

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
- [x] Notification templates (`ns-templates`): storage, rendering, API, CLI
- [x] Admin CLI (`ns-cli`)
- [x] Observability (`ns-metrics`: instruments, Prometheus `/metrics`,
      worker and HTTP instrumentation)
- [x] Auth & multi-tenancy (`ns-auth`: API keys, scopes, per-tenant
      isolation across notifications, templates and recipients)

# notification-service

A multi-channel notification delivery service written in Rust: submit a
notification once, and it gets delivered over email, SMS, push, or webhook,
with retries, scheduling, templating, and delivery tracking.

This is a real, actively-developed project, built incrementally rather than
scaffolded in one shot. See [ARCHITECTURE.md](./ARCHITECTURE.md) for the
overall design and current build status.

## Status

Early and under active development. The domain model, configuration layer,
SQLite storage layer, job queue, channel providers, worker process, HTTP
API, notification templates, the `ns` admin CLI, Prometheus metrics,
API-key authentication with per-tenant isolation, a per-attempt delivery
history, a per-tenant suppression list, idempotent submission and
per-recipient topic preferences are all in place.

## Workspace layout

```
crates/
  core/     ns-core     -- domain models (Notification, Recipient, Channel, DeliveryStatus)
  config/   ns-config   -- TOML + environment variable configuration loading
  storage/  ns-storage  -- Repository traits + SQLite implementation (sqlx)
  queue/    ns-queue    -- job queue: submit, claim-for-processing, complete/fail with backoff
  channels/ ns-channels -- email, SMS, push, and webhook delivery behind a shared trait
  templates/ns-templates -- reusable {{variable}} message templates, rendered at send time
  metrics/  ns-metrics  -- counters, gauges and latency histograms, exposed as Prometheus text
  auth/     ns-auth     -- API keys, scopes and the tenant a request acts for
  worker/   ns-worker   -- dequeues due notifications and dispatches them, with per-channel rate limiting and circuit breaking
  api/      ns-api      -- axum HTTP API for submitting notifications and querying delivery status
  cli/      ns-cli      -- the `ns` admin binary: submit test notifications, inspect the queue, manage templates
```

See ARCHITECTURE.md for the roadmap and the reasoning behind each
boundary.

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
| GET    | `/health`                   | Liveness check (no credential)                 |
| GET    | `/metrics`                  | Prometheus exposition (no credential)          |
| POST   | `/recipients`                | Register a recipient and its channel addresses |
| GET    | `/recipients/{id}`           | Fetch a recipient                              |
| POST   | `/notifications`             | Submit a notification (queues it for delivery) |
| GET    | `/notifications/{id}`        | Fetch a notification's current status          |
| GET    | `/notifications/by-key/{key}` | Fetch the notification submitted under an idempotency key |
| GET    | `/notifications/{id}/attempts` | Every delivery attempt made for it, oldest first |
| GET    | `/attempts?tenant_id=`       | A tenant's recent attempts, newest-first       |
| GET    | `/notifications?tenant_id=`  | List a tenant's notifications, newest-first    |
| POST   | `/notifications/{id}/cancel` | Cancel a notification that hasn't started sending |
| POST   | `/templates`                 | Create a template                              |
| GET    | `/templates?tenant_id=`      | List a tenant's templates                      |
| GET    | `/templates/{id}?tenant_id=` | Fetch a template and the variables it needs    |
| PUT    | `/templates/{id}?tenant_id=` | Replace a template's subject and body          |
| DELETE | `/templates/{id}?tenant_id=` | Delete a template                              |
| POST   | `/templates/{id}/preview?tenant_id=` | Render a template against candidate variables |
| POST   | `/suppressions`              | Suppress an address, so nothing is sent to it  |
| GET    | `/suppressions?tenant_id=`   | List a tenant's suppressed addresses           |
| GET    | `/suppressions/lookup?channel=&address=` | Whether one address is suppressed  |
| DELETE | `/suppressions?channel=&address=` | Lift a suppression                        |
| PUT    | `/preferences`               | Record a recipient's answer about one topic     |
| GET    | `/preferences?recipient_id=`  | Everything one recipient has said              |
| GET    | `/preferences/resolve?recipient_id=&topic=&channel=` | Whether one topic would be sent |
| DELETE | `/preferences?recipient_id=&topic=` | Forget one recorded answer                |
| POST   | `/api-keys`                  | Mint an API key for the caller's tenant (admin) |
| GET    | `/api-keys`                  | List the caller's tenant's keys (admin)        |
| DELETE | `/api-keys/{id}`             | Revoke a key (admin)                           |

Errors are returned as `{"error": "..."}` with an appropriate status code --
`400` for validation failures (including malformed JSON), `401` for a
missing or unusable credential, `403` for a valid key that isn't allowed to
do this, `404` for unknown ids, `409` for an illegal status transition (e.g.
cancelling a notification that's already `Sending`) or a template id that
already exists, `422` for a recipient missing an address for the requested
channel, a template missing the variables it needs, or a submission to a
suppressed address.

## Authentication and tenancy

Every endpoint except `/health` and `/metrics` requires an API key:

```sh
curl localhost:8080/notifications -H "Authorization: Bearer nsk_..."
```

Mint the first one with the CLI, which reaches the database directly and
so doesn't need a key of its own:

```sh
cargo run -p ns-cli --bin ns -- apikey create \
    --tenant $T --name ci-deploy --scope write
```

The token is printed once and stored only as a SHA-256 digest -- there is
no way to read it back, so an operator who loses it mints a replacement
and revokes the old key. Keys carry one or more scopes: `read` (observe),
`write` (submit, cancel, edit -- and implies `read`), and `admin` (manage
keys). After the first key exists, the `/api-keys` endpoints do the same
job over HTTP.

A key belongs to exactly one tenant, and that is the tenant its requests
act for. `tenant_id` is therefore optional on every request body and query
string: supply it and it must agree with the key, or you get a `403`
rather than a silent override. Notifications, templates and recipients
belonging to another tenant answer `404`, so ids can't be used to probe
for what exists.

For a single-tenant deployment on a private network, `auth.enabled =
false` (or `NS_AUTH_ENABLED=false`) turns credential checking off
entirely; requests then name their own `tenant_id`. The serve binary logs
a warning at startup whenever it comes up that way.

## Metrics

`GET /metrics` serves Prometheus text exposition covering both halves of
the service:

| Metric | Type | Labels |
| ------ | ---- | ------ |
| `ns_notifications_submitted_total` | counter | `channel` |
| `ns_notifications_deduplicated_total` | counter | `channel` |
| `ns_notification_attempts_total` | counter | `channel`, `outcome` |
| `ns_notification_skipped_total` | counter | `channel`, `reason` |
| `ns_notification_send_duration_seconds` | histogram | `channel` |
| `ns_queue_notifications` | gauge | `status` |
| `ns_worker_polls_total`, `ns_worker_claimed_total` | counter | -- |
| `ns_http_requests_total` | counter | `method`, `route`, `status` |
| `ns_http_request_duration_seconds` | histogram | `method`, `route` |
| `ns_http_requests_in_flight` | gauge | -- |

Every series a closed enum can produce exists from startup, so a channel
that has seen no traffic reports an explicit `0` rather than a gap. The
`route` label is the matched route pattern (`/notifications/{id}`), never
the concrete path, and no metric carries a tenant label -- which is part
of why `/metrics` can be served without a credential.

To export them from a process that also runs the worker, share one
`ServiceMetrics` between both halves:

```rust
let metrics = Arc::new(ServiceMetrics::new());
let state = AppState::with_metrics(storage.clone(), Arc::clone(&metrics));
let worker = Worker::new(storage, channels, rate_limiter, breaker)
    .with_metrics(metrics);
```

## Idempotent submission

A client whose connection drops mid-submission cannot tell "never
arrived" from "arrived, and the response was lost". Retrying risks
sending twice; not retrying risks not sending at all. An
`Idempotency-Key` header removes the choice:

```sh
curl -X POST localhost:8080/notifications \
     -H "Authorization: Bearer $K" \
     -H "Idempotency-Key: order-1041-shipped" \
     -d '{"recipient_id": "...", "channel": "email", "body": "Your order shipped"}'
```

The first submission answers `201` with a new notification. Any repeat of
it answers `200` with that same notification, plus an
`Idempotent-Replay: true` header, and queues nothing:

| Situation | Response |
| --------- | -------- |
| Key unused | `201`, the newly queued notification |
| Same key, same content | `200`, the notification the first call created |
| Same key, different content | `409` -- the second message would otherwise vanish silently |
| Same key, different tenant | `201` -- keys are scoped to the tenant that used them |

Keys are validated, not normalised: at most 255 printable ASCII
characters, no spaces. A retry is byte-for-byte the client's first
attempt, so a service that stored a trimmed or lowercased key would fail
to match the retry that arrives. A malformed key is a `400` rather than
being ignored -- ignoring it would send the message and answer `201` to a
caller that believes it asked for deduplication.

A replayed submission skips the recipient, suppression and template
checks. Those describe a message that was already accepted, and
re-running them would let a retry fail over a recipient deleted or an
address suppressed since. A *first* submission still faces all of them.

If a client never saw the response at all, it can ask without being
willing to send:

```sh
curl "localhost:8080/notifications/by-key/order-1041-shipped" -H "Authorization: Bearer $K"
```

The same is available on the host, through the CLI:

```sh
ns notification submit --tenant $T --recipient $R --channel email \
    --body "Your order shipped" --topic order-updates --idempotency-key order-1041-shipped
ns notification by-key --tenant $T order-1041-shipped
```

Replaying a dead-lettered notification deliberately drops its key: a
replay is an operator asking for the message to go out again, which is
the one thing an idempotency key exists to prevent happening by accident.

## Delivery history

A notification's row carries a status and an attempt count, which between
them explain nothing about *why* something dead-lettered. Every dispatch
therefore appends a line to an append-only attempt log:

```sh
curl localhost:8080/notifications/$ID/attempts -H "Authorization: Bearer $K"
```

```json
[
  {"attempt_number": 1, "result": "skipped",   "reached_provider": false,
   "error": "rate limit exhausted for email",  "duration_ms": null},
  {"attempt_number": 1, "result": "failed",    "reached_provider": true,
   "error": "550 mailbox full",                "duration_ms": 214},
  {"attempt_number": 2, "result": "delivered", "reached_provider": true,
   "error": null,                              "duration_ms": 187}
]
```

`skipped` means nothing reached a provider -- an open circuit breaker, an
exhausted rate limit, an unrenderable template, a recipient with no
address for the channel -- and is kept distinct from `failed` because a
skip says nothing about the provider's health. A skip doesn't spend an
attempt, which is why consecutive skips share an `attempt_number`.

`GET /attempts` answers the other question, across a whole tenant: what is
failing right now.

The worker writes these only if given somewhere to write them, and a
failure to record one never fails the delivery it describes:

```rust
let worker = Worker::new(storage.clone(), channels, rate_limiter, breaker)
    .with_attempt_log(Arc::new(storage));
```

This is the one table where a single notification writes many rows and
nothing ever removes one, so it is the one table with a retention job:

```sh
ns queue purge-attempts --older-than-days 30
```

## Suppression list

An address on a tenant's suppression list is never sent to:

```sh
curl -X POST localhost:8080/suppressions -H 'content-type: application/json' \
     -H "Authorization: Bearer $K" \
     -d '{"channel": "email", "address": "ada@example.com", "reason": "unsubscribed"}'
```

Reasons are `unsubscribed`, `bounced`, `complaint` and `manual`, recorded
because they carry different weight -- stale `bounced` entries can safely
be revisited, `unsubscribed` and `complaint` ones must not be.

Suppression is keyed by **address**, not by recipient: the same inbox may
be registered under several recipient ids, one per integration that ever
imported it, and a suppression hanging off a recipient would be quietly
undone by the next import. It is also **per-channel**, because an address
belongs to exactly one -- "stop emailing me" is not "stop sending me
one-time passcodes". Email addresses match case-insensitively, since
suppression is the one place where matching too much is the safe failure.

The list is enforced twice. Submitting to a suppressed address is refused
with a `422` while the caller is still on the line; the worker checks
again before every send, because an address can be suppressed after a
notification is queued and that unsubscribe has to catch the batch already
waiting behind a backoff delay. A notification stopped that way ends in
the terminal `suppressed` status -- not `dead_lettered`, which is
replayable, and replaying it would send to the address that asked not to
be sent to.

## Delivery preferences

A suppression says an address is undeliverable. A **preference** says what
a person actually wants:

```sh
curl -X PUT localhost:8080/preferences -H 'content-type: application/json' \
     -H "Authorization: Bearer $K" \
     -d '{"recipient_id": "...", "topic": "marketing", "setting": "blocked"}'
```

Submit a notification with a `topic` and the recipient's answer about that
topic applies to it:

```sh
curl -X POST localhost:8080/notifications -H 'content-type: application/json' \
     -H "Authorization: Bearer $K" \
     -d '{"recipient_id": "...", "channel": "email", "topic": "marketing",
          "body": "Our summer sale is on"}'
```

Submit one **without** a topic and nothing anyone has said applies to it.
That is the point: "stop emailing me" is almost never what someone means,
and a password reset must not be silenced by a marketing opt-out. Topics
are lowercase slugs the tenant chooses (`order-updates`, `security.alerts`,
`weekly_digest`) and are validated rather than normalised -- `Marketing` is
refused rather than folded, because a folded spelling would match a
different preference row than the one anybody meant.

Omit `channel` and the answer covers every channel, including channels the
deployment adds later; supply one and it covers only that channel. The
more specific answer wins, so an exception is expressible:

```sh
# Nothing about orders -- except the text messages.
ns preference set --tenant $T --recipient $R --topic orders --setting blocked
ns preference set --tenant $T --recipient $R --topic orders --channel sms --setting allowed
ns preference check --tenant $T --recipient $R --topic orders --channel sms
```

Silence means yes. A topic nobody has expressed an opinion about is
delivered, because under an opt-in default the first message a tenant ever
sent under a new topic would go nowhere. `/preferences/resolve` reports
which preference decided a verdict, or `null` when the default did, so a
preference centre can tell an explicit yes from silence.

Like suppression, this is enforced twice: submitting a topic the recipient
has opted out of is a `422` while the caller is still on the line, and the
worker checks again before every send, since somebody can opt out after a
notification is queued. A notification stopped this way ends in the
terminal `suppressed` status, but is reported as `opted_out` in the
delivery log and in `ns_notification_attempts_total` -- a rising
suppression count is a deliverability problem, while a rising opt-out
count means people are tired of a topic, and those call for different
people to do different things.

## Templates

Rather than inlining message text into every notification, store it once
as a template and reference it by id:

```sh
curl -X POST localhost:8080/templates -H 'content-type: application/json' -d '{
  "tenant_id": "...", "id": "order_shipped", "channel": "email",
  "subject": "Your order is on its way, {{name}}",
  "body": "Hi {{name}}, order {{order_id}} shipped. Track it at {{tracking_url}}."
}'
```

Then submit against it with `"template_id": "order_shipped"` and a
`template_vars` object. The template is resolved and test-rendered at
submit time, so a typo'd id or a missing variable is a 404/422 straight
away -- but the text is rendered again by the worker at *send* time, so
editing a template also fixes the wording of everything still queued
behind it.

A missing variable is an error rather than an empty substitution: a
notification that goes out reading "Hi , your order  shipped" is worse
than one that fails loudly. Write `{{{{` for a literal `{{`.

## Admin CLI

`ns` operates on the service's database directly, so it keeps working when
the API is what's broken:

```sh
cargo run -p ns-cli --bin ns -- queue stats
```

```sh
ns apikey create --tenant $T --name ci-deploy --scope write
ns apikey list --tenant $T
ns recipient add --tenant $T --email ada@example.com --display-name "Ada Lovelace"
ns notification submit --tenant $T --recipient $R --channel email \
    --template order_shipped --var name=Ada --var order_id=1234
ns notification submit --tenant $T --recipient $R --channel email \
    --body "Your order shipped" --topic order-updates --idempotency-key order-1041
ns notification by-key --tenant $T order-1041
ns notification list --tenant $T --status dead_lettered
ns notification attempts $N       # why did this one dead-letter?
ns queue due                      # what the next worker poll would claim
ns queue replay --tenant $T       # re-queue dead-lettered work
ns queue purge-attempts --older-than-days 30
ns suppression add --tenant $T --channel email ada@example.com --reason complaint
ns suppression check --tenant $T --channel email ada@example.com
ns suppression remove --tenant $T --channel email ada@example.com
ns preference set --tenant $T --recipient $R --topic marketing --setting blocked
ns preference list --tenant $T --recipient $R
ns preference check --tenant $T --recipient $R --topic marketing --channel email
ns preference clear --tenant $T --recipient $R --topic marketing
ns template render --tenant $T order_shipped --var name=Ada
```

Every command takes `--output json` for scripting, and `--database-url` to
point at a database other than the configured one.

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

let worker = Worker::new(storage.clone(), channels, rate_limiter, circuit_breaker)
    .with_attempt_log(Arc::new(storage));
let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
worker.run(settings.queue.batch_size, poll_interval, shutdown_rx).await?;
```

The storage handed to `Worker::new` must implement `SuppressionRepository`
as well, so a deployment can't come up without the suppression list
wired; the attempt log is opt-in by comparison, because a worker that
records nothing still delivers correctly.

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
including `NS_QUEUE_MAX_RETRY_ATTEMPTS`, `NS_SMTP_FROM_ADDRESS` and
`NS_AUTH_ENABLED`.

## License

MIT

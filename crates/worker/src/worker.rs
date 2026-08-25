use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use ns_core::{AttemptResult, DeliveryAttempt, DeliveryStatus, Notification};
use ns_metrics::{AttemptOutcome, ServiceMetrics, SkipReason};
use ns_queue::NotificationQueue;
use ns_storage::{
    DeliveryAttemptRepository, NotificationRepository, RecipientRepository, SuppressionRepository,
    TemplateRepository,
};
use tracing::{info, warn};

use crate::circuit_breaker::CircuitBreaker;
use crate::error::{WorkerError, WorkerResult};
use crate::rate_limit::RateLimiter;
use crate::registry::ChannelRegistry;

/// How long to hold a rate-limited notification before it becomes
/// claimable again. Short, because a token bucket refills continuously --
/// this only needs to be long enough that the next poll doesn't
/// immediately re-claim the same work and find the bucket still empty.
const RATE_LIMIT_DEFER: Duration = Duration::seconds(5);

/// What happened to a single claimed notification during one dispatch pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchOutcome {
    Delivered,
    /// The send failed and the notification was requeued for another
    /// attempt after a backoff delay.
    Failed,
    /// No send was attempted -- a guard (rate limiter or circuit breaker)
    /// declined it -- so the notification went back on the queue with its
    /// attempt budget intact.
    Deferred,
    /// The send failed and no attempts remained, so the notification was
    /// dead-lettered.
    DeadLettered,
    /// The recipient's address is on the tenant's suppression list, so
    /// nothing was sent and the notification is terminally suppressed.
    Suppressed,
}

fn outcome_from_status(status: DeliveryStatus) -> DispatchOutcome {
    match status {
        DeliveryStatus::DeadLettered => DispatchOutcome::DeadLettered,
        _ => DispatchOutcome::Failed,
    }
}

impl DispatchOutcome {
    /// The metrics-layer spelling of this outcome. `ns-metrics` keeps its
    /// own enum rather than importing this one, so that a crate needing
    /// only a four-variant label doesn't have to depend on the worker's
    /// storage, channel and runtime dependencies.
    fn as_metric(&self) -> AttemptOutcome {
        match self {
            DispatchOutcome::Delivered => AttemptOutcome::Delivered,
            DispatchOutcome::Failed => AttemptOutcome::Failed,
            DispatchOutcome::Deferred => AttemptOutcome::Deferred,
            DispatchOutcome::DeadLettered => AttemptOutcome::DeadLettered,
            DispatchOutcome::Suppressed => AttemptOutcome::Suppressed,
        }
    }
}

/// Polls `ns-queue` for due notifications and dispatches each to the
/// `ns-channels` implementation matching its `ChannelKind`, reporting the
/// outcome back to the queue -- which owns all retry/backoff/dead-letter
/// decisions, so `Worker` only ever decides *when* to attempt a send, never
/// what happens after one fails.
pub struct Worker<R> {
    queue: NotificationQueue<R>,
    repo: R,
    channels: ChannelRegistry,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    /// Where templated notifications get their text from. Held as a
    /// separate trait object rather than another bound on `R` so a
    /// deployment that doesn't use templates -- and every existing
    /// caller of `Worker::new` -- is unaffected.
    templates: Option<Arc<dyn TemplateRepository>>,
    /// Optional for the same reason: a `Worker` with no metrics handle
    /// records nothing and behaves identically, so embedding one in a test
    /// (or in a deployment that doesn't scrape) needs no extra wiring.
    metrics: Option<Arc<ServiceMetrics>>,
    /// Where the per-attempt history goes. Also optional, and for a
    /// stronger reason than the other two: the log is a record of
    /// delivery, not a part of it, so a worker without one must still
    /// deliver exactly as it would with one.
    attempts: Option<Arc<dyn DeliveryAttemptRepository>>,
}

// SuppressionRepository is a bound on `R` rather than another optional
// handle like `templates` or `metrics`, and the difference is deliberate.
// A worker missing its template store fails the notifications that need
// one, loudly; a worker missing its metrics handle records nothing and
// costs nobody anything. A worker missing its suppression list would
// silently send to every address that ever asked not to be contacted --
// the one failure mode here that is worse than not running at all. Making
// it a bound means a deployment cannot forget to wire it.
impl<R: NotificationRepository + RecipientRepository + SuppressionRepository + Clone> Worker<R> {
    pub fn new(
        repo: R,
        channels: ChannelRegistry,
        rate_limiter: RateLimiter,
        circuit_breaker: CircuitBreaker,
    ) -> Self {
        Self {
            queue: NotificationQueue::new(repo.clone()),
            repo,
            channels,
            rate_limiter,
            circuit_breaker,
            templates: None,
            metrics: None,
            attempts: None,
        }
    }

    /// Gives this worker a template store, enabling delivery of
    /// notifications that carry a `template_id`. Without one, such a
    /// notification fails (and eventually dead-letters) rather than going
    /// out with an unrendered body.
    pub fn with_templates(mut self, templates: Arc<dyn TemplateRepository>) -> Self {
        self.templates = Some(templates);
        self
    }

    /// Gives this worker somewhere to report what it did. Shared as an
    /// `Arc` so a process running the worker and the HTTP API together
    /// exports one set of counters covering both.
    pub fn with_metrics(mut self, metrics: Arc<ServiceMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Gives this worker somewhere to write each attempt's outcome, which
    /// is what turns "attempt 4 of 5" on a dead-lettered notification into
    /// an answerable question.
    pub fn with_attempt_log(mut self, attempts: Arc<dyn DeliveryAttemptRepository>) -> Self {
        self.attempts = Some(attempts);
        self
    }

    /// Writes one line of delivery history, and swallows any failure to do
    /// so.
    ///
    /// This is the one place in the worker where a storage error does not
    /// abort the pass. Everywhere else that would be wrong -- if the queue
    /// can't be updated, continuing means double-sending. Here it is the
    /// only defensible choice: the notification has already been sent (or
    /// already been refused), and failing the dispatch over an unwritable
    /// audit row would turn a logging problem into a duplicate message.
    /// The warning is what an operator sees instead.
    async fn log_attempt(&self, attempt: DeliveryAttempt) {
        let Some(attempts) = &self.attempts else {
            return;
        };

        if let Err(err) = attempts.record_attempt(&attempt).await {
            warn!(
                notification_id = %attempt.notification_id,
                error = %err,
                "could not record delivery attempt; delivery itself was unaffected"
            );
        }
    }

    /// Records an attempt that never reached a provider, alongside the
    /// reason. Every caller pairs this with `defer` or `fail` -- the log
    /// entry says what happened, the queue call decides what happens next.
    async fn log_skip(&self, notification: &Notification, reason: String, now: DateTime<Utc>) {
        self.log_attempt(DeliveryAttempt::new(notification, AttemptResult::Skipped, now).with_error(reason))
            .await;
    }

    fn record_skip(&self, notification: &Notification, reason: SkipReason) {
        if let Some(metrics) = &self.metrics {
            metrics.record_skip(notification.channel, reason);
        }
    }

    /// Replaces a templated notification's subject/body with the rendered
    /// text. Notifications without a `template_id` pass through untouched,
    /// so this is a no-op for the common case.
    ///
    /// Rendering happens against the template as it exists *now*, not as it
    /// existed at submit time: editing a template fixes the wording of
    /// everything still queued behind it, which is the point of having
    /// templates at all rather than inlining the text into each
    /// notification.
    async fn render_if_templated(&self, notification: &mut Notification) -> WorkerResult<()> {
        let Some(template_id) = notification.template_id.clone() else {
            return Ok(());
        };

        let templates = self
            .templates
            .as_ref()
            .ok_or_else(|| WorkerError::TemplatesNotConfigured { template_id: template_id.clone() })?;

        let template = templates.get_template(notification.tenant_id, &template_id).await?;
        template.render_for(notification)?.apply_to(notification);
        Ok(())
    }

    /// Dispatches a single already-claimed (`Sending`) notification: checks
    /// in two phases. First, whether it should be sent at all: it renders,
    /// resolves the channel and the recipient's address, and checks that
    /// address against the tenant's suppression list. Then, whether it can
    /// be sent now: the circuit breaker and the rate limiter. Only then
    /// does it send and report the outcome back to the queue and the
    /// circuit breaker. Every
    /// business-level failure (breaker open, rate limited, unregistered
    /// channel, missing address, provider rejection/transport error) is
    /// routed through `NotificationQueue::fail` so it gets the same
    /// backoff/dead-letter treatment as any other failed attempt --
    /// infrastructure errors (storage/queue) instead propagate via `?` and
    /// abort the batch, since retrying those without fixing the underlying
    /// problem would just busy-loop. Only an actual send attempt reaching
    /// the provider (success or failure) is reported to the circuit
    /// breaker; being skipped by the rate limiter or a missing
    /// address/channel says nothing about the provider's health.
    async fn dispatch_one(
        &self,
        mut notification: Notification,
        now: DateTime<Utc>,
    ) -> WorkerResult<DispatchOutcome> {
        // Rendering runs before the guards on purpose: a message that
        // can't even be built is never going to reach the provider, so
        // there's no reason to spend one of that channel's rate-limit
        // tokens discovering that.
        if let Err(err) = self.render_if_templated(&mut notification).await {
            warn!(notification_id = %notification.id, error = %err, "could not render template");
            // Logged as a skip rather than a failure even though it does
            // spend an attempt: nothing reached a provider, and an
            // operator scanning the log for a provider's health should not
            // find this service's own rendering problem among the results.
            self.log_skip(&notification, err.to_string(), now).await;
            let failed = self.queue.fail(notification, now).await?;
            return Ok(outcome_from_status(failed.status));
        }

        let Some(channel) = self.channels.get(notification.channel) else {
            warn!(channel = %notification.channel, "no channel registered, failing notification");
            self.log_skip(
                &notification,
                format!("no {} channel is registered on this worker", notification.channel),
                now,
            )
            .await;
            let failed = self.queue.fail(notification, now).await?;
            return Ok(outcome_from_status(failed.status));
        };

        let recipient = match self.repo.get_recipient(notification.recipient_id).await {
            Ok(recipient) => recipient,
            Err(err) => {
                warn!(notification_id = %notification.id, error = %err, "could not load recipient");
                self.log_skip(&notification, err.to_string(), now).await;
                let failed = self.queue.fail(notification, now).await?;
                return Ok(outcome_from_status(failed.status));
            }
        };

        let address = match recipient.address_for(notification.channel) {
            Ok(address) => address,
            Err(err) => {
                warn!(notification_id = %notification.id, error = %err, "recipient missing address");
                self.log_skip(&notification, err.to_string(), now).await;
                let failed = self.queue.fail(notification, now).await?;
                return Ok(outcome_from_status(failed.status));
            }
        };

        // The last of the "should this be sent at all" checks, and the
        // first moment the service knows which address this notification
        // would actually reach -- a notification names a recipient and a
        // channel, and the address only falls out of resolving both.
        if let Some(suppression) = self.repo.find_suppression(notification.tenant_id, address).await? {
            info!(
                notification_id = %notification.id,
                reason = %suppression.reason,
                "address is suppressed, not sending"
            );
            self.log_skip(&notification, format!("address is suppressed ({})", suppression.reason), now)
                .await;
            self.queue.suppress(notification, now).await?;
            return Ok(DispatchOutcome::Suppressed);
        }

        // Everything above decided whether this should be sent; the two
        // guards below decide whether it can be sent *now*. Running them
        // in that order means a notification that will never go out --
        // unrenderable, unaddressable, suppressed -- doesn't consume one
        // of the channel's rate-limit tokens or sit behind an open breaker
        // waiting to discover that.
        //
        // A guard declining a send is not a failed delivery: nothing
        // reached a provider, so spending one of the notification's
        // attempts on it would let a provider outage or a sustained burst
        // dead-letter work that was never actually tried. Both guards
        // therefore defer rather than fail. The breaker's cooldown is the
        // natural delay for an open breaker -- the notification comes back
        // around about when the breaker is ready to trial one.
        if !self.circuit_breaker.allow(notification.channel, now) {
            self.record_skip(&notification, SkipReason::CircuitOpen);
            self.log_skip(&notification, format!("circuit breaker open for {}", notification.channel), now)
                .await;
            self.queue.defer(notification, self.circuit_breaker.cooldown(), now).await?;
            return Ok(DispatchOutcome::Deferred);
        }

        if !self.rate_limiter.try_acquire(notification.channel, now) {
            self.record_skip(&notification, SkipReason::RateLimited);
            self.log_skip(&notification, format!("rate limit exhausted for {}", notification.channel), now)
                .await;
            self.queue.defer(notification, RATE_LIMIT_DEFER, now).await?;
            return Ok(DispatchOutcome::Deferred);
        }

        // Timed around the provider call only, so the histogram measures
        // the provider rather than this worker's own bookkeeping -- and
        // timed only here, because a send a guard refused never happened
        // and reporting microseconds of doing nothing as provider latency
        // would drag every percentile down.
        let started = std::time::Instant::now();
        let result = channel.send(&notification, address).await;
        let elapsed = started.elapsed();
        if let Some(metrics) = &self.metrics {
            metrics.observe_send_duration(notification.channel, elapsed.as_secs_f64());
        }
        let duration_ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;

        match result {
            Ok(()) => {
                self.circuit_breaker.record_success(notification.channel);
                self.log_attempt(
                    DeliveryAttempt::new(&notification, AttemptResult::Delivered, now)
                        .with_duration_ms(duration_ms),
                )
                .await;
                self.queue.complete(notification, now).await?;
                Ok(DispatchOutcome::Delivered)
            }
            Err(err) => {
                self.circuit_breaker.record_failure(notification.channel, now);
                warn!(notification_id = %notification.id, error = %err, "delivery attempt failed");
                self.log_attempt(
                    DeliveryAttempt::new(&notification, AttemptResult::Failed, now)
                        .with_error(err.to_string())
                        .with_duration_ms(duration_ms),
                )
                .await;
                let failed = self.queue.fail(notification, now).await?;
                Ok(outcome_from_status(failed.status))
            }
        }
    }

    /// Claims up to `batch_size` due notifications and dispatches each in
    /// turn, returning the outcome for every notification it claimed (in
    /// claim order). An empty batch is not an error -- it just means
    /// nothing was due.
    pub async fn run_once(&self, batch_size: u32) -> WorkerResult<Vec<(Notification, DispatchOutcome)>> {
        let now = Utc::now();
        let claimed = self.queue.claim_batch(now, batch_size).await?;
        let mut outcomes = Vec::with_capacity(claimed.len());

        if let Some(metrics) = &self.metrics {
            metrics.record_poll(claimed.len());
        }

        for notification in claimed {
            let channel = notification.channel;
            let outcome = self.dispatch_one(notification.clone(), Utc::now()).await?;
            if let Some(metrics) = &self.metrics {
                metrics.record_attempt(channel, outcome.as_metric());
            }
            outcomes.push((notification, outcome));
        }

        Ok(outcomes)
    }

    /// Repeatedly calls `run_once` on `poll_interval`, until `shutdown`
    /// carries `true`. Each pass runs to completion before the shutdown
    /// flag is checked, so a shutdown signal never interrupts a
    /// notification that's already been claimed.
    pub async fn run(
        &self,
        batch_size: u32,
        poll_interval: std::time::Duration,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> WorkerResult<()> {
        loop {
            self.run_once(batch_size).await?;

            if *shutdown.borrow() {
                info!("worker received shutdown signal, stopping");
                return Ok(());
            }

            tokio::select! {
                _ = tokio::time::sleep(poll_interval) => {}
                _ = shutdown.changed() => {}
            }

            if *shutdown.borrow() {
                info!("worker received shutdown signal, stopping");
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Duration;
    use ns_channels::{ChannelError, ChannelResult, NotificationChannel};
    use ns_core::{ChannelKind, Priority, Recipient, RecipientAddress};
    use ns_storage::SqliteStorage;
    use ns_storage::TemplateRepository;
    use ns_templates::Template;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[derive(Default)]
    struct FakeChannel {
        should_succeed: bool,
        calls: AtomicUsize,
        /// Every notification handed to `send`, so tests can assert on the
        /// text that actually went out rather than only on the stored row.
        sent: Mutex<Vec<Notification>>,
    }

    impl FakeChannel {
        fn succeeding() -> Self {
            Self { should_succeed: true, calls: AtomicUsize::new(0), sent: Mutex::default() }
        }

        fn failing() -> Self {
            Self { should_succeed: false, calls: AtomicUsize::new(0), sent: Mutex::default() }
        }

        fn last_sent(&self) -> Notification {
            self.sent.lock().unwrap().last().cloned().expect("channel was never invoked")
        }
    }

    #[async_trait]
    impl NotificationChannel for FakeChannel {
        async fn send(&self, notification: &Notification, _address: &RecipientAddress) -> ChannelResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.sent.lock().unwrap().push(notification.clone());
            if self.should_succeed {
                Ok(())
            } else {
                Err(ChannelError::Rejected("simulated rejection".into()))
            }
        }
    }

    async fn test_repo() -> (SqliteStorage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let storage = SqliteStorage::connect(&format!("sqlite://{}", db_path.display())).await.unwrap();
        (storage, dir)
    }

    async fn seed_recipient(repo: &SqliteStorage, channel: ChannelKind) -> Uuid {
        let address = match channel {
            ChannelKind::Email => RecipientAddress::Email("ada@example.com".into()),
            ChannelKind::Sms => RecipientAddress::Phone("+15555550100".into()),
            ChannelKind::Push => RecipientAddress::PushToken("device-token".into()),
            ChannelKind::Webhook => RecipientAddress::WebhookUrl("https://example.com/hook".into()),
        };
        let recipient = Recipient::new(Uuid::new_v4(), Uuid::new_v4()).with_address(address).unwrap();
        repo.insert_recipient(&recipient).await.unwrap();
        recipient.id
    }

    async fn seed_notification(repo: &SqliteStorage, recipient_id: Uuid, max_attempts: u32) -> Notification {
        let queue = NotificationQueue::new(repo.clone());
        let notification = Notification::builder(Uuid::new_v4(), recipient_id, ChannelKind::Webhook)
            .body("hello")
            .priority(Priority::Normal)
            .max_attempts(max_attempts)
            .build()
            .unwrap();
        queue.submit(notification, Utc::now()).await.unwrap()
    }

    // -- suppression ---------------------------------------------------

    async fn suppress_address(repo: &SqliteStorage, tenant_id: Uuid, address: RecipientAddress) {
        use ns_core::{Suppression, SuppressionReason};
        let entry =
            Suppression::new(tenant_id, address, SuppressionReason::Unsubscribed, Utc::now()).unwrap();
        ns_storage::SuppressionRepository::insert_suppression(repo, &entry).await.unwrap();
    }

    /// A recipient plus a queued notification for it, sharing one tenant,
    /// so a suppression added for that tenant actually covers the send.
    async fn seed_for_suppression(repo: &SqliteStorage) -> (Uuid, Notification, RecipientAddress) {
        let tenant_id = Uuid::new_v4();
        let address = RecipientAddress::WebhookUrl("https://example.com/hook".into());
        let recipient = Recipient::new(Uuid::new_v4(), tenant_id).with_address(address.clone()).unwrap();
        repo.insert_recipient(&recipient).await.unwrap();

        let queue = NotificationQueue::new(repo.clone());
        let notification = Notification::builder(tenant_id, recipient.id, ChannelKind::Webhook)
            .body("hello")
            .max_attempts(3)
            .build()
            .unwrap();
        let submitted = queue.submit(notification, Utc::now()).await.unwrap();
        (tenant_id, submitted, address)
    }

    #[tokio::test]
    async fn a_suppressed_address_is_never_sent_to() {
        let (repo, _dir) = test_repo().await;
        let (tenant_id, submitted, address) = seed_for_suppression(&repo).await;
        suppress_address(&repo, tenant_id, address).await;

        let channel = Arc::new(FakeChannel::succeeding());
        let registry = ChannelRegistry::new().register(ChannelKind::Webhook, Arc::clone(&channel) as _);
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled());

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Suppressed);
        assert_eq!(channel.calls.load(Ordering::SeqCst), 0, "the provider was never contacted");
        let stored = repo.get_notification(submitted.id).await.unwrap();
        assert_eq!(stored.status, DeliveryStatus::Suppressed);
    }

    #[tokio::test]
    async fn suppression_spends_no_attempt_and_never_comes_back() {
        let (repo, _dir) = test_repo().await;
        let (tenant_id, submitted, address) = seed_for_suppression(&repo).await;
        suppress_address(&repo, tenant_id, address).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled());
        worker.run_once(10).await.unwrap();

        let stored = repo.get_notification(submitted.id).await.unwrap();
        assert_eq!(stored.attempt_count, 0);
        assert!(worker.run_once(10).await.unwrap().is_empty(), "and it is not claimable again");
    }

    #[tokio::test]
    async fn another_tenants_suppression_does_not_apply() {
        let (repo, _dir) = test_repo().await;
        let (_tenant_id, submitted, address) = seed_for_suppression(&repo).await;
        suppress_address(&repo, Uuid::new_v4(), address).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled());
        worker.run_once(10).await.unwrap();

        assert_eq!(
            repo.get_notification(submitted.id).await.unwrap().status,
            DeliveryStatus::Delivered,
            "suppression lists are per-tenant"
        );
    }

    #[tokio::test]
    async fn suppression_on_another_channel_does_not_apply() {
        let (repo, _dir) = test_repo().await;
        let (tenant_id, submitted, _address) = seed_for_suppression(&repo).await;
        // Same tenant, same person, different channel: unsubscribing from
        // email says nothing about the webhook.
        suppress_address(&repo, tenant_id, RecipientAddress::Email("ada@example.com".into())).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled());
        worker.run_once(10).await.unwrap();

        assert_eq!(repo.get_notification(submitted.id).await.unwrap().status, DeliveryStatus::Delivered);
    }

    #[tokio::test]
    async fn a_suppressed_dispatch_is_recorded_in_the_history() {
        let (repo, _dir) = test_repo().await;
        let (tenant_id, submitted, address) = seed_for_suppression(&repo).await;
        suppress_address(&repo, tenant_id, address).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        logging_worker(&repo, registry).run_once(10).await.unwrap();

        let attempts = attempts_for(&repo, submitted.id).await;
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].result, AttemptResult::Skipped);
        assert!(attempts[0].error.as_deref().unwrap().contains("suppressed"));
        assert!(attempts[0].error.as_deref().unwrap().contains("unsubscribed"), "the reason is recorded");
    }

    #[tokio::test]
    async fn a_suppressed_dispatch_costs_no_rate_limit_token() {
        let (repo, _dir) = test_repo().await;
        let (tenant_id, _submitted, address) = seed_for_suppression(&repo).await;
        suppress_address(&repo, tenant_id, address).await;
        // A second, deliverable notification for a different recipient,
        // against a bucket holding exactly one token.
        let other = seed_recipient(&repo, ChannelKind::Webhook).await;
        let deliverable = seed_notification(&repo, other, 3).await;

        let channel = Arc::new(FakeChannel::succeeding());
        let registry = ChannelRegistry::new().register(ChannelKind::Webhook, Arc::clone(&channel) as _);
        let worker = Worker::new(
            repo.clone(),
            registry,
            RateLimiter::new().with_limit(ChannelKind::Webhook, 1, 0.0),
            CircuitBreaker::disabled(),
        );

        worker.run_once(10).await.unwrap();

        assert_eq!(
            repo.get_notification(deliverable.id).await.unwrap().status,
            DeliveryStatus::Delivered,
            "the suppressed notification must not have eaten the only token"
        );
        assert_eq!(channel.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn suppression_is_checked_even_while_the_breaker_is_open() {
        let (repo, _dir) = test_repo().await;
        let (tenant_id, submitted, address) = seed_for_suppression(&repo).await;
        suppress_address(&repo, tenant_id, address).await;

        // An open breaker would previously have deferred this forever;
        // "should we send at all" is now settled first, so it ends here.
        let breaker = CircuitBreaker::new(1, Duration::hours(1));
        breaker.record_failure(ChannelKind::Webhook, Utc::now());
        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), breaker);

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Suppressed);
        assert_eq!(repo.get_notification(submitted.id).await.unwrap().status, DeliveryStatus::Suppressed);
    }

    // -- delivery attempt log ------------------------------------------

    /// A worker wired to write its history, plus the storage to read it
    /// back from. Every attempt-log test wants both halves.
    fn logging_worker(repo: &SqliteStorage, registry: ChannelRegistry) -> Worker<SqliteStorage> {
        Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled())
            .with_attempt_log(Arc::new(repo.clone()))
    }

    async fn attempts_for(repo: &SqliteStorage, notification_id: Uuid) -> Vec<DeliveryAttempt> {
        repo.list_attempts(notification_id, 50).await.unwrap()
    }

    #[tokio::test]
    async fn a_successful_send_is_recorded_as_one_delivered_attempt() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        logging_worker(&repo, registry).run_once(10).await.unwrap();

        let attempts = attempts_for(&repo, submitted.id).await;
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].result, AttemptResult::Delivered);
        assert_eq!(attempts[0].attempt_number, 1);
        assert_eq!(attempts[0].tenant_id, submitted.tenant_id);
        assert_eq!(attempts[0].error, None);
        assert!(attempts[0].duration_ms.is_some(), "a send that reached a provider has a duration");
    }

    #[tokio::test]
    async fn a_rejected_send_records_what_the_provider_said() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::failing()));
        logging_worker(&repo, registry).run_once(10).await.unwrap();

        let attempts = attempts_for(&repo, submitted.id).await;
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].result, AttemptResult::Failed);
        assert!(attempts[0].error.as_deref().unwrap().contains("simulated rejection"));
        assert!(attempts[0].reached_provider());
    }

    #[tokio::test]
    async fn each_retry_appends_a_numbered_line_of_history() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::failing()));
        let worker = logging_worker(&repo, registry);

        // Three passes, each clearing the backoff the previous one set, so
        // the notification is due again.
        for _ in 0..3 {
            let mut stored = repo.get_notification(submitted.id).await.unwrap();
            stored.scheduled_for = None;
            repo.update_notification(&stored).await.unwrap();
            worker.run_once(10).await.unwrap();
        }

        let attempts = attempts_for(&repo, submitted.id).await;
        assert_eq!(attempts.iter().map(|a| a.attempt_number).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert!(attempts.iter().all(|a| a.result == AttemptResult::Failed));
        assert_eq!(repo.get_notification(submitted.id).await.unwrap().status, DeliveryStatus::DeadLettered);
    }

    #[tokio::test]
    async fn a_rate_limited_dispatch_is_recorded_as_a_skip() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let worker = Worker::new(
            repo.clone(),
            registry,
            RateLimiter::new().with_limit(ChannelKind::Webhook, 0, 0.0),
            CircuitBreaker::disabled(),
        )
        .with_attempt_log(Arc::new(repo.clone()));

        worker.run_once(10).await.unwrap();

        let attempts = attempts_for(&repo, submitted.id).await;
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].result, AttemptResult::Skipped);
        assert!(!attempts[0].reached_provider());
        assert!(attempts[0].error.as_deref().unwrap().contains("rate limit"));
        assert_eq!(attempts[0].duration_ms, None, "nothing was timed, because nothing was sent");
    }

    #[tokio::test]
    async fn a_skip_does_not_advance_the_attempt_number_the_next_send_gets() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        // First pass: no tokens, so the dispatch is skipped and deferred.
        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        Worker::new(
            repo.clone(),
            registry,
            RateLimiter::new().with_limit(ChannelKind::Webhook, 0, 0.0),
            CircuitBreaker::disabled(),
        )
        .with_attempt_log(Arc::new(repo.clone()))
        .run_once(10)
        .await
        .unwrap();

        // Second pass: the defer's delay is cleared and the limiter is
        // open, so the send actually happens.
        let mut stored = repo.get_notification(submitted.id).await.unwrap();
        stored.scheduled_for = None;
        repo.update_notification(&stored).await.unwrap();
        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        logging_worker(&repo, registry).run_once(10).await.unwrap();

        let attempts = attempts_for(&repo, submitted.id).await;
        let numbered: Vec<(u32, AttemptResult)> =
            attempts.iter().map(|a| (a.attempt_number, a.result)).collect();
        assert_eq!(
            numbered,
            vec![(1, AttemptResult::Skipped), (1, AttemptResult::Delivered)],
            "the send was still the notification's first real attempt"
        );
    }

    #[tokio::test]
    async fn an_unregistered_channel_is_recorded_as_a_skip_naming_it() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        logging_worker(&repo, ChannelRegistry::new()).run_once(10).await.unwrap();

        let attempts = attempts_for(&repo, submitted.id).await;
        assert_eq!(attempts[0].result, AttemptResult::Skipped);
        assert!(attempts[0].error.as_deref().unwrap().contains("webhook"));
    }

    #[tokio::test]
    async fn an_unrenderable_template_is_recorded_as_a_skip() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let queue = NotificationQueue::new(repo.clone());
        let notification = Notification::builder(Uuid::new_v4(), recipient_id, ChannelKind::Webhook)
            .template("nonexistent")
            .build()
            .unwrap();
        let submitted = queue.submit(notification, Utc::now()).await.unwrap();

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        // No template store configured, so rendering can't even be tried.
        logging_worker(&repo, registry).run_once(10).await.unwrap();

        let attempts = attempts_for(&repo, submitted.id).await;
        assert_eq!(attempts[0].result, AttemptResult::Skipped);
        assert!(!attempts[0].reached_provider(), "the service's own problem, not the provider's");
    }

    #[tokio::test]
    async fn a_worker_without_an_attempt_log_delivers_identically() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled());

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Delivered);
        assert_eq!(repo.get_notification(submitted.id).await.unwrap().status, DeliveryStatus::Delivered);
        assert!(attempts_for(&repo, submitted.id).await.is_empty());
    }

    #[tokio::test]
    async fn one_notifications_history_does_not_bleed_into_anothers() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let first = seed_notification(&repo, recipient_id, 3).await;
        let second = seed_notification(&repo, recipient_id, 3).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        logging_worker(&repo, registry).run_once(10).await.unwrap();

        assert_eq!(attempts_for(&repo, first.id).await.len(), 1);
        assert_eq!(attempts_for(&repo, second.id).await.len(), 1);
    }

    #[tokio::test]
    async fn delivers_successfully_and_marks_delivered() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled());

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].0.id, submitted.id);
        assert_eq!(outcomes[0].1, DispatchOutcome::Delivered);

        let stored = repo.get_notification(submitted.id).await.unwrap();
        assert_eq!(stored.status, DeliveryStatus::Delivered);
    }

    #[tokio::test]
    async fn requeues_with_backoff_when_send_fails_and_attempts_remain() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::failing()));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled());

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Failed);
        let stored = repo.get_notification(submitted.id).await.unwrap();
        assert_eq!(stored.status, DeliveryStatus::Queued);
        assert_eq!(stored.attempt_count, 1);
    }

    #[tokio::test]
    async fn dead_letters_once_attempts_are_exhausted() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 1).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::failing()));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled());

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::DeadLettered);
        let stored = repo.get_notification(submitted.id).await.unwrap();
        assert_eq!(stored.status, DeliveryStatus::DeadLettered);
    }

    #[tokio::test]
    async fn fails_when_no_channel_is_registered() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        seed_notification(&repo, recipient_id, 3).await;

        let worker =
            Worker::new(repo.clone(), ChannelRegistry::new(), RateLimiter::new(), CircuitBreaker::disabled());

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Failed);
    }

    #[tokio::test]
    async fn fails_when_recipient_has_no_address_for_the_channel() {
        let (repo, _dir) = test_repo().await;
        // Recipient only has an email address, but the notification is on
        // the webhook channel.
        let recipient_id = seed_recipient(&repo, ChannelKind::Email).await;
        seed_notification(&repo, recipient_id, 3).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled());

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Failed);
    }

    #[tokio::test]
    async fn rate_limited_channel_fails_the_attempt_instead_of_sending() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        seed_notification(&repo, recipient_id, 3).await;

        let channel = Arc::new(FakeChannel::succeeding());
        let registry = ChannelRegistry::new().register(ChannelKind::Webhook, channel.clone());
        // Zero capacity: the very first acquire attempt is denied.
        let rate_limiter = RateLimiter::new().with_limit(ChannelKind::Webhook, 0, 1.0);
        let worker = Worker::new(repo.clone(), registry, rate_limiter, CircuitBreaker::disabled());

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Deferred);
        assert_eq!(channel.calls.load(Ordering::SeqCst), 0, "channel must not be invoked when rate limited");
    }

    #[tokio::test]
    async fn run_once_is_a_noop_when_nothing_is_due() {
        let (repo, _dir) = test_repo().await;
        let worker =
            Worker::new(repo, ChannelRegistry::new(), RateLimiter::new(), CircuitBreaker::disabled());

        let outcomes = worker.run_once(10).await.unwrap();

        assert!(outcomes.is_empty());
    }

    #[tokio::test]
    async fn open_circuit_breaker_fails_the_attempt_without_invoking_the_channel() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        seed_notification(&repo, recipient_id, 3).await;

        let channel = Arc::new(FakeChannel::succeeding());
        let registry = ChannelRegistry::new().register(ChannelKind::Webhook, channel.clone());
        let circuit_breaker = CircuitBreaker::new(1, Duration::seconds(60));
        // Pre-trip the breaker as if a prior send had already failed.
        circuit_breaker.record_failure(ChannelKind::Webhook, Utc::now());
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), circuit_breaker);

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Deferred);
        assert_eq!(
            channel.calls.load(Ordering::SeqCst),
            0,
            "channel must not be invoked while the breaker is open"
        );
    }

    #[tokio::test]
    async fn repeated_send_failures_trip_the_breaker_and_stop_further_attempts() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        for _ in 0..3 {
            seed_notification(&repo, recipient_id, 5).await;
        }

        let channel = Arc::new(FakeChannel::failing());
        let registry = ChannelRegistry::new().register(ChannelKind::Webhook, channel.clone());
        let circuit_breaker = CircuitBreaker::new(2, Duration::seconds(60));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), circuit_breaker);

        // Three notifications claimed in one pass: the first two fail and
        // trip the breaker after the second, so the third is denied before
        // the channel is ever invoked for it.
        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes.len(), 3);
        assert_eq!(outcomes[0].1, DispatchOutcome::Failed);
        assert_eq!(outcomes[1].1, DispatchOutcome::Failed);
        assert_eq!(outcomes[2].1, DispatchOutcome::Deferred, "the third was never attempted");
        assert_eq!(channel.calls.load(Ordering::SeqCst), 2, "breaker should stop the third send attempt");
    }

    #[tokio::test]
    async fn run_polls_until_shutdown_and_delivers_in_the_meantime() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let worker =
            Arc::new(Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled()));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let running = Arc::clone(&worker);
        let handle =
            tokio::spawn(
                async move { running.run(10, std::time::Duration::from_millis(5), shutdown_rx).await },
            );

        // Give the loop a few polls to pick up and deliver the seeded
        // notification before asking it to stop.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown_tx.send(true).unwrap();

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("worker should stop promptly after the shutdown signal")
            .unwrap();
        assert!(outcome.is_ok());

        let stored = repo.get_notification(submitted.id).await.unwrap();
        assert_eq!(stored.status, DeliveryStatus::Delivered);
    }

    /// Submits a notification that renders through `template_id` instead of
    /// carrying its own body, returning the tenant it belongs to so the
    /// test can store a template under the same tenant.
    async fn seed_templated_notification(
        repo: &SqliteStorage,
        recipient_id: Uuid,
        template_id: &str,
        vars: &[(&str, &str)],
    ) -> Notification {
        let queue = NotificationQueue::new(repo.clone());
        let mut builder = Notification::builder(Uuid::new_v4(), recipient_id, ChannelKind::Webhook)
            .template(template_id)
            .max_attempts(3);
        for (key, value) in vars {
            builder = builder.template_var(*key, *value);
        }
        queue.submit(builder.build().unwrap(), Utc::now()).await.unwrap()
    }

    #[tokio::test]
    async fn renders_a_templated_notification_before_sending_it() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_templated_notification(&repo, recipient_id, "welcome", &[("name", "Ada")]).await;

        let template = Template::new(submitted.tenant_id, "welcome", "Hi {{name}}, welcome aboard.")
            .unwrap()
            .with_subject("Welcome, {{name}}")
            .unwrap();
        repo.insert_template(&template).await.unwrap();

        let channel = Arc::new(FakeChannel::succeeding());
        let registry = ChannelRegistry::new().register(ChannelKind::Webhook, channel.clone());
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled())
            .with_templates(Arc::new(repo.clone()));

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Delivered);
        let sent = channel.last_sent();
        assert_eq!(sent.body, "Hi Ada, welcome aboard.");
        assert_eq!(sent.subject.as_deref(), Some("Welcome, Ada"));
    }

    #[tokio::test]
    async fn stores_the_rendered_text_on_the_delivered_notification() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_templated_notification(&repo, recipient_id, "welcome", &[("name", "Ada")]).await;
        repo.insert_template(&Template::new(submitted.tenant_id, "welcome", "Hi {{name}}").unwrap())
            .await
            .unwrap();

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled())
            .with_templates(Arc::new(repo.clone()));

        worker.run_once(10).await.unwrap();

        let stored = repo.get_notification(submitted.id).await.unwrap();
        assert_eq!(stored.status, DeliveryStatus::Delivered);
        assert_eq!(stored.body, "Hi Ada", "the delivered record shows what was actually sent");
    }

    #[tokio::test]
    async fn fails_when_the_referenced_template_does_not_exist() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        seed_templated_notification(&repo, recipient_id, "missing", &[]).await;

        let channel = Arc::new(FakeChannel::succeeding());
        let registry = ChannelRegistry::new().register(ChannelKind::Webhook, channel.clone());
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled())
            .with_templates(Arc::new(repo.clone()));

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Failed);
        assert_eq!(channel.calls.load(Ordering::SeqCst), 0, "nothing sendable was ever built");
    }

    #[tokio::test]
    async fn fails_when_a_template_variable_is_missing() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        // The notification supplies no variables at all, but the template
        // needs one -- better to fail than to send "Hi ".
        let submitted = seed_templated_notification(&repo, recipient_id, "welcome", &[]).await;
        repo.insert_template(&Template::new(submitted.tenant_id, "welcome", "Hi {{name}}").unwrap())
            .await
            .unwrap();

        let channel = Arc::new(FakeChannel::succeeding());
        let registry = ChannelRegistry::new().register(ChannelKind::Webhook, channel.clone());
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled())
            .with_templates(Arc::new(repo.clone()));

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Failed);
        assert_eq!(channel.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn fails_a_templated_notification_when_no_template_store_is_configured() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        seed_templated_notification(&repo, recipient_id, "welcome", &[]).await;

        let channel = Arc::new(FakeChannel::succeeding());
        let registry = ChannelRegistry::new().register(ChannelKind::Webhook, channel.clone());
        // No .with_templates(..) -- an unrendered body must not go out.
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled());

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Failed);
        assert_eq!(channel.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_failed_render_does_not_consume_a_rate_limit_token() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        // First notification can't render; second is an ordinary one that
        // should still get the single available token.
        seed_templated_notification(&repo, recipient_id, "missing", &[]).await;
        let plain = seed_notification(&repo, recipient_id, 3).await;

        let channel = Arc::new(FakeChannel::succeeding());
        let registry = ChannelRegistry::new().register(ChannelKind::Webhook, channel.clone());
        let rate_limiter = RateLimiter::new().with_limit(ChannelKind::Webhook, 1, 0.0);
        let worker = Worker::new(repo.clone(), registry, rate_limiter, CircuitBreaker::disabled())
            .with_templates(Arc::new(repo.clone()));

        worker.run_once(10).await.unwrap();

        assert_eq!(channel.calls.load(Ordering::SeqCst), 1);
        assert_eq!(channel.last_sent().id, plain.id);
    }

    #[tokio::test]
    async fn untemplated_notifications_are_unaffected_by_a_template_store() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        let channel = Arc::new(FakeChannel::succeeding());
        let registry = ChannelRegistry::new().register(ChannelKind::Webhook, channel.clone());
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled())
            .with_templates(Arc::new(repo.clone()));

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Delivered);
        assert_eq!(channel.last_sent().body, submitted.body);
    }

    #[tokio::test]
    async fn a_rate_limited_notification_keeps_its_attempt_budget() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let rate_limiter = RateLimiter::new().with_limit(ChannelKind::Webhook, 0, 1.0);
        let worker = Worker::new(repo.clone(), registry, rate_limiter, CircuitBreaker::disabled());

        worker.run_once(10).await.unwrap();

        let stored = repo.get_notification(submitted.id).await.unwrap();
        assert_eq!(stored.status, DeliveryStatus::Queued);
        assert_eq!(stored.attempt_count, 0, "no provider was contacted, so no attempt was spent");
        assert!(stored.scheduled_for.is_some(), "it comes back around after a short delay");
    }

    #[tokio::test]
    async fn an_open_breaker_does_not_dead_letter_work_it_never_attempted() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        // One attempt left: under the old behaviour, being denied by the
        // breaker would have dead-lettered this outright.
        let submitted = seed_notification(&repo, recipient_id, 1).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let circuit_breaker = CircuitBreaker::new(1, Duration::seconds(60));
        circuit_breaker.record_failure(ChannelKind::Webhook, Utc::now());
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), circuit_breaker);

        let outcomes = worker.run_once(10).await.unwrap();

        assert_eq!(outcomes[0].1, DispatchOutcome::Deferred);
        let stored = repo.get_notification(submitted.id).await.unwrap();
        assert_eq!(stored.status, DeliveryStatus::Queued);
        assert_eq!(stored.attempt_count, 0);
    }

    #[tokio::test]
    async fn deferred_work_is_held_until_the_breaker_cooldown_elapses() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let circuit_breaker = CircuitBreaker::new(1, Duration::seconds(60));
        circuit_breaker.record_failure(ChannelKind::Webhook, Utc::now());
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), circuit_breaker);

        worker.run_once(10).await.unwrap();

        let stored = repo.get_notification(submitted.id).await.unwrap();
        let scheduled_for = stored.scheduled_for.expect("deferred work is scheduled");
        assert!(
            scheduled_for >= stored.updated_at + Duration::seconds(59),
            "deferral tracks the breaker's own cooldown"
        );
    }

    #[tokio::test]
    async fn a_real_send_failure_still_spends_an_attempt() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        let submitted = seed_notification(&repo, recipient_id, 3).await;

        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::failing()));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled());

        worker.run_once(10).await.unwrap();

        let stored = repo.get_notification(submitted.id).await.unwrap();
        assert_eq!(stored.attempt_count, 1, "deferral must not have softened ordinary failures");
    }

    #[tokio::test]
    async fn a_delivered_notification_is_counted_and_timed() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        seed_notification(&repo, recipient_id, 3).await;

        let metrics = Arc::new(ServiceMetrics::new());
        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled())
            .with_metrics(Arc::clone(&metrics));

        worker.run_once(10).await.unwrap();

        let text = metrics.render();
        assert!(
            text.contains("ns_notification_attempts_total{channel=\"webhook\",outcome=\"delivered\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("ns_notification_send_duration_seconds_count{channel=\"webhook\"} 1"),
            "{text}"
        );
        assert!(text.contains("ns_worker_polls_total 1"), "{text}");
        assert!(text.contains("ns_worker_claimed_total 1"), "{text}");
    }

    #[tokio::test]
    async fn a_failed_send_is_counted_as_failed_and_still_timed() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        seed_notification(&repo, recipient_id, 3).await;

        let metrics = Arc::new(ServiceMetrics::new());
        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::failing()));
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), CircuitBreaker::disabled())
            .with_metrics(Arc::clone(&metrics));

        worker.run_once(10).await.unwrap();

        let text = metrics.render();
        assert!(
            text.contains("ns_notification_attempts_total{channel=\"webhook\",outcome=\"failed\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("ns_notification_send_duration_seconds_count{channel=\"webhook\"} 1"),
            "a send that reached the provider is timed whether or not it succeeded: {text}"
        );
    }

    #[tokio::test]
    async fn a_guarded_dispatch_records_its_reason_and_no_send_duration() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        seed_notification(&repo, recipient_id, 3).await;

        let metrics = Arc::new(ServiceMetrics::new());
        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let limiter = RateLimiter::new().with_limit(ChannelKind::Webhook, 0, 0.0);
        let worker = Worker::new(repo.clone(), registry, limiter, CircuitBreaker::disabled())
            .with_metrics(Arc::clone(&metrics));

        worker.run_once(10).await.unwrap();

        let text = metrics.render();
        assert!(
            text.contains("ns_notification_skipped_total{channel=\"webhook\",reason=\"rate_limited\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("ns_notification_attempts_total{channel=\"webhook\",outcome=\"deferred\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("ns_notification_send_duration_seconds_count{channel=\"webhook\"} 0"),
            "a send that never happened must not land in the provider latency histogram: {text}"
        );
    }

    #[tokio::test]
    async fn an_open_breaker_is_a_distinct_skip_reason() {
        let (repo, _dir) = test_repo().await;
        let recipient_id = seed_recipient(&repo, ChannelKind::Webhook).await;
        seed_notification(&repo, recipient_id, 3).await;

        let metrics = Arc::new(ServiceMetrics::new());
        let registry =
            ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(FakeChannel::succeeding()));
        let breaker = CircuitBreaker::new(1, Duration::seconds(60));
        breaker.record_failure(ChannelKind::Webhook, Utc::now());
        let worker = Worker::new(repo.clone(), registry, RateLimiter::new(), breaker)
            .with_metrics(Arc::clone(&metrics));

        worker.run_once(10).await.unwrap();

        let text = metrics.render();
        assert!(
            text.contains("ns_notification_skipped_total{channel=\"webhook\",reason=\"circuit_open\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("ns_notification_skipped_total{channel=\"webhook\",reason=\"rate_limited\"} 0"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn an_idle_poll_is_still_counted() {
        let (repo, _dir) = test_repo().await;
        let metrics = Arc::new(ServiceMetrics::new());
        let worker =
            Worker::new(repo, ChannelRegistry::new(), RateLimiter::new(), CircuitBreaker::disabled())
                .with_metrics(Arc::clone(&metrics));

        worker.run_once(10).await.unwrap();

        let text = metrics.render();
        assert!(text.contains("ns_worker_polls_total 1"), "{text}");
        assert!(text.contains("ns_worker_claimed_total 0"), "{text}");
    }
}

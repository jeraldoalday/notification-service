use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use ns_core::{DeliveryStatus, Notification};
use ns_queue::NotificationQueue;
use ns_storage::{NotificationRepository, RecipientRepository, TemplateRepository};
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
}

fn outcome_from_status(status: DeliveryStatus) -> DispatchOutcome {
    match status {
        DeliveryStatus::DeadLettered => DispatchOutcome::DeadLettered,
        _ => DispatchOutcome::Failed,
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
}

impl<R: NotificationRepository + RecipientRepository + Clone> Worker<R> {
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
    /// the circuit breaker and rate limiter, looks up the registered
    /// channel and the recipient's address for it, sends, and reports the
    /// outcome back to the queue and the circuit breaker. Every
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
            let failed = self.queue.fail(notification, now).await?;
            return Ok(outcome_from_status(failed.status));
        }

        // A guard declining a send is not a failed delivery: nothing
        // reached a provider, so spending one of the notification's
        // attempts on it would let a provider outage or a sustained burst
        // dead-letter work that was never actually tried. Both guards
        // therefore defer rather than fail. The breaker's cooldown is the
        // natural delay for an open breaker -- the notification comes back
        // around about when the breaker is ready to trial one.
        if !self.circuit_breaker.allow(notification.channel, now) {
            self.queue.defer(notification, self.circuit_breaker.cooldown(), now).await?;
            return Ok(DispatchOutcome::Deferred);
        }

        if !self.rate_limiter.try_acquire(notification.channel, now) {
            self.queue.defer(notification, RATE_LIMIT_DEFER, now).await?;
            return Ok(DispatchOutcome::Deferred);
        }

        let Some(channel) = self.channels.get(notification.channel) else {
            warn!(channel = %notification.channel, "no channel registered, failing notification");
            let failed = self.queue.fail(notification, now).await?;
            return Ok(outcome_from_status(failed.status));
        };

        let recipient = match self.repo.get_recipient(notification.recipient_id).await {
            Ok(recipient) => recipient,
            Err(err) => {
                warn!(notification_id = %notification.id, error = %err, "could not load recipient");
                let failed = self.queue.fail(notification, now).await?;
                return Ok(outcome_from_status(failed.status));
            }
        };

        let address = match recipient.address_for(notification.channel) {
            Ok(address) => address,
            Err(err) => {
                warn!(notification_id = %notification.id, error = %err, "recipient missing address");
                let failed = self.queue.fail(notification, now).await?;
                return Ok(outcome_from_status(failed.status));
            }
        };

        match channel.send(&notification, address).await {
            Ok(()) => {
                self.circuit_breaker.record_success(notification.channel);
                self.queue.complete(notification, now).await?;
                Ok(DispatchOutcome::Delivered)
            }
            Err(err) => {
                self.circuit_breaker.record_failure(notification.channel, now);
                warn!(notification_id = %notification.id, error = %err, "delivery attempt failed");
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

        for notification in claimed {
            let outcome = self.dispatch_one(notification.clone(), Utc::now()).await?;
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
        let recipient = Recipient::new(Uuid::new_v4()).with_address(address).unwrap();
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
}

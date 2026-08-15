use chrono::{DateTime, Utc};
use ns_core::{DeliveryStatus, Notification};
use ns_queue::NotificationQueue;
use ns_storage::{NotificationRepository, RecipientRepository};
use tracing::{info, warn};

use crate::circuit_breaker::CircuitBreaker;
use crate::error::WorkerResult;
use crate::rate_limit::RateLimiter;
use crate::registry::ChannelRegistry;

/// What happened to a single claimed notification during one dispatch pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchOutcome {
    Delivered,
    /// The send failed (or was skipped) and the notification was
    /// requeued for another attempt after a backoff delay.
    Failed,
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
}

impl<R: NotificationRepository + RecipientRepository + Clone> Worker<R> {
    pub fn new(
        repo: R,
        channels: ChannelRegistry,
        rate_limiter: RateLimiter,
        circuit_breaker: CircuitBreaker,
    ) -> Self {
        Self { queue: NotificationQueue::new(repo.clone()), repo, channels, rate_limiter, circuit_breaker }
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
        notification: Notification,
        now: DateTime<Utc>,
    ) -> WorkerResult<DispatchOutcome> {
        if !self.circuit_breaker.allow(notification.channel, now) {
            let failed = self.queue.fail(notification, now).await?;
            return Ok(outcome_from_status(failed.status));
        }

        if !self.rate_limiter.try_acquire(notification.channel, now) {
            let failed = self.queue.fail(notification, now).await?;
            return Ok(outcome_from_status(failed.status));
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use uuid::Uuid;

    #[derive(Default)]
    struct FakeChannel {
        should_succeed: bool,
        calls: AtomicUsize,
    }

    impl FakeChannel {
        fn succeeding() -> Self {
            Self { should_succeed: true, calls: AtomicUsize::new(0) }
        }

        fn failing() -> Self {
            Self { should_succeed: false, calls: AtomicUsize::new(0) }
        }
    }

    #[async_trait]
    impl NotificationChannel for FakeChannel {
        async fn send(&self, _notification: &Notification, _address: &RecipientAddress) -> ChannelResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
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

        assert_eq!(outcomes[0].1, DispatchOutcome::Failed);
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

        assert_eq!(outcomes[0].1, DispatchOutcome::Failed);
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
        assert!(outcomes.iter().all(|(_, outcome)| *outcome == DispatchOutcome::Failed));
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
}

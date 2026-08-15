use chrono::{DateTime, Utc};
use ns_core::{DeliveryStatus, Notification};
use ns_queue::NotificationQueue;
use ns_storage::{NotificationRepository, RecipientRepository};
use tracing::{info, warn};

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
}

impl<R: NotificationRepository + RecipientRepository + Clone> Worker<R> {
    pub fn new(repo: R, channels: ChannelRegistry, rate_limiter: RateLimiter) -> Self {
        Self { queue: NotificationQueue::new(repo.clone()), repo, channels, rate_limiter }
    }

    /// Dispatches a single already-claimed (`Sending`) notification:
    /// checks the rate limiter, looks up the registered channel and the
    /// recipient's address for it, sends, and reports the outcome back to
    /// the queue. Every business-level failure (rate limited, unregistered
    /// channel, missing address, provider rejection/transport error) is
    /// routed through `NotificationQueue::fail` so it gets the same
    /// backoff/dead-letter treatment as any other failed attempt --
    /// infrastructure errors (storage/queue) instead propagate via `?` and
    /// abort the batch, since retrying those without fixing the underlying
    /// problem would just busy-loop.
    async fn dispatch_one(
        &self,
        notification: Notification,
        now: DateTime<Utc>,
    ) -> WorkerResult<DispatchOutcome> {
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
                self.queue.complete(notification, now).await?;
                Ok(DispatchOutcome::Delivered)
            }
            Err(err) => {
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

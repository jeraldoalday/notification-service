use chrono::{DateTime, Utc};
use ns_core::{DeliveryStatus, Notification};
use ns_storage::NotificationRepository;
use uuid::Uuid;

use crate::backoff;
use crate::error::{QueueError, QueueResult};

/// Durable job queue built on top of a `NotificationRepository`: submitting
/// new work, claiming due work for processing without double-dispatching
/// it, and reporting outcomes back (completion, or failure with retry
/// backoff / dead-lettering). Priority ordering and delayed/scheduled
/// delivery are handled by the repository's `due_for_delivery` query --
/// this type is concerned only with the state transitions around it.
pub struct NotificationQueue<R> {
    repo: R,
}

impl<R: NotificationRepository> NotificationQueue<R> {
    pub fn new(repo: R) -> Self {
        Self { repo }
    }

    /// Moves a freshly-built (`Pending`) notification into the queue and
    /// persists it.
    pub async fn submit(
        &self,
        mut notification: Notification,
        now: DateTime<Utc>,
    ) -> QueueResult<Notification> {
        notification.transition(DeliveryStatus::Queued, now)?;
        self.repo.insert_notification(&notification).await?;
        Ok(notification)
    }

    /// Claims up to `limit` due notifications for processing, moving each
    /// to `Sending`. Fetches candidates via `due_for_delivery` and then
    /// claims each one with a conditional write, so if another worker
    /// (using a separate `NotificationQueue` over the same storage) claims
    /// a candidate first, this call simply skips it rather than
    /// double-dispatching -- the returned `Vec` may be shorter than
    /// `limit` even when more due work exists.
    pub async fn claim_batch(&self, now: DateTime<Utc>, limit: u32) -> QueueResult<Vec<Notification>> {
        let candidates = self.repo.due_for_delivery(now, limit).await?;
        let mut claimed = Vec::with_capacity(candidates.len());

        for mut notification in candidates {
            let expected_status = notification.status;
            notification.transition(DeliveryStatus::Sending, now)?;
            if self.repo.try_update_notification(&notification, expected_status).await? {
                claimed.push(notification);
            }
        }

        Ok(claimed)
    }

    /// Marks a `Sending` notification `Delivered`.
    pub async fn complete(
        &self,
        mut notification: Notification,
        now: DateTime<Utc>,
    ) -> QueueResult<Notification> {
        notification.transition(DeliveryStatus::Delivered, now)?;
        self.repo.update_notification(&notification).await?;
        Ok(notification)
    }

    /// Reports a failed delivery attempt. Records the attempt, then either
    /// reschedules the notification back onto the queue after a backoff
    /// delay (if attempts remain) or dead-letters it (if this was the
    /// last one).
    pub async fn fail(
        &self,
        mut notification: Notification,
        now: DateTime<Utc>,
    ) -> QueueResult<Notification> {
        notification.record_attempt(now);
        notification.transition(DeliveryStatus::Failed, now)?;

        if notification.has_attempts_remaining() {
            notification.scheduled_for = Some(now + backoff::delay(notification.attempt_count));
            notification.transition(DeliveryStatus::Queued, now)?;
        } else {
            notification.transition(DeliveryStatus::DeadLettered, now)?;
        }

        self.repo.update_notification(&notification).await?;
        Ok(notification)
    }

    /// Re-queues work that dead-lettered, as a *new* notification cloned
    /// from the old one.
    ///
    /// It has to be a new notification rather than a revival of the old
    /// row: `DeliveryStatus` is one-directional and `DeadLettered` is
    /// terminal, so there is no legal path back to `Queued`. Cloning keeps
    /// that guarantee intact and leaves the original row as the record of
    /// what happened -- which attempts were made, when it gave up -- while
    /// the replay starts with a clean attempt count and no leftover
    /// backoff schedule.
    ///
    /// Only dead-lettered notifications can be replayed: replaying
    /// something that was cancelled would undo a deliberate decision, and
    /// replaying live work would double-send it.
    pub async fn replay(&self, notification: &Notification, now: DateTime<Utc>) -> QueueResult<Notification> {
        if notification.status != DeliveryStatus::DeadLettered {
            return Err(QueueError::NotReplayable { id: notification.id, status: notification.status });
        }

        let mut replayed = notification.clone();
        replayed.id = Uuid::new_v4();
        replayed.status = DeliveryStatus::default();
        replayed.attempt_count = 0;
        replayed.scheduled_for = None;
        replayed.created_at = now;
        replayed.updated_at = now;

        self.submit(replayed, now).await
    }

    /// Replays up to `limit` of a tenant's dead-lettered notifications,
    /// oldest work last (the repository lists newest-first). Returns the
    /// newly queued copies.
    pub async fn replay_dead_lettered(
        &self,
        tenant_id: Uuid,
        limit: u32,
        now: DateTime<Utc>,
    ) -> QueueResult<Vec<Notification>> {
        let dead_lettered =
            self.repo.list_by_tenant(tenant_id, Some(DeliveryStatus::DeadLettered), limit).await?;

        let mut replayed = Vec::with_capacity(dead_lettered.len());
        for notification in &dead_lettered {
            replayed.push(self.replay(notification, now).await?);
        }
        Ok(replayed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ns_core::{ChannelKind, Notification};
    use ns_storage::SqliteStorage;
    use uuid::Uuid;

    async fn test_queue() -> (NotificationQueue<SqliteStorage>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let storage = SqliteStorage::connect(&format!("sqlite://{}", db_path.display())).await.unwrap();
        (NotificationQueue::new(storage), dir)
    }

    fn sample_notification() -> Notification {
        Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .body("Hello there")
            .max_attempts(2)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn submit_queues_a_pending_notification() {
        let (queue, _dir) = test_queue().await;
        let notification = sample_notification();

        let submitted = queue.submit(notification, Utc::now()).await.unwrap();

        assert_eq!(submitted.status, DeliveryStatus::Queued);
    }

    #[tokio::test]
    async fn claim_batch_moves_due_notifications_to_sending() {
        let (queue, _dir) = test_queue().await;
        let notification = queue.submit(sample_notification(), Utc::now()).await.unwrap();

        let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap();

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, notification.id);
        assert_eq!(claimed[0].status, DeliveryStatus::Sending);
    }

    #[tokio::test]
    async fn claim_batch_does_not_reclaim_already_sending_work() {
        let (queue, _dir) = test_queue().await;
        queue.submit(sample_notification(), Utc::now()).await.unwrap();
        queue.claim_batch(Utc::now(), 10).await.unwrap();

        let second_claim = queue.claim_batch(Utc::now(), 10).await.unwrap();

        assert!(second_claim.is_empty());
    }

    #[tokio::test]
    async fn complete_marks_delivered() {
        let (queue, _dir) = test_queue().await;
        queue.submit(sample_notification(), Utc::now()).await.unwrap();
        let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap().remove(0);

        let completed = queue.complete(claimed, Utc::now()).await.unwrap();

        assert_eq!(completed.status, DeliveryStatus::Delivered);
    }

    #[tokio::test]
    async fn fail_requeues_with_backoff_when_attempts_remain() {
        let (queue, _dir) = test_queue().await;
        queue.submit(sample_notification(), Utc::now()).await.unwrap();
        let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap().remove(0);
        let now = Utc::now();

        let failed = queue.fail(claimed, now).await.unwrap();

        assert_eq!(failed.status, DeliveryStatus::Queued);
        assert_eq!(failed.attempt_count, 1);
        assert!(failed.scheduled_for.unwrap() > now);
    }

    #[tokio::test]
    async fn fail_dead_letters_once_attempts_are_exhausted() {
        let (queue, _dir) = test_queue().await;
        // max_attempts(2): the first failure requeues, the second exhausts.
        queue.submit(sample_notification(), Utc::now()).await.unwrap();

        let first_claim = queue.claim_batch(Utc::now(), 10).await.unwrap().remove(0);
        let requeued = queue.fail(first_claim, Utc::now()).await.unwrap();

        let second_claim = queue.claim_batch(requeued.scheduled_for.unwrap(), 10).await.unwrap().remove(0);
        let dead_lettered = queue.fail(second_claim, Utc::now()).await.unwrap();

        assert_eq!(dead_lettered.status, DeliveryStatus::DeadLettered);
        assert_eq!(dead_lettered.attempt_count, 2);
    }

    /// Drives a notification all the way to `DeadLettered` through the
    /// ordinary failure path, so replay is tested against a row the queue
    /// itself produced rather than a hand-built one.
    async fn dead_letter(queue: &NotificationQueue<SqliteStorage>) -> Notification {
        let submitted = queue.submit(sample_notification(), Utc::now()).await.unwrap();
        let mut current = submitted;
        loop {
            let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap();
            let claimed = claimed.into_iter().find(|n| n.id == current.id).expect("claimable");
            current = queue.fail(claimed, Utc::now()).await.unwrap();
            if current.status == DeliveryStatus::DeadLettered {
                return current;
            }
            // The retry was scheduled behind a backoff delay; clear it so
            // the next claim_batch picks it straight back up.
            current.scheduled_for = None;
            queue.repo.update_notification(&current).await.unwrap();
        }
    }

    #[tokio::test]
    async fn replay_queues_a_fresh_copy_of_dead_lettered_work() {
        let (queue, _dir) = test_queue().await;
        let dead = dead_letter(&queue).await;

        let replayed = queue.replay(&dead, Utc::now()).await.unwrap();

        assert_ne!(replayed.id, dead.id, "replay creates a successor, not a revival");
        assert_eq!(replayed.status, DeliveryStatus::Queued);
        assert_eq!(replayed.attempt_count, 0);
        assert_eq!(replayed.scheduled_for, None);
        assert_eq!(replayed.body, dead.body);
        assert_eq!(replayed.tenant_id, dead.tenant_id);
        assert_eq!(replayed.recipient_id, dead.recipient_id);
    }

    #[tokio::test]
    async fn replay_leaves_the_original_dead_lettered() {
        let (queue, _dir) = test_queue().await;
        let dead = dead_letter(&queue).await;

        queue.replay(&dead, Utc::now()).await.unwrap();

        let stored = queue.repo.get_notification(dead.id).await.unwrap();
        assert_eq!(stored.status, DeliveryStatus::DeadLettered, "the record of what happened survives");
        assert_eq!(stored.attempt_count, dead.attempt_count);
    }

    #[tokio::test]
    async fn replayed_work_is_claimable_again() {
        let (queue, _dir) = test_queue().await;
        let dead = dead_letter(&queue).await;

        let replayed = queue.replay(&dead, Utc::now()).await.unwrap();
        let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap();

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, replayed.id);
    }

    #[tokio::test]
    async fn replaying_live_work_is_rejected() {
        let (queue, _dir) = test_queue().await;
        let queued = queue.submit(sample_notification(), Utc::now()).await.unwrap();

        let err = queue.replay(&queued, Utc::now()).await.unwrap_err();

        assert!(matches!(err, QueueError::NotReplayable { .. }));
    }

    #[tokio::test]
    async fn replaying_a_cancelled_notification_is_rejected() {
        let (queue, _dir) = test_queue().await;
        let mut notification = sample_notification();
        notification.transition(DeliveryStatus::Cancelled, Utc::now()).unwrap();

        let err = queue.replay(&notification, Utc::now()).await.unwrap_err();

        assert!(matches!(err, QueueError::NotReplayable { .. }), "cancelling was a deliberate decision");
    }

    #[tokio::test]
    async fn replay_dead_lettered_is_tenant_scoped_and_limited() {
        let (queue, _dir) = test_queue().await;
        let first = dead_letter(&queue).await;
        let second = dead_letter(&queue).await;
        assert_ne!(first.tenant_id, second.tenant_id, "sample notifications get distinct tenants");

        let replayed = queue.replay_dead_lettered(first.tenant_id, 10, Utc::now()).await.unwrap();

        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].tenant_id, first.tenant_id);
    }

    #[tokio::test]
    async fn replay_dead_lettered_finds_nothing_when_the_tenant_is_clean() {
        let (queue, _dir) = test_queue().await;
        queue.submit(sample_notification(), Utc::now()).await.unwrap();

        let replayed = queue.replay_dead_lettered(Uuid::new_v4(), 10, Utc::now()).await.unwrap();

        assert!(replayed.is_empty());
    }
}

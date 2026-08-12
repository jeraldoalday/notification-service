use chrono::{DateTime, Utc};
use ns_core::{DeliveryStatus, Notification};
use ns_storage::NotificationRepository;

use crate::backoff;
use crate::error::QueueResult;

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
}

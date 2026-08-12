use chrono::{DateTime, Utc};
use ns_core::{DeliveryStatus, Notification};
use ns_storage::NotificationRepository;

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
}

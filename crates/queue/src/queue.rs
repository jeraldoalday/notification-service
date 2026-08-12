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
}

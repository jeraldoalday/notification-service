use std::sync::Arc;

use ns_queue::NotificationQueue;
use ns_storage::{NotificationRepository, RecipientRepository, TemplateRepository};

/// Shared state handed to every request handler: a `NotificationQueue` for
/// the submit/claim/complete/fail lifecycle, plus direct access to the
/// underlying repository for reads (`get_notification`, `get_recipient`,
/// ...) that don't go through queue semantics. `NotificationQueue` doesn't
/// implement `Clone` itself, so it's wrapped in an `Arc` here rather than
/// adding a `Clone` bound to every caller of `ns-queue`.
pub struct AppState<R> {
    pub queue: Arc<NotificationQueue<R>>,
    pub repo: R,
}

impl<R> Clone for AppState<R>
where
    R: Clone,
{
    fn clone(&self) -> Self {
        Self { queue: Arc::clone(&self.queue), repo: self.repo.clone() }
    }
}

impl<R> AppState<R>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone,
{
    pub fn new(repo: R) -> Self {
        Self { queue: Arc::new(NotificationQueue::new(repo.clone())), repo }
    }
}

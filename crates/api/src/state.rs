use std::sync::Arc;

use ns_metrics::ServiceMetrics;
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
    /// Shared rather than owned so that a process running the API and the
    /// worker together exports one set of counters covering both, instead
    /// of two disjoint halves of the same story.
    pub metrics: Arc<ServiceMetrics>,
}

impl<R> Clone for AppState<R>
where
    R: Clone,
{
    fn clone(&self) -> Self {
        Self { queue: Arc::clone(&self.queue), repo: self.repo.clone(), metrics: Arc::clone(&self.metrics) }
    }
}

impl<R> AppState<R>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone,
{
    pub fn new(repo: R) -> Self {
        Self::with_metrics(repo, Arc::new(ServiceMetrics::new()))
    }

    /// Builds state over a metric set the caller already owns, for a
    /// process that also runs a `ns_worker::Worker` and wants both halves
    /// reporting into the same registry.
    pub fn with_metrics(repo: R, metrics: Arc<ServiceMetrics>) -> Self {
        Self { queue: Arc::new(NotificationQueue::new(repo.clone())), repo, metrics }
    }
}

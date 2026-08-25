use std::sync::Arc;

use ns_metrics::ServiceMetrics;
use ns_queue::NotificationQueue;
use ns_storage::{
    ApiKeyRepository, DeliveryAttemptRepository, NotificationRepository, RecipientRepository,
    TemplateRepository,
};

use crate::auth::AuthMode;

/// Every repository the API needs, as a single bound.
///
/// `AppState` is generic over its storage so the router can be built over
/// a test double as easily as over `SqliteStorage`, but that genericity
/// was being paid for in full at every one of the fifteen-odd handlers:
/// each repeated the same list of traits, and adding one meant editing all
/// of them. Collapsing the list into one trait keeps the flexibility and
/// puts the list in a single place.
///
/// It is a blanket-implemented marker rather than a trait anyone writes an
/// impl for -- any type satisfying the parts satisfies the whole,
/// automatically.
pub trait Repositories:
    NotificationRepository
    + RecipientRepository
    + TemplateRepository
    + ApiKeyRepository
    + DeliveryAttemptRepository
    + Clone
    + Send
    + Sync
    + 'static
{
}

impl<R> Repositories for R where
    R: NotificationRepository
        + RecipientRepository
        + TemplateRepository
        + ApiKeyRepository
        + DeliveryAttemptRepository
        + Clone
        + Send
        + Sync
        + 'static
{
}

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
    /// Whether credentials are checked. Held on the state rather than
    /// baked into the router so that the same `build_router` serves both
    /// modes and the choice stays visible at the one call site that makes
    /// it.
    pub auth: AuthMode,
}

impl<R> Clone for AppState<R>
where
    R: Clone,
{
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            repo: self.repo.clone(),
            metrics: Arc::clone(&self.metrics),
            auth: self.auth,
        }
    }
}

impl<R> AppState<R>
where
    R: Repositories,
{
    /// State with authentication required, which is what a deployment
    /// should want; `without_auth` is the deliberate opt-out.
    pub fn new(repo: R) -> Self {
        Self::with_metrics(repo, Arc::new(ServiceMetrics::new()))
    }

    /// Builds state over a metric set the caller already owns, for a
    /// process that also runs a `ns_worker::Worker` and wants both halves
    /// reporting into the same registry.
    pub fn with_metrics(repo: R, metrics: Arc<ServiceMetrics>) -> Self {
        Self {
            queue: Arc::new(NotificationQueue::new(repo.clone())),
            repo,
            metrics,
            auth: AuthMode::Required,
        }
    }

    /// Turns credential checking off. Only safe where the network already
    /// decides who can reach the service, and named so that a reader of
    /// the call site can see that's what was chosen.
    pub fn without_auth(mut self) -> Self {
        self.auth = AuthMode::Disabled;
        self
    }
}

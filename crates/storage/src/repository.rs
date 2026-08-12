use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ns_core::{DeliveryStatus, Notification, Recipient};
use uuid::Uuid;

use crate::error::StorageResult;

/// Persistence for `Notification`s. Implementations are responsible for
/// durability and for the `due_for_delivery` query the worker polls on --
/// everything else about queueing semantics (priority ordering, batching)
/// lives here rather than in `ns-queue` so a single index can serve it.
// Methods are named per-entity (`insert_notification`, not `insert`) even
// though each trait only ever holds one entity type: `SqliteStorage`
// implements both traits at once, and identically-named async trait
// methods on the same concrete type are ambiguous to call without
// fully-qualified syntax. Distinct names keep direct calls on a concrete
// storage ergonomic; generic code written against `impl NotificationRepository`
// is unaffected either way.
#[async_trait]
pub trait NotificationRepository: Send + Sync {
    async fn insert_notification(&self, notification: &Notification) -> StorageResult<()>;

    async fn get_notification(&self, id: Uuid) -> StorageResult<Notification>;

    /// Persists the full current state of `notification`, keyed by its id.
    /// Callers are expected to have already validated any status
    /// transition via `Notification::transition` -- this just saves it.
    async fn update_notification(&self, notification: &Notification) -> StorageResult<()>;

    /// Like `update_notification`, but only writes if the row's current
    /// status still matches `expected_status`. This is the primitive
    /// `ns-queue` builds dequeue-for-processing on: multiple workers can
    /// race on the same `due_for_delivery` result set, and only the one
    /// whose write lands while the row is still in the expected status
    /// wins the claim. Returns `true` if the write happened, `false` if
    /// another caller had already moved the row on.
    async fn try_update_notification(
        &self,
        notification: &Notification,
        expected_status: DeliveryStatus,
    ) -> StorageResult<bool>;

    /// Notifications in `Queued` status whose `scheduled_for` has passed
    /// (or is unset), ordered highest-priority-first and oldest-first
    /// within a priority tier, capped at `limit`.
    async fn due_for_delivery(&self, now: DateTime<Utc>, limit: u32) -> StorageResult<Vec<Notification>>;
}

/// Persistence for `Recipient`s.
#[async_trait]
pub trait RecipientRepository: Send + Sync {
    async fn insert_recipient(&self, recipient: &Recipient) -> StorageResult<()>;

    async fn get_recipient(&self, id: Uuid) -> StorageResult<Recipient>;
}

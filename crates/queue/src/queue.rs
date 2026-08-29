use chrono::{DateTime, Utc};
use ns_core::{DeliveryStatus, Notification};
use ns_storage::{NotificationRepository, StorageError};
use uuid::Uuid;

use crate::backoff;
use crate::error::{QueueError, QueueResult};

/// What an idempotent submission did.
///
/// Two variants rather than a bare `Notification` plus a bool, because
/// the caller almost always has to act on the difference: an HTTP API
/// answers 201 for one and 200 for the other, a CLI says "queued" or
/// "already queued", and a metric counts them apart. A bool alongside the
/// value is the same information in a shape that is easier to ignore.
#[derive(Debug, Clone)]
pub enum Submission {
    /// Nothing had used this key, so the notification is now on the queue.
    Created(Notification),
    /// The key had already been used, and this is the notification it
    /// created. Nothing was queued by this call and nothing extra will be
    /// sent -- which is the entire point.
    Duplicate(Notification),
}

impl Submission {
    pub fn notification(&self) -> &Notification {
        match self {
            Submission::Created(notification) | Submission::Duplicate(notification) => notification,
        }
    }

    pub fn into_notification(self) -> Notification {
        match self {
            Submission::Created(notification) | Submission::Duplicate(notification) => notification,
        }
    }

    pub fn is_duplicate(&self) -> bool {
        matches!(self, Submission::Duplicate(_))
    }
}

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

    /// Submits a notification, honouring its idempotency key if it has
    /// one.
    ///
    /// A notification with no key is submitted exactly as `submit` would:
    /// the caller did not ask for deduplication and does not pay for a
    /// lookup it never wanted.
    ///
    /// With a key, the flow is look-then-insert, and the insert is the
    /// authority. The lookup is the common path -- a retry usually arrives
    /// long after the original, and answering it with one indexed read
    /// beats provoking a constraint violation. But two of a client's own
    /// retries can also arrive concurrently, which is exactly the
    /// situation retries exist for, and then both lookups miss and both
    /// inserts run. The unique index refuses the second, and the loser
    /// reads the winner's notification back rather than failing: from the
    /// client's side, two racing retries of one request must not be
    /// distinguishable from two sequential ones.
    pub async fn submit_idempotent(
        &self,
        notification: Notification,
        now: DateTime<Utc>,
    ) -> QueueResult<Submission> {
        let Some(key) = notification.idempotency_key.clone() else {
            return Ok(Submission::Created(self.submit(notification, now).await?));
        };
        let tenant_id = notification.tenant_id;

        if let Some(existing) = self.repo.find_by_idempotency_key(tenant_id, &key).await? {
            return Ok(Submission::Duplicate(existing));
        }

        match self.submit(notification, now).await {
            Ok(submitted) => Ok(Submission::Created(submitted)),
            Err(QueueError::Storage(StorageError::DuplicateIdempotencyKey { .. })) => {
                match self.repo.find_by_idempotency_key(tenant_id, &key).await? {
                    Some(existing) => Ok(Submission::Duplicate(existing)),
                    // The index says the key is taken and the lookup says
                    // it isn't. Nothing in this service deletes
                    // notifications, so this is not a state it can reach
                    // on its own -- reporting the constraint violation is
                    // better than pretending to a caller that its message
                    // was queued when no row for it exists.
                    None => Err(QueueError::Storage(StorageError::DuplicateIdempotencyKey {
                        tenant_id,
                        key: key.to_string(),
                    })),
                }
            }
            Err(err) => Err(err),
        }
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

    /// Puts a claimed (`Sending`) notification back on the queue after
    /// `retry_after`, *without* recording a delivery attempt.
    ///
    /// This is for the cases where the worker decided not to try at all --
    /// the channel's rate limit is exhausted, or its circuit breaker is
    /// open. Reporting those through `fail` would spend one of the
    /// notification's `max_attempts` on an attempt that never happened, so
    /// a long provider outage or a sustained burst would dead-letter
    /// perfectly good work that no provider ever saw. Deferring instead
    /// leaves the attempt budget for actual delivery attempts.
    ///
    /// The status still travels `Sending` -> `Failed` -> `Queued`, because
    /// that's the only way out of `Sending` that `DeliveryStatus` allows;
    /// what changes is that `attempt_count` is untouched.
    pub async fn defer(
        &self,
        mut notification: Notification,
        retry_after: chrono::Duration,
        now: DateTime<Utc>,
    ) -> QueueResult<Notification> {
        notification.transition(DeliveryStatus::Failed, now)?;
        notification.scheduled_for = Some(now + retry_after);
        notification.transition(DeliveryStatus::Queued, now)?;

        self.repo.update_notification(&notification).await?;
        Ok(notification)
    }

    /// Ends a claimed (`Sending`) notification because the recipient's
    /// address is on the tenant's suppression list.
    ///
    /// Terminal and one-way, unlike `fail`: there is no attempt to record
    /// because nothing was attempted, no backoff because retrying would
    /// reach the same suppressed address, and no dead-lettering because
    /// dead-lettered work is replayable and replaying this would send to
    /// someone who asked not to be sent to. `attempt_count` is left
    /// untouched for the same reason it is in `defer` -- no provider saw
    /// this message.
    pub async fn suppress(
        &self,
        mut notification: Notification,
        now: DateTime<Utc>,
    ) -> QueueResult<Notification> {
        notification.transition(DeliveryStatus::Suppressed, now)?;
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
    ///
    /// The copy carries no idempotency key. That key named the caller's
    /// submission; a replay is the operator's decision to send the
    /// message again, which is the one thing an idempotency key exists to
    /// prevent happening by accident.
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
        // The key named the *caller's* submission, and this is not one:
        // an operator replaying dead-lettered work is deliberately asking
        // for the message to go out again. Carrying the key over would
        // collide with the original on the unique index and fail the
        // replay outright -- and if it somehow didn't, it would leave two
        // rows answering to a key that is supposed to identify exactly
        // one.
        replayed.idempotency_key = None;

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

    fn keyed_notification(tenant_id: Uuid, key: &str) -> Notification {
        Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email)
            .body("Hello there")
            .max_attempts(2)
            .idempotency_key(ns_core::IdempotencyKey::new(key).unwrap())
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn an_unkeyed_submission_is_never_deduplicated() {
        let (queue, _dir) = test_queue().await;

        let first = queue.submit_idempotent(sample_notification(), Utc::now()).await.unwrap();
        let second = queue.submit_idempotent(sample_notification(), Utc::now()).await.unwrap();

        assert!(!first.is_duplicate());
        assert!(!second.is_duplicate(), "no key means no claim that these are the same message");
        assert_ne!(first.notification().id, second.notification().id);
    }

    #[tokio::test]
    async fn a_first_keyed_submission_is_queued_normally() {
        let (queue, _dir) = test_queue().await;

        let submission = queue.submit_idempotent(keyed_notification(Uuid::new_v4(), "k1"), Utc::now()).await;

        let submission = submission.unwrap();
        assert!(!submission.is_duplicate());
        assert_eq!(submission.notification().status, DeliveryStatus::Queued);
    }

    #[tokio::test]
    async fn a_retry_resolves_to_the_notification_the_first_attempt_created() {
        let (queue, _dir) = test_queue().await;
        let tenant_id = Uuid::new_v4();
        let first =
            queue.submit_idempotent(keyed_notification(tenant_id, "order-1041"), Utc::now()).await.unwrap();

        let retry =
            queue.submit_idempotent(keyed_notification(tenant_id, "order-1041"), Utc::now()).await.unwrap();

        assert!(retry.is_duplicate());
        assert_eq!(retry.notification().id, first.notification().id);
    }

    #[tokio::test]
    async fn a_deduplicated_retry_queues_nothing_extra() {
        let (queue, _dir) = test_queue().await;
        let tenant_id = Uuid::new_v4();
        queue.submit_idempotent(keyed_notification(tenant_id, "order-1041"), Utc::now()).await.unwrap();
        queue.submit_idempotent(keyed_notification(tenant_id, "order-1041"), Utc::now()).await.unwrap();

        // The whole point: one message reaches the recipient, not two.
        let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap();
        assert_eq!(claimed.len(), 1);
    }

    #[tokio::test]
    async fn a_key_deduplicates_only_within_its_own_tenant() {
        let (queue, _dir) = test_queue().await;
        let mine = queue.submit_idempotent(keyed_notification(Uuid::new_v4(), "k1"), Utc::now()).await;
        let theirs = queue.submit_idempotent(keyed_notification(Uuid::new_v4(), "k1"), Utc::now()).await;

        assert!(!mine.unwrap().is_duplicate());
        assert!(!theirs.unwrap().is_duplicate(), "two tenants picking the same string is a coincidence");
    }

    #[tokio::test]
    async fn a_retry_arriving_after_delivery_still_deduplicates() {
        let (queue, _dir) = test_queue().await;
        let tenant_id = Uuid::new_v4();
        let first =
            queue.submit_idempotent(keyed_notification(tenant_id, "order-1041"), Utc::now()).await.unwrap();
        let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap().remove(0);
        queue.complete(claimed, Utc::now()).await.unwrap();

        // A client that never saw the response retries whenever it comes
        // back up, which may be long after the message went out.
        let retry =
            queue.submit_idempotent(keyed_notification(tenant_id, "order-1041"), Utc::now()).await.unwrap();

        assert!(retry.is_duplicate());
        assert_eq!(retry.notification().id, first.notification().id);
        assert_eq!(retry.notification().status, DeliveryStatus::Delivered, "and reports what became of it");
    }

    #[tokio::test]
    async fn concurrent_retries_of_one_request_send_once() {
        let (queue, _dir) = test_queue().await;
        let tenant_id = Uuid::new_v4();
        let now = Utc::now();

        // Both lookups miss, both inserts run, and the unique index
        // decides. Two racing retries must not be distinguishable, from
        // the client's side, from two sequential ones.
        let (first, second) = tokio::join!(
            queue.submit_idempotent(keyed_notification(tenant_id, "order-1041"), now),
            queue.submit_idempotent(keyed_notification(tenant_id, "order-1041"), now),
        );

        let (first, second) = (first.unwrap(), second.unwrap());
        assert_eq!(first.notification().id, second.notification().id, "one notification, two answers");
        assert!(first.is_duplicate() ^ second.is_duplicate(), "exactly one call created it");
        assert_eq!(queue.claim_batch(now, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn replaying_keyed_work_drops_the_key() {
        let (queue, _dir) = test_queue().await;
        let tenant_id = Uuid::new_v4();
        let dead = dead_letter_keyed(&queue, tenant_id, "order-1041").await;
        assert!(dead.idempotency_key.is_some(), "the original kept the key it was submitted under");

        let replayed = queue.replay(&dead, Utc::now()).await.unwrap();

        // A replay is the operator deciding to send again, which is the
        // one thing the key exists to stop happening by accident -- and
        // carrying it over would collide on the unique index besides.
        assert_eq!(replayed.idempotency_key, None);
        assert_eq!(replayed.status, DeliveryStatus::Queued);
    }

    #[tokio::test]
    async fn a_replayed_notification_can_be_replayed_again() {
        let (queue, _dir) = test_queue().await;
        let tenant_id = Uuid::new_v4();
        let dead = dead_letter_keyed(&queue, tenant_id, "order-1041").await;

        queue.replay(&dead, Utc::now()).await.unwrap();
        // Replaying the same dead-lettered original twice would trip the
        // unique index if the key travelled with the copies.
        queue.replay(&dead, Utc::now()).await.unwrap();

        assert_eq!(queue.claim_batch(Utc::now(), 10).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn suppress_ends_a_claimed_notification_terminally() {
        let (queue, _dir) = test_queue().await;
        queue.submit(sample_notification(), Utc::now()).await.unwrap();
        let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap().remove(0);

        let suppressed = queue.suppress(claimed, Utc::now()).await.unwrap();

        assert_eq!(suppressed.status, DeliveryStatus::Suppressed);
        assert!(suppressed.status.is_terminal());
    }

    #[tokio::test]
    async fn suppress_spends_no_attempt_and_schedules_no_retry() {
        let (queue, _dir) = test_queue().await;
        queue.submit(sample_notification(), Utc::now()).await.unwrap();
        let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap().remove(0);

        let suppressed = queue.suppress(claimed, Utc::now()).await.unwrap();

        assert_eq!(suppressed.attempt_count, 0, "no provider ever saw this message");
        assert_eq!(suppressed.scheduled_for, None);
    }

    #[tokio::test]
    async fn a_suppressed_notification_is_never_claimed_again() {
        let (queue, _dir) = test_queue().await;
        queue.submit(sample_notification(), Utc::now()).await.unwrap();
        let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap().remove(0);
        queue.suppress(claimed, Utc::now()).await.unwrap();

        assert!(queue.claim_batch(Utc::now(), 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_suppressed_notification_cannot_be_replayed() {
        let (queue, _dir) = test_queue().await;
        queue.submit(sample_notification(), Utc::now()).await.unwrap();
        let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap().remove(0);
        let suppressed = queue.suppress(claimed, Utc::now()).await.unwrap();

        // The point of not folding suppression into dead-lettering:
        // replaying would send to the address that asked not to be sent to.
        let err = queue.replay(&suppressed, Utc::now()).await.unwrap_err();
        assert!(matches!(err, QueueError::NotReplayable { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn suppressing_unclaimed_work_is_rejected_by_the_state_machine() {
        let (queue, _dir) = test_queue().await;
        let queued = queue.submit(sample_notification(), Utc::now()).await.unwrap();

        let err = queue.suppress(queued, Utc::now()).await.unwrap_err();

        assert!(matches!(err, QueueError::Core(_)), "{err:?}");
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

    /// The same, for a notification submitted under an idempotency key.
    async fn dead_letter_keyed(
        queue: &NotificationQueue<SqliteStorage>,
        tenant_id: Uuid,
        key: &str,
    ) -> Notification {
        let submitted = queue
            .submit_idempotent(keyed_notification(tenant_id, key), Utc::now())
            .await
            .unwrap()
            .into_notification();
        let mut current = submitted;
        loop {
            let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap();
            let claimed = claimed.into_iter().find(|n| n.id == current.id).expect("claimable");
            current = queue.fail(claimed, Utc::now()).await.unwrap();
            if current.status == DeliveryStatus::DeadLettered {
                return current;
            }
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

    #[tokio::test]
    async fn defer_requeues_without_spending_an_attempt() {
        let (queue, _dir) = test_queue().await;
        queue.submit(sample_notification(), Utc::now()).await.unwrap();
        let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap().remove(0);
        let now = Utc::now();

        let deferred = queue.defer(claimed, chrono::Duration::seconds(30), now).await.unwrap();

        assert_eq!(deferred.status, DeliveryStatus::Queued);
        assert_eq!(deferred.attempt_count, 0, "nothing was attempted, so nothing was spent");
        assert_eq!(deferred.scheduled_for, Some(now + chrono::Duration::seconds(30)));
    }

    #[tokio::test]
    async fn deferred_work_is_claimable_again_once_its_delay_passes() {
        let (queue, _dir) = test_queue().await;
        queue.submit(sample_notification(), Utc::now()).await.unwrap();
        let claimed = queue.claim_batch(Utc::now(), 10).await.unwrap().remove(0);
        let now = Utc::now();
        let deferred = queue.defer(claimed, chrono::Duration::seconds(30), now).await.unwrap();

        assert!(queue.claim_batch(now, 10).await.unwrap().is_empty(), "still behind its delay");

        let later = queue.claim_batch(now + chrono::Duration::seconds(31), 10).await.unwrap();
        assert_eq!(later.len(), 1);
        assert_eq!(later[0].id, deferred.id);
    }

    #[tokio::test]
    async fn deferring_repeatedly_never_dead_letters() {
        let (queue, _dir) = test_queue().await;
        // max_attempts is 2 for sample notifications; deferring far more
        // times than that must not exhaust them.
        queue.submit(sample_notification(), Utc::now()).await.unwrap();
        let mut now = Utc::now();

        for _ in 0..5 {
            let claimed = queue.claim_batch(now, 10).await.unwrap().remove(0);
            let deferred = queue.defer(claimed, chrono::Duration::seconds(1), now).await.unwrap();
            assert_eq!(deferred.status, DeliveryStatus::Queued);
            now += chrono::Duration::seconds(2);
        }

        let claimed = queue.claim_batch(now, 10).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].attempt_count, 0);
    }

    #[tokio::test]
    async fn deferring_work_that_was_not_claimed_is_rejected() {
        let (queue, _dir) = test_queue().await;
        let queued = queue.submit(sample_notification(), Utc::now()).await.unwrap();

        let err = queue.defer(queued, chrono::Duration::seconds(1), Utc::now()).await.unwrap_err();

        assert!(matches!(err, QueueError::Core(_)), "only Sending work can be deferred");
    }
}

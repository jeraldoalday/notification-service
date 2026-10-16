use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ns_auth::ApiKey;
use ns_core::{
    DeliveryAttempt, DeliveryStatus, IdempotencyKey, Notification, Preference, PreferenceSet, Recipient,
    RecipientAddress, Suppression, Topic,
};
use ns_templates::Template;
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

    /// Notifications belonging to `tenant_id`, optionally narrowed to a
    /// single `status`, newest-first, capped at `limit`.
    ///
    /// Backed by `idx_notifications_tenant` -- or by
    /// `idx_notifications_tenant_status` when a status filter is supplied.
    /// Both carry `created_at DESC`, so the newest-first ordering is read
    /// straight off the index rather than sorted afterwards: without that,
    /// answering a fifty-row page costs work proportional to everything
    /// the tenant has ever sent.
    async fn list_by_tenant(
        &self,
        tenant_id: Uuid,
        status: Option<DeliveryStatus>,
        limit: u32,
    ) -> StorageResult<Vec<Notification>>;

    /// The notification `tenant_id` created under `key`, or `None`.
    ///
    /// Scoped to a tenant because an idempotency key is a string one
    /// caller chose: two tenants both submitting `order-1041` have said
    /// nothing about each other, and a global lookup would let one tenant
    /// discover -- or collide with -- another's keys.
    ///
    /// `None` rather than an error, because a key nobody has used yet is
    /// the ordinary case on the submission path, not an exceptional one.
    async fn find_by_idempotency_key(
        &self,
        tenant_id: Uuid,
        key: &IdempotencyKey,
    ) -> StorageResult<Option<Notification>>;

    /// How many notifications sit in each status, optionally narrowed to
    /// one tenant. Statuses with no rows are absent from the map rather
    /// than present as zero -- callers that want a full table iterate
    /// `DeliveryStatus::all()` and default the gaps, which keeps the
    /// aggregate query a plain `GROUP BY`.
    async fn count_by_status(&self, tenant_id: Option<Uuid>) -> StorageResult<HashMap<DeliveryStatus, u64>>;
}

/// Persistence for `Recipient`s.
#[async_trait]
pub trait RecipientRepository: Send + Sync {
    async fn insert_recipient(&self, recipient: &Recipient) -> StorageResult<()>;

    async fn get_recipient(&self, id: Uuid) -> StorageResult<Recipient>;
}

/// Persistence for `Template`s.
///
/// Every method is keyed by `(tenant_id, id)` rather than by id alone:
/// template ids are caller-chosen slugs, so `welcome_email` is expected to
/// exist for many tenants at once and a lookup that forgot the tenant would
/// silently cross that boundary.
#[async_trait]
pub trait TemplateRepository: Send + Sync {
    /// Stores a new template, failing with
    /// `StorageError::TemplateAlreadyExists` if the tenant already has one
    /// with this id. Kept distinct from `update_template` so the API can
    /// answer "create" and "replace" with different status codes instead of
    /// letting a POST silently clobber an existing template.
    async fn insert_template(&self, template: &Template) -> StorageResult<()>;

    async fn get_template(&self, tenant_id: Uuid, id: &str) -> StorageResult<Template>;

    /// Replaces an existing template's stored content, failing with
    /// `StorageError::TemplateNotFound` if there's nothing to replace.
    async fn update_template(&self, template: &Template) -> StorageResult<()>;

    /// A tenant's templates ordered by id, capped at `limit`.
    async fn list_templates(&self, tenant_id: Uuid, limit: u32) -> StorageResult<Vec<Template>>;

    async fn delete_template(&self, tenant_id: Uuid, id: &str) -> StorageResult<()>;
}

/// Persistence for `ApiKey`s.
///
/// The lookup authentication actually uses is `find_api_key_by_digest`,
/// keyed on the digest rather than on the key's id: a caller presents only
/// a token, so the digest is the only thing the service can compute before
/// it knows which key -- or which tenant -- it is dealing with.
#[async_trait]
pub trait ApiKeyRepository: Send + Sync {
    async fn insert_api_key(&self, key: &ApiKey) -> StorageResult<()>;

    async fn get_api_key(&self, id: Uuid) -> StorageResult<ApiKey>;

    /// Resolves a presented token's digest to the key that issued it,
    /// returning `StorageError::UnknownApiKey` if nothing matches.
    ///
    /// Revoked keys are returned rather than filtered out here. Whether a
    /// revoked credential is acceptable is `ns-auth`'s decision, and
    /// hiding those rows would leave the caller unable to tell "this token
    /// was never real" from "this token was turned off" -- a distinction
    /// the operator's logs need even though the response to the caller is
    /// the same either way.
    async fn find_api_key_by_digest(&self, digest: &str) -> StorageResult<ApiKey>;

    /// A tenant's keys, newest-first, capped at `limit`. Includes revoked
    /// keys, since an operator auditing who had access needs to see the
    /// ones that were turned off as much as the ones still live.
    async fn list_api_keys(&self, tenant_id: Uuid, limit: u32) -> StorageResult<Vec<ApiKey>>;

    /// Persists the current state of `key` -- in practice, its revocation.
    async fn update_api_key(&self, key: &ApiKey) -> StorageResult<()>;
}

/// Persistence for `DeliveryAttempt`s: the append-only record of what the
/// service actually did on each try.
///
/// Deliberately append-only. There is no `update_attempt`, because an
/// attempt describes a moment that has already passed and a log that can
/// be rewritten is not evidence of anything. The only way a row leaves is
/// `purge_attempts_before`, which drops whole time ranges rather than
/// individual records.
#[async_trait]
pub trait DeliveryAttemptRepository: Send + Sync {
    async fn record_attempt(&self, attempt: &DeliveryAttempt) -> StorageResult<()>;

    /// One notification's attempts, oldest first, capped at `limit`.
    ///
    /// Chronological rather than newest-first -- the opposite of every
    /// other listing here -- because this is read as a narrative: an
    /// operator wants the rate-limit skip that came before the rejection
    /// that came before the dead-letter, in that order.
    async fn list_attempts(&self, notification_id: Uuid, limit: u32) -> StorageResult<Vec<DeliveryAttempt>>;

    /// A tenant's most recent attempts across all its notifications,
    /// newest-first, capped at `limit`. This is the "what is failing right
    /// now" view, which is a different question from one notification's
    /// history.
    async fn list_tenant_attempts(&self, tenant_id: Uuid, limit: u32) -> StorageResult<Vec<DeliveryAttempt>>;

    /// Deletes every attempt recorded before `cutoff`, returning how many
    /// rows went. This table grows without bound otherwise: it is the only
    /// one where a single notification writes many rows, and nothing else
    /// ever removes one.
    async fn purge_attempts_before(&self, cutoff: DateTime<Utc>) -> StorageResult<u64>;

    /// How many attempts `purge_attempts_before(cutoff)` would delete,
    /// without deleting any of them. Lets a `--dry-run` report the same
    /// number a real purge would, against a log an operator has one
    /// chance to get the cutoff right on -- there is no way to tell which
    /// rows a completed purge took.
    async fn count_attempts_before(&self, cutoff: DateTime<Utc>) -> StorageResult<u64>;
}

/// Persistence for `Suppression`s: the addresses a tenant may no longer
/// send to.
///
/// `find_suppression` is on the delivery path -- it runs before every send
/// -- so it is a point query on the primary key rather than anything that
/// grows with the size of the list. That is also why the trait takes a
/// `RecipientAddress` rather than a pre-computed key string: normalising
/// is the implementation's job, and a caller that computed the key itself
/// could compute it differently and silently miss a match.
#[async_trait]
pub trait SuppressionRepository: Send + Sync {
    /// Adds an address to the list, replacing any existing entry for it.
    ///
    /// Replacing rather than failing is deliberate. Suppressing an
    /// already-suppressed address is not a mistake worth an error -- the
    /// caller's intent is satisfied either way -- and the same address
    /// legitimately arrives twice with different reasons, most often a
    /// bounce followed by a complaint. Keeping the newer record means the
    /// list reflects the most recent thing known about the address.
    async fn insert_suppression(&self, suppression: &Suppression) -> StorageResult<()>;

    /// The suppression covering `address` for `tenant_id`, or `None`.
    ///
    /// `None` rather than an error, because "this address is fine to send
    /// to" is the overwhelmingly common answer and not an exceptional one.
    async fn find_suppression(
        &self,
        tenant_id: Uuid,
        address: &RecipientAddress,
    ) -> StorageResult<Option<Suppression>>;

    /// A tenant's suppressions, newest-first, capped at `limit`.
    async fn list_suppressions(&self, tenant_id: Uuid, limit: u32) -> StorageResult<Vec<Suppression>>;

    /// Removes an address from the list, returning whether there was one
    /// to remove.
    ///
    /// A bool rather than a `NotFound` error: unsuppressing is an
    /// idempotent operation an operator may well run without knowing the
    /// current state, and the caller that does care can still tell.
    async fn delete_suppression(&self, tenant_id: Uuid, address: &RecipientAddress) -> StorageResult<bool>;
}

/// Persistence for `Preference`s: what each recipient has asked to be
/// sent, per topic.
///
/// The delivery-path method is `preferences_for`, and it returns a whole
/// `PreferenceSet` rather than a single row on purpose. Deciding a send
/// needs every preference the recipient holds for that topic -- a
/// wildcard block and a channel exception only mean something read as a
/// pair -- and there are at most `ChannelKind::all().len() + 1` of them,
/// so fetching the set costs one indexed range read rather than the two
/// point queries a "look for the channel row, then fall back to the
/// wildcard" API would need.
#[async_trait]
pub trait PreferenceRepository: Send + Sync {
    /// Records a recipient's answer, replacing any previous one for the
    /// same (recipient, topic, channel).
    ///
    /// Replacing rather than failing, for the same reason
    /// `insert_suppression` does: a recipient changing their mind is the
    /// ordinary case, not an error, and the caller's intent is satisfied
    /// either way. The write is named `set_preference` rather than
    /// `insert_preference` so that is visible at the call site.
    async fn set_preference(&self, preference: &Preference) -> StorageResult<()>;

    /// Every preference `recipient_id` holds for `topic`, as the set the
    /// delivery path resolves.
    ///
    /// An empty set is the answer for a recipient who has said nothing,
    /// not an error: silence is the common case and it means "send it".
    async fn preferences_for(
        &self,
        tenant_id: Uuid,
        recipient_id: Uuid,
        topic: &Topic,
    ) -> StorageResult<PreferenceSet>;

    /// Everything one recipient has said, across every topic, most
    /// recently changed first, capped at `limit`. This is what a
    /// preference centre renders.
    async fn list_preferences(
        &self,
        tenant_id: Uuid,
        recipient_id: Uuid,
        limit: u32,
    ) -> StorageResult<Vec<Preference>>;

    /// Forgets one answer, returning whether there was one to forget.
    ///
    /// Deleting is not the same as setting `Allowed`, and both are worth
    /// having. Setting `Allowed` records that the recipient said yes,
    /// which is what carves an exception out of a broader block; deleting
    /// returns them to having said nothing at all, which is what an
    /// operator undoing a mistaken import wants.
    async fn delete_preference(
        &self,
        tenant_id: Uuid,
        recipient_id: Uuid,
        topic: &Topic,
        channel: Option<ns_core::ChannelKind>,
    ) -> StorageResult<bool>;
}

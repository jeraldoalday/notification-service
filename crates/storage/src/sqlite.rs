use std::collections::HashMap;
use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use ns_auth::ApiKey;
use ns_core::{
    DeliveryAttempt, DeliveryStatus, IdempotencyKey, Notification, Recipient, RecipientAddress, Suppression,
};
use ns_templates::Template;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use uuid::Uuid;

use crate::error::{StorageError, StorageResult};
use crate::repository::{
    ApiKeyRepository, DeliveryAttemptRepository, NotificationRepository, RecipientRepository,
    SuppressionRepository, TemplateRepository,
};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// SQLite-backed storage. Rows carry a handful of indexed columns (status,
/// priority, scheduling, tenant) alongside a `data` column holding the full
/// entity as JSON -- the indexed columns exist only to make
/// `due_for_delivery` and future filtered queries efficient, the JSON blob
/// is the single source of truth for round-tripping the entity itself.
#[derive(Clone)]
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    /// Connects to `url` (e.g. `sqlite://notifications.db` or
    /// `sqlite::memory:`), creating the database file if it doesn't exist,
    /// and runs any pending migrations before returning.
    pub async fn connect(url: &str) -> StorageResult<Self> {
        let options = SqliteConnectOptions::from_str(url)?.create_if_missing(true);
        let pool = SqlitePoolOptions::new().connect_with(options).await?;
        let storage = Self { pool };
        storage.migrate().await?;
        Ok(storage)
    }

    pub async fn migrate(&self) -> StorageResult<()> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }
}

/// Matches the `#[serde(rename_all = "snake_case")]` representation on
/// `DeliveryStatus`, kept as an explicit mapping (rather than round-tripping
/// through serde_json) so the indexed `status` column stays cheap to bind
/// and to query against.
fn status_str(status: DeliveryStatus) -> &'static str {
    match status {
        DeliveryStatus::Pending => "pending",
        DeliveryStatus::Queued => "queued",
        DeliveryStatus::Sending => "sending",
        DeliveryStatus::Delivered => "delivered",
        DeliveryStatus::Failed => "failed",
        DeliveryStatus::DeadLettered => "dead_lettered",
        DeliveryStatus::Cancelled => "cancelled",
        DeliveryStatus::Suppressed => "suppressed",
    }
}

/// Inverse of `status_str`, for reading the indexed `status` column back
/// in aggregate queries where there's no `data` blob to deserialize.
fn status_from_str(value: &str) -> StorageResult<DeliveryStatus> {
    DeliveryStatus::all()
        .into_iter()
        .find(|status| status.as_str() == value)
        .ok_or_else(|| StorageError::UnknownStatus(value.to_string()))
}

/// Fixed-width, zero-padded RFC 3339 so that lexical string ordering (used
/// by the `due_for_delivery` query and its supporting index) agrees with
/// chronological ordering. `DateTime::to_rfc3339`'s default formatting
/// varies the number of fractional-second digits, which would break that.
fn to_ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[async_trait]
impl NotificationRepository for SqliteStorage {
    async fn insert_notification(&self, notification: &Notification) -> StorageResult<()> {
        let data = serde_json::to_string(notification)?;
        let result = sqlx::query(
            "INSERT INTO notifications
                (id, tenant_id, status, priority, scheduled_for, created_at, updated_at,
                 idempotency_key, data)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(notification.id.to_string())
        .bind(notification.tenant_id.to_string())
        .bind(status_str(notification.status))
        .bind(notification.priority.as_u8() as i64)
        .bind(notification.scheduled_for.map(to_ts))
        .bind(to_ts(notification.created_at))
        .bind(to_ts(notification.updated_at))
        .bind(notification.idempotency_key.as_ref().map(|key| key.as_str().to_string()))
        .bind(data)
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => Ok(()),
            // The unique index over (tenant_id, idempotency_key) is what
            // makes a duplicate submission detectable at all -- a check
            // followed by an insert leaves a gap two concurrent retries
            // fit through. Translating it here means the queue gets a
            // domain error rather than having to recognise SQLite's own
            // constraint message.
            Err(sqlx::Error::Database(err)) if err.is_unique_violation() => {
                Err(StorageError::DuplicateIdempotencyKey {
                    tenant_id: notification.tenant_id,
                    key: notification.idempotency_key.as_ref().map(|key| key.to_string()).unwrap_or_default(),
                })
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn find_by_idempotency_key(
        &self,
        tenant_id: Uuid,
        key: &IdempotencyKey,
    ) -> StorageResult<Option<Notification>> {
        // A point query on the unique index, because this runs on the
        // submission path ahead of every keyed insert.
        let row = sqlx::query("SELECT data FROM notifications WHERE tenant_id = ? AND idempotency_key = ?")
            .bind(tenant_id.to_string())
            .bind(key.as_str())
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some(row) => {
                let data: String = row.try_get("data")?;
                Ok(Some(serde_json::from_str(&data)?))
            }
            None => Ok(None),
        }
    }

    async fn get_notification(&self, id: Uuid) -> StorageResult<Notification> {
        let row = sqlx::query("SELECT data FROM notifications WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StorageError::NotificationNotFound(id))?;
        let data: String = row.try_get("data")?;
        Ok(serde_json::from_str(&data)?)
    }

    // Neither update path writes `idempotency_key`. The key names the
    // submission that created the row, so it is set once at insert and is
    // never a thing an update could legitimately change -- and leaving it
    // out means an update can never trip the unique index.
    async fn update_notification(&self, notification: &Notification) -> StorageResult<()> {
        let data = serde_json::to_string(notification)?;
        let result = sqlx::query(
            "UPDATE notifications
             SET tenant_id = ?, status = ?, priority = ?, scheduled_for = ?,
                 created_at = ?, updated_at = ?, data = ?
             WHERE id = ?",
        )
        .bind(notification.tenant_id.to_string())
        .bind(status_str(notification.status))
        .bind(notification.priority.as_u8() as i64)
        .bind(notification.scheduled_for.map(to_ts))
        .bind(to_ts(notification.created_at))
        .bind(to_ts(notification.updated_at))
        .bind(data)
        .bind(notification.id.to_string())
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(StorageError::NotificationNotFound(notification.id));
        }
        Ok(())
    }

    async fn try_update_notification(
        &self,
        notification: &Notification,
        expected_status: DeliveryStatus,
    ) -> StorageResult<bool> {
        let data = serde_json::to_string(notification)?;
        let result = sqlx::query(
            "UPDATE notifications
             SET tenant_id = ?, status = ?, priority = ?, scheduled_for = ?,
                 created_at = ?, updated_at = ?, data = ?
             WHERE id = ? AND status = ?",
        )
        .bind(notification.tenant_id.to_string())
        .bind(status_str(notification.status))
        .bind(notification.priority.as_u8() as i64)
        .bind(notification.scheduled_for.map(to_ts))
        .bind(to_ts(notification.created_at))
        .bind(to_ts(notification.updated_at))
        .bind(data)
        .bind(notification.id.to_string())
        .bind(status_str(expected_status))
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    async fn due_for_delivery(&self, now: DateTime<Utc>, limit: u32) -> StorageResult<Vec<Notification>> {
        let rows = sqlx::query(
            "SELECT data FROM notifications
             WHERE status = ? AND (scheduled_for IS NULL OR scheduled_for <= ?)
             ORDER BY priority DESC, created_at ASC
             LIMIT ?",
        )
        .bind(status_str(DeliveryStatus::Queued))
        .bind(to_ts(now))
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let data: String = row.try_get("data")?;
                Ok(serde_json::from_str(&data)?)
            })
            .collect()
    }

    async fn list_by_tenant(
        &self,
        tenant_id: Uuid,
        status: Option<DeliveryStatus>,
        limit: u32,
    ) -> StorageResult<Vec<Notification>> {
        let rows = match status {
            Some(status) => {
                sqlx::query(
                    "SELECT data FROM notifications
                     WHERE tenant_id = ? AND status = ?
                     ORDER BY created_at DESC
                     LIMIT ?",
                )
                .bind(tenant_id.to_string())
                .bind(status_str(status))
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query(
                    "SELECT data FROM notifications
                     WHERE tenant_id = ?
                     ORDER BY created_at DESC
                     LIMIT ?",
                )
                .bind(tenant_id.to_string())
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
        };

        rows.into_iter()
            .map(|row| {
                let data: String = row.try_get("data")?;
                Ok(serde_json::from_str(&data)?)
            })
            .collect()
    }

    async fn count_by_status(&self, tenant_id: Option<Uuid>) -> StorageResult<HashMap<DeliveryStatus, u64>> {
        // Counting off the indexed `status` column rather than loading and
        // deserializing every row: this is a dashboard query that can span
        // the whole table.
        let rows =
            match tenant_id {
                Some(tenant_id) => sqlx::query(
                    "SELECT status, COUNT(*) AS total FROM notifications WHERE tenant_id = ? GROUP BY status",
                )
                .bind(tenant_id.to_string())
                .fetch_all(&self.pool)
                .await?,
                None => {
                    sqlx::query("SELECT status, COUNT(*) AS total FROM notifications GROUP BY status")
                        .fetch_all(&self.pool)
                        .await?
                }
            };

        let mut counts = HashMap::new();
        for row in rows {
            let status: String = row.try_get("status")?;
            let total: i64 = row.try_get("total")?;
            counts.insert(status_from_str(&status)?, total as u64);
        }
        Ok(counts)
    }
}

#[async_trait]
impl RecipientRepository for SqliteStorage {
    async fn insert_recipient(&self, recipient: &Recipient) -> StorageResult<()> {
        let data = serde_json::to_string(recipient)?;
        sqlx::query("INSERT INTO recipients (id, data) VALUES (?, ?)")
            .bind(recipient.id.to_string())
            .bind(data)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_recipient(&self, id: Uuid) -> StorageResult<Recipient> {
        let row = sqlx::query("SELECT data FROM recipients WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StorageError::RecipientNotFound(id))?;
        let data: String = row.try_get("data")?;
        Ok(serde_json::from_str(&data)?)
    }
}

#[async_trait]
impl TemplateRepository for SqliteStorage {
    async fn insert_template(&self, template: &Template) -> StorageResult<()> {
        let data = serde_json::to_string(template)?;
        let result = sqlx::query(
            "INSERT INTO templates (tenant_id, id, channel, created_at, updated_at, data)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(template.tenant_id.to_string())
        .bind(&template.id)
        .bind(template.channel.map(|c| c.as_str()))
        .bind(to_ts(template.created_at))
        .bind(to_ts(template.updated_at))
        .bind(data)
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => Ok(()),
            // The (tenant_id, id) primary key is what makes "already
            // exists" detectable at all; translating it here means callers
            // get a domain error instead of having to sniff SQLite's own
            // constraint message.
            Err(sqlx::Error::Database(err)) if err.is_unique_violation() => {
                Err(StorageError::TemplateAlreadyExists {
                    tenant_id: template.tenant_id,
                    id: template.id.clone(),
                })
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn get_template(&self, tenant_id: Uuid, id: &str) -> StorageResult<Template> {
        let row = sqlx::query("SELECT data FROM templates WHERE tenant_id = ? AND id = ?")
            .bind(tenant_id.to_string())
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StorageError::TemplateNotFound { tenant_id, id: id.to_string() })?;
        let data: String = row.try_get("data")?;
        Ok(serde_json::from_str(&data)?)
    }

    async fn update_template(&self, template: &Template) -> StorageResult<()> {
        let data = serde_json::to_string(template)?;
        let result = sqlx::query(
            "UPDATE templates SET channel = ?, created_at = ?, updated_at = ?, data = ?
             WHERE tenant_id = ? AND id = ?",
        )
        .bind(template.channel.map(|c| c.as_str()))
        .bind(to_ts(template.created_at))
        .bind(to_ts(template.updated_at))
        .bind(data)
        .bind(template.tenant_id.to_string())
        .bind(&template.id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(StorageError::TemplateNotFound {
                tenant_id: template.tenant_id,
                id: template.id.clone(),
            });
        }
        Ok(())
    }

    async fn list_templates(&self, tenant_id: Uuid, limit: u32) -> StorageResult<Vec<Template>> {
        let rows = sqlx::query("SELECT data FROM templates WHERE tenant_id = ? ORDER BY id ASC LIMIT ?")
            .bind(tenant_id.to_string())
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(|row| {
                let data: String = row.try_get("data")?;
                Ok(serde_json::from_str(&data)?)
            })
            .collect()
    }

    async fn delete_template(&self, tenant_id: Uuid, id: &str) -> StorageResult<()> {
        let result = sqlx::query("DELETE FROM templates WHERE tenant_id = ? AND id = ?")
            .bind(tenant_id.to_string())
            .bind(id)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(StorageError::TemplateNotFound { tenant_id, id: id.to_string() });
        }
        Ok(())
    }
}

#[async_trait]
impl ApiKeyRepository for SqliteStorage {
    async fn insert_api_key(&self, key: &ApiKey) -> StorageResult<()> {
        let data = serde_json::to_string(key)?;
        sqlx::query(
            "INSERT INTO api_keys (id, tenant_id, token_digest, created_at, revoked_at, data)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(key.id.to_string())
        .bind(key.tenant_id.to_string())
        .bind(&key.token_digest)
        .bind(to_ts(key.created_at))
        .bind(key.revoked_at.map(to_ts))
        .bind(data)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_api_key(&self, id: Uuid) -> StorageResult<ApiKey> {
        let row = sqlx::query("SELECT data FROM api_keys WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StorageError::ApiKeyNotFound(id))?;
        let data: String = row.try_get("data")?;
        Ok(serde_json::from_str(&data)?)
    }

    async fn find_api_key_by_digest(&self, digest: &str) -> StorageResult<ApiKey> {
        // A point query on the UNIQUE token_digest index. This runs on
        // every authenticated request, so it has to stay one indexed
        // lookup rather than anything that grows with the number of keys
        // issued.
        let row = sqlx::query("SELECT data FROM api_keys WHERE token_digest = ?")
            .bind(digest)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StorageError::UnknownApiKey)?;
        let data: String = row.try_get("data")?;
        Ok(serde_json::from_str(&data)?)
    }

    async fn list_api_keys(&self, tenant_id: Uuid, limit: u32) -> StorageResult<Vec<ApiKey>> {
        let rows = sqlx::query(
            "SELECT data FROM api_keys WHERE tenant_id = ? ORDER BY created_at DESC, id ASC LIMIT ?",
        )
        .bind(tenant_id.to_string())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let data: String = row.try_get("data")?;
                Ok(serde_json::from_str(&data)?)
            })
            .collect()
    }

    async fn update_api_key(&self, key: &ApiKey) -> StorageResult<()> {
        let data = serde_json::to_string(key)?;
        // token_digest is deliberately absent from the SET list: a key's
        // token is fixed for its lifetime, and rotating one means issuing
        // a new key and revoking this one, so that the old token stops
        // working at a moment the audit trail records.
        let result = sqlx::query("UPDATE api_keys SET revoked_at = ?, data = ? WHERE id = ?")
            .bind(key.revoked_at.map(to_ts))
            .bind(data)
            .bind(key.id.to_string())
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(StorageError::ApiKeyNotFound(key.id));
        }
        Ok(())
    }
}

#[async_trait]
impl DeliveryAttemptRepository for SqliteStorage {
    async fn record_attempt(&self, attempt: &DeliveryAttempt) -> StorageResult<()> {
        let data = serde_json::to_string(attempt)?;
        sqlx::query(
            "INSERT INTO delivery_attempts
                (id, notification_id, tenant_id, result, occurred_at, data)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(attempt.id.to_string())
        .bind(attempt.notification_id.to_string())
        .bind(attempt.tenant_id.to_string())
        .bind(attempt.result.as_str())
        .bind(to_ts(attempt.occurred_at))
        .bind(data)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_attempts(&self, notification_id: Uuid, limit: u32) -> StorageResult<Vec<DeliveryAttempt>> {
        // `id` breaks ties after `occurred_at`, because two attempts on
        // the same notification can land in the same millisecond -- a
        // rate-limit skip is recorded without ever waiting on a provider
        // -- and an unstable order would shuffle the history between two
        // reads of the same rows.
        let rows = sqlx::query(
            "SELECT data FROM delivery_attempts
             WHERE notification_id = ?
             ORDER BY occurred_at ASC, id ASC
             LIMIT ?",
        )
        .bind(notification_id.to_string())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        decode_attempts(rows)
    }

    async fn list_tenant_attempts(&self, tenant_id: Uuid, limit: u32) -> StorageResult<Vec<DeliveryAttempt>> {
        let rows = sqlx::query(
            "SELECT data FROM delivery_attempts
             WHERE tenant_id = ?
             ORDER BY occurred_at DESC, id ASC
             LIMIT ?",
        )
        .bind(tenant_id.to_string())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        decode_attempts(rows)
    }

    async fn purge_attempts_before(&self, cutoff: DateTime<Utc>) -> StorageResult<u64> {
        // Compared as a string against the same fixed-width RFC 3339
        // format every timestamp here is written in, which is exactly why
        // `to_ts` pads: lexical ordering has to agree with chronological
        // ordering for this range delete to mean what it says.
        let result = sqlx::query("DELETE FROM delivery_attempts WHERE occurred_at < ?")
            .bind(to_ts(cutoff))
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

/// Shared by the two attempt listings, which differ only in their ORDER BY.
fn decode_attempts(rows: Vec<sqlx::sqlite::SqliteRow>) -> StorageResult<Vec<DeliveryAttempt>> {
    rows.into_iter()
        .map(|row| {
            let data: String = row.try_get("data")?;
            Ok(serde_json::from_str(&data)?)
        })
        .collect()
}

#[async_trait]
impl SuppressionRepository for SqliteStorage {
    async fn insert_suppression(&self, suppression: &Suppression) -> StorageResult<()> {
        let data = serde_json::to_string(suppression)?;
        // Upsert rather than plain insert: the same address legitimately
        // arrives twice with different reasons -- a bounce, then a
        // complaint -- and the newer record is the one worth keeping.
        // `created_at` moves with it, since the row now describes the
        // later event.
        sqlx::query(
            "INSERT INTO suppressions (tenant_id, channel, address_key, reason, created_at, data)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT (tenant_id, channel, address_key)
             DO UPDATE SET reason = excluded.reason,
                           created_at = excluded.created_at,
                           data = excluded.data",
        )
        .bind(suppression.tenant_id.to_string())
        .bind(suppression.channel().as_str())
        .bind(suppression.key())
        .bind(suppression.reason.as_str())
        .bind(to_ts(suppression.created_at))
        .bind(data)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn find_suppression(
        &self,
        tenant_id: Uuid,
        address: &RecipientAddress,
    ) -> StorageResult<Option<Suppression>> {
        // A point query on the primary key. This runs before every send,
        // so it has to stay one indexed lookup rather than anything that
        // grows with the length of the list.
        let row = sqlx::query(
            "SELECT data FROM suppressions WHERE tenant_id = ? AND channel = ? AND address_key = ?",
        )
        .bind(tenant_id.to_string())
        .bind(address.channel_kind().as_str())
        .bind(address.suppression_key())
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => {
                let data: String = row.try_get("data")?;
                Ok(Some(serde_json::from_str(&data)?))
            }
            None => Ok(None),
        }
    }

    async fn list_suppressions(&self, tenant_id: Uuid, limit: u32) -> StorageResult<Vec<Suppression>> {
        let rows = sqlx::query(
            "SELECT data FROM suppressions
             WHERE tenant_id = ?
             ORDER BY created_at DESC, address_key ASC
             LIMIT ?",
        )
        .bind(tenant_id.to_string())
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let data: String = row.try_get("data")?;
                Ok(serde_json::from_str(&data)?)
            })
            .collect()
    }

    async fn delete_suppression(&self, tenant_id: Uuid, address: &RecipientAddress) -> StorageResult<bool> {
        let result =
            sqlx::query("DELETE FROM suppressions WHERE tenant_id = ? AND channel = ? AND address_key = ?")
                .bind(tenant_id.to_string())
                .bind(address.channel_kind().as_str())
                .bind(address.suppression_key())
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected() > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use ns_auth::ApiKey;
    use ns_core::{ChannelKind, DeliveryStatus, IdempotencyKey, Priority, Recipient, RecipientAddress};

    /// Each test gets its own on-disk SQLite file rather than
    /// `sqlite::memory:`, since a pooled connection to `:memory:` gives
    /// every connection its own separate database once the pool grows
    /// past one connection -- writes from one connection wouldn't be
    /// visible to reads from another.
    async fn test_storage() -> (SqliteStorage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let storage = SqliteStorage::connect(&format!("sqlite://{}", db_path.display())).await.unwrap();
        (storage, dir)
    }

    fn sample_notification() -> Notification {
        Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .subject("Welcome")
            .body("Hello there")
            .build()
            .unwrap()
    }

    fn sample_recipient() -> Recipient {
        Recipient::new(Uuid::new_v4(), Uuid::new_v4())
            .with_display_name("Ada Lovelace")
            .with_address(RecipientAddress::Email("ada@example.com".into()))
            .unwrap()
    }

    fn sample_attempt(notification: &Notification, result: ns_core::AttemptResult) -> DeliveryAttempt {
        DeliveryAttempt::new(notification, result, Utc::now())
    }

    fn keyed_notification(tenant_id: Uuid, key: &str) -> Notification {
        Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email)
            .body("Hello there")
            .idempotency_key(IdempotencyKey::new(key).unwrap())
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn finds_a_notification_by_the_key_it_was_submitted_under() {
        let (storage, _dir) = test_storage().await;
        let tenant_id = Uuid::new_v4();
        let notification = keyed_notification(tenant_id, "order-1041");
        storage.insert_notification(&notification).await.unwrap();

        let key = IdempotencyKey::new("order-1041").unwrap();
        let found = storage.find_by_idempotency_key(tenant_id, &key).await.unwrap();

        assert_eq!(found.map(|n| n.id), Some(notification.id));
    }

    #[tokio::test]
    async fn an_unused_key_is_a_miss_rather_than_an_error() {
        let (storage, _dir) = test_storage().await;
        let key = IdempotencyKey::new("never-used").unwrap();

        let found = storage.find_by_idempotency_key(Uuid::new_v4(), &key).await.unwrap();

        assert!(found.is_none(), "a fresh key is the ordinary case, not a failure");
    }

    #[tokio::test]
    async fn idempotency_keys_do_not_reach_across_tenants() {
        let (storage, _dir) = test_storage().await;
        let mine = Uuid::new_v4();
        let theirs = Uuid::new_v4();
        storage.insert_notification(&keyed_notification(mine, "order-1041")).await.unwrap();

        // The same string, chosen independently by two callers who have
        // never heard of each other. Both submissions have to go out.
        let theirs_notification = keyed_notification(theirs, "order-1041");
        storage.insert_notification(&theirs_notification).await.unwrap();

        let key = IdempotencyKey::new("order-1041").unwrap();
        let found = storage.find_by_idempotency_key(theirs, &key).await.unwrap().unwrap();
        assert_eq!(found.id, theirs_notification.id);
    }

    #[tokio::test]
    async fn reusing_a_key_within_a_tenant_is_refused_by_the_index() {
        let (storage, _dir) = test_storage().await;
        let tenant_id = Uuid::new_v4();
        storage.insert_notification(&keyed_notification(tenant_id, "order-1041")).await.unwrap();

        // The check-then-insert the queue does above this has a gap in it;
        // the unique index is what actually closes it.
        let err =
            storage.insert_notification(&keyed_notification(tenant_id, "order-1041")).await.unwrap_err();

        assert!(
            matches!(&err, StorageError::DuplicateIdempotencyKey { tenant_id: t, key } if *t == tenant_id && key == "order-1041"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn notifications_without_a_key_never_collide() {
        let (storage, _dir) = test_storage().await;
        let tenant_id = Uuid::new_v4();

        // Most notifications carry no key at all, and the index must not
        // make them exclusive with each other.
        for _ in 0..3 {
            let notification = Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email)
                .body("Hello there")
                .build()
                .unwrap();
            storage.insert_notification(&notification).await.unwrap();
        }

        assert_eq!(storage.list_by_tenant(tenant_id, None, 10).await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn a_key_survives_the_notifications_whole_lifecycle() {
        let (storage, _dir) = test_storage().await;
        let tenant_id = Uuid::new_v4();
        let mut notification = keyed_notification(tenant_id, "order-1041");
        storage.insert_notification(&notification).await.unwrap();

        // Updates deliberately leave the column alone, so a notification
        // that has since been queued and claimed is still findable by the
        // key its caller submitted it under -- which is exactly when a
        // retry is most likely to arrive.
        notification.transition(DeliveryStatus::Queued, Utc::now()).unwrap();
        storage.update_notification(&notification).await.unwrap();
        notification.transition(DeliveryStatus::Sending, Utc::now()).unwrap();
        assert!(storage.try_update_notification(&notification, DeliveryStatus::Queued).await.unwrap());

        let key = IdempotencyKey::new("order-1041").unwrap();
        let found = storage.find_by_idempotency_key(tenant_id, &key).await.unwrap().unwrap();
        assert_eq!(found.id, notification.id);
        assert_eq!(found.status, DeliveryStatus::Sending);
        assert_eq!(found.idempotency_key, notification.idempotency_key);
    }

    #[tokio::test]
    async fn records_and_lists_a_notifications_attempts() {
        use ns_core::AttemptResult;
        let (storage, _dir) = test_storage().await;
        let notification = sample_notification();

        let attempt = sample_attempt(&notification, AttemptResult::Failed).with_error("550 mailbox full");
        storage.record_attempt(&attempt).await.unwrap();

        let listed = storage.list_attempts(notification.id, 10).await.unwrap();
        assert_eq!(listed, vec![attempt]);
    }

    #[tokio::test]
    async fn attempts_list_oldest_first() {
        use ns_core::AttemptResult;
        let (storage, _dir) = test_storage().await;
        let notification = sample_notification();

        let mut recorded = Vec::new();
        for minutes_ago in [3, 2, 1] {
            let mut attempt = sample_attempt(&notification, AttemptResult::Failed);
            attempt.occurred_at = Utc::now() - Duration::minutes(minutes_ago);
            storage.record_attempt(&attempt).await.unwrap();
            recorded.push(attempt);
        }

        let listed = storage.list_attempts(notification.id, 10).await.unwrap();
        let order: Vec<Uuid> = listed.iter().map(|a| a.id).collect();
        assert_eq!(order, recorded.iter().map(|a| a.id).collect::<Vec<_>>(), "history reads forwards");
    }

    #[tokio::test]
    async fn attempts_are_scoped_to_their_own_notification() {
        use ns_core::AttemptResult;
        let (storage, _dir) = test_storage().await;
        let mine = sample_notification();
        let theirs = sample_notification();

        storage.record_attempt(&sample_attempt(&mine, AttemptResult::Delivered)).await.unwrap();
        storage.record_attempt(&sample_attempt(&theirs, AttemptResult::Delivered)).await.unwrap();

        assert_eq!(storage.list_attempts(mine.id, 10).await.unwrap().len(), 1);
        assert_eq!(storage.list_attempts(theirs.id, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn listing_attempts_respects_the_limit() {
        use ns_core::AttemptResult;
        let (storage, _dir) = test_storage().await;
        let notification = sample_notification();

        for _ in 0..5 {
            storage.record_attempt(&sample_attempt(&notification, AttemptResult::Skipped)).await.unwrap();
        }

        assert_eq!(storage.list_attempts(notification.id, 2).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_notification_with_no_attempts_lists_none() {
        let (storage, _dir) = test_storage().await;
        assert!(storage.list_attempts(Uuid::new_v4(), 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn tenant_attempts_span_notifications_newest_first() {
        use ns_core::AttemptResult;
        let (storage, _dir) = test_storage().await;
        let tenant = Uuid::new_v4();

        let mut newest = None;
        for minutes_ago in [5, 1, 3] {
            let mut notification = sample_notification();
            notification.tenant_id = tenant;
            let mut attempt = sample_attempt(&notification, AttemptResult::Failed);
            attempt.occurred_at = Utc::now() - Duration::minutes(minutes_ago);
            storage.record_attempt(&attempt).await.unwrap();
            if minutes_ago == 1 {
                newest = Some(attempt.id);
            }
        }

        let listed = storage.list_tenant_attempts(tenant, 10).await.unwrap();
        assert_eq!(listed.len(), 3);
        assert_eq!(listed[0].id, newest.unwrap());
    }

    #[tokio::test]
    async fn one_tenants_attempts_are_invisible_to_another() {
        use ns_core::AttemptResult;
        let (storage, _dir) = test_storage().await;
        let notification = sample_notification();
        storage.record_attempt(&sample_attempt(&notification, AttemptResult::Delivered)).await.unwrap();

        assert!(storage.list_tenant_attempts(Uuid::new_v4(), 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn purging_drops_only_attempts_older_than_the_cutoff() {
        use ns_core::AttemptResult;
        let (storage, _dir) = test_storage().await;
        let notification = sample_notification();

        let mut old = sample_attempt(&notification, AttemptResult::Failed);
        old.occurred_at = Utc::now() - Duration::days(40);
        storage.record_attempt(&old).await.unwrap();

        let recent = sample_attempt(&notification, AttemptResult::Delivered);
        storage.record_attempt(&recent).await.unwrap();

        let purged = storage.purge_attempts_before(Utc::now() - Duration::days(30)).await.unwrap();

        assert_eq!(purged, 1);
        let remaining = storage.list_attempts(notification.id, 10).await.unwrap();
        assert_eq!(remaining.iter().map(|a| a.id).collect::<Vec<_>>(), vec![recent.id]);
    }

    #[tokio::test]
    async fn purging_a_range_with_nothing_in_it_removes_nothing() {
        use ns_core::AttemptResult;
        let (storage, _dir) = test_storage().await;
        let notification = sample_notification();
        storage.record_attempt(&sample_attempt(&notification, AttemptResult::Delivered)).await.unwrap();

        assert_eq!(storage.purge_attempts_before(Utc::now() - Duration::days(1)).await.unwrap(), 0);
        assert_eq!(storage.list_attempts(notification.id, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn purging_attempts_leaves_the_notification_alone() {
        use ns_core::AttemptResult;
        let (storage, _dir) = test_storage().await;
        let notification = sample_notification();
        storage.insert_notification(&notification).await.unwrap();

        let mut old = sample_attempt(&notification, AttemptResult::Failed);
        old.occurred_at = Utc::now() - Duration::days(90);
        storage.record_attempt(&old).await.unwrap();

        storage.purge_attempts_before(Utc::now()).await.unwrap();

        // And the reverse holds by construction: there is no foreign key,
        // so the log outlives its subject too.
        assert!(storage.get_notification(notification.id).await.is_ok());
    }

    #[tokio::test]
    async fn an_attempts_full_record_round_trips() {
        use ns_core::AttemptResult;
        let (storage, _dir) = test_storage().await;
        let notification = sample_notification();

        let attempt = sample_attempt(&notification, AttemptResult::Failed)
            .with_error("connection reset by peer")
            .with_duration_ms(1234);
        storage.record_attempt(&attempt).await.unwrap();

        let fetched = storage.list_attempts(notification.id, 1).await.unwrap().remove(0);
        assert_eq!(fetched, attempt);
        assert_eq!(fetched.error.as_deref(), Some("connection reset by peer"));
        assert_eq!(fetched.duration_ms, Some(1234));
    }

    fn suppression(tenant_id: Uuid, address: RecipientAddress) -> Suppression {
        Suppression::new(tenant_id, address, ns_core::SuppressionReason::Unsubscribed, Utc::now()).unwrap()
    }

    #[tokio::test]
    async fn suppresses_and_finds_an_address() {
        let (storage, _dir) = test_storage().await;
        let tenant = Uuid::new_v4();
        let address = RecipientAddress::Email("ada@example.com".into());

        let entry = suppression(tenant, address.clone());
        storage.insert_suppression(&entry).await.unwrap();

        assert_eq!(storage.find_suppression(tenant, &address).await.unwrap(), Some(entry));
    }

    #[tokio::test]
    async fn an_unsuppressed_address_is_simply_absent() {
        let (storage, _dir) = test_storage().await;
        let address = RecipientAddress::Email("ada@example.com".into());

        assert_eq!(storage.find_suppression(Uuid::new_v4(), &address).await.unwrap(), None);
    }

    #[tokio::test]
    async fn suppression_matches_an_email_whatever_its_case() {
        let (storage, _dir) = test_storage().await;
        let tenant = Uuid::new_v4();
        storage
            .insert_suppression(&suppression(tenant, RecipientAddress::Email("Ada@Example.COM".into())))
            .await
            .unwrap();

        let looked_up = RecipientAddress::Email("ada@example.com".into());
        assert!(storage.find_suppression(tenant, &looked_up).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn suppression_is_scoped_to_one_tenant() {
        let (storage, _dir) = test_storage().await;
        let address = RecipientAddress::Email("ada@example.com".into());
        storage.insert_suppression(&suppression(Uuid::new_v4(), address.clone())).await.unwrap();

        assert_eq!(storage.find_suppression(Uuid::new_v4(), &address).await.unwrap(), None);
    }

    #[tokio::test]
    async fn suppression_is_scoped_to_one_channel() {
        let (storage, _dir) = test_storage().await;
        let tenant = Uuid::new_v4();
        // A push token and a webhook URL could in principle be the same
        // string; the channel in the key is what keeps them apart.
        storage
            .insert_suppression(&suppression(tenant, RecipientAddress::PushToken("shared".into())))
            .await
            .unwrap();

        let webhook = RecipientAddress::WebhookUrl("shared".into());
        assert_eq!(storage.find_suppression(tenant, &webhook).await.unwrap(), None);
    }

    #[tokio::test]
    async fn re_suppressing_replaces_rather_than_duplicating() {
        use ns_core::SuppressionReason;
        let (storage, _dir) = test_storage().await;
        let tenant = Uuid::new_v4();
        let address = RecipientAddress::Email("ada@example.com".into());

        storage
            .insert_suppression(
                &Suppression::new(tenant, address.clone(), SuppressionReason::Bounced, Utc::now()).unwrap(),
            )
            .await
            .unwrap();
        storage
            .insert_suppression(
                &Suppression::new(tenant, address.clone(), SuppressionReason::Complaint, Utc::now())
                    .unwrap()
                    .with_note("provider webhook"),
            )
            .await
            .unwrap();

        let found = storage.find_suppression(tenant, &address).await.unwrap().unwrap();
        assert_eq!(found.reason, SuppressionReason::Complaint, "the newer record wins");
        assert_eq!(found.note.as_deref(), Some("provider webhook"));
        assert_eq!(storage.list_suppressions(tenant, 10).await.unwrap().len(), 1, "and there is only one");
    }

    #[tokio::test]
    async fn listing_is_newest_first_and_tenant_scoped() {
        let (storage, _dir) = test_storage().await;
        let tenant = Uuid::new_v4();

        let mut older = suppression(tenant, RecipientAddress::Email("older@example.com".into()));
        older.created_at = Utc::now() - Duration::hours(2);
        storage.insert_suppression(&older).await.unwrap();
        let newer = suppression(tenant, RecipientAddress::Email("newer@example.com".into()));
        storage.insert_suppression(&newer).await.unwrap();
        storage
            .insert_suppression(&suppression(Uuid::new_v4(), RecipientAddress::Email("other@x.com".into())))
            .await
            .unwrap();

        let listed = storage.list_suppressions(tenant, 10).await.unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].address, newer.address);
    }

    #[tokio::test]
    async fn listing_suppressions_respects_the_limit() {
        let (storage, _dir) = test_storage().await;
        let tenant = Uuid::new_v4();
        for index in 0..5 {
            let address = RecipientAddress::Email(format!("user{index}@example.com"));
            storage.insert_suppression(&suppression(tenant, address)).await.unwrap();
        }

        assert_eq!(storage.list_suppressions(tenant, 3).await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn deleting_reports_whether_there_was_anything_to_delete() {
        let (storage, _dir) = test_storage().await;
        let tenant = Uuid::new_v4();
        let address = RecipientAddress::Email("ada@example.com".into());
        storage.insert_suppression(&suppression(tenant, address.clone())).await.unwrap();

        assert!(storage.delete_suppression(tenant, &address).await.unwrap());
        assert_eq!(storage.find_suppression(tenant, &address).await.unwrap(), None);
        assert!(!storage.delete_suppression(tenant, &address).await.unwrap(), "deleting again is a no-op");
    }

    #[tokio::test]
    async fn deleting_matches_the_same_normalisation_inserting_used() {
        let (storage, _dir) = test_storage().await;
        let tenant = Uuid::new_v4();
        storage
            .insert_suppression(&suppression(tenant, RecipientAddress::Email("Ada@Example.COM".into())))
            .await
            .unwrap();

        let lowered = RecipientAddress::Email("ada@example.com".into());
        assert!(storage.delete_suppression(tenant, &lowered).await.unwrap());
    }

    #[tokio::test]
    async fn one_tenant_cannot_delete_anothers_suppression() {
        let (storage, _dir) = test_storage().await;
        let owner = Uuid::new_v4();
        let address = RecipientAddress::Email("ada@example.com".into());
        storage.insert_suppression(&suppression(owner, address.clone())).await.unwrap();

        assert!(!storage.delete_suppression(Uuid::new_v4(), &address).await.unwrap());
        assert!(storage.find_suppression(owner, &address).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn inserts_and_gets_notification() {
        let (storage, _dir) = test_storage().await;
        let notification = sample_notification();

        storage.insert_notification(&notification).await.unwrap();
        let fetched = storage.get_notification(notification.id).await.unwrap();

        assert_eq!(fetched.id, notification.id);
        assert_eq!(fetched.body, notification.body);
        assert_eq!(fetched.status, DeliveryStatus::Pending);
    }

    #[tokio::test]
    async fn get_missing_notification_errors() {
        let (storage, _dir) = test_storage().await;
        let err = storage.get_notification(Uuid::new_v4()).await.unwrap_err();
        assert!(matches!(err, StorageError::NotificationNotFound(_)));
    }

    #[tokio::test]
    async fn update_persists_transitioned_status() {
        let (storage, _dir) = test_storage().await;
        let mut notification = sample_notification();
        storage.insert_notification(&notification).await.unwrap();

        notification.transition(DeliveryStatus::Queued, Utc::now()).unwrap();
        storage.update_notification(&notification).await.unwrap();

        let fetched = storage.get_notification(notification.id).await.unwrap();
        assert_eq!(fetched.status, DeliveryStatus::Queued);
    }

    #[tokio::test]
    async fn update_missing_notification_errors() {
        let (storage, _dir) = test_storage().await;
        let err = storage.update_notification(&sample_notification()).await.unwrap_err();
        assert!(matches!(err, StorageError::NotificationNotFound(_)));
    }

    #[tokio::test]
    async fn due_for_delivery_filters_by_status_and_schedule() {
        let (storage, _dir) = test_storage().await;
        let now = Utc::now();

        let still_pending = sample_notification();
        storage.insert_notification(&still_pending).await.unwrap();

        let mut queued_future = sample_notification();
        queued_future.transition(DeliveryStatus::Queued, now).unwrap();
        queued_future.scheduled_for = Some(now + Duration::hours(1));
        storage.insert_notification(&queued_future).await.unwrap();

        let mut queued_due = sample_notification();
        queued_due.transition(DeliveryStatus::Queued, now).unwrap();
        queued_due.scheduled_for = Some(now - Duration::minutes(1));
        storage.insert_notification(&queued_due).await.unwrap();

        let due = storage.due_for_delivery(now, 10).await.unwrap();
        let due_ids: Vec<Uuid> = due.iter().map(|n| n.id).collect();

        assert_eq!(due_ids, vec![queued_due.id]);
    }

    #[tokio::test]
    async fn due_for_delivery_orders_by_priority_then_age() {
        let (storage, _dir) = test_storage().await;
        let now = Utc::now();

        let mut low = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .body("low")
            .priority(Priority::Low)
            .build()
            .unwrap();
        low.transition(DeliveryStatus::Queued, now).unwrap();

        let mut critical = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .body("critical")
            .priority(Priority::Critical)
            .build()
            .unwrap();
        critical.transition(DeliveryStatus::Queued, now).unwrap();

        // Insert low-priority first so, absent priority ordering, it would
        // sort first by created_at too -- this proves priority wins.
        storage.insert_notification(&low).await.unwrap();
        storage.insert_notification(&critical).await.unwrap();

        let due = storage.due_for_delivery(now, 10).await.unwrap();
        let due_ids: Vec<Uuid> = due.iter().map(|n| n.id).collect();

        assert_eq!(due_ids, vec![critical.id, low.id]);
    }

    #[tokio::test]
    async fn due_for_delivery_respects_limit() {
        let (storage, _dir) = test_storage().await;
        let now = Utc::now();

        for _ in 0..3 {
            let mut n = sample_notification();
            n.transition(DeliveryStatus::Queued, now).unwrap();
            storage.insert_notification(&n).await.unwrap();
        }

        let due = storage.due_for_delivery(now, 2).await.unwrap();
        assert_eq!(due.len(), 2);
    }

    #[tokio::test]
    async fn inserts_and_gets_recipient() {
        let (storage, _dir) = test_storage().await;
        let recipient = sample_recipient();

        storage.insert_recipient(&recipient).await.unwrap();
        let fetched = storage.get_recipient(recipient.id).await.unwrap();

        assert_eq!(fetched.id, recipient.id);
        assert_eq!(fetched.display_name.as_deref(), Some("Ada Lovelace"));
        assert!(fetched.supports(ChannelKind::Email));
    }

    #[tokio::test]
    async fn get_missing_recipient_errors() {
        let (storage, _dir) = test_storage().await;
        let err = storage.get_recipient(Uuid::new_v4()).await.unwrap_err();
        assert!(matches!(err, StorageError::RecipientNotFound(_)));
    }

    #[tokio::test]
    async fn try_update_succeeds_when_status_matches() {
        let (storage, _dir) = test_storage().await;
        let mut notification = sample_notification();
        storage.insert_notification(&notification).await.unwrap();

        notification.transition(DeliveryStatus::Queued, Utc::now()).unwrap();
        let won = storage.try_update_notification(&notification, DeliveryStatus::Pending).await.unwrap();

        assert!(won);
        let fetched = storage.get_notification(notification.id).await.unwrap();
        assert_eq!(fetched.status, DeliveryStatus::Queued);
    }

    #[tokio::test]
    async fn try_update_loses_race_when_status_has_moved_on() {
        let (storage, _dir) = test_storage().await;
        let mut notification = sample_notification();
        storage.insert_notification(&notification).await.unwrap();

        // Simulate another worker having already claimed this notification.
        notification.transition(DeliveryStatus::Queued, Utc::now()).unwrap();
        storage.update_notification(&notification).await.unwrap();

        // This caller still thinks the notification is Pending and tries to
        // claim it on that basis -- it should lose, and leave the row alone.
        let stale_claim = sample_notification_with_id(notification.id);
        let won = storage.try_update_notification(&stale_claim, DeliveryStatus::Pending).await.unwrap();

        assert!(!won);
        let fetched = storage.get_notification(notification.id).await.unwrap();
        assert_eq!(fetched.status, DeliveryStatus::Queued);
    }

    fn sample_notification_with_id(id: Uuid) -> Notification {
        let mut n = sample_notification();
        n.id = id;
        n
    }

    #[tokio::test]
    async fn list_by_tenant_filters_out_other_tenants() {
        let (storage, _dir) = test_storage().await;
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();

        let mut n = Notification::builder(tenant_a, Uuid::new_v4(), ChannelKind::Email)
            .body("for a")
            .build()
            .unwrap();
        n.tenant_id = tenant_a;
        storage.insert_notification(&n).await.unwrap();

        let mut other = Notification::builder(tenant_b, Uuid::new_v4(), ChannelKind::Email)
            .body("for b")
            .build()
            .unwrap();
        other.tenant_id = tenant_b;
        storage.insert_notification(&other).await.unwrap();

        let listed = storage.list_by_tenant(tenant_a, None, 10).await.unwrap();

        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, n.id);
    }

    #[tokio::test]
    async fn list_by_tenant_filters_by_status_when_given() {
        let (storage, _dir) = test_storage().await;
        let tenant_id = Uuid::new_v4();
        let now = Utc::now();

        let mut pending = Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email)
            .body("pending")
            .build()
            .unwrap();
        pending.tenant_id = tenant_id;
        storage.insert_notification(&pending).await.unwrap();

        let mut queued = Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email)
            .body("queued")
            .build()
            .unwrap();
        queued.tenant_id = tenant_id;
        queued.transition(DeliveryStatus::Queued, now).unwrap();
        storage.insert_notification(&queued).await.unwrap();

        let listed = storage.list_by_tenant(tenant_id, Some(DeliveryStatus::Queued), 10).await.unwrap();

        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, queued.id);
    }

    #[tokio::test]
    async fn list_by_tenant_orders_newest_first_and_respects_limit() {
        let (storage, _dir) = test_storage().await;
        let tenant_id = Uuid::new_v4();
        let now = Utc::now();

        let mut ids = Vec::new();
        for i in 0..3 {
            let mut n = Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email)
                .body(format!("body {i}"))
                .build()
                .unwrap();
            n.tenant_id = tenant_id;
            n.created_at = now + Duration::seconds(i);
            n.updated_at = n.created_at;
            ids.push(n.id);
            storage.insert_notification(&n).await.unwrap();
        }

        let listed = storage.list_by_tenant(tenant_id, None, 2).await.unwrap();

        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].id, ids[2]);
        assert_eq!(listed[1].id, ids[1]);
    }

    fn sample_template(tenant_id: Uuid, id: &str) -> Template {
        Template::new(tenant_id, id, "Hi {{name}}").unwrap().with_subject("Hello").unwrap()
    }

    #[tokio::test]
    async fn inserts_and_gets_template() {
        let (storage, _dir) = test_storage().await;
        let tenant_id = Uuid::new_v4();
        let template = sample_template(tenant_id, "welcome");

        storage.insert_template(&template).await.unwrap();
        let fetched = storage.get_template(tenant_id, "welcome").await.unwrap();

        assert_eq!(fetched, template);
    }

    #[tokio::test]
    async fn get_missing_template_errors() {
        let (storage, _dir) = test_storage().await;
        let err = storage.get_template(Uuid::new_v4(), "nope").await.unwrap_err();
        assert!(matches!(err, StorageError::TemplateNotFound { .. }));
    }

    #[tokio::test]
    async fn inserting_the_same_id_twice_for_one_tenant_conflicts() {
        let (storage, _dir) = test_storage().await;
        let tenant_id = Uuid::new_v4();
        storage.insert_template(&sample_template(tenant_id, "welcome")).await.unwrap();

        let err = storage.insert_template(&sample_template(tenant_id, "welcome")).await.unwrap_err();

        assert!(matches!(err, StorageError::TemplateAlreadyExists { .. }));
    }

    #[tokio::test]
    async fn the_same_template_id_is_free_for_another_tenant() {
        let (storage, _dir) = test_storage().await;
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();

        storage.insert_template(&sample_template(tenant_a, "welcome")).await.unwrap();
        storage.insert_template(&sample_template(tenant_b, "welcome")).await.unwrap();

        assert!(storage.get_template(tenant_a, "welcome").await.is_ok());
        assert!(storage.get_template(tenant_b, "welcome").await.is_ok());
    }

    #[tokio::test]
    async fn get_template_does_not_cross_tenants() {
        let (storage, _dir) = test_storage().await;
        let owner = Uuid::new_v4();
        storage.insert_template(&sample_template(owner, "welcome")).await.unwrap();

        let err = storage.get_template(Uuid::new_v4(), "welcome").await.unwrap_err();

        assert!(matches!(err, StorageError::TemplateNotFound { .. }));
    }

    #[tokio::test]
    async fn update_persists_revised_content() {
        let (storage, _dir) = test_storage().await;
        let tenant_id = Uuid::new_v4();
        let mut template = sample_template(tenant_id, "welcome");
        storage.insert_template(&template).await.unwrap();

        template.revise(None, "Goodbye {{name}}".into(), Utc::now()).unwrap();
        storage.update_template(&template).await.unwrap();

        let fetched = storage.get_template(tenant_id, "welcome").await.unwrap();
        assert_eq!(fetched.body, "Goodbye {{name}}");
        assert_eq!(fetched.subject, None);
    }

    #[tokio::test]
    async fn update_missing_template_errors() {
        let (storage, _dir) = test_storage().await;
        let err = storage.update_template(&sample_template(Uuid::new_v4(), "welcome")).await.unwrap_err();
        assert!(matches!(err, StorageError::TemplateNotFound { .. }));
    }

    #[tokio::test]
    async fn list_templates_is_tenant_scoped_and_ordered_by_id() {
        let (storage, _dir) = test_storage().await;
        let tenant_id = Uuid::new_v4();

        for id in ["welcome", "alert", "receipt"] {
            storage.insert_template(&sample_template(tenant_id, id)).await.unwrap();
        }
        storage.insert_template(&sample_template(Uuid::new_v4(), "someone_elses")).await.unwrap();

        let listed = storage.list_templates(tenant_id, 10).await.unwrap();

        let ids: Vec<&str> = listed.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids, vec!["alert", "receipt", "welcome"]);
    }

    #[tokio::test]
    async fn list_templates_respects_limit() {
        let (storage, _dir) = test_storage().await;
        let tenant_id = Uuid::new_v4();
        for id in ["a", "b", "c"] {
            storage.insert_template(&sample_template(tenant_id, id)).await.unwrap();
        }

        let listed = storage.list_templates(tenant_id, 2).await.unwrap();

        assert_eq!(listed.len(), 2);
    }

    #[tokio::test]
    async fn delete_removes_the_template() {
        let (storage, _dir) = test_storage().await;
        let tenant_id = Uuid::new_v4();
        storage.insert_template(&sample_template(tenant_id, "welcome")).await.unwrap();

        storage.delete_template(tenant_id, "welcome").await.unwrap();

        let err = storage.get_template(tenant_id, "welcome").await.unwrap_err();
        assert!(matches!(err, StorageError::TemplateNotFound { .. }));
    }

    #[tokio::test]
    async fn delete_missing_template_errors() {
        let (storage, _dir) = test_storage().await;
        let err = storage.delete_template(Uuid::new_v4(), "welcome").await.unwrap_err();
        assert!(matches!(err, StorageError::TemplateNotFound { .. }));
    }

    #[tokio::test]
    async fn count_by_status_groups_across_tenants() {
        let (storage, _dir) = test_storage().await;
        let tenant_id = Uuid::new_v4();
        let now = Utc::now();

        let mut pending = Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email)
            .body("pending")
            .build()
            .unwrap();
        pending.tenant_id = tenant_id;
        storage.insert_notification(&pending).await.unwrap();

        for _ in 0..2 {
            let mut queued = Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email)
                .body("queued")
                .build()
                .unwrap();
            queued.transition(DeliveryStatus::Queued, now).unwrap();
            storage.insert_notification(&queued).await.unwrap();
        }

        let counts = storage.count_by_status(None).await.unwrap();

        assert_eq!(counts.get(&DeliveryStatus::Pending), Some(&1));
        assert_eq!(counts.get(&DeliveryStatus::Queued), Some(&2));
        assert_eq!(counts.get(&DeliveryStatus::Delivered), None, "empty statuses are absent, not zero");
    }

    #[tokio::test]
    async fn count_by_status_narrows_to_one_tenant() {
        let (storage, _dir) = test_storage().await;
        let mine = Uuid::new_v4();

        let mut ours =
            Notification::builder(mine, Uuid::new_v4(), ChannelKind::Email).body("ours").build().unwrap();
        ours.tenant_id = mine;
        storage.insert_notification(&ours).await.unwrap();

        let mut theirs = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .body("theirs")
            .build()
            .unwrap();
        theirs.tenant_id = Uuid::new_v4();
        storage.insert_notification(&theirs).await.unwrap();

        let counts = storage.count_by_status(Some(mine)).await.unwrap();

        assert_eq!(counts.get(&DeliveryStatus::Pending), Some(&1));
    }

    #[tokio::test]
    async fn count_by_status_on_an_empty_table_is_empty() {
        let (storage, _dir) = test_storage().await;
        assert!(storage.count_by_status(None).await.unwrap().is_empty());
    }

    #[test]
    fn status_str_and_status_from_str_round_trip() {
        for status in DeliveryStatus::all() {
            assert_eq!(status_from_str(status_str(status)).unwrap(), status);
        }
    }

    #[test]
    fn status_from_str_rejects_an_unknown_value() {
        let err = status_from_str("exploded").unwrap_err();
        assert!(matches!(err, StorageError::UnknownStatus(_)));
    }

    fn sample_api_key(tenant_id: Uuid) -> (ApiKey, ns_auth::SecretToken) {
        ApiKey::generate(tenant_id, "ci-deploy", [ns_auth::Scope::Write], Utc::now())
    }

    #[tokio::test]
    async fn api_keys_round_trip_by_id_and_by_digest() {
        let (storage, _dir) = test_storage().await;
        let (key, token) = sample_api_key(Uuid::new_v4());
        storage.insert_api_key(&key).await.unwrap();

        assert_eq!(storage.get_api_key(key.id).await.unwrap(), key);
        assert_eq!(storage.find_api_key_by_digest(&token.digest()).await.unwrap(), key);
    }

    #[tokio::test]
    async fn an_unknown_digest_is_its_own_error() {
        let (storage, _dir) = test_storage().await;
        let err = storage.find_api_key_by_digest("not-a-digest").await.unwrap_err();
        assert!(matches!(err, StorageError::UnknownApiKey));
        assert!(!err.to_string().contains("not-a-digest"), "the presented digest must not reach the message");
    }

    #[tokio::test]
    async fn the_clear_token_is_never_written_to_the_row() {
        let (storage, _dir) = test_storage().await;
        let (key, token) = sample_api_key(Uuid::new_v4());
        storage.insert_api_key(&key).await.unwrap();

        let row = sqlx::query("SELECT data, token_digest FROM api_keys WHERE id = ?")
            .bind(key.id.to_string())
            .fetch_one(&storage.pool)
            .await
            .unwrap();
        let data: String = row.try_get("data").unwrap();
        let digest: String = row.try_get("token_digest").unwrap();
        assert!(!data.contains(token.expose()), "{data}");
        assert!(!digest.contains(token.expose()));
    }

    #[tokio::test]
    async fn revocation_is_persisted() {
        let (storage, _dir) = test_storage().await;
        let (mut key, _) = sample_api_key(Uuid::new_v4());
        storage.insert_api_key(&key).await.unwrap();

        key.revoke(Utc::now());
        storage.update_api_key(&key).await.unwrap();

        assert!(storage.get_api_key(key.id).await.unwrap().is_revoked());
    }

    #[tokio::test]
    async fn a_revoked_key_is_still_resolvable_by_digest() {
        let (storage, _dir) = test_storage().await;
        let (mut key, token) = sample_api_key(Uuid::new_v4());
        storage.insert_api_key(&key).await.unwrap();
        key.revoke(Utc::now());
        storage.update_api_key(&key).await.unwrap();

        let found = storage.find_api_key_by_digest(&token.digest()).await.unwrap();
        assert!(
            found.is_revoked(),
            "storage returns the row and lets ns-auth decide, so 'never real' stays distinguishable \
             from 'turned off' in the operator's logs"
        );
    }

    #[tokio::test]
    async fn updating_a_missing_key_reports_not_found() {
        let (storage, _dir) = test_storage().await;
        let (key, _) = sample_api_key(Uuid::new_v4());
        let err = storage.update_api_key(&key).await.unwrap_err();
        assert!(matches!(err, StorageError::ApiKeyNotFound(id) if id == key.id));
    }

    #[tokio::test]
    async fn listing_is_scoped_to_one_tenant_and_newest_first() {
        let (storage, _dir) = test_storage().await;
        let tenant = Uuid::new_v4();
        let other_tenant = Uuid::new_v4();

        let (mut older, _) = sample_api_key(tenant);
        older.created_at = Utc::now() - Duration::hours(1);
        older.name = "older".into();
        let (mut newer, _) = sample_api_key(tenant);
        newer.name = "newer".into();
        let (theirs, _) = sample_api_key(other_tenant);

        storage.insert_api_key(&older).await.unwrap();
        storage.insert_api_key(&newer).await.unwrap();
        storage.insert_api_key(&theirs).await.unwrap();

        let listed = storage.list_api_keys(tenant, 10).await.unwrap();
        let names: Vec<_> = listed.iter().map(|key| key.name.as_str()).collect();
        assert_eq!(names, vec!["newer", "older"]);
    }

    #[tokio::test]
    async fn listing_respects_its_limit() {
        let (storage, _dir) = test_storage().await;
        let tenant = Uuid::new_v4();
        for _ in 0..5 {
            let (key, _) = sample_api_key(tenant);
            storage.insert_api_key(&key).await.unwrap();
        }
        assert_eq!(storage.list_api_keys(tenant, 2).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_duplicate_digest_is_rejected_by_the_unique_index() {
        let (storage, _dir) = test_storage().await;
        let (first, _) = sample_api_key(Uuid::new_v4());
        let mut clash = first.clone();
        clash.id = Uuid::new_v4();

        storage.insert_api_key(&first).await.unwrap();
        let err = storage.insert_api_key(&clash).await.unwrap_err();
        assert!(matches!(err, StorageError::Database(_)), "{err:?}");
    }
}

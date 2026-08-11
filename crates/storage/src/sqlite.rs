use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use ns_core::{DeliveryStatus, Notification, Recipient};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use uuid::Uuid;

use crate::error::{StorageError, StorageResult};
use crate::repository::{NotificationRepository, RecipientRepository};

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
    }
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
        sqlx::query(
            "INSERT INTO notifications
                (id, tenant_id, status, priority, scheduled_for, created_at, updated_at, data)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(notification.id.to_string())
        .bind(notification.tenant_id.to_string())
        .bind(status_str(notification.status))
        .bind(notification.priority.as_u8() as i64)
        .bind(notification.scheduled_for.map(to_ts))
        .bind(to_ts(notification.created_at))
        .bind(to_ts(notification.updated_at))
        .bind(data)
        .execute(&self.pool)
        .await?;
        Ok(())
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

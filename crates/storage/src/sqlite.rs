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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use ns_core::{ChannelKind, DeliveryStatus, Priority, Recipient, RecipientAddress};

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
        Recipient::new(Uuid::new_v4())
            .with_display_name("Ada Lovelace")
            .with_address(RecipientAddress::Email("ada@example.com".into()))
            .unwrap()
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
}

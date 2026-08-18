use std::collections::HashMap;
use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use ns_core::{DeliveryStatus, Notification, Recipient};
use ns_templates::Template;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use uuid::Uuid;

use crate::error::{StorageError, StorageResult};
use crate::repository::{NotificationRepository, RecipientRepository, TemplateRepository};

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
}

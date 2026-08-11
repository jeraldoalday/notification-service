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

use thiserror::Error;
use uuid::Uuid;

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("notification {0} not found")]
    NotificationNotFound(Uuid),

    #[error("recipient {0} not found")]
    RecipientNotFound(Uuid),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("failed to (de)serialize stored record: {0}")]
    Serialization(#[from] serde_json::Error),
}

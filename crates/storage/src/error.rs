use thiserror::Error;
use uuid::Uuid;

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("notification {0} not found")]
    NotificationNotFound(Uuid),

    #[error("recipient {0} not found")]
    RecipientNotFound(Uuid),

    #[error("template `{id}` not found for tenant {tenant_id}")]
    TemplateNotFound { tenant_id: Uuid, id: String },

    #[error("template `{id}` already exists for tenant {tenant_id}")]
    TemplateAlreadyExists { tenant_id: Uuid, id: String },

    #[error("api key {0} not found")]
    ApiKeyNotFound(Uuid),

    /// Deliberately carries no payload. This is the error a token lookup
    /// returns, and the only thing it could carry is the digest of the
    /// token that was presented -- which is a value derived from a live
    /// secret and has no business in an error message or a log line.
    #[error("no api key matches the presented token")]
    UnknownApiKey,

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("stored notification has unknown status `{0}`")]
    UnknownStatus(String),

    #[error("failed to (de)serialize stored record: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub mod error;
pub mod repository;

pub use error::{StorageError, StorageResult};
pub use repository::{NotificationRepository, RecipientRepository};

use thiserror::Error;

pub type QueueResult<T> = Result<T, QueueError>;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error(transparent)]
    Storage(#[from] ns_storage::StorageError),

    #[error(transparent)]
    Core(#[from] ns_core::CoreError),
}

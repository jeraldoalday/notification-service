use thiserror::Error;
use uuid::Uuid;

use ns_core::ChannelKind;

pub type WorkerResult<T> = Result<T, WorkerError>;

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    Queue(#[from] ns_queue::QueueError),

    #[error(transparent)]
    Storage(#[from] ns_storage::StorageError),

    #[error(transparent)]
    Core(#[from] ns_core::CoreError),

    #[error("no channel registered for {0}")]
    UnregisteredChannel(ChannelKind),

    #[error("recipient {recipient_id} has no address for channel {channel}")]
    MissingRecipientAddress { recipient_id: Uuid, channel: ChannelKind },
}

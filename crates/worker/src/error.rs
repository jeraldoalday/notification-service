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

    #[error(transparent)]
    Template(#[from] ns_templates::TemplateError),

    /// A notification asked for a template but this worker was built
    /// without a template repository, so there's nothing to render from.
    /// Distinct from a missing template: the deployment is misconfigured,
    /// not the notification.
    #[error("notification references template `{template_id}` but no template store is configured")]
    TemplatesNotConfigured { template_id: String },
}

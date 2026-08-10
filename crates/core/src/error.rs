use thiserror::Error;

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("validation failed: {0}")]
    Validation(String),

    #[error("recipient is missing an address for channel {0:?}")]
    MissingAddress(crate::channel::ChannelKind),

    #[error("invalid state transition: cannot move from {from:?} to {to:?}")]
    InvalidTransition {
        from: crate::status::DeliveryStatus,
        to: crate::status::DeliveryStatus,
    },

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

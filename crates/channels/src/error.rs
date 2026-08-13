use thiserror::Error;

pub type ChannelResult<T> = Result<T, ChannelError>;

#[derive(Debug, Error)]
pub enum ChannelError {
    #[error("recipient address does not match this channel")]
    AddressMismatch,

    #[error("provider request failed: {0}")]
    Transport(String),

    #[error("provider rejected the request: {0}")]
    Rejected(String),
}

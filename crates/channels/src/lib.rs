pub mod channel;
pub mod error;
pub mod webhook;

pub use channel::NotificationChannel;
pub use error::{ChannelError, ChannelResult};
pub use webhook::WebhookChannel;

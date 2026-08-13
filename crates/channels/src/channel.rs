use async_trait::async_trait;
use ns_core::{Notification, RecipientAddress};

use crate::error::ChannelResult;

/// Sends a single notification to a channel-specific recipient address.
/// Implemented once per delivery channel (email, SMS, push, webhook); the
/// worker (planned `ns-worker`) picks the implementation matching
/// `Notification::channel` and dispatches through this trait so it never
/// needs to know about provider-specific request/response shapes.
///
/// Callers are expected to look up `address` via `Recipient::address_for`
/// using `notification.channel`, so it already matches this channel's
/// `RecipientAddress` variant; implementations return
/// `ChannelError::AddressMismatch` rather than panicking if that
/// invariant is violated.
#[async_trait]
pub trait NotificationChannel: Send + Sync {
    async fn send(&self, notification: &Notification, address: &RecipientAddress) -> ChannelResult<()>;
}

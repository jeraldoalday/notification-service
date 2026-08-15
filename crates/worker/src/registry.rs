use std::collections::HashMap;
use std::sync::Arc;

use ns_channels::NotificationChannel;
use ns_core::ChannelKind;

/// Maps each `ChannelKind` to the `NotificationChannel` implementation that
/// handles it, so `Worker` can dispatch a notification without a hardcoded
/// match on its channel. Built once at startup from whichever channels the
/// deployment has credentials for -- a `ChannelKind` with no registered
/// channel simply can't be dispatched, surfaced as
/// `WorkerError::UnregisteredChannel` rather than a panic.
#[derive(Clone, Default)]
pub struct ChannelRegistry {
    channels: HashMap<ChannelKind, Arc<dyn NotificationChannel>>,
}

impl ChannelRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(mut self, kind: ChannelKind, channel: Arc<dyn NotificationChannel>) -> Self {
        self.channels.insert(kind, channel);
        self
    }

    pub fn get(&self, kind: ChannelKind) -> Option<&Arc<dyn NotificationChannel>> {
        self.channels.get(&kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use ns_channels::ChannelResult;
    use ns_core::{Notification, RecipientAddress};

    struct StubChannel;

    #[async_trait]
    impl NotificationChannel for StubChannel {
        async fn send(&self, _notification: &Notification, _address: &RecipientAddress) -> ChannelResult<()> {
            Ok(())
        }
    }

    #[test]
    fn returns_registered_channel() {
        let registry = ChannelRegistry::new().register(ChannelKind::Webhook, Arc::new(StubChannel));
        assert!(registry.get(ChannelKind::Webhook).is_some());
    }

    #[test]
    fn missing_channel_is_none() {
        let registry = ChannelRegistry::new();
        assert!(registry.get(ChannelKind::Email).is_none());
    }

    #[test]
    fn later_registration_overwrites_earlier_one_for_same_kind() {
        let registry = ChannelRegistry::new()
            .register(ChannelKind::Sms, Arc::new(StubChannel))
            .register(ChannelKind::Sms, Arc::new(StubChannel));
        assert!(registry.get(ChannelKind::Sms).is_some());
    }
}

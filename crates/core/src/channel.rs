use serde::{Deserialize, Serialize};
use std::fmt;

/// The delivery channel a notification is sent through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelKind {
    Email,
    Sms,
    Push,
    Webhook,
}

impl ChannelKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChannelKind::Email => "email",
            ChannelKind::Sms => "sms",
            ChannelKind::Push => "push",
            ChannelKind::Webhook => "webhook",
        }
    }

    pub fn all() -> [ChannelKind; 4] {
        [ChannelKind::Email, ChannelKind::Sms, ChannelKind::Push, ChannelKind::Webhook]
    }
}

impl fmt::Display for ChannelKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Marker trait implemented by concrete channel dispatchers (email, sms, ...).
/// Kept minimal in `ns-core`; the async `send` behavior lives in downstream
/// crates that can depend on network clients without dragging that weight
/// into the core domain crate.
pub trait Channel {
    fn kind(&self) -> ChannelKind;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_matches_as_str() {
        for kind in ChannelKind::all() {
            assert_eq!(kind.to_string(), kind.as_str());
        }
    }

    #[test]
    fn serde_round_trip() {
        for kind in ChannelKind::all() {
            let json = serde_json::to_string(&kind).unwrap();
            let back: ChannelKind = serde_json::from_str(&json).unwrap();
            assert_eq!(kind, back);
        }
    }

    #[test]
    fn serializes_snake_case() {
        let json = serde_json::to_string(&ChannelKind::Webhook).unwrap();
        assert_eq!(json, "\"webhook\"");
    }
}

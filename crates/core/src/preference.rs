use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::channel::ChannelKind;
use crate::topic::Topic;

/// The `channel` value a preference covering every channel is stored
/// under.
///
/// A wildcard needs a spelling, because the storage key has to be a
/// concrete string and `NULL` in a primary key is not a value SQLite will
/// treat as equal to itself. `*` is safe to use for it precisely because
/// [`Topic`](crate::topic::Topic) and `ChannelKind` both refuse it: no
/// real channel can ever collide with the wildcard.
pub const ANY_CHANNEL_KEY: &str = "*";

/// What a recipient has said about one topic.
///
/// Two values rather than a `bool`, and rather than three. A `bool` field
/// called `blocked` reads backwards at every call site that asks whether
/// something may be sent; a third "unset" variant would duplicate the
/// `Option` that the absence of a row already expresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreferenceSetting {
    /// Send it. Worth storing explicitly rather than deleting the row,
    /// because it is how a recipient carves an exception out of a broader
    /// block -- "nothing about orders except the SMS" is one blocking row
    /// and one allowing one.
    Allowed,
    /// Don't send it. The notification is not retried and not failed: the
    /// recipient's answer will be the same on every attempt.
    Blocked,
}

impl PreferenceSetting {
    pub fn as_str(&self) -> &'static str {
        match self {
            PreferenceSetting::Allowed => "allowed",
            PreferenceSetting::Blocked => "blocked",
        }
    }

    pub fn all() -> [PreferenceSetting; 2] {
        [PreferenceSetting::Allowed, PreferenceSetting::Blocked]
    }

    pub fn is_blocked(&self) -> bool {
        matches!(self, PreferenceSetting::Blocked)
    }
}

impl std::fmt::Display for PreferenceSetting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for PreferenceSetting {
    type Err = crate::error::CoreError;

    fn from_str(value: &str) -> crate::error::CoreResult<Self> {
        PreferenceSetting::all().into_iter().find(|setting| setting.as_str() == value).ok_or_else(|| {
            crate::error::CoreError::Validation(format!("unknown preference setting `{value}`"))
        })
    }
}

/// One recipient's stated choice about one topic, optionally narrowed to
/// one channel.
///
/// This is the *person's* half of the delivery decision, and it is
/// deliberately a different concept from a
/// [`Suppression`](crate::suppression::Suppression) even though both stop
/// a send. A suppression is keyed by address and is usually the provider
/// or an operator talking: this inbox bounced, this number complained,
/// nothing may go to it again. A preference is keyed by recipient and
/// topic and is the recipient talking: keep sending me receipts, stop
/// sending me the newsletter. Collapsing them would mean a recipient
/// unsubscribing from one topic could only be expressed by making their
/// address undeliverable for everything, which is exactly the outcome
/// topics exist to avoid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Preference {
    pub tenant_id: Uuid,
    pub recipient_id: Uuid,
    pub topic: Topic,
    /// The channel this covers, or `None` for every channel.
    ///
    /// The wildcard is what makes an ordinary unsubscribe one row instead
    /// of one row per channel -- and, more importantly, one row that
    /// keeps holding when a fifth channel is added to the service later.
    /// A recipient who said "nothing about marketing" in 2024 meant it
    /// about whatever the service learns to send in 2026.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<ChannelKind>,
    pub setting: PreferenceSetting,
    pub updated_at: DateTime<Utc>,
}

impl Preference {
    pub fn new(
        tenant_id: Uuid,
        recipient_id: Uuid,
        topic: Topic,
        channel: Option<ChannelKind>,
        setting: PreferenceSetting,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self { tenant_id, recipient_id, topic, channel, setting, updated_at }
    }

    /// A preference blocking `topic` on every channel: the shape an
    /// ordinary "unsubscribe me from this" produces.
    pub fn block(tenant_id: Uuid, recipient_id: Uuid, topic: Topic, updated_at: DateTime<Utc>) -> Self {
        Self::new(tenant_id, recipient_id, topic, None, PreferenceSetting::Blocked, updated_at)
    }

    /// A preference allowing `topic` on one channel: the shape an
    /// exception to a broader block produces.
    pub fn allow_on(
        tenant_id: Uuid,
        recipient_id: Uuid,
        topic: Topic,
        channel: ChannelKind,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self::new(tenant_id, recipient_id, topic, Some(channel), PreferenceSetting::Allowed, updated_at)
    }

    /// The string this preference's channel is stored and matched under.
    /// Kept here rather than at each query site so the wildcard has one
    /// spelling.
    pub fn channel_key(&self) -> &str {
        channel_key(self.channel)
    }

    /// Whether this preference has anything to say about a send on
    /// `channel`. A wildcard preference speaks for every channel; a
    /// channel-specific one speaks only for its own.
    pub fn covers(&self, channel: ChannelKind) -> bool {
        match self.channel {
            Some(pinned) => pinned == channel,
            None => true,
        }
    }

    /// How specific this preference is, used to break ties between two
    /// that both cover the same send. Higher wins.
    fn specificity(&self) -> u8 {
        match self.channel {
            Some(_) => 1,
            None => 0,
        }
    }
}

/// The stored spelling of an optional channel. See [`ANY_CHANNEL_KEY`].
pub fn channel_key(channel: Option<ChannelKind>) -> &'static str {
    match channel {
        Some(channel) => channel.as_str(),
        None => ANY_CHANNEL_KEY,
    }
}

/// Every preference one recipient holds for one topic, resolved together.
///
/// The set is the unit rather than a single row because answering "may
/// this be sent?" needs all of them: a wildcard block and a channel
/// exception are two rows that only mean something read as a pair. Making
/// that the type the delivery path receives means the precedence rule
/// lives here, in a pure function with no I/O, rather than being
/// re-derived by the worker and by the API and drifting between the two.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreferenceSet {
    entries: Vec<Preference>,
}

impl PreferenceSet {
    pub fn new(entries: Vec<Preference>) -> Self {
        Self { entries }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn entries(&self) -> &[Preference] {
        &self.entries
    }

    /// The preference that decides a send on `channel`, if the recipient
    /// has expressed one.
    ///
    /// Most specific wins: a preference naming this channel beats a
    /// wildcard one. That is the only ordering that lets "nothing about
    /// orders except the text messages" be said at all -- under the
    /// opposite rule the wildcard block would swallow its own exception.
    ///
    /// Among rows of equal specificity the most recently updated wins.
    /// There should never be two -- storage keys them by (recipient,
    /// topic, channel) -- but a set assembled by hand, or read across a
    /// migration, shouldn't resolve by whichever row happened to be read
    /// first.
    pub fn deciding(&self, channel: ChannelKind) -> Option<&Preference> {
        self.entries
            .iter()
            .filter(|preference| preference.covers(channel))
            .max_by(|a, b| a.specificity().cmp(&b.specificity()).then(a.updated_at.cmp(&b.updated_at)))
    }

    /// What the recipient has said about being sent this topic on
    /// `channel`.
    ///
    /// Defaults to [`PreferenceSetting::Allowed`] when they have said
    /// nothing. This is an opt-*out* model on purpose: under an opt-in
    /// one, the first message a tenant ever sends under a new topic would
    /// go nowhere, and every existing recipient would have to be migrated
    /// into consent they had already given by other means.
    pub fn setting_for(&self, channel: ChannelKind) -> PreferenceSetting {
        self.deciding(channel).map(|preference| preference.setting).unwrap_or(PreferenceSetting::Allowed)
    }

    /// Whether a send of this topic on `channel` may go ahead.
    pub fn allows(&self, channel: ChannelKind) -> bool {
        !self.setting_for(channel).is_blocked()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic() -> Topic {
        Topic::new("order-updates").unwrap()
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + seconds, 0).unwrap()
    }

    fn blocked_everywhere() -> Preference {
        Preference::block(Uuid::new_v4(), Uuid::new_v4(), topic(), at(0))
    }

    #[test]
    fn settings_parse_back_from_their_own_spelling() {
        for setting in PreferenceSetting::all() {
            assert_eq!(setting.as_str().parse::<PreferenceSetting>().unwrap(), setting);
            assert_eq!(serde_json::to_string(&setting).unwrap(), format!("\"{}\"", setting.as_str()));
            assert_eq!(setting.to_string(), setting.as_str());
        }
        assert!("maybe".parse::<PreferenceSetting>().is_err());
    }

    #[test]
    fn a_wildcard_preference_covers_every_channel() {
        let preference = blocked_everywhere();
        assert_eq!(preference.channel_key(), ANY_CHANNEL_KEY);
        for channel in ChannelKind::all() {
            assert!(preference.covers(channel));
        }
    }

    #[test]
    fn a_channel_preference_covers_only_its_own() {
        let preference =
            Preference::allow_on(Uuid::new_v4(), Uuid::new_v4(), topic(), ChannelKind::Sms, at(0));
        assert_eq!(preference.channel_key(), "sms");
        assert!(preference.covers(ChannelKind::Sms));
        assert!(!preference.covers(ChannelKind::Email));
    }

    #[test]
    fn silence_means_yes() {
        // Opt-out, not opt-in: a topic nobody has an opinion about is
        // deliverable, or the first message under a new topic would go
        // nowhere.
        let empty = PreferenceSet::default();
        assert!(empty.is_empty());
        for channel in ChannelKind::all() {
            assert_eq!(empty.setting_for(channel), PreferenceSetting::Allowed);
            assert!(empty.allows(channel));
        }
    }

    #[test]
    fn a_wildcard_block_stops_every_channel() {
        let set = PreferenceSet::new(vec![blocked_everywhere()]);
        assert_eq!(set.len(), 1);
        for channel in ChannelKind::all() {
            assert!(!set.allows(channel), "{channel} should be blocked");
        }
    }

    #[test]
    fn a_channel_exception_survives_a_wildcard_block() {
        // The rule the whole precedence scheme exists for: "nothing about
        // orders except the text messages" has to be sayable.
        let tenant = Uuid::new_v4();
        let recipient = Uuid::new_v4();
        let set = PreferenceSet::new(vec![
            Preference::block(tenant, recipient, topic(), at(0)),
            Preference::allow_on(tenant, recipient, topic(), ChannelKind::Sms, at(1)),
        ]);

        assert!(set.allows(ChannelKind::Sms));
        assert!(!set.allows(ChannelKind::Email));
        assert!(!set.allows(ChannelKind::Push));
    }

    #[test]
    fn a_channel_block_survives_a_wildcard_allow() {
        // And the mirror image, so specificity is genuinely the rule
        // rather than "blocks win".
        let tenant = Uuid::new_v4();
        let recipient = Uuid::new_v4();
        let set = PreferenceSet::new(vec![
            Preference::new(tenant, recipient, topic(), None, PreferenceSetting::Allowed, at(0)),
            Preference::new(
                tenant,
                recipient,
                topic(),
                Some(ChannelKind::Email),
                PreferenceSetting::Blocked,
                at(1),
            ),
        ]);

        assert!(!set.allows(ChannelKind::Email));
        assert!(set.allows(ChannelKind::Sms));
    }

    #[test]
    fn a_more_specific_row_wins_however_stale_it_is() {
        // Specificity is checked before recency, so a wildcard written
        // later cannot quietly swallow a channel exception written
        // earlier.
        let tenant = Uuid::new_v4();
        let recipient = Uuid::new_v4();
        let set = PreferenceSet::new(vec![
            Preference::allow_on(tenant, recipient, topic(), ChannelKind::Sms, at(0)),
            Preference::block(tenant, recipient, topic(), at(9_000)),
        ]);

        assert!(set.allows(ChannelKind::Sms));
    }

    #[test]
    fn equally_specific_rows_resolve_by_recency() {
        // Storage cannot produce this pair -- the key is (recipient,
        // topic, channel) -- but a set built by hand shouldn't resolve to
        // whichever row was read first.
        let tenant = Uuid::new_v4();
        let recipient = Uuid::new_v4();
        let older = Preference::new(tenant, recipient, topic(), None, PreferenceSetting::Blocked, at(0));
        let newer = Preference::new(tenant, recipient, topic(), None, PreferenceSetting::Allowed, at(60));

        assert!(PreferenceSet::new(vec![older.clone(), newer.clone()]).allows(ChannelKind::Email));
        assert!(PreferenceSet::new(vec![newer, older]).allows(ChannelKind::Email));
    }

    #[test]
    fn the_deciding_preference_is_reported_not_just_the_answer() {
        // The worker writes the reason into the delivery log, so it needs
        // the row rather than the verdict alone.
        let tenant = Uuid::new_v4();
        let recipient = Uuid::new_v4();
        let set = PreferenceSet::new(vec![
            Preference::block(tenant, recipient, topic(), at(0)),
            Preference::allow_on(tenant, recipient, topic(), ChannelKind::Sms, at(1)),
        ]);

        assert_eq!(set.deciding(ChannelKind::Sms).unwrap().channel, Some(ChannelKind::Sms));
        assert_eq!(set.deciding(ChannelKind::Email).unwrap().channel, None);
        assert!(PreferenceSet::default().deciding(ChannelKind::Email).is_none());
    }

    #[test]
    fn round_trips_through_serde() {
        let preference = blocked_everywhere();
        let json = serde_json::to_string(&preference).unwrap();
        assert!(!json.contains("\"channel\""), "a wildcard writes no channel field");
        assert_eq!(serde_json::from_str::<Preference>(&json).unwrap(), preference);

        let pinned = Preference::allow_on(Uuid::new_v4(), Uuid::new_v4(), topic(), ChannelKind::Push, at(0));
        let json = serde_json::to_string(&pinned).unwrap();
        assert_eq!(serde_json::from_str::<Preference>(&json).unwrap(), pinned);
    }

    #[test]
    fn channel_key_spells_the_wildcard_the_same_way_everywhere() {
        assert_eq!(channel_key(None), ANY_CHANNEL_KEY);
        for channel in ChannelKind::all() {
            assert_eq!(channel_key(Some(channel)), channel.as_str());
            assert_ne!(channel.as_str(), ANY_CHANNEL_KEY);
        }
    }
}

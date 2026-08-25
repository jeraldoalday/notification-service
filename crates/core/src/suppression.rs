use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::channel::ChannelKind;
use crate::error::{CoreError, CoreResult};
use crate::recipient::RecipientAddress;

/// Longest operator note kept on a suppression.
const MAX_NOTE_LEN: usize = 500;

/// Why an address stopped being deliverable.
///
/// Recorded because the reasons carry different weight: an operator
/// clearing out stale entries can safely revisit `Bounced` addresses, and
/// must not touch `Unsubscribed` or `Complaint` ones. Collapsing them into
/// a single "suppressed" flag would make that distinction unrecoverable
/// the moment it was needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuppressionReason {
    /// The recipient asked not to be contacted.
    Unsubscribed,
    /// The provider reported the address as undeliverable.
    Bounced,
    /// The recipient reported the message as spam. Worse than an
    /// unsubscribe: continuing to send costs the sending domain its
    /// reputation, not just the recipient's patience.
    Complaint,
    /// An operator added it by hand.
    Manual,
}

impl SuppressionReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            SuppressionReason::Unsubscribed => "unsubscribed",
            SuppressionReason::Bounced => "bounced",
            SuppressionReason::Complaint => "complaint",
            SuppressionReason::Manual => "manual",
        }
    }

    pub fn all() -> [SuppressionReason; 4] {
        use SuppressionReason::*;
        [Unsubscribed, Bounced, Complaint, Manual]
    }
}

impl std::fmt::Display for SuppressionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for SuppressionReason {
    type Err = CoreError;

    fn from_str(value: &str) -> CoreResult<Self> {
        SuppressionReason::all()
            .into_iter()
            .find(|reason| reason.as_str() == value)
            .ok_or_else(|| CoreError::Validation(format!("unknown suppression reason `{value}`")))
    }
}

/// One address a tenant may no longer send to.
///
/// Keyed by *address* rather than by recipient, which is the distinction
/// that makes this worth having as its own concept. A person unsubscribing
/// is unsubscribing an inbox, and the same inbox may be registered under
/// several recipient ids -- one per integration that ever imported it. A
/// suppression attached to a recipient would let the next import quietly
/// undo it; attached to the address, it holds no matter which recipient
/// row a later notification names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Suppression {
    pub tenant_id: Uuid,
    pub address: RecipientAddress,
    pub reason: SuppressionReason,
    /// Free text from whoever added it -- a bounce code, a ticket
    /// reference. Truncated, because a webhook handler feeding provider
    /// output straight in shouldn't be able to write unbounded rows.
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl Suppression {
    pub fn new(
        tenant_id: Uuid,
        address: RecipientAddress,
        reason: SuppressionReason,
        created_at: DateTime<Utc>,
    ) -> CoreResult<Self> {
        // Validated on the way in for the same reason a recipient's
        // address is: an address that could never have been sent to also
        // can't be meaningfully suppressed, and letting it in would leave
        // a row that matches nothing and can't be explained.
        address.validate()?;
        Ok(Self { tenant_id, address, reason, note: None, created_at })
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        let note: String = note.into();
        let note = match note.char_indices().nth(MAX_NOTE_LEN) {
            Some((cut, _)) => note[..cut].to_string(),
            None => note,
        };
        self.note = Some(note);
        self
    }

    /// The channel this suppression applies to.
    ///
    /// Suppression is per-address, and an address belongs to exactly one
    /// channel, so unsubscribing an email address says nothing about the
    /// same person's phone number. That is the behaviour a recipient
    /// expects: "stop emailing me" is not "stop sending me one-time
    /// passcodes".
    pub fn channel(&self) -> ChannelKind {
        self.address.channel_kind()
    }

    /// The stable key this suppression is stored and looked up under.
    pub fn key(&self) -> String {
        self.address.suppression_key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn email() -> RecipientAddress {
        RecipientAddress::Email("ada@example.com".into())
    }

    #[test]
    fn as_str_matches_the_serde_representation() {
        for reason in SuppressionReason::all() {
            let json = serde_json::to_string(&reason).unwrap();
            assert_eq!(json, format!("\"{}\"", reason.as_str()));
            assert_eq!(reason.to_string(), reason.as_str());
        }
    }

    #[test]
    fn reasons_parse_back_from_their_own_spelling() {
        for reason in SuppressionReason::all() {
            assert_eq!(reason.as_str().parse::<SuppressionReason>().unwrap(), reason);
        }
        assert!("shouting".parse::<SuppressionReason>().is_err());
    }

    #[test]
    fn a_suppression_takes_its_channel_from_its_address() {
        let suppression =
            Suppression::new(Uuid::new_v4(), email(), SuppressionReason::Unsubscribed, Utc::now()).unwrap();
        assert_eq!(suppression.channel(), ChannelKind::Email);

        let phone = RecipientAddress::Phone("+15555550100".into());
        let suppression =
            Suppression::new(Uuid::new_v4(), phone, SuppressionReason::Complaint, Utc::now()).unwrap();
        assert_eq!(suppression.channel(), ChannelKind::Sms);
    }

    #[test]
    fn an_unusable_address_cannot_be_suppressed() {
        let err = Suppression::new(
            Uuid::new_v4(),
            RecipientAddress::Email("not-an-email".into()),
            SuppressionReason::Bounced,
            Utc::now(),
        )
        .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn a_note_is_optional_and_bounded() {
        let suppression =
            Suppression::new(Uuid::new_v4(), email(), SuppressionReason::Bounced, Utc::now()).unwrap();
        assert_eq!(suppression.note, None);

        let long = suppression.with_note("x".repeat(MAX_NOTE_LEN * 2));
        assert_eq!(long.note.unwrap().chars().count(), MAX_NOTE_LEN);
    }

    #[test]
    fn a_multibyte_note_is_truncated_on_a_character_boundary() {
        let suppression =
            Suppression::new(Uuid::new_v4(), email(), SuppressionReason::Bounced, Utc::now()).unwrap();
        let note = suppression.with_note("☃".repeat(MAX_NOTE_LEN + 5)).note.unwrap();
        assert_eq!(note.chars().count(), MAX_NOTE_LEN);
    }

    #[test]
    fn the_storage_key_is_the_addresss_own() {
        let suppression = Suppression::new(
            Uuid::new_v4(),
            RecipientAddress::Email("Ada@Example.COM".into()),
            SuppressionReason::Unsubscribed,
            Utc::now(),
        )
        .unwrap();
        assert_eq!(suppression.key(), "ada@example.com");
    }

    #[test]
    fn round_trips_through_serde() {
        let suppression = Suppression::new(Uuid::new_v4(), email(), SuppressionReason::Complaint, Utc::now())
            .unwrap()
            .with_note("reported as spam via provider webhook");
        let json = serde_json::to_string(&suppression).unwrap();
        assert_eq!(serde_json::from_str::<Suppression>(&json).unwrap(), suppression);
    }
}

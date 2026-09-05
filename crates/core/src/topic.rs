use serde::{Deserialize, Serialize};

use crate::error::{CoreError, CoreResult};

/// Longest topic the service will accept.
///
/// A topic is a slug an integration writes into its code and a person
/// eventually reads off a preference centre, so it needs to be short
/// enough to be a label rather than a sentence. Sixty-four characters is
/// longer than any of the shapes that turn up in practice
/// (`order-updates`, `security.alerts`, `weekly_digest`) and keeps the
/// column it is keyed by small.
pub const MAX_TOPIC_LEN: usize = 64;

/// What a notification is about, from the recipient's point of view.
///
/// A channel says *how* a message travels and a template says how it is
/// worded; neither says anything a person could reasonably have an
/// opinion about. "Stop emailing me" is almost never what someone means:
/// they mean stop emailing them about the thing they are tired of, while
/// still telling them their password changed. A topic is the unit that
/// distinction needs, and it is the only thing
/// [`Preference`](crate::preference::Preference) is keyed by.
///
/// Topics are strings the tenant chooses rather than an enum this crate
/// defines. What one deployment calls `marketing` another calls
/// `newsletter`, and a fixed list would either be wrong for everyone or
/// grow a `Other(String)` variant that is this type with extra steps.
///
/// # Validation
///
/// Validated, never normalised -- the same decision, and for a sharper
/// reason, as [`IdempotencyKey`](crate::idempotency::IdempotencyKey).
/// Preferences are stored keyed by topic and looked up by topic on the
/// delivery path. If `Marketing` were quietly folded to `marketing` in one
/// place and not another, the mismatch would not surface as an error: it
/// would surface as a message delivered to somebody who had opted out of
/// exactly that message. Refusing the odd spelling while the caller is
/// still on the line means the topic stored and the topic matched are
/// always the same bytes.
///
/// Uppercase is therefore refused rather than lowercased, which is the one
/// rule callers are most likely to trip over. Accepting it and folding it
/// would be friendlier right up until the first time it silenced a
/// notification nobody meant to silence.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Topic(String);

impl Topic {
    /// Validates `raw` as a topic.
    ///
    /// Accepts a non-empty run of at most [`MAX_TOPIC_LEN`] lowercase
    /// ASCII letters, digits, `-`, `_` and `.`, beginning with a letter or
    /// a digit. The leading-character rule exists so a topic can never be
    /// confused with the `*` this crate uses to mean "every channel", and
    /// so a topic never begins with punctuation that a URL path or a shell
    /// would want to interpret.
    pub fn new(raw: impl Into<String>) -> CoreResult<Self> {
        let raw: String = raw.into();

        if raw.is_empty() {
            return Err(CoreError::Validation("topic cannot be empty".into()));
        }
        if raw.len() > MAX_TOPIC_LEN {
            return Err(CoreError::Validation(format!(
                "topic must be at most {MAX_TOPIC_LEN} characters, got {}",
                raw.len()
            )));
        }
        if let Some(bad) = raw.chars().find(|c| !matches!(c, 'a'..='z' | '0'..='9' | '-' | '_' | '.')) {
            return Err(CoreError::Validation(format!(
                "topic must be lowercase letters, digits, `-`, `_` or `.`, found {bad:?}"
            )));
        }
        if !raw.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit()) {
            return Err(CoreError::Validation("topic must start with a letter or a digit".into()));
        }

        Ok(Self(raw))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for Topic {
    type Err = CoreError;

    fn from_str(raw: &str) -> CoreResult<Self> {
        Topic::new(raw)
    }
}

impl TryFrom<String> for Topic {
    type Error = CoreError;

    fn try_from(raw: String) -> CoreResult<Self> {
        Topic::new(raw)
    }
}

impl From<Topic> for String {
    fn from(topic: Topic) -> Self {
        topic.0
    }
}

impl AsRef<str> for Topic {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_the_shapes_integrations_actually_use() {
        for raw in ["order-updates", "security.alerts", "weekly_digest", "billing", "v2.receipts"] {
            assert_eq!(Topic::new(raw).unwrap().as_str(), raw);
        }
    }

    #[test]
    fn rejects_an_empty_topic() {
        assert!(matches!(Topic::new("").unwrap_err(), CoreError::Validation(_)));
    }

    #[test]
    fn rejects_a_topic_past_the_length_bound() {
        let err = Topic::new("a".repeat(MAX_TOPIC_LEN + 1)).unwrap_err();
        assert!(err.to_string().contains(&MAX_TOPIC_LEN.to_string()), "{err}");
        assert!(Topic::new("a".repeat(MAX_TOPIC_LEN)).is_ok(), "the bound itself is fine");
    }

    #[test]
    fn refuses_uppercase_rather_than_folding_it() {
        // The important one. Folding would mean the topic stored is not
        // the topic presented, and a preference keyed by one spelling
        // would silently fail to match a notification carrying the other
        // -- which shows up as a message delivered to someone who opted
        // out of it, not as an error anybody sees.
        let err = Topic::new("Marketing").unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
        assert!(err.to_string().contains("lowercase"), "{err}");
    }

    #[test]
    fn refuses_whitespace_and_punctuation_that_would_need_escaping() {
        for raw in ["order updates", "orders/receipts", "orders?x=1", "orders#1", "orders\n"] {
            assert!(Topic::new(raw).is_err(), "wrongly accepted {raw:?}");
        }
    }

    #[test]
    fn refuses_a_leading_separator() {
        // `*` is the wildcard a preference uses to mean "every channel",
        // and a topic that could start with punctuation is a topic that
        // could one day be mistaken for a wildcard of some sort.
        for raw in ["-orders", "_orders", ".orders", "*"] {
            assert!(Topic::new(raw).is_err(), "wrongly accepted {raw:?}");
        }
    }

    #[test]
    fn round_trips_through_serde_as_a_bare_string() {
        let topic = Topic::new("order-updates").unwrap();
        assert_eq!(serde_json::to_string(&topic).unwrap(), "\"order-updates\"");
        assert_eq!(serde_json::from_str::<Topic>("\"order-updates\"").unwrap(), topic);
    }

    #[test]
    fn deserializing_an_invalid_topic_fails_rather_than_arriving_intact() {
        assert!(serde_json::from_str::<Topic>("\"Marketing\"").is_err());
        assert!(serde_json::from_str::<Topic>("\"\"").is_err());
    }

    #[test]
    fn parses_and_displays_as_itself() {
        let topic: Topic = "billing".parse().unwrap();
        assert_eq!(topic.to_string(), "billing");
        assert_eq!(String::from(topic.clone()), "billing");
        assert_eq!(topic.as_ref(), "billing");
    }
}

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, CoreResult};

/// Longest idempotency key the service will accept.
///
/// The key is caller-chosen, stored in an indexed column and echoed back
/// in error messages, so it needs a bound somewhere. 255 characters is
/// comfortably more than any of the shapes callers actually use -- a UUID,
/// a ULID, an order id, a hash of a request body -- and small enough that
/// the unique index over it stays cheap.
pub const MAX_IDEMPOTENCY_KEY_LEN: usize = 255;

/// A caller-chosen name for one submission attempt.
///
/// Submitting a notification is not a safe operation to retry blindly: a
/// client whose connection drops after the service accepted the request
/// cannot tell "never arrived" from "arrived, and the response was lost",
/// and the two call for opposite responses. An idempotency key lets the
/// client stop having to tell them apart -- it names the *intent*, so a
/// second submission carrying a key the tenant has already used resolves
/// to the notification that key already created instead of sending a
/// second message.
///
/// The key is scoped to a tenant, not to the whole deployment. It is a
/// string one caller picked, and two tenants both submitting `order-1041`
/// have said nothing about each other.
///
/// # Validation
///
/// Keys are validated rather than normalised, and that is the important
/// design decision here. Trimming whitespace or lowercasing would mean the
/// key the service stored is not the key the client sent, so the client's
/// retry -- byte-for-byte identical to its first attempt, as a retry
/// should be -- would have to be normalised the same way by every future
/// code path that ever looks one up. Refusing the odd key outright, while
/// the caller is still on the line and before anything has been sent,
/// keeps the stored key and the presented key the same string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// Validates `raw` as an idempotency key.
    ///
    /// Accepts non-empty strings of at most `MAX_IDEMPOTENCY_KEY_LEN`
    /// visible ASCII characters. The character restriction is not
    /// fussiness: the key travels as an HTTP header value, where a control
    /// character is a request-smuggling primitive rather than a key, and
    /// arrives at a log line and an error message besides. Restricting it
    /// to the printable range costs callers nothing -- every identifier
    /// anyone actually uses as a key already fits -- and means the value
    /// can be echoed back safely everywhere it appears.
    pub fn new(raw: impl Into<String>) -> CoreResult<Self> {
        let raw: String = raw.into();

        if raw.is_empty() {
            return Err(CoreError::Validation("idempotency key cannot be empty".into()));
        }
        if raw.len() > MAX_IDEMPOTENCY_KEY_LEN {
            return Err(CoreError::Validation(format!(
                "idempotency key must be at most {MAX_IDEMPOTENCY_KEY_LEN} characters, got {}",
                raw.len()
            )));
        }
        // 0x21..=0x7e is printable ASCII minus the space: every ASCII
        // character a header value can carry unambiguously.
        if let Some(bad) = raw.chars().find(|c| !matches!(c, '\u{21}'..='\u{7e}')) {
            return Err(CoreError::Validation(format!(
                "idempotency key must be printable ASCII with no spaces, found {bad:?}"
            )));
        }

        Ok(Self(raw))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for IdempotencyKey {
    type Err = CoreError;

    fn from_str(raw: &str) -> CoreResult<Self> {
        IdempotencyKey::new(raw)
    }
}

impl TryFrom<String> for IdempotencyKey {
    type Error = CoreError;

    fn try_from(raw: String) -> CoreResult<Self> {
        IdempotencyKey::new(raw)
    }
}

impl From<IdempotencyKey> for String {
    fn from(key: IdempotencyKey) -> String {
        key.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_the_shapes_callers_actually_use() {
        for raw in ["order-1041", uuid::Uuid::new_v4().to_string().as_str(), "01HQ8Z", "a"] {
            assert_eq!(IdempotencyKey::new(raw).unwrap().as_str(), raw);
        }
    }

    #[test]
    fn rejects_an_empty_key() {
        assert!(matches!(IdempotencyKey::new(""), Err(CoreError::Validation(_))));
    }

    #[test]
    fn rejects_a_key_past_the_length_bound() {
        let key = IdempotencyKey::new("k".repeat(MAX_IDEMPOTENCY_KEY_LEN));
        assert!(key.is_ok(), "the bound itself is allowed");

        let err = IdempotencyKey::new("k".repeat(MAX_IDEMPOTENCY_KEY_LEN + 1)).unwrap_err();
        assert!(err.to_string().contains(&MAX_IDEMPOTENCY_KEY_LEN.to_string()), "{err}");
    }

    #[test]
    fn rejects_control_characters_and_whitespace() {
        // A key arrives as a header value and leaves in a log line; a
        // newline in one is a smuggled header, not a key.
        for raw in ["with space", "tab\there", "line\nbreak", "null\0byte", "café"] {
            assert!(IdempotencyKey::new(raw).is_err(), "accepted {raw:?}");
        }
    }

    #[test]
    fn keys_are_not_normalised() {
        // The client's retry is byte-for-byte its first attempt. If the
        // service stored something else, the retry would miss.
        let key = IdempotencyKey::new("Order-1041").unwrap();
        assert_eq!(key.as_str(), "Order-1041");
        assert_ne!(key, IdempotencyKey::new("order-1041").unwrap());
    }

    #[test]
    fn round_trips_through_serde_as_a_bare_string() {
        let key = IdempotencyKey::new("order-1041").unwrap();
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, "\"order-1041\"");
        assert_eq!(serde_json::from_str::<IdempotencyKey>(&json).unwrap(), key);
    }

    #[test]
    fn deserializing_an_invalid_key_fails_rather_than_landing_in_storage() {
        assert!(serde_json::from_str::<IdempotencyKey>("\"\"").is_err());
        assert!(serde_json::from_str::<IdempotencyKey>("\"has space\"").is_err());
    }

    #[test]
    fn parses_from_str_the_same_way_it_validates() {
        assert_eq!("order-1041".parse::<IdempotencyKey>().unwrap().as_str(), "order-1041");
        assert!("".parse::<IdempotencyKey>().is_err());
    }
}

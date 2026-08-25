use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::channel::ChannelKind;
use crate::notification::Notification;

/// Longest provider error text kept on an attempt record.
///
/// Provider errors are not written by this service: an HTML error page or
/// a stack trace from someone else's gateway arrives as the body of a
/// rejection, and storing it whole would let one bad provider turn the
/// attempt log into the largest table in the database. Five hundred
/// characters is comfortably more than any real diagnostic and small
/// enough that a notification exhausting its attempts costs a few
/// kilobytes rather than a few megabytes.
pub const MAX_ERROR_LEN: usize = 500;

/// What became of one delivery attempt.
///
/// Distinct from `DeliveryStatus`, which describes the *notification*.
/// A notification that is `Queued` may already have three failed attempts
/// behind it, and the two facts answer different questions: the status
/// says what happens next, an attempt says what happened once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptResult {
    /// The provider accepted the message.
    Delivered,
    /// The attempt reached a provider and the provider (or the transport)
    /// refused it.
    Failed,
    /// Nothing was sent. Either the worker declined to try -- rate limit,
    /// open circuit breaker -- or the message could not be built at all.
    /// Kept separate from `Failed` because a skip says nothing about the
    /// provider's health, and an operator reading the log needs to know
    /// which of the two they are looking at.
    Skipped,
}

impl AttemptResult {
    pub fn as_str(&self) -> &'static str {
        match self {
            AttemptResult::Delivered => "delivered",
            AttemptResult::Failed => "failed",
            AttemptResult::Skipped => "skipped",
        }
    }

    pub fn all() -> [AttemptResult; 3] {
        [AttemptResult::Delivered, AttemptResult::Failed, AttemptResult::Skipped]
    }
}

impl std::fmt::Display for AttemptResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// One line in a notification's delivery history.
///
/// The notification row itself only ever holds the *current* state, so
/// without this an operator asking "why did this dead-letter?" gets an
/// attempt count and nothing else -- not which provider refused it, not
/// what it said, not whether the five attempts were five rejections or
/// five rate-limit skips. Attempts are append-only: nothing rewrites one
/// after it is recorded, which is what makes the sequence readable as
/// history rather than as a summary that happens to be stale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryAttempt {
    pub id: Uuid,
    pub notification_id: Uuid,
    /// Carried on the attempt rather than looked up through the
    /// notification, so that listing a tenant's attempts and enforcing
    /// tenant isolation over them are both single-table operations.
    pub tenant_id: Uuid,
    pub channel: ChannelKind,
    /// Which of the notification's attempts this was, counting from 1.
    ///
    /// A skip doesn't spend an attempt, so consecutive skips all carry the
    /// same number: three rate-limit skips followed by a send read as
    /// attempts 1, 1, 1, 1, which is exactly what happened -- the send was
    /// still the first attempt anyone made.
    pub attempt_number: u32,
    pub result: AttemptResult,
    /// Why it failed or was skipped, truncated to `MAX_ERROR_LEN`.
    pub error: Option<String>,
    /// Wall time of the provider call, present only when a provider was
    /// actually contacted. A skip has no duration to report, and recording
    /// one would put "time spent deciding not to send" into the same
    /// column as provider latency.
    pub duration_ms: Option<u64>,
    pub occurred_at: DateTime<Utc>,
}

impl DeliveryAttempt {
    /// Records an attempt against `notification` as it stands *before* the
    /// queue is told the outcome.
    ///
    /// That ordering is what keeps `attempt_number` uniform. At this
    /// moment `attempt_count` holds attempts already spent, so the one
    /// being described is always the next one -- whether the queue goes on
    /// to spend it (`fail`), to finish without spending it (`complete`,
    /// which never increments), or to hand it back untouched (`defer`).
    /// Deriving the number from the count *after* the fact would need this
    /// type to know which of those three the caller chose.
    pub fn new(notification: &Notification, result: AttemptResult, occurred_at: DateTime<Utc>) -> Self {
        let attempt_number = notification.attempt_count.saturating_add(1);

        Self {
            id: Uuid::new_v4(),
            notification_id: notification.id,
            tenant_id: notification.tenant_id,
            channel: notification.channel,
            attempt_number,
            result,
            error: None,
            duration_ms: None,
            occurred_at,
        }
    }

    /// Attaches the reason this attempt failed or was skipped, truncated
    /// to `MAX_ERROR_LEN`.
    pub fn with_error(mut self, error: impl Into<String>) -> Self {
        self.error = Some(truncate(error.into()));
        self
    }

    /// Attaches how long the provider call took. Only meaningful when one
    /// happened.
    pub fn with_duration_ms(mut self, duration_ms: u64) -> Self {
        self.duration_ms = Some(duration_ms);
        self
    }

    /// Whether this attempt reached a provider, and so says something
    /// about that provider's health.
    pub fn reached_provider(&self) -> bool {
        self.result != AttemptResult::Skipped
    }
}

/// Truncates on a character boundary, marking that it happened.
///
/// Slicing a `String` at a byte index would panic on a multi-byte
/// character, and a provider's error text is exactly the kind of foreign
/// input that contains one.
fn truncate(mut error: String) -> String {
    if error.chars().count() <= MAX_ERROR_LEN {
        return error;
    }

    let cut = error.char_indices().nth(MAX_ERROR_LEN).map(|(index, _)| index).unwrap_or(error.len());
    error.truncate(cut);
    error.push('…');
    error
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::DeliveryStatus;

    fn notification() -> Notification {
        Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .body("hello")
            .build()
            .unwrap()
    }

    #[test]
    fn as_str_matches_the_serde_representation() {
        for result in AttemptResult::all() {
            let json = serde_json::to_string(&result).unwrap();
            assert_eq!(json, format!("\"{}\"", result.as_str()));
            assert_eq!(result.to_string(), result.as_str());
        }
    }

    #[test]
    fn an_attempt_inherits_the_notification_it_describes() {
        let n = notification();
        let attempt = DeliveryAttempt::new(&n, AttemptResult::Delivered, Utc::now());

        assert_eq!(attempt.notification_id, n.id);
        assert_eq!(attempt.tenant_id, n.tenant_id);
        assert_eq!(attempt.channel, n.channel);
    }

    #[test]
    fn the_first_attempt_on_a_fresh_notification_is_number_one() {
        let n = notification();
        for result in AttemptResult::all() {
            assert_eq!(DeliveryAttempt::new(&n, result, Utc::now()).attempt_number, 1);
        }
    }

    #[test]
    fn each_spent_attempt_advances_the_number() {
        let mut n = notification();
        n.record_attempt(Utc::now());
        assert_eq!(DeliveryAttempt::new(&n, AttemptResult::Failed, Utc::now()).attempt_number, 2);

        n.record_attempt(Utc::now());
        assert_eq!(DeliveryAttempt::new(&n, AttemptResult::Failed, Utc::now()).attempt_number, 3);
    }

    #[test]
    fn a_skip_does_not_advance_the_number() {
        // Skips are recorded against an unchanged notification, because
        // `defer` leaves the attempt budget alone -- so repeated skips
        // describe the same pending attempt over and over.
        let n = notification();
        let first = DeliveryAttempt::new(&n, AttemptResult::Skipped, Utc::now());
        let second = DeliveryAttempt::new(&n, AttemptResult::Skipped, Utc::now());
        let eventual_send = DeliveryAttempt::new(&n, AttemptResult::Delivered, Utc::now());

        assert_eq!((first.attempt_number, second.attempt_number), (1, 1));
        assert_eq!(eventual_send.attempt_number, 1, "the send was still the first attempt anyone made");
    }

    #[test]
    fn only_a_real_attempt_reached_a_provider() {
        let n = notification();
        assert!(DeliveryAttempt::new(&n, AttemptResult::Delivered, Utc::now()).reached_provider());
        assert!(DeliveryAttempt::new(&n, AttemptResult::Failed, Utc::now()).reached_provider());
        assert!(!DeliveryAttempt::new(&n, AttemptResult::Skipped, Utc::now()).reached_provider());
    }

    #[test]
    fn a_short_error_is_kept_verbatim() {
        let attempt =
            DeliveryAttempt::new(&notification(), AttemptResult::Failed, Utc::now()).with_error("550 nope");
        assert_eq!(attempt.error.as_deref(), Some("550 nope"));
    }

    #[test]
    fn a_long_error_is_truncated_and_marked() {
        let attempt = DeliveryAttempt::new(&notification(), AttemptResult::Failed, Utc::now())
            .with_error("x".repeat(MAX_ERROR_LEN * 3));
        let error = attempt.error.unwrap();

        assert_eq!(error.chars().count(), MAX_ERROR_LEN + 1);
        assert!(error.ends_with('…'));
    }

    #[test]
    fn truncation_does_not_split_a_multibyte_character() {
        // A byte-index truncate would panic here: every character is three
        // bytes, so no character boundary lands on MAX_ERROR_LEN.
        let attempt = DeliveryAttempt::new(&notification(), AttemptResult::Failed, Utc::now())
            .with_error("☃".repeat(MAX_ERROR_LEN + 10));
        let error = attempt.error.unwrap();

        assert_eq!(error.chars().count(), MAX_ERROR_LEN + 1);
        assert!(error.starts_with('☃'));
    }

    #[test]
    fn a_duration_is_only_attached_when_asked_for() {
        let n = notification();
        assert_eq!(DeliveryAttempt::new(&n, AttemptResult::Skipped, Utc::now()).duration_ms, None);
        assert_eq!(
            DeliveryAttempt::new(&n, AttemptResult::Delivered, Utc::now()).with_duration_ms(42).duration_ms,
            Some(42)
        );
    }

    #[test]
    fn round_trips_through_serde() {
        let attempt = DeliveryAttempt::new(&notification(), AttemptResult::Failed, Utc::now())
            .with_error("connection reset")
            .with_duration_ms(1200);
        let json = serde_json::to_string(&attempt).unwrap();

        assert_eq!(serde_json::from_str::<DeliveryAttempt>(&json).unwrap(), attempt);
    }

    #[test]
    fn an_attempt_is_independent_of_the_notifications_current_status() {
        // The notification moves on; the attempt that was recorded does
        // not change with it. That is the whole point of an append-only
        // history.
        let mut n = notification();
        let attempt = DeliveryAttempt::new(&n, AttemptResult::Failed, Utc::now());

        n.record_attempt(Utc::now());
        n.transition(DeliveryStatus::Queued, Utc::now()).unwrap();
        assert_eq!(attempt.result, AttemptResult::Failed);
        assert_eq!(attempt.attempt_number, 1);
    }
}

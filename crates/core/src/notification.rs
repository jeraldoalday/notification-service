use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::channel::ChannelKind;
use crate::error::{CoreError, CoreResult};
use crate::idempotency::IdempotencyKey;
use crate::priority::Priority;
use crate::status::DeliveryStatus;
use crate::topic::Topic;

/// Ceiling on a notification's `max_attempts`.
///
/// There has to be one. Retry backoff caps at fifteen minutes, so a
/// notification with a large budget doesn't retry for longer -- it retries
/// forever, every fifteen minutes, and every one of those retries is a row
/// `due_for_delivery` has to return before it can return anything else. A
/// caller passing `max_attempts: 4294967295` therefore doesn't just
/// inconvenience itself: at `Critical` priority it occupies the front of
/// every worker batch indefinitely, and the notifications behind it never
/// go out at all.
///
/// Twenty attempts against a fifteen-minute cap is a little over four
/// hours of retrying, which is longer than any outage worth waiting out
/// with a retry loop rather than a replay.
pub const MAX_ATTEMPTS_CEILING: u32 = 20;

/// A single notification: one recipient, one channel, one message.
/// Fan-out to multiple channels for the same logical event is handled by
/// creating multiple `Notification`s upstream (see `ns-queue`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub recipient_id: Uuid,
    pub channel: ChannelKind,
    pub priority: Priority,
    pub status: DeliveryStatus,
    pub subject: Option<String>,
    pub body: String,
    pub template_id: Option<String>,
    pub template_vars: HashMap<String, String>,
    /// What this message is about, if the caller said.
    ///
    /// Optional, and deliberately so: a notification with no topic is one
    /// nobody has been given the chance to opt out of, so it is delivered
    /// regardless of what the recipient has asked for. That makes the
    /// preference mechanism opt-in per message rather than per
    /// deployment, and means a password reset added to a codebase years
    /// before topics existed cannot be silenced by a marketing opt-out.
    ///
    /// `default` on the way in and skipped on the way out, for the same
    /// reason `idempotency_key` is: notifications are stored as JSON
    /// documents, so rows written before this field existed have to keep
    /// deserializing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<Topic>,
    /// The caller's name for this submission, if it gave one.
    ///
    /// `default` on the way in and skipped on the way out, because a
    /// notification is stored as a JSON document: rows written before
    /// this field existed have to keep deserializing, and the ones
    /// written without a key shouldn't grow a `null` for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<IdempotencyKey>,
    pub attempt_count: u32,
    pub max_attempts: u32,
    pub created_at: DateTime<Utc>,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl Notification {
    pub fn builder(tenant_id: Uuid, recipient_id: Uuid, channel: ChannelKind) -> NotificationBuilder {
        NotificationBuilder::new(tenant_id, recipient_id, channel)
    }

    /// Whether this notification is eligible to be picked up right now,
    /// i.e. it's queued and its scheduled time (if any) has passed.
    pub fn is_due(&self, now: DateTime<Utc>) -> bool {
        if self.status != DeliveryStatus::Queued {
            return false;
        }
        match self.scheduled_for {
            Some(t) => t <= now,
            None => true,
        }
    }

    pub fn has_attempts_remaining(&self) -> bool {
        self.attempt_count < self.max_attempts
    }

    /// Whether `other` asks for the same message to be sent as this one.
    ///
    /// This is the comparison an idempotent submission needs: a caller
    /// retrying a request presents the same key *and* the same content, so
    /// the key can be honoured. A caller reusing a key for a different
    /// message has made a mistake -- most often a key derived from
    /// something less unique than it looked -- and must be told, because
    /// silently returning the first notification would drop the second
    /// message entirely without anything ever reporting a failure.
    ///
    /// Compared field by field rather than through a stored hash of the
    /// request body: the fields below are what the service will actually
    /// act on, whereas two request bodies can differ in key order or
    /// whitespace while asking for exactly the same thing. Identity,
    /// timestamps, status and attempt bookkeeping are deliberately not
    /// compared -- they describe this notification's own history, not what
    /// the caller asked for.
    pub fn has_same_content_as(&self, other: &Notification) -> bool {
        self.tenant_id == other.tenant_id
            && self.recipient_id == other.recipient_id
            && self.channel == other.channel
            && self.priority == other.priority
            && self.subject == other.subject
            && self.body == other.body
            && self.template_id == other.template_id
            && self.template_vars == other.template_vars
            && self.topic == other.topic
            && self.max_attempts == other.max_attempts
            && self.scheduled_for == other.scheduled_for
    }

    pub fn record_attempt(&mut self, now: DateTime<Utc>) {
        // Saturating rather than wrapping: `build` bounds `max_attempts`,
        // so this can't be reached in practice, but a count that wrapped
        // to zero would hand a notification an unlimited retry budget --
        // exactly the thing the bound exists to prevent.
        self.attempt_count = self.attempt_count.saturating_add(1);
        self.updated_at = now;
    }

    pub fn transition(&mut self, next: DeliveryStatus, now: DateTime<Utc>) -> CoreResult<()> {
        self.status = self.status.transition_to(next)?;
        self.updated_at = now;
        Ok(())
    }
}

pub struct NotificationBuilder {
    tenant_id: Uuid,
    recipient_id: Uuid,
    channel: ChannelKind,
    priority: Priority,
    subject: Option<String>,
    body: Option<String>,
    template_id: Option<String>,
    template_vars: HashMap<String, String>,
    topic: Option<Topic>,
    idempotency_key: Option<IdempotencyKey>,
    max_attempts: u32,
    scheduled_for: Option<DateTime<Utc>>,
}

impl NotificationBuilder {
    fn new(tenant_id: Uuid, recipient_id: Uuid, channel: ChannelKind) -> Self {
        Self {
            tenant_id,
            recipient_id,
            channel,
            priority: Priority::default(),
            subject: None,
            body: None,
            template_id: None,
            template_vars: HashMap::new(),
            topic: None,
            idempotency_key: None,
            max_attempts: 5,
            scheduled_for: None,
        }
    }

    pub fn priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    pub fn body(mut self, body: impl Into<String>) -> Self {
        self.body = Some(body.into());
        self
    }

    pub fn template(mut self, template_id: impl Into<String>) -> Self {
        self.template_id = Some(template_id.into());
        self
    }

    pub fn template_var(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.template_vars.insert(key.into(), value.into());
        self
    }

    /// Says what this message is about, so the recipient's preferences
    /// for that topic apply to it. A notification with no topic is
    /// delivered whatever the recipient has opted out of.
    pub fn topic(mut self, topic: Topic) -> Self {
        self.topic = Some(topic);
        self
    }

    /// Names this submission, so a retry carrying the same key resolves
    /// to the notification this one creates rather than sending again.
    pub fn idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.idempotency_key = Some(key);
        self
    }

    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    pub fn scheduled_for(mut self, at: DateTime<Utc>) -> Self {
        self.scheduled_for = Some(at);
        self
    }

    pub fn build(self) -> CoreResult<Notification> {
        if self.body.is_none() && self.template_id.is_none() {
            return Err(CoreError::Validation(
                "notification must have either a body or a template_id".into(),
            ));
        }
        if self.max_attempts == 0 {
            return Err(CoreError::Validation("max_attempts must be at least 1".into()));
        }
        // Refused rather than clamped, unlike an over-large list `limit`.
        // A caller asking for a thousand attempts and silently getting
        // twenty has been told something untrue about when its
        // notification will stop being retried; a caller asking for a
        // thousand rows and getting five hundred just pages again.
        if self.max_attempts > MAX_ATTEMPTS_CEILING {
            return Err(CoreError::Validation(format!(
                "max_attempts must be at most {MAX_ATTEMPTS_CEILING}, got {}",
                self.max_attempts
            )));
        }

        let now = Utc::now();
        Ok(Notification {
            id: Uuid::new_v4(),
            tenant_id: self.tenant_id,
            recipient_id: self.recipient_id,
            channel: self.channel,
            priority: self.priority,
            status: DeliveryStatus::default(),
            subject: self.subject,
            body: self.body.unwrap_or_default(),
            template_id: self.template_id,
            template_vars: self.template_vars,
            topic: self.topic,
            idempotency_key: self.idempotency_key,
            attempt_count: 0,
            max_attempts: self.max_attempts,
            created_at: now,
            scheduled_for: self.scheduled_for,
            updated_at: now,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::Topic;
    use chrono::Duration;

    fn build_ok() -> Notification {
        Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .subject("Welcome")
            .body("Hello there")
            .priority(Priority::High)
            .build()
            .unwrap()
    }

    #[test]
    fn builder_requires_body_or_template() {
        let err =
            Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Sms).build().unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn builder_accepts_template_without_body() {
        let n = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Push)
            .template("welcome_push")
            .template_var("name", "Ada")
            .build()
            .unwrap();
        assert_eq!(n.template_id.as_deref(), Some("welcome_push"));
        assert_eq!(n.template_vars.get("name").map(String::as_str), Some("Ada"));
    }

    #[test]
    fn rejects_zero_max_attempts() {
        let err = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .body("hi")
            .max_attempts(0)
            .build()
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
    }

    #[test]
    fn rejects_max_attempts_beyond_the_ceiling() {
        // Unbounded attempts don't retry for longer -- backoff caps at
        // fifteen minutes -- they retry forever, and at high priority they
        // sit at the front of every worker batch while they do it.
        let err = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .body("hi")
            .max_attempts(u32::MAX)
            .build()
            .unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
        assert!(err.to_string().contains(&MAX_ATTEMPTS_CEILING.to_string()), "{err}");
    }

    #[test]
    fn accepts_max_attempts_at_the_ceiling() {
        let n = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .body("hi")
            .max_attempts(MAX_ATTEMPTS_CEILING)
            .build()
            .unwrap();
        assert_eq!(n.max_attempts, MAX_ATTEMPTS_CEILING);
    }

    #[test]
    fn the_attempt_count_can_never_wrap_past_its_budget() {
        let mut n = build_ok();
        n.attempt_count = u32::MAX;
        n.record_attempt(Utc::now());

        // Wrapping to zero would hand the notification an unlimited retry
        // budget, which is the thing the ceiling exists to prevent.
        assert_eq!(n.attempt_count, u32::MAX);
        assert!(!n.has_attempts_remaining());
    }

    #[test]
    fn defaults_are_sane() {
        let n = build_ok();
        assert_eq!(n.status, DeliveryStatus::Pending);
        assert_eq!(n.attempt_count, 0);
        assert_eq!(n.max_attempts, 5);
    }

    #[test]
    fn is_due_requires_queued_status() {
        let n = build_ok();
        assert!(!n.is_due(Utc::now()), "pending notifications aren't due yet");
    }

    #[test]
    fn is_due_respects_future_schedule() {
        let mut n = build_ok();
        n.transition(DeliveryStatus::Queued, Utc::now()).unwrap();
        n.scheduled_for = Some(Utc::now() + Duration::hours(1));
        assert!(!n.is_due(Utc::now()));

        n.scheduled_for = Some(Utc::now() - Duration::minutes(1));
        assert!(n.is_due(Utc::now()));
    }

    #[test]
    fn record_attempt_increments_and_touches_updated_at() {
        let mut n = build_ok();
        let before = n.updated_at;
        std::thread::sleep(std::time::Duration::from_millis(2));
        n.record_attempt(Utc::now());
        assert_eq!(n.attempt_count, 1);
        assert!(n.updated_at >= before);
    }

    #[test]
    fn has_attempts_remaining_tracks_max() {
        let mut n = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .body("hi")
            .max_attempts(2)
            .build()
            .unwrap();
        assert!(n.has_attempts_remaining());
        n.record_attempt(Utc::now());
        assert!(n.has_attempts_remaining());
        n.record_attempt(Utc::now());
        assert!(!n.has_attempts_remaining());
    }

    #[test]
    fn a_notification_carries_no_idempotency_key_unless_asked() {
        let n = build_ok();
        assert_eq!(n.idempotency_key, None);
    }

    #[test]
    fn the_builder_carries_an_idempotency_key_through() {
        let key = IdempotencyKey::new("order-1041").unwrap();
        let n = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .body("hi")
            .idempotency_key(key.clone())
            .build()
            .unwrap();
        assert_eq!(n.idempotency_key, Some(key));
    }

    #[test]
    fn a_notification_stored_before_keys_existed_still_deserializes() {
        // Notifications are stored as JSON documents, so the field has to
        // be optional on the way in as well as on the way out.
        let mut json = serde_json::to_value(build_ok()).unwrap();
        assert!(json.get("idempotency_key").is_none(), "an absent key adds no field");

        json.as_object_mut().unwrap().remove("subject");
        let restored: Notification = serde_json::from_value(json).unwrap();
        assert_eq!(restored.idempotency_key, None);
    }

    #[test]
    fn a_retry_of_the_same_request_compares_equal() {
        let tenant = Uuid::new_v4();
        let recipient = Uuid::new_v4();
        let submit = || {
            Notification::builder(tenant, recipient, ChannelKind::Email)
                .subject("Welcome")
                .body("Hello there")
                .template_var("name", "Ada")
                .build()
                .unwrap()
        };

        let first = submit();
        let second = submit();
        assert_ne!(first.id, second.id, "two builds, two identities");
        assert!(first.has_same_content_as(&second), "but the same message");
    }

    #[test]
    fn a_different_message_does_not_compare_equal() {
        let tenant = Uuid::new_v4();
        let recipient = Uuid::new_v4();
        let base = Notification::builder(tenant, recipient, ChannelKind::Email)
            .subject("Welcome")
            .body("Hello there")
            .build()
            .unwrap();

        let variants = [
            Notification::builder(tenant, recipient, ChannelKind::Email)
                .subject("Welcome")
                .body("Something else")
                .build()
                .unwrap(),
            Notification::builder(tenant, recipient, ChannelKind::Sms)
                .subject("Welcome")
                .body("Hello there")
                .build()
                .unwrap(),
            Notification::builder(tenant, Uuid::new_v4(), ChannelKind::Email)
                .subject("Welcome")
                .body("Hello there")
                .build()
                .unwrap(),
            Notification::builder(tenant, recipient, ChannelKind::Email)
                .subject("Welcome")
                .body("Hello there")
                .priority(Priority::Critical)
                .build()
                .unwrap(),
            Notification::builder(tenant, recipient, ChannelKind::Email)
                .subject("Welcome")
                .body("Hello there")
                .max_attempts(9)
                .build()
                .unwrap(),
        ];
        for variant in &variants {
            assert!(!base.has_same_content_as(variant), "wrongly matched {variant:?}");
        }
    }

    #[test]
    fn content_comparison_ignores_a_notifications_own_history() {
        // Only what the caller asked for is compared: a first submission
        // that has since been claimed, retried and rescheduled still
        // matches the retry that arrives after it.
        let mut first = build_ok();
        let second = first.clone();

        first.transition(DeliveryStatus::Queued, Utc::now()).unwrap();
        first.transition(DeliveryStatus::Sending, Utc::now()).unwrap();
        first.record_attempt(Utc::now());
        first.id = Uuid::new_v4();

        assert!(first.has_same_content_as(&second));
    }

    #[test]
    fn a_notification_carries_no_topic_unless_asked() {
        let n = build_ok();
        assert_eq!(n.topic, None);

        // An absent topic adds no field, so a row written today is
        // readable by a build that predates topics entirely.
        let json = serde_json::to_value(&n).unwrap();
        assert!(json.get("topic").is_none());
    }

    #[test]
    fn the_builder_carries_a_topic_through() {
        let topic = Topic::new("order-updates").unwrap();
        let n = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .body("your order shipped")
            .topic(topic.clone())
            .build()
            .unwrap();
        assert_eq!(n.topic, Some(topic));
    }

    #[test]
    fn a_notification_stored_before_topics_existed_still_deserializes() {
        let mut json = serde_json::to_value(build_ok()).unwrap();
        json.as_object_mut().unwrap().remove("topic");
        assert_eq!(serde_json::from_value::<Notification>(json).unwrap().topic, None);
    }

    #[test]
    fn two_topics_are_two_different_messages() {
        // A retry that changed the topic changed what the recipient's
        // preferences will be applied to, which is a different request
        // however identical the words are.
        let tenant = Uuid::new_v4();
        let recipient = Uuid::new_v4();
        let with = |topic: Option<&str>| {
            let mut builder = Notification::builder(tenant, recipient, ChannelKind::Email).body("hi");
            if let Some(topic) = topic {
                builder = builder.topic(Topic::new(topic).unwrap());
            }
            builder.build().unwrap()
        };

        assert!(with(Some("orders")).has_same_content_as(&with(Some("orders"))));
        assert!(!with(Some("orders")).has_same_content_as(&with(Some("marketing"))));
        assert!(!with(Some("orders")).has_same_content_as(&with(None)));
    }

    #[test]
    fn transition_rejects_illegal_moves() {
        let mut n = build_ok();
        let err = n.transition(DeliveryStatus::Delivered, Utc::now()).unwrap_err();
        assert!(matches!(err, CoreError::InvalidTransition { .. }));
    }
}

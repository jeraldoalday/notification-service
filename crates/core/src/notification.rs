use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::channel::ChannelKind;
use crate::error::{CoreError, CoreResult};
use crate::priority::Priority;
use crate::status::DeliveryStatus;

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

    pub fn record_attempt(&mut self, now: DateTime<Utc>) {
        self.attempt_count += 1;
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
    fn transition_rejects_illegal_moves() {
        let mut n = build_ok();
        let err = n.transition(DeliveryStatus::Delivered, Utc::now()).unwrap_err();
        assert!(matches!(err, CoreError::InvalidTransition { .. }));
    }
}

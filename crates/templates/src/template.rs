use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, Utc};
use ns_core::{ChannelKind, Notification};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{TemplateError, TemplateResult};
use crate::render;

/// Upper bound on a template id, so the storage key stays a short,
/// index-friendly string rather than an arbitrarily long blob.
const MAX_ID_LEN: usize = 64;

/// A reusable, tenant-scoped message body (and optional subject) with
/// `{{placeholder}}` markers.
///
/// The id is a caller-chosen slug -- `welcome_email`, `order.shipped` --
/// rather than a generated UUID, because it's what application code writes
/// into `Notification::template_id` at submit time. Being tenant-scoped
/// means two tenants can both own a `welcome_email` without collision, so
/// the storage key is the (tenant_id, id) pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Template {
    pub id: String,
    pub tenant_id: Uuid,
    pub name: Option<String>,
    /// Restricts the template to one channel. `None` means it can render
    /// for any channel -- useful for text that reads the same over SMS and
    /// push, while an HTML-ish email body can pin itself to `Email` and be
    /// rejected if a notification tries to send it as a text message.
    pub channel: Option<ChannelKind>,
    pub subject: Option<String>,
    pub body: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// The output of rendering a template against a set of variables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenderedMessage {
    pub subject: Option<String>,
    pub body: String,
}

impl RenderedMessage {
    /// Writes the rendered text onto a notification, which is how
    /// `ns-worker` turns a templated notification into a sendable one just
    /// before handing it to a channel. A template without a subject leaves
    /// any subject already on the notification alone, so a caller can
    /// override just the subject line of a shared body.
    pub fn apply_to(&self, notification: &mut Notification) {
        notification.body = self.body.clone();
        if self.subject.is_some() {
            notification.subject = self.subject.clone();
        }
    }
}

impl Template {
    /// Creates a template, validating the id and that both the body and
    /// subject parse. Authoring mistakes are caught here, once, rather
    /// than on the delivery path where they'd cost a notification its
    /// retries.
    pub fn new(tenant_id: Uuid, id: impl Into<String>, body: impl Into<String>) -> TemplateResult<Self> {
        let id = id.into();
        let body = body.into();
        validate_id(&id)?;
        if body.trim().is_empty() {
            return Err(TemplateError::Validation("template body cannot be empty".into()));
        }
        render::parse(&body)?;

        let now = Utc::now();
        Ok(Self {
            id,
            tenant_id,
            name: None,
            channel: None,
            subject: None,
            body,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_channel(mut self, channel: ChannelKind) -> Self {
        self.channel = Some(channel);
        self
    }

    pub fn with_subject(mut self, subject: impl Into<String>) -> TemplateResult<Self> {
        let subject = subject.into();
        render::parse(&subject)?;
        self.subject = Some(subject);
        Ok(self)
    }

    /// Replaces the subject and body in place, re-validating both, and
    /// stamps `updated_at`. This is what the API's update endpoint calls:
    /// id, tenant and creation time are identity and stay put.
    pub fn revise(
        &mut self,
        subject: Option<String>,
        body: String,
        now: DateTime<Utc>,
    ) -> TemplateResult<()> {
        if body.trim().is_empty() {
            return Err(TemplateError::Validation("template body cannot be empty".into()));
        }
        render::parse(&body)?;
        if let Some(subject) = &subject {
            render::parse(subject)?;
        }

        self.subject = subject;
        self.body = body;
        self.updated_at = now;
        Ok(())
    }

    /// Every variable this template needs, across subject and body.
    pub fn variables(&self) -> TemplateResult<BTreeSet<String>> {
        let mut names = render::placeholders(&self.body)?;
        if let Some(subject) = &self.subject {
            names.extend(render::placeholders(subject)?);
        }
        Ok(names)
    }

    /// Variables this template needs that `vars` doesn't supply, across
    /// subject and body.
    pub fn missing_variables(&self, vars: &HashMap<String, String>) -> TemplateResult<BTreeSet<String>> {
        Ok(self.variables()?.into_iter().filter(|name| !vars.contains_key(name)).collect())
    }

    pub fn render(&self, vars: &HashMap<String, String>) -> TemplateResult<RenderedMessage> {
        let subject = match &self.subject {
            Some(subject) => Some(render::render(&self.id, subject, vars)?),
            None => None,
        };
        let body = render::render(&self.id, &self.body, vars)?;
        Ok(RenderedMessage { subject, body })
    }

    /// Renders for a specific notification, using its own `template_vars`
    /// and rejecting a channel this template isn't meant for. The channel
    /// check lives here rather than in the worker so every caller
    /// (worker, API preview, CLI) enforces it identically.
    pub fn render_for(&self, notification: &Notification) -> TemplateResult<RenderedMessage> {
        if let Some(expected) = self.channel {
            if expected != notification.channel {
                return Err(TemplateError::ChannelMismatch {
                    template_id: self.id.clone(),
                    expected,
                    actual: notification.channel,
                });
            }
        }
        self.render(&notification.template_vars)
    }
}

fn validate_id(id: &str) -> TemplateResult<()> {
    if id.is_empty() {
        return Err(TemplateError::Validation("template id cannot be empty".into()));
    }
    if id.len() > MAX_ID_LEN {
        return Err(TemplateError::Validation(format!(
            "template id cannot be longer than {MAX_ID_LEN} characters"
        )));
    }
    if !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.') {
        return Err(TemplateError::Validation(format!(
            "template id `{id}` may only contain letters, digits, `_`, `-` or `.`"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn sample() -> Template {
        Template::new(Uuid::new_v4(), "welcome_email", "Hi {{name}}, welcome to {{product}}.")
            .unwrap()
            .with_subject("Welcome, {{name}}")
            .unwrap()
    }

    #[test]
    fn rejects_an_empty_id() {
        let err = Template::new(Uuid::new_v4(), "", "body").unwrap_err();
        assert!(matches!(err, TemplateError::Validation(_)));
    }

    #[test]
    fn rejects_an_id_with_spaces() {
        let err = Template::new(Uuid::new_v4(), "welcome email", "body").unwrap_err();
        assert!(matches!(err, TemplateError::Validation(_)));
    }

    #[test]
    fn rejects_an_overlong_id() {
        let err = Template::new(Uuid::new_v4(), "x".repeat(MAX_ID_LEN + 1), "body").unwrap_err();
        assert!(matches!(err, TemplateError::Validation(_)));
    }

    #[test]
    fn rejects_a_blank_body() {
        let err = Template::new(Uuid::new_v4(), "welcome", "   ").unwrap_err();
        assert!(matches!(err, TemplateError::Validation(_)));
    }

    #[test]
    fn rejects_a_body_that_does_not_parse() {
        let err = Template::new(Uuid::new_v4(), "welcome", "Hi {{name").unwrap_err();
        assert!(matches!(err, TemplateError::UnclosedPlaceholder { .. }));
    }

    #[test]
    fn rejects_a_subject_that_does_not_parse() {
        let err =
            Template::new(Uuid::new_v4(), "welcome", "body").unwrap().with_subject("Hi {{name").unwrap_err();
        assert!(matches!(err, TemplateError::UnclosedPlaceholder { .. }));
    }

    #[test]
    fn variables_span_subject_and_body() {
        let names: Vec<String> = sample().variables().unwrap().into_iter().collect();
        assert_eq!(names, vec!["name".to_string(), "product".to_string()]);
    }

    #[test]
    fn missing_variables_ignores_supplied_ones() {
        let missing: Vec<String> =
            sample().missing_variables(&vars(&[("name", "Ada")])).unwrap().into_iter().collect();
        assert_eq!(missing, vec!["product".to_string()]);
    }

    #[test]
    fn renders_subject_and_body() {
        let rendered = sample().render(&vars(&[("name", "Ada"), ("product", "Notify")])).unwrap();
        assert_eq!(rendered.subject.as_deref(), Some("Welcome, Ada"));
        assert_eq!(rendered.body, "Hi Ada, welcome to Notify.");
    }

    #[test]
    fn render_reports_a_missing_variable_against_this_template_id() {
        let err = sample().render(&vars(&[("name", "Ada")])).unwrap_err();
        assert_eq!(
            err,
            TemplateError::MissingVariable { template_id: "welcome_email".into(), name: "product".into() }
        );
    }

    #[test]
    fn render_for_uses_the_notifications_own_variables() {
        let tenant_id = Uuid::new_v4();
        let notification = Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email)
            .template("welcome_email")
            .template_var("name", "Ada")
            .template_var("product", "Notify")
            .build()
            .unwrap();

        let rendered = sample().render_for(&notification).unwrap();
        assert_eq!(rendered.body, "Hi Ada, welcome to Notify.");
    }

    #[test]
    fn render_for_rejects_a_channel_the_template_is_not_for() {
        let template =
            Template::new(Uuid::new_v4(), "email_only", "hi").unwrap().with_channel(ChannelKind::Email);
        let notification = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Sms)
            .template("email_only")
            .build()
            .unwrap();

        let err = template.render_for(&notification).unwrap_err();
        assert_eq!(
            err,
            TemplateError::ChannelMismatch {
                template_id: "email_only".into(),
                expected: ChannelKind::Email,
                actual: ChannelKind::Sms,
            }
        );
    }

    #[test]
    fn render_for_allows_any_channel_when_unpinned() {
        let template = Template::new(Uuid::new_v4(), "any", "hi").unwrap();
        let notification = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Push)
            .template("any")
            .build()
            .unwrap();
        assert!(template.render_for(&notification).is_ok());
    }

    #[test]
    fn apply_to_overwrites_the_body() {
        let mut notification = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .body("placeholder body")
            .subject("original subject")
            .build()
            .unwrap();

        let rendered = RenderedMessage { subject: None, body: "rendered".into() };
        rendered.apply_to(&mut notification);

        assert_eq!(notification.body, "rendered");
        assert_eq!(
            notification.subject.as_deref(),
            Some("original subject"),
            "a template without a subject leaves the caller's own subject in place"
        );
    }

    #[test]
    fn apply_to_overwrites_the_subject_when_the_template_has_one() {
        let mut notification = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .body("body")
            .subject("original subject")
            .build()
            .unwrap();

        let rendered = RenderedMessage { subject: Some("templated subject".into()), body: "rendered".into() };
        rendered.apply_to(&mut notification);

        assert_eq!(notification.subject.as_deref(), Some("templated subject"));
    }

    #[test]
    fn revise_replaces_content_and_stamps_updated_at() {
        let mut template = sample();
        let created_at = template.created_at;
        let later = Utc::now() + chrono::Duration::seconds(30);

        template.revise(None, "New body for {{name}}".into(), later).unwrap();

        assert_eq!(template.body, "New body for {{name}}");
        assert_eq!(template.subject, None);
        assert_eq!(template.created_at, created_at, "creation time is identity, not content");
        assert_eq!(template.updated_at, later);
    }

    #[test]
    fn revise_rejects_invalid_content_without_mutating() {
        let mut template = sample();
        let before = template.clone();

        let err = template.revise(None, "Hi {{name".into(), Utc::now()).unwrap_err();

        assert!(matches!(err, TemplateError::UnclosedPlaceholder { .. }));
        assert_eq!(template, before, "a rejected revision leaves the template untouched");
    }

    #[test]
    fn serde_round_trip() {
        let template = sample().with_channel(ChannelKind::Email).with_name("Welcome email");
        let json = serde_json::to_string(&template).unwrap();
        let back: Template = serde_json::from_str(&json).unwrap();
        assert_eq!(back, template);
    }
}

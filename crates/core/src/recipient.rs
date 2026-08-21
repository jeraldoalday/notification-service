use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::channel::ChannelKind;
use crate::error::{CoreError, CoreResult};

/// A channel-specific delivery address: an email, an E.164 phone number,
/// a device push token, or a webhook URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum RecipientAddress {
    Email(String),
    Phone(String),
    PushToken(String),
    WebhookUrl(String),
}

impl RecipientAddress {
    pub fn channel_kind(&self) -> ChannelKind {
        match self {
            RecipientAddress::Email(_) => ChannelKind::Email,
            RecipientAddress::Phone(_) => ChannelKind::Sms,
            RecipientAddress::PushToken(_) => ChannelKind::Push,
            RecipientAddress::WebhookUrl(_) => ChannelKind::Webhook,
        }
    }

    /// Cheap, dependency-free sanity checks. Full validation (MX lookups,
    /// carrier lookups, etc.) belongs in the channel providers.
    pub fn validate(&self) -> CoreResult<()> {
        match self {
            RecipientAddress::Email(addr) => {
                if addr.contains('@') && addr.len() > 3 && !addr.starts_with('@') && !addr.ends_with('@') {
                    Ok(())
                } else {
                    Err(CoreError::Validation(format!("invalid email address: {addr}")))
                }
            }
            RecipientAddress::Phone(number) => {
                let digits_only: String = number.chars().filter(|c| c.is_ascii_digit()).collect();
                if number.starts_with('+') && digits_only.len() >= 8 && digits_only.len() <= 15 {
                    Ok(())
                } else {
                    Err(CoreError::Validation(format!("phone number must be E.164 formatted: {number}")))
                }
            }
            RecipientAddress::PushToken(token) => {
                if token.trim().is_empty() {
                    Err(CoreError::Validation("push token cannot be empty".into()))
                } else {
                    Ok(())
                }
            }
            RecipientAddress::WebhookUrl(url) => {
                if url.starts_with("https://") || url.starts_with("http://") {
                    Ok(())
                } else {
                    Err(CoreError::Validation(format!("webhook url must be http(s): {url}")))
                }
            }
        }
    }
}

/// A notification recipient, identified by a stable id and holding one
/// address per channel it can be reached on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recipient {
    pub id: Uuid,
    /// The tenant this recipient belongs to.
    ///
    /// Recipients predate multi-tenancy in this service, so stored rows
    /// written before it have no tenant in their JSON and deserialize to
    /// the nil UUID. That is deliberately a tenant no API key can ever
    /// hold: such a recipient is visible to nobody rather than to
    /// everybody, so the gap fails closed and an operator re-registers the
    /// handful of rows affected.
    #[serde(default)]
    pub tenant_id: Uuid,
    pub display_name: Option<String>,
    addresses: HashMap<ChannelKind, RecipientAddress>,
}

impl Recipient {
    pub fn new(id: Uuid, tenant_id: Uuid) -> Self {
        Self { id, tenant_id, display_name: None, addresses: HashMap::new() }
    }

    /// Whether this recipient may be addressed by `tenant_id`.
    ///
    /// A recipient with the nil tenant belongs to no one and answers
    /// `false` for every tenant, including the nil one -- a caller can't
    /// hold a nil-tenant key by accident, but spelling the rule out here
    /// means the check can't be defeated by one that somehow does.
    pub fn belongs_to(&self, tenant_id: Uuid) -> bool {
        !self.tenant_id.is_nil() && self.tenant_id == tenant_id
    }

    pub fn with_display_name(mut self, name: impl Into<String>) -> Self {
        self.display_name = Some(name.into());
        self
    }

    pub fn with_address(mut self, address: RecipientAddress) -> CoreResult<Self> {
        address.validate()?;
        self.addresses.insert(address.channel_kind(), address);
        Ok(self)
    }

    pub fn address_for(&self, channel: ChannelKind) -> CoreResult<&RecipientAddress> {
        self.addresses.get(&channel).ok_or(CoreError::MissingAddress(channel))
    }

    pub fn supports(&self, channel: ChannelKind) -> bool {
        self.addresses.contains_key(&channel)
    }

    pub fn supported_channels(&self) -> Vec<ChannelKind> {
        self.addresses.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_recipient() -> Recipient {
        Recipient::new(Uuid::new_v4(), Uuid::new_v4())
            .with_display_name("Ada Lovelace")
            .with_address(RecipientAddress::Email("ada@example.com".into()))
            .unwrap()
            .with_address(RecipientAddress::Phone("+15555550100".into()))
            .unwrap()
    }

    #[test]
    fn builds_recipient_with_multiple_addresses() {
        let r = valid_recipient();
        assert!(r.supports(ChannelKind::Email));
        assert!(r.supports(ChannelKind::Sms));
        assert!(!r.supports(ChannelKind::Push));
        assert_eq!(r.supported_channels().len(), 2);
    }

    #[test]
    fn missing_address_errors() {
        let r = Recipient::new(Uuid::new_v4(), Uuid::new_v4());
        let err = r.address_for(ChannelKind::Email).unwrap_err();
        assert!(matches!(err, CoreError::MissingAddress(ChannelKind::Email)));
    }

    #[test]
    fn rejects_invalid_email() {
        let err = RecipientAddress::Email("not-an-email".into()).validate();
        assert!(err.is_err());
    }

    #[test]
    fn rejects_non_e164_phone() {
        let err = RecipientAddress::Phone("555-0100".into()).validate();
        assert!(err.is_err());
    }

    #[test]
    fn accepts_valid_e164_phone() {
        assert!(RecipientAddress::Phone("+15555550100".into()).validate().is_ok());
    }

    #[test]
    fn rejects_empty_push_token() {
        assert!(RecipientAddress::PushToken("  ".into()).validate().is_err());
    }

    #[test]
    fn webhook_requires_scheme() {
        assert!(RecipientAddress::WebhookUrl("example.com/hook".into()).validate().is_err());
        assert!(RecipientAddress::WebhookUrl("https://example.com/hook".into()).validate().is_ok());
    }

    #[test]
    fn a_recipient_belongs_only_to_its_own_tenant() {
        let tenant = Uuid::new_v4();
        let recipient = Recipient::new(Uuid::new_v4(), tenant);
        assert!(recipient.belongs_to(tenant));
        assert!(!recipient.belongs_to(Uuid::new_v4()));
    }

    #[test]
    fn a_recipient_stored_before_multi_tenancy_belongs_to_nobody() {
        let legacy: Recipient = serde_json::from_str(
            r#"{"id":"00000000-0000-0000-0000-0000000000ff","display_name":null,"addresses":{}}"#,
        )
        .unwrap();

        assert!(legacy.tenant_id.is_nil());
        assert!(!legacy.belongs_to(Uuid::new_v4()));
        assert!(!legacy.belongs_to(Uuid::nil()), "the gap has to fail closed, not open");
    }

    #[test]
    fn tenant_round_trips_through_serde() {
        let recipient = valid_recipient();
        let json = serde_json::to_string(&recipient).unwrap();
        assert_eq!(serde_json::from_str::<Recipient>(&json).unwrap(), recipient);
    }

    #[test]
    fn address_for_returns_matching_channel() {
        let r = valid_recipient();
        let addr = r.address_for(ChannelKind::Email).unwrap();
        assert_eq!(addr, &RecipientAddress::Email("ada@example.com".into()));
    }
}

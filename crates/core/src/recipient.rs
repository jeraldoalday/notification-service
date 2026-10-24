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

    /// Builds the address of `channel`'s kind from a bare string.
    ///
    /// The inverse of `channel_kind`, for the callers -- a suppression
    /// query string, a CLI flag -- that receive a channel and a value as
    /// two separate fields rather than as this enum's tagged JSON. Keeping
    /// the mapping here means those callers can't invent a different one.
    pub fn for_channel(channel: ChannelKind, value: impl Into<String>) -> Self {
        let value = value.into();
        match channel {
            ChannelKind::Email => RecipientAddress::Email(value),
            ChannelKind::Sms => RecipientAddress::Phone(value),
            ChannelKind::Push => RecipientAddress::PushToken(value),
            ChannelKind::Webhook => RecipientAddress::WebhookUrl(value),
        }
    }

    /// The raw address, whichever variant this is.
    pub fn value(&self) -> &str {
        match self {
            RecipientAddress::Email(value)
            | RecipientAddress::Phone(value)
            | RecipientAddress::PushToken(value)
            | RecipientAddress::WebhookUrl(value) => value,
        }
    }

    /// The normalised form this address is suppressed under.
    ///
    /// Email is lowercased; nothing else is. Domains are case-insensitive
    /// outright, and while RFC 5321 does permit a case-sensitive
    /// local-part, no mail system in practice treats `Ada@` and `ada@` as
    /// different people -- so matching case-sensitively would let an
    /// unsubscribe be defeated by a shift key. Suppression is the one
    /// place where the safe failure is to match too much: sending to
    /// someone who asked you not to is a much worse outcome than declining
    /// to send to an address that merely looks like theirs.
    ///
    /// Push tokens, webhook URLs and phone numbers are left verbatim,
    /// because for those, case really is significant -- a token is an
    /// opaque credential and a URL path is case-sensitive by spec.
    pub fn suppression_key(&self) -> String {
        match self {
            RecipientAddress::Email(address) => address.to_lowercase(),
            other => other.value().to_string(),
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
                let parsed = url::Url::parse(url)
                    .map_err(|_| CoreError::Validation(format!("invalid webhook url: {url}")))?;

                if parsed.scheme() != "http" && parsed.scheme() != "https" {
                    return Err(CoreError::Validation(format!("webhook url must be http(s): {url}")));
                }

                let host = parsed
                    .host()
                    .ok_or_else(|| CoreError::Validation(format!("webhook url must name a host: {url}")))?;

                if is_disallowed_webhook_host(&host) {
                    return Err(CoreError::Validation(format!(
                        "webhook url may not target a local or private address: {url}"
                    )));
                }

                Ok(())
            }
        }
    }
}

/// Rejects a webhook host that is loopback, unspecified, link-local
/// (including the `169.254.169.254` cloud instance-metadata address), or
/// in a private range -- a tenant-chosen webhook destination that lands
/// inside this deployment's own network is a server-side-request-forgery
/// primitive, not a legitimate delivery target, and the worker would
/// otherwise happily POST notification content to it on every retry.
///
/// This only catches an IP address or `localhost` written directly in the
/// URL. A hostname that *resolves* to one of these ranges only once DNS
/// answers -- which can also answer differently each time a request is
/// made -- is a known gap a string check at registration time cannot
/// close; closing that fully needs the resolved address checked again at
/// connect time, on every delivery attempt.
fn is_disallowed_webhook_host(host: &url::Host<&str>) -> bool {
    match host {
        // `url::Host::host_str()`/`Display` would bracket an IPv6 literal
        // for re-embedding in a URL (`[::1]`), which `IpAddr::from_str`
        // then refuses to parse -- matching the already-typed `Ipv4Addr`/
        // `Ipv6Addr` the `Host` enum carries sidesteps that entirely.
        url::Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
        url::Host::Ipv4(ip) => is_disallowed_ipv4(*ip),
        url::Host::Ipv6(ip) => {
            // An IPv4-mapped IPv6 address (`::ffff:a.b.c.d`) names exactly
            // the IPv4 address it embeds, so it has to clear the same bar
            // -- otherwise `::ffff:169.254.169.254` walks straight past
            // every check below under cover of being "an IPv6 address".
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return is_disallowed_ipv4(mapped);
            }

            let first = ip.segments()[0];
            ip.is_loopback()
                || ip.is_unspecified()
                || (first & 0xfe00) == 0xfc00 // fc00::/7, unique local
                || (first & 0xffc0) == 0xfe80 // fe80::/10, link-local
        }
    }
}

fn is_disallowed_ipv4(ip: std::net::Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
    ip.is_loopback()
        || ip.is_unspecified()
        || a == 10
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 168)
        || (a == 169 && b == 254)
        // 100.64.0.0/10, the RFC 6598 shared address space carriers use for
        // CGNAT -- and, on Alibaba Cloud, exactly where the instance-metadata
        // service answers (100.100.100.200), the same role 169.254.169.254
        // plays on AWS/GCP/Azure. Not RFC 1918 private space, so it would
        // otherwise walk straight past every check above.
        || (a == 100 && (64..=127).contains(&b))
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
    fn for_channel_is_the_inverse_of_channel_kind() {
        for channel in ChannelKind::all() {
            let address = RecipientAddress::for_channel(channel, "whatever");
            assert_eq!(address.channel_kind(), channel);
            assert_eq!(address.value(), "whatever");
        }
    }

    #[test]
    fn an_email_is_suppressed_case_insensitively() {
        let shouted = RecipientAddress::Email("Ada@Example.COM".into());
        let quiet = RecipientAddress::Email("ada@example.com".into());
        assert_eq!(shouted.suppression_key(), quiet.suppression_key());
    }

    #[test]
    fn other_address_kinds_keep_their_case() {
        // A push token is an opaque credential and a URL path is
        // case-sensitive by spec, so folding case here would merge two
        // genuinely different destinations.
        let token = RecipientAddress::PushToken("AbC123".into());
        assert_eq!(token.suppression_key(), "AbC123");

        let url = RecipientAddress::WebhookUrl("https://example.com/Hook".into());
        assert_eq!(url.suppression_key(), "https://example.com/Hook");
    }

    #[test]
    fn webhook_requires_scheme() {
        assert!(RecipientAddress::WebhookUrl("example.com/hook".into()).validate().is_err());
        assert!(RecipientAddress::WebhookUrl("https://example.com/hook".into()).validate().is_ok());
    }

    #[test]
    fn webhook_rejects_an_unparseable_url() {
        assert!(RecipientAddress::WebhookUrl("http://".into()).validate().is_err());
        assert!(RecipientAddress::WebhookUrl("not a url at all".into()).validate().is_err());
    }

    #[test]
    fn webhook_rejects_loopback_and_localhost() {
        for url in [
            "http://localhost/hook",
            "http://LOCALHOST/hook",
            "http://127.0.0.1/hook",
            "http://127.0.0.42/hook",
            "http://[::1]/hook",
        ] {
            assert!(RecipientAddress::WebhookUrl(url.into()).validate().is_err(), "accepted {url}");
        }
    }

    #[test]
    fn webhook_rejects_private_and_link_local_ranges_including_cloud_metadata() {
        for url in [
            "http://10.1.2.3/hook",
            "http://172.16.0.5/hook",
            "http://172.31.255.255/hook",
            "http://192.168.1.1/hook",
            // The AWS/GCP/Azure instance-metadata address: reachable only
            // from inside the instance, and a classic SSRF target.
            "http://169.254.169.254/latest/meta-data/",
            // Alibaba Cloud's equivalent, inside the RFC 6598 shared
            // address space (100.64.0.0/10) rather than RFC 1918 space.
            "http://100.100.100.200/latest/meta-data/",
            "http://100.64.0.1/hook",
            "http://100.127.255.255/hook",
            "http://0.0.0.0/hook",
            "http://[fc00::1]/hook",
            "http://[fe80::1]/hook",
        ] {
            assert!(RecipientAddress::WebhookUrl(url.into()).validate().is_err(), "accepted {url}");
        }
    }

    #[test]
    fn webhook_rejects_ipv4_mapped_ipv6_addresses_in_the_same_disallowed_ranges() {
        for url in [
            "http://[::ffff:127.0.0.1]/hook",
            "http://[::ffff:169.254.169.254]/latest/meta-data/",
            "http://[::ffff:10.0.0.1]/hook",
            "http://[::ffff:192.168.1.1]/hook",
        ] {
            assert!(RecipientAddress::WebhookUrl(url.into()).validate().is_err(), "accepted {url}");
        }
    }

    #[test]
    fn webhook_accepts_an_ipv4_mapped_ipv6_address_outside_any_disallowed_range() {
        assert!(RecipientAddress::WebhookUrl("http://[::ffff:8.8.8.8]/hook".into()).validate().is_ok());
    }

    #[test]
    fn webhook_does_not_reject_a_172_address_outside_the_private_range() {
        // 172.16.0.0/12 is private; 172.32.0.0 and 172.15.0.0 are not --
        // this pins the octet-16..=31 boundary down on both sides.
        assert!(RecipientAddress::WebhookUrl("http://172.32.0.1/hook".into()).validate().is_ok());
        assert!(RecipientAddress::WebhookUrl("http://172.15.255.255/hook".into()).validate().is_ok());
    }

    #[test]
    fn webhook_does_not_reject_a_100_address_outside_the_shared_address_space() {
        // 100.64.0.0/10 is the shared address space; 100.63.255.255 and
        // 100.128.0.0 fall just outside it on either side.
        assert!(RecipientAddress::WebhookUrl("http://100.63.255.255/hook".into()).validate().is_ok());
        assert!(RecipientAddress::WebhookUrl("http://100.128.0.0/hook".into()).validate().is_ok());
    }

    #[test]
    fn webhook_accepts_an_ordinary_public_host() {
        assert!(RecipientAddress::WebhookUrl("https://example.com/hook".into()).validate().is_ok());
        assert!(RecipientAddress::WebhookUrl("https://8.8.8.8/hook".into()).validate().is_ok());
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

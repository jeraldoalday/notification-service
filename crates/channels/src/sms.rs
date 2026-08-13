use async_trait::async_trait;
use ns_core::{Notification, RecipientAddress};
use serde::Serialize;

use crate::channel::NotificationChannel;
use crate::error::{ChannelError, ChannelResult};

/// Sends SMS through a Twilio-style REST provider: `POST {base_url}/messages`
/// with a bearer token and a `{to, from, body}` JSON payload.
pub struct SmsChannel {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    from_number: String,
}

impl SmsChannel {
    pub fn new(
        client: reqwest::Client,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        from_number: impl Into<String>,
    ) -> Self {
        Self { client, base_url: base_url.into(), api_key: api_key.into(), from_number: from_number.into() }
    }
}

#[derive(Serialize)]
struct SmsRequest<'a> {
    to: &'a str,
    from: &'a str,
    body: &'a str,
}

#[async_trait]
impl NotificationChannel for SmsChannel {
    async fn send(&self, notification: &Notification, address: &RecipientAddress) -> ChannelResult<()> {
        let RecipientAddress::Phone(to) = address else {
            return Err(ChannelError::AddressMismatch);
        };

        let request = SmsRequest { to, from: &self.from_number, body: &notification.body };

        let response = self
            .client
            .post(format!("{}/messages", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&request)
            .send()
            .await
            .map_err(|e| ChannelError::Transport(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(ChannelError::Rejected(format!("sms provider responded with status {}", response.status())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ns_core::ChannelKind;
    use uuid::Uuid;
    use wiremock::matchers::{bearer_token, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_notification() -> Notification {
        Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Sms)
            .body("Your code is 123456")
            .build()
            .unwrap()
    }

    fn channel(base_url: String) -> SmsChannel {
        SmsChannel::new(reqwest::Client::new(), base_url, "test-api-key", "+15555550100")
    }

    #[tokio::test]
    async fn posts_to_messages_endpoint_with_bearer_auth() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/messages"))
            .and(bearer_token("test-api-key"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;

        let channel = channel(server.uri());
        let address = RecipientAddress::Phone("+15555550199".into());

        channel.send(&sample_notification(), &address).await.unwrap();
    }

    #[tokio::test]
    async fn returns_rejected_on_non_2xx() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/messages"))
            .respond_with(ResponseTemplate::new(400))
            .mount(&server)
            .await;

        let channel = channel(server.uri());
        let address = RecipientAddress::Phone("+15555550199".into());

        let err = channel.send(&sample_notification(), &address).await.unwrap_err();
        assert!(matches!(err, ChannelError::Rejected(_)));
    }

    #[tokio::test]
    async fn rejects_mismatched_address() {
        let channel = channel("http://example.invalid".into());
        let address = RecipientAddress::Email("a@example.com".into());

        let err = channel.send(&sample_notification(), &address).await.unwrap_err();
        assert!(matches!(err, ChannelError::AddressMismatch));
    }
}

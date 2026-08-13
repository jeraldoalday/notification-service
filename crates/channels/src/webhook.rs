use async_trait::async_trait;
use ns_core::{Notification, RecipientAddress};
use serde::Serialize;
use uuid::Uuid;

use crate::channel::NotificationChannel;
use crate::error::{ChannelError, ChannelResult};

/// Delivers by POSTing a JSON payload describing the notification to the
/// recipient's webhook URL. Any non-2xx response is treated as a
/// rejection so the queue's retry/backoff can kick in.
pub struct WebhookChannel {
    client: reqwest::Client,
}

impl WebhookChannel {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl Default for WebhookChannel {
    fn default() -> Self {
        Self::new(reqwest::Client::new())
    }
}

#[derive(Serialize)]
struct WebhookPayload<'a> {
    notification_id: Uuid,
    tenant_id: Uuid,
    subject: Option<&'a str>,
    body: &'a str,
}

#[async_trait]
impl NotificationChannel for WebhookChannel {
    async fn send(&self, notification: &Notification, address: &RecipientAddress) -> ChannelResult<()> {
        let RecipientAddress::WebhookUrl(url) = address else {
            return Err(ChannelError::AddressMismatch);
        };

        let payload = WebhookPayload {
            notification_id: notification.id,
            tenant_id: notification.tenant_id,
            subject: notification.subject.as_deref(),
            body: &notification.body,
        };

        let response = self
            .client
            .post(url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| ChannelError::Transport(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(ChannelError::Rejected(format!("webhook responded with status {}", response.status())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ns_core::ChannelKind;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_notification() -> Notification {
        Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Webhook)
            .subject("Alert")
            .body("Something happened")
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn posts_payload_and_succeeds_on_2xx() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/hook"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let channel = WebhookChannel::default();
        let address = RecipientAddress::WebhookUrl(format!("{}/hook", server.uri()));

        channel.send(&sample_notification(), &address).await.unwrap();
    }

    #[tokio::test]
    async fn returns_rejected_on_non_2xx() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/hook"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let channel = WebhookChannel::default();
        let address = RecipientAddress::WebhookUrl(format!("{}/hook", server.uri()));

        let err = channel.send(&sample_notification(), &address).await.unwrap_err();
        assert!(matches!(err, ChannelError::Rejected(_)));
    }

    #[tokio::test]
    async fn rejects_mismatched_address() {
        let channel = WebhookChannel::default();
        let address = RecipientAddress::Email("a@example.com".into());

        let err = channel.send(&sample_notification(), &address).await.unwrap_err();
        assert!(matches!(err, ChannelError::AddressMismatch));
    }
}

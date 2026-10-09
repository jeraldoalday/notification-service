use std::time::Duration;

use async_trait::async_trait;
use ns_core::{Notification, RecipientAddress};
use serde::Serialize;

use crate::channel::NotificationChannel;
use crate::error::{ChannelError, ChannelResult};

/// Request timeout `PushChannel::with_default_client` gives its client, for
/// the same reason `WebhookChannel::default` does: the provider is a
/// third-party host reached over the network, and a request with no bound
/// can hang a worker slot forever instead of ever failing and tripping the
/// circuit breaker.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Sends push notifications through an FCM-style REST provider:
/// `POST {base_url}/send` with a `key={server_key}` Authorization header
/// and a `{to, notification: {title, body}}` JSON payload.
pub struct PushChannel {
    client: reqwest::Client,
    base_url: String,
    server_key: String,
}

impl PushChannel {
    pub fn new(client: reqwest::Client, base_url: impl Into<String>, server_key: impl Into<String>) -> Self {
        Self { client, base_url: base_url.into(), server_key: server_key.into() }
    }

    /// Builds the client with a bounded request timeout instead of
    /// requiring every caller to remember one. Prefer this over
    /// `new(reqwest::Client::new(), ..)`, which has no timeout at all.
    pub fn with_default_client(base_url: impl Into<String>, server_key: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .expect("the default client configuration is always valid");
        Self::new(client, base_url, server_key)
    }
}

#[derive(Serialize)]
struct PushRequest<'a> {
    to: &'a str,
    notification: PushNotificationBody<'a>,
}

#[derive(Serialize)]
struct PushNotificationBody<'a> {
    title: Option<&'a str>,
    body: &'a str,
}

#[async_trait]
impl NotificationChannel for PushChannel {
    async fn send(&self, notification: &Notification, address: &RecipientAddress) -> ChannelResult<()> {
        let RecipientAddress::PushToken(token) = address else {
            return Err(ChannelError::AddressMismatch);
        };

        let request = PushRequest {
            to: token,
            notification: PushNotificationBody {
                title: notification.subject.as_deref(),
                body: &notification.body,
            },
        };

        let response = self
            .client
            .post(format!("{}/send", self.base_url))
            .header("Authorization", format!("key={}", self.server_key))
            .json(&request)
            .send()
            .await
            .map_err(|e| ChannelError::Transport(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(ChannelError::Rejected(format!("push provider responded with status {}", response.status())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ns_core::ChannelKind;
    use uuid::Uuid;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_notification() -> Notification {
        Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Push)
            .subject("New message")
            .body("You have a new message")
            .build()
            .unwrap()
    }

    fn channel(base_url: String) -> PushChannel {
        PushChannel::new(reqwest::Client::new(), base_url, "test-server-key")
    }

    #[tokio::test]
    async fn posts_to_send_endpoint_with_server_key() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/send"))
            .and(header("Authorization", "key=test-server-key"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let channel = channel(server.uri());
        let address = RecipientAddress::PushToken("device-token-abc".into());

        channel.send(&sample_notification(), &address).await.unwrap();
    }

    #[tokio::test]
    async fn returns_rejected_on_non_2xx() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/send"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let channel = channel(server.uri());
        let address = RecipientAddress::PushToken("device-token-abc".into());

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

    #[tokio::test]
    async fn a_hung_provider_times_out_instead_of_blocking_forever() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/send"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(60)))
            .mount(&server)
            .await;

        let client = reqwest::Client::builder().timeout(Duration::from_millis(50)).build().unwrap();
        let channel = PushChannel::new(client, server.uri(), "test-server-key");
        let address = RecipientAddress::PushToken("device-token-abc".into());

        let err =
            tokio::time::timeout(Duration::from_secs(5), channel.send(&sample_notification(), &address))
                .await
                .expect("the client's own timeout should fire well before this outer guard")
                .unwrap_err();
        assert!(matches!(err, ChannelError::Transport(_)));
    }
}

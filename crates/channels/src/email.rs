use std::sync::Arc;

use async_trait::async_trait;
use lettre::message::Mailbox;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use ns_core::{Notification, RecipientAddress};

use crate::channel::NotificationChannel;
use crate::error::{ChannelError, ChannelResult};

/// Sends a constructed email somewhere. Abstracted so `EmailChannel` can
/// be unit-tested against an in-memory double instead of a real SMTP
/// server; `AsyncSmtpTransport<Tokio1Executor>` is the production
/// implementation, built by `smtp_transport` below.
#[async_trait]
pub trait EmailTransport: Send + Sync {
    async fn send(&self, message: Message) -> ChannelResult<()>;
}

#[async_trait]
impl EmailTransport for AsyncSmtpTransport<Tokio1Executor> {
    async fn send(&self, message: Message) -> ChannelResult<()> {
        AsyncTransport::send(self, message)
            .await
            .map(|_| ())
            .map_err(|e| ChannelError::Transport(e.to_string()))
    }
}

#[async_trait]
impl<T: EmailTransport + ?Sized> EmailTransport for Arc<T> {
    async fn send(&self, message: Message) -> ChannelResult<()> {
        T::send(self, message).await
    }
}

/// Builds an `AsyncSmtpTransport` authenticated against `relay` with
/// `username`/`password`, using STARTTLS over rustls.
pub fn smtp_transport(
    relay: &str,
    username: impl Into<String>,
    password: impl Into<String>,
) -> ChannelResult<AsyncSmtpTransport<Tokio1Executor>> {
    let creds = lettre::transport::smtp::authentication::Credentials::new(username.into(), password.into());
    AsyncSmtpTransport::<Tokio1Executor>::relay(relay)
        .map_err(|e| ChannelError::Transport(e.to_string()))
        .map(|builder| builder.credentials(creds).build())
}

/// Delivers by building a MIME message and handing it to an
/// `EmailTransport`.
pub struct EmailChannel<T> {
    transport: T,
    from: Mailbox,
}

impl<T: EmailTransport> EmailChannel<T> {
    pub fn new(transport: T, from: Mailbox) -> Self {
        Self { transport, from }
    }
}

#[async_trait]
impl<T: EmailTransport> NotificationChannel for EmailChannel<T> {
    async fn send(&self, notification: &Notification, address: &RecipientAddress) -> ChannelResult<()> {
        let RecipientAddress::Email(to) = address else {
            return Err(ChannelError::AddressMismatch);
        };

        let to_mailbox: Mailbox =
            to.parse().map_err(|e: lettre::address::AddressError| ChannelError::Rejected(e.to_string()))?;

        let message = Message::builder()
            .from(self.from.clone())
            .to(to_mailbox)
            .subject(notification.subject.clone().unwrap_or_default())
            .body(notification.body.clone())
            .map_err(|e| ChannelError::Rejected(e.to_string()))?;

        self.transport.send(message).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ns_core::ChannelKind;
    use std::sync::Mutex;
    use uuid::Uuid;

    #[derive(Default)]
    struct RecordingTransport {
        sent: Mutex<Vec<Vec<u8>>>,
    }

    #[async_trait]
    impl EmailTransport for RecordingTransport {
        async fn send(&self, message: Message) -> ChannelResult<()> {
            self.sent.lock().unwrap().push(message.formatted());
            Ok(())
        }
    }

    fn sample_notification() -> Notification {
        Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
            .subject("Welcome")
            .body("Hello there")
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn sends_email_with_subject_and_body() {
        let transport = Arc::new(RecordingTransport::default());
        let from: Mailbox = "sender@example.com".parse().unwrap();
        let channel = EmailChannel::new(Arc::clone(&transport), from);
        let address = RecipientAddress::Email("recipient@example.com".into());

        channel.send(&sample_notification(), &address).await.unwrap();

        let sent = transport.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let raw = String::from_utf8_lossy(&sent[0]);
        assert!(raw.contains("Welcome"));
        assert!(raw.contains("Hello there"));
        assert!(raw.contains("recipient@example.com"));
    }

    #[tokio::test]
    async fn rejects_mismatched_address() {
        let transport = Arc::new(RecordingTransport::default());
        let from: Mailbox = "sender@example.com".parse().unwrap();
        let channel = EmailChannel::new(transport, from);
        let address = RecipientAddress::Phone("+15555550100".into());

        let err = channel.send(&sample_notification(), &address).await.unwrap_err();
        assert!(matches!(err, ChannelError::AddressMismatch));
    }
}

mod support;

use std::collections::HashMap;

use ns_cli::commands::notification::{self, SubmitArgs};
use ns_cli::OutputFormat;
use ns_core::{ChannelKind, DeliveryStatus, Priority, Recipient, RecipientAddress};
use ns_queue::NotificationQueue;
use ns_storage::{NotificationRepository, RecipientRepository, SqliteStorage, TemplateRepository};
use ns_templates::Template;
use support::test_storage;
use uuid::Uuid;

async fn seed_recipient(storage: &SqliteStorage) -> Uuid {
    let recipient = Recipient::new(Uuid::new_v4(), Uuid::new_v4())
        .with_address(RecipientAddress::Email("ada@example.com".into()))
        .unwrap();
    storage.insert_recipient(&recipient).await.unwrap();
    recipient.id
}

fn args(tenant_id: Uuid, recipient_id: Uuid) -> SubmitArgs {
    SubmitArgs {
        tenant_id,
        recipient_id,
        channel: ChannelKind::Email,
        priority: Priority::Normal,
        subject: Some("Welcome".into()),
        body: Some("Hello there".into()),
        template_id: None,
        template_vars: HashMap::new(),
        idempotency_key: None,
        max_attempts: None,
        scheduled_for: None,
    }
}

#[tokio::test]
async fn submit_queues_a_notification() {
    let (storage, _dir) = test_storage().await;
    let recipient_id = seed_recipient(&storage).await;
    let tenant_id = Uuid::new_v4();

    let rendered =
        notification::submit(&storage, args(tenant_id, recipient_id), OutputFormat::Json).await.unwrap();

    let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(parsed["status"], "queued");
    let id: Uuid = parsed["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(storage.get_notification(id).await.unwrap().subject.as_deref(), Some("Welcome"));
}

#[tokio::test]
async fn submit_rejects_an_unknown_recipient() {
    let (storage, _dir) = test_storage().await;

    let err = notification::submit(&storage, args(Uuid::new_v4(), Uuid::new_v4()), OutputFormat::Text)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("recipient"), "got {err}");
}

#[tokio::test]
async fn submit_rejects_an_unknown_template() {
    let (storage, _dir) = test_storage().await;
    let recipient_id = seed_recipient(&storage).await;
    let mut args = args(Uuid::new_v4(), recipient_id);
    args.body = None;
    args.template_id = Some("nope".into());

    let err = notification::submit(&storage, args, OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("template"), "got {err}");
}

#[tokio::test]
async fn submit_rejects_missing_template_variables() {
    let (storage, _dir) = test_storage().await;
    let recipient_id = seed_recipient(&storage).await;
    let tenant_id = Uuid::new_v4();
    storage.insert_template(&Template::new(tenant_id, "welcome", "Hi {{name}}").unwrap()).await.unwrap();

    let mut args = args(tenant_id, recipient_id);
    args.body = None;
    args.template_id = Some("welcome".into());

    let err = notification::submit(&storage, args, OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("name"), "got {err}");
}

#[tokio::test]
async fn submit_accepts_a_template_with_its_variables() {
    let (storage, _dir) = test_storage().await;
    let recipient_id = seed_recipient(&storage).await;
    let tenant_id = Uuid::new_v4();
    storage.insert_template(&Template::new(tenant_id, "welcome", "Hi {{name}}").unwrap()).await.unwrap();

    let mut args = args(tenant_id, recipient_id);
    args.body = None;
    args.template_id = Some("welcome".into());
    args.template_vars = HashMap::from([("name".to_string(), "Ada".to_string())]);

    let rendered = notification::submit(&storage, args, OutputFormat::Text).await.unwrap();

    assert!(rendered.contains("template: welcome"), "got {rendered}");
}

#[tokio::test]
async fn show_renders_grep_friendly_fields() {
    let (storage, _dir) = test_storage().await;
    let recipient_id = seed_recipient(&storage).await;
    let tenant_id = Uuid::new_v4();
    let submitted =
        notification::submit(&storage, args(tenant_id, recipient_id), OutputFormat::Json).await.unwrap();
    let id: Uuid = serde_json::from_str::<serde_json::Value>(&submitted).unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    let rendered = notification::show(&storage, id, OutputFormat::Text).await.unwrap();

    assert!(rendered.contains(&format!("id: {id}")), "got {rendered}");
    assert!(rendered.contains("status: queued"), "got {rendered}");
    assert!(rendered.contains("attempts: 0/5"), "got {rendered}");
}

#[tokio::test]
async fn show_reports_an_unknown_id() {
    let (storage, _dir) = test_storage().await;
    let err = notification::show(&storage, Uuid::new_v4(), OutputFormat::Text).await.unwrap_err();
    assert!(err.to_string().contains("not found"), "got {err}");
}

#[tokio::test]
async fn list_is_tenant_scoped_and_filterable() {
    let (storage, _dir) = test_storage().await;
    let recipient_id = seed_recipient(&storage).await;
    let tenant_id = Uuid::new_v4();
    notification::submit(&storage, args(tenant_id, recipient_id), OutputFormat::Json).await.unwrap();
    notification::submit(&storage, args(Uuid::new_v4(), recipient_id), OutputFormat::Json).await.unwrap();

    let mine: serde_json::Value = serde_json::from_str(
        &notification::list(&storage, tenant_id, None, 50, OutputFormat::Json).await.unwrap(),
    )
    .unwrap();
    assert_eq!(mine.as_array().unwrap().len(), 1);

    let delivered: serde_json::Value = serde_json::from_str(
        &notification::list(&storage, tenant_id, Some(DeliveryStatus::Delivered), 50, OutputFormat::Json)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(delivered.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn list_renders_a_header_even_when_empty() {
    let (storage, _dir) = test_storage().await;

    let rendered = notification::list(&storage, Uuid::new_v4(), None, 50, OutputFormat::Text).await.unwrap();

    assert_eq!(rendered.lines().count(), 1);
    assert!(rendered.starts_with("id"), "got {rendered}");
}

#[tokio::test]
async fn cancel_moves_a_queued_notification_to_cancelled() {
    let (storage, _dir) = test_storage().await;
    let recipient_id = seed_recipient(&storage).await;
    let submitted =
        notification::submit(&storage, args(Uuid::new_v4(), recipient_id), OutputFormat::Json).await.unwrap();
    let id: Uuid = serde_json::from_str::<serde_json::Value>(&submitted).unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    notification::cancel(&storage, id, OutputFormat::Text).await.unwrap();

    assert_eq!(storage.get_notification(id).await.unwrap().status, DeliveryStatus::Cancelled);
}

#[tokio::test]
async fn cancel_refuses_work_that_is_already_sending() {
    let (storage, _dir) = test_storage().await;
    let recipient_id = seed_recipient(&storage).await;
    let submitted =
        notification::submit(&storage, args(Uuid::new_v4(), recipient_id), OutputFormat::Json).await.unwrap();
    let id: Uuid = serde_json::from_str::<serde_json::Value>(&submitted).unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    // Claiming it moves it to Sending, past the point of cancellation.
    NotificationQueue::new(storage.clone()).claim_batch(chrono::Utc::now(), 10).await.unwrap();

    let err = notification::cancel(&storage, id, OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("transition"), "got {err}");
}

#[tokio::test]
async fn replay_refuses_a_notification_that_did_not_dead_letter() {
    let (storage, _dir) = test_storage().await;
    let recipient_id = seed_recipient(&storage).await;
    let submitted =
        notification::submit(&storage, args(Uuid::new_v4(), recipient_id), OutputFormat::Json).await.unwrap();
    let id: Uuid = serde_json::from_str::<serde_json::Value>(&submitted).unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    let err = notification::replay(&storage, id, OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("replay"), "got {err}");
}

#[test]
fn parse_timestamp_accepts_rfc_3339_and_rejects_anything_else() {
    assert!(notification::parse_timestamp("2026-01-31T09:00:00Z").is_ok());
    let err = notification::parse_timestamp("next tuesday").unwrap_err();
    assert!(err.to_string().contains("RFC 3339"), "got {err}");
}

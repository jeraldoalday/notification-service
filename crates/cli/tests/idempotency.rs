mod support;

use std::collections::HashMap;

use ns_cli::commands::notification::{self, SubmitArgs};
use ns_cli::OutputFormat;
use ns_core::{ChannelKind, IdempotencyKey, Priority, Recipient, RecipientAddress};
use ns_storage::{NotificationRepository, RecipientRepository, SqliteStorage};
use support::test_storage;
use uuid::Uuid;

/// A recipient belonging to `tenant_id`, so a submission for that tenant
/// is one the service would accept.
async fn seed_recipient(storage: &SqliteStorage, tenant_id: Uuid) -> Uuid {
    let recipient = Recipient::new(Uuid::new_v4(), tenant_id)
        .with_address(RecipientAddress::Email("ada@example.com".into()))
        .unwrap();
    storage.insert_recipient(&recipient).await.unwrap();
    recipient.id
}

fn args(tenant_id: Uuid, recipient_id: Uuid, key: Option<&str>) -> SubmitArgs {
    SubmitArgs {
        tenant_id,
        recipient_id,
        channel: ChannelKind::Email,
        priority: Priority::Normal,
        subject: Some("Welcome".into()),
        body: Some("Hello there".into()),
        template_id: None,
        template_vars: HashMap::new(),
        topic: None,
        idempotency_key: key.map(|k| IdempotencyKey::new(k).unwrap()),
        max_attempts: None,
        scheduled_for: None,
    }
}

fn parse(rendered: &str) -> serde_json::Value {
    serde_json::from_str(rendered).unwrap()
}

#[tokio::test]
async fn a_keyed_submission_records_the_key_it_was_submitted_under() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    let recipient_id = seed_recipient(&storage, tenant_id).await;

    let rendered =
        notification::submit(&storage, args(tenant_id, recipient_id, Some("order-1041")), OutputFormat::Json)
            .await
            .unwrap();

    assert_eq!(parse(&rendered)["idempotency_key"], "order-1041");
}

#[tokio::test]
async fn re_running_the_same_command_queues_nothing_extra() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    let recipient_id = seed_recipient(&storage, tenant_id).await;

    let first =
        notification::submit(&storage, args(tenant_id, recipient_id, Some("order-1041")), OutputFormat::Json)
            .await
            .unwrap();
    let second =
        notification::submit(&storage, args(tenant_id, recipient_id, Some("order-1041")), OutputFormat::Json)
            .await
            .unwrap();

    // An operator re-running a command they weren't sure had gone through
    // is exactly who this is for.
    assert_eq!(parse(&first)["id"], parse(&second)["id"]);
    assert_eq!(storage.list_by_tenant(tenant_id, None, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_deduplicated_run_says_so_in_the_text_output() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    let recipient_id = seed_recipient(&storage, tenant_id).await;
    notification::submit(&storage, args(tenant_id, recipient_id, Some("order-1041")), OutputFormat::Text)
        .await
        .unwrap();

    let rendered =
        notification::submit(&storage, args(tenant_id, recipient_id, Some("order-1041")), OutputFormat::Text)
            .await
            .unwrap();

    // Printing an id it didn't create, silently, is the one case where
    // saying nothing would mislead.
    assert!(rendered.contains("nothing was queued"), "got {rendered}");
    assert!(rendered.contains("idempotency-key: order-1041"), "got {rendered}");
}

#[tokio::test]
async fn an_unkeyed_run_is_never_deduplicated() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    let recipient_id = seed_recipient(&storage, tenant_id).await;

    for _ in 0..2 {
        let rendered =
            notification::submit(&storage, args(tenant_id, recipient_id, None), OutputFormat::Text)
                .await
                .unwrap();
        assert!(!rendered.contains("nothing was queued"), "got {rendered}");
    }

    assert_eq!(storage.list_by_tenant(tenant_id, None, 10).await.unwrap().len(), 2);
}

#[tokio::test]
async fn a_notification_can_be_found_by_the_key_it_was_submitted_under() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    let recipient_id = seed_recipient(&storage, tenant_id).await;
    let submitted =
        notification::submit(&storage, args(tenant_id, recipient_id, Some("order-1041")), OutputFormat::Json)
            .await
            .unwrap();

    let found = notification::by_key(&storage, tenant_id, "order-1041", OutputFormat::Json).await.unwrap();

    assert_eq!(parse(&found)["id"], parse(&submitted)["id"]);
}

#[tokio::test]
async fn an_unused_key_is_an_error_rather_than_empty_output() {
    let (storage, _dir) = test_storage().await;

    let err =
        notification::by_key(&storage, Uuid::new_v4(), "never-used", OutputFormat::Text).await.unwrap_err();

    // "never submitted" and "submitted, here it is" must not be told
    // apart by noticing that stdout was blank.
    assert!(err.to_string().contains("never-used"), "got {err}");
}

#[tokio::test]
async fn a_key_lookup_does_not_cross_tenants() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    let recipient_id = seed_recipient(&storage, tenant_id).await;
    notification::submit(&storage, args(tenant_id, recipient_id, Some("order-1041")), OutputFormat::Json)
        .await
        .unwrap();

    let err =
        notification::by_key(&storage, Uuid::new_v4(), "order-1041", OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("order-1041"), "got {err}");
}

#[tokio::test]
async fn a_malformed_key_is_reported_before_anything_is_looked_up() {
    let (storage, _dir) = test_storage().await;

    let err =
        notification::by_key(&storage, Uuid::new_v4(), "has spaces", OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("idempotency key"), "got {err}");
}

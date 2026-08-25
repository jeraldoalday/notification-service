mod support;

use ns_cli::commands;
use ns_cli::OutputFormat;
use ns_core::{ChannelKind, RecipientAddress, SuppressionReason};
use ns_storage::SuppressionRepository;
use support::test_storage;
use uuid::Uuid;

#[tokio::test]
async fn add_suppresses_an_address() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();

    let rendered = commands::suppression::add(
        &storage,
        tenant,
        ChannelKind::Email,
        "ada@example.com".into(),
        SuppressionReason::Unsubscribed,
        Some("asked via the footer link".into()),
        OutputFormat::Text,
    )
    .await
    .unwrap();

    assert!(rendered.contains("address: ada@example.com"), "got {rendered}");
    assert!(rendered.contains("reason: unsubscribed"), "got {rendered}");
    assert!(rendered.contains("note: asked via the footer link"), "got {rendered}");

    let address = RecipientAddress::Email("ada@example.com".into());
    assert!(storage.find_suppression(tenant, &address).await.unwrap().is_some());
}

#[tokio::test]
async fn add_rejects_an_address_that_could_never_be_sent_to() {
    let (storage, _dir) = test_storage().await;

    let err = commands::suppression::add(
        &storage,
        Uuid::new_v4(),
        ChannelKind::Sms,
        "555-0100".into(),
        SuppressionReason::Manual,
        None,
        OutputFormat::Text,
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("E.164"), "got {err}");
}

#[tokio::test]
async fn add_is_idempotent_and_updates_the_reason() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();

    for reason in [SuppressionReason::Bounced, SuppressionReason::Complaint] {
        commands::suppression::add(
            &storage,
            tenant,
            ChannelKind::Email,
            "ada@example.com".into(),
            reason,
            None,
            OutputFormat::Text,
        )
        .await
        .unwrap();
    }

    let listed = storage.list_suppressions(tenant, 10).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].reason, SuppressionReason::Complaint);
}

#[tokio::test]
async fn list_renders_a_table_and_says_so_when_empty() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();

    let empty = commands::suppression::list(&storage, tenant, 100, OutputFormat::Text).await.unwrap();
    assert_eq!(empty, "no suppressed addresses\n");

    commands::suppression::add(
        &storage,
        tenant,
        ChannelKind::Sms,
        "+15555550100".into(),
        SuppressionReason::Complaint,
        None,
        OutputFormat::Text,
    )
    .await
    .unwrap();

    let rendered = commands::suppression::list(&storage, tenant, 100, OutputFormat::Text).await.unwrap();
    let lines: Vec<&str> = rendered.lines().collect();
    assert!(lines[0].starts_with("channel"), "got {rendered}");
    assert!(lines[1].contains("+15555550100") && lines[1].contains("complaint"), "got {rendered}");
}

#[tokio::test]
async fn list_is_tenant_scoped() {
    let (storage, _dir) = test_storage().await;
    let mine = Uuid::new_v4();
    for tenant in [mine, Uuid::new_v4()] {
        commands::suppression::add(
            &storage,
            tenant,
            ChannelKind::Email,
            "ada@example.com".into(),
            SuppressionReason::Manual,
            None,
            OutputFormat::Text,
        )
        .await
        .unwrap();
    }

    let rendered = commands::suppression::list(&storage, mine, 100, OutputFormat::Json).await.unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(parsed.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn check_answers_both_ways_without_erroring() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();

    // "Not suppressed" is a good answer, not a failure -- exiting
    // non-zero would make this useless in a shell conditional.
    let clean = commands::suppression::check(
        &storage,
        tenant,
        ChannelKind::Email,
        "ada@example.com".into(),
        OutputFormat::Text,
    )
    .await
    .unwrap();
    assert!(clean.contains("is not suppressed"), "got {clean}");

    commands::suppression::add(
        &storage,
        tenant,
        ChannelKind::Email,
        "ada@example.com".into(),
        SuppressionReason::Bounced,
        None,
        OutputFormat::Text,
    )
    .await
    .unwrap();

    let found = commands::suppression::check(
        &storage,
        tenant,
        ChannelKind::Email,
        "ADA@EXAMPLE.COM".into(),
        OutputFormat::Text,
    )
    .await
    .unwrap();
    assert!(found.contains("reason: bounced"), "case shouldn't matter for email: {found}");
}

#[tokio::test]
async fn remove_lifts_a_suppression() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();
    commands::suppression::add(
        &storage,
        tenant,
        ChannelKind::Email,
        "ada@example.com".into(),
        SuppressionReason::Bounced,
        None,
        OutputFormat::Text,
    )
    .await
    .unwrap();

    let rendered = commands::suppression::remove(
        &storage,
        tenant,
        ChannelKind::Email,
        "ada@example.com".into(),
        OutputFormat::Text,
    )
    .await
    .unwrap();

    assert!(rendered.contains("lifted suppression"), "got {rendered}");
    let address = RecipientAddress::Email("ada@example.com".into());
    assert!(storage.find_suppression(tenant, &address).await.unwrap().is_none());
}

#[tokio::test]
async fn remove_reports_an_address_that_was_never_suppressed() {
    let (storage, _dir) = test_storage().await;

    let err = commands::suppression::remove(
        &storage,
        Uuid::new_v4(),
        ChannelKind::Email,
        "ada@example.com".into(),
        OutputFormat::Text,
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("is not suppressed"), "got {err}");
}

#[tokio::test]
async fn remove_cannot_reach_another_tenants_list() {
    let (storage, _dir) = test_storage().await;
    let owner = Uuid::new_v4();
    commands::suppression::add(
        &storage,
        owner,
        ChannelKind::Email,
        "ada@example.com".into(),
        SuppressionReason::Unsubscribed,
        None,
        OutputFormat::Text,
    )
    .await
    .unwrap();

    assert!(commands::suppression::remove(
        &storage,
        Uuid::new_v4(),
        ChannelKind::Email,
        "ada@example.com".into(),
        OutputFormat::Text,
    )
    .await
    .is_err());

    let address = RecipientAddress::Email("ada@example.com".into());
    assert!(storage.find_suppression(owner, &address).await.unwrap().is_some());
}

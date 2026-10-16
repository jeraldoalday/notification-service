mod support;

use chrono::{Duration, Utc};
use ns_cli::commands;
use ns_cli::OutputFormat;
use ns_core::{AttemptResult, ChannelKind, DeliveryAttempt, Notification};
use ns_storage::{DeliveryAttemptRepository, NotificationRepository, SqliteStorage};
use support::test_storage;
use uuid::Uuid;

async fn seed_notification(storage: &SqliteStorage) -> Notification {
    let notification = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ChannelKind::Email)
        .body("hi")
        .max_attempts(3)
        .build()
        .unwrap();
    storage.insert_notification(&notification).await.unwrap();
    notification
}

async fn record(
    storage: &SqliteStorage,
    notification: &Notification,
    result: AttemptResult,
    error: Option<&str>,
    at: chrono::DateTime<Utc>,
) {
    let mut attempt = DeliveryAttempt::new(notification, result, at);
    if let Some(error) = error {
        attempt = attempt.with_error(error);
    }
    if result != AttemptResult::Skipped {
        attempt = attempt.with_duration_ms(87);
    }
    storage.record_attempt(&attempt).await.unwrap();
}

#[tokio::test]
async fn attempts_renders_a_history_table() {
    let (storage, _dir) = test_storage().await;
    let notification = seed_notification(&storage).await;
    record(
        &storage,
        &notification,
        AttemptResult::Skipped,
        Some("rate limit exhausted for email"),
        Utc::now() - Duration::minutes(2),
    )
    .await;
    record(&storage, &notification, AttemptResult::Failed, Some("550 mailbox full"), Utc::now()).await;

    let rendered =
        commands::notification::attempts(&storage, notification.id, 100, OutputFormat::Text).await.unwrap();

    let lines: Vec<&str> = rendered.lines().collect();
    assert!(lines[0].starts_with('#'), "there's a header: {rendered}");
    assert!(lines[1].contains("skipped") && lines[1].contains("rate limit"), "{rendered}");
    assert!(lines[2].contains("failed") && lines[2].contains("550 mailbox full"), "{rendered}");
    assert!(lines[2].contains("87ms"), "a real attempt reports how long it took: {rendered}");
}

#[tokio::test]
async fn a_skipped_attempt_reports_no_duration() {
    let (storage, _dir) = test_storage().await;
    let notification = seed_notification(&storage).await;
    record(&storage, &notification, AttemptResult::Skipped, Some("circuit breaker open"), Utc::now()).await;

    let rendered =
        commands::notification::attempts(&storage, notification.id, 100, OutputFormat::Text).await.unwrap();

    assert!(rendered.contains(" - "), "nothing was timed, because nothing was sent: {rendered}");
}

#[tokio::test]
async fn attempts_says_so_when_there_is_no_history() {
    let (storage, _dir) = test_storage().await;
    let notification = seed_notification(&storage).await;

    let rendered =
        commands::notification::attempts(&storage, notification.id, 100, OutputFormat::Text).await.unwrap();

    assert_eq!(rendered, "no delivery attempts recorded\n");
}

#[tokio::test]
async fn attempts_reports_an_unknown_id_as_missing_rather_than_empty() {
    let (storage, _dir) = test_storage().await;

    let err = commands::notification::attempts(&storage, Uuid::new_v4(), 100, OutputFormat::Text)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("not found"), "got {err}");
}

#[tokio::test]
async fn attempts_json_output_carries_the_full_record() {
    let (storage, _dir) = test_storage().await;
    let notification = seed_notification(&storage).await;
    record(&storage, &notification, AttemptResult::Delivered, None, Utc::now()).await;

    let rendered =
        commands::notification::attempts(&storage, notification.id, 100, OutputFormat::Json).await.unwrap();

    let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(parsed[0]["result"], "delivered");
    assert_eq!(parsed[0]["attempt_number"], 1);
    assert_eq!(parsed[0]["notification_id"], notification.id.to_string());
    assert_eq!(parsed[0]["duration_ms"], 87);
}

#[tokio::test]
async fn attempts_respects_its_limit() {
    let (storage, _dir) = test_storage().await;
    let notification = seed_notification(&storage).await;
    for minutes_ago in [3, 2, 1] {
        record(
            &storage,
            &notification,
            AttemptResult::Failed,
            Some("nope"),
            Utc::now() - Duration::minutes(minutes_ago),
        )
        .await;
    }

    let rendered =
        commands::notification::attempts(&storage, notification.id, 2, OutputFormat::Json).await.unwrap();

    let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(parsed.as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn purge_attempts_drops_only_what_is_older_than_the_cutoff() {
    let (storage, _dir) = test_storage().await;
    let notification = seed_notification(&storage).await;
    record(&storage, &notification, AttemptResult::Failed, Some("old"), Utc::now() - Duration::days(45))
        .await;
    record(&storage, &notification, AttemptResult::Delivered, None, Utc::now()).await;

    let rendered = commands::queue::purge_attempts(&storage, 30, false, OutputFormat::Text).await.unwrap();

    assert!(rendered.starts_with("purged 1 delivery attempt(s)"), "got {rendered}");
    let remaining = storage.list_attempts(notification.id, 10).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].result, AttemptResult::Delivered);
}

#[tokio::test]
async fn purge_attempts_dry_run_reports_without_deleting() {
    let (storage, _dir) = test_storage().await;
    let notification = seed_notification(&storage).await;
    record(&storage, &notification, AttemptResult::Failed, Some("old"), Utc::now() - Duration::days(45))
        .await;
    record(&storage, &notification, AttemptResult::Delivered, None, Utc::now()).await;

    let rendered = commands::queue::purge_attempts(&storage, 30, true, OutputFormat::Text).await.unwrap();

    assert!(rendered.starts_with("would purge 1 delivery attempt(s)"), "got {rendered}");
    let remaining = storage.list_attempts(notification.id, 10).await.unwrap();
    assert_eq!(remaining.len(), 2, "a dry run must not delete anything");

    // And it's safe to run as many times as it takes to pick the right
    // cutoff -- running it again reports the same count.
    let rendered_again =
        commands::queue::purge_attempts(&storage, 30, true, OutputFormat::Text).await.unwrap();
    assert!(rendered_again.starts_with("would purge 1 delivery attempt(s)"), "got {rendered_again}");
}

#[tokio::test]
async fn purge_attempts_leaves_the_notifications_themselves_alone() {
    let (storage, _dir) = test_storage().await;
    let notification = seed_notification(&storage).await;
    record(&storage, &notification, AttemptResult::Failed, Some("old"), Utc::now() - Duration::days(45))
        .await;

    commands::queue::purge_attempts(&storage, 1, false, OutputFormat::Text).await.unwrap();

    assert!(storage.get_notification(notification.id).await.is_ok());
}

#[tokio::test]
async fn purge_attempts_refuses_a_cutoff_of_now() {
    let (storage, _dir) = test_storage().await;

    let err = commands::queue::purge_attempts(&storage, 0, false, OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("at least 1"), "got {err}");
}

#[tokio::test]
async fn purge_attempts_reports_the_cutoff_it_used() {
    let (storage, _dir) = test_storage().await;

    let rendered = commands::queue::purge_attempts(&storage, 7, false, OutputFormat::Json).await.unwrap();

    let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(parsed["purged"], 0);
    let cutoff: chrono::DateTime<Utc> = parsed["cutoff"].as_str().unwrap().parse().unwrap();
    assert!((Utc::now() - cutoff - Duration::days(7)).num_seconds().abs() < 5);
}

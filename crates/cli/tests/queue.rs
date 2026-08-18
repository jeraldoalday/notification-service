mod support;

use ns_cli::commands;
use ns_cli::OutputFormat;
use ns_core::{ChannelKind, DeliveryStatus, Notification};
use ns_queue::NotificationQueue;
use ns_storage::NotificationRepository;
use support::test_storage;
use uuid::Uuid;

#[tokio::test]
async fn stats_reports_every_status_including_the_empty_ones() {
    let (storage, _dir) = test_storage().await;
    let queue = NotificationQueue::new(storage.clone());
    let tenant_id = Uuid::new_v4();
    let notification =
        Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email).body("hi").build().unwrap();
    queue.submit(notification, chrono::Utc::now()).await.unwrap();

    let rendered = commands::queue::stats(&storage, None, OutputFormat::Text).await.unwrap();

    assert!(rendered.contains("queued"), "got {rendered}");
    assert!(rendered.contains("dead_lettered"), "an empty status still gets a row: {rendered}");
    let total_line = rendered.lines().last().unwrap();
    assert!(total_line.starts_with("total") && total_line.ends_with('1'), "got {rendered}");
}

#[tokio::test]
async fn stats_can_be_narrowed_to_one_tenant() {
    let (storage, _dir) = test_storage().await;
    let queue = NotificationQueue::new(storage.clone());
    let mine = Uuid::new_v4();
    for tenant_id in [mine, Uuid::new_v4()] {
        let notification =
            Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email).body("hi").build().unwrap();
        queue.submit(notification, chrono::Utc::now()).await.unwrap();
    }

    let rendered = commands::queue::stats(&storage, Some(mine), OutputFormat::Json).await.unwrap();

    let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(parsed["total"], 1);
    assert_eq!(parsed["tenant_id"], mine.to_string());
}

#[tokio::test]
async fn stats_on_an_empty_database_totals_zero() {
    let (storage, _dir) = test_storage().await;

    let rendered = commands::queue::stats(&storage, None, OutputFormat::Json).await.unwrap();

    let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(parsed["total"], 0);
    let counts = parsed["counts"].as_array().unwrap();
    assert_eq!(counts.len(), DeliveryStatus::all().len());
    assert!(counts.iter().all(|entry| entry["count"] == 0));
}

#[tokio::test]
async fn json_output_is_machine_readable() {
    let (storage, _dir) = test_storage().await;
    let _ = storage.count_by_status(None).await.unwrap();

    let rendered = commands::queue::stats(&storage, None, OutputFormat::Json).await.unwrap();

    serde_json::from_str::<serde_json::Value>(&rendered).expect("valid JSON");
}

/// Drives one notification all the way to `DeadLettered` through the
/// ordinary failure path, so replay is exercised against a row the queue
/// itself produced.
async fn dead_letter(storage: &ns_storage::SqliteStorage, tenant_id: Uuid) -> Uuid {
    let queue = NotificationQueue::new(storage.clone());
    let notification = Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email)
        .body("hi")
        .max_attempts(1)
        .build()
        .unwrap();
    let submitted = queue.submit(notification, chrono::Utc::now()).await.unwrap();

    let claimed = queue.claim_batch(chrono::Utc::now(), 10).await.unwrap();
    let claimed = claimed.into_iter().find(|n| n.id == submitted.id).unwrap();
    let failed = queue.fail(claimed, chrono::Utc::now()).await.unwrap();
    assert_eq!(failed.status, DeliveryStatus::DeadLettered);
    failed.id
}

#[tokio::test]
async fn due_lists_only_work_the_worker_would_claim_now() {
    let (storage, _dir) = test_storage().await;
    let queue = NotificationQueue::new(storage.clone());
    let tenant_id = Uuid::new_v4();

    let ready =
        Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email).body("ready").build().unwrap();
    let ready = queue.submit(ready, chrono::Utc::now()).await.unwrap();

    let later = Notification::builder(tenant_id, Uuid::new_v4(), ChannelKind::Email)
        .body("later")
        .scheduled_for(chrono::Utc::now() + chrono::Duration::hours(1))
        .build()
        .unwrap();
    queue.submit(later, chrono::Utc::now()).await.unwrap();

    let rendered = commands::queue::due(&storage, 25, OutputFormat::Text).await.unwrap();

    assert!(rendered.contains(&ready.id.to_string()), "got {rendered}");
    assert_eq!(rendered.lines().count(), 2, "header plus one due row: {rendered}");
}

#[tokio::test]
async fn replay_requeues_dead_lettered_work_as_new_notifications() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    let dead_id = dead_letter(&storage, tenant_id).await;

    let rendered = commands::queue::replay(&storage, tenant_id, 25, OutputFormat::Json).await.unwrap();

    let replayed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    let replayed = replayed.as_array().unwrap();
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0]["status"], "queued");
    assert_ne!(replayed[0]["id"].as_str().unwrap(), dead_id.to_string());
    assert_eq!(
        storage.get_notification(dead_id).await.unwrap().status,
        DeliveryStatus::DeadLettered,
        "the original stays as the record of what happened"
    );
}

#[tokio::test]
async fn replay_is_tenant_scoped() {
    let (storage, _dir) = test_storage().await;
    let mine = Uuid::new_v4();
    dead_letter(&storage, mine).await;
    dead_letter(&storage, Uuid::new_v4()).await;

    let rendered = commands::queue::replay(&storage, mine, 25, OutputFormat::Json).await.unwrap();

    assert_eq!(serde_json::from_str::<serde_json::Value>(&rendered).unwrap().as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn replay_says_so_when_there_is_nothing_to_replay() {
    let (storage, _dir) = test_storage().await;

    let rendered = commands::queue::replay(&storage, Uuid::new_v4(), 25, OutputFormat::Text).await.unwrap();

    assert_eq!(rendered.trim(), "nothing dead-lettered to replay");
}

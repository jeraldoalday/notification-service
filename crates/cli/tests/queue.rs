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

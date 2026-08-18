mod support;

use ns_cli::commands::recipient::{self, AddArgs};
use ns_cli::OutputFormat;
use support::test_storage;
use uuid::Uuid;

#[tokio::test]
async fn add_registers_a_recipient_with_its_channels() {
    let (storage, _dir) = test_storage().await;

    let args = AddArgs {
        display_name: Some("Ada Lovelace".into()),
        email: Some("ada@example.com".into()),
        phone: Some("+15555550100".into()),
        ..AddArgs::default()
    };
    let rendered = recipient::add(&storage, args, OutputFormat::Text).await.unwrap();

    assert!(rendered.contains("name: Ada Lovelace"), "got {rendered}");
    assert!(rendered.contains("email"), "got {rendered}");
    assert!(rendered.contains("sms"), "a phone number means the sms channel: {rendered}");
}

#[tokio::test]
async fn add_requires_at_least_one_address() {
    let (storage, _dir) = test_storage().await;

    let err = recipient::add(&storage, AddArgs::default(), OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("--email"), "got {err}");
}

#[tokio::test]
async fn add_rejects_a_malformed_address() {
    let (storage, _dir) = test_storage().await;

    let args = AddArgs { phone: Some("555-0100".into()), ..AddArgs::default() };
    let err = recipient::add(&storage, args, OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("E.164"), "got {err}");
}

#[tokio::test]
async fn show_round_trips_an_added_recipient() {
    let (storage, _dir) = test_storage().await;
    let args = AddArgs { email: Some("ada@example.com".into()), ..AddArgs::default() };
    let added = recipient::add(&storage, args, OutputFormat::Json).await.unwrap();
    let id: Uuid =
        serde_json::from_str::<serde_json::Value>(&added).unwrap()["id"].as_str().unwrap().parse().unwrap();

    let rendered = recipient::show(&storage, id, OutputFormat::Text).await.unwrap();

    assert!(rendered.contains(&id.to_string()), "got {rendered}");
}

#[tokio::test]
async fn show_reports_an_unknown_recipient() {
    let (storage, _dir) = test_storage().await;
    let err = recipient::show(&storage, Uuid::new_v4(), OutputFormat::Text).await.unwrap_err();
    assert!(err.to_string().contains("not found"), "got {err}");
}

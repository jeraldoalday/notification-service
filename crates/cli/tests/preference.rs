mod support;

use ns_cli::commands;
use ns_cli::OutputFormat;
use ns_core::{ChannelKind, PreferenceSetting, Recipient, RecipientAddress, Topic};
use ns_storage::{PreferenceRepository, RecipientRepository, SqliteStorage};
use support::test_storage;
use uuid::Uuid;

fn topic(raw: &str) -> Topic {
    Topic::new(raw).unwrap()
}

async fn seed_recipient(storage: &SqliteStorage, tenant_id: Uuid) -> Uuid {
    let recipient = Recipient::new(Uuid::new_v4(), tenant_id)
        .with_address(RecipientAddress::Email("ada@example.com".into()))
        .unwrap();
    storage.insert_recipient(&recipient).await.unwrap();
    recipient.id
}

async fn block(storage: &SqliteStorage, tenant: Uuid, recipient: Uuid, name: &str) -> String {
    commands::preference::set(
        storage,
        tenant,
        recipient,
        topic(name),
        None,
        PreferenceSetting::Blocked,
        OutputFormat::Text,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn set_records_an_answer_and_describes_it() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();
    let recipient = seed_recipient(&storage, tenant).await;

    let rendered = block(&storage, tenant, recipient, "marketing").await;

    assert!(rendered.contains("marketing"), "{rendered}");
    assert!(rendered.contains("blocked"), "{rendered}");
    // The stored spelling of a wildcard is `*`, which is right for a key
    // and wrong for a person reading a terminal.
    assert!(rendered.contains("all-channel"), "{rendered}");

    let stored = storage.preferences_for(tenant, recipient, &topic("marketing")).await.unwrap();
    assert!(!stored.allows(ChannelKind::Email));
}

#[tokio::test]
async fn set_replaces_an_existing_answer() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();
    let recipient = seed_recipient(&storage, tenant).await;

    block(&storage, tenant, recipient, "marketing").await;
    commands::preference::set(
        &storage,
        tenant,
        recipient,
        topic("marketing"),
        None,
        PreferenceSetting::Allowed,
        OutputFormat::Text,
    )
    .await
    .unwrap();

    let listed = storage.list_preferences(tenant, recipient, 10).await.unwrap();
    assert_eq!(listed.len(), 1, "one answer, not two");
    assert_eq!(listed[0].setting, PreferenceSetting::Allowed);
}

#[tokio::test]
async fn set_refuses_another_tenants_recipient() {
    // A preference keyed by somebody else's recipient would sit there
    // looking like the opt-out was recorded while matching nothing.
    let (storage, _dir) = test_storage().await;
    let recipient = seed_recipient(&storage, Uuid::new_v4()).await;

    let err = commands::preference::set(
        &storage,
        Uuid::new_v4(),
        recipient,
        topic("marketing"),
        None,
        PreferenceSetting::Blocked,
        OutputFormat::Text,
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("belongs to tenant"), "{err}");
}

#[tokio::test]
async fn list_reports_every_answer_a_recipient_has_given() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();
    let recipient = seed_recipient(&storage, tenant).await;
    block(&storage, tenant, recipient, "marketing").await;
    block(&storage, tenant, recipient, "digest").await;

    let rendered =
        commands::preference::list(&storage, tenant, recipient, 50, OutputFormat::Text).await.unwrap();

    assert!(rendered.contains("marketing") && rendered.contains("digest"), "{rendered}");
}

#[tokio::test]
async fn list_says_so_when_nothing_has_been_recorded() {
    let (storage, _dir) = test_storage().await;

    let rendered =
        commands::preference::list(&storage, Uuid::new_v4(), Uuid::new_v4(), 50, OutputFormat::Text)
            .await
            .unwrap();

    assert!(rendered.contains("no preferences"), "{rendered}");
}

#[tokio::test]
async fn check_reports_the_verdict_and_what_decided_it() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();
    let recipient = seed_recipient(&storage, tenant).await;
    block(&storage, tenant, recipient, "orders").await;
    commands::preference::set(
        &storage,
        tenant,
        recipient,
        topic("orders"),
        Some(ChannelKind::Sms),
        PreferenceSetting::Allowed,
        OutputFormat::Text,
    )
    .await
    .unwrap();

    let sms = commands::preference::check(
        &storage,
        tenant,
        recipient,
        topic("orders"),
        ChannelKind::Sms,
        OutputFormat::Text,
    )
    .await
    .unwrap();
    assert!(sms.contains("would be sent"), "{sms}");
    assert!(!sms.contains("would not be sent"), "{sms}");

    let email = commands::preference::check(
        &storage,
        tenant,
        recipient,
        topic("orders"),
        ChannelKind::Email,
        OutputFormat::Text,
    )
    .await
    .unwrap();
    assert!(email.contains("would not be sent"), "{email}");
    assert!(email.contains("all-channel"), "{email}");
}

#[tokio::test]
async fn check_distinguishes_silence_from_an_explicit_yes() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();
    let recipient = seed_recipient(&storage, tenant).await;

    let quiet = commands::preference::check(
        &storage,
        tenant,
        recipient,
        topic("anything"),
        ChannelKind::Email,
        OutputFormat::Text,
    )
    .await
    .unwrap();
    assert!(quiet.contains("no preference"), "{quiet}");

    commands::preference::set(
        &storage,
        tenant,
        recipient,
        topic("anything"),
        None,
        PreferenceSetting::Allowed,
        OutputFormat::Text,
    )
    .await
    .unwrap();

    let explicit = commands::preference::check(
        &storage,
        tenant,
        recipient,
        topic("anything"),
        ChannelKind::Email,
        OutputFormat::Text,
    )
    .await
    .unwrap();
    assert!(explicit.contains("would be sent") && !explicit.contains("no preference"), "{explicit}");
}

#[tokio::test]
async fn check_is_a_json_object_a_script_can_read() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();
    let recipient = seed_recipient(&storage, tenant).await;
    block(&storage, tenant, recipient, "marketing").await;

    let rendered = commands::preference::check(
        &storage,
        tenant,
        recipient,
        topic("marketing"),
        ChannelKind::Email,
        OutputFormat::Json,
    )
    .await
    .unwrap();

    let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(parsed["allowed"], false);
    assert_eq!(parsed["setting"], "blocked");
    assert_eq!(parsed["topic"], "marketing");
}

#[tokio::test]
async fn clear_forgets_one_answer() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();
    let recipient = seed_recipient(&storage, tenant).await;
    block(&storage, tenant, recipient, "marketing").await;

    let rendered = commands::preference::clear(
        &storage,
        tenant,
        recipient,
        topic("marketing"),
        None,
        OutputFormat::Text,
    )
    .await
    .unwrap();

    assert!(rendered.contains("cleared"), "{rendered}");
    assert!(storage.preferences_for(tenant, recipient, &topic("marketing")).await.unwrap().is_empty());
}

#[tokio::test]
async fn clearing_an_answer_that_was_never_given_is_an_error() {
    // Clearing re-enables sending to somebody who asked not to be sent
    // to, so a mistyped topic must not be reported as a success.
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();
    let recipient = seed_recipient(&storage, tenant).await;

    let err = commands::preference::clear(
        &storage,
        tenant,
        recipient,
        topic("marketing"),
        None,
        OutputFormat::Text,
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("marketing"), "{err}");
}

#[tokio::test]
async fn submitting_a_topic_the_recipient_opted_out_of_is_refused() {
    use ns_cli::commands::notification::SubmitArgs;

    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();
    let recipient = seed_recipient(&storage, tenant).await;
    block(&storage, tenant, recipient, "marketing").await;

    let args = |topic: Option<Topic>| SubmitArgs {
        tenant_id: tenant,
        recipient_id: recipient,
        channel: ChannelKind::Email,
        priority: ns_core::Priority::Normal,
        subject: Some("Hello".into()),
        body: Some("Hello there".into()),
        template_id: None,
        template_vars: Default::default(),
        topic,
        idempotency_key: None,
        max_attempts: None,
        scheduled_for: None,
    };

    let err = commands::notification::submit(&storage, args(Some(topic("marketing"))), OutputFormat::Text)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("opted out"), "{err}");

    // And the same message with no topic still goes out: nobody was
    // offered the chance to decline it.
    assert!(commands::notification::submit(&storage, args(None), OutputFormat::Text).await.is_ok());
}

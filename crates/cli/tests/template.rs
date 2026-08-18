mod support;

use std::collections::HashMap;
use std::io::Write;

use ns_cli::commands::template::{self, SaveArgs};
use ns_cli::OutputFormat;
use ns_core::ChannelKind;
use ns_storage::{SqliteStorage, TemplateRepository};
use support::test_storage;
use uuid::Uuid;

fn args(tenant_id: Uuid, id: &str, body: &str) -> SaveArgs {
    SaveArgs {
        tenant_id,
        id: id.to_string(),
        name: None,
        channel: None,
        subject: None,
        body: Some(body.to_string()),
        body_file: None,
    }
}

async fn seed(storage: &SqliteStorage, tenant_id: Uuid, id: &str) {
    template::create(storage, args(tenant_id, id, "Hi {{name}}"), OutputFormat::Text).await.unwrap();
}

#[tokio::test]
async fn create_stores_a_template() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();

    let mut save = args(tenant_id, "welcome", "Hi {{name}}, welcome to {{product}}.");
    save.subject = Some("Welcome, {{name}}".into());
    save.channel = Some(ChannelKind::Email);
    let rendered = template::create(&storage, save, OutputFormat::Text).await.unwrap();

    assert!(rendered.contains("variables: name, product"), "got {rendered}");
    let stored = storage.get_template(tenant_id, "welcome").await.unwrap();
    assert_eq!(stored.channel, Some(ChannelKind::Email));
}

#[tokio::test]
async fn create_reads_the_body_from_a_file() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "Dear {{{{name}}}},\n\nYour order shipped.").unwrap();

    let mut save = args(tenant_id, "shipped", "");
    save.body = None;
    save.body_file = Some(file.path().to_path_buf());
    template::create(&storage, save, OutputFormat::Text).await.unwrap();

    let stored = storage.get_template(tenant_id, "shipped").await.unwrap();
    assert!(stored.body.contains("Your order shipped."), "got {}", stored.body);
}

#[tokio::test]
async fn create_rejects_both_body_and_body_file() {
    let (storage, _dir) = test_storage().await;
    let mut save = args(Uuid::new_v4(), "welcome", "Hi");
    save.body_file = Some("/tmp/whatever".into());

    let err = template::create(&storage, save, OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("not both"), "got {err}");
}

#[tokio::test]
async fn create_requires_a_body() {
    let (storage, _dir) = test_storage().await;
    let mut save = args(Uuid::new_v4(), "welcome", "");
    save.body = None;

    let err = template::create(&storage, save, OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("--body"), "got {err}");
}

#[tokio::test]
async fn create_reports_a_missing_body_file() {
    let (storage, _dir) = test_storage().await;
    let mut save = args(Uuid::new_v4(), "welcome", "");
    save.body = None;
    save.body_file = Some("/definitely/not/here.txt".into());

    let err = template::create(&storage, save, OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("/definitely/not/here.txt"), "got {err}");
}

#[tokio::test]
async fn create_rejects_an_unparseable_body() {
    let (storage, _dir) = test_storage().await;

    let err = template::create(&storage, args(Uuid::new_v4(), "broken", "Hi {{name"), OutputFormat::Text)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("unclosed"), "got {err}");
}

#[tokio::test]
async fn create_conflicts_on_a_duplicate_id() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    seed(&storage, tenant_id, "welcome").await;

    let err = template::create(&storage, args(tenant_id, "welcome", "different"), OutputFormat::Text)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("already exists"), "got {err}");
}

#[tokio::test]
async fn update_replaces_the_body() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    seed(&storage, tenant_id, "welcome").await;

    template::update(&storage, args(tenant_id, "welcome", "Hey {{name}}!"), OutputFormat::Text)
        .await
        .unwrap();

    assert_eq!(storage.get_template(tenant_id, "welcome").await.unwrap().body, "Hey {{name}}!");
}

#[tokio::test]
async fn update_reports_an_unknown_template() {
    let (storage, _dir) = test_storage().await;

    let err =
        template::update(&storage, args(Uuid::new_v4(), "nope", "hi"), OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("not found"), "got {err}");
}

#[tokio::test]
async fn list_is_tenant_scoped() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    seed(&storage, tenant_id, "welcome").await;
    seed(&storage, Uuid::new_v4(), "someone_elses").await;

    let rendered = template::list(&storage, tenant_id, 50, OutputFormat::Text).await.unwrap();

    assert!(rendered.contains("welcome"), "got {rendered}");
    assert!(!rendered.contains("someone_elses"), "got {rendered}");
}

#[tokio::test]
async fn delete_removes_the_template() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    seed(&storage, tenant_id, "welcome").await;

    template::delete(&storage, tenant_id, "welcome", OutputFormat::Text).await.unwrap();

    assert!(storage.get_template(tenant_id, "welcome").await.is_err());
}

#[tokio::test]
async fn render_substitutes_the_supplied_variables() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    seed(&storage, tenant_id, "welcome").await;

    let vars = HashMap::from([("name".to_string(), "Ada".to_string())]);
    let rendered = template::render(&storage, tenant_id, "welcome", vars, OutputFormat::Text).await.unwrap();

    assert_eq!(rendered.trim(), "Hi Ada");
}

#[tokio::test]
async fn render_names_every_missing_variable_in_one_message() {
    let (storage, _dir) = test_storage().await;
    let tenant_id = Uuid::new_v4();
    template::create(&storage, args(tenant_id, "welcome", "{{a}} {{b}}"), OutputFormat::Text).await.unwrap();

    let err = template::render(&storage, tenant_id, "welcome", HashMap::new(), OutputFormat::Text)
        .await
        .unwrap_err();

    let message = err.to_string();
    assert!(message.contains('a') && message.contains('b'), "got {message}");
}

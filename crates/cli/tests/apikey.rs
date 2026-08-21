mod support;

use ns_auth::Scope;
use ns_cli::commands::apikey;
use ns_cli::OutputFormat;
use ns_storage::ApiKeyRepository;
use support::test_storage;
use uuid::Uuid;

fn parse_json(rendered: &str) -> serde_json::Value {
    serde_json::from_str(rendered).unwrap()
}

#[tokio::test]
async fn create_prints_the_token_exactly_once_and_stores_only_its_digest() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();

    let rendered =
        apikey::create(&storage, tenant, "ci-deploy".into(), vec![Scope::Write], OutputFormat::Json)
            .await
            .unwrap();

    let created = parse_json(&rendered);
    let token = created["token"].as_str().unwrap();
    assert!(token.starts_with("nsk_"), "{token}");

    let stored = storage.list_api_keys(tenant, 10).await.unwrap();
    assert_eq!(stored.len(), 1);
    assert_ne!(stored[0].token_digest, token);
    assert!(stored[0].authenticate(token).is_ok(), "the printed token must be the one that works");
}

#[tokio::test]
async fn the_text_rendering_says_the_token_will_not_be_shown_again() {
    let (storage, _dir) = test_storage().await;

    let rendered =
        apikey::create(&storage, Uuid::new_v4(), "ci".into(), vec![Scope::Read], OutputFormat::Text)
            .await
            .unwrap();

    assert!(rendered.contains("nsk_"), "{rendered}");
    assert!(rendered.contains("shown once"), "{rendered}");
}

#[tokio::test]
async fn create_refuses_a_key_with_no_scopes() {
    let (storage, _dir) = test_storage().await;

    let err =
        apikey::create(&storage, Uuid::new_v4(), "ci".into(), vec![], OutputFormat::Text).await.unwrap_err();

    assert!(err.to_string().contains("--scope"), "{err}");
    assert!(
        storage.list_api_keys(Uuid::new_v4(), 10).await.unwrap().is_empty(),
        "a refused create must not leave a key behind"
    );
}

#[tokio::test]
async fn list_shows_a_tenants_keys_without_any_secret_material() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();

    let created = parse_json(
        &apikey::create(&storage, tenant, "ci".into(), vec![Scope::Write], OutputFormat::Json).await.unwrap(),
    );
    let token = created["token"].as_str().unwrap().to_string();

    let rendered = apikey::list(&storage, tenant, 10, OutputFormat::Json).await.unwrap();

    assert!(!rendered.contains(&token), "{rendered}");
    let listed = parse_json(&rendered);
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["name"], "ci");
    assert_eq!(listed[0]["scopes"], serde_json::json!(["write"]));
    assert!(listed[0]["revoked_at"].is_null());
}

#[tokio::test]
async fn list_is_scoped_to_one_tenant() {
    let (storage, _dir) = test_storage().await;
    let ours = Uuid::new_v4();
    let theirs = Uuid::new_v4();

    apikey::create(&storage, ours, "ours".into(), vec![Scope::Read], OutputFormat::Json).await.unwrap();
    apikey::create(&storage, theirs, "theirs".into(), vec![Scope::Read], OutputFormat::Json).await.unwrap();

    let listed = parse_json(&apikey::list(&storage, ours, 10, OutputFormat::Json).await.unwrap());
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["name"], "ours");
}

#[tokio::test]
async fn revoke_stops_the_token_working_and_is_idempotent() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();

    let created = parse_json(
        &apikey::create(&storage, tenant, "ci".into(), vec![Scope::Write], OutputFormat::Json).await.unwrap(),
    );
    let id: Uuid = created["id"].as_str().unwrap().parse().unwrap();
    let token = created["token"].as_str().unwrap();

    let first = apikey::revoke(&storage, id, OutputFormat::Text).await.unwrap();
    assert!(first.contains("revoked key"), "{first}");

    let key = storage.get_api_key(id).await.unwrap();
    assert!(key.authenticate(token).is_err());
    let revoked_at = key.revoked_at.unwrap();

    let second = apikey::revoke(&storage, id, OutputFormat::Text).await.unwrap();
    assert!(second.contains("already revoked"), "{second}");
    assert_eq!(
        storage.get_api_key(id).await.unwrap().revoked_at,
        Some(revoked_at),
        "re-revoking must not rewrite when the credential actually stopped working"
    );
}

#[tokio::test]
async fn revoking_an_unknown_key_reports_it() {
    let (storage, _dir) = test_storage().await;
    let err = apikey::revoke(&storage, Uuid::new_v4(), OutputFormat::Text).await.unwrap_err();
    assert!(err.to_string().contains("not found"), "{err}");
}

#[tokio::test]
async fn a_revoked_key_still_appears_in_the_listing() {
    let (storage, _dir) = test_storage().await;
    let tenant = Uuid::new_v4();

    let created = parse_json(
        &apikey::create(&storage, tenant, "ci".into(), vec![Scope::Read], OutputFormat::Json).await.unwrap(),
    );
    let id: Uuid = created["id"].as_str().unwrap().parse().unwrap();
    apikey::revoke(&storage, id, OutputFormat::Text).await.unwrap();

    let text = apikey::list(&storage, tenant, 10, OutputFormat::Text).await.unwrap();
    assert!(text.contains("revoked"), "an access audit has to include access that was taken away: {text}");
}

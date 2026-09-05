mod support;

use axum::http::StatusCode;
use ns_auth::Scope;
use serde_json::{json, Value};
use support::{
    delete, get, issue_key, json_body, put_json, send, test_app_with_auth, test_app_without_auth, with_auth,
};
use uuid::Uuid;

const TENANT: &str = "33333333-3333-3333-3333-333333333333";

fn tenant() -> Uuid {
    TENANT.parse().unwrap()
}

async fn create_recipient(app: &axum::Router) -> String {
    let response = send(
        app,
        support::post_json(
            "/recipients",
            json!({"tenant_id": TENANT, "addresses": [{"type": "email", "value": "ada@example.com"}]}),
        ),
    )
    .await;
    let body: Value = json_body(response).await;
    body["id"].as_str().unwrap().to_string()
}

fn set_body(recipient: &str, topic: &str, channel: Option<&str>, setting: &str) -> Value {
    let mut body = json!({
        "tenant_id": TENANT,
        "recipient_id": recipient,
        "topic": topic,
        "setting": setting,
    });
    if let Some(channel) = channel {
        body["channel"] = json!(channel);
    }
    body
}

async fn set(app: &axum::Router, body: Value) -> axum::http::Response<axum::body::Body> {
    send(app, put_json("/preferences", body)).await
}

#[tokio::test]
async fn records_and_lists_a_recipients_answer() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;

    let response = set(&app, set_body(&recipient, "marketing", None, "blocked")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let created: Value = json_body(response).await;
    assert_eq!(created["topic"], "marketing");
    assert_eq!(created["setting"], "blocked");
    assert!(created.get("channel").is_none(), "a wildcard reports no channel: {created}");

    let listed: Vec<Value> = json_body(
        send(&app, get(&format!("/preferences?tenant_id={TENANT}&recipient_id={recipient}"))).await,
    )
    .await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["topic"], "marketing");
}

#[tokio::test]
async fn saving_the_same_answer_twice_is_not_an_error() {
    // An unsubscribe link followed twice, or a preference form saved
    // unchanged, must not look like a failure.
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;

    set(&app, set_body(&recipient, "marketing", None, "blocked")).await;
    let second = set(&app, set_body(&recipient, "marketing", None, "blocked")).await;
    assert_eq!(second.status(), StatusCode::OK);

    let listed: Vec<Value> = json_body(
        send(&app, get(&format!("/preferences?tenant_id={TENANT}&recipient_id={recipient}"))).await,
    )
    .await;
    assert_eq!(listed.len(), 1, "one answer, not two");
}

#[tokio::test]
async fn changing_an_answer_replaces_it() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;

    set(&app, set_body(&recipient, "marketing", None, "blocked")).await;
    set(&app, set_body(&recipient, "marketing", None, "allowed")).await;

    let listed: Vec<Value> = json_body(
        send(&app, get(&format!("/preferences?tenant_id={TENANT}&recipient_id={recipient}"))).await,
    )
    .await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["setting"], "allowed");
}

#[tokio::test]
async fn a_wildcard_and_a_channel_answer_coexist() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;

    set(&app, set_body(&recipient, "orders", None, "blocked")).await;
    set(&app, set_body(&recipient, "orders", Some("sms"), "allowed")).await;

    let listed: Vec<Value> = json_body(
        send(&app, get(&format!("/preferences?tenant_id={TENANT}&recipient_id={recipient}"))).await,
    )
    .await;
    assert_eq!(listed.len(), 2);
}

#[tokio::test]
async fn resolving_reports_the_answer_and_what_decided_it() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;
    set(&app, set_body(&recipient, "orders", None, "blocked")).await;
    set(&app, set_body(&recipient, "orders", Some("sms"), "allowed")).await;

    let sms: Value = json_body(
        send(
            &app,
            get(&format!(
                "/preferences/resolve?tenant_id={TENANT}&recipient_id={recipient}&topic=orders&channel=sms"
            )),
        )
        .await,
    )
    .await;
    assert_eq!(sms["allowed"], true);
    assert_eq!(sms["deciding"]["channel"], "sms", "the more specific row decided it");

    let email: Value = json_body(
        send(
            &app,
            get(&format!(
                "/preferences/resolve?tenant_id={TENANT}&recipient_id={recipient}&topic=orders&channel=email"
            )),
        )
        .await,
    )
    .await;
    assert_eq!(email["allowed"], false);
    assert!(email["deciding"]["channel"].is_null(), "the wildcard decided it: {email}");
}

#[tokio::test]
async fn resolving_a_topic_nobody_has_an_opinion_about_allows_it() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;

    let resolved: Value = json_body(
        send(
            &app,
            get(&format!(
                "/preferences/resolve?tenant_id={TENANT}&recipient_id={recipient}&topic=anything&channel=email"
            )),
        )
        .await,
    )
    .await;

    assert_eq!(resolved["allowed"], true);
    assert_eq!(resolved["setting"], "allowed");
    // Silence, not an explicit yes -- and a preference centre needs to be
    // able to tell those apart.
    assert!(resolved["deciding"].is_null(), "{resolved}");
}

#[tokio::test]
async fn forgetting_an_answer_removes_only_that_one() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;
    set(&app, set_body(&recipient, "orders", None, "blocked")).await;
    set(&app, set_body(&recipient, "orders", Some("sms"), "allowed")).await;

    let response =
        send(&app, delete(&format!("/preferences?tenant_id={TENANT}&recipient_id={recipient}&topic=orders")))
            .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let listed: Vec<Value> = json_body(
        send(&app, get(&format!("/preferences?tenant_id={TENANT}&recipient_id={recipient}"))).await,
    )
    .await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["channel"], "sms");
}

#[tokio::test]
async fn forgetting_an_answer_that_was_never_given_is_a_404() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;

    let response =
        send(&app, delete(&format!("/preferences?tenant_id={TENANT}&recipient_id={recipient}&topic=orders")))
            .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_unusable_topic_or_setting_is_refused() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;

    assert_eq!(
        set(&app, set_body(&recipient, "Marketing", None, "blocked")).await.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        set(&app, set_body(&recipient, "marketing", None, "maybe")).await.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[tokio::test]
async fn a_preference_for_an_unknown_recipient_is_refused() {
    // A preference naming nobody is a row that can never match a send.
    let (app, _dir, _storage) = test_app_without_auth().await;

    let response = set(&app, set_body(&Uuid::new_v4().to_string(), "marketing", None, "blocked")).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn another_tenants_recipient_is_not_found() {
    let (app, _dir, storage) = test_app_with_auth().await;
    let bearer = issue_key(&storage, tenant(), &[Scope::Read, Scope::Write]).await;

    // Registered under a different tenant, so the caller's key must not
    // be able to record an answer for it -- and must not learn it exists.
    let stranger = ns_core::Recipient::new(Uuid::new_v4(), Uuid::new_v4())
        .with_address(ns_core::RecipientAddress::Email("stranger@example.com".into()))
        .unwrap();
    ns_storage::RecipientRepository::insert_recipient(&storage, &stranger).await.unwrap();

    let response = send(
        &app,
        with_auth(
            put_json(
                "/preferences",
                json!({"recipient_id": stranger.id, "topic": "marketing", "setting": "blocked"}),
            ),
            &bearer,
        ),
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn recording_an_answer_needs_more_than_a_read_key() {
    // An unsubscribe handler changes what the service will send, so it
    // needs `write` -- but not `admin`, or it would need a credential
    // that could also mint API keys.
    let (app, _dir, storage) = test_app_with_auth().await;
    let read_only = issue_key(&storage, tenant(), &[Scope::Read]).await;

    let response = send(
        &app,
        with_auth(
            put_json(
                "/preferences",
                json!({"recipient_id": Uuid::new_v4(), "topic": "marketing", "setting": "blocked"}),
            ),
            &read_only,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let deletion = send(
        &app,
        with_auth(
            delete(&format!("/preferences?recipient_id={}&topic=marketing", Uuid::new_v4())),
            &read_only,
        ),
    )
    .await;
    assert_eq!(deletion.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn preferences_require_a_credential() {
    let (app, _dir, _storage) = test_app_with_auth().await;

    let response = send(&app, get(&format!("/preferences?recipient_id={}", Uuid::new_v4()))).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

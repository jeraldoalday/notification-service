mod support;

use axum::http::StatusCode;
use serde_json::{json, Value};
use support::{
    delete, get, issue_key, json_body, post_json, send, test_app_with_auth, test_app_without_auth, with_auth,
};
use uuid::Uuid;

const TENANT: &str = "11111111-1111-1111-1111-111111111111";

fn tenant() -> Uuid {
    TENANT.parse().unwrap()
}

async fn suppress(app: &axum::Router, address: &str, reason: &str) -> axum::http::Response<axum::body::Body> {
    send(
        app,
        post_json(
            "/suppressions",
            json!({"tenant_id": TENANT, "channel": "email", "address": address, "reason": reason}),
        ),
    )
    .await
}

async fn create_recipient(app: &axum::Router, address: &str) -> String {
    let response = send(
        app,
        post_json(
            "/recipients",
            json!({"tenant_id": TENANT, "addresses": [{"type": "email", "value": address}]}),
        ),
    )
    .await;
    let body: Value = json_body(response).await;
    body["id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn suppresses_and_lists_an_address() {
    let (app, _dir, _storage) = test_app_without_auth().await;

    let response = suppress(&app, "ada@example.com", "unsubscribed").await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: Value = json_body(response).await;
    assert_eq!(body["address"], "ada@example.com");
    assert_eq!(body["channel"], "email");
    assert_eq!(body["reason"], "unsubscribed");

    let listed: Vec<Value> =
        json_body(send(&app, get(&format!("/suppressions?tenant_id={TENANT}"))).await).await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["address"], "ada@example.com");
}

#[tokio::test]
async fn the_listing_reports_the_address_as_it_was_given() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    suppress(&app, "Ada@Example.COM", "bounced").await;

    let listed: Vec<Value> =
        json_body(send(&app, get(&format!("/suppressions?tenant_id={TENANT}"))).await).await;

    assert_eq!(listed[0]["address"], "Ada@Example.COM", "an operator should see what they typed");
}

#[tokio::test]
async fn suppressing_the_same_address_twice_succeeds_and_keeps_one_row() {
    let (app, _dir, _storage) = test_app_without_auth().await;

    suppress(&app, "ada@example.com", "bounced").await;
    // A retried provider webhook must not look like a failure.
    let second = suppress(&app, "ada@example.com", "complaint").await;
    assert_eq!(second.status(), StatusCode::CREATED);

    let listed: Vec<Value> =
        json_body(send(&app, get(&format!("/suppressions?tenant_id={TENANT}"))).await).await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["reason"], "complaint", "the newer reason wins");
}

#[tokio::test]
async fn an_unusable_address_is_refused() {
    let (app, _dir, _storage) = test_app_without_auth().await;

    let response = suppress(&app, "not-an-email", "manual").await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn an_unknown_reason_is_refused() {
    let (app, _dir, _storage) = test_app_without_auth().await;

    let response = suppress(&app, "ada@example.com", "annoyed").await;

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn looking_up_one_address_matches_case_insensitively() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    suppress(&app, "Ada@Example.COM", "unsubscribed").await;

    let response = send(
        &app,
        get(&format!("/suppressions/lookup?tenant_id={TENANT}&channel=email&address=ada@example.com")),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = json_body(response).await;
    assert_eq!(body["reason"], "unsubscribed");
}

#[tokio::test]
async fn looking_up_an_unsuppressed_address_is_a_404() {
    let (app, _dir, _storage) = test_app_without_auth().await;

    let response = send(
        &app,
        get(&format!("/suppressions/lookup?tenant_id={TENANT}&channel=email&address=ada@example.com")),
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn deleting_lifts_the_suppression() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    suppress(&app, "ada@example.com", "bounced").await;

    let response = send(
        &app,
        delete(&format!("/suppressions?tenant_id={TENANT}&channel=email&address=ada@example.com")),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let listed: Vec<Value> =
        json_body(send(&app, get(&format!("/suppressions?tenant_id={TENANT}"))).await).await;
    assert!(listed.is_empty());
}

#[tokio::test]
async fn deleting_something_that_was_never_suppressed_is_a_404() {
    let (app, _dir, _storage) = test_app_without_auth().await;

    // Re-enabling sending to someone who asked not to be sent to is worth
    // being told about when the address was slightly wrong.
    let response = send(
        &app,
        delete(&format!("/suppressions?tenant_id={TENANT}&channel=email&address=ada@example.com")),
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn submitting_to_a_suppressed_address_is_refused_up_front() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient_id = create_recipient(&app, "ada@example.com").await;
    suppress(&app, "ada@example.com", "unsubscribed").await;

    let response = send(
        &app,
        post_json(
            "/notifications",
            json!({
                "tenant_id": TENANT,
                "recipient_id": recipient_id,
                "channel": "email",
                "body": "hello again"
            }),
        ),
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body: Value = json_body(response).await;
    assert!(body["error"].as_str().unwrap().contains("suppressed"), "{body}");
}

#[tokio::test]
async fn suppressing_one_channel_leaves_the_others_sendable() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let response = send(
        &app,
        post_json(
            "/recipients",
            json!({
                "tenant_id": TENANT,
                "addresses": [
                    {"type": "email", "value": "ada@example.com"},
                    {"type": "phone", "value": "+15555550100"}
                ]
            }),
        ),
    )
    .await;
    let recipient: Value = json_body(response).await;
    suppress(&app, "ada@example.com", "unsubscribed").await;

    // "Stop emailing me" is not "stop sending me one-time passcodes".
    let response = send(
        &app,
        post_json(
            "/notifications",
            json!({
                "tenant_id": TENANT,
                "recipient_id": recipient["id"],
                "channel": "sms",
                "body": "your code is 123456"
            }),
        ),
    )
    .await;

    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn one_tenants_list_is_invisible_to_another() {
    let (app, _dir, storage) = test_app_with_auth().await;
    let owner = issue_key(&storage, tenant(), &[ns_auth::Scope::Write]).await;
    send(
        &app,
        with_auth(
            post_json(
                "/suppressions",
                json!({"channel": "email", "address": "ada@example.com", "reason": "unsubscribed"}),
            ),
            &owner,
        ),
    )
    .await;

    let stranger = issue_key(&storage, Uuid::new_v4(), &[ns_auth::Scope::Write]).await;
    let listed: Vec<Value> = json_body(send(&app, with_auth(get("/suppressions"), &stranger)).await).await;
    assert!(listed.is_empty());

    let response =
        send(&app, with_auth(delete("/suppressions?channel=email&address=ada@example.com"), &stranger)).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND, "and cannot be lifted by them either");
}

#[tokio::test]
async fn reading_the_list_needs_read_but_changing_it_needs_write() {
    let (app, _dir, storage) = test_app_with_auth().await;
    let reader = issue_key(&storage, tenant(), &[ns_auth::Scope::Read]).await;

    let response = send(&app, with_auth(get("/suppressions"), &reader)).await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = send(
        &app,
        with_auth(
            post_json(
                "/suppressions",
                json!({"channel": "email", "address": "ada@example.com", "reason": "manual"}),
            ),
            &reader,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn the_list_requires_a_credential() {
    let (app, _dir, _storage) = test_app_with_auth().await;

    let response = send(&app, get("/suppressions")).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

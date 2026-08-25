mod support;

use axum::http::StatusCode;
use chrono::Utc;
use ns_core::{AttemptResult, DeliveryAttempt, Notification};
use ns_storage::{DeliveryAttemptRepository, NotificationRepository, SqliteStorage};
use serde_json::{json, Value};
use support::{
    get, issue_key, json_body, post_json, send, test_app_with_auth, test_app_without_auth, with_auth,
};
use uuid::Uuid;

const TENANT: &str = "11111111-1111-1111-1111-111111111111";

fn tenant() -> Uuid {
    TENANT.parse().unwrap()
}

async fn submit_notification(app: &axum::Router) -> Uuid {
    let recipient = send(
        app,
        post_json(
            "/recipients",
            json!({"tenant_id": TENANT, "addresses": [{"type": "email", "value": "ada@example.com"}]}),
        ),
    )
    .await;
    let recipient: Value = json_body(recipient).await;

    let response = send(
        app,
        post_json(
            "/notifications",
            json!({
                "tenant_id": TENANT,
                "recipient_id": recipient["id"],
                "channel": "email",
                "body": "Hello there"
            }),
        ),
    )
    .await;
    let body: Value = json_body(response).await;
    body["id"].as_str().unwrap().parse().unwrap()
}

/// Writes history directly, the way the worker would. These tests are
/// about the endpoints, so they seed the log rather than running a
/// dispatch pass to produce one.
async fn record(storage: &SqliteStorage, notification_id: Uuid, result: AttemptResult, error: Option<&str>) {
    record_at(storage, notification_id, result, error, Utc::now()).await
}

/// The same, at a chosen instant. Ordering assertions use this rather than
/// recording twice in a row: stored timestamps are millisecond-precision
/// and ties break on a random uuid, so two attempts written back to back
/// have no defined order -- which is fine for the service and useless for
/// a test.
async fn record_at(
    storage: &SqliteStorage,
    notification_id: Uuid,
    result: AttemptResult,
    error: Option<&str>,
    occurred_at: chrono::DateTime<Utc>,
) {
    let notification = storage.get_notification(notification_id).await.unwrap();
    let mut attempt = DeliveryAttempt::new(&notification, result, occurred_at);
    if let Some(error) = error {
        attempt = attempt.with_error(error);
    }
    if result != AttemptResult::Skipped {
        attempt = attempt.with_duration_ms(120);
    }
    storage.record_attempt(&attempt).await.unwrap();
}

#[tokio::test]
async fn a_notification_with_no_attempts_has_an_empty_history() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let id = submit_notification(&app).await;

    let response = send(&app, get(&format!("/notifications/{id}/attempts"))).await;

    assert_eq!(response.status(), StatusCode::OK);
    let body: Vec<Value> = json_body(response).await;
    assert!(body.is_empty());
}

#[tokio::test]
async fn history_reads_oldest_attempt_first() {
    let (app, _dir, storage) = test_app_without_auth().await;
    let id = submit_notification(&app).await;

    let earlier = Utc::now() - chrono::Duration::minutes(2);
    record_at(&storage, id, AttemptResult::Skipped, Some("rate limit exhausted for email"), earlier).await;
    // Spend an attempt, so the second record carries the next number.
    let mut notification = storage.get_notification(id).await.unwrap();
    notification.record_attempt(Utc::now());
    storage.update_notification(&notification).await.unwrap();
    record(&storage, id, AttemptResult::Failed, Some("550 mailbox full")).await;

    let response = send(&app, get(&format!("/notifications/{id}/attempts"))).await;
    let body: Vec<Value> = json_body(response).await;

    assert_eq!(body.len(), 2);
    assert_eq!(body[0]["result"], "skipped");
    assert_eq!(body[0]["attempt_number"], 1);
    assert_eq!(body[0]["reached_provider"], false);
    assert!(body[0]["duration_ms"].is_null());

    assert_eq!(body[1]["result"], "failed");
    assert_eq!(body[1]["attempt_number"], 2);
    assert_eq!(body[1]["reached_provider"], true);
    assert_eq!(body[1]["error"], "550 mailbox full");
    assert_eq!(body[1]["duration_ms"], 120);
}

#[tokio::test]
async fn history_does_not_echo_the_tenant_back() {
    let (app, _dir, storage) = test_app_without_auth().await;
    let id = submit_notification(&app).await;
    record(&storage, id, AttemptResult::Delivered, None).await;

    let response = send(&app, get(&format!("/notifications/{id}/attempts"))).await;
    let body: Vec<Value> = json_body(response).await;

    assert!(body[0].get("tenant_id").is_none(), "the route is already scoped to one tenant");
    assert_eq!(body[0]["notification_id"], id.to_string());
}

#[tokio::test]
async fn an_explicit_limit_is_honoured_and_capped() {
    let (app, _dir, storage) = test_app_without_auth().await;
    let id = submit_notification(&app).await;
    for _ in 0..5 {
        record(&storage, id, AttemptResult::Skipped, Some("circuit breaker open for email")).await;
    }

    let response = send(&app, get(&format!("/notifications/{id}/attempts?limit=2"))).await;
    let body: Vec<Value> = json_body(response).await;
    assert_eq!(body.len(), 2);

    // Over-large limits clamp rather than being refused, matching the
    // notification and template listings.
    let response = send(&app, get(&format!("/notifications/{id}/attempts?limit=100000"))).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Vec<Value> = json_body(response).await;
    assert_eq!(body.len(), 5);
}

#[tokio::test]
async fn asking_about_an_unknown_notification_is_a_404() {
    let (app, _dir, _storage) = test_app_without_auth().await;

    let response = send(&app, get(&format!("/notifications/{}/attempts", Uuid::new_v4()))).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn another_tenants_history_is_indistinguishable_from_a_missing_one() {
    let (app, _dir, storage) = test_app_with_auth().await;

    // A notification belonging to somebody else entirely.
    let notification = Notification::builder(Uuid::new_v4(), Uuid::new_v4(), ns_core::ChannelKind::Email)
        .body("not yours")
        .build()
        .unwrap();
    storage.insert_notification(&notification).await.unwrap();
    record(&storage, notification.id, AttemptResult::Failed, Some("secret provider detail")).await;

    let bearer = issue_key(&storage, tenant(), &[ns_auth::Scope::Read]).await;
    let response =
        send(&app, with_auth(get(&format!("/notifications/{}/attempts", notification.id)), &bearer)).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND, "a 403 would confirm the id names something real");
}

#[tokio::test]
async fn reading_history_needs_only_the_read_scope() {
    let (app, _dir, storage) = test_app_with_auth().await;
    let bearer = issue_key(&storage, tenant(), &[ns_auth::Scope::Read]).await;

    let response = send(&app, with_auth(get("/attempts"), &bearer)).await;

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn history_requires_a_credential() {
    let (app, _dir, _storage) = test_app_with_auth().await;

    let response = send(&app, get(&format!("/notifications/{}/attempts", Uuid::new_v4()))).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_tenant_wide_listing_spans_notifications_newest_first() {
    let (app, _dir, storage) = test_app_without_auth().await;
    let first = submit_notification(&app).await;
    let second = submit_notification(&app).await;

    let earlier = Utc::now() - chrono::Duration::minutes(2);
    record_at(&storage, first, AttemptResult::Failed, Some("older"), earlier).await;
    record(&storage, second, AttemptResult::Delivered, None).await;

    let response = send(&app, get(&format!("/attempts?tenant_id={TENANT}"))).await;
    let body: Vec<Value> = json_body(response).await;

    assert_eq!(body.len(), 2);
    assert_eq!(body[0]["notification_id"], second.to_string(), "newest first");
}

#[tokio::test]
async fn the_tenant_wide_listing_shows_only_the_callers_tenant() {
    let (app, _dir, storage) = test_app_with_auth().await;

    // Seeded directly rather than over HTTP: this app requires a
    // credential, and the point of the test is what a *different*
    // tenant's key can see.
    let notification = Notification::builder(tenant(), Uuid::new_v4(), ns_core::ChannelKind::Email)
        .body("mine")
        .build()
        .unwrap();
    storage.insert_notification(&notification).await.unwrap();
    record(&storage, notification.id, AttemptResult::Delivered, None).await;

    let stranger = issue_key(&storage, Uuid::new_v4(), &[ns_auth::Scope::Read]).await;
    let response = send(&app, with_auth(get("/attempts"), &stranger)).await;
    let body: Vec<Value> = json_body(response).await;

    assert!(body.is_empty());
}

#[tokio::test]
async fn naming_another_tenant_explicitly_is_refused() {
    let (app, _dir, storage) = test_app_with_auth().await;
    let bearer = issue_key(&storage, tenant(), &[ns_auth::Scope::Read]).await;

    let response =
        send(&app, with_auth(get(&format!("/attempts?tenant_id={}", Uuid::new_v4())), &bearer)).await;

    assert_eq!(response.status(), StatusCode::FORBIDDEN, "an explicit mismatch is worth telling the client");
}

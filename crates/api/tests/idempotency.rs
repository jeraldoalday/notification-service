mod support;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ns_core::{RecipientAddress, Suppression, SuppressionReason};
use ns_storage::SuppressionRepository;
use serde_json::{json, Value};
use support::{get, json_body, post_json, send, test_app, test_app_without_auth};
use uuid::Uuid;

/// The tenant every test in this file works under.
const TENANT: &str = "11111111-1111-1111-1111-111111111111";

async fn create_recipient(app: &axum::Router) -> String {
    let response = send(
        app,
        post_json(
            "/recipients",
            json!({"tenant_id": TENANT, "addresses": [{"type": "email", "value": "ada@example.com"}]}),
        ),
    )
    .await;
    let body: Value = json_body(response).await;
    body["id"].as_str().unwrap().to_string()
}

fn submission(recipient_id: &str) -> Value {
    json!({
        "tenant_id": TENANT,
        "recipient_id": recipient_id,
        "channel": "email",
        "subject": "Welcome",
        "body": "Hello there"
    })
}

/// A `POST /notifications` carrying an `Idempotency-Key`.
fn keyed_submit(key: &str, body: Value) -> Request<Body> {
    let (mut parts, body) = post_json("/notifications", body).into_parts();
    parts.headers.insert("idempotency-key", key.parse().unwrap());
    Request::from_parts(parts, body)
}

#[tokio::test]
async fn a_first_keyed_submission_is_created_like_any_other() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;

    let response = send(&app, keyed_submit("order-1041", submission(&recipient_id))).await;

    assert_eq!(response.status(), StatusCode::CREATED);
    assert!(response.headers().get("idempotent-replay").is_none());
    let created: Value = json_body(response).await;
    assert_eq!(created["status"], "queued");
    assert_eq!(created["idempotency_key"], "order-1041");
}

#[tokio::test]
async fn a_repeated_submission_returns_the_first_notification() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;

    let first = send(&app, keyed_submit("order-1041", submission(&recipient_id))).await;
    let first: Value = json_body(first).await;

    let retry = send(&app, keyed_submit("order-1041", submission(&recipient_id))).await;

    // 200, not 201: nothing was created by this call.
    assert_eq!(retry.status(), StatusCode::OK);
    assert_eq!(retry.headers().get("idempotent-replay").unwrap(), "true");
    let retried: Value = json_body(retry).await;
    assert_eq!(retried["id"], first["id"]);
}

#[tokio::test]
async fn a_repeated_submission_queues_nothing_extra() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;

    for _ in 0..3 {
        send(&app, keyed_submit("order-1041", submission(&recipient_id))).await;
    }

    // The point of the whole feature: one message, however many times the
    // client asked.
    let listed: Value = json_body(send(&app, get(&format!("/notifications?tenant_id={TENANT}"))).await).await;
    assert_eq!(listed.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn submissions_without_a_key_are_never_deduplicated() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;

    for _ in 0..3 {
        let response = send(&app, post_json("/notifications", submission(&recipient_id))).await;
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    // Identical requests, deliberately sent three times, are three
    // messages unless the caller said otherwise.
    let listed: Value = json_body(send(&app, get(&format!("/notifications?tenant_id={TENANT}"))).await).await;
    assert_eq!(listed.as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn reusing_a_key_for_a_different_message_is_a_conflict() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;
    send(&app, keyed_submit("order-1041", submission(&recipient_id))).await;

    let mut different = submission(&recipient_id);
    different["body"] = json!("Something else entirely");
    let response = send(&app, keyed_submit("order-1041", different)).await;

    // Answering with the first notification would drop this message
    // without anything ever reporting a failure.
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body: Value = json_body(response).await;
    assert!(body["error"].as_str().unwrap().contains("order-1041"), "{body}");
}

#[tokio::test]
async fn a_key_is_scoped_to_the_tenant_that_used_it() {
    let (app, _dir) = test_app().await;
    let mine = create_recipient(&app).await;

    let theirs_tenant = Uuid::new_v4().to_string();
    let theirs: Value = json_body(
        send(
            &app,
            post_json(
                "/recipients",
                json!({
                    "tenant_id": theirs_tenant,
                    "addresses": [{"type": "email", "value": "grace@example.com"}]
                }),
            ),
        )
        .await,
    )
    .await;

    send(&app, keyed_submit("order-1041", submission(&mine))).await;

    let mut other = submission(theirs["id"].as_str().unwrap());
    other["tenant_id"] = json!(theirs_tenant);
    let response = send(&app, keyed_submit("order-1041", other)).await;

    // Two callers who have never heard of each other both picking
    // `order-1041` is a coincidence, not a duplicate.
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn a_malformed_key_is_refused_rather_than_ignored() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;

    // Ignoring it would send the message and answer 201, and the caller --
    // believing it had asked for deduplication -- would send it again.
    let response = send(&app, keyed_submit("has spaces", submission(&recipient_id))).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let listed: Value = json_body(send(&app, get(&format!("/notifications?tenant_id={TENANT}"))).await).await;
    assert!(listed.as_array().unwrap().is_empty(), "nothing was queued");
}

#[tokio::test]
async fn an_over_long_key_is_refused() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;

    let response = send(&app, keyed_submit(&"k".repeat(256), submission(&recipient_id))).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_retry_survives_the_address_being_suppressed_in_between() {
    let (app, _dir, storage) = test_app_without_auth().await;
    let recipient_id = create_recipient(&app).await;
    let first: Value =
        json_body(send(&app, keyed_submit("order-1041", submission(&recipient_id))).await).await;

    let suppression = Suppression::new(
        TENANT.parse().unwrap(),
        RecipientAddress::Email("ada@example.com".into()),
        SuppressionReason::Complaint,
        chrono::Utc::now(),
    )
    .unwrap();
    storage.insert_suppression(&suppression).await.unwrap();

    let retry = send(&app, keyed_submit("order-1041", submission(&recipient_id))).await;

    // The message was accepted before the address was suppressed. Failing
    // the retry would turn the one call that exists to make retrying safe
    // into the one call that punishes it -- and would tell the caller its
    // message never went out, when in fact it is already queued.
    assert_eq!(retry.status(), StatusCode::OK);
    let retried: Value = json_body(retry).await;
    assert_eq!(retried["id"], first["id"]);
}

#[tokio::test]
async fn a_first_submission_to_a_suppressed_address_is_still_refused() {
    let (app, _dir, storage) = test_app_without_auth().await;
    let recipient_id = create_recipient(&app).await;
    let suppression = Suppression::new(
        TENANT.parse().unwrap(),
        RecipientAddress::Email("ada@example.com".into()),
        SuppressionReason::Unsubscribed,
        chrono::Utc::now(),
    )
    .unwrap();
    storage.insert_suppression(&suppression).await.unwrap();

    let response = send(&app, keyed_submit("order-1041", submission(&recipient_id))).await;

    // Carrying a key doesn't buy a caller past the checks; it only skips
    // them for a submission that already passed.
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn a_deduplicated_submission_is_counted_apart_from_a_queued_one() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;
    send(&app, keyed_submit("order-1041", submission(&recipient_id))).await;
    send(&app, keyed_submit("order-1041", submission(&recipient_id))).await;

    let metrics = send(&app, get("/metrics")).await;
    let body = axum::body::to_bytes(metrics.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();

    assert!(text.contains("ns_notifications_submitted_total{channel=\"email\"} 1"), "{text}");
    assert!(text.contains("ns_notifications_deduplicated_total{channel=\"email\"} 1"), "{text}");
}

#[tokio::test]
async fn a_notification_can_be_looked_up_by_the_key_it_was_submitted_under() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;
    let created: Value =
        json_body(send(&app, keyed_submit("order-1041", submission(&recipient_id))).await).await;

    // What a client does when it never saw the response and doesn't want
    // to re-send a thousand buffered requests to find out which landed.
    let response = send(&app, get(&format!("/notifications/by-key/order-1041?tenant_id={TENANT}"))).await;

    assert_eq!(response.status(), StatusCode::OK);
    let found: Value = json_body(response).await;
    assert_eq!(found["id"], created["id"]);
    assert_eq!(found["idempotency_key"], "order-1041");
}

#[tokio::test]
async fn an_unused_key_is_not_found() {
    let (app, _dir) = test_app().await;

    let response = send(&app, get(&format!("/notifications/by-key/never-used?tenant_id={TENANT}"))).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn another_tenants_key_is_not_found_either() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;
    send(&app, keyed_submit("order-1041", submission(&recipient_id))).await;

    let theirs = Uuid::new_v4();
    let response = send(&app, get(&format!("/notifications/by-key/order-1041?tenant_id={theirs}"))).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn looking_up_by_key_does_not_shadow_looking_up_by_id() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;
    let created: Value =
        json_body(send(&app, post_json("/notifications", submission(&recipient_id))).await).await;

    // `by-key` is a literal path segment sharing a prefix with
    // /notifications/{id}; the id route has to keep working.
    let response = send(&app, get(&format!("/notifications/{}", created["id"].as_str().unwrap()))).await;

    assert_eq!(response.status(), StatusCode::OK);
}

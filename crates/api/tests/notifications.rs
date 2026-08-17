mod support;

use axum::http::StatusCode;
use serde_json::{json, Value};
use support::{get, json_body, post_json, send, test_app};

async fn create_recipient(app: &axum::Router) -> String {
    let response = send(
        app,
        post_json("/recipients", json!({"addresses": [{"type": "email", "value": "ada@example.com"}]})),
    )
    .await;
    let body: Value = json_body(response).await;
    body["id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn submits_and_fetches_a_notification() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;

    let submit = post_json(
        "/notifications",
        json!({
            "tenant_id": "11111111-1111-1111-1111-111111111111",
            "recipient_id": recipient_id,
            "channel": "email",
            "subject": "Welcome",
            "body": "Hello there"
        }),
    );
    let response = send(&app, submit).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let created: Value = json_body(response).await;
    assert_eq!(created["status"], "queued");
    assert_eq!(created["channel"], "email");
    let id = created["id"].as_str().unwrap();

    let fetch = send(&app, get(&format!("/notifications/{id}"))).await;
    assert_eq!(fetch.status(), StatusCode::OK);
    let fetched: Value = json_body(fetch).await;
    assert_eq!(fetched["id"], id);
    assert_eq!(fetched["status"], "queued");
}

#[tokio::test]
async fn submit_without_body_or_template_is_rejected() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;

    let submit = post_json(
        "/notifications",
        json!({
            "tenant_id": "11111111-1111-1111-1111-111111111111",
            "recipient_id": recipient_id,
            "channel": "email"
        }),
    );
    let response = send(&app, submit).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn submit_against_unknown_recipient_is_404() {
    let (app, _dir) = test_app().await;

    let submit = post_json(
        "/notifications",
        json!({
            "tenant_id": "11111111-1111-1111-1111-111111111111",
            "recipient_id": "00000000-0000-0000-0000-000000000000",
            "channel": "email",
            "body": "hi"
        }),
    );
    let response = send(&app, submit).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_unknown_notification_is_404() {
    let (app, _dir) = test_app().await;

    let response = send(&app, get("/notifications/00000000-0000-0000-0000-000000000000")).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn cancels_a_queued_notification() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;

    let submit = post_json(
        "/notifications",
        json!({
            "tenant_id": "11111111-1111-1111-1111-111111111111",
            "recipient_id": recipient_id,
            "channel": "email",
            "body": "hi"
        }),
    );
    let created: Value = json_body(send(&app, submit).await).await;
    let id = created["id"].as_str().unwrap();

    let cancel = send(&app, post_json(&format!("/notifications/{id}/cancel"), json!({}))).await;
    assert_eq!(cancel.status(), StatusCode::OK);
    let cancelled: Value = json_body(cancel).await;
    assert_eq!(cancelled["status"], "cancelled");

    let fetched: Value = json_body(send(&app, get(&format!("/notifications/{id}"))).await).await;
    assert_eq!(fetched["status"], "cancelled");
}

#[tokio::test]
async fn cancelling_an_already_cancelled_notification_is_a_conflict() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;

    let submit = post_json(
        "/notifications",
        json!({
            "tenant_id": "11111111-1111-1111-1111-111111111111",
            "recipient_id": recipient_id,
            "channel": "email",
            "body": "hi"
        }),
    );
    let created: Value = json_body(send(&app, submit).await).await;
    let id = created["id"].as_str().unwrap();

    send(&app, post_json(&format!("/notifications/{id}/cancel"), json!({}))).await;
    let second = send(&app, post_json(&format!("/notifications/{id}/cancel"), json!({}))).await;

    assert_eq!(second.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn cancelling_an_unknown_notification_is_404() {
    let (app, _dir) = test_app().await;

    let response =
        send(&app, post_json("/notifications/00000000-0000-0000-0000-000000000000/cancel", json!({}))).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn lists_notifications_for_a_tenant() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;
    let tenant_id = "22222222-2222-2222-2222-222222222222";
    let other_tenant_id = "33333333-3333-3333-3333-333333333333";

    for tenant in [tenant_id, tenant_id, other_tenant_id] {
        let submit = post_json(
            "/notifications",
            json!({
                "tenant_id": tenant,
                "recipient_id": recipient_id,
                "channel": "email",
                "body": "hi"
            }),
        );
        send(&app, submit).await;
    }

    let response = send(&app, get(&format!("/notifications?tenant_id={tenant_id}"))).await;

    assert_eq!(response.status(), StatusCode::OK);
    let listed: Value = json_body(response).await;
    assert_eq!(listed.as_array().unwrap().len(), 2);
    for notification in listed.as_array().unwrap() {
        assert_eq!(notification["tenant_id"], tenant_id);
    }
}

#[tokio::test]
async fn lists_notifications_filtered_by_status() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;
    let tenant_id = "44444444-4444-4444-4444-444444444444";

    let submit = post_json(
        "/notifications",
        json!({
            "tenant_id": tenant_id,
            "recipient_id": recipient_id,
            "channel": "email",
            "body": "hi"
        }),
    );
    let created: Value = json_body(send(&app, submit).await).await;
    let id = created["id"].as_str().unwrap();
    send(&app, post_json(&format!("/notifications/{id}/cancel"), json!({}))).await;

    let queued = json_body::<Value>(
        send(&app, get(&format!("/notifications?tenant_id={tenant_id}&status=queued"))).await,
    )
    .await;
    assert_eq!(queued.as_array().unwrap().len(), 0);

    let cancelled = json_body::<Value>(
        send(&app, get(&format!("/notifications?tenant_id={tenant_id}&status=cancelled"))).await,
    )
    .await;
    assert_eq!(cancelled.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn list_notifications_requires_tenant_id() {
    let (app, _dir) = test_app().await;

    let response = send(&app, get("/notifications")).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn submitted_notification_defaults_to_normal_priority_and_five_attempts() {
    let (app, _dir) = test_app().await;
    let recipient_id = create_recipient(&app).await;

    let submit = post_json(
        "/notifications",
        json!({
            "tenant_id": "11111111-1111-1111-1111-111111111111",
            "recipient_id": recipient_id,
            "channel": "email",
            "body": "hi"
        }),
    );
    let created: Value = json_body(send(&app, submit).await).await;

    assert_eq!(created["priority"], "normal");
    assert_eq!(created["max_attempts"], 5);
    assert_eq!(created["attempt_count"], 0);
}

mod support;

use axum::http::StatusCode;
use chrono::Utc;
use ns_core::{ChannelKind, Preference, Topic};
use ns_storage::{PreferenceRepository, SqliteStorage};
use serde_json::{json, Value};
use support::{get, json_body, post_json, send, test_app_without_auth};
use uuid::Uuid;

const TENANT: &str = "22222222-2222-2222-2222-222222222222";

fn tenant() -> Uuid {
    TENANT.parse().unwrap()
}

fn topic(raw: &str) -> Topic {
    Topic::new(raw).unwrap()
}

async fn create_recipient(app: &axum::Router) -> Uuid {
    let response = send(
        app,
        post_json(
            "/recipients",
            json!({"tenant_id": TENANT, "addresses": [{"type": "email", "value": "ada@example.com"}]}),
        ),
    )
    .await;
    let body: Value = json_body(response).await;
    body["id"].as_str().unwrap().parse().unwrap()
}

fn submit(recipient: Uuid, topic: Option<&str>) -> axum::http::Request<axum::body::Body> {
    let mut body = json!({
        "tenant_id": TENANT,
        "recipient_id": recipient,
        "channel": "email",
        "body": "hello",
    });
    if let Some(topic) = topic {
        body["topic"] = json!(topic);
    }
    post_json("/notifications", body)
}

async fn block(storage: &SqliteStorage, recipient: Uuid, name: &str) {
    storage.set_preference(&Preference::block(tenant(), recipient, topic(name), Utc::now())).await.unwrap();
}

#[tokio::test]
async fn a_submitted_topic_is_stored_and_echoed_back() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;

    let response = send(&app, submit(recipient, Some("order-updates"))).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let created: Value = json_body(response).await;
    assert_eq!(created["topic"], "order-updates");

    let fetched: Value =
        json_body(send(&app, get(&format!("/notifications/{}", created["id"].as_str().unwrap()))).await)
            .await;
    assert_eq!(fetched["topic"], "order-updates");
}

#[tokio::test]
async fn a_submission_without_a_topic_reports_none_rather_than_null() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;

    let created: Value = json_body(send(&app, submit(recipient, None)).await).await;

    // Absent, not null: "no topic" and "a topic named null" are different
    // things and a client shouldn't have to tell them apart.
    assert!(created.get("topic").is_none(), "{created}");
}

#[tokio::test]
async fn an_unusable_topic_is_refused_while_the_caller_is_still_on_the_line() {
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;

    for bad in ["Marketing", "order updates", ""] {
        let response = send(&app, submit(recipient, Some(bad))).await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY, "accepted {bad:?}");
    }
}

#[tokio::test]
async fn submitting_a_topic_the_recipient_opted_out_of_is_refused() {
    let (app, _dir, storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;
    block(&storage, recipient, "marketing").await;

    let response = send(&app, submit(recipient, Some("marketing"))).await;

    // 422, not 403: the request was well-formed and the caller was
    // entitled to make it, the service just won't act on it.
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body: Value = json_body(response).await;
    let error = body["error"].as_str().unwrap();
    assert!(error.contains("opted out") && error.contains("marketing"), "{error}");
}

#[tokio::test]
async fn a_different_topic_is_unaffected_by_the_opt_out() {
    let (app, _dir, storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;
    block(&storage, recipient, "marketing").await;

    let response = send(&app, submit(recipient, Some("receipts"))).await;
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn an_untopiced_message_is_accepted_however_much_was_opted_out_of() {
    let (app, _dir, storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;
    block(&storage, recipient, "marketing").await;

    // The password-reset property: nobody was offered the chance to opt
    // out of a message with no topic, so nothing they said applies to it.
    assert_eq!(send(&app, submit(recipient, None)).await.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn a_channel_the_recipient_kept_still_accepts_the_topic() {
    let (app, _dir, storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;
    block(&storage, recipient, "orders").await;
    storage
        .set_preference(&Preference::allow_on(
            tenant(),
            recipient,
            topic("orders"),
            ChannelKind::Email,
            Utc::now(),
        ))
        .await
        .unwrap();

    assert_eq!(send(&app, submit(recipient, Some("orders"))).await.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn a_retry_under_an_idempotency_key_is_not_re_checked_against_preferences() {
    // A key exists to make retrying safe. Re-running the up-front checks
    // would let a retry fail over an opt-out recorded since the first
    // submission -- turning the one call that makes retrying safe into
    // the one that punishes it.
    let (app, _dir, storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;

    let keyed = || {
        let (mut parts, body) = submit(recipient, Some("marketing")).into_parts();
        parts.headers.insert("idempotency-key", "order-1041".parse().unwrap());
        axum::http::Request::from_parts(parts, body)
    };

    assert_eq!(send(&app, keyed()).await.status(), StatusCode::CREATED);
    block(&storage, recipient, "marketing").await;

    let replay = send(&app, keyed()).await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers().get("idempotent-replay").unwrap(), "true");
}

#[tokio::test]
async fn changing_only_the_topic_is_an_idempotency_conflict() {
    // Two topics are two different messages: the same words sent under a
    // topic the recipient can decline is not the same request.
    let (app, _dir, _storage) = test_app_without_auth().await;
    let recipient = create_recipient(&app).await;

    let keyed = |topic: &str| {
        let (mut parts, body) = submit(recipient, Some(topic)).into_parts();
        parts.headers.insert("idempotency-key", "order-1041".parse().unwrap());
        axum::http::Request::from_parts(parts, body)
    };

    assert_eq!(send(&app, keyed("orders")).await.status(), StatusCode::CREATED);
    assert_eq!(send(&app, keyed("marketing")).await.status(), StatusCode::CONFLICT);
}

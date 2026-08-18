mod support;

use axum::http::StatusCode;
use serde_json::{json, Value};
use support::{delete, get, json_body, post_json, put_json, send, test_app};
use uuid::Uuid;

/// Creates a straightforward template and returns the tenant it belongs to.
async fn seed_template(app: &axum::Router, id: &str) -> Uuid {
    let tenant_id = Uuid::new_v4();
    let response = send(
        app,
        post_json(
            "/templates",
            json!({
                "tenant_id": tenant_id,
                "id": id,
                "subject": "Welcome, {{name}}",
                "body": "Hi {{name}}, welcome to {{product}}."
            }),
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    tenant_id
}

#[tokio::test]
async fn creates_and_fetches_a_template() {
    let (app, _dir) = test_app().await;
    let tenant_id = Uuid::new_v4();

    let response = send(
        &app,
        post_json(
            "/templates",
            json!({
                "tenant_id": tenant_id,
                "id": "welcome_email",
                "name": "Welcome email",
                "channel": "email",
                "subject": "Welcome, {{name}}",
                "body": "Hi {{name}}, welcome to {{product}}."
            }),
        ),
    )
    .await;

    assert_eq!(response.status(), StatusCode::CREATED);
    let created: Value = json_body(response).await;
    assert_eq!(created["id"], "welcome_email");
    assert_eq!(created["channel"], "email");
    assert_eq!(created["variables"], json!(["name", "product"]));

    let fetched: Value =
        json_body(send(&app, get(&format!("/templates/welcome_email?tenant_id={tenant_id}"))).await).await;
    assert_eq!(fetched["body"], "Hi {{name}}, welcome to {{product}}.");
}

#[tokio::test]
async fn creating_a_duplicate_template_id_conflicts() {
    let (app, _dir) = test_app().await;
    let tenant_id = seed_template(&app, "welcome").await;

    let response = send(
        &app,
        post_json("/templates", json!({"tenant_id": tenant_id, "id": "welcome", "body": "different"})),
    )
    .await;

    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn another_tenant_may_reuse_a_template_id() {
    let (app, _dir) = test_app().await;
    seed_template(&app, "welcome").await;

    let response = send(
        &app,
        post_json(
            "/templates",
            json!({"tenant_id": Uuid::new_v4(), "id": "welcome", "body": "a different tenant's copy"}),
        ),
    )
    .await;

    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn create_rejects_an_unparseable_body() {
    let (app, _dir) = test_app().await;

    let response = send(
        &app,
        post_json("/templates", json!({"tenant_id": Uuid::new_v4(), "id": "broken", "body": "Hi {{name"})),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value = json_body(response).await;
    assert!(body["error"].as_str().unwrap().contains("unclosed"), "got {body}");
}

#[tokio::test]
async fn create_rejects_an_invalid_id() {
    let (app, _dir) = test_app().await;

    let response = send(
        &app,
        post_json("/templates", json!({"tenant_id": Uuid::new_v4(), "id": "not a slug", "body": "hi"})),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn fetching_an_unknown_template_is_a_404() {
    let (app, _dir) = test_app().await;
    let tenant_id = Uuid::new_v4();

    let response = send(&app, get(&format!("/templates/nope?tenant_id={tenant_id}"))).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fetching_another_tenants_template_is_a_404() {
    let (app, _dir) = test_app().await;
    seed_template(&app, "welcome").await;

    let response = send(&app, get(&format!("/templates/welcome?tenant_id={}", Uuid::new_v4()))).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn lists_only_the_requested_tenants_templates() {
    let (app, _dir) = test_app().await;
    let tenant_id = seed_template(&app, "welcome").await;
    send(&app, post_json("/templates", json!({"tenant_id": tenant_id, "id": "alert", "body": "Alert!"})))
        .await;
    seed_template(&app, "someone_elses").await;

    let listed: Value = json_body(send(&app, get(&format!("/templates?tenant_id={tenant_id}"))).await).await;

    let ids: Vec<&str> = listed.as_array().unwrap().iter().map(|t| t["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["alert", "welcome"]);
}

#[tokio::test]
async fn list_respects_limit() {
    let (app, _dir) = test_app().await;
    let tenant_id = seed_template(&app, "welcome").await;
    send(&app, post_json("/templates", json!({"tenant_id": tenant_id, "id": "alert", "body": "Alert!"})))
        .await;

    let listed: Value =
        json_body(send(&app, get(&format!("/templates?tenant_id={tenant_id}&limit=1"))).await).await;

    assert_eq!(listed.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn updates_a_templates_content() {
    let (app, _dir) = test_app().await;
    let tenant_id = seed_template(&app, "welcome").await;

    let response = send(
        &app,
        put_json(
            &format!("/templates/welcome?tenant_id={tenant_id}"),
            json!({"body": "Hey {{name}}, things changed."}),
        ),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let updated: Value = json_body(response).await;
    assert_eq!(updated["body"], "Hey {{name}}, things changed.");
    assert_eq!(updated["subject"], Value::Null, "an omitted subject clears the old one");
    assert_eq!(updated["variables"], json!(["name"]));
}

#[tokio::test]
async fn update_rejects_an_unparseable_body_and_leaves_the_template_alone() {
    let (app, _dir) = test_app().await;
    let tenant_id = seed_template(&app, "welcome").await;

    let response = send(
        &app,
        put_json(&format!("/templates/welcome?tenant_id={tenant_id}"), json!({"body": "Hi {{name"})),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let fetched: Value =
        json_body(send(&app, get(&format!("/templates/welcome?tenant_id={tenant_id}"))).await).await;
    assert_eq!(fetched["body"], "Hi {{name}}, welcome to {{product}}.");
}

#[tokio::test]
async fn updating_an_unknown_template_is_a_404() {
    let (app, _dir) = test_app().await;

    let response =
        send(&app, put_json(&format!("/templates/nope?tenant_id={}", Uuid::new_v4()), json!({"body": "hi"})))
            .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn deletes_a_template() {
    let (app, _dir) = test_app().await;
    let tenant_id = seed_template(&app, "welcome").await;

    let response = send(&app, delete(&format!("/templates/welcome?tenant_id={tenant_id}"))).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let fetch = send(&app, get(&format!("/templates/welcome?tenant_id={tenant_id}"))).await;
    assert_eq!(fetch.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn deleting_an_unknown_template_is_a_404() {
    let (app, _dir) = test_app().await;

    let response = send(&app, delete(&format!("/templates/nope?tenant_id={}", Uuid::new_v4()))).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn previews_a_template_without_sending_anything() {
    let (app, _dir) = test_app().await;
    let tenant_id = seed_template(&app, "welcome").await;

    let response = send(
        &app,
        post_json(
            &format!("/templates/welcome/preview?tenant_id={tenant_id}"),
            json!({"vars": {"name": "Ada", "product": "Notify"}}),
        ),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let rendered: Value = json_body(response).await;
    assert_eq!(rendered["subject"], "Welcome, Ada");
    assert_eq!(rendered["body"], "Hi Ada, welcome to Notify.");
}

#[tokio::test]
async fn preview_reports_every_missing_variable_at_once() {
    let (app, _dir) = test_app().await;
    let tenant_id = seed_template(&app, "welcome").await;

    let response =
        send(&app, post_json(&format!("/templates/welcome/preview?tenant_id={tenant_id}"), json!({}))).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value = json_body(response).await;
    let message = body["error"].as_str().unwrap();
    assert!(message.contains("name") && message.contains("product"), "got {message}");
}

#[tokio::test]
async fn list_limit_is_capped() {
    let (app, _dir) = test_app().await;
    let tenant_id = seed_template(&app, "welcome").await;

    let response = send(&app, get(&format!("/templates?tenant_id={tenant_id}&limit=100000"))).await;

    assert_eq!(response.status(), StatusCode::OK);
    let listed: Value = json_body(response).await;
    assert_eq!(listed.as_array().unwrap().len(), 1);
}

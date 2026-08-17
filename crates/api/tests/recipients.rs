mod support;

use axum::http::StatusCode;
use serde_json::{json, Value};
use support::{get, json_body, post_json, send, test_app};

#[tokio::test]
async fn creates_and_fetches_a_recipient() {
    let (app, _dir) = test_app().await;

    let create = post_json(
        "/recipients",
        json!({
            "display_name": "Ada Lovelace",
            "addresses": [{"type": "email", "value": "ada@example.com"}]
        }),
    );
    let response = send(&app, create).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let created: Value = json_body(response).await;
    let id = created["id"].as_str().unwrap();
    assert_eq!(created["display_name"], "Ada Lovelace");
    assert_eq!(created["supported_channels"], json!(["email"]));

    let fetch = send(&app, get(&format!("/recipients/{id}"))).await;
    assert_eq!(fetch.status(), StatusCode::OK);
    let fetched: Value = json_body(fetch).await;
    assert_eq!(fetched["id"], id);
}

#[tokio::test]
async fn create_recipient_without_addresses_defaults_to_empty() {
    let (app, _dir) = test_app().await;

    let response = send(&app, post_json("/recipients", json!({}))).await;

    assert_eq!(response.status(), StatusCode::CREATED);
    let created: Value = json_body(response).await;
    assert_eq!(created["display_name"], Value::Null);
    assert_eq!(created["supported_channels"], json!([]));
}

#[tokio::test]
async fn create_recipient_rejects_invalid_address() {
    let (app, _dir) = test_app().await;

    let response = send(
        &app,
        post_json("/recipients", json!({"addresses": [{"type": "email", "value": "not-an-email"}]})),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_unknown_recipient_is_404() {
    let (app, _dir) = test_app().await;

    let response = send(&app, get("/recipients/00000000-0000-0000-0000-000000000000")).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body: Value = json_body(response).await;
    assert!(body["error"].as_str().unwrap().contains("not found"));
}

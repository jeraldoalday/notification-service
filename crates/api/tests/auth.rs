mod support;

use axum::http::StatusCode;
use ns_auth::Scope;
use support::{get, issue_key, json_body, send, test_app_with_auth, with_auth};
use uuid::Uuid;

fn error_message(body: &serde_json::Value) -> &str {
    body["error"].as_str().unwrap()
}

#[tokio::test]
async fn a_data_endpoint_refuses_a_request_with_no_credentials() {
    let (router, _dir, _storage) = test_app_with_auth().await;

    let response = send(&router, get(&format!("/notifications?tenant_id={}", Uuid::new_v4()))).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers().get("www-authenticate").unwrap(), "Bearer");
    let body: serde_json::Value = json_body(response).await;
    assert_eq!(error_message(&body), "missing credentials");
}

#[tokio::test]
async fn health_and_metrics_stay_reachable_without_a_key() {
    let (router, _dir, _storage) = test_app_with_auth().await;

    assert_eq!(send(&router, get("/health")).await.status(), StatusCode::OK);
    assert_eq!(send(&router, get("/metrics")).await.status(), StatusCode::OK);
}

#[tokio::test]
async fn an_unknown_path_is_a_404_not_a_401() {
    let (router, _dir, _storage) = test_app_with_auth().await;

    let response = send(&router, get("/does-not-exist")).await;

    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "answering 401 for every unknown path would turn the auth layer into a route directory"
    );
}

#[tokio::test]
async fn a_valid_key_is_accepted() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let tenant = Uuid::new_v4();
    let bearer = issue_key(&storage, tenant, &[Scope::Read]).await;

    let response =
        send(&router, with_auth(get(&format!("/notifications?tenant_id={tenant}")), &bearer)).await;

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn malformed_authorization_headers_are_refused_identically() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let tenant = Uuid::new_v4();
    issue_key(&storage, tenant, &[Scope::Read]).await;

    for header in ["Basic nsk_whatever", "nsk_whatever", "Bearer", "Bearer not-a-ns-token"] {
        let response =
            send(&router, with_auth(get(&format!("/notifications?tenant_id={tenant}")), header)).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "accepted {header:?}");
        let body: serde_json::Value = json_body(response).await;
        assert_eq!(error_message(&body), "invalid credentials");
    }
}

#[tokio::test]
async fn an_unknown_token_is_indistinguishable_from_a_revoked_one() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let tenant = Uuid::new_v4();

    let bearer = issue_key(&storage, tenant, &[Scope::Read]).await;
    let keys = ns_storage::ApiKeyRepository::list_api_keys(&storage, tenant, 10).await.unwrap();
    let mut key = keys.into_iter().next().unwrap();
    key.revoke(chrono::Utc::now());
    ns_storage::ApiKeyRepository::update_api_key(&storage, &key).await.unwrap();

    let revoked = send(&router, with_auth(get(&format!("/notifications?tenant_id={tenant}")), &bearer)).await;
    let unknown = send(
        &router,
        with_auth(
            get(&format!("/notifications?tenant_id={tenant}")),
            "Bearer nsk_0000000000000000000000000000000000000000000",
        ),
    )
    .await;

    assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(unknown.status(), StatusCode::UNAUTHORIZED);

    let revoked_body: serde_json::Value = json_body(revoked).await;
    let unknown_body: serde_json::Value = json_body(unknown).await;
    assert_eq!(
        error_message(&revoked_body),
        error_message(&unknown_body),
        "telling a prober that a token is revoked confirms it was once real"
    );
}

// Each integration test binary compiles this module separately, so a
// helper used by only some of them looks dead to the others. CI builds
// with -D warnings, so opt out here rather than letting the shared
// helpers fragment per test file.
#![allow(dead_code)]

use axum::body::Body;
use axum::http::{Request, Response};
use chrono::Utc;
use ns_api::{build_router, AppState};
use ns_auth::{ApiKey, Scope};
use ns_storage::{ApiKeyRepository, SqliteStorage};
use serde::de::DeserializeOwned;
use tower::ServiceExt;
use uuid::Uuid;

/// Boots a fresh on-disk SQLite-backed router for one test, mirroring the
/// per-test temp-file convention used by `ns-storage` and `ns-queue`'s own
/// test suites (a pooled `sqlite::memory:` gives every connection its own
/// database once the pool grows past one connection).
pub async fn test_app() -> (axum::Router, tempfile::TempDir) {
    let (router, dir, _storage) = test_app_without_auth().await;
    (router, dir)
}

/// A router with authentication switched off, for the tests that are
/// about a handler's own behaviour rather than about who is allowed to
/// reach it. Keeping those tests credential-free means a failure in one
/// of them points at the handler, not at the fixture's key handling.
pub async fn test_app_without_auth() -> (axum::Router, tempfile::TempDir, SqliteStorage) {
    let (storage, dir) = test_storage().await;
    let router = build_router(AppState::new(storage.clone()).without_auth());
    (router, dir, storage)
}

/// A router with authentication required, handing back the storage so a
/// test can mint the keys it wants to present.
pub async fn test_app_with_auth() -> (axum::Router, tempfile::TempDir, SqliteStorage) {
    let (storage, dir) = test_storage().await;
    let router = build_router(AppState::new(storage.clone()));
    (router, dir, storage)
}

async fn test_storage() -> (SqliteStorage, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::connect(&format!("sqlite://{}", db_path.display())).await.unwrap();
    (storage, dir)
}

/// Mints a key for `tenant_id` with `scopes`, stores it, and returns the
/// bearer header value to present.
pub async fn issue_key(storage: &SqliteStorage, tenant_id: Uuid, scopes: &[Scope]) -> String {
    let (key, token) = ApiKey::generate(tenant_id, "test", scopes.iter().copied(), Utc::now());
    storage.insert_api_key(&key).await.unwrap();
    format!("Bearer {}", token.expose())
}

/// Adds an `Authorization` header to a request built by the helpers above.
pub fn with_auth(request: Request<Body>, bearer: &str) -> Request<Body> {
    let (mut parts, body) = request.into_parts();
    parts.headers.insert(axum::http::header::AUTHORIZATION, bearer.parse().unwrap());
    Request::from_parts(parts, body)
}

pub async fn send(router: &axum::Router, request: Request<Body>) -> Response<Body> {
    router.clone().oneshot(request).await.unwrap()
}

pub async fn json_body<T: DeserializeOwned>(response: Response<Body>) -> T {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

pub fn post_json(uri: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

pub fn get(uri: &str) -> Request<Body> {
    Request::builder().method("GET").uri(uri).body(Body::empty()).unwrap()
}

pub fn put_json(uri: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

pub fn delete(uri: &str) -> Request<Body> {
    Request::builder().method("DELETE").uri(uri).body(Body::empty()).unwrap()
}

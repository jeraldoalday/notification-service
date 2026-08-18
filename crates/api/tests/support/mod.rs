use axum::body::Body;
use axum::http::{Request, Response};
use ns_api::{build_router, AppState};
use ns_storage::SqliteStorage;
use serde::de::DeserializeOwned;
use tower::ServiceExt;

/// Boots a fresh on-disk SQLite-backed router for one test, mirroring the
/// per-test temp-file convention used by `ns-storage` and `ns-queue`'s own
/// test suites (a pooled `sqlite::memory:` gives every connection its own
/// database once the pool grows past one connection).
pub async fn test_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::connect(&format!("sqlite://{}", db_path.display())).await.unwrap();
    let router = build_router(AppState::new(storage));
    (router, dir)
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

// Each integration test binary compiles this module separately, so a
// helper only some of them use looks dead to the rest.
#![allow(dead_code)]

use ns_storage::SqliteStorage;

/// A throwaway on-disk database for one test, following the same
/// convention as ns-storage's own suite: a pooled `sqlite::memory:` gives
/// each connection its own database once the pool grows past one.
pub async fn test_storage() -> (SqliteStorage, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::connect(&format!("sqlite://{}", db_path.display())).await.unwrap();
    (storage, dir)
}

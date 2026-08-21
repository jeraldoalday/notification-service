mod support;

use axum::http::StatusCode;
use ns_auth::Scope;
use support::{delete, get, issue_key, json_body, post_json, send, test_app_with_auth, with_auth};
use uuid::Uuid;

#[tokio::test]
async fn an_admin_key_mints_a_key_that_works() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let tenant = Uuid::new_v4();
    let admin = issue_key(&storage, tenant, &[Scope::Admin]).await;

    let response = send(
        &router,
        with_auth(
            post_json("/api-keys", serde_json::json!({"name": "ci-deploy", "scopes": ["write"]})),
            &admin,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let created: serde_json::Value = json_body(response).await;
    assert_eq!(created["tenant_id"].as_str().unwrap().parse::<Uuid>().unwrap(), tenant);
    assert_eq!(created["scopes"], serde_json::json!(["write"]));

    let minted = format!("Bearer {}", created["token"].as_str().unwrap());
    let used = send(&router, with_auth(get("/notifications"), &minted)).await;
    assert_eq!(used.status(), StatusCode::OK);
}

#[tokio::test]
async fn a_write_key_cannot_mint_keys() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let bearer = issue_key(&storage, Uuid::new_v4(), &[Scope::Write]).await;

    let response = send(
        &router,
        with_auth(post_json("/api-keys", serde_json::json!({"name": "x", "scopes": ["read"]})), &bearer),
    )
    .await;

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "minting credentials is the one thing write deliberately doesn't reach"
    );
}

#[tokio::test]
async fn a_minted_key_is_confined_to_the_minters_tenant() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let tenant = Uuid::new_v4();
    let admin = issue_key(&storage, tenant, &[Scope::Admin]).await;

    // There is no tenant_id field to supply, so an admin can't mint into
    // someone else's tenant even by trying -- the extra key is ignored.
    let created: serde_json::Value = json_body(
        send(
            &router,
            with_auth(
                post_json(
                    "/api-keys",
                    serde_json::json!({"name": "x", "scopes": ["read"], "tenant_id": Uuid::new_v4()}),
                ),
                &admin,
            ),
        )
        .await,
    )
    .await;

    assert_eq!(created["tenant_id"].as_str().unwrap().parse::<Uuid>().unwrap(), tenant);
}

#[tokio::test]
async fn a_key_with_no_scopes_or_no_name_is_refused() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let admin = issue_key(&storage, Uuid::new_v4(), &[Scope::Admin]).await;

    for body in [
        serde_json::json!({"name": "x", "scopes": []}),
        serde_json::json!({"name": "   ", "scopes": ["read"]}),
    ] {
        let response = send(&router, with_auth(post_json("/api-keys", body.clone()), &admin)).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "accepted {body}");
    }
}

#[tokio::test]
async fn listing_shows_the_tenants_keys_and_never_a_token() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let admin = issue_key(&storage, Uuid::new_v4(), &[Scope::Admin]).await;

    let created: serde_json::Value = json_body(
        send(
            &router,
            with_auth(post_json("/api-keys", serde_json::json!({"name": "ci", "scopes": ["read"]})), &admin),
        )
        .await,
    )
    .await;
    let token = created["token"].as_str().unwrap().to_string();

    let response = send(&router, with_auth(get("/api-keys"), &admin)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let listed: Vec<serde_json::Value> = json_body(response).await;

    assert_eq!(listed.len(), 2, "the admin key itself plus the one just minted");
    for key in &listed {
        assert!(key.get("token").is_none(), "{key}");
        assert!(key.get("token_digest").is_none(), "{key}");
        assert!(!key.to_string().contains(&token));
        assert_eq!(key["fingerprint"].as_str().unwrap().len(), 8);
    }
}

#[tokio::test]
async fn revoking_a_key_stops_it_working_and_is_idempotent() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let admin = issue_key(&storage, Uuid::new_v4(), &[Scope::Admin]).await;

    let created: serde_json::Value = json_body(
        send(
            &router,
            with_auth(post_json("/api-keys", serde_json::json!({"name": "ci", "scopes": ["read"]})), &admin),
        )
        .await,
    )
    .await;
    let id = created["id"].as_str().unwrap().to_string();
    let minted = format!("Bearer {}", created["token"].as_str().unwrap());

    let revoked = send(&router, with_auth(delete(&format!("/api-keys/{id}")), &admin)).await;
    assert_eq!(revoked.status(), StatusCode::OK);
    let body: serde_json::Value = json_body(revoked).await;
    let revoked_at = body["revoked_at"].as_str().unwrap().to_string();

    let refused = send(&router, with_auth(get("/notifications"), &minted)).await;
    assert_eq!(refused.status(), StatusCode::UNAUTHORIZED);

    let again = send(&router, with_auth(delete(&format!("/api-keys/{id}")), &admin)).await;
    assert_eq!(again.status(), StatusCode::OK, "the caller's intent already holds, so this isn't a conflict");
    let again_body: serde_json::Value = json_body(again).await;
    assert_eq!(again_body["revoked_at"].as_str().unwrap(), revoked_at);
}

#[tokio::test]
async fn one_tenants_admin_cannot_revoke_anothers_key() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let ours = issue_key(&storage, Uuid::new_v4(), &[Scope::Admin]).await;
    let theirs = issue_key(&storage, Uuid::new_v4(), &[Scope::Admin]).await;

    let created: serde_json::Value = json_body(
        send(
            &router,
            with_auth(post_json("/api-keys", serde_json::json!({"name": "ci", "scopes": ["read"]})), &ours),
        )
        .await,
    )
    .await;
    let id = created["id"].as_str().unwrap();
    let minted = format!("Bearer {}", created["token"].as_str().unwrap());

    let response = send(&router, with_auth(delete(&format!("/api-keys/{id}")), &theirs)).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let still_works = send(&router, with_auth(get("/notifications"), &minted)).await;
    assert_eq!(still_works.status(), StatusCode::OK);
}

#[tokio::test]
async fn an_admin_key_can_revoke_itself() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let tenant = Uuid::new_v4();
    let admin = issue_key(&storage, tenant, &[Scope::Admin]).await;

    let listed: Vec<serde_json::Value> =
        json_body(send(&router, with_auth(get("/api-keys"), &admin)).await).await;
    let id = listed[0]["id"].as_str().unwrap();

    let response = send(&router, with_auth(delete(&format!("/api-keys/{id}")), &admin)).await;
    assert_eq!(response.status(), StatusCode::OK);

    let after = send(&router, with_auth(get("/api-keys"), &admin)).await;
    assert_eq!(after.status(), StatusCode::UNAUTHORIZED, "the key it just turned off is its own");
}

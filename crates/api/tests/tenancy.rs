mod support;

use axum::http::StatusCode;
use ns_auth::Scope;
use support::{delete, get, issue_key, json_body, post_json, put_json, send, test_app_with_auth, with_auth};
use uuid::Uuid;

fn error_message(body: &serde_json::Value) -> &str {
    body["error"].as_str().unwrap()
}

async fn create_recipient(router: &axum::Router, bearer: &str) -> Uuid {
    let response = send(
        router,
        with_auth(
            post_json(
                "/recipients",
                serde_json::json!({"addresses": [{"type": "email", "value": "ada@example.com"}]}),
            ),
            bearer,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: serde_json::Value = json_body(response).await;
    body["id"].as_str().unwrap().parse().unwrap()
}

async fn submit(router: &axum::Router, bearer: &str, body: serde_json::Value) -> serde_json::Value {
    let response = send(router, with_auth(post_json("/notifications", body), bearer)).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    json_body(response).await
}

#[tokio::test]
async fn a_submitted_notification_takes_its_tenant_from_the_key() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let tenant = Uuid::new_v4();
    let bearer = issue_key(&storage, tenant, &[Scope::Write]).await;
    let recipient_id = create_recipient(&router, &bearer).await;

    let body = submit(
        &router,
        &bearer,
        serde_json::json!({"recipient_id": recipient_id, "channel": "email", "body": "Hello"}),
    )
    .await;

    assert_eq!(body["tenant_id"].as_str().unwrap().parse::<Uuid>().unwrap(), tenant);
}

#[tokio::test]
async fn submitting_for_another_tenant_is_refused_rather_than_silently_rewritten() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let tenant = Uuid::new_v4();
    let bearer = issue_key(&storage, tenant, &[Scope::Write]).await;
    let recipient_id = create_recipient(&router, &bearer).await;

    let response = send(
        &router,
        with_auth(
            post_json(
                "/notifications",
                serde_json::json!({
                    "tenant_id": Uuid::new_v4(),
                    "recipient_id": recipient_id,
                    "channel": "email",
                    "body": "Hello"
                }),
            ),
            &bearer,
        ),
    )
    .await;

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "quietly overriding the tenant would let a misconfigured client write somewhere it didn't mean to"
    );
}

#[tokio::test]
async fn one_tenants_notification_is_invisible_to_another() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let ours = issue_key(&storage, Uuid::new_v4(), &[Scope::Write]).await;
    let theirs = issue_key(&storage, Uuid::new_v4(), &[Scope::Write]).await;

    let recipient_id = create_recipient(&router, &ours).await;
    let submitted = submit(
        &router,
        &ours,
        serde_json::json!({"recipient_id": recipient_id, "channel": "email", "body": "Hello"}),
    )
    .await;
    let id = submitted["id"].as_str().unwrap();

    let response = send(&router, with_auth(get(&format!("/notifications/{id}")), &theirs)).await;
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "a 403 would confirm the id names a real notification"
    );

    let ours_again = send(&router, with_auth(get(&format!("/notifications/{id}")), &ours)).await;
    assert_eq!(ours_again.status(), StatusCode::OK);
}

#[tokio::test]
async fn one_tenant_cannot_cancel_anothers_notification() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let ours = issue_key(&storage, Uuid::new_v4(), &[Scope::Write]).await;
    let theirs = issue_key(&storage, Uuid::new_v4(), &[Scope::Write]).await;

    let recipient_id = create_recipient(&router, &ours).await;
    let submitted = submit(
        &router,
        &ours,
        serde_json::json!({"recipient_id": recipient_id, "channel": "email", "body": "Hello"}),
    )
    .await;
    let id = submitted["id"].as_str().unwrap();

    let response = send(
        &router,
        with_auth(post_json(&format!("/notifications/{id}/cancel"), serde_json::json!({})), &theirs),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let still_queued: serde_json::Value =
        json_body(send(&router, with_auth(get(&format!("/notifications/{id}")), &ours)).await).await;
    assert_eq!(still_queued["status"], "queued");
}

#[tokio::test]
async fn listing_only_ever_returns_the_callers_own_tenant() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let our_tenant = Uuid::new_v4();
    let their_tenant = Uuid::new_v4();
    let ours = issue_key(&storage, our_tenant, &[Scope::Write]).await;
    let theirs = issue_key(&storage, their_tenant, &[Scope::Write]).await;

    let recipient_id = create_recipient(&router, &ours).await;
    submit(
        &router,
        &ours,
        serde_json::json!({"recipient_id": recipient_id, "channel": "email", "body": "a"}),
    )
    .await;
    submit(&router, &ours, serde_json::json!({"recipient_id": recipient_id, "channel": "sms", "body": "b"}))
        .await;
    submit(
        &router,
        &theirs,
        serde_json::json!({"recipient_id": recipient_id, "channel": "push", "body": "c"}),
    )
    .await;

    let listed: Vec<serde_json::Value> =
        json_body(send(&router, with_auth(get("/notifications"), &theirs)).await).await;
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0]["tenant_id"].as_str().unwrap().parse::<Uuid>().unwrap(), their_tenant);
}

#[tokio::test]
async fn asking_to_list_another_tenant_is_refused() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let bearer = issue_key(&storage, Uuid::new_v4(), &[Scope::Read]).await;

    let response =
        send(&router, with_auth(get(&format!("/notifications?tenant_id={}", Uuid::new_v4())), &bearer)).await;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn templates_are_isolated_between_tenants() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let ours = issue_key(&storage, Uuid::new_v4(), &[Scope::Write]).await;
    let theirs = issue_key(&storage, Uuid::new_v4(), &[Scope::Write]).await;

    let created = send(
        &router,
        with_auth(
            post_json(
                "/templates",
                serde_json::json!({"id": "welcome", "body": "Hi {{name}}", "subject": "Hello"}),
            ),
            &ours,
        ),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);

    // The same slug is free for another tenant, which is the whole point
    // of keying templates by (tenant_id, id).
    let theirs_created = send(
        &router,
        with_auth(
            post_json("/templates", serde_json::json!({"id": "welcome", "body": "Different"})),
            &theirs,
        ),
    )
    .await;
    assert_eq!(theirs_created.status(), StatusCode::CREATED);

    let fetched: serde_json::Value =
        json_body(send(&router, with_auth(get("/templates/welcome"), &theirs)).await).await;
    assert_eq!(fetched["body"], "Different");

    let listed: Vec<serde_json::Value> =
        json_body(send(&router, with_auth(get("/templates"), &theirs)).await).await;
    assert_eq!(listed.len(), 1);
}

#[tokio::test]
async fn deleting_a_template_only_reaches_the_callers_own() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let ours = issue_key(&storage, Uuid::new_v4(), &[Scope::Write]).await;
    let theirs = issue_key(&storage, Uuid::new_v4(), &[Scope::Write]).await;

    for bearer in [&ours, &theirs] {
        let created = send(
            &router,
            with_auth(post_json("/templates", serde_json::json!({"id": "shared", "body": "x"})), bearer),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
    }

    assert_eq!(
        send(&router, with_auth(delete("/templates/shared"), &theirs)).await.status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        send(&router, with_auth(get("/templates/shared"), &ours)).await.status(),
        StatusCode::OK,
        "our copy must survive their delete"
    );
}

#[tokio::test]
async fn updating_a_template_cannot_be_pointed_at_another_tenant() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let victim_tenant = Uuid::new_v4();
    let victim = issue_key(&storage, victim_tenant, &[Scope::Write]).await;
    let attacker = issue_key(&storage, Uuid::new_v4(), &[Scope::Write]).await;

    send(
        &router,
        with_auth(post_json("/templates", serde_json::json!({"id": "welcome", "body": "original"})), &victim),
    )
    .await;

    let response = send(
        &router,
        with_auth(
            put_json(
                &format!("/templates/welcome?tenant_id={victim_tenant}"),
                serde_json::json!({"body": "rewritten"}),
            ),
            &attacker,
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let unchanged: serde_json::Value =
        json_body(send(&router, with_auth(get("/templates/welcome"), &victim)).await).await;
    assert_eq!(unchanged["body"], "original");
}
#[tokio::test]
async fn a_read_only_key_cannot_write() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let tenant = Uuid::new_v4();
    let bearer = issue_key(&storage, tenant, &[Scope::Read]).await;

    let response = send(
        &router,
        with_auth(
            post_json(
                "/recipients",
                serde_json::json!({"addresses": [{"type": "email", "value": "ada@example.com"}]}),
            ),
            &bearer,
        ),
    )
    .await;

    assert_eq!(response.status(), StatusCode::FORBIDDEN, "a valid credential that isn't allowed is a 403");
    let body: serde_json::Value = json_body(response).await;
    assert!(error_message(&body).contains("write"), "{body}");
}

#[tokio::test]
async fn a_write_key_may_also_read() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let tenant = Uuid::new_v4();
    let bearer = issue_key(&storage, tenant, &[Scope::Write]).await;

    let response =
        send(&router, with_auth(get(&format!("/notifications?tenant_id={tenant}")), &bearer)).await;

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn a_scopeless_key_authenticates_but_can_do_nothing() {
    let (router, _dir, storage) = test_app_with_auth().await;
    let tenant = Uuid::new_v4();
    let bearer = issue_key(&storage, tenant, &[]).await;

    let response =
        send(&router, with_auth(get(&format!("/notifications?tenant_id={tenant}")), &bearer)).await;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

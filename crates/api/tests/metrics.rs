mod support;

use axum::http::StatusCode;
use support::{get, json_body, post_json, send, test_app};

async fn text_body(response: axum::http::Response<axum::body::Body>) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn scrape(router: &axum::Router) -> String {
    let response = send(router, get("/metrics")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("content-type").unwrap(), "text/plain; version=0.0.4; charset=utf-8");
    text_body(response).await
}

async fn create_recipient(router: &axum::Router) -> uuid::Uuid {
    let response = send(
        router,
        post_json(
            "/recipients",
            serde_json::json!({
                "display_name": "Ada",
                "addresses": [{"type": "email", "value": "ada@example.com"}]
            }),
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: serde_json::Value = json_body(response).await;
    body["id"].as_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn metrics_endpoint_exposes_the_full_metric_set_before_any_traffic() {
    let (router, _dir) = test_app().await;
    let text = scrape(&router).await;

    assert!(text.contains("# TYPE ns_notifications_submitted_total counter"), "{text}");
    assert!(text.contains("# TYPE ns_notification_send_duration_seconds histogram"), "{text}");
    assert!(text.contains("ns_notifications_submitted_total{channel=\"email\"} 0"), "{text}");
    assert!(text.contains("ns_worker_polls_total 0"), "{text}");
}

#[tokio::test]
async fn submitting_a_notification_increments_its_channel_counter() {
    let (router, _dir) = test_app().await;
    let recipient_id = create_recipient(&router).await;

    let response = send(
        &router,
        post_json(
            "/notifications",
            serde_json::json!({
                "tenant_id": uuid::Uuid::new_v4(),
                "recipient_id": recipient_id,
                "channel": "email",
                "subject": "Hi",
                "body": "Hello"
            }),
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);

    let text = scrape(&router).await;
    assert!(text.contains("ns_notifications_submitted_total{channel=\"email\"} 1"), "{text}");
    assert!(text.contains("ns_notifications_submitted_total{channel=\"sms\"} 0"), "{text}");
}

#[tokio::test]
async fn queue_depth_is_refreshed_at_scrape_time() {
    let (router, _dir) = test_app().await;
    let recipient_id = create_recipient(&router).await;

    assert!(scrape(&router).await.contains("ns_queue_notifications{status=\"queued\"} 0"));

    for _ in 0..3 {
        let response = send(
            &router,
            post_json(
                "/notifications",
                serde_json::json!({
                    "tenant_id": uuid::Uuid::new_v4(),
                    "recipient_id": recipient_id,
                    "channel": "email",
                    "body": "Hello"
                }),
            ),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let text = scrape(&router).await;
    assert!(text.contains("ns_queue_notifications{status=\"queued\"} 3"), "{text}");
}

#[tokio::test]
async fn requests_are_counted_under_their_route_pattern_not_their_path() {
    let (router, _dir) = test_app().await;

    let missing = uuid::Uuid::new_v4();
    let response = send(&router, get(&format!("/notifications/{missing}"))).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let text = scrape(&router).await;
    assert!(
        text.contains(
            "ns_http_requests_total{method=\"GET\",route=\"/notifications/{id}\",status=\"404\"} 1"
        ),
        "{text}"
    );
    assert!(!text.contains(&missing.to_string()), "a concrete id must never become a label value: {text}");
}

#[tokio::test]
async fn unmatched_requests_are_counted_under_a_single_placeholder_route() {
    let (router, _dir) = test_app().await;

    for path in ["/nope", "/also-nope", "/definitely/not/here"] {
        let response = send(&router, get(path)).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    let text = scrape(&router).await;
    assert!(
        text.contains("ns_http_requests_total{method=\"GET\",route=\"<unmatched>\",status=\"404\"} 3"),
        "three unknown paths must collapse into one series: {text}"
    );
}

#[tokio::test]
async fn request_latency_is_observed_per_route() {
    let (router, _dir) = test_app().await;
    send(&router, get("/health")).await;

    let text = scrape(&router).await;
    assert!(
        text.contains("ns_http_request_duration_seconds_count{method=\"GET\",route=\"/health\"} 1"),
        "{text}"
    );
}

#[tokio::test]
async fn exposition_is_stable_between_scrapes_of_an_idle_service() {
    let (router, _dir) = test_app().await;
    // The first scrape is itself counted, so compare the second and third
    // -- which differ only by the /metrics series the second one recorded.
    scrape(&router).await;
    let second = scrape(&router).await;
    let third = scrape(&router).await;

    let bump = |text: &str, n: u64| {
        text.replace(
            &format!("ns_http_requests_total{{method=\"GET\",route=\"/metrics\",status=\"200\"}} {n}"),
            "COUNT",
        )
    };
    assert_eq!(
        bump(&second, 1).lines().filter(|line| !line.contains("duration_seconds")).collect::<Vec<_>>(),
        bump(&third, 2).lines().filter(|line| !line.contains("duration_seconds")).collect::<Vec<_>>()
    );
}

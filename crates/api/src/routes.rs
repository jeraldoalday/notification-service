use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use ns_storage::{NotificationRepository, RecipientRepository, TemplateRepository};
use tower_http::trace::TraceLayer;

use crate::state::AppState;

mod health;
pub mod notifications;
pub mod recipients;
pub mod templates;

/// Builds the full route table over a generic repository, mirroring
/// `ns_worker::Worker<R>`'s approach: the API doesn't care whether `R` is
/// `SqliteStorage` or a test double, only that it implements the
/// repository traits it needs.
pub fn build_router<R>(state: AppState<R>) -> Router
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone + Send + Sync + 'static,
{
    let metrics = Arc::clone(&state.metrics);

    Router::new()
        .route("/health", get(health::health))
        .route("/metrics", get(crate::metrics::metrics_endpoint))
        .route("/recipients", post(recipients::create_recipient))
        .route("/recipients/{id}", get(recipients::get_recipient))
        .route(
            "/notifications",
            post(notifications::submit_notification).get(notifications::list_notifications),
        )
        .route("/notifications/{id}", get(notifications::get_notification))
        .route("/notifications/{id}/cancel", post(notifications::cancel_notification))
        .route("/templates", post(templates::create_template).get(templates::list_templates))
        .route(
            "/templates/{id}",
            get(templates::get_template).put(templates::update_template).delete(templates::delete_template),
        )
        .route("/templates/{id}/preview", post(templates::preview_template))
        .layer(TraceLayer::new_for_http())
        // Applied outside TraceLayer so the recorded duration covers the
        // whole response, and applied via `Router::layer` (not
        // `route_layer`) so unmatched requests are counted too -- a flood
        // of 404s is exactly the sort of thing a scrape should show.
        .layer(axum::middleware::from_fn_with_state(metrics, crate::metrics::track_requests))
        .with_state(state)
}

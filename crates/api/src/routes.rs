use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use ns_storage::{ApiKeyRepository, NotificationRepository, RecipientRepository, TemplateRepository};
use tower_http::trace::TraceLayer;

use crate::state::AppState;

pub mod api_keys;
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
    R: NotificationRepository
        + RecipientRepository
        + TemplateRepository
        + ApiKeyRepository
        + Clone
        + Send
        + Sync
        + 'static,
{
    let metrics = Arc::clone(&state.metrics);

    // /health and /metrics sit outside the authenticated routes on
    // purpose. A liveness probe that needs a credential is a liveness
    // probe that reports the service as down whenever the credential is
    // the thing that's wrong, and a scrape endpoint behind a bearer token
    // is a scrape endpoint most Prometheus deployments won't be pointed
    // at. Neither exposes tenant data: /metrics reports totals across the
    // whole deployment and carries no tenant label, precisely so that
    // exposing it doesn't leak one tenant's volume to another.
    let unauthenticated = Router::new()
        .route("/health", get(health::health))
        .route("/metrics", get(crate::metrics::metrics_endpoint));

    let authenticated = Router::new()
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
        .route("/api-keys", post(api_keys::create_api_key).get(api_keys::list_api_keys))
        .route("/api-keys/{id}", axum::routing::delete(api_keys::revoke_api_key))
        // route_layer, so a request for an unknown path is a plain 404
        // rather than a 401 that invites guessing at which paths exist.
        .route_layer(axum::middleware::from_fn_with_state(state.clone(), crate::auth::authenticate));

    unauthenticated
        .merge(authenticated)
        .layer(TraceLayer::new_for_http())
        // Applied outside TraceLayer so the recorded duration covers the
        // whole response, and applied via `Router::layer` (not
        // `route_layer`) so unmatched requests are counted too -- a flood
        // of 404s is exactly the sort of thing a scrape should show.
        .layer(axum::middleware::from_fn_with_state(metrics, crate::metrics::track_requests))
        .with_state(state)
}

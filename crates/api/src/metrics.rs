use std::sync::Arc;
use std::time::Instant;

use axum::extract::{MatchedPath, Request, State};
use axum::http::header::CONTENT_TYPE;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use ns_metrics::{prometheus, ServiceMetrics};
use ns_storage::{NotificationRepository, RecipientRepository, TemplateRepository};

use crate::error::ApiResult;
use crate::state::AppState;

/// Label used for a request that matched no route. Reporting the raw URI
/// instead would let anyone scanning the service for endpoints mint an
/// unbounded number of time series just by requesting random paths.
const UNMATCHED_ROUTE: &str = "<unmatched>";

/// Records one counter and one latency observation per request, plus an
/// in-flight gauge.
///
/// The route label comes from axum's `MatchedPath` -- the *pattern*
/// (`/notifications/{id}`), not the concrete path -- so the series count
/// stays proportional to the size of the route table rather than to the
/// number of notifications the service has ever handled.
pub async fn track_requests(
    State(metrics): State<Arc<ServiceMetrics>>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().to_string();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_string())
        .unwrap_or_else(|| UNMATCHED_ROUTE.to_string());

    metrics.http_in_flight().inc();
    let started = Instant::now();
    let response = next.run(request).await;
    let elapsed = started.elapsed().as_secs_f64();
    metrics.http_in_flight().dec();

    metrics.http_requests(&method, &route, response.status().as_u16()).inc();
    metrics.http_duration(&method, &route).observe(elapsed);

    response
}

/// Serves the Prometheus exposition text.
///
/// Queue depth is refreshed here rather than being maintained as the queue
/// moves, for two reasons: a gauge kept by increment/decrement drifts
/// permanently out of true the first time a process dies mid-transition,
/// and the worker isn't the only writer -- the API and the `ns` CLI both
/// move notifications between statuses. One aggregate query per scrape is
/// authoritative and, being a `GROUP BY` over an indexed column, cheap at
/// scrape frequency.
pub async fn metrics_endpoint<R>(State(state): State<AppState<R>>) -> ApiResult<Response>
where
    R: NotificationRepository + RecipientRepository + TemplateRepository + Clone,
{
    let counts = state.repo.count_by_status(None).await?;
    state.metrics.set_queue_depth(&counts);

    let body = state.metrics.render();
    Ok(([(CONTENT_TYPE, prometheus::CONTENT_TYPE)], body).into_response())
}

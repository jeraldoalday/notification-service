use std::sync::Arc;
use std::time::Instant;

use axum::extract::{MatchedPath, Request, State};
use axum::http::header::CONTENT_TYPE;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use ns_metrics::{prometheus, ServiceMetrics};

use crate::error::ApiResult;
use crate::state::{AppState, Repositories};

/// Label used for a request that matched no route. Reporting the raw URI
/// instead would let anyone scanning the service for endpoints mint an
/// unbounded number of time series just by requesting random paths.
const UNMATCHED_ROUTE: &str = "<unmatched>";

/// Label used for a request method this service doesn't serve.
///
/// HTTP allows extension methods -- any token is a syntactically valid
/// method -- and hyper parses them faithfully, so `request.method()` is
/// attacker-controlled text with no bounded set of values. Reporting it
/// verbatim is the same unbounded-cardinality hole `UNMATCHED_ROUTE`
/// exists to close, just through a different field: a client looping over
/// random method tokens mints a new time series per request, and the
/// registry holds every series it has ever created until the process
/// restarts.
const OTHER_METHOD: &str = "<other>";

/// The methods this service actually routes. Anything else is folded into
/// `OTHER_METHOD` rather than labelled with itself.
///
/// An allowlist rather than a length or character check, because the
/// question isn't whether the method is well-formed -- an attacker can
/// send well-formed methods all day -- but whether it is one of the
/// finitely many this service can answer.
fn method_label(method: &axum::http::Method) -> &'static str {
    use axum::http::Method;
    match *method {
        Method::GET => "GET",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::PATCH => "PATCH",
        Method::DELETE => "DELETE",
        Method::HEAD => "HEAD",
        Method::OPTIONS => "OPTIONS",
        Method::TRACE => "TRACE",
        Method::CONNECT => "CONNECT",
        _ => OTHER_METHOD,
    }
}

/// Records one counter and one latency observation per request, plus an
/// in-flight gauge.
///
/// Both labels are drawn from bounded sets, which is what keeps the
/// series count proportional to the size of the route table rather than
/// to the traffic. The route comes from axum's `MatchedPath` -- the
/// *pattern* (`/notifications/{id}`), not the concrete path -- and the
/// method is mapped through `method_label`, since HTTP permits arbitrary
/// extension method tokens and hyper passes them straight through.
pub async fn track_requests(
    State(metrics): State<Arc<ServiceMetrics>>,
    request: Request,
    next: Next,
) -> Response {
    let method = method_label(request.method());
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

    metrics.http_requests(method, &route, response.status().as_u16()).inc();
    metrics.http_duration(method, &route).observe(elapsed);

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
    R: Repositories,
{
    let counts = state.repo.count_by_status(None).await?;
    state.metrics.set_queue_depth(&counts);

    let body = state.metrics.render();
    Ok(([(CONTENT_TYPE, prometheus::CONTENT_TYPE)], body).into_response())
}

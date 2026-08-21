use std::collections::HashMap;
use std::sync::Arc;

use ns_core::{ChannelKind, DeliveryStatus};

use crate::instrument::{Counter, Gauge, Histogram, DEFAULT_LATENCY_BUCKETS};
use crate::labels::{label, Labels};
use crate::prometheus;
use crate::registry::Registry;

/// What became of one dispatch attempt, as the metrics layer sees it.
///
/// Deliberately mirrors `ns_worker::DispatchOutcome` rather than being
/// imported from it: `ns-worker` depends on channels, storage and a
/// runtime, and a metric set that only needs a four-variant label
/// shouldn't drag any of that in. `ns-worker` converts at the call site,
/// which is one `match` and keeps the dependency arrow pointing the sane
/// way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AttemptOutcome {
    Delivered,
    Failed,
    Deferred,
    DeadLettered,
}

impl AttemptOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            AttemptOutcome::Delivered => "delivered",
            AttemptOutcome::Failed => "failed",
            AttemptOutcome::Deferred => "deferred",
            AttemptOutcome::DeadLettered => "dead_lettered",
        }
    }

    pub fn all() -> [AttemptOutcome; 4] {
        use AttemptOutcome::*;
        [Delivered, Failed, Deferred, DeadLettered]
    }
}

/// Why a notification never reached a provider. Recorded separately from
/// the attempt outcome because "deferred" alone doesn't say whether the
/// provider is down or the deployment is simply sending faster than its
/// rate limit allows -- and those call for opposite responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SkipReason {
    RateLimited,
    CircuitOpen,
}

impl SkipReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            SkipReason::RateLimited => "rate_limited",
            SkipReason::CircuitOpen => "circuit_open",
        }
    }

    pub fn all() -> [SkipReason; 2] {
        [SkipReason::RateLimited, SkipReason::CircuitOpen]
    }
}

/// The notification service's metric set: every metric the service
/// exports, resolved once and held as instrument handles.
///
/// Pre-resolving matters for the delivery-path metrics. `ChannelKind` and
/// `AttemptOutcome` are both small closed enums, so every series they can
/// produce is created here at startup and recording one is a map lookup
/// plus a relaxed atomic add -- no allocation, no registry lock. HTTP
/// metrics can't be pre-resolved (the route label is only known per
/// request) and so do take the lock once per request, which is
/// comfortably cheap next to the work of serving one.
pub struct ServiceMetrics {
    registry: Arc<Registry>,
    submitted: HashMap<ChannelKind, Arc<Counter>>,
    attempts: HashMap<(ChannelKind, AttemptOutcome), Arc<Counter>>,
    skips: HashMap<(ChannelKind, SkipReason), Arc<Counter>>,
    send_duration: HashMap<ChannelKind, Arc<Histogram>>,
    queue_depth: HashMap<DeliveryStatus, Arc<Gauge>>,
    polls: Arc<Counter>,
    claimed: Arc<Counter>,
    http_in_flight: Arc<Gauge>,
}

impl ServiceMetrics {
    pub fn new() -> Self {
        Self::with_registry(Arc::new(Registry::new()))
    }

    pub fn with_registry(registry: Arc<Registry>) -> Self {
        let mut submitted = HashMap::new();
        let mut attempts = HashMap::new();
        let mut skips = HashMap::new();
        let mut send_duration = HashMap::new();

        for channel in ChannelKind::all() {
            let channel_label = label("channel", channel.as_str());
            submitted.insert(
                channel,
                registry.counter(
                    "ns_notifications_submitted_total",
                    "Notifications accepted onto the queue.",
                    channel_label.clone(),
                ),
            );
            send_duration.insert(
                channel,
                registry.histogram(
                    "ns_notification_send_duration_seconds",
                    "Wall time of a delivery attempt that reached a provider.",
                    DEFAULT_LATENCY_BUCKETS,
                    channel_label.clone(),
                ),
            );

            for outcome in AttemptOutcome::all() {
                attempts.insert(
                    (channel, outcome),
                    registry.counter(
                        "ns_notification_attempts_total",
                        "Dispatch attempts by channel and outcome.",
                        channel_label.clone().with("outcome", outcome.as_str()),
                    ),
                );
            }

            for reason in SkipReason::all() {
                skips.insert(
                    (channel, reason),
                    registry.counter(
                        "ns_notification_skipped_total",
                        "Dispatches a guard declined before any provider was contacted.",
                        channel_label.clone().with("reason", reason.as_str()),
                    ),
                );
            }
        }

        let queue_depth = DeliveryStatus::all()
            .into_iter()
            .map(|status| {
                let gauge = registry.gauge(
                    "ns_queue_notifications",
                    "Notifications currently in each delivery status.",
                    label("status", status.as_str()),
                );
                (status, gauge)
            })
            .collect();

        let polls = registry.counter(
            "ns_worker_polls_total",
            "Times the worker polled the queue for due work.",
            Labels::new(),
        );
        let claimed = registry.counter(
            "ns_worker_claimed_total",
            "Notifications claimed for processing.",
            Labels::new(),
        );
        let http_in_flight =
            registry.gauge("ns_http_requests_in_flight", "HTTP requests being served.", Labels::new());

        Self {
            registry,
            submitted,
            attempts,
            skips,
            send_duration,
            queue_depth,
            polls,
            claimed,
            http_in_flight,
        }
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// The full exposition text for a `GET /metrics` response.
    pub fn render(&self) -> String {
        prometheus::encode(&self.registry)
    }

    pub fn record_submitted(&self, channel: ChannelKind) {
        // Every ChannelKind is populated in `with_registry`, so the lookup
        // cannot miss -- but a missing metric is never worth taking the
        // process down over, so it's a silent no-op rather than an unwrap.
        if let Some(counter) = self.submitted.get(&channel) {
            counter.inc();
        }
    }

    pub fn record_attempt(&self, channel: ChannelKind, outcome: AttemptOutcome) {
        if let Some(counter) = self.attempts.get(&(channel, outcome)) {
            counter.inc();
        }
    }

    pub fn record_skip(&self, channel: ChannelKind, reason: SkipReason) {
        if let Some(counter) = self.skips.get(&(channel, reason)) {
            counter.inc();
        }
    }

    /// Records how long a delivery attempt took. Only called for attempts
    /// that actually reached a provider: timing a send the rate limiter
    /// refused would report microseconds of doing nothing as if it were
    /// provider latency.
    pub fn observe_send_duration(&self, channel: ChannelKind, seconds: f64) {
        if let Some(histogram) = self.send_duration.get(&channel) {
            histogram.observe(seconds);
        }
    }

    /// Publishes a queue-depth snapshot. Statuses absent from `counts` are
    /// set to zero rather than left at their previous value, so a status
    /// that empties out reads as empty instead of holding its last
    /// non-zero sample forever.
    pub fn set_queue_depth(&self, counts: &HashMap<DeliveryStatus, u64>) {
        for (status, gauge) in &self.queue_depth {
            gauge.set(counts.get(status).copied().unwrap_or(0) as f64);
        }
    }

    pub fn record_poll(&self, claimed: usize) {
        self.polls.inc();
        self.claimed.inc_by(claimed as u64);
    }

    pub fn http_in_flight(&self) -> &Arc<Gauge> {
        &self.http_in_flight
    }

    /// Resolves the counter for one HTTP result. `route` is the matched
    /// route pattern (`/notifications/{id}`), never the concrete path:
    /// using the latter would mint a new time series per notification id
    /// and blow the cardinality budget within a day of real traffic.
    pub fn http_requests(&self, method: &str, route: &str, status: u16) -> Arc<Counter> {
        self.registry.counter(
            "ns_http_requests_total",
            "HTTP requests served, by method, route and response status.",
            Labels::new().with("method", method).with("route", route).with("status", status.to_string()),
        )
    }

    pub fn http_duration(&self, method: &str, route: &str) -> Arc<Histogram> {
        self.registry.histogram(
            "ns_http_request_duration_seconds",
            "Wall time spent serving an HTTP request.",
            DEFAULT_LATENCY_BUCKETS,
            Labels::new().with("method", method).with("route", route),
        )
    }
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_channel_and_outcome_has_a_series_from_the_start() {
        let metrics = ServiceMetrics::new();
        let text = metrics.render();

        for channel in ChannelKind::all() {
            for outcome in AttemptOutcome::all() {
                let expected = format!(
                    "ns_notification_attempts_total{{channel=\"{}\",outcome=\"{}\"}} 0",
                    channel.as_str(),
                    outcome.as_str()
                );
                assert!(text.contains(&expected), "missing series: {expected}");
            }
        }
    }

    #[test]
    fn recording_an_attempt_increments_only_its_own_series() {
        let metrics = ServiceMetrics::new();
        metrics.record_attempt(ChannelKind::Email, AttemptOutcome::Delivered);
        metrics.record_attempt(ChannelKind::Email, AttemptOutcome::Delivered);
        metrics.record_attempt(ChannelKind::Sms, AttemptOutcome::Failed);

        let text = metrics.render();
        assert!(text.contains("ns_notification_attempts_total{channel=\"email\",outcome=\"delivered\"} 2"));
        assert!(text.contains("ns_notification_attempts_total{channel=\"sms\",outcome=\"failed\"} 1"));
        assert!(text.contains("ns_notification_attempts_total{channel=\"sms\",outcome=\"delivered\"} 0"));
    }

    #[test]
    fn submissions_and_skips_are_tracked_per_channel() {
        let metrics = ServiceMetrics::new();
        metrics.record_submitted(ChannelKind::Push);
        metrics.record_skip(ChannelKind::Push, SkipReason::RateLimited);

        let text = metrics.render();
        assert!(text.contains("ns_notifications_submitted_total{channel=\"push\"} 1"));
        assert!(text.contains("ns_notification_skipped_total{channel=\"push\",reason=\"rate_limited\"} 1"));
        assert!(text.contains("ns_notification_skipped_total{channel=\"push\",reason=\"circuit_open\"} 0"));
    }

    #[test]
    fn send_durations_land_in_the_latency_histogram() {
        let metrics = ServiceMetrics::new();
        metrics.observe_send_duration(ChannelKind::Webhook, 0.02);

        let text = metrics.render();
        assert!(
            text.contains("ns_notification_send_duration_seconds_count{channel=\"webhook\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("ns_notification_send_duration_seconds_bucket{channel=\"webhook\",le=\"0.025\"} 1")
        );
        assert!(
            text.contains("ns_notification_send_duration_seconds_bucket{channel=\"webhook\",le=\"0.01\"} 0")
        );
    }

    #[test]
    fn queue_depth_zeroes_statuses_that_emptied_out() {
        let metrics = ServiceMetrics::new();
        let mut counts = HashMap::new();
        counts.insert(DeliveryStatus::Queued, 12);
        metrics.set_queue_depth(&counts);
        assert!(metrics.render().contains("ns_queue_notifications{status=\"queued\"} 12"));

        metrics.set_queue_depth(&HashMap::new());
        let text = metrics.render();
        assert!(text.contains("ns_queue_notifications{status=\"queued\"} 0"), "{text}");
    }

    #[test]
    fn poll_counters_track_batches_and_claims() {
        let metrics = ServiceMetrics::new();
        metrics.record_poll(3);
        metrics.record_poll(0);

        let text = metrics.render();
        assert!(text.contains("ns_worker_polls_total 2"), "{text}");
        assert!(text.contains("ns_worker_claimed_total 3"), "{text}");
    }

    #[test]
    fn http_series_are_keyed_by_method_route_and_status() {
        let metrics = ServiceMetrics::new();
        metrics.http_requests("GET", "/notifications/{id}", 200).inc();
        metrics.http_requests("GET", "/notifications/{id}", 404).inc();
        metrics.http_duration("GET", "/notifications/{id}").observe(0.004);

        let text = metrics.render();
        assert!(text.contains(
            "ns_http_requests_total{method=\"GET\",route=\"/notifications/{id}\",status=\"200\"} 1"
        ));
        assert!(text.contains(
            "ns_http_requests_total{method=\"GET\",route=\"/notifications/{id}\",status=\"404\"} 1"
        ));
        assert!(text.contains(
            "ns_http_request_duration_seconds_count{method=\"GET\",route=\"/notifications/{id}\"} 1"
        ));
    }

    #[test]
    fn in_flight_gauge_returns_to_zero() {
        let metrics = ServiceMetrics::new();
        metrics.http_in_flight().inc();
        metrics.http_in_flight().inc();
        metrics.http_in_flight().dec();
        metrics.http_in_flight().dec();
        assert!(metrics.render().contains("ns_http_requests_in_flight 0"));
    }

    #[test]
    fn two_handles_over_one_registry_share_their_counters() {
        let registry = Arc::new(Registry::new());
        let api = ServiceMetrics::with_registry(Arc::clone(&registry));
        let worker = ServiceMetrics::with_registry(Arc::clone(&registry));

        api.record_submitted(ChannelKind::Email);
        worker.record_submitted(ChannelKind::Email);

        assert!(api.render().contains("ns_notifications_submitted_total{channel=\"email\"} 2"));
    }
}
